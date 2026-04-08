from pathlib import Path
import pandas as pd
import asyncio, pendulum, nest_asyncio
import logging
import os
from core.dellattachments import DellAttachments
from core.emailNotification import notify_failures
from kafka_framework.consumer import MessageHandler
from kafka_framework.producer import KafkaMessageProducer
from typing import Dict, List
from sqlalchemy import text as querytext
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.consultant_recommendation.Main import main
from core.db.crud import DatabaseManager
# from services.consultant_recommendation.search_documents import search_documents
from services.consultant_recommendation.recommendations import get_recommendations
# from services.summarization.models import RecommendationRequest, SummaryResponse 
from core.exceptions import CustomBaseException, DatabaseReadError, DatabaseWriteError, DellAttachmentsApiError, FileFormatNotSupported,InvalidMetadataError, SOWFileNotFoundOnDellAttachments, UnExpectedError, UnableToFindAnyRecommendation
# from config import Config as cfg
from config import Configuration
from core.utility import get_custom_logger
cfg = Configuration()
logger = get_custom_logger(__name__)
SUPPORTED_FILES = ['.pdf','.docx']
# logger.propagate = False
nest_asyncio.apply()

class Consultant_Recommendation(MessageHandler):
    """Handles summarization and recommendation requests"""
    
    def __init__(self, producer: KafkaMessageProducer, output_topic: str, debug: bool=False):
        self.producer = producer
        self.output_topic = output_topic
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.semaphore = asyncio.Semaphore(os.cpu_count())
    
    def get_message_types(self) -> List[str]:
        return ['PROJECT_REQUEST_RECOMMENDATION']
    
    def send_failure(self, reqheader: Dict, payload: Dict, project_id : str,stage : str,  eventSubType:str ="PROCESSING_ERROR"):
        context = reqheader.copy()
        context['error_text'] = payload.get('error_message')
        notify_failures(context,f'Consult Flow|{stage}|{payload.get('error_code')}')
        
        response_headers = {
            'eventType': "PROJECT_REQUEST_RECOMMENDATION_ACK",
            'eventSubType': eventSubType,
            'createdOn': pendulum.now("UTC").format("ddd MMM DD HH:mm:ss [UTC] YYYY"),
            'requestId': reqheader.get('requestId')
        }
         
        self.producer.send_message(
            self.output_topic,
            response_headers,
            payload
        )
        response_headers["eventSubType"] = "STREAM_COMPLETION"

        statuspayload={"FF_ProjectId": project_id,
                       "Stage": stage,
                       "Status": "Completed"}
            
        self.producer.send_message(
                self.output_topic,
                response_headers,
                statuspayload
            )

    def _check_all_messages_received(self, request_id: str) -> None:
        try:
            db = DatabaseManager(cfg.DEBUG)
            qry = querytext(f"""select case when sqry.idx=sqry.total_count then true else false end receivedallmessages 
                               from (select event.request_id,total_count,idx,row_number() over(order by idx desc) rnk 
                                      from {cfg.DATABASE_SCHEMA}.{cfg.AN_EVENT_TABLE} event 
                                     where event.event_sub_type ='SOW_FILE' 
                                       and event.request_id = (:request_id)) sqry 
                              where rnk =1""")
            logger.debug(f"{request_id} - Query: {qry}")
            params={'request_id': request_id}
            received_all = db.query_database(qry,params=params)

            if received_all.shape[0] == 0:
                raise DatabaseReadError(error=f"No messages received for requestid: {request_id}")
            
            logger.info(f"{request_id} - Received all messages: {received_all.receivedallmessages.iloc[0]}")
            return received_all.receivedallmessages[0]
        except DatabaseReadError as db_err:
            logger.error(f'Failed while getting latest SOW messages from DB: {db_err}', exc_info=True)
            raise db_err
        # finally:
        #     if 'db' in locals(): del db
        # pass

    async def get_latest_sow_request(self, request_id: str):
        for attempt in range(1,6):
            await asyncio.sleep(5*attempt)
            if self._check_all_messages_received(request_id):
                break
            if attempt == 5:
                raise DatabaseReadError(error=f"No messages received for requestid: {request_id}")
            
            logger.info(f'{request_id} - Waiting 5 secs for SOW messages Attempt: {attempt}')
        try:
            db = DatabaseManager(cfg.DEBUG)
            qry = querytext(f"""select event_type,
                                      event_sub_type,
                                      created_on,
                                      request_id,
                                      idx,
                                      total_count,
                                      payload,
                                      payload->'daFileId' dafileid,
                                      payload->'name' filename,
                                      payload->'projectId' projectId 
                                  from {cfg.DATABASE_SCHEMA}.{cfg.AN_EVENT_TABLE} event 
                                  where event.event_sub_type ='SOW_FILE' 
                                  and event.request_id in (:request_id)""")
            logger.debug(f"{request_id} - Query: {qry}")
            params={'request_id': request_id}
            df = db.query_database(qry,params=params)
            logger.debug(f'{request_id} - {df.shape}')
            if df[('.'+df['filename'].str.split('.').str[-1].str.lower()).isin(SUPPORTED_FILES)].shape[0]==0:
                raise FileFormatNotSupported(fileformat = ','.join(df['filename'].str.split('.').str[-1].drop_duplicates().to_list()))
            else:
                df=df[('.'+df['filename'].str.split('.').str[-1].str.lower()).isin(SUPPORTED_FILES)]
            try:
                logger.info(f'{request_id} - getting files metadata from DellAttachments for project id {df.projectid.drop_duplicates().to_list()[0]}')
                da = DellAttachments(cfg.DEBUG)
                dalist = await da.getListOfFile(metadata = {'FF_ProjectId': df.projectid.drop_duplicates().to_list()[0]})
                dalist.sort_values('updatedDate',ascending=False)
                logger.debug(f'{request_id} - {dalist.shape[0]} files found on DellAttachments')
                logger.debug(f'{request_id} - Records rececived from DellAttachments:\n{dalist}')

                logger.info(f'{request_id}  - matching with dafileids from payload received to DellAttachments')
                dfout=dalist[['id','name','updatedDate']].set_index('id').join(df.set_index('dafileid'),how='inner').reset_index()
                if dfout.shape[0]==0:
                    raise SOWFileNotFoundOnDellAttachments()
                latestrow = dfout.sort_values('updatedDate',ascending=False).iloc[0]
            except DellAttachmentsApiError as da_err:
                logger.warning(f'{request_id} - Failed while getting latest SOW messages from DellAttachments: {da_err}', exc_info=True)
                logger.info(f'{request_id} - Using file with max index')
                latestrow = df.sort_values('idx',ascending=False).iloc[0]


            logger.debug(f'{request_id} - payload selected - {latestrow}')
            header_dict = {
                                "eventType": latestrow.event_type,
                                "eventSubType": latestrow.event_sub_type,
                                "createdOn": latestrow.created_on.isoformat() if latestrow.created_on else None,
                                "requestId": str(latestrow.request_id) if latestrow.request_id else None,
                                "index": latestrow.idx,
                                "totalCount": latestrow.total_count
                            }
            payload = latestrow.payload
        except DatabaseReadError as db_err:
            logger.error(f'Failed while getting latest SOW messages from DB: {db_err}', exc_info=True)
            raise db_err
        return header_dict, payload
    
    async def async_send_recommendation_messages(self, request_id, recommendations: list, response_headers: dict):
        async def send_recommendation_messages(idx, request_id,rec:dict,response_headers:dict) -> None:
            async with self.semaphore:
                try:
                    recrequestid = rec.pop('requestid')
                    self.producer.send_message(self.output_topic, response_headers, rec,idx=recrequestid+' - '+str(idx)+' - ')

                    db = DatabaseManager(cfg.DEBUG)
                    db.update_recommendation(
	    				where_clause={
	    					'requestid': recrequestid, 
	    					'projectid': rec['FF_ProjectId'], 
	    					'templateid': rec['templateid']
	    				},
                        update_values={'status': 'SENT'})
                    return 1
                except DatabaseWriteError as db_err:
                    logger.info(f" {idx} - Error while updating status Column for Request-{recrequestid}, Project-{rec['FF_ProjectId']} and Template-{rec['templateid']}: {db_err}")
                    return 1
                except (CustomBaseException,Exception) as e:
                    logger.error(f"{idx} - Error sending recommendation message for fileid {rec['daFileId']}: {str(e)}", exc_info=True)
                    return 0
                
        task=[asyncio.create_task(send_recommendation_messages(idx, request_id, rec, response_headers)) for idx, rec in enumerate(recommendations)]
        results = await asyncio.gather(*task,return_exceptions=False)

        if sum(results) == 0:
            logger.error(f"Failed to send Kafka messages for {len(results)-sum(results)} recommendations")
        # results = asyncio.run(asyncio.gather(*(self.send_recommendation_messages(request_id, rec, response_headers) for rec in recommendations),return_exceptions=True))
    
    def handle(self, header: Dict, payload: Dict) -> None:
        """
        Business logic for summarisation & recommendation.
        Expected header keys
        * ``requestId`` - a **request-level identifier** used for tracing the
          workflow (it is **not** the correlation id passed to downstream
          services)
        * ``eventSubType`` - may be ``STREAM_COMPLETION`` or other values

        Expected payload keys
        * ``projectId`` - used when persisting recommendations
        * other fields required by :func:`services.summarization.main.main`
        """
                
        if header.get("eventSubType") == "STREAM_COMPLETION":
            cfg.load_active_config()
            request_id = header.get("requestId")
            project_id = payload.get("projectId")

            logger.info(f"{request_id} - Started summarisation/recommendation processing for request %s", request_id)
            try:
                header, payload = asyncio.run(self.get_latest_sow_request(request_id))
            except CustomBaseException as e:
                logger.error(f" Unable to get Latest sow: {str(e)}", exc_info=True)
                self.send_failure(header, e.to_dict(),project_id = project_id, stage = "Summarization")
                return
            except Exception as e:
                logger.error(f" Error: {str(e)}", exc_info=True)
                self.send_failure(header, UnExpectedError(e).to_dict(),project_id = project_id, stage = "Summarization")
                return

            project_id = payload.get("projectId")
            try:
                summary_payload_data = asyncio.run(main(header, payload, debug=cfg.DEBUG))
                
                db = DatabaseManager(cfg.DEBUG)
                with db.engine.connect() as conn:
                    query = querytext(f"""SELECT count(1) cnt FROM {cfg.DATABASE_SCHEMA}.{cfg.STATEMENTOFWORK_VIEW} where requestid =:request_id""")
                    recdf = pd.read_sql(query, conn, params={'request_id': request_id})
                    conn.commit()

                response_headers = {
                                    "eventType": "PROJECT_REQUEST_RECOMMENDATION_ACK",
                                    "eventSubType": "SOW_SUMMARY",
                                    "createdOn": pendulum.now("UTC").format(
                                        "ddd MMM DD HH:mm:ss [UTC] YYYY"
                                    ),
                                    "requestId": request_id,
                                }
                
                if recdf.cnt[0]==0:
                    summary_payload_data['summary'] += "<br><b>Note:</b><b><i>No Consulting offers match the Scope of Services, please use the refine recommendation feature and provide further search parameters</i></b>"
                    self.producer.send_message(self.output_topic, response_headers, summary_payload_data)

            except CustomBaseException as e:
                logger.error(f" {str(e)}", exc_info=True)
                self.send_failure(header, e.to_dict(),project_id = project_id, stage = "Summarization")
                return
            except Exception as e:
                logger.error(f" {str(e)}", exc_info=True)
                self.send_failure(header, UnExpectedError(e).to_dict(),project_id = project_id, stage = "Summarization")
                return
            
            
            if recdf.cnt[0]!=0:
                filestorecommend = False
                try:
                    recommendations = asyncio.run(get_recommendations(header, payload, debug=cfg.DEBUG))
                    filestorecommend = True
                except UnableToFindAnyRecommendation as norec:
                    logger.error(f" Unable to find recommendation for sow: {str(norec)}", exc_info=True)
                    summary_payload_data['summary'] += "<br><b>Note:</b><b><i>There is currently no IP available for the associated offer, please use the refine recommendation feature and provide further search parameters</i></b>"
                    filestorecommend = False
                except CustomBaseException as e:
                    logger.error(f" {str(e)}", exc_info=True)
                    self.send_failure(reqheader = header,
                                      payload =  e.to_dict(),
                                      project_id = project_id, 
                                      stage = "Recommendation")
                    return
                except Exception as e:
                    logger.error(f"{str(e)}", exc_info=True)
                    self.send_failure(reqheader = header,
                                      payload = UnExpectedError(e).to_dict(),
                                      project_id = project_id, 
                                      stage = "Recommendation")
                    return
                
                self.producer.send_message(self.output_topic, response_headers, summary_payload_data)
                if filestorecommend:
                    response_headers["eventSubType"] = "RECOMMENDED_FILE"
                    asyncio.run(self.async_send_recommendation_messages(request_id, recommendations, response_headers))

            # task=[asyncio.create_task(self.send_recommendation_messages(request_id, rec, response_headers)) for rec in recommendations]
            # results = asyncio.run(asyncio.gather(*task,return_exceptions=True))
            # results = asyncio.run(asyncio.gather(*(self.send_recommendation_messages(request_id, rec, response_headers) for rec in recommendations),return_exceptions=True))

            # if sum(results) == 0:
            #     logger.error(f"{request_id} - Failed to send Kafka messages for {len(results)-sum(results)} recommendations")

            response_headers["eventSubType"] = "STREAM_COMPLETION"
            statuspayload={"FF_ProjectId": project_id,
                           "Status": "Completed",
                           "Stage":"Both"}

            self.producer.send_message(
                    self.output_topic,
                    response_headers,
                    statuspayload
                )
            
        # elif header.get("eventSubType") == "REFINE_RECOMMENDATION":
        #     missing = [key for key in ["projectId","phases","refineText"] if key not in payload.keys()]
        #     missing.extend([key for key in ["requestId"] if key not in header.keys()])
        #     project_id = payload.get("projectId")
        #     request_id = header.get("requestId")
        #     request_uuid = payload.get("uuid")
        #     if len(missing)>0:
        #         logger.error(f"{request_id} - Unexpected error on {InvalidMetadataError(missing)}")
        #         self.send_failure(header, InvalidMetadataError(missing).to_dict(),project_id=project_id, stage = "RefineRecommendation")
        #         return

        #     response_headers = {
        #         "eventType": "PROJECT_REQUEST_RECOMMENDATION_ACK",
        #         "eventSubType": "SEARCHED_FILE",
        #         "createdOn": pendulum.now("UTC").format("ddd MMM DD HH:mm:ss [UTC] YYYY"),
        #         "requestId": request_id,
        #     }
            
        #     try:
        #         # payload.pop("requestId")
        #         searchedfiles = asyncio.run(search_documents(header, payload, debug=cfg.DEBUG))
        #     except CustomBaseException as exc:
        #         logger.error(f"{request_id} - {exc}", exc_info=True)
        #         self.send_failure(header, exc.to_dict(),project_id=project_id, stage = "RefineRecommendation")
        #         return
        #     except Exception as e:
        #         self.send_failure(header, UnExpectedError(e).to_dict(),project_id = project_id, stage = "RefineRecommendation")
        #         return
            
        #     asyncio.run(self.async_send_recommendation_messages(request_id, searchedfiles, response_headers))
        #     # tasks=[asyncio.create_task(self.send_recommendation_messages(request_id, rec, response_headers)) for rec in searchedfiles]
        #     # results = asyncio.run(asyncio.gather(*tasks, return_exceptions=True))
        #     # results = asyncio.run(asyncio.gather(*(self.send_recommendation_messages(request_id, rec, response_headers) for rec in searchedfiles),return_exceptions=True))


        #     # if sum(results) == 0:
        #     #     logger.error(f"{request_id} - Failed to send Kafka Messages for {len(results)-sum(results)} searchedfiles")
                                
        #     response_headers["eventSubType"] = "STREAM_COMPLETION"
        #     statuspayload = {
        #         "FF_ProjectId": project_id,
        #         'requestUuid': request_uuid,
        #         "Status": "Completed",
        #         "Stage":"RefineRecommendations"
        #     }
        #     self.producer.send_message(
        #         self.output_topic,
        #         response_headers,
        #         statuspayload
        #     )

        # else:
        #     return
        