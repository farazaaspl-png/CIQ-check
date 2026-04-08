import logging
from pathlib import Path
import nest_asyncio, os, asyncio
from sqlalchemy import text, inspect
from typing import Dict, List
import pendulum
from core.dellattachments import DellAttachments
from core.dispatcher import Dispatcher,SUPPORTED_FILES
from core.emailNotification import notify_failures, notify_feedbacks
from core.exceptions import UnExpectedError
from core.s3_helper import StorageManager
from kafka_framework.consumer import MessageHandler
from kafka_framework.producer import KafkaMessageProducer


# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# from core.embedding.vectorizer import VectorInterface
from core.embedding.vectorizer_content import ContentVectorInterface
from core.db.crud import DatabaseManager
# from services.gtl_feedback.summarizerMigration import Summarizer
from core.utility import get_custom_logger
from config import Configuration
from services.gtl_feedback.summarizerMigration import Summarizer

cfg = Configuration()

logger = get_custom_logger(__name__)
# logger.propagate = False

nest_asyncio.apply()

HARDCODED_REQUEST_ID = "00000000-0000-0000-0000-000000000001" 

class GTLFeedbackHandler(MessageHandler):
    """Handles summarization and recommendation requests"""
    
    def __init__(self, producer: KafkaMessageProducer, output_topic: str, debug: bool = False):
        self.producer = producer
        self.output_topic = output_topic
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
    
    def get_message_types(self) -> List[str]:
        return ['IP_RECOMMENDATION_UPDATE','IP_RECOMMENDATION_DELETE','IP_RECOMMENDATION_FEEDBACK']

    def send_notification(self,header, exp):
        context = header.copy()
        context['error_text'] = exp.error_message
        notify_failures(context,f'GTL Feedback|{exp.error_code}') 

    def handle(self, header: Dict, payload: Dict) -> None:
        logger.info("Feedback proces started")
        """Business logic for summarization and recommendation for SOW"""

        # vector = VectorInterface(cfg.COLLECTIONNAME,cfg.DEBUG)
        eventSubType = header.get('eventSubType','')
        eventType = header.get('eventType','')
        # requestId = header.get('requestId')
        fuuid = payload.get('uuid')
        cfg.load_active_config()
        if eventSubType in ('ARCHIVE_DOCUMENT','APPROVE_DOCUMENT','SEND_FEEDBACK') or eventType == 'IP_RECOMMENDATION_DELETE':
            try:
                status = {'ARCHIVE_DOCUMENT':'ARCHIVED',
                          'APPROVE_DOCUMENT':'APPROVED',
                          'REPROCESS_DOCUMENT':'REPROCESS_REQUESTED',
                          'IP_RECOMMENDATION_DELETE':'DELETED',
                          'SEND_FEEDBACK':'FEEDBACK_RECEIVED'}
                dafileid = payload.get('daFileId',payload.get('uuid'))

                db = DatabaseManager(cfg.DEBUG)
                updated = db.update_document(where_clause={'fuuid': fuuid, 'dafileid': dafileid},#,'status': 'Sent For Review'},
                                             update_values={'status': status[header.get('eventSubType',header.get('eventType', ''))],
                                                            'ipid': payload.get('ipId')}
                )
                if not updated:
                    updated = db.update_document(where_clause={'fuuid':fuuid, 'daoriginalfileid': dafileid},#,'status': 'Sent For Review'},
                                                 update_values={'status': status[header.get('eventSubType',header.get('eventType', ''))],
                                                                'ipid': payload.get('ipId')}
                    )
                logger.info(f"{fuuid} - Update status of file dafileid={dafileid} to {payload.get('status')}")

                
                if eventSubType == 'APPROVE_DOCUMENT':
                    type = payload.get('type','others')
                    if type=='weblink' or not updated:
                        rowdict = {'requestid': header.get('requestId'),
                                   'fuuid' : fuuid,
                                   'daoriginal_fileid' : dafileid,
                                   'ipid' : payload.get('ipId'),
                                   'document_type' : payload.get('source'),
                                   'filename' : payload.get('name'),
                                   'description' : payload.get('Description'),
                                   'offer' : payload.get('offer'),
                                   'dtpm_phase' : payload.get('phase'),
                                   'projectid' : payload.get('projectId'),
                                   'ip_type' : '|'.join(payload.get('ipTypes',[])),
                                   'created_by' : 'System',
                                   'status' : payload.get('status'),
                                   'type' : payload.get('type'),
                                   'url' : payload.get('url'),
                                   'uploadedby' : payload.get('owner')}
                        db.insert_document(**rowdict)
                    if type=='others':
                        vec = ContentVectorInterface(cfg.DOCUMENT_CONTENT_STORE,self.debug)
                        vec.vectorize_by_dafileids([dafileid])
                #     vector.vectorize_documents_by_dafileids([dafileid])
                #     logger.info(f"{requestId} - Document vectorized for approved status, dafileid={dafileid}")

                elif eventSubType =='ARCHIVE_DOCUMENT' or eventType == 'IP_RECOMMENDATION_DELETE':
                    vec = ContentVectorInterface(cfg.DOCUMENT_CONTENT_STORE,self.debug)
                    vec.delete_documents_by_dafileids([dafileid])
                #     vector.delete_documents_by_dafileids([dafileid])
                #     logger.info(f"{requestId} - Vector deleted for dafileid={dafileid} as it was status changed to {eventSubType}")
                # if eventSubType == 'REPROCESS_DOCUMENT':
                #     feedback = {
                #         'fuuid': payload.get('uuid'),
                #         'filename': payload.get('filename'),
                #         'dafileid': payload.get('daFileId'),
                #         'status': 'REPROCESS_REQUESTED',
                #         'feedback': payload.get('reprocessCommand')
                #         }   
                #     db.insert_feedback(**feedback)
                if eventSubType == 'SEND_FEEDBACK':
                    feedback = {
                        'fuuid': payload.get('uuid'),
                        'filename': payload.get('name'),
                        'dafileid': payload.get('daFileId'),
                        'status': 'FEEDBACK_RECEIVED',
                        'feedback': payload.get('feedback')
                        }
                    db.insert_feedback(**feedback)
                    context = header.copy()
                    context['dafileid'] = payload.get('daFileId')
                    context['filename'] = payload.get('name')
                    context['feedback'] = payload.get('feedback')
                    notify_feedbacks(context)
 
            except Exception as e:
                logger.error(f"{fuuid} - Failed to update TDocument status: {e}", exc_info=True)
                self.send_notification(header,UnExpectedError(e))
            # finally:
            #         if 'db' in locals(): del db
        
        elif eventType == 'IP_RECOMMENDATION_UPDATE':

            if eventSubType == 'FILE_INFO_UPDATE':
                try:
                    fuuid = payload.get('uuid')
                    dafileid = payload.get('oldDaFileId',payload.get('daFileId'))
                    db = DatabaseManager(cfg.DEBUG)
                    # vectorcolumns = ['document_type','ip_type','dtpm_phase','practice','offerfamily','offer','filename','title']
                    TDOC_COLS = {c["name"] for c in inspect(db.engine).get_columns(table_name=cfg.DOCUMENT_TABLE,schema=cfg.DATABASE_SCHEMA)}
                    # revectorize = False
                    # dafileid = payload.get('daFileId')
                    if not dafileid:
                        return

                    new_items = {k: v for k, v in payload.items() if k.startswith('new') and k.replace('new','').lower() in TDOC_COLS}
                    if not new_items:
                        return
                    
                    update_values = {}
                    for new_key, new_val in new_items.items():
                        col_name = new_key.replace('new', '').lower()
                        if col_name == 'iptypes' and isinstance(new_val, (list, tuple)):
                            new_val = '|'.join(str(v) for v in new_val)
                            col_name='iptype'
                        # if col_name in vectorcolumns:
                        #     revectorize = True
                        update_values[col_name] = new_val

                        old_val = payload.get(new_key.replace('new', 'old'), '')
                        db.insert_change_document(
                            dafileid=dafileid,
                            column_name=col_name,
                            oldvalue=str(old_val),
                            newvalue=str(new_val),
                            created_by=payload.get('owner', 'system')
                        )

                    updated = db.update_document(
                        where_clause={'fuuid':fuuid, 'dafileid': dafileid},#,'status': 'Processed'},
                        update_values=update_values,
                    )
                    if not updated:
                        db.update_document(
                        where_clause={'fuuid':fuuid, 'daoriginalfileid': dafileid},#,'status': 'Processed'},
                        update_values=update_values,
                    )
                    logger.info(f"tdocument updated for daFileId {dafileid}")
                    # if revectorize:
                    #     vector.update_vectors([dafile_id])
                    if payload.get('oldDaFileId') is not None:
                        df = db.get_vwclassificationout_row(
                            fuuid   = fuuid,
                            dafileid= dafileid
                        )
                        uuid_columns = ['requestid', 'fuuid', 'daoriginal_fileid', 'dafileid', 'dasanitizationoutfileid']
                        df[uuid_columns] = df[uuid_columns].fillna('').astype(str)
                        df = df.drop('requestid', axis=1)

                        logger.info(f"{fuuid} - view returned {df.shape[0]} rows")
                        response_payload = df.to_dict(orient='records')[0]
                        response_payload['requestUuid'] = fuuid

                        # Step 2: build response headers
                        requestid = header.get('requestId')
                        response_headers = {
                            'eventType'   : 'IP_GOLDEN_COPY_REQUEST_RECOMMENDATION_ACK',
                            'eventSubType': 'PROCESSED_FILE',
                            'createdOn'   : pendulum.now("UTC").format("ddd MMM DD HH:mm:ss [UTC] YYYY"),
                            'requestId'   : requestid,
                        }
                        self.producer.send_message(self.output_topic,response_headers,response_payload)
                        logger.info(f"{fuuid} - Updated payload message sent ")


                except Exception as e:
                    logger.error(f"Failed to process FILE_INFO_UPDATE: {e}", exc_info=True)
                    self.send_notification(header,UnExpectedError(e))
                # finally:
                #     if 'db' in locals(): del db
            elif eventSubType == 'MIGRATION_APPROVE':
                                
                try:
                    logger.info(f"Starting migration approve processing for document: {payload.get('name')}")
                    doc_dict = {
                        'requestid': HARDCODED_REQUEST_ID,
                        'fuuid': payload.get('uuid'),
                        'daoriginal_fileid': payload.get('daFileId'),
                      #  'dafileid': payload.get('daFileId'),
                        'filename': payload.get('name'),
                        'dtpm_phase': payload.get('phase'),
                        'ip_type': '|'.join(payload.get('ipTypes', [])),  # added default empty list 
                        'document_type': payload.get('source'),
                        'offer': payload.get('offer'),
                        'status': payload.get('status'),
                        'ipid': payload.get('ipId'),
                        'initialgrade': payload.get('initialGrade'),
                        'priority': payload.get('priority'),
                        'created_by': 'system'
                    }
    
                    db = DatabaseManager(cfg.DEBUG)
                    db.insert_document(**doc_dict)
                    logger.info(f"Document inserted into DB")

                    if Path(payload.get('name')).suffix.lower() in SUPPORTED_FILES:
                        result = self.generate_summary(header, payload)
                        if result is not None:
                            db.update_document(where_clause={'requestid': header.get('requestId'),
                                                             'daoriginal_fileid': payload.get('daFileId'),
                                                             'fuuid': payload.get('uuid') },
                                               update_values={'title': result.get('title'),
                                                              'description': result.get('description'),
                                                              'gtl_synopsis': result.get('gtl_synopsis'),
                                                              'updated_by': 'system'})
                        logger.info(f"Document updated with title and description")
                except Exception as e:
                    logger.error(f"Failed to process document for migration approve: {e}", exc_info=True)
                    self.send_notification(header,UnExpectedError(e))
                # finally:
                #     if 'db' in locals(): del db
        else: 
            logger.error(f"Invalid event type: {header.get('eventType')}", exc_info=True)
            self.send_notification(header,UnExpectedError(e))
            return
    logger.info("Feedback recording completed")

    def generate_summary(self,header, payload):
        dafileid = payload.get('daFileId')
        request_dir = Path(os.path.join(cfg.DATA_DIR,cfg.GTL_FLOW_DIR,dafileid))
        filepath = Path(request_dir,payload.get('name'))
        try:
            
            file_dict = {
                'id': dafileid,
                'name': filepath.name,
                'filepath': filepath
            }
            s3 = StorageManager()
            da = DellAttachments()
            file_dict = asyncio.run(da.download(filedict=file_dict))
            logger.info(f"File downloaded from Dell Attachments")
            s3.upload(filepath, overwrite = True)
    
            dispatcher = Dispatcher(filepath, dafileid,analyze_images=cfg.IMAGE_ANALYZE_SWITCH,debug=self.cfg.DEBUG)
            extractor =  dispatcher.getExtractor()
            _ = extractor.extract_content()
            filecontent, _ = extractor.get_filecontent()
    
            if not filecontent.strip():
                return
            sm = Summarizer(dafileid, filecontent, debug=cfg.DEBUG)
            result = sm.summarize()
    
            logger.info(f"Document updated with title and description")
            extractor.clean_up()
            return result

        except Exception as e:
            logger.error(f"Error while generating description and title for approved document: {e}", exc_info=True)
            self.send_notification(header,UnExpectedError(e))
        # vector.vectorize_documents_by_dafileids([dafileid])                
        # logger.info(f"{requestId} - Document vectorized for approved status, dafileid={dafileid}")


