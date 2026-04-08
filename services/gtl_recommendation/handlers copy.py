from pathlib import Path
from zipfile import BadZipFile
import pendulum, asyncio#, pandas as pd
import logging
from typing import Dict, List
import nest_asyncio
# from sqlalchemy import inspect, text as querytext

from core.db.crud import DatabaseManager
from core.dispatcher import SUPPORTED_FILES
from kafka_framework.consumer import MessageHandler
from kafka_framework.producer import KafkaMessageProducer
from services.gtl_recommendation.Main import main
from core.exceptions import CustomBaseException, FileFormatNotSupported, UnExpectedError,InvalidMetadataError
from config import Config as cfg
nest_asyncio.apply()

from core.utility import get_custom_logger
logger = get_custom_logger(__name__)
# logger.propagate = False

# SUPPORTED_IP_TYPES = ['discovery & assessment reports','designs','build config guides','procedure & runbooks','executive presentation']
class ProcessIpHandler(MessageHandler):
    """Handles document sanitization and redaction requests"""

    def __init__(self, producer: KafkaMessageProducer, output_topic: str, debug: bool = False):
        self.producer = producer
        self.output_topic = output_topic
        self.debug = debug
        self.db = DatabaseManager()
        if self.debug:
            logger.setLevel(logging.DEBUG)

    def get_message_types(self) -> List[str]:
        return ['IP_GOLDEN_COPY_REQUEST_RECOMMENDATION']
    
    def send_failure(self, request_id: str, payload: Dict,eventSubType:str ="PROCESSING_ERROR") -> None:
        
        failure_headers = {
            "eventType": "IP_GOLDEN_COPY_REQUEST_RECOMMENDATION_ACK",
            "eventSubType": eventSubType,
            "createdOn": pendulum.now("UTC").format("ddd MMM DD HH:mm:ss [UTC] YYYY"),
            "requestId": request_id,
        }
        self.producer.send_message(
            self.output_topic, failure_headers, payload
        )

    def handle(self, header: Dict, payload: Dict, **kwargs) -> None:
        """Business logic for sanitization and redaction of IP"""

        request_id = header.get("requestId")
        fUuid = payload.get("uuid")
        
        missing_fields = []
        # if not project_id : missing_fields.append("projectId")
        if not request_id : missing_fields.append("requestId")
        if not fUuid : missing_fields.append("uuid")

        if len(missing_fields)>0:
            fpayload = InvalidMetadataError(missing_fields).to_dict()
            logger.error(f"fUuid: {fUuid}-{fpayload}")
            fpayload['requestUuid'] = fUuid
            self.send_failure(request_id, fpayload)
            return
        
        if Path(payload["name"].strip()).suffix.lower() not in SUPPORTED_FILES:
            fpayload = FileFormatNotSupported(fileformat=Path(payload["name"]).suffix).to_dict()
            logger.error(f"fUuid: {fUuid}-{fpayload}")
            fpayload['requestUuid'] = fUuid
            self.send_failure(request_id, fpayload)
            return
        
        # if header.get("eventSubType") not in ("UPLOAD_NEW_FILE","MANUAL_UPLOAD"):
        #     ip_types  = payload.get("ipTypes")
        #     ip_types = [ip_type for ip_type in ip_types if ip_type is not None]
            # try:
            #     db = DatabaseManager()
            #     with db.engine.connect() as conn:
            #         query = querytext(f"""SELECT distinct lower(ip_type) as ip_types FROM {cfg.DATABASE_SCHEMA}.{cfg.DTPMMAPPING_VIEW}""")
            #         logger.info(f"{request_id} - Getting list of supported ip types from database")
            #         supportedips = pd.read_sql(query, conn)
            #         conn.commit()
            #     SUPPORTED_IP_TYPES =supportedips['ip_types'].tolist()
            #     logger.info(f"{request_id} - Getting list of supported ip types from database")
            # except Exception as e:
            #     logger.error(f"{request_id} - Failed to get list of supported ip types: {e}", exc_info=True)
            #     SUPPORTED_IP_TYPES = ['discovery & assessment reports','designs','build config guides','procedure & runbooks','executive presentation']        
            
            # if len(ip_types)> 0:
            #     chk_ip_types = [ip_type for ip_type in ip_types if ip_type.lower() in SUPPORTED_IP_TYPES]
            # else:
            #     chk_ip_types = ["Not Provided"]
            # if len(chk_ip_types) == 0:
            #     fpayload = IpTypeNotSupported(','.join(ip_types)).to_dict()
            #     logger.error(f"fUuid: {fUuid}-{IpTypeNotSupported(','.join(ip_types))}")
            #     fpayload['requestUuid'] = fUuid
            #     fpayload['ipTypes'] = payload.get('ipTypes')
            #     fpayload['phase'] = payload.get("phase")
            #     self.send_failure(request_id, fpayload)
            #     return
        # else:
        #     payload['ipTypes'] = [ip_type for ip_type in payload.get("ipTypes") if ip_type is not None or ip_type.lower() not in ('null')]
        iptype = [ip_type for ip_type in payload.get("ipTypes") if ip_type is not None]
        if len(iptype)>0:
            payload['ipTypes'] = iptype  
        else:
            payload.pop('ipTypes')

        try:
            if header.get("eventSubType") == "UPLOAD_NEW_FILE":
                self.db.update_document(
                    where_clause={'dafileid': payload.get('oldDaFileId')},
                    update_values={'status': 'REJECTED'}
                )
                logger.info(f"{request_id} - Status updated for old dafileid {payload.get('oldDaFileId')} to REJECTED")
            self.db.update_document(where_clause={'daoriginal_fileid': payload.get('daFileId')},
                                    update_values={'status': 'Rejected'}) 

            rowdict = {'requestid': request_id,
                   'daoriginal_fileid' : payload.get('daFileId'),
                   'dafileid' : payload.get('uuid'),
                   'filename' : payload.get('name'),
                   'dtpm_phase' : payload.get('phase'),
                   'ip_type' : '|'.join(payload.get('ipTypes',[])), #payload.get('phase',None)
                   'ipid' : payload.get('ipId'),
                   'document_type' : 'Field Template',
                   'created_by' : 'System',
                   'status' : 'STARTED',
                   'projectid' : payload.get('projectId'),
                   'uploadedby' : payload.get('uploadedBy')}
            self.db.insert_document(**rowdict)
            logger.info(f"{request_id} - Inserted row in database {payload.get('daFileId')}")
            status_headers = {
                "eventType": "IP_GOLDEN_COPY_REQUEST_RECOMMENDATION_ACK",
                "eventSubType": "STATUS",
                "createdOn": pendulum.now("UTC").format("ddd MMM DD HH:mm:ss [UTC] YYYY"),
                "requestId": request_id,
            }
            status_payload = {
                "requestUuid": fUuid,
                "stage": "PIPELINE",
                "status": "STARTED",
                "message": "Classification and redaction started",
            }
            self.producer.send_message(self.output_topic, status_headers, status_payload)

        except UnExpectedError as e:
            logger.warning(f"{request_id} - Failed to upsert database entries: {e}", exc_info=True)
            fpayload = e.to_dict()
            fpayload['requestUuid'] = fUuid
            fpayload['ipTypes'] = payload.get('ipTypes')
            fpayload['phase'] = payload.get("phase")
            self.send_failure(request_id, fpayload)
            return


        response_headers = {
            "eventType": "IP_GOLDEN_COPY_REQUEST_RECOMMENDATION_ACK",
            "eventSubType": "PROCESSED_FILE",
            'createdOn': pendulum.now("UTC").format("ddd MMM DD HH:mm:ss [UTC] YYYY"),
            "requestId": request_id,
        }

        try:
            # response_payload = asyncio.run(main(header, payload, debug=cfg.DEBUG))
            response_payload = asyncio.run(
                                            main(header, payload, 
                                                debug=cfg.DEBUG, 
                                                producer=self.producer)
                                        )
        except UnExpectedError as e:
            logger.error(f"fUuid: {fUuid}-Unexpected error on {e}", exc_info=True)
            fpayload = e.to_dict()
            fpayload['requestUuid'] = fUuid
            fpayload['ipTypes'] = payload.get('ipTypes')
            fpayload['phase'] = payload.get("phase")
            self.send_failure(request_id, fpayload)
            return
        except CustomBaseException as e:
            logger.error(f"fUuid: {fUuid}-{e}", exc_info=True)
            fpayload = e.to_dict()
            fpayload['requestUuid'] = fUuid
            fpayload['ipTypes'] = payload.get('ipTypes')
            fpayload['phase'] = payload.get("phase")
            self.send_failure(request_id, fpayload)
            return
        except Exception as e:
            logger.error(f"fUuid: {fUuid}-{e}", exc_info=True)
            fpayload = UnExpectedError(e).to_dict()
            fpayload['requestUuid'] = fUuid
            fpayload['ipTypes'] = payload.get('ipTypes')
            fpayload['phase'] = payload.get("phase")
            self.send_failure(request_id, fpayload)
            return
        

        response_payload['requestUuid'] = fUuid
        self.producer.send_message(
            self.output_topic, response_headers, response_payload
        )
        self.db.update_document(where_clause={'requestid': request_id,'daoriginal_fileid': payload.get('daFileId')},update_values={'status': 'Sent For Review'}) 