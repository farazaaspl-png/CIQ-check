from pathlib import Path
import pendulum, asyncio#, pandas as pd
import logging
from typing import Dict, List
import nest_asyncio
# from sqlalchemy import inspect, text as querytext

from core.db.crud import DatabaseManager
# from core.dispatcher import SUPPORTED_FILES
from core.emailNotification import notify_failures, notify_feedbacks
from kafka_framework.consumer import MessageHandler
from kafka_framework.producer import KafkaMessageProducer
from services.gtl_recommendation.Main import main
from services.gtl_recommendation.zip_summarization.Main import main as main_zip
from core.exceptions import CustomBaseException, UnExpectedError,InvalidMetadataError

nest_asyncio.apply()

from core.utility import get_custom_logger
logger = get_custom_logger(__name__)
# logger.propagate = False

class Gtl_Recommendation(MessageHandler):
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
    
    def send_failure(self, reqheader: str, payload: Dict,eventSubType:str ="PROCESSING_ERROR") -> None:
        context = reqheader.copy()
        context['error_text'] = payload.get('error_message')
        notify_failures(context,f'GTL Flow|{payload.get('error_code')}|{payload.get('requestUuid')}')

        failure_headers = {
            "eventType": "IP_GOLDEN_COPY_REQUEST_RECOMMENDATION_ACK",
            "eventSubType": eventSubType,
            "createdOn": pendulum.now("UTC").format("ddd MMM DD HH:mm:ss [UTC] YYYY"),
            "requestId": reqheader.get('requestId'),
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
            logger.error(f"Required fields missing: {missing_fields}")
            fpayload['requestUuid'] = fUuid
            self.send_failure(header, fpayload)
            return
        
        iptype = [ip_type for ip_type in payload.get("ipTypes") if ip_type is not None]
        if len(iptype)>0:
            payload['ipTypes'] = iptype  
        else:
            payload.pop('ipTypes')

        # response_headers = {
        #     "eventType": "IP_GOLDEN_COPY_REQUEST_RECOMMENDATION_ACK",
        #     "eventSubType": "PROCESSED_FILE",
        #     'createdOn': pendulum.now("UTC").format("ddd MMM DD HH:mm:ss [UTC] YYYY"),
        #     "requestId": request_id,
        # }

        # # ── NEW: define send_message callable ──
        # # producer and output_topic already available here
        # # response_headers already built above
        # # this callable is passed into main() → WorkflowOrchestrator
        # # → called after every stage via generate_stage_payload()
        # def send_message(response_payload):
        #     self.producer.send_message(
        #         self.output_topic,
        #         response_headers,
        #         response_payload
        #     )

        try:
            if header.get('eventSubType') == 'REPROCESS_DOCUMENT':
                    feedback = {
                        'fuuid': payload.get('uuid'),
                        'filename': payload.get('name'),
                        'dafileid': payload.get('daFileId'),
                        'status': 'REPROCESS_REQUESTED',
                        'feedback': payload.get('reprocessCommand')
                        }   
                    self.db.insert_feedback(**feedback)
                    context = header.copy()
                    context['dafileid'] = payload.get('daFileId')
                    context['filename'] = payload.get('name')
                    context['feedback'] = payload.get('reprocessCommand')
                    notify_feedbacks(context)
            # response_payload = asyncio.run(main(header, payload, debug=cfg.DEBUG))
            if Path(payload.get('name','')).suffix =='.zip' and header.get("eventSubType",'') == 'MANUAL_UPLOAD':
                main_zip(header, payload,producer=self.producer)
            else:
                main(header, payload,producer=self.producer)
                        # response_headers=response_headers)

            
        except CustomBaseException as e:
            logger.error(f"{request_id}~~{fUuid}-{e}", exc_info=True)
            fpayload = e.to_dict()
            fpayload['requestUuid'] = fUuid
            fpayload['ipTypes'] = payload.get('ipTypes')
            fpayload['phase'] = payload.get("phase")
            self.send_failure(header, fpayload)
            return
        except Exception as e:
            logger.error(f"{request_id}~~{fUuid}-{e}", exc_info=True)
            fpayload = UnExpectedError(e).to_dict()
            fpayload['requestUuid'] = fUuid
            fpayload['ipTypes'] = payload.get('ipTypes')
            fpayload['phase'] = payload.get("phase")
            self.send_failure(header, fpayload)
            return
        
        # Final message is now sent by GeneratePayloadStage, so no need to send here
        # Just update database status to indicate completion
        # response_payload['requestUuid'] = fUuid
        # self.producer.send_message(
        #     self.output_topic, response_headers, response_payload
        # )
        # self.db.update_document(
        #     where_clause={'requestid': request_id, 'fuuid': fUuid, 'daoriginal_fileid': payload.get('daFileId')},
        #     update_values={'status': 'Sent For Review'}
        # )