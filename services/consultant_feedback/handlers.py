
# import asyncio,logging,os
from typing import Dict, List
import pendulum
import logging
# from services.recommendation_feedback.models import FeedbackRequest, RefineRecommendationRequest
# from core.db.crud import DatabaseManager
from core.db.crud import DatabaseManager
from core.emailNotification import notify_failures
from core.exceptions import InvalidMetadataError, CustomBaseException, UnExpectedError
# from services.recommendation_feedback.refine_recommendations import generate_search_results
from services.consultant_feedback.feedbacks import record_feedback
from kafka_framework.consumer import MessageHandler
from kafka_framework.producer import KafkaMessageProducer
# from config import Config as cfg
from config import Configuration

cfg = Configuration()
from core.utility import get_custom_logger
logger = get_custom_logger(__name__)
# logger.propagate = False

class RecommendationFeedbackHandler(MessageHandler):
    """Handles summarization and recommendation requests"""
    
    def __init__(self, producer: KafkaMessageProducer, output_topic: str, debug: bool = False):
        self.producer = producer
        self.output_topic = output_topic
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
    
    def get_message_types(self) -> List[str]:
        return ['PROJECT_UPDATE_RECOMMENDATION']
    
    def send_notification(self,header, exp):
        context = header.copy()
        context['error_text'] = exp['internal_message']
        notify_failures(context,f'GTL Feedback|{exp['error_code']}')
        
    # def send_failure(self, requestUuid: str, payload: Dict, eventSubType:str ="PROCESSING_ERROR",project_id:str = "",requestId:str = ""):
    #     response_headers = {
    #         'eventType': "PROJECT_UPDATE_RECOMMENDATION_ACK",
    #         'eventSubType': eventSubType,
    #         'createdOn': pendulum.now("UTC").format("ddd MMM DD HH:mm:ss [UTC] YYYY") 
    #     }
    #     if len(requestId)>0:
    #         response_headers['requestId'] = requestId
    #     payload['requestUuid']= requestUuid
    #     self.producer.send_message(
    #         self.output_topic,
    #         response_headers,
    #         payload
    #     )
        # response_headers["eventSubType"] = "STREAM_COMPLETION"
        # statuspayload={"Status": "Completed",
        #                "FF_ProjectId": project_id,
        #                'requestUuid': requestUuid}

        # self.producer.send_message(
        #     self.output_topic,
        #     response_headers,
        #     statuspayload
        # )
    
    def remove_keys(self, pdict: dict):
        removeKeys = [ 'createdBy', 'updatedBy', 'createdOn', 'updatedOn']
        for key in removeKeys:    
            if key in pdict: pdict.pop(key, None) 
    
    def handle(self, header: Dict, payload: Dict) -> None:
        cfg.load_active_config()
        if header.get("eventSubType") in ("ACCEPT_RECOMMENDATION", "SKIP_RECOMMENDATION"):
            missing = [k for k in ("status", "recommendationId") if not payload.get(k)]
            if len(missing)>0:
                exp = InvalidMetadataError(missing).to_dict()
                logger.error(f"Unexpected error on {exp}", exc_info=True)
                self.send_notification(header, exp)
                # self.send_failure(request_uuid, InvalidMetadataError(missing).to_dict(),requestId = request_id)
                return
            
            try:
               record_feedback(header,payload,cfg.DEBUG)
            except CustomBaseException as exc:
                logger.error(f"{exc}", exc_info=True)
                self.send_notification(header, exc.to_dict())
                # self.send_failure(request_uuid, exc.to_dict(),requestId = request_id)
            except Exception as exc:
                logger.error(f"{exc}", exc_info=True)
                self.send_notification(header, UnExpectedError(exc).to_dict())

        elif header.get("eventSubType") == "FEEDBACK": 
       
            try:
                db = DatabaseManager(cfg.DEBUG)
                consultant_feedback = {
                    'requestid': header.get('requestId'),
                    'dafileid': payload.get('daFileId'),
                    'feedback': payload.get('feedback'),  
                    'message': payload.get('message'),
                    'usercomments': payload.get('usercomments') if payload.get('message', '').lower() == 'other' else None
                    # 'usercomments': payload.get('usercomments') if payload.get('message', '').lower() == 'other' else None
                }
                db.insert_consultant_feedback(**consultant_feedback)
                logger.info(f"Consultant feedback recorded for requestid: {header.get('requestId')}")
            except Exception as e:
                logger.error(f"Failed to process consultant feedback: {e}", exc_info=True)
                self.send_notification(header,UnExpectedError(e).to_dict())

