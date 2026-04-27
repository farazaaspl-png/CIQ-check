# deep_search_handler.py (complete, fixed)
import logging, pandas as pd, pendulum
from typing import Dict, List

from kafka_framework.consumer import MessageHandler
from kafka_framework.producer import KafkaMessageProducer
from core.utility import get_custom_logger
from config import Configuration

# ✅ Import YOUR vectorizer_content
from core.embedding.vectorizer_content import ContentVectorInterface
from core.emailNotification import notify_failures
from core.exceptions import InvalidMetadataError,UnExpectedError,UnableToFindAnyDocument
from core.db.crud import DatabaseManager

logger = get_custom_logger(__name__)

class DeepSearchHandler(MessageHandler):
    """Handles ONLY deep search requests"""
    
    def __init__(self, producer: KafkaMessageProducer, output_topic: str, debug: bool = False):
        self.producer = producer
        self.output_topic = output_topic
        self.debug = debug
        if self.debug:
            logger.setLevel(logging.DEBUG)
    
    def get_message_types(self) -> List[str]:
        return ['CONSULTING_SEARCH_REQUEST_RECOMMENDATION']
    
    def send_failure(self, reqheader: str, payload: Dict) -> None:
        context = reqheader.copy()
        context['error_text'] = payload.get('error_message')
        notify_failures(context,f'Deep Search|{payload.get('error_code')}')

        failure_header = {
            "eventType": reqheader.get('eventType', '')+'_ACK',
            "eventSubType": "SEARCH_COMPLETED",
            "createdOn": pendulum.now("UTC").format("ddd MMM DD HH:mm:ss [UTC] YYYY"),
            "requestId": reqheader.get('requestId'),
        }
        self.producer.send_message(
            self.output_topic, failure_header, payload
        )
    
    def handle(self, header: Dict, payload: Dict) -> None:
        """Handle deep search with search_and_rank"""
        eventType = header.get('eventType', '')+'_ACK'
        requestId = header.get('requestId', payload.get('uuid', 'unknown'))
        query = payload.get('searchQuery', '')

        missing_fields = []

        if not requestId : missing_fields.append("requestId")
        if not query : missing_fields.append("searchQuery")

        if len(missing_fields)>0:
            fpayload = InvalidMetadataError(missing_fields).to_dict()
            logger.error(f"Required fields missing: {missing_fields}")
            self.send_failure(header, fpayload)
            return

        cfg = Configuration()
        cfg.load_active_config()
        results_df = pd.DataFrame()
        logger.info(f"Started searching..")
        response_header = {
                "eventType": eventType,
                "eventSubType": "SEARCH_RUNNING",
                "createdOn": pendulum.now("UTC").format("ddd MMM DD HH:mm:ss [UTC] YYYY"),
                "requestId": requestId,
            }
        try:
            vec = ContentVectorInterface(cfg.DOCUMENT_CONTENT_STORE)
            results_df = vec.search_and_rank_with_llm(query)
            filtered_results_df = results_df[results_df['relevance_score']>cfg.DEEP_SEARCH_RELEVANCE_SCORE_THRESHOLD]
            logger.info(f"After apply {cfg.DEEP_SEARCH_RELEVANCE_SCORE_THRESHOLD} threshold, got {filtered_results_df.shape[0]} documents")

            if filtered_results_df.shape[0]==0:
                raise UnableToFindAnyDocument()
            list_of_dfs = [filtered_results_df.iloc[i:i + cfg.DEEP_SEARCH_PUSH_SIZE].copy() for i in range(0, len(filtered_results_df), cfg.DEEP_SEARCH_PUSH_SIZE)]
            cumulative_cnt = 0
            for df in list_of_dfs:
                cumulative_cnt = cumulative_cnt+df.shape[0]
                response_payload =  {
                            "cumulativeFilesCount": cumulative_cnt,
                            "fileUUID": df['fuuid'].tolist(),
                            "status": "Running",
                            "relevance": df.set_index('fuuid').to_dict(orient='index')
                           }
                self.producer.send_message(self.output_topic, response_header, response_payload)
                logger.info(f"{cumulative_cnt} searched files sent")
            
            response_header["eventSubType"] = "SEARCH_COMPLETED"
            response_payload =  {"totalFilesCount": cumulative_cnt,
                                 "status": "Completed"}
            self.producer.send_message(self.output_topic, response_header, response_payload)
            logger.info(f"Completion message sent")
        
        except UnableToFindAnyDocument as e:
            logger.warning(f"{requestId} - Failed: {e}", exc_info=True)
            response_header["eventSubType"] = "SEARCH_COMPLETED"
            response_payload =  {"totalFilesCount": 0,
                                 "status": "Completed"}
            self.producer.send_message(self.output_topic, response_header, response_payload)
            logger.info(f"Completion message sent")
        except Exception as e:
            logger.error(f"{requestId} - Failed: {e}", exc_info=True)
            fpayload = UnExpectedError(e).to_dict()
            self.send_failure(header, fpayload)

        if results_df.shape[0]>0:
            results_df['requestid'] = requestId
            results_df['userquery'] = query
            db = DatabaseManager()
            db.insert_deep_search_logs(results_df.to_dict(orient='records'))
            logger.info(f"recorded {results_df.shape[0]} records in logs table.")
    
