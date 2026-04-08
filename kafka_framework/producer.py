import logging
import json, sys, os
from kafka import KafkaProducer
from kafka.errors import KafkaError
from typing import List, Dict, Optional

from core.emailNotification import notify_failures
from core.db.crud import DatabaseManager
from core.utility import get_custom_logger
from config import Config
# add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logging.getLogger('kafka').setLevel(logging.CRITICAL)

logger = get_custom_logger(__name__)

def _json_default(obj):
    """
    Fallback for json.dumps when it encounters an object it cannot serialize.
    Currently handles:
      - uuid.UUID → str
      - datetime/date → isoformat string
    Extend as needed.
    """
    try:
        # UUID objects
        import uuid
        if isinstance(obj, uuid.UUID):
            return str(obj)
    except Exception:
        pass

    # datetime / date objects
    if hasattr(obj, "isoformat"):
        return obj.isoformat()

    # Fallback – raise the original TypeError so we notice unsupported types
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

class KafkaMessageProducer:
    """Reusable Kafka producer for sending messages"""
    
    def __init__(self, bootstrap_servers: List[str],         
                 security_protocol: str,
                    sasl_mechanism: str,
                    sasl_plain_username: str,
                    sasl_plain_password: str):
        self.bootstrap_servers = bootstrap_servers
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            value_serializer=lambda v: json.dumps(v, default=_json_default).encode('utf-8'),
            retries=3,
            retry_backoff_ms=20000,  # 20 seconds base wait time
            reconnect_backoff_max_ms=60000,  # Max 60 seconds for exponential backoff
            request_timeout_ms=120000,   # 2 minutes
            metadata_max_age_ms=300000  # 5 minutes
        )
    
    def send_message(
        self,
        topic: str,
        header: Dict,
        data: Dict,
        key: Optional[str] = None,
        idx: Optional[str] = ""
    ) -> None:
        """
        Send a message to Kafka topic
        
        Args:
            topic: Kafka topic name
            header: Message header
            data: Message payload
            key: Optional partition key
        """
        message = {
            'header': header,
            'payload': data
        }
        logger.info(f"{idx} RESPONSE MESSAGE : {message}")
        try:
            future = self.producer.send(
                topic,
                headers=[(k, str(v).encode("utf-8")) for k, v in header.items()],
                value=message,
                key=key.encode('utf-8') if key else None
            )
            future.get(timeout=13)

            db=DatabaseManager()
            db.insert_event_record(header, data, is_request=False)
            logger.info(f"{idx} Message sent to topic {topic}: {header.get('eventType')}")
            
        except Exception as e:
            logger.error(f"{idx} Failed to send message: {e}",exc_info=True)
            self.send_notification(header,e)
            raise
        # finally:
        #     if 'db' in locals(): del db
    
    def send_notification(self,header, exp):
        context = header.copy()
        context['error_text'] = exp
        notify_failures(context,f'Unknown error occured') 

    def close(self) -> None:
        """Close the producer"""
        self.producer.close()



if __name__=='__main__':
    """Initialize and start the service"""
    
    logger.info(f"Starting {Config.SERVICE_NAME}")
    logger.info(f"Kafka Bootstrap Servers: {Config.KAFKA_BOOTSTRAP_SERVERS_INPUT}")
    logger.info(f"Input Topics: {Config.INPUT_TOPICS}")
    logger.info(f"Output Topic: {Config.OUTPUT_TOPIC}")
    logger.info(f"Consumer Group: {Config.KAFKA_GROUP_ID}")
    logger.info(f"SECURITY_PROTOCOL: {Config.SECURITY_PROTOCOL}")
    logger.info(f"SASL_MECHANISM: {Config.SASL_MECHANISM}")
    logger.info(f"SASL_PLAIN_USERNAME: {Config.SASL_PLAIN_USERNAME}")
    logger.info(f"SASL_PLAIN_PASSWORD: {Config.SASL_PLAIN_PASSWORD}")
    
    # Initialize producer
    producer = KafkaMessageProducer(
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS_INPUT,
        security_protocol=Config.SECURITY_PROTOCOL,
        sasl_mechanism=Config.SASL_MECHANISM,
        sasl_plain_username=Config.SASL_PLAIN_USERNAME,
        sasl_plain_password=Config.SASL_PLAIN_PASSWORD)
    
    project_response_message = {
        'header': {
            'eventType': 'PROJECT_REQUEST_RECOMMENDATION',
            'eventSubType': None,
            'createdOn': 'Thu Sep 18 10:51:51 IST 2025',
            'requestId': 'df2e9f9f-e122-4151-9453-eb0ed27bd27f'
        },
        'payload': {
            'createdBy': 0,
            'updatedBy': None,
            'createdOn': 1758172910407,
            'updatedOn': 1758172910407,
            'ipId': None,
            'uuid': '67703ecc-8c13-4c99-b9f4-f324263f40b9',
            'name': 'SOW.doc',
            'daFileId': '9632587410lop789',
            'status': 'REQUESTED',
            'documentType': None,
            'ownerId': None,
            'projectId': 'PR-1950642',
            'projectFileId': 'c38adf08-f0fa-4ddd-e138-08ddef10ef46'
        }
    }

    accepted_feedback_message = {
        'header': {
            'eventType': 'PROJECT_UPDATE_RECOMMENDATION',
            'eventSubType': "ACCEPT_RECOMMENDATION",
            'createdOn': 'Thu Sep 18 10:51:51 IST 2025',
        },
        'payload': {
            'createdBy': 0,
            'createdOn': 1758172910407,
            'updatedOn': 1758173958025,
            'uuid': '67703ecc-8c13-4c99-b9f4-f324263f40b9',
            'status': 'ACCEPTED',
            'recommendationId': '963258ed-7410l-op789-rfh7-gfgg4445'

        }
    }

    skip_feedback_message = {
        'header': {
            'eventType': 'PROJECT_UPDATE_RECOMMENDATION',
            'eventSubType': "ACCEPT_RECOMMENDATION",
            'createdOn': 'Thu Sep 18 10:51:51 IST 2025',
        },
        'payload': {
            'createdBy': 0,
            'createdOn': 1758172910407,
            'updatedOn': 1758173958025,
            'uuid': '67703ecc-8c13-4c99-b9f4-f324263f40b9',
            'status': 'SKIPPED',
            'reason': '1234',
            'recommendationId': '963258ed-7410l-op789-rfh7-gfgg4445'
        }
    }


    refine_recommendations_message = {
        "header": {
            "eventType": "PROJECT_UPDATE_RECOMMENDATION",
            "eventSubType": "REFINE_RECOMMENDATION",
            "createdOn": "Thu Sep 18 11:09:18 IST 2025"
        },
        "payload": {
            "createdBy": 0,
            "createdOn": 1758172910407,
            "updatedOn": 1758173958025,
            "uuid": "67703ecc-8c13-4c99-b9f4-f324263f40b9",
            "phase": "Updated Phase",
            "status": "REFINED",
            "reason": "text with 200 characters",
            "includeSkipped": "TRUE|FALSE",
            "recommendationId": "963258ed-7410l-op789-rfh7-gfgg4445"

        }
    }

    producer.send_message(Config.INPUT_TOPICS[0], {}, {'header': refine_recommendations_message['header'], 'payload':{'payload': refine_recommendations_message['payload']}})