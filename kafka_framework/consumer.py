import os
import traceback
import threading,time,json,logging
from core.emailNotification import notify_failures
from core.log_helper import add_session_file_handler, remove_session_file_handler, delete_all_files
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from typing import Callable, Dict, List, Optional
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from core.log_helper import move_log_file

from core.db.crud import DatabaseManager
from core.utility import get_custom_logger, request_id_var, session_id_var
from core.s3_helper import StorageManager
import uuid

logging.getLogger('KafkaConsumer').setLevel(logging.CRITICAL)
logging.getLogger('KafkaError').setLevel(logging.CRITICAL)

logger = get_custom_logger(__name__)
# logger.propagate = False
s3=StorageManager()

class MessageHandler(ABC):
    """Base class for business logic handlers"""
    
    @abstractmethod
    def handle(self, header: Dict, payload: Dict) -> None:
        """
        Process the message based on business logic
        
        Args:
            header: Message header containing metadata
            data: Message payload data
        """
        pass
    
    @abstractmethod
    def get_message_types(self) -> List[str]:
        """Return list of message types this handler processes"""
        pass


class KafkaMessageListener:
    """Reusable Kafka consumer with message routing"""
    
    def __init__(
        self,
        bootstrap_servers: List[str],
        group_id: str,
        topics: List[str],
        security_protocol: str,
        sasl_mechanism: str,
        sasl_plain_username: str,
        sasl_plain_password: str,
        auto_offset_reset: str = 'earliest',
        enable_auto_commit: bool = True,

    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topics = topics
        self.security_protocol=security_protocol
        self.sasl_mechanism=sasl_mechanism
        self.sasl_plain_username=sasl_plain_username
        self.sasl_plain_password=sasl_plain_password
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.handlers: Dict[str, MessageHandler] = {}
        self.consumer: Optional[KafkaConsumer] = None
        
    def register_handler(self, handler: MessageHandler) -> None:
        """Register a business logic handler for specific message types"""
        for msg_type in handler.get_message_types():
            if msg_type in self.handlers:
                logger.warning(f"Overwriting handler for message type: {msg_type}")
            self.handlers[msg_type] = handler
            logger.info(f"Registered handler for message type: {msg_type}")
    
    def _parse_message(self, raw_message: bytes) -> tuple:
        """Parse JSON message and extract header and data"""
        try:
            # message = json.loads(raw_message.decode('utf-8'))
            header = raw_message.get('header', {})
            payload = raw_message.get('payload', {})
            request_id = header.get("requestId", "-")
            request_id_var.set(request_id)
            # session_handler = add_session_file_handler(logger, session_id_var.get(), request_id)

            logger.warning(f"RAW MESSAGE : {raw_message}")

            if isinstance(payload, dict) and 'payload' in payload:
                payload = payload.get('payload', {})

            return header, payload
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}",exc_info=True)
            raise
    
    def _route_message(self, header: Dict, payload: Dict) -> None:
        """Route message to appropriate handler based on message type"""
        event_type = header.get('eventType')
        event_sub_type = header.get('eventSubType')
        
        if not event_type:
            logger.warning("Message has no eventType in header, skipping")
            return
        
        #workaround remove after frontend EPS changes
        if event_type == 'IP_GOLDEN_COPY_REQUEST_RECOMMENDATION' and event_sub_type == 'UPLOAD_NEW_FILE':
            header['eventType']    = 'IP_RECOMMENDATION_UPDATE'
            header['eventSubType'] = 'FILE_INFO_UPDATE'
            if payload.get('newFileId') is not None:
                payload['newDaFileId'] = payload.pop('newFileId')
            logger.warning(f"Message header change {event_type}")
        elif event_type == 'IP_RECOMMENDATION_UPDATE' and event_sub_type == 'REPROCESS_DOCUMENT':
            header['eventType'] = 'IP_GOLDEN_COPY_REQUEST_RECOMMENDATION'
        
        handler = self.handlers.get(header.get('eventType'))
        
        if handler:
            try:
                logger.info(f"Processing message type: {event_type}")
                db=DatabaseManager()
                logger.info(f"Created db instance")

                db.insert_event_record(header, payload, is_request=True)
                handler.handle(header, payload)
                # Run the function asynchronously
                # thread = threading.Thread(target=handler.handle(header, payload))
                # thread.start()
            except Exception as e:
                logger.error(f"Error processing message type {event_type}: {e}")
                traceback.print_exc()
                if 'db' in locals(): del db
                raise
            finally:
                if 'db' in locals(): del db
        else:
            logger.debug(f"No handler registered for message type: {event_type}")
    
    def start(self) -> None:
        """Start consuming messages from Kafka"""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=self.auto_offset_reset,
                enable_auto_commit=False,
                security_protocol=self.security_protocol,
                sasl_mechanism=self.sasl_mechanism,
                sasl_plain_username=self.sasl_plain_username,
                sasl_plain_password=self.sasl_plain_password,
                value_deserializer=lambda v: json.loads(v.decode("utf-8"))
            )
            
            logger.info(f"Connected to Kafka. Listening to topics: {self.topics}")
            logger.info(f"Registered message types: {list(self.handlers.keys())}")
            
            for message in self.consumer:
                header = None
                try:
                    
                    session_id = str(uuid.uuid4())
                    session_id_var.set(session_id)
                    header, data = self._parse_message(message.value)
                    self.consumer.commit()
                    self._route_message(header, data)
                    logger.warning(f"MESSAGE PROCESSED :: header: {header}, payload: {data}")
                except Exception as e:
                #     self.consumer.commit(offset=message.offset)
                    logger.error(f"Error processing message: {e}",exc_info=True)
                    context = header if header else message.value.get('header',{
                        "request_id": "N/A",
                        "event_type": "N/A",
                        "event_sub_type": "N/A",
                        "request_time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    })
                    self.send_notification(context, e)
                    # Continue processing next message
                finally:
                    # remove_session_file_handler(logger, session_handler)
                    try:
                        move_log_file(request_id_var.get(), session_id)
                        s3.upload_folder(folder_path="deployment/logs", prefix=f"logs", delete_after_upload=True)
                    except Exception as e:
                        logger.error(f"Error uploading logs: {e}", exc_info=True)

                    session_id_var.set("-")
                    request_id_var.set("-")

        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            raise
        finally:
            self.stop()
    
    def stop(self) -> None:
        """Stop the consumer and close connections"""
        if self.consumer:
            logger.info("Closing Kafka consumer")
            self.consumer.close()
    
    def send_notification(self,header, exp):
        context = header.copy()
        context['error_text'] = exp
        notify_failures(context,f'Unknown error occured') 
