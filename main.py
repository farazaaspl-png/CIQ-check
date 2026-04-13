
from kafka_framework.consumer import KafkaMessageListener
from kafka_framework.producer import KafkaMessageProducer

from services.advance_search.handlers import DeepSearchHandler
from services.gtl_recommendation.handlers import Gtl_Recommendation
from services.gtl_feedback.handlers import GTLFeedbackHandler
from services.consultant_feedback.handlers import RecommendationFeedbackHandler
from services.consultant_recommendation.handlers import Consultant_Recommendation
from config import Config
# import logging
# import asyncio


# logging.basicConfig(
#     level=getattr(logging, Config.LOG_LEVEL),
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
# )
from core.utility import get_custom_logger
logger = get_custom_logger(__name__)
# logger.propagate = False


def main():
    """Initialize and start the service"""
    
    logger.info(f"Starting {Config.SERVICE_NAME} - Version 25.11.25.01")
    logger.info(f"Kafka Bootstrap Servers: {Config.KAFKA_BOOTSTRAP_SERVERS_INPUT}")
    logger.info(f"Input Topics: {Config.INPUT_TOPICS}")
    logger.info(f"Output Topic: {Config.OUTPUT_TOPIC}")
    logger.info(f"Consumer Group: {Config.KAFKA_GROUP_ID}")
    logger.info(f"SECURITY_PROTOCOL: {Config.SECURITY_PROTOCOL}")
    logger.info(f"SASL_MECHANISM: {Config.SASL_MECHANISM}")
    logger.info(f"SASL_PLAIN_USERNAME: {Config.SASL_PLAIN_USERNAME}")
    # logger.info(f"SASL_PLAIN_PASSWORD: {Config.SASL_PLAIN_PASSWORD}")

    # Initialize producer
    producer = KafkaMessageProducer(
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS_OUTPUT,
        security_protocol=Config.SECURITY_PROTOCOL,
        sasl_mechanism=Config.SASL_MECHANISM,
        sasl_plain_username=Config.SASL_PLAIN_USERNAME,
        sasl_plain_password=Config.SASL_PLAIN_PASSWORD)
    
    # Initialize consumer
    listener = KafkaMessageListener(
        bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS_INPUT,
        group_id=Config.KAFKA_GROUP_ID,
        topics=Config.INPUT_TOPICS,
        auto_offset_reset=Config.AUTO_OFFSET_RESET,
        enable_auto_commit=Config.ENABLE_AUTO_COMMIT,
        security_protocol=Config.SECURITY_PROTOCOL,
        sasl_mechanism=Config.SASL_MECHANISM,
        sasl_plain_username=Config.SASL_PLAIN_USERNAME,
        sasl_plain_password=Config.SASL_PLAIN_PASSWORD,
    )
    
    # Register business logic handlers
    #fast running process
    listener.register_handler(
        Consultant_Recommendation(producer, Config.OUTPUT_TOPIC, Config.DEBUG)
    )
    listener.register_handler(
        RecommendationFeedbackHandler(producer, Config.OUTPUT_TOPIC, Config.DEBUG)
    )
    listener.register_handler(
        DeepSearchHandler(producer, Config.OUTPUT_TOPIC, Config.DEBUG)
    )

    #slow running process
    listener.register_handler(
        GTLFeedbackHandler(producer, Config.OUTPUT_TOPIC, Config.DEBUG)
    )
    listener.register_handler(
        Gtl_Recommendation(producer, Config.OUTPUT_TOPIC, Config.DEBUG)
    )

    
    
    # Start listening
    try:
        listener.start()
    except KeyboardInterrupt:
        logger.info("Shutting down service")
    finally:
        producer.close()


if __name__ == '__main__':
    import multiprocessing
    # Use the “spawn” start method explicitly – works everywhere
    multiprocessing.set_start_method("spawn", force=True)
    main()
