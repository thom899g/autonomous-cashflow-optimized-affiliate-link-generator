from kafka import KafkaConsumer, KafkaProducer
from typing import Optional, Dict, Any
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data-ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataIngestionPipeline:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.consumer = None
        self.producer = None

    def _connect_kafka(self):
        """Connect to Kafka brokers and create producer/consumer instances."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                retries=5,
                max_retries=10
            )
            logger.info("Connected to Kafka as producer.")
            
            # Initialize consumer for processed data topic
            self.consumer = KafkaConsumer(
                self.config['kafka']['topic']['processed_data'],
                bootstrap_servers=self.config['kafka']['bootstrap_servers'],
                group_id='affiliate_optimizer'
            )
            logger.info("Connected to Kafka as consumer.")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            raise

    def send_processed_data(self, data: Dict[str, Any]):
        """Send processed affiliate data to Kafka."""
        try:
            self.producer.send(
                self.config['kafka']['topic']['processed_data'],
                value=data
            )
            logger.info(f"Sent data: {data}")
        except Exception as e:
            logger.error(f"Failed to send data: {str(e)}")
            raise

    def consume_processed_data(self):
        """Consume processed affiliate data from Kafka."""
        try:
            for message in self.consumer:
                data = message.value
                logger.info(f"Received processed data: {data}")
                yield data
        except Exception as e:
            logger.error(f"Failed to consume data: {str(e)}")
            raise

    def close(self):
        """Close Kafka connections."""
        try:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.flush()
                self.producer.close()
            logger.info("Kafka connections closed.")
        except Exception as e:
            logger.error(f"Failed to close Kafka connections: {str(e)}")