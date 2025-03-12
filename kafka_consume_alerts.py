import boto3
import json
from kafka import KafkaConsumer
import logging
from typing import Dict, Optional
from botocore.exceptions import ClientError

class HealthAlertProcessor:
    def __init__(self, 
                 kafka_broker: str,
                 kafka_topic: str,
                 aws_region: str,
                 sns_topic_arn: str):
        """Initialize the Health Alert Processor.
        
        Args:
            kafka_broker (str): Kafka broker address
            kafka_topic (str): Kafka topic to consume from
            aws_region (str): AWS region
            sns_topic_arn (str): AWS SNS topic ARN
        """
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.aws_region = aws_region
        self.sns_topic_arn = sns_topic_arn
        
        # Set up logging
        self._setup_logging()
        
        # Initialize clients
        self.consumer = self._create_kafka_consumer()
        self.sns_client = self._create_sns_client()
        
    def _setup_logging(self):
        """Configure logging for the application."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def _create_kafka_consumer(self) -> KafkaConsumer:
        """Create and configure Kafka consumer.
        
        Returns:
            KafkaConsumer: Configured Kafka consumer instance
        """
        try:
            return KafkaConsumer(
                self.kafka_topic,
                bootstrap_servers=[self.kafka_broker],
                auto_offset_reset='earliest',
                value_deserializer=lambda m: m.decode('utf-8'),
                enable_auto_commit=True,
                group_id='health_alert_group'
            )
        except Exception as e:
            self.logger.error(f"Failed to create Kafka consumer: {str(e)}")
            raise
            
    def _create_sns_client(self) -> boto3.client:
        """Create and configure AWS SNS client.
        
        Returns:
            boto3.client: Configured SNS client instance
        """
        try:
            return boto3.client(
                'sns',
                region_name=self.aws_region,
            )
        except Exception as e:
            self.logger.error(f"Failed to create SNS client: {str(e)}")
            raise
            
    def _parse_message(self, message: str) -> Optional[Dict]:
        """Parse Kafka message into JSON object.
        
        Args:
            message (str): Raw message string
            
        Returns:
            dict: Parsed message object or None if parsing fails
        """
        try:
            return json.loads(message)
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse message: {str(e)}")
            return None
            
    def _publish_to_sns(self, message_obj: Dict, subject: str) -> bool:
        """Publish message to SNS topic.
        
        Args:
            message_obj (dict): Message to publish
            subject (str): Subject line for the SNS message
            
        Returns:
            bool: True if published successfully, False otherwise
        """
        try:
            response = self.sns_client.publish(
                TopicArn=self.sns_topic_arn,
                Message=json.dumps(message_obj, indent=2),
                Subject=subject
            )
            message_id = response['MessageId']
            self.logger.info(f"Message published successfully with ID: {message_id}")
            return True
        except ClientError as e:
            self.logger.error(f"Failed to publish message to SNS: {str(e)}")
            return False
            
    def process_messages(self):
        """Main method to process messages from Kafka and publish to SNS."""
        self.logger.info("Starting to process messages...")
        
        try:
            for message in self.consumer:
                # Parse message
                message_obj = self._parse_message(message.value)
                if not message_obj:
                    continue
                
                # Create subject line
                subject = f"Health Alert: Patient- {message_obj.get('patientname', 'Unknown')}"
                
                # Publish to SNS
                success = self._publish_to_sns(message_obj, subject)
                
                if not success:
                    self.logger.warning("Failed to process message, continuing to next...")
                    
        except KeyboardInterrupt:
            self.logger.info("Received interrupt, shutting down...")
        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            raise
        finally:
            self.consumer.close()
            
def main():
    # Configuration
    CONFIG = {
        'kafka_broker': 'localhost:9092',
        'kafka_topic': 'Alerts_Message',
        'aws_region': 'us-east-1',
        'sns_topic_arn': 'arn:aws:sns:us-east-1:663479431872:sns-health-alert'
    }
    
    # Initialize and run the processor
    processor = HealthAlertProcessor(**CONFIG)
    processor.process_messages()

if __name__ == "__main__":
    main()