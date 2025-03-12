from kafka import KafkaProducer
import mysql.connector
import time
import json
import logging
from typing import Dict, List, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
DB_CONFIG = {
    'host': 'upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com',
    'database': 'testdatabase',
    'user': 'student',
    'password': 'STUDENT123'
}

KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'value_serializer': lambda x: json.dumps(x).encode('utf-8')
}

KAFKA_TOPIC = 'patients_vital_topic'
QUERY = "SELECT customerId, heartBeat, bp FROM patients_vital_info"

def connect_to_database() -> mysql.connector.connection.MySQLConnection:
    """Establish connection to MySQL database."""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        logger.info("Successfully connected to database")
        return connection
    except mysql.connector.Error as err:
        logger.error(f"Failed to connect to database: {err}")
        raise

def fetch_patient_records(connection: mysql.connector.connection.MySQLConnection) -> List[Tuple]:
    """Fetch patient vital records from database."""
    try:
        with connection.cursor() as cursor:
            cursor.execute(QUERY)
            records = cursor.fetchall()
            logger.info(f"Successfully fetched {len(records)} patient records")
            return records
    except mysql.connector.Error as err:
        logger.error(f"Error fetching records: {err}")
        raise

def create_kafka_producer() -> KafkaProducer:
    """Create and return a Kafka producer instance."""
    try:
        producer = KafkaProducer(**KAFKA_CONFIG)
        logger.info("Successfully created Kafka producer")
        return producer
    except Exception as err:
        logger.error(f"Failed to create Kafka producer: {err}")
        raise

def create_message(record: Tuple) -> Dict:
    """Create a message dictionary from a patient record."""
    customer_id, heart_beat, bp = record
    return {
        "customerId": customer_id,
        "heartBeat": heart_beat,
        "bp": bp
    }

def main():
    """Main function to run the data pipeline."""
    try:
        # Connect to database and fetch records
        connection = connect_to_database()
        patient_records = fetch_patient_records(connection)
        connection.close()

        # Create Kafka producer
        producer = create_kafka_producer()

        # Process and send records
        for record in patient_records:
            message = create_message(record)
            producer.send(KAFKA_TOPIC, message)
            logger.info(f"Sent message: {message}")
            time.sleep(1)

        # Ensure all messages are sent
        producer.flush()
        producer.close()
        logger.info("Successfully completed data transfer")

    except Exception as err:
        logger.error(f"Pipeline failed: {err}")
        raise
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()