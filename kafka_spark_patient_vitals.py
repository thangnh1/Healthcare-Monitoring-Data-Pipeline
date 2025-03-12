from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime
from typing import Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('patient_vitals_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)

# Configuration
APP_CONFIG = {
    'app_name': 'kafka_spark_patient_vitals',
    'kafka_server': 'localhost:9092',
    'kafka_topic': 'patients_vital_topic',
    'output_path': '/user/livy/output/',
    'checkpoint_path': '/tmp/checkpoint/'
}

# Schema definition
VITAL_SCHEMA = StructType([
    StructField('customerId', IntegerType(), True),
    StructField('heartBeat', IntegerType(), True),
    StructField('bp', IntegerType(), True)
])

def create_spark_session(app_name: str) -> SparkSession:
    """Initialize and return a Spark session with Kafka dependencies."""
    try:
        logger.info("Starting to create Spark session...")
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel('ERROR')
        logger.info(f"Successfully created Spark session with app name: {app_name}")
        logger.info(f"Spark version: {spark.version}")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}", exc_info=True)
        raise

def create_kafka_stream(spark: SparkSession, config: Dict) -> DataFrame:
    """Create and return a streaming DataFrame from Kafka source."""
    try:
        logger.info(f"Connecting to Kafka broker at {config['kafka_server']}")
        logger.info(f"Subscribing to topic: {config['kafka_topic']}")
        
        stream_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config['kafka_server']) \
            .option("subscribe", config['kafka_topic']) \
            .option("startingOffsets", "earliest") \
            .load()
        
        logger.info("Successfully created Kafka stream")
        logger.info(f"Stream schema: {stream_df.schema}")
        return stream_df
    except Exception as e:
        logger.error(f"Failed to create Kafka stream: {str(e)}", exc_info=True)
        raise

def transform_data(stream_df: DataFrame, schema: StructType) -> DataFrame:
    """Apply transformations to the streaming DataFrame."""
    try:
        logger.info("Starting data transformation...")
        
        # Parse JSON and select fields
        logger.info("Parsing JSON data from Kafka messages...")
        parsed_df = stream_df \
            .select(from_json(col('value').cast('string'), schema).alias('data')) \
            .select('data.*')
        
        # Add timestamp and rename columns
        logger.info("Adding timestamp and renaming columns...")
        transformed_df = parsed_df \
            .withColumn('Message_time', current_timestamp()) \
            .withColumnRenamed('customerId', 'CustomerID') \
            .withColumnRenamed('bp', 'BP') \
            .withColumnRenamed('heartBeat', 'HeartBeat') \
            .withColumn('date', date_format('Message_time', 'yyyy-MM-dd'))
        
        # Select final columns
        final_df = transformed_df.select(
            'CustomerID', 'BP', 'HeartBeat', 'Message_time', 'date'
        )
        
        logger.info("Data transformation completed successfully")
        return final_df
    except Exception as e:
        logger.error(f"Failed to transform data: {str(e)}", exc_info=True)
        raise

def monitor_progress(stream_query):
    """Monitor streaming query progress."""
    try:
        # Get the latest progress
        latest_progress = stream_query.lastProgress
        if latest_progress:
            num_input_rows = latest_progress["numInputRows"]
            processing_time = latest_progress["triggerExecution"]["durationMs"]
            logger.info(f"Processed {num_input_rows} rows in {processing_time}ms")
    except Exception as e:
        logger.warning(f"Could not log progress: {str(e)}")

def write_stream(transformed_df: DataFrame, config: Dict):
    """Write the transformed DataFrame to HDFS in parquet format."""
    try:
        logger.info(f"Starting to write stream to path: {config['output_path']}")
        logger.info(f"Using checkpoint location: {config['checkpoint_path']}")
        
        # Create the streaming query
        stream_query = transformed_df.writeStream \
            .format("parquet") \
            .partitionBy("date") \
            .option("path", config['output_path']) \
            .option("checkpointLocation", config['checkpoint_path']) \
            .trigger(processingTime="10 seconds") \
            .start()
        
        # Add progress monitoring
        while stream_query.isActive:
            monitor_progress(stream_query)
            stream_query.awaitTermination(timeout=10)
        
        logger.info("Stream writing process started successfully")
        return stream_query
    except Exception as e:
        logger.error(f"Failed to write stream: {str(e)}", exc_info=True)
        raise

def main():
    """Main function to run the streaming pipeline."""
    logger.info("=" * 50)
    logger.info("Starting Patient Vitals Pipeline")
    logger.info("=" * 50)
    
    try:
        # Initialize Spark
        logger.info("Step 1: Initializing Spark session")
        spark = create_spark_session(APP_CONFIG['app_name'])
        
        # Create Kafka stream
        logger.info("Step 2: Creating Kafka stream")
        vital_info_stream = create_kafka_stream(spark, APP_CONFIG)
        
        # Transform data
        logger.info("Step 3: Transforming data")
        final_df = transform_data(vital_info_stream, VITAL_SCHEMA)
        
        # Print schema for debugging
        logger.info("Final DataFrame Schema:")
        final_df.printSchema()
        
        # Write stream
        logger.info("Step 4: Starting stream writing process")
        query = write_stream(final_df, APP_CONFIG)
        
        logger.info("Pipeline setup completed. Processing data...")
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if 'spark' in locals():
            logger.info("Stopping Spark session")
            spark.stop()
            logger.info("Spark session stopped")
            logger.info("=" * 50)
            logger.info("Pipeline shutdown complete")
            logger.info("=" * 50)

if __name__ == "__main__":
    main()