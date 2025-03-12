from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_json, struct
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType

class PatientMonitoringSystem:
    def __init__(self, kafka_broker, streaming_data_path):
        """Initialize the Patient Monitoring System.
        
        Args:
            kafka_broker (str): Kafka broker address
            streaming_data_path (str): HDFS path for streaming data
        """
        self.kafka_broker = kafka_broker
        self.streaming_data_path = streaming_data_path
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self):
        """Create and configure Spark session with Hive support."""
        return SparkSession.builder \
            .appName("PatientHealthMonitoring") \
            .enableHiveSupport() \
            .getOrCreate()
    
    def _get_vital_signs_schema(self):
        """Define schema for patient vital signs data."""
        return StructType([
            StructField('CustomerID', IntegerType(), True),
            StructField('BP', IntegerType(), True),
            StructField('HeartBeat', IntegerType(), True),
            StructField('Message_time', TimestampType(), True)
        ])
    
    def _read_patient_contact_info(self):
        """Read patient contact information from Hive table."""
        contact_df = self.spark.table("patient_health_care.Patients_Contact_Info")
        contact_df.createOrReplaceTempView("Patients_Contact_Info")
        return contact_df
    
    def _read_threshold_info(self):
        """Read threshold reference information from Hive table."""
        threshold_df = self.spark.table("patient_health_care.Threshold_Reference_Table")
        threshold_df.createOrReplaceTempView("Threshold_Reference_Table")
        return threshold_df
    
    def _read_streaming_vitals(self):
        """Read streaming vital signs data from HDFS."""
        return self.spark.readStream \
            .format("parquet") \
            .schema(self._get_vital_signs_schema()) \
            .load(self.streaming_data_path)
    
    def _create_alert_query(self):
        """Create SQL query to identify patients with abnormal vitals."""
        return """
        WITH BP_Alerts AS (
            SELECT 
                v.CustomerID,
                v.BP,
                v.HeartBeat,
                v.Message_time,
                t.alert_message
            FROM Patients_Vital_Info v
            JOIN Threshold_Reference_Table t ON t.attribute = 'bp'
            WHERE v.BP NOT BETWEEN t.low_range_value AND t.high_range_value
            AND t.alert_flag = 1
        ),
        HeartBeat_Alerts AS (
            SELECT 
                v.CustomerID,
                v.BP,
                v.HeartBeat,
                v.Message_time,
                t.alert_message
            FROM Patients_Vital_Info v
            JOIN Threshold_Reference_Table t ON t.attribute = 'heartBeat'
            WHERE v.HeartBeat NOT BETWEEN t.low_range_value AND t.high_range_value
            AND t.alert_flag = 1
        ),
        Combined_Alerts AS (
            SELECT * FROM BP_Alerts
            UNION ALL
            SELECT * FROM HeartBeat_Alerts
        )
        SELECT 
            c.patientname,
            c.age,
            c.patientaddress,
            c.phone_number,
            c.admitted_ward,
            a.BP as bp,
            a.HeartBeat as heartbeat,
            a.Message_time as input_message_time,
            a.alert_message
        FROM Combined_Alerts a
        JOIN Patients_Contact_Info c ON a.CustomerID = c.patientid
        """
    
    def _write_alerts_to_kafka(self, alert_df):
        """Stream alerts to Kafka topic.
        
        Args:
            alert_df: DataFrame containing alert information
        """
        return alert_df.selectExpr("to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .outputMode("append") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("topic", "Alerts_Message") \
            .option("checkpointLocation", "/tmp/alert_checkpoint/") \
            .start()
    
    def process_patient_data(self):
        """Main method to process patient data and generate alerts."""
        try:
            # Read reference data
            self._read_patient_contact_info()
            self._read_threshold_info()
            
            # Read streaming vital signs
            patient_vital_df = self._read_streaming_vitals()
            
            # Create temporary view for streaming data
            patient_vital_df.createOrReplaceTempView("Patients_Vital_Info")
            
            # Generate alerts for abnormal vitals
            alert_df = self.spark.sql(self._create_alert_query())
            
            # Add timestamp for alert generation
            alert_df = alert_df.withColumn("alert_generated_time", current_timestamp())
            
            # Stream alerts to Kafka
            query = self._write_alerts_to_kafka(alert_df)
            
            # Wait for termination
            query.awaitTermination()
            
        except Exception as e:
            print(f"Error processing patient data: {str(e)}")
            raise

def main():
    # Configuration
    KAFKA_BROKER = "localhost:9092"
    STREAMING_DATA_PATH = "/user/livy/output/date=2025-02-23"
    
    # Initialize and run the monitoring system
    monitoring_system = PatientMonitoringSystem(KAFKA_BROKER, STREAMING_DATA_PATH)
    monitoring_system.process_patient_data()

if __name__ == "__main__":
    main()