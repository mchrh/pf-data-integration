from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import sys
from pathlib import Path
import json

sys.path.append(str(Path(__file__).parent.parent))
from config.config import *

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/consumer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('WaterQualityConsumer')

class WaterQualityConsumer:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.schema = self._create_schema()
        self._load_static_data()
        logger.info("Initialized Spark consumer")

    def _create_spark_session(self):
        return SparkSession.builder \
            .appName("WaterQualityMonitoring") \
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()

    def _create_schema(self):
        """Create schema for LTM data"""
        return StructType([
            StructField("SITE_ID", StringType(), True),
            StructField("PROGRAM_ID", StringType(), True),
            StructField("DATE_SMP", TimestampType(), True),
            StructField("SAMPLE_LOCATION", StringType(), True),
            StructField("SAMPLE_TYPE", StringType(), True),
            StructField("WATERBODY_TYPE", StringType(), True),
            StructField("PH_LAB", DoubleType(), True),
            StructField("SO4_UEQ_L", DoubleType(), True),
            StructField("CA_UEQ_L", DoubleType(), True),
            StructField("ANC_UEQ_L", DoubleType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("batch_id", IntegerType(), True)
        ])

    def _load_static_data(self):
        try:
            self.site_info = self.spark.read.parquet(SITE_INFO_PATH)
            self.methods = self.spark.read.parquet(METHODS_PATH)
            logger.info("Successfully loaded static data from HDFS")
        except Exception as e:
            logger.error(f"Error loading static data: {str(e)}")
            raise

    def _calculate_metrics(self, df):
        return df \
            .withWatermark("ingestion_timestamp", "1 hour") \
            .groupBy(
                window("ingestion_timestamp", "1 hour"),
                "WATERBODY_TYPE"
            ) \
            .agg(
                avg("PH_LAB").alias("avg_ph"),
                stddev("PH_LAB").alias("std_ph"),
                avg("SO4_UEQ_L").alias("avg_sulfate"),
                avg("CA_UEQ_L").alias("avg_calcium"),
                avg("ANC_UEQ_L").alias("avg_anc"),
                count("*").alias("sample_count"),
                countDistinct("SITE_ID").alias("unique_sites")
            )

    def process_stream(self):
        try:
            stream_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", ",".join(KAFKA_BOOTSTRAP_SERVERS)) \
                .option("subscribe", KAFKA_TOPIC) \
                .option("startingOffsets", "latest") \
                .load()

            parsed_df = stream_df \
                .select(from_json(col("value").cast("string"), self.schema).alias("data")) \
                .select("data.*")

            enriched_df = parsed_df \
                .join(self.site_info, "SITE_ID", "left") \
                .join(self.methods, "PROGRAM_ID", "left")

            metrics_df = self._calculate_metrics(enriched_df)

            raw_query = parsed_df \
                .writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", RAW_DATA_PATH) \
                .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/raw") \
                .start()

            metrics_query = metrics_df \
                .writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", METRICS_PATH) \
                .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/metrics") \
                .start()

            console_query = metrics_df \
                .writeStream \
                .outputMode("complete") \
                .format("console") \
                .start()

            logger.info("Started streaming queries")
            
            self.spark.streams.awaitAnyTermination()

        except Exception as e:
            logger.error(f"Error processing stream: {str(e)}")
            raise

def main():
    try:
        consumer = WaterQualityConsumer()
        consumer.process_stream()
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()