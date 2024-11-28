from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime, timedelta
import sys
from pathlib import Path
import os
print(os.getcwd())

sys.path.append(str(Path(__file__).parent.parent))
from config.config import *
from utils.logging_setup import setup_logging

logger = setup_logging('WaterQualityProcessor', 'processor.log')

class DataReprocessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("WaterQualityReprocessing") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.default.parallelism", "4") \
            .getOrCreate()
        
        self.raw_data_schema = StructType([
            StructField("PROGRAM_ID", StringType(), True),
            StructField("SITE_ID", StringType(), True),
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
        
        self.methods_schema = StructType([
            StructField("PROGRAM_ID", StringType(), True),
            StructField("PARAM", StringType(), True),
            StructField("START_YR", StringType(), True),
            StructField("END_YR", DoubleType(), True),
            StructField("METHOD_CODE", StringType(), True),
            StructField("METHOD_NAME", StringType(), True)
        ])
        
        self._load_static_data()
        logger.info("Initialized data reprocessor")

    def _load_static_data(self):
        try:
            logger.info(f"Loading methods from {METHODS_PATH}")
            self.methods = self.spark.read.parquet(METHODS_PATH)
            logger.info(f"Loaded methods with {self.methods.count()} records")
            
        except Exception as e:
            logger.error(f"Error loading static data: {str(e)}")
            raise

    def _calculate_metrics(self, df):
        return df.groupBy(
            window("ingestion_timestamp", "1 hour"),
            "WATERBODY_TYPE"
        ).agg(
            avg("PH_LAB").alias("avg_ph"),
            stddev("PH_LAB").alias("std_ph"),
            avg("SO4_UEQ_L").alias("avg_sulfate"),
            avg("CA_UEQ_L").alias("avg_calcium"),
            avg("ANC_UEQ_L").alias("avg_anc"),
            count("*").alias("sample_count"),
            countDistinct("SITE_ID").alias("unique_sites")
        )

    def _check_raw_data_exists(self):
        try:
            if not os.path.exists(RAW_DATA_PATH):
                logger.warning(f"Raw data path {RAW_DATA_PATH} does not exist. No data to process.")
                return False
                
            sample = self.spark.read.schema(self.raw_data_schema).parquet(RAW_DATA_PATH)
            count = sample.count()
            
            if count == 0:
                logger.warning("Raw data path exists but contains no data.")
                return False
                
            logger.info(f"Found {count} records in raw data.")
            return True
            
        except Exception as e:
            logger.warning(f"Error checking raw data: {str(e)}")
            return False

    def reprocess_time_range(self, start_time, end_time, version_id=None):
        try:
            logger.info(f"Starting reprocessing for time range: {start_time} to {end_time}")
            
            if not self._check_raw_data_exists():
                logger.error("No raw data available for processing")
                return None
            
            raw_data = self.spark.read.schema(self.raw_data_schema).parquet(RAW_DATA_PATH)
            logger.info(f"Read {raw_data.count()} records from raw data")
            
            filtered_data = raw_data.filter(
                (col("ingestion_timestamp") >= start_time) &
                (col("ingestion_timestamp") <= end_time)
            )
            
            logger.info(f"Filtered to {filtered_data.count()} records in time range")
            
            enriched_data = filtered_data.join(
                self.methods,
                ["PROGRAM_ID"],
                "left"
            )
            
            logger.info(f"Enriched data has {enriched_data.count()} records")
            
            metrics = self._calculate_metrics(enriched_data)
            
            if version_id is None:
                version_id = f"reprocessed_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            os.makedirs(METRICS_PATH, exist_ok=True)
            
            output_path = f"{METRICS_PATH}/version_{version_id}"
            metrics.write.mode("overwrite").parquet(output_path)
            
            logger.info(f"Successfully wrote reprocessed data to {output_path}")
            logger.info("\nSample of processed metrics:")
            metrics.show(5, truncate=False)
            
            return output_path
            
        except Exception as e:
            logger.error(f"Error during reprocessing: {str(e)}")
            raise

    def list_available_versions(self):
        try:
            if not os.path.exists(METRICS_PATH):
                logger.info("No metrics directory exists yet.")
                return []
                
            versions = [d for d in os.listdir(METRICS_PATH) 
                       if os.path.isdir(os.path.join(METRICS_PATH, d))]
            logger.info(f"Available versions: {versions}")
            return versions
            
        except Exception as e:
            logger.error(f"Error listing versions: {str(e)}")
            raise

def main():
    try:
        processor = DataReprocessor()
        
        end_time = datetime.now()
        start_time = end_time - timedelta(days=2000)
        
        logger.info("Listing existing versions:")
        processor.list_available_versions()
        
        new_version = processor.reprocess_time_range(
            start_time=start_time,
            end_time=end_time,
            version_id=f"daily_{end_time.strftime('%Y%m%d')}"
        )
        
        if new_version:
            logger.info(f"Reprocessing completed. New version available at: {new_version}")
        else:
            logger.warning("No data was processed.")
        
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()