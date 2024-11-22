from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
import os
import logging
from pathlib import Path

import sys
sys.path.append(str(Path(__file__).parent.parent))
from config.config import *
from utils.logging_setup import setup_logging

logger = setup_logging('DataLoader', 'data_loader.log')

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

class DataLoader:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("WaterQualityDataLoader") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.default.parallelism", "4") \
            .getOrCreate()
        logger.info("Initialized Spark session")

    def _infer_schema_from_pandas(self, df):
        type_mapping = {
            'object': StringType(),
            'int64': LongType(),
            'float64': DoubleType(),
            'bool': BooleanType(),
            'datetime64[ns]': TimestampType()
        }
        
        fields = []
        for column, dtype in df.dtypes.items():
            spark_type = type_mapping.get(str(dtype), StringType())
            fields.append(StructField(column, spark_type, True))
        
        return StructType(fields)

    def load_excel_to_parquet(self, excel_path, parquet_path):
        try:
            logger.info(f"Reading Excel file: {excel_path}")
            pandas_df = pd.read_excel(excel_path)
            
            pandas_df.columns = [col.strip().replace(" ", "_").replace(".", "_") for col in pandas_df.columns]
            
            schema = self._infer_schema_from_pandas(pandas_df)
            logger.info(f"Inferred schema: {schema}")
            
            spark_df = self.spark.createDataFrame(pandas_df, schema=schema)
            
            logger.info(f"Writing Parquet file to: {parquet_path}")
            spark_df.write.mode("overwrite").parquet(parquet_path)
            
            count = spark_df.count()
            logger.info(f"Successfully wrote {count} records to {parquet_path}")
            
            return True
        except Exception as e:
            logger.error(f"Error processing {excel_path}: {str(e)}")
            return False

    def load_all_static_data(self):
        try:
            methods_excel = os.path.join(RAW_DATA_DIR, METHODS_FILE)
            success_methods = self.load_excel_to_parquet(methods_excel, METHODS_PATH)
            
            site_info_excel = os.path.join(RAW_DATA_DIR, SITE_INFO_FILE)
            success_site = self.load_excel_to_parquet(site_info_excel, SITE_INFO_PATH)
            
            if success_methods and success_site:
                logger.info("Successfully loaded all static data")
                return True
            else:
                logger.error("Failed to load some data files")
                return False
        except Exception as e:
            logger.error(f"Error loading static data: {str(e)}")
            return False

    def verify_data(self):
        try:
            methods_df = self.spark.read.parquet(METHODS_PATH)
            site_info_df = self.spark.read.parquet(SITE_INFO_PATH)
            
            logger.info("=== Data Verification ===")
            logger.info(f"Methods data count: {methods_df.count()}")
            logger.info("Methods columns: " + ", ".join(methods_df.columns))
            
            logger.info(f"Site Information data count: {site_info_df.count()}")
            logger.info("Site Information columns: " + ", ".join(site_info_df.columns))
            
            logger.info("\nSample of Methods data:")
            methods_df.show(5, truncate=False)
            
            logger.info("\nSample of Site Information data:")
            site_info_df.show(5, truncate=False)
            
            return True
        except Exception as e:
            logger.error(f"Error verifying data: {str(e)}")
            return False

def main():
    try:
        loader = DataLoader()
        if loader.load_all_static_data():
            loader.verify_data()
        else:
            logger.error("Failed to load data")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()