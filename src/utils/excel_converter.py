import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
import logging
from pathlib import Path
from datetime import datetime

import sys
sys.path.append(str(Path(__file__).parent.parent))
from config.config import *
from utils.logging_setup import setup_logging

logger = setup_logging('DataConverter', 'converter.log')

def convert_ltm_to_parquet():
    spark = SparkSession.builder \
        .appName("ExcelToParquet") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    ltm_schema = StructType([
        StructField("SITE_ID", StringType(), True),
        StructField("PROGRAM_ID", StringType(), True),
        StructField("DATE_SMP", TimestampType(), True),
        StructField("SAMPLE_LOCATION", StringType(), True),
        StructField("SAMPLE_TYPE", StringType(), True),
        StructField("WATERBODY_TYPE", StringType(), True),
        StructField("PH_LAB", DoubleType(), True),
        StructField("SO4_UEQ_L", DoubleType(), True),
        StructField("CA_UEQ_L", DoubleType(), True),
        StructField("ANC_UEQ_L", DoubleType(), True)
    ])

    try:
        excel_path = os.path.join(RAW_DATA_DIR, LTM_DATA_FILE)
        logger.info(f"Reading Excel file: {excel_path}")
        
        df = pd.read_excel(excel_path)
        
        df['ingestion_timestamp'] = datetime.now()
        df['batch_id'] = 1 
        
        df.columns = [col.upper().replace(" ", "_") for col in df.columns]
        
        spark_df = spark.createDataFrame(df)
        
        os.makedirs(RAW_DATA_PATH, exist_ok=True)
        
        output_path = RAW_DATA_PATH
        logger.info(f"Writing to Parquet: {output_path}")
        
        spark_df.write.mode("overwrite").parquet(output_path)
        
        verification_df = spark.read.parquet(output_path)
        record_count = verification_df.count()
        logger.info(f"Successfully converted {record_count} records to Parquet")
        
        logger.info("\nSample of converted data:")
        verification_df.show(5)
        
        logger.info("\nParquet Schema:")
        verification_df.printSchema()
        
    except Exception as e:
        logger.error(f"Error during conversion: {str(e)}", exc_info=True)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    convert_ltm_to_parquet()