import pandas as pd
from pyspark.sql import SparkSession
import os
print(os.getcwd())

def create_spark_session():
    return SparkSession.builder \
        .appName("WaterQualityHDFSLoader") \
        .getOrCreate()

def load_to_hdfs():
    spark = create_spark_session()
    
    methods_df = pd.read_excel('data/raw/Methods.xlsx')
    spark_methods = spark.createDataFrame(methods_df)
    spark_methods.write.mode('overwrite').parquet('/user/water_quality/methods')
    
    site_info_df = pd.read_excel('data/raw/Site Information.xlsx')
    spark_site_info = spark.createDataFrame(site_info_df)
    spark_site_info.write.mode('overwrite').parquet('/user/water_quality/site_information')
    
    print("Files successfully loaded to HDFS")

if __name__ == "__main__":
    load_to_hdfs()