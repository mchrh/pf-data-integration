from kafka import KafkaProducer
import pandas as pd
import json
import time
from datetime import datetime
import logging
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from config.config import *

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/producer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('WaterQualityProducer')

class WaterQualityProducer:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, 
                 topic=KAFKA_TOPIC):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
            retries=3
        )
        self.topic = topic
        logger.info(f"Initialized producer for topic: {topic}")

    def load_data(self, file_path):
        try:
            logger.info(f"Loading data from {file_path}")
            df = pd.read_excel(file_path)
            
            if 'DATE_SMP' in df.columns:
                df['DATE_SMP'] = pd.to_datetime(df['DATE_SMP'])
            
            numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
            df[numeric_cols] = df[numeric_cols].astype(float)
            df = df.replace({pd.NA: None})
            
            logger.info(f"Successfully loaded {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise

    def stream_data(self, data_path, batch_size=BATCH_SIZE, sleep_time=SLEEP_TIME):
        try:
            df = self.load_data(data_path)
            records = df.to_dict('records')
            total_batches = (len(records) + batch_size - 1) // batch_size
            
            logger.info(f"Starting to stream {len(records)} records in batches of {batch_size}")
            
            batch_num = 0
            while True:  
                for i in range(0, len(records), batch_size):
                    batch = records[i:i+batch_size]
                    batch_num += 1
                    
                    for record in batch:
                        record['ingestion_timestamp'] = datetime.now().isoformat()
                        record['batch_id'] = batch_num
                    
                    try:
                        future = self.producer.send(self.topic, value=batch)
                        record_metadata = future.get(timeout=10)
                        
                        logger.info(f"Sent batch {batch_num}/{total_batches} to "
                                  f"partition {record_metadata.partition} at offset {record_metadata.offset}")
                        
                        self.producer.flush()
                        
                        time.sleep(sleep_time)
                        
                    except Exception as e:
                        logger.error(f"Error sending batch {batch_num}: {str(e)}")
                        continue
                
                logger.info("Completed one cycle of data streaming, starting over...")
                
        except KeyboardInterrupt:
            logger.info("Streaming interrupted by user")
            self.producer.close()
        except Exception as e:
            logger.error(f"Streaming error: {str(e)}")
            self.producer.close()
            raise

def main():
    try:
        file_path = os.path.join(RAW_DATA_DIR, LTM_DATA_FILE)
        
        producer = WaterQualityProducer()
        producer.stream_data(file_path)
        
    except Exception as e:
        logger.error(f"Main execution error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()