from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging
import os
print(os.getcwd())

#from src.config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'water_quality'
BATCH_SIZE = 10
SLEEP_TIME = 10

HDFS_BASE_PATH = '/user/water_quality'
METHODS_PATH = f'{HDFS_BASE_PATH}/methods'
SITE_INFO_PATH = f'{HDFS_BASE_PATH}/site_information'
RAW_DATA_PATH = f'{HDFS_BASE_PATH}/raw_data'
METRICS_PATH = f'{HDFS_BASE_PATH}/metrics'

CHECKPOINT_LOCATION = '/tmp/water_quality_checkpoint'

RAW_DATA_DIR = '../data/raw'
LTM_DATA_FILE = 'LTM Data Aug 1 2022.xlsx'
METHODS_FILE = 'Methods.xlsx'
SITE_INFO_FILE = 'Site Information.xlsx'

def setup_kafka():
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
    )

    topic = NewTopic(
        name=KAFKA_TOPIC,
        num_partitions=1,
        replication_factor=1
    )

    try:
        admin_client.create_topics([topic])
        print(f"Created topic: {KAFKA_TOPIC}")
    except TopicAlreadyExistsError:
        print(f"Topic {KAFKA_TOPIC} already exists")
    except Exception as e:
        print(f"Error creating topic: {str(e)}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    setup_kafka()