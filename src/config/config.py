"""KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'water_quality'
BATCH_SIZE = 10
SLEEP_TIME = 10

HDFS_BASE_PATH = '/user/water_quality'
METHODS_PATH = f'{HDFS_BASE_PATH}/methods'
SITE_INFO_PATH = f'{HDFS_BASE_PATH}/site_information'
RAW_DATA_PATH = f'{HDFS_BASE_PATH}/raw_data'
METRICS_PATH = f'{HDFS_BASE_PATH}/metrics'

CHECKPOINT_LOCATION = '/tmp/water_quality_checkpoint'

RAW_DATA_DIR = 'data/raw'
LTM_DATA_FILE = 'LTM Data Aug 1 2022.xlsx'
METHODS_FILE = 'Methods.xlsx'
SITE_INFO_FILE = 'Site Information.xlsx'"""

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'water_quality'
BATCH_SIZE = 10
SLEEP_TIME = 10

import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, 'data')

LOCAL_STORAGE_PATH = os.path.join(DATA_DIR, 'processed')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
METHODS_PATH = os.path.join(LOCAL_STORAGE_PATH, 'methods')
SITE_INFO_PATH = os.path.join(LOCAL_STORAGE_PATH, 'site_information')
RAW_DATA_PATH = os.path.join(LOCAL_STORAGE_PATH, 'raw_data')
METRICS_PATH = os.path.join(LOCAL_STORAGE_PATH, 'metrics')

os.makedirs(LOCAL_STORAGE_PATH, exist_ok=True)
os.makedirs(METHODS_PATH, exist_ok=True)
os.makedirs(SITE_INFO_PATH, exist_ok=True)
os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(METRICS_PATH, exist_ok=True)

LTM_DATA_FILE = 'LTM Data Aug 1 2022.xlsx'
METHODS_FILE = 'Methods.xlsx'
SITE_INFO_FILE = 'Site Information.xlsx'

CHECKPOINT_LOCATION = os.path.join(LOCAL_STORAGE_PATH, 'checkpoints')