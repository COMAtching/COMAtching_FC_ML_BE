import os
from dotenv import load_dotenv

load_dotenv()

CSV_FILE_PATH_HOME = os.getenv('CSV_FILE_PATH_HOME')
CSV_FILE_PATH_AWAY = os.getenv('CSV_FILE_PATH_AWAY')
RABBITMQ_URL = os.getenv('RABBITMQ_URL')
RABBITMQ_USERNAME = os.getenv('RABBITMQ_USERNAME')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT'))
