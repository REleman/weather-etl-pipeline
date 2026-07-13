from sqlalchemy import create_engine 
import pandas as pd
import logging as log
from telegram_logging import TelegramHandler, TelegramFormatter
from dotenv import load_dotenv
import os
import boto3
from botocore.client import Config
import io

load_dotenv(dotenv_path='/home/weather-etl-pipeline/config/.env')

tg_token = os.getenv("TG_TOKEN")
tg_chat_id = os.getenv("TG_CHAT_ID")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
s3_endpoint_url = os.getenv("S3_ENDPOINT_URL")
minio_aws_access_key_id = os.getenv("MINIO_AWS_ACCESS_KEY_ID")
minio_aws_secret_access_key = os.getenv("MINIO_AWS_SECRET_ACCESS_KEY")

formatter = TelegramFormatter(
    fmt="[%(asctime)s %(name)s] %(levelname)8s\n\n%(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
    use_emoji=True,
    emoji_map={
        log.DEBUG: "🐛",
        log.INFO: "💡",
        log.ERROR: "🚨",
    })

handler = TelegramHandler(
    bot_token=tg_token, #Используйте свой .env
    chat_id=tg_chat_id) #Используйте свой .env

handler.setFormatter(formatter)

telelogger = log.getLogger(__name__)
telelogger.addHandler(handler)

log.basicConfig(level=log.INFO, filename="py_log_load.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

#Подключение к MinIO
def get_s3_client():
    return boto3.client(
            's3',
            endpoint_url = s3_endpoint_url,
            aws_access_key_id = minio_aws_access_key_id,
            aws_secret_access_key = minio_aws_secret_access_key,
            region_name = 'us-east-1',
            config = Config(signature_version = 's3v4')
        )

#Подключение к postgres
def connect_to_database():
    try:
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        log.info("INFO. Successfully connected to the database")
        return engine
    except Exception as err:
        log.error(f"ERROR. Database connection failed ({err})")
        telelogger.error(f"ERROR. Database connection failed ({err})")
        raise

#Получение parquet файла из MinIO
def get_silver_data_in_s3(bucket_name, prefix = "silver/"):
    try:
        s3 = get_s3_client()
        response = s3.list_objects_v2(
            Bucket = bucket_name,
            Prefix = prefix
        )

        if 'Contents' not in response:
            log.error("ERROR. File not fount")
            raise FileNotFoundError("File not found")

        latest_file = sorted(response['Contents'], key = lambda x: x['LastModified'], reverse = True)[0]
        return latest_file['Key']
    except Exception as err:
        log.error(f"ERROR. Cant access to file list - {err}")
        raise

#Чтение parquet файла
def read_parquet_from_s3(bucket_name, key):
    try:
        s3 = get_s3_client()
        response = s3.get_object(Bucket=bucket_name, Key=key)
        parquet_data = response['Body'].read()
        df = pd.read_parquet(io.BytesIO(parquet_data))
        return df
    except Exception as err:
        log.error(f"ERROR. Cant read Parquet from S3 - {err}")
        raise

#Загрузка parquet в postgres
def load_data():
    try:
        latest_key = get_silver_data_in_s3("weather-data")
        dataframe = read_parquet_from_s3("weather-data", latest_key)
        engine = connect_to_database()
        dataframe.to_sql('weather', engine, if_exists = 'append', index = False)
        log.info(f"INFO. Data added to database. \nData info:\n{len(dataframe)} rows\n from silver/")
        telelogger.info(f"INFO. Data added to database. \nData info:\n{len(dataframe)} rows\n from silver/")
    except Exception as err:
        log.error(f"ERROR. Cant add data in database ({err})")    
        telelogger.error(f"ERROR. Cant add data in database ({err})")
        raise

if __name__ == "__main__":
    load_data()