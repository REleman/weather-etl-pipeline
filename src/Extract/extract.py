import logging as log 
import pandas as pd
import requests as req
from requests.exceptions import HTTPError, ConnectionError, Timeout 
import datetime as dt
from telegram_logging import TelegramHandler, TelegramFormatter
from dotenv import load_dotenv
import os
import boto3
from botocore.client import Config

load_dotenv(dotenv_path='/home/weather-etl-pipeline/config/.env')

tg_token = os.getenv("TG_TOKEN")
tg_chat_id = os.getenv("TG_CHAT_ID")
api_token = os.getenv("API_KEY")
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

LAT = 55.750556 #Широта 
LON = 37.617500 #Долгота 


API_PATH = f"https://api.openweathermap.org/data/2.5/weather?lat={LAT}&lon={LON}&appid={api_token}"

telelogger = log.getLogger(__name__)
telelogger.addHandler(handler)

log.basicConfig(level=log.INFO, filename="py_log.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

# Получение данных с API
def get_data():
    try:
        response = req.get(API_PATH)
        response.raise_for_status()
        log.info("INFO. Data is been added!")
        return response.json()
    except ConnectionError:
        log.error("ERROR. Internet lost")
        telelogger.error("ERROR. Lost internet conection")
        raise
    except Timeout:
        log.error("ERROR. Server not response")
        telelogger.error("ERROR. Server not response")
        raise
    except HTTPError as httper:
        log.error(f"ERROR. Server access error - {httper}")
        telelogger.error(f"ERROR. Server access error ({httper})")
        raise
    except req.exceptions.RequestException as err:
        log.error(f"ERROR. Not user error - {err}")
        telelogger.error(f"ERROR. Not user error ({err})")
        raise

#Создание подклюения к MinIO
def get_s3_client():
    return boto3.client(
            's3',
            endpoint_url = s3_endpoint_url,
            aws_access_key_id = minio_aws_access_key_id,
            aws_secret_access_key = minio_aws_secret_access_key,
            region_name = 'us-east-1',
            config = Config(signature_version = 's3v4')
        )
 
#Сохранение сырых данных в MinIO /raw
def save_raw_data():
    try:
        s3 = get_s3_client()
        log.info("INFO. S3 connection successful")
    except Exception as err:
        log.error(f"ERROR. Cant create s3 connection - {err}")
        raise
    
    try:
        bucket_name = 'weather-data'
        if not s3.list_buckets().get('Buckets', []) or not any(b['Name'] == bucket_name for b in s3.list_buckets()['Buckets']):
            s3.create_bucket(Bucket = bucket_name)
        log.info("IFNO. Bucket has been found")
    except Exception as err:
        log.error(f"ERROR. Cant create bucket - {err}")
        raise
    
    try:
        data = get_data()
        file_name = dt.datetime.now().strftime("%y-%m-%d_%H-%M-%S")
        df = pd.DataFrame([data])
        json_str = df.to_json(orient = 'records')
        s3.put_object(
            Bucket = bucket_name,
            Key = f"raw/year={today.year}/month={today.month}/day={today.day}/{today.isoformat(sep=' ', timespec='seconds')}.json",
            Body = json_str
        )
        log.info(f"INFO. Data if been added to Bucket weather-data in raw/, check {s3_endpoint_url}")
    except Exception as err:
        log.error(f"ERRRO. Cant upload json file in s3 - {err}")
        raise
    
    
if __name__ == "__main__":
    save_raw_data()