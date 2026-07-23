import pandas as pd
import logging as log
from telegram_logging import TelegramHandler, TelegramFormatter
import os 
from dotenv import load_dotenv
import boto3
from botocore.client import Config
import io
import json

load_dotenv(dotenv_path='/home/weather-etl-pipeline/config/.env')

tg_token = os.getenv("TG_TOKEN")
tg_chat_id = os.getenv("TG_CHAT_ID")
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

log.basicConfig(level=log.INFO, filename="py_log_transform.log",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

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

#Получение последнего файла из MinIO /raw
def get_raw_data_in_s3(bucket_name, prefix = "/raw"):
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

# Чтение данных из последнего файла
def read_json_from_s3(bucket_name, key):
    try:
        s3 = get_s3_client()
        response = s3.get_object(Bucket = bucket_name, Key = key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        return data
    except Exception as err:
        log.error(f"ERROR. Cant read JSON - {err}")
        raise

def get_raw_data():
    try:
        latest_key = get_raw_data_in_s3("weather-data", "raw/")
        latest_file = read_json_from_s3("weather-data", latest_key)
        folder_path = os.path.dirname(latest_key)
        silver_folder = folder_path.replace("raw", "silver")
        filename = os.path.basename(latest_key).replace('.json', '')
        df = pd.DataFrame(latest_file)
        log.info("INFO. Raw data loaded")
        copydf = transform(df.copy(), filename)
        log.info("INFO. Raw data transformed")
        save_transformed_data(copydf, silver_folder, filename)
    except Exception as err:
        log.error(f"ERROR. Cant load trasform data - {err}")

def transform(dataframe : pd.DataFrame, filename:str):
    try:
        coord_df = pd.json_normalize(dataframe['coord'])
        weather_df = pd.json_normalize(next(iter(dataframe['weather']))).drop(columns = 'id', errors = "ignore")
        main_df = pd.json_normalize(dataframe['main'])
        sys_df = pd.json_normalize(dataframe['sys']).drop(columns=["id"],errors = "ignore")
        wind_df = pd.json_normalize(dataframe["wind"])
        doc_data = dataframe
        date = dt.datetime.now().isoformat(sep=' ', timespec='seconds')
        doc_data["document_data"] = date
        transformed_df = pd.concat([dataframe['id'], dataframe["name"], sys_df, coord_df, weather_df, main_df, wind_df, doc_data["document_data"]], axis = 1)
        transformed_df["main"] = transformed_df["main"].str.lower()
        transformed_df = transformed_df.drop(["icon","gust"], axis = 1)
        return transformed_df
    except Exception as err:
        log.error(f"ERROR. Transfromation error ({err})")
        telelogger.error(f"ERROR. Transfromation error ({err})")
    
#Функция загрузки трансформированных данных в MinIO
def save_transformed_data(dataframe : pd.DataFrame, silver_folder, filename : str):
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
        bucket_name = 'weather-data'
        try:
            s3.head_bucket(Bucket=bucket_name)
        except:
            s3.create_bucket(Bucket=bucket_name)
            log.info("INFO. Bucket created")
        
        parquet_buffer = io.BytesIO()
        dataframe.to_parquet(parquet_buffer, index=False)
        
        s3_key = f"{silver_folder}/{filename}.parquet"
        s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=parquet_buffer.getvalue()
        )
        log.info(f"SUCCESS. Transformed data saved to s3://{bucket_name}/{s3_key}")
        
    except Exception as err:
        log.error(f"ERROR. Cant save transformed data: {err}")
        raise

if __name__ == "__main__":
    get_raw_data()