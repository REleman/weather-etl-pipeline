from sqlalchemy import create_engine 
import pandas as pd
import logging as log
from telegram_logging import TelegramHandler, TelegramFormatter
import glob
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='/home/weather-etl-pipeline/config/.env')

tg_token = os.getenv("TG_TOKEN")
tg_chat_id = os.getenv("TG_CHAT_ID")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
silver_path = os.getenv("SILVER_PATH")

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


def connect_to_database():
    try:
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        log.info("INFO. Successfully connected to the database")
        return engine
    except Exception as err:
        log.error(f"ERROR. Database connection failed ({err})")
        telelogger.error(f"ERROR. Database connection failed ({err})")
        raise


def load_file(path = silver_path):
    try:
        file = glob.glob(f"{path}/*.parquet")
        if not file:
            log.warning("WARNING. No parquet file")
            telelogger.warning("WARNING. No parquet file")
            raise FileNotFoundError("No parquet file")
        log.info("INFO. Parquet file found")
        latest_file = max(file, key=os.path.getctime)
        return latest_file
    except Exception as err:
        log.error(f"ERROR. Cant load paquet file ({err})")
        telelogger.error(f"ERROR. Cant load parquet file ({err})")
        raise

def load_data():
    try:
        latest_file = load_file()
        dataframe = pd.read_parquet(latest_file)
        engine = connect_to_database()
        dataframe.to_sql('weather', engine, if_exists = 'append', index = False)
        log.info(f"INFO. Data added to database. \nData info:\n{len(dataframe)} rows\n from {latest_file}")
        telelogger.info(f"INFO. Data added to database. \nData info:\n{len(dataframe)} rows\n from {latest_file}")
    except Exception as err:
        log.error(f"ERROR. Cant add data in database ({err})")    
        telelogger.error(f"ERROR. Cant add data in database ({err})")
        raise


if __name__ == "__main__":
    load_data()