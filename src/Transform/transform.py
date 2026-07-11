import pandas as pd
import logging as log
from telegram_logging import TelegramHandler, TelegramFormatter
import glob 
import os 
from dotenv import load_dotenv

load_dotenv(dotenv_path='/home/weather-etl-pipeline/config/.env')

tg_token = os.getenv("TG_TOKEN")
tg_chat_id = os.getenv("TG_CHAT_ID")
silver_path = os.getenv("SILVER_PATH")
raw_path = os.getenv("RAW_PATH")

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


def get_raw_data():
    files = glob.glob(f'{raw_path}*.json')
    if files:
        latest_file = max(files, key=os.path.getctime)
        log.info("INFO. Raw data file found")
    else:
        latest_file = False
        log.error("ERROR. Transformation raw file is not found")
        telelogger.warning("ERROR. Transformation raw file is not found")
    
    if latest_file:
        try:
            data = pd.read_json(latest_file)
            log.info("INFO. Raw data loaded")
            copydf = transform(data.copy(), latest_file)
            log.info("INFO. Raw data transformed")
            save_transformed_data(copydf, latest_file)
        except Exception as err:
            log.error("ERROR. Cant access to file")
            telelogger.error(f"ERROR. Cant access to file ({err})")
            raise

def transform(dataframe : pd.DataFrame, filename:str):
    try:
        coord_df = pd.json_normalize(dataframe['coord'])
        weather_df = pd.json_normalize(next(iter(dataframe['weather']))).drop(columns = 'id', errors = "ignore")
        main_df = pd.json_normalize(dataframe['main'])
        sys_df = pd.json_normalize(dataframe['sys']).drop(columns=["id"],errors = "ignore")
        wind_df = pd.json_normalize(dataframe["wind"])
        doc_data = dataframe
        date = os.path.basename(filename).split('.')[0]
        doc_data["document_data"] = pd.to_datetime(date, format = "%y-%m-%d_%H-%M-%S")
        transformed_df = pd.concat([dataframe['id'], dataframe["name"], sys_df, coord_df, weather_df, main_df, wind_df, doc_data["document_data"]], axis = 1)
        transformed_df["main"] = transformed_df["main"].str.lower()
        transformed_df = transformed_df.drop(["icon","gust"], axis = 1)
        return transformed_df
    except Exception as err:
        log.error(f"ERROR. Transfromation error ({err})")
        telelogger.error(f"ERROR. Transfromation error ({err})")
    
def save_transformed_data(dataframe : pd.DataFrame,filename : str):
    try:
        date = os.path.basename(filename).split('.')[0]
        dataframe.to_parquet(os.path.join(silver_path, f"{date}.parquet"))
        log.info("INFO. Transformed data added in layers/silver")
    except Exception as err:
        log.error(f"ERROR. Error save transformed data - {err}")
        telelogger.error(f"ERROR. Error save transformed data - {err}")

if __name__ == "__main__":
    get_raw_data()