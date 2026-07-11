import logging as log 
import pandas as pd
import requests as req
from requests.exceptions import HTTPError, ConnectionError, Timeout 
import datetime as dt
from telegram_logging import TelegramHandler, TelegramFormatter
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path='/home/weather-etl-pipeline/config/.env')

tg_token = os.getenv("TG_TOKEN")
tg_chat_id = os.getenv("TG_CHAT_ID")
api_token = os.getenv("API_KEY")
raw_path = os.getenv("RAW_PATH")
log.info("DEBUG - " + raw_path)

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
 
def save_raw_data():
    try:
        data = get_data()
        file_name = dt.datetime.now().strftime("%y-%m-%d_%H-%M-%S")
        df = pd.DataFrame([data])
        df.to_json(os.path.join(raw_path, f"{file_name}.json"), orient='records')
        log.info("INFO. Data save in layers/raw as JSON")
    except Exception as err:
        log.error(f"ERROR. Error - {err}")
        telelogger.error(f"ERROR. Error ({err})")
        raise
    
    
if __name__ == "__main__":
    save_raw_data()