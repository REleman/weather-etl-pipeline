from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append('/home/weather-etl-pipeline/')

from src.Extract.extract import save_raw_data
from src.Transform.transform import get_raw_data
from src.Load.load import load_data

default_args = {
    'owner': "REleman",
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    'weather_etl_pipeline',
    default_args=default_args,
    description="Простой ETL пайплайн сбора данных о погоде",
    schedule='*/15 * * * *', #Для удобства проверки поставил 15 минут
    catchup=False,
    tags=['weather', 'etl'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_weather',
        python_callable=save_raw_data,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=get_raw_data,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task