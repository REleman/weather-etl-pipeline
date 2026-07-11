import pandas as pd
import sys
import os
from sqlalchemy import create_engine

sys.path.append('/home/weather-etl-pipeline/')

from src.Load.load import load_data, load_file, silver_path, connect_to_database

def test_load_data_success(tmp_path, monkeypatch):
    """Тест проверяет, что данные из Parquet успешно читаются и записываются в БД"""

    test_df = pd.DataFrame([{
        "id": 524901,
        "name": "Moscow",
        "temp": 285.5
    }])
    
    parquet_file_path = os.path.join(tmp_path, "26-07-11_15-00-00.parquet")
    test_df.to_parquet(parquet_file_path)
    
    monkeypatch.setattr("src.Load.load.silver_path", str(tmp_path))
    
    test_engine = create_engine("sqlite:///:memory:")
    
    def mock_connect():
        return test_engine
    
    monkeypatch.setattr("src.Load.load.connect_to_database", mock_connect)

    load_data()
    
    db_result_df = pd.read_sql('weather', test_engine)
    
    assert len(db_result_df) == 1, "Данные не записались в базу"
    assert db_result_df['name'].iloc[0] == "Moscow"