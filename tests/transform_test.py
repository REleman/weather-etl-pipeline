import pandas as pd
<<<<<<< HEAD
=======
import pytest
>>>>>>> f822f2315fd359219230f7a1a3a04f4bd338cae3
import sys

sys.path.append('/home/weather-etl-pipeline/')

<<<<<<< HEAD
from src.Transform.transform import transform
=======
from src.Transform.transform import transform, get_raw_data
>>>>>>> f822f2315fd359219230f7a1a3a04f4bd338cae3

def test_transform_success():
    #Правильность очистки и объединения данных

    raw_data = [{"coord":{"lon":37.6175,"lat":55.7504},
                          "weather":[{"id":500,"main":"Rain","description":"light rain","icon":"10d"}],
                          "base":"stations","main":{"temp":300.6,"feels_like":301.48,"temp_min":300.6,
                                                    "temp_max":300.6,"pressure":1014,"humidity":56,"sea_level":1014,"grnd_level":995},
                                                    "visibility":10000,"wind":{"speed":3.9,"deg":130,"gust":9.98},"rain":{"1h":0.29},
                                                    "clouds":{"all":86},"dt":1783697403,"sys":{"type":2,"id":2094500,"country":"RU","sunrise":1783645126,
                                                                                               "sunset":1783707032},"timezone":10800,"id":524901,"name":"Moscow",
                                                                                               "cod":200}]

    df_input = pd.DataFrame(raw_data)
    test_file_name = "26-07-11_15-00-00.json"
    df_result = transform(df_input, test_file_name)

    assert df_result["main"].iloc[0] == 'rain'
    assert "icon" not in df_result.columns
    assert "gust" not in df_result.columns
    assert "document_data" in df_result.columns
<<<<<<< HEAD
    assert pd.api.types.is_datetime64_any_dtype(df_result['document_data'])
=======
    assert pd.api.types.is_datetime64_any_dtype(df_result['document_data'])

    
>>>>>>> f822f2315fd359219230f7a1a3a04f4bd338cae3
