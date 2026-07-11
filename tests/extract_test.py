import pytest
<<<<<<< HEAD
=======
# from jsonschema import validate
import requests_mock
>>>>>>> f822f2315fd359219230f7a1a3a04f4bd338cae3
import requests
import sys

sys.path.append('/home/weather-etl-pipeline/')

from src.Extract.extract import get_data, API_PATH

test_weather_data = [{"coord":{"lon":37.6175,"lat":55.7504},
                          "weather":[{"id":500,"main":"Rain","description":"light rain","icon":"10d"}],
                          "base":"stations","main":{"temp":300.6,"feels_like":301.48,"temp_min":300.6,
                                                    "temp_max":300.6,"pressure":1014,"humidity":56,"sea_level":1014,"grnd_level":995},
                                                    "visibility":10000,"wind":{"speed":3.9,"deg":130,"gust":9.98},"rain":{"1h":0.29},
                                                    "clouds":{"all":86},"dt":1783697403,"sys":{"type":2,"id":2094500,"country":"RU","sunrise":1783645126,
                                                                                               "sunset":1783707032},"timezone":10800,"id":524901,"name":"Moscow",
                                                                                               "cod":200}]
    
def test_happy_path(requests_mock):
    #Сервер работает, возвращает правильные данные, хороший исход
    requests_mock.get(API_PATH, json = test_weather_data, status_code = 200)

    result = get_data()

    assert isinstance(result, list)
    assert isinstance(result[0], dict)
    assert result[0]["name"] == "Moscow"
   

def test_HTTPError(requests_mock):
    #Сервер вернул ошибку 401 
    requests_mock.get(API_PATH, json = test_weather_data, status_code = 401)

    with pytest.raises(requests.exceptions.HTTPError):
        get_data()

def test_ConnectionError(requests_mock):
    #Потеря интернет соединения
    requests_mock.get(API_PATH, exc = requests.exceptions.ConnectionError)

    with pytest.raises(requests.exceptions.ConnectionError):
        get_data()

def test_Timeout(requests_mock):
    #При таймауте
    requests_mock.get(API_PATH, exc = requests.exceptions.Timeout)
    
    with pytest.raises(requests.exceptions.Timeout):
        get_data()



