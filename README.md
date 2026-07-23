# Weather ETL Pipeline

ETL-пайплайн для сбора данных о погоде из OpenWeatherMap API с оркестрацией через Apache Airflow.

Проект представляет собой развитие простого ETL-скрипта до полноценного пайплайна с оркестрацией в Apache Airflow.

[Ссылка на старый репозиторий](https://github.com/REleman/Simple-weather-pipeline)

## Архитектура

- **Extract**: сбор данных из API
- **Transform**: нормализация JSON, очистка, сохранение в Parquet
- **Load**: загрузка в PostgreSQL
- **Orchestration**: Apache Airflow

## Запуск

1. Клонируйте репозиторий
2. Создайте `.env` из `.env.example`
3. Установите зависимости: `pip install -r requirements.txt`
4. Запустите MinIO: `minio server ~/minio-data --console-address ":9001"`
5. Запустите Airflow: `airflow standalone`

*Подразумевается, что вы уже установиили MinIO и Airflow и запустити его в виртуальном окружении*

## Переменные окружения

См. `config/.env.example`

## Автор

[REleman](https://github.com/REleman)
