# get_weather.py
import requests
import pandas as pd
from sqlalchemy import create_engine, text

# --- Настройки базы данных ---
DB_USER = 'postgres'       # поменяй если нужно
DB_PASSWORD = 'teacher'    # поменяй на свой пароль
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'weather_db'

engine = create_engine(
    f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}',
    echo=False,
)

API_URL = "https://api.open-meteo.com/v1/forecast"

# --- Создание таблицы, если её нет ---
def create_table_if_not_exists():
    ddl = """
    CREATE TABLE IF NOT EXISTS weather (
        datetime TIMESTAMP NOT NULL,
        temperature NUMERIC,
        precipitation NUMERIC,
        city TEXT NOT NULL,
        PRIMARY KEY (datetime, city)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

# --- Загрузка данных по координатам города ---
def download_weather_data(city_name, latitude, longitude):
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,precipitation",
        "timezone": "Europe/Moscow"
    }
    resp = requests.get(API_URL, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame({
        "datetime": data["hourly"]["time"],
        "temperature": data["hourly"]["temperature_2m"],
        "precipitation": data["hourly"]["precipitation"]
    })
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["city"] = city_name
    return df

# --- Сохранение данных в PostgreSQL ---
def save_to_postgres(df):
    records = df.to_dict(orient="records")
    insert_sql = text("""
    INSERT INTO weather (datetime, temperature, precipitation, city)
    VALUES (:datetime, :temperature, :precipitation, :city)
    ON CONFLICT (datetime, city) DO UPDATE
      SET temperature = EXCLUDED.temperature,
          precipitation = EXCLUDED.precipitation;
    """)
    with engine.begin() as conn:
        conn.execute(insert_sql, records)
    print(f"{len(records)} строк записано/обновлено в PostgreSQL")

# --- Основной блок ---
if __name__ == "__main__":
    create_table_if_not_exists()

    # Города и их координаты
    cities = {
        "Moscow": (55.75, 37.62),
        "Saint Petersburg": (59.93, 30.31),
        "Kazan": (55.79, 49.12)
    }

    for city, coords in cities.items():
        df = download_weather_data(city, *coords)
        print(f"{city}: {len(df)} записей получено")
        save_to_postgres(df)
