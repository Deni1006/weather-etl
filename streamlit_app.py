# streamlit_app.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text

# --- Настройки базы данных ---
DB_USER = 'postgres'
DB_PASSWORD = 'teacher'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'weather_db'

engine = create_engine(
    f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}',
    echo=False,
)

# --- Загрузка списка городов ---
@st.cache_data(ttl=300)
def get_city_list():
    query = "SELECT DISTINCT city FROM weather ORDER BY city;"
    with engine.connect() as conn:
        result = conn.execute(text(query))
        cities = [row[0] for row in result]
    return cities

# --- Загрузка данных по конкретному городу ---
@st.cache_data(ttl=300)
def load_data(city, limit=720):
    query = text("""
        SELECT * FROM weather 
        WHERE city = :city
        ORDER BY datetime DESC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"city": city, "limit": limit}, parse_dates=['datetime'])
    return df.sort_values('datetime')

# --- Интерфейс ---
st.title("Погода (Open-Meteo)")

# Выбор города
city_list = get_city_list()
selected_city = st.sidebar.selectbox("Выберите город", city_list)

# Настройка лимита
limit = st.sidebar.slider("Сколько записей показать", 24, 1680, 168)

# Загрузка данных
df = load_data(selected_city, limit)

# --- Отображение ---
st.subheader(f"Таблица ({selected_city})")
st.dataframe(df)

st.subheader("Температура (°C)")
st.line_chart(df.set_index('datetime')['temperature'])

st.subheader("Осадки (мм)")
st.bar_chart(df.set_index('datetime')['precipitation'])
