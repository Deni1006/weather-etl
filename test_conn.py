import psycopg2

try:
    conn = psycopg2.connect(
        user="postgres",
        password="teacher",
        host="localhost",
        port="5432",
        dbname="weather_db"
    )
    print("OK: подключение установлено")
    conn.close()
except Exception as e:
    print("Ошибка:", type(e).__name__, e)
