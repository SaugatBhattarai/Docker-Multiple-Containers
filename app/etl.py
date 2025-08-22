import psycopg2, os, time , json
from faker import Faker

fake = Faker()

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME", "warehouse")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

while True:
    try: 
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )

        cur = conn.cursor()

        data = {
            "name": fake.name(),
            "address": fake.address(),
            "email": fake.email(),
            "age": fake.random_int(min=18, max=99),
            "phone": fake.phone_number()
        }
        cur.execute("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name VARCHAR(100), address VARCHAR(255), email VARCHAR(100), age INT, phone VARCHAR(255))")
        cur.execute(
            """
            INSERT INTO users (name, address, email, age, phone)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (data["name"], data["address"], data["email"], data["age"], data["phone"])
        )

        conn.commit()
        cur.close()
        conn.close()
        print("Data inserted successfully",data)
    except Exception as e:
        print(f"Error: {e}")
        time.sleep(5000)