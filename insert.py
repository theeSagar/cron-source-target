import random, string, psycopg2
from datetime import datetime
from dotenv import load_dotenv

import os

load_dotenv()


def random_string(length=5):
    return ''.join(random.choices(string.ascii_uppercase, k=length))

conn = psycopg2.connect(
    dbname=os.getenv("POSTGRES_NAME"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT")
)

cursor = conn.cursor()
# for _ in range(5):
val = random_string()
cursor.execute("INSERT INTO source (data, created_at) VALUES (%s, %s)", (val, datetime.utcnow()))
conn.commit()
cursor.close()
conn.close()