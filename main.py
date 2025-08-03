import os
from dotenv import load_dotenv
import psycopg2


load_dotenv()


conn = psycopg2.connect(
    dbname=os.getenv("POSTGRES_NAME"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT")
)
print("16",conn)
# DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
cursor = conn.cursor() # we have create an object of the connection with databse and tables like a is like a temporary SQL workspace.

cursor.execute("""
    SELECT s.id, s.data
    FROM source s
    LEFT JOIN target t ON s.data = t.data
    WHERE t.data IS NULL
    ORDER BY s.id
    LIMIT 1;
""")

# rows = cursor.fetchall()
row = cursor.fetchone()

# print(rows)
# conn.commit()
# Print the fetched rows

if row:
    cursor.execute("INSERT INTO target (data) VALUES (%s);", (row[1],))
    conn.commit()
    print(f"Inserted: {row[1]}")
else:
    print("No new data to copy.")

cursor.close()
conn.close()
