import pandas as pd
import psycopg2
import os

# Подключение к PostgreSQL через переменные среды
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT", 5432),
    sslmode='require'
)

query = """
SELECT chat_id, author_name, text, timestamp
FROM messages
WHERE timestamp >= now() - interval '7 days'
  AND text IS NOT NULL
ORDER BY chat_id, timestamp;
"""

print("📥 Загружаем данные из базы...")
df = pd.read_sql_query(query, conn)
conn.close()

output_file = "dialogs_last_week.csv"
df.to_csv(output_file, index=False)
print(f"✅ CSV сохранён как {output_file}")
