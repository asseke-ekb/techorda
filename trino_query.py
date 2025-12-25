"""
Скрипт для подключения к Trino и выполнения запросов.
Установка: pip install trino
"""

from trino.dbapi import connect

# Подключение к Trino
# Попробуем разные порты: 8080 (default), 30300, 32770, 31390
TRINO_PORT = 32770  # порт Trino

conn = connect(
    host='109.248.170.228',
    port=TRINO_PORT,
    user='admin',
    catalog='iceberg',
    schema='bronze'
)

cursor = conn.cursor()

# 1. Показать все таблицы
print("=== TABLES ===")
cursor.execute("SHOW TABLES FROM bronze")
tables = cursor.fetchall()
for t in tables:
    print(t[0])

# 2. Структура таблицы отчётов
print("\n=== DESCRIBE service_techordareport_cdc_test ===")
try:
    cursor.execute("DESCRIBE bronze.service_techordareport_cdc_test")
    columns = cursor.fetchall()
    for col in columns:
        print(f"{col[0]}: {col[1]}")
except Exception as e:
    print(f"Error: {e}")

# 3. Примеры данных
print("\n=== SAMPLE DATA ===")
try:
    cursor.execute("""
        SELECT *
        FROM bronze.service_techordareport_cdc_test
        LIMIT 5
    """)
    rows = cursor.fetchall()
    for row in rows:
        print(row)
except Exception as e:
    print(f"Error: {e}")

cursor.close()
conn.close()
