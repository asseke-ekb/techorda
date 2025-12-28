#!/usr/bin/env python3
"""
Тестовый скрипт для проверки Spark на кластере.
Только читает данные и выводит статистику, ничего не записывает.

Запуск через Argo:
    script-path=s3a://lakehouse/scripts/test_spark_read.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test-spark-read").getOrCreate()

print("=" * 60)
print("TEST: Reading from iceberg.bronze.service_report_cdc")
print("=" * 60)

# Читаем таблицу
df = spark.table("iceberg.bronze.service_report_cdc")

# Базовая статистика
total = df.count()
print(f"Total records: {total}")

# Уникальные значения op (CDC операции)
print("\n--- CDC Operations (op) ---")
df.groupBy("op").count().show()

# Статусы
print("\n--- Statuses ---")
df.groupBy("status").count().show()

# Типы отчётов
print("\n--- Report Types ---")
df.groupBy("report_type").count().show()

# По годам
print("\n--- Years ---")
df.groupBy("year").count().orderBy("year").show()

# Проверка дедупликации: сколько дубликатов
print("\n--- Duplicates Check ---")
from pyspark.sql import functions as F
from pyspark.sql.window import Window

window = Window.partitionBy("service_request_id", "report_type", "year")
with_count = df.withColumn("cnt", F.count("*").over(window))
dups = with_count.filter(F.col("cnt") > 1).select("service_request_id", "report_type", "year", "cnt").distinct()
dup_count = dups.count()
print(f"Groups with duplicates: {dup_count}")

if dup_count > 0:
    print("Sample duplicates:")
    dups.show(5)

print("\n" + "=" * 60)
print("TEST COMPLETED SUCCESSFULLY")
print("=" * 60)

spark.stop()
