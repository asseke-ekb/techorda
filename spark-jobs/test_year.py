#!/usr/bin/env python3
"""
Тестовый скрипт для проверки агрегации по году.

Запуск на кластере:
    spark-submit \
      --master spark://spark-master-service.iceberg-spark.svc.cluster.local:7077 \
      --deploy-mode client \
      /opt/spark/app/test_year.py 2024
"""

from pyspark.sql import SparkSession
import sys

year = int(sys.argv[1])

spark = (
    SparkSession.builder
    .appName("test-year-aggregation")
    .getOrCreate()
)

df = spark.sql(f"""
    SELECT
        COUNT(*) AS total_reports,
        SUM(CAST(get_json_object(data, '$.income_total') AS BIGINT)) AS income_total
    FROM bronze.service_report_cdc
    WHERE year = {year}
      AND status = 'signed'
""")

result = df.collect()[0]

print(f"[RESULT] year={year}")
print(f"[RESULT] total_reports={result['total_reports']}, income_total={result['income_total']}")

spark.stop()
