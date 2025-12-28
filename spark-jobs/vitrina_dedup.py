#!/usr/bin/env python3
"""
PySpark скрипт для построения витрины отчётов с дедупликацией.

Использует ROW_NUMBER() для выбора последней версии отчёта
по каждой комбинации (service_request_id, report_type, year).

Запуск на кластере:
    spark-submit vitrina_dedup.py
    spark-submit vitrina_dedup.py --output-format iceberg --output-table gold.reports_vitrina

Локальный запуск (Python 3.11):
    C:/Python311/python.exe vitrina_dedup.py --output-format csv
"""

import os
import sys
import argparse
from datetime import datetime

# Локальные настройки (игнорируются на кластере если JAVA_HOME уже установлен)
JAVA_HOME = "C:/Users/asseke/Desktop/tech/jdk-17.0.17+10"
if os.path.exists(JAVA_HOME) and "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = JAVA_HOME
    os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, StringType


def create_spark_session(app_name: str = "vitrina-dedup") -> SparkSession:
    """Создаёт SparkSession с поддержкой Iceberg."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def read_reports(spark: SparkSession):
    """Читает отчёты из bronze.service_report_cdc."""
    return spark.table("iceberg.bronze.service_report_cdc")


def build_vitrina_with_dedup(df):
    """
    Строит витрину с дедупликацией и учётом CDC операций.

    Логика:
    1. Берём последнюю версию записи по (service_request_id, report_type, year)
       используя ROW_NUMBER() ORDER BY updated_at DESC
    2. Исключаем записи с op='d' (удалённые)
    3. Оставляем только op='c' (create) или op='u' (update)
    """

    # Окно для дедупликации: последняя версия по updated_at
    window_spec = Window.partitionBy(
        "service_request_id", "report_type", "year"
    ).orderBy(F.desc("updated_at"))

    # Добавляем номер строки
    ranked = df.withColumn("rn", F.row_number().over(window_spec))

    # Фильтруем: только первую (последнюю по времени) запись
    # и исключаем удалённые записи (op='d')
    deduped = ranked.filter(
        (F.col("rn") == 1) &
        (F.col("op") != "d")  # исключаем DELETE операции
    )

    # Строим витрину
    vitrina = deduped.select(
        # Идентификация
        F.col("id").alias("report_id"),
        F.col("service_request_id"),
        F.col("year"),
        F.col("report_type"),
        F.col("status"),
        F.col("version"),
        F.col("created_at"),
        F.col("updated_at"),
        F.col("signed_at"),
        F.col("author_id"),

        # Компания
        F.get_json_object(F.col("data"), "$.company_tin").alias("company_tin"),
        F.get_json_object(F.col("data"), "$.company_name").alias("company_name"),
        F.get_json_object(F.col("data"), "$.certificate_number").cast(StringType()).alias("certificate_number"),
        F.get_json_object(F.col("data"), "$.oked").cast(StringType()).alias("oked"),

        # Сотрудники
        F.get_json_object(F.col("data"), "$.residents_count").cast(LongType()).alias("residents_count"),
        F.get_json_object(F.col("data"), "$.nonresidents_count").cast(LongType()).alias("nonresidents_count"),
        F.get_json_object(F.col("data"), "$.gph_count").cast(LongType()).alias("gph_count"),

        # Доходы
        F.get_json_object(F.col("data"), "$.income_total").cast(LongType()).alias("income_total"),
        F.get_json_object(F.col("data"), "$.income_international").cast(LongType()).alias("income_international"),

        # Инвестиции / финансирование
        F.get_json_object(F.col("data"), "$.finance_source_increase_authorized_capital").cast(LongType()).alias("finance_source_increase_authorized_capital"),
        F.get_json_object(F.col("data"), "$.main_capital_investments").cast(LongType()).alias("main_capital_investments"),
        F.get_json_object(F.col("data"), "$.finance_source_loan").cast(LongType()).alias("finance_source_loan"),
        F.get_json_object(F.col("data"), "$.finance_source_loan_foreign").cast(LongType()).alias("finance_source_loan_foreign"),
        F.get_json_object(F.col("data"), "$.finance_source_government").cast(LongType()).alias("finance_source_government"),
        F.get_json_object(F.col("data"), "$.finance_source_investment").cast(LongType()).alias("finance_source_investment"),
        F.get_json_object(F.col("data"), "$.investor_amount").cast(LongType()).alias("investor_amount"),
        F.get_json_object(F.col("data"), "$.investor_country_company").alias("investor_country_company"),

        # Налоги
        F.get_json_object(F.col("data"), "$.tax_incentives").cast(LongType()).alias("tax_incentives"),
        F.get_json_object(F.col("data"), "$.tax_incentives_kpn").cast(LongType()).alias("tax_incentives_kpn"),
        F.get_json_object(F.col("data"), "$.tax_incentives_nds").cast(LongType()).alias("tax_incentives_nds"),
        F.get_json_object(F.col("data"), "$.tax_incentives_ipn").cast(LongType()).alias("tax_incentives_ipn"),
        F.get_json_object(F.col("data"), "$.tax_incentives_sn").cast(LongType()).alias("tax_incentives_sn"),

        # ETL метаданные
        F.current_timestamp().alias("etl_loaded_at"),
    )

    return vitrina


def save_to_csv(df, output_path: str):
    """Сохраняет DataFrame в CSV."""
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(output_path)
    )
    print(f"Saved to CSV: {output_path}")


def save_to_iceberg(df, table_name: str):
    """Сохраняет DataFrame в Iceberg таблицу."""
    (
        df.writeTo(f"iceberg.{table_name}")
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )
    print(f"Saved to Iceberg: iceberg.{table_name}")


def main():
    parser = argparse.ArgumentParser(description="Vitrina with deduplication")
    parser.add_argument("--output-format", choices=["csv", "iceberg"], default="csv")
    parser.add_argument("--output-path", type=str, default="/tmp/vitrina_dedup")
    parser.add_argument("--output-table", type=str, default="gold.reports_vitrina")

    args = parser.parse_args()

    print("=" * 60)
    print(f"Vitrina Dedup Export - {datetime.now().isoformat()}")
    print("=" * 60)
    print(f"Output format: {args.output_format}")
    print("=" * 60)

    spark = create_spark_session()

    try:
        # Читаем данные
        print("\nReading from iceberg.bronze.service_report_cdc...")
        df = read_reports(spark)

        total_count = df.count()
        print(f"Total records: {total_count}")

        # Строим витрину с дедупликацией
        print("\nBuilding vitrina with deduplication...")
        vitrina = build_vitrina_with_dedup(df)

        result_count = vitrina.count()
        print(f"After dedup: {result_count} records (removed {total_count - result_count} duplicates)")
        print(f"Columns: {len(vitrina.columns)}")

        # Показываем превью
        print("\nPreview:")
        vitrina.show(10, truncate=False)

        # Сохраняем
        if args.output_format == "csv":
            save_to_csv(vitrina, args.output_path)
        else:
            save_to_iceberg(vitrina, args.output_table)

        print("\nDone!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
