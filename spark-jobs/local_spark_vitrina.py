#!/usr/bin/env python3
"""
Локальный PySpark скрипт для подключения к Spark Thrift Server.

Подключается через JDBC к Spark Thrift (HiveServer2) на 109.248.170.228:31000.

Установка:
    pip install pyspark==3.5.0

Запуск:
    set JAVA_HOME=C:/Users/asseke/Desktop/tech/jdk-17.0.17+10
    python local_spark_vitrina.py

    # С фильтрами
    python local_spark_vitrina.py --year 2024
    python local_spark_vitrina.py --year 2024 --report-type quarter1
"""

import os
import sys
import argparse
from datetime import datetime

# Устанавливаем JAVA_HOME
JAVA_HOME = "C:/Users/asseke/Desktop/tech/jdk-17.0.17+10"
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + ";" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType


# Конфигурация подключения
SPARK_THRIFT_HOST = "109.248.170.228"
SPARK_THRIFT_PORT = "31000"
JDBC_URL = f"jdbc:hive2://{SPARK_THRIFT_HOST}:{SPARK_THRIFT_PORT}/iceberg.bronze"


def create_spark_session():
    """Создаёт локальную SparkSession."""
    return (
        SparkSession.builder
        .appName("local-vitrina")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def read_via_jdbc(spark, query: str):
    """
    Читает данные через JDBC (Spark Thrift Server).

    Примечание: Spark Thrift может не работать с Iceberg
    из-за отсутствия iceberg-spark-runtime JAR.
    """
    return (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", "org.apache.hive.jdbc.HiveDriver")
        .option("query", query)
        .load()
    )


def build_vitrina_query(year: int = None, report_type: str = None, limit: int = 100) -> str:
    """SQL запрос для витрины."""
    # ВАЖНО: Исключаем удалённые записи (op != 'd') - замечание от Дмитрия
    where_clauses = ["status = 'signed'", "op != 'd'"]

    if year:
        where_clauses.append(f"year = {year}")
    else:
        where_clauses.append("year >= 2019")

    if report_type:
        where_clauses.append(f"report_type = '{report_type}'")

    where_sql = " AND ".join(where_clauses)

    return f"""
SELECT
    id as report_id,
    year,
    report_type,
    status,
    signed_at,
    version,
    created_at,
    updated_at,
    service_request_id,
    author_id,
    data
FROM service_report_cdc
WHERE {where_sql}
ORDER BY year DESC, report_type, id DESC
LIMIT {limit}
"""


def extract_json_fields(df):
    """Извлекает поля из JSON колонки data."""
    return df.select(
        F.col("report_id"),
        F.col("year"),
        F.col("report_type"),
        F.col("status"),
        F.col("signed_at"),
        F.col("version"),
        F.col("created_at"),
        F.col("updated_at"),
        F.col("service_request_id"),
        F.col("author_id"),

        # Компания
        F.get_json_object(F.col("data"), "$.company_name").alias("company_name"),
        F.get_json_object(F.col("data"), "$.company_tin").alias("company_tin"),
        F.get_json_object(F.col("data"), "$.certificate_number").cast(IntegerType()).alias("certificate_number"),

        # Сотрудники
        F.get_json_object(F.col("data"), "$.residents_count").cast(IntegerType()).alias("residents_count"),
        F.get_json_object(F.col("data"), "$.nonresidents_count").cast(IntegerType()).alias("nonresidents_count"),
        F.get_json_object(F.col("data"), "$.gph_count").cast(IntegerType()).alias("gph_count"),

        # Доходы
        F.get_json_object(F.col("data"), "$.income_total").cast(LongType()).alias("income_total"),
        F.get_json_object(F.col("data"), "$.income_international").cast(LongType()).alias("income_international"),

        # Инвестиции
        F.get_json_object(F.col("data"), "$.main_capital_investments").cast(LongType()).alias("main_capital_investments"),
        F.get_json_object(F.col("data"), "$.finance_source_increase_authorized_capital").cast(LongType()).alias("finance_source_increase_authorized_capital"),

        # Инвесторы
        F.get_json_object(F.col("data"), "$.investor_amount").cast(LongType()).alias("investor_amount"),

        # Займы
        F.get_json_object(F.col("data"), "$.finance_source_loan").cast(LongType()).alias("finance_source_loan"),
        F.get_json_object(F.col("data"), "$.finance_source_loan_foreign").cast(LongType()).alias("finance_source_loan_foreign"),

        # Господдержка
        F.get_json_object(F.col("data"), "$.finance_source_government").cast(LongType()).alias("finance_source_government"),

        # Налоговые льготы
        F.get_json_object(F.col("data"), "$.tax_incentives").cast(LongType()).alias("tax_incentives"),

        # ОКЭД и исполнитель
        F.get_json_object(F.col("data"), "$.oked").alias("oked"),
        F.get_json_object(F.col("data"), "$.executor_fullname").alias("executor_fullname"),
    )


def save_to_csv(df, output_path: str):
    """Сохраняет в CSV (один файл)."""
    # Собираем в один файл через pandas
    pdf = df.toPandas()
    pdf.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"Saved to: {output_path}")
    return pdf


def main():
    parser = argparse.ArgumentParser(description="Local PySpark vitrina export")
    parser.add_argument("--year", type=int, help="Filter by year")
    parser.add_argument("--report-type", type=str, help="Filter by report type")
    parser.add_argument("--limit", type=int, default=100, help="Limit rows")
    parser.add_argument("--output", type=str, help="Output CSV file")
    parser.add_argument("--test-connection", action="store_true", help="Only test connection")

    args = parser.parse_args()

    print("=" * 60)
    print(f"Local PySpark Vitrina - {datetime.now().isoformat()}")
    print("=" * 60)
    print(f"JAVA_HOME: {JAVA_HOME}")
    print(f"Spark Thrift: {SPARK_THRIFT_HOST}:{SPARK_THRIFT_PORT}")
    print(f"Year: {args.year or 'all'}")
    print(f"Report type: {args.report_type or 'all'}")
    print(f"Limit: {args.limit}")
    print("=" * 60)

    # Проверяем Java
    java_exe = os.path.join(JAVA_HOME, "bin", "java.exe")
    if not os.path.exists(java_exe):
        print(f"ERROR: Java not found at {java_exe}")
        sys.exit(1)

    print("\nCreating Spark session...")
    spark = create_spark_session()

    try:
        if args.test_connection:
            # Тест простого запроса - SELECT вместо SHOW
            print("\nTesting connection with: SELECT from service_report_cdc")
            try:
                df = read_via_jdbc(spark, "SELECT id, year, report_type, status FROM service_report_cdc LIMIT 5")
                df.show()
                print("Connection successful!")
            except Exception as e:
                print(f"Connection failed: {e}")
            return

        # Основной запрос
        query = build_vitrina_query(args.year, args.report_type, args.limit)
        print(f"\nExecuting query...")
        print("-" * 40)

        df = read_via_jdbc(spark, query)

        # Извлекаем JSON поля
        print("Extracting JSON fields...")
        vitrina = extract_json_fields(df)

        row_count = vitrina.count()
        print(f"\nResults: {row_count} rows, {len(vitrina.columns)} columns")

        # Показываем превью
        print("\nFirst 5 rows:")
        vitrina.show(5, truncate=False)

        # Сохранение
        if args.output:
            output_path = args.output
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"vitrina_{timestamp}.csv"

        pdf = save_to_csv(vitrina, output_path)

        # Статистика
        print("\nNumeric summary:")
        numeric_cols = ['residents_count', 'nonresidents_count', 'income_total', 'tax_incentives']
        existing_cols = [c for c in numeric_cols if c in pdf.columns]
        if existing_cols:
            print(pdf[existing_cols].describe())

        print("\nDone!")

    except Exception as e:
        print(f"\nError: {e}")
        print("\n" + "=" * 60)
        print("TROUBLESHOOTING:")
        print("=" * 60)
        print("""
1. Spark Thrift Server может не поддерживать Iceberg SELECT.
   Ошибка 'InstantiationException' означает отсутствие JAR.

2. Альтернативы:
   - Используйте Argo Workflow (уже настроен)
   - Попросите DevOps добавить iceberg-spark-runtime JAR
   - Получите kubeconfig для kubectl port-forward

3. Проверьте доступность:
   telnet 109.248.170.228 31000
""")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
