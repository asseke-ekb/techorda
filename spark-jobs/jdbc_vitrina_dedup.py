#!/usr/bin/env python3
"""
Витрина с дедупликацией через JDBC (как DBeaver).

Выполняет SQL с ROW_NUMBER() через JDBC, затем обрабатывает в PySpark.

Запуск:
    C:/Python311/python.exe jdbc_vitrina_dedup.py
    C:/Python311/python.exe jdbc_vitrina_dedup.py --output vitrina_dedup.csv
"""

import os
import sys
import argparse
from datetime import datetime

# JAVA_HOME
JAVA_HOME = "C:/Users/asseke/Desktop/tech/jdk-17.0.17+10"
if os.path.exists(JAVA_HOME) and "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = JAVA_HOME
    os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# JDBC настройки (такие же как в DBeaver)
JDBC_URL = "jdbc:hive2://109.248.170.228:31000/iceberg.bronze"
JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver"
JDBC_JAR = "C:/Users/asseke/Desktop/tech/hive-jdbc-standalone.jar"


def fetch_deduped_data():
    """
    Выполняет SQL с дедупликацией через JDBC.
    Точно такой же запрос как в DBeaver.
    """
    import jaydebeapi
    import pandas as pd

    # SQL с ROW_NUMBER() для дедупликации + учёт CDC операций (op)
    # op='c' - create, op='u' - update, op='d' - delete
    query = """
    WITH ranked AS (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY service_request_id, report_type, year
                ORDER BY updated_at DESC
            ) AS rn
        FROM bronze.service_report_cdc r
    )
    SELECT
        id,
        service_request_id,
        year,
        report_type,
        status,
        version,
        op,
        created_at,
        updated_at,
        signed_at,
        author_id,
        data
    FROM ranked
    WHERE rn = 1
      AND op != 'd'  -- исключаем удалённые записи
    ORDER BY year DESC, report_type, id DESC
    """

    print("Connecting via JDBC (same as DBeaver)...")
    print(f"  URL: {JDBC_URL}")

    conn = jaydebeapi.connect(
        JDBC_DRIVER, JDBC_URL, ["", ""], JDBC_JAR
    )
    cursor = conn.cursor()

    print("Executing deduplication query...")
    cursor.execute(query)

    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    print(f"Fetched {len(rows)} deduplicated rows")
    return pd.DataFrame(rows, columns=columns)


def build_vitrina_spark(pdf):
    """Строит витрину из pandas DataFrame через PySpark."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import LongType, StringType

    spark = (
        SparkSession.builder
        .appName("vitrina-dedup-jdbc")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Конвертируем pandas в Spark DataFrame
    columns = pdf.columns.tolist()
    rows = [tuple(x) for x in pdf.to_numpy()]
    rdd = spark.sparkContext.parallelize(rows)
    df = rdd.toDF(columns)

    # Извлекаем поля из JSON
    vitrina = df.select(
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

    return spark, vitrina


def main():
    parser = argparse.ArgumentParser(description="Vitrina with deduplication via JDBC")
    parser.add_argument("--output", type=str, help="Output CSV file")
    args = parser.parse_args()

    print("=" * 60)
    print(f"Vitrina Dedup (JDBC) - {datetime.now().isoformat()}")
    print("=" * 60)

    # Проверяем зависимости
    if not os.path.exists(JDBC_JAR):
        print(f"ERROR: JDBC JAR not found at {JDBC_JAR}")
        sys.exit(1)

    try:
        import jaydebeapi
    except ImportError:
        print("ERROR: jaydebeapi not installed. Run: pip install jaydebeapi")
        sys.exit(1)

    # Получаем дедуплицированные данные через JDBC
    pdf = fetch_deduped_data()

    if pdf.empty:
        print("No data found. Exiting.")
        return

    # Строим витрину через PySpark
    print("\nBuilding vitrina with PySpark...")
    spark, vitrina = build_vitrina_spark(pdf)

    try:
        result_count = vitrina.count()
        print(f"Result: {result_count} records, {len(vitrina.columns)} columns")

        print("\nPreview:")
        vitrina.show(10, truncate=False)

        # Сохраняем
        if args.output:
            output_path = args.output
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"vitrina_dedup_{timestamp}.csv"

        result_pdf = vitrina.toPandas()
        result_pdf.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\nSaved to: {output_path}")

        print("\nDone!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
