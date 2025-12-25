#!/usr/bin/env python3
"""
PySpark скрипт для построения витрины отчётов из Iceberg через JDBC.

Использует jaydebeapi для чтения данных, затем обрабатывает через PySpark.

Запуск:
    python pyspark_vitrina.py
    python pyspark_vitrina.py --year 2024
    python pyspark_vitrina.py --year 2024 --report-type quarter1 --output vitrina.csv
"""

import os
import sys
import argparse
from datetime import datetime

# JAVA_HOME
JAVA_HOME = "C:/Users/asseke/Desktop/tech/jdk-17.0.17+10"
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

# PySpark Python path (Windows не имеет python3)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# JDBC настройки
JDBC_URL = "jdbc:hive2://109.248.170.228:31000/iceberg.bronze"
JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver"
JDBC_JAR = "C:/Users/asseke/Desktop/tech/hive-jdbc-standalone.jar"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, TimestampType, ArrayType
)


def create_spark_session():
    """Создаёт локальную SparkSession."""
    return (
        SparkSession.builder
        .appName("vitrina-reports")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def fetch_data_jdbc(year: int = None, report_type: str = None, limit: int = None):
    """Получает данные через JDBC (jaydebeapi)."""
    import jaydebeapi
    import pandas as pd

    # Строим WHERE
    where_clauses = ["status = 'signed'"]
    if year:
        where_clauses.append(f"year = {year}")
    else:
        where_clauses.append("year >= 2019")
    if report_type:
        where_clauses.append(f"report_type = '{report_type}'")

    where_sql = " AND ".join(where_clauses)
    limit_sql = f"LIMIT {limit}" if limit else ""

    query = f"""
    SELECT
        id, year, report_type, status, signed_at, version,
        created_at, updated_at, service_request_id, author_id, data
    FROM service_report_cdc
    WHERE {where_sql}
    ORDER BY year DESC, report_type, id DESC
    {limit_sql}
    """

    print(f"Fetching data from JDBC...")
    print(f"  Year: {year or 'all (>= 2019)'}")
    print(f"  Report type: {report_type or 'all'}")
    print(f"  Limit: {limit or 'no limit'}")

    conn = jaydebeapi.connect(
        JDBC_DRIVER, JDBC_URL, ["", ""], JDBC_JAR
    )
    cursor = conn.cursor()
    cursor.execute(query)

    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    print(f"  Fetched {len(rows)} rows")
    return pd.DataFrame(rows, columns=columns)


def build_vitrina(spark, pdf):
    """
    Строит витрину из pandas DataFrame.
    Извлекает поля из JSON колонки data.
    """
    # Конвертируем pandas в Spark DataFrame через RDD (обход проблемы с distutils)
    columns = pdf.columns.tolist()
    rows = [tuple(x) for x in pdf.to_numpy()]
    rdd = spark.sparkContext.parallelize(rows)
    df = rdd.toDF(columns)

    # Извлекаем поля из JSON
    vitrina = df.select(
        # Основные поля
        F.col("id").alias("report_id"),
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
        F.get_json_object(F.col("data"), "$.income_total_previous_quarter").cast(LongType()).alias("income_total_previous_quarter"),
        F.get_json_object(F.col("data"), "$.income_total_current_quarter").cast(LongType()).alias("income_total_current_quarter"),

        # Инвестиции
        F.get_json_object(F.col("data"), "$.investments_total_current_quarter").cast(LongType()).alias("investments_total_current_quarter"),
        F.get_json_object(F.col("data"), "$.main_capital_investments").cast(LongType()).alias("main_capital_investments"),
        F.get_json_object(F.col("data"), "$.main_tangible_capital_investments").cast(LongType()).alias("main_tangible_capital_investments"),
        F.get_json_object(F.col("data"), "$.main_intangible_capital_investments").cast(LongType()).alias("main_intangible_capital_investments"),
        F.get_json_object(F.col("data"), "$.finance_source_increase_authorized_capital").cast(LongType()).alias("finance_source_increase_authorized_capital"),

        # Инвесторы
        F.get_json_object(F.col("data"), "$.investor_amount").cast(LongType()).alias("investor_amount"),
        F.get_json_object(F.col("data"), "$.investor_country_company").alias("investor_country_company"),

        # Займы
        F.get_json_object(F.col("data"), "$.finance_source_loan").cast(LongType()).alias("finance_source_loan"),
        F.get_json_object(F.col("data"), "$.finance_source_loan_foreign").cast(LongType()).alias("finance_source_loan_foreign"),

        # Господдержка
        F.get_json_object(F.col("data"), "$.finance_source_government").cast(LongType()).alias("finance_source_government"),
        F.get_json_object(F.col("data"), "$.finance_source_investment").cast(LongType()).alias("finance_source_investment"),

        # Налоговые льготы
        F.get_json_object(F.col("data"), "$.tax_incentives").cast(LongType()).alias("tax_incentives"),
        F.get_json_object(F.col("data"), "$.tax_incentives_kpn").cast(LongType()).alias("tax_incentives_kpn"),
        F.get_json_object(F.col("data"), "$.tax_incentives_nds").cast(LongType()).alias("tax_incentives_nds"),
        F.get_json_object(F.col("data"), "$.tax_incentives_ipn").cast(LongType()).alias("tax_incentives_ipn"),
        F.get_json_object(F.col("data"), "$.tax_incentives_sn").cast(LongType()).alias("tax_incentives_sn"),
        F.get_json_object(F.col("data"), "$.collection_amount").cast(LongType()).alias("collection_amount"),

        # ОКЭД и исполнитель
        F.get_json_object(F.col("data"), "$.oked").alias("oked"),
        F.get_json_object(F.col("data"), "$.executor_fullname").alias("executor_fullname"),
        F.get_json_object(F.col("data"), "$.executor_phone").alias("executor_phone"),

        # Флаги
        F.get_json_object(F.col("data"), "$.has_nonresidents").alias("has_nonresidents"),
        F.get_json_object(F.col("data"), "$.has_borrowed_funds").alias("has_borrowed_funds"),
        F.get_json_object(F.col("data"), "$.has_raised_investors_funds").alias("has_raised_investors_funds"),

        # ETL метаданные
        F.current_timestamp().alias("etl_loaded_at"),
    )

    return vitrina


def build_summary(spark, pdf):
    """Строит агрегированную сводку по годам и кварталам."""
    columns = pdf.columns.tolist()
    rows = [tuple(x) for x in pdf.to_numpy()]
    rdd = spark.sparkContext.parallelize(rows)
    df = rdd.toDF(columns)

    # Извлекаем числовые поля для агрегации
    with_numbers = df.select(
        F.col("year"),
        F.col("report_type"),
        F.get_json_object(F.col("data"), "$.residents_count").cast(IntegerType()).alias("residents_count"),
        F.get_json_object(F.col("data"), "$.nonresidents_count").cast(IntegerType()).alias("nonresidents_count"),
        F.get_json_object(F.col("data"), "$.gph_count").cast(IntegerType()).alias("gph_count"),
        F.get_json_object(F.col("data"), "$.income_total").cast(LongType()).alias("income_total"),
        F.get_json_object(F.col("data"), "$.income_international").cast(LongType()).alias("income_international"),
        F.get_json_object(F.col("data"), "$.main_capital_investments").cast(LongType()).alias("main_capital_investments"),
        F.get_json_object(F.col("data"), "$.tax_incentives").cast(LongType()).alias("tax_incentives"),
        F.get_json_object(F.col("data"), "$.investor_amount").cast(LongType()).alias("investor_amount"),
        F.get_json_object(F.col("data"), "$.finance_source_loan").cast(LongType()).alias("finance_source_loan"),
        F.get_json_object(F.col("data"), "$.finance_source_government").cast(LongType()).alias("finance_source_government"),
    )

    # Агрегация
    summary = with_numbers.groupBy("year", "report_type").agg(
        F.count("*").alias("reports_count"),
        F.sum(F.coalesce(F.col("residents_count"), F.lit(0))).alias("total_residents"),
        F.sum(F.coalesce(F.col("nonresidents_count"), F.lit(0))).alias("total_nonresidents"),
        F.sum(F.coalesce(F.col("gph_count"), F.lit(0))).alias("total_gph"),
        F.sum(F.coalesce(F.col("income_total"), F.lit(0))).alias("total_income"),
        F.sum(F.coalesce(F.col("income_international"), F.lit(0))).alias("total_income_international"),
        F.sum(F.coalesce(F.col("main_capital_investments"), F.lit(0))).alias("total_investments"),
        F.sum(F.coalesce(F.col("tax_incentives"), F.lit(0))).alias("total_tax_incentives"),
        F.sum(F.coalesce(F.col("investor_amount"), F.lit(0))).alias("total_investor_amount"),
        F.sum(F.coalesce(F.col("finance_source_loan"), F.lit(0))).alias("total_loans"),
        F.sum(F.coalesce(F.col("finance_source_government"), F.lit(0))).alias("total_government_support"),
    )

    # Читаемый период
    summary = summary.withColumn(
        "period",
        F.when(F.col("report_type") == "quarter1", F.concat(F.lit("Q1 "), F.col("year").cast(StringType())))
        .when(F.col("report_type") == "quarter2", F.concat(F.lit("Q2 "), F.col("year").cast(StringType())))
        .when(F.col("report_type") == "quarter3", F.concat(F.lit("Q3 "), F.col("year").cast(StringType())))
        .when(F.col("report_type") == "quarter4", F.concat(F.lit("Q4 "), F.col("year").cast(StringType())))
        .otherwise(F.concat(F.lit("Year "), F.col("year").cast(StringType())))
    )

    summary = summary.withColumn("etl_loaded_at", F.current_timestamp())

    return summary.orderBy(F.desc("year"), F.col("report_type"))


def save_to_csv(df, output_path: str):
    """Сохраняет в CSV через pandas (один файл)."""
    pdf = df.toPandas()
    pdf.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"Saved to: {output_path}")
    return pdf


def main():
    parser = argparse.ArgumentParser(description="PySpark vitrina builder")
    parser.add_argument("--year", type=int, help="Filter by year")
    parser.add_argument("--report-type", type=str, help="Filter by report type")
    parser.add_argument("--limit", type=int, help="Limit rows from source")
    parser.add_argument("--output", type=str, help="Output CSV file")
    parser.add_argument("--mode", choices=["vitrina", "summary"], default="vitrina",
                        help="Build mode: full vitrina or aggregated summary")

    args = parser.parse_args()

    print("=" * 60)
    print(f"PySpark Vitrina Builder - {datetime.now().isoformat()}")
    print("=" * 60)
    print(f"Mode: {args.mode}")
    print(f"Year: {args.year or 'all'}")
    print(f"Report type: {args.report_type or 'all'}")
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

    # Получаем данные через JDBC
    pdf = fetch_data_jdbc(args.year, args.report_type, args.limit)

    if pdf.empty:
        print("No data found. Exiting.")
        return

    # Создаём Spark сессию
    print("\nCreating Spark session...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Строим витрину или сводку
        if args.mode == "summary":
            print("\nBuilding aggregated summary...")
            result_df = build_summary(spark, pdf)
        else:
            print("\nBuilding full vitrina...")
            result_df = build_vitrina(spark, pdf)

        # Показываем результат
        row_count = result_df.count()
        col_count = len(result_df.columns)
        print(f"\nResult: {row_count} rows, {col_count} columns")

        print("\nSchema:")
        result_df.printSchema()

        print("\nFirst 10 rows:")
        result_df.show(10, truncate=False)

        # Сохранение
        if args.output:
            output_path = args.output
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"vitrina_{args.mode}_{timestamp}.csv"

        save_to_csv(result_df, output_path)

        # Статистика для vitrina
        if args.mode == "vitrina":
            print("\nNumeric summary:")
            result_df.select(
                "residents_count", "nonresidents_count",
                "income_total", "tax_incentives"
            ).summary().show()

        print("\nDone!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
