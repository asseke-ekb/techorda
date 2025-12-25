#!/usr/bin/env python3
"""
PySpark скрипт для построения витрины отчётов участников Technopark.

Читает данные из Iceberg таблицы bronze.service_report_cdc,
извлекает поля из JSON и сохраняет в CSV или Iceberg gold слой.

Запуск через spark-submit:
    spark-submit vitrina_export.py
    spark-submit vitrina_export.py --year 2024
    spark-submit vitrina_export.py --year 2024 --report-type quarter1 --output-format csv
    spark-submit vitrina_export.py --mode summary --output-table gold.reports_summary
"""

import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType


def create_spark_session(app_name: str = "vitrina-export") -> SparkSession:
    """Создаёт SparkSession с поддержкой Iceberg."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def read_reports(spark: SparkSession, year: int = None, report_type: str = None):
    """
    Читает отчёты из bronze.service_report_cdc.

    Args:
        spark: SparkSession
        year: Фильтр по году
        report_type: Фильтр по типу (quarter1, quarter2, quarter3, quarter4, yearly)
    """
    df = spark.table("iceberg.bronze.service_report_cdc")

    # Только подписанные отчёты
    df = df.filter(F.col("status") == "signed")

    if year:
        df = df.filter(F.col("year") == year)
    else:
        df = df.filter(F.col("year") >= 2019)

    if report_type:
        df = df.filter(F.col("report_type") == report_type)

    return df


def build_vitrina(df):
    """
    Строит витрину из сырых данных отчётов.
    Извлекает поля из JSON колонки `data`.
    """
    return df.select(
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
    ).orderBy(F.desc("year"), F.col("report_type"), F.desc("report_id"))


def build_summary(df):
    """Строит агрегированную сводку по годам и кварталам."""
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
    parser = argparse.ArgumentParser(description="Export reports vitrina")
    parser.add_argument("--year", type=int, help="Filter by year")
    parser.add_argument("--report-type", type=str, help="Filter by report type")
    parser.add_argument("--output-format", choices=["csv", "iceberg"], default="csv")
    parser.add_argument("--output-path", type=str, default="/tmp/vitrina_output")
    parser.add_argument("--output-table", type=str, default="gold.reports_vitrina")
    parser.add_argument("--mode", choices=["vitrina", "summary"], default="vitrina")

    args = parser.parse_args()

    print("=" * 60)
    print(f"Vitrina Export - {datetime.now().isoformat()}")
    print("=" * 60)
    print(f"Mode: {args.mode}")
    print(f"Year: {args.year or 'all (>= 2019)'}")
    print(f"Report type: {args.report_type or 'all'}")
    print(f"Output format: {args.output_format}")
    print("=" * 60)

    spark = create_spark_session()

    try:
        # Читаем данные
        print("\nReading from iceberg.bronze.service_report_cdc...")
        df = read_reports(spark, args.year, args.report_type)

        record_count = df.count()
        print(f"Found {record_count} signed reports")

        if record_count == 0:
            print("No data found. Exiting.")
            return

        # Строим витрину или сводку
        if args.mode == "summary":
            print("\nBuilding summary...")
            result_df = build_summary(df)
        else:
            print("\nBuilding vitrina...")
            result_df = build_vitrina(df)

        result_count = result_df.count()
        print(f"Result: {result_count} records, {len(result_df.columns)} columns")

        # Показываем превью
        print("\nPreview:")
        result_df.show(10, truncate=False)

        # Сохраняем
        if args.output_format == "csv":
            save_to_csv(result_df, args.output_path)
        else:
            save_to_iceberg(result_df, args.output_table)

        print("\nDone!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
