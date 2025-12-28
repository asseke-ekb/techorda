#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, StringType
import argparse


def create_spark_session():
    return SparkSession.builder.appName("bronze-to-silver").getOrCreate()


def create_silver_table_if_not_exists(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.service_report (
            report_id INT,
            service_request_id INT,
            year INT,
            report_type STRING,
            status STRING,
            op STRING,
            version STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            signed_at TIMESTAMP,
            author_id INT,
            company_tin STRING,
            company_name STRING,
            certificate_number STRING,
            oked STRING,
            residents_count BIGINT,
            nonresidents_count BIGINT,
            gph_count BIGINT,
            income_total BIGINT,
            income_international BIGINT,
            finance_source_increase_authorized_capital BIGINT,
            main_capital_investments BIGINT,
            finance_source_loan BIGINT,
            finance_source_loan_foreign BIGINT,
            finance_source_government BIGINT,
            finance_source_investment BIGINT,
            investor_amount BIGINT,
            investor_country_company STRING,
            tax_incentives BIGINT,
            tax_incentives_kpn BIGINT,
            tax_incentives_nds BIGINT,
            tax_incentives_ipn BIGINT,
            tax_incentives_sn BIGINT,
            etl_loaded_at TIMESTAMP
        )
        USING iceberg
    """)
    print("Table iceberg.silver.service_report ready")


def read_bronze(spark, year=None):
    df = spark.table("iceberg.bronze.service_report_cdc")
    if year:
        df = df.filter(F.col("year") == year)
        print(f"Filtered by year={year}")
    return df


def transform_to_silver(df):
    window_spec = Window.partitionBy(
        "service_request_id", "report_type", "year"
    ).orderBy(F.desc("updated_at"))

    ranked = df.withColumn("rn", F.row_number().over(window_spec))

    deduped = ranked.filter(
        (F.col("rn") == 1) &
        (F.col("op") != "d")
    )

    silver = deduped.select(
        F.col("id").cast("int").alias("report_id"),
        F.col("service_request_id").cast("int"),
        F.col("year").cast("int"),
        F.col("report_type"),
        F.col("status"),
        F.col("op"),
        F.col("version"),
        F.col("created_at"),
        F.col("updated_at"),
        F.col("signed_at"),
        F.col("author_id").cast("int"),
        F.get_json_object(F.col("data"), "$.company_tin").alias("company_tin"),
        F.get_json_object(F.col("data"), "$.company_name").alias("company_name"),
        F.get_json_object(F.col("data"), "$.certificate_number").cast(StringType()).alias("certificate_number"),
        F.get_json_object(F.col("data"), "$.oked").cast(StringType()).alias("oked"),
        F.get_json_object(F.col("data"), "$.residents_count").cast(LongType()).alias("residents_count"),
        F.get_json_object(F.col("data"), "$.nonresidents_count").cast(LongType()).alias("nonresidents_count"),
        F.get_json_object(F.col("data"), "$.gph_count").cast(LongType()).alias("gph_count"),
        F.get_json_object(F.col("data"), "$.income_total").cast(LongType()).alias("income_total"),
        F.get_json_object(F.col("data"), "$.income_international").cast(LongType()).alias("income_international"),
        F.get_json_object(F.col("data"), "$.finance_source_increase_authorized_capital").cast(LongType()).alias("finance_source_increase_authorized_capital"),
        F.get_json_object(F.col("data"), "$.main_capital_investments").cast(LongType()).alias("main_capital_investments"),
        F.get_json_object(F.col("data"), "$.finance_source_loan").cast(LongType()).alias("finance_source_loan"),
        F.get_json_object(F.col("data"), "$.finance_source_loan_foreign").cast(LongType()).alias("finance_source_loan_foreign"),
        F.get_json_object(F.col("data"), "$.finance_source_government").cast(LongType()).alias("finance_source_government"),
        F.get_json_object(F.col("data"), "$.finance_source_investment").cast(LongType()).alias("finance_source_investment"),
        F.get_json_object(F.col("data"), "$.investor_amount").cast(LongType()).alias("investor_amount"),
        F.get_json_object(F.col("data"), "$.investor_country_company").alias("investor_country_company"),
        F.get_json_object(F.col("data"), "$.tax_incentives").cast(LongType()).alias("tax_incentives"),
        F.get_json_object(F.col("data"), "$.tax_incentives_kpn").cast(LongType()).alias("tax_incentives_kpn"),
        F.get_json_object(F.col("data"), "$.tax_incentives_nds").cast(LongType()).alias("tax_incentives_nds"),
        F.get_json_object(F.col("data"), "$.tax_incentives_ipn").cast(LongType()).alias("tax_incentives_ipn"),
        F.get_json_object(F.col("data"), "$.tax_incentives_sn").cast(LongType()).alias("tax_incentives_sn"),
        F.current_timestamp().alias("etl_loaded_at"),
    )

    return silver


def write_to_silver(df):
    df.writeTo("iceberg.silver.service_report").overwritePartitions()
    print("Data written to iceberg.silver.service_report")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("ETL: Bronze -> Silver")
    print(f"Year: {args.year or 'ALL'}, Dry run: {args.dry_run}")
    print("=" * 60)

    spark = create_spark_session()

    try:
        if not args.dry_run:
            create_silver_table_if_not_exists(spark)

        print("Reading from iceberg.bronze.service_report_cdc...")
        bronze_df = read_bronze(spark, args.year)
        bronze_count = bronze_df.count()
        print(f"Bronze records: {bronze_count}")

        print("Transforming to Silver...")
        silver_df = transform_to_silver(bronze_df)
        silver_count = silver_df.count()
        print(f"Silver records: {silver_count} (removed {bronze_count - silver_count} duplicates)")

        silver_df.select("report_id", "service_request_id", "year", "report_type", "status", "op", "company_tin", "income_total").show(10, truncate=False)

        if not args.dry_run:
            write_to_silver(silver_df)
            print("Done!")
        else:
            print("[DRY RUN] Skipping write")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
