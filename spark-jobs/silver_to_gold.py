#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse


def create_spark_session():
    return SparkSession.builder.appName("silver-to-gold").getOrCreate()


def create_gold_table_if_not_exists(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.reports_summary (
            service_request_id INT,
            year INT,
            signed_quarters INT,
            residents_count BIGINT,
            nonresidents_count BIGINT,
            income_total BIGINT,
            income_international BIGINT,
            finance_source_increase_authorized_capital BIGINT,
            main_capital_investments BIGINT,
            finance_source_loan BIGINT,
            finance_source_loan_foreign BIGINT,
            finance_source_government BIGINT,
            finance_source_investment BIGINT,
            investor_amount BIGINT,
            taxes_saved BIGINT,
            tax_incentives_kpn BIGINT,
            tax_incentives_nds BIGINT,
            tax_incentives_ipn BIGINT,
            tax_incentives_sn BIGINT,
            etl_loaded_at TIMESTAMP
        )
        USING iceberg
    """)
    print("Table iceberg.gold.reports_summary ready")


def read_silver(spark, year=None):
    df = spark.table("iceberg.silver.service_report")
    df = df.filter(
        (F.col("status") == "signed") &
        (F.col("report_type").isin("quarter1", "quarter2", "quarter3", "quarter4"))
    )
    if year:
        df = df.filter(F.col("year") == year)
        print(f"Filtered by year={year}")
    return df


def aggregate_to_gold(df):
    gold = df.groupBy("service_request_id", "year").agg(
        F.count("*").alias("signed_quarters"),
        F.sum(F.coalesce(F.col("residents_count"), F.lit(0))).alias("residents_count"),
        F.sum(F.coalesce(F.col("nonresidents_count"), F.lit(0))).alias("nonresidents_count"),
        F.sum(F.coalesce(F.col("income_total"), F.lit(0))).alias("income_total"),
        F.sum(F.coalesce(F.col("income_international"), F.lit(0))).alias("income_international"),
        F.sum(F.coalesce(F.col("finance_source_increase_authorized_capital"), F.lit(0))).alias("finance_source_increase_authorized_capital"),
        F.sum(F.coalesce(F.col("main_capital_investments"), F.lit(0))).alias("main_capital_investments"),
        F.sum(F.coalesce(F.col("finance_source_loan"), F.lit(0))).alias("finance_source_loan"),
        F.sum(F.coalesce(F.col("finance_source_loan_foreign"), F.lit(0))).alias("finance_source_loan_foreign"),
        F.sum(F.coalesce(F.col("finance_source_government"), F.lit(0))).alias("finance_source_government"),
        F.sum(F.coalesce(F.col("finance_source_investment"), F.lit(0))).alias("finance_source_investment"),
        F.sum(F.coalesce(F.col("investor_amount"), F.lit(0))).alias("investor_amount"),
        F.sum(F.coalesce(F.col("tax_incentives"), F.lit(0))).alias("taxes_saved"),
        F.sum(F.coalesce(F.col("tax_incentives_kpn"), F.lit(0))).alias("tax_incentives_kpn"),
        F.sum(F.coalesce(F.col("tax_incentives_nds"), F.lit(0))).alias("tax_incentives_nds"),
        F.sum(F.coalesce(F.col("tax_incentives_ipn"), F.lit(0))).alias("tax_incentives_ipn"),
        F.sum(F.coalesce(F.col("tax_incentives_sn"), F.lit(0))).alias("tax_incentives_sn"),
        F.current_timestamp().alias("etl_loaded_at"),
    )
    return gold


def write_to_gold(df):
    df.writeTo("iceberg.gold.reports_summary").overwritePartitions()
    print("Data written to iceberg.gold.reports_summary")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("ETL: Silver -> Gold")
    print(f"Year: {args.year or 'ALL'}, Dry run: {args.dry_run}")
    print("=" * 60)

    spark = create_spark_session()

    try:
        if not args.dry_run:
            create_gold_table_if_not_exists(spark)

        print("Reading from iceberg.silver.service_report (signed quarters only)...")
        silver_df = read_silver(spark, args.year)
        silver_count = silver_df.count()
        print(f"Silver records: {silver_count}")

        print("Aggregating to Gold...")
        gold_df = aggregate_to_gold(silver_df)
        gold_count = gold_df.count()
        print(f"Gold records: {gold_count}")

        gold_df.select("service_request_id", "year", "signed_quarters", "income_total", "income_international", "taxes_saved").orderBy("year", "service_request_id").show(10, truncate=False)

        if not args.dry_run:
            write_to_gold(gold_df)
            print("Done!")
        else:
            print("[DRY RUN] Skipping write")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
