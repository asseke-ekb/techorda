import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, StringType

SOURCE_TABLE = "iceberg.bronze.service_report_cdc"
TARGET_TABLE = "iceberg.silver.service_report"
START_YEAR = 2019


def create_spark_session():
    return SparkSession.builder.appName("bronze-to-silver").getOrCreate()


def read_bronze(spark):
    return spark.table(SOURCE_TABLE).filter(F.col("year") >= START_YEAR)


def apply_dedup_logic(df):
    window_spec = (
        Window
        .partitionBy("service_request_id", "report_type", "year")
        .orderBy(F.desc("updated_at"))
    )

    return (
        df
        .withColumn("rn", F.row_number().over(window_spec))
        .filter((F.col("rn") == 1) & (F.col("op") != "d"))
        .select(
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

            F.get_json_object("data", "$.company_tin").alias("company_tin"),
            F.get_json_object("data", "$.company_name").alias("company_name"),
            F.get_json_object("data", "$.certificate_number").cast(StringType()).alias("certificate_number"),
            F.get_json_object("data", "$.oked").cast(StringType()).alias("oked"),

            F.get_json_object("data", "$.residents_count").cast(LongType()).alias("residents_count"),
            F.get_json_object("data", "$.nonresidents_count").cast(LongType()).alias("nonresidents_count"),
            F.get_json_object("data", "$.gph_count").cast(LongType()).alias("gph_count"),

            F.get_json_object("data", "$.income_total").cast(LongType()).alias("income_total"),
            F.get_json_object("data", "$.income_international").cast(LongType()).alias("income_international"),

            F.get_json_object("data", "$.finance_source_increase_authorized_capital")
                .cast(LongType()).alias("finance_source_increase_authorized_capital"),
            F.get_json_object("data", "$.main_capital_investments")
                .cast(LongType()).alias("main_capital_investments"),
            F.get_json_object("data", "$.finance_source_loan")
                .cast(LongType()).alias("finance_source_loan"),
            F.get_json_object("data", "$.finance_source_loan_foreign")
                .cast(LongType()).alias("finance_source_loan_foreign"),
            F.get_json_object("data", "$.finance_source_government")
                .cast(LongType()).alias("finance_source_government"),
            F.get_json_object("data", "$.finance_source_investment")
                .cast(LongType()).alias("finance_source_investment"),

            F.get_json_object("data", "$.investor_amount").cast(LongType()).alias("investor_amount"),
            F.get_json_object("data", "$.investor_country_company").alias("investor_country_company"),

            F.get_json_object("data", "$.tax_incentives").cast(LongType()).alias("tax_incentives"),
            F.get_json_object("data", "$.tax_incentives_kpn").cast(LongType()).alias("tax_incentives_kpn"),
            F.get_json_object("data", "$.tax_incentives_nds").cast(LongType()).alias("tax_incentives_nds"),
            F.get_json_object("data", "$.tax_incentives_ipn").cast(LongType()).alias("tax_incentives_ipn"),
            F.get_json_object("data", "$.tax_incentives_sn").cast(LongType()).alias("tax_incentives_sn"),

            F.current_timestamp().alias("etl_loaded_at"),
        )
    )


def write_to_silver(df):
    df.writeTo(TARGET_TABLE).overwritePartitions()


def parse_args():
    parser = argparse.ArgumentParser(description="ETL: Bronze → Silver")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = create_spark_session()

    try:
        bronze_df = read_bronze(spark)
        if bronze_df.rdd.isEmpty():
            return

        silver_df = apply_dedup_logic(bronze_df)

        if args.dry_run:
            silver_df.select(
                "service_request_id", "report_type", "year", "status", "op"
            ).show(20, truncate=False)
        else:
            write_to_silver(silver_df)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
