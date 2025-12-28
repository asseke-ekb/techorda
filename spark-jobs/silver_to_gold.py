#!/usr/bin/env python3
"""
ETL: Silver → Gold

Читает данные из iceberg.silver.service_report,
агрегирует по (service_request_id, year) только signed quarters,
записывает в iceberg.gold.reports_summary.

Запуск на кластере:
    spark-submit silver_to_gold.py

Параметры:
    --year 2024          # фильтр по году (опционально)
    --dry-run            # только показать превью, не записывать
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse


def create_spark_session():
    return (
        SparkSession.builder
        .appName("silver-to-gold")
        .getOrCreate()
    )


def create_gold_table_if_not_exists(spark):
    """Создаёт таблицу gold.reports_summary если её нет."""

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.gold.reports_summary (
            service_request_id INT COMMENT 'ID заявки',
            year INT COMMENT 'Год',
            signed_quarters INT COMMENT 'Количество подписанных кварталов',

            -- Сотрудники
            residents_count BIGINT COMMENT 'Всего резидентов за год',
            nonresidents_count BIGINT COMMENT 'Всего нерезидентов за год',

            -- Доходы
            income_total BIGINT COMMENT 'Общий доход за год',
            income_international BIGINT COMMENT 'Экспорт за год',

            -- Финансирование / инвестиции
            finance_source_increase_authorized_capital BIGINT,
            main_capital_investments BIGINT,
            finance_source_loan BIGINT,
            finance_source_loan_foreign BIGINT,
            finance_source_government BIGINT,
            finance_source_investment BIGINT,
            investor_amount BIGINT,

            -- Налоги
            taxes_saved BIGINT COMMENT 'Сэкономлено на налогах',
            tax_incentives_kpn BIGINT,
            tax_incentives_nds BIGINT,
            tax_incentives_ipn BIGINT,
            tax_incentives_sn BIGINT,

            -- ETL
            etl_loaded_at TIMESTAMP COMMENT 'Дата загрузки ETL'
        )
        USING iceberg
        COMMENT 'Годовой свод отчётов по заявкам (только signed quarters)'
    """)
    print("Table iceberg.gold.reports_summary ready")


def read_silver(spark, year=None):
    """Читает данные из Silver."""
    df = spark.table("iceberg.silver.service_report")

    # Только signed quarters (без yearly чтобы не было дублей)
    df = df.filter(
        (F.col("status") == "signed") &
        (F.col("report_type").isin("quarter1", "quarter2", "quarter3", "quarter4"))
    )

    if year:
        df = df.filter(F.col("year") == year)
        print(f"Filtered by year={year}")

    return df


def aggregate_to_gold(df):
    """
    Агрегация Silver → Gold:
    GROUP BY (service_request_id, year)
    SUM всех метрик
    """

    gold = df.groupBy("service_request_id", "year").agg(
        F.count("*").alias("signed_quarters"),

        # Сотрудники
        F.sum(F.coalesce(F.col("residents_count"), F.lit(0))).alias("residents_count"),
        F.sum(F.coalesce(F.col("nonresidents_count"), F.lit(0))).alias("nonresidents_count"),

        # Доходы
        F.sum(F.coalesce(F.col("income_total"), F.lit(0))).alias("income_total"),
        F.sum(F.coalesce(F.col("income_international"), F.lit(0))).alias("income_international"),

        # Финансирование
        F.sum(F.coalesce(F.col("finance_source_increase_authorized_capital"), F.lit(0))).alias("finance_source_increase_authorized_capital"),
        F.sum(F.coalesce(F.col("main_capital_investments"), F.lit(0))).alias("main_capital_investments"),
        F.sum(F.coalesce(F.col("finance_source_loan"), F.lit(0))).alias("finance_source_loan"),
        F.sum(F.coalesce(F.col("finance_source_loan_foreign"), F.lit(0))).alias("finance_source_loan_foreign"),
        F.sum(F.coalesce(F.col("finance_source_government"), F.lit(0))).alias("finance_source_government"),
        F.sum(F.coalesce(F.col("finance_source_investment"), F.lit(0))).alias("finance_source_investment"),
        F.sum(F.coalesce(F.col("investor_amount"), F.lit(0))).alias("investor_amount"),

        # Налоги (tax_incentives как taxes_saved)
        F.sum(F.coalesce(F.col("tax_incentives"), F.lit(0))).alias("taxes_saved"),
        F.sum(F.coalesce(F.col("tax_incentives_kpn"), F.lit(0))).alias("tax_incentives_kpn"),
        F.sum(F.coalesce(F.col("tax_incentives_nds"), F.lit(0))).alias("tax_incentives_nds"),
        F.sum(F.coalesce(F.col("tax_incentives_ipn"), F.lit(0))).alias("tax_incentives_ipn"),
        F.sum(F.coalesce(F.col("tax_incentives_sn"), F.lit(0))).alias("tax_incentives_sn"),

        # ETL
        F.current_timestamp().alias("etl_loaded_at"),
    )

    return gold


def write_to_gold(df):
    """Записывает в Gold таблицу (MERGE - upsert)."""

    df.createOrReplaceTempView("gold_updates")

    df.sparkSession.sql("""
        MERGE INTO iceberg.gold.reports_summary t
        USING gold_updates s
        ON t.service_request_id = s.service_request_id AND t.year = s.year
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print("Data merged into iceberg.gold.reports_summary")


def write_to_gold_overwrite(df):
    """Перезаписывает Gold таблицу полностью."""
    (
        df.writeTo("iceberg.gold.reports_summary")
        .overwritePartitions()
    )
    print("Data written to iceberg.gold.reports_summary (overwrite)")


def main():
    parser = argparse.ArgumentParser(description="Silver to Gold ETL")
    parser.add_argument("--year", type=int, help="Filter by year")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, don't write")
    parser.add_argument("--mode", choices=["merge", "overwrite"], default="overwrite",
                        help="Write mode: merge (upsert) or overwrite")
    args = parser.parse_args()

    print("=" * 60)
    print("ETL: Silver → Gold")
    print("=" * 60)
    print(f"Year filter: {args.year or 'ALL'}")
    print(f"Mode: {args.mode}")
    print(f"Dry run: {args.dry_run}")
    print("=" * 60)

    spark = create_spark_session()

    try:
        # Создаём таблицу если нет
        if not args.dry_run:
            create_gold_table_if_not_exists(spark)

        # Читаем Silver (только signed quarters)
        print("\nReading from iceberg.silver.service_report...")
        print("Filter: status='signed' AND report_type IN (quarter1-4)")
        silver_df = read_silver(spark, args.year)
        silver_count = silver_df.count()
        print(f"Silver records (signed quarters): {silver_count}")

        # Агрегируем
        print("\nAggregating to Gold (GROUP BY service_request_id, year)...")
        gold_df = aggregate_to_gold(silver_df)
        gold_count = gold_df.count()
        print(f"Gold records (unique request-year pairs): {gold_count}")

        # Превью
        print("\n--- Preview (first 10 rows) ---")
        gold_df.select(
            "service_request_id", "year", "signed_quarters",
            "income_total", "income_international", "taxes_saved"
        ).orderBy("year", "service_request_id").show(10, truncate=False)

        # Статистика
        print("\n--- Statistics ---")
        print("By year:")
        gold_df.groupBy("year").agg(
            F.count("*").alias("companies"),
            F.sum("signed_quarters").alias("total_quarters"),
            F.sum("income_total").alias("total_income")
        ).orderBy("year").show()

        # Записываем
        if not args.dry_run:
            print("\nWriting to Gold...")
            if args.mode == "merge":
                write_to_gold(gold_df)
            else:
                write_to_gold_overwrite(gold_df)
            print("Done!")
        else:
            print("\n[DRY RUN] Skipping write")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
