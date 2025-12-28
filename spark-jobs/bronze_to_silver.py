#!/usr/bin/env python3
"""
ETL: Bronze → Silver

Читает данные из iceberg.bronze.service_report_cdc,
применяет дедупликацию и парсинг JSON,
записывает в iceberg.silver.service_report.

Запуск на кластере:
    spark-submit bronze_to_silver.py

Параметры:
    --year 2024          # фильтр по году (опционально)
    --dry-run            # только показать превью, не записывать
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, StringType
import argparse


def create_spark_session():
    return (
        SparkSession.builder
        .appName("bronze-to-silver")
        .getOrCreate()
    )


def create_silver_table_if_not_exists(spark):
    """Создаёт таблицу silver.service_report если её нет."""

    spark.sql("""
        CREATE TABLE IF NOT EXISTS iceberg.silver.service_report (
            report_id INT COMMENT 'ID отчёта',
            service_request_id INT COMMENT 'ID заявки',
            year INT COMMENT 'Год отчёта',
            report_type STRING COMMENT 'Тип: quarter1-4, yearly',
            status STRING COMMENT 'Статус: draft, signed, rejected',
            op STRING COMMENT 'CDC операция: c, u, d',
            version STRING COMMENT 'Версия отчёта',
            created_at TIMESTAMP COMMENT 'Дата создания',
            updated_at TIMESTAMP COMMENT 'Дата обновления',
            signed_at TIMESTAMP COMMENT 'Дата подписания',
            author_id INT COMMENT 'ID автора',

            -- Компания
            company_tin STRING COMMENT 'БИН/ИИН',
            company_name STRING COMMENT 'Название компании',
            certificate_number STRING COMMENT 'Номер сертификата',
            oked STRING COMMENT 'Код ОКЭД',

            -- Сотрудники
            residents_count BIGINT COMMENT 'Количество резидентов',
            nonresidents_count BIGINT COMMENT 'Количество нерезидентов',
            gph_count BIGINT COMMENT 'Количество по ГПХ',

            -- Доходы
            income_total BIGINT COMMENT 'Общий доход',
            income_international BIGINT COMMENT 'Международный доход (экспорт)',

            -- Финансирование / инвестиции
            finance_source_increase_authorized_capital BIGINT,
            main_capital_investments BIGINT,
            finance_source_loan BIGINT,
            finance_source_loan_foreign BIGINT,
            finance_source_government BIGINT,
            finance_source_investment BIGINT,
            investor_amount BIGINT,
            investor_country_company STRING,

            -- Налоги / льготы
            tax_incentives BIGINT,
            tax_incentives_kpn BIGINT,
            tax_incentives_nds BIGINT,
            tax_incentives_ipn BIGINT,
            tax_incentives_sn BIGINT,

            -- ETL
            etl_loaded_at TIMESTAMP COMMENT 'Дата загрузки ETL'
        )
        USING iceberg
        COMMENT 'Очищенные отчёты (дедупликация + парсинг JSON)'
    """)
    print("Table iceberg.silver.service_report ready")


def read_bronze(spark, year=None):
    """Читает данные из Bronze."""
    df = spark.table("iceberg.bronze.service_report_cdc")

    if year:
        df = df.filter(F.col("year") == year)
        print(f"Filtered by year={year}")

    return df


def transform_to_silver(df):
    """
    Трансформация Bronze → Silver:
    1. Дедупликация по (service_request_id, report_type, year) - последняя версия
    2. Исключение удалённых записей (op='d')
    3. Парсинг JSON полей
    """

    # Окно для дедупликации
    window_spec = Window.partitionBy(
        "service_request_id", "report_type", "year"
    ).orderBy(F.desc("updated_at"))

    # Добавляем номер строки
    ranked = df.withColumn("rn", F.row_number().over(window_spec))

    # Фильтруем: rn=1 и op != 'd'
    deduped = ranked.filter(
        (F.col("rn") == 1) &
        (F.col("op") != "d")
    )

    # Парсим JSON и строим Silver
    silver = deduped.select(
        # Идентификация
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

        # Финансирование
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

        # ETL
        F.current_timestamp().alias("etl_loaded_at"),
    )

    return silver


def write_to_silver(df):
    """Записывает в Silver таблицу (MERGE - upsert)."""

    # Используем MERGE для upsert по ключу
    df.createOrReplaceTempView("silver_updates")

    # MERGE INTO для Iceberg
    df.sparkSession.sql("""
        MERGE INTO iceberg.silver.service_report t
        USING silver_updates s
        ON t.report_id = s.report_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print("Data merged into iceberg.silver.service_report")


def write_to_silver_overwrite(df):
    """Перезаписывает Silver таблицу полностью."""
    (
        df.writeTo("iceberg.silver.service_report")
        .overwritePartitions()
    )
    print("Data written to iceberg.silver.service_report (overwrite)")


def main():
    parser = argparse.ArgumentParser(description="Bronze to Silver ETL")
    parser.add_argument("--year", type=int, help="Filter by year")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, don't write")
    parser.add_argument("--mode", choices=["merge", "overwrite"], default="overwrite",
                        help="Write mode: merge (upsert) or overwrite")
    args = parser.parse_args()

    print("=" * 60)
    print("ETL: Bronze → Silver")
    print("=" * 60)
    print(f"Year filter: {args.year or 'ALL'}")
    print(f"Mode: {args.mode}")
    print(f"Dry run: {args.dry_run}")
    print("=" * 60)

    spark = create_spark_session()

    try:
        # Создаём таблицу если нет
        if not args.dry_run:
            create_silver_table_if_not_exists(spark)

        # Читаем Bronze
        print("\nReading from iceberg.bronze.service_report_cdc...")
        bronze_df = read_bronze(spark, args.year)
        bronze_count = bronze_df.count()
        print(f"Bronze records: {bronze_count}")

        # Трансформируем
        print("\nTransforming to Silver (dedup + parse JSON)...")
        silver_df = transform_to_silver(bronze_df)
        silver_count = silver_df.count()
        print(f"Silver records: {silver_count}")
        print(f"Removed duplicates: {bronze_count - silver_count}")

        # Превью
        print("\n--- Preview (first 10 rows) ---")
        silver_df.select(
            "report_id", "service_request_id", "year", "report_type",
            "status", "op", "company_tin", "income_total"
        ).show(10, truncate=False)

        # Статистика
        print("\n--- Statistics ---")
        print("By status:")
        silver_df.groupBy("status").count().show()
        print("By report_type:")
        silver_df.groupBy("report_type").count().show()
        print("By year:")
        silver_df.groupBy("year").count().orderBy("year").show()

        # Записываем
        if not args.dry_run:
            print("\nWriting to Silver...")
            if args.mode == "merge":
                write_to_silver(silver_df)
            else:
                write_to_silver_overwrite(silver_df)
            print("Done!")
        else:
            print("\n[DRY RUN] Skipping write")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
