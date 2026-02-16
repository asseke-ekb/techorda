"""
PySpark Job: Refresh Silver Layer - service_report_v2
Трансформация: Bronze → Silver (отчёты участников технопарка)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, get_json_object, regexp_extract, row_number, coalesce, cast, lit
)
from pyspark.sql.window import Window
import sys


def create_spark_session(app_name: str) -> SparkSession:
    """Создание Spark сессии с настройками для Iceberg"""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://109.248.170.228:9083") \
        .enableHiveSupport() \
        .getOrCreate()


def refresh_silver_service_report_v2(spark: SparkSession, year: int = None):
    """
    Обновление таблицы silver.service_report_v2

    Args:
        spark: SparkSession
        year: Год для загрузки (None = все годы)
    """

    print(f"[INFO] Starting refresh_silver_service_report_v2 for year: {year if year else 'ALL'}")

    # Чтение данных из Bronze CDC
    bronze_df = spark.table("iceberg.bronze.service_report_cdc")

    # Дедупликация: берём последнюю версию каждой записи
    window_spec = Window.partitionBy("id").orderBy(col("updated_ts").desc())

    bronze_dedup = bronze_df \
        .filter(col("op") != "d") \
        .filter(col("data").isNotNull()) \
        .filter(col("data") != "__debezium_unavailable_value") \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    # Извлечение полей из JSON и XML
    silver_df = bronze_dedup.select(
        col("id").alias("report_id"),
        col("service_request_id"),
        col("year"),
        get_json_object(col("data"), "$.report_type").alias("report_type"),
        get_json_object(col("data"), "$.status").alias("status"),

        # Извлечение БИН и названия компании (с fallback для 2019-2020)
        coalesce(
            get_json_object(col("data"), "$.company_tin"),
            regexp_extract(
                get_json_object(col("signature"), "$.signed_xml"),
                r'<company>.*?<tin>([0-9]+)</tin>.*?</company>',
                1
            )
        ).alias("company_tin"),

        coalesce(
            get_json_object(col("data"), "$.company_name"),
            regexp_extract(
                get_json_object(col("signature"), "$.signed_xml"),
                r'<company>.*?<name>(.*?)</name>.*?</company>',
                1
            )
        ).alias("company_name"),

        # Сертификат из данных отчёта
        get_json_object(col("data"), "$.certificate_number").alias("certificate_number"),

        # Направления деятельности
        get_json_object(col("data"), "$.activity_fields").alias("activity_fields"),

        # Финансовые показатели (приведение типов)
        cast(get_json_object(col("data"), "$.income_total"), "bigint").alias("income_total"),
        cast(get_json_object(col("data"), "$.income_total_current_quarter"), "bigint").alias("income_total_current_quarter"),
        cast(get_json_object(col("data"), "$.income_international"), "bigint").alias("income_international"),
        cast(get_json_object(col("data"), "$.income_local"), "bigint").alias("income_local"),

        # Инвестиции
        cast(get_json_object(col("data"), "$.investments_total_current_quarter"), "bigint").alias("investments_total_current_quarter"),
        cast(get_json_object(col("data"), "$.finance_source_loan"), "bigint").alias("finance_source_loan"),
        cast(get_json_object(col("data"), "$.finance_source_increase_authorized_capital"), "bigint").alias("finance_source_increase_authorized_capital"),
        cast(get_json_object(col("data"), "$.finance_source_investment"), "bigint").alias("finance_source_investment"),

        # Налоговые льготы
        cast(get_json_object(col("data"), "$.tax_incentives_kpn"), "bigint").alias("tax_incentives_kpn"),
        cast(get_json_object(col("data"), "$.tax_incentives_nds"), "bigint").alias("tax_incentives_nds"),
        cast(get_json_object(col("data"), "$.tax_incentives_ipn"), "bigint").alias("tax_incentives_ipn"),
        cast(get_json_object(col("data"), "$.tax_incentives_sn"), "bigint").alias("tax_incentives_sn"),
        cast(get_json_object(col("data"), "$.total_tax_saved"), "bigint").alias("total_tax_saved"),

        # Господдержка
        cast(get_json_object(col("data"), "$.government_support_measures"), "bigint").alias("government_support_measures"),

        # Кадры
        cast(get_json_object(col("data"), "$.residents_count"), "bigint").alias("residents_count"),
        cast(get_json_object(col("data"), "$.nonresidents_count"), "bigint").alias("nonresidents_count"),
        cast(get_json_object(col("data"), "$.gph_count"), "bigint").alias("gph_count"),

        # Страна инвестора
        get_json_object(col("data"), "$.investor_country_company").alias("investor_country_company")
    )

    # Фильтрация по году (если указан)
    if year:
        silver_df = silver_df.filter(col("year") == year)

    # Запись в Silver таблицу
    silver_df.write \
        .format("iceberg") \
        .mode("append") \
        .partitionBy("year") \
        .saveAsTable("iceberg.silver.service_report_v2")

    record_count = silver_df.count()
    print(f"[INFO] Inserted {record_count} records into silver.service_report_v2 for year {year if year else 'ALL'}")

    return record_count


def create_table_if_not_exists(spark: SparkSession):
    """Создание таблицы silver.service_report_v2 если её нет"""

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS iceberg.silver.service_report_v2 (
        report_id BIGINT,
        service_request_id BIGINT,
        year INT,
        report_type STRING,
        status STRING,
        company_tin STRING,
        company_name STRING,
        certificate_number STRING,
        activity_fields STRING,
        income_total BIGINT,
        income_total_current_quarter BIGINT,
        income_international BIGINT,
        income_local BIGINT,
        investments_total_current_quarter BIGINT,
        finance_source_loan BIGINT,
        finance_source_increase_authorized_capital BIGINT,
        finance_source_investment BIGINT,
        tax_incentives_kpn BIGINT,
        tax_incentives_nds BIGINT,
        tax_incentives_ipn BIGINT,
        tax_incentives_sn BIGINT,
        total_tax_saved BIGINT,
        government_support_measures BIGINT,
        residents_count BIGINT,
        nonresidents_count BIGINT,
        gph_count BIGINT,
        investor_country_company STRING
    )
    USING iceberg
    PARTITIONED BY (year)
    """

    spark.sql(create_table_sql)
    print("[INFO] Table silver.service_report_v2 created or already exists")


def truncate_table(spark: SparkSession):
    """Очистка таблицы перед полной перезагрузкой"""
    spark.sql("DELETE FROM iceberg.silver.service_report_v2")
    print("[INFO] Table silver.service_report_v2 truncated")


def main():
    """Главная функция"""

    # Параметры из командной строки
    mode = sys.argv[1] if len(sys.argv) > 1 else "full"  # full | incremental
    year = int(sys.argv[2]) if len(sys.argv) > 2 else None

    print(f"[INFO] Starting job with mode={mode}, year={year}")

    # Создание Spark сессии
    spark = create_spark_session("Refresh Silver - service_report_v2")

    try:
        # Создание таблицы
        create_table_if_not_exists(spark)

        # Полная перезагрузка
        if mode == "full":
            truncate_table(spark)

            if year:
                # Загрузка конкретного года
                refresh_silver_service_report_v2(spark, year)
            else:
                # Загрузка по годам (2019-2026)
                for y in range(2019, 2027):
                    print(f"\n{'='*60}")
                    print(f"Processing year: {y}")
                    print(f"{'='*60}\n")
                    refresh_silver_service_report_v2(spark, y)

        # Инкрементальная загрузка
        elif mode == "incremental":
            refresh_silver_service_report_v2(spark, year)

        print("\n[SUCCESS] Job completed successfully")

    except Exception as e:
        print(f"\n[ERROR] Job failed: {str(e)}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
