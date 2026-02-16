"""
PySpark Job: Refresh Silver Layer - techpark_participants
Трансформация: Bronze → Silver (участники технопарка)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, get_json_object, row_number, regexp_extract, cast
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


def refresh_silver_participants(spark: SparkSession):
    """
    Обновление таблицы silver.techpark_participants
    """

    print("[INFO] Starting refresh_silver_participants")

    # Чтение данных из Bronze CDC
    bronze_df = spark.table("iceberg.bronze.service_servicerequest_cdc")

    # Фильтрация: только заявки на технопарк со статусом registered/deactivated
    filtered_df = bronze_df \
        .filter(col("service_id") == "techpark") \
        .filter(col("bp_status").isin("registered", "deactivated")) \
        .filter(col("op") != "d")

    # Дедупликация: берём последнюю версию заявки
    window_spec = Window.partitionBy("id").orderBy(col("updated_at").desc())

    dedup_df = filtered_df \
        .withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    # Извлечение полей
    participants_df = dedup_df.select(
        col("id").alias("service_request_id"),
        col("company_id"),
        col("bp_status"),

        # Извлечение данных свидетельства из JSON
        get_json_object(col("data"), "$.activation_info.certificate_number").alias("certificate_number"),
        cast(
            get_json_object(col("data"), "$.activation_info.issue_date"),
            "date"
        ).alias("certificate_issue_date"),
        cast(
            get_json_object(col("data"), "$.activation_info.end_date"),
            "date"
        ).alias("certificate_end_date"),

        # Извлечение БИН из search_field (формат: "БИН: 123456789012")
        regexp_extract(col("search_field"), r'БИН:\s*(\d+)', 1).alias("company_tin"),

        # Название компании из поля company_name
        col("company_name"),

        # Данные деактивации
        get_json_object(col("data"), "$.deactivation_info.reason").alias("deactivation_reason"),
        cast(
            get_json_object(col("data"), "$.deactivation_info.date"),
            "date"
        ).alias("deactivation_date")
    )

    # Запись в Silver таблицу (перезапись)
    participants_df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable("iceberg.silver.techpark_participants")

    record_count = participants_df.count()
    print(f"[INFO] Inserted {record_count} records into silver.techpark_participants")

    return record_count


def create_table_if_not_exists(spark: SparkSession):
    """Создание таблицы silver.techpark_participants если её нет"""

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS iceberg.silver.techpark_participants (
        service_request_id BIGINT,
        company_id BIGINT,
        bp_status STRING,
        certificate_number STRING,
        certificate_issue_date DATE,
        certificate_end_date DATE,
        company_tin STRING,
        company_name STRING,
        deactivation_reason STRING,
        deactivation_date DATE
    )
    USING iceberg
    """

    spark.sql(create_table_sql)
    print("[INFO] Table silver.techpark_participants created or already exists")


def main():
    """Главная функция"""

    print("[INFO] Starting job: Refresh Silver - techpark_participants")

    # Создание Spark сессии
    spark = create_spark_session("Refresh Silver - techpark_participants")

    try:
        # Создание таблицы
        create_table_if_not_exists(spark)

        # Обновление данных
        refresh_silver_participants(spark)

        print("\n[SUCCESS] Job completed successfully")

    except Exception as e:
        print(f"\n[ERROR] Job failed: {str(e)}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
