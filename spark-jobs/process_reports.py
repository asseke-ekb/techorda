#!/usr/bin/env python3
"""
PySpark скрипт для обработки ежеквартальных отчётов AstanaHub.
Читает данные из Trino, обрабатывает и сохраняет результаты обратно в Trino/Hive.
"""

import argparse
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType, ArrayType, MapType, DateType
)
import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt


def create_spark_session(app_name: str, trino_host: str) -> SparkSession:
    """Создаёт SparkSession с подключением к Trino."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", "io.trino:trino-jdbc:435")
        .config("spark.sql.catalog.trino", "org.apache.spark.sql.trino.TrinoDataSource")
        .getOrCreate()
    )


def read_from_trino(spark: SparkSession, trino_url: str, query: str) -> "DataFrame":
    """Читает данные из Trino по SQL-запросу."""
    return (
        spark.read
        .format("jdbc")
        .option("url", trino_url)
        .option("query", query)
        .option("driver", "io.trino.jdbc.TrinoDriver")
        .load()
    )


def write_to_trino(df: "DataFrame", trino_url: str, table_name: str, mode: str = "overwrite"):
    """Записывает DataFrame в таблицу Trino/Hive."""
    (
        df.write
        .format("jdbc")
        .option("url", trino_url)
        .option("dbtable", table_name)
        .option("driver", "io.trino.jdbc.TrinoDriver")
        .mode(mode)
        .save()
    )


# ============================================================================
# Функции обработки для каждого типа отчёта
# ============================================================================

def process_reports_summary(spark, df, year: int):
    """Сводная таблица по кварталам."""

    # Маппинг типов отчётов
    report_types = {
        'quarter1': '1 кв.',
        'quarter2': '2 кв.',
        'quarter3': '3 кв.',
        'quarter4': '4 кв.',
        'yearly': 'Итого'
    }

    # Агрегация по типу отчёта
    summary = df.groupBy("report_type").agg(
        F.count("*").alias("total"),
        F.sum(F.coalesce(F.get_json_object(F.col("data"), "$.residents_count").cast(IntegerType()), F.lit(0))).alias("residents_count"),
        F.sum(F.coalesce(F.get_json_object(F.col("data"), "$.nonresidents_count").cast(IntegerType()), F.lit(0))).alias("nonresidents_count"),
        F.sum(F.coalesce(F.get_json_object(F.col("data"), "$.income_total").cast(LongType()), F.lit(0))).alias("income_total"),
        F.sum(F.coalesce(F.get_json_object(F.col("data"), "$.income_international").cast(LongType()), F.lit(0))).alias("income_international"),
        F.sum(F.coalesce(F.get_json_object(F.col("data"), "$.finance_source_increase_authorized_capital").cast(LongType()), F.lit(0))).alias("finance_source_increase_authorized_capital"),
        F.sum(F.coalesce(F.get_json_object(F.col("data"), "$.main_capital_investments").cast(LongType()), F.lit(0))).alias("main_capital_investments"),
        F.sum(F.coalesce(F.get_json_object(F.col("data"), "$.finance_source_loan").cast(LongType()), F.lit(0))).alias("finance_source_loan"),
        F.sum(F.coalesce(F.get_json_object(F.col("data"), "$.finance_source_loan_foreign").cast(LongType()), F.lit(0))).alias("finance_source_loan_foreign"),
        F.sum(F.coalesce(F.get_json_object(F.col("data"), "$.finance_source_government").cast(LongType()), F.lit(0))).alias("finance_source_government"),
        F.sum(F.coalesce(F.get_json_object(F.col("data"), "$.finance_source_investment").cast(LongType()), F.lit(0))).alias("finance_source_investment"),
        F.sum(F.coalesce(F.get_json_object(F.col("data"), "$.investor_amount").cast(LongType()), F.lit(0))).alias("investor_amount"),
        F.sum(F.coalesce(F.get_json_object(F.col("data"), "$.tax_incentives").cast(LongType()), F.lit(0))).alias("tax_incentives"),
    )

    # Добавляем год и период
    summary = summary.withColumn("year", F.lit(year))
    summary = summary.withColumn(
        "period",
        F.when(F.col("report_type") == "quarter1", F.concat(F.lit("1 кв. "), F.lit(year), F.lit(" г.")))
        .when(F.col("report_type") == "quarter2", F.concat(F.lit("2 кв. "), F.lit(year), F.lit(" г.")))
        .when(F.col("report_type") == "quarter3", F.concat(F.lit("3 кв. "), F.lit(year), F.lit(" г.")))
        .when(F.col("report_type") == "quarter4", F.concat(F.lit("4 кв. "), F.lit(year), F.lit(" г.")))
        .otherwise(F.concat(F.lit("Итого "), F.lit(year), F.lit(" г.")))
    )

    return summary


def process_report_residents(spark, df):
    """Реестр резидентов."""

    return df.select(
        F.col("id"),
        F.col("company_name"),
        F.col("number").alias("request_number"),
        F.get_json_object(F.col("activation_info"), "$.certificate_number").alias("certificate_number"),
        F.get_json_object(F.col("activation_info"), "$.issue_date").alias("issue_date"),
        F.get_json_object(F.col("request_data"), "$.registration_date").alias("registration_date"),
        F.get_json_object(F.col("hub_form_data"), "$.heads_full_name").alias("heads_full_name"),
        F.col("company_tin"),
        F.get_json_object(F.col("data"), "$.oked").alias("oked"),
        F.get_json_object(F.col("data"), "$.activity_fields").alias("activity_fields"),
        F.get_json_object(F.col("data"), "$.residents_count").cast(IntegerType()).alias("residents_count"),
        F.get_json_object(F.col("data"), "$.nonresidents_count").cast(IntegerType()).alias("nonresidents_count"),
        F.get_json_object(F.col("data"), "$.gph_count").cast(IntegerType()).alias("gph_count"),
        F.get_json_object(F.col("data"), "$.investments_total_current_quarter").cast(LongType()).alias("investments_total_current_quarter"),
        F.get_json_object(F.col("data"), "$.finance_source_increase_authorized_capital").cast(LongType()).alias("finance_source_increase_authorized_capital"),
        F.get_json_object(F.col("data"), "$.finance_source_loan").cast(LongType()).alias("finance_source_loan"),
        F.get_json_object(F.col("data"), "$.finance_source_loan_foreign").cast(LongType()).alias("finance_source_loan_foreign"),
        F.get_json_object(F.col("data"), "$.main_capital_investments").cast(LongType()).alias("main_capital_investments"),
        F.get_json_object(F.col("data"), "$.main_tangible_capital_investments").cast(LongType()).alias("main_tangible_capital_investments"),
        F.get_json_object(F.col("data"), "$.main_intangible_capital_investments").cast(LongType()).alias("main_intangible_capital_investments"),
        F.get_json_object(F.col("data"), "$.income_total").cast(LongType()).alias("income_total"),
        F.get_json_object(F.col("data"), "$.income_total_previous_quarter").cast(LongType()).alias("income_total_previous_quarter"),
        F.get_json_object(F.col("data"), "$.income_total_current_quarter").cast(LongType()).alias("income_total_current_quarter"),
        F.get_json_object(F.col("data"), "$.income_international").cast(LongType()).alias("income_international"),
        F.get_json_object(F.col("data"), "$.income_international_previous_quarter").cast(LongType()).alias("income_international_previous_quarter"),
        F.get_json_object(F.col("data"), "$.income_international_current_quarter").cast(LongType()).alias("income_international_current_quarter"),
        F.get_json_object(F.col("data"), "$.income_to_collect").cast(LongType()).alias("income_to_collect"),
        F.get_json_object(F.col("data"), "$.collection_amount").cast(LongType()).alias("collection_amount"),
        F.get_json_object(F.col("data"), "$.tax_incentives").cast(LongType()).alias("tax_incentives"),
        F.get_json_object(F.col("data"), "$.tax_incentives_kpn").cast(LongType()).alias("tax_incentives_kpn"),
        F.get_json_object(F.col("data"), "$.tax_incentives_nds").cast(LongType()).alias("tax_incentives_nds"),
        F.get_json_object(F.col("data"), "$.tax_incentives_ipn").cast(LongType()).alias("tax_incentives_ipn"),
        F.get_json_object(F.col("data"), "$.tax_incentives_sn").cast(LongType()).alias("tax_incentives_sn"),
        F.col("signed_at"),
        F.get_json_object(F.col("data"), "$.executor_fullname").alias("executor_fullname"),
        F.get_json_object(F.col("data"), "$.executor_phone").alias("executor_phone"),
    )


def process_report_nonresidents(spark, df):
    """Нерезиденты - иностранные граждане."""

    # Распаковываем JSON массив nonresidents
    df_exploded = df.withColumn(
        "nonresident",
        F.explode(F.from_json(
            F.get_json_object(F.col("data"), "$.nonresidents"),
            ArrayType(MapType(StringType(), StringType()))
        ))
    )

    return df_exploded.select(
        F.col("id"),
        F.col("company_name"),
        F.col("company_tin"),
        F.col("nonresident.fullname").alias("fullname"),
        F.col("nonresident.birth_date").alias("birth_date"),
        F.col("nonresident.citizenship").alias("citizenship"),
        F.col("nonresident.passport").alias("passport"),
        F.col("nonresident.has_visa").alias("has_visa"),
        F.col("nonresident.stay_period").alias("stay_period"),
        F.col("nonresident.qualification").alias("qualification"),
        F.col("nonresident.visa_info").alias("visa_info"),
        F.col("nonresident.visit_goal").alias("visit_goal"),
        F.col("nonresident.address").alias("address"),
    )


def process_report_nonresidents_family(spark, df):
    """Нерезиденты - члены семьи."""

    df_exploded = df.withColumn(
        "family_member",
        F.explode(F.from_json(
            F.get_json_object(F.col("data"), "$.nonresidents_family"),
            ArrayType(MapType(StringType(), StringType()))
        ))
    )

    return df_exploded.select(
        F.col("id"),
        F.col("company_name"),
        F.col("company_tin"),
        F.col("family_member.fullname").alias("fullname"),
        F.col("family_member.birth_date").alias("birth_date"),
        F.col("family_member.citizenship").alias("citizenship"),
        F.col("family_member.fullname_astanahub").alias("fullname_astanahub"),
        F.col("family_member.relation_degree").alias("relation_degree"),
        F.col("family_member.documents").alias("documents"),
        F.col("family_member.passport").alias("passport"),
        F.col("family_member.has_visa").alias("has_visa"),
        F.col("family_member.visa_info").alias("visa_info"),
        F.col("family_member.address").alias("address"),
    )


def process_report_nonresidents_outside(spark, df):
    """Нерезиденты - дистанционно."""

    df_exploded = df.withColumn(
        "outside",
        F.explode(F.from_json(
            F.get_json_object(F.col("data"), "$.nonresidents_outside"),
            ArrayType(MapType(StringType(), StringType()))
        ))
    )

    return df_exploded.select(
        F.col("id"),
        F.col("company_name"),
        F.col("company_tin"),
        F.col("outside.fullname").alias("fullname"),
        F.col("outside.birth_date").alias("birth_date"),
        F.col("outside.citizenship").alias("citizenship"),
        F.col("outside.passport").alias("passport"),
        F.col("outside.stay_period").alias("stay_period"),
        F.col("outside.qualification").alias("qualification"),
        F.col("outside.visa_info").alias("visa_info"),
        F.col("outside.visit_goal").alias("visit_goal"),
        F.col("outside.address").alias("address"),
    )


def process_report_authorized_capital(spark, df):
    """2.2.1 Вклад в уставной капитал действующим учредителем."""

    df_exploded = df.withColumn(
        "founder_data",
        F.explode(F.from_json(
            F.get_json_object(F.col("data"), "$.finance_source_increase_authorized_capital_current_founder"),
            ArrayType(MapType(StringType(), StringType()))
        ))
    )

    return df_exploded.select(
        F.col("id"),
        F.col("company_name"),
        F.col("company_tin"),
        F.col("founder_data.founder").alias("founder"),
        F.col("founder_data.capital_amount").cast(LongType()).alias("capital_amount"),
    )


def process_report_raised_investors(spark, df):
    """2.2.2 Привлечённые средства инвесторов."""

    df_exploded = df.withColumn(
        "investor_data",
        F.explode(F.from_json(
            F.get_json_object(F.col("data"), "$.raised_investors_funds"),
            ArrayType(MapType(StringType(), StringType()))
        ))
    )

    return df_exploded.select(
        F.col("id"),
        F.col("company_name"),
        F.col("company_tin"),
        F.col("investor_data.country").alias("country"),
        F.col("investor_data.investor").alias("investor"),
        # Учитываем разные версии отчёта
        F.coalesce(
            F.col("investor_data.invested_sum").cast(LongType()),
            F.col("investor_data.capital_amount").cast(LongType())
        ).alias("sum"),
        F.col("investor_data.deal_evaluation").alias("deal_evaluation"),
    )


def process_report_borrowed_funds(spark, df):
    """2.3.1 Займы от казахстанских лиц."""

    df_exploded = df.withColumn(
        "borrow_data",
        F.explode(F.from_json(
            F.get_json_object(F.col("data"), "$.borrowed_funds"),
            ArrayType(MapType(StringType(), StringType()))
        ))
    )

    return df_exploded.select(
        F.col("id"),
        F.col("company_name"),
        F.col("company_tin"),
        F.col("borrow_data.borrower").alias("borrower"),
        F.col("borrow_data.borrow_sum").cast(LongType()).alias("borrow_sum"),
    )


def process_report_borrowed_international(spark, df):
    """2.3.2 Зарубежные займы."""

    df_exploded = df.withColumn(
        "borrow_data",
        F.explode(F.from_json(
            F.get_json_object(F.col("data"), "$.borrowed_funds_international"),
            ArrayType(MapType(StringType(), StringType()))
        ))
    )

    return df_exploded.select(
        F.col("id"),
        F.col("company_name"),
        F.col("company_tin"),
        F.col("borrow_data.country").alias("country"),
        F.col("borrow_data.borrower").alias("borrower"),
        F.col("borrow_data.borrow_sum").cast(LongType()).alias("borrow_sum"),
    )


def process_report_another_method(spark, df):
    """2.3.3 Иной способ денежного вклада инвестором."""

    df_exploded = df.withColumn(
        "method_data",
        F.explode(F.from_json(
            F.get_json_object(F.col("data"), "$.another_method_investors_funds"),
            ArrayType(MapType(StringType(), StringType()))
        ))
    )

    return df_exploded.select(
        F.col("id"),
        F.col("company_name"),
        F.col("company_tin"),
        F.col("method_data.country").alias("country"),
        F.col("method_data.investor").alias("investor"),
        F.col("method_data.invested_sum").cast(LongType()).alias("sum"),
        F.col("method_data.cash_deposit_type").alias("cash_deposit_type"),
    )


def process_report_debt_instruments(spark, df):
    """2.4.1 Инвестиции в виде долговых инструментов."""

    df_exploded = df.withColumn(
        "debt_data",
        F.explode(F.from_json(
            F.get_json_object(F.col("data"), "$.debt_instruments"),
            ArrayType(MapType(StringType(), StringType()))
        ))
    )

    return df_exploded.select(
        F.col("id"),
        F.col("company_name"),
        F.col("company_tin"),
        F.col("debt_data.country").alias("country"),
        F.col("debt_data.investor").alias("investor"),
        F.col("debt_data.invested_sum").cast(LongType()).alias("sum"),
    )


def process_report_income_international_prev(spark, df):
    """3.3.1 Международные продажи за предыдущий квартал."""

    df_exploded = df.withColumn(
        "income_data",
        F.explode(F.from_json(
            F.get_json_object(F.col("data"), "$.income_international_previous_quarter"),
            ArrayType(MapType(StringType(), StringType()))
        ))
    )

    return df_exploded.select(
        F.col("id"),
        F.col("company_name"),
        F.col("company_tin"),
        F.col("income_data.country").alias("country_previous"),
        F.col("income_data.sum").cast(LongType()).alias("sum_previous"),
    )


def process_report_income_international_curr(spark, df):
    """3.3.2 Международные продажи за текущий квартал."""

    df_exploded = df.withColumn(
        "income_data",
        F.explode(F.from_json(
            F.get_json_object(F.col("data"), "$.income_international_current_quarter"),
            ArrayType(MapType(StringType(), StringType()))
        ))
    )

    return df_exploded.select(
        F.col("id"),
        F.col("company_name"),
        F.col("company_tin"),
        F.col("income_data.country").alias("country_current"),
        F.col("income_data.current_sum").cast(LongType()).alias("sum_current"),
    )


# ============================================================================
# Маппинг функций обработки
# ============================================================================

JOB_PROCESSORS = {
    "reports_summary": process_reports_summary,
    "report_residents": process_report_residents,
    "report_nonresidents": process_report_nonresidents,
    "report_nonresidents_family": process_report_nonresidents_family,
    "report_nonresidents_outside": process_report_nonresidents_outside,
    "report_authorized_capital": process_report_authorized_capital,
    "report_raised_investors": process_report_raised_investors,
    "report_borrowed_funds": process_report_borrowed_funds,
    "report_borrowed_international": process_report_borrowed_international,
    "report_another_method": process_report_another_method,
    "report_debt_instruments": process_report_debt_instruments,
    "report_income_international_prev": process_report_income_international_prev,
    "report_income_international_curr": process_report_income_international_curr,
}


def main():
    parser = argparse.ArgumentParser(description="Process AstanaHub reports")
    parser.add_argument("--year", type=int, required=True, help="Report year")
    parser.add_argument("--report_type", type=str, required=True, help="Report type (quarter1, quarter2, etc.)")
    parser.add_argument("--job_name", type=str, required=True, help="Job name to execute")
    parser.add_argument("--output_table", type=str, required=True, help="Output table name")
    parser.add_argument("--filter_field", type=str, default="", help="Filter field in data JSON")
    parser.add_argument("--trino_host", type=str, required=True, help="Trino host:port")
    parser.add_argument("--trino_catalog", type=str, required=True, help="Trino catalog")
    parser.add_argument("--trino_schema", type=str, required=True, help="Trino schema")
    parser.add_argument("--output_schema", type=str, required=True, help="Output schema for results")

    args = parser.parse_args()

    # Trino URL
    trino_url = f"jdbc:trino://{args.trino_host}/{args.trino_catalog}/{args.trino_schema}"
    output_table = f"{args.trino_catalog}.{args.output_schema}.{args.output_table}"

    # Создаём Spark сессию
    spark = create_spark_session(f"astanahub-{args.job_name}", args.trino_host)

    try:
        # Базовый запрос для чтения данных
        base_query = f"""
        SELECT
            r.id,
            r.year,
            r.status,
            r.report_type,
            r.data,
            r.version,
            r.signed_at,
            sr.number,
            sr.data as request_data,
            c.name as company_name,
            c.tin as company_tin,
            json_extract_scalar(sr.data, '$.activation_info') as activation_info,
            hf.data as hub_form_data
        FROM service_report r
        LEFT JOIN service_request sr ON r.service_request_id = sr.id
        LEFT JOIN company c ON sr.company_id = c.id
        LEFT JOIN hub_form hf ON sr.hub_form_id = hf.id
        WHERE r.year = {args.year}
          AND r.status = 'signed'
          AND r.report_type = '{args.report_type}'
        """

        # Добавляем фильтр если указан
        if args.filter_field:
            base_query += f"\n  AND json_extract_scalar(r.data, '$.{args.filter_field}') = 'true'"

        print(f"Executing query:\n{base_query}")

        # Читаем данные
        df = read_from_trino(spark, trino_url, base_query)

        print(f"Read {df.count()} records from Trino")

        # Получаем функцию обработки
        processor = JOB_PROCESSORS.get(args.job_name)
        if not processor:
            raise ValueError(f"Unknown job: {args.job_name}")

        # Обрабатываем данные
        if args.job_name == "reports_summary":
            result_df = processor(spark, df, args.year)
        else:
            result_df = processor(spark, df)

        print(f"Processed {result_df.count()} records")

        # Записываем результат
        write_to_trino(result_df, trino_url, output_table)

        print(f"Successfully wrote to {output_table}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
