#!/usr/bin/env python3
"""
Локальный тест витрины через Trino (без Iceberg).

Trino работает, Spark Thrift - нет (InstantiationException).
Поэтому для локального теста используем Trino + pandas.

Установка:
    pip install trino pandas pyspark

Запуск:
    python local_test.py
    python local_test.py --year 2024
    python local_test.py --year 2024 --report-type quarter1
"""

import argparse
from datetime import datetime
import pandas as pd

# Конфигурация Trino
TRINO_HOST = '109.248.170.228'
TRINO_PORT = 32770
TRINO_USER = 'admin'
TRINO_CATALOG = 'iceberg'
TRINO_SCHEMA = 'bronze'


def get_trino_connection():
    """Подключение к Trino."""
    from trino.dbapi import connect
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA
    )


def build_vitrina_query(year: int = None, report_type: str = None) -> str:
    """SQL запрос для витрины (Trino синтаксис)."""

    where_clauses = ["r.status = 'signed'"]

    if year:
        where_clauses.append(f"r.year = {year}")
    else:
        where_clauses.append("r.year >= 2019")

    if report_type:
        where_clauses.append(f"r.report_type = '{report_type}'")

    where_sql = " AND ".join(where_clauses)

    return f"""
SELECT
    r.id as report_id,
    r.year,
    r.report_type,
    r.status,
    r.signed_at,
    r.version,
    r.created_at,
    r.updated_at,
    r.service_request_id,
    r.author_id,

    -- Компания
    json_extract_scalar(r.data, '$.company_name') as company_name,
    json_extract_scalar(r.data, '$.company_tin') as company_tin,
    CAST(json_extract_scalar(r.data, '$.certificate_number') AS INTEGER) as certificate_number,

    -- Сотрудники
    CAST(json_extract_scalar(r.data, '$.residents_count') AS INTEGER) as residents_count,
    CAST(json_extract_scalar(r.data, '$.nonresidents_count') AS INTEGER) as nonresidents_count,
    CAST(json_extract_scalar(r.data, '$.gph_count') AS INTEGER) as gph_count,

    -- Доходы
    CAST(json_extract_scalar(r.data, '$.income_total') AS BIGINT) as income_total,
    CAST(json_extract_scalar(r.data, '$.income_international') AS BIGINT) as income_international,

    -- Инвестиции
    CAST(json_extract_scalar(r.data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(json_extract_scalar(r.data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,

    -- Инвесторы
    CAST(json_extract_scalar(r.data, '$.investor_amount') AS BIGINT) as investor_amount,

    -- Займы
    CAST(json_extract_scalar(r.data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(json_extract_scalar(r.data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,

    -- Господдержка
    CAST(json_extract_scalar(r.data, '$.finance_source_government') AS BIGINT) as finance_source_government,

    -- Налоговые льготы
    CAST(json_extract_scalar(r.data, '$.tax_incentives') AS BIGINT) as tax_incentives,

    -- ОКЭД и исполнитель
    json_extract_scalar(r.data, '$.oked') as oked,
    json_extract_scalar(r.data, '$.executor_fullname') as executor_fullname

FROM bronze.service_report_cdc r
WHERE {where_sql}
ORDER BY r.year DESC, r.report_type, r.id DESC
"""


def build_summary_query(year: int = None) -> str:
    """SQL запрос для агрегированной сводки."""

    year_filter = f"AND r.year = {year}" if year else "AND r.year >= 2019"

    return f"""
SELECT
    r.year,
    r.report_type,
    COUNT(*) as reports_count,
    SUM(COALESCE(CAST(json_extract_scalar(r.data, '$.residents_count') AS INTEGER), 0)) as total_residents,
    SUM(COALESCE(CAST(json_extract_scalar(r.data, '$.nonresidents_count') AS INTEGER), 0)) as total_nonresidents,
    SUM(COALESCE(CAST(json_extract_scalar(r.data, '$.income_total') AS BIGINT), 0)) as total_income,
    SUM(COALESCE(CAST(json_extract_scalar(r.data, '$.income_international') AS BIGINT), 0)) as total_income_international,
    SUM(COALESCE(CAST(json_extract_scalar(r.data, '$.main_capital_investments') AS BIGINT), 0)) as total_investments,
    SUM(COALESCE(CAST(json_extract_scalar(r.data, '$.tax_incentives') AS BIGINT), 0)) as total_tax_incentives
FROM bronze.service_report_cdc r
WHERE r.status = 'signed' {year_filter}
GROUP BY r.year, r.report_type
ORDER BY r.year DESC, r.report_type
"""


def main():
    parser = argparse.ArgumentParser(description="Local test for reports vitrina")
    parser.add_argument("--year", type=int, help="Filter by year")
    parser.add_argument("--report-type", type=str, help="Filter by report type")
    parser.add_argument("--mode", choices=["vitrina", "summary"], default="vitrina")
    parser.add_argument("--limit", type=int, default=100, help="Limit rows for testing")
    parser.add_argument("--output", type=str, help="Output CSV file path")

    args = parser.parse_args()

    print("=" * 60)
    print(f"Local Test - {datetime.now().isoformat()}")
    print("=" * 60)
    print(f"Trino: {TRINO_HOST}:{TRINO_PORT}")
    print(f"Mode: {args.mode}")
    print(f"Year: {args.year or 'all'}")
    print(f"Report type: {args.report_type or 'all'}")
    print("=" * 60)

    try:
        print("\nConnecting to Trino...")
        conn = get_trino_connection()

        # Выбираем запрос
        if args.mode == "summary":
            query = build_summary_query(args.year)
        else:
            query = build_vitrina_query(args.year, args.report_type)

        # Добавляем LIMIT для теста
        if args.limit and args.mode == "vitrina":
            query = query.rstrip().rstrip(';') + f"\nLIMIT {args.limit}"

        print(f"\nExecuting query...")
        print("-" * 40)

        df = pd.read_sql(query, conn)
        conn.close()

        print(f"\nResults: {len(df)} rows, {len(df.columns)} columns")
        print("-" * 40)

        # Показываем колонки
        print("\nColumns:")
        for col in df.columns:
            print(f"  - {col}")

        # Показываем первые записи
        print("\nFirst 5 rows:")
        print(df.head().to_string())

        # Статистика по числовым полям
        if args.mode == "summary":
            print("\nSummary statistics:")
            print(df.to_string())
        else:
            numeric_cols = ['residents_count', 'nonresidents_count', 'income_total', 'tax_incentives']
            existing_cols = [c for c in numeric_cols if c in df.columns]
            if existing_cols:
                print(f"\nNumeric summary:")
                print(df[existing_cols].describe().to_string())

        # Сохранение в CSV
        if args.output:
            df.to_csv(args.output, index=False, encoding='utf-8-sig')
            print(f"\nSaved to: {args.output}")
        else:
            # Автосохранение
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"vitrina_{args.mode}_{timestamp}.csv"
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"\nAuto-saved to: {output_file}")

        print("\nDone!")

    except Exception as e:
        print(f"\nError: {e}")
        print("\nTroubleshooting:")
        print("1. Check if Trino is accessible: telnet 109.248.170.228 32770")
        print("2. Install dependencies: pip install trino pandas")
        print("3. Try different port: 8080, 30300, 31390")
        raise


if __name__ == "__main__":
    main()
