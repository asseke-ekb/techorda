#!/usr/bin/env python3
"""
Локальный тест витрины через Hive JDBC.

Установка:
    pip install pyhive pandas thrift thrift-sasl sasl

Запуск:
    python local_test.py
    python local_test.py --year 2024
    python local_test.py --year 2024 --report-type quarter1
"""

import argparse
from datetime import datetime
import pandas as pd

# Конфигурация Hive/Spark Thrift Server
HIVE_HOST = '109.248.170.228'
HIVE_PORT = 31000


def get_hive_connection():
    """Подключение к Hive/Spark Thrift Server."""
    from pyhive import hive
    return hive.connect(
        host=HIVE_HOST,
        port=HIVE_PORT,
        auth='NONE'  # Без SASL аутентификации
    )


def build_vitrina_query(year: int = None, report_type: str = None) -> str:
    """SQL запрос для витрины (Hive/Spark SQL синтаксис)."""

    where_clauses = ["r.status = 'signed'", "r.op != 'd'"]

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
    r.op as cdc_operation,

    -- Компания
    get_json_object(r.data, '$.company_name') as company_name,
    get_json_object(r.data, '$.company_tin') as company_tin,
    CAST(get_json_object(r.data, '$.certificate_number') AS INT) as certificate_number,

    -- Сотрудники
    CAST(get_json_object(r.data, '$.residents_count') AS INT) as residents_count,
    CAST(get_json_object(r.data, '$.nonresidents_count') AS INT) as nonresidents_count,
    CAST(get_json_object(r.data, '$.gph_count') AS INT) as gph_count,

    -- Доходы
    CAST(get_json_object(r.data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(r.data, '$.income_international') AS BIGINT) as income_international,

    -- Инвестиции
    CAST(get_json_object(r.data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(r.data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,

    -- Инвесторы
    CAST(get_json_object(r.data, '$.investor_amount') AS BIGINT) as investor_amount,

    -- Займы
    CAST(get_json_object(r.data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(r.data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,

    -- Господдержка
    CAST(get_json_object(r.data, '$.finance_source_government') AS BIGINT) as finance_source_government,

    -- Налоговые льготы
    CAST(get_json_object(r.data, '$.tax_incentives') AS BIGINT) as tax_incentives,

    -- ОКЭД и исполнитель
    get_json_object(r.data, '$.oked') as oked,
    get_json_object(r.data, '$.executor_fullname') as executor_fullname

FROM iceberg.bronze.service_report_cdc r
WHERE {where_sql}
ORDER BY r.year DESC, r.report_type, r.id DESC
"""


def build_summary_query(year: int = None) -> str:
    """SQL запрос для агрегированной сводки (Hive/Spark SQL)."""

    year_filter = f"AND r.year = {year}" if year else "AND r.year >= 2019"

    return f"""
SELECT
    r.year,
    r.report_type,
    COUNT(*) as reports_count,
    SUM(COALESCE(CAST(get_json_object(r.data, '$.residents_count') AS INT), 0)) as total_residents,
    SUM(COALESCE(CAST(get_json_object(r.data, '$.nonresidents_count') AS INT), 0)) as total_nonresidents,
    SUM(COALESCE(CAST(get_json_object(r.data, '$.income_total') AS BIGINT), 0)) as total_income,
    SUM(COALESCE(CAST(get_json_object(r.data, '$.income_international') AS BIGINT), 0)) as total_income_international,
    SUM(COALESCE(CAST(get_json_object(r.data, '$.main_capital_investments') AS BIGINT), 0)) as total_investments,
    SUM(COALESCE(CAST(get_json_object(r.data, '$.tax_incentives') AS BIGINT), 0)) as total_tax_incentives
FROM iceberg.bronze.service_report_cdc r
WHERE r.status = 'signed' AND r.op != 'd' {year_filter}
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
    print(f"Hive: {HIVE_HOST}:{HIVE_PORT}")
    print(f"Mode: {args.mode}")
    print(f"Year: {args.year or 'all'}")
    print(f"Report type: {args.report_type or 'all'}")
    print("=" * 60)

    try:
        print("\nConnecting to Hive/Spark Thrift Server...")
        conn = get_hive_connection()

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
        print(f"1. Check if Hive is accessible: telnet {HIVE_HOST} {HIVE_PORT}")
        print("2. Install dependencies: pip install pyhive pandas thrift sasl thrift-sasl")
        print("3. Check JDBC URL in DBeaver: jdbc:hive2://109.248.170.228:31000/iceberg.bronze")
        raise


if __name__ == "__main__":
    main()
