"""
Экспорт витрины "Отчёты участников Technopark" в CSV.

Два варианта подключения:
1. Через Trino (порт 32770) - рекомендуется
2. Через Spark Thrift (порт 31000) - может падать с InstantiationException

Установка зависимостей:
    pip install trino pandas
    # или для Spark Thrift:
    pip install pyhive pandas thrift sasl thrift-sasl
"""

import pandas as pd
from datetime import datetime

# ============ КОНФИГУРАЦИЯ ============
HOST = '109.248.170.228'
USE_TRINO = True  # True = Trino, False = Spark Thrift

TRINO_PORT = 32770
SPARK_THRIFT_PORT = 31000

OUTPUT_FILE = f'vitrina_reports_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'

# ============ SQL ЗАПРОС ВИТРИНЫ ============
VITRINA_SQL = """
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
    CAST(get_json_object(r.data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(r.data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,

    -- Инвестиции
    CAST(get_json_object(r.data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(r.data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(r.data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(r.data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,

    -- Инвесторы
    CAST(get_json_object(r.data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(r.data, '$.investor_country_company') as investor_country_company,

    -- Займы
    CAST(get_json_object(r.data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(r.data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,

    -- Господдержка
    CAST(get_json_object(r.data, '$.finance_source_government') AS BIGINT) as finance_source_government,
    CAST(get_json_object(r.data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,

    -- Налоговые льготы
    CAST(get_json_object(r.data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(r.data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(r.data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(r.data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(r.data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(r.data, '$.collection_amount') AS BIGINT) as collection_amount,

    -- ОКЭД и исполнитель
    get_json_object(r.data, '$.oked') as oked,
    get_json_object(r.data, '$.executor_fullname') as executor_fullname,
    get_json_object(r.data, '$.executor_phone') as executor_phone,

    -- Флаги
    get_json_object(r.data, '$.has_nonresidents') as has_nonresidents,
    get_json_object(r.data, '$.has_borrowed_funds') as has_borrowed_funds,
    get_json_object(r.data, '$.has_raised_investors_funds') as has_raised_investors_funds

FROM bronze.service_report_cdc r
WHERE r.status = 'signed' AND r.year >= 2019
ORDER BY r.year DESC, r.report_type, r.id DESC
"""

# Для Trino нужен другой синтаксис JSON
VITRINA_SQL_TRINO = """
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

    -- Компания (Trino использует json_extract_scalar)
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
    CAST(json_extract_scalar(r.data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(json_extract_scalar(r.data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,

    -- Инвестиции
    CAST(json_extract_scalar(r.data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(json_extract_scalar(r.data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(json_extract_scalar(r.data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(json_extract_scalar(r.data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,

    -- Инвесторы
    CAST(json_extract_scalar(r.data, '$.investor_amount') AS BIGINT) as investor_amount,
    json_extract_scalar(r.data, '$.investor_country_company') as investor_country_company,

    -- Займы
    CAST(json_extract_scalar(r.data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(json_extract_scalar(r.data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,

    -- Господдержка
    CAST(json_extract_scalar(r.data, '$.finance_source_government') AS BIGINT) as finance_source_government,
    CAST(json_extract_scalar(r.data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,

    -- Налоговые льготы
    CAST(json_extract_scalar(r.data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(json_extract_scalar(r.data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(json_extract_scalar(r.data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(json_extract_scalar(r.data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(json_extract_scalar(r.data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(json_extract_scalar(r.data, '$.collection_amount') AS BIGINT) as collection_amount,

    -- ОКЭД и исполнитель
    json_extract_scalar(r.data, '$.oked') as oked,
    json_extract_scalar(r.data, '$.executor_fullname') as executor_fullname,
    json_extract_scalar(r.data, '$.executor_phone') as executor_phone,

    -- Флаги
    json_extract_scalar(r.data, '$.has_nonresidents') as has_nonresidents,
    json_extract_scalar(r.data, '$.has_borrowed_funds') as has_borrowed_funds,
    json_extract_scalar(r.data, '$.has_raised_investors_funds') as has_raised_investors_funds

FROM bronze.service_report_cdc r
WHERE r.status = 'signed' AND r.year >= 2019
ORDER BY r.year DESC, r.report_type, r.id DESC
"""


def export_via_trino():
    """Экспорт через Trino."""
    from trino.dbapi import connect

    print(f"Подключение к Trino: {HOST}:{TRINO_PORT}")

    conn = connect(
        host=HOST,
        port=TRINO_PORT,
        user='admin',
        catalog='iceberg',
        schema='bronze'
    )

    print("Выполнение запроса...")
    df = pd.read_sql(VITRINA_SQL_TRINO, conn)
    conn.close()

    return df


def export_via_spark_thrift():
    """Экспорт через Spark Thrift Server (PyHive)."""
    from pyhive import hive

    print(f"Подключение к Spark Thrift: {HOST}:{SPARK_THRIFT_PORT}")

    conn = hive.connect(
        host=HOST,
        port=SPARK_THRIFT_PORT,
        database='bronze'
    )

    print("Выполнение запроса...")
    df = pd.read_sql(VITRINA_SQL, conn)
    conn.close()

    return df


def main():
    print("=" * 50)
    print("Экспорт витрины отчётов в CSV")
    print("=" * 50)

    try:
        if USE_TRINO:
            df = export_via_trino()
        else:
            df = export_via_spark_thrift()

        print(f"\nПолучено записей: {len(df)}")
        print(f"Колонок: {len(df.columns)}")

        # Сохранение в CSV
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        print(f"\nФайл сохранён: {OUTPUT_FILE}")

        # Показать первые записи
        print("\nПервые 5 записей:")
        print(df.head().to_string())

    except Exception as e:
        print(f"\nОшибка: {e}")
        print("\nВозможные решения:")
        print("1. Проверьте доступность сервера")
        print("2. Установите зависимости: pip install trino pandas")
        print("3. Попробуйте другой порт (измените TRINO_PORT)")
        raise


if __name__ == '__main__':
    main()
