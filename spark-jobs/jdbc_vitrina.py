#!/usr/bin/env python3
"""
Прямое JDBC подключение к Spark Thrift Server через jaydebeapi.

Установка:
    pip install jaydebeapi pandas

Запуск:
    set JAVA_HOME=C:/Users/asseke/Desktop/tech/jdk-17.0.17+10
    python jdbc_vitrina.py
    python jdbc_vitrina.py --query "SELECT * FROM service_report_cdc LIMIT 5"
"""

import os
import sys
import argparse
from datetime import datetime

# Устанавливаем JAVA_HOME
JAVA_HOME = "C:/Users/asseke/Desktop/tech/jdk-17.0.17+10"
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

# JDBC настройки
JDBC_URL = "jdbc:hive2://109.248.170.228:31000/iceberg.bronze"
JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver"
JDBC_JAR = "C:/Users/asseke/Desktop/tech/hive-jdbc-standalone.jar"


def get_connection():
    """Создаёт JDBC подключение."""
    import jaydebeapi
    return jaydebeapi.connect(
        JDBC_DRIVER,
        JDBC_URL,
        ["", ""],  # user, password (пустые)
        JDBC_JAR
    )


def execute_query(query: str):
    """Выполняет SQL запрос и возвращает результаты."""
    import pandas as pd

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)

        # Получаем колонки
        columns = [desc[0] for desc in cursor.description] if cursor.description else []

        # Получаем данные
        rows = cursor.fetchall()

        cursor.close()

        return pd.DataFrame(rows, columns=columns)
    finally:
        conn.close()


def test_describe():
    """Тестирует DESCRIBE запрос (работает)."""
    print("Testing DESCRIBE (should work)...")
    try:
        df = execute_query("DESCRIBE service_report_cdc")
        print("DESCRIBE result:")
        print(df.to_string())
        return True
    except Exception as e:
        print(f"DESCRIBE failed: {e}")
        return False


def test_select():
    """Тестирует SELECT запрос (может не работать с Iceberg)."""
    print("\nTesting SELECT (may fail with Iceberg)...")
    try:
        df = execute_query("SELECT id, year FROM service_report_cdc LIMIT 3")
        print("SELECT result:")
        print(df.to_string())
        return True
    except Exception as e:
        print(f"SELECT failed: {e}")
        print("\nThis is expected! Spark Thrift Server cannot read Iceberg tables.")
        print("The iceberg-spark-runtime JAR is missing on the server.")
        return False


def build_vitrina_query(year: int = None, report_type: str = None, limit: int = 1000) -> str:
    """SQL запрос для витрины."""
    where_clauses = ["status = 'signed'"]

    if year:
        where_clauses.append(f"year = {year}")
    else:
        where_clauses.append("year >= 2019")

    if report_type:
        where_clauses.append(f"report_type = '{report_type}'")

    where_sql = " AND ".join(where_clauses)

    return f"""
SELECT
    id as report_id,
    year,
    report_type,
    status,
    signed_at,
    version,
    created_at,
    updated_at,
    service_request_id,
    author_id,
    get_json_object(data, '$.company_name') as company_name,
    get_json_object(data, '$.company_tin') as company_tin,
    CAST(get_json_object(data, '$.certificate_number') AS INT) as certificate_number,
    CAST(get_json_object(data, '$.residents_count') AS INT) as residents_count,
    CAST(get_json_object(data, '$.nonresidents_count') AS INT) as nonresidents_count,
    CAST(get_json_object(data, '$.gph_count') AS INT) as gph_count,
    CAST(get_json_object(data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(data, '$.investor_amount') AS BIGINT) as investor_amount,
    CAST(get_json_object(data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(data, '$.finance_source_government') AS BIGINT) as finance_source_government,
    CAST(get_json_object(data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    get_json_object(data, '$.oked') as oked,
    get_json_object(data, '$.executor_fullname') as executor_fullname
FROM service_report_cdc
WHERE {where_sql}
ORDER BY year DESC, report_type, id DESC
LIMIT {limit}
"""


def export_vitrina(year: int = None, report_type: str = None, limit: int = 1000, output: str = None):
    """Экспортирует витрину в CSV."""
    import pandas as pd

    query = build_vitrina_query(year, report_type, limit)
    print(f"Executing vitrina query...")
    print(f"  Year: {year or 'all (>= 2019)'}")
    print(f"  Report type: {report_type or 'all'}")
    print(f"  Limit: {limit}")

    df = execute_query(query)
    print(f"\nResults: {len(df)} rows, {len(df.columns)} columns")

    # Показываем превью
    print("\nFirst 5 rows:")
    print(df.head().to_string())

    # Статистика
    numeric_cols = ['residents_count', 'nonresidents_count', 'income_total', 'tax_incentives']
    existing_cols = [c for c in numeric_cols if c in df.columns]
    if existing_cols:
        print(f"\nNumeric summary:")
        print(df[existing_cols].describe().to_string())

    # Сохранение
    if output:
        output_path = output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"vitrina_{timestamp}.csv"

    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"\nSaved to: {output_path}")

    return df


def main():
    parser = argparse.ArgumentParser(description="JDBC connection to Spark Thrift")
    parser.add_argument("--query", type=str, help="SQL query to execute")
    parser.add_argument("--output", type=str, help="Output CSV file")
    parser.add_argument("--test", action="store_true", help="Run tests")
    parser.add_argument("--vitrina", action="store_true", help="Export full vitrina")
    parser.add_argument("--year", type=int, help="Filter by year")
    parser.add_argument("--report-type", type=str, help="Filter by report type")
    parser.add_argument("--limit", type=int, default=1000, help="Limit rows")

    args = parser.parse_args()

    print("=" * 60)
    print(f"JDBC Vitrina - {datetime.now().isoformat()}")
    print("=" * 60)
    print(f"JAVA_HOME: {JAVA_HOME}")
    print(f"JDBC URL: {JDBC_URL}")
    print(f"JDBC JAR: {JDBC_JAR}")
    print("=" * 60)

    # Проверяем JAR
    if not os.path.exists(JDBC_JAR):
        print(f"\nERROR: JDBC JAR not found at {JDBC_JAR}")
        print("\nDownload from:")
        print("https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/3.1.3/hive-jdbc-3.1.3-standalone.jar")
        sys.exit(1)

    try:
        import jaydebeapi
    except ImportError:
        print("\nERROR: jaydebeapi not installed")
        print("Run: pip install jaydebeapi pandas")
        sys.exit(1)

    if args.test:
        test_describe()
        test_select()
        return

    if args.vitrina:
        export_vitrina(args.year, args.report_type, args.limit, args.output)
        return

    if args.query:
        print(f"\nExecuting: {args.query}")
        try:
            df = execute_query(args.query)
            print(f"\nResults: {len(df)} rows")
            print(df.to_string())

            if args.output:
                df.to_csv(args.output, index=False, encoding='utf-8-sig')
                print(f"\nSaved to: {args.output}")
        except Exception as e:
            print(f"\nError: {e}")
    else:
        # По умолчанию - тесты
        test_describe()
        test_select()


if __name__ == "__main__":
    main()
