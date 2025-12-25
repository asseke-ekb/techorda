#!/usr/bin/env python3
"""
Тест агрегации по году через JDBC (как DBeaver).

Запуск:
    C:/Python311/python.exe test_year_jdbc.py 2024
"""

import os
import sys

# JAVA_HOME
JAVA_HOME = "C:/Users/asseke/Desktop/tech/jdk-17.0.17+10"
if os.path.exists(JAVA_HOME) and "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = JAVA_HOME
    os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

# JDBC настройки
JDBC_URL = "jdbc:hive2://109.248.170.228:31000/iceberg.bronze"
JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver"
JDBC_JAR = "C:/Users/asseke/Desktop/tech/hive-jdbc-standalone.jar"


def main():
    if len(sys.argv) < 2:
        print("Usage: python test_year_jdbc.py <year>")
        sys.exit(1)

    year = int(sys.argv[1])

    import jaydebeapi

    query = f"""
    SELECT
        COUNT(*) AS total_reports,
        SUM(CAST(get_json_object(data, '$.income_total') AS BIGINT)) AS income_total
    FROM bronze.service_report_cdc
    WHERE year = {year}
      AND status = 'signed'
    """

    print(f"Connecting via JDBC...")
    print(f"  URL: {JDBC_URL}")

    conn = jaydebeapi.connect(
        JDBC_DRIVER, JDBC_URL, ["", ""], JDBC_JAR
    )
    cursor = conn.cursor()

    print(f"Executing query for year={year}...")
    cursor.execute(query)

    row = cursor.fetchone()
    cursor.close()
    conn.close()

    print(f"[RESULT] year={year}")
    print(f"[RESULT] total_reports={row[0]}, income_total={row[1]}")


if __name__ == "__main__":
    main()
