-- Gold витрина: Работники
-- Соответствует Power Query: "Обработка 2 кв отчета 2025 г для отображения количества работников"

CREATE TABLE IF NOT EXISTS iceberg.gold.employees (
    year INT,
    certificate_number STRING,
    bin STRING,
    residents_count INT COMMENT 'Количество работников резиденты',
    non_residents_count INT COMMENT 'Количество работников нерезиденты',
    civil_contracts_count INT COMMENT 'Количество договоров ГПХ'
)
USING iceberg
PARTITIONED BY (year)
COMMENT 'Работники для дашборда Power BI';

-- Заполнение витрины
INSERT OVERWRITE iceberg.gold.employees
SELECT
    year,
    certificate_number,
    bin,
    residents_count,
    non_residents_count,
    civil_contracts_count
FROM iceberg.silver.service_report_v2
WHERE op != 'd';
