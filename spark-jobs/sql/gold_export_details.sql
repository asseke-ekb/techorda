-- Gold витрина: Экспорт с детализацией по странам
-- Соответствует функции report_income_international_current_quarter() из arm.py
-- Разворачивает JSON массив income_international_current_quarter

CREATE TABLE IF NOT EXISTS iceberg.gold.export_details (
    year INT,
    report_type STRING,
    certificate_number STRING,
    bin STRING,
    company_name STRING,
    country STRING COMMENT 'Страна экспорта',
    export_amount BIGINT COMMENT 'Сумма экспорта в страну'
)
USING iceberg
PARTITIONED BY (year)
COMMENT 'Детализация экспорта по странам для дашборда Power BI';

-- Заполнение витрины
-- ВАЖНО: Используем lateral view explode для разворачивания JSON массива
INSERT OVERWRITE iceberg.gold.export_details
SELECT
    year,
    report_type,
    certificate_number,
    bin,
    company_name,
    get_json_object(export_item, '$.country') as country,
    CAST(get_json_object(export_item, '$.current_sum') AS BIGINT) as export_amount
FROM (
    SELECT
        year,
        report_type,
        certificate_number,
        bin,
        company_name,
        income_international_current_quarter
    FROM iceberg.silver.service_report_v2
    WHERE income_international_current_quarter IS NOT NULL
      AND income_international_current_quarter != '[]'
      AND op != 'd'
) t
LATERAL VIEW explode(from_json(income_international_current_quarter, 'array<string>')) AS export_item
WHERE get_json_object(export_item, '$.country') IS NOT NULL;
