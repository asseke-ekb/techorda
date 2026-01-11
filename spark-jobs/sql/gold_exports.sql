-- Gold витрина: Экспорт
-- Соответствует Power Query: "Обработка 2 кв отчета 2025 г для отображения экспорта"
-- ПРИМЕЧАНИЕ: В Silver нет детализации по странам, только общая сумма экспорта

CREATE TABLE IF NOT EXISTS iceberg.gold.exports (
    year INT,
    certificate_number STRING,
    bin STRING,
    company_name STRING,
    export_income BIGINT COMMENT 'Доход за счет международных продаж за текущий квартал'
)
USING iceberg
PARTITIONED BY (year)
COMMENT 'Экспорт для дашборда Power BI (без детализации по странам)';

-- Заполнение витрины
INSERT OVERWRITE iceberg.gold.exports
SELECT
    year,
    certificate_number,
    bin,
    company_name,
    income_international as export_income
FROM iceberg.silver.service_report_v2
WHERE op != 'd';

-- ВАЖНО: В Power Query используется отдельная Google Sheets таблица "2kv"
-- с детализацией экспорта по странам (БИН, Страна, Сумма).
-- Такой информации в service_report_v2 НЕТ.
-- Если нужна детализация по странам - нужно добавить отдельную таблицу export_details
-- или расширить Bronze/Silver слои для хранения этой информации.
