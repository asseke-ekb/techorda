-- Gold витрина: Общие показатели
-- Соответствует Power Query: "Обработка 2 кв отчета 2025 г для отображения общих показателей"

CREATE TABLE IF NOT EXISTS iceberg.gold.general_indicators (
    year INT,
    certificate_number STRING,
    bin STRING,
    company_name STRING,
    certificate_issue_date DATE,
    government_support BIGINT COMMENT 'Меры государственной поддержки',
    tax_saved BIGINT COMMENT 'Сэкономлено за отчетный квартал',
    export_income BIGINT COMMENT 'Доход за счет международных продаж',
    total_funding BIGINT COMMENT 'Всего инвестиций за отчетный квартал',
    income_total BIGINT COMMENT 'Доход от реализации товаров (накопительно)'
)
USING iceberg
PARTITIONED BY (year)
COMMENT 'Общие показатели для дашборда Power BI';

-- Заполнение витрины
INSERT OVERWRITE iceberg.gold.general_indicators
SELECT
    year,
    certificate_number,
    bin,
    company_name,
    certificate_issue_date,
    government_support_measures as government_support,
    total_tax_saved as tax_saved,
    income_international as export_income,
    investments_total_current_quarter as total_funding,
    income_total
FROM iceberg.silver.service_report_v2
WHERE op != 'd';
