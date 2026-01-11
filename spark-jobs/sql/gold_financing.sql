-- Gold витрина: Финансирование
-- Соответствует Power Query: "Обработка 2 кв отчета 2025 г для отображения финансирования"

CREATE TABLE IF NOT EXISTS iceberg.gold.financing (
    year INT,
    certificate_number STRING,
    bin STRING,
    government_support BIGINT COMMENT 'Меры государственной поддержки',
    loan_funds BIGINT COMMENT 'Возмездные и безвозмездные заемные средства',
    authorized_capital_increase BIGINT COMMENT 'Сумма вложений учредителем для увеличения уставного капитала',
    total_funding BIGINT COMMENT 'Всего инвестиций за отчетный квартал',
    attracted_investments BIGINT COMMENT 'Привлеченные инвестиции (вклад в УК + долговые инструменты + иной вклад)'
)
USING iceberg
PARTITIONED BY (year)
COMMENT 'Финансирование для дашборда Power BI';

-- Заполнение витрины
INSERT OVERWRITE iceberg.gold.financing
SELECT
    year,
    certificate_number,
    bin,
    government_support_measures as government_support,
    finance_source_loan as loan_funds,
    finance_source_increase_authorized_capital as authorized_capital_increase,
    investments_total_current_quarter as total_funding,
    -- Расчет привлеченных инвестиций (как в Power Query)
    COALESCE(finance_source_increase_authorized_capital, 0) +
    COALESCE(finance_source_investment, 0) as attracted_investments
FROM iceberg.silver.service_report_v2
WHERE op != 'd';
