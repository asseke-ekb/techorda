-- ============================================================================
-- Пересборка Gold витрин из Silver
-- Запускать через Spark SQL / DBeaver (Hive JDBC)
-- Выполнять каждый блок отдельно!
-- ============================================================================
-- ВАЖНО: Сначала выполнить refresh_silver_participants.sql для создания
-- таблицы iceberg.silver.techpark_participants
-- ============================================================================


-- ============================================================================
-- 1. ОБЩИЕ ПОКАЗАТЕЛИ (general_indicators)
-- Power Query: "Обработка 2 кв отчета 2025 г для отображения общих показателей"
-- Поля: Год, № свид-ва, БИН, Наименование, Дата выдачи свидетельства,
--       ГОСПОДДЕРЖКА, TAXES, EXPORT, total_funding, INCOME_TOTAL
-- ============================================================================

DROP TABLE IF EXISTS iceberg.gold.general_indicators;

CREATE TABLE iceberg.gold.general_indicators (
    year INT,
    report_type STRING,
    certificate_number STRING,
    bin STRING,
    company_name STRING,
    certificate_issue_date DATE COMMENT 'Дата выдачи свидетельства участника',
    activity_fields STRING COMMENT 'Направления деятельности (JSON массив)',
    government_support BIGINT COMMENT 'Меры государственной поддержки',
    tax_saved BIGINT COMMENT 'Сэкономлено за отчетный квартал',
    export_income BIGINT COMMENT 'Доход за счет международных продаж',
    total_funding BIGINT COMMENT 'Всего инвестиций за отчетный квартал',
    income_total BIGINT COMMENT 'Доход от реализации (накопительно)'
)
USING iceberg
PARTITIONED BY (year);

INSERT INTO iceberg.gold.general_indicators
SELECT
    r.year,
    r.report_type,
    COALESCE(r.certificate_number, p.certificate_number) as certificate_number,
    COALESCE(r.company_tin, p.company_tin) as bin,
    COALESCE(r.company_name, p.company_name) as company_name,
    p.certificate_issue_date,
    r.activity_fields,
    r.government_support_measures as government_support,
    r.total_tax_saved as tax_saved,
    r.income_international as export_income,
    r.investments_total_current_quarter as total_funding,
    r.income_total
FROM iceberg.silver.service_report_v2 r
LEFT JOIN iceberg.silver.techpark_participants p
    ON r.service_request_id = p.service_request_id
WHERE r.status = 'signed' AND r.op != 'd';


-- ============================================================================
-- 2. ФИНАНСИРОВАНИЕ (financing)
-- Power Query: "Обработка 2 кв отчета 2025 г для отображения финансирования"
-- ============================================================================

DROP TABLE IF EXISTS iceberg.gold.financing;

CREATE TABLE iceberg.gold.financing (
    year INT,
    report_type STRING,
    certificate_number STRING,
    bin STRING,
    government_support BIGINT,
    loan_funds BIGINT,
    authorized_capital_increase BIGINT,
    total_funding BIGINT,
    attracted_investments BIGINT
)
USING iceberg
PARTITIONED BY (year);

INSERT INTO iceberg.gold.financing
SELECT
    r.year,
    r.report_type,
    COALESCE(r.certificate_number, p.certificate_number) as certificate_number,
    COALESCE(r.company_tin, p.company_tin) as bin,
    r.government_support_measures as government_support,
    r.finance_source_loan as loan_funds,
    r.finance_source_increase_authorized_capital as authorized_capital_increase,
    r.investments_total_current_quarter as total_funding,
    COALESCE(r.finance_source_increase_authorized_capital, 0) +
    COALESCE(r.finance_source_investment, 0) as attracted_investments
FROM iceberg.silver.service_report_v2 r
LEFT JOIN iceberg.silver.techpark_participants p
    ON r.service_request_id = p.service_request_id
WHERE r.status = 'signed' AND r.op != 'd';


-- ============================================================================
-- 3. НАЛОГОВЫЕ ЛЬГОТЫ (tax_benefits)
-- Power Query: "Обработка 2 кв отчета 2025 г для отображения налоговых льгот"
-- ============================================================================

DROP TABLE IF EXISTS iceberg.gold.tax_benefits;

CREATE TABLE iceberg.gold.tax_benefits (
    year INT,
    report_type STRING,
    certificate_number STRING,
    bin STRING,
    kpn BIGINT,
    nds BIGINT,
    ipn BIGINT,
    sn BIGINT,
    total_tax_saved BIGINT
)
USING iceberg
PARTITIONED BY (year);

INSERT INTO iceberg.gold.tax_benefits
SELECT
    r.year,
    r.report_type,
    COALESCE(r.certificate_number, p.certificate_number) as certificate_number,
    COALESCE(r.company_tin, p.company_tin) as bin,
    r.tax_incentives_kpn as kpn,
    r.tax_incentives_nds as nds,
    r.tax_incentives_ipn as ipn,
    r.tax_incentives_sn as sn,
    r.total_tax_saved
FROM iceberg.silver.service_report_v2 r
LEFT JOIN iceberg.silver.techpark_participants p
    ON r.service_request_id = p.service_request_id
WHERE r.status = 'signed' AND r.op != 'd';


-- ============================================================================
-- 4. РАБОТНИКИ (employees)
-- Power Query: "Обработка 2 кв отчета 2025 г для отображения количества работников"
-- ============================================================================

DROP TABLE IF EXISTS iceberg.gold.employees;

CREATE TABLE iceberg.gold.employees (
    year INT,
    report_type STRING,
    certificate_number STRING,
    bin STRING,
    residents_count BIGINT,
    nonresidents_count BIGINT,
    gph_count BIGINT
)
USING iceberg
PARTITIONED BY (year);

INSERT INTO iceberg.gold.employees
SELECT
    r.year,
    r.report_type,
    COALESCE(r.certificate_number, p.certificate_number) as certificate_number,
    COALESCE(r.company_tin, p.company_tin) as bin,
    r.residents_count,
    r.nonresidents_count,
    r.gph_count
FROM iceberg.silver.service_report_v2 r
LEFT JOIN iceberg.silver.techpark_participants p
    ON r.service_request_id = p.service_request_id
WHERE r.status = 'signed' AND r.op != 'd';


-- ============================================================================
-- 5. ЭКСПОРТ (exports)
-- Power Query: "Обработка 2 кв отчета 2025 г для отображения экспорта"
-- ============================================================================

DROP TABLE IF EXISTS iceberg.gold.exports;

CREATE TABLE iceberg.gold.exports (
    year INT,
    report_type STRING,
    certificate_number STRING,
    bin STRING,
    company_name STRING,
    export_income BIGINT
)
USING iceberg
PARTITIONED BY (year);

INSERT INTO iceberg.gold.exports
SELECT
    r.year,
    r.report_type,
    COALESCE(r.certificate_number, p.certificate_number) as certificate_number,
    COALESCE(r.company_tin, p.company_tin) as bin,
    COALESCE(r.company_name, p.company_name) as company_name,
    r.income_international as export_income
FROM iceberg.silver.service_report_v2 r
LEFT JOIN iceberg.silver.techpark_participants p
    ON r.service_request_id = p.service_request_id
WHERE r.status = 'signed' AND r.op != 'd';


-- ============================================================================
-- ПРОВЕРКА РЕЗУЛЬТАТОВ
-- ============================================================================

SELECT 'general_indicators' as table_name, COUNT(*) as cnt FROM iceberg.gold.general_indicators
UNION ALL
SELECT 'financing', COUNT(*) FROM iceberg.gold.financing
UNION ALL
SELECT 'tax_benefits', COUNT(*) FROM iceberg.gold.tax_benefits
UNION ALL
SELECT 'employees', COUNT(*) FROM iceberg.gold.employees
UNION ALL
SELECT 'exports', COUNT(*) FROM iceberg.gold.exports;
