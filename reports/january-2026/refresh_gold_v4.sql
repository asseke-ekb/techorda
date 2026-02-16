
DROP TABLE IF EXISTS iceberg.gold.general_indicators;

CREATE TABLE iceberg.gold.general_indicators (
    year INT,
    report_type STRING,
    certificate_number STRING,
    bin STRING,
    company_name STRING,
    certificate_issue_date DATE ,
    certificate_end_date DATE ,
    activity_fields STRING,
    government_support BIGINT,
    tax_saved BIGINT,
    export_income BIGINT,
    total_funding BIGINT,
    income_total BIGINT,
    income_total_current_quarter BIGINT,
    is_resident BOOLEAN,
    country STRING
)
USING iceberg
PARTITIONED BY (year);

INSERT INTO iceberg.gold.general_indicators
WITH servicerequest AS (
    SELECT id,
        get_json_object(data, '$.activation_info.certificate_number') as cert_number,
        CAST(get_json_object(data, '$.activation_info.issue_date') AS DATE) as cert_issue_date,
        CAST(get_json_object(data, '$.activation_info.end_date') AS DATE) as cert_end_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM iceberg.bronze.service_servicerequest_cdc
        WHERE service_id = 'techpark' AND op <> 'd'
    ) t WHERE rn = 1
)
SELECT
    sr.year,
    sr.report_type,
    COALESCE(
        NULLIF(sr.certificate_number, ''),
        p.certificate_number,
        NULLIF(ss.cert_number, '-1')
    ) as certificate_number,
    sr.company_tin as bin,
    sr.company_name,
    COALESCE(p.certificate_issue_date, ss.cert_issue_date) as certificate_issue_date,
    COALESCE(p.certificate_end_date, ss.cert_end_date) as certificate_end_date,
    sr.activity_fields,
    sr.government_support_measures as government_support,
    sr.total_tax_saved as tax_saved,
    sr.income_international as export_income,
    sr.investments_total_current_quarter as total_funding,
    sr.income_total,
    sr.income_total_current_quarter,
    CASE
        WHEN sr.investor_country_company IS NULL OR sr.investor_country_company = '' OR sr.investor_country_company = 'Казахстан'
        THEN true
        ELSE false
    END as is_resident,
    NULLIF(sr.investor_country_company, '') as country
FROM iceberg.silver.service_report_v2 sr
LEFT JOIN iceberg.silver.techpark_participants p ON sr.service_request_id = p.service_request_id
LEFT JOIN servicerequest ss ON sr.service_request_id = ss.id
WHERE sr.status = 'signed';



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
WITH servicerequest AS (
    SELECT id,
        get_json_object(data, '$.activation_info.certificate_number') as cert_number
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM iceberg.bronze.service_servicerequest_cdc
        WHERE service_id = 'techpark' AND op <> 'd'
    ) t WHERE rn = 1
)
SELECT
    sr.year,
    sr.report_type,
    COALESCE(
        NULLIF(sr.certificate_number, ''),
        p.certificate_number,
        NULLIF(ss.cert_number, '-1')
    ) as certificate_number,
    sr.company_tin as bin,
    sr.government_support_measures as government_support,
    sr.finance_source_loan as loan_funds,
    sr.finance_source_increase_authorized_capital as authorized_capital_increase,
    sr.investments_total_current_quarter as total_funding,
    COALESCE(sr.finance_source_increase_authorized_capital, 0) +
    COALESCE(sr.finance_source_investment, 0) as attracted_investments
FROM iceberg.silver.service_report_v2 sr
LEFT JOIN iceberg.silver.techpark_participants p ON sr.service_request_id = p.service_request_id
LEFT JOIN servicerequest ss ON sr.service_request_id = ss.id
WHERE sr.status = 'signed';


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
WITH servicerequest AS (
    SELECT id,
        get_json_object(data, '$.activation_info.certificate_number') as cert_number
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM iceberg.bronze.service_servicerequest_cdc
        WHERE service_id = 'techpark' AND op <> 'd'
    ) t WHERE rn = 1
)
SELECT
    sr.year,
    sr.report_type,
    COALESCE(
        NULLIF(sr.certificate_number, ''),
        p.certificate_number,
        NULLIF(ss.cert_number, '-1')
    ) as certificate_number,
    sr.company_tin as bin,
    sr.tax_incentives_kpn as kpn,
    sr.tax_incentives_nds as nds,
    sr.tax_incentives_ipn as ipn,
    sr.tax_incentives_sn as sn,
    sr.total_tax_saved
FROM iceberg.silver.service_report_v2 sr
LEFT JOIN iceberg.silver.techpark_participants p ON sr.service_request_id = p.service_request_id
LEFT JOIN servicerequest ss ON sr.service_request_id = ss.id
WHERE sr.status = 'signed';



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
WITH servicerequest AS (
    SELECT id,
        get_json_object(data, '$.activation_info.certificate_number') as cert_number
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM iceberg.bronze.service_servicerequest_cdc
        WHERE service_id = 'techpark' AND op <> 'd'
    ) t WHERE rn = 1
)
SELECT
    sr.year,
    sr.report_type,
    COALESCE(
        NULLIF(sr.certificate_number, ''),
        p.certificate_number,
        NULLIF(ss.cert_number, '-1')
    ) as certificate_number,
    sr.company_tin as bin,
    sr.residents_count,
    sr.nonresidents_count,
    sr.gph_count
FROM iceberg.silver.service_report_v2 sr
LEFT JOIN iceberg.silver.techpark_participants p ON sr.service_request_id = p.service_request_id
LEFT JOIN servicerequest ss ON sr.service_request_id = ss.id
WHERE sr.status = 'signed';


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
WITH servicerequest AS (
    SELECT id,
        get_json_object(data, '$.activation_info.certificate_number') as cert_number
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM iceberg.bronze.service_servicerequest_cdc
        WHERE service_id = 'techpark' AND op <> 'd'
    ) t WHERE rn = 1
)
SELECT
    sr.year,
    sr.report_type,
    COALESCE(
        NULLIF(sr.certificate_number, ''),
        p.certificate_number,
        NULLIF(ss.cert_number, '-1')
    ) as certificate_number,
    sr.company_tin as bin,
    sr.company_name,
    sr.income_international as export_income
FROM iceberg.silver.service_report_v2 sr
LEFT JOIN iceberg.silver.techpark_participants p ON sr.service_request_id = p.service_request_id
LEFT JOIN servicerequest ss ON sr.service_request_id = ss.id
WHERE sr.status = 'signed';


DROP TABLE IF EXISTS iceberg.gold.export_by_country;

CREATE TABLE iceberg.gold.export_by_country (
    year INT,
    report_type STRING,
    certificate_number STRING,
    bin STRING,
    company_name STRING,
    country STRING COMMENT 'Страна экспорта',
    export_income BIGINT COMMENT 'Доход от экспорта в эту страну'
)
USING iceberg
PARTITIONED BY (year);

INSERT INTO iceberg.gold.export_by_country
WITH bronze_export AS (
    SELECT id,
        get_json_object(data, '$.income_international_current_quarter') as export_json
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'r' AND year >= 2019
          AND data IS NOT NULL AND data <> '__debezium_unavailable_value'
          AND get_json_object(data, '$.income_international_current_quarter') LIKE '[%'
    ) t WHERE rn = 1
),
servicerequest AS (
    SELECT id,
        get_json_object(data, '$.activation_info.certificate_number') as cert_number
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM iceberg.bronze.service_servicerequest_cdc
        WHERE service_id = 'techpark' AND op <> 'd'
    ) t WHERE rn = 1
)
SELECT
    sr.year,
    sr.report_type,
    COALESCE(
        NULLIF(sr.certificate_number, ''),
        p.certificate_number,
        NULLIF(ss.cert_number, '-1')
    ) as certificate_number,
    sr.company_tin as bin,
    sr.company_name,
    get_json_object(export_item, '$.country') as country,
    CAST(get_json_object(export_item, '$.current_sum') AS BIGINT) as export_income
FROM iceberg.silver.service_report_v2 sr
INNER JOIN bronze_export be ON sr.report_id = be.id
LEFT JOIN iceberg.silver.techpark_participants p ON sr.service_request_id = p.service_request_id
LEFT JOIN servicerequest ss ON sr.service_request_id = ss.id
LATERAL VIEW explode(
    from_json(be.export_json, 'array<string>')
) AS export_item
WHERE sr.status = 'signed'
  AND get_json_object(export_item, '$.country') IS NOT NULL
  AND get_json_object(export_item, '$.country') <> ''
  AND get_json_object(export_item, '$.country') <> 'None';



SELECT 'general_indicators' as table_name, COUNT(*) as cnt FROM iceberg.gold.general_indicators
UNION ALL
SELECT 'financing', COUNT(*) FROM iceberg.gold.financing
UNION ALL
SELECT 'tax_benefits', COUNT(*) FROM iceberg.gold.tax_benefits
UNION ALL
SELECT 'employees', COUNT(*) FROM iceberg.gold.employees
UNION ALL
SELECT 'exports', COUNT(*) FROM iceberg.gold.exports
UNION ALL
SELECT 'export_by_country', COUNT(*) FROM iceberg.gold.export_by_country;

-- Проверка маскировки BIN
SELECT
    CASE WHEN bin LIKE '777777%' THEN 'MASKED' ELSE 'REAL' END as bin_status,
    COUNT(*) as cnt
FROM iceberg.gold.general_indicators
GROUP BY CASE WHEN bin LIKE '777777%' THEN 'MASKED' ELSE 'REAL' END;

-- Проверка пустых сертификатов
SELECT
    CASE WHEN certificate_number IS NULL OR certificate_number = '' THEN 'EMPTY' ELSE 'FILLED' END as cert_status,
    COUNT(*) as cnt
FROM iceberg.gold.general_indicators
GROUP BY CASE WHEN certificate_number IS NULL OR certificate_number = '' THEN 'EMPTY' ELSE 'FILLED' END;
