-- ============================================================================
-- Пересборка Gold витрин (v4 - с сертификатами из servicerequest)
-- Запускать через Spark SQL / DBeaver (Hive JDBC)
-- ============================================================================
-- Изменения v4:
-- 1. Добавлен JOIN с service_servicerequest_cdc для получения сертификатов
-- 2. COALESCE для certificate: data -> participants -> servicerequest
-- 3. Исправлена проблема с 85 пустыми сертификатами (теперь ~2)
-- ============================================================================
-- Источники данных:
-- - service_report_cdc (op='r'): финансовые данные из data
-- - service_report_cdc (op='u'): замаскированные tin/name из signature.signed_xml
-- - techpark_participants: сертификаты для registered/deactivated
-- - service_servicerequest_cdc: сертификаты для остальных статусов
-- ============================================================================


-- ============================================================================
-- 1. ОБЩИЕ ПОКАЗАТЕЛИ (general_indicators)
-- ============================================================================

DROP TABLE IF EXISTS iceberg.gold.general_indicators;

CREATE TABLE iceberg.gold.general_indicators (
    year INT,
    report_type STRING,
    certificate_number STRING,
    bin STRING,
    company_name STRING,
    certificate_issue_date DATE COMMENT 'Дата выдачи свидетельства участника',
    certificate_end_date DATE COMMENT 'Срок действия свидетельства',
    activity_fields STRING COMMENT 'Направления деятельности (JSON массив)',
    government_support BIGINT COMMENT 'Меры государственной поддержки',
    tax_saved BIGINT COMMENT 'Сэкономлено за отчетный квартал',
    export_income BIGINT COMMENT 'Доход за счет международных продаж',
    total_funding BIGINT COMMENT 'Всего инвестиций за отчетный квартал',
    income_total BIGINT COMMENT 'Доход от реализации (накопительно)',
    income_total_current_quarter BIGINT COMMENT 'Доход от реализации за текущий квартал',
    is_resident BOOLEAN COMMENT 'Резидент РК',
    country STRING COMMENT 'Страна (если нерезидент)'
)
USING iceberg
PARTITIONED BY (year);

INSERT INTO iceberg.gold.general_indicators
WITH old_data AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE op = 'r' AND year >= 2019 AND data IS NOT NULL AND data <> '__debezium_unavailable_value'
),
new_sig AS (
    SELECT id,
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1) as masked_tin,
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1) as masked_name
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'u' AND year >= 2019
    ) t WHERE rn = 1
),
servicerequest AS (
    SELECT id,
        get_json_object(data, '$.activation_info.certificate_number') as cert_number,
        CAST(get_json_object(data, '$.activation_info.issue_date') AS DATE) as cert_issue_date,
        CAST(get_json_object(data, '$.activation_info.end_date') AS DATE) as cert_end_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM iceberg.bronze.service_servicerequest_cdc
        WHERE service_id = 'techpark' AND op != 'd'
    ) t WHERE rn = 1
)
SELECT
    CAST(o.year AS INT) as year,
    o.report_type,
    -- certificate: data -> participants -> servicerequest
    COALESCE(
        NULLIF(get_json_object(o.data, '$.certificate_number'), ''),
        p.certificate_number,
        NULLIF(ss.cert_number, '-1')
    ) as certificate_number,
    -- tin: приоритет замаскированному из new_sig
    COALESCE(
        n.masked_tin,
        get_json_object(o.data, '$.company_tin'),
        p.company_tin
    ) as bin,
    -- name: приоритет замаскированному из new_sig
    COALESCE(
        n.masked_name,
        get_json_object(o.data, '$.company_name'),
        p.company_name
    ) as company_name,
    COALESCE(p.certificate_issue_date, ss.cert_issue_date) as certificate_issue_date,
    COALESCE(p.certificate_end_date, ss.cert_end_date) as certificate_end_date,
    get_json_object(o.data, '$.activity_fields') as activity_fields,
    CAST(get_json_object(o.data, '$.government_support_measures') AS BIGINT) as government_support,
    CAST(get_json_object(o.data, '$.total_tax_saved') AS BIGINT) as tax_saved,
    CAST(get_json_object(o.data, '$.income_international') AS BIGINT) as export_income,
    CAST(get_json_object(o.data, '$.investments_total_current_quarter') AS BIGINT) as total_funding,
    CAST(get_json_object(o.data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(o.data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CASE
        WHEN get_json_object(o.data, '$.country') IS NULL OR get_json_object(o.data, '$.country') = '' OR get_json_object(o.data, '$.country') = 'Казахстан'
        THEN true
        ELSE false
    END as is_resident,
    NULLIF(get_json_object(o.data, '$.country'), '') as country
FROM old_data o
LEFT JOIN new_sig n ON o.id = n.id
LEFT JOIN iceberg.silver.techpark_participants p ON o.service_request_id = p.service_request_id
LEFT JOIN servicerequest ss ON o.service_request_id = ss.id
WHERE o.rn = 1 AND o.status = 'signed';


-- ============================================================================
-- 2. ФИНАНСИРОВАНИЕ (financing)
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
WITH old_data AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE op = 'r' AND year >= 2019 AND data IS NOT NULL AND data <> '__debezium_unavailable_value'
),
new_sig AS (
    SELECT id,
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1) as masked_tin
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'u' AND year >= 2019
    ) t WHERE rn = 1
),
servicerequest AS (
    SELECT id,
        get_json_object(data, '$.activation_info.certificate_number') as cert_number
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM iceberg.bronze.service_servicerequest_cdc
        WHERE service_id = 'techpark' AND op != 'd'
    ) t WHERE rn = 1
)
SELECT
    CAST(o.year AS INT) as year,
    o.report_type,
    COALESCE(
        NULLIF(get_json_object(o.data, '$.certificate_number'), ''),
        p.certificate_number,
        NULLIF(ss.cert_number, '-1')
    ) as certificate_number,
    COALESCE(
        n.masked_tin,
        get_json_object(o.data, '$.company_tin'),
        p.company_tin
    ) as bin,
    CAST(get_json_object(o.data, '$.government_support_measures') AS BIGINT) as government_support,
    CAST(get_json_object(o.data, '$.finance_source_loan') AS BIGINT) as loan_funds,
    CAST(get_json_object(o.data, '$.finance_source_increase_authorized_capital') AS BIGINT) as authorized_capital_increase,
    CAST(get_json_object(o.data, '$.investments_total_current_quarter') AS BIGINT) as total_funding,
    COALESCE(CAST(get_json_object(o.data, '$.finance_source_increase_authorized_capital') AS BIGINT), 0) +
    COALESCE(CAST(get_json_object(o.data, '$.finance_source_investment') AS BIGINT), 0) as attracted_investments
FROM old_data o
LEFT JOIN new_sig n ON o.id = n.id
LEFT JOIN iceberg.silver.techpark_participants p ON o.service_request_id = p.service_request_id
LEFT JOIN servicerequest ss ON o.service_request_id = ss.id
WHERE o.rn = 1 AND o.status = 'signed';


-- ============================================================================
-- 3. НАЛОГОВЫЕ ЛЬГОТЫ (tax_benefits)
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
WITH old_data AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE op = 'r' AND year >= 2019 AND data IS NOT NULL AND data <> '__debezium_unavailable_value'
),
new_sig AS (
    SELECT id,
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1) as masked_tin
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'u' AND year >= 2019
    ) t WHERE rn = 1
),
servicerequest AS (
    SELECT id,
        get_json_object(data, '$.activation_info.certificate_number') as cert_number
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM iceberg.bronze.service_servicerequest_cdc
        WHERE service_id = 'techpark' AND op != 'd'
    ) t WHERE rn = 1
)
SELECT
    CAST(o.year AS INT) as year,
    o.report_type,
    COALESCE(
        NULLIF(get_json_object(o.data, '$.certificate_number'), ''),
        p.certificate_number,
        NULLIF(ss.cert_number, '-1')
    ) as certificate_number,
    COALESCE(
        n.masked_tin,
        get_json_object(o.data, '$.company_tin'),
        p.company_tin
    ) as bin,
    CAST(get_json_object(o.data, '$.tax_incentives_kpn') AS BIGINT) as kpn,
    CAST(get_json_object(o.data, '$.tax_incentives_nds') AS BIGINT) as nds,
    CAST(get_json_object(o.data, '$.tax_incentives_ipn') AS BIGINT) as ipn,
    CAST(get_json_object(o.data, '$.tax_incentives_sn') AS BIGINT) as sn,
    CAST(get_json_object(o.data, '$.total_tax_saved') AS BIGINT) as total_tax_saved
FROM old_data o
LEFT JOIN new_sig n ON o.id = n.id
LEFT JOIN iceberg.silver.techpark_participants p ON o.service_request_id = p.service_request_id
LEFT JOIN servicerequest ss ON o.service_request_id = ss.id
WHERE o.rn = 1 AND o.status = 'signed';


-- ============================================================================
-- 4. РАБОТНИКИ (employees)
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
WITH old_data AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE op = 'r' AND year >= 2019 AND data IS NOT NULL AND data <> '__debezium_unavailable_value'
),
new_sig AS (
    SELECT id,
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1) as masked_tin
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'u' AND year >= 2019
    ) t WHERE rn = 1
),
servicerequest AS (
    SELECT id,
        get_json_object(data, '$.activation_info.certificate_number') as cert_number
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM iceberg.bronze.service_servicerequest_cdc
        WHERE service_id = 'techpark' AND op != 'd'
    ) t WHERE rn = 1
)
SELECT
    CAST(o.year AS INT) as year,
    o.report_type,
    COALESCE(
        NULLIF(get_json_object(o.data, '$.certificate_number'), ''),
        p.certificate_number,
        NULLIF(ss.cert_number, '-1')
    ) as certificate_number,
    COALESCE(
        n.masked_tin,
        get_json_object(o.data, '$.company_tin'),
        p.company_tin
    ) as bin,
    CAST(get_json_object(o.data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(o.data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(o.data, '$.gph_count') AS BIGINT) as gph_count
FROM old_data o
LEFT JOIN new_sig n ON o.id = n.id
LEFT JOIN iceberg.silver.techpark_participants p ON o.service_request_id = p.service_request_id
LEFT JOIN servicerequest ss ON o.service_request_id = ss.id
WHERE o.rn = 1 AND o.status = 'signed';


-- ============================================================================
-- 5. ЭКСПОРТ (exports)
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
WITH old_data AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE op = 'r' AND year >= 2019 AND data IS NOT NULL AND data <> '__debezium_unavailable_value'
),
new_sig AS (
    SELECT id,
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1) as masked_tin,
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1) as masked_name
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'u' AND year >= 2019
    ) t WHERE rn = 1
),
servicerequest AS (
    SELECT id,
        get_json_object(data, '$.activation_info.certificate_number') as cert_number
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM iceberg.bronze.service_servicerequest_cdc
        WHERE service_id = 'techpark' AND op != 'd'
    ) t WHERE rn = 1
)
SELECT
    CAST(o.year AS INT) as year,
    o.report_type,
    COALESCE(
        NULLIF(get_json_object(o.data, '$.certificate_number'), ''),
        p.certificate_number,
        NULLIF(ss.cert_number, '-1')
    ) as certificate_number,
    COALESCE(
        n.masked_tin,
        get_json_object(o.data, '$.company_tin'),
        p.company_tin
    ) as bin,
    COALESCE(
        n.masked_name,
        get_json_object(o.data, '$.company_name'),
        p.company_name
    ) as company_name,
    CAST(get_json_object(o.data, '$.income_international') AS BIGINT) as export_income
FROM old_data o
LEFT JOIN new_sig n ON o.id = n.id
LEFT JOIN iceberg.silver.techpark_participants p ON o.service_request_id = p.service_request_id
LEFT JOIN servicerequest ss ON o.service_request_id = ss.id
WHERE o.rn = 1 AND o.status = 'signed';


-- ============================================================================
-- 6. ЭКСПОРТ ПО СТРАНАМ (export_by_country)
-- ============================================================================

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
WITH old_data AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE op = 'r' AND year >= 2019
      AND data IS NOT NULL AND data <> '__debezium_unavailable_value'
      AND get_json_object(data, '$.income_international_current_quarter') LIKE '[%'
),
new_sig AS (
    SELECT id,
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1) as masked_tin,
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1) as masked_name
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'u' AND year >= 2019
    ) t WHERE rn = 1
),
servicerequest AS (
    SELECT id,
        get_json_object(data, '$.activation_info.certificate_number') as cert_number
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rn
        FROM iceberg.bronze.service_servicerequest_cdc
        WHERE service_id = 'techpark' AND op != 'd'
    ) t WHERE rn = 1
)
SELECT
    CAST(o.year AS INT) as year,
    o.report_type,
    COALESCE(
        NULLIF(get_json_object(o.data, '$.certificate_number'), ''),
        p.certificate_number,
        NULLIF(ss.cert_number, '-1')
    ) as certificate_number,
    COALESCE(
        n.masked_tin,
        get_json_object(o.data, '$.company_tin'),
        p.company_tin
    ) as bin,
    COALESCE(
        n.masked_name,
        get_json_object(o.data, '$.company_name'),
        p.company_name
    ) as company_name,
    get_json_object(export_item, '$.country') as country,
    CAST(get_json_object(export_item, '$.current_sum') AS BIGINT) as export_income
FROM old_data o
LEFT JOIN new_sig n ON o.id = n.id
LEFT JOIN iceberg.silver.techpark_participants p ON o.service_request_id = p.service_request_id
LEFT JOIN servicerequest ss ON o.service_request_id = ss.id
LATERAL VIEW explode(
    from_json(
        get_json_object(o.data, '$.income_international_current_quarter'),
        'array<string>'
    )
) AS export_item
WHERE o.rn = 1 AND o.status = 'signed'
  AND get_json_object(export_item, '$.country') IS NOT NULL
  AND get_json_object(export_item, '$.country') != ''
  AND get_json_object(export_item, '$.country') != 'None';


-- ============================================================================
-- ПРОВЕРКА РЕЗУЛЬТАТОВ
-- ============================================================================

-- Количество записей по витринам
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
    COUNT(*) as cnt,
    SUM(income_total_current_quarter) as total_income
FROM iceberg.gold.general_indicators
GROUP BY CASE WHEN bin LIKE '777777%' THEN 'MASKED' ELSE 'REAL' END;

-- Проверка пустых BIN
SELECT
    CASE WHEN bin IS NULL OR bin = '' THEN 'EMPTY' ELSE 'FILLED' END as bin_status,
    COUNT(*) as cnt
FROM iceberg.gold.general_indicators
GROUP BY CASE WHEN bin IS NULL OR bin = '' THEN 'EMPTY' ELSE 'FILLED' END;

-- Проверка пустых сертификатов (должно быть ~2 вместо 85)
SELECT
    CASE WHEN certificate_number IS NULL OR certificate_number = '' THEN 'EMPTY' ELSE 'FILLED' END as cert_status,
    COUNT(*) as cnt
FROM iceberg.gold.general_indicators
GROUP BY CASE WHEN certificate_number IS NULL OR certificate_number = '' THEN 'EMPTY' ELSE 'FILLED' END;

-- Детали пустых сертификатов (если есть)
SELECT year, bin, company_name
FROM iceberg.gold.general_indicators
WHERE certificate_number IS NULL OR certificate_number = '';
