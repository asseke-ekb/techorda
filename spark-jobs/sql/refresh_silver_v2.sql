-- ============================================================================
-- Пересборка Silver слоя service_report_v2 из Bronze
-- Запускать через Spark SQL / DBeaver (Hive JDBC)
-- ============================================================================

-- 1. Удаляем старую таблицу
DROP TABLE IF EXISTS iceberg.silver.service_report_v2;

-- 2. Создаём таблицу заново
CREATE TABLE iceberg.silver.service_report_v2 (
    report_id INT,
    service_request_id INT,
    year INT,
    report_type STRING,
    status STRING,
    op STRING,
    version STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    signed_at TIMESTAMP,
    author_id INT,
    company_tin STRING,
    company_name STRING,
    certificate_number STRING,
    oked STRING,
    residents_count BIGINT,
    nonresidents_count BIGINT,
    gph_count BIGINT,
    income_total BIGINT,
    income_international BIGINT,
    income_total_current_quarter BIGINT,
    income_total_previous_quarter BIGINT,
    investments_total_current_quarter BIGINT,
    finance_source_increase_authorized_capital BIGINT,
    main_capital_investments BIGINT,
    main_tangible_capital_investments BIGINT,
    main_intangible_capital_investments BIGINT,
    finance_source_loan BIGINT,
    finance_source_loan_foreign BIGINT,
    government_support_measures BIGINT,
    finance_source_investment BIGINT,
    investor_amount BIGINT,
    investor_country_company STRING,
    tax_incentives BIGINT,
    tax_incentives_kpn BIGINT,
    tax_incentives_nds BIGINT,
    tax_incentives_ipn BIGINT,
    tax_incentives_sn BIGINT,
    total_tax_saved BIGINT,
    collection_amount BIGINT,
    activity_fields STRING COMMENT 'Направления деятельности (JSON массив)',
    etl_loaded_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (year);

-- ============================================================================
-- 3. Заливаем данные из Bronze ПО ГОДАМ
-- ВАЖНО: Для 2019-2020 company_tin/name в data JSON
--        Для 2021+ company_tin/name в signature.signed_xml (XML внутри JSON)
-- Выполнять каждый INSERT отдельно!
-- ============================================================================

-- ============================================================================
-- 3.1. Год 2019
-- ============================================================================
INSERT INTO iceberg.silver.service_report_v2
SELECT
    CAST(id AS INT) as report_id,
    CAST(service_request_id AS INT) as service_request_id,
    CAST(year AS INT) as year,
    report_type,
    status,
    op,
    version,
    created_at,
    updated_at,
    signed_at,
    CAST(author_id AS INT) as author_id,
    COALESCE(
        get_json_object(data, '$.company_tin'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<tin>([0-9]+)</tin>.*?</company>', 1)
    ) as company_tin,
    COALESCE(
        get_json_object(data, '$.company_name'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<name>([^<]+)</name>.*?</company>', 1)
    ) as company_name,
    get_json_object(data, '$.certificate_number') as certificate_number,
    get_json_object(data, '$.oked') as oked,
    CAST(get_json_object(data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY service_request_id, report_type, year ORDER BY updated_at DESC) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE year = 2019 AND op != 'd'
) t WHERE rn = 1;

-- ============================================================================
-- 3.2. Год 2020
-- ============================================================================
INSERT INTO iceberg.silver.service_report_v2
SELECT
    CAST(id AS INT) as report_id,
    CAST(service_request_id AS INT) as service_request_id,
    CAST(year AS INT) as year,
    report_type,
    status,
    op,
    version,
    created_at,
    updated_at,
    signed_at,
    CAST(author_id AS INT) as author_id,
    COALESCE(
        get_json_object(data, '$.company_tin'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<tin>([0-9]+)</tin>.*?</company>', 1)
    ) as company_tin,
    COALESCE(
        get_json_object(data, '$.company_name'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<name>([^<]+)</name>.*?</company>', 1)
    ) as company_name,
    get_json_object(data, '$.certificate_number') as certificate_number,
    get_json_object(data, '$.oked') as oked,
    CAST(get_json_object(data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY service_request_id, report_type, year ORDER BY updated_at DESC) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE year = 2020 AND op != 'd'
) t WHERE rn = 1;

-- ============================================================================
-- 3.3. Год 2021
-- ============================================================================
INSERT INTO iceberg.silver.service_report_v2
SELECT
    CAST(id AS INT) as report_id,
    CAST(service_request_id AS INT) as service_request_id,
    CAST(year AS INT) as year,
    report_type,
    status,
    op,
    version,
    created_at,
    updated_at,
    signed_at,
    CAST(author_id AS INT) as author_id,
    COALESCE(
        get_json_object(data, '$.company_tin'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<tin>([0-9]+)</tin>.*?</company>', 1)
    ) as company_tin,
    COALESCE(
        get_json_object(data, '$.company_name'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<name>([^<]+)</name>.*?</company>', 1)
    ) as company_name,
    get_json_object(data, '$.certificate_number') as certificate_number,
    get_json_object(data, '$.oked') as oked,
    CAST(get_json_object(data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY service_request_id, report_type, year ORDER BY updated_at DESC) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE year = 2021 AND op != 'd'
) t WHERE rn = 1;

-- ============================================================================
-- 3.4. Год 2022
-- ============================================================================
INSERT INTO iceberg.silver.service_report_v2
SELECT
    CAST(id AS INT) as report_id,
    CAST(service_request_id AS INT) as service_request_id,
    CAST(year AS INT) as year,
    report_type,
    status,
    op,
    version,
    created_at,
    updated_at,
    signed_at,
    CAST(author_id AS INT) as author_id,
    COALESCE(
        get_json_object(data, '$.company_tin'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<tin>([0-9]+)</tin>.*?</company>', 1)
    ) as company_tin,
    COALESCE(
        get_json_object(data, '$.company_name'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<name>([^<]+)</name>.*?</company>', 1)
    ) as company_name,
    get_json_object(data, '$.certificate_number') as certificate_number,
    get_json_object(data, '$.oked') as oked,
    CAST(get_json_object(data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY service_request_id, report_type, year ORDER BY updated_at DESC) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE year = 2022 AND op != 'd'
) t WHERE rn = 1;

-- ============================================================================
-- 3.5. Год 2023
-- ============================================================================
INSERT INTO iceberg.silver.service_report_v2
SELECT
    CAST(id AS INT) as report_id,
    CAST(service_request_id AS INT) as service_request_id,
    CAST(year AS INT) as year,
    report_type,
    status,
    op,
    version,
    created_at,
    updated_at,
    signed_at,
    CAST(author_id AS INT) as author_id,
    COALESCE(
        get_json_object(data, '$.company_tin'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<tin>([0-9]+)</tin>.*?</company>', 1)
    ) as company_tin,
    COALESCE(
        get_json_object(data, '$.company_name'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<name>([^<]+)</name>.*?</company>', 1)
    ) as company_name,
    get_json_object(data, '$.certificate_number') as certificate_number,
    get_json_object(data, '$.oked') as oked,
    CAST(get_json_object(data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY service_request_id, report_type, year ORDER BY updated_at DESC) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE year = 2023 AND op != 'd'
) t WHERE rn = 1;

-- ============================================================================
-- 3.6. Год 2024
-- ============================================================================
INSERT INTO iceberg.silver.service_report_v2
SELECT
    CAST(id AS INT) as report_id,
    CAST(service_request_id AS INT) as service_request_id,
    CAST(year AS INT) as year,
    report_type,
    status,
    op,
    version,
    created_at,
    updated_at,
    signed_at,
    CAST(author_id AS INT) as author_id,
    COALESCE(
        get_json_object(data, '$.company_tin'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<tin>([0-9]+)</tin>.*?</company>', 1)
    ) as company_tin,
    COALESCE(
        get_json_object(data, '$.company_name'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<name>([^<]+)</name>.*?</company>', 1)
    ) as company_name,
    get_json_object(data, '$.certificate_number') as certificate_number,
    get_json_object(data, '$.oked') as oked,
    CAST(get_json_object(data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY service_request_id, report_type, year ORDER BY updated_at DESC) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE year = 2024 AND op != 'd'
) t WHERE rn = 1;

-- ============================================================================
-- 3.7. Год 2025
-- ============================================================================
INSERT INTO iceberg.silver.service_report_v2
SELECT
    CAST(id AS INT) as report_id,
    CAST(service_request_id AS INT) as service_request_id,
    CAST(year AS INT) as year,
    report_type,
    status,
    op,
    version,
    created_at,
    updated_at,
    signed_at,
    CAST(author_id AS INT) as author_id,
    COALESCE(
        get_json_object(data, '$.company_tin'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<tin>([0-9]+)</tin>.*?</company>', 1)
    ) as company_tin,
    COALESCE(
        get_json_object(data, '$.company_name'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<name>([^<]+)</name>.*?</company>', 1)
    ) as company_name,
    get_json_object(data, '$.certificate_number') as certificate_number,
    get_json_object(data, '$.oked') as oked,
    CAST(get_json_object(data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY service_request_id, report_type, year ORDER BY updated_at DESC) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE year = 2025 AND op != 'd'
) t WHERE rn = 1;

-- ============================================================================
-- 3.8. Год 2026
-- ============================================================================
INSERT INTO iceberg.silver.service_report_v2
SELECT
    CAST(id AS INT) as report_id,
    CAST(service_request_id AS INT) as service_request_id,
    CAST(year AS INT) as year,
    report_type,
    status,
    op,
    version,
    created_at,
    updated_at,
    signed_at,
    CAST(author_id AS INT) as author_id,
    COALESCE(
        get_json_object(data, '$.company_tin'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<tin>([0-9]+)</tin>.*?</company>', 1)
    ) as company_tin,
    COALESCE(
        get_json_object(data, '$.company_name'),
        regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<name>([^<]+)</name>.*?</company>', 1)
    ) as company_name,
    get_json_object(data, '$.certificate_number') as certificate_number,
    get_json_object(data, '$.oked') as oked,
    CAST(get_json_object(data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY service_request_id, report_type, year ORDER BY updated_at DESC) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE year = 2026 AND op != 'd'
) t WHERE rn = 1;

-- ============================================================================
-- 4. ПРОВЕРКА РЕЗУЛЬТАТОВ
-- ============================================================================
SELECT year, COUNT(*) as cnt FROM iceberg.silver.service_report_v2 GROUP BY year ORDER BY year;
