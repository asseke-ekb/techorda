DROP TABLE IF EXISTS iceberg.silver.service_report_v2;

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

INSERT INTO iceberg.silver.service_report_v2
WITH old_data AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'r' AND year = 2019 AND data IS NOT NULL AND data <> '__debezium_unavailable_value'
    ) t WHERE rn = 1
),
new_sig AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'u' AND year = 2019
    ) t WHERE rn = 1
)
SELECT
    CAST(COALESCE(n.id, o.id) AS INT) as report_id,
    CAST(COALESCE(n.service_request_id, o.service_request_id) AS INT) as service_request_id,
    CAST(COALESCE(n.year, o.year) AS INT) as year,
    COALESCE(n.report_type, o.report_type) as report_type,
    COALESCE(n.status, o.status) as status,
    COALESCE(n.op, o.op) as op,
    COALESCE(n.version, o.version) as version,
    COALESCE(n.created_at, o.created_at) as created_at,
    COALESCE(n.updated_at, o.updated_at) as updated_at,
    COALESCE(n.signed_at, o.signed_at) as signed_at,
    CAST(COALESCE(n.author_id, o.author_id) AS INT) as author_id,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1),
        get_json_object(o.data, '$.company_tin'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1)
    ) as company_tin,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1),
        get_json_object(o.data, '$.company_name'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1)
    ) as company_name,
    get_json_object(o.data, '$.certificate_number') as certificate_number,
    get_json_object(o.data, '$.oked') as oked,
    CAST(get_json_object(o.data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(o.data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(o.data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(o.data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(o.data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(o.data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(o.data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(o.data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(o.data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(o.data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(o.data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(o.data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(o.data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(o.data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(o.data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(o.data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(o.data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(o.data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(o.data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(o.data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(o.data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(o.data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(o.data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(o.data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(o.data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(o.data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM old_data o
LEFT JOIN new_sig n ON o.id = n.id;

INSERT INTO iceberg.silver.service_report_v2
WITH old_data AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'r' AND year = 2020 AND data IS NOT NULL AND data <> '__debezium_unavailable_value'
    ) t WHERE rn = 1
),
new_sig AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'u' AND year = 2020
    ) t WHERE rn = 1
)
SELECT
    CAST(COALESCE(n.id, o.id) AS INT) as report_id,
    CAST(COALESCE(n.service_request_id, o.service_request_id) AS INT) as service_request_id,
    CAST(COALESCE(n.year, o.year) AS INT) as year,
    COALESCE(n.report_type, o.report_type) as report_type,
    COALESCE(n.status, o.status) as status,
    COALESCE(n.op, o.op) as op,
    COALESCE(n.version, o.version) as version,
    COALESCE(n.created_at, o.created_at) as created_at,
    COALESCE(n.updated_at, o.updated_at) as updated_at,
    COALESCE(n.signed_at, o.signed_at) as signed_at,
    CAST(COALESCE(n.author_id, o.author_id) AS INT) as author_id,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1),
        get_json_object(o.data, '$.company_tin'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1)
    ) as company_tin,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1),
        get_json_object(o.data, '$.company_name'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1)
    ) as company_name,
    get_json_object(o.data, '$.certificate_number') as certificate_number,
    get_json_object(o.data, '$.oked') as oked,
    CAST(get_json_object(o.data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(o.data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(o.data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(o.data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(o.data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(o.data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(o.data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(o.data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(o.data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(o.data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(o.data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(o.data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(o.data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(o.data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(o.data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(o.data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(o.data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(o.data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(o.data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(o.data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(o.data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(o.data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(o.data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(o.data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(o.data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(o.data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM old_data o
LEFT JOIN new_sig n ON o.id = n.id;


INSERT INTO iceberg.silver.service_report_v2
WITH old_data AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'r' AND year = 2021 AND data IS NOT NULL AND data <> '__debezium_unavailable_value'
    ) t WHERE rn = 1
),
new_sig AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'u' AND year = 2021
    ) t WHERE rn = 1
)
SELECT
    CAST(COALESCE(n.id, o.id) AS INT) as report_id,
    CAST(COALESCE(n.service_request_id, o.service_request_id) AS INT) as service_request_id,
    CAST(COALESCE(n.year, o.year) AS INT) as year,
    COALESCE(n.report_type, o.report_type) as report_type,
    COALESCE(n.status, o.status) as status,
    COALESCE(n.op, o.op) as op,
    COALESCE(n.version, o.version) as version,
    COALESCE(n.created_at, o.created_at) as created_at,
    COALESCE(n.updated_at, o.updated_at) as updated_at,
    COALESCE(n.signed_at, o.signed_at) as signed_at,
    CAST(COALESCE(n.author_id, o.author_id) AS INT) as author_id,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1),
        get_json_object(o.data, '$.company_tin'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1)
    ) as company_tin,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1),
        get_json_object(o.data, '$.company_name'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1)
    ) as company_name,
    get_json_object(o.data, '$.certificate_number') as certificate_number,
    get_json_object(o.data, '$.oked') as oked,
    CAST(get_json_object(o.data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(o.data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(o.data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(o.data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(o.data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(o.data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(o.data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(o.data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(o.data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(o.data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(o.data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(o.data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(o.data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(o.data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(o.data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(o.data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(o.data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(o.data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(o.data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(o.data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(o.data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(o.data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(o.data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(o.data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(o.data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(o.data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM old_data o
LEFT JOIN new_sig n ON o.id = n.id;


INSERT INTO iceberg.silver.service_report_v2
WITH old_data AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'r' AND year = 2022 AND data IS NOT NULL AND data <> '__debezium_unavailable_value'
    ) t WHERE rn = 1
),
new_sig AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'u' AND year = 2022
    ) t WHERE rn = 1
)
SELECT
    CAST(COALESCE(n.id, o.id) AS INT) as report_id,
    CAST(COALESCE(n.service_request_id, o.service_request_id) AS INT) as service_request_id,
    CAST(COALESCE(n.year, o.year) AS INT) as year,
    COALESCE(n.report_type, o.report_type) as report_type,
    COALESCE(n.status, o.status) as status,
    COALESCE(n.op, o.op) as op,
    COALESCE(n.version, o.version) as version,
    COALESCE(n.created_at, o.created_at) as created_at,
    COALESCE(n.updated_at, o.updated_at) as updated_at,
    COALESCE(n.signed_at, o.signed_at) as signed_at,
    CAST(COALESCE(n.author_id, o.author_id) AS INT) as author_id,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1),
        get_json_object(o.data, '$.company_tin'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1)
    ) as company_tin,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1),
        get_json_object(o.data, '$.company_name'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1)
    ) as company_name,
    get_json_object(o.data, '$.certificate_number') as certificate_number,
    get_json_object(o.data, '$.oked') as oked,
    CAST(get_json_object(o.data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(o.data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(o.data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(o.data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(o.data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(o.data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(o.data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(o.data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(o.data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(o.data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(o.data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(o.data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(o.data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(o.data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(o.data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(o.data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(o.data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(o.data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(o.data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(o.data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(o.data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(o.data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(o.data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(o.data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(o.data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(o.data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM old_data o
LEFT JOIN new_sig n ON o.id = n.id;


INSERT INTO iceberg.silver.service_report_v2
WITH old_data AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'r' AND year = 2023 AND data IS NOT NULL AND data <> '__debezium_unavailable_value'
    ) t WHERE rn = 1
),
new_sig AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'u' AND year = 2023
    ) t WHERE rn = 1
)
SELECT
    CAST(COALESCE(n.id, o.id) AS INT) as report_id,
    CAST(COALESCE(n.service_request_id, o.service_request_id) AS INT) as service_request_id,
    CAST(COALESCE(n.year, o.year) AS INT) as year,
    COALESCE(n.report_type, o.report_type) as report_type,
    COALESCE(n.status, o.status) as status,
    COALESCE(n.op, o.op) as op,
    COALESCE(n.version, o.version) as version,
    COALESCE(n.created_at, o.created_at) as created_at,
    COALESCE(n.updated_at, o.updated_at) as updated_at,
    COALESCE(n.signed_at, o.signed_at) as signed_at,
    CAST(COALESCE(n.author_id, o.author_id) AS INT) as author_id,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1),
        get_json_object(o.data, '$.company_tin'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1)
    ) as company_tin,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1),
        get_json_object(o.data, '$.company_name'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1)
    ) as company_name,
    get_json_object(o.data, '$.certificate_number') as certificate_number,
    get_json_object(o.data, '$.oked') as oked,
    CAST(get_json_object(o.data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(o.data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(o.data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(o.data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(o.data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(o.data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(o.data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(o.data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(o.data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(o.data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(o.data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(o.data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(o.data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(o.data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(o.data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(o.data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(o.data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(o.data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(o.data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(o.data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(o.data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(o.data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(o.data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(o.data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(o.data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(o.data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM old_data o
LEFT JOIN new_sig n ON o.id = n.id;

INSERT INTO iceberg.silver.service_report_v2
WITH old_data AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'r' AND year = 2024 AND data IS NOT NULL AND data <> '__debezium_unavailable_value'
    ) t WHERE rn = 1
),
new_sig AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'u' AND year = 2024
    ) t WHERE rn = 1
)
SELECT
    CAST(COALESCE(n.id, o.id) AS INT) as report_id,
    CAST(COALESCE(n.service_request_id, o.service_request_id) AS INT) as service_request_id,
    CAST(COALESCE(n.year, o.year) AS INT) as year,
    COALESCE(n.report_type, o.report_type) as report_type,
    COALESCE(n.status, o.status) as status,
    COALESCE(n.op, o.op) as op,
    COALESCE(n.version, o.version) as version,
    COALESCE(n.created_at, o.created_at) as created_at,
    COALESCE(n.updated_at, o.updated_at) as updated_at,
    COALESCE(n.signed_at, o.signed_at) as signed_at,
    CAST(COALESCE(n.author_id, o.author_id) AS INT) as author_id,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1),
        get_json_object(o.data, '$.company_tin'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1)
    ) as company_tin,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1),
        get_json_object(o.data, '$.company_name'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1)
    ) as company_name,
    get_json_object(o.data, '$.certificate_number') as certificate_number,
    get_json_object(o.data, '$.oked') as oked,
    CAST(get_json_object(o.data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(o.data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(o.data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(o.data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(o.data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(o.data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(o.data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(o.data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(o.data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(o.data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(o.data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(o.data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(o.data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(o.data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(o.data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(o.data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(o.data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(o.data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(o.data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(o.data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(o.data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(o.data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(o.data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(o.data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(o.data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(o.data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM old_data o
LEFT JOIN new_sig n ON o.id = n.id;


INSERT INTO iceberg.silver.service_report_v2
WITH old_data AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'r' AND year = 2025 AND data IS NOT NULL AND data <> '__debezium_unavailable_value'
    ) t WHERE rn = 1
),
new_sig AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'u' AND year = 2025
    ) t WHERE rn = 1
)
SELECT
    CAST(COALESCE(n.id, o.id) AS INT) as report_id,
    CAST(COALESCE(n.service_request_id, o.service_request_id) AS INT) as service_request_id,
    CAST(COALESCE(n.year, o.year) AS INT) as year,
    COALESCE(n.report_type, o.report_type) as report_type,
    COALESCE(n.status, o.status) as status,
    COALESCE(n.op, o.op) as op,
    COALESCE(n.version, o.version) as version,
    COALESCE(n.created_at, o.created_at) as created_at,
    COALESCE(n.updated_at, o.updated_at) as updated_at,
    COALESCE(n.signed_at, o.signed_at) as signed_at,
    CAST(COALESCE(n.author_id, o.author_id) AS INT) as author_id,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1),
        get_json_object(o.data, '$.company_tin'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1)
    ) as company_tin,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1),
        get_json_object(o.data, '$.company_name'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1)
    ) as company_name,
    get_json_object(o.data, '$.certificate_number') as certificate_number,
    get_json_object(o.data, '$.oked') as oked,
    CAST(get_json_object(o.data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(o.data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(o.data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(o.data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(o.data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(o.data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(o.data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(o.data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(o.data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(o.data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(o.data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(o.data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(o.data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(o.data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(o.data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(o.data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(o.data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(o.data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(o.data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(o.data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(o.data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(o.data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(o.data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(o.data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(o.data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(o.data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM old_data o
LEFT JOIN new_sig n ON o.id = n.id;

INSERT INTO iceberg.silver.service_report_v2
WITH old_data AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'r' AND year = 2026 AND data IS NOT NULL AND data <> '__debezium_unavailable_value'
    ) t WHERE rn = 1
),
new_sig AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC) as rn
        FROM iceberg.bronze.service_report_cdc
        WHERE op = 'u' AND year = 2026
    ) t WHERE rn = 1
)
SELECT
    CAST(COALESCE(n.id, o.id) AS INT) as report_id,
    CAST(COALESCE(n.service_request_id, o.service_request_id) AS INT) as service_request_id,
    CAST(COALESCE(n.year, o.year) AS INT) as year,
    COALESCE(n.report_type, o.report_type) as report_type,
    COALESCE(n.status, o.status) as status,
    COALESCE(n.op, o.op) as op,
    COALESCE(n.version, o.version) as version,
    COALESCE(n.created_at, o.created_at) as created_at,
    COALESCE(n.updated_at, o.updated_at) as updated_at,
    COALESCE(n.signed_at, o.signed_at) as signed_at,
    CAST(COALESCE(n.author_id, o.author_id) AS INT) as author_id,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1),
        get_json_object(o.data, '$.company_tin'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<tin>([0-9]+)</tin>', 1)
    ) as company_tin,
    COALESCE(
        regexp_extract(get_json_object(n.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1),
        get_json_object(o.data, '$.company_name'),
        regexp_extract(get_json_object(o.signature, '$.signed_xml'), '<company>.*<name>([^<]+)</name>', 1)
    ) as company_name,
    get_json_object(o.data, '$.certificate_number') as certificate_number,
    get_json_object(o.data, '$.oked') as oked,
    CAST(get_json_object(o.data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(o.data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(o.data, '$.gph_count') AS BIGINT) as gph_count,
    CAST(get_json_object(o.data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(o.data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(o.data, '$.income_total_current_quarter') AS BIGINT) as income_total_current_quarter,
    CAST(get_json_object(o.data, '$.income_total_previous_quarter') AS BIGINT) as income_total_previous_quarter,
    CAST(get_json_object(o.data, '$.investments_total_current_quarter') AS BIGINT) as investments_total_current_quarter,
    CAST(get_json_object(o.data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(o.data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,
    CAST(get_json_object(o.data, '$.main_tangible_capital_investments') AS BIGINT) as main_tangible_capital_investments,
    CAST(get_json_object(o.data, '$.main_intangible_capital_investments') AS BIGINT) as main_intangible_capital_investments,
    CAST(get_json_object(o.data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(o.data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(o.data, '$.government_support_measures') AS BIGINT) as government_support_measures,
    CAST(get_json_object(o.data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,
    CAST(get_json_object(o.data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(o.data, '$.investor_country_company') as investor_country_company,
    CAST(get_json_object(o.data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(o.data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(o.data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(o.data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(o.data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,
    CAST(get_json_object(o.data, '$.total_tax_saved') AS BIGINT) as total_tax_saved,
    CAST(get_json_object(o.data, '$.collection_amount') AS BIGINT) as collection_amount,
    get_json_object(o.data, '$.activity_fields') as activity_fields,
    current_timestamp() as etl_loaded_at
FROM old_data o
LEFT JOIN new_sig n ON o.id = n.id;

SELECT year, COUNT(*) as cnt FROM iceberg.silver.service_report_v2 GROUP BY year ORDER BY year;

SELECT
    CASE WHEN company_tin LIKE '777777%' THEN 'MASKED' ELSE 'REAL' END as tin_status,
    COUNT(*) as cnt
FROM iceberg.silver.service_report_v2
GROUP BY CASE WHEN company_tin LIKE '777777%' THEN 'MASKED' ELSE 'REAL' END;
