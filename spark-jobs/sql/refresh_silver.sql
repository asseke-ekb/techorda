-- ============================================================================
-- Пересборка Silver слоя из Bronze
-- Запускать через Spark SQL / DBeaver (Hive JDBC)
-- ============================================================================

-- 1. Основная таблица service_report
INSERT OVERWRITE iceberg.silver.service_report
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

    -- Компания
    get_json_object(data, '$.company_tin') as company_tin,
    get_json_object(data, '$.company_name') as company_name,
    get_json_object(data, '$.certificate_number') as certificate_number,
    get_json_object(data, '$.oked') as oked,

    -- Сотрудники
    CAST(get_json_object(data, '$.residents_count') AS BIGINT) as residents_count,
    CAST(get_json_object(data, '$.nonresidents_count') AS BIGINT) as nonresidents_count,
    CAST(get_json_object(data, '$.gph_count') AS BIGINT) as gph_count,

    -- Доходы
    CAST(get_json_object(data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(data, '$.income_international') AS BIGINT) as income_international,

    -- Инвестиции
    CAST(get_json_object(data, '$.finance_source_increase_authorized_capital') AS BIGINT) as finance_source_increase_authorized_capital,
    CAST(get_json_object(data, '$.main_capital_investments') AS BIGINT) as main_capital_investments,

    -- Займы
    CAST(get_json_object(data, '$.finance_source_loan') AS BIGINT) as finance_source_loan,
    CAST(get_json_object(data, '$.finance_source_loan_foreign') AS BIGINT) as finance_source_loan_foreign,
    CAST(get_json_object(data, '$.government_support_measures') AS BIGINT) as finance_source_government,
    CAST(get_json_object(data, '$.finance_source_investment') AS BIGINT) as finance_source_investment,

    -- Инвесторы
    CAST(get_json_object(data, '$.investor_amount') AS BIGINT) as investor_amount,
    get_json_object(data, '$.investor_country_company') as investor_country_company,

    -- Налоговые льготы
    CAST(get_json_object(data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    CAST(get_json_object(data, '$.tax_incentives_kpn') AS BIGINT) as tax_incentives_kpn,
    CAST(get_json_object(data, '$.tax_incentives_nds') AS BIGINT) as tax_incentives_nds,
    CAST(get_json_object(data, '$.tax_incentives_ipn') AS BIGINT) as tax_incentives_ipn,
    CAST(get_json_object(data, '$.tax_incentives_sn') AS BIGINT) as tax_incentives_sn,

    current_timestamp() as etl_loaded_at

FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY service_request_id, report_type, year
            ORDER BY updated_at DESC
        ) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE year >= 2019
      AND op != 'd'
) t
WHERE rn = 1;
