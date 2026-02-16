-- ============================================================================
-- Gold витрина: Экспорт по странам (export_by_country)
-- Разворачивает JSON массив income_international_current_quarter
-- ============================================================================

DROP TABLE IF EXISTS iceberg.gold.export_by_country;

CREATE TABLE iceberg.gold.export_by_country (
    year INT,
    report_type STRING,
    certificate_number STRING,
    bin STRING,
    company_name STRING,
    country STRING COMMENT 'Страна экспорта',
    export_amount BIGINT COMMENT 'Сумма экспорта'
)
USING iceberg
PARTITIONED BY (year);

-- Заполняем из Bronze с извлечением company_tin/name из signature (2021+) или data (2019-2020)
-- JOIN на participants оставлен как fallback для certificate_number
INSERT INTO iceberg.gold.export_by_country
SELECT
    CAST(b.year AS INT) as year,
    b.report_type,
    COALESCE(get_json_object(b.data, '$.certificate_number'), p.certificate_number) as certificate_number,
    -- BIN: приоритет data → signature → participants
    COALESCE(
        get_json_object(b.data, '$.company_tin'),
        regexp_extract(get_json_object(b.signature, '$.signed_xml'), '<company>.*?<tin>([0-9]+)</tin>.*?</company>', 1),
        p.company_tin
    ) as bin,
    -- Company name: приоритет data → signature → participants
    COALESCE(
        get_json_object(b.data, '$.company_name'),
        regexp_extract(get_json_object(b.signature, '$.signed_xml'), '<company>.*?<name>([^<]+)</name>.*?</company>', 1),
        p.company_name
    ) as company_name,
    get_json_object(export_item, '$.country') as country,
    CAST(get_json_object(export_item, '$.current_sum') AS BIGINT) as export_amount
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY service_request_id, report_type, year
            ORDER BY updated_at DESC
        ) as rn
    FROM iceberg.bronze.service_report_cdc
    WHERE year >= 2019
      AND op != 'd'
      AND status = 'signed'
      AND get_json_object(data, '$.income_international_current_quarter') LIKE '[%'
) b
LEFT JOIN iceberg.silver.techpark_participants p
    ON b.service_request_id = p.service_request_id
LATERAL VIEW explode(
    from_json(
        get_json_object(b.data, '$.income_international_current_quarter'),
        'array<string>'
    )
) AS export_item
WHERE b.rn = 1
  AND get_json_object(export_item, '$.country') IS NOT NULL
  AND get_json_object(export_item, '$.country') != ''
  AND get_json_object(export_item, '$.country') != 'None';


-- Проверка
SELECT year, COUNT(*) as cnt, COUNT(DISTINCT country) as countries
FROM iceberg.gold.export_by_country
GROUP BY year
ORDER BY year;
