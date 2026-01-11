-- Silver таблица: Детализация экспорта по странам
-- Развёрнутые данные из массива income_international_current_quarter

DROP TABLE IF EXISTS iceberg.silver.export_details;

CREATE TABLE iceberg.silver.export_details (
    -- Связь с основной таблицей
    report_id INT COMMENT 'ID отчёта (FK -> service_report_v2.report_id)',
    service_request_id INT COMMENT 'ID заявки',
    year INT COMMENT 'Год отчёта',
    report_type STRING COMMENT 'Тип отчёта: quarter1, quarter2, quarter3, quarter4',

    -- Компания
    company_tin STRING COMMENT 'БИН компании',
    company_name STRING COMMENT 'Название компании',
    certificate_number STRING COMMENT 'Номер свидетельства',

    -- Детали экспорта
    country STRING COMMENT 'Страна экспорта',
    export_amount BIGINT COMMENT 'Сумма экспорта в страну',
    quarter_type STRING COMMENT 'Период: current или previous',

    -- ETL
    etl_loaded_at TIMESTAMP COMMENT 'Время загрузки'
)
USING iceberg
PARTITIONED BY (year, quarter_type)
COMMENT 'Детализация экспорта по странам для каждого отчёта';

-- Загрузка данных: разворачиваем JSON массив
INSERT OVERWRITE iceberg.silver.export_details
SELECT
    b.id AS report_id,
    b.service_request_id,
    b.year,
    b.report_type,
    get_json_object(b.data, '$.company_tin') AS company_tin,
    get_json_object(b.data, '$.company_name') AS company_name,
    get_json_object(b.data, '$.certificate_number') AS certificate_number,

    -- Текущий квартал
    get_json_object(export_item, '$.country') AS country,
    CAST(get_json_object(export_item, '$.current_sum') AS BIGINT) AS export_amount,
    'current' AS quarter_type,

    CURRENT_TIMESTAMP() AS etl_loaded_at
FROM iceberg.bronze.service_report_cdc b
LATERAL VIEW explode(
    from_json(
        get_json_object(b.data, '$.income_international_current_quarter'),
        'array<string>'
    )
) AS export_item
WHERE b.op != 'd'
  AND b.year >= 2019
  AND get_json_object(b.data, '$.income_international_current_quarter') IS NOT NULL
  AND get_json_object(b.data, '$.income_international_current_quarter') != '[]'
  AND get_json_object(export_item, '$.country') IS NOT NULL

UNION ALL

-- Предыдущий квартал
SELECT
    b.id AS report_id,
    b.service_request_id,
    b.year,
    b.report_type,
    get_json_object(b.data, '$.company_tin') AS company_tin,
    get_json_object(b.data, '$.company_name') AS company_name,
    get_json_object(b.data, '$.certificate_number') AS certificate_number,

    get_json_object(export_item, '$.country') AS country,
    CAST(get_json_object(export_item, '$.sum') AS BIGINT) AS export_amount,
    'previous' AS quarter_type,

    CURRENT_TIMESTAMP() AS etl_loaded_at
FROM iceberg.bronze.service_report_cdc b
LATERAL VIEW explode(
    from_json(
        get_json_object(b.data, '$.income_international_previous_quarter'),
        'array<string>'
    )
) AS export_item
WHERE b.op != 'd'
  AND b.year >= 2019
  AND get_json_object(b.data, '$.income_international_previous_quarter') IS NOT NULL
  AND get_json_object(b.data, '$.income_international_previous_quarter') != '[]'
  AND get_json_object(export_item, '$.country') IS NOT NULL;
