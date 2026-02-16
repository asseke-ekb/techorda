
DROP TABLE IF EXISTS iceberg.silver.techpark_participants;

CREATE TABLE iceberg.silver.techpark_participants (
    service_request_id INT COMMENT 'ID заявки (для связи с отчетами)',
    company_id INT COMMENT 'ID компании (для связи с account_company)',

    bp_status STRING COMMENT 'Статус: registered, deactivated',

    certificate_number STRING COMMENT 'Номер свидетельства участника',
    certificate_issue_date DATE COMMENT 'Дата выдачи свидетельства',
    certificate_end_date DATE COMMENT 'Срок действия свидетельства',

    company_name STRING COMMENT 'Название компании',
    company_tin STRING COMMENT 'БИН компании',

    deactivation_reason STRING COMMENT 'Причина расторжения',
    deactivation_date DATE COMMENT 'Дата расторжения',

    search_field STRING COMMENT 'Поисковое поле (ФИО, ИИН, email, БИН, название)',

    etl_loaded_at TIMESTAMP COMMENT 'Время загрузки'
)
USING iceberg
COMMENT 'Участники технопарка - registered и deactivated';

INSERT INTO iceberg.silver.techpark_participants
SELECT
    CAST(id AS INT) as service_request_id,
    CAST(company_id AS INT) as company_id,
    bp_status,

    get_json_object(data, '$.activation_info.certificate_number') as certificate_number,
    CAST(get_json_object(data, '$.activation_info.issue_date') AS DATE) as certificate_issue_date,
    CAST(get_json_object(data, '$.activation_info.end_date') AS DATE) as certificate_end_date,

    get_json_object(data, '$.protocol.info.name') as company_name,
    -- БИН извлекаем из search_field (4-й элемент: ФИО***ИИН***email***БИН***название...)
    substring_index(substring_index(search_field, '***', 4), '***', -1) as company_tin,

    get_json_object(data, '$.deactivation_info.reason') as deactivation_reason,
    CAST(get_json_object(data, '$.deactivation_info.exit_date') AS DATE) as deactivation_date,

    search_field,

    current_timestamp() as etl_loaded_at

FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY updated_at DESC
        ) as rn
    FROM iceberg.bronze.service_servicerequest_cdc
    WHERE service_id = 'techpark'
      AND bp_status IN ('deactivated', 'registered')
      AND op != 'd'
) t
WHERE rn = 1;
