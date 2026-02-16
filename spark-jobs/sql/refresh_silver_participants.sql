-- ============================================================================
-- Silver: Участники технопарка (service_servicerequest_cdc -> silver.techpark_participants)
-- Данные для связи отчетов с информацией о компаниях
-- ============================================================================

-- 1. Удаляем старую таблицу
DROP TABLE IF EXISTS iceberg.silver.techpark_participants;

-- 2. Создаём таблицу
CREATE TABLE iceberg.silver.techpark_participants (
    -- ID
    service_request_id INT COMMENT 'ID заявки (для связи с отчетами)',
    company_id INT COMMENT 'ID компании (для связи с account_company)',

    -- Статус участника
    bp_status STRING COMMENT 'Статус: registered, deactivated',

    -- Информация о свидетельстве
    certificate_number STRING COMMENT 'Номер свидетельства участника',
    certificate_issue_date DATE COMMENT 'Дата выдачи свидетельства',
    certificate_end_date DATE COMMENT 'Срок действия свидетельства',

    -- Компания
    company_name STRING COMMENT 'Название компании',
    company_tin STRING COMMENT 'БИН компании',

    -- Деактивация (если есть)
    deactivation_reason STRING COMMENT 'Причина расторжения',
    deactivation_date DATE COMMENT 'Дата расторжения',

    -- Поиск
    search_field STRING COMMENT 'Поисковое поле (ФИО, ИИН, email, БИН, название)',

    -- ETL
    etl_loaded_at TIMESTAMP COMMENT 'Время загрузки'
)
USING iceberg
COMMENT 'Участники технопарка - registered и deactivated';

-- 3. Заливаем данные
INSERT INTO iceberg.silver.techpark_participants
SELECT
    CAST(id AS INT) as service_request_id,
    CAST(company_id AS INT) as company_id,
    bp_status,

    -- Свидетельство
    get_json_object(data, '$.activation_info.certificate_number') as certificate_number,
    CAST(get_json_object(data, '$.activation_info.issue_date') AS DATE) as certificate_issue_date,
    CAST(get_json_object(data, '$.activation_info.end_date') AS DATE) as certificate_end_date,

    -- Компания
    get_json_object(data, '$.protocol.info.name') as company_name,
    -- БИН извлекаем из search_field (4-й элемент: ФИО***ИИН***email***БИН***название...)
    substring_index(substring_index(search_field, '***', 4), '***', -1) as company_tin,

    -- Деактивация
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
