-- ============================================================================
-- Silver: Компании (account_company_cdc -> silver.companies)
-- Справочник компаний-участников
-- ============================================================================

-- 1. Проверить наличие таблицы в Bronze
-- SHOW TABLES IN iceberg.bronze LIKE '*company*';
-- DESCRIBE iceberg.bronze.account_company_cdc;

-- 2. Удаляем старую таблицу
DROP TABLE IF EXISTS iceberg.silver.companies;

-- 3. Создаём таблицу
CREATE TABLE iceberg.silver.companies (
    company_id INT COMMENT 'ID компании',
    name STRING COMMENT 'Полное название компании',
    short_name STRING COMMENT 'Краткое название',
    tin STRING COMMENT 'БИН компании',
    etl_loaded_at TIMESTAMP COMMENT 'Время загрузки'
)
USING iceberg
COMMENT 'Справочник компаний';

-- 4. Заливаем данные (с дедупликацией по последней версии)
INSERT INTO iceberg.silver.companies
SELECT
    CAST(id AS INT) as company_id,
    name,
    short_name,
    tin,
    current_timestamp() as etl_loaded_at
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY updated_at DESC
        ) as rn
    FROM iceberg.bronze.account_company_cdc
    WHERE op != 'd'
) t
WHERE rn = 1;

-- 5. Проверка
SELECT COUNT(*) as total_companies FROM iceberg.silver.companies;
