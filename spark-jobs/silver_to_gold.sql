-- ============================================================================
-- ETL: Silver → Gold
-- Формирование витрины iceberg.gold.reports_summary
-- ============================================================================
-- Описание:
--   Агрегирует данные из Silver таблицы по (service_request_id, year)
--   Берёт только подписанные квартальные отчёты (quarter1-4)
--   Суммирует все метрики за год для каждой заявки
-- ============================================================================

-- Шаг 1: Создать таблицу Gold (если не существует)
CREATE TABLE IF NOT EXISTS iceberg.gold.reports_summary (
    service_request_id INT COMMENT 'ID заявки',
    year INT COMMENT 'Год',
    signed_quarters INT COMMENT 'Количество подписанных кварталов',

    -- Сотрудники
    residents_count BIGINT COMMENT 'Всего резидентов за год',
    nonresidents_count BIGINT COMMENT 'Всего нерезидентов за год',

    -- Доходы
    income_total BIGINT COMMENT 'Общий доход за год',
    income_international BIGINT COMMENT 'Международный доход (экспорт) за год',

    -- Финансирование / инвестиции
    finance_source_increase_authorized_capital BIGINT COMMENT 'Увеличение уставного капитала',
    main_capital_investments BIGINT COMMENT 'Основные капитальные инвестиции',
    finance_source_loan BIGINT COMMENT 'Займы',
    finance_source_loan_foreign BIGINT COMMENT 'Иностранные займы',
    finance_source_government BIGINT COMMENT 'Господдержка',
    finance_source_investment BIGINT COMMENT 'Инвестиционное финансирование',
    investor_amount BIGINT COMMENT 'Сумма от инвесторов',

    -- Налоги / льготы
    taxes_saved BIGINT COMMENT 'Сэкономлено на налогах',
    tax_incentives_kpn BIGINT COMMENT 'Льготы КПН',
    tax_incentives_nds BIGINT COMMENT 'Льготы НДС',
    tax_incentives_ipn BIGINT COMMENT 'Льготы ИПН',
    tax_incentives_sn BIGINT COMMENT 'Льготы СН',

    -- ETL метаданные
    etl_loaded_at TIMESTAMP COMMENT 'Дата загрузки ETL'
)
USING iceberg
COMMENT 'Годовой свод отчётов по заявкам (только signed quarters)'
;

-- ============================================================================
-- Шаг 2: Очистить существующие данные (опционально)
-- ============================================================================
-- Раскомментируйте, если нужна полная перезапись:
-- TRUNCATE TABLE iceberg.gold.reports_summary;

-- ============================================================================
-- Шаг 3: Вставить агрегированные данные
-- ============================================================================

INSERT OVERWRITE TABLE iceberg.gold.reports_summary
SELECT
    service_request_id,
    year,

    -- Количество подписанных кварталов
    COUNT(*) AS signed_quarters,

    -- Сотрудники (сумма за год)
    SUM(COALESCE(residents_count, 0))    AS residents_count,
    SUM(COALESCE(nonresidents_count, 0)) AS nonresidents_count,

    -- Доходы (сумма за год)
    SUM(COALESCE(income_total, 0))         AS income_total,
    SUM(COALESCE(income_international, 0)) AS income_international,

    -- Финансирование / инвестиции (сумма за год)
    SUM(COALESCE(finance_source_increase_authorized_capital, 0))
        AS finance_source_increase_authorized_capital,
    SUM(COALESCE(main_capital_investments, 0))
        AS main_capital_investments,
    SUM(COALESCE(finance_source_loan, 0))
        AS finance_source_loan,
    SUM(COALESCE(finance_source_loan_foreign, 0))
        AS finance_source_loan_foreign,
    SUM(COALESCE(finance_source_government, 0))
        AS finance_source_government,
    SUM(COALESCE(finance_source_investment, 0))
        AS finance_source_investment,
    SUM(COALESCE(investor_amount, 0))
        AS investor_amount,

    -- Налоги / льготы (сумма за год)
    -- Примечание: используем tax_incentives как taxes_saved
    -- Если в Silver есть total_tax_saved, можно использовать COALESCE(total_tax_saved, tax_incentives)
    SUM(COALESCE(tax_incentives, 0))     AS taxes_saved,
    SUM(COALESCE(tax_incentives_kpn, 0)) AS tax_incentives_kpn,
    SUM(COALESCE(tax_incentives_nds, 0)) AS tax_incentives_nds,
    SUM(COALESCE(tax_incentives_ipn, 0)) AS tax_incentives_ipn,
    SUM(COALESCE(tax_incentives_sn, 0))  AS tax_incentives_sn,

    -- ETL метаданные
    CURRENT_TIMESTAMP() AS etl_loaded_at

FROM iceberg.silver.service_report

WHERE
    -- Только подписанные отчёты
    status = 'signed'

    -- Только квартальные отчёты (НЕ yearly, чтобы избежать задвоения)
    AND report_type IN ('quarter1', 'quarter2', 'quarter3', 'quarter4')

    -- Опционально: фильтр по году (раскомментируйте при необходимости)
    -- AND year = 2024

GROUP BY service_request_id, year

ORDER BY year, service_request_id
;

-- ============================================================================
-- Шаг 4: Проверка результатов
-- ============================================================================

-- Посмотреть первые 10 записей
SELECT
    service_request_id,
    year,
    signed_quarters,
    income_total,
    income_international,
    taxes_saved,
    etl_loaded_at
FROM iceberg.gold.reports_summary
ORDER BY year DESC, service_request_id
LIMIT 10;

-- Статистика по годам
SELECT
    year,
    COUNT(*) AS companies_count,
    SUM(signed_quarters) AS total_signed_quarters,
    SUM(income_total) AS total_income,
    SUM(income_international) AS total_export,
    SUM(taxes_saved) AS total_taxes_saved
FROM iceberg.gold.reports_summary
GROUP BY year
ORDER BY year DESC;

-- ============================================================================
-- Опциональные запросы для инкрементальной загрузки
-- ============================================================================

-- Вариант 1: Загрузка только за конкретный год (инкрементально)
/*
INSERT OVERWRITE TABLE iceberg.gold.reports_summary
-- Добавьте PARTITION (year = 2024) если таблица партиционирована
SELECT
    service_request_id,
    year,
    COUNT(*) AS signed_quarters,
    SUM(COALESCE(residents_count, 0)) AS residents_count,
    SUM(COALESCE(nonresidents_count, 0)) AS nonresidents_count,
    SUM(COALESCE(income_total, 0)) AS income_total,
    SUM(COALESCE(income_international, 0)) AS income_international,
    SUM(COALESCE(finance_source_increase_authorized_capital, 0)) AS finance_source_increase_authorized_capital,
    SUM(COALESCE(main_capital_investments, 0)) AS main_capital_investments,
    SUM(COALESCE(finance_source_loan, 0)) AS finance_source_loan,
    SUM(COALESCE(finance_source_loan_foreign, 0)) AS finance_source_loan_foreign,
    SUM(COALESCE(finance_source_government, 0)) AS finance_source_government,
    SUM(COALESCE(finance_source_investment, 0)) AS finance_source_investment,
    SUM(COALESCE(investor_amount, 0)) AS investor_amount,
    SUM(COALESCE(tax_incentives, 0)) AS taxes_saved,
    SUM(COALESCE(tax_incentives_kpn, 0)) AS tax_incentives_kpn,
    SUM(COALESCE(tax_incentives_nds, 0)) AS tax_incentives_nds,
    SUM(COALESCE(tax_incentives_ipn, 0)) AS tax_incentives_ipn,
    SUM(COALESCE(tax_incentives_sn, 0)) AS tax_incentives_sn,
    CURRENT_TIMESTAMP() AS etl_loaded_at
FROM iceberg.silver.service_report
WHERE
    status = 'signed'
    AND report_type IN ('quarter1', 'quarter2', 'quarter3', 'quarter4')
    AND year = 2024  -- Конкретный год
GROUP BY service_request_id, year
ORDER BY year, service_request_id;
*/

-- Вариант 2: MERGE для upsert (если поддерживается Iceberg)
/*
MERGE INTO iceberg.gold.reports_summary AS target
USING (
    SELECT
        service_request_id,
        year,
        COUNT(*) AS signed_quarters,
        SUM(COALESCE(residents_count, 0)) AS residents_count,
        SUM(COALESCE(nonresidents_count, 0)) AS nonresidents_count,
        SUM(COALESCE(income_total, 0)) AS income_total,
        SUM(COALESCE(income_international, 0)) AS income_international,
        SUM(COALESCE(finance_source_increase_authorized_capital, 0)) AS finance_source_increase_authorized_capital,
        SUM(COALESCE(main_capital_investments, 0)) AS main_capital_investments,
        SUM(COALESCE(finance_source_loan, 0)) AS finance_source_loan,
        SUM(COALESCE(finance_source_loan_foreign, 0)) AS finance_source_loan_foreign,
        SUM(COALESCE(finance_source_government, 0)) AS finance_source_government,
        SUM(COALESCE(finance_source_investment, 0)) AS finance_source_investment,
        SUM(COALESCE(investor_amount, 0)) AS investor_amount,
        SUM(COALESCE(tax_incentives, 0)) AS taxes_saved,
        SUM(COALESCE(tax_incentives_kpn, 0)) AS tax_incentives_kpn,
        SUM(COALESCE(tax_incentives_nds, 0)) AS tax_incentives_nds,
        SUM(COALESCE(tax_incentives_ipn, 0)) AS tax_incentives_ipn,
        SUM(COALESCE(tax_incentives_sn, 0)) AS tax_incentives_sn,
        CURRENT_TIMESTAMP() AS etl_loaded_at
    FROM iceberg.silver.service_report
    WHERE
        status = 'signed'
        AND report_type IN ('quarter1', 'quarter2', 'quarter3', 'quarter4')
        AND year >= 2024  -- Только новые данные
    GROUP BY service_request_id, year
) AS source
ON target.service_request_id = source.service_request_id
   AND target.year = source.year
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
*/
