-- ============================================================
-- Создание gold-слоя таблиц для витрины отчётов
-- Выполнить через DBeaver или spark-sql
-- ============================================================

-- 1. Создание схемы gold (если не существует)
CREATE DATABASE IF NOT EXISTS iceberg.gold;

-- 2. Таблица витрины отчётов (детальная)
CREATE TABLE IF NOT EXISTS iceberg.gold.reports_vitrina (
    report_id INT COMMENT 'ID отчёта',
    year INT COMMENT 'Год отчёта',
    report_type STRING COMMENT 'Тип: quarter1, quarter2, quarter3, quarter4, yearly',
    status STRING COMMENT 'Статус отчёта',
    signed_at TIMESTAMP COMMENT 'Дата подписания',
    version STRING COMMENT 'Версия отчёта',
    created_at TIMESTAMP COMMENT 'Дата создания',
    updated_at TIMESTAMP COMMENT 'Дата обновления',
    service_request_id INT COMMENT 'ID заявки',
    author_id INT COMMENT 'ID автора',

    -- Компания
    company_name STRING COMMENT 'Название компании',
    company_tin STRING COMMENT 'БИН/ИИН компании',
    certificate_number INT COMMENT 'Номер сертификата',

    -- Сотрудники
    residents_count INT COMMENT 'Количество резидентов',
    nonresidents_count INT COMMENT 'Количество нерезидентов',
    gph_count INT COMMENT 'Количество по ГПХ',

    -- Доходы
    income_total BIGINT COMMENT 'Общий доход',
    income_international BIGINT COMMENT 'Международный доход',
    income_total_previous_quarter BIGINT COMMENT 'Доход за прошлый квартал',
    income_total_current_quarter BIGINT COMMENT 'Доход за текущий квартал',

    -- Инвестиции
    investments_total_current_quarter BIGINT COMMENT 'Инвестиции за текущий квартал',
    main_capital_investments BIGINT COMMENT 'Основные капитальные инвестиции',
    main_tangible_capital_investments BIGINT COMMENT 'Материальные капитальные инвестиции',
    main_intangible_capital_investments BIGINT COMMENT 'Нематериальные капитальные инвестиции',
    finance_source_increase_authorized_capital BIGINT COMMENT 'Увеличение уставного капитала',

    -- Инвесторы
    investor_amount BIGINT COMMENT 'Сумма инвестиций',
    investor_country_company STRING COMMENT 'Страна инвестора',

    -- Займы
    finance_source_loan BIGINT COMMENT 'Займы',
    finance_source_loan_foreign BIGINT COMMENT 'Иностранные займы',

    -- Господдержка
    finance_source_government BIGINT COMMENT 'Господдержка',
    finance_source_investment BIGINT COMMENT 'Инвестиционное финансирование',

    -- Налоговые льготы
    tax_incentives BIGINT COMMENT 'Налоговые льготы (всего)',
    tax_incentives_kpn BIGINT COMMENT 'Льготы КПН',
    tax_incentives_nds BIGINT COMMENT 'Льготы НДС',
    tax_incentives_ipn BIGINT COMMENT 'Льготы ИПН',
    tax_incentives_sn BIGINT COMMENT 'Льготы СН',
    collection_amount BIGINT COMMENT 'Сумма сборов',

    -- ОКЭД и исполнитель
    oked STRING COMMENT 'Код ОКЭД',
    executor_fullname STRING COMMENT 'ФИО исполнителя',
    executor_phone STRING COMMENT 'Телефон исполнителя',

    -- Флаги
    has_nonresidents STRING COMMENT 'Есть нерезиденты',
    has_borrowed_funds STRING COMMENT 'Есть заёмные средства',
    has_raised_investors_funds STRING COMMENT 'Есть привлечённые инвестиции',

    -- ETL метаданные
    etl_loaded_at TIMESTAMP COMMENT 'Дата загрузки ETL'
)
USING iceberg
COMMENT 'Витрина отчётов участников Technopark (детальная)'
TBLPROPERTIES (
    'format-version' = '2'
);

-- 3. Таблица агрегированной сводки
CREATE TABLE IF NOT EXISTS iceberg.gold.reports_summary (
    year INT COMMENT 'Год',
    report_type STRING COMMENT 'Тип отчёта',
    period STRING COMMENT 'Читаемый период (Q1 2024, Year 2024)',
    reports_count BIGINT COMMENT 'Количество отчётов',
    total_residents BIGINT COMMENT 'Всего резидентов',
    total_nonresidents BIGINT COMMENT 'Всего нерезидентов',
    total_gph BIGINT COMMENT 'Всего по ГПХ',
    total_income BIGINT COMMENT 'Общий доход',
    total_income_international BIGINT COMMENT 'Международный доход',
    total_investments BIGINT COMMENT 'Всего инвестиций',
    total_tax_incentives BIGINT COMMENT 'Всего налоговых льгот',
    total_investor_amount BIGINT COMMENT 'Всего от инвесторов',
    total_loans BIGINT COMMENT 'Всего займов',
    total_government_support BIGINT COMMENT 'Всего господдержки',
    etl_loaded_at TIMESTAMP COMMENT 'Дата загрузки ETL'
)
USING iceberg
COMMENT 'Агрегированная сводка отчётов по периодам'
TBLPROPERTIES (
    'format-version' = '2'
);

-- ============================================================
-- Проверка созданных таблиц
-- ============================================================

SHOW TABLES IN iceberg.gold;

DESCRIBE iceberg.gold.reports_vitrina;

DESCRIBE iceberg.gold.reports_summary;
