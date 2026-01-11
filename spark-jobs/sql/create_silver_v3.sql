-- Создание Silver таблицы service_report_v3 с массивами стран экспорта

DROP TABLE IF EXISTS iceberg.silver.service_report_v3;

CREATE TABLE iceberg.silver.service_report_v3 (
    -- Системные поля
    report_id INT COMMENT 'ID отчёта',
    service_request_id INT COMMENT 'ID заявки',
    year INT COMMENT 'Год отчёта',
    report_type STRING COMMENT 'Тип отчёта: quarter1, quarter2, quarter3, quarter4, yearly',
    status STRING COMMENT 'Статус: signed, draft, rejected',
    op STRING COMMENT 'CDC операция: c, u, d, r',
    version STRING COMMENT 'Версия формы отчёта: v5, v7, v8',
    created_at TIMESTAMP COMMENT 'Дата создания',
    updated_at TIMESTAMP COMMENT 'Дата обновления',
    signed_at TIMESTAMP COMMENT 'Дата подписания',
    author_id INT COMMENT 'ID автора',

    -- Компания
    company_tin STRING COMMENT 'БИН компании',
    company_name STRING COMMENT 'Название компании',
    certificate_number STRING COMMENT 'Номер свидетельства участника',
    oked STRING COMMENT 'Код ОКЭД',

    -- Сотрудники
    residents_count BIGINT COMMENT 'Количество работников-резидентов',
    nonresidents_count BIGINT COMMENT 'Количество работников-нерезидентов',
    gph_count BIGINT COMMENT 'Количество работников по ГПХ',

    -- Доходы
    income_total BIGINT COMMENT 'Общий доход (накопительно)',
    income_international BIGINT COMMENT 'Доход от экспорта (накопительно)',
    income_total_current_quarter BIGINT COMMENT 'Доход за текущий квартал',
    income_total_previous_quarter BIGINT COMMENT 'Доход за предыдущий квартал',

    -- Экспорт по странам (JSON массивы)
    income_international_current_quarter STRING COMMENT 'Массив стран экспорта текущего квартала: [{"country":"...", "current_sum":123}]',
    income_international_previous_quarter STRING COMMENT 'Массив стран экспорта предыдущего квартала: [{"country":"...", "sum":123}]',

    -- Инвестиции
    investments_total_current_quarter BIGINT COMMENT 'Всего инвестиций за текущий квартал',
    finance_source_increase_authorized_capital BIGINT COMMENT 'Вклад в уставной капитал',
    main_capital_investments BIGINT COMMENT 'Инвестиции в основной капитал',
    main_tangible_capital_investments BIGINT COMMENT 'Инвестиции в материальный капитал',
    main_intangible_capital_investments BIGINT COMMENT 'Инвестиции в нематериальный капитал',

    -- Займы и финансирование
    finance_source_loan BIGINT COMMENT 'Заемные средства',
    finance_source_loan_foreign BIGINT COMMENT 'Иностранные займы',
    government_support_measures BIGINT COMMENT 'Меры государственной поддержки',
    finance_source_investment BIGINT COMMENT 'Привлеченные инвестиции',

    -- Инвесторы
    investor_amount BIGINT COMMENT 'Сумма от инвесторов',
    investor_country_company STRING COMMENT 'Страна/компания инвестора',

    -- Налоговые льготы
    tax_incentives BIGINT COMMENT 'Налоговые льготы (всего)',
    tax_incentives_kpn BIGINT COMMENT 'Льготы КПН',
    tax_incentives_nds BIGINT COMMENT 'Льготы НДС',
    tax_incentives_ipn BIGINT COMMENT 'Льготы ИПН',
    tax_incentives_sn BIGINT COMMENT 'Льготы СН',
    total_tax_saved BIGINT COMMENT 'Всего сэкономлено налогов',

    -- Прочее
    collection_amount BIGINT COMMENT 'Сумма сбора 1%',

    -- ETL
    etl_loaded_at TIMESTAMP COMMENT 'Время загрузки в Silver'
)
USING iceberg
PARTITIONED BY (year)
COMMENT 'Silver слой отчётов участников Technopark с массивами экспорта по странам';
