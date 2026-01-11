-- Gold витрина: Сводная по кварталам и годам
-- Соответствует функции reports_summary() из arm.py
-- Агрегирует данные по year + report_type (квартал)

CREATE TABLE IF NOT EXISTS iceberg.gold.summary_by_quarter (
    year INT,
    report_type STRING COMMENT 'quarter1, quarter2, quarter3, quarter4, yearly',
    period STRING COMMENT 'Название периода: 1кв.2025г, 2кв.2025г и т.д.',
    total_reports BIGINT COMMENT 'Количество отчетов',
    residents_count BIGINT COMMENT 'Сумма работников резидентов',
    nonresidents_count BIGINT COMMENT 'Сумма работников нерезидентов',
    income_total BIGINT COMMENT 'Сумма доходов от реализации',
    income_international BIGINT COMMENT 'Сумма доходов от экспорта',
    finance_source_increase_authorized_capital BIGINT COMMENT 'Вклад в уставной капитал',
    main_capital_investments BIGINT COMMENT 'Инвестиции в основной капитал',
    finance_source_loan BIGINT COMMENT 'Заемные средства',
    finance_source_loan_foreign BIGINT COMMENT 'Иностранные займы',
    finance_source_government BIGINT COMMENT 'Господдержка (старое поле)',
    government_support_measures BIGINT COMMENT 'Меры господдержки (новое поле)',
    finance_source_investment BIGINT COMMENT 'Инвестиции',
    tax_incentives BIGINT COMMENT 'Налоговые льготы',
    total_tax_saved BIGINT COMMENT 'Сэкономлено налогов'
)
USING iceberg
PARTITIONED BY (year)
COMMENT 'Агрегированные данные по кварталам для reports_summary';

-- Заполнение витрины
INSERT OVERWRITE iceberg.gold.summary_by_quarter
SELECT
    year,
    report_type,
    -- Формируем название периода как в Django
    CASE report_type
        WHEN 'quarter1' THEN CONCAT('1кв.', year, 'г.')
        WHEN 'quarter2' THEN CONCAT('2кв.', year, 'г.')
        WHEN 'quarter3' THEN CONCAT('3кв.', year, 'г.')
        WHEN 'quarter4' THEN CONCAT('4кв.', year, 'г.')
        WHEN 'yearly' THEN CONCAT('итого.', year, 'г.')
        ELSE report_type
    END as period,
    COUNT(*) as total_reports,

    -- Суммы по полям (как в arm.py)
    SUM(COALESCE(residents_count, 0)) as residents_count,
    SUM(COALESCE(non_residents_count, 0)) as nonresidents_count,
    SUM(COALESCE(income_total, 0)) as income_total,
    SUM(COALESCE(income_international, 0)) as income_international,
    SUM(COALESCE(finance_source_increase_authorized_capital, 0)) as finance_source_increase_authorized_capital,
    SUM(COALESCE(main_capital_investments, 0)) as main_capital_investments,
    SUM(COALESCE(finance_source_loan, 0)) as finance_source_loan,
    SUM(COALESCE(finance_source_loan_foreign, 0)) as finance_source_loan_foreign,
    SUM(COALESCE(finance_source_government, 0)) as finance_source_government,
    SUM(COALESCE(government_support_measures, 0)) as government_support_measures,
    SUM(COALESCE(finance_source_investment, 0)) as finance_source_investment,
    SUM(COALESCE(tax_incentives, 0)) as tax_incentives,
    SUM(COALESCE(total_tax_saved, 0)) as total_tax_saved
FROM iceberg.silver.service_report_v2
WHERE status = 'signed' AND op != 'd'
GROUP BY year, report_type
ORDER BY year, report_type;
