-- Gold витрина: Налоговые льготы
-- Соответствует Power Query: "Обработка 2 кв отчета 2025 г для отображения налоговых льгот"

CREATE TABLE IF NOT EXISTS iceberg.gold.tax_benefits (
    year INT,
    certificate_number STRING,
    bin STRING,
    kpn BIGINT COMMENT 'КПН на прибыль годовой',
    nds BIGINT COMMENT 'НДС за текущий квартал',
    ipn BIGINT COMMENT 'ИПН за текущий квартал',
    sn BIGINT COMMENT 'СН за текущий квартал',
    total_tax_saved BIGINT COMMENT 'Сэкономлено за отчетный квартал'
)
USING iceberg
PARTITIONED BY (year)
COMMENT 'Налоговые льготы для дашборда Power BI';

-- Заполнение витрины
INSERT OVERWRITE iceberg.gold.tax_benefits
SELECT
    year,
    certificate_number,
    bin,
    tax_incentives_kpn as kpn,
    tax_incentives_nds as nds,
    tax_incentives_ipn as ipn,
    tax_incentives_sn as sn,
    total_tax_saved
FROM iceberg.silver.service_report_v2
WHERE op != 'd';
