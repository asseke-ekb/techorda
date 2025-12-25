# Витрина отчётов участников Technopark

## Обзор

ETL-процесс для построения витрины отчётов из bronze-слоя Iceberg.

**Источник:** `iceberg.bronze.service_report_cdc`
**Назначение:** `iceberg.gold.reports_vitrina`, `iceberg.gold.reports_summary`

---

## Структура источника

Таблица `service_report_cdc` содержит:

| Колонка | Тип | Описание |
|---------|-----|----------|
| id | INT | ID отчёта |
| year | INT | Год отчёта |
| report_type | STRING | Тип: quarter1, quarter2, quarter3, quarter4, yearly |
| status | STRING | Статус: draft, signed, rejected |
| signed_at | TIMESTAMP | Дата подписания |
| version | STRING | Версия отчёта |
| created_at | TIMESTAMP | Дата создания |
| updated_at | TIMESTAMP | Дата обновления |
| service_request_id | INT | ID заявки |
| author_id | INT | ID автора |
| **data** | **STRING (JSON)** | **JSON с данными отчёта** |

---

## Парсинг JSON-поля `data`

Колонка `data` содержит JSON-объект. Извлечение полей через `get_json_object()`:

### Компания
```sql
get_json_object(data, '$.company_name')        -- Название компании
get_json_object(data, '$.company_tin')         -- БИН/ИИН
get_json_object(data, '$.certificate_number')  -- Номер сертификата (INT)
```

### Сотрудники
```sql
get_json_object(data, '$.residents_count')     -- Резиденты (INT)
get_json_object(data, '$.nonresidents_count')  -- Нерезиденты (INT)
get_json_object(data, '$.gph_count')           -- По ГПХ (INT)
```

### Доходы
```sql
get_json_object(data, '$.income_total')                    -- Общий доход (BIGINT)
get_json_object(data, '$.income_international')            -- Международный доход (BIGINT)
get_json_object(data, '$.income_total_previous_quarter')   -- Доход прошлого квартала (BIGINT)
get_json_object(data, '$.income_total_current_quarter')    -- Доход текущего квартала (BIGINT)
```

### Инвестиции
```sql
get_json_object(data, '$.investments_total_current_quarter')           -- Инвестиции за квартал (BIGINT)
get_json_object(data, '$.main_capital_investments')                    -- Капитальные инвестиции (BIGINT)
get_json_object(data, '$.main_tangible_capital_investments')           -- Материальные (BIGINT)
get_json_object(data, '$.main_intangible_capital_investments')         -- Нематериальные (BIGINT)
get_json_object(data, '$.finance_source_increase_authorized_capital')  -- Увеличение уставного капитала (BIGINT)
```

### Инвесторы
```sql
get_json_object(data, '$.investor_amount')          -- Сумма инвестиций (BIGINT)
get_json_object(data, '$.investor_country_company') -- Страна инвестора (STRING)
```

### Займы
```sql
get_json_object(data, '$.finance_source_loan')          -- Займы (BIGINT)
get_json_object(data, '$.finance_source_loan_foreign')  -- Иностранные займы (BIGINT)
```

### Господдержка
```sql
get_json_object(data, '$.finance_source_government')   -- Господдержка (BIGINT)
get_json_object(data, '$.finance_source_investment')   -- Инвестиционное финансирование (BIGINT)
```

### Налоговые льготы
```sql
get_json_object(data, '$.tax_incentives')      -- Всего льгот (BIGINT)
get_json_object(data, '$.tax_incentives_kpn')  -- КПН (BIGINT)
get_json_object(data, '$.tax_incentives_nds')  -- НДС (BIGINT)
get_json_object(data, '$.tax_incentives_ipn')  -- ИПН (BIGINT)
get_json_object(data, '$.tax_incentives_sn')   -- СН (BIGINT)
get_json_object(data, '$.collection_amount')   -- Сумма сборов (BIGINT)
```

### ОКЭД и исполнитель
```sql
get_json_object(data, '$.oked')              -- Код ОКЭД (STRING)
get_json_object(data, '$.executor_fullname') -- ФИО исполнителя (STRING)
get_json_object(data, '$.executor_phone')    -- Телефон исполнителя (STRING)
```

### Флаги
```sql
get_json_object(data, '$.has_nonresidents')            -- Есть нерезиденты (STRING: "true"/"false")
get_json_object(data, '$.has_borrowed_funds')          -- Есть заёмные средства (STRING)
get_json_object(data, '$.has_raised_investors_funds')  -- Есть привлечённые инвестиции (STRING)
```

---

## Фильтрация данных

```sql
WHERE status = 'signed'    -- Только подписанные отчёты
  AND year >= 2019         -- С 2019 года
```

---

## SQL для построения витрины

### Детальная витрина (reports_vitrina)

```sql
INSERT INTO iceberg.gold.reports_vitrina
SELECT
    id AS report_id,
    year,
    report_type,
    status,
    signed_at,
    version,
    created_at,
    updated_at,
    service_request_id,
    author_id,

    -- Компания
    get_json_object(data, '$.company_name') AS company_name,
    get_json_object(data, '$.company_tin') AS company_tin,
    CAST(get_json_object(data, '$.certificate_number') AS INT) AS certificate_number,

    -- Сотрудники
    CAST(get_json_object(data, '$.residents_count') AS INT) AS residents_count,
    CAST(get_json_object(data, '$.nonresidents_count') AS INT) AS nonresidents_count,
    CAST(get_json_object(data, '$.gph_count') AS INT) AS gph_count,

    -- Доходы
    CAST(get_json_object(data, '$.income_total') AS BIGINT) AS income_total,
    CAST(get_json_object(data, '$.income_international') AS BIGINT) AS income_international,
    CAST(get_json_object(data, '$.income_total_previous_quarter') AS BIGINT) AS income_total_previous_quarter,
    CAST(get_json_object(data, '$.income_total_current_quarter') AS BIGINT) AS income_total_current_quarter,

    -- Инвестиции
    CAST(get_json_object(data, '$.investments_total_current_quarter') AS BIGINT) AS investments_total_current_quarter,
    CAST(get_json_object(data, '$.main_capital_investments') AS BIGINT) AS main_capital_investments,
    CAST(get_json_object(data, '$.main_tangible_capital_investments') AS BIGINT) AS main_tangible_capital_investments,
    CAST(get_json_object(data, '$.main_intangible_capital_investments') AS BIGINT) AS main_intangible_capital_investments,
    CAST(get_json_object(data, '$.finance_source_increase_authorized_capital') AS BIGINT) AS finance_source_increase_authorized_capital,

    -- Инвесторы
    CAST(get_json_object(data, '$.investor_amount') AS BIGINT) AS investor_amount,
    get_json_object(data, '$.investor_country_company') AS investor_country_company,

    -- Займы
    CAST(get_json_object(data, '$.finance_source_loan') AS BIGINT) AS finance_source_loan,
    CAST(get_json_object(data, '$.finance_source_loan_foreign') AS BIGINT) AS finance_source_loan_foreign,

    -- Господдержка
    CAST(get_json_object(data, '$.finance_source_government') AS BIGINT) AS finance_source_government,
    CAST(get_json_object(data, '$.finance_source_investment') AS BIGINT) AS finance_source_investment,

    -- Налоговые льготы
    CAST(get_json_object(data, '$.tax_incentives') AS BIGINT) AS tax_incentives,
    CAST(get_json_object(data, '$.tax_incentives_kpn') AS BIGINT) AS tax_incentives_kpn,
    CAST(get_json_object(data, '$.tax_incentives_nds') AS BIGINT) AS tax_incentives_nds,
    CAST(get_json_object(data, '$.tax_incentives_ipn') AS BIGINT) AS tax_incentives_ipn,
    CAST(get_json_object(data, '$.tax_incentives_sn') AS BIGINT) AS tax_incentives_sn,
    CAST(get_json_object(data, '$.collection_amount') AS BIGINT) AS collection_amount,

    -- ОКЭД и исполнитель
    get_json_object(data, '$.oked') AS oked,
    get_json_object(data, '$.executor_fullname') AS executor_fullname,
    get_json_object(data, '$.executor_phone') AS executor_phone,

    -- Флаги
    get_json_object(data, '$.has_nonresidents') AS has_nonresidents,
    get_json_object(data, '$.has_borrowed_funds') AS has_borrowed_funds,
    get_json_object(data, '$.has_raised_investors_funds') AS has_raised_investors_funds,

    -- ETL метаданные
    current_timestamp() AS etl_loaded_at

FROM iceberg.bronze.service_report_cdc
WHERE status = 'signed'
  AND year >= 2019
ORDER BY year DESC, report_type, id DESC;
```

### Агрегированная сводка (reports_summary)

```sql
INSERT INTO iceberg.gold.reports_summary
SELECT
    year,
    report_type,

    -- Читаемый период
    CASE
        WHEN report_type = 'quarter1' THEN CONCAT('Q1 ', CAST(year AS STRING))
        WHEN report_type = 'quarter2' THEN CONCAT('Q2 ', CAST(year AS STRING))
        WHEN report_type = 'quarter3' THEN CONCAT('Q3 ', CAST(year AS STRING))
        WHEN report_type = 'quarter4' THEN CONCAT('Q4 ', CAST(year AS STRING))
        ELSE CONCAT('Year ', CAST(year AS STRING))
    END AS period,

    -- Агрегаты
    COUNT(*) AS reports_count,
    SUM(COALESCE(CAST(get_json_object(data, '$.residents_count') AS INT), 0)) AS total_residents,
    SUM(COALESCE(CAST(get_json_object(data, '$.nonresidents_count') AS INT), 0)) AS total_nonresidents,
    SUM(COALESCE(CAST(get_json_object(data, '$.gph_count') AS INT), 0)) AS total_gph,
    SUM(COALESCE(CAST(get_json_object(data, '$.income_total') AS BIGINT), 0)) AS total_income,
    SUM(COALESCE(CAST(get_json_object(data, '$.income_international') AS BIGINT), 0)) AS total_income_international,
    SUM(COALESCE(CAST(get_json_object(data, '$.main_capital_investments') AS BIGINT), 0)) AS total_investments,
    SUM(COALESCE(CAST(get_json_object(data, '$.tax_incentives') AS BIGINT), 0)) AS total_tax_incentives,
    SUM(COALESCE(CAST(get_json_object(data, '$.investor_amount') AS BIGINT), 0)) AS total_investor_amount,
    SUM(COALESCE(CAST(get_json_object(data, '$.finance_source_loan') AS BIGINT), 0)) AS total_loans,
    SUM(COALESCE(CAST(get_json_object(data, '$.finance_source_government') AS BIGINT), 0)) AS total_government_support,

    current_timestamp() AS etl_loaded_at

FROM iceberg.bronze.service_report_cdc
WHERE status = 'signed'
  AND year >= 2019
GROUP BY year, report_type
ORDER BY year DESC, report_type;
```

---

## Режимы запуска

### 1. Полная перезагрузка (OVERWRITE)

```sql
-- Очистить и загрузить заново
DELETE FROM iceberg.gold.reports_vitrina WHERE 1=1;
-- Затем INSERT...
```

### 2. Инкрементальная загрузка

```sql
-- Только новые/изменённые за последние 24 часа
WHERE status = 'signed'
  AND year >= 2019
  AND updated_at >= date_sub(current_timestamp(), 1)
```

### 3. По конкретному году

```sql
WHERE status = 'signed'
  AND year = 2024
```

---

## PySpark скрипт

Файл: `vitrina_export.py`

```bash
# Детальная витрина в Iceberg
spark-submit vitrina_export.py \
    --output-format iceberg \
    --output-table gold.reports_vitrina

# Сводка в Iceberg
spark-submit vitrina_export.py \
    --mode summary \
    --output-format iceberg \
    --output-table gold.reports_summary

# С фильтром по году
spark-submit vitrina_export.py \
    --year 2024 \
    --output-format iceberg \
    --output-table gold.reports_vitrina

# Экспорт в CSV
spark-submit vitrina_export.py \
    --year 2024 \
    --output-format csv \
    --output-path /tmp/vitrina_2024
```

---

## Конфигурация Spark

Для работы с Iceberg нужны следующие настройки:

```bash
--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.iceberg.type=hive
--conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore-service.iceberg-spark.svc.cluster.local:9083
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

---

## Расписание (Argo Workflow)

Рекомендуется:
- **Ежедневно** — инкрементальная загрузка
- **Еженедельно** — полная перезагрузка

---

## Контакты

При вопросах обращаться к команде Data Platform.
