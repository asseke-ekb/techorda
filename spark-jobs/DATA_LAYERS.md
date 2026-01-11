# Архитектура: Bronze → Silver → Gold

## Обзор

Данный документ описывает трёхслойную архитектуру одну из витрин поквартальньного отчета.

```
┌─────────────────────────────────────────────────────────────────┐
│                         BRONZE                                  │
│  iceberg.bronze.service_report_cdc                              │
│  • Сырые данные из CDC                                          │
│  • JSON в колонке `data`                                        │
│  • Все версии записей                                           │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                         SILVER                                  │
│  iceberg.silver.service_report                                  │
│  • Дедупликация (ROW_NUMBER)                                    │
│  • Распарсенные поля из JSON                                    │
│  • Типизация (BIGINT, STRING)                                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                          GOLD                                   │
│  iceberg.gold.reports_summary                                   │
│  • Агрегация по (service_request_id, year)                      │
│  • Только signed + quarter1-4                                   │
│  • Готово для Excel/BI                                          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 1. BRONZE — Сырые данные

### 1.1 Назначение

- Хранит **все** записи из CDC (Change Data Capture)
- JSON остаётся в колонке `data` как есть
- **Дедупликация настроена при загрузке** по полям `id` и `updated_ts`

### 1.2 Структура таблицы

| Колонка            | Тип       | Описание                                    |
|--------------------|-----------|---------------------------------------------|
| id                 | INT       | ID отчёта                                   |
| year               | INT       | Год отчёта                                  |
| report_type        | STRING    | Тип: quarter1, quarter2, quarter3, quarter4, yearly |
| status             | STRING    | Статус: draft, signed, rejected             |
| signed_at          | TIMESTAMP | Дата подписания                             |
| version            | STRING    | Версия отчёта                               |
| created_at         | TIMESTAMP | Дата создания                               |
| updated_at         | TIMESTAMP | Дата обновления                             |
| service_request_id | INT       | ID заявки                                   |
| author_id          | INT       | ID автора                                   |
| **op**             | **STRING** | **Операция CDC: c (create), u (update), d (delete)** |
| **data**           | **STRING (JSON)** | **JSON с данными отчёта**          |

### 1.3 Поле `op` — Операции CDC

| Значение | Операция | Описание |
|----------|----------|----------|
| `c`      | CREATE   | Новая запись добавлена |
| `u`      | UPDATE   | Запись изменена |
| `d`      | DELETE   | Запись удалена |

> **Важно:** При формировании Silver/Gold слоёв необходимо фильтровать `WHERE op != 'd'` для исключения удалённых записей.

---

## 2. SILVER — Очищенные данные

### 2.1 Назначение

- **Фильтрация удалённых записей**: исключаем `op = 'd'`
- **Парсинг JSON**: извлечение полей из `data`
- **Типизация**: приведение к правильным типам (BIGINT, STRING)

> **Примечание:** Дедупликация уже выполнена в Bronze по `id` + `updated_ts`

### 2.2 Логика обработки CDC

> **Важно:** Дедупликация по `id` и `updated_ts` уже настроена при загрузке в Bronze.
> В Silver нужно учитывать поле `op` (операция CDC):
> - `c` (create) — добавление записи
> - `u` (update) — модификация записи
> - `d` (delete) — удаление записи

```sql
-- Исключаем удалённые записи (op = 'd')
WHERE op != 'd'
```

### 2.3 SQL для Silver (Preview)

> **Примечание:** Дедупликация уже выполнена в Bronze по `id` + `updated_ts`.
> Здесь только фильтруем удалённые записи и парсим JSON.

```sql
SELECT
    r.id AS report_id,
    r.service_request_id,
    r.year,
    r.report_type,
    r.status,
    r.version,
    r.created_at,
    r.updated_at,
    r.signed_at,
    r.author_id,
    r.op AS cdc_operation,  -- Сохраняем операцию CDC для аудита

    -- Компания / идентификаторы
    get_json_object(r.data, '$.company_tin')  AS company_tin,
    get_json_object(r.data, '$.company_name') AS company_name,
    CAST(get_json_object(r.data, '$.certificate_number') AS STRING) AS certificate_number,
    CAST(get_json_object(r.data, '$.oked') AS STRING) AS oked,

    -- Сотрудники
    CAST(get_json_object(r.data, '$.residents_count') AS BIGINT)    AS residents_count,
    CAST(get_json_object(r.data, '$.nonresidents_count') AS BIGINT) AS nonresidents_count,
    CAST(get_json_object(r.data, '$.gph_count') AS BIGINT)          AS gph_count,

    -- Доходы
    CAST(get_json_object(r.data, '$.income_total') AS BIGINT)         AS income_total,
    CAST(get_json_object(r.data, '$.income_international') AS BIGINT) AS income_international,
    CAST(get_json_object(r.data, '$.income_total_previous_quarter') AS BIGINT) AS income_total_previous_quarter,
    CAST(get_json_object(r.data, '$.income_total_current_quarter') AS BIGINT)  AS income_total_current_quarter,

    -- Финансирование / инвестиции
    CAST(get_json_object(r.data, '$.finance_source_increase_authorized_capital') AS BIGINT)
        AS finance_source_increase_authorized_capital,
    CAST(get_json_object(r.data, '$.main_capital_investments') AS BIGINT)
        AS main_capital_investments,
    CAST(get_json_object(r.data, '$.main_tangible_capital_investments') AS BIGINT)
        AS main_tangible_capital_investments,
    CAST(get_json_object(r.data, '$.main_intangible_capital_investments') AS BIGINT)
        AS main_intangible_capital_investments,

    CAST(get_json_object(r.data, '$.finance_source_loan') AS BIGINT)
        AS finance_source_loan,
    CAST(get_json_object(r.data, '$.finance_source_loan_foreign') AS BIGINT)
        AS finance_source_loan_foreign,
    CAST(get_json_object(r.data, '$.finance_source_government') AS BIGINT)
        AS finance_source_government,
    CAST(get_json_object(r.data, '$.finance_source_investment') AS BIGINT)
        AS finance_source_investment,

    CAST(get_json_object(r.data, '$.investor_amount') AS BIGINT)
        AS investor_amount,
    get_json_object(r.data, '$.investor_country_company')
        AS investor_country_company,

    -- Налоги / льготы
    CAST(get_json_object(r.data, '$.tax_incentives') AS BIGINT)     AS tax_incentives,
    CAST(get_json_object(r.data, '$.tax_incentives_kpn') AS BIGINT) AS tax_incentives_kpn,
    CAST(get_json_object(r.data, '$.tax_incentives_nds') AS BIGINT) AS tax_incentives_nds,
    CAST(get_json_object(r.data, '$.tax_incentives_ipn') AS BIGINT) AS tax_incentives_ipn,
    CAST(get_json_object(r.data, '$.tax_incentives_sn') AS BIGINT)  AS tax_incentives_sn,

    -- Подстраховка под Excel (если ключ существует)
    CAST(get_json_object(r.data, '$.total_tax_saved') AS BIGINT)    AS total_tax_saved

FROM iceberg.bronze.service_report_cdc r
WHERE r.op != 'd'  -- Исключаем удалённые записи
ORDER BY service_request_id, year, report_type, status;
```

---

## 3. GOLD — Агрегированные данные

### 3.1 Назначение

Gold хранит **годовой свод** на уровне заявки/компании:

- Берём только `status = 'signed'`
- Берём только квартальные отчёты (`quarter1`, `quarter2`, `quarter3`, `quarter4`)
- Суммируем метрики по `(service_request_id, year)`

### 3.2 Важное бизнес-правило

> **`yearly` НЕ суммируем с кварталами**, иначе происходит задвоение данных.
>
> `yearly` можно использовать как **контроль качества** (отдельно).

### 3.3 SQL для Gold (Preview)

> **Примечание:** Дедупликация уже выполнена в Bronze.
> Здесь фильтруем по `op != 'd'`, `status = 'signed'` и агрегируем.

```sql
WITH silver_data AS (
    -- Парсим JSON и фильтруем удалённые записи
    SELECT
        service_request_id,
        year,
        report_type,
        status,
        op,

        CAST(get_json_object(data, '$.residents_count') AS BIGINT)    AS residents_count,
        CAST(get_json_object(data, '$.nonresidents_count') AS BIGINT) AS nonresidents_count,

        CAST(get_json_object(data, '$.income_total') AS BIGINT)         AS income_total,
        CAST(get_json_object(data, '$.income_international') AS BIGINT) AS income_international,

        CAST(get_json_object(data, '$.finance_source_increase_authorized_capital') AS BIGINT)
            AS finance_source_increase_authorized_capital,
        CAST(get_json_object(data, '$.main_capital_investments') AS BIGINT)
            AS main_capital_investments,
        CAST(get_json_object(data, '$.finance_source_loan') AS BIGINT)
            AS finance_source_loan,
        CAST(get_json_object(data, '$.finance_source_loan_foreign') AS BIGINT)
            AS finance_source_loan_foreign,
        CAST(get_json_object(data, '$.finance_source_government') AS BIGINT)
            AS finance_source_government,
        CAST(get_json_object(data, '$.finance_source_investment') AS BIGINT)
            AS finance_source_investment,

        CAST(get_json_object(data, '$.investor_amount') AS BIGINT)
            AS investor_amount,

        CAST(get_json_object(data, '$.tax_incentives') AS BIGINT)     AS tax_incentives,
        CAST(get_json_object(data, '$.tax_incentives_kpn') AS BIGINT) AS tax_incentives_kpn,
        CAST(get_json_object(data, '$.tax_incentives_nds') AS BIGINT) AS tax_incentives_nds,
        CAST(get_json_object(data, '$.tax_incentives_ipn') AS BIGINT) AS tax_incentives_ipn,
        CAST(get_json_object(data, '$.tax_incentives_sn') AS BIGINT)  AS tax_incentives_sn,

        CAST(get_json_object(data, '$.total_tax_saved') AS BIGINT)    AS total_tax_saved

    FROM iceberg.bronze.service_report_cdc
    WHERE op != 'd'  -- Исключаем удалённые записи
),

signed_quarters AS (
    -- Фильтруем только подписанные квартальные отчёты
    SELECT *
    FROM silver_data
    WHERE status = 'signed'
      AND report_type IN ('quarter1', 'quarter2', 'quarter3', 'quarter4')
)

SELECT
    service_request_id,
    year,
    COUNT(*) AS signed_quarters,

    SUM(COALESCE(residents_count, 0))    AS residents_count,
    SUM(COALESCE(nonresidents_count, 0)) AS nonresidents_count,

    SUM(COALESCE(income_total, 0))         AS income_total,
    SUM(COALESCE(income_international, 0)) AS income_international,

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

    -- "Сэкономлено" в Excel: если есть total_tax_saved — берём его, иначе tax_incentives
    SUM(COALESCE(total_tax_saved, tax_incentives, 0))
        AS taxes_saved,

    SUM(COALESCE(tax_incentives_kpn, 0)) AS tax_incentives_kpn,
    SUM(COALESCE(tax_incentives_nds, 0)) AS tax_incentives_nds,
    SUM(COALESCE(tax_incentives_ipn, 0)) AS tax_incentives_ipn,
    SUM(COALESCE(tax_incentives_sn, 0))  AS tax_incentives_sn

FROM signed_quarters
GROUP BY service_request_id, year
ORDER BY year, service_request_id;
```

---

## 4. Сопоставление с arm.py

### 4.1 Что делает arm.py

1. Выбирает отчёты за год
2. Отдельно по `quarter1..quarter4` (+ `yearly`)
3. Фильтр `status = 'signed'`
4. Суммирует поля из `data`

### 4.2 Что делает Gold

1. Берёт данные из Silver (или silver_preview)
2. Фильтр `status = 'signed' AND report_type IN (quarter1..quarter4)`
3. `GROUP BY service_request_id, year`
4. `SUM()` тех же полей

### 4.3 Результат

| arm.py                          | Gold                                              |
|---------------------------------|---------------------------------------------------|
| `reports_summary(year)`         | `SELECT FROM gold.reports_summary WHERE year=...` |

**Функциональный результат совпадает.**

> Разница только в том, что у arm.py есть разрез по кварталам внутри ответа.
> Если нужен поквартальный разрез — можно создать второй gold "по кварталам".

---

## 5. Сопоставление с Excel/PowerQuery

### 5.1 Что делает Excel (из docx/PowerQuery)

- Выбирает и переименовывает поля:
  - "БИН" → `company_tin`
  - "№ свид-ва" → `certificate_number`
  - "Сэкономлено…" → `taxes_saved`
  - "Доход…" → `income_total`
  - "Инвестиции…" → `main_capital_investments`
  - "Господдержка…" → `finance_source_government`
  - "Экспорт…" → `income_international`
- Добавляет "Год"
- Приводит типы к `Int64`

### 5.2 Что делает Gold

| Excel поле          | Gold колонка                  | Комментарий                              |
|---------------------|-------------------------------|------------------------------------------|
| Год                 | `year`                        | —                                        |
| БИН                 | `company_tin`                 | В Silver; в Gold можно JOIN по `service_request_id` |
| Экспорт / EXPORT    | `income_international`        | По смыслу                                |
| Сэкономлено / TAXES | `taxes_saved`                 | `COALESCE(total_tax_saved, tax_incentives)` |
| Инвестиции          | `main_capital_investments`    | —                                        |
| Господдержка        | `finance_source_government`   | —                                        |

### 5.3 Результат

```
После: Bronze → Silver → Gold → Excel (только отображение)
```

---

## 6. DDL для создания таблиц

### 6.1 Silver

```sql
CREATE TABLE IF NOT EXISTS iceberg.silver.service_report (
    report_id INT COMMENT 'ID отчёта',
    service_request_id INT COMMENT 'ID заявки',
    year INT COMMENT 'Год отчёта',
    report_type STRING COMMENT 'Тип: quarter1-4, yearly',
    status STRING COMMENT 'Статус: draft, signed, rejected',
    version STRING COMMENT 'Версия отчёта',
    created_at TIMESTAMP COMMENT 'Дата создания',
    updated_at TIMESTAMP COMMENT 'Дата обновления',
    signed_at TIMESTAMP COMMENT 'Дата подписания',
    author_id INT COMMENT 'ID автора',

    -- Компания
    company_tin STRING COMMENT 'БИН/ИИН',
    company_name STRING COMMENT 'Название компании',
    certificate_number STRING COMMENT 'Номер сертификата',
    oked STRING COMMENT 'Код ОКЭД',

    -- Сотрудники
    residents_count BIGINT COMMENT 'Количество резидентов',
    nonresidents_count BIGINT COMMENT 'Количество нерезидентов',
    gph_count BIGINT COMMENT 'Количество по ГПХ',

    -- Доходы
    income_total BIGINT COMMENT 'Общий доход (накопительно)',
    income_international BIGINT COMMENT 'Международный доход/экспорт (накопительно)',
    income_total_current_quarter BIGINT COMMENT 'Доход за текущий квартал',
    income_total_previous_quarter BIGINT COMMENT 'Доход за прошлый квартал',

    -- Инвестиции
    investments_total_current_quarter BIGINT COMMENT 'Всего инвестиций за квартал (total_funding)',
    finance_source_increase_authorized_capital BIGINT COMMENT 'Вклад в уставной капитал',
    main_capital_investments BIGINT COMMENT 'Инвестиции в основной капитал',
    main_tangible_capital_investments BIGINT COMMENT 'Инвестиции в материальный капитал',
    main_intangible_capital_investments BIGINT COMMENT 'Инвестиции в нематериальный капитал',

    -- Займы и финансирование
    finance_source_loan BIGINT COMMENT 'Заемные средства',
    finance_source_loan_foreign BIGINT COMMENT 'Иные инвестиции',
    government_support_measures BIGINT COMMENT 'Господдержка (ГОСПОДДЕРЖКА в Power BI)',
    finance_source_investment BIGINT COMMENT 'Привлеченные инвестиции',

    -- Инвесторы
    investor_amount BIGINT COMMENT 'Сумма от инвесторов',
    investor_country_company STRING COMMENT 'Страна/компания инвестора',

    -- Налоговые льготы
    tax_incentives BIGINT COMMENT 'Налоговые льготы (всего)',
    tax_incentives_kpn BIGINT COMMENT 'КПН',
    tax_incentives_nds BIGINT COMMENT 'НДС',
    tax_incentives_ipn BIGINT COMMENT 'ИПН',
    tax_incentives_sn BIGINT COMMENT 'СН',
    total_tax_saved BIGINT COMMENT 'Всего сэкономлено (TAXES в Power BI)',

    -- Сбор
    collection_amount BIGINT COMMENT 'Сумма сбора 1%',

    -- ETL
    etl_loaded_at TIMESTAMP COMMENT 'Дата загрузки ETL'
)

COMMENT 'Очищенные отчёты (дедупликация + парсинг JSON)'

```

### 6.2 Gold

```sql
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
    taxes_saved BIGINT COMMENT 'Сэкономлено на налогах (total_tax_saved или tax_incentives)',
    tax_incentives_kpn BIGINT COMMENT 'Льготы КПН',
    tax_incentives_nds BIGINT COMMENT 'Льготы НДС',
    tax_incentives_ipn BIGINT COMMENT 'Льготы ИПН',
    tax_incentives_sn BIGINT COMMENT 'Льготы СН',

    -- ETL
    etl_loaded_at TIMESTAMP COMMENT 'Дата загрузки ETL'
)
COMMENT 'Годовой свод отчётов по заявкам (только signed quarters)'

```

