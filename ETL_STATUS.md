# ETL Status - Techorda Data Platform

## Дата последнего обновления: 2026-01-20

---

## 📊 Архитектура данных

```
PostgreSQL (источник)
    ↓ CDC (Debezium)
Bronze Layer (iceberg.bronze.*)
    ↓ Трансформация
Silver Layer (iceberg.silver.*)
    ↓ Агрегация
Gold Layer (iceberg.gold.*)
    ↓
Power BI / Metabase
```

---

## 📁 Скрипты ETL

| Файл | Назначение | Слой |
|------|------------|------|
| `refresh_silver_v2.sql` | Отчёты участников (42 поля, 8 годовых INSERT) | Bronze → Silver |
| `refresh_silver_participants.sql` | Участники технопарка | Bronze → Silver |
| `refresh_silver_companies.sql` | Справочник компаний (id, name, short_name, tin) | Bronze → Silver |
| `refresh_gold_vitrinas.sql` | 5 витрин для Power BI | Silver → Gold |
| `refresh_gold_export_by_country.sql` | Экспорт по странам (JSON explode) | Bronze → Gold |

---

## 🗃️ Таблицы Bronze

| Таблица | Описание |
|---------|----------|
| `service_report_cdc` | Квартальные отчёты участников |
| `service_servicerequest_cdc` | Заявки на услуги (включая регистрацию в технопарке) |
| `account_company_cdc` | Справочник компаний |

---

## 🗃️ Таблицы Silver

### 1. service_report_v2
- **Скрипт**: `refresh_silver_v2.sql`
- **Записей**: ~27000
- **Партиционирование**: по году
- **Особенности**:
  - 42 поля
  - company_tin/company_name извлекаются из:
    - 2019-2020: `data JSON` (`$.company_tin`, `$.company_name`)
    - 2021+: `signature.signed_xml` (XML внутри JSON)
  - 8 отдельных INSERT по годам (2019-2026)
  - Дедупликация: `ROW_NUMBER() OVER (PARTITION BY service_request_id, report_type, year ORDER BY updated_at DESC)`

### 2. techpark_participants
- **Скрипт**: `refresh_silver_participants.sql`
- **Фильтр**: `service_id = 'techpark'`, `bp_status IN ('registered', 'deactivated')`
- **Поля**:
  - service_request_id (для связи с отчётами)
  - company_id (для связи с companies)
  - bp_status
  - certificate_number, certificate_issue_date, certificate_end_date
  - company_name, company_tin
  - deactivation_reason, deactivation_date

### 3. companies (НОВАЯ)
- **Скрипт**: `refresh_silver_companies.sql`
- **Источник**: `account_company_cdc`
- **Поля**: company_id, name, short_name, tin

---

## 🗃️ Таблицы Gold (витрины для Power BI)

| Витрина | Поля | Партиции |
|---------|------|----------|
| `general_indicators` | year, report_type, certificate_number, bin, company_name, certificate_issue_date, activity_fields, government_support, tax_saved, export_income, total_funding, income_total | year |
| `financing` | year, report_type, certificate_number, bin, government_support, loan_funds, authorized_capital_increase, total_funding, attracted_investments | year |
| `tax_benefits` | year, report_type, certificate_number, bin, kpn, nds, ipn, sn, total_tax_saved | year |
| `employees` | year, report_type, certificate_number, bin, residents_count, nonresidents_count, gph_count | year |
| `exports` | year, report_type, certificate_number, bin, company_name, export_income | year |
| `export_by_country` | year, report_type, certificate_number, bin, company_name, country, export_amount | year |

---

## 🔧 Порядок выполнения ETL

### Шаг 1: Silver слой
```sql
-- 1.1. Участники (выполнить целиком)
-- spark-jobs/sql/refresh_silver_participants.sql

-- 1.2. Компании (выполнить целиком)
-- spark-jobs/sql/refresh_silver_companies.sql

-- 1.3. Отчёты (выполнять по блокам!)
-- spark-jobs/sql/refresh_silver_v2.sql
-- Порядок: DROP → CREATE → INSERT 2019 → INSERT 2020 → ... → INSERT 2026
```

### Шаг 2: Gold слой
```sql
-- 2.1. Основные витрины (выполнять каждый блок отдельно!)
-- spark-jobs/sql/refresh_gold_vitrinas.sql
-- Порядок для каждой: DROP → CREATE → INSERT

-- 2.2. Экспорт по странам
-- spark-jobs/sql/refresh_gold_export_by_country.sql
```

### Шаг 3: Проверка
```sql
-- Проверка Silver
SELECT year, COUNT(*) as cnt FROM iceberg.silver.service_report_v2 GROUP BY year ORDER BY year;
SELECT COUNT(*) FROM iceberg.silver.techpark_participants;
SELECT COUNT(*) FROM iceberg.silver.companies;

-- Проверка Gold
SELECT 'general_indicators' as table_name, COUNT(*) as cnt FROM iceberg.gold.general_indicators
UNION ALL SELECT 'financing', COUNT(*) FROM iceberg.gold.financing
UNION ALL SELECT 'tax_benefits', COUNT(*) FROM iceberg.gold.tax_benefits
UNION ALL SELECT 'employees', COUNT(*) FROM iceberg.gold.employees
UNION ALL SELECT 'exports', COUNT(*) FROM iceberg.gold.exports
UNION ALL SELECT 'export_by_country', COUNT(*) FROM iceberg.gold.export_by_country;
```

---

## 📝 Технические детали

### Извлечение company_tin / company_name

| Годы | Источник | Путь |
|------|----------|------|
| 2019-2020 | data JSON | `$.company_tin`, `$.company_name` |
| 2021+ | signature JSON → XML | `<company><tin>`, `<name>` |

```sql
-- Универсальный COALESCE для всех годов:
COALESCE(
    get_json_object(data, '$.company_tin'),
    regexp_extract(get_json_object(signature, '$.signed_xml'), '<company>.*?<tin>([0-9]+)</tin>.*?</company>', 1)
) as company_tin
```

### Дедупликация CDC
```sql
SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY service_request_id, report_type, year
        ORDER BY updated_at DESC
    ) as rn
FROM iceberg.bronze.service_report_cdc
WHERE op != 'd'  -- исключаем удалённые записи
```

### JSON Array Explode (для export_by_country)
```sql
LATERAL VIEW explode(
    from_json(
        get_json_object(data, '$.income_international_current_quarter'),
        'array<string>'
    )
) AS export_item
```

---

## 🔌 Подключения

| Сервис | URL |
|--------|-----|
| Spark JDBC | `jdbc:hive2://109.248.170.228:31000` |
| Metabase | http://109.248.170.228:30300/ |

---

## ⚠️ Известные особенности

1. **activity_fields** — появляется только в новых отчётах (id > 17000)
2. **income_international_current_quarter** — может быть числом или JSON массивом, фильтруем по `LIKE '[%'`
3. **Spark Thrift Server** — может падать, требуется перезапуск на сервере
4. **INSERT выполнять по одному** — не группировать, иначе NullPointerException

---

## 📊 Соответствие Power Query

| Power Query | Gold витрина |
|-------------|--------------|
| Общие показатели | general_indicators |
| Финансирование | financing |
| Налоговые льготы | tax_benefits |
| Работники | employees |
| Экспорт (общий) | exports |
| Экспорт по странам | export_by_country |

---

## 🚀 TODO (после получения доступа к Bronze)

- [ ] Проверить наличие `account_company_cdc` в Bronze: `SHOW TABLES IN iceberg.bronze LIKE '*company*'`
- [ ] Посмотреть структуру: `DESCRIBE iceberg.bronze.account_company_cdc`
- [ ] Выполнить `refresh_silver_companies.sql`
- [ ] Обновить Gold витрины для использования companies (если нужно)
