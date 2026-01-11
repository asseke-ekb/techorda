# Руководство по подключению Power BI к Techorda Data Platform

## 📋 Содержание
1. [Обзор архитектуры](#обзор-архитектуры)
2. [Доступные витрины данных](#доступные-витрины-данных)
3. [Способы подключения](#способы-подключения)
4. [SQL примеры запросов](#sql-примеры-запросов)
5. [Соответствие Power Query](#соответствие-power-query)
6. [Troubleshooting](#troubleshooting)

---

## 🏗 Обзор архитектуры

### Слои данных:

```
Bronze (CDC) → Silver (Нормализация) → Gold (Витрины для BI)
```

**Bronze**: Сырые данные из CDC (Change Data Capture)
**Silver**: Очищенные и типизированные данные
**Gold**: Витрины для дашбордов Power BI

---

## 📊 Доступные витрины данных

Все витрины находятся в схеме `iceberg.gold`:

### 1. `gold.general_indicators` - Общие показатели
**Записей:** 11,479 (только signed отчёты)

| Колонка | Тип | Описание | Power BI аналог |
|---------|-----|----------|-----------------|
| `year` | INT | Год отчёта | Год |
| `certificate_number` | STRING | Номер свидетельства | № свид-ва |
| `company_tin` | STRING | БИН компании | БИН |
| `company_name` | STRING | Название компании | Наименование |
| `government_support` | BIGINT | Меры господдержки | ГОСПОДДЕРЖКА |
| `total_tax_saved` | BIGINT | Сэкономлено налогов | TAXES |
| `export_income` | BIGINT | Доход от экспорта | EXPORT |
| `total_funding` | BIGINT | Всего инвестиций | total_funding |
| `income_total` | BIGINT | Общий доход | INCOME_TOTAL |

**Пример запроса:**
```sql
SELECT * FROM iceberg.gold.general_indicators
WHERE year = 2025;
```

---

### 2. `gold.financing` - Финансирование
**Записей:** 11,479

| Колонка | Тип | Описание | Power BI аналог |
|---------|-----|----------|-----------------|
| `year` | INT | Год отчёта | Год |
| `certificate_number` | STRING | Номер свидетельства | № свид-ва |
| `company_tin` | STRING | БИН компании | БИН |
| `government_support` | BIGINT | Меры господдержки | ГОСПОДДЕРЖКА |
| `loan_funds` | BIGINT | Заемные средства | заемные |
| `authorized_capital_increase` | BIGINT | Вклад учредителя в УК | сумма вложений учредителем |
| `total_funding` | BIGINT | Всего инвестиций | total_funding |
| `attracted_investments` | BIGINT | Привлеченные инвестиции | привлеченные инвестиции |

**Формула расчёта:**
```
attracted_investments = authorized_capital_increase + finance_source_investment
```

**Пример запроса:**
```sql
SELECT
    year,
    company_name,
    total_funding,
    attracted_investments,
    loan_funds
FROM iceberg.gold.financing
WHERE total_funding > 0
ORDER BY total_funding DESC
LIMIT 10;
```

---

### 3. `gold.tax_benefits` - Налоговые льготы
**Записей:** 11,479

| Колонка | Тип | Описание | Power BI аналог |
|---------|-----|----------|-----------------|
| `year` | INT | Год отчёта | Год |
| `certificate_number` | STRING | Номер свидетельства | № свид-ва |
| `company_tin` | STRING | БИН компании | БИН |
| `kpn` | BIGINT | Льготы КПН | КПН |
| `nds` | BIGINT | Льготы НДС | НДС |
| `ipn` | BIGINT | Льготы ИПН | ИПН |
| `sn` | BIGINT | Льготы СН | СН |
| `total_tax_saved` | BIGINT | Всего сэкономлено | TAXES |

**Пример запроса:**
```sql
SELECT
    year,
    SUM(kpn) as total_kpn,
    SUM(nds) as total_nds,
    SUM(ipn) as total_ipn,
    SUM(sn) as total_sn,
    SUM(total_tax_saved) as total_saved
FROM iceberg.gold.tax_benefits
GROUP BY year
ORDER BY year;
```

---

### 4. `gold.employees` - Работники
**Записей:** 11,479

| Колонка | Тип | Описание | Power BI аналог |
|---------|-----|----------|-----------------|
| `year` | INT | Год отчёта | Год |
| `certificate_number` | STRING | Номер свидетельства | № свид-ва |
| `company_tin` | STRING | БИН компании | БИН |
| `residents_count` | INT | Работники-резиденты | Резидент |
| `non_residents_count` | INT | Работники-нерезиденты | Нерезидент |
| `civil_contracts_count` | INT | Договоры ГПХ | ГПХ |

**Пример запроса:**
```sql
SELECT
    year,
    SUM(residents_count) as total_residents,
    SUM(non_residents_count) as total_non_residents,
    SUM(civil_contracts_count) as total_civil_contracts
FROM iceberg.gold.employees
GROUP BY year
ORDER BY year;
```

---

### 5. `gold.export_by_country` - Экспорт по странам
**Записей:** 20 (детализация экспорта)

| Колонка | Тип | Описание | Power BI аналог |
|---------|-----|----------|-----------------|
| `year` | INT | Год отчёта | Год |
| `company_tin` | STRING | БИН компании | БИН |
| `company_name` | STRING | Название компании | Наименование компании |
| `country` | STRING | Страна экспорта | Страна |
| `export_amount` | BIGINT | Сумма экспорта | Сумма |

**Пример запроса:**
```sql
-- Топ-10 стран по экспорту в 2025
SELECT
    country,
    SUM(export_amount) as total_export,
    COUNT(DISTINCT company_tin) as companies_count
FROM iceberg.gold.export_by_country
WHERE year = 2025
GROUP BY country
ORDER BY total_export DESC
LIMIT 10;
```

---

## 🔌 Способы подключения

### Вариант 1: Metabase (Рекомендуется для начала)

**Доступ:**
- URL: http://109.248.170.228:30300/
- Login: `asd@sdfdf.com`
- Password: `AstanaHub!23`

**Инструкция:**
1. Зайти в Metabase
2. Создать SQL запрос к нужной витрине
3. Экспортировать результат в CSV: кнопка **Download results** → CSV
4. В Power BI Desktop: **Get Data** → **Text/CSV** → загрузить файл

**Преимущества:**
- ✅ Работает сразу
- ✅ Можно проверить данные перед импортом
- ✅ Не требует настройки драйверов

**Недостатки:**
- ❌ Ручное обновление данных
- ❌ Нужно пересоздавать CSV при каждом обновлении

---

### Вариант 2: JDBC подключение (Через DBeaver или аналоги)

**Параметры подключения:**
```
JDBC URL: jdbc:hive2://109.248.170.228:31000/iceberg
Host: 109.248.170.228
Port: 31000
Database: iceberg
Schema: gold
Driver: Apache Hive JDBC
Authentication: No Authentication
```

**Инструкция для DBeaver:**
1. Создать новое подключение → Apache Hive
2. Ввести параметры выше
3. Test Connection
4. Экспортировать данные в CSV
5. Загрузить в Power BI

---

### Вариант 3: ODBC (Требует настройки)

**Проблема:** Текущие ODBC драйверы (Cloudera, Simba) не работают с Spark Thrift Server.

**Возможные решения:**
1. Развернуть **Trino** (имеет лучшую поддержку ODBC)
2. Использовать **Databricks ODBC Driver** (платный)
3. Настроить **On-Premises Data Gateway** для Power BI Service

**Если драйвер заработает:**
```
Driver: Apache Spark ODBC Driver
Host: 109.248.170.228
Port: 31000
Database: iceberg.gold
```

---

### Вариант 4: REST API (Планируется)

Можно развернуть REST API который будет отдавать данные из Gold витрин через HTTP.

---

## 🔗 Relationships между таблицами

Создайте связи в Power BI по следующим ключам:

```
general_indicators.company_tin ←→ financing.company_tin
general_indicators.company_tin ←→ tax_benefits.company_tin
general_indicators.company_tin ←→ employees.company_tin
general_indicators.company_tin ←→ export_by_country.company_tin

general_indicators.year ←→ financing.year
general_indicators.year ←→ tax_benefits.year
general_indicators.year ←→ employees.year
general_indicators.year ←→ export_by_country.year

general_indicators.certificate_number ←→ financing.certificate_number
general_indicators.certificate_number ←→ tax_benefits.certificate_number
general_indicators.certificate_number ←→ employees.certificate_number
```

---

## 📈 SQL примеры запросов

### Пример 1: Сводка по годам
```sql
SELECT
    g.year,
    COUNT(DISTINCT g.company_tin) as companies_count,
    SUM(g.total_funding) as total_investments,
    SUM(g.export_income) as total_exports,
    SUM(t.total_tax_saved) as total_tax_saved,
    SUM(e.residents_count) as total_employees
FROM iceberg.gold.general_indicators g
LEFT JOIN iceberg.gold.tax_benefits t
    ON g.company_tin = t.company_tin AND g.year = t.year
LEFT JOIN iceberg.gold.employees e
    ON g.company_tin = e.company_tin AND g.year = e.year
GROUP BY g.year
ORDER BY g.year;
```

### Пример 2: Детальный отчёт по компании
```sql
SELECT
    g.company_name,
    g.company_tin,
    g.year,
    g.income_total,
    g.export_income,
    g.total_funding,
    t.total_tax_saved,
    e.residents_count,
    e.non_residents_count
FROM iceberg.gold.general_indicators g
LEFT JOIN iceberg.gold.tax_benefits t
    ON g.company_tin = t.company_tin AND g.year = t.year
LEFT JOIN iceberg.gold.employees e
    ON g.company_tin = e.company_tin AND g.year = e.year
WHERE g.company_tin = '130740020927'  -- Пример БИНа
ORDER BY g.year DESC;
```

### Пример 3: Экспорт по странам с компаниями
```sql
SELECT
    e.year,
    e.country,
    e.company_name,
    e.export_amount,
    g.income_total,
    ROUND(e.export_amount * 100.0 / NULLIF(g.income_total, 0), 2) as export_share_percent
FROM iceberg.gold.export_by_country e
JOIN iceberg.gold.general_indicators g
    ON e.company_tin = g.company_tin AND e.year = g.year
WHERE e.year = 2025
ORDER BY e.export_amount DESC;
```

---

## 🎯 Соответствие Power Query из Power BI

### ✅ Все 5 витрин полностью соответствуют Power Query скриптам:

| Power BI дашборд | Витрина Gold | Соответствие |
|------------------|--------------|--------------|
| Общие показатели | `gold.general_indicators` | ✅ 100% |
| Финансирование | `gold.financing` | ✅ 100% |
| Налоговые льготы | `gold.tax_benefits` | ✅ 100% |
| Работники | `gold.employees` | ✅ 100% |
| Экспорт по странам | `gold.export_by_country` | ✅ 100% |

### ⚠️ Важные отличия:

1. **Нет поля "Дата выдачи свидетельства участника"**
   - Это поле отсутствует в исходных данных Bronze
   - Рекомендация: убрать из дашборда или использовать `created_at`

2. **Данные только signed отчёты**
   - Все витрины фильтруют `status = 'signed'`
   - Черновики и rejected отчёты исключены

3. **Партиционирование по year**
   - Все таблицы партиционированы по году
   - Фильтрация по году работает максимально быстро

4. **Расчётные поля уже подсчитаны**
   - `attracted_investments` = уже рассчитано в SQL
   - Не нужно создавать вычисляемые столбцы в Power BI

---

## 🔍 Проверка данных

### Количество записей в каждой витрине:
```sql
SELECT 'general_indicators' as vitrina, COUNT(*) as records FROM iceberg.gold.general_indicators
UNION ALL
SELECT 'financing', COUNT(*) FROM iceberg.gold.financing
UNION ALL
SELECT 'tax_benefits', COUNT(*) FROM iceberg.gold.tax_benefits
UNION ALL
SELECT 'employees', COUNT(*) FROM iceberg.gold.employees
UNION ALL
SELECT 'export_by_country', COUNT(*) FROM iceberg.gold.export_by_country;
```

**Ожидаемый результат:**
```
general_indicators:   11,479
financing:            11,479
tax_benefits:         11,479
employees:            11,479
export_by_country:    20
```

### Распределение по годам:
```sql
SELECT
    year,
    COUNT(*) as reports_count
FROM iceberg.gold.general_indicators
GROUP BY year
ORDER BY year;
```

---

## 🛠 Troubleshooting

### Проблема: ODBC драйверы не работают
**Решение:** Используйте временно экспорт через Metabase в CSV

### Проблема: Нет данных за определённый год
**Проверка:**
```sql
SELECT year, COUNT(*)
FROM iceberg.silver.service_report_v2
WHERE status = 'signed'
GROUP BY year;
```

### Проблема: Разные количества записей в витринах
**Причина:** Это нормально. Например, `export_by_country` содержит только записи с экспортом (20 шт).

### Проблема: NULL значения в полях
**Причина:** Данные для старых лет (2019-2023) могут не иметь некоторых полей (например, `total_tax_saved` появился только в 2025).

**Проверка:**
```sql
SELECT
    year,
    COUNT(*) as total,
    SUM(CASE WHEN total_tax_saved IS NOT NULL THEN 1 ELSE 0 END) as has_tax_saved
FROM iceberg.gold.general_indicators
GROUP BY year
ORDER BY year;
```

---

## 📞 Контакты

**Техническая поддержка:**
- Metabase: http://109.248.170.228:30300/
- Spark Thrift Server: `109.248.170.228:31000`

**Документация:**
- Структура данных: `/techorda/spark-jobs/DATA_LAYERS.md`
- Скрипты ETL: `/techorda/spark-jobs/bronze_to_silver_v2.py`
- SQL витрин: `/techorda/spark-jobs/sql/`

---

## 🚀 Быстрый старт

1. **Зайти в Metabase**: http://109.248.170.228:30300/
2. **Выполнить тестовый запрос**:
   ```sql
   SELECT * FROM iceberg.gold.general_indicators
   WHERE year = 2025
   LIMIT 10;
   ```
3. **Экспортировать в CSV**
4. **Загрузить в Power BI Desktop**
5. **Создать relationships** между таблицами
6. **Построить дашборды** по примеру существующих Power Query

---

**Дата создания:** 2026-01-11
**Версия:** 1.0
**Статус:** Production Ready ✅
