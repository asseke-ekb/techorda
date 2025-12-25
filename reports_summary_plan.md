# Задача: reports_summary в Data Lake

## Вопросы к команде Data

1. **Можете добавить CDC для таблицы `service_report`?**
   - Сейчас её нет в Iceberg
   - Нужна для квартальных отчётов участников (не TechOrda)

2. **Как запускаете Spark Jobs?**
   - PySpark или Scala?
   - Через spark-submit в Argo Workflows?
   - Есть пример существующего job'а?

3. **Куда класть скрипты?**
   - Git репозиторий (какой)?
   - Docker образ?

4. **Схема `gold` существует в Iceberg?**
   - Если нет - нужно создать для агрегированных таблиц

---

## Текущая ситуация

В Iceberg загружены таблицы:
- `bronze.service_servicerequest_cdc` - заявки
- `bronze.service_techordareport_cdc` - отчёты TechOrda (студенты/курсы)
- `bronze.service_techordareportstudent_cdc` - студенты

**Проблема:** Для `reports_summary` нужна таблица `service_report` (квартальные отчёты участников) - её нет.

---

## План после получения ответов

### Шаг 1: Написать PySpark скрипт
```
bronze.service_report_cdc → [агрегация] → gold.reports_summary
```

Логика из Django кода:
- Фильтр: `status = 'signed'`
- Группировка: по `year` + `report_type`
- Агрегация полей из `data` (JSON):
  - residents_count
  - nonresidents_count
  - income_total
  - income_international
  - finance_source_increase_authorized_capital
  - main_capital_investments
  - finance_source_loan
  - finance_source_loan_foreign
  - finance_source_government
  - finance_source_investment
  - investor_amount
  - tax_incentives

### Шаг 2: Настроить Argo Workflow
- Запуск по расписанию (раз в день/час)

### Шаг 3: Подключить Metabase
- Дашборд на основе `gold.reports_summary`

---

## Архитектура

```
PostgreSQL (service_report)
    │
    │ CDC (Debezium)
    ▼
Iceberg bronze.service_report_cdc
    │
    │ Spark Job (PySpark)
    ▼
Iceberg gold.reports_summary
    │
    │ SQL
    ▼
Metabase (дашборды)
```

---

## Доступы

- Metabase: http://109.248.170.228:30300/
- Argo Workflows: http://109.248.170.228:31366/
- Spark Thrift Server: внутри кластера на порту 10000
