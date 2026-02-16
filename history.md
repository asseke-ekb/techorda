# История работ — Techorda DWH

---

## Январь 2026

### Настройка CDC и Bronze слоя
- Настроен CDC через Debezium (PostgreSQL → Kafka → Bronze)
- Загружены 3 CDC-потока в Apache Iceberg:
  - `service_report_cdc` (~85K записей)
  - `service_servicerequest_cdc` (~196K записей)
  - `account_company_cdc`
- Добавлена обработка поля `op` (исключение удалённых записей `op <> 'd'`)

### Silver слой — 2 таблицы
- **`silver.service_report_v2`** (~30K записей, 2,983 компании)
  - Дедупликация CDC через `ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC)`
  - Парсинг JSON (`get_json_object`) — извлечение 30+ полей из `data`
  - Парсинг XML (`regexp_extract`) — извлечение БИН из `signed_xml`
  - Приведение типов (`CAST`)
  - Партиционирование по `year`
- **`silver.techpark_participants`** (~3,400 записей)
  - Участники технопарка из `service_servicerequest_cdc`
  - Фильтрация по `service_id = 'techpark'`, статус `signed`
  - Извлечение `certificate_number`, `certificate_issue_date`, `certificate_end_date`

### Gold слой — 6 витрин для Power BI
- **`gold.general_indicators`** (27,259) — общие показатели участников
- **`gold.financing`** (27,259) — финансирование (займы, инвестиции)
- **`gold.tax_benefits`** (27,259) — налоговые льготы (КПН, НДС, ИПН, СН)
- **`gold.employees`** (27,259) — занятость (резиденты, нерезиденты, ГПХ)
- **`gold.exports`** (27,259) — экспортная выручка
- **`gold.export_by_country`** (7,737) — экспорт по странам (explode JSON array)
- Трёхуровневый fallback для `certificate_number` через COALESCE:
  1. Из отчёта (`service_report_v2`)
  2. Из участников (`techpark_participants`)
  3. Из заявки (`service_servicerequest_cdc`)
- Результат: пустых сертификатов снизилось с 85 до 2

### Решённые проблемы с качеством данных
| Метрика | Значение |
|---------|----------|
| Заполненность BIN | 99.99% |
| Заполненность certificate_number | 99.99% |
| Маскирование БИН (2021+) | 100% |
| Пустых сертификатов | 2 из 27,259 |

### Документация
- Написан отчёт за январь (`reports/january-2026/report.md`)
- ERD диаграмма (`reports/january-2026/erd_diagram.md`)
- Скриншоты витрин из DBeaver (6 PNG файлов)

### SQL-скрипты
- `refresh_silver.sql` — начальная версия Silver
- `refresh_silver_v2.sql` — улучшенная версия Silver
- `refresh_silver_v2_masked.sql` — версия с маскированием
- `refresh_silver_v3_workaround.sql` — workaround для ограничений Spark
- `refresh_silver_companies.sql` — компании
- `refresh_silver_participants.sql` — участники технопарка
- `refresh_gold_v2.sql` — Gold витрины v2
- `refresh_gold_v3.sql` — Gold витрины v3
- `refresh_gold_v4.sql` — Gold витрины v4 (финальная)
- `refresh_gold_vitrinas.sql` — общий скрипт витрин
- `refresh_gold_export_by_country.sql` — экспорт по странам

---

## Февраль 2026

### Анализ систем для интеграции
- Проанализированы все 17 систем для интеграции с DWH
- Создан обзорный документ (`reports/systems_integration_overview.md`):
  - ARM Astana Hub (157 таблиц, 26 модулей) — подробная разбивка по модулям
  - Alaqan, AmoCRM, Techpreneurs, Documentolog, 1C УХ, Notion, Google Disk, Noco DB, Aiokk, Mitwork, Data Vera, Telegram, Google Analytics, Яндекс Метрика, Jira, Confluence
  - Этапы интеграции и бюджет (114.6 млн тг)

### Анализ API — Alaqan (СКУД)
- Получен API токен от поставщика
- Проанализированы 2 эндпоинта:
  - `GET /employees/byDate` — все посещения за дату
  - `GET /employees/byIin` — посещения по ИИН за период
- Авторизация: `X-Api-Key` header
- Формат ответа: JSON `{ИИН: [{event, time}]}`
- Базовый URL: `https://api.hub.alaqan.kz/api/astanahub/`

### Анализ API — AmoCRM
- Проанализирована документация AmoCRM API v4
- Endpoints: leads, contacts, companies, tasks, pipelines, events, catalogs, users
- Авторизация: OAuth 2.0
- Rate limit: 50 req/sec, макс 250 записей на страницу
- Тариф: от 599 руб/мес (API на всех тарифах)

### Документ интеграции Alaqan + AmoCRM
- Создан `reports/integration_alaqan_amocrm.md`:
  - Полное описание API обеих систем
  - Схемы Bronze/Silver/Gold для каждой
  - Стратегия загрузки (Alaqan — daily по byDate, AmoCRM — инкрементально по updated_at)
  - Связь с ARM (Alaqan через ИИН, AmoCRM через БИН)
  - Оценка сроков: Alaqan ~2 недели, AmoCRM ~3-5 недель

### Excel-файл системной интеграции
- Создан скрипт `reports/generate_excel.py` (генерирует `systems_integration.xlsx`)
- 6 листов:
  1. **ARM - Таблицы** — 26 модулей, 157 таблиц с описанием
  2. **Все системы** — 17 систем: тип интеграции, данные, блокеры, статус
  3. **Блокеры и риски** — 12 блокеров с критичностью и влиянием
  4. **Вопросы для обсуждения** — 22 вопроса по категориям (инфраструктура, доступы, команда, согласование)
  5. **Статус задач** — матрица: BRD / документация / доступы / Bronze / Silver / Gold / BI
  6. **Оценка сроков** — поэтапные оценки по каждой системе + сводка
- Обновления:
  - Alaqan: статус "Готов к интеграции" (API + токен получены)
  - AmoCRM: документация API готова, нужен OAuth токен
  - Добавлены вопросы по команде (Data Engineer, BI-разработчик)
  - Добавлены вопросы по инфраструктуре (Production, Airflow, RBAC)
  - Добавлены вопросы по согласованию (BRD, бюджет)

### Анализ Trello-доски проекта
- Проанализирована доска "Astana Hub Data Lake"
- Текущие задачи In Progress:
  - Согласование плана интеграций
  - Подписание договоров
  - Согласование бюджета
  - Разворачивание prod среды (DevOps)
  - Реализация требований ИБ
  - Концепция RBAC
  - Оценка объёма данных по всем источникам (Asset Slambekov)

### Шпаргалка для зумов
- Создан `reports/zoom_cheatsheet.md` — ответы на типичные вопросы:
  - Что сделано, какие трансформации, сколько таблиц
  - Как запускается ETL, какая модель данных
  - Качество данных, блокеры по системам
  - Вопросы для обсуждения, цифры наизусть

### Airflow (подготовка)
- Создана структура для Airflow:
  - `airflow/dags/` — папка для DAG-ов
  - `airflow/jobs/refresh_silver_reports.py` — PySpark job для Silver reports
  - `airflow/jobs/refresh_silver_participants.py` — PySpark job для Silver participants

### Статус ETL
- Создан `ETL_STATUS.md` — текущий статус ETL пайплайна
