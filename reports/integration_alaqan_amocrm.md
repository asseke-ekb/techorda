# Интеграция систем с DWH: Alaqan и AmoCRM

---

## 1. ALAQAN

### 1.1. Описание системы

**Назначение:** Система контроля доступа (СКУД) Astana Hub. Фиксирует посещения объекта сотрудниками/резидентами по ИИН.

**Тип интеграции:** REST API
**Авторизация:** API Key (заголовок `X-Api-Key`)
**Базовый URL:** `https://api.hub.alaqan.kz/api/astanahub/`
**Токен:** `<REDACTED — хранится в .env>`

### 1.2. Доступные API endpoints

| № | Endpoint | Метод | Описание | Параметры |
|---|----------|-------|----------|-----------|
| 1 | `/employees/byIin` | GET | Посещения конкретного сотрудника по ИИН за период | `start_date`, `end_date`, `iin` |
| 2 | `/employees/byDate` | GET | Все посещения за конкретную дату | `date` |

### 1.3. Структура ответа API

**Endpoint 1: byIin** — посещения одного сотрудника за период
```json
{
  "data": {
    "2025-01-30": [{"event": "entry", "time": "09:15:23"}],
    "2025-01-28": [{"event": "exit", "time": "18:30:45"}],
    "2025-01-27": [{"event": "entry", "time": "08:55:10"}, {"event": "exit", "time": "19:00:33"}]
  }
}
```

**Endpoint 2: byDate** — все посещения за одну дату (ключ = ИИН)
```json
{
  "data": {
    "650727300949": [
      {"event": "exit", "time": "20:10:56"},
      {"event": "exit", "time": "12:34:27"},
      {"event": "entry", "time": "08:45:00"}
    ],
    "910505350319": [
      {"event": "entry", "time": "09:00:15"},
      {"event": "exit", "time": "18:30:00"}
    ]
  }
}
```

### 1.4. Стратегия загрузки в DWH

**Рекомендация:** Использовать endpoint `/employees/byDate` для ежедневной выгрузки.

**Почему:**
- `byDate` выгружает ВСЕ посещения за день за один запрос
- `byIin` требует знать ИИН каждого сотрудника — нужен справочник + N запросов
- Для исторической загрузки: цикл по дням от начала периода до текущей даты

**Периодичность:** Ежедневно (daily), инкрементально — загружаем данные за вчера.

**Стратегия:**
- Первая загрузка: Full Load — пройтись по всем дням от начала (например, 2024-01-01)
- Далее: Incremental Load — ежедневно подгружать данные за предыдущий день

### 1.5. Схема данных: Bronze → Silver → Gold

#### Bronze (сырые данные из API)

```sql
CREATE TABLE iceberg.bronze.alaqan_visits (
    load_date       DATE,           -- дата загрузки (техническое поле)
    visit_date      DATE,           -- дата посещения (ключ из JSON)
    iin             STRING,         -- ИИН сотрудника (ключ из JSON)
    event           STRING,         -- тип события: entry / exit
    event_time      STRING,         -- время события: HH:MM:SS
    raw_json        STRING,         -- полный JSON ответа (для аудита)
    loaded_at       TIMESTAMP       -- timestamp загрузки
) USING iceberg
PARTITIONED BY (visit_date);
```

**Трансформация API → Bronze:**
- Получаем JSON от `/employees/byDate?date=YYYY-MM-DD`
- Разворачиваем вложенную структуру: ключ (ИИН) → массив событий
- Каждое событие = одна строка в Bronze

#### Silver (очищенные данные)

```sql
CREATE TABLE iceberg.silver.alaqan_visits (
    visit_date      DATE,           -- дата посещения
    iin             STRING,         -- ИИН сотрудника
    event           STRING,         -- entry / exit
    event_time      TIME,           -- время (приведённое)
    event_datetime  TIMESTAMP,      -- visit_date + event_time (полный timestamp)
    loaded_at       TIMESTAMP       -- timestamp загрузки
) USING iceberg
PARTITIONED BY (visit_date);
```

**Трансформации Bronze → Silver:**
- Приведение типов: `event_time` STRING → TIME
- Формирование `event_datetime` = `visit_date` + `event_time`
- Дедупликация (если одно и то же событие пришло дважды)
- Валидация: проверка формата ИИН (12 цифр), фильтрация мусорных записей

#### Gold (витрины для BI)

**Витрина 1: Ежедневная посещаемость**
```sql
CREATE TABLE iceberg.gold.alaqan_daily_attendance (
    visit_date          DATE,
    iin                 STRING,
    first_entry         TIMESTAMP,      -- первый вход за день
    last_exit           TIMESTAMP,      -- последний выход за день
    total_entries       INT,            -- кол-во входов
    total_exits         INT,            -- кол-во выходов
    hours_on_site       DECIMAL(5,2),   -- время на объекте (часы)
    year                INT             -- партиция
) USING iceberg
PARTITIONED BY (year);
```

**Витрина 2: Сводка по месяцам**
```sql
CREATE TABLE iceberg.gold.alaqan_monthly_summary (
    year                INT,
    month               INT,
    iin                 STRING,
    total_days_visited  INT,            -- кол-во дней посещения
    avg_hours_per_day   DECIMAL(5,2),   -- среднее время на объекте
    total_hours         DECIMAL(7,2),   -- всего часов за месяц
    first_visit_date    DATE,           -- первый визит в месяце
    last_visit_date     DATE            -- последний визит в месяце
) USING iceberg
PARTITIONED BY (year);
```

### 1.6. Связь с ARM

Alaqan содержит ИИН сотрудников. Для связи с данными ARM (где есть БИН компаний) потребуется справочник **ИИН → БИН компании**. Возможные источники:
- `account_user` (ARM) — если содержит ИИН
- `account_usercompany` (ARM) — связь пользователь ↔ компания
- Отдельный справочник от заказчика

### 1.7. Вопросы для уточнения

| № | Вопрос | Зачем |
|---|--------|-------|
| 1 | Какой период исторических данных нужен? | Определить объём первой загрузки |
| 2 | Есть ли справочник ИИН → компания/сотрудник? | Для связи посещений с резидентами |
| 3 | Какие витрины нужны бизнесу? | Уточнить Gold-слой |
| 4 | Нужно ли маскировать ИИН в DWH? | Требования ИБ |
| 5 | API имеет rate limits? | Планирование загрузки |

---

## 2. AmoCRM

### 2.1. Описание системы

**Назначение:** CRM-система для управления продажами, клиентами и воронками.

**Тип интеграции:** REST API v4
**Авторизация:** OAuth 2.0 (рекомендуется) / API Key (legacy)
**Базовый URL:** `https://{subdomain}.amocrm.ru/api/v4/`
**Документация:** https://www.amocrm.ru/developers/content/crm_platform/api-reference
**Тарифы:** Все тарифы (от 599 руб/мес) включают REST API
**Rate limit:** 50 запросов/сек на аккаунт, макс. 250 записей на страницу

### 2.2. Доступные API endpoints

| № | Endpoint | Метод | Описание | Ключевые поля |
|---|----------|-------|----------|---------------|
| 1 | `/api/v4/leads` | GET | Сделки (воронка продаж) | id, name, price, status_id, pipeline_id, created_at, closed_at, custom_fields_values |
| 2 | `/api/v4/contacts` | GET | Контакты (физ. лица) | id, name, first_name, last_name, custom_fields_values |
| 3 | `/api/v4/companies` | GET | Компании (юр. лица) | id, name, custom_fields_values |
| 4 | `/api/v4/tasks` | GET | Задачи менеджеров | id, text, entity_id, entity_type, is_completed, complete_till, task_type_id |
| 5 | `/api/v4/leads/pipelines` | GET | Воронки и статусы | id, name, statuses[], is_main |
| 6 | `/api/v4/events` | GET | Журнал событий | id, type, entity_id, created_at |
| 7 | `/api/v4/catalogs` | GET | Пользовательские каталоги (списки) | id, name, type |
| 8 | `/api/v4/users` | GET | Пользователи CRM | id, name, email, rights |

### 2.3. Структура основных сущностей

#### Leads (Сделки)
```json
{
  "id": 12345,
  "name": "Заявка на участие",
  "price": 500000,
  "responsible_user_id": 100,
  "status_id": 142,
  "pipeline_id": 33,
  "loss_reason_id": null,
  "created_by": 100,
  "updated_by": 100,
  "created_at": 1706745600,
  "updated_at": 1706832000,
  "closed_at": null,
  "is_deleted": false,
  "custom_fields_values": [
    {"field_id": 501, "field_name": "БИН", "values": [{"value": "123456789012"}]},
    {"field_id": 502, "field_name": "Источник", "values": [{"value": "Сайт"}]}
  ],
  "score": null,
  "labor_cost": 3600,
  "_embedded": {
    "tags": [{"id": 1, "name": "VIP"}],
    "contacts": [{"id": 5001}],
    "companies": [{"id": 7001}]
  }
}
```

#### Contacts (Контакты)
```json
{
  "id": 5001,
  "name": "Иванов Иван",
  "first_name": "Иван",
  "last_name": "Иванов",
  "responsible_user_id": 100,
  "created_at": 1706745600,
  "updated_at": 1706832000,
  "custom_fields_values": [
    {"field_id": 601, "field_name": "Телефон", "values": [{"value": "+77001234567"}]},
    {"field_id": 602, "field_name": "Email", "values": [{"value": "ivanov@mail.kz"}]}
  ],
  "_embedded": {
    "companies": [{"id": 7001}],
    "leads": [{"id": 12345}]
  }
}
```

#### Companies (Компании)
```json
{
  "id": 7001,
  "name": "ТОО Технокомпания",
  "responsible_user_id": 100,
  "created_at": 1706745600,
  "updated_at": 1706832000,
  "custom_fields_values": [
    {"field_id": 701, "field_name": "БИН", "values": [{"value": "123456789012"}]},
    {"field_id": 702, "field_name": "Сфера", "values": [{"value": "IT"}]}
  ]
}
```

### 2.4. Стратегия загрузки в DWH

**Подход:** Инкрементальная загрузка по `updated_at`.

**Порядок загрузки (зависимости):**
1. **Pipelines + Statuses** (справочники) — редко меняются, Full Load
2. **Users** (справочник) — Full Load
3. **Companies** — Incremental по updated_at
4. **Contacts** — Incremental по updated_at
5. **Leads** — Incremental по updated_at (с `?with=contacts,companies`)
6. **Tasks** — Incremental по updated_at
7. **Events** — Incremental по created_at

**Пагинация:** макс. 250 записей на страницу, использовать `page` + `limit`.

**Периодичность:** Ежедневно или каждые 4 часа (зависит от бизнес-требований).

### 2.5. Схема данных: Bronze → Silver → Gold

#### Bronze (сырые данные из API)

```sql
-- Сделки
CREATE TABLE iceberg.bronze.amocrm_leads (
    id                    INT,
    name                  STRING,
    price                 BIGINT,
    responsible_user_id   INT,
    status_id             INT,
    pipeline_id           INT,
    loss_reason_id        INT,
    created_by            INT,
    updated_by            INT,
    created_at            BIGINT,         -- Unix timestamp
    updated_at            BIGINT,
    closed_at             BIGINT,
    is_deleted            BOOLEAN,
    custom_fields_values  STRING,         -- JSON array
    score                 INT,
    labor_cost            INT,
    tags_json             STRING,         -- JSON (embedded.tags)
    contacts_json         STRING,         -- JSON (embedded.contacts)
    companies_json        STRING,         -- JSON (embedded.companies)
    loaded_at             TIMESTAMP
) USING iceberg;

-- Контакты
CREATE TABLE iceberg.bronze.amocrm_contacts (
    id                    INT,
    name                  STRING,
    first_name            STRING,
    last_name             STRING,
    responsible_user_id   INT,
    created_by            INT,
    updated_by            INT,
    created_at            BIGINT,
    updated_at            BIGINT,
    is_deleted            BOOLEAN,
    custom_fields_values  STRING,         -- JSON array
    companies_json        STRING,         -- JSON (embedded.companies)
    leads_json            STRING,         -- JSON (embedded.leads)
    loaded_at             TIMESTAMP
) USING iceberg;

-- Компании
CREATE TABLE iceberg.bronze.amocrm_companies (
    id                    INT,
    name                  STRING,
    responsible_user_id   INT,
    created_by            INT,
    updated_by            INT,
    created_at            BIGINT,
    updated_at            BIGINT,
    is_deleted            BOOLEAN,
    custom_fields_values  STRING,         -- JSON array
    contacts_json         STRING,
    leads_json            STRING,
    loaded_at             TIMESTAMP
) USING iceberg;

-- Задачи
CREATE TABLE iceberg.bronze.amocrm_tasks (
    id                    INT,
    text                  STRING,
    responsible_user_id   INT,
    entity_id             INT,
    entity_type           STRING,         -- leads, contacts, companies
    is_completed          BOOLEAN,
    task_type_id          INT,            -- 1=Звонок, 2=Встреча
    complete_till         BIGINT,
    result_text           STRING,
    created_at            BIGINT,
    updated_at            BIGINT,
    loaded_at             TIMESTAMP
) USING iceberg;

-- Воронки (справочник)
CREATE TABLE iceberg.bronze.amocrm_pipelines (
    id                    INT,
    name                  STRING,
    is_main               BOOLEAN,
    statuses_json         STRING,         -- JSON array статусов
    loaded_at             TIMESTAMP
) USING iceberg;

-- Пользователи (справочник)
CREATE TABLE iceberg.bronze.amocrm_users (
    id                    INT,
    name                  STRING,
    email                 STRING,
    rights_json           STRING,
    loaded_at             TIMESTAMP
) USING iceberg;
```

#### Silver (очищенные данные)

```sql
-- Сделки (очищенные)
CREATE TABLE iceberg.silver.amocrm_leads (
    lead_id               INT,
    lead_name             STRING,
    price                 BIGINT,
    responsible_user_id   INT,
    responsible_user_name STRING,         -- JOIN с users
    status_id             INT,
    status_name           STRING,         -- JOIN с pipelines.statuses
    pipeline_id           INT,
    pipeline_name         STRING,         -- JOIN с pipelines
    is_won                BOOLEAN,        -- status = "Успешно реализовано"
    is_lost               BOOLEAN,        -- status = "Закрыто и не реализовано"
    loss_reason           STRING,
    created_at            TIMESTAMP,      -- из Unix timestamp
    updated_at            TIMESTAMP,
    closed_at             TIMESTAMP,
    is_deleted            BOOLEAN,
    -- custom fields (распарсенные)
    cf_bin                STRING,         -- БИН из custom_fields
    cf_source             STRING,         -- источник сделки
    -- связи
    contact_ids           ARRAY<INT>,     -- ID связанных контактов
    company_ids           ARRAY<INT>,     -- ID связанных компаний
    tags                  ARRAY<STRING>,  -- теги
    year                  INT             -- EXTRACT(YEAR FROM created_at)
) USING iceberg
PARTITIONED BY (year);

-- Контакты (очищенные)
CREATE TABLE iceberg.silver.amocrm_contacts (
    contact_id            INT,
    full_name             STRING,
    first_name            STRING,
    last_name             STRING,
    responsible_user_name STRING,
    phone                 STRING,         -- из custom_fields (маскировать!)
    email                 STRING,         -- из custom_fields (маскировать!)
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    is_deleted            BOOLEAN,
    company_ids           ARRAY<INT>,
    lead_ids              ARRAY<INT>
) USING iceberg;

-- Компании (очищенные)
CREATE TABLE iceberg.silver.amocrm_companies (
    company_id            INT,
    company_name          STRING,
    responsible_user_name STRING,
    bin                   STRING,         -- БИН из custom_fields
    industry              STRING,         -- сфера из custom_fields
    created_at            TIMESTAMP,
    updated_at            TIMESTAMP,
    is_deleted            BOOLEAN
) USING iceberg;

-- Справочник воронок и статусов (flat)
CREATE TABLE iceberg.silver.amocrm_pipeline_statuses (
    pipeline_id           INT,
    pipeline_name         STRING,
    is_main_pipeline      BOOLEAN,
    status_id             INT,
    status_name           STRING,
    status_sort           INT,
    status_color          STRING
) USING iceberg;
```

**Трансформации Bronze → Silver:**
- Unix timestamp → TIMESTAMP: `FROM_UNIXTIME(created_at)`
- Парсинг `custom_fields_values` JSON → извлечение конкретных полей (БИН, телефон, email и т.д.)
- Парсинг `_embedded` JSON → массивы ID связанных сущностей
- JOIN с справочниками (users → имена, pipelines → названия)
- Маскирование ПД: телефоны, email (если требуется ИБ)
- Дедупликация по `id` + `updated_at` (берём последнюю версию)

#### Gold (витрины для BI)

**Витрина 1: Воронка продаж (Sales Funnel)**
```sql
CREATE TABLE iceberg.gold.amocrm_sales_funnel (
    year                  INT,
    month                 INT,
    pipeline_name         STRING,
    status_name           STRING,
    status_sort           INT,
    leads_count           INT,            -- кол-во сделок
    total_price           BIGINT,         -- сумма сделок
    avg_price             BIGINT,         -- средний чек
    won_count             INT,            -- выиграно
    lost_count            INT,            -- проиграно
    conversion_rate       DECIMAL(5,2)    -- % конверсии
) USING iceberg
PARTITIONED BY (year);
```

**Витрина 2: Активность менеджеров**
```sql
CREATE TABLE iceberg.gold.amocrm_manager_activity (
    year                  INT,
    month                 INT,
    responsible_user_name STRING,
    leads_created         INT,            -- создано сделок
    leads_closed_won      INT,            -- успешно закрыто
    leads_closed_lost     INT,            -- проиграно
    total_revenue         BIGINT,         -- выручка
    tasks_completed       INT,            -- задач выполнено
    avg_deal_cycle_days   INT             -- средний цикл сделки (дни)
) USING iceberg
PARTITIONED BY (year);
```

**Витрина 3: Аналитика клиентов**
```sql
CREATE TABLE iceberg.gold.amocrm_client_analytics (
    company_id            INT,
    company_name          STRING,
    bin                   STRING,
    total_leads           INT,
    won_leads             INT,
    lost_leads            INT,
    total_revenue         BIGINT,
    first_deal_date       DATE,
    last_deal_date        DATE,
    avg_deal_price        BIGINT,
    tags                  STRING
) USING iceberg;
```

### 2.6. ERD: Связи сущностей AmoCRM

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────┐
│  Pipelines   │     │     Leads        │     │   Contacts   │
│──────────────│     │──────────────────│     │──────────────│
│ id           │←────│ pipeline_id      │     │ id           │
│ name         │     │ id               │←───→│ name         │
│ statuses[]   │     │ name, price      │     │ phone, email │
└──────────────┘     │ status_id        │     │ company_ids  │
                     │ contact_ids[]    │────→│              │
┌──────────────┐     │ company_ids[]    │     └──────────────┘
│   Users      │     │ responsible_user │
│──────────────│     │ custom_fields    │     ┌──────────────┐
│ id           │←────│ created_at       │     │  Companies   │
│ name         │     │ closed_at        │     │──────────────│
│ email        │     └──────────────────┘     │ id           │
└──────────────┘              │               │ name, БИН    │
                              │               │ contact_ids  │
                     ┌────────┘               └──────────────┘
                     ↓                               ↑
              ┌──────────────┐                       │
              │    Tasks     │               Leads.company_ids
              │──────────────│
              │ id           │
              │ entity_id    │──→ leads / contacts / companies
              │ entity_type  │
              │ is_completed │
              │ text         │
              └──────────────┘
```

### 2.7. Связь AmoCRM с ARM

Ключ связи: **БИН компании**
- AmoCRM: `companies.custom_fields` → поле "БИН"
- ARM: `silver.service_report_v2.company_tin` (БИН)

Это позволит:
- Сопоставить сделки AmoCRM с резидентами технопарка
- Обогатить данные воронки информацией о статусе резидентства
- Анализировать корреляцию: активность в CRM ↔ финансовые показатели ARM

### 2.8. Вопросы для уточнения

| № | Вопрос | Зачем |
|---|--------|-------|
| 1 | Какой subdomain AmoCRM? (xxx.amocrm.ru) | Для настройки API |
| 2 | Какие custom_fields настроены? | Для маппинга полей в Silver |
| 3 | Сколько сделок в системе? | Оценка объёма Bronze |
| 4 | Какие воронки (pipelines) используются? | Для Gold-витрины воронки |
| 5 | Нужно ли маскировать контакты (телефон, email)? | Требования ИБ |
| 6 | OAuth 2.0 — кто создаст интеграцию в AmoCRM? | Для получения токена |
| 7 | Какая частота синхронизации нужна? (daily / hourly) | Настройка Airflow |

---

## 3. Сводная таблица: Alaqan vs AmoCRM

| Параметр | Alaqan | AmoCRM |
|----------|--------|--------|
| **Тип API** | REST (API Key) | REST v4 (OAuth 2.0) |
| **Готовность API** | Готов, токен есть | Готов, нужен OAuth |
| **Кол-во endpoints** | 2 | 8+ |
| **Кол-во таблиц Bronze** | 1 | 6 |
| **Кол-во таблиц Silver** | 1 | 4 |
| **Кол-во витрин Gold** | 2 | 3 |
| **Сложность** | Низкая | Средняя |
| **Оценка (дни)** | 5-7 | 15-20 |
| **Связь с ARM** | ИИН (нужен справочник) | БИН (через custom_fields) |
| **Маскирование** | ИИН | Телефоны, email |
| **Блокер** | Нет | OAuth токен + subdomain |

---

## 4. План работ

### Alaqan (5-7 дней)
1. **День 1:** Тестирование API, пробная выгрузка за 1 день
2. **День 2-3:** Скрипт выгрузки (Python) + загрузка в Bronze
3. **День 4:** Silver трансформации (SQL)
4. **День 5-6:** Gold витрины + проверка
5. **День 7:** Документация, подключение BI

### AmoCRM (15-20 дней)
1. **День 1-2:** Получить OAuth токен, тестирование API, маппинг custom_fields
2. **День 3-5:** Скрипты выгрузки всех сущностей → Bronze (6 таблиц)
3. **День 6-8:** Silver трансформации (парсинг JSON, JOIN справочников, маскирование)
4. **День 9-12:** Gold витрины (воронка, активность, клиенты)
5. **День 13-15:** Тестирование, проверка качества данных
6. **День 16-20:** Документация, подключение BI, доработки

---

## 5. Airflow DAG (планируемый)

```
alaqan_daily_load    →    alaqan_silver    →    alaqan_gold
                                                     ↓
amocrm_load_pipelines ─┐                      BI refresh
amocrm_load_users ─────┤
amocrm_load_companies ─┤→ amocrm_silver → amocrm_gold
amocrm_load_contacts ──┤                      ↓
amocrm_load_leads ─────┤                 BI refresh
amocrm_load_tasks ─────┘
```
