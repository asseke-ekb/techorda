# Entity-Relationship Diagram (ERD) - Techorda Data Warehouse

## Схема данных: Bronze → Silver → Gold

```mermaid
graph TB
    subgraph "ARM PostgreSQL"
        ARM[("ARM Database<br/>(источник)")]
    end

    subgraph "Bronze Layer (Iceberg)"
        BR1["bronze.service_report_cdc<br/>~85K записей<br/>────────────────<br/>• id (PK)<br/>• service_request_id (FK)<br/>• year<br/>• report_type<br/>• status<br/>• data (JSON)<br/>• signature (JSON→XML)<br/>• updated_ts<br/>• op (CDC operation)"]

        BR2["bronze.service_servicerequest_cdc<br/>~196K записей<br/>────────────────<br/>• id (PK)<br/>• service_id<br/>• company_id<br/>• bp_status<br/>• data (JSON)<br/>• updated_at"]
    end

    subgraph "Silver Layer (Iceberg)"
        SL1["silver.service_report_v2<br/>~30K записей, 2,983 компании<br/>────────────────<br/>• report_id (PK)<br/>• service_request_id (FK)<br/>• year (PARTITION KEY)<br/>• report_type<br/>• company_tin (БИН)<br/>• company_name<br/>• certificate_number<br/>• income_total<br/>• tax_incentives_kpn/nds/ipn/sn<br/>• residents_count<br/>• ...30+ финансовых полей"]

        SL2["silver.techpark_participants<br/>~3,400 записей<br/>────────────────<br/>• service_request_id (PK, FK)<br/>• company_id<br/>• bp_status<br/>• certificate_number<br/>• certificate_issue_date<br/>• certificate_end_date<br/>• company_tin<br/>• company_name"]
    end

    subgraph "Gold Layer (Iceberg) - Витрины для BI"
        GL1["general_indicators<br/>27,259 записей<br/>────────────────<br/>Общие показатели:<br/>• certificate_number<br/>• bin, company_name<br/>• income_total<br/>• tax_saved<br/>• export_income<br/>• total_funding<br/>PARTITION BY (year)"]

        GL2["financing<br/>27,259 записей<br/>────────────────<br/>Финансирование:<br/>• loan_funds<br/>• authorized_capital<br/>• attracted_investments<br/>PARTITION BY (year)"]

        GL3["tax_benefits<br/>27,259 записей<br/>────────────────<br/>Налоговые льготы:<br/>• kpn, nds<br/>• ipn, sn<br/>• total_tax_saved<br/>PARTITION BY (year)"]

        GL4["employees<br/>27,259 записей<br/>────────────────<br/>Занятость:<br/>• residents_count<br/>• nonresidents_count<br/>• gph_count<br/>PARTITION BY (year)"]

        GL5["exports<br/>27,259 записей<br/>────────────────<br/>Экспорт:<br/>• export_income<br/>PARTITION BY (year)"]

        GL6["export_by_country<br/>7,737 записей<br/>────────────────<br/>Экспорт по странам:<br/>• country<br/>• export_income<br/>(JSON array explode)<br/>PARTITION BY (year)"]
    end

    subgraph "BI Layer"
        BI["Power BI / Metabase<br/>────────────────<br/>Дашборды и отчёты"]
    end

    %% Связи Bronze → Silver
    ARM -->|"CDC (Debezium)"| BR1
    ARM -->|"CDC (Debezium)"| BR2

    BR1 -->|"Дедупликация (ROW_NUMBER)<br/>Парсинг JSON/XML (regex)<br/>Маскирование БИН<br/>CAST типов"| SL1

    BR2 -->|"Фильтр: service_id='techpark'<br/>bp_status IN ('registered', 'deactivated')<br/>Извлечение certificate"| SL2

    %% Связи Silver → Gold
    SL1 -->|"JOIN (service_request_id)<br/>COALESCE (3-level fallback)<br/>WHERE status='signed'"| GL1
    SL1 --> GL2
    SL1 --> GL3
    SL1 --> GL4
    SL1 --> GL5
    SL1 --> GL6

    SL2 -->|"LEFT JOIN<br/>(certificate_number fallback)"| GL1
    SL2 --> GL2
    SL2 --> GL3
    SL2 --> GL4
    SL2 --> GL5
    SL2 --> GL6

    BR2 -.->|"Fallback для<br/>certificate_number"| GL1

    %% Связи Gold → BI
    GL1 --> BI
    GL2 --> BI
    GL3 --> BI
    GL4 --> BI
    GL5 --> BI
    GL6 --> BI

    %% Стили
    classDef bronzeStyle fill:#e8976c,stroke:#d97542,stroke-width:2px,color:#000
    classDef silverStyle fill:#c0c0c0,stroke:#999,stroke-width:2px,color:#000
    classDef goldStyle fill:#ffd700,stroke:#ccaa00,stroke-width:2px,color:#000
    classDef biStyle fill:#90EE90,stroke:#6B8E23,stroke-width:2px,color:#000
    classDef armStyle fill:#87CEEB,stroke:#4682B4,stroke-width:2px,color:#000

    class BR1,BR2 bronzeStyle
    class SL1,SL2 silverStyle
    class GL1,GL2,GL3,GL4,GL5,GL6 goldStyle
    class BI biStyle
    class ARM armStyle
```

## Ключевые трансформации

### Bronze → Silver

**1. service_report_cdc → service_report_v2**
- Дедупликация через `ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_ts DESC)`
- Парсинг БИН из XML: `regexp_extract(get_json_object(signature, '$.signed_xml'), '<tin>([0-9]+)</tin>', 1)`
- Извлечение 30+ финансовых показателей из JSON с `CAST AS BIGINT`
- Фильтрация: `op != 'd'` (исключение удалённых записей)

**2. service_servicerequest_cdc → techpark_participants**
- Фильтр: `service_id = 'techpark'` AND `bp_status IN ('registered', 'deactivated')`
- Извлечение сертификата: `get_json_object(data, '$.activation_info.certificate_number')`
- Парсинг БИН из search_field: `regexp_extract(search_field, 'БИН:\s*(\d+)', 1)`

### Silver → Gold

**3-уровневый COALESCE для certificate_number:**
```sql
COALESCE(
    NULLIF(sr.certificate_number, ''),      -- 1. Из отчёта
    p.certificate_number,                    -- 2. Из participants
    NULLIF(ss.cert_number, '-1')            -- 3. Из servicerequest (fallback)
)
```

**Результат:** Заполненность сертификатов увеличена с 85 пустых → 2 пустых (99.99%)

## Связи между таблицами

| Связь | Кардинальность | Тип JOIN | Назначение |
|-------|---------------|----------|------------|
| `silver.service_report_v2.service_request_id` → `silver.techpark_participants.service_request_id` | 1:1 | LEFT JOIN | Получение certificate_number |
| `silver.service_report_v2.service_request_id` → `bronze.service_servicerequest_cdc.id` | 1:1 | LEFT JOIN | Fallback для certificate_number |
| `silver.service_report_v2` → все Gold витрины | 1:N | FROM | Основной источник данных |

## Партиционирование

Все Gold-витрины партиционированы по полю `year` (INT) для оптимизации запросов:
- Годы: 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026
- Формат: `PARTITIONED BY (year)` (Apache Iceberg)

---

**Инструменты:**
- CDC: Debezium
- Storage: Apache Iceberg
- Engine: Apache Spark SQL
- Orchestration (planned): Apache Airflow
- BI: Power BI, Metabase
