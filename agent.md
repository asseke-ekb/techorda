# Techorda Data Platform - Agent Notes

## Доступы и подключения

### Argo Workflows
- **URL**: http://109.248.170.228:31366/
- **Token**: Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjBTMzJ1Y1JkQnZnekdQaGxpVTY2d0dyRm5BcVBOcHgzQWV1SXVFTXFtVzAifQ.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJhcmdvIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZWNyZXQubmFtZSI6ImplbmtpbnMuc2VydmljZS1hY2NvdW50LXRva2VuIiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQubmFtZSI6ImplbmtpbnMiLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiJhODAyN2RiMS1iZDIzLTRmOGEtOWRlMy04NWI5NWU2OWUyZmQiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6YXJnbzpqZW5raW5zIn0.pu15D8nW0BrKz7wMugl_e-zBtb6MdvH_F2waZwI8L1kuYYoZYBu4Lo1c40830xpcMIstTFMvVwty0VnLUFJS5TeBapGELPr1HSIz9h9Wwr2VhrOBi2wPkL9yQuLbWp2pL9dpSdkTCLGT0A0Loymh5p-QIsJDCRtG-1E0F4pLLEGZUV9ULxDboqtG4_wjOQ7gMEDzviLGZvhWSx2RvWztreG8ESRwPlt3rPlO-0hePvHD_jx6IZh7h98eEyZp2vTmpHE2zQ0CenCEi5gce5pwhzqCmlunQG11hlYE6JvrLHgP4c0DLlWIWzrahAzy7-Tzxr67ICBt_itrm7HGDL7f-Q
- **Namespace**: argo
- **Service Account**: jenkins

### Metabase
- **URL**: http://109.248.170.228:30300/
- **Login**: asd@sdfdf.com
- **Password**: AstanaHub!23

### Spark SQL (Thrift Server)
- **JDBC URL**: jdbc:hive2://109.248.170.228:31000
- **Host**: 109.248.170.228
- **Port**: 31000
- **Driver**: Apache Hive

### Hive Metastore
- **URI**: thrift://hive-metastore-service.iceberg-spark.svc.cluster.local:9083

### MinIO (S3)
- **User**: admin
- **Password**: 9xUwARJRU3TAYL4
- Креды уже прописаны в Spark

---

## Структура данных

### Bronze Layer
База: `iceberg.bronze`

#### Таблица: service_report_cdc
| Колонка | Тип | Описание |
|---------|-----|----------|
| op | string | Операция CDC |
| updated_ts | timestamp | Время обновления CDC |
| id | int | ID отчета |
| created_at | timestamp | Дата создания |
| updated_at | timestamp | Дата обновления |
| year | int | Год отчета |
| report_type | string | Тип отчета (quarter1, quarter2, etc.) |
| data | string | JSON с данными отчета |
| signature | string | JSON с подписями |
| status | string | Статус (signed, draft, etc.) |
| author_id | int | ID автора |
| service_request_id | int | ID заявки |
| viewed | array<int> | Кто просматривал |
| signed_at | timestamp | Дата подписания |
| version | string | Версия (v7, v8, etc.) |

#### Таблица: service_servicerequest_cdc
Заявки на услуги с полями: id, created_at, updated_at, status, search_field, assignee_id, author_id, hub_form_data_id, service_id, bp_status, data...

---

## Витрина в Metabase

**Название**: Витрина: Отчеты участников Technopark
**Card ID**: 38
**Записей**: 2000

### SQL запрос витрины:
```sql
SELECT
    r.id as report_id,
    r.year,
    r.report_type,
    r.status,
    r.signed_at,
    r.version,
    r.created_at,
    r.updated_at,
    r.service_request_id,
    r.author_id,

    -- Из JSON data
    get_json_object(r.data, '$.company_name') as company_name,
    get_json_object(r.data, '$.company_tin') as company_tin,
    CAST(get_json_object(r.data, '$.certificate_number') AS INT) as certificate_number,
    CAST(get_json_object(r.data, '$.residents_count') AS INT) as residents_count,
    CAST(get_json_object(r.data, '$.nonresidents_count') AS INT) as nonresidents_count,
    CAST(get_json_object(r.data, '$.gph_count') AS INT) as gph_count,
    CAST(get_json_object(r.data, '$.income_total') AS BIGINT) as income_total,
    CAST(get_json_object(r.data, '$.income_international') AS BIGINT) as income_international,
    CAST(get_json_object(r.data, '$.tax_incentives') AS BIGINT) as tax_incentives,
    -- ... и другие поля

FROM bronze.service_report_cdc r
WHERE r.status = 'signed' AND r.year >= 2019
ORDER BY r.year DESC, r.report_type, r.id DESC
```

### Поля витрины:
- **Основные**: report_id, year, report_type, status, signed_at, version
- **Компания**: company_name, company_tin, certificate_number
- **Сотрудники**: residents_count, nonresidents_count, gph_count
- **Доходы**: income_total, income_international, income_total_previous_quarter, income_total_current_quarter
- **Инвестиции**: investments_total_current_quarter, main_capital_investments, main_tangible/intangible_capital_investments
- **Инвесторы**: investor_amount, investor_country_company
- **Займы**: finance_source_loan, finance_source_loan_foreign
- **Господдержка**: finance_source_government, finance_source_investment
- **Налоги**: tax_incentives, tax_incentives_kpn/nds/ipn/sn, collection_amount
- **Прочее**: oked, executor_fullname, executor_phone
- **Флаги**: has_nonresidents, has_borrowed_funds, has_raised_investors_funds...

---

## Argo Workflow Templates

### spark-iceberg-job
Тестовый job с SparkPi

### spark-iceberg-template
Параметризованный шаблон:
- **main-class**: класс для запуска
- **main-jar**: jar файл

---

## TODO

### Для Silver слоя нужно:
1. [ ] Получить kubeconfig к кластеру
2. [ ] Права на создание таблиц в silver схеме
3. [x] S3 креды - уже в Spark
4. [ ] Создать DDL для Silver таблиц
5. [ ] Написать PySpark ETL Bronze -> Silver

### Созвон 12:30
- Сверка с BI аналитиком (@DiGab) по полям витрины
- Согласование структуры дашбордов

---

## Проблемы

### Spark Thrift Server
- DESCRIBE работает
- SELECT падает с `InstantiationException` - возможно не загружен iceberg-spark-runtime jar
- Metabase работает нормально (подключен через тот же Spark)

### Локальное подключение к Spark
- Порты 7077, 10000, 8080 не открыты наружу
- Только через NodePort 31000 (Thrift) или Argo Workflows
