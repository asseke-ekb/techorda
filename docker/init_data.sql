-- Drop if exists
DROP TABLE IF EXISTS iceberg.bronze.test_simple;
DROP TABLE IF EXISTS iceberg.bronze.service_report_cdc;

-- Create Bronze table
CREATE TABLE iceberg.bronze.service_report_cdc (
    id BIGINT,
    service_request_id BIGINT,
    year INT,
    report_type STRING,
    status STRING,
    op STRING,
    version INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    signed_at TIMESTAMP,
    author_id INT,
    data STRING
) USING iceberg
PARTITIONED BY (year);

-- Insert 10000 rows into Bronze
INSERT INTO iceberg.bronze.service_report_cdc
SELECT
    id,
    (id % 3000) + 1 as service_request_id,
    2024 as year,
    CASE WHEN id % 3 = 0 THEN 'annual' WHEN id % 3 = 1 THEN 'quarterly' ELSE 'monthly' END as report_type,
    CASE WHEN id % 5 = 0 THEN 'draft' WHEN id % 5 = 1 THEN 'submitted' WHEN id % 5 = 2 THEN 'approved' WHEN id % 5 = 3 THEN 'rejected' ELSE 'signed' END as status,
    CASE WHEN id % 20 = 0 THEN 'd' WHEN id % 10 = 0 THEN 'u' ELSE 'c' END as op,
    (id % 5) + 1 as version,
    current_timestamp() as created_at,
    current_timestamp() as updated_at,
    NULL as signed_at,
    (id % 100) + 1 as author_id,
    CONCAT('{"company_tin":"', LPAD(CAST((id % 100000) AS STRING), 12, '0'), '"}') as data
FROM (SELECT explode(sequence(1, 10000)) as id);

-- Create Silver table
CREATE TABLE iceberg.silver.service_report (
    report_id INT,
    service_request_id INT,
    year INT,
    report_type STRING,
    status STRING,
    company_tin STRING,
    etl_loaded_at TIMESTAMP
) USING iceberg
PARTITIONED BY (year);

-- Insert ~8500 rows (exclude deleted op='d')
INSERT INTO iceberg.silver.service_report
SELECT
    CAST(id AS INT) as report_id,
    CAST(service_request_id AS INT),
    year,
    report_type,
    status,
    get_json_object(data, '$.company_tin') as company_tin,
    current_timestamp() as etl_loaded_at
FROM iceberg.bronze.service_report_cdc
WHERE op != 'd';
