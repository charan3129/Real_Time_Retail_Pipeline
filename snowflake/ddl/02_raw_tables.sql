USE DATABASE RETAIL_ANALYTICS;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS RAW.TRANSACTIONS (
    order_id            VARCHAR(100),
    product_id          VARCHAR(50),
    product_name        VARCHAR(500),
    category            VARCHAR(100),
    quantity            INTEGER,
    unit_price          FLOAT,
    total_amount        FLOAT,
    store_id            VARCHAR(50),
    store_name          VARCHAR(200),
    region              VARCHAR(50),
    state               VARCHAR(10),
    city                VARCHAR(100),
    payment_method      VARCHAR(50),
    customer_segment    VARCHAR(50),
    transaction_timestamp TIMESTAMP_NTZ,
    event_type          VARCHAR(50),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    batch_id            VARCHAR(50),
    source_system       VARCHAR(50) DEFAULT 'kafka',
    row_hash            VARCHAR(256),
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE STAGE IF NOT EXISTS RAW.RETAIL_STAGE FILE_FORMAT = (TYPE = 'PARQUET' SNAPPY_COMPRESSION = TRUE);
CREATE FILE FORMAT IF NOT EXISTS RAW.JSON_FORMAT TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE;
CREATE FILE FORMAT IF NOT EXISTS RAW.PARQUET_FORMAT TYPE = 'PARQUET' SNAPPY_COMPRESSION = TRUE;
