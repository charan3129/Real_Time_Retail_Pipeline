-- Snowpipe for automated ingestion from external stage.
-- Uncomment and configure for your cloud storage.
USE DATABASE RETAIL_ANALYTICS;
USE SCHEMA RAW;

-- CREATE STAGE IF NOT EXISTS RAW.EXTERNAL_RETAIL_STAGE
--     URL = 's3://<your-bucket>/retail/bronze/'
--     STORAGE_INTEGRATION = <your_storage_integration>
--     FILE_FORMAT = RAW.PARQUET_FORMAT;

-- CREATE PIPE IF NOT EXISTS RAW.RETAIL_TRANSACTIONS_PIPE
--     AUTO_INGEST = TRUE AS
--     COPY INTO RAW.TRANSACTIONS (
--         order_id, product_id, product_name, category, quantity,
--         unit_price, total_amount, store_id, store_name, region,
--         state, city, payment_method, customer_segment,
--         transaction_timestamp, event_type, batch_id, row_hash
--     ) FROM (
--         SELECT $1:order_id::VARCHAR, $1:product_id::VARCHAR,
--             $1:product_name::VARCHAR, $1:category::VARCHAR,
--             $1:quantity::INTEGER, $1:unit_price::FLOAT,
--             $1:total_amount::FLOAT, $1:store_id::VARCHAR,
--             $1:store_name::VARCHAR, $1:region::VARCHAR,
--             $1:state::VARCHAR, $1:city::VARCHAR,
--             $1:payment_method::VARCHAR, $1:customer_segment::VARCHAR,
--             $1:transaction_timestamp::TIMESTAMP_NTZ,
--             $1:event_type::VARCHAR, $1:batch_id::VARCHAR,
--             $1:row_hash::VARCHAR
--         FROM @RAW.EXTERNAL_RETAIL_STAGE
--     ) FILE_FORMAT = RAW.PARQUET_FORMAT ON_ERROR = 'CONTINUE';

-- Manual batch load alternative:
-- COPY INTO RAW.TRANSACTIONS FROM @RAW.RETAIL_STAGE
--     FILE_FORMAT = RAW.PARQUET_FORMAT ON_ERROR = 'CONTINUE' PURGE = FALSE;

-- Monitor: SELECT SYSTEM$PIPE_STATUS('RAW.RETAIL_TRANSACTIONS_PIPE');
