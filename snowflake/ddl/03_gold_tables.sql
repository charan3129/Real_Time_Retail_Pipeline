USE DATABASE RETAIL_ANALYTICS;
USE SCHEMA ANALYTICS;

CREATE TABLE IF NOT EXISTS ANALYTICS.FACT_SALES (
    sale_key                VARCHAR(256) NOT NULL,
    order_id                VARCHAR(100) NOT NULL,
    product_key             VARCHAR(256) NOT NULL,
    store_key               VARCHAR(256) NOT NULL,
    date_key                INTEGER NOT NULL,
    quantity                INTEGER,
    unit_price              FLOAT,
    total_amount            FLOAT,
    payment_method          VARCHAR(50),
    customer_segment        VARCHAR(50),
    transaction_timestamp   TIMESTAMP_NTZ,
    _loaded_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_fact_sales PRIMARY KEY (sale_key)
) CLUSTER BY (date_key, store_key);

CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_PRODUCT (
    product_key         VARCHAR(256) NOT NULL,
    product_id          VARCHAR(50) NOT NULL,
    product_name        VARCHAR(500),
    category            VARCHAR(100),
    current_unit_price  FLOAT,
    effective_from      DATE,
    effective_to        DATE,
    is_current          BOOLEAN DEFAULT TRUE,
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_product PRIMARY KEY (product_key)
) CLUSTER BY (category, is_current);

CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_STORE (
    store_key       VARCHAR(256) NOT NULL,
    store_id        VARCHAR(50) NOT NULL,
    store_name      VARCHAR(200),
    region          VARCHAR(50),
    state           VARCHAR(10),
    city            VARCHAR(100),
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_store PRIMARY KEY (store_key)
) CLUSTER BY (region);

CREATE TABLE IF NOT EXISTS ANALYTICS.DIM_DATE (
    date_key        INTEGER NOT NULL,
    full_date       DATE NOT NULL,
    year            INTEGER,
    quarter         INTEGER,
    month           INTEGER,
    month_name      VARCHAR(20),
    week_of_year    INTEGER,
    day_of_month    INTEGER,
    day_of_week     INTEGER,
    day_name        VARCHAR(20),
    is_weekend      BOOLEAN,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
);
