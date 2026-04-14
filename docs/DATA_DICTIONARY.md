# Data Dictionary

## Bronze Layer (RAW.TRANSACTIONS)
| Column | Type | Description |
|--------|------|-------------|
| order_id | VARCHAR(100) | Transaction UUID |
| product_id | VARCHAR(50) | Product identifier |
| product_name | VARCHAR(500) | Product display name |
| category | VARCHAR(100) | Product category |
| quantity | INTEGER | Units purchased |
| unit_price | FLOAT | Price per unit (USD) |
| total_amount | FLOAT | quantity * unit_price |
| store_id | VARCHAR(50) | Store identifier |
| store_name | VARCHAR(200) | Store display name |
| region | VARCHAR(50) | Geographic region |
| state | VARCHAR(10) | US state abbreviation |
| city | VARCHAR(100) | City |
| payment_method | VARCHAR(50) | credit_card/debit_card/cash/mobile_pay |
| customer_segment | VARCHAR(50) | regular/premium/new/loyalty |
| transaction_timestamp | TIMESTAMP | When transaction occurred |
| event_type | VARCHAR(50) | purchase |
| ingestion_timestamp | TIMESTAMP | When ingested to Bronze |
| batch_id | VARCHAR(50) | Micro-batch identifier |
| row_hash | VARCHAR(256) | SHA-256 dedup hash |

## Gold Layer - Star Schema

### FACT_SALES
| Column | Type | Description |
|--------|------|-------------|
| sale_key | VARCHAR(256) | Surrogate PK |
| order_id | VARCHAR(100) | Natural key |
| product_key | VARCHAR(256) | FK to DIM_PRODUCT |
| store_key | VARCHAR(256) | FK to DIM_STORE |
| date_key | INTEGER | FK to DIM_DATE (YYYYMMDD) |
| quantity | INTEGER | Units |
| unit_price | FLOAT | Price per unit |
| total_amount | FLOAT | Total sale amount |
| payment_method | VARCHAR(50) | Payment type |
| customer_segment | VARCHAR(50) | Customer type |

Clustering: date_key, store_key

### DIM_PRODUCT (SCD Type 2)
| Column | Type | Description |
|--------|------|-------------|
| product_key | VARCHAR(256) | Surrogate PK |
| product_id | VARCHAR(50) | Natural key |
| product_name | VARCHAR(500) | Name |
| category | VARCHAR(100) | Category |
| current_unit_price | FLOAT | Price for this version |
| effective_from | DATE | Version start |
| effective_to | DATE | Version end (NULL=current) |
| is_current | BOOLEAN | Active version flag |

### DIM_STORE
| Column | Type | Description |
|--------|------|-------------|
| store_key | VARCHAR(256) | Surrogate PK |
| store_id | VARCHAR(50) | Natural key |
| store_name | VARCHAR(200) | Name |
| region | VARCHAR(50) | Region |
| state | VARCHAR(10) | State |
| city | VARCHAR(100) | City |

### DIM_DATE (2020-2030)
| Column | Type | Description |
|--------|------|-------------|
| date_key | INTEGER | PK (YYYYMMDD) |
| full_date | DATE | Calendar date |
| year/quarter/month | INTEGER | Date parts |
| month_name/day_name | VARCHAR | Names |
| is_weekend | BOOLEAN | Weekend flag |

## Analytical Views
- V_DAILY_REVENUE: daily revenue, orders, units
- V_TOP_PRODUCTS: product ranking by revenue
- V_STORE_PERFORMANCE: store-level metrics
- V_REVENUE_BY_REGION_MONTH: regional monthly breakdown
- V_PAYMENT_ANALYSIS: payment method distribution
- V_RECENT_TRANSACTIONS: last 24h hourly counts
