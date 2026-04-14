"""
PySpark Schema Definitions for all pipeline layers.
"""

from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

BRONZE_SCHEMA = StructType([
    StructField("kafka_key", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("source_topic", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("_is_duplicate", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("batch_id", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("row_hash", StringType(), True),
])

SILVER_TRANSACTION_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("store_id", StringType(), False),
    StructField("store_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("event_type", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("batch_id", StringType(), True),
    StructField("row_hash", StringType(), True),
    StructField("is_late_arrival", BooleanType(), True),
    StructField("processed_timestamp", TimestampType(), True),
])

FACT_SALES_SCHEMA = StructType([
    StructField("sale_key", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("product_key", StringType(), False),
    StructField("store_key", StringType(), False),
    StructField("date_key", IntegerType(), False),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
])

DIM_PRODUCT_SCHEMA = StructType([
    StructField("product_key", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("current_unit_price", DoubleType(), True),
    StructField("effective_from", DateType(), True),
    StructField("effective_to", DateType(), True),
    StructField("is_current", BooleanType(), True),
])

DIM_STORE_SCHEMA = StructType([
    StructField("store_key", StringType(), False),
    StructField("store_id", StringType(), False),
    StructField("store_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),
])

DIM_DATE_SCHEMA = StructType([
    StructField("date_key", IntegerType(), False),
    StructField("full_date", DateType(), False),
    StructField("year", IntegerType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("month_name", StringType(), True),
    StructField("week_of_year", IntegerType(), True),
    StructField("day_of_month", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("is_weekend", BooleanType(), True),
])
