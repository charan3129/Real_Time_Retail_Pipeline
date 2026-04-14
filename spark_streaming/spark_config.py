"""
Spark Session Configuration.
Centralized factory for SparkSession creation with Delta Lake support.
"""

import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "RetailPipeline") -> SparkSession:
    """Create and return a configured SparkSession with Delta Lake."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.checkpointLocation",
                os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


DELTA_PATHS = {
    "bronze": os.getenv("BRONZE_PATH", "/data/delta/bronze/transactions"),
    "silver": os.getenv("SILVER_PATH", "/data/delta/silver/transactions"),
    "gold_fact_sales": os.getenv("GOLD_FACT_PATH", "/data/delta/gold/fact_sales"),
    "gold_dim_product": os.getenv("GOLD_DIM_PRODUCT_PATH", "/data/delta/gold/dim_product"),
    "gold_dim_store": os.getenv("GOLD_DIM_STORE_PATH", "/data/delta/gold/dim_store"),
    "gold_dim_date": os.getenv("GOLD_DIM_DATE_PATH", "/data/delta/gold/dim_date"),
}

KAFKA_READ_CONFIG = {
    "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    "subscribe": os.getenv("KAFKA_TOPIC", "retail_transactions"),
    "startingOffsets": "latest",
    "failOnDataLoss": "false",
    "maxOffsetsPerTrigger": 10000,
}
