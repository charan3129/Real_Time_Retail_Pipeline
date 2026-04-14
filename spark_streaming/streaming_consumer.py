"""
Spark Structured Streaming Consumer.

Reads retail transaction events from Kafka, validates schema,
writes to Bronze layer in Delta Lake format with metadata columns.
"""

import logging
import uuid
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, from_json, lit, sha2, concat_ws,
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

from spark_config import get_spark_session, DELTA_PATHS, KAFKA_READ_CONFIG

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("StreamingConsumer")

TRANSACTION_SCHEMA = StructType([
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
])


class BronzeStreamWriter:
    """Reads from Kafka and writes raw data to Bronze Delta Lake layer."""

    def __init__(self):
        self.spark = get_spark_session("RetailPipeline-Bronze")
        self.bronze_path = DELTA_PATHS["bronze"]
        self.batch_id = str(uuid.uuid4())[:8]

    def read_from_kafka(self) -> DataFrame:
        raw_stream = (
            self.spark.readStream.format("kafka")
            .options(**KAFKA_READ_CONFIG).load()
        )
        parsed = (
            raw_stream
            .selectExpr("CAST(key AS STRING) as kafka_key",
                        "CAST(value AS STRING) as raw_json",
                        "topic", "partition", "offset",
                        "timestamp as kafka_timestamp")
            .withColumn("parsed", from_json(col("raw_json"), TRANSACTION_SCHEMA))
            .select(
                col("kafka_key"), col("raw_json"),
                col("topic").alias("source_topic"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                col("kafka_timestamp"), col("parsed.*"),
            )
        )
        return parsed

    def add_bronze_metadata(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("batch_id", lit(self.batch_id))
            .withColumn("source_system", lit("kafka"))
            .withColumn("row_hash", sha2(
                concat_ws("||", col("order_id"), col("product_id"),
                          col("store_id"), col("transaction_timestamp")), 256))
        )

    def write_to_bronze(self):
        raw_stream = self.read_from_kafka()
        enriched = self.add_bronze_metadata(raw_stream)

        def process_batch(batch_df: DataFrame, batch_id: int):
            if batch_df.isEmpty():
                return
            count = batch_df.count()
            logger.info("Processing batch %d: %d records", batch_id, count)

            valid_df = batch_df.filter(col("order_id").isNotNull())
            invalid_df = batch_df.filter(col("order_id").isNull())

            invalid_count = invalid_df.count()
            if invalid_count > 0:
                logger.warning("Batch %d: %d invalid records routed to dead-letter", batch_id, invalid_count)
                invalid_df.write.format("delta").mode("append").save(f"{self.bronze_path}_dead_letter")

            valid_df.write.format("delta").mode("append").option("mergeSchema", "true").save(self.bronze_path)
            logger.info("Batch %d written: %d valid, %d invalid", batch_id, count - invalid_count, invalid_count)

        query = (
            enriched.writeStream.foreachBatch(process_batch)
            .outputMode("append").trigger(processingTime="30 seconds")
            .option("checkpointLocation", f"{self.bronze_path}/_checkpoint")
            .start()
        )
        logger.info("Streaming query started.")
        query.awaitTermination()


def main():
    logger.info("Starting Bronze Streaming Consumer")
    BronzeStreamWriter().write_to_bronze()


if __name__ == "__main__":
    main()
