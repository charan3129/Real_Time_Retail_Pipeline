"""
Bronze to Silver Transformation.
Deduplication, null handling, type standardization, SCD Type 2 for products.
"""
import logging, sys, os
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    coalesce, col, current_date, current_timestamp, expr, lit,
    row_number, sha2, concat_ws, to_date, to_timestamp, when,
)
from delta.tables import DeltaTable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark_streaming"))
from spark_config import get_spark_session, DELTA_PATHS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("BronzeToSilver")


class BronzeToSilverTransformer:
    def __init__(self):
        self.spark = get_spark_session("RetailPipeline-BronzeToSilver")
        self.bronze_path = DELTA_PATHS["bronze"]
        self.silver_path = DELTA_PATHS["silver"]

    def read_bronze(self):
        logger.info("Reading Bronze: %s", self.bronze_path)
        return self.spark.read.format("delta").load(self.bronze_path)

    def deduplicate(self, df):
        window = Window.partitionBy("order_id").orderBy("ingestion_timestamp")
        deduped = df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")
        orig, dedup = df.count(), deduped.count()
        logger.info("Dedup: %d -> %d (removed %d)", orig, dedup, orig - dedup)
        return deduped

    def handle_nulls(self, df):
        return (df
            .withColumn("product_name", coalesce(col("product_name"), lit("Unknown Product")))
            .withColumn("category", coalesce(col("category"), lit("Uncategorized")))
            .withColumn("payment_method", coalesce(col("payment_method"), lit("unknown")))
            .withColumn("customer_segment", coalesce(col("customer_segment"), lit("unknown")))
            .withColumn("quantity", coalesce(col("quantity"), lit(1)))
            .withColumn("unit_price", coalesce(col("unit_price"), lit(0.0)))
            .withColumn("total_amount", coalesce(col("total_amount"), col("quantity") * col("unit_price"))))

    def standardize(self, df):
        return (df
            .withColumn("transaction_timestamp", to_timestamp(col("transaction_timestamp")))
            .withColumn("transaction_date", to_date(col("transaction_timestamp")))
            .withColumn("quantity", col("quantity").cast("int"))
            .withColumn("unit_price", col("unit_price").cast("double"))
            .withColumn("total_amount", col("total_amount").cast("double"))
            .withColumn("category", expr("initcap(trim(category))"))
            .withColumn("payment_method", expr("lower(trim(payment_method))")))

    def flag_late_arrivals(self, df):
        return df.withColumn("is_late_arrival",
            when(col("ingestion_timestamp").cast("long") - col("transaction_timestamp").cast("long") > 300,
                 lit(True)).otherwise(lit(False)))

    def write_silver(self, df):
        silver_df = df.select(
            "order_id","product_id","product_name","category","quantity","unit_price","total_amount",
            "store_id","store_name","region","state","city","payment_method","customer_segment",
            "transaction_timestamp","transaction_date","event_type","ingestion_timestamp",
            "batch_id","row_hash","is_late_arrival"
        ).withColumn("processed_timestamp", current_timestamp())

        if DeltaTable.isDeltaTable(self.spark, self.silver_path):
            DeltaTable.forPath(self.spark, self.silver_path).alias("t").merge(
                silver_df.alias("s"), "t.order_id = s.order_id"
            ).whenNotMatchedInsertAll().execute()
        else:
            silver_df.write.format("delta").mode("overwrite").partitionBy("transaction_date").save(self.silver_path)

    def build_product_scd2(self):
        silver_df = self.spark.read.format("delta").load(self.silver_path)
        dim_path = DELTA_PATHS["gold_dim_product"]
        window = Window.partitionBy("product_id").orderBy(col("transaction_timestamp").desc())
        latest = (silver_df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1)
            .select(col("product_id"), col("product_name"), col("category"),
                    col("unit_price").alias("current_unit_price"))
            .withColumn("product_key", sha2(concat_ws("||", col("product_id"), col("product_name"),
                        col("category"), col("current_unit_price")), 256))
            .withColumn("effective_from", current_date())
            .withColumn("effective_to", lit(None).cast("date"))
            .withColumn("is_current", lit(True)))

        if DeltaTable.isDeltaTable(self.spark, dim_path):
            DeltaTable.forPath(self.spark, dim_path).alias("t").merge(
                latest.alias("s"), "t.product_id = s.product_id AND t.is_current = true"
            ).whenMatchedUpdate(
                condition="t.product_name != s.product_name OR t.category != s.category OR t.current_unit_price != s.current_unit_price",
                set={"effective_to": "current_date()", "is_current": "false"}
            ).whenNotMatchedInsertAll().execute()
        else:
            latest.write.format("delta").mode("overwrite").save(dim_path)

    def run(self):
        logger.info("Starting Bronze -> Silver")
        df = self.read_bronze()
        df = self.deduplicate(df)
        df = self.handle_nulls(df)
        df = self.standardize(df)
        df = self.flag_late_arrivals(df)
        self.write_silver(df)
        self.build_product_scd2()
        logger.info("Bronze -> Silver complete")

if __name__ == "__main__":
    BronzeToSilverTransformer().run()
