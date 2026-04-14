"""
Silver to Gold Transformation.
Builds Kimball star schema: fact_sales, dim_store, dim_date.
"""
import logging, sys, os
from datetime import date, timedelta
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, concat_ws, current_timestamp, date_format, dayofmonth, dayofweek,
    expr, lit, month, quarter, row_number, sha2, weekofyear, when, year,
)
from delta.tables import DeltaTable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark_streaming"))
from spark_config import get_spark_session, DELTA_PATHS

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SilverToGold")


class SilverToGoldTransformer:
    def __init__(self):
        self.spark = get_spark_session("RetailPipeline-SilverToGold")
        self.silver_path = DELTA_PATHS["silver"]

    def read_silver(self):
        return self.spark.read.format("delta").load(self.silver_path)

    def build_dim_store(self, df):
        dim_path = DELTA_PATHS["gold_dim_store"]
        w = Window.partitionBy("store_id").orderBy(col("transaction_timestamp").desc())
        dim = (df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1)
            .select(sha2(col("store_id"), 256).alias("store_key"),
                    "store_id", "store_name", "region", "state", "city"))
        dim.write.format("delta").mode("overwrite").save(dim_path)
        logger.info("dim_store: %d records", dim.count())

    def build_dim_date(self):
        dim_path = DELTA_PATHS["gold_dim_date"]
        dates = [(date(2020,1,1) + timedelta(days=i),) for i in range((date(2030,12,31) - date(2020,1,1)).days + 1)]
        df = self.spark.createDataFrame(dates, ["full_date"])
        dim = (df
            .withColumn("date_key", expr("cast(date_format(full_date,'yyyyMMdd') as int)"))
            .withColumn("year", year("full_date")).withColumn("quarter", quarter("full_date"))
            .withColumn("month", month("full_date")).withColumn("month_name", date_format("full_date","MMMM"))
            .withColumn("week_of_year", weekofyear("full_date"))
            .withColumn("day_of_month", dayofmonth("full_date"))
            .withColumn("day_of_week", dayofweek("full_date"))
            .withColumn("day_name", date_format("full_date","EEEE"))
            .withColumn("is_weekend", when(dayofweek("full_date").isin(1,7), True).otherwise(False)))
        dim.write.format("delta").mode("overwrite").save(dim_path)
        logger.info("dim_date: %d records", dim.count())

    def build_fact_sales(self, df):
        fact_path = DELTA_PATHS["gold_fact_sales"]
        fact = (df
            .withColumn("sale_key", sha2(concat_ws("||", col("order_id"), col("transaction_timestamp").cast("string")), 256))
            .withColumn("product_key", sha2(col("product_id"), 256))
            .withColumn("store_key", sha2(col("store_id"), 256))
            .withColumn("date_key", expr("cast(date_format(transaction_date,'yyyyMMdd') as int)"))
            .select("sale_key","order_id","product_key","store_key","date_key",
                    "quantity","unit_price","total_amount","payment_method",
                    "customer_segment","transaction_timestamp"))
        if DeltaTable.isDeltaTable(self.spark, fact_path):
            DeltaTable.forPath(self.spark, fact_path).alias("t").merge(
                fact.alias("s"), "t.sale_key = s.sale_key"
            ).whenNotMatchedInsertAll().execute()
        else:
            fact.write.format("delta").mode("overwrite").partitionBy("date_key").save(fact_path)
        logger.info("fact_sales: %d records", self.spark.read.format("delta").load(fact_path).count())

    def run(self):
        logger.info("Starting Silver -> Gold")
        df = self.read_silver()
        self.build_dim_store(df)
        self.build_dim_date()
        self.build_fact_sales(df)
        logger.info("Silver -> Gold complete")

if __name__ == "__main__":
    SilverToGoldTransformer().run()
