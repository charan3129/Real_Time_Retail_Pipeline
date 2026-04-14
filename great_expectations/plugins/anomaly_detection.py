"""
Anomaly Detection Plugin.
Z-score based anomaly detection on transaction counts and revenue.
"""
import logging, os, numpy as np
from datetime import datetime, timedelta

logger = logging.getLogger("AnomalyDetector")

class AnomalyDetector:
    def __init__(self, spark_session=None):
        if spark_session is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.builder.appName("AnomalyDetector").getOrCreate()
        else:
            self.spark = spark_session
        self.gold_path = os.getenv("GOLD_FACT_PATH", "/data/delta/gold/fact_sales")

    def _daily_counts(self, days):
        from pyspark.sql.functions import col, count, to_date
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        df = (self.spark.read.format("delta").load(self.gold_path)
            .withColumn("d", to_date(col("transaction_timestamp")))
            .filter(col("d") >= cutoff).groupBy("d").agg(count("*").alias("c")).orderBy("d"))
        return [{"date": str(r["d"]), "count": r["c"]} for r in df.collect()]

    def detect_transaction_count_anomalies(self, lookback_days=30, std_threshold=2.0):
        data = self._daily_counts(lookback_days + 10)
        if len(data) < lookback_days: return []
        counts = np.array([d["count"] for d in data])
        dates = [d["date"] for d in data]
        anomalies = []
        for i in range(lookback_days, len(counts)):
            w = counts[i-lookback_days:i]
            m, s = np.mean(w), np.std(w)
            if s == 0: continue
            z = (counts[i] - m) / s
            if abs(z) > std_threshold:
                anomalies.append({"date": dates[i], "count": int(counts[i]),
                    "mean": round(float(m),2), "std": round(float(s),2), "z_score": round(float(z),2),
                    "direction": "spike" if z > 0 else "drop",
                    "severity": "CRITICAL" if abs(z) > 3.0 else "WARNING"})
        return anomalies
