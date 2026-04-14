"""Kafka Producer Configuration."""
import os

KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "retail_transactions"),
    "client_id": "retail-producer-01",
    "acks": "all",
    "retries": 3,
    "retry_backoff_ms": 500,
    "batch_size": 16384,
    "linger_ms": 10,
    "compression_type": "snappy",
}

PRODUCER_CONFIG = {
    "min_delay_seconds": 1,
    "max_delay_seconds": 5,
    "null_probability": 0.03,
    "duplicate_probability": 0.02,
    "late_arrival_probability": 0.05,
    "late_arrival_max_minutes": 30,
    "num_stores": 50,
    "num_products": 200,
}

PRODUCT_CATEGORIES = [
    "Electronics", "Clothing", "Groceries", "Home & Garden",
    "Sports", "Beauty", "Toys", "Books", "Automotive", "Health",
]

STORE_REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
