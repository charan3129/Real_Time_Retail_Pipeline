"""Unit tests for Kafka producer configuration and logic."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "kafka_producer"))

from config import KAFKA_CONFIG, PRODUCER_CONFIG  # noqa: E402
from config import PRODUCT_CATEGORIES, STORE_REGIONS  # noqa: E402


class TestConfig:
    """Test producer configuration values."""

    def test_required_keys(self):
        for k in ["bootstrap_servers", "topic", "acks"]:
            assert k in KAFKA_CONFIG

    def test_topic_name(self):
        assert KAFKA_CONFIG["topic"] == "retail_transactions"

    def test_probabilities(self):
        for k in ["null_probability", "duplicate_probability", "late_arrival_probability"]:
            assert 0 <= PRODUCER_CONFIG[k] <= 1

    def test_delay_range(self):
        assert PRODUCER_CONFIG["min_delay_seconds"] < PRODUCER_CONFIG["max_delay_seconds"]
        assert PRODUCER_CONFIG["min_delay_seconds"] >= 0

    def test_product_categories_not_empty(self):
        assert len(PRODUCT_CATEGORIES) > 0

    def test_store_regions_not_empty(self):
        assert len(STORE_REGIONS) > 0

    def test_num_stores_positive(self):
        assert PRODUCER_CONFIG["num_stores"] > 0

    def test_num_products_positive(self):
        assert PRODUCER_CONFIG["num_products"] > 0
