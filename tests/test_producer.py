"""Unit tests for Kafka producer."""

import json
import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "kafka_producer"))

from config import KAFKA_CONFIG, PRODUCER_CONFIG


class TestConfig:
    """Test producer configuration values."""

    def test_required_keys(self):
        for k in ["bootstrap_servers", "topic", "acks"]:
            assert k in KAFKA_CONFIG

    def test_probabilities(self):
        for k in ["null_probability", "duplicate_probability", "late_arrival_probability"]:
            assert 0 <= PRODUCER_CONFIG[k] <= 1


class TestTransactions:
    """Test transaction event generation."""

    @patch("kafka_producer.producer.KafkaProducer")
    def test_fields(self, mock):
        mock.return_value = MagicMock()
        from producer import RetailTransactionProducer

        p = RetailTransactionProducer()
        t = p._generate_transaction()
        for f in ["order_id", "product_id", "quantity", "store_id", "total_amount"]:
            assert f in t

    @patch("kafka_producer.producer.KafkaProducer")
    def test_duplicate(self, mock):
        mock.return_value = MagicMock()
        from producer import RetailTransactionProducer

        p = RetailTransactionProducer()
        t = p._generate_transaction()
        d = p._generate_duplicate(t)
        assert d["order_id"] == t["order_id"]
        assert d.get("_is_duplicate")
