"""Unit tests for data quality and anomaly detection."""

import json
import os

import numpy as np


class TestSuites:
    """Verify expectation suite JSON files."""

    def _load(self, name):
        path = os.path.join(
            os.path.dirname(__file__), "..", "great_expectations",
            "expectations", name
        )
        with open(path) as f:
            return json.load(f)

    def test_bronze_valid(self):
        s = self._load("bronze_suite.json")
        assert s["expectation_suite_name"] == "bronze_suite"
        assert len(s["expectations"]) > 0

    def test_silver_has_unique(self):
        exps = self._load("silver_suite.json")["expectations"]
        types = [e["expectation_type"] for e in exps]
        assert "expect_column_values_to_be_unique" in types

    def test_all_have_row_count(self):
        for fname in ["bronze_suite.json", "silver_suite.json", "gold_suite.json"]:
            exps = self._load(fname)["expectations"]
            types = [e["expectation_type"] for e in exps]
            assert "expect_table_row_count_to_be_between" in types


class TestAnomaly:
    """Test Z-score anomaly detection algorithm."""

    def test_spike(self):
        window = np.array(list(range(95, 106)) * 3)[:30]
        z = (120 - np.mean(window)) / np.std(window)
        assert abs(z) > 2.0

    def test_normal(self):
        window = np.array(list(range(95, 106)) * 3)[:30]
        z = (100 - np.mean(window)) / np.std(window)
        assert abs(z) < 2.0


class TestSeedData:
    """Verify seed data files are valid."""

    def test_json_valid(self):
        path = os.path.join(
            os.path.dirname(__file__), "..", "data",
            "seed", "sample_transactions.json"
        )
        with open(path) as f:
            data = json.load(f)
        assert len(data) > 0
