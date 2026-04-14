"""Unit tests for data quality and anomaly detection."""
import os, json, pytest, numpy as np

class TestSuites:
    def _load(self, name):
        with open(os.path.join(os.path.dirname(__file__),"..","great_expectations","expectations",name)) as f:
            return json.load(f)
    def test_bronze_valid(self):
        s = self._load("bronze_suite.json")
        assert s["expectation_suite_name"] == "bronze_suite" and len(s["expectations"]) > 0
    def test_silver_has_unique(self):
        assert any(e["expectation_type"]=="expect_column_values_to_be_unique" for e in self._load("silver_suite.json")["expectations"])
    def test_all_have_row_count(self):
        for f in ["bronze_suite.json","silver_suite.json","gold_suite.json"]:
            assert any(e["expectation_type"]=="expect_table_row_count_to_be_between" for e in self._load(f)["expectations"])

class TestAnomaly:
    def test_spike(self):
        w = np.array(list(range(95,106))*3)[:30]
        z = (120 - np.mean(w)) / np.std(w)
        assert abs(z) > 2.0
    def test_normal(self):
        w = np.array(list(range(95,106))*3)[:30]
        z = (100 - np.mean(w)) / np.std(w)
        assert abs(z) < 2.0

class TestSeedData:
    def test_json_valid(self):
        with open(os.path.join(os.path.dirname(__file__),"..","data","seed","sample_transactions.json")) as f:
            assert len(json.load(f)) > 0
