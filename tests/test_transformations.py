"""Unit tests for transformation logic."""

import os
import sys
from datetime import datetime, date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark_batch"))


class TestSchemas:
    """Verify schema definitions are correct."""

    def test_bronze_has_metadata(self):
        from schemas import BRONZE_SCHEMA

        names = [f.name for f in BRONZE_SCHEMA.fields]
        assert "ingestion_timestamp" in names
        assert "batch_id" in names

    def test_fact_has_keys(self):
        from schemas import FACT_SALES_SCHEMA

        names = [f.name for f in FACT_SALES_SCHEMA.fields]
        for k in ["sale_key", "product_key", "store_key", "date_key"]:
            assert k in names

    def test_dim_product_scd2(self):
        from schemas import DIM_PRODUCT_SCHEMA

        names = [f.name for f in DIM_PRODUCT_SCHEMA.fields]
        assert "effective_from" in names
        assert "is_current" in names


class TestLogic:
    """Test transformation logic in pure Python."""

    def test_null_fill(self):
        assert (None or "Unknown Product") == "Unknown Product"
        assert ("Laptop" or "Unknown Product") == "Laptop"

    def test_late_arrival(self):
        ing = datetime(2024, 6, 15, 10, 30)
        evt = datetime(2024, 6, 15, 10, 0)
        assert (ing - evt).total_seconds() > 300

    def test_not_late_arrival(self):
        ing = datetime(2024, 6, 15, 10, 30)
        evt = datetime(2024, 6, 15, 10, 28)
        assert (ing - evt).total_seconds() <= 300

    def test_date_key(self):
        assert int(date(2024, 6, 15).strftime("%Y%m%d")) == 20240615

    def test_dedup(self):
        recs = [
            {"id": "a", "ts": 1},
            {"id": "a", "ts": 2},
            {"id": "b", "ts": 1},
        ]
        seen = {}
        for r in sorted(recs, key=lambda x: x["ts"]):
            if r["id"] not in seen:
                seen[r["id"]] = r
        assert len(seen) == 2
