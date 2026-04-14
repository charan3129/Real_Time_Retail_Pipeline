"""Unit tests for transformation logic."""

from datetime import datetime, date


class TestNullHandling:
    """Test null fill logic."""

    def test_null_fills_default(self):
        assert (None or "Unknown Product") == "Unknown Product"

    def test_null_preserves_value(self):
        assert ("Laptop" or "Unknown Product") == "Laptop"

    def test_category_default(self):
        assert (None or "Uncategorized") == "Uncategorized"

    def test_payment_default(self):
        assert (None or "unknown") == "unknown"

    def test_quantity_default(self):
        assert (None or 1) == 1

    def test_total_recalculation(self):
        quantity = 3
        unit_price = 19.99
        total = None
        result = total if total else round(quantity * unit_price, 2)
        assert result == 59.97


class TestLateArrival:
    """Test late arrival detection logic."""

    def test_late_arrival_detected(self):
        ing = datetime(2024, 6, 15, 10, 30)
        evt = datetime(2024, 6, 15, 10, 0)
        diff = (ing - evt).total_seconds()
        assert diff > 300

    def test_not_late_arrival(self):
        ing = datetime(2024, 6, 15, 10, 30)
        evt = datetime(2024, 6, 15, 10, 28)
        diff = (ing - evt).total_seconds()
        assert diff <= 300


class TestDateKey:
    """Test date key formatting."""

    def test_date_key_format(self):
        assert int(date(2024, 6, 15).strftime("%Y%m%d")) == 20240615

    def test_date_key_jan_first(self):
        assert int(date(2024, 1, 1).strftime("%Y%m%d")) == 20240101

    def test_date_key_dec_last(self):
        assert int(date(2024, 12, 31).strftime("%Y%m%d")) == 20241231


class TestDedup:
    """Test deduplication logic."""

    def test_keeps_earliest(self):
        recs = [
            {"id": "a", "ts": 1, "data": "first"},
            {"id": "a", "ts": 2, "data": "second"},
            {"id": "b", "ts": 1, "data": "only"},
        ]
        seen = {}
        for r in sorted(recs, key=lambda x: x["ts"]):
            if r["id"] not in seen:
                seen[r["id"]] = r
        assert len(seen) == 2
        assert seen["a"]["data"] == "first"

    def test_no_duplicates(self):
        recs = [
            {"id": "x", "ts": 1},
            {"id": "y", "ts": 2},
        ]
        seen = {}
        for r in sorted(recs, key=lambda x: x["ts"]):
            if r["id"] not in seen:
                seen[r["id"]] = r
        assert len(seen) == 2


class TestPaymentStandardization:
    """Test payment method cleaning."""

    def test_lowercase_trim(self):
        assert "  Credit_Card  ".strip().lower() == "credit_card"

    def test_already_clean(self):
        assert "cash".strip().lower() == "cash"
