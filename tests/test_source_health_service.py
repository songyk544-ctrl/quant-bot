import tempfile
import unittest
from pathlib import Path

from services.source_health_service import record_source_health, should_fail_workflow


class SourceHealthServiceTest(unittest.TestCase):
    def test_fallback_counts_once_per_trade_date_and_recovers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "source_health.csv"
            first = record_source_health(
                checked_at="2026-09-15 16:00",
                trade_date="2026-09-15",
                source="CACHE",
                primary_ok=False,
                stock_count=324,
                message="failed",
                path=path,
            )
            same_day = record_source_health(
                checked_at="2026-09-15 17:00",
                trade_date="2026-09-15",
                source="CACHE",
                primary_ok=False,
                stock_count=324,
                message="failed",
                path=path,
            )
            next_day = record_source_health(
                checked_at="2026-09-16 16:00",
                trade_date="2026-09-16",
                source="CACHE",
                primary_ok=False,
                stock_count=324,
                message="failed",
                path=path,
            )
            recovered = record_source_health(
                checked_at="2026-09-17 16:00",
                trade_date="2026-09-17",
                source="KIS_MASTER",
                primary_ok=True,
                stock_count=354,
                message="ok",
                path=path,
            )

        self.assertEqual(first["상태"], "fallback_once")
        self.assertEqual(same_day["연속실패거래일"], 1)
        self.assertEqual(next_day["상태"], "fallback_blocked")
        self.assertTrue(should_fail_workflow(next_day))
        self.assertEqual(recovered["상태"], "ok")
        self.assertFalse(should_fail_workflow(recovered))


if __name__ == "__main__":
    unittest.main()
