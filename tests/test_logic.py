"""Запуск без Docker: python -m unittest discover -s tests -v"""
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "worker"))
from logic import unwrap, event_timestamp, after_or_before, monotonic_start, same_values

class TestCDCLogic(unittest.TestCase):
    def test_snapshot(self):
        op, row = after_or_before({"op": "r", "after": {"account_id": 101}})
        self.assertEqual((op, row["account_id"]), ("r", 101))

    def test_delete_uses_before(self):
        op, row = after_or_before({"op": "d", "before": {"account_id": 102}})
        self.assertEqual((op, row["account_id"]), ("d", 102))

    def test_unwrap(self):
        self.assertEqual(unwrap({"payload": {"op": "u"}})["op"], "u")

    def test_timestamp_from_source(self):
        self.assertEqual(event_timestamp({"source": {"ts_ms": 1_000}}),
                         datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc))

    def test_monotonic_collision(self):
        t = datetime(2026, 11, 22, tzinfo=timezone.utc)
        self.assertEqual(monotonic_start(t, t), t + timedelta(microseconds=1))

    def test_no_extra_version(self):
        self.assertTrue(same_values({"owner_name": "Анна", "status": "active"},
                                    {"owner_name": "Анна", "status": "active"}))

if __name__ == "__main__":
    unittest.main()
