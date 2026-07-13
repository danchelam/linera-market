import json
import random
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linera2.auto_session import (  # noqa: E402
    AutoSessionRecord,
    AutoSessionState,
    AutoSessionStore,
)


NOW = datetime(2026, 7, 14, 3, 0, tzinfo=timezone.utc)


class AutoSessionStoreTests(unittest.TestCase):
    def make_store(self, folder: str) -> AutoSessionStore:
        return AutoSessionStore(Path(folder) / "auto_sessions.json")

    def test_new_daily_session_randomizes_target_once_between_four_and_seven(self):
        with tempfile.TemporaryDirectory() as folder:
            record = self.make_store(folder).get_or_create_daily(
                "625421671", now=NOW, rng=random.Random(7)
            )

        self.assertEqual(record.state, AutoSessionState.WAITING.value)
        self.assertGreaterEqual(record.target_rounds, 4)
        self.assertLessEqual(record.target_rounds, 7)

    def test_same_utc_day_reuses_persisted_target(self):
        with tempfile.TemporaryDirectory() as folder:
            store = self.make_store(folder)
            first = store.get_or_create_daily("625421671", now=NOW, rng=random.Random(1))
            second = store.get_or_create_daily("625421671", now=NOW, rng=random.Random(99))

        self.assertEqual(second.target_rounds, first.target_rounds)

    def test_completed_same_day_is_not_runnable(self):
        with tempfile.TemporaryDirectory() as folder:
            store = self.make_store(folder)
            record = store.get_or_create_daily("625421671", now=NOW, rng=random.Random(1))
            record.state = AutoSessionState.COMPLETED.value
            store.update(record)

            loaded = store.get_or_create_daily("625421671", now=NOW, rng=random.Random(2))

        self.assertEqual(loaded.state, AutoSessionState.COMPLETED.value)

    def test_new_utc_day_creates_a_fresh_session(self):
        with tempfile.TemporaryDirectory() as folder:
            store = self.make_store(folder)
            old = store.get_or_create_daily("625421671", now=NOW, target_override=1)
            old.state = AutoSessionState.COMPLETED.value
            store.update(old)

            next_day = datetime(2026, 7, 15, 0, 1, tzinfo=timezone.utc)
            fresh = store.get_or_create_daily(
                "625421671", now=next_day, rng=random.Random(4)
            )

        self.assertEqual(fresh.utc_date, "2026-07-15")
        self.assertEqual(fresh.state, AutoSessionState.WAITING.value)
        self.assertEqual(fresh.completed_rounds, 0)

    def test_update_is_atomic_and_keeps_other_accounts(self):
        with tempfile.TemporaryDirectory() as folder:
            store = self.make_store(folder)
            one = store.get_or_create_daily("one", now=NOW, target_override=1)
            two = store.get_or_create_daily("two", now=NOW, target_override=1)
            one.completed_rounds = 1
            store.update(one)

            payload = json.loads(store.path.read_text(encoding="utf-8"))

        self.assertEqual(set(payload["accounts"]), {"one", "two"})
        self.assertEqual(payload["accounts"]["one"]["completed_rounds"], 1)
        self.assertEqual(two.account_id, "two")

    def test_serialized_record_contains_no_sensitive_fields(self):
        record = AutoSessionRecord(
            account_id="625421671",
            utc_date="2026-07-14",
            state=AutoSessionState.RUNNING.value,
            target_rounds=5,
            start_coins=433,
            current_coins=431,
        )

        payload = record.to_dict()

        self.assertEqual(record.net_change, -2)
        self.assertNotIn("wallet_address", payload)
        self.assertNotIn("headers", payload)
        self.assertNotIn("response", payload)

    def test_target_override_accepts_only_one(self):
        with tempfile.TemporaryDirectory() as folder:
            store = self.make_store(folder)
            with self.assertRaises(ValueError):
                store.get_or_create_daily("acct", now=NOW, target_override=2)


if __name__ == "__main__":
    unittest.main()
