import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linera2.webapp import create_app  # noqa: E402


class FakeStore:
    def as_list(self):
        return [
            {
                "account_id": "625421671",
                "state": "ready",
                "ready": True,
                "coins": 433,
                "backend_ok": True,
                "reason": "已就绪",
            }
        ]


class FakeAutoStore:
    def as_dict(self):
        return {
            "625421671": {
                "account_id": "625421671",
                "utc_date": "2026-07-14",
                "state": "running",
                "target_rounds": 6,
                "completed_rounds": 2,
                "start_coins": 433,
                "current_coins": 431,
                "end_coins": None,
                "nominal_stake": 4,
                "net_change": -2,
                "auto_still_running": True,
                "failure_reason": None,
                "baseline_resolution_keys": [1, 2],
            }
        }


class ReadinessWebAppTests(unittest.TestCase):
    def setUp(self):
        self.app = create_app(FakeStore())
        self.client = self.app.test_client()

    def test_api_returns_account_list(self):
        response = self.client.get("/api/readiness")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()[0]["account_id"], "625421671")

    def test_api_merges_only_public_auto_session_fields(self):
        app = create_app(FakeStore(), FakeAutoStore())

        item = app.test_client().get("/api/readiness").get_json()[0]

        self.assertEqual(item["session_state"], "running")
        self.assertEqual(item["target_rounds"], 6)
        self.assertEqual(item["completed_rounds"], 2)
        self.assertEqual(item["net_change"], -2)
        self.assertTrue(item["auto_still_running"])
        self.assertNotIn("baseline_resolution_keys", item)
        self.assertNotIn("utc_date", item)

    def test_api_uses_null_session_fields_without_auto_record(self):
        app = create_app(FakeStore())

        item = app.test_client().get("/api/readiness").get_json()[0]

        self.assertIsNone(item["session_state"])
        self.assertIsNone(item["target_rounds"])

    def test_index_contains_status_table_and_api_polling(self):
        response = self.client.get("/")
        html = response.get_data(as_text=True)

        self.assertEqual(response.status_code, 200)
        self.assertIn("Linera 2.0 账号状态", html)
        self.assertIn("/api/readiness", html)
        self.assertIn("只读检测模式", html)

    def test_auto_mode_hint_and_session_columns_are_rendered(self):
        app = create_app(FakeStore(), FakeAutoStore(), auto_enabled=True)

        html = app.test_client().get("/").get_data(as_text=True)

        self.assertIn("测试网 Auto 会话模式", html)
        self.assertIn("目标/完成", html)


if __name__ == "__main__":
    unittest.main()
