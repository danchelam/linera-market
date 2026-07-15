import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linera2.account_support import load_accounts  # noqa: E402
from linera2.local_config import (  # noqa: E402
    get_wallet_password,
    load_local_config,
    migrate_legacy_wallet_password,
)


class LocalSupportTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)

    def test_environment_password_wins_over_local_config(self):
        config = self.root / "local_config.json"
        config.write_text('{"wallet_password":"file-secret"}', encoding="utf-8")
        with patch.dict(os.environ, {"OKX_WALLET_PASSWORD": "env-secret"}):
            self.assertEqual(get_wallet_password(self.root), "env-secret")

    def test_migration_extracts_only_password_and_never_copies_source(self):
        legacy = self.root / "base_module.py"
        legacy.write_text(
            'OKX_DEFAULT_PASSWORD = "local-secret"\nOTHER = "ignored"',
            encoding="utf-8",
        )
        self.assertTrue(migrate_legacy_wallet_password(self.root, legacy))
        self.assertEqual(
            load_local_config(self.root), {"wallet_password": "local-secret"}
        )
        self.assertEqual(
            legacy.read_text(encoding="utf-8"),
            'OKX_DEFAULT_PASSWORD = "local-secret"\nOTHER = "ignored"',
        )

    def test_load_local_config_rejects_malformed_or_non_string_values(self):
        config = self.root / "local_config.json"
        for content in ('not-json', '["wallet_password"]', '{"wallet_password": 1}'):
            with self.subTest(content=content):
                config.write_text(content, encoding="utf-8")
                self.assertEqual(load_local_config(self.root), {})

    def test_load_accounts_supports_environment_id_column(self):
        path = self.root / "accounts.csv"
        path.write_text("环境ID,环境名称\n625421710,A70\n", encoding="utf-8-sig")
        self.assertEqual(
            [(x.id, x.ua) for x in load_accounts(path)], [("625421710", "A70")]
        )


if __name__ == "__main__":
    unittest.main()
