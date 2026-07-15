import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linera2 import cli  # noqa: E402
from linera2.cli import build_parser, default_account_file  # noqa: E402


class CliTests(unittest.TestCase):
    def test_default_account_file_is_parent_hubshuju(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            project_dir = Path(temp_dir) / "Linera2.0"
            project_dir.mkdir()
            expected = project_dir.parent / "hubshuju.xlsx"
            expected.touch()
            self.assertFalse((project_dir / "hubshuju.xlsx").exists())
            with patch.object(cli, "PROJECT_DIR", project_dir):
                self.assertEqual(default_account_file(), expected)

    def test_default_account_file_prefers_project_copy(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            project_dir = Path(temp_dir)
            local = project_dir / "hubshuju.xlsx"
            local.touch()
            with patch.object(cli, "PROJECT_DIR", project_dir):
                self.assertEqual(default_account_file(), local)

    def test_default_arguments_run_one_cli_scan(self):
        args = build_parser().parse_args([])
        self.assertEqual(args.workers, 1)
        self.assertEqual(args.timeout, 60)
        self.assertFalse(args.web)
        self.assertFalse(args.auto_session)
        self.assertEqual(args.auto_timeout, 1200)
        self.assertIsNone(args.integration_target)

    def test_auto_session_flag_enables_write_mode(self):
        args = build_parser().parse_args(["--auto-session"])

        self.assertTrue(args.auto_session)

    def test_target_override_is_rejected_without_auto_session(self):
        with self.assertRaises(SystemExit):
            build_parser().parse_args(["--integration-target", "1"])

    def test_target_override_rejects_values_other_than_one(self):
        with self.assertRaises(SystemExit):
            build_parser().parse_args(
                ["--auto-session", "--integration-target", "2"]
            )


if __name__ == "__main__":
    unittest.main()
