from __future__ import annotations

import hashlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from publish import (  # noqa: E402
    LEGACY_REMOVALS,
    PublishError,
    _verify_remote,
    build_manifest,
    ensure_fast_forward,
    publish,
    runtime_files,
    scan_sensitive_files,
)


def write(root: Path, relative: str, content: bytes = b"content") -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def create_runtime_tree(root: Path) -> None:
    write(root, "linera_runner.py", b"runner-bytes")
    write(root, "Linera2.0/README.md", b"readme")
    write(root, "Linera2.0/requirements.txt", b"requirements")
    write(root, "Linera2.0/linera2/runtime.py", b"runtime-bytes")
    write(root, "Linera2.0/linera2/nested/ignored.py", b"ignored")
    write(root, "Linera2.0/linera2/runtime.pyc", b"ignored")
    write(root, "Linera2.0/templates/index.html", b"template")
    write(root, "Linera2.0/templates/nested/ignored.html", b"ignored")
    write(root, "Linera2.0/local_config.json", b'{"password":"private"}')
    write(root, "Linera2.0/auto_sessions.json", b"state")


def git_result(returncode: int = 0, stdout: str = "", stderr: str = ""):
    return subprocess.CompletedProcess(["git"], returncode, stdout, stderr)


class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        create_runtime_tree(self.root)

    def test_runtime_files_use_only_the_deterministic_allowlist(self):
        names = [p.relative_to(self.root).as_posix() for p in runtime_files(self.root)]

        self.assertEqual(names, sorted(names))
        self.assertIn("Linera2.0/linera2/runtime.py", names)
        self.assertIn("Linera2.0/templates/index.html", names)
        self.assertNotIn("Linera2.0/local_config.json", names)
        self.assertNotIn("Linera2.0/auto_sessions.json", names)
        self.assertNotIn("Linera2.0/linera2/nested/ignored.py", names)

    def test_runtime_files_reject_symlinks(self):
        link = self.root / "Linera2.0/linera2/link.py"
        try:
            link.symlink_to(self.root / "linera_runner.py")
        except (OSError, NotImplementedError):
            self.skipTest("symlinks are unavailable")

        with self.assertRaises(PublishError):
            runtime_files(self.root)

    def test_manifest_is_schema_two_and_hashes_raw_bytes(self):
        manifest = build_manifest(self.root, "2026.07.15.1")
        entry = next(x for x in manifest["files"] if x["path"].endswith("runtime.py"))

        self.assertEqual(manifest["schema_version"], 2)
        self.assertIs(type(manifest["schema_version"]), int)
        self.assertEqual(manifest["app_version"], "2026.07.15.1")
        self.assertEqual(manifest["runner_version"], "2026.07.15.1")
        self.assertEqual((manifest["task_version"], manifest["base_version"]), ("", ""))
        self.assertEqual(manifest["remove"], list(LEGACY_REMOVALS))
        self.assertEqual(entry["sha256"], hashlib.sha256(b"runtime-bytes").hexdigest())
        self.assertEqual(
            [item["path"] for item in manifest["files"]],
            sorted(item["path"] for item in manifest["files"]),
        )

    def test_manifest_serialization_is_deterministic(self):
        first = json.dumps(build_manifest(self.root, "2026.07.15.1"), sort_keys=True)
        second = json.dumps(build_manifest(self.root, "2026.07.15.1"), sort_keys=True)
        self.assertEqual(first, second)

    def test_sensitive_scan_blocks_supported_secret_rules_without_disclosure(self):
        cases = {
            "wallet.py": 'OKX_WALLET_PASSWORD = "embedded-value"',
            "auth.py": 'Authorization = "Bearer embedded-value"',
            "cookie.py": 'Cookie = "session=embedded-value"',
            "header_map.py": 'headers = {"Authorization": "Bearer embedded-value"}',
            "key.py": "-----BEGIN PRIVATE KEY-----",
            "address.py": 'target = "0x1234567890abcdef1234567890abcdef12345678"',
            "token.py": 'api_token = "embedded-value"',
            "typed_secret.py": 'client_secret: str = "embedded-value"',
        }
        paths = [write(self.root, f"Linera2.0/linera2/{name}", value.encode()) for name, value in cases.items()]

        findings = scan_sensitive_files(paths)
        rendered = repr(findings)

        self.assertEqual(len(findings), len(cases))
        self.assertNotIn("embedded-value", rendered)
        self.assertTrue(all(finding.path in paths for finding in findings))
        self.assertTrue(all(finding.rule_name for finding in findings))

    def test_sensitive_scan_permits_environment_lookups(self):
        path = write(
            self.root,
            "Linera2.0/linera2/config.py",
            b'OKX_WALLET_PASSWORD = os.environ.get("OKX_WALLET_PASSWORD")\nAPI_TOKEN = os.getenv("API_TOKEN")',
        )
        self.assertEqual(scan_sensitive_files([path]), [])

    def test_non_fast_forward_aborts_without_push(self):
        with patch("publish.run_git") as git:
            git.side_effect = [git_result(), git_result(returncode=1)]
            with self.assertRaises(PublishError):
                ensure_fast_forward(self.root)

        flattened = [str(value) for call in git.call_args_list for value in call.args]
        self.assertNotIn("push", flattened)
        self.assertEqual(git.call_args_list[0].args, ("fetch", "origin", "main"))
        self.assertEqual(
            git.call_args_list[1].args,
            ("merge-base", "--is-ancestor", "origin/main", "HEAD"),
        )

    @patch("publish._run_tests")
    @patch("publish.scan_sensitive_files", return_value=[])
    def test_non_fast_forward_does_not_rewrite_manifest(self, _scan, _tests):
        with patch("publish.run_git") as git:
            git.side_effect = [git_result(), git_result(returncode=1)]
            with self.assertRaises(PublishError):
                publish(self.root, "2026.07.15.1")

        self.assertFalse((self.root / "version.json").exists())

    @patch("publish._run_tests")
    @patch("publish.scan_sensitive_files", return_value=[])
    @patch("publish.run_git")
    def test_dry_run_tests_and_prints_exact_paths_without_git_or_network(
        self, git, _scan, run_tests
    ):
        output = io.StringIO()
        with redirect_stdout(output):
            result = publish(self.root, "2026.07.15.1", dry_run=True)

        run_tests.assert_called_once_with(self.root)
        git.assert_not_called()
        self.assertFalse(result.pushed)
        self.assertFalse(result.remote_verified)
        self.assertIn("version.json", output.getvalue())
        for relative in LEGACY_REMOVALS:
            self.assertIn(relative, output.getvalue())

    @patch("publish._verify_remote", return_value=True)
    @patch("publish._run_tests")
    @patch("publish.scan_sensitive_files", return_value=[])
    @patch("publish.run_git", return_value=git_result())
    def test_publish_stages_exact_files_and_uses_safe_git_sequence(
        self, git, _scan, run_tests, verify_remote
    ):
        result = publish(self.root, "2026.07.15.1")

        self.assertTrue(result.pushed)
        self.assertTrue(result.remote_verified)
        run_tests.assert_called_once_with(self.root)
        commands = [call.args for call in git.call_args_list]
        self.assertIn(("fetch", "origin", "main"), commands)
        add = next(args for args in commands if args[:2] == ("add", "--"))
        self.assertEqual(add[2:], tuple(result.staged_paths))
        self.assertIn("version.json", add)
        self.assertNotIn(".", add)
        self.assertIn(("rm", "--ignore-unmatch", "--", *LEGACY_REMOVALS), commands)
        self.assertIn(("push", "origin", "HEAD:main"), commands)
        verify_remote.assert_called_once()
        written = json.loads((self.root / "version.json").read_text(encoding="utf-8"))
        self.assertEqual(written["app_version"], "2026.07.15.1")
        self.assertEqual(written["schema_version"], 2)
        self.assertEqual(list(self.root.glob(".version.json.*.tmp")), [])

    def test_remote_verification_checks_runner_and_package_hashes(self):
        manifest = build_manifest(self.root, "2026.07.15.1")
        requested = []

        def open_url(url):
            requested.append(url)
            relative = url.split("?", 1)[0].split("/main/", 1)[1]
            if relative == "version.json":
                return json.dumps(manifest).encode("utf-8")
            return (self.root / relative).read_bytes()

        with patch("publish._open_url", side_effect=open_url):
            self.assertTrue(_verify_remote(self.root, "2026.07.15.1", manifest))

        checked = {url.split("?", 1)[0].split("/main/", 1)[1] for url in requested}
        self.assertIn("version.json", checked)
        self.assertIn("linera_runner.py", checked)
        self.assertTrue(any(path.startswith("Linera2.0/linera2/") for path in checked))
        self.assertTrue(all("?" in url for url in requested))

    def test_remote_verification_rejects_manifest_version_mismatch(self):
        manifest = build_manifest(self.root, "2026.07.15.1")
        remote = dict(manifest, app_version="2026.07.15.2")
        with patch("publish._open_url", return_value=json.dumps(remote).encode("utf-8")):
            with self.assertRaises(PublishError):
                _verify_remote(self.root, "2026.07.15.1", manifest)

    @patch("publish._verify_remote", side_effect=OSError("CDN unavailable"))
    @patch("publish._run_tests")
    @patch("publish.scan_sensitive_files", return_value=[])
    @patch("publish.run_git", return_value=git_result())
    def test_remote_verify_failure_preserves_successful_push_result(
        self, _git, _scan, _tests, _verify
    ):
        result = publish(self.root, "2026.07.15.1")

        self.assertTrue(result.pushed)
        self.assertFalse(result.remote_verified)
        self.assertNotIn("CDN unavailable", repr(result))

    @patch("publish._run_tests")
    def test_sensitive_finding_aborts_before_git(self, _tests):
        write(
            self.root,
            "Linera2.0/linera2/runtime.py",
            b'OKX_WALLET_PASSWORD = "embedded-value"',
        )
        with patch("publish.run_git") as git, self.assertRaises(PublishError) as caught:
            publish(self.root, "2026.07.15.1")

        git.assert_not_called()
        self.assertNotIn("embedded-value", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
