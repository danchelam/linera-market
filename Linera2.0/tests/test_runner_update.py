from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from linera_runner import (  # noqa: E402
    ManifestError,
    ManifestFile,
    UpdateManifest,
    apply_manifest,
    migrate_private_config,
    parse_manifest,
    sha256_file,
)


def digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def manifest_json(**overrides) -> str:
    data = {
        "schema_version": 2,
        "runner_version": "2026.07.16.1",
        "app_version": "2026.07.16.1",
        "files": [
            {
                "path": "Linera2.0/linera2/runtime.py",
                "sha256": digest(b"runtime"),
            }
        ],
        "remove": [],
    }
    data.update(overrides)
    return json.dumps(data)


def manifest_for(files, remove=()) -> UpdateManifest:
    return UpdateManifest(
        schema_version=2,
        runner_version="2026.07.16.1",
        app_version="2026.07.16.1",
        files=tuple(ManifestFile(path, digest(content)) for path, content in files),
        remove=tuple(remove),
    )


class ManifestParsingTests(unittest.TestCase):
    def test_parse_manifest_returns_immutable_schema_v2_model(self):
        result = parse_manifest(manifest_json())

        self.assertEqual(result.schema_version, 2)
        self.assertEqual(result.files[0].path, "Linera2.0/linera2/runtime.py")
        self.assertIsInstance(result.files, tuple)

    def test_parse_manifest_rejects_path_traversal_and_windows_paths(self):
        unsafe = (
            "../secret",
            "/absolute.py",
            "C:/secret.py",
            "Linera2.0/linera2/../../secret.py",
            r"Linera2.0\linera2\runtime.py",
        )
        for path in unsafe:
            with self.subTest(path=path), self.assertRaises(ManifestError):
                parse_manifest(
                    manifest_json(files=[{"path": path, "sha256": "0" * 64}])
                )

    def test_parse_manifest_rejects_paths_outside_allowlist(self):
        with self.assertRaises(ManifestError):
            parse_manifest(
                manifest_json(
                    files=[{"path": "hubshuju.xlsx", "sha256": "0" * 64}]
                )
            )

    def test_parse_manifest_rejects_bad_hashes(self):
        for value in ("f" * 63, "F" * 64, "g" * 64, 123):
            with self.subTest(value=value), self.assertRaises(ManifestError):
                parse_manifest(
                    manifest_json(
                        files=[
                            {
                                "path": "Linera2.0/linera2/runtime.py",
                                "sha256": value,
                            }
                        ]
                    )
                )

    def test_parse_manifest_rejects_duplicate_normalized_paths(self):
        with self.assertRaises(ManifestError):
            parse_manifest(
                manifest_json(
                    files=[
                        {
                            "path": "Linera2.0/linera2/runtime.py",
                            "sha256": "0" * 64,
                        },
                        {
                            "path": "Linera2.0/linera2/./runtime.py",
                            "sha256": "1" * 64,
                        },
                    ]
                )
            )

    def test_parse_manifest_requires_schema_two_and_correct_types(self):
        invalid = (
            {"schema_version": 1},
            {"schema_version": "2"},
            {"schema_version": 2.0},
            {"runner_version": 2},
            {"app_version": None},
            {"task_version": 2},
            {"base_version": None},
            {"files": {}},
            {"remove": "linera_task.py"},
        )
        for override in invalid:
            with self.subTest(override=override), self.assertRaises(ManifestError):
                parse_manifest(manifest_json(**override))

    def test_parse_manifest_preserves_optional_legacy_version_fields(self):
        result = parse_manifest(
            manifest_json(task_version="legacy-task", base_version="legacy-base")
        )

        self.assertEqual(result.task_version, "legacy-task")
        self.assertEqual(result.base_version, "legacy-base")
        defaulted = parse_manifest(manifest_json())
        self.assertEqual((defaulted.task_version, defaulted.base_version), ("", ""))

    def test_parse_manifest_still_requires_app_version(self):
        data = json.loads(manifest_json())
        del data["app_version"]
        with self.assertRaises(ManifestError):
            parse_manifest(json.dumps(data))

    def test_parse_manifest_allows_only_exact_legacy_removals(self):
        result = parse_manifest(
            manifest_json(remove=["linera_task.py", "base_module.py"])
        )
        self.assertEqual(result.remove, ("linera_task.py", "base_module.py"))

        for value in ("Linera2.0/auto_sessions.json", "./linera_task.py", "base_module.py.bak"):
            with self.subTest(value=value), self.assertRaises(ManifestError):
                parse_manifest(manifest_json(remove=[value]))


class ApplyManifestTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)

    def write_live(self, relative: str, content: bytes) -> Path:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path

    def test_sha256_file_hashes_raw_bytes(self):
        path = self.write_live("payload.bin", b"\x00\xff\r\n")
        self.assertEqual(sha256_file(path), digest(b"\x00\xff\r\n"))

    def test_hash_mismatch_replaces_nothing(self):
        live = self.write_live("Linera2.0/linera2/runtime.py", b"old")
        manifest = UpdateManifest(
            2,
            "v",
            "v",
            (ManifestFile("Linera2.0/linera2/runtime.py", "f" * 64),),
            (),
        )

        result = apply_manifest(manifest, self.root, lambda _path: b"new")

        self.assertFalse(result.updated)
        self.assertFalse(result.restart_required)
        self.assertEqual(live.read_bytes(), b"old")

    def test_staging_failure_removes_transaction_created_install_root(self):
        install_root = self.root / "new-install"
        manifest = UpdateManifest(
            2,
            "v",
            "v",
            (ManifestFile("Linera2.0/linera2/runtime.py", "f" * 64),),
            (),
        )

        result = apply_manifest(manifest, install_root, lambda _path: b"corrupt")

        self.assertFalse(result.updated)
        self.assertFalse(install_root.exists())

    def test_direct_manifest_model_cannot_bypass_path_validation(self):
        outside = self.root.parent / "outside-runner-test.py"
        outside.unlink(missing_ok=True)
        self.addCleanup(outside.unlink, missing_ok=True)
        manifest = UpdateManifest(
            2,
            "v",
            "v",
            (ManifestFile("../outside-runner-test.py", digest(b"escaped")),),
            (),
        )

        result = apply_manifest(manifest, self.root, lambda _path: b"escaped")

        self.assertFalse(result.updated)
        self.assertFalse(outside.exists())

    def test_direct_manifest_model_rejects_float_schema_version(self):
        manifest = UpdateManifest(
            2.0,
            "v",
            "v",
            (ManifestFile("Linera2.0/linera2/runtime.py", digest(b"new")),),
            (),
        )

        result = apply_manifest(manifest, self.root, lambda _path: b"new")

        self.assertFalse(result.updated)
        self.assertEqual(result.reason, "invalid manifest")

    def test_direct_manifest_model_rejects_empty_app_version(self):
        manifest = UpdateManifest(
            2,
            "v",
            "",
            (ManifestFile("Linera2.0/linera2/runtime.py", digest(b"new")),),
            (),
        )

        result = apply_manifest(manifest, self.root, lambda _path: b"new")

        self.assertFalse(result.updated)
        self.assertEqual(result.reason, "invalid manifest")

    def test_parent_symlink_cannot_escape_install_root(self):
        outside_dir = tempfile.TemporaryDirectory()
        self.addCleanup(outside_dir.cleanup)
        outside = Path(outside_dir.name)
        link = self.root / "Linera2.0"
        try:
            link.symlink_to(outside, target_is_directory=True)
        except OSError as error:
            self.skipTest(f"directory symlinks unavailable: {type(error).__name__}")
        manifest = manifest_for((("Linera2.0/linera2/runtime.py", b"escaped"),))

        result = apply_manifest(manifest, self.root, lambda _path: b"escaped")

        self.assertFalse(result.updated)
        self.assertFalse((outside / "linera2/runtime.py").exists())

    def test_all_downloads_are_verified_before_first_replace(self):
        a = self.write_live("Linera2.0/linera2/a.py", b"old-a")
        b = self.write_live("Linera2.0/linera2/b.py", b"old-b")
        manifest = manifest_for(
            (("Linera2.0/linera2/a.py", b"new-a"), ("Linera2.0/linera2/b.py", b"new-b"))
        )

        def fetch(path):
            return b"new-a" if path.endswith("a.py") else b"corrupt"

        result = apply_manifest(manifest, self.root, fetch)

        self.assertFalse(result.updated)
        self.assertEqual((a.read_bytes(), b.read_bytes()), (b"old-a", b"old-b"))

    def test_second_replace_failure_rolls_back_first(self):
        a = self.write_live("Linera2.0/linera2/a.py", b"old-a")
        b = self.write_live("Linera2.0/linera2/b.py", b"old-b")
        payloads = {
            "Linera2.0/linera2/a.py": b"new-a",
            "Linera2.0/linera2/b.py": b"new-b",
        }
        manifest = manifest_for(tuple(payloads.items()))

        with patch(
            "linera_runner.os.replace",
            side_effect=[None, OSError("locked"), None],
        ):
            result = apply_manifest(manifest, self.root, payloads.__getitem__)

        self.assertFalse(result.updated)
        self.assertEqual((a.read_bytes(), b.read_bytes()), (b"old-a", b"old-b"))

    def test_replace_failure_removes_new_files_created_earlier(self):
        existing = self.write_live("Linera2.0/linera2/b.py", b"old-b")
        payloads = {
            "Linera2.0/linera2/a.py": b"new-a",
            "Linera2.0/linera2/b.py": b"new-b",
        }
        manifest = manifest_for(tuple(payloads.items()))

        with patch("linera_runner.os.replace", side_effect=[None, OSError("locked")]):
            result = apply_manifest(manifest, self.root, payloads.__getitem__)

        self.assertFalse(result.updated)
        self.assertFalse((self.root / "Linera2.0/linera2/a.py").exists())
        self.assertEqual(existing.read_bytes(), b"old-b")

    def test_replace_failure_removes_transaction_created_directories(self):
        existing = self.write_live("Linera2.0/linera2/b.py", b"old-b")
        payloads = {
            "Linera2.0/templates/nested/a.html": b"new-a",
            "Linera2.0/linera2/b.py": b"new-b",
        }
        manifest = manifest_for(tuple(payloads.items()))

        with patch("linera_runner.os.replace", side_effect=[None, OSError("locked")]):
            result = apply_manifest(manifest, self.root, payloads.__getitem__)

        self.assertFalse(result.updated)
        self.assertFalse((self.root / "Linera2.0/templates").exists())
        self.assertEqual(existing.read_bytes(), b"old-b")

    def test_replace_failure_removes_transaction_created_install_root(self):
        install_root = self.root / "new-install"
        payloads = {
            "Linera2.0/linera2/a.py": b"new-a",
            "Linera2.0/linera2/b.py": b"new-b",
        }
        manifest = manifest_for(tuple(payloads.items()))

        with patch("linera_runner.os.replace", side_effect=[None, OSError("locked")]):
            result = apply_manifest(manifest, install_root, payloads.__getitem__)

        self.assertFalse(result.updated)
        self.assertFalse(install_root.exists())

    def test_unsafe_target_reports_when_prior_file_rollback_fails(self):
        self.write_live("Linera2.0/linera2/a.py", b"old-a")
        (self.root / "Linera2.0/linera2/b.py").mkdir()
        payloads = {
            "Linera2.0/linera2/a.py": b"new-a",
            "Linera2.0/linera2/b.py": b"new-b",
        }
        manifest = manifest_for(tuple(payloads.items()))
        real_replace = os.replace

        def fail_backup_restore(source, destination):
            if "backups" in str(source):
                raise OSError("restore locked")
            return real_replace(source, destination)

        with patch("linera_runner.os.replace", side_effect=fail_backup_restore):
            result = apply_manifest(manifest, self.root, payloads.__getitem__)

        self.assertFalse(result.updated)
        self.assertEqual(result.reason, "unsafe update target and rollback failed")

    def test_backup_failure_reports_when_prior_file_rollback_fails(self):
        self.write_live("Linera2.0/linera2/a.py", b"old-a")
        self.write_live("Linera2.0/linera2/b.py", b"old-b")
        payloads = {
            "Linera2.0/linera2/a.py": b"new-a",
            "Linera2.0/linera2/b.py": b"new-b",
        }
        manifest = manifest_for(tuple(payloads.items()))
        real_replace = os.replace
        real_copy = shutil.copy2

        def fail_backup_restore(source, destination):
            if "backups" in str(source):
                raise OSError("restore locked")
            return real_replace(source, destination)

        def fail_second_backup(source, destination):
            if str(source).endswith("b.py"):
                raise OSError("backup locked")
            return real_copy(source, destination)

        with patch("linera_runner.os.replace", side_effect=fail_backup_restore), patch(
            "linera_runner.shutil.copy2", side_effect=fail_second_backup
        ):
            result = apply_manifest(manifest, self.root, payloads.__getitem__)

        self.assertFalse(result.updated)
        self.assertEqual(result.reason, "backup and rollback failed")

    def test_success_replaces_changed_files_and_skips_unchanged_fetch(self):
        unchanged = self.write_live("Linera2.0/linera2/a.py", b"same")
        changed = self.write_live("Linera2.0/linera2/b.py", b"old")
        payloads = {
            "Linera2.0/linera2/a.py": b"same",
            "Linera2.0/linera2/b.py": b"new",
        }
        manifest = manifest_for(tuple(payloads.items()))
        fetched = []

        def fetch(path):
            fetched.append(path)
            return payloads[path]

        result = apply_manifest(manifest, self.root, fetch)

        self.assertTrue(result.updated)
        self.assertFalse(result.restart_required)
        self.assertEqual(fetched, ["Linera2.0/linera2/b.py"])
        self.assertEqual((unchanged.read_bytes(), changed.read_bytes()), (b"same", b"new"))

    def test_runner_change_requires_restart(self):
        self.write_live("linera_runner.py", b"old")
        manifest = manifest_for((("linera_runner.py", b"new"),))

        result = apply_manifest(manifest, self.root, lambda _path: b"new")

        self.assertTrue(result.updated)
        self.assertTrue(result.restart_required)

    def test_only_manifest_named_legacy_files_are_removed(self):
        legacy = self.write_live("linera_task.py", b"legacy")
        other_legacy = self.write_live("base_module.py", b"keep")
        state = self.write_live("Linera2.0/auto_sessions.json", b"{}")
        account = self.write_live("hubshuju.xlsx", b"private")
        log = self.write_live("Linera2.0/run.log", b"log")
        screenshot = self.write_live("Linera2.0/screenshot.png", b"png")
        manifest = manifest_for(
            (("Linera2.0/linera2/runtime.py", b"runtime"),),
            remove=("linera_task.py",),
        )

        with patch.dict(os.environ, {"OKX_WALLET_PASSWORD": "configured"}):
            result = apply_manifest(manifest, self.root, lambda _path: b"runtime")

        self.assertTrue(result.updated)
        self.assertFalse(legacy.exists())
        for protected in (other_legacy, state, account, log, screenshot):
            self.assertTrue(protected.exists())

    def test_no_changes_does_not_remove_legacy_files(self):
        self.write_live("Linera2.0/linera2/runtime.py", b"runtime")
        legacy = self.write_live("linera_task.py", b"legacy")
        manifest = manifest_for(
            (("Linera2.0/linera2/runtime.py", b"runtime"),),
            remove=("linera_task.py",),
        )

        result = apply_manifest(manifest, self.root, lambda _path: self.fail("fetch"))

        self.assertFalse(result.updated)
        self.assertTrue(legacy.exists())

    def test_removal_failure_restores_updates_and_prior_removals(self):
        runtime = self.write_live("Linera2.0/linera2/runtime.py", b"old")
        first_legacy = self.write_live("linera_task.py", b"legacy")
        blocked = self.root / "base_module.py"
        blocked.mkdir()
        manifest = manifest_for(
            (("Linera2.0/linera2/runtime.py", b"new"),),
            remove=("linera_task.py", "base_module.py"),
        )

        with patch.dict(os.environ, {"OKX_WALLET_PASSWORD": "configured"}):
            result = apply_manifest(manifest, self.root, lambda _path: b"new")

        self.assertFalse(result.updated)
        self.assertEqual(runtime.read_bytes(), b"old")
        self.assertEqual(first_legacy.read_bytes(), b"legacy")
        self.assertTrue(blocked.is_dir())

    def test_removal_restore_failure_is_reported(self):
        self.write_live("Linera2.0/linera2/runtime.py", b"old")
        self.write_live("linera_task.py", b"legacy")
        (self.root / "base_module.py").mkdir()
        manifest = manifest_for(
            (("Linera2.0/linera2/runtime.py", b"new"),),
            remove=("linera_task.py", "base_module.py"),
        )
        real_replace = os.replace

        def fail_removal_restore(source, destination):
            if "removal_backups" in str(source):
                raise OSError("restore locked")
            return real_replace(source, destination)

        with patch.dict(os.environ, {"OKX_WALLET_PASSWORD": "configured"}), patch(
            "linera_runner.os.replace", side_effect=fail_removal_restore
        ):
            result = apply_manifest(manifest, self.root, lambda _path: b"new")

        self.assertFalse(result.updated)
        self.assertEqual(result.reason, "removal and rollback failed")

    def test_private_config_unavailable_skips_all_legacy_removals(self):
        runtime = self.write_live("Linera2.0/linera2/runtime.py", b"old")
        task = self.write_live("linera_task.py", b"legacy-task")
        base = self.write_live("base_module.py", b"no private assignment")
        manifest = manifest_for(
            (("Linera2.0/linera2/runtime.py", b"new"),),
            remove=("linera_task.py", "base_module.py"),
        )

        with patch.dict(os.environ, {}, clear=True):
            result = apply_manifest(manifest, self.root, lambda _path: b"new")

        self.assertTrue(result.updated)
        self.assertEqual(runtime.read_bytes(), b"new")
        self.assertTrue(task.exists())
        self.assertTrue(base.exists())
        self.assertEqual(
            result.reason,
            "updated; legacy removal skipped/private config unavailable",
        )

    def test_private_config_is_migrated_before_base_module_removal(self):
        self.write_live("Linera2.0/linera2/runtime.py", b"old")
        base = self.write_live(
            "base_module.py", b'OKX_DEFAULT_PASSWORD = "legacy-private"\n'
        )
        manifest = manifest_for(
            (("Linera2.0/linera2/runtime.py", b"new"),),
            remove=("base_module.py",),
        )

        with patch.dict(os.environ, {}, clear=True):
            result = apply_manifest(manifest, self.root, lambda _path: b"new")

        self.assertTrue(result.updated)
        self.assertFalse(base.exists())
        config = json.loads(
            (self.root / "Linera2.0/local_config.json").read_text(encoding="utf-8")
        )
        self.assertEqual(config, {"wallet_password": "legacy-private"})

    def test_private_config_write_error_skips_all_legacy_removals(self):
        runtime = self.write_live("Linera2.0/linera2/runtime.py", b"old")
        task = self.write_live("linera_task.py", b"legacy-task")
        base = self.write_live(
            "base_module.py", b'OKX_DEFAULT_PASSWORD = "legacy-private"\n'
        )
        manifest = manifest_for(
            (("Linera2.0/linera2/runtime.py", b"new"),),
            remove=("linera_task.py", "base_module.py"),
        )

        with patch.dict(os.environ, {}, clear=True), patch(
            "linera_runner._atomic_write_private_config",
            side_effect=OSError("write denied"),
        ):
            result = apply_manifest(manifest, self.root, lambda _path: b"new")

        self.assertTrue(result.updated)
        self.assertEqual(runtime.read_bytes(), b"new")
        self.assertTrue(task.exists())
        self.assertTrue(base.exists())
        self.assertEqual(
            result.reason,
            "updated; legacy removal skipped/private config unavailable",
        )

    def test_sensitive_values_never_reach_output_or_result_reason(self):
        secret_download = b"download-secret-marker"
        manifest = UpdateManifest(
            2,
            "v",
            "v",
            (ManifestFile("Linera2.0/linera2/runtime.py", "f" * 64),),
            (),
        )
        output = io.StringIO()

        with redirect_stdout(output), redirect_stderr(output):
            result = apply_manifest(manifest, self.root, lambda _path: secret_download)

        self.assertNotIn("download-secret-marker", output.getvalue())
        self.assertNotIn("download-secret-marker", result.reason)


class PrivateConfigMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)

    def test_environment_private_config_is_sufficient_without_writing_file(self):
        with patch.dict(os.environ, {"OKX_WALLET_PASSWORD": "from-env"}, clear=True):
            self.assertTrue(migrate_private_config(self.root))
        self.assertFalse((self.root / "Linera2.0/local_config.json").exists())

    def test_existing_local_private_config_is_sufficient(self):
        config = self.root / "Linera2.0/local_config.json"
        config.parent.mkdir(parents=True)
        config.write_text('{"wallet_password":"already-local"}', encoding="utf-8")
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(migrate_private_config(self.root))
        self.assertEqual(json.loads(config.read_text(encoding="utf-8")), {"wallet_password": "already-local"})

    def test_migration_extracts_only_strict_legacy_assignment(self):
        legacy = self.root / "base_module.py"
        legacy.write_text(
            '  OKX_DEFAULT_PASSWORD = "indented-ignore"\n'
            'OKX_DEFAULT_PASSWORD = "migrated-value"\n'
            'AUTHORIZATION = "never-copy"\n',
            encoding="utf-8",
        )
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(migrate_private_config(self.root))

        config = self.root / "Linera2.0/local_config.json"
        self.assertEqual(json.loads(config.read_text(encoding="utf-8")), {"wallet_password": "migrated-value"})
        self.assertIn("AUTHORIZATION", legacy.read_text(encoding="utf-8"))

    def test_migration_does_not_emit_private_or_unrelated_values(self):
        legacy = self.root / "base_module.py"
        legacy.write_text(
            'OKX_DEFAULT_PASSWORD = "wallet-secret-marker"\n'
            'OTHER = "unrelated-secret-marker"\n',
            encoding="utf-8",
        )
        output = io.StringIO()

        with patch.dict(os.environ, {}, clear=True), redirect_stdout(output), redirect_stderr(output):
            self.assertTrue(migrate_private_config(self.root))

        self.assertNotIn("wallet-secret-marker", output.getvalue())
        self.assertNotIn("unrelated-secret-marker", output.getvalue())

    def test_missing_or_unparseable_private_config_returns_false(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(migrate_private_config(self.root))
            (self.root / "base_module.py").write_text(
                'OKX_DEFAULT_PASSWORD = os.environ.get("PASSWORD")', encoding="utf-8"
            )
            self.assertFalse(migrate_private_config(self.root))
        self.assertFalse((self.root / "Linera2.0/local_config.json").exists())


if __name__ == "__main__":
    unittest.main()
