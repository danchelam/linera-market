from __future__ import annotations

import hashlib
import http.client
import io
import json
import os
import re
import shutil
import sys
import tempfile
import types
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
    UpdateResult,
    apply_manifest,
    fetch_remote_manifest,
    launch_linera2,
    main,
    migrate_private_config,
    parse_manifest,
    run_update,
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


class ReadFailureResponse:
    def __init__(self, error):
        self.error = error

    def __enter__(self):
        return self

    def __exit__(self, _error_type, _error, _traceback):
        return False

    def read(self):
        raise self.error


class ManifestParsingTests(unittest.TestCase):
    def test_update_result_defaults_removals_completed_for_legacy_callers(self):
        result = UpdateResult(False, False, "up to date")

        self.assertIs(result.removals_completed, True)

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

    def test_legacy_compatibility_fields_are_accepted(self):
        result = parse_manifest(
            manifest_json(
                runner_version="2026.07.15.1",
                task_version="",
                base_version="",
            )
        )

        self.assertEqual(result.runner_version, "2026.07.15.1")

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

    def test_partial_parent_creation_failure_rolls_back_created_directories(self):
        target_parent = self.root / "Linera2.0/templates/nested"
        manifest = manifest_for(
            (("Linera2.0/templates/nested/index.html", b"new"),)
        )
        real_mkdir = Path.mkdir

        def fail_after_partial_creation(path, *args, **kwargs):
            if path == target_parent:
                real_mkdir(path.parent, parents=True, exist_ok=True)
                raise OSError("directory creation failed")
            return real_mkdir(path, *args, **kwargs)

        with patch(
            "pathlib.Path.mkdir", autospec=True, side_effect=fail_after_partial_creation
        ):
            result = apply_manifest(manifest, self.root, lambda _path: b"new")

        self.assertFalse(result.updated)
        self.assertFalse((self.root / "Linera2.0").exists())

    def test_replacement_backup_directory_failure_rolls_back_prior_file(self):
        first = self.write_live("Linera2.0/linera2/a.py", b"old-a")
        second = self.write_live("Linera2.0/linera2/b.py", b"old-b")
        payloads = {
            "Linera2.0/linera2/a.py": b"new-a",
            "Linera2.0/linera2/b.py": b"new-b",
        }
        manifest = manifest_for(tuple(payloads.items()))
        real_mkdir = Path.mkdir
        backup_calls = 0

        def fail_second_backup_directory(path, *args, **kwargs):
            nonlocal backup_calls
            if "backups" in path.parts and path.name == "linera2":
                backup_calls += 1
                if backup_calls == 2:
                    raise OSError("backup directory failed")
            return real_mkdir(path, *args, **kwargs)

        with patch(
            "pathlib.Path.mkdir", autospec=True, side_effect=fail_second_backup_directory
        ):
            result = apply_manifest(manifest, self.root, payloads.__getitem__)

        self.assertFalse(result.updated)
        self.assertEqual(result.reason, "backup failed")
        self.assertEqual((first.read_bytes(), second.read_bytes()), (b"old-a", b"old-b"))

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
        self.assertTrue(result.removals_completed)
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

    def test_removal_backup_directory_failure_restores_prior_removal_and_update(self):
        runtime = self.write_live("Linera2.0/linera2/runtime.py", b"old")
        first = self.write_live("linera_task.py", b"legacy-task")
        second = self.write_live("base_module.py", b"legacy-base")
        manifest = manifest_for(
            (("Linera2.0/linera2/runtime.py", b"new"),),
            remove=("linera_task.py", "base_module.py"),
        )
        real_mkdir = Path.mkdir
        removal_backup_calls = 0

        def fail_second_removal_backup_directory(path, *args, **kwargs):
            nonlocal removal_backup_calls
            if "removal_backups" in path.parts:
                removal_backup_calls += 1
                if removal_backup_calls == 2:
                    raise OSError("removal backup directory failed")
            return real_mkdir(path, *args, **kwargs)

        with patch.dict(os.environ, {"OKX_WALLET_PASSWORD": "configured"}), patch(
            "pathlib.Path.mkdir",
            autospec=True,
            side_effect=fail_second_removal_backup_directory,
        ):
            result = apply_manifest(manifest, self.root, lambda _path: b"new")

        self.assertFalse(result.updated)
        self.assertEqual(result.reason, "removal failed")
        self.assertEqual(runtime.read_bytes(), b"old")
        self.assertEqual(first.read_bytes(), b"legacy-task")
        self.assertEqual(second.read_bytes(), b"legacy-base")

    def test_install_root_cleanup_failure_reports_rollback_failure(self):
        install_root = self.root / "new-install"
        manifest = UpdateManifest(
            2,
            "v",
            "v",
            (ManifestFile("Linera2.0/linera2/runtime.py", "f" * 64),),
            (),
        )
        real_rmdir = Path.rmdir

        def fail_install_root_cleanup(path):
            if path == install_root:
                raise OSError("cleanup denied")
            return real_rmdir(path)

        with patch(
            "pathlib.Path.rmdir", autospec=True, side_effect=fail_install_root_cleanup
        ):
            result = apply_manifest(
                manifest, install_root, lambda _path: b"corrupt"
            )

        self.assertFalse(result.updated)
        self.assertEqual(
            result.reason,
            "staging verification failed; install root rollback failed",
        )

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

    def test_runner_change_with_unavailable_migration_marks_removals_incomplete(self):
        self.write_live("linera_runner.py", b"old-runner")
        legacy = self.write_live("linera_task.py", b"legacy-task")
        manifest = manifest_for(
            (("linera_runner.py", b"new-runner"),),
            remove=("linera_task.py",),
        )

        with patch.dict(os.environ, {}, clear=True):
            result = apply_manifest(manifest, self.root, lambda _path: b"new-runner")

        self.assertTrue(result.updated)
        self.assertTrue(result.restart_required)
        self.assertIs(result.removals_completed, False)
        self.assertTrue(legacy.exists())

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


class RunnerBridgeTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)

    def test_fetch_manifest_falls_back_to_jsdelivr_with_cache_busters(self):
        primary = (
            "https://raw.githubusercontent.com/danchelam/linera-market/"
            "refs/heads/main"
        )
        with patch(
            "linera_runner.urllib.request.urlopen",
            side_effect=[OSError("raw unavailable"), io.BytesIO(manifest_json().encode())],
        ) as open_url:
            result = fetch_remote_manifest(primary)

        self.assertEqual(result.schema_version, 2)
        urls = [item.args[0] for item in open_url.call_args_list]
        self.assertRegex(urls[0], rf"^{re.escape(primary)}/version\.json\?t=\d+$")
        self.assertRegex(
            urls[1],
            r"^https://cdn\.jsdelivr\.net/gh/danchelam/linera-market@main/"
            r"version\.json\?t=\d+$",
        )

    def test_incomplete_raw_response_falls_back_to_jsdelivr(self):
        primary = (
            "https://raw.githubusercontent.com/danchelam/linera-market/"
            "refs/heads/main"
        )
        incomplete = ReadFailureResponse(
            http.client.IncompleteRead(b"partial-secret-response")
        )
        with patch(
            "linera_runner.urllib.request.urlopen",
            side_effect=[incomplete, io.BytesIO(manifest_json().encode())],
        ) as open_url:
            result = fetch_remote_manifest(primary)

        self.assertEqual(result.schema_version, 2)
        self.assertIn("cdn.jsdelivr.net", open_url.call_args_list[1].args[0])

    def test_incomplete_responses_launch_installed_version_without_leaking(self):
        (self.root / "Linera2.0/linera2").mkdir(parents=True)
        responses = [
            ReadFailureResponse(
                http.client.IncompleteRead(b"authorization-secret-one")
            ),
            ReadFailureResponse(
                http.client.HTTPException("credential-secret-two")
            ),
        ]
        output = io.StringIO()
        with patch("linera_runner._install_root", return_value=self.root), patch(
            "linera_runner.urllib.request.urlopen", side_effect=responses
        ), patch(
            "linera_runner.launch_linera2", return_value=0
        ) as launch, redirect_stderr(output):
            self.assertEqual(main([]), 0)

        launch.assert_called_once_with(self.root, [])
        self.assertEqual(len(output.getvalue().splitlines()), 1)
        self.assertNotIn("authorization-secret-one", output.getvalue())
        self.assertNotIn("credential-secret-two", output.getvalue())

    def test_run_update_downloads_manifest_files_from_release_base(self):
        payload = b"new-runtime"
        manifest = manifest_for((("Linera2.0/linera2/runtime.py", payload),))

        with patch("linera_runner.fetch_remote_manifest", return_value=manifest), patch(
            "linera_runner.urllib.request.urlopen", return_value=io.BytesIO(payload)
        ) as open_url:
            result = run_update(self.root, "https://updates.example/release")

        self.assertTrue(result.updated)
        self.assertEqual(
            (self.root / "Linera2.0/linera2/runtime.py").read_bytes(), payload
        )
        self.assertRegex(
            open_url.call_args.args[0],
            r"^https://updates\.example/release/Linera2\.0/linera2/runtime\.py\?t=\d+$",
        )

    def test_manifest_unavailable_launches_installed_version(self):
        (self.root / "Linera2.0/linera2").mkdir(parents=True)
        with patch("linera_runner._install_root", return_value=self.root), patch(
            "linera_runner.fetch_remote_manifest", side_effect=OSError("offline")
        ), patch("linera_runner.launch_linera2", return_value=0) as launch:
            self.assertEqual(main([]), 0)

        launch.assert_called_once_with(self.root, [])

    def test_manifest_failure_warning_does_not_include_exception_text(self):
        (self.root / "Linera2.0/linera2").mkdir(parents=True)
        output = io.StringIO()
        with patch("linera_runner._install_root", return_value=self.root), patch(
            "linera_runner.fetch_remote_manifest",
            side_effect=OSError("authorization-secret-marker"),
        ), patch("linera_runner.launch_linera2", return_value=0), redirect_stderr(
            output
        ):
            self.assertEqual(main([]), 0)

        self.assertNotIn("authorization-secret-marker", output.getvalue())
        self.assertEqual(len(output.getvalue().splitlines()), 1)

    def test_failed_update_launches_installed_version(self):
        (self.root / "Linera2.0/linera2").mkdir(parents=True)
        with patch("linera_runner._install_root", return_value=self.root), patch(
            "linera_runner.run_update",
            return_value=UpdateResult(False, False, "staging verification failed"),
        ), patch("linera_runner.launch_linera2", return_value=7) as launch:
            self.assertEqual(main(["--web"]), 7)

        launch.assert_called_once_with(self.root, ["--web"])

    def test_failed_first_install_returns_error_without_launching(self):
        with patch("linera_runner._install_root", return_value=self.root), patch(
            "linera_runner.run_update",
            return_value=UpdateResult(False, False, "staging verification failed"),
        ), patch("linera_runner.launch_linera2") as launch:
            self.assertEqual(main([]), 1)

        launch.assert_not_called()

    def test_successful_runner_update_restarts_without_launching_old_process(self):
        with patch("linera_runner._install_root", return_value=self.root), patch(
            "linera_runner.run_update",
            return_value=UpdateResult(True, True, "updated"),
        ), patch("linera_runner._restart_self") as restart, patch(
            "linera_runner.launch_linera2"
        ) as launch:
            self.assertEqual(main(["--web"]), 0)

        restart.assert_called_once_with(["--web"])
        launch.assert_not_called()

    def test_runner_update_with_incomplete_removals_does_not_restart(self):
        (self.root / "Linera2.0/linera2").mkdir(parents=True)
        output = io.StringIO()
        result = UpdateResult(
            True,
            True,
            "dynamic-secret-reason",
            removals_completed=False,
        )
        with patch("linera_runner._install_root", return_value=self.root), patch(
            "linera_runner.run_update", return_value=result
        ), patch("linera_runner._restart_self") as restart, patch(
            "linera_runner.launch_linera2", return_value=0
        ) as launch, redirect_stderr(output):
            self.assertEqual(main(["--web"]), 0)

        restart.assert_not_called()
        launch.assert_called_once_with(self.root, ["--web"])
        self.assertEqual(
            output.getvalue().strip(),
            "【更新】旧文件移除未完成，当前进程继续启动已安装版本。",
        )
        self.assertNotIn("dynamic-secret-reason", output.getvalue())

    def test_launch_inserts_package_root_and_calls_cli(self):
        fake_cli = types.ModuleType("linera2.cli")
        fake_cli.argv = None

        def cli_main(argv):
            fake_cli.argv = argv
            return 0

        fake_cli.main = cli_main
        fake_package = types.ModuleType("linera2")
        fake_package.__path__ = []
        package_root = str(self.root / "Linera2.0")
        original_path = list(sys.path)
        self.addCleanup(lambda: setattr(sys, "path", original_path))

        with patch.dict(
            sys.modules,
            {"linera2": fake_package, "linera2.cli": fake_cli},
        ):
            result = launch_linera2(self.root, ["--web", "--workers", "1"])

        self.assertEqual(sys.path[0], package_root)
        self.assertEqual(fake_cli.argv, ["--web", "--workers", "1"])
        self.assertEqual(result, 0)

    def test_runtime_requirements_are_bounded_and_portable(self):
        requirements = (REPO_ROOT / "Linera2.0/requirements.txt").read_text(
            encoding="utf-8"
        )

        self.assertEqual(
            requirements.splitlines(),
            [
                "flask>=3.0,<4",
                "openpyxl>=3.1,<4",
                "pandas>=2.2,<3",
                "playwright>=1.50,<2",
                "requests>=2.32,<3",
            ],
        )


if __name__ == "__main__":
    unittest.main()
