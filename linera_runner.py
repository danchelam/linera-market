"""Linera 2.0 bootstrap update primitives.

This module deliberately has no import-time network, launch, or filesystem
side effects.  The update bridge and command-line entry point are layered on
top of these primitives.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Callable


__version__ = "2026.07.16.1"


ALLOWED_PREFIXES = ("Linera2.0/linera2/", "Linera2.0/templates/")
ALLOWED_ROOT_FILES = {
    "linera_runner.py",
    "Linera2.0/requirements.txt",
    "Linera2.0/README.md",
}
ALLOWED_REMOVALS = {"linera_task.py", "base_module.py", "test_full_flow.py"}
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_FILE_ATTRIBUTE_REPARSE_POINT = 0x400
_LEGACY_PASSWORD_PATTERN = re.compile(
    r'^OKX_DEFAULT_PASSWORD\s*=\s*["\']([^"\']+)["\']',
    re.M,
)


class ManifestError(ValueError):
    """Raised when a release manifest violates the schema or path policy."""


@dataclass(frozen=True)
class ManifestFile:
    path: str
    sha256: str


@dataclass(frozen=True)
class UpdateManifest:
    schema_version: int
    runner_version: str
    app_version: str
    files: tuple[ManifestFile, ...]
    remove: tuple[str, ...]
    task_version: str = ""
    base_version: str = ""


@dataclass(frozen=True)
class UpdateResult:
    updated: bool
    restart_required: bool
    reason: str


def validate_relative_path(value: str) -> str:
    """Return the canonical manifest path or reject it as unsafe."""
    if not isinstance(value, str) or not value or "\\" in value:
        raise ManifestError("unsafe manifest path")
    candidate = PurePosixPath(value)
    if (
        candidate.is_absolute()
        or ".." in candidate.parts
        or re.match(r"^[A-Za-z]:", value)
    ):
        raise ManifestError("unsafe manifest path")
    normalized = candidate.as_posix()
    if normalized not in ALLOWED_ROOT_FILES and not normalized.startswith(
        ALLOWED_PREFIXES
    ):
        raise ManifestError("path outside update allowlist")
    return normalized


def _manifest_string(data: dict, name: str) -> str:
    value = data.get(name)
    if not isinstance(value, str) or not value:
        raise ManifestError(f"invalid {name}")
    return value


def _optional_manifest_string(data: dict, name: str) -> str:
    value = data.get(name, "")
    if not isinstance(value, str):
        raise ManifestError(f"invalid {name}")
    return value


def parse_manifest(payload: str) -> UpdateManifest:
    """Parse and strictly validate a schema-v2 update manifest."""
    if not isinstance(payload, str):
        raise ManifestError("manifest payload must be text")
    try:
        data = json.loads(payload)
    except (json.JSONDecodeError, UnicodeError) as error:
        raise ManifestError("invalid manifest JSON") from error
    if not isinstance(data, dict):
        raise ManifestError("manifest must be an object")
    schema_version = data.get("schema_version")
    if type(schema_version) is not int or schema_version != 2:
        raise ManifestError("unsupported manifest schema")

    runner_version = _manifest_string(data, "runner_version")
    app_version = _manifest_string(data, "app_version")
    task_version = _optional_manifest_string(data, "task_version")
    base_version = _optional_manifest_string(data, "base_version")
    raw_files = data.get("files")
    raw_removals = data.get("remove")
    if not isinstance(raw_files, list):
        raise ManifestError("manifest files must be a list")
    if not isinstance(raw_removals, list):
        raise ManifestError("manifest remove must be a list")

    files: list[ManifestFile] = []
    seen_paths: set[str] = set()
    for item in raw_files:
        if not isinstance(item, dict):
            raise ManifestError("invalid manifest file entry")
        path = validate_relative_path(item.get("path"))
        file_hash = item.get("sha256")
        if not isinstance(file_hash, str) or not _SHA256_PATTERN.fullmatch(file_hash):
            raise ManifestError("invalid SHA-256")
        if path in seen_paths:
            raise ManifestError("duplicate manifest path")
        seen_paths.add(path)
        files.append(ManifestFile(path, file_hash))

    removals: list[str] = []
    seen_removals: set[str] = set()
    for item in raw_removals:
        if not isinstance(item, str) or item not in ALLOWED_REMOVALS:
            raise ManifestError("removal outside legacy allowlist")
        if item in seen_removals:
            raise ManifestError("duplicate removal path")
        seen_removals.add(item)
        removals.append(item)

    return UpdateManifest(
        schema_version=2,
        runner_version=runner_version,
        app_version=app_version,
        files=tuple(files),
        remove=tuple(removals),
        task_version=task_version,
        base_version=base_version,
    )


def sha256_file(path: Path) -> str:
    """Hash raw file bytes without loading private content into logs."""
    hasher = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _safe_install_path(install_root: Path, relative: str) -> Path:
    root = install_root.resolve()
    target = install_root / relative
    try:
        target.resolve(strict=False).relative_to(root)
    except (OSError, ValueError) as error:
        raise ManifestError("update path escapes install root") from error

    cursor = install_root
    for part in PurePosixPath(relative).parts[:-1]:
        cursor /= part
        if not cursor.exists() and not cursor.is_symlink():
            continue
        try:
            metadata = os.lstat(cursor)
        except OSError as error:
            raise ManifestError("unsafe update path component") from error
        if cursor.is_symlink() or (
            getattr(metadata, "st_file_attributes", 0)
            & _FILE_ATTRIBUTE_REPARSE_POINT
        ):
            raise ManifestError("unsafe update path component")
    return target


def _changed_entries(
    entries: tuple[ManifestFile, ...], install_root: Path
) -> tuple[ManifestFile, ...]:
    changed: list[ManifestFile] = []
    for entry in entries:
        target = _safe_install_path(install_root, entry.path)
        if target.is_symlink():
            raise ManifestError("update target must not be a symlink")
        try:
            current_hash = sha256_file(target)
        except (FileNotFoundError, IsADirectoryError, PermissionError, OSError):
            current_hash = None
        if current_hash != entry.sha256:
            changed.append(entry)
    return tuple(changed)


def _validate_manifest_for_apply(manifest: UpdateManifest) -> None:
    if (
        not isinstance(manifest, UpdateManifest)
        or type(manifest.schema_version) is not int
        or manifest.schema_version != 2
    ):
        raise ManifestError("invalid manifest model")
    if (
        not isinstance(manifest.runner_version, str)
        or not manifest.runner_version
        or not isinstance(manifest.app_version, str)
        or not manifest.app_version
    ):
        raise ManifestError("invalid manifest model")
    if not isinstance(manifest.task_version, str) or not isinstance(
        manifest.base_version, str
    ):
        raise ManifestError("invalid manifest model")
    seen_paths: set[str] = set()
    for entry in manifest.files:
        if not isinstance(entry, ManifestFile):
            raise ManifestError("invalid manifest file entry")
        normalized = validate_relative_path(entry.path)
        if normalized != entry.path or normalized in seen_paths:
            raise ManifestError("invalid manifest path")
        if not isinstance(entry.sha256, str) or not _SHA256_PATTERN.fullmatch(
            entry.sha256
        ):
            raise ManifestError("invalid SHA-256")
        seen_paths.add(normalized)
    seen_removals: set[str] = set()
    for relative in manifest.remove:
        if relative not in ALLOWED_REMOVALS or relative in seen_removals:
            raise ManifestError("invalid removal path")
        seen_removals.add(relative)


def _stage_and_verify(
    changed: tuple[ManifestFile, ...],
    staging_root: Path,
    fetch_file: Callable[[str], bytes],
) -> tuple[tuple[ManifestFile, Path], ...]:
    staged: list[tuple[ManifestFile, Path]] = []
    for entry in changed:
        content = fetch_file(entry.path)
        if not isinstance(content, bytes):
            raise TypeError("downloaded file must be bytes")
        target = staging_root / "staged" / entry.path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        if sha256_file(target) != entry.sha256:
            raise ManifestError("downloaded file hash mismatch")
        staged.append((entry, target))
    return tuple(staged)


def _remove_created_directories(created_directories: list[Path]) -> bool:
    restored = True
    for directory in reversed(created_directories):
        try:
            directory.rmdir()
        except FileNotFoundError:
            pass
        except OSError:
            restored = False
    return restored


def _restore_replaced(
    replaced: list[tuple[Path, Path | None]],
    created_directories: list[Path] | None = None,
) -> bool:
    restored = True
    for live, backup in reversed(replaced):
        try:
            if backup is None:
                live.unlink(missing_ok=True)
            else:
                os.replace(backup, live)
        except OSError:
            restored = False
    if created_directories is not None:
        restored = _remove_created_directories(created_directories) and restored
    return restored


def _mkdir_parents_tracked(
    parent: Path, install_root: Path, created_directories: list[Path]
) -> None:
    missing: list[Path] = []
    cursor = parent
    while cursor != install_root and not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
    for directory in reversed(missing):
        try:
            directory.mkdir()
        except FileExistsError:
            if not directory.is_dir():
                raise
        else:
            created_directories.append(directory)


def _replace_with_rollback(
    staged: tuple[tuple[ManifestFile, Path], ...],
    install_root: Path,
    temporary_root: Path,
) -> tuple[
    UpdateResult,
    list[tuple[Path, Path | None]],
    list[Path],
]:
    replaced: list[tuple[Path, Path | None]] = []
    created_directories: list[Path] = []
    for entry, staged_path in staged:
        try:
            live = _safe_install_path(install_root, entry.path)
            _mkdir_parents_tracked(
                live.parent, install_root, created_directories
            )
            live = _safe_install_path(install_root, entry.path)
        except (ManifestError, OSError):
            restored = _restore_replaced(replaced, created_directories)
            reason = (
                "unsafe update target"
                if restored
                else "unsafe update target and rollback failed"
            )
            return UpdateResult(False, False, reason), [], []
        backup: Path | None = None
        if live.exists():
            if not live.is_file() or live.is_symlink():
                restored = _restore_replaced(replaced, created_directories)
                reason = (
                    "unsafe update target"
                    if restored
                    else "unsafe update target and rollback failed"
                )
                return UpdateResult(False, False, reason), [], []
            backup = temporary_root / "backups" / entry.path
            try:
                backup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(live, backup)
            except OSError:
                restored = _restore_replaced(replaced, created_directories)
                reason = (
                    "backup failed"
                    if restored
                    else "backup and rollback failed"
                )
                return UpdateResult(False, False, reason), [], []
        try:
            os.replace(staged_path, live)
        except OSError:
            restored = _restore_replaced(replaced, created_directories)
            reason = "replace failed" if restored else "replace and rollback failed"
            return UpdateResult(False, False, reason), [], []
        replaced.append((live, backup))
    return UpdateResult(True, False, "updated"), replaced, created_directories


def _remove_exact_legacy_paths(
    removals: tuple[str, ...], install_root: Path, temporary_root: Path
) -> tuple[bool, bool]:
    removed: list[tuple[Path, Path]] = []

    def restore_removed() -> bool:
        restored = True
        for live, backup in reversed(removed):
            try:
                os.replace(backup, live)
            except OSError:
                restored = False
        return restored

    for relative in removals:
        if relative not in ALLOWED_REMOVALS:
            raise ManifestError("removal outside legacy allowlist")
        target = install_root / relative
        if target.is_symlink() or (target.exists() and not target.is_file()):
            return False, restore_removed()
        if not target.exists():
            continue
        backup = temporary_root / "removal_backups" / relative
        try:
            backup.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(target, backup)
            target.unlink()
        except OSError:
            return False, restore_removed()
        removed.append((target, backup))
    return True, True


def _apply_manifest_transaction(
    manifest: UpdateManifest,
    changed: tuple[ManifestFile, ...],
    install_root: Path,
    fetch_file: Callable[[str], bytes],
) -> UpdateResult:
    install_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=install_root) as temp_name:
        temporary_root = Path(temp_name)
        try:
            staged = _stage_and_verify(changed, temporary_root, fetch_file)
        except (ManifestError, OSError, TypeError):
            return UpdateResult(False, False, "staging verification failed")
        result, replaced_files, created_directories = _replace_with_rollback(
            staged, install_root, temporary_root
        )
        if not result.updated:
            return result
        restart_required = any(item.path == "linera_runner.py" for item in changed)
        private_config_ready = True
        if manifest.remove:
            try:
                private_config_ready = migrate_private_config(install_root)
            except (OSError, UnicodeError):
                private_config_ready = False
        if not private_config_ready:
            return UpdateResult(
                updated=True,
                restart_required=restart_required,
                reason="updated; legacy removal skipped/private config unavailable",
            )
        try:
            removals_ok, removal_rollback_ok = _remove_exact_legacy_paths(
                manifest.remove, install_root, temporary_root
            )
        except (ManifestError, AttributeError, TypeError, OSError):
            removals_ok = False
            removal_rollback_ok = False
        if not removals_ok:
            restored = _restore_replaced(replaced_files, created_directories)
            reason = (
                "removal failed"
                if restored and removal_rollback_ok
                else "removal and rollback failed"
            )
            return UpdateResult(False, False, reason)
    return replace(result, restart_required=restart_required)


def apply_manifest(
    manifest: UpdateManifest,
    install_root: Path,
    fetch_file: Callable[[str], bytes],
) -> UpdateResult:
    """Apply a verified manifest transaction without touching private state."""
    install_root = Path(install_root)
    try:
        _validate_manifest_for_apply(manifest)
        changed = _changed_entries(manifest.files, install_root)
    except (ManifestError, AttributeError, TypeError):
        return UpdateResult(False, False, "invalid manifest")
    if not changed:
        return UpdateResult(False, False, "up to date")

    install_root_created = not install_root.exists()
    try:
        result = _apply_manifest_transaction(
            manifest, changed, install_root, fetch_file
        )
    except OSError:
        result = UpdateResult(False, False, "staging verification failed")

    if install_root_created and not result.updated and install_root.exists():
        try:
            install_root.rmdir()
        except OSError:
            result = replace(
                result,
                reason=f"{result.reason}; install root rollback failed",
            )
    return result


def _read_local_password(config_path: Path) -> str | None:
    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    value = data.get("wallet_password")
    if not isinstance(value, str):
        return None
    return value.strip() or None


def _atomic_write_private_config(target: Path, password: str) -> bool:
    temporary_path: Path | None = None
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            delete=False,
            dir=target.parent,
        ) as temporary:
            json.dump({"wallet_password": password}, temporary, ensure_ascii=False)
            temporary_path = Path(temporary.name)
        os.replace(temporary_path, target)
        temporary_path = None
        return True
    except OSError:
        return False
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass


def migrate_private_config(install_root: Path) -> bool:
    """Migrate only the legacy wallet password without importing old code."""
    if os.environ.get("OKX_WALLET_PASSWORD", "").strip():
        return True
    install_root = Path(install_root)
    config_path = install_root / "Linera2.0" / "local_config.json"
    if _read_local_password(config_path):
        return True
    legacy_path = install_root / "base_module.py"
    try:
        legacy_text = legacy_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return False
    match = _LEGACY_PASSWORD_PATTERN.search(legacy_text)
    if not match:
        return False
    return _atomic_write_private_config(config_path, match.group(1))
