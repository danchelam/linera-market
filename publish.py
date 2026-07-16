"""Build and safely publish the Linera2 runtime manifest."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


GITHUB_REPO = "danchelam/linera-market"
GITHUB_BRANCH = "main"
RAW_ROOT = f"https://raw.githubusercontent.com/{GITHUB_REPO}/refs/heads/{GITHUB_BRANCH}"

ROOT_RUNTIME_FILES = {
    "linera_runner.py",
    "Linera2.0/README.md",
    "Linera2.0/requirements.txt",
}
PACKAGE_PATTERNS = ("Linera2.0/linera2/*.py", "Linera2.0/templates/*")
LEGACY_REMOVALS = ("linera_task.py", "base_module.py", "test_full_flow.py")


class PublishError(RuntimeError):
    """A publication safety check failed."""


@dataclass(frozen=True)
class Finding:
    path: Path
    rule_name: str


@dataclass(frozen=True)
class PublishResult:
    pushed: bool
    remote_verified: bool
    version: str
    staged_paths: tuple[str, ...]
    deleted_paths: tuple[str, ...]


def _relative_posix(path: Path, repo_root: Path) -> str:
    try:
        return path.relative_to(repo_root).as_posix()
    except ValueError as exc:
        raise PublishError("publication path is outside the repository") from exc


def _reject_symlink(path: Path, repo_root: Path) -> None:
    relative = path.relative_to(repo_root)
    current = repo_root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise PublishError(f"symlink is not publishable: {relative.as_posix()}")


def runtime_files(repo_root: Path) -> tuple[Path, ...]:
    """Return the deterministic, existing runtime allowlist."""
    root = Path(repo_root).absolute()
    selected: set[Path] = set()
    for relative in ROOT_RUNTIME_FILES:
        candidate = root / relative
        if candidate.is_file() or candidate.is_symlink():
            selected.add(candidate)
    for pattern in PACKAGE_PATTERNS:
        selected.update(path for path in root.glob(pattern) if path.is_file() or path.is_symlink())

    for path in selected:
        _reject_symlink(path, root)
        if not path.is_file():
            raise PublishError(f"runtime entry is not a regular file: {_relative_posix(path, root)}")
    return tuple(sorted(selected, key=lambda path: _relative_posix(path, root)))


_SENSITIVE_RULES = (
    ("private-key-header", re.compile(r"-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----")),
    (
        "authorization-assignment",
        re.compile(
            r"(?im)(?:^|[,{])\s*['\"]?(?:authorization|cookie)['\"]?\s*[:=]\s*"
            r"(['\"])[^'\"\r\n]+\1"
        ),
    ),
    ("full-hex-address", re.compile(r"(?i)(?<![0-9a-f])0x[0-9a-f]{40}(?![0-9a-f])")),
    (
        "secret-assignment",
        re.compile(
            r"(?im)^\s*[A-Za-z_][A-Za-z0-9_]*(?:password|token|secret)[A-Za-z0-9_]*"
            r"(?:\s*:\s*[^=\r\n]+)?\s*=\s*(['\"])(?=.)[^'\"\r\n]+\1"
        ),
    ),
)


def scan_sensitive_files(paths: Iterable[Path]) -> list[Finding]:
    """Scan only selected publication files without retaining matched values."""
    findings: list[Finding] = []
    for path in sorted((Path(path) for path in paths), key=lambda item: item.as_posix()):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            text = path.read_bytes().decode("utf-8", errors="ignore")
        for rule_name, pattern in _SENSITIVE_RULES:
            if pattern.search(text):
                findings.append(Finding(path=path, rule_name=rule_name))
    return findings


def build_manifest(repo_root: Path, version: str) -> dict:
    if not isinstance(version, str) or not version.strip():
        raise PublishError("version must be a non-empty string")
    root = Path(repo_root).absolute()
    files = [
        {
            "path": _relative_posix(path, root),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
        for path in runtime_files(root)
    ]
    return {
        "task_version": "",
        "base_version": "",
        "runner_version": version,
        "schema_version": 2,
        "app_version": version,
        "files": files,
        "remove": list(LEGACY_REMOVALS),
    }


def _write_manifest_atomic(repo_root: Path, manifest: dict) -> None:
    target = Path(repo_root) / "version.json"
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            newline="\n",
            dir=target.parent,
            prefix=".version.json.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_name = handle.name
            json.dump(manifest, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, target)
        temporary_name = None
    finally:
        if temporary_name:
            Path(temporary_name).unlink(missing_ok=True)


def run_git(*args: str, check: bool = True, cwd: Path | None = None) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if check and result.returncode != 0:
        raise PublishError(f"git command failed: {' '.join(args)}")
    return result


def ensure_fast_forward(repo_root: Path, remote_ref: str = "origin/main") -> None:
    if "/" not in remote_ref:
        raise PublishError("remote_ref must contain a remote and branch")
    remote, branch = remote_ref.split("/", 1)
    run_git("fetch", remote, branch, cwd=Path(repo_root))
    result = run_git(
        "merge-base", "--is-ancestor", remote_ref, "HEAD", check=False, cwd=Path(repo_root)
    )
    if result.returncode != 0:
        raise PublishError(f"{remote_ref} is not an ancestor of HEAD")


def ensure_clean_index(repo_root: Path) -> None:
    """Refuse a real publication when the caller already staged any path."""
    result = run_git(
        "diff", "--cached", "--quiet", "--exit-code", check=False, cwd=Path(repo_root)
    )
    if result.returncode != 0:
        raise PublishError("Git index is not clean; publication aborted")


def _run_tests(repo_root: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "Linera2.0/tests", "-v"],
        cwd=repo_root,
    )
    if result.returncode != 0:
        raise PublishError("test suite failed; publication aborted")


def _open_url(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"Cache-Control": "no-cache"})
    with urllib.request.urlopen(request, timeout=20) as response:
        return response.read()


def _cache_busted(relative: str) -> str:
    return f"{RAW_ROOT}/{relative}?publish_verify={time.time_ns()}"


def _verify_remote(repo_root: Path, version: str, expected_manifest: dict) -> bool:
    del repo_root  # Kept in the interface for a future configurable remote.
    remote_manifest = json.loads(_open_url(_cache_busted("version.json")).decode("utf-8"))
    if remote_manifest.get("app_version") != version:
        raise PublishError("remote manifest version does not match the published version")

    expected = {entry["path"]: entry["sha256"] for entry in expected_manifest["files"]}
    remote = {entry["path"]: entry["sha256"] for entry in remote_manifest.get("files", [])}
    package_paths = sorted(path for path in expected if path.startswith("Linera2.0/linera2/"))
    verification_paths = ["linera_runner.py"]
    if not package_paths:
        raise PublishError("manifest has no package file to verify")
    verification_paths.append(package_paths[0])

    for relative in verification_paths:
        if remote.get(relative) != expected.get(relative):
            raise PublishError(f"remote manifest hash mismatch: {relative}")
        digest = hashlib.sha256(_open_url(_cache_busted(relative))).hexdigest()
        if digest != expected[relative]:
            raise PublishError(f"remote file hash mismatch: {relative}")
    return True


def _next_version(repo_root: Path) -> str:
    today = dt.date.today().strftime("%Y.%m.%d")
    current = ""
    try:
        data = json.loads((Path(repo_root) / "version.json").read_text(encoding="utf-8"))
        current = str(data.get("app_version") or data.get("runner_version") or "")
    except (OSError, ValueError, TypeError):
        pass
    match = re.fullmatch(re.escape(today) + r"\.(\d+)", current)
    return f"{today}.{int(match.group(1)) + 1 if match else 1}"


def publish(
    repo_root: Path,
    version: str | None = None,
    *,
    dry_run: bool = False,
    skip_remote_verify: bool = False,
) -> PublishResult:
    root = Path(repo_root).absolute()
    if not dry_run:
        ensure_clean_index(root)
    release_version = version or _next_version(root)
    manifest = build_manifest(root, release_version)
    selected = runtime_files(root)
    findings = scan_sensitive_files(selected)
    if findings:
        summary = ", ".join(
            f"{_relative_posix(item.path, root)}:{item.rule_name}" for item in findings
        )
        raise PublishError(f"sensitive content found: {summary}")

    _run_tests(root)
    staged_paths = tuple(
        sorted([*(_relative_posix(path, root) for path in selected), "version.json"])
    )
    if dry_run:
        print("STAGE")
        for relative in staged_paths:
            print(relative)
        print("DELETE")
        for relative in LEGACY_REMOVALS:
            print(relative)
        return PublishResult(False, False, release_version, staged_paths, LEGACY_REMOVALS)

    ensure_fast_forward(root)
    _write_manifest_atomic(root, manifest)
    run_git("add", "--", *staged_paths, cwd=root)
    run_git("rm", "--ignore-unmatch", "--", *LEGACY_REMOVALS, cwd=root)
    run_git("commit", "-m", f"release: Linera2 v{release_version}", cwd=root)
    run_git("push", "origin", "HEAD:main", cwd=root)

    verified = False
    if not skip_remote_verify:
        try:
            verified = _verify_remote(root, release_version, manifest)
        except Exception:
            verified = False
    return PublishResult(True, verified, release_version, staged_paths, LEGACY_REMOVALS)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="build, scan, test, and list paths")
    parser.add_argument("--version", help="release version (defaults to today's next sequence)")
    parser.add_argument(
        "--skip-remote-verify",
        action="store_true",
        help="skip post-push Raw verification (for diagnostics only; forbidden for final publish)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        result = publish(
            Path(__file__).resolve().parent,
            args.version,
            dry_run=args.dry_run,
            skip_remote_verify=args.skip_remote_verify,
        )
    except PublishError as exc:
        print(f"publish aborted: {exc}", file=sys.stderr)
        return 1
    if result.pushed and not result.remote_verified and not args.skip_remote_verify:
        print("push completed, but Raw verification failed", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
