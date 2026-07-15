# Linera 2.0 Manifest Updater Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the legacy Linera GitHub payload with a self-contained Linera 2.0 runtime, a backward-compatible manifest updater, and a safe one-command publisher, then publish and verify it on `danchelam/linera-market` `main`.

**Architecture:** Keep the root filename `linera_runner.py` as the compatibility bridge that legacy clients already know how to self-update. The rewritten runner validates a schema-v2 `version.json`, stages and hashes the complete runtime bundle, rolls back partial replacements, migrates the legacy wallet password into an excluded local config, removes only explicitly allowed legacy files, and launches the package. A separate `publish.py` builds the allowlisted manifest, runs tests and secret scanning, creates an exact-path commit, fast-forward pushes `HEAD:main`, and verifies GitHub Raw.

**Tech Stack:** Python 3.12, `unittest`, Playwright async API, Flask, pandas/openpyxl, requests/urllib, Git, GitHub Raw.

## Global Constraints

- Preserve the compatibility keys `runner_version`, `task_version`, and `base_version` in `version.json`; set `schema_version` to integer `2`.
- Never publish or delete `hubshuju.xlsx`, `Linera2.0/local_config.json`, readiness/auto/alert JSON, logs, screenshots, or diagnostic directories.
- Read the wallet password only from `OKX_WALLET_PASSWORD` or `Linera2.0/local_config.json` after migration.
- Reject absolute paths, drive-qualified paths, `..`, duplicate paths, and paths outside the exact update allowlist.
- Download and verify every changed file before replacing any live file; roll back all replaced files if replacement fails.
- Delete legacy files only after config migration and bundle replacement both succeed.
- Publishing must use exact `git add`/`git rm`, require `origin/main` to be an ancestor of `HEAD`, push `HEAD:main`, and never use `--force`.
- Do not expose cookies, authorization headers, full wallet addresses, response bodies, passwords, private keys, or HubStudio credentials in source, manifests, logs, tests, or final output.

---

## File Map

- Create `Linera2.0/linera2/local_config.py`: private config loading and one-time legacy password migration.
- Create `Linera2.0/linera2/account_support.py`: `AccountInfo`, Excel/CSV account loading, and timestamped logging.
- Create `Linera2.0/linera2/wallet_support.py`: the two OKX helper interfaces currently imported from the legacy base module.
- Modify `Linera2.0/linera2/cli.py`: remove all parent `base_module` imports and use the new support modules.
- Modify `Linera2.0/linera2/wallet_recovery.py`: inject the local wallet support implementation.
- Rewrite `linera_runner.py`: self-contained legacy bridge, manifest updater, rollback engine, and Linera 2.0 launcher.
- Rewrite `publish.py`: safe manifest builder, exact-path Git publisher, and Raw verification.
- Modify `version.json`: schema-v2 compatibility manifest.
- Create `Linera2.0/requirements.txt`: explicit runtime dependencies.
- Modify `.gitignore` and `Linera2.0/.gitignore`: private/runtime artifact exclusions.
- Modify root `README.md` or create it if absent; modify `Linera2.0/README.md`.
- Create `Linera2.0/tests/test_local_support.py`, `test_wallet_support.py`, `test_runner_update.py`, and `test_publish.py`.
- Delete tracked legacy runtime files only after replacements pass: `linera_task.py`, `base_module.py`, `test_full_flow.py`.

---

### Task 1: Private config and self-contained account support

**Files:**
- Create: `Linera2.0/linera2/local_config.py`
- Create: `Linera2.0/linera2/account_support.py`
- Modify: `Linera2.0/linera2/cli.py`
- Create: `Linera2.0/tests/test_local_support.py`
- Modify: `Linera2.0/tests/test_cli.py`

**Interfaces:**
- Produces: `load_local_config(project_dir: Path) -> dict[str, str]`
- Produces: `get_wallet_password(project_dir: Path) -> str | None`
- Produces: `migrate_legacy_wallet_password(project_dir: Path, legacy_path: Path) -> bool`
- Produces: `AccountInfo(id: str, ua: str = "", proxy: str = "")`
- Produces: `load_accounts(path: Path) -> list[AccountInfo]`
- Produces: `log(account_id: str, message: str) -> None`

- [ ] **Step 1: Write failing private-config and account-loader tests**

```python
def test_environment_password_wins_over_local_config(self):
    config = self.root / "local_config.json"
    config.write_text('{"wallet_password":"file-secret"}', encoding="utf-8")
    with patch.dict(os.environ, {"OKX_WALLET_PASSWORD": "env-secret"}):
        self.assertEqual(get_wallet_password(self.root), "env-secret")

def test_migration_extracts_only_password_and_never_copies_source(self):
    legacy = self.root / "base_module.py"
    legacy.write_text('OKX_DEFAULT_PASSWORD = "local-secret"\nOTHER = "ignored"', encoding="utf-8")
    self.assertTrue(migrate_legacy_wallet_password(self.root, legacy))
    self.assertEqual(load_local_config(self.root), {"wallet_password": "local-secret"})

def test_load_accounts_supports_environment_id_column(self):
    path = self.root / "accounts.csv"
    path.write_text("\u73af\u5883ID,\u73af\u5883\u540d\u79f0\n625421710,A70\n", encoding="utf-8-sig")
    self.assertEqual([(x.id, x.ua) for x in load_accounts(path)], [("625421710", "A70")])
```

- [ ] **Step 2: Run the new tests and verify RED**

Run: `cd Linera2.0; python -m unittest tests.test_local_support tests.test_cli -v`

Expected: import failures for `linera2.local_config` and `linera2.account_support`.

- [ ] **Step 3: Implement private config with atomic writes and strict parsing**

```python
def get_wallet_password(project_dir: Path) -> str | None:
    value = os.environ.get("OKX_WALLET_PASSWORD", "").strip()
    if value:
        return value
    stored = load_local_config(project_dir).get("wallet_password", "").strip()
    return stored or None

def migrate_legacy_wallet_password(project_dir: Path, legacy_path: Path) -> bool:
    if get_wallet_password(project_dir):
        return True
    if not legacy_path.is_file():
        return False
    match = re.search(r'^OKX_DEFAULT_PASSWORD\s*=\s*["\']([^"\']+)["\']', legacy_path.read_text(encoding="utf-8"), re.M)
    if not match:
        return False
    _atomic_write_json(project_dir / "local_config.json", {"wallet_password": match.group(1)})
    return True
```

`load_local_config` must return `{}` for missing, malformed, non-object, or non-string values. `_atomic_write_json` must use `tempfile.NamedTemporaryFile(delete=False, dir=target.parent)` followed by `os.replace`.

- [ ] **Step 4: Implement account loading and remove `base_module` from CLI**

```python
@dataclass(frozen=True)
class AccountInfo:
    id: str
    ua: str = ""
    proxy: str = ""

def default_account_file() -> Path:
    local = PROJECT_DIR / "hubshuju.xlsx"
    return local if local.exists() else PROJECT_DIR.parent / "hubshuju.xlsx"
```

Use pandas with `dtype=str`, Excel `header=1`, CSV `encoding="utf-8-sig"`, and the exact ID fallback order `环境ID`, `id`, `user_id`, `containerCode`. Replace `_parent_log` calls with `account_support.log`.

- [ ] **Step 5: Run focused and full tests**

Run: `cd Linera2.0; python -m unittest tests.test_local_support tests.test_cli -v`

Expected: all focused tests pass.

Run: `cd Linera2.0; python -m unittest discover -s tests -v`

Expected: all existing and new tests pass.

- [ ] **Step 6: Commit only Task 1 files**

```powershell
git add -- Linera2.0/linera2/local_config.py Linera2.0/linera2/account_support.py Linera2.0/linera2/cli.py Linera2.0/tests/test_local_support.py Linera2.0/tests/test_cli.py
git commit -m "refactor: make Linera2 account support self-contained"
```

---

### Task 2: Self-contained OKX wallet support

**Files:**
- Create: `Linera2.0/linera2/wallet_support.py`
- Modify: `Linera2.0/linera2/wallet_recovery.py`
- Create: `Linera2.0/tests/test_wallet_support.py`
- Modify: `Linera2.0/tests/test_wallet_recovery.py`

**Interfaces:**
- Consumes: `get_wallet_password(project_dir: Path) -> str | None`
- Produces: `OKX_EXTENSION_ID: str`
- Produces: `unlock_okx_wallet(context, account_id, password: str | None = None, extension_id: str = OKX_EXTENSION_ID, log_func=None) -> bool | str`
- Produces: `click_wallet_button(page, account_id, max_rounds: int = 5, log_func=None) -> bool`
- `wallet_recovery._load_wallet_helpers()` returns `SimpleNamespace(unlock=..., confirm=..., extension_id=...)` without changing `ensure_wallet_connected` or `ensure_auto_sign_enabled` signatures.

- [ ] **Step 1: Write failing dependency and behavior tests**

```python
def test_wallet_recovery_does_not_import_parent_base_module(self):
    source = Path(wallet_recovery.__file__).read_text(encoding="utf-8")
    self.assertNotIn("from base_module import", source)

async def test_unlock_refuses_write_mode_without_private_password(self):
    with patch("linera2.wallet_support.get_wallet_password", return_value=None):
        result = await unlock_okx_wallet(FakeContext(), "acct")
    self.assertFalse(result)

async def test_click_wallet_button_uses_semantic_confirm_button(self):
    page = FakeWalletPage(button_text="Confirm")
    self.assertTrue(await click_wallet_button(page, "acct", max_rounds=1))
    self.assertEqual(page.clicks, ["Confirm"])
```

- [ ] **Step 2: Run focused tests and verify RED**

Run: `cd Linera2.0; python -m unittest tests.test_wallet_support tests.test_wallet_recovery -v`

Expected: missing `linera2.wallet_support` and legacy-import assertion failure.

- [ ] **Step 3: Extract the minimum wallet implementation**

Move the behavior used by Linera 2.0 from the legacy module into `wallet_support.py`: password field discovery across frames, React-compatible password fill fallback, semantic unlock click, provider activation, `personal_sign` popup capture, unlock verification, and multi-step semantic confirmation. Do not migrate HubStudio credentials, Clash configuration, task batching, reporting, or legacy market code.

Use this dependency boundary:

```python
def _emit(log_func, account_id: str, message: str) -> None:
    if log_func:
        log_func(account_id, message)

async def unlock_okx_wallet(context, account_id, password=None, extension_id=OKX_EXTENSION_ID, log_func=None):
    password = password or get_wallet_password(PROJECT_DIR)
    if not password:
        _emit(log_func, account_id, "未配置 OKX 钱包密码")
        return False
    return await _unlock_with_provider_popup(
        context,
        account_id,
        password,
        extension_id,
        log_func,
    )
```

Network timeout handling logs a controlled failure and returns `False`; it must not import or invoke the legacy Clash manager.

- [ ] **Step 4: Replace dynamic parent import in wallet recovery**

```python
from .wallet_support import OKX_EXTENSION_ID, click_wallet_button, unlock_okx_wallet

def _load_wallet_helpers() -> SimpleNamespace:
    return SimpleNamespace(
        unlock=unlock_okx_wallet,
        confirm=click_wallet_button,
        extension_id=OKX_EXTENSION_ID,
    )
```

Update calls so the existing `log_func` is passed into the local helper. Preserve test injection by allowing `_load_wallet_helpers` to be patched.

- [ ] **Step 5: Run wallet and full regressions**

Run: `cd Linera2.0; python -m unittest tests.test_wallet_support tests.test_wallet_recovery tests.test_runtime -v`

Expected: all focused tests pass.

Run: `cd Linera2.0; python -m unittest discover -s tests -v`

Expected: all tests pass with no parent `base_module` import.

- [ ] **Step 6: Commit Task 2 files**

```powershell
git add -- Linera2.0/linera2/wallet_support.py Linera2.0/linera2/wallet_recovery.py Linera2.0/tests/test_wallet_support.py Linera2.0/tests/test_wallet_recovery.py
git commit -m "refactor: embed Linera2 wallet support"
```

---

### Task 3: Schema-v2 manifest updater and rollback engine

**Files:**
- Rewrite: `linera_runner.py`
- Create: `Linera2.0/tests/test_runner_update.py`

**Interfaces:**
- Produces: `ManifestError(ValueError)`
- Produces: `ManifestFile(path: str, sha256: str)`
- Produces: `UpdateManifest(schema_version: int, runner_version: str, app_version: str, files: tuple[ManifestFile, ...], remove: tuple[str, ...])`
- Produces: `parse_manifest(payload: str) -> UpdateManifest`
- Produces: `sha256_file(path: Path) -> str`
- Produces: `apply_manifest(manifest: UpdateManifest, install_root: Path, fetch_file: Callable[[str], bytes]) -> UpdateResult`
- Produces: `migrate_private_config(install_root: Path) -> bool`

- [ ] **Step 1: Write failing manifest security and rollback tests**

```python
def test_parse_manifest_rejects_path_traversal(self):
    payload = manifest_json(files=[{"path": "../secret", "sha256": "0" * 64}])
    with self.assertRaises(ManifestError):
        parse_manifest(payload)

def test_hash_mismatch_replaces_nothing(self):
    live = self.root / "Linera2.0" / "linera2" / "runtime.py"
    live.parent.mkdir(parents=True)
    live.write_text("old", encoding="utf-8")
    result = apply_manifest(manifest_for(live, "f" * 64), self.root, lambda _path: b"new")
    self.assertFalse(result.updated)
    self.assertEqual(live.read_text(encoding="utf-8"), "old")

def test_second_replace_failure_rolls_back_first(self):
    with patch("linera_runner.os.replace", side_effect=[None, OSError("locked"), None]):
        result = apply_manifest(two_file_manifest(), self.root, fetch_valid_bytes)
    self.assertFalse(result.updated)
    self.assertEqual(read_live_files(self.root), {"a.py": "old-a", "b.py": "old-b"})

def test_runtime_state_is_never_removed(self):
    state = self.root / "Linera2.0" / "auto_sessions.json"
    state.write_text("{}", encoding="utf-8")
    apply_manifest(valid_manifest(remove=["linera_task.py"]), self.root, fetch_valid_bytes)
    self.assertTrue(state.exists())
```

- [ ] **Step 2: Run updater tests and verify RED**

Run: `cd Linera2.0; python -m unittest tests.test_runner_update -v`

Expected: imports or symbols from `linera_runner.py` are missing.

- [ ] **Step 3: Implement strict manifest parsing**

```python
ALLOWED_PREFIXES = ("Linera2.0/linera2/", "Linera2.0/templates/")
ALLOWED_ROOT_FILES = {"linera_runner.py", "Linera2.0/requirements.txt", "Linera2.0/README.md"}
ALLOWED_REMOVALS = {"linera_task.py", "base_module.py", "test_full_flow.py"}

def validate_relative_path(value: str) -> str:
    candidate = PurePosixPath(value)
    if not value or candidate.is_absolute() or ".." in candidate.parts or re.match(r"^[A-Za-z]:", value):
        raise ManifestError("unsafe manifest path")
    normalized = candidate.as_posix()
    if normalized not in ALLOWED_ROOT_FILES and not normalized.startswith(ALLOWED_PREFIXES):
        raise ManifestError("path outside update allowlist")
    return normalized
```

Require exactly 64 lowercase hexadecimal characters for each SHA-256, reject duplicate normalized paths, require `schema_version == 2`, and restrict `remove` to `ALLOWED_REMOVALS`.

- [ ] **Step 4: Implement stage, verify, replace, rollback, and exact removal**

```python
@dataclass(frozen=True)
class UpdateResult:
    updated: bool
    restart_required: bool
    reason: str

def apply_manifest(manifest, install_root, fetch_file):
    changed = _changed_entries(manifest.files, install_root)
    with tempfile.TemporaryDirectory(dir=install_root) as temp_name:
        staged = _stage_and_verify(changed, Path(temp_name), fetch_file)
        result = _replace_with_rollback(staged, install_root)
    if not result.updated:
        return result
    _remove_exact_legacy_paths(manifest.remove, install_root)
    return replace(
        result,
        restart_required=any(item.path == "linera_runner.py" for item in changed),
    )
```

Tests must verify every numbered transition through observable file state. Do not log downloaded content or local config values.

- [ ] **Step 5: Implement legacy private-config migration and safe startup fallback**

`migrate_private_config` imports no legacy module. It calls the same strict regex semantics as `local_config.migrate_legacy_wallet_password`, writes `Linera2.0/local_config.json` atomically, and returns `False` when neither environment/local config nor a parseable legacy file exists. Read-only readiness remains launchable without a password; write mode reports the missing private config.

- [ ] **Step 6: Run updater tests**

Run: `cd Linera2.0; python -m unittest tests.test_runner_update -v`

Expected: all manifest, hash, rollback, removal, and migration tests pass.

- [ ] **Step 7: Commit Task 3 files**

```powershell
git add -- linera_runner.py Linera2.0/tests/test_runner_update.py
git commit -m "feat: add atomic Linera2 manifest updater"
```

---

### Task 4: Runner update bridge and Linera 2.0 launch

**Files:**
- Modify: `linera_runner.py`
- Modify: `Linera2.0/tests/test_runner_update.py`
- Create: `Linera2.0/requirements.txt`

**Interfaces:**
- Consumes: `parse_manifest`, `apply_manifest`, `UpdateResult`
- Produces: `fetch_remote_manifest(base_url: str) -> UpdateManifest`
- Produces: `run_update(install_root: Path, base_url: str) -> UpdateResult`
- Produces: `launch_linera2(install_root: Path, argv: list[str]) -> int`
- Produces: `main(argv: list[str] | None = None) -> int`

- [ ] **Step 1: Write failing bridge and launch tests**

```python
def test_legacy_compatibility_fields_are_accepted(self):
    manifest = parse_manifest(schema_v2_json(runner_version="2026.07.15.1", task_version="", base_version=""))
    self.assertEqual(manifest.runner_version, "2026.07.15.1")

def test_manifest_unavailable_launches_installed_version(self):
    with patch("linera_runner.fetch_remote_manifest", side_effect=OSError("offline")), \
         patch("linera_runner.launch_linera2", return_value=0) as launch:
        self.assertEqual(main([]), 0)
    launch.assert_called_once()

def test_launch_inserts_package_root_and_calls_cli(self):
    result = launch_linera2(self.root, ["--web", "--workers", "1"])
    self.assertEqual(self.fake_cli.argv, ["--web", "--workers", "1"])
    self.assertEqual(result, 0)
```

- [ ] **Step 2: Run bridge tests and verify RED**

Run: `cd Linera2.0; python -m unittest tests.test_runner_update -v`

Expected: missing fetch/launch/main behavior.

- [ ] **Step 3: Implement fetch, update orchestration, and direct package launch**

Use GitHub Raw primary and jsDelivr fallback with a timestamp query. After update, insert `install_root / "Linera2.0"` at the front of `sys.path`, import `linera2.cli.main`, and call it with forwarded arguments. Do not spawn `sys.executable -m linera2` because a frozen runner may relaunch itself.

```python
def launch_linera2(install_root: Path, argv: list[str]) -> int:
    package_root = install_root / "Linera2.0"
    sys.path.insert(0, str(package_root))
    from linera2.cli import main as cli_main
    return int(cli_main(argv))
```

If `restart_required` is true, restart only after all files and removals succeed. If update fetch fails and an installed package exists, log one concise warning and launch it.

- [ ] **Step 4: Declare runtime dependencies**

`Linera2.0/requirements.txt` must contain bounded minimums without machine-specific paths:

```text
flask>=3.0,<4
openpyxl>=3.1,<4
pandas>=2.2,<3
playwright>=1.50,<2
requests>=2.32,<3
```

- [ ] **Step 5: Run runner and full tests**

Run: `cd Linera2.0; python -m unittest tests.test_runner_update -v`

Run: `cd Linera2.0; python -m unittest discover -s tests -v`

Expected: all tests pass.

- [ ] **Step 6: Commit Task 4 files**

```powershell
git add -- linera_runner.py Linera2.0/tests/test_runner_update.py Linera2.0/requirements.txt
git commit -m "feat: bridge legacy runner to Linera2"
```

---

### Task 5: Safe manifest publisher

**Files:**
- Rewrite: `publish.py`
- Modify: `version.json`
- Create: `Linera2.0/tests/test_publish.py`

**Interfaces:**
- Produces: `runtime_files(repo_root: Path) -> tuple[Path, ...]`
- Produces: `scan_sensitive_files(paths: Iterable[Path]) -> list[Finding]`
- Produces: `build_manifest(repo_root: Path, version: str) -> dict`
- Produces: `ensure_fast_forward(repo_root: Path, remote_ref: str = "origin/main") -> None`
- Produces: `publish(repo_root: Path, version: str | None = None) -> PublishResult`
- Produces: CLI flags `--dry-run`, `--version`, and `--skip-remote-verify` (the last flag is forbidden for the final real publish).

- [ ] **Step 1: Write failing publisher allowlist, secret scan, and Git safety tests**

```python
def test_runtime_files_exclude_state_and_private_config(self):
    create_runtime_tree(self.root)
    names = {p.relative_to(self.root).as_posix() for p in runtime_files(self.root)}
    self.assertIn("Linera2.0/linera2/runtime.py", names)
    self.assertNotIn("Linera2.0/local_config.json", names)
    self.assertNotIn("Linera2.0/auto_sessions.json", names)

def test_manifest_hashes_match_bytes(self):
    manifest = build_manifest(self.root, "2026.07.15.1")
    entry = next(x for x in manifest["files"] if x["path"].endswith("runtime.py"))
    self.assertEqual(entry["sha256"], hashlib.sha256(b"runtime-bytes").hexdigest())

def test_sensitive_scan_blocks_assignment_to_wallet_password(self):
    path = self.root / "Linera2.0" / "linera2" / "bad.py"
    path.write_text('OKX_WALLET_PASSWORD = "embedded-value"', encoding="utf-8")
    self.assertTrue(scan_sensitive_files([path]))

def test_non_fast_forward_aborts_without_push(self):
    with patch("publish.run_git") as git:
        git.side_effect = fake_non_ancestor_results()
        with self.assertRaises(PublishError):
            ensure_fast_forward(self.root)
    self.assertNotIn("push", flatten_git_args(git))
```

- [ ] **Step 2: Run publisher tests and verify RED**

Run: `cd Linera2.0; python -m unittest tests.test_publish -v`

Expected: missing safe publisher interfaces.

- [ ] **Step 3: Implement deterministic allowlist and manifest generation**

Include only:

```python
ROOT_RUNTIME_FILES = {"linera_runner.py", "Linera2.0/README.md", "Linera2.0/requirements.txt"}
PACKAGE_PATTERNS = ("Linera2.0/linera2/*.py", "Linera2.0/templates/*")
LEGACY_REMOVALS = ("linera_task.py", "base_module.py", "test_full_flow.py")
```

Sort POSIX paths, reject symlinks, hash raw bytes, and atomically write UTF-8 `version.json` with legacy blank fields and schema-v2 fields.

- [ ] **Step 4: Implement scoped sensitive scan**

Scan only files selected for publication. Findings must report path and rule name, never the matched secret. Rules cover private-key headers, Authorization/Cookie assignments, full `0x` addresses of 40 hex characters, and non-empty password/token/secret assignments. Permit environment lookups such as `os.environ.get("OKX_WALLET_PASSWORD")` and test fixture literals only in `tests/`, which are not published.

- [ ] **Step 5: Implement non-interactive safe Git publication**

```python
run_git("fetch", "origin", "main")
if run_git("merge-base", "--is-ancestor", "origin/main", "HEAD", check=False).returncode != 0:
    raise PublishError("origin/main is not an ancestor of HEAD")
run_git("add", "--", *publish_paths)
run_git("rm", "--ignore-unmatch", "--", *LEGACY_REMOVALS)
run_git("commit", "-m", f"release: Linera2 v{version}")
run_git("push", "origin", "HEAD:main")
```

Before staging, run `python -m unittest discover -s Linera2.0/tests -v`. `--dry-run` must build, scan, test, and print the exact staged/deleted paths without committing or pushing.

- [ ] **Step 6: Implement Raw verification**

Fetch the remote manifest with cache busting, require matching `app_version`, then verify SHA-256 for `linera_runner.py` and at least one package file. Return `PublishResult(pushed=True, remote_verified=False, ...)` if the push succeeded but CDN/Raw verification fails.

- [ ] **Step 7: Run publisher and full tests**

Run: `cd Linera2.0; python -m unittest tests.test_publish tests.test_runner_update -v`

Run: `cd Linera2.0; python -m unittest discover -s tests -v`

Expected: all tests pass.

- [ ] **Step 8: Commit Task 5 files**

```powershell
git add -- publish.py version.json Linera2.0/tests/test_publish.py
git commit -m "feat: publish Linera2 manifest safely"
```

---

### Task 6: Ignore rules, documentation, and legacy removal readiness

**Files:**
- Modify: `.gitignore`
- Modify: `Linera2.0/.gitignore`
- Create or modify: `README.md`
- Modify: `Linera2.0/README.md`

**Interfaces:**
- Documents: local config creation, `python linera_runner.py`, `python publish.py --dry-run`, and `python publish.py`.

- [ ] **Step 1: Write a failing publication-boundary test**

Add to `Linera2.0/tests/test_publish.py`:

```python
def test_repository_private_artifacts_are_ignored(self):
    root_ignore = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8")
    local_ignore = (REPO_ROOT / "Linera2.0" / ".gitignore").read_text(encoding="utf-8")
    for pattern in ("hubshuju.xlsx", "local_config.json", "*_auto_*.json", "*_readiness_*.json", "*.png", "*.log"):
        self.assertIn(pattern, root_ignore + "\n" + local_ignore)
```

- [ ] **Step 2: Run the test and verify RED**

Run: `cd Linera2.0; python -m unittest tests.test_publish.PublishTests.test_repository_private_artifacts_are_ignored -v`

Expected: one or more required patterns are absent.

- [ ] **Step 3: Update ignore rules without ignoring runtime templates or publisher source**

Remove the legacy root exclusions for `templates/` and `publish.py`. Add exact private/runtime patterns, including `Linera2.0/local_config.json`, `Linera2.0/alerts.json`, `Linera2.0/*_auto_*.json`, `Linera2.0/*_readiness_*.json`, `Linera2.0/**/*.png`, and `Linera2.0/**/*.log`.

- [ ] **Step 4: Update operator documentation**

Document these exact commands:

```powershell
$env:OKX_WALLET_PASSWORD = "your-local-wallet-password"
python .\linera_runner.py --web --auto-session --workers 1
python .\publish.py --dry-run
python .\publish.py
```

Explain that first launch migrates an existing legacy password into `Linera2.0/local_config.json`, update failure launches the last installed bundle, and the publisher never force-pushes.

- [ ] **Step 5: Run documentation boundary and full tests**

Run: `cd Linera2.0; python -m unittest tests.test_publish -v`

Run: `cd Linera2.0; python -m unittest discover -s tests -v`

Expected: all tests pass.

- [ ] **Step 6: Commit Task 6 files**

```powershell
git add -- .gitignore Linera2.0/.gitignore README.md Linera2.0/README.md Linera2.0/tests/test_publish.py
git commit -m "docs: document Linera2 secure updates"
```

---

### Task 7: Local migration rehearsal and final verification

**Files:**
- Modify only if a test exposes a defect in Tasks 1-6.

**Interfaces:**
- Verifies the complete old-runner-to-new-package boundary without contacting live browsers or changing GitHub.

- [ ] **Step 1: Run a temporary HTTP integration rehearsal**

Use `tempfile.TemporaryDirectory` for both a fake legacy install and a fake Raw tree. Seed fake `base_module.py`, `linera_task.py`, `test_full_flow.py`, `hubshuju.xlsx`, `Linera2.0/auto_sessions.json`, and a legacy runner. Serve the generated schema-v2 tree with `python -m http.server` on loopback, run the new updater, and assert:

```python
assert (install / "Linera2.0/linera2/runtime.py").is_file()
assert (install / "Linera2.0/local_config.json").is_file()
assert (install / "hubshuju.xlsx").is_file()
assert (install / "Linera2.0/auto_sessions.json").is_file()
assert not (install / "base_module.py").exists()
assert not (install / "linera_task.py").exists()
```

- [ ] **Step 2: Run dry-run publication**

Run: `python publish.py --dry-run`

Expected: tests pass; output lists only runtime/manifest/documentation paths and the three exact legacy removals; no state JSON, screenshot, log, account file, or private config appears.

- [ ] **Step 3: Run final verification commands**

Run: `cd Linera2.0; python -m unittest discover -s tests -v`

Run: `python -m compileall -q Linera2.0/linera2 linera_runner.py publish.py`

Run: `git diff --check`

Expected: all tests pass, compileall exits 0, and diff check reports no whitespace errors.

- [ ] **Step 4: Inspect the exact release diff**

Run: `git status --short` and `git diff --stat origin/main...HEAD`.

Verify that private untracked run artifacts remain untracked/ignored and are absent from every staged commit. Do not remove user-owned diagnostics merely to clean status.

- [ ] **Step 5: Commit any integration-only test or defect fix**

If Task 7 added the permanent integration test, stage only that test and its direct runner fix:

```powershell
git add -- Linera2.0/tests/test_runner_integration.py linera_runner.py
git commit -m "test: verify Linera2 update migration"
```

If no files changed, skip this commit.

---

### Task 8: Replace legacy GitHub payload and verify the live update source

**Files:**
- Delete from tracked tree: `linera_task.py`, `base_module.py`, `test_full_flow.py`
- Publish all allowlisted files generated by Tasks 1-7.

**Interfaces:**
- Produces a verified schema-v2 release on `https://github.com/danchelam/linera-market` branch `main`.

- [ ] **Step 1: Confirm Git authentication without exposing credentials**

Run: `git fetch origin main` and `git ls-remote --heads origin main`.

Expected: both commands succeed; do not print credential-store contents.

- [ ] **Step 2: Execute the real publisher**

Run: `python publish.py`

Expected: publisher runs tests and secret scan, generates the next version, removes only the three legacy files, creates a release commit, fast-forward pushes `HEAD:main`, and reports `remote_verified=True`.

- [ ] **Step 3: Independently verify the remote manifest and hashes**

Fetch with cache busting:

```powershell
$stamp = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
$manifest = Invoke-RestMethod "https://raw.githubusercontent.com/danchelam/linera-market/refs/heads/main/version.json?t=$stamp"
$manifest.schema_version
$manifest.app_version
```

Expected: schema version is `2`; the app version equals the publisher result. Download `linera_runner.py` and one `Linera2.0/linera2/*.py` file, compute SHA-256 locally, and require equality with the manifest entries.

- [ ] **Step 4: Verify legacy remote paths are gone**

Request the three old Raw paths with cache busting. Expected: HTTP 404 for `linera_task.py`, `base_module.py`, and `test_full_flow.py`.

- [ ] **Step 5: Report the release**

Report the release version, commit ID, repository URL, remote verification status, test count, published file count, deleted legacy paths, and confirmation that private artifacts were excluded. Never report the migrated wallet password or any sensitive match contents.
