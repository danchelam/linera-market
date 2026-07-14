# Linera 2.0 Wallet Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reuse the proven parent OKX unlock and confirmation primitives so Linera 2.0 can recover a disconnected wallet once before an explicitly requested Auto session, while keeping normal readiness scans read-only.

**Architecture:** Add a small `linera2.wallet_recovery` adapter for the new Dynamic/Shadow DOM UI and OKX notification popup. Integrate it in `scan_one_account()` only for `run_auto=True` plus an initial `wallet_disconnected` result, then rerun the full readiness check and let Auto consume only that final result.

**Tech Stack:** Python 3.12, Playwright async API, `unittest`, existing parent `base_module` wallet helpers.

## Global Constraints

- Do not copy the wallet password or secret-bearing parent implementation into Linera 2.0.
- Do not refresh or restart the browser, and attempt recovery at most once per scan.
- Do not start Auto or place a bet during direct live wallet verification.
- Never persist or log cookies, authorization headers, response bodies, passwords, or full wallet addresses.
- Do not stage the existing unrelated Auto safety edits while committing wallet-recovery work.

---

## Task 1: Implement the Wallet Recovery Adapter

**Files:**

- Create: `Linera2.0/linera2/wallet_recovery.py`
- Create: `Linera2.0/tests/test_wallet_recovery.py`

- [ ] **Step 1: Write failing result and early-exit tests**

Add tests for the frozen `WalletRecoveryResult`, an already-connected snapshot that performs no unlock/click, and an unlock failure that stops before Connect. Patch a module-level lazy parent-helper loader so tests never import or expose parent secrets.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```powershell
python -m unittest tests.test_wallet_recovery -v
```

Expected: failure because `linera2.wallet_recovery` does not exist.

- [ ] **Step 3: Add the minimal result type, lazy loader, and guarded entry flow**

Implement the frozen result below and the exact public function signature from the
approved design specification:

```python
@dataclass(frozen=True)
class WalletRecoveryResult:
    recovered: bool
    reason: str
```

Load `unlock_okx_wallet`, `_click_wallet_button`, and `OKX_EXTENSION_ID` from the parent `base_module` only inside the loader. Treat `True` and the legacy `"NEED_DAPP"` signal as permission to continue the DApp connection flow; treat false and exceptions as a controlled failure.

- [ ] **Step 4: Write failing Dynamic modal and trusted-click tests**

Use small fake page/context objects to assert this order: popup observation is registered, stale Dynamic modal is closed, visible Connect is clicked, the OKX tile bounds are read from the open Shadow DOM, and `page.mouse.click(center_x, center_y)` is used. Add cases for missing Connect and missing tile.

- [ ] **Step 5: Implement Dynamic modal helpers**

Add private helpers that:

- close only an existing Dynamic login dialog;
- click the visible `Connect` role button;
- poll Shadow DOM for a `data-testid="ListTile"` whose text contains `OKX Wallet`;
- return its live `getBoundingClientRect()` center;
- use the Playwright mouse API for a trusted click, with no fixed coordinates.

- [ ] **Step 6: Write failing popup and final-state tests**

Cover a newly opened OKX `notification.html` page, a popup whose hash changes between routes, delegated parent confirmations, popup timeout, confirmation failure, and the requirement that the frontend snapshot must eventually report `wallet_connected=True`.

- [ ] **Step 7: Implement popup matching and connected-state polling**

Register a `context.on("page", callback)` handler before the trusted click and also
scan `context.pages` so fast or pre-existing notifications are not missed. Match only:

```text
chrome-extension://<configured OKX id>/notification.html
```

Ignore the hash route, call the parent confirmation helper on the current popup page, then poll the existing frontend snapshot reader until it reports a masked connected wallet or the shared deadline expires. Convert expected failures to concise `WalletRecoveryResult(False, reason)` values.

- [ ] **Step 8: Run focused tests and commit Task 1 files**

Run:

```powershell
python -m unittest tests.test_wallet_recovery -v
```

Expected: all focused tests pass.

Commit only:

```powershell
git add -- Linera2.0/linera2/wallet_recovery.py Linera2.0/tests/test_wallet_recovery.py
git commit -m "feat: add Linera wallet recovery adapter"
```

---

## Task 2: Integrate One-Shot Recovery Into Auto Runtime

**Files:**

- Modify: `Linera2.0/linera2/runtime.py`
- Modify: `Linera2.0/tests/test_runtime.py`

- [ ] **Step 1: Write failing runtime orchestration tests**

Add tests that prove:

- a default disconnected scan never calls wallet recovery;
- `run_auto=True` plus `wallet_disconnected` calls recovery exactly once and readiness exactly twice;
- only the second readiness result is stored and passed to Auto;
- a failed recovery or non-ready second result never starts Auto;
- other initial states do not trigger recovery.

- [ ] **Step 2: Run the runtime tests and verify RED**

Run:

```powershell
python -m unittest tests.test_runtime -v
```

Expected: new recovery assertions fail before production integration.

- [ ] **Step 3: Implement the smallest runtime integration**

After the initial readiness check:

```python
if run_auto and result.state == ReadinessState.WALLET_DISCONNECTED.value:
    await ensure_wallet_connected(page, context, account_id, log_func=log_func)
    result = await check_account_ready(page, context, account_id, timeout=timeout)
```

Store and log only the final result. Keep the existing `result.ready` and `auto_session_store` gate around `run_auto_session()`.

- [ ] **Step 4: Run runtime and full automated tests**

Run:

```powershell
python -m unittest tests.test_runtime -v
python -m unittest discover -s tests -v
python -m compileall linera2
```

Expected: all tests pass and compilation succeeds.

- [ ] **Step 5: Commit only runtime integration files**

```powershell
git add -- Linera2.0/linera2/runtime.py Linera2.0/tests/test_runtime.py
git commit -m "feat: recover wallet before Linera auto session"
```

---

## Task 3: Document and Verify Both HubStudio Environments

**Files:**

- Modify: `Linera2.0/README.md`
- Verify: `Linera2.0/readiness_status.json`

- [ ] **Step 1: Document the opt-in boundary**

Explain that normal readiness remains read-only, runtime recovery is enabled only through `--auto-session`, and the standalone live verification calls only the wallet adapter so it cannot configure Auto or place a bet.

- [ ] **Step 2: Run privacy and repository checks**

Run targeted searches over Linera 2.0 source, tests, logs, and JSON status output for obvious secret-bearing fields and full `0x` addresses. Inspect matches instead of printing any secret values.

- [ ] **Step 3: Verify environment 625421671 without Auto**

Close/reopen the HubStudio environment if needed, connect over CDP, call `ensure_wallet_connected()` directly, then run `check_account_ready()`. Confirm the final masked wallet state and coins/backend/UI evidence. Do not call `run_auto_session()`.

- [ ] **Step 4: Verify new environment 625421688 without Auto**

Repeat the direct wallet-only flow for `625421688`. Capture only account id, recovery reason, readiness enum, masked wallet address, coins, backend flag, and UI flag. If the new environment needs a user-owned password/setup step that the reused parent helper cannot complete, stop and report the exact non-sensitive stage.

- [ ] **Step 5: Re-run the complete suite after live testing**

```powershell
python -m unittest discover -s tests -v
python -m compileall linera2
git diff --check
```

Expected: all tests pass, compilation succeeds, and no whitespace errors are reported.

- [ ] **Step 6: Commit documentation only**

```powershell
git add -- Linera2.0/README.md
git commit -m "docs: explain Linera wallet recovery boundary"
```
