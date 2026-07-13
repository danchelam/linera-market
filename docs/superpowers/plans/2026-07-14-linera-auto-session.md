# Linera 2.0 Auto Session Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:test-driven-development` for each task and `superpowers:verification-before-completion` before claiming completion.

**Goal:** Add an opt-in, once-per-UTC-day testnet Auto session that places 1 Coin on HIGHER and 1 Coin on LOWER for a persisted random target of 4–7 complete rounds, then stops and records the result.

**Architecture:** Keep the existing readiness scan as the gate and add four isolated pieces: an atomic session store, a read-only network/History round tracker, a narrow page-control adapter, and an orchestrator with a fail-safe Stop path. The CLI enables writes only with `--auto-session`; the existing default command remains read-only. Round counting uses numeric Linera worker GraphQL resolution keys, correlated with newly added HIGHER/LOWER History rows, because the DOM rows contain no stable round ID.

**Tech Stack:** Python 3.12, Playwright async API over HubStudio CDP, Flask, `unittest`, JSON atomic replacement.

**Global Constraints:** Target site is testnet `https://app.linera.xyz/originals/ride?market=BTC&duration=1`; never log cookies, authorization headers, complete wallet addresses, GraphQL bodies, or response bodies; never restart or close a HubStudio window after CDP failure; ordinary readiness scans must never click; every exit from an active Auto session must attempt exactly one Stop and explicitly report whether Auto is still visible.

---

## Task 1: Persisted Auto session model and store

**Files:**

- Create: `Linera2.0/linera2/auto_session.py`
- Create: `Linera2.0/tests/test_auto_session.py`
- Modify: `Linera2.0/.gitignore`

### Step 1: Write failing model and store tests

Add tests that construct a session for account `625421671` and verify:

```python
class AutoSessionStoreTests(unittest.TestCase):
    def test_new_daily_session_randomizes_target_once_between_four_and_seven(self): ...
    def test_same_utc_day_reuses_persisted_target(self): ...
    def test_completed_same_day_is_not_runnable(self): ...
    def test_new_utc_day_creates_a_fresh_session(self): ...
    def test_update_is_atomic_and_keeps_other_accounts(self): ...
    def test_serialized_record_contains_no_sensitive_fields(self): ...
```

The tests must pass a seeded `random.Random` and a fixed UTC `datetime`, so they never depend on wall-clock time.

Run:

```powershell
python -m unittest Linera2.0.tests.test_auto_session -v
```

Expected: `ImportError` or missing type failures.

### Step 2: Implement the state model

Create these public interfaces in `auto_session.py`:

```python
class AutoSessionState(str, Enum):
    WAITING = "waiting"
    CONFIGURING = "configuring"
    RUNNING = "running"
    STOPPING = "stopping"
    SETTLING = "settling"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class AutoSessionRecord:
    account_id: str
    utc_date: str
    state: str
    target_rounds: int
    completed_rounds: int = 0
    start_coins: int | None = None
    current_coins: int | None = None
    end_coins: int | None = None
    nominal_stake: int = 0
    baseline_resolution_keys: list[int] = field(default_factory=list)
    counted_resolution_keys: list[int] = field(default_factory=list)
    baseline_higher_rows: int = 0
    baseline_lower_rows: int = 0
    started_at: str | None = None
    ended_at: str | None = None
    failure_reason: str | None = None
    auto_still_running: bool = False

    @property
    def net_change(self) -> int | None: ...
    def to_dict(self) -> dict: ...
    @classmethod
    def from_dict(cls, payload: dict) -> "AutoSessionRecord": ...
```

Do not include wallet addresses, request metadata, or response data in this model.

### Step 3: Implement the atomic daily store

```python
class AutoSessionStore:
    def __init__(self, path: str | Path) -> None: ...
    def get(self, account_id: str) -> AutoSessionRecord | None: ...
    def get_or_create_daily(
        self,
        account_id: str,
        *,
        now: datetime | None = None,
        rng: random.Random | None = None,
        target_override: int | None = None,
    ) -> AutoSessionRecord: ...
    def update(self, record: AutoSessionRecord) -> None: ...
    def as_dict(self) -> dict[str, dict]: ...
```

Use UTC dates only. Validate ordinary targets as 4–7; permit `target_override=1` only for explicit integration tests. Use the same locked `mkstemp` plus `os.replace` pattern as `ReadinessStore`.

Add `auto_sessions.json` to `Linera2.0/.gitignore`.

### Step 4: Run tests and commit

Run:

```powershell
python -m unittest Linera2.0.tests.test_auto_session -v
```

Expected: all Task 1 tests pass.

Commit:

```powershell
git add Linera2.0/linera2/auto_session.py Linera2.0/tests/test_auto_session.py Linera2.0/.gitignore
git commit -m "feat: persist daily Linera auto sessions"
```

---

## Task 2: Stable worker-key and History round tracking

**Files:**

- Create: `Linera2.0/linera2/auto_tracking.py`
- Create: `Linera2.0/tests/test_auto_tracking.py`

### Step 1: Write failing parser tests

Cover the observed worker request format without saving the body:

```python
class ResolutionKeyParserTests(unittest.TestCase):
    def test_extracts_numeric_entry_keys_from_resolutions_query(self):
        query = "resolutions { g21090: entry(key: 21090) g21091: entry(key: 21091) }"
        self.assertEqual(extract_resolution_keys(query), {21090, 21091})

    def test_ignores_unrelated_graphql_entry_keys(self): ...
    def test_malformed_or_missing_payload_returns_empty_set(self): ...
```

The parser must first locate the `resolutions { ... }` selection and only then extract `entry(key: N)` values. It must accept GraphQL payloads shaped as either one dict or a batch list.

### Step 2: Write failing round-correlation tests

Define the tracker contract through tests:

```python
class RoundTrackerTests(unittest.TestCase):
    def test_baseline_keys_and_rows_never_count(self): ...
    def test_new_key_with_only_higher_row_does_not_count(self): ...
    def test_new_key_with_higher_and_lower_row_counts_once(self): ...
    def test_duplicate_requests_do_not_double_count(self): ...
    def test_two_new_keys_require_two_new_pairs(self): ...
    def test_out_of_order_keys_are_counted_in_numeric_order(self): ...
```

Run:

```powershell
python -m unittest Linera2.0.tests.test_auto_tracking -v
```

Expected: missing module failures.

### Step 3: Implement the request monitor

Create:

```python
WORKER_APPLICATION_MARKER = "worker.infra.linera.net/chains/"

def extract_resolution_keys(payload: str | dict | list | None) -> set[int]: ...

class ResolutionKeyMonitor:
    def __init__(self) -> None:
        self.keys: set[int] = set()
    def on_request(self, request: Request) -> None: ...
    def snapshot(self) -> set[int]: ...
```

`on_request` must require both the worker URL marker and `/applications/`, read only `request.post_data`, pass it to the parser, and discard the raw payload immediately. It must never log URL query strings, payloads, headers, or responses.

### Step 4: Implement deterministic round correlation

```python
@dataclass(frozen=True)
class HistoryCounts:
    higher: int
    lower: int
    active_higher: int = 0
    active_lower: int = 0

class RoundTracker:
    def __init__(
        self,
        baseline_keys: set[int],
        baseline_history: HistoryCounts,
        already_counted: set[int] | None = None,
    ) -> None: ...

    def observe(
        self,
        keys: set[int],
        history: HistoryCounts,
    ) -> list[int]: ...
```

Candidate keys are keys greater than the maximum baseline key and not already observed. The tracker records whether both HIGHER and LOWER are currently `Live/Open`; a newly observed resolution key can count only when that active-pair evidence exists. Repeated keys never count twice. If several unseen keys arrive together, count only the newest because skipped intervals cannot be reconstructed safely from the two-row current-position History view.

This deliberately does not infer a round from request count or a single History row.

### Step 5: Run tests and commit

Run:

```powershell
python -m unittest Linera2.0.tests.test_auto_tracking -v
```

Expected: all Task 2 tests pass.

Commit:

```powershell
git add Linera2.0/linera2/auto_tracking.py Linera2.0/tests/test_auto_tracking.py
git commit -m "feat: track Linera rounds by worker resolution key"
```

---

## Task 3: Narrow Playwright adapter for the built-in Auto controls

**Files:**

- Create: `Linera2.0/linera2/auto_page.py`
- Create: `Linera2.0/tests/test_auto_page.py`

### Step 1: Write failing page-adapter tests with fakes

Cover the observed controls and rows:

```python
class AutoPageTests(unittest.IsolatedAsyncioTestCase):
    async def test_read_state_requires_auto_on_pause_and_stop(self): ...
    async def test_configure_fills_exactly_one_and_one(self): ...
    async def test_start_uses_start_auto_button_and_waits_for_running_markers(self): ...
    async def test_stop_clicks_once_and_waits_until_auto_on_disappears(self): ...
    async def test_stop_reports_false_when_auto_remains_visible(self): ...
    async def test_history_counts_only_btc_one_minute_one_coin_rows(self): ...
```

Assert that no adapter method calls page reload, navigation, wallet connection, browser close, or context close.

### Step 2: Implement selectors and state reads

Create:

```python
@dataclass(frozen=True)
class AutoPageState:
    running: bool
    paused: bool
    stop_visible: bool

class AutoPage:
    def __init__(self, page: Page) -> None: ...
    async def read_state(self) -> AutoPageState: ...
    async def read_history_counts(self) -> HistoryCounts: ...
    async def open_configuration(self) -> None: ...
    async def configure_one_plus_one(self) -> None: ...
    async def start(self) -> None: ...
    async def stop_once(self) -> bool: ...
```

Use the observed selectors:

```python
AUTO_CARD_TEXT = re.compile(r"Auto\s+bet every round", re.I)
HIGHER_INPUT = 'input[aria-label="Higher coins"]'
LOWER_INPUT = 'input[aria-label="Lower coins"]'
START_TEXT = re.compile(r"Start Auto")
HISTORY_ROW = "tr.border-t.border-white\\/5"
```

For inputs, call `fill("1")` and verify `input_value() == "1"`. For History, normalize whitespace and count only rows containing BTC, `1m`, `1 coins`, and exactly one of HIGHER/LOWER. Also expose separate `active_higher`/`active_lower` counts for Live/Open rows; settled rows remain in total counts but do not provide active-pair evidence.

### Step 3: Add bounded waits

- Configuration open: 10 seconds.
- Running markers after Start: 15 seconds.
- `AUTO ON` disappearance after Stop: 15 seconds.
- `stop_once` clicks at most once per call and returns `False` if the running marker remains.

Raise typed `AutoPageError` messages that contain no page HTML or network content.

### Step 4: Run tests and commit

Run:

```powershell
python -m unittest Linera2.0.tests.test_auto_page -v
```

Expected: all Task 3 tests pass.

Commit:

```powershell
git add Linera2.0/linera2/auto_page.py Linera2.0/tests/test_auto_page.py
git commit -m "feat: control Linera built-in auto test mode"
```

---

## Task 4: Fail-safe daily session orchestration

**Files:**

- Create: `Linera2.0/linera2/auto_runtime.py`
- Create: `Linera2.0/tests/test_auto_runtime.py`
- Modify: `Linera2.0/linera2/runtime.py`

### Step 1: Write failing orchestration tests

Use fake readiness checks, page adapters, monitors, clocks, and stores. Required cases:

```python
class RunAutoSessionTests(unittest.IsolatedAsyncioTestCase):
    async def test_not_ready_never_clicks(self): ...
    async def test_less_than_two_coins_never_starts(self): ...
    async def test_completed_today_skips_without_clicking(self): ...
    async def test_residual_auto_is_stopped_before_baseline(self): ...
    async def test_target_rounds_are_persisted_before_start(self): ...
    async def test_counts_only_correlated_complete_rounds(self): ...
    async def test_reaching_target_stops_once_then_settles(self): ...
    async def test_timeout_attempts_stop_and_marks_failed(self): ...
    async def test_exception_attempts_stop_and_records_if_auto_is_still_running(self): ...
    async def test_stop_failure_never_claims_completion(self): ...
```

Run:

```powershell
python -m unittest Linera2.0.tests.test_auto_runtime -v
```

Expected: missing function failures.

### Step 2: Implement the public orchestrator

```python
async def run_auto_session(
    page: Page,
    context: BrowserContext,
    account_id: str,
    *,
    store: AutoSessionStore,
    readiness: ReadinessResult | None = None,
    timeout: int = 1_200,
    settle_timeout: int = 180,
    poll_interval: float = 2.0,
    target_override: int | None = None,
    log_func: LogFunction = _default_log,
) -> AutoSessionRecord: ...
```

Implement this exact transition order:

1. Reuse a passed readiness result or call `check_account_ready`.
2. Return a failed record without clicking unless readiness is `ready` and Coins ≥ 2.
3. Load/create the UTC daily record; return immediately if it is already `completed` today.
4. Attach `ResolutionKeyMonitor` before taking the baseline and allow one 2-second observation window.
5. If Auto is already running, call `stop_once`. Whether Auto was running or not, wait for all existing Live/Open History positions to clear, then reset the baseline. A failed residual stop or settlement timeout ends the run as `failed`.
6. Persist `configuring`, baseline keys, baseline row counts, start Coins, and started time before the first configuration click.
7. Configure 1+1, click Start, verify running markers, then persist `running`.
8. Poll keys and History. Record active HIGHER/LOWER evidence per resolution interval; persist each newly correlated key, increment `completed_rounds`, and set `nominal_stake = completed_rounds * 2`.
9. At target, persist `stopping`, call `stop_once`, then persist `settling`.
10. For at most 180 seconds, require Auto off, all Live/Open positions cleared, and History counts stable for two polls. Re-run the existing frontend snapshot reader to get end Coins, then mark `completed`.
11. On timeout or exception, detect whether Auto is running and call `stop_once` at most once from the failure handler. Persist `failed`, a concise reason, and `auto_still_running` from the post-stop state.

Do not mark completion based on Coins decreasing. Do not retry Start or Stop silently.

### Step 3: Extend account runtime without changing default behavior

Add optional parameters to `scan_one_account` and `scan_accounts`:

```python
auto_session_store: AutoSessionStore | None = None
run_auto: bool = False
auto_timeout: int = 1_200
target_override: int | None = None
```

After readiness is stored, call `run_auto_session` only when `run_auto is True`, `auto_session_store` is provided, and readiness is ready. Keep the same connected page/context and never close the HubStudio browser.

### Step 4: Run tests and commit

Run:

```powershell
python -m unittest Linera2.0.tests.test_auto_runtime Linera2.0.tests.test_runtime -v
```

Expected: orchestration and existing runtime tests pass.

Commit:

```powershell
git add Linera2.0/linera2/auto_runtime.py Linera2.0/linera2/runtime.py Linera2.0/tests/test_auto_runtime.py Linera2.0/tests/test_runtime.py
git commit -m "feat: orchestrate fail-safe daily auto sessions"
```

---

## Task 5: Opt-in CLI and combined Web status

**Files:**

- Modify: `Linera2.0/linera2/cli.py`
- Modify: `Linera2.0/linera2/store.py`
- Modify: `Linera2.0/linera2/webapp.py`
- Modify: `Linera2.0/templates/index.html`
- Modify: `Linera2.0/README.md`
- Modify: `Linera2.0/tests/test_cli.py`
- Modify: `Linera2.0/tests/test_webapp.py`

### Step 1: Write failing CLI safety tests

Add parser tests:

```python
def test_default_command_keeps_auto_session_disabled(self): ...
def test_auto_session_flag_enables_write_mode(self): ...
def test_target_override_is_rejected_without_integration_flag(self): ...
```

Public arguments:

```text
--auto-session        Run the daily testnet Auto session after readiness passes
--auto-timeout 1200   Hard session timeout in seconds
--integration-target  Internal/manual validation override; choices: 1
```

`--integration-target` must also require `--auto-session`; reject any value except `1`.

### Step 2: Write failing combined API tests

Change `create_app` to accept both stores:

```python
def create_app(
    store: ReadinessStore,
    auto_store: AutoSessionStore | None = None,
) -> Flask: ...
```

For each readiness row, `/api/readiness` must merge only these session fields:

```text
session_state, target_rounds, completed_rounds, start_coins,
current_coins, end_coins, nominal_stake, net_change,
auto_still_running, session_failure_reason
```

Accounts without sessions receive `null` session values. The readiness store itself remains unchanged and `readiness_status.json` never contains Auto session data.

### Step 3: Wire CLI and Web

Add:

```python
AUTO_STATUS_FILE = PROJECT_DIR / "auto_sessions.json"
```

Construct `AutoSessionStore` once, pass it through the scan runtime only when `args.auto_session` is true, and always pass it to the Web app so previous session status remains visible.

Update the table with compact columns for session state, rounds, start/current/end Coins, nominal Stake, net change, and Auto safety. Replace the old “只读检测” hint dynamically:

- without `--auto-session`: `只读检测模式`
- with `--auto-session`: `测试网 Auto 会话模式：HIGHER 1 + LOWER 1，每日 4–7 轮`

The API must continue HTML-escaping all rendered content.

### Step 4: Update operator documentation

Document these exact commands in `Linera2.0/README.md`:

```powershell
# Existing read-only scan
python -m linera2

# Daily Auto session, one worker recommended for first rollout
python -m linera2 --auto-session --workers 1 --web

# Explicit one-round integration validation only
python -m linera2 --auto-session --integration-target 1 --workers 1
```

State clearly that `--auto-session` clicks testnet trade controls, that a completed UTC day is skipped, and that `auto_still_running=true` requires manual inspection.

### Step 5: Run tests and commit

Run:

```powershell
python -m unittest discover -s Linera2.0/tests -v
```

Expected: all old and new tests pass.

Commit:

```powershell
git add Linera2.0/linera2/cli.py Linera2.0/linera2/store.py Linera2.0/linera2/webapp.py Linera2.0/templates/index.html Linera2.0/README.md Linera2.0/tests/test_cli.py Linera2.0/tests/test_webapp.py
git commit -m "feat: expose opt-in Linera auto session status"
```

---

## Task 6: Verification and one-round read/write integration

**Files:**

- Modify only if verification reveals a defect in files from Tasks 1–5.

### Step 1: Run the complete automated suite

```powershell
python -m unittest discover -s Linera2.0/tests -v
python -m compileall -q Linera2.0/linera2 Linera2.0/tests
```

Expected: zero failures and zero syntax errors.

### Step 2: Verify default mode is read-only

With a fake or non-production account fixture, verify parsing defaults:

```powershell
python -m linera2 --help
```

Confirm `--auto-session` defaults false and no Auto store transition occurs in the existing CLI tests.

### Step 3: Perform the authorized one-round integration on `625421671`

Before this step, obtain explicit user confirmation because it clicks Start/Stop and consumes test coins. Then run only account `625421671` with integration target 1.

Acceptance evidence:

- readiness is `ready` and starting Coins are recorded;
- both inputs are verified as `1`;
- running state requires `AUTO ON`, `Pause`, and `Stop`;
- one new worker resolution key plus one new HIGHER/LOWER pair produces exactly one counted round;
- Stop is clicked once;
- final state is `completed`, `completed_rounds=1`, `nominal_stake=2`;
- final `AUTO ON` is absent and `auto_still_running=false`;
- `auto_sessions.json` contains no wallet address or network payload.

If Auto remains on, do not close the browser and do not claim success; report the account for manual inspection.

### Step 4: Inspect persisted privacy boundaries

```powershell
rg -n -i "cookie|authorization|bearer|0x[a-f0-9]{20,}|graphql|post_data" Linera2.0/readiness_status.json Linera2.0/auto_sessions.json
```

Expected: no sensitive match. The word `graphql` may appear only in a concise failure reason if implementation deliberately uses it; remove it if it reveals request content.

### Step 5: Review the final diff and commit any verification fix

```powershell
git status --short
git diff --check
git diff -- Linera2.0 docs/superpowers
```

Preserve unrelated user-owned untracked files. If verification required changes, commit only the files changed for this feature:

```powershell
git add Linera2.0
git commit -m "fix: harden Linera auto session verification"
```

Do not stage unrelated workspace files.
