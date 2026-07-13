import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linera2.auto_page import AutoPageState  # noqa: E402
from linera2.auto_runtime import run_auto_session  # noqa: E402
from linera2.auto_session import AutoSessionState, AutoSessionStore  # noqa: E402
from linera2.auto_tracking import HistoryCounts  # noqa: E402
from linera2.readiness import ReadinessResult  # noqa: E402


async def no_sleep(_seconds):
    return None


async def end_snapshot(_page):
    return SimpleNamespace(coins=429)


def ready_result(coins=433, ready=True, state="ready"):
    return ReadinessResult(
        account_id="acct",
        ready=ready,
        state=state,
        reason="test",
        wallet_address_masked="0x1234...abcd",
        coins=coins,
        wallet_connected=True,
        backend_ok=ready,
        ride_ui_ready=True,
        checked_at="2026-07-14T01:00:00+00:00",
    )


def active_history(higher=1, lower=1):
    return HistoryCounts(
        higher=higher,
        lower=lower,
        active_higher=int(higher > 0),
        active_lower=int(lower > 0),
    )


class FakePage:
    def __init__(self):
        self.listeners = []

    def on(self, event, callback):
        self.listeners.append((event, callback))

    def remove_listener(self, event, callback):
        if (event, callback) in self.listeners:
            self.listeners.remove((event, callback))


class FakeMonitor:
    def __init__(self, snapshots):
        self.snapshots = list(snapshots)
        self.last = set()

    def on_request(self, _request):
        return None

    def snapshot(self):
        if self.snapshots:
            self.last = set(self.snapshots.pop(0))
        return set(self.last)


class FakeAdapter:
    def __init__(
        self,
        histories,
        *,
        initial_running=False,
        stop_success=True,
        fail_history_after=None,
        on_start=None,
        partial_active=False,
    ):
        self.histories = list(histories)
        self.last_history = HistoryCounts(0, 0)
        self.running = initial_running
        self.stop_success = stop_success
        self.fail_history_after = fail_history_after
        self.history_reads = 0
        self.on_start = on_start
        self.partial_active = partial_active
        self.open_calls = 0
        self.configure_calls = 0
        self.start_calls = 0
        self.stop_calls = 0

    async def read_state(self):
        if self.partial_active and self.running:
            return AutoPageState(
                running=False,
                paused=False,
                stop_visible=True,
                auto_on_visible=True,
            )
        return AutoPageState(self.running, self.running, self.running)

    async def read_history_counts(self):
        self.history_reads += 1
        if self.fail_history_after is not None and self.history_reads > self.fail_history_after:
            raise RuntimeError("history failed")
        if self.histories:
            self.last_history = self.histories.pop(0)
        return self.last_history

    async def open_configuration(self):
        self.open_calls += 1

    async def configure_one_plus_one(self):
        self.configure_calls += 1

    async def start(self):
        self.start_calls += 1
        if self.on_start:
            self.on_start()
        self.running = True

    async def stop_once(self):
        self.stop_calls += 1
        if self.stop_success:
            self.running = False
            return True
        return False


class RecordingStore(AutoSessionStore):
    def __init__(self, path):
        self.transitions = []
        super().__init__(path)

    def update(self, record):
        self.transitions.append(record.state)
        super().update(record)


class StepClock:
    def __init__(self):
        self.value = 0

    def __call__(self):
        self.value += 1
        return self.value


class RunAutoSessionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = RecordingStore(Path(self.temp.name) / "auto_sessions.json")
        self.page = FakePage()

    def tearDown(self):
        self.temp.cleanup()

    async def run_with(self, adapter, monitor, readiness=None, **kwargs):
        return await run_auto_session(
            self.page,
            object(),
            "acct",
            store=self.store,
            readiness=readiness or ready_result(),
            target_override=1,
            poll_interval=0,
            settle_timeout=0.1,
            adapter_factory=lambda _page: adapter,
            monitor_factory=lambda: monitor,
            sleep_func=no_sleep,
            snapshot_reader=end_snapshot,
            **kwargs,
        )

    async def test_not_ready_never_clicks(self):
        adapter = FakeAdapter([HistoryCounts(0, 0)])

        record = await self.run_with(
            adapter,
            FakeMonitor([]),
            readiness=ready_result(ready=False, state="wallet_syncing"),
        )

        self.assertEqual(record.state, AutoSessionState.FAILED.value)
        self.assertEqual(adapter.start_calls, 0)
        self.assertEqual(adapter.stop_calls, 0)

    async def test_less_than_two_coins_never_starts(self):
        adapter = FakeAdapter([HistoryCounts(0, 0)])

        record = await self.run_with(adapter, FakeMonitor([]), readiness=ready_result(1))

        self.assertEqual(record.state, AutoSessionState.FAILED.value)
        self.assertEqual(adapter.open_calls, 0)

    async def test_missing_resolution_baseline_never_starts(self):
        adapter = FakeAdapter([HistoryCounts(0, 0)])

        record = await self.run_with(
            adapter,
            FakeMonitor([set(), set(), set()]),
            clock=StepClock(),
        )

        self.assertEqual(record.state, AutoSessionState.FAILED.value)
        self.assertIn("轮次基线", record.failure_reason)
        self.assertEqual(adapter.start_calls, 0)

    async def test_completed_today_skips_without_clicking(self):
        existing = self.store.get_or_create_daily("acct", target_override=1)
        existing.state = AutoSessionState.COMPLETED.value
        self.store.update(existing)
        adapter = FakeAdapter([HistoryCounts(0, 0)])

        record = await self.run_with(adapter, FakeMonitor([]))

        self.assertEqual(record.state, AutoSessionState.COMPLETED.value)
        self.assertEqual(adapter.start_calls, 0)

    async def test_residual_auto_is_stopped_before_new_start(self):
        adapter = FakeAdapter(
            [HistoryCounts(0, 0), HistoryCounts(0, 0), HistoryCounts(0, 0),
             active_history(), HistoryCounts(1, 1), HistoryCounts(1, 1)],
            initial_running=True,
        )
        monitor = FakeMonitor([{100}, {100}, {100}, {100, 101}])

        record = await self.run_with(adapter, monitor)

        self.assertEqual(record.state, AutoSessionState.COMPLETED.value)
        self.assertEqual(adapter.stop_calls, 2)

    async def test_persists_configuration_and_baseline_before_start(self):
        observed = []

        def on_start():
            current = self.store.get("acct")
            observed.append((current.state, current.baseline_resolution_keys))

        adapter = FakeAdapter(
            [HistoryCounts(2, 2), active_history(3, 3),
             HistoryCounts(3, 3), HistoryCounts(3, 3)],
            on_start=on_start,
        )
        monitor = FakeMonitor([{10, 11}, {10, 11, 12}])

        await self.run_with(adapter, monitor)

        self.assertEqual(observed, [(AutoSessionState.CONFIGURING.value, [10, 11])])

    async def test_correlated_round_stops_and_completes(self):
        adapter = FakeAdapter(
            [HistoryCounts(4, 4), active_history(1, 0), active_history(),
             HistoryCounts(5, 5), HistoryCounts(5, 5)]
        )
        monitor = FakeMonitor([{200}, {200, 201}, {200, 201}])

        record = await self.run_with(adapter, monitor)

        self.assertEqual(record.state, AutoSessionState.COMPLETED.value)
        self.assertEqual(record.completed_rounds, 1)
        self.assertEqual(record.counted_resolution_keys, [201])
        self.assertEqual(record.nominal_stake, 2)
        self.assertEqual(record.start_coins, 433)
        self.assertEqual(record.end_coins, 429)
        self.assertEqual(adapter.stop_calls, 1)
        self.assertIn(AutoSessionState.STOPPING.value, self.store.transitions)
        self.assertIn(AutoSessionState.SETTLING.value, self.store.transitions)

    async def test_timeout_attempts_stop_and_marks_failed(self):
        adapter = FakeAdapter([HistoryCounts(0, 0)])
        monitor = FakeMonitor([{1}])

        record = await self.run_with(
            adapter, monitor, timeout=2, clock=StepClock()
        )

        self.assertEqual(record.state, AutoSessionState.FAILED.value)
        self.assertEqual(adapter.stop_calls, 1)
        self.assertFalse(record.auto_still_running)

    async def test_exception_while_running_attempts_stop(self):
        adapter = FakeAdapter(
            [HistoryCounts(0, 0)], fail_history_after=1
        )

        record = await self.run_with(adapter, FakeMonitor([{1}]))

        self.assertEqual(record.state, AutoSessionState.FAILED.value)
        self.assertEqual(adapter.stop_calls, 1)

    async def test_partial_auto_on_marker_still_triggers_failure_stop(self):
        adapter = FakeAdapter(
            [HistoryCounts(0, 0)],
            fail_history_after=1,
            partial_active=True,
        )

        record = await self.run_with(adapter, FakeMonitor([{1}]))

        self.assertEqual(record.state, AutoSessionState.FAILED.value)
        self.assertEqual(adapter.stop_calls, 1)
        self.assertFalse(record.auto_still_running)

    async def test_stop_failure_never_claims_completion_or_retries(self):
        adapter = FakeAdapter(
            [HistoryCounts(0, 0), active_history()], stop_success=False
        )
        monitor = FakeMonitor([{9}, {9, 10}])

        record = await self.run_with(adapter, monitor)

        self.assertEqual(record.state, AutoSessionState.FAILED.value)
        self.assertTrue(record.auto_still_running)
        self.assertEqual(adapter.stop_calls, 1)

    async def test_existing_live_positions_settle_before_start(self):
        observed = []

        def on_start():
            current = self.store.get("acct")
            observed.append(
                (adapter.history_reads, current.baseline_resolution_keys)
            )

        adapter = FakeAdapter(
            [
                active_history(),
                HistoryCounts(1, 1),
                active_history(),
                HistoryCounts(1, 1),
                HistoryCounts(1, 1),
            ],
            on_start=on_start,
        )
        monitor = FakeMonitor([{10}, {10, 11}, {10, 11, 12}])

        record = await self.run_with(adapter, monitor)

        self.assertEqual(record.state, AutoSessionState.COMPLETED.value)
        self.assertEqual(observed, [(2, [10, 11])])

    async def test_completion_waits_until_active_positions_clear(self):
        adapter = FakeAdapter(
            [
                HistoryCounts(0, 0),
                active_history(),
                active_history(),
                active_history(),
                HistoryCounts(1, 1),
                HistoryCounts(1, 1),
            ]
        )

        async def snapshot_after_clear(_page):
            if adapter.last_history.active_higher or adapter.last_history.active_lower:
                raise AssertionError("snapshot read before active positions cleared")
            return SimpleNamespace(coins=430)

        record = await run_auto_session(
            self.page,
            object(),
            "acct",
            store=self.store,
            readiness=ready_result(),
            target_override=1,
            poll_interval=0,
            settle_timeout=0.1,
            adapter_factory=lambda _page: adapter,
            monitor_factory=lambda: FakeMonitor([{20}, {20, 21}]),
            sleep_func=no_sleep,
            snapshot_reader=snapshot_after_clear,
        )

        self.assertEqual(record.state, AutoSessionState.COMPLETED.value)
        self.assertEqual(record.end_coins, 430)


if __name__ == "__main__":
    unittest.main()
