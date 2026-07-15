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
from linera2.wallet_recovery import AutoSignResult  # noqa: E402


async def no_sleep(_seconds):
    return None


async def end_snapshot(_page):
    return SimpleNamespace(coins=429)


def settlement_snapshot_reader(locked=427, settled=429):
    calls = 0

    async def reader(_page):
        nonlocal calls
        calls += 1
        return SimpleNamespace(coins=locked if calls == 1 else settled)

    return reader


async def enabled_auto_sign(*_args, **_kwargs):
    return AutoSignResult(True, "already enabled")


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

    def on_response(self, _response):
        return None

    async def drain(self):
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
        on_pause=None,
        partial_active=False,
    ):
        self.histories = list(histories)
        self.last_history = HistoryCounts(0, 0)
        self.running = initial_running
        self.stop_success = stop_success
        self.fail_history_after = fail_history_after
        self.history_reads = 0
        self.on_start = on_start
        self.on_pause = on_pause
        self.partial_active = partial_active
        self.paused = False
        self.open_calls = 0
        self.configure_calls = 0
        self.start_calls = 0
        self.stop_calls = 0
        self.ensure_market_calls = 0
        self.validate_market_calls = 0
        self.pause_calls = 0

    async def ensure_target_market(self):
        self.ensure_market_calls += 1

    async def validate_target_market(self):
        self.validate_market_calls += 1

    async def read_state(self):
        if self.partial_active and self.running:
            return AutoPageState(
                running=False,
                paused=False,
                stop_visible=True,
                auto_on_visible=True,
            )
        if self.paused:
            return AutoPageState(False, True, True, True)
        return AutoPageState(self.running, False, self.running, self.running)

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

    async def start(self, *, before_click=None):
        self.start_calls += 1
        if before_click:
            await before_click()
        if self.on_start:
            self.on_start()
        self.running = True

    async def pause_once(self):
        self.pause_calls += 1
        if self.on_pause:
            self.on_pause()
        self.running = False
        self.paused = True
        return True

    async def stop_once(self):
        self.stop_calls += 1
        if self.stop_success:
            self.running = False
            self.paused = False
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
        auto_sign_ensurer = kwargs.pop(
            "auto_sign_ensurer",
            enabled_auto_sign,
        )
        snapshot_reader = kwargs.pop(
            "snapshot_reader",
            settlement_snapshot_reader(),
        )
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
            snapshot_reader=snapshot_reader,
            auto_sign_ensurer=auto_sign_ensurer,
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
        monitor = FakeMonitor(
            [{10, 11}, {10, 11, 12}, {10, 11, 12, 13}]
        )

        await self.run_with(adapter, monitor)

        self.assertEqual(
            observed,
            [(AutoSessionState.CONFIGURING.value, [10, 11, 12])],
        )

    async def test_auto_sign_failure_never_opens_or_starts_auto(self):
        adapter = FakeAdapter([HistoryCounts(0, 0)])

        async def failed_auto_sign(*_args, **_kwargs):
            return AutoSignResult(False, "wallet rejected")

        record = await self.run_with(
            adapter,
            FakeMonitor([{10}]),
            auto_sign_ensurer=failed_auto_sign,
        )

        self.assertEqual(record.state, AutoSessionState.FAILED.value)
        self.assertIn("Auto-sign", record.failure_reason)
        self.assertEqual(adapter.open_calls, 0)
        self.assertEqual(adapter.start_calls, 0)

    async def test_target_market_is_enforced_before_auto_configuration(self):
        adapter = FakeAdapter(
            [HistoryCounts(0, 0), active_history(), HistoryCounts(1, 1), HistoryCounts(1, 1)]
        )

        await self.run_with(adapter, FakeMonitor([{10}, {10}, {10, 11}]))

        self.assertEqual(adapter.ensure_market_calls, 2)
        self.assertEqual(adapter.validate_market_calls, 1)
        self.assertEqual(adapter.open_calls, 1)

    async def test_final_complete_pair_is_paused_before_round_is_counted(self):
        observed = []

        def on_pause():
            current = self.store.get("acct")
            observed.append(current.completed_rounds)

        adapter = FakeAdapter(
            [HistoryCounts(0, 0), active_history(), HistoryCounts(1, 1), HistoryCounts(1, 1)],
            on_pause=on_pause,
        )

        record = await self.run_with(
            adapter,
            FakeMonitor([{10}, {10}, {10, 11}]),
        )

        self.assertEqual(record.state, AutoSessionState.COMPLETED.value)
        self.assertEqual(observed, [0])
        self.assertEqual(adapter.pause_calls, 1)

    async def test_correlated_round_stops_and_completes(self):
        adapter = FakeAdapter(
            [HistoryCounts(4, 4), active_history(1, 0), active_history(),
             HistoryCounts(5, 5), HistoryCounts(5, 5)]
        )
        monitor = FakeMonitor([{200}, {200}, {200}, {200}, {200, 201}])

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

    async def test_persistent_stop_failure_never_claims_completion(self):
        adapter = FakeAdapter(
            [HistoryCounts(0, 0), active_history()], stop_success=False
        )
        monitor = FakeMonitor([{9}, {9}, {9, 10}])

        record = await self.run_with(adapter, monitor)

        self.assertEqual(record.state, AutoSessionState.FAILED.value)
        self.assertTrue(record.auto_still_running)
        self.assertEqual(adapter.stop_calls, 3)

    async def test_failure_cleanup_retries_stop_and_confirms_inactive(self):
        class RetryStopAdapter(FakeAdapter):
            async def stop_once(self):
                self.stop_calls += 1
                if self.stop_calls == 1:
                    return False
                self.running = False
                self.paused = False
                return True

        adapter = RetryStopAdapter(
            [HistoryCounts(0, 0), active_history(), HistoryCounts(1, 1)]
        )

        record = await self.run_with(
            adapter,
            FakeMonitor([{9}, {9}, {9, 10}]),
        )

        self.assertEqual(record.state, AutoSessionState.FAILED.value)
        self.assertFalse(record.auto_still_running)
        self.assertEqual(adapter.stop_calls, 2)

    async def test_auto_reappearing_after_stop_never_completes(self):
        class ReappearingAdapter(FakeAdapter):
            async def read_state(self):
                if self.stop_calls:
                    return AutoPageState(
                        running=False,
                        paused=True,
                        stop_visible=True,
                        auto_on_visible=True,
                    )
                return AutoPageState(False, False, False)

            async def stop_once(self):
                self.stop_calls += 1
                return True

        adapter = ReappearingAdapter(
            [
                HistoryCounts(0, 0),
                active_history(),
                HistoryCounts(1, 1),
                HistoryCounts(1, 1),
            ]
        )

        record = await self.run_with(
            adapter, FakeMonitor([{40}, {40}, {40, 41}])
        )

        self.assertEqual(record.state, AutoSessionState.FAILED.value)
        self.assertTrue(record.auto_still_running)
        self.assertNotIn(AutoSessionState.COMPLETED.value, self.store.transitions)

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
        monitor = FakeMonitor(
            [{10}, {10, 11}, {10, 11}, {10, 11, 12}]
        )

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
                return SimpleNamespace(coins=427)
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
            monitor_factory=lambda: FakeMonitor([{20}, {20}, {20, 21}]),
            sleep_func=no_sleep,
            snapshot_reader=snapshot_after_clear,
            auto_sign_ensurer=enabled_auto_sign,
        )

        self.assertEqual(record.state, AutoSessionState.COMPLETED.value)
        self.assertEqual(record.end_coins, 430)

    async def test_completion_waits_for_coin_balance_to_stabilize(self):
        adapter = FakeAdapter(
            [
                HistoryCounts(0, 0),
                active_history(),
                HistoryCounts(1, 1),
                HistoryCounts(1, 1),
            ]
        )
        observed = [427, 429, 429]
        reads = []

        async def delayed_balance(_page):
            value = observed.pop(0) if observed else 429
            reads.append(value)
            return SimpleNamespace(coins=value)

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
            monitor_factory=lambda: FakeMonitor([{30}, {30}, {30, 31}]),
            sleep_func=no_sleep,
            snapshot_reader=delayed_balance,
            auto_sign_ensurer=enabled_auto_sign,
        )

        self.assertEqual(record.state, AutoSessionState.COMPLETED.value)
        self.assertEqual(record.end_coins, 429)
        self.assertGreaterEqual(len(reads), 3)

    async def test_completion_rejects_stable_debit_until_payout_arrives(self):
        adapter = FakeAdapter(
            [
                HistoryCounts(0, 0),
                active_history(),
                HistoryCounts(1, 1),
                HistoryCounts(1, 1),
            ]
        )
        observed = [431, 431, 431, 433, 433, 433]

        async def delayed_payout(_page):
            value = observed.pop(0) if observed else 433
            return SimpleNamespace(coins=value)

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
            monitor_factory=lambda: FakeMonitor([{60}, {60}, {60, 61}]),
            sleep_func=no_sleep,
            snapshot_reader=delayed_payout,
            auto_sign_ensurer=enabled_auto_sign,
        )

        self.assertEqual(record.state, AutoSessionState.COMPLETED.value)
        self.assertEqual(record.end_coins, 433)

    async def test_completion_accepts_unchanged_balance_when_debit_was_never_visible(self):
        class ManualClock:
            def __init__(self):
                self.value = 0.0

            def __call__(self):
                return self.value

            async def sleep(self, seconds):
                self.value += seconds

        clock = ManualClock()
        adapter = FakeAdapter(
            [
                HistoryCounts(0, 0),
                active_history(),
                HistoryCounts(1, 1),
                HistoryCounts(1, 1),
            ]
        )

        async def unchanged_balance(_page):
            return SimpleNamespace(coins=134)

        record = await run_auto_session(
            self.page,
            object(),
            "acct",
            store=self.store,
            readiness=ready_result(coins=134),
            target_override=1,
            poll_interval=0,
            settle_timeout=0.1,
            adapter_factory=lambda _page: adapter,
            monitor_factory=lambda: FakeMonitor([{70}, {70}, {70, 71}]),
            sleep_func=clock.sleep,
            snapshot_reader=unchanged_balance,
            auto_sign_ensurer=enabled_auto_sign,
            clock=clock,
        )

        self.assertEqual(record.state, AutoSessionState.COMPLETED.value)
        self.assertEqual(record.end_coins, 134)

    async def test_coin_stability_has_an_independent_deadline(self):
        class ManualClock:
            def __init__(self):
                self.value = 0.0

            def __call__(self):
                return self.value

            async def sleep(self, seconds):
                self.value += seconds

        clock = ManualClock()

        class DeadlineConsumingAdapter(FakeAdapter):
            async def read_history_counts(self):
                result = await super().read_history_counts()
                if self.stop_calls and self.history_reads == 5:
                    clock.value += 0.095
                return result

        adapter = DeadlineConsumingAdapter(
            [
                HistoryCounts(0, 0),
                active_history(),
                HistoryCounts(1, 1),
                HistoryCounts(1, 1),
                HistoryCounts(1, 1),
            ]
        )

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
            monitor_factory=lambda: FakeMonitor([{50}, {50}, {50, 51}]),
            sleep_func=clock.sleep,
            snapshot_reader=settlement_snapshot_reader(),
            auto_sign_ensurer=enabled_auto_sign,
            clock=clock,
        )

        self.assertEqual(record.state, AutoSessionState.COMPLETED.value)
        self.assertEqual(record.end_coins, 429)


if __name__ == "__main__":
    unittest.main()
