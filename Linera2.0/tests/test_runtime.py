import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linera2.hubstudio import HubstudioReadOnlyClient  # noqa: E402
from linera2.readiness import ReadinessResult, ReadinessState  # noqa: E402
from linera2.runtime import browser_unreachable_result, scan_one_account  # noqa: E402
from linera2.store import ReadinessStore  # noqa: E402
from linera2.wallet_recovery import WalletRecoveryResult  # noqa: E402


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, *payloads):
        self.payloads = list(payloads)
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse(self.payloads.pop(0))


class HubstudioClientTests(unittest.TestCase):
    def test_start_browser_adds_cdp_origin_argument(self):
        session = FakeSession({"code": 0, "data": {"debuggingPort": "12420"}})
        client = HubstudioReadOnlyClient(session=session)

        address = client.start_browser("625421671")

        self.assertEqual(address, "127.0.0.1:12420")
        self.assertEqual(
            session.calls[0][1]["json"],
            {
                "containerCode": "625421671",
                "args": ["--remote-allow-origins=*"],
            },
        )

    def test_start_failure_does_not_call_stop_or_retry(self):
        session = FakeSession({"code": -10013, "msg": "already running", "data": {}})
        client = HubstudioReadOnlyClient(session=session)

        address = client.start_browser("acct")

        self.assertIsNone(address)
        self.assertEqual(len(session.calls), 1)

    @patch("time.sleep")
    def test_restart_stops_once_then_starts_with_cdp_argument(self, sleep):
        session = FakeSession(
            {"code": 0, "data": {}},
            {"code": 0, "data": {"debuggingPort": "32517"}},
        )
        client = HubstudioReadOnlyClient(session=session)

        self.assertTrue(hasattr(client, "restart_browser_once"))
        address = client.restart_browser_once("acct")

        self.assertEqual(address, "127.0.0.1:32517")
        self.assertEqual(len(session.calls), 2)
        self.assertTrue(session.calls[0][0].endswith("/api/v1/browser/stop"))
        self.assertEqual(
            session.calls[0][1]["json"],
            {"containerCode": "acct"},
        )
        self.assertTrue(session.calls[1][0].endswith("/api/v1/browser/start"))
        self.assertEqual(
            session.calls[1][1]["json"],
            {
                "containerCode": "acct",
                "args": ["--remote-allow-origins=*"],
            },
        )
        sleep.assert_called_once_with(3)

    @patch("time.sleep")
    def test_restart_stop_failure_does_not_start(self, sleep):
        session = FakeSession({"code": -1, "msg": "stop failed", "data": {}})
        client = HubstudioReadOnlyClient(session=session)

        self.assertTrue(hasattr(client, "restart_browser_once"))
        address = client.restart_browser_once("acct")

        self.assertIsNone(address)
        self.assertEqual(len(session.calls), 1)
        self.assertIn("stop", client.last_error)
        sleep.assert_not_called()

    @patch("time.sleep")
    def test_restart_start_failure_is_not_retried(self, sleep):
        session = FakeSession(
            {"code": 0, "data": {}},
            {"code": -2, "msg": "start failed", "data": {}},
        )
        client = HubstudioReadOnlyClient(session=session)

        self.assertTrue(hasattr(client, "restart_browser_once"))
        address = client.restart_browser_once("acct")

        self.assertIsNone(address)
        self.assertEqual(len(session.calls), 2)
        self.assertIn("start", client.last_error)
        sleep.assert_called_once_with(3)


class StoreTests(unittest.TestCase):
    def test_result_is_saved_under_account_without_full_wallet_address(self):
        result = ReadinessResult(
            account_id="acct",
            ready=True,
            state="ready",
            reason="ok",
            wallet_address_masked="0x091e...85e3",
            coins=433,
            wallet_connected=True,
            backend_ok=True,
            ride_ui_ready=True,
            checked_at="2026-07-14T01:00:00+08:00",
        )
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "readiness_status.json"
            store = ReadinessStore(path)

            store.update(result)

            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["accounts"]["acct"]["state"], "ready")
            self.assertEqual(payload["accounts"]["acct"]["wallet_address_masked"], "0x091e...85e3")
            self.assertNotIn("wallet_address", payload["accounts"]["acct"])


class RuntimeResultTests(unittest.TestCase):
    def test_browser_connection_failure_has_fixed_state(self):
        result = browser_unreachable_result("acct", "CDP timeout")

        self.assertFalse(result.ready)
        self.assertEqual(result.state, ReadinessState.BROWSER_UNREACHABLE.value)
        self.assertIn("CDP timeout", result.reason)


class FakeHub:
    def __init__(self, restart_address="127.0.0.1:2398"):
        self.last_error = None
        self.restart_address = restart_address
        self.restart_calls = []

    def start_browser(self, _account_id):
        return "127.0.0.1:2397"

    def restart_browser_once(self, account_id):
        self.restart_calls.append(account_id)
        return self.restart_address


class FakeChromium:
    def __init__(self, *browsers):
        self.browsers = list(browsers)
        self.calls = []

    async def connect_over_cdp(self, address, **_kwargs):
        self.calls.append(address)
        if len(self.browsers) > 1:
            return self.browsers.pop(0)
        return self.browsers[0]


class FakeResultStore:
    def __init__(self):
        self.results = []

    def update(self, result):
        self.results.append(result)


class AutoRuntimeIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.page = SimpleNamespace(url="https://app.linera.xyz/originals/ride")
        context = SimpleNamespace(pages=[self.page])
        browser = SimpleNamespace(contexts=[context])
        self.pw = SimpleNamespace(chromium=FakeChromium(browser))
        self.store = FakeResultStore()
        self.result = ReadinessResult(
            account_id="acct",
            ready=True,
            state="ready",
            reason="ok",
            wallet_address_masked="0x1234...abcd",
            coins=433,
            wallet_connected=True,
            backend_ok=True,
            ride_ui_ready=True,
            checked_at="2026-07-14T01:00:00+00:00",
        )

    def disconnected_result(self):
        return ReadinessResult(
            account_id="acct",
            ready=False,
            state=ReadinessState.WALLET_DISCONNECTED.value,
            reason="not connected",
            wallet_address_masked=None,
            coins=None,
            wallet_connected=False,
            backend_ok=True,
            ride_ui_ready=True,
            checked_at="2026-07-14T01:00:00+00:00",
        )

    async def test_default_scan_does_not_run_auto_session(self):
        auto = AsyncMock()
        with patch("linera2.runtime.check_account_ready", AsyncMock(return_value=self.result)), \
             patch("linera2.runtime.run_auto_session", auto):
            await scan_one_account(
                self.pw,
                SimpleNamespace(id="acct", ua="acct"),
                hub=FakeHub(),
                store=self.store,
                auto_session_store=object(),
            )

        auto.assert_not_awaited()

    async def test_default_disconnected_scan_never_recovers_wallet(self):
        recovery = AsyncMock()
        disconnected = self.disconnected_result()
        hub = FakeHub()
        with patch(
            "linera2.runtime.check_account_ready",
            AsyncMock(return_value=disconnected),
        ), patch("linera2.runtime.ensure_wallet_connected", recovery):
            result = await scan_one_account(
                self.pw,
                SimpleNamespace(id="acct", ua="acct"),
                hub=hub,
                store=self.store,
            )

        self.assertIs(result, disconnected)
        recovery.assert_not_awaited()
        self.assertEqual(hub.restart_calls, [])
        self.assertEqual(self.store.results, [disconnected])

    async def test_explicit_auto_flag_reuses_ready_page_and_context(self):
        auto_store = object()
        auto = AsyncMock()
        with patch("linera2.runtime.check_account_ready", AsyncMock(return_value=self.result)), \
             patch("linera2.runtime.run_auto_session", auto):
            await scan_one_account(
                self.pw,
                SimpleNamespace(id="acct", ua="acct"),
                hub=FakeHub(),
                store=self.store,
                auto_session_store=auto_store,
                run_auto=True,
                auto_timeout=321,
                target_override=1,
            )

        auto.assert_awaited_once()
        call = auto.await_args
        self.assertIs(call.args[0], self.page)
        self.assertEqual(call.kwargs["readiness"], self.result)
        self.assertEqual(call.kwargs["store"], auto_store)
        self.assertEqual(call.kwargs["timeout"], 321)
        self.assertEqual(call.kwargs["target_override"], 1)

    async def test_auto_disconnected_recovers_once_and_uses_second_readiness(self):
        disconnected = self.disconnected_result()
        readiness = AsyncMock(side_effect=[disconnected, self.result])
        recovery = AsyncMock(
            return_value=WalletRecoveryResult(True, "wallet connected")
        )
        auto = AsyncMock()
        auto_store = object()

        with patch("linera2.runtime.check_account_ready", readiness), \
             patch("linera2.runtime.ensure_wallet_connected", recovery), \
             patch("linera2.runtime.run_auto_session", auto):
            result = await scan_one_account(
                self.pw,
                SimpleNamespace(id="acct", ua="acct"),
                hub=FakeHub(),
                store=self.store,
                auto_session_store=auto_store,
                run_auto=True,
            )

        self.assertIs(result, self.result)
        self.assertEqual(readiness.await_count, 2)
        recovery.assert_awaited_once()
        self.assertIs(recovery.await_args.args[0], self.page)
        self.assertEqual(self.store.results, [self.result])
        auto.assert_awaited_once()
        self.assertIs(auto.await_args.kwargs["readiness"], self.result)

    async def test_failed_recovery_reruns_readiness_but_never_starts_auto(self):
        first = self.disconnected_result()
        second = self.disconnected_result()
        second.reason = "still disconnected"
        readiness = AsyncMock(side_effect=[first, second])
        recovery = AsyncMock(
            return_value=WalletRecoveryResult(False, "wallet confirm failed")
        )
        auto = AsyncMock()
        hub = FakeHub()

        with patch("linera2.runtime.check_account_ready", readiness), \
             patch("linera2.runtime.ensure_wallet_connected", recovery), \
             patch("linera2.runtime.run_auto_session", auto):
            result = await scan_one_account(
                self.pw,
                SimpleNamespace(id="acct", ua="acct"),
                hub=hub,
                store=self.store,
                auto_session_store=object(),
                run_auto=True,
            )

        self.assertIs(result, second)
        self.assertEqual(readiness.await_count, 2)
        recovery.assert_awaited_once()
        self.assertEqual(hub.restart_calls, [])
        self.assertEqual(self.store.results, [second])
        auto.assert_not_awaited()

    async def test_missing_okx_popup_restarts_once_then_runs_auto_when_ready(self):
        first = self.disconnected_result()
        after_first_recovery = self.disconnected_result()
        after_restart = self.disconnected_result()
        readiness = AsyncMock(
            side_effect=[first, after_first_recovery, after_restart, self.result]
        )
        recovery = AsyncMock(
            side_effect=[
                WalletRecoveryResult(False, "未检测到 OKX 钱包确认窗口"),
                WalletRecoveryResult(True, "wallet connected after restart"),
            ]
        )
        second_page = SimpleNamespace(url="https://app.linera.xyz/originals/ride")
        second_context = SimpleNamespace(pages=[second_page])
        second_browser = SimpleNamespace(contexts=[second_context])
        first_browser = self.pw.chromium.browsers[0]
        chromium = FakeChromium(first_browser, second_browser)
        pw = SimpleNamespace(chromium=chromium)
        hub = FakeHub()
        auto = AsyncMock()

        with patch("linera2.runtime.check_account_ready", readiness), \
             patch("linera2.runtime.ensure_wallet_connected", recovery), \
             patch("linera2.runtime.run_auto_session", auto):
            result = await scan_one_account(
                pw,
                SimpleNamespace(id="acct", ua="acct"),
                hub=hub,
                store=self.store,
                auto_session_store=object(),
                run_auto=True,
            )

        self.assertIs(result, self.result)
        self.assertEqual(hub.restart_calls, ["acct"])
        self.assertEqual(len(chromium.calls), 2)
        self.assertEqual(readiness.await_count, 4)
        self.assertEqual(recovery.await_count, 2)
        self.assertIs(recovery.await_args_list[1].args[0], second_page)
        auto.assert_awaited_once()

    async def test_signing_without_okx_popup_uses_same_one_shot_restart(self):
        first = self.disconnected_result()
        after_first_recovery = self.disconnected_result()
        after_restart = self.disconnected_result()
        readiness = AsyncMock(
            side_effect=[first, after_first_recovery, after_restart, self.result]
        )
        recovery = AsyncMock(
            side_effect=[
                WalletRecoveryResult(False, "Signing 状态未恢复 OKX 确认窗口"),
                WalletRecoveryResult(True, "wallet connected after restart"),
            ]
        )
        hub = FakeHub()
        auto = AsyncMock()

        with patch("linera2.runtime.check_account_ready", readiness), \
             patch("linera2.runtime.ensure_wallet_connected", recovery), \
             patch("linera2.runtime.run_auto_session", auto):
            result = await scan_one_account(
                self.pw,
                SimpleNamespace(id="acct", ua="acct"),
                hub=hub,
                store=self.store,
                auto_session_store=object(),
                run_auto=True,
            )

        self.assertIs(result, self.result)
        self.assertEqual(hub.restart_calls, ["acct"])
        self.assertEqual(recovery.await_count, 2)
        auto.assert_awaited_once()

    async def test_missing_okx_wallet_tile_uses_same_one_shot_restart(self):
        first = self.disconnected_result()
        after_first_recovery = self.disconnected_result()
        after_restart = self.disconnected_result()
        readiness = AsyncMock(
            side_effect=[first, after_first_recovery, after_restart, self.result]
        )
        recovery = AsyncMock(
            side_effect=[
                WalletRecoveryResult(
                    False,
                    "重载 Ride 页面后：Dynamic 弹窗中未找到 OKX Wallet",
                ),
                WalletRecoveryResult(True, "wallet connected after restart"),
            ]
        )
        hub = FakeHub()
        auto = AsyncMock()

        with patch("linera2.runtime.check_account_ready", readiness), \
             patch("linera2.runtime.ensure_wallet_connected", recovery), \
             patch("linera2.runtime.run_auto_session", auto):
            result = await scan_one_account(
                self.pw,
                SimpleNamespace(id="acct", ua="acct"),
                hub=hub,
                store=self.store,
                auto_session_store=object(),
                run_auto=True,
            )

        self.assertIs(result, self.result)
        self.assertEqual(hub.restart_calls, ["acct"])
        self.assertEqual(recovery.await_count, 2)
        auto.assert_awaited_once()

    async def test_missing_okx_popup_still_disconnected_never_restarts_twice(self):
        states = [self.disconnected_result() for _ in range(4)]
        states[-1].reason = "still disconnected after restart"
        readiness = AsyncMock(side_effect=states)
        recovery = AsyncMock(
            side_effect=[
                WalletRecoveryResult(False, "未检测到 OKX 钱包确认窗口"),
                WalletRecoveryResult(False, "未检测到 OKX 钱包确认窗口"),
            ]
        )
        hub = FakeHub()
        auto = AsyncMock()

        with patch("linera2.runtime.check_account_ready", readiness), \
             patch("linera2.runtime.ensure_wallet_connected", recovery), \
             patch("linera2.runtime.run_auto_session", auto):
            result = await scan_one_account(
                self.pw,
                SimpleNamespace(id="acct", ua="acct"),
                hub=hub,
                store=self.store,
                auto_session_store=object(),
                run_auto=True,
            )

        self.assertEqual(hub.restart_calls, ["acct"])
        self.assertEqual(recovery.await_count, 2)
        self.assertIn("首次恢复", result.reason)
        self.assertIn("冷启动重启后", result.reason)
        auto.assert_not_awaited()

    async def test_restart_creates_and_navigates_page_when_only_extension_remains(self):
        states = [
            self.disconnected_result(),
            self.disconnected_result(),
            self.disconnected_result(),
            self.result,
        ]
        recovery = AsyncMock(
            side_effect=[
                WalletRecoveryResult(False, "未检测到 OKX 钱包确认窗口"),
                WalletRecoveryResult(True, "wallet connected after restart"),
            ]
        )
        created_page = SimpleNamespace(url="about:blank", goto=AsyncMock())
        extension_page = SimpleNamespace(
            url="chrome-extension://okx-extension-id/home.html"
        )
        second_context = SimpleNamespace(
            pages=[extension_page],
            new_page=AsyncMock(return_value=created_page),
        )
        second_browser = SimpleNamespace(contexts=[second_context])
        first_browser = self.pw.chromium.browsers[0]
        pw = SimpleNamespace(chromium=FakeChromium(first_browser, second_browser))
        auto = AsyncMock()

        with patch(
            "linera2.runtime.check_account_ready",
            AsyncMock(side_effect=states),
        ), patch(
            "linera2.runtime.ensure_wallet_connected",
            recovery,
        ), patch("linera2.runtime.run_auto_session", auto):
            result = await scan_one_account(
                pw,
                SimpleNamespace(id="acct", ua="acct"),
                hub=FakeHub(),
                store=self.store,
                auto_session_store=object(),
                run_auto=True,
            )

        self.assertIs(result, self.result)
        second_context.new_page.assert_awaited_once()
        created_page.goto.assert_awaited_once_with(
            "https://app.linera.xyz/originals/ride?market=BTC&duration=1",
            wait_until="domcontentloaded",
            timeout=30_000,
        )
        self.assertIs(recovery.await_args_list[1].args[0], created_page)
        auto.assert_awaited_once()

    async def test_recovery_exception_still_reruns_and_stores_readiness(self):
        first = self.disconnected_result()
        second = self.disconnected_result()
        second.reason = "final disconnected state"
        readiness = AsyncMock(side_effect=[first, second])
        recovery = AsyncMock(side_effect=RuntimeError("CDP closed"))
        auto = AsyncMock()

        with patch("linera2.runtime.check_account_ready", readiness), \
             patch("linera2.runtime.ensure_wallet_connected", recovery), \
             patch("linera2.runtime.run_auto_session", auto):
            result = await scan_one_account(
                self.pw,
                SimpleNamespace(id="acct", ua="acct"),
                hub=FakeHub(),
                store=self.store,
                auto_session_store=object(),
                run_auto=True,
            )

        self.assertIs(result, second)
        self.assertEqual(readiness.await_count, 2)
        self.assertEqual(self.store.results, [second])
        auto.assert_not_awaited()

    async def test_other_not_ready_state_does_not_trigger_recovery(self):
        syncing = self.disconnected_result()
        syncing.state = ReadinessState.WALLET_SYNCING.value
        syncing.wallet_connected = True
        recovery = AsyncMock()
        auto = AsyncMock()

        with patch(
            "linera2.runtime.check_account_ready",
            AsyncMock(return_value=syncing),
        ), patch("linera2.runtime.ensure_wallet_connected", recovery), \
             patch("linera2.runtime.run_auto_session", auto):
            result = await scan_one_account(
                self.pw,
                SimpleNamespace(id="acct", ua="acct"),
                hub=FakeHub(),
                store=self.store,
                auto_session_store=object(),
                run_auto=True,
            )

        self.assertIs(result, syncing)
        recovery.assert_not_awaited()
        auto.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
