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
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse(self.payload)


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
    last_error = None

    def start_browser(self, _account_id):
        return "127.0.0.1:2397"


class FakeChromium:
    def __init__(self, browser):
        self.browser = browser

    async def connect_over_cdp(self, *_args, **_kwargs):
        return self.browser


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
        with patch(
            "linera2.runtime.check_account_ready",
            AsyncMock(return_value=disconnected),
        ), patch("linera2.runtime.ensure_wallet_connected", recovery):
            result = await scan_one_account(
                self.pw,
                SimpleNamespace(id="acct", ua="acct"),
                hub=FakeHub(),
                store=self.store,
            )

        self.assertIs(result, disconnected)
        recovery.assert_not_awaited()
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
        recovery.assert_awaited_once()
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
