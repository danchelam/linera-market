import asyncio
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linera2.readiness import (  # noqa: E402
    BackendEvidence,
    BackendMonitor,
    FrontendSnapshot,
    ReadinessState,
    evaluate_readiness,
    mask_wallet_address,
    read_frontend_snapshot,
    wait_for_stable_readiness,
)


def healthy_snapshot(**overrides):
    values = {
        "page_available": True,
        "wallet_connected": True,
        "wallet_address": "0x091e1234567890abcdef85e3",
        "coins": 433,
        "ride_ui_ready": True,
        "loading": False,
    }
    values.update(overrides)
    return FrontendSnapshot(**values)


class ReadinessEvaluationTests(unittest.TestCase):
    def test_masks_wallet_address_before_returning_it(self):
        self.assertEqual(mask_wallet_address("0x091e1234567890abcdef85e3"), "0x091e...85e3")

    def test_ready_requires_frontend_backend_and_positive_coins(self):
        result = evaluate_readiness(
            "625421671",
            healthy_snapshot(),
            BackendEvidence(success_count=1),
        )

        self.assertTrue(result.ready)
        self.assertEqual(result.state, ReadinessState.READY.value)
        self.assertEqual(result.coins, 433)
        self.assertEqual(result.wallet_address_masked, "0x091e...85e3")

    def test_missing_wallet_address_is_disconnected(self):
        result = evaluate_readiness(
            "acct",
            healthy_snapshot(wallet_connected=False, wallet_address=None),
            BackendEvidence(success_count=1),
        )

        self.assertFalse(result.ready)
        self.assertEqual(result.state, ReadinessState.WALLET_DISCONNECTED.value)

    def test_wallet_with_no_backend_success_is_syncing(self):
        result = evaluate_readiness(
            "acct",
            healthy_snapshot(),
            BackendEvidence(),
        )

        self.assertEqual(result.state, ReadinessState.WALLET_SYNCING.value)

    def test_backend_error_has_priority_over_page_ui(self):
        result = evaluate_readiness(
            "acct",
            healthy_snapshot(ride_ui_ready=False),
            BackendEvidence(failure_count=1, last_error="HTTP 500"),
        )

        self.assertEqual(result.state, ReadinessState.BACKEND_UNAVAILABLE.value)
        self.assertIn("HTTP 500", result.reason)

    def test_missing_ride_controls_is_page_loading(self):
        result = evaluate_readiness(
            "acct",
            healthy_snapshot(ride_ui_ready=False),
            BackendEvidence(success_count=1),
        )

        self.assertEqual(result.state, ReadinessState.PAGE_LOADING.value)

    def test_zero_coins_is_synced_but_not_ready(self):
        result = evaluate_readiness(
            "acct",
            healthy_snapshot(coins=0),
            BackendEvidence(success_count=1),
        )

        self.assertFalse(result.ready)
        self.assertTrue(result.wallet_connected)
        self.assertTrue(result.backend_ok)
        self.assertEqual(result.state, ReadinessState.INSUFFICIENT_BALANCE.value)


class StableReadinessTests(unittest.IsolatedAsyncioTestCase):
    async def test_requires_two_consecutive_ready_samples(self):
        samples = iter([
            evaluate_readiness("acct", healthy_snapshot(), BackendEvidence(success_count=1)),
            evaluate_readiness("acct", healthy_snapshot(), BackendEvidence(success_count=1)),
        ])

        async def reader():
            return next(samples)

        result = await wait_for_stable_readiness(
            reader,
            timeout=1,
            stable_samples=2,
            sample_interval=0,
        )

        self.assertTrue(result.ready)

    async def test_returns_last_state_when_timeout_expires(self):
        last = evaluate_readiness("acct", healthy_snapshot(), BackendEvidence())

        async def reader():
            await asyncio.sleep(0)
            return last

        result = await wait_for_stable_readiness(
            reader,
            timeout=0.01,
            stable_samples=2,
            sample_interval=0,
        )

        self.assertEqual(result.state, ReadinessState.WALLET_SYNCING.value)


class FakeLocator:
    def __init__(self, text="", count=1):
        self.text = text
        self._count = count

    @property
    def first(self):
        return self

    @property
    def last(self):
        return self

    async def count(self):
        return self._count

    async def is_visible(self):
        return self._count > 0

    async def inner_text(self, timeout=0):
        return self.text


class FakePage:
    def locator(self, selector):
        values = {
            "header button span.font-mono": "433",
            "header button:has(span.bg-success) span.font-mono": "0x091e...85e3",
            '[title="Coins — Originals"] span.font-mono': "433",
            "svg.animate-spin, [aria-busy='true']": "",
        }
        text = values.get(selector, "")
        count = 0 if selector == "svg.animate-spin, [aria-busy='true']" else bool(text)
        return FakeLocator(text, int(count))

    def get_by_role(self, role, name=None):
        pattern = getattr(name, "pattern", "")
        if "connect" in pattern.lower():
            return FakeLocator("", 0)
        if "BULL" in pattern:
            return FakeLocator("▲ BULL")
        if "BEAR" in pattern:
            return FakeLocator("▼ BEAR")
        return FakeLocator("", 0)

    def get_by_text(self, text, exact=False):
        return FakeLocator("Stake" if text == "Stake" else "", int(text == "Stake"))


class FrontendSnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def test_wallet_address_comes_from_connected_wallet_button_not_coin_balance(self):
        snapshot = await read_frontend_snapshot(FakePage())

        self.assertTrue(snapshot.wallet_connected)
        self.assertEqual(snapshot.wallet_address, "0x091e...85e3")
        self.assertEqual(snapshot.coins, 433)


class FakeBackendResponse:
    def __init__(self, status, payload):
        self.status = status
        self._payload = payload

    async def json(self):
        return self._payload


class BackendMonitorTests(unittest.IsolatedAsyncioTestCase):
    async def test_graphql_null_data_is_not_counted_as_backend_success(self):
        monitor = BackendMonitor()

        await monitor._inspect_response(FakeBackendResponse(200, {"data": None}))

        self.assertEqual(monitor.evidence.success_count, 0)
        self.assertEqual(monitor.evidence.failure_count, 1)

    async def test_nonempty_graphql_data_is_counted_as_success(self):
        monitor = BackendMonitor()

        await monitor._inspect_response(FakeBackendResponse(200, {"data": {"balance": 433}}))

        self.assertEqual(monitor.evidence.success_count, 1)
        self.assertEqual(monitor.evidence.failure_count, 0)


if __name__ == "__main__":
    unittest.main()
