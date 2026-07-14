import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linera2.readiness import FrontendSnapshot  # noqa: E402
from linera2.wallet_recovery import (  # noqa: E402
    WalletRecoveryResult,
    ensure_wallet_connected,
)


class FakeLocator:
    def __init__(self, *, count=1, visible=True, events=None):
        self._count = count
        self._visible = visible
        self.events = events if events is not None else []
        self.first = self

    async def count(self):
        return self._count

    async def is_visible(self):
        return self._visible

    async def click(self, **_kwargs):
        self.events.append("connect_clicked")


class FakeMouse:
    def __init__(self, events):
        self.events = events

    async def click(self, x, y):
        self.events.append(("mouse_clicked", x, y))


class FakePage:
    def __init__(self, events, *, connect_count=1, tile=None):
        self.events = events
        self.url = "https://app.linera.xyz/originals/ride"
        self.mouse = FakeMouse(events)
        self.connect = FakeLocator(count=connect_count, events=events)
        self.tile = tile

    def get_by_role(self, role, **_kwargs):
        assert role == "button"
        return self.connect

    async def evaluate(self, script):
        if "ListTile" in script:
            self.events.append("tile_bounds_read")
            return self.tile
        self.events.append("stale_modal_checked")
        return False


class FakeContext:
    def __init__(self, events, pages=None):
        self.events = events
        self.pages = list(pages or [])
        self.listeners = []

    def on(self, event, callback):
        assert event == "page"
        self.events.append("popup_observer_registered")
        self.listeners.append(callback)

    def remove_listener(self, event, callback):
        assert event == "page"
        if callback in self.listeners:
            self.listeners.remove(callback)


def disconnected_snapshot():
    return FrontendSnapshot(wallet_connected=False, wallet_address=None)


def connected_snapshot():
    return FrontendSnapshot(
        wallet_connected=True,
        wallet_address="0x1234...abcd",
        coins=433,
        ride_ui_ready=True,
    )


class WalletRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def helpers(self, *, unlock=True, confirm=True):
        return SimpleNamespace(
            unlock=AsyncMock(return_value=unlock),
            confirm=AsyncMock(return_value=confirm),
            extension_id="okx-extension-id",
        )

    async def test_result_is_frozen_value_object(self):
        result = WalletRecoveryResult(True, "connected")

        self.assertTrue(result.recovered)
        with self.assertRaises(AttributeError):
            result.reason = "changed"

    async def test_already_connected_skips_parent_wallet_and_clicks(self):
        events = []
        page = FakePage(events)
        context = FakeContext(events)
        loader = Mock()

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(return_value=connected_snapshot()),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", loader):
            result = await ensure_wallet_connected(page, context, "acct")

        self.assertTrue(result.recovered)
        loader.assert_not_called()
        self.assertEqual(events, [])

    async def test_unlock_failure_stops_before_connect(self):
        events = []
        page = FakePage(events)
        context = FakeContext(events)
        helpers = self.helpers(unlock=False)

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(return_value=disconnected_snapshot()),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers):
            result = await ensure_wallet_connected(page, context, "acct")

        self.assertFalse(result.recovered)
        self.assertIn("解锁", result.reason)
        self.assertNotIn("connect_clicked", events)
        helpers.confirm.assert_not_awaited()

    async def test_need_dapp_unlock_signal_continues_with_trusted_tile_click(self):
        events = []
        popup = SimpleNamespace(
            url=(
                "chrome-extension://okx-extension-id/notification.html"
                "#/dapp-read"
            )
        )
        page = FakePage(
            events,
            tile={"x": 10, "y": 20, "width": 100, "height": 40},
        )
        context = FakeContext(events, pages=[page, popup])
        helpers = self.helpers(unlock="NEED_DAPP")
        snapshot_reader = AsyncMock(
            side_effect=[disconnected_snapshot(), connected_snapshot()]
        )

        with patch("linera2.wallet_recovery.read_frontend_snapshot", snapshot_reader), \
             patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertTrue(result.recovered)
        helpers.confirm.assert_awaited_once_with(popup, "acct", max_rounds=5)
        self.assertLess(
            events.index("popup_observer_registered"),
            events.index(("mouse_clicked", 60.0, 40.0)),
        )
        self.assertIn("connect_clicked", events)
        self.assertIn("tile_bounds_read", events)

    async def test_missing_connect_returns_controlled_failure(self):
        events = []
        page = FakePage(events, connect_count=0)
        context = FakeContext(events)
        helpers = self.helpers()

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(return_value=disconnected_snapshot()),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertFalse(result.recovered)
        self.assertIn("Connect", result.reason)

    async def test_missing_okx_tile_returns_controlled_failure(self):
        events = []
        page = FakePage(events, tile=None)
        context = FakeContext(events)
        helpers = self.helpers()

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(return_value=disconnected_snapshot()),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers), \
             patch("linera2.wallet_recovery._wait_for_okx_tile_center", AsyncMock(return_value=None)):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertFalse(result.recovered)
        self.assertIn("OKX", result.reason)

    async def test_popup_hash_is_ignored_when_matching_notification_page(self):
        events = []
        popup = SimpleNamespace(
            url=(
                "chrome-extension://okx-extension-id/notification.html"
                "#/unlock"
            )
        )
        page = FakePage(
            events,
            tile={"x": 0, "y": 0, "width": 20, "height": 20},
        )
        context = FakeContext(events, pages=[page, popup])
        helpers = self.helpers()

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(side_effect=[disconnected_snapshot(), connected_snapshot()]),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertTrue(result.recovered)
        helpers.confirm.assert_awaited_once()

    async def test_confirmation_failure_does_not_claim_connected(self):
        events = []
        popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        page = FakePage(
            events,
            tile={"x": 0, "y": 0, "width": 20, "height": 20},
        )
        context = FakeContext(events, pages=[page, popup])
        helpers = self.helpers(confirm=False)

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(return_value=disconnected_snapshot()),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertFalse(result.recovered)
        self.assertIn("确认", result.reason)

    async def test_connected_snapshot_is_required_after_confirmation(self):
        events = []
        popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        page = FakePage(
            events,
            tile={"x": 0, "y": 0, "width": 20, "height": 20},
        )
        context = FakeContext(events, pages=[page, popup])
        helpers = self.helpers()

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(return_value=disconnected_snapshot()),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers), \
             patch("linera2.wallet_recovery._wait_for_connected_snapshot", AsyncMock(return_value=False)):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertFalse(result.recovered)
        self.assertIn("连接", result.reason)


if __name__ == "__main__":
    unittest.main()
