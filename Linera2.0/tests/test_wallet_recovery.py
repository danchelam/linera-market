import asyncio
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
    _click_connect,
    ensure_wallet_connected,
    _confirm_wallet_steps,
    _wait_for_connect_click,
    _wait_for_wallet_popup,
)


class FakeLocator:
    def __init__(self, *, count=1, visible=True, events=None, click_event="connect_clicked"):
        self._count = count
        self._visible = visible
        self.events = events if events is not None else []
        self.click_event = click_event
        self.first = self

    async def count(self):
        return self._count

    async def is_visible(self):
        return self._visible

    async def click(self, **_kwargs):
        self.events.append(self.click_event)


class FakeMouse:
    def __init__(self, events):
        self.events = events
        self.on_click = None

    async def click(self, x, y):
        self.events.append(("mouse_clicked", x, y))
        if self.on_click is not None:
            self.on_click()


class FakePage:
    def __init__(
        self,
        events,
        *,
        connect_count=1,
        signing_count=0,
        onboarding_count=0,
        tile=None,
    ):
        self.events = events
        self.url = "https://app.linera.xyz/originals/ride"
        self.mouse = FakeMouse(events)
        self.connect = FakeLocator(count=connect_count, events=events)
        self.signing = FakeLocator(
            count=signing_count,
            events=events,
            click_event="signing_clicked",
        )
        self.welcome = FakeLocator(
            count=onboarding_count,
            events=events,
            click_event="welcome_read",
        )
        self.skip = FakeLocator(
            count=onboarding_count,
            events=events,
            click_event="onboarding_skipped",
        )
        self.tile = tile

    def get_by_role(self, role, **kwargs):
        assert role == "button"
        if kwargs.get("name") == "Skip":
            return self.skip
        name = kwargs.get("name")
        if hasattr(name, "pattern") and "Signing" in name.pattern:
            return self.signing
        return self.connect

    def get_by_text(self, text, **_kwargs):
        assert text == "Welcome to Linera"
        return self.welcome

    async def evaluate(self, script):
        if "ListTile" in script:
            self.events.append("tile_bounds_read")
            return self.tile
        self.events.append("stale_modal_checked")
        return False

    async def goto(self, url, **_kwargs):
        self.url = url
        self.events.append(("goto", url))


class FakeContext:
    def __init__(self, events, pages=None):
        self.events = events
        self.pages = list(pages or [])
        self.listeners = []
        self.pending_pages = []
        for page in self.pages:
            mouse = getattr(page, "mouse", None)
            if mouse is not None:
                mouse.on_click = self._emit_next_page

    def queue_page(self, page):
        self.pending_pages.append(page)

    def _emit_next_page(self):
        if not self.pending_pages:
            return
        page = self.pending_pages.pop(0)
        self.pages.append(page)
        for callback in tuple(self.listeners):
            callback(page)

    def on(self, event, callback):
        assert event == "page"
        self.events.append("popup_observer_registered")
        self.listeners.append(callback)

    def remove_listener(self, event, callback):
        assert event == "page"
        if callback in self.listeners:
            self.listeners.remove(callback)


class FakeBodyFrame:
    def __init__(self, text):
        self.text = text

    def locator(self, selector):
        assert selector == "body"
        return self

    async def inner_text(self, **_kwargs):
        return self.text


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

    async def test_connect_waits_through_a_transient_overlay(self):
        page = SimpleNamespace()
        with patch(
            "linera2.wallet_recovery._click_connect",
            AsyncMock(side_effect=[TimeoutError(), True]),
        ) as click:
            result = await _wait_for_connect_click(
                page,
                asyncio.get_running_loop().time() + 1,
            )

        self.assertTrue(result)
        self.assertEqual(click.await_count, 2)

    async def test_connect_uses_locator_actionability_even_when_bounds_exist(self):
        events = []
        page = FakePage(events)
        page.connect.bounding_box = AsyncMock(
            return_value={"x": 10, "y": 20, "width": 100, "height": 40}
        )

        result = await _click_connect(page)

        self.assertTrue(result)
        self.assertIn("connect_clicked", events)
        self.assertFalse(any(isinstance(event, tuple) for event in events))

    async def test_popup_wait_excludes_notification_present_before_click(self):
        stale = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        fresh = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        context = SimpleNamespace(pages=[stale, fresh])

        popup = await _wait_for_wallet_popup(
            context,
            [fresh],
            "okx-extension-id",
            asyncio.get_running_loop().time() + 1,
            excluded_page_ids={id(stale)},
        )

        self.assertIs(popup, fresh)

    async def test_signing_resume_selects_existing_linera_popup_not_other_dapp(self):
        unrelated = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry",
            frames=[FakeBodyFrame("Confirm signature for unrelated.example")],
        )
        linera = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry",
            frames=[FakeBodyFrame("Confirm signature for app.linera.xyz")],
        )
        context = SimpleNamespace(pages=[linera, unrelated])

        popup = await _wait_for_wallet_popup(
            context,
            [],
            "okx-extension-id",
            asyncio.get_running_loop().time() + 1,
            require_linera_semantics=True,
        )

        self.assertIs(popup, linera)

    async def test_wallet_confirmation_waits_for_each_step_to_settle(self):
        popup = SimpleNamespace(
            is_closed=Mock(side_effect=[False, False, False, True]),
        )
        confirm = AsyncMock(return_value=True)
        with patch(
            "linera2.wallet_recovery._popup_state_marker",
            AsyncMock(side_effect=["first", "second"]),
        ), patch(
            "linera2.wallet_recovery._wait_for_popup_state_change",
            AsyncMock(return_value=True),
        ) as wait_change:
            result = await _confirm_wallet_steps(
                confirm,
                popup,
                "acct",
                asyncio.get_running_loop().time() + 5,
            )

        self.assertTrue(result)
        self.assertEqual(confirm.await_count, 2)
        confirm.assert_any_await(popup, "acct", max_rounds=1)
        self.assertEqual(wait_change.await_count, 1)

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

    async def test_popup_listener_registration_failure_is_controlled(self):
        events = []
        page = FakePage(events)
        context = FakeContext(events, pages=[page])
        context.on = Mock(side_effect=RuntimeError("context closed"))
        helpers = self.helpers()

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(return_value=disconnected_snapshot()),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertFalse(result.recovered)
        self.assertIn("RuntimeError", result.reason)

    async def test_unlock_can_restore_existing_dapp_connection_without_connect(self):
        events = []
        page = FakePage(events, connect_count=0)
        context = FakeContext(events)
        helpers = self.helpers(unlock=True)

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(side_effect=[disconnected_snapshot(), connected_snapshot()]),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers):
            result = await ensure_wallet_connected(page, context, "acct")

        self.assertTrue(result.recovered)
        self.assertIn("恢复", result.reason)
        self.assertNotIn("connect_clicked", events)
        helpers.confirm.assert_not_awaited()

    async def test_unlock_external_navigation_returns_to_original_linera_page(self):
        events = []
        original_url = "https://app.linera.xyz/originals/ride?market=BTC&duration=1"
        popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        page = FakePage(
            events,
            tile={"x": 10, "y": 20, "width": 100, "height": 40},
        )
        page.url = original_url
        context = FakeContext(events, pages=[page])
        context.queue_page(popup)
        helpers = self.helpers()

        async def navigate_while_unlocking(*_args, **_kwargs):
            page.url = "https://example.com/"
            return True

        helpers.unlock.side_effect = navigate_while_unlocking
        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(
                side_effect=[
                    disconnected_snapshot(),
                    disconnected_snapshot(),
                    connected_snapshot(),
                ]
            ),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertTrue(result.recovered)
        self.assertIn(("goto", original_url), events)
        self.assertLess(
            events.index(("goto", original_url)),
            events.index("connect_clicked"),
        )

    async def test_first_run_onboarding_is_dismissed_before_connect(self):
        events = []
        popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        page = FakePage(
            events,
            onboarding_count=1,
            tile={"x": 10, "y": 20, "width": 100, "height": 40},
        )
        context = FakeContext(events, pages=[page])
        context.queue_page(popup)
        helpers = self.helpers()

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(
                side_effect=[
                    disconnected_snapshot(),
                    disconnected_snapshot(),
                    connected_snapshot(),
                ]
            ),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertTrue(result.recovered)
        self.assertLess(
            events.index("onboarding_skipped"),
            events.index("connect_clicked"),
        )

    async def test_pending_signing_resumes_popup_without_reopening_connect_modal(self):
        events = []
        popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        page = FakePage(events, connect_count=0, signing_count=1)
        context = FakeContext(events, pages=[page, popup])
        helpers = self.helpers()

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(
                side_effect=[
                    disconnected_snapshot(),
                    disconnected_snapshot(),
                    connected_snapshot(),
                ]
            ),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertTrue(result.recovered)
        self.assertIn("signing_clicked", events)
        self.assertNotIn("connect_clicked", events)
        self.assertNotIn("tile_bounds_read", events)

    async def test_unsupported_network_runs_one_update_confirmation_stage(self):
        events = []
        first_popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        network_popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        page = FakePage(
            events,
            tile={"x": 10, "y": 20, "width": 100, "height": 40},
        )
        context = FakeContext(events, pages=[page])
        helpers = self.helpers()

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(return_value=disconnected_snapshot()),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers), \
             patch(
                 "linera2.wallet_recovery._wait_for_wallet_popup",
                 AsyncMock(side_effect=[first_popup, network_popup]),
             ), patch(
                 "linera2.wallet_recovery._wait_for_connected_snapshot",
                 AsyncMock(side_effect=[False, True]),
             ), patch(
                 "linera2.wallet_recovery._wait_for_network_update_center",
                 AsyncMock(return_value=(80.0, 90.0)),
             ):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertTrue(result.recovered)
        self.assertEqual(helpers.confirm.await_count, 2)
        self.assertEqual(
            [event for event in events if isinstance(event, tuple)],
            [
                ("mouse_clicked", 60.0, 40.0),
                ("mouse_clicked", 80.0, 90.0),
            ],
        )

    async def test_blank_unlock_popup_gets_one_bounded_unlock_retry(self):
        events = []
        blank_unlock = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/unlock",
            close=AsyncMock(),
            is_closed=lambda: False,
        )
        connected_popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry",
            is_closed=lambda: False,
        )
        page = FakePage(
            events,
            tile={"x": 10, "y": 20, "width": 100, "height": 40},
        )
        context = FakeContext(events, pages=[page])
        helpers = self.helpers()
        helpers.unlock.side_effect = [True, True]
        helpers.confirm.side_effect = [False, True]

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(return_value=disconnected_snapshot()),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers), \
             patch(
                 "linera2.wallet_recovery._wait_for_wallet_popup",
                 AsyncMock(side_effect=[blank_unlock, connected_popup]),
             ), patch(
                 "linera2.wallet_recovery._wait_for_okx_tile_center",
                 AsyncMock(side_effect=[(60.0, 40.0), (60.0, 40.0)]),
             ), patch(
                 "linera2.wallet_recovery._wait_for_connected_snapshot",
                 AsyncMock(return_value=True),
             ):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertTrue(result.recovered)
        self.assertEqual(helpers.unlock.await_count, 2)
        self.assertEqual(helpers.confirm.await_count, 2)
        blank_unlock.close.assert_awaited_once()
        self.assertEqual(events.count("connect_clicked"), 2)

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
        context = FakeContext(events, pages=[page])
        context.queue_page(popup)
        helpers = self.helpers(unlock="NEED_DAPP")
        snapshot_reader = AsyncMock(
            side_effect=[
                disconnected_snapshot(),
                disconnected_snapshot(),
                connected_snapshot(),
            ]
        )

        with patch("linera2.wallet_recovery.read_frontend_snapshot", snapshot_reader), \
             patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertTrue(result.recovered)
        helpers.confirm.assert_awaited_once_with(popup, "acct", max_rounds=1)
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
        context = FakeContext(events, pages=[page])
        context.queue_page(popup)
        helpers = self.helpers()

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(
                side_effect=[
                    disconnected_snapshot(),
                    disconnected_snapshot(),
                    connected_snapshot(),
                ]
            ),
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
        context = FakeContext(events, pages=[page])
        context.queue_page(popup)
        helpers = self.helpers(confirm=False)

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(return_value=disconnected_snapshot()),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertFalse(result.recovered)
        self.assertIn("确认", result.reason)

    async def test_connected_frontend_wins_when_notification_does_not_close(self):
        events = []
        popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        page = FakePage(
            events,
            tile={"x": 0, "y": 0, "width": 20, "height": 20},
        )
        context = FakeContext(events, pages=[page])
        context.queue_page(popup)
        helpers = self.helpers()

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(
                side_effect=[
                    disconnected_snapshot(),
                    disconnected_snapshot(),
                    connected_snapshot(),
                ]
            ),
        ), patch("linera2.wallet_recovery._load_parent_wallet_helpers", return_value=helpers), \
             patch(
                 "linera2.wallet_recovery._confirm_wallet_steps",
                 AsyncMock(return_value=False),
             ):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertTrue(result.recovered)
        self.assertIn("连接", result.reason)

    async def test_connected_snapshot_is_required_after_confirmation(self):
        events = []
        popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        page = FakePage(
            events,
            tile={"x": 0, "y": 0, "width": 20, "height": 20},
        )
        context = FakeContext(events, pages=[page])
        context.queue_page(popup)
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
