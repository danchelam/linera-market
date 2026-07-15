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
    AutoSignResult,
    WalletRecoveryResult,
    _click_connect,
    _click_pending_signing,
    ensure_wallet_connected,
    ensure_auto_sign_enabled,
    _confirm_wallet_steps,
    _dismiss_linera_overlays,
    _open_wallet_confirmation,
    _wait_for_popup_state_change,
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
        signing_text="Signing…",
        onboarding_count=0,
        follow_count=0,
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
        self.signing_text = signing_text
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
        self.follow = FakeLocator(
            count=follow_count,
            events=events,
            click_event="follow_prompt_dismissed",
        )
        self.tile = tile

    def get_by_role(self, role, **kwargs):
        assert role == "button"
        if kwargs.get("name") == "Skip":
            return self.skip
        if kwargs.get("name") == "Maybe later":
            return self.follow
        name = kwargs.get("name")
        if hasattr(name, "search") and name.search(self.signing_text):
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


class FakeAutoSignLocator:
    def __init__(
        self,
        *,
        count=1,
        visible=True,
        checked=False,
        events=None,
        click_event="auto_sign_clicked",
    ):
        self._count = count
        self._visible = visible
        self._checked = checked
        self.events = events if events is not None else []
        self.click_event = click_event
        self.first = self

    async def count(self):
        return self._count

    async def is_visible(self):
        return self._visible

    async def is_checked(self):
        return self._checked

    async def is_disabled(self):
        return False

    async def click(self, **_kwargs):
        self.events.append(self.click_event)


class FakeAutoSignPage:
    def __init__(
        self,
        events,
        *,
        checked=False,
        label_visible=True,
        close_hides=True,
    ):
        self.events = events
        self.label = FakeAutoSignLocator(visible=label_visible, events=events)
        self.menu = FakeAutoSignLocator(events=events)
        self.close_menu = FakeAutoSignLocator(
            events=events,
            click_event="wallet_menu_closed",
        )
        if close_hides:
            original_click = self.close_menu.click

            async def close_and_hide(**kwargs):
                await original_click(**kwargs)
                self.close_menu._visible = False
                self.label._visible = False

            self.close_menu.click = close_and_hide
        self.switch = FakeAutoSignLocator(checked=checked, events=events)

    def get_by_text(self, text, **_kwargs):
        assert text == "Auto-sign trades"
        return self.label

    def get_by_role(self, role, **kwargs):
        if role == "button":
            if kwargs.get("name") == "Close menu":
                self.close_menu.events.append("close_menu_located")
                return self.close_menu
            assert kwargs.get("name") == "Menu"
            return self.menu
        assert role == "switch"
        assert "name" not in kwargs
        return self.switch


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

    async def test_auto_sign_result_is_frozen_value_object(self):
        result = AutoSignResult(True, "enabled")

        self.assertTrue(result.enabled)
        with self.assertRaises(AttributeError):
            result.reason = "changed"

    async def test_auto_sign_already_enabled_skips_wallet_confirmation(self):
        events = []
        page = FakeAutoSignPage(events, checked=True)
        context = FakeContext(events)
        loader = Mock()

        with patch("linera2.wallet_recovery._load_parent_wallet_helpers", loader):
            result = await ensure_auto_sign_enabled(page, context, "acct", timeout=1)

        self.assertTrue(result.enabled)
        self.assertIn("已经开启", result.reason)
        loader.assert_not_called()
        self.assertNotIn("auto_sign_clicked", events)
        self.assertIn("close_menu_located", events)
        self.assertIn("wallet_menu_closed", events)

    async def test_auto_sign_opens_wallet_confirmation_and_verifies_switch(self):
        events = []
        page = FakeAutoSignPage(events, checked=False)
        popup = SimpleNamespace(url="chrome-extension://okx/notification.html")
        context = FakeContext(events)
        helpers = self.helpers()

        with patch(
            "linera2.wallet_recovery._load_parent_wallet_helpers",
            return_value=helpers,
        ), patch(
            "linera2.wallet_recovery._wait_for_wallet_popup",
            AsyncMock(return_value=popup),
        ), patch(
            "linera2.wallet_recovery._confirm_wallet_steps",
            AsyncMock(return_value=True),
        ) as confirm, patch(
            "linera2.wallet_recovery._wait_for_auto_sign_enabled",
            AsyncMock(return_value=True),
        ):
            result = await ensure_auto_sign_enabled(page, context, "acct", timeout=1)

        self.assertTrue(result.enabled)
        self.assertIn("已开启", result.reason)
        self.assertIn("auto_sign_clicked", events)
        confirm.assert_awaited_once_with(helpers.confirm, popup, "acct", unittest.mock.ANY)

    async def test_auto_sign_missing_wallet_popup_is_controlled_failure(self):
        events = []
        page = FakeAutoSignPage(events, checked=False)
        context = FakeContext(events)

        with patch(
            "linera2.wallet_recovery._load_parent_wallet_helpers",
            return_value=self.helpers(),
        ), patch(
            "linera2.wallet_recovery._wait_for_wallet_popup",
            AsyncMock(return_value=None),
        ), patch(
            "linera2.wallet_recovery._wait_for_auto_sign_enabled",
            AsyncMock(return_value=False),
        ):
            result = await ensure_auto_sign_enabled(page, context, "acct", timeout=1)

        self.assertFalse(result.enabled)
        self.assertIn("确认窗口", result.reason)

    async def test_auto_sign_unlock_failure_never_clicks_switch(self):
        events = []
        page = FakeAutoSignPage(events, checked=False)
        context = FakeContext(events)
        helpers = self.helpers(unlock=False)

        with patch(
            "linera2.wallet_recovery._load_parent_wallet_helpers",
            return_value=helpers,
        ):
            result = await ensure_auto_sign_enabled(page, context, "acct", timeout=1)

        self.assertFalse(result.enabled)
        self.assertIn("解锁", result.reason)
        self.assertNotIn("auto_sign_clicked", events)

    async def test_auto_sign_success_requires_wallet_menu_to_close(self):
        events = []
        page = FakeAutoSignPage(events, checked=True, close_hides=False)
        context = FakeContext(events)

        result = await ensure_auto_sign_enabled(page, context, "acct", timeout=0.05)

        self.assertFalse(result.enabled)
        self.assertIn("菜单", result.reason)

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

    async def test_connect_wait_dismisses_late_onboarding_before_click(self):
        events = []
        page = FakePage(events, onboarding_count=1)

        result = await _wait_for_connect_click(
            page,
            asyncio.get_running_loop().time() + 1,
        )

        self.assertTrue(result)
        self.assertLess(
            events.index("onboarding_skipped"),
            events.index("connect_clicked"),
        )

    async def test_linera_overlay_cleanup_dismisses_follow_prompt(self):
        events = []
        page = FakePage(events, follow_count=1)

        dismissed = await _dismiss_linera_overlays(page)

        self.assertTrue(dismissed)
        self.assertIn("follow_prompt_dismissed", events)

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

    async def test_pending_signing_recognizes_retry_button_copy(self):
        for copy in ("Still signing · retry", "Connect failed · retry"):
            with self.subTest(copy=copy):
                events = []
                page = FakePage(
                    events,
                    connect_count=0,
                    signing_count=1,
                    signing_text=copy,
                )

                clicked = await _click_pending_signing(page)

                self.assertTrue(clicked)
                self.assertIn("signing_clicked", events)

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

    async def test_popup_wait_rejects_excluded_notification_even_with_linera_content(self):
        reused = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry",
            frames=[FakeBodyFrame("Confirm connection for app.linera.xyz")],
        )
        context = SimpleNamespace(pages=[reused])

        popup = await _wait_for_wallet_popup(
            context,
            [],
            "okx-extension-id",
            asyncio.get_running_loop().time() + 0.05,
            excluded_page_ids={id(reused)},
            require_linera_semantics=True,
        )

        self.assertIsNone(popup)

    async def test_wallet_confirmation_handles_existing_network_update_before_connect(self):
        events = []
        page = FakePage(events, connect_count=0)
        popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry",
            frames=[FakeBodyFrame("Confirm network for app.linera.xyz")],
        )
        context = FakeContext(events, pages=[page])

        with patch(
            "linera2.wallet_recovery._read_network_update_center",
            AsyncMock(return_value=(80.0, 90.0)),
        ), patch(
            "linera2.wallet_recovery._wait_for_wallet_popup",
            AsyncMock(return_value=popup),
        ):
            found, error = await _open_wallet_confirmation(
                page,
                context,
                [],
                "okx-extension-id",
                asyncio.get_running_loop().time() + 0.05,
            )

        self.assertIs(found, popup)
        self.assertEqual(error, "")
        self.assertIn(("mouse_clicked", 80.0, 90.0), events)
        self.assertNotIn("connect_clicked", events)

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

    async def test_initial_signing_resume_has_sixty_second_popup_budget(self):
        events = []
        page = FakePage(events, connect_count=0, signing_count=1)
        context = FakeContext(events, pages=[page])
        signing_popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry",
            frames=[FakeBodyFrame("Confirm request from app.linera.xyz")],
        )

        async def popup_only_with_long_budget(
            _context,
            _observed,
            _extension_id,
            deadline,
            **_kwargs,
        ):
            remaining = deadline - asyncio.get_running_loop().time()
            return signing_popup if remaining >= 50 else None

        with patch(
            "linera2.wallet_recovery._wait_for_wallet_popup",
            AsyncMock(side_effect=popup_only_with_long_budget),
        ):
            popup, error = await _open_wallet_confirmation(
                page,
                context,
                [],
                "okx-extension-id",
                asyncio.get_running_loop().time() - 1,
            )

        self.assertIs(popup, signing_popup)
        self.assertEqual(error, "")
        self.assertEqual(events.count("signing_clicked"), 1)

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

    async def test_wallet_confirmation_retries_one_unchanged_enabled_step(self):
        state = {"closed": False, "clicks": 0}
        popup = SimpleNamespace(is_closed=lambda: state["closed"])

        async def close_on_second_click(*_args, **_kwargs):
            state["clicks"] += 1
            if state["clicks"] == 2:
                state["closed"] = True
            return True

        confirm = AsyncMock(side_effect=close_on_second_click)
        with patch(
            "linera2.wallet_recovery._popup_state_marker",
            AsyncMock(return_value="unchanged"),
        ), patch(
            "linera2.wallet_recovery._wait_for_popup_state_change",
            AsyncMock(return_value=False),
        ) as wait_change:
            result = await _confirm_wallet_steps(
                confirm,
                popup,
                "acct",
                asyncio.get_running_loop().time() + 5,
            )

        self.assertTrue(result)
        self.assertEqual(confirm.await_count, 2)
        self.assertEqual(wait_change.await_count, 1)

    async def test_popup_state_change_allows_thirty_second_slow_render(self):
        popup = SimpleNamespace()
        clock = {"now": 0.0}
        loop = SimpleNamespace(time=lambda: clock["now"])

        async def advance(delay):
            clock["now"] += delay

        async def marker_after_slow_render(_popup):
            return "after" if clock["now"] >= 10 else "before"

        with patch(
            "linera2.wallet_recovery.asyncio.get_running_loop",
            return_value=loop,
        ), patch(
            "linera2.wallet_recovery.asyncio.sleep",
            AsyncMock(side_effect=advance),
        ), patch(
            "linera2.wallet_recovery._popup_state_marker",
            AsyncMock(side_effect=marker_after_slow_render),
        ):
            changed = await _wait_for_popup_state_change(
                popup,
                "before",
                deadline=100,
            )

        self.assertTrue(changed)
        self.assertGreaterEqual(clock["now"], 10)

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

        self.assertTrue(result.recovered, result.reason)
        self.assertEqual(helpers.confirm.await_count, 2)
        self.assertEqual(
            [event for event in events if isinstance(event, tuple)],
            [
                ("mouse_clicked", 60.0, 40.0),
                ("mouse_clicked", 80.0, 90.0),
            ],
        )

    async def test_signing_that_appears_after_connection_is_resumed_once(self):
        events = []
        first_popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        signing_popup = SimpleNamespace(
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
        ), patch(
            "linera2.wallet_recovery._load_parent_wallet_helpers",
            return_value=helpers,
        ), patch(
            "linera2.wallet_recovery._click_pending_signing",
            AsyncMock(side_effect=[False, True]),
        ) as pending, patch(
            "linera2.wallet_recovery._wait_for_wallet_popup",
            AsyncMock(side_effect=[first_popup, signing_popup]),
        ), patch(
            "linera2.wallet_recovery._confirm_wallet_steps",
            AsyncMock(side_effect=[True, True]),
        ) as confirm, patch(
            "linera2.wallet_recovery._wait_for_connected_snapshot",
            AsyncMock(side_effect=[False, False, True]),
        ), patch(
            "linera2.wallet_recovery._wait_for_network_update_center",
            AsyncMock(return_value=None),
        ):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertTrue(result.recovered)
        self.assertEqual(pending.await_count, 1)
        self.assertEqual(confirm.await_count, 2)

    async def test_existing_linera_signing_popup_is_confirmed_without_retry(self):
        events = []
        first_popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        signing_popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry",
            frames=[FakeBodyFrame("Confirm request from app.linera.xyz")],
            is_closed=lambda: False,
            close=AsyncMock(),
        )
        page = FakePage(
            events,
            tile={"x": 10, "y": 20, "width": 100, "height": 40},
        )
        context = FakeContext(events, pages=[page])
        helpers = self.helpers()
        connected_checks = 0

        async def connected_after_existing_popup(*_args, **_kwargs):
            nonlocal connected_checks
            connected_checks += 1
            if connected_checks == 2:
                context.pages.append(signing_popup)
            return connected_checks >= 3

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(return_value=disconnected_snapshot()),
        ), patch(
            "linera2.wallet_recovery._load_parent_wallet_helpers",
            return_value=helpers,
        ), patch(
            "linera2.wallet_recovery._click_pending_signing",
            AsyncMock(side_effect=[False, True]),
        ) as pending, patch(
            "linera2.wallet_recovery._wait_for_wallet_popup",
            AsyncMock(side_effect=[first_popup, signing_popup]),
        ), patch(
            "linera2.wallet_recovery._confirm_wallet_steps",
            AsyncMock(side_effect=[True, True]),
        ) as confirm, patch(
            "linera2.wallet_recovery._wait_for_connected_snapshot",
            AsyncMock(side_effect=connected_after_existing_popup),
        ), patch(
            "linera2.wallet_recovery._wait_for_network_update_center",
            AsyncMock(return_value=None),
        ):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertTrue(result.recovered)
        self.assertEqual(pending.await_count, 1)
        self.assertEqual(confirm.await_count, 2)
        self.assertIs(confirm.await_args_list[1].args[1], signing_popup)
        signing_popup.close.assert_not_awaited()

    async def test_late_linera_signing_popup_gets_fresh_bounded_grace(self):
        events = []
        first_popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry",
            frames=[FakeBodyFrame("Confirm request from app.linera.xyz")],
            closed=False,
        )
        first_popup.is_closed = lambda: first_popup.closed
        late_popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry",
            frames=[FakeBodyFrame("Confirm request from app.linera.xyz")],
            is_closed=lambda: False,
        )
        page = FakePage(
            events,
            tile={"x": 10, "y": 20, "width": 100, "height": 40},
        )
        context = FakeContext(events, pages=[page])
        context.queue_page(first_popup)
        helpers = self.helpers()
        connected_checks = 0
        confirm_checks = 0

        async def confirm_and_close_first(*_args, **_kwargs):
            nonlocal confirm_checks
            confirm_checks += 1
            if confirm_checks == 1:
                first_popup.closed = True
            return True

        async def consume_deadline_then_publish_popup(*_args, **_kwargs):
            nonlocal connected_checks
            connected_checks += 1
            if connected_checks == 1:
                await asyncio.sleep(0.04)
                return False
            if connected_checks == 2:
                async def publish_late():
                    await asyncio.sleep(0.08)
                    context.pages.append(late_popup)

                asyncio.create_task(publish_late())
                await asyncio.sleep(0.04)
                return False
            return True

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(return_value=disconnected_snapshot()),
        ), patch(
            "linera2.wallet_recovery._load_parent_wallet_helpers",
            return_value=helpers,
        ), patch(
            "linera2.wallet_recovery._click_pending_signing",
            AsyncMock(side_effect=[False, True]),
        ) as pending, patch(
            "linera2.wallet_recovery._confirm_wallet_steps",
            AsyncMock(side_effect=confirm_and_close_first),
        ) as confirm, patch(
            "linera2.wallet_recovery._wait_for_connected_snapshot",
            AsyncMock(side_effect=consume_deadline_then_publish_popup),
        ), patch(
            "linera2.wallet_recovery._wait_for_network_update_center",
            AsyncMock(return_value=None),
        ):
            result = await ensure_wallet_connected(page, context, "acct", timeout=0.05)

        self.assertTrue(result.recovered)
        self.assertEqual(pending.await_count, 1)
        self.assertEqual(confirm.await_count, 2)
        self.assertIs(confirm.await_args_list[1].args[1], late_popup)

    async def test_signing_retry_has_sixty_second_total_popup_budget(self):
        events = []
        first_popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        late_popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry",
            frames=[FakeBodyFrame("Confirm request from app.linera.xyz")],
        )
        page = FakePage(
            events,
            tile={"x": 10, "y": 20, "width": 100, "height": 40},
        )
        context = FakeContext(events, pages=[page])
        helpers = self.helpers()
        popup_wait_calls = 0

        async def popup_only_with_long_budget(
            _context,
            _observed,
            _extension_id,
            deadline,
            **_kwargs,
        ):
            nonlocal popup_wait_calls
            popup_wait_calls += 1
            if popup_wait_calls == 1:
                return first_popup
            remaining = deadline - asyncio.get_running_loop().time()
            return late_popup if remaining >= 50 else None

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(return_value=disconnected_snapshot()),
        ), patch(
            "linera2.wallet_recovery._load_parent_wallet_helpers",
            return_value=helpers,
        ), patch(
            "linera2.wallet_recovery._click_pending_signing",
            AsyncMock(side_effect=[False, True]),
        ) as pending, patch(
            "linera2.wallet_recovery._wait_for_wallet_popup",
            AsyncMock(side_effect=popup_only_with_long_budget),
        ), patch(
            "linera2.wallet_recovery._confirm_wallet_steps",
            AsyncMock(side_effect=[True, True]),
        ), patch(
            "linera2.wallet_recovery._wait_for_connected_snapshot",
            AsyncMock(side_effect=[False, False, True]),
        ), patch(
            "linera2.wallet_recovery._wait_for_network_update_center",
            AsyncMock(return_value=None),
        ):
            result = await ensure_wallet_connected(page, context, "acct", timeout=0.05)

        self.assertTrue(result.recovered)
        self.assertEqual(pending.await_count, 2)

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

    async def test_missing_connect_reloads_original_ride_page_and_retries_once(self):
        events = []
        page = FakePage(events, connect_count=0)
        context = FakeContext(events, pages=[page])
        popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        helpers = self.helpers()
        open_confirmation = AsyncMock(
            side_effect=[
                (None, "未找到可用的 Connect 按钮"),
                (popup, ""),
            ]
        )

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(return_value=disconnected_snapshot()),
        ), patch(
            "linera2.wallet_recovery._load_parent_wallet_helpers",
            return_value=helpers,
        ), patch(
            "linera2.wallet_recovery._open_wallet_confirmation",
            open_confirmation,
        ), patch(
            "linera2.wallet_recovery._confirm_wallet_steps",
            AsyncMock(return_value=True),
        ), patch(
            "linera2.wallet_recovery._wait_for_connected_snapshot",
            AsyncMock(return_value=True),
        ):
            result = await ensure_wallet_connected(page, context, "acct", timeout=1)

        self.assertTrue(result.recovered)
        self.assertEqual(open_confirmation.await_count, 2)
        self.assertEqual(
            events.count(("goto", "https://app.linera.xyz/originals/ride")),
            1,
        )

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

    async def test_connect_transition_to_signing_wins_over_missing_okx_tile(self):
        events = []
        page = FakePage(events, tile=None)
        context = FakeContext(events, pages=[page])
        popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry",
            frames=[FakeBodyFrame("Confirm request from app.linera.xyz")],
        )

        with patch(
            "linera2.wallet_recovery._click_pending_signing",
            AsyncMock(side_effect=[False, True]),
        ) as pending, patch(
            "linera2.wallet_recovery._wait_for_okx_tile_center",
            AsyncMock(return_value=None),
        ), patch(
            "linera2.wallet_recovery._wait_for_wallet_popup",
            AsyncMock(return_value=popup),
        ):
            found, error = await _open_wallet_confirmation(
                page,
                context,
                [],
                "okx-extension-id",
                asyncio.get_running_loop().time() + 1,
            )

        self.assertIs(found, popup)
        self.assertEqual(error, "")
        self.assertEqual(pending.await_count, 2)

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

    async def test_late_signing_popup_gets_fresh_confirmation_budget(self):
        events = []
        page = FakePage(events)
        context = FakeContext(events, pages=[page])
        popup = SimpleNamespace(
            url="chrome-extension://okx-extension-id/notification.html#/dapp-entry"
        )
        helpers = self.helpers()

        async def late_popup(*_args, **_kwargs):
            await asyncio.sleep(0.06)
            return popup, ""

        async def slow_confirmation(*_args, **_kwargs):
            await asyncio.sleep(0.15)
            return True

        with patch(
            "linera2.wallet_recovery.read_frontend_snapshot",
            AsyncMock(return_value=disconnected_snapshot()),
        ), patch(
            "linera2.wallet_recovery._load_parent_wallet_helpers",
            return_value=helpers,
        ), patch(
            "linera2.wallet_recovery._open_wallet_confirmation",
            AsyncMock(side_effect=late_popup),
        ), patch(
            "linera2.wallet_recovery._confirm_wallet_steps",
            AsyncMock(side_effect=slow_confirmation),
        ), patch(
            "linera2.wallet_recovery._wait_for_connected_snapshot",
            AsyncMock(return_value=True),
        ):
            result = await ensure_wallet_connected(page, context, "acct", timeout=0.05)

        self.assertTrue(result.recovered)

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
