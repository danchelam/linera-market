import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linera2 import wallet_support  # noqa: E402
from linera2.wallet_support import (  # noqa: E402
    _click_unlock_button,
    _find_and_fill_password,
    _unlock_with_provider_popup,
    click_wallet_button,
    unlock_okx_wallet,
)


class FakeContext:
    pages = []


class FakeButton:
    def __init__(self, text, clicks):
        self.text = text
        self.clicks = clicks
        self.first = self

    async def count(self):
        return 1

    async def click(self, **_kwargs):
        self.clicks.append(self.text)


class MissingLocator:
    first = None

    async def count(self):
        return 0


class FailingLocator:
    first = None

    async def count(self):
        raise RuntimeError("locator failed")


class FakeWalletFrame:
    def __init__(self, button_text, clicks):
        self.button_text = button_text
        self.clicks = clicks

    def locator(self, selector):
        if selector == f'button:has-text("{self.button_text}")':
            return FakeButton(self.button_text, self.clicks)
        return MissingLocator()

    async def evaluate(self, _script):
        return False


class FakeWalletPage:
    def __init__(self, button_text):
        self.clicks = []
        self.main_frame = FakeWalletFrame(button_text, self.clicks)
        self.frames = [self.main_frame]

    def is_closed(self):
        return False


class FakePasswordLocator:
    def __init__(self, *, fill_error=None):
        self.first = self
        self.filled = []
        self.fill_error = fill_error

    async def count(self):
        return 1

    async def fill(self, value):
        if self.fill_error is not None:
            raise self.fill_error
        self.filled.append(value)


class FakePasswordFrame:
    def __init__(self, locator=None):
        self.password = locator

    def locator(self, selector):
        if selector == 'input[type="password"]' and self.password is not None:
            return self.password
        return MissingLocator()


class FakeFailingPasswordFrame:
    def locator(self, selector):
        if selector == 'input[type="password"]':
            return FailingLocator()
        return MissingLocator()


class FakeShadowProbePopup:
    def __init__(self, frames, shadow_result="absent"):
        self.frames = frames
        self.main_frame = frames[0]
        self.shadow_result = shadow_result

    async def evaluate(self, _script, _password):
        return self.shadow_result


class FakeRequest:
    def __init__(self, url, failure=None):
        self.url = url
        self.failure = failure


class FakeUnlockFrame:
    def __init__(self, events):
        self.events = events

    def locator(self, selector):
        if selector == 'button:has-text("Unlock")':
            frame = self

            class UnlockButton:
                first = None

                async def count(self):
                    return 1

                async def click(self):
                    frame.events.append("unlock_clicked")

            button = UnlockButton()
            button.first = button
            return button
        return MissingLocator()


class FakeUnlockPopup:
    def __init__(self):
        self.events = []
        self.main_frame = FakeUnlockFrame(self.events)
        self.frames = [self.main_frame]
        self.url = "chrome-extension://okx/notification.html#/done"
        self.keyboard = SimpleNamespace(press=AsyncMock())

    def on(self, event, _callback):
        self.events.append(f"listener:{event}")

    def remove_listener(self, _event, _callback):
        pass

    def is_closed(self):
        return False


class FakeProviderPage:
    def __init__(self, sensitive_url, popup):
        self.url = "https://app.linera.xyz/originals/ride"
        self.sensitive_url = sensitive_url
        self.popup = popup
        self.capture_popup = None

    async def evaluate(self, script):
        if "!!window.okxwallet" in script:
            return True
        if "window.ethereum" in script and "eth_accounts" not in script:
            return True
        if "isUnlocked" in script and "eth_accounts" not in script:
            return {"known": True, "unlocked": False}
        if "personal_sign" in script:
            await self.capture_popup(self.popup)
            raise RuntimeError(f"provider failed at {self.sensitive_url}")
        raise AssertionError("unexpected evaluate")


class FakeProviderPopup:
    def __init__(self, sensitive_url):
        self.url = sensitive_url

        class RenderedFrame:
            def locator(self, selector):
                if selector == "button":
                    class OneButton:
                        async def count(self):
                            return 1

                    return OneButton()
                return MissingLocator()

        self.main_frame = RenderedFrame()
        self.frames = [self.main_frame]

    async def wait_for_load_state(self, *_args, **_kwargs):
        pass

    async def close(self):
        pass

    async def evaluate(self, _script, _password):
        return "absent"

    def is_closed(self):
        return False


class FakeProviderContext:
    def __init__(self, page):
        self.pages = [page]
        self.page = page

    def on(self, event, callback):
        assert event == "page"
        self.page.capture_popup = callback

    def remove_listener(self, _event, _callback):
        pass


class WalletSupportTests(unittest.IsolatedAsyncioTestCase):
    async def test_unlock_refuses_write_mode_without_private_password(self):
        with patch("linera2.wallet_support.get_wallet_password", return_value=None):
            result = await unlock_okx_wallet(FakeContext(), "acct")

        self.assertFalse(result)

    async def test_click_wallet_button_uses_semantic_confirm_button(self):
        page = FakeWalletPage(button_text="Confirm")

        self.assertTrue(await click_wallet_button(page, "acct", max_rounds=1))
        self.assertEqual(page.clicks, ["Confirm"])

    async def test_password_field_discovery_searches_all_frames(self):
        password = FakePasswordLocator()
        main_frame = FakePasswordFrame()
        popup = FakeShadowProbePopup([main_frame, FakePasswordFrame(password)])

        self.assertTrue(
            await _find_and_fill_password(popup, "acct", "private-password")
        )
        self.assertEqual(password.filled, ["private-password"])

    async def test_password_present_but_locator_fill_failure_is_not_absent(self):
        password = FakePasswordLocator(fill_error=RuntimeError("fill failed"))
        popup = FakeShadowProbePopup(
            [FakePasswordFrame(), FakePasswordFrame(password)],
            shadow_result="absent",
        )

        result = await _find_and_fill_password(
            popup,
            "acct",
            "private-password",
        )

        self.assertIs(result, False)

    async def test_password_locator_probe_failure_is_not_absent(self):
        popup = FakeShadowProbePopup(
            [FakePasswordFrame(), FakeFailingPasswordFrame()],
            shadow_result="absent",
        )

        result = await _find_and_fill_password(
            popup,
            "acct",
            "private-password",
        )

        self.assertIs(result, False)

    def test_long_request_url_is_removed_by_matching_response(self):
        monitor = wallet_support._UnlockNetworkMonitor()
        url = "https://wallet.example/api/unlock?secret=" + "x" * 300

        monitor.on_request(FakeRequest(url))
        monitor.on_response(FakeRequest(url))

        self.assertFalse(monitor.has_network_problem)

    async def test_unlock_listeners_are_registered_before_unlock_click(self):
        popup = FakeUnlockPopup()

        with patch("linera2.wallet_support.asyncio.sleep", AsyncMock()):
            self.assertTrue(await _click_unlock_button(popup, "acct"))

        self.assertLess(
            popup.events.index("listener:requestfailed"),
            popup.events.index("unlock_clicked"),
        )

    async def test_unlock_logs_do_not_expose_popup_url_or_exception_text(self):
        sensitive_url = "chrome-extension://okx/notification.html?token=private-secret"
        popup = FakeProviderPopup(sensitive_url)
        page = FakeProviderPage(sensitive_url, popup)
        messages = []

        with patch("linera2.wallet_support.asyncio.sleep", AsyncMock()):
            result = await _unlock_with_provider_popup(
                FakeProviderContext(page),
                "acct",
                "private-password",
                "okx",
                lambda _account, message: messages.append(message),
            )

        self.assertTrue(result)
        combined = "\n".join(messages)
        self.assertNotIn(sensitive_url, combined)
        self.assertNotIn("private-secret", combined)
        self.assertNotIn("provider failed at", combined)
        self.assertNotIn("password=", combined)
        self.assertNotIn("frames=", combined)


if __name__ == "__main__":
    unittest.main()
