import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linera2.wallet_support import (  # noqa: E402
    _find_and_fill_password,
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
    def __init__(self):
        self.first = self
        self.filled = []

    async def count(self):
        return 1

    async def fill(self, value):
        self.filled.append(value)


class FakePasswordFrame:
    def __init__(self, locator=None):
        self.password = locator

    def locator(self, selector):
        if selector == 'input[type="password"]' and self.password is not None:
            return self.password
        return MissingLocator()


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
        popup = SimpleNamespace(
            main_frame=main_frame,
            frames=[main_frame, FakePasswordFrame(password)],
            evaluate=lambda *_args: None,
        )

        self.assertTrue(
            await _find_and_fill_password(popup, "acct", "private-password")
        )
        self.assertEqual(password.filled, ["private-password"])


if __name__ == "__main__":
    unittest.main()
