import re
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from linera2.auto_page import AutoPage, AutoPageError  # noqa: E402
from linera2.auto_tracking import HistoryCounts  # noqa: E402


class FakeLocator:
    def __init__(self, page, key, *, texts=None):
        self.page = page
        self.key = key
        self.texts = texts

    @property
    def first(self):
        return self

    async def count(self):
        if self.texts is not None:
            return len(self.texts)
        return 1 if self.key in self.page.visible else 0

    async def is_visible(self):
        return self.key in self.page.visible

    async def click(self, **kwargs):
        self.page.clicks.append(self.key)
        callback = self.page.click_callbacks.get(self.key)
        if callback:
            callback()

    async def fill(self, value):
        self.page.values[self.key] = value

    async def input_value(self):
        return self.page.values.get(self.key, "")

    async def all_inner_texts(self):
        return list(self.texts or [])


class FakePage:
    def __init__(self):
        self.visible = set()
        self.values = {}
        self.clicks = []
        self.click_callbacks = {}
        self.history_rows = []

    def _key_for_name(self, name):
        pattern = name.pattern if isinstance(name, re.Pattern) else str(name)
        if "Start Auto" in pattern:
            return "start"
        if "Auto" in pattern and "bet every round" in pattern:
            return "auto_card"
        return pattern

    def get_by_text(self, name, exact=False):
        return FakeLocator(self, self._key_for_name(name))

    def get_by_role(self, role, name=None):
        return FakeLocator(self, self._key_for_name(name))

    def locator(self, selector):
        if selector.startswith("tr"):
            return FakeLocator(self, "history", texts=self.history_rows)
        return FakeLocator(self, selector)


class AutoPageTests(unittest.IsolatedAsyncioTestCase):
    def adapter(self, page):
        return AutoPage(page, timeout=0.03, poll_interval=0)

    async def test_read_state_requires_auto_on_pause_and_stop(self):
        page = FakePage()
        page.visible.update({"AUTO ON", "Pause", "Stop"})

        state = await self.adapter(page).read_state()

        self.assertTrue(state.running)
        page.visible.remove("Pause")
        partial = await self.adapter(page).read_state()
        self.assertFalse(partial.running)
        self.assertTrue(partial.auto_on_visible)

    async def test_configure_fills_exactly_one_and_one(self):
        page = FakePage()
        higher = 'input[aria-label="Higher coins"]'
        lower = 'input[aria-label="Lower coins"]'
        page.visible.update({higher, lower})

        await self.adapter(page).configure_one_plus_one()

        self.assertEqual(page.values, {higher: "1", lower: "1"})

    async def test_open_configuration_clicks_observed_auto_card(self):
        page = FakePage()
        higher = 'input[aria-label="Higher coins"]'
        page.visible.add("auto_card")
        page.click_callbacks["auto_card"] = lambda: page.visible.add(higher)

        await self.adapter(page).open_configuration()

        self.assertEqual(page.clicks, ["auto_card"])

    async def test_start_uses_start_auto_button_and_waits_for_running_markers(self):
        page = FakePage()
        page.visible.add("start")
        page.click_callbacks["start"] = lambda: page.visible.update(
            {"AUTO ON", "Pause", "Stop"}
        )

        await self.adapter(page).start()

        self.assertEqual(page.clicks, ["start"])

    async def test_stop_clicks_once_and_waits_until_auto_on_disappears(self):
        page = FakePage()
        page.visible.update({"AUTO ON", "Pause", "Stop"})
        page.click_callbacks["Stop"] = lambda: page.visible.difference_update(
            {"AUTO ON", "Pause", "Stop"}
        )

        stopped = await self.adapter(page).stop_once()

        self.assertTrue(stopped)
        self.assertEqual(page.clicks, ["Stop"])

    async def test_stop_reports_false_when_auto_remains_visible(self):
        page = FakePage()
        page.visible.update({"AUTO ON", "Pause", "Stop"})

        stopped = await self.adapter(page).stop_once()

        self.assertFalse(stopped)
        self.assertEqual(page.clicks, ["Stop"])

    async def test_history_counts_only_btc_one_minute_one_coin_rows(self):
        page = FakePage()
        page.history_rows = [
            "BTC 1m HIGHER 1 coins +~1.05 coins 2.05× · Live",
            "BTC 1m LOWER 1 coins +~0.95 coins 1.95× · Open",
            "ETH 1m HIGHER 1 coins · Open",
            "BTC 5m LOWER 1 coins · Open",
            "BTC 1m HIGHER 2 coins · Open",
            "BTC 1m HIGHER LOWER 1 coins · Open",
        ]

        counts = await self.adapter(page).read_history_counts()

        self.assertEqual(
            counts,
            HistoryCounts(
                higher=1,
                lower=1,
                active_higher=1,
                active_lower=1,
            ),
        )

    async def test_history_marks_live_and_open_rows_as_active(self):
        page = FakePage()
        page.history_rows = [
            "BTC 1m HIGHER 1 coins · Live",
            "BTC 1m LOWER 1 coins · Open",
            "BTC 1m HIGHER 1 coins · Won",
            "BTC 1m LOWER 1 coins · Lost",
        ]

        counts = await self.adapter(page).read_history_counts()

        self.assertEqual(counts.higher, 2)
        self.assertEqual(counts.lower, 2)
        self.assertEqual(counts.active_higher, 1)
        self.assertEqual(counts.active_lower, 1)

    async def test_configuration_verification_failure_is_typed(self):
        page = FakePage()
        page.visible.update(
            {
                'input[aria-label="Higher coins"]',
                'input[aria-label="Lower coins"]',
            }
        )
        adapter = self.adapter(page)
        original_locator = page.locator

        def bad_locator(selector):
            locator = original_locator(selector)
            if "Lower" in selector:
                async def wrong_value():
                    return "2"
                locator.input_value = wrong_value
            return locator

        page.locator = bad_locator

        with self.assertRaises(AutoPageError):
            await adapter.configure_one_plus_one()


if __name__ == "__main__":
    unittest.main()
