import asyncio
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
        sequence = self.page.visibility_sequences.get(self.key)
        if sequence:
            value = sequence.pop(0)
            if not sequence and not value:
                self.page.visible.discard(self.key)
            return value
        return self.key in self.page.visible

    async def is_enabled(self):
        sequence = self.page.enabled_sequences.get(self.key)
        if sequence:
            return sequence.pop(0)
        return True

    async def click(self, **kwargs):
        if not await self.is_enabled():
            raise RuntimeError("button disabled")
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
        self.url = "https://app.linera.xyz/originals/ride?market=BTC&duration=1"
        self.visible = set()
        self.values = {}
        self.clicks = []
        self.click_callbacks = {}
        self.history_rows = []
        self.position_rows = []
        self.auto_card_role_only = False
        self.visibility_sequences = {}
        self.enabled_sequences = {}
        self.navigations = []

    def _key_for_name(self, name, *, role=False):
        pattern = name.pattern if isinstance(name, re.Pattern) else str(name)
        if "Start Auto" in pattern:
            return "start"
        if "Auto" in pattern and "bet every round" in pattern:
            if role and self.auto_card_role_only:
                return "auto_role_card"
            return "auto_card"
        return pattern

    def get_by_text(self, name, exact=False):
        return FakeLocator(self, self._key_for_name(name))

    def get_by_role(self, role, name=None):
        return FakeLocator(self, self._key_for_name(name, role=True))

    def locator(self, selector):
        if selector.startswith("tr"):
            return FakeLocator(self, "history", texts=self.history_rows)
        if selector == "aside button":
            return FakeLocator(self, "positions", texts=self.position_rows)
        return FakeLocator(self, selector)

    async def goto(self, url, **_kwargs):
        self.navigations.append(url)
        self.url = url


class AutoPageTests(unittest.IsolatedAsyncioTestCase):
    def adapter(self, page):
        return AutoPage(
            page,
            timeout=0.03,
            start_timeout=0.03,
            poll_interval=0,
        )

    async def test_read_state_requires_auto_on_pause_and_stop(self):
        page = FakePage()
        page.visible.update({"AUTO ON", "Pause", "Stop"})

        state = await self.adapter(page).read_state()

        self.assertTrue(state.running)
        page.visible.remove("Pause")
        partial = await self.adapter(page).read_state()
        self.assertFalse(partial.running)
        self.assertTrue(partial.auto_on_visible)

    async def test_ensure_target_market_reasserts_btc_one_minute_route(self):
        page = FakePage()
        page.url = "https://app.linera.xyz/originals/ride?market=BTC&duration=5"

        await self.adapter(page).ensure_target_market()

        self.assertEqual(
            page.navigations,
            ["https://app.linera.xyz/originals/ride?market=BTC&duration=1"],
        )
        self.assertIn("duration=1", page.url)

    async def test_ensure_target_market_does_not_reload_correct_route(self):
        page = FakePage()

        await self.adapter(page).ensure_target_market()

        self.assertEqual(page.navigations, [])

    async def test_validate_target_market_rejects_route_drift(self):
        page = FakePage()
        page.url = "https://app.linera.xyz/originals/ride?market=BTC&duration=5"

        with self.assertRaises(AutoPageError):
            await self.adapter(page).validate_target_market()

    async def test_pause_clicks_once_and_waits_for_resume_marker(self):
        page = FakePage()
        page.visible.update({"Pause", "Stop"})
        page.click_callbacks["Pause"] = lambda: (
            page.visible.discard("Pause"),
            page.visible.add("Resume"),
        )

        paused = await self.adapter(page).pause_once()

        self.assertTrue(paused)
        self.assertEqual(page.clicks, ["Pause"])

    async def test_read_state_uses_pause_and_stop_when_auto_text_is_unavailable(self):
        page = FakePage()
        page.visible.update({"Pause", "Stop"})

        state = await self.adapter(page).read_state()

        self.assertTrue(state.running)
        self.assertTrue(state.auto_on_visible)

    async def test_read_state_treats_resume_and_stop_as_paused_auto(self):
        page = FakePage()
        page.visible.update({"Resume", "Stop"})

        state = await self.adapter(page).read_state()

        self.assertFalse(state.running)
        self.assertTrue(state.paused)
        self.assertTrue(state.auto_on_visible)
        self.assertTrue(state.stop_visible)

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

    async def test_open_configuration_uses_auto_button_accessible_name(self):
        page = FakePage()
        page.auto_card_role_only = True
        higher = 'input[aria-label="Higher coins"]'
        page.visible.add("auto_role_card")
        page.click_callbacks["auto_role_card"] = lambda: page.visible.add(higher)

        await self.adapter(page).open_configuration()

        self.assertEqual(page.clicks, ["auto_role_card"])

    async def test_open_configuration_falls_back_to_legacy_text_card(self):
        page = FakePage()
        page.auto_card_role_only = True
        higher = 'input[aria-label="Higher coins"]'
        page.visible.add("auto_card")
        page.click_callbacks["auto_card"] = lambda: page.visible.add(higher)

        await self.adapter(page).open_configuration()

        self.assertEqual(page.clicks, ["auto_card"])

    async def test_open_configuration_expands_collapsed_stake_controls_first(self):
        page = FakePage()
        higher = 'input[aria-label="Higher coins"]'
        page.visible.add("Show bet controls")
        page.click_callbacks["Show bet controls"] = lambda: page.visible.add(
            "auto_card"
        )
        page.click_callbacks["auto_card"] = lambda: page.visible.add(higher)

        await self.adapter(page).open_configuration()

        self.assertEqual(page.clicks, ["Show bet controls", "auto_card"])

    async def test_open_configuration_waits_for_round_transition_to_finish(self):
        page = FakePage()
        higher = 'input[aria-label="Higher coins"]'
        page.click_callbacks["auto_card"] = lambda: page.visible.add(higher)

        async def reveal_auto_card():
            await asyncio.sleep(0)
            page.visible.add("auto_card")

        reveal_task = asyncio.create_task(reveal_auto_card())
        await self.adapter(page).open_configuration()
        await reveal_task

        self.assertEqual(page.clicks, ["auto_card"])

    async def test_start_uses_start_auto_button_and_waits_for_running_markers(self):
        page = FakePage()
        page.visible.add("start")
        page.click_callbacks["start"] = lambda: page.visible.update(
            {"AUTO ON", "Pause", "Stop"}
        )

        await self.adapter(page).start()

        self.assertEqual(page.clicks, ["start"])

    async def test_start_waits_until_round_unlocks_and_button_is_enabled(self):
        page = FakePage()
        page.visible.add("start")
        page.enabled_sequences["start"] = [False, False, True]
        page.click_callbacks["start"] = lambda: page.visible.update(
            {"AUTO ON", "Pause", "Stop"}
        )

        await self.adapter(page).start()

        self.assertEqual(page.clicks, ["start"])
        self.assertEqual(page.enabled_sequences["start"], [])

    async def test_start_revalidates_route_immediately_before_click(self):
        page = FakePage()
        page.visible.add("start")
        page.url = "https://app.linera.xyz/originals/ride?market=BTC&duration=5"

        with self.assertRaises(AutoPageError):
            await self.adapter(page).start()

        self.assertEqual(page.clicks, [])

    async def test_start_runs_baseline_hook_after_enable_wait_before_click(self):
        page = FakePage()
        page.visible.add("start")
        page.enabled_sequences["start"] = [False, True]
        observed = []

        async def before_click():
            observed.append(list(page.clicks))

        page.click_callbacks["start"] = lambda: page.visible.update(
            {"AUTO ON", "Pause", "Stop"}
        )

        await self.adapter(page).start(before_click=before_click)

        self.assertEqual(observed, [[]])
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

    async def test_stop_clicks_when_only_pause_and_stop_markers_are_detectable(self):
        page = FakePage()
        page.visible.update({"Pause", "Stop"})
        page.click_callbacks["Stop"] = lambda: page.visible.difference_update(
            {"Pause", "Stop"}
        )

        stopped = await self.adapter(page).stop_once()

        self.assertTrue(stopped)
        self.assertEqual(page.clicks, ["Stop"])

    async def test_stop_waits_through_transient_button_disappearance(self):
        page = FakePage()
        page.visible.update({"AUTO ON", "Pause", "Stop"})

        def transient_rerender():
            page.visible.difference_update({"AUTO ON", "Pause"})
            page.visibility_sequences["Stop"] = [
                False,
                True,
                False,
                False,
                False,
            ]

        page.click_callbacks["Stop"] = transient_rerender

        stopped = await self.adapter(page).stop_once()

        self.assertTrue(stopped)
        self.assertEqual(page.clicks, ["Stop"])
        self.assertEqual(page.visibility_sequences["Stop"], [])

    async def test_stop_waits_for_temporarily_hidden_stop_button(self):
        page = FakePage()
        page.visible.update({"Pause", "Stop"})
        page.visibility_sequences["Stop"] = [False, True]
        page.click_callbacks["Stop"] = lambda: page.visible.difference_update(
            {"Pause", "Stop"}
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

    async def test_history_reads_new_portfolio_position_buttons(self):
        page = FakePage()
        page.position_rows = [
            "ORIGINALS BTC 1m LOWER 1 coins · Open +~0.98 coins",
            "ORIGINALS BTC 1m HIGHER 1 coins · Live +1 coins",
            "Share",
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
