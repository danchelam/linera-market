from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from time import monotonic
from typing import Awaitable, Callable
from urllib.parse import parse_qs, urlparse

from playwright.async_api import Page

from .auto_tracking import HistoryCounts


AUTO_CARD_TEXT = re.compile(r"Auto\s+bet every round", re.I)
HIGHER_INPUT = 'input[aria-label="Higher coins"]'
LOWER_INPUT = 'input[aria-label="Lower coins"]'
START_TEXT = re.compile(r"Start Auto", re.I)
HISTORY_ROW = "tr.border-t.border-white\\/5"
POSITION_BUTTON = "aside button"
TARGET_MARKET_URL = "https://app.linera.xyz/originals/ride?market=BTC&duration=1"


class AutoPageError(RuntimeError):
    pass


@dataclass(frozen=True)
class AutoPageState:
    running: bool
    paused: bool
    stop_visible: bool
    auto_on_visible: bool = False


class AutoPage:
    def __init__(
        self,
        page: Page,
        *,
        timeout: float = 15.0,
        start_timeout: float = 75.0,
        poll_interval: float = 0.2,
    ) -> None:
        self.page = page
        self.timeout = max(0.01, timeout)
        self.start_timeout = max(self.timeout, start_timeout)
        self.poll_interval = max(0, poll_interval)

    async def _visible(self, locator) -> bool:
        try:
            return await locator.count() > 0 and await locator.first.is_visible()
        except Exception:
            return False

    async def _wait_until(
        self,
        predicate,
        message: str,
        *,
        timeout: float | None = None,
    ) -> None:
        deadline = monotonic() + (self.timeout if timeout is None else timeout)
        while monotonic() < deadline:
            if await predicate():
                return
            await asyncio.sleep(self.poll_interval)
        raise AutoPageError(message)

    async def read_state(self) -> AutoPageState:
        auto_on = await self._visible(self.page.get_by_text("AUTO ON", exact=True))
        pause_visible = await self._visible(
            self.page.get_by_role("button", name="Pause")
        )
        resume_visible = await self._visible(
            self.page.get_by_role("button", name="Resume")
        )
        stop_visible = await self._visible(
            self.page.get_by_role("button", name="Stop")
        )
        running = pause_visible and stop_visible
        return AutoPageState(
            running=running,
            paused=resume_visible and stop_visible,
            stop_visible=stop_visible,
            auto_on_visible=auto_on or stop_visible,
        )

    @staticmethod
    def _is_target_market_url(url: str) -> bool:
        try:
            parsed = urlparse(url or "")
            query = parse_qs(parsed.query)
            return (
                parsed.hostname == "app.linera.xyz"
                and parsed.path == "/originals/ride"
                and query.get("market") == ["BTC"]
                and query.get("duration") == ["1"]
            )
        except Exception:
            return False

    async def validate_target_market(self) -> None:
        if not self._is_target_market_url(self.page.url or ""):
            raise AutoPageError("页面已偏离 BTC 1m，禁止启动 Auto")

    async def ensure_target_market(self) -> None:
        if not self._is_target_market_url(self.page.url or ""):
            try:
                await self.page.goto(
                    TARGET_MARKET_URL,
                    wait_until="domcontentloaded",
                    timeout=30_000,
                )
            except Exception as exc:
                raise AutoPageError("无法打开 BTC 1m 页面") from exc

        deadline = monotonic() + self.timeout
        stable = 0
        reassertions = 0
        while monotonic() < deadline:
            correct = self._is_target_market_url(self.page.url or "")
            if correct:
                stable += 1
                if stable >= 10:
                    return
            else:
                stable = 0
                if reassertions < 2:
                    reassertions += 1
                    try:
                        await self.page.goto(
                            TARGET_MARKET_URL,
                            wait_until="domcontentloaded",
                            timeout=30_000,
                        )
                    except Exception as exc:
                        raise AutoPageError("BTC 1m 页面被切换后无法恢复") from exc
            await asyncio.sleep(self.poll_interval)
        raise AutoPageError("BTC 1m 页面未能保持稳定")

    async def pause_once(self) -> bool:
        state = await self.read_state()
        if state.paused:
            return True
        if not state.running:
            return False
        button = self.page.get_by_role("button", name="Pause").first
        if not await self._visible(button):
            return False
        try:
            await button.click()
        except Exception:
            return False

        async def paused() -> bool:
            return (await self.read_state()).paused

        try:
            await self._wait_until(paused, "Auto 暂停标志未出现")
        except AutoPageError:
            return False
        return True

    async def read_history_counts(self) -> HistoryCounts:
        try:
            history_rows = await self.page.locator(HISTORY_ROW).all_inner_texts()
            position_rows = await self.page.locator(
                POSITION_BUTTON
            ).all_inner_texts()
        except Exception as exc:
            raise AutoPageError("无法读取 History") from exc
        active_position_rows = [
            raw
            for raw in position_rows
            if re.search(r"\bBTC\s+1m\b", " ".join(str(raw).split()), re.I)
            and re.search(
                r"\b(?:HIGHER|LOWER)\b", " ".join(str(raw).split()), re.I
            )
            and re.search(
                r"\b1\s+coins?\b", " ".join(str(raw).split()), re.I
            )
            and re.search(
                r"\b(?:Live|Open)\b", " ".join(str(raw).split()), re.I
            )
        ]
        rows = active_position_rows or history_rows
        higher = 0
        lower = 0
        active_higher = 0
        active_lower = 0
        for raw in rows:
            text = " ".join(str(raw).split())
            if not (
                re.search(r"\bBTC\b", text, re.I)
                and re.search(r"\b1m\b", text, re.I)
                and re.search(r"\b1\s+coins?\b", text, re.I)
            ):
                continue
            has_higher = bool(re.search(r"\bHIGHER\b", text, re.I))
            has_lower = bool(re.search(r"\bLOWER\b", text, re.I))
            if has_higher == has_lower:
                continue
            higher += int(has_higher)
            lower += int(has_lower)
            is_active = bool(re.search(r"\b(?:Live|Open)\b", text, re.I))
            if is_active:
                active_higher += int(has_higher)
                active_lower += int(has_lower)
        return HistoryCounts(
            higher=higher,
            lower=lower,
            active_higher=active_higher,
            active_lower=active_lower,
        )

    async def open_configuration(self) -> None:
        role_card = self.page.get_by_role(
            "button", name=AUTO_CARD_TEXT
        ).first
        text_card = self.page.get_by_text(AUTO_CARD_TEXT).first
        stake_toggle = self.page.get_by_role(
            "button", name="Show bet controls"
        ).first

        async def visible_card():
            if await self._visible(role_card):
                return role_card
            if await self._visible(text_card):
                return text_card
            return None

        card = await visible_card()
        if card is None:
            async def entry_ready() -> bool:
                return (
                    await visible_card() is not None
                    or await self._visible(stake_toggle)
                )

            try:
                await self._wait_until(
                    entry_ready, "Auto 配置入口未加载"
                )
            except AutoPageError as exc:
                raise AutoPageError("未找到 Auto 配置入口") from exc

        card = await visible_card()
        if card is None:
            if not await self._visible(stake_toggle):
                raise AutoPageError("未找到 Auto 配置入口")
            try:
                await stake_toggle.click()
            except Exception as exc:
                raise AutoPageError("无法展开 Stake 控制区") from exc

            async def card_ready() -> bool:
                return await visible_card() is not None

            await self._wait_until(card_ready, "Auto 配置入口未加载")
            card = await visible_card()
        if card is None:
            raise AutoPageError("未找到 Auto 配置入口")
        try:
            await card.click()
        except Exception as exc:
            raise AutoPageError("无法打开 Auto 配置") from exc

        async def input_ready() -> bool:
            return await self._visible(self.page.locator(HIGHER_INPUT))

        await self._wait_until(input_ready, "Auto 配置未加载")

    async def configure_one_plus_one(self) -> None:
        higher = self.page.locator(HIGHER_INPUT)
        lower = self.page.locator(LOWER_INPUT)
        if not await self._visible(higher) or not await self._visible(lower):
            raise AutoPageError("Auto 金额输入框不可用")
        try:
            await higher.fill("1")
            await lower.fill("1")
            values = (await higher.input_value(), await lower.input_value())
        except Exception as exc:
            raise AutoPageError("无法设置 Auto 金额") from exc
        if values != ("1", "1"):
            raise AutoPageError("Auto 金额校验失败")

    async def start(
        self,
        *,
        before_click: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        button = self.page.get_by_role("button", name=START_TEXT).first

        async def start_ready() -> bool:
            try:
                return await self._visible(button) and await button.is_enabled()
            except Exception:
                return False

        await self._wait_until(
            start_ready,
            "Start Auto 按钮在轮次开放前一直不可用",
            timeout=self.start_timeout,
        )
        await self.validate_target_market()
        if before_click is not None:
            await before_click()
        try:
            await button.click()
        except Exception as exc:
            raise AutoPageError("无法启动 Auto") from exc

        async def running() -> bool:
            return (await self.read_state()).running

        await self._wait_until(running, "Auto 启动标志未出现")

    async def stop_once(self) -> bool:
        button = self.page.get_by_role("button", name="Stop").first
        deadline = monotonic() + self.timeout
        clicked = False
        inactive_samples = 0
        while monotonic() < deadline:
            state = await self.read_state()
            active = (
                state.running
                or state.paused
                or state.stop_visible
                or state.auto_on_visible
            )
            if not clicked and state.stop_visible:
                try:
                    await button.click()
                    clicked = True
                    inactive_samples = 0
                except Exception:
                    await asyncio.sleep(self.poll_interval)
                continue
            if active:
                inactive_samples = 0
            else:
                inactive_samples += 1
                if inactive_samples >= 3:
                    return True
            await asyncio.sleep(self.poll_interval)
        return False
