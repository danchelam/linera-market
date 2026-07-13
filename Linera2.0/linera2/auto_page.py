from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from time import monotonic

from playwright.async_api import Page

from .auto_tracking import HistoryCounts


AUTO_CARD_TEXT = re.compile(r"Auto\s+bet every round", re.I)
HIGHER_INPUT = 'input[aria-label="Higher coins"]'
LOWER_INPUT = 'input[aria-label="Lower coins"]'
START_TEXT = re.compile(r"Start Auto", re.I)
HISTORY_ROW = "tr.border-t.border-white\\/5"


class AutoPageError(RuntimeError):
    pass


@dataclass(frozen=True)
class AutoPageState:
    running: bool
    paused: bool
    stop_visible: bool


class AutoPage:
    def __init__(
        self,
        page: Page,
        *,
        timeout: float = 15.0,
        poll_interval: float = 0.2,
    ) -> None:
        self.page = page
        self.timeout = max(0.01, timeout)
        self.poll_interval = max(0, poll_interval)

    async def _visible(self, locator) -> bool:
        try:
            return await locator.count() > 0 and await locator.first.is_visible()
        except Exception:
            return False

    async def _wait_until(self, predicate, message: str) -> None:
        deadline = monotonic() + self.timeout
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
        stop_visible = await self._visible(
            self.page.get_by_role("button", name="Stop")
        )
        return AutoPageState(
            running=auto_on and pause_visible and stop_visible,
            paused=pause_visible,
            stop_visible=stop_visible,
        )

    async def read_history_counts(self) -> HistoryCounts:
        try:
            rows = await self.page.locator(HISTORY_ROW).all_inner_texts()
        except Exception as exc:
            raise AutoPageError("无法读取 History") from exc
        higher = 0
        lower = 0
        for raw in rows:
            text = " ".join(str(raw).split())
            if not (
                re.search(r"\bBTC\b", text, re.I)
                and re.search(r"\b1m\b", text, re.I)
                and re.search(r"\b1\s+coins\b", text, re.I)
            ):
                continue
            has_higher = bool(re.search(r"\bHIGHER\b", text, re.I))
            has_lower = bool(re.search(r"\bLOWER\b", text, re.I))
            if has_higher == has_lower:
                continue
            higher += int(has_higher)
            lower += int(has_lower)
        return HistoryCounts(higher=higher, lower=lower)

    async def open_configuration(self) -> None:
        card = self.page.get_by_text(AUTO_CARD_TEXT).first
        if not await self._visible(card):
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

    async def start(self) -> None:
        button = self.page.get_by_role("button", name=START_TEXT).first
        if not await self._visible(button):
            raise AutoPageError("Start Auto 按钮不可用")
        try:
            await button.click()
        except Exception as exc:
            raise AutoPageError("无法启动 Auto") from exc

        async def running() -> bool:
            return (await self.read_state()).running

        await self._wait_until(running, "Auto 启动标志未出现")

    async def stop_once(self) -> bool:
        auto_on = self.page.get_by_text("AUTO ON", exact=True)
        if not await self._visible(auto_on):
            return True
        button = self.page.get_by_role("button", name="Stop").first
        if not await self._visible(button):
            return False
        try:
            await button.click()
        except Exception:
            return False

        async def stopped() -> bool:
            return not await self._visible(auto_on)

        try:
            await self._wait_until(stopped, "Auto 停止标志未消失")
        except AutoPageError:
            return False
        return True
