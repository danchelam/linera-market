from __future__ import annotations

import asyncio
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from typing import Awaitable, Callable

from playwright.async_api import BrowserContext, Page, Response


TARGET_URL = "https://app.linera.xyz/originals/ride?market=BTC&duration=1"
WORKER_HOST_SUFFIX = ".worker.infra.linera.net"
WALLET_PATTERN = re.compile(r"0x[0-9a-fA-F]{4,}(?:\.\.\.)?[0-9a-fA-F]{4}")


class ReadinessState(str, Enum):
    READY = "ready"
    BROWSER_UNREACHABLE = "browser_unreachable"
    PAGE_UNAVAILABLE = "page_unavailable"
    WALLET_DISCONNECTED = "wallet_disconnected"
    WALLET_SYNCING = "wallet_syncing"
    BACKEND_UNAVAILABLE = "backend_unavailable"
    PAGE_LOADING = "page_loading"
    INSUFFICIENT_BALANCE = "insufficient_balance"
    UNKNOWN = "unknown"


@dataclass(slots=True)
class ReadinessResult:
    account_id: str
    ready: bool
    state: str
    reason: str
    wallet_address_masked: str | None
    coins: int | None
    wallet_connected: bool
    backend_ok: bool
    ride_ui_ready: bool
    checked_at: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(slots=True)
class FrontendSnapshot:
    page_available: bool = True
    wallet_connected: bool = False
    wallet_address: str | None = None
    coins: int | None = None
    ride_ui_ready: bool = False
    loading: bool = False


@dataclass(slots=True)
class BackendEvidence:
    success_count: int = 0
    failure_count: int = 0
    last_error: str = ""

    @property
    def ok(self) -> bool:
        return self.success_count > 0


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def mask_wallet_address(address: str | None) -> str | None:
    if not address:
        return None
    if "..." in address:
        head, tail = address.split("...", 1)
        return f"{head[:6]}...{tail[-4:]}"
    if len(address) <= 12:
        return address
    return f"{address[:6]}...{address[-4:]}"


def _result(
    account_id: str,
    state: ReadinessState,
    reason: str,
    snapshot: FrontendSnapshot,
    backend: BackendEvidence,
) -> ReadinessResult:
    return ReadinessResult(
        account_id=account_id,
        ready=state is ReadinessState.READY,
        state=state.value,
        reason=reason,
        wallet_address_masked=mask_wallet_address(snapshot.wallet_address),
        coins=snapshot.coins,
        wallet_connected=snapshot.wallet_connected,
        backend_ok=backend.ok,
        ride_ui_ready=snapshot.ride_ui_ready,
        checked_at=_now(),
    )


def evaluate_readiness(
    account_id: str,
    snapshot: FrontendSnapshot,
    backend: BackendEvidence,
) -> ReadinessResult:
    if not snapshot.page_available:
        return _result(account_id, ReadinessState.PAGE_UNAVAILABLE, "目标页面不可用", snapshot, backend)
    if not snapshot.wallet_connected or not snapshot.wallet_address:
        return _result(account_id, ReadinessState.WALLET_DISCONNECTED, "未检测到已连接的钱包地址", snapshot, backend)
    if backend.failure_count > 0 and not backend.ok:
        detail = f"：{backend.last_error}" if backend.last_error else ""
        return _result(account_id, ReadinessState.BACKEND_UNAVAILABLE, f"Linera 后端请求失败{detail}", snapshot, backend)
    if not backend.ok or snapshot.coins is None:
        return _result(account_id, ReadinessState.WALLET_SYNCING, "钱包已连接，等待 Linera 链数据同步", snapshot, backend)
    if snapshot.loading or not snapshot.ride_ui_ready:
        return _result(account_id, ReadinessState.PAGE_LOADING, "Ride 操作区尚未加载完成", snapshot, backend)
    if snapshot.coins <= 0:
        return _result(account_id, ReadinessState.INSUFFICIENT_BALANCE, "Coins 为 0，账号不能开始任务", snapshot, backend)
    return _result(account_id, ReadinessState.READY, "钱包、链数据和 Ride 页面均已就绪", snapshot, backend)


def _stable_signature(result: ReadinessResult) -> tuple:
    return (
        result.state,
        result.wallet_address_masked,
        result.coins,
        result.backend_ok,
        result.ride_ui_ready,
    )


async def wait_for_stable_readiness(
    reader: Callable[[], Awaitable[ReadinessResult]],
    *,
    timeout: float,
    stable_samples: int,
    sample_interval: float,
) -> ReadinessResult:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    previous_signature = None
    stable_count = 0
    last_result: ReadinessResult | None = None

    while loop.time() < deadline:
        last_result = await reader()
        signature = _stable_signature(last_result)
        if last_result.ready and signature == previous_signature:
            stable_count += 1
        elif last_result.ready:
            stable_count = 1
        else:
            stable_count = 0
        previous_signature = signature
        if stable_count >= max(1, stable_samples):
            return last_result
        await asyncio.sleep(sample_interval)

    if last_result is not None:
        return last_result
    empty = FrontendSnapshot(page_available=False)
    return _result("", ReadinessState.UNKNOWN, "状态检测未产生结果", empty, BackendEvidence())


class BackendMonitor:
    def __init__(self) -> None:
        self.evidence = BackendEvidence()
        self._tasks: set[asyncio.Task] = set()

    def on_response(self, response: Response) -> None:
        try:
            url = response.url
            from urllib.parse import urlparse

            parsed = urlparse(url)
            if not parsed.hostname or not parsed.hostname.endswith(WORKER_HOST_SUFFIX):
                return
            if "/chains/" not in parsed.path or "/applications/" not in parsed.path:
                return
            task = asyncio.create_task(self._inspect_response(response))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        except Exception as exc:
            self.evidence.failure_count += 1
            self.evidence.last_error = str(exc)

    async def _inspect_response(self, response: Response) -> None:
        if response.status != 200:
            self.evidence.failure_count += 1
            self.evidence.last_error = f"HTTP {response.status}"
            return
        try:
            payload = await response.json()
            if isinstance(payload, dict) and payload.get("errors"):
                self.evidence.failure_count += 1
                self.evidence.last_error = "响应包含业务错误"
                return
            if isinstance(payload, dict) and "data" in payload and payload.get("data") in (None, "", {}, []):
                self.evidence.failure_count += 1
                self.evidence.last_error = "响应中的链数据为空"
                return
            if payload in (None, "", {}, []):
                self.evidence.failure_count += 1
                self.evidence.last_error = "响应体为空"
                return
            self.evidence.success_count += 1
        except (json.JSONDecodeError, ValueError):
            self.evidence.failure_count += 1
            self.evidence.last_error = "响应体不是有效 JSON"
        except Exception as exc:
            self.evidence.failure_count += 1
            self.evidence.last_error = str(exc)

    async def drain(self) -> None:
        if self._tasks:
            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)


async def _visible_text(locator) -> str:
    try:
        if await locator.count() == 0:
            return ""
        if not await locator.first.is_visible():
            return ""
        return (await locator.first.inner_text(timeout=2000)).strip()
    except Exception:
        return ""


async def read_frontend_snapshot(page: Page) -> FrontendSnapshot:
    try:
        address_text = await _visible_text(
            page.locator("header button:has(span.bg-success) span.font-mono")
        )
        address_match = WALLET_PATTERN.search(address_text)
        connect_visible = bool(await _visible_text(page.get_by_role("button", name=re.compile("connect", re.I))))

        coins_text = await _visible_text(
            page.locator('[title="Coins — Originals"] span.font-mono').last
        )
        coins = None
        if coins_text:
            number = re.search(r"-?[\d,]+", coins_text)
            if number:
                coins = int(number.group(0).replace(",", ""))

        bull = await _visible_text(page.get_by_role("button", name=re.compile(r"BULL", re.I)))
        bear = await _visible_text(page.get_by_role("button", name=re.compile(r"BEAR", re.I)))
        stake = await _visible_text(page.get_by_text("Stake", exact=True))
        loading = await page.locator("svg.animate-spin, [aria-busy='true']").count() > 0

        return FrontendSnapshot(
            page_available=True,
            wallet_connected=bool(address_match) and not connect_visible,
            wallet_address=address_match.group(0) if address_match else None,
            coins=coins,
            ride_ui_ready=bool(bull and bear and stake),
            loading=loading,
        )
    except Exception:
        return FrontendSnapshot(page_available=False)


async def check_account_ready(
    page: Page,
    context: BrowserContext,
    account_id: str,
    timeout: int = 60,
    stable_samples: int = 2,
) -> ReadinessResult:
    del context  # Reserved for later wallet-frame diagnostics; no interaction in phase one.
    monitor = BackendMonitor()
    page.on("response", monitor.on_response)
    try:
        try:
            await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=30_000)
        except Exception as exc:
            snapshot = FrontendSnapshot(page_available=False)
            return _result(
                account_id,
                ReadinessState.PAGE_UNAVAILABLE,
                f"目标页面打开失败：{exc}",
                snapshot,
                monitor.evidence,
            )

        async def reader() -> ReadinessResult:
            await monitor.drain()
            snapshot = await read_frontend_snapshot(page)
            return evaluate_readiness(account_id, snapshot, monitor.evidence)

        return await wait_for_stable_readiness(
            reader,
            timeout=timeout,
            stable_samples=stable_samples,
            sample_interval=2,
        )
    finally:
        page.remove_listener("response", monitor.on_response)
        await monitor.drain()
