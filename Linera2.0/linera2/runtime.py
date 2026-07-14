from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Callable, Iterable

from playwright.async_api import Playwright, async_playwright

from .auto_runtime import run_auto_session
from .auto_session import AutoSessionStore
from .hubstudio import HubstudioReadOnlyClient
from .readiness import ReadinessResult, ReadinessState, check_account_ready
from .store import ReadinessStore
from .wallet_recovery import ensure_wallet_connected


LogFunction = Callable[[str, str], None]


def browser_unreachable_result(account_id: str, reason: str) -> ReadinessResult:
    return ReadinessResult(
        account_id=account_id,
        ready=False,
        state=ReadinessState.BROWSER_UNREACHABLE.value,
        reason=f"无法连接 HubStudio 浏览器：{reason}",
        wallet_address_masked=None,
        coins=None,
        wallet_connected=False,
        backend_ok=False,
        ride_ui_ready=False,
        checked_at=datetime.now().astimezone().isoformat(timespec="seconds"),
    )


def _account_identity(account) -> tuple[str, str]:
    account_id = str(getattr(account, "id", account))
    display_name = str(getattr(account, "ua", "") or account_id)
    return account_id, display_name


def _default_log(account_id: str, message: str) -> None:
    print(f"[{account_id}] {message}", flush=True)


async def scan_one_account(
    pw: Playwright,
    account,
    *,
    hub: HubstudioReadOnlyClient,
    store: ReadinessStore,
    log_func: LogFunction = _default_log,
    timeout: int = 60,
    auto_session_store: AutoSessionStore | None = None,
    run_auto: bool = False,
    auto_timeout: int = 1_200,
    target_override: int | None = None,
) -> ReadinessResult:
    account_id, display_name = _account_identity(account)
    log_func(display_name, "开始检测账号就绪状态")
    cdp_address = await asyncio.to_thread(hub.start_browser, account_id)
    if not cdp_address:
        result = browser_unreachable_result(account_id, hub.last_error or "未取得调试端口")
        store.update(result)
        log_func(display_name, f"{result.state}: {result.reason}")
        return result

    try:
        browser = await pw.chromium.connect_over_cdp(
            f"http://{cdp_address}",
            timeout=20_000,
        )
    except Exception as exc:
        result = browser_unreachable_result(account_id, str(exc))
        store.update(result)
        log_func(display_name, f"{result.state}: {result.reason}")
        return result

    if not browser.contexts:
        result = browser_unreachable_result(account_id, "浏览器没有可用上下文")
        store.update(result)
        log_func(display_name, f"{result.state}: {result.reason}")
        return result

    context = browser.contexts[0]
    pages = [page for page in context.pages if not (page.url or "").startswith("chrome-extension://")]
    if not pages:
        result = browser_unreachable_result(account_id, "浏览器没有可用页面")
        store.update(result)
        log_func(display_name, f"{result.state}: {result.reason}")
        return result

    page = next(
        (item for item in pages if (item.url or "").startswith("https://app.linera.xyz/")),
        pages[0],
    )
    result = await check_account_ready(page, context, account_id, timeout=timeout)
    if run_auto and result.state == ReadinessState.WALLET_DISCONNECTED.value:
        recovery = await ensure_wallet_connected(
            page,
            context,
            account_id,
            log_func=log_func,
        )
        log_func(display_name, f"钱包恢复：{recovery.reason}")
        result = await check_account_ready(page, context, account_id, timeout=timeout)

    store.update(result)
    log_func(
        display_name,
        f"{result.state}: coins={result.coins if result.coins is not None else '-'}; {result.reason}",
    )
    if run_auto and auto_session_store is not None and result.ready:
        await run_auto_session(
            page,
            context,
            account_id,
            store=auto_session_store,
            readiness=result,
            timeout=auto_timeout,
            target_override=target_override,
            log_func=log_func,
        )
    return result


async def scan_accounts(
    accounts: Iterable,
    *,
    max_workers: int,
    store: ReadinessStore,
    hub_factory: Callable[[], HubstudioReadOnlyClient] = HubstudioReadOnlyClient,
    log_func: LogFunction = _default_log,
    timeout: int = 60,
    auto_session_store: AutoSessionStore | None = None,
    run_auto: bool = False,
    auto_timeout: int = 1_200,
    target_override: int | None = None,
) -> list[ReadinessResult]:
    semaphore = asyncio.Semaphore(max(1, max_workers))

    async with async_playwright() as pw:
        async def run(account) -> ReadinessResult:
            async with semaphore:
                return await scan_one_account(
                    pw,
                    account,
                    hub=hub_factory(),
                    store=store,
                    log_func=log_func,
                    timeout=timeout,
                    auto_session_store=auto_session_store,
                    run_auto=run_auto,
                    auto_timeout=auto_timeout,
                    target_override=target_override,
                )

        return await asyncio.gather(*(run(account) for account in accounts))
