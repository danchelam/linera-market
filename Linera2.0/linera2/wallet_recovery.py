from __future__ import annotations

import asyncio
import hashlib
import re
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Callable

from playwright.async_api import BrowserContext, Page

from .readiness import read_frontend_snapshot
from .wallet_support import OKX_EXTENSION_ID, click_wallet_button, unlock_okx_wallet


LogFunction = Callable[[str, str], None]
SIGNING_EXISTING_POPUP_WAIT_SECONDS = 15
SIGNING_TOTAL_WAIT_SECONDS = 60
POPUP_STATE_CHANGE_WAIT_SECONDS = 30


@dataclass(frozen=True)
class WalletRecoveryResult:
    recovered: bool
    reason: str


@dataclass(frozen=True)
class AutoSignResult:
    enabled: bool
    reason: str


def _load_wallet_helpers() -> SimpleNamespace:
    return SimpleNamespace(
        unlock=unlock_okx_wallet,
        confirm=click_wallet_button,
        extension_id=OKX_EXTENSION_ID,
    )


def _emit(log_func: LogFunction | None, account_id: str, message: str) -> None:
    if log_func is not None:
        log_func(account_id, message)


def _is_connected(snapshot) -> bool:
    return bool(snapshot.wallet_connected and snapshot.wallet_address)


def _remaining(deadline: float) -> float:
    return max(0.0, deadline - asyncio.get_running_loop().time())


async def _close_stale_dynamic_modal(page: Page) -> bool:
    return bool(
        await page.evaluate(
            """() => {
                const roots = [document];
                for (let i = 0; i < roots.length; i += 1) {
                    for (const element of roots[i].querySelectorAll('*')) {
                        if (element.shadowRoot) roots.push(element.shadowRoot);
                    }
                }
                for (const root of roots) {
                    const dialogs = root.querySelectorAll(
                        '[role="dialog"], [data-testid*="modal" i], [class*="modal" i]'
                    );
                    for (const dialog of dialogs) {
                        const text = (dialog.innerText || dialog.textContent || '').trim();
                        if (!/OKX Wallet|Log in or sign up|Select your wallet/i.test(text)) continue;
                        const close = dialog.querySelector(
                            'button[aria-label*="close" i], button[data-testid*="close" i], '
                            + 'button[class*="close" i]'
                        );
                        if (close) {
                            close.click();
                            return true;
                        }
                    }
                }
                return false;
            }"""
        )
    )


async def _dismiss_linera_onboarding(page: Page) -> bool:
    welcome = page.get_by_text("Welcome to Linera", exact=True)
    if await welcome.count() == 0 or not await welcome.first.is_visible():
        return False
    skip = page.get_by_role("button", name="Skip", exact=True)
    if await skip.count() == 0 or not await skip.first.is_visible():
        return False
    await skip.first.click(timeout=5_000)
    return True


async def _dismiss_linera_overlays(page: Page) -> bool:
    dismissed = await _dismiss_linera_onboarding(page)
    follow = page.get_by_role("button", name="Maybe later", exact=True)
    if await follow.count() > 0 and await follow.first.is_visible():
        await follow.first.click(timeout=5_000)
        dismissed = True
    return dismissed


async def _click_connect(page: Page) -> bool:
    locator = page.get_by_role("button", name=re.compile(r"^\s*Connect\s*$", re.I))
    if await locator.count() == 0:
        return False
    button = locator.first
    if not await button.is_visible():
        return False
    await button.click(timeout=5_000)
    return True


async def _wait_for_connect_click(page: Page, deadline: float) -> bool:
    stage_deadline = min(deadline, asyncio.get_running_loop().time() + 10)
    while _remaining(stage_deadline) > 0:
        try:
            await _dismiss_linera_overlays(page)
        except Exception:
            pass
        try:
            if await _click_connect(page):
                return True
        except Exception:
            pass
        await asyncio.sleep(min(0.25, _remaining(stage_deadline)))
    return False


async def _click_pending_signing(page: Page) -> bool:
    locator = page.get_by_role(
        "button",
        name=re.compile(
            r"^\s*(?:Signing(?:…|\.\.\.)?|Still\s+signing\s*·\s*retry|"
            r"Connect\s+failed\s*·\s*retry)\s*$",
            re.I,
        ),
    )
    if await locator.count() == 0:
        return False
    button = locator.first
    if not await button.is_visible():
        return False
    bounding_box = getattr(button, "bounding_box", None)
    if callable(bounding_box):
        box = await bounding_box()
        if box:
            await page.mouse.click(
                float(box["x"]) + float(box["width"]) / 2,
                float(box["y"]) + float(box["height"]) / 2,
            )
            return True
    await button.click(timeout=5_000)
    return True


async def _read_okx_tile_center(page: Page) -> tuple[float, float] | None:
    bounds = await page.evaluate(
        """() => {
            const roots = [document];
            for (let i = 0; i < roots.length; i += 1) {
                for (const element of roots[i].querySelectorAll('*')) {
                    if (element.shadowRoot) roots.push(element.shadowRoot);
                }
            }
            for (const root of roots) {
                for (const tile of root.querySelectorAll('[data-testid="ListTile"]')) {
                    const text = (tile.innerText || tile.textContent || '').trim();
                    if (!/OKX Wallet/i.test(text)) continue;
                    const rect = tile.getBoundingClientRect();
                    const style = getComputedStyle(tile);
                    if (rect.width <= 0 || rect.height <= 0
                            || style.visibility === 'hidden' || style.display === 'none') continue;
                    return {x: rect.x, y: rect.y, width: rect.width, height: rect.height};
                }
            }
            return null;
        }"""
    )
    if not bounds:
        return None
    return (
        float(bounds["x"]) + float(bounds["width"]) / 2,
        float(bounds["y"]) + float(bounds["height"]) / 2,
    )


async def _wait_for_okx_tile_center(page: Page, deadline: float) -> tuple[float, float] | None:
    while _remaining(deadline) > 0:
        try:
            center = await _read_okx_tile_center(page)
            if center is not None:
                return center
        except Exception:
            pass
        await asyncio.sleep(min(0.25, _remaining(deadline)))
    return None


async def _read_network_update_center(page: Page) -> tuple[float, float] | None:
    bounds = await page.evaluate(
        """() => {
            const roots = [document];
            for (let i = 0; i < roots.length; i += 1) {
                for (const element of roots[i].querySelectorAll('*')) {
                    if (element.shadowRoot) roots.push(element.shadowRoot);
                }
            }
            for (const root of roots) {
                const button = root.querySelector('[data-testid="SelectNetworkButton"]');
                if (!button) continue;
                const rect = button.getBoundingClientRect();
                if (rect.width <= 0 || rect.height <= 0) continue;
                return {x: rect.x, y: rect.y, width: rect.width, height: rect.height};
            }
            return null;
        }"""
    )
    if not bounds:
        return None
    return (
        float(bounds["x"]) + float(bounds["width"]) / 2,
        float(bounds["y"]) + float(bounds["height"]) / 2,
    )


async def _wait_for_network_update_center(
    page: Page,
    deadline: float,
) -> tuple[float, float] | None:
    while _remaining(deadline) > 0:
        try:
            center = await _read_network_update_center(page)
            if center is not None:
                return center
        except Exception:
            pass
        await asyncio.sleep(min(0.25, _remaining(deadline)))
    return None


def _is_okx_notification(page, extension_id: str) -> bool:
    try:
        is_closed = getattr(page, "is_closed", None)
        if callable(is_closed) and is_closed():
            return False
        url = page.url or ""
    except Exception:
        return False
    prefix = f"chrome-extension://{extension_id}/notification.html"
    return url.startswith(prefix)


async def _has_linera_popup_semantics(page) -> bool:
    frames = getattr(page, "frames", None)
    if frames is None:
        return True
    for frame in frames:
        try:
            text = await frame.locator("body").inner_text(timeout=750)
            if "app.linera.xyz" in text.lower():
                return True
        except Exception:
            continue
    return False


async def _wait_for_wallet_popup(
    context: BrowserContext,
    observed_pages: list,
    extension_id: str,
    deadline: float,
    *,
    excluded_page_ids: set[int] | None = None,
    require_linera_semantics: bool = False,
):
    excluded = excluded_page_ids or set()
    while _remaining(deadline) > 0:
        candidates = list(reversed(observed_pages)) + list(reversed(context.pages))
        seen: set[int] = set()
        for candidate in candidates:
            marker = id(candidate)
            if marker in seen or marker in excluded:
                continue
            seen.add(marker)
            if not _is_okx_notification(candidate, extension_id):
                continue
            has_linera_semantics = (
                await _has_linera_popup_semantics(candidate)
                if require_linera_semantics
                else False
            )
            if require_linera_semantics and not has_linera_semantics:
                continue
            return candidate
        await asyncio.sleep(min(0.2, _remaining(deadline)))
    return None


def _notification_page_ids(context: BrowserContext, extension_id: str) -> set[int]:
    return {
        id(page)
        for page in context.pages
        if _is_okx_notification(page, extension_id)
    }


async def _close_stale_wallet_notifications(
    context: BrowserContext,
    extension_id: str,
) -> None:
    for candidate in list(context.pages):
        if not _is_okx_notification(candidate, extension_id):
            continue
        try:
            await candidate.close()
        except Exception:
            pass


async def _open_network_update_confirmation(
    page: Page,
    context: BrowserContext,
    observed_pages: list,
    extension_id: str,
    deadline: float,
    center: tuple[float, float],
):
    await _close_stale_wallet_notifications(context, extension_id)
    existing_notifications = _notification_page_ids(context, extension_id)
    observed_pages.clear()
    await page.mouse.click(*center)
    popup = await _wait_for_wallet_popup(
        context,
        observed_pages,
        extension_id,
        deadline,
        excluded_page_ids=existing_notifications,
        require_linera_semantics=True,
    )
    if popup is None:
        return None, "未检测到 OKX 网络更新确认窗口"
    return popup, ""


async def _wait_for_popup_or_network_update(
    page: Page,
    context: BrowserContext,
    observed_pages: list,
    extension_id: str,
    deadline: float,
    *,
    excluded_page_ids: set[int],
):
    while _remaining(deadline) > 0:
        popup_deadline = min(deadline, asyncio.get_running_loop().time() + 0.5)
        popup = await _wait_for_wallet_popup(
            context,
            observed_pages,
            extension_id,
            popup_deadline,
            excluded_page_ids=excluded_page_ids,
            require_linera_semantics=True,
        )
        if popup is not None:
            return popup, None
        try:
            network_center = await _read_network_update_center(page)
        except Exception:
            network_center = None
        if network_center is not None:
            return None, network_center
        await asyncio.sleep(min(0.1, _remaining(deadline)))
    return None, None


async def _open_wallet_confirmation(
    page: Page,
    context: BrowserContext,
    observed_pages: list,
    extension_id: str,
    deadline: float,
):
    observed_pages.clear()
    await _dismiss_linera_overlays(page)
    try:
        network_center = await _read_network_update_center(page)
    except Exception:
        network_center = None
    if network_center is not None:
        return await _open_network_update_confirmation(
            page,
            context,
            observed_pages,
            extension_id,
            deadline,
            network_center,
        )
    await _close_stale_wallet_notifications(context, extension_id)
    if await _click_pending_signing(page):
        signing_deadline = max(
            deadline,
            asyncio.get_running_loop().time() + SIGNING_TOTAL_WAIT_SECONDS,
        )
        popup = await _wait_for_wallet_popup(
            context,
            observed_pages,
            extension_id,
            signing_deadline,
            require_linera_semantics=True,
        )
        if popup is None:
            return None, "Signing 状态未恢复 OKX 确认窗口"
        return popup, ""

    if not await _wait_for_connect_click(page, deadline):
        return None, "未找到可用的 Connect 按钮"
    center = await _wait_for_okx_tile_center(page, deadline)
    if center is None:
        if await _click_pending_signing(page):
            signing_deadline = max(
                deadline,
                asyncio.get_running_loop().time() + SIGNING_TOTAL_WAIT_SECONDS,
            )
            popup = await _wait_for_wallet_popup(
                context,
                observed_pages,
                extension_id,
                signing_deadline,
                require_linera_semantics=True,
            )
            if popup is None:
                return None, "Signing 状态未恢复 OKX 确认窗口"
            return popup, ""
        return None, "Dynamic 弹窗中未找到 OKX Wallet"
    await _close_stale_wallet_notifications(context, extension_id)
    existing_notifications = _notification_page_ids(context, extension_id)
    await page.mouse.click(*center)
    popup, network_center = await _wait_for_popup_or_network_update(
        page,
        context,
        observed_pages,
        extension_id,
        deadline,
        excluded_page_ids=existing_notifications,
    )
    if network_center is not None:
        return await _open_network_update_confirmation(
            page,
            context,
            observed_pages,
            extension_id,
            deadline,
            network_center,
        )
    if popup is None:
        return None, "未检测到 OKX 钱包确认窗口"
    return popup, ""


def _popup_is_closed(popup) -> bool:
    try:
        is_closed = getattr(popup, "is_closed", None)
        if not callable(is_closed):
            return False
        return bool(is_closed())
    except Exception:
        return True


async def _popup_state_marker(popup) -> str | None:
    frames = getattr(popup, "frames", None)
    if frames is None:
        return None
    parts: list[str] = []
    for frame in frames:
        try:
            parts.append(await frame.locator("body").inner_text(timeout=1_000))
        except Exception:
            continue
    if not parts:
        return None
    return hashlib.sha256("\n".join(parts).encode("utf-8", errors="ignore")).hexdigest()


async def _wait_for_popup_state_change(popup, before: str, deadline: float) -> bool:
    stage_deadline = min(
        deadline,
        asyncio.get_running_loop().time() + POPUP_STATE_CHANGE_WAIT_SECONDS,
    )
    while _remaining(stage_deadline) > 0:
        if _popup_is_closed(popup):
            return True
        marker = await _popup_state_marker(popup)
        if marker is not None and marker != before:
            return True
        await asyncio.sleep(min(0.25, _remaining(stage_deadline)))
    return False


async def _confirm_wallet_steps(
    confirm_func,
    popup,
    account_id: str,
    deadline: float,
    *,
    max_steps: int = 8,
    log_func: LogFunction | None = None,
) -> bool:
    any_clicked = False
    stagnant_retry_available = True
    for _ in range(max_steps):
        if _popup_is_closed(popup):
            return any_clicked
        before = await _popup_state_marker(popup)
        if log_func is None:
            confirmation = confirm_func(popup, account_id, max_rounds=1)
        else:
            confirmation = confirm_func(
                popup,
                account_id,
                max_rounds=1,
                log_func=log_func,
            )
        clicked = await asyncio.wait_for(
            confirmation,
            timeout=max(0.1, _remaining(deadline)),
        )
        if not clicked:
            return False
        any_clicked = True
        if _popup_is_closed(popup):
            return True
        if before is None:
            return True
        if not await _wait_for_popup_state_change(popup, before, deadline):
            if stagnant_retry_available:
                stagnant_retry_available = False
                continue
            return False
        stagnant_retry_available = True
    return _popup_is_closed(popup)


async def _wait_for_connected_snapshot(page: Page, deadline: float) -> bool:
    while _remaining(deadline) > 0:
        try:
            if _is_connected(await read_frontend_snapshot(page)):
                return True
        except Exception:
            pass
        await asyncio.sleep(min(0.5, _remaining(deadline)))
    return False


async def _wait_for_auto_sign_enabled(switch, deadline: float) -> bool:
    while _remaining(deadline) > 0:
        try:
            if await switch.is_checked():
                return True
        except Exception:
            pass
        await asyncio.sleep(min(0.25, _remaining(deadline)))
    return False


async def _find_auto_sign_switch(page: Page, deadline: float):
    label = page.get_by_text("Auto-sign trades", exact=True)

    async def visible_label() -> bool:
        try:
            return await label.count() > 0 and await label.first.is_visible()
        except Exception:
            return False

    if not await visible_label():
        menu = page.get_by_role("button", name="Menu")
        try:
            if await menu.count() == 0 or not await menu.first.is_visible():
                return None
            await menu.first.click(timeout=5_000)
        except Exception:
            return None

    while _remaining(deadline) > 0:
        if await visible_label():
            # The live control points aria-labelledby at a generated id whose
            # label is not exposed as an accessible name in Chromium 133.
            # The wallet menu currently contains exactly one switch.
            switch = page.get_by_role("switch")
            try:
                if await switch.count() > 0 and await switch.first.is_visible():
                    return switch.first
            except Exception:
                pass
        await asyncio.sleep(min(0.25, _remaining(deadline)))
    return None


async def _finish_auto_sign(
    page: Page,
    enabled: bool,
    reason: str,
) -> AutoSignResult:
    close_error = ""
    try:
        close = page.get_by_role("button", name="Close menu")
        label = page.get_by_text("Auto-sign trades", exact=True)
        close_visible = await close.count() > 0 and await close.first.is_visible()
        label_visible = await label.count() > 0 and await label.first.is_visible()
        if close_visible:
            await close.first.click(timeout=5_000)
            close_deadline = asyncio.get_running_loop().time() + 2
            while _remaining(close_deadline) > 0:
                if not await close.first.is_visible():
                    break
                await asyncio.sleep(min(0.1, _remaining(close_deadline)))
            if await close.first.is_visible():
                close_error = "钱包菜单点击关闭后仍然可见"
        elif label_visible:
            close_error = "钱包菜单可见但没有可用的关闭按钮"
    except Exception as exc:
        close_error = f"关闭钱包菜单失败：{type(exc).__name__}"
    if close_error:
        return AutoSignResult(False, f"{reason}；{close_error}")
    return AutoSignResult(enabled, reason)


async def ensure_auto_sign_enabled(
    page: Page,
    context: BrowserContext,
    account_id: str,
    *,
    timeout: int = 90,
    log_func: LogFunction | None = None,
) -> AutoSignResult:
    """Enable Linera delegated signing through the visible wallet menu."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.1, float(timeout))
    switch = await _find_auto_sign_switch(page, deadline)
    if switch is None:
        return await _finish_auto_sign(
            page,
            False,
            "钱包菜单中未找到 Auto-sign trades",
        )

    try:
        if await switch.is_checked():
            return await _finish_auto_sign(page, True, "Auto-sign 已经开启")
        if await switch.is_disabled():
            return await _finish_auto_sign(page, False, "Auto-sign 开关当前不可用")
    except Exception as exc:
        return await _finish_auto_sign(
            page,
            False,
            f"读取 Auto-sign 状态失败：{type(exc).__name__}",
        )

    try:
        helpers = _load_wallet_helpers()
    except Exception as exc:
        return await _finish_auto_sign(
            page,
            False,
            f"加载 OKX 钱包能力失败：{type(exc).__name__}",
        )

    try:
        original_url = page.url if (page.url or "").startswith("https://app.linera.xyz/") else None
    except Exception:
        original_url = None
    try:
        unlock_result = await asyncio.wait_for(
            helpers.unlock(context, account_id, log_func=log_func),
            timeout=max(0.1, _remaining(deadline)),
        )
    except asyncio.TimeoutError:
        return await _finish_auto_sign(page, False, "开启 Auto-sign 前解锁 OKX 超时")
    except Exception as exc:
        return await _finish_auto_sign(
            page,
            False,
            f"开启 Auto-sign 前解锁 OKX 失败：{type(exc).__name__}",
        )
    if unlock_result is not True and unlock_result != "NEED_DAPP":
        return await _finish_auto_sign(page, False, "开启 Auto-sign 前解锁 OKX 失败")

    try:
        current_url = (page.url or "") if original_url else ""
        if original_url and not current_url.startswith("https://app.linera.xyz/"):
            await page.goto(
                original_url,
                wait_until="domcontentloaded",
                timeout=max(100, min(30_000, int(_remaining(deadline) * 1_000))),
            )
    except Exception as exc:
        return await _finish_auto_sign(
            page,
            False,
            f"解锁后返回 Linera 页面失败：{type(exc).__name__}",
        )

    switch = await _find_auto_sign_switch(page, deadline)
    if switch is None:
        return await _finish_auto_sign(
            page,
            False,
            "OKX 解锁后未找到 Auto-sign trades",
        )
    try:
        if await switch.is_checked():
            return await _finish_auto_sign(page, True, "Auto-sign 已经开启")
    except Exception as exc:
        return await _finish_auto_sign(
            page,
            False,
            f"OKX 解锁后读取 Auto-sign 失败：{type(exc).__name__}",
        )

    observed_pages: list = []
    listener_registered = False

    def observe_popup(new_page) -> None:
        observed_pages.append(new_page)

    try:
        context.on("page", observe_popup)
        listener_registered = True
        await _close_stale_wallet_notifications(context, helpers.extension_id)
        existing_notifications = _notification_page_ids(
            context,
            helpers.extension_id,
        )
        await switch.click(timeout=5_000)
        _emit(log_func, account_id, "已请求开启 Auto-sign，等待 OKX 授权")

        popup_deadline = min(deadline, loop.time() + 15)
        popup = await _wait_for_wallet_popup(
            context,
            observed_pages,
            helpers.extension_id,
            popup_deadline,
            excluded_page_ids=existing_notifications,
            require_linera_semantics=True,
        )
        if popup is None:
            immediate_deadline = min(deadline, loop.time() + 3)
            if await _wait_for_auto_sign_enabled(switch, immediate_deadline):
                return await _finish_auto_sign(page, True, "Auto-sign 已直接开启")
            return await _finish_auto_sign(
                page,
                False,
                "未检测到 Auto-sign 的 OKX 确认窗口",
            )

        try:
            confirmed = await asyncio.wait_for(
                _confirm_wallet_steps(
                    helpers.confirm,
                    popup,
                    account_id,
                    deadline,
                    log_func=log_func,
                ),
                timeout=max(0.1, _remaining(deadline)),
            )
        except asyncio.TimeoutError:
            return await _finish_auto_sign(page, False, "Auto-sign 的 OKX 授权超时")
        except Exception as exc:
            return await _finish_auto_sign(
                page,
                False,
                f"Auto-sign 的 OKX 授权失败：{type(exc).__name__}",
            )

        if not confirmed:
            verify_deadline = min(deadline, loop.time() + 3)
            if not await _wait_for_auto_sign_enabled(switch, verify_deadline):
                return await _finish_auto_sign(
                    page,
                    False,
                    "Auto-sign 的 OKX 授权未完成",
                )
            return await _finish_auto_sign(page, True, "Auto-sign 已开启")

        if not await _wait_for_auto_sign_enabled(switch, deadline):
            return await _finish_auto_sign(
                page,
                False,
                "OKX 已确认，但 Auto-sign 开关未生效",
            )
        return await _finish_auto_sign(page, True, "Auto-sign 已开启")
    except Exception as exc:
        return await _finish_auto_sign(
            page,
            False,
            f"开启 Auto-sign 失败：{type(exc).__name__}",
        )
    finally:
        if listener_registered:
            try:
                context.remove_listener("page", observe_popup)
            except Exception:
                pass


async def ensure_wallet_connected(
    page: Page,
    context: BrowserContext,
    account_id: str,
    *,
    timeout: int = 90,
    log_func: LogFunction | None = None,
) -> WalletRecoveryResult:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + max(0.1, float(timeout))
    try:
        original_url = page.url if (page.url or "").startswith("https://app.linera.xyz/") else None
    except Exception:
        original_url = None

    try:
        if _is_connected(await read_frontend_snapshot(page)):
            return WalletRecoveryResult(True, "钱包已经连接")
    except Exception as exc:
        return WalletRecoveryResult(False, f"读取钱包状态失败：{type(exc).__name__}")

    try:
        helpers = _load_wallet_helpers()
        unlock_result = await asyncio.wait_for(
            helpers.unlock(context, account_id, log_func=log_func),
            timeout=max(0.1, _remaining(deadline)),
        )
    except asyncio.TimeoutError:
        return WalletRecoveryResult(False, "OKX 钱包解锁超时")
    except Exception as exc:
        return WalletRecoveryResult(False, f"OKX 钱包解锁失败：{type(exc).__name__}")

    if unlock_result is not True and unlock_result != "NEED_DAPP":
        return WalletRecoveryResult(False, "OKX 钱包解锁失败")

    try:
        current_url = page.url or ""
        if original_url and not current_url.startswith("https://app.linera.xyz/"):
            await page.goto(
                original_url,
                wait_until="domcontentloaded",
                timeout=max(100, min(30_000, int(_remaining(deadline) * 1_000))),
            )
    except Exception as exc:
        return WalletRecoveryResult(False, f"返回 Linera 页面失败：{type(exc).__name__}")

    try:
        if _is_connected(await read_frontend_snapshot(page)):
            return WalletRecoveryResult(True, "钱包解锁后已恢复 Linera 连接")
    except Exception:
        pass

    _emit(log_func, account_id, "OKX 钱包已解锁，准备连接 Linera")
    try:
        await _dismiss_linera_overlays(page)
        await _close_stale_dynamic_modal(page)
    except Exception as exc:
        return WalletRecoveryResult(False, f"打开 Connect 弹窗失败：{type(exc).__name__}")

    observed_pages: list = []
    listener_registered = False

    def observe_popup(new_page) -> None:
        observed_pages.append(new_page)

    try:
        context.on("page", observe_popup)
        listener_registered = True
        popup, open_error = await _open_wallet_confirmation(
            page,
            context,
            observed_pages,
            helpers.extension_id,
            deadline,
        )
        if (
            popup is None
            and open_error == "未找到可用的 Connect 按钮"
            and original_url
        ):
            try:
                await page.goto(
                    original_url,
                    wait_until="domcontentloaded",
                    timeout=max(100, min(30_000, int(_remaining(deadline) * 1_000))),
                )
                await _dismiss_linera_overlays(page)
                await _close_stale_dynamic_modal(page)
                popup, retry_error = await _open_wallet_confirmation(
                    page,
                    context,
                    observed_pages,
                    helpers.extension_id,
                    deadline,
                )
                if popup is None:
                    open_error = f"重载 Ride 页面后：{retry_error}"
            except Exception as exc:
                open_error = f"重载 Ride 页面失败：{type(exc).__name__}"
        if popup is None:
            return WalletRecoveryResult(False, open_error)

        deadline = max(
            deadline,
            loop.time() + SIGNING_TOTAL_WAIT_SECONDS,
        )
        try:
            confirmed = await asyncio.wait_for(
                _confirm_wallet_steps(
                    helpers.confirm,
                    popup,
                    account_id,
                    deadline,
                    log_func=log_func,
                ),
                timeout=max(0.1, _remaining(deadline)),
            )
        except asyncio.TimeoutError:
            return WalletRecoveryResult(False, "OKX 钱包确认超时")
        except Exception as exc:
            return WalletRecoveryResult(False, f"OKX 钱包确认失败：{type(exc).__name__}")

        popup_url = ""
        try:
            popup_url = popup.url or ""
        except Exception:
            pass
        if not confirmed:
            try:
                if _is_connected(await read_frontend_snapshot(page)):
                    return WalletRecoveryResult(
                        True,
                        "Linera 已显示钱包连接，忽略未关闭的确认窗口",
                    )
            except Exception:
                pass
        if not confirmed and "#/unlock" in popup_url:
            try:
                if not popup.is_closed():
                    await popup.close()
            except Exception:
                pass
            try:
                retry_unlock = await asyncio.wait_for(
                    helpers.unlock(context, account_id, log_func=log_func),
                    timeout=max(0.1, _remaining(deadline)),
                )
            except asyncio.TimeoutError:
                return WalletRecoveryResult(False, "OKX 二次解锁超时")
            except Exception as exc:
                return WalletRecoveryResult(False, f"OKX 二次解锁失败：{type(exc).__name__}")
            if retry_unlock is not True and retry_unlock != "NEED_DAPP":
                return WalletRecoveryResult(False, "OKX 二次解锁失败")

            try:
                current_url = page.url or ""
                if original_url and not current_url.startswith("https://app.linera.xyz/"):
                    await page.goto(
                        original_url,
                        wait_until="domcontentloaded",
                        timeout=max(100, min(30_000, int(_remaining(deadline) * 1_000))),
                    )
                await _close_stale_dynamic_modal(page)
                popup, retry_error = await _open_wallet_confirmation(
                    page,
                    context,
                    observed_pages,
                    helpers.extension_id,
                    deadline,
                )
                if popup is None:
                    return WalletRecoveryResult(False, f"二次解锁后：{retry_error}")
                confirmed = await asyncio.wait_for(
                    _confirm_wallet_steps(
                        helpers.confirm,
                        popup,
                        account_id,
                        deadline,
                        log_func=log_func,
                    ),
                    timeout=max(0.1, _remaining(deadline)),
                )
            except asyncio.TimeoutError:
                return WalletRecoveryResult(False, "二次解锁后的钱包确认超时")
            except Exception as exc:
                return WalletRecoveryResult(False, f"二次解锁后的连接失败：{type(exc).__name__}")
        if not confirmed:
            return WalletRecoveryResult(False, "OKX 钱包确认未完成")

        initial_connection_deadline = min(deadline, loop.time() + 5)
        if await _wait_for_connected_snapshot(page, initial_connection_deadline):
            return WalletRecoveryResult(True, "钱包已解锁并连接 Linera")

        network_deadline = min(deadline, loop.time() + 5)
        network_center = await _wait_for_network_update_center(page, network_deadline)
        if network_center is not None:
            existing_notifications = _notification_page_ids(
                context,
                helpers.extension_id,
            )
            observed_pages.clear()
            await page.mouse.click(*network_center)
            network_popup = await _wait_for_wallet_popup(
                context,
                observed_pages,
                helpers.extension_id,
                deadline,
                excluded_page_ids=existing_notifications,
            )
            if network_popup is None:
                return WalletRecoveryResult(False, "未检测到 OKX 网络更新确认窗口")
            try:
                network_confirmed = await asyncio.wait_for(
                    _confirm_wallet_steps(
                        helpers.confirm,
                        network_popup,
                        account_id,
                        deadline,
                        log_func=log_func,
                    ),
                    timeout=max(0.1, _remaining(deadline)),
                )
            except asyncio.TimeoutError:
                return WalletRecoveryResult(False, "OKX 网络更新确认超时")
            except Exception as exc:
                return WalletRecoveryResult(False, f"OKX 网络更新失败：{type(exc).__name__}")
            if not network_confirmed:
                return WalletRecoveryResult(False, "OKX 网络更新未完成")

        final_connection_deadline = min(deadline, loop.time() + 5)
        if await _wait_for_connected_snapshot(page, final_connection_deadline):
            return WalletRecoveryResult(True, "钱包已解锁并连接 Linera")

        signing_deadline = max(
            deadline,
            loop.time() + SIGNING_TOTAL_WAIT_SECONDS,
        )
        existing_popup_deadline = min(
            signing_deadline,
            loop.time() + SIGNING_EXISTING_POPUP_WAIT_SECONDS,
        )
        signing_popup = await _wait_for_wallet_popup(
            context,
            observed_pages,
            helpers.extension_id,
            existing_popup_deadline,
            require_linera_semantics=True,
        )
        retry_clicked = False
        if signing_popup is None:
            observed_pages.clear()
            retry_clicked = await _click_pending_signing(page)
            if retry_clicked:
                signing_popup = await _wait_for_wallet_popup(
                    context,
                    observed_pages,
                    helpers.extension_id,
                    signing_deadline,
                    require_linera_semantics=True,
                )

        if retry_clicked and signing_popup is None:
            return WalletRecoveryResult(False, "Signing 重试未恢复 OKX 确认窗口")

        if signing_popup is not None:
            try:
                await asyncio.wait_for(
                    _confirm_wallet_steps(
                        helpers.confirm,
                        signing_popup,
                        account_id,
                        signing_deadline,
                        log_func=log_func,
                    ),
                    timeout=max(0.1, _remaining(signing_deadline)),
                )
            except asyncio.TimeoutError:
                return WalletRecoveryResult(False, "Signing 重试确认超时")
            except Exception as exc:
                return WalletRecoveryResult(
                    False,
                    f"Signing 重试确认失败：{type(exc).__name__}",
                )
            if await _wait_for_connected_snapshot(page, signing_deadline):
                return WalletRecoveryResult(True, "钱包已解锁并连接 Linera")

        return WalletRecoveryResult(False, "钱包确认完成，但 Linera 未显示已连接")
    except Exception as exc:
        return WalletRecoveryResult(False, f"钱包连接流程失败：{type(exc).__name__}")
    finally:
        if listener_registered:
            try:
                context.remove_listener("page", observe_popup)
            except Exception:
                pass
