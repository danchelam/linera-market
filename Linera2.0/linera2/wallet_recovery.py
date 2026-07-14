from __future__ import annotations

import asyncio
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Callable

from playwright.async_api import BrowserContext, Page

from .readiness import read_frontend_snapshot


LogFunction = Callable[[str, str], None]


@dataclass(frozen=True)
class WalletRecoveryResult:
    recovered: bool
    reason: str


def _load_parent_wallet_helpers() -> SimpleNamespace:
    parent_root = Path(__file__).resolve().parents[2]
    if str(parent_root) not in sys.path:
        sys.path.insert(0, str(parent_root))

    from base_module import (  # pylint: disable=import-outside-toplevel
        OKX_EXTENSION_ID,
        _click_wallet_button,
        unlock_okx_wallet,
    )

    return SimpleNamespace(
        unlock=unlock_okx_wallet,
        confirm=_click_wallet_button,
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


async def _click_connect(page: Page) -> bool:
    locator = page.get_by_role("button", name=re.compile(r"^\s*Connect\s*$", re.I))
    if await locator.count() == 0:
        return False
    button = locator.first
    if not await button.is_visible():
        return False
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


def _is_okx_notification(page, extension_id: str) -> bool:
    try:
        url = page.url or ""
    except Exception:
        return False
    prefix = f"chrome-extension://{extension_id}/notification.html"
    return url.startswith(prefix)


async def _wait_for_wallet_popup(
    context: BrowserContext,
    observed_pages: list,
    extension_id: str,
    deadline: float,
):
    while _remaining(deadline) > 0:
        candidates = list(reversed(observed_pages)) + list(reversed(context.pages))
        seen: set[int] = set()
        for candidate in candidates:
            marker = id(candidate)
            if marker in seen:
                continue
            seen.add(marker)
            if _is_okx_notification(candidate, extension_id):
                return candidate
        await asyncio.sleep(min(0.2, _remaining(deadline)))
    return None


async def _wait_for_connected_snapshot(page: Page, deadline: float) -> bool:
    while _remaining(deadline) > 0:
        try:
            if _is_connected(await read_frontend_snapshot(page)):
                return True
        except Exception:
            pass
        await asyncio.sleep(min(0.5, _remaining(deadline)))
    return False


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
        if _is_connected(await read_frontend_snapshot(page)):
            return WalletRecoveryResult(True, "钱包已经连接")
    except Exception as exc:
        return WalletRecoveryResult(False, f"读取钱包状态失败：{type(exc).__name__}")

    try:
        helpers = _load_parent_wallet_helpers()
        unlock_result = await asyncio.wait_for(
            helpers.unlock(context, account_id),
            timeout=max(0.1, _remaining(deadline)),
        )
    except asyncio.TimeoutError:
        return WalletRecoveryResult(False, "OKX 钱包解锁超时")
    except Exception as exc:
        return WalletRecoveryResult(False, f"OKX 钱包解锁失败：{type(exc).__name__}")

    if unlock_result is not True and unlock_result != "NEED_DAPP":
        return WalletRecoveryResult(False, "OKX 钱包解锁失败")

    _emit(log_func, account_id, "OKX 钱包已解锁，准备连接 Linera")
    try:
        await _close_stale_dynamic_modal(page)
        if not await _click_connect(page):
            return WalletRecoveryResult(False, "未找到可用的 Connect 按钮")
    except Exception as exc:
        return WalletRecoveryResult(False, f"打开 Connect 弹窗失败：{type(exc).__name__}")

    observed_pages: list = []

    def observe_popup(new_page) -> None:
        observed_pages.append(new_page)

    context.on("page", observe_popup)
    try:
        center = await _wait_for_okx_tile_center(page, deadline)
        if center is None:
            return WalletRecoveryResult(False, "Dynamic 弹窗中未找到 OKX Wallet")

        await page.mouse.click(*center)
        popup = await _wait_for_wallet_popup(
            context,
            observed_pages,
            helpers.extension_id,
            deadline,
        )
        if popup is None:
            return WalletRecoveryResult(False, "未检测到 OKX 钱包确认窗口")

        try:
            confirmed = await asyncio.wait_for(
                helpers.confirm(popup, account_id, max_rounds=5),
                timeout=max(0.1, _remaining(deadline)),
            )
        except asyncio.TimeoutError:
            return WalletRecoveryResult(False, "OKX 钱包确认超时")
        except Exception as exc:
            return WalletRecoveryResult(False, f"OKX 钱包确认失败：{type(exc).__name__}")
        if not confirmed:
            return WalletRecoveryResult(False, "OKX 钱包确认未完成")

        if not await _wait_for_connected_snapshot(page, deadline):
            return WalletRecoveryResult(False, "钱包确认完成，但 Linera 未显示已连接")
        return WalletRecoveryResult(True, "钱包已解锁并连接 Linera")
    except Exception as exc:
        return WalletRecoveryResult(False, f"钱包连接流程失败：{type(exc).__name__}")
    finally:
        try:
            context.remove_listener("page", observe_popup)
        except Exception:
            pass
