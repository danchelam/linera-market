from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Callable

from playwright.async_api import BrowserContext, Page

from .local_config import get_wallet_password


LogFunction = Callable[[str, str], None]
OKX_EXTENSION_ID = "mcohilncbfahbmgdjkbpemcciiolgcge"
PROJECT_DIR = Path(__file__).resolve().parents[1]
WALLET_BUTTON_KEYWORDS = (
    "确认",
    "连接",
    "签名",
    "Confirm",
    "Connect",
    "Sign",
    "Approve",
)


def _emit(log_func, account_id: str, message: str) -> None:
    if log_func:
        log_func(account_id, message)


async def _find_and_fill_password(
    popup: Page,
    account_id: str,
    password: str,
    log_func: LogFunction | None = None,
) -> bool:
    for frame in popup.frames:
        try:
            locator = frame.locator('input[type="password"]')
            if await locator.count() > 0:
                await locator.first.fill(password)
                where = "iframe" if frame != popup.main_frame else "主文档"
                _emit(log_func, account_id, f"在{where}中找到密码框并填写")
                return True
        except Exception:
            continue

    try:
        filled = await popup.evaluate(
            """(password) => {
                function deep(root, selector) {
                    if (!root) return null;
                    try {
                        const match = root.querySelector(selector);
                        if (match) return match;
                    } catch (_) {}
                    try {
                        for (const element of root.querySelectorAll('*')) {
                            if (element.shadowRoot) {
                                const match = deep(element.shadowRoot, selector);
                                if (match) return match;
                            }
                        }
                    } catch (_) {}
                    return null;
                }
                const input = deep(document, 'input[type="password"]');
                if (!input) return false;
                input.focus();
                const setter = Object.getOwnPropertyDescriptor(
                    HTMLInputElement.prototype,
                    'value'
                ).set;
                setter.call(input, password);
                input.dispatchEvent(new Event('input', {bubbles: true}));
                input.dispatchEvent(new Event('change', {bubbles: true}));
                return true;
            }""",
            password,
        )
        if filled:
            _emit(log_func, account_id, "JS 递归找到密码框")
            return True
    except Exception:
        pass
    return False


async def _click_unlock_button(
    popup: Page,
    account_id: str,
    log_func: LogFunction | None = None,
) -> bool:
    clicked = False
    for frame in popup.frames:
        for text in ("解锁", "Unlock"):
            try:
                button = frame.locator(f'button:has-text("{text}")')
                if await button.count() > 0:
                    await button.first.click()
                    _emit(log_func, account_id, f"已点击 [{text}]")
                    clicked = True
                    break
            except Exception:
                continue
        if clicked:
            break
        for selector in ('button[type="submit"]', 'button[data-testid="okd-button"]'):
            try:
                button = frame.locator(selector)
                if await button.count() > 0:
                    await button.first.click()
                    _emit(log_func, account_id, f"已点击 [{selector}]")
                    clicked = True
                    break
            except Exception:
                continue
        if clicked:
            break

    if not clicked:
        for frame in popup.frames:
            try:
                clicked = bool(
                    await frame.evaluate(
                        """() => {
                            const keywords = ['解锁', 'Unlock'];
                            for (const button of document.querySelectorAll('button')) {
                                const text = (button.innerText || '').trim();
                                if (keywords.some(keyword => text.includes(keyword))) {
                                    button.click();
                                    return true;
                                }
                            }
                            const submit = document.querySelector('button[type="submit"]');
                            if (submit) {
                                submit.click();
                                return true;
                            }
                            return false;
                        }"""
                    )
                )
                if clicked:
                    _emit(log_func, account_id, "JS 点击解锁按钮成功")
                    break
            except Exception:
                continue

    if not clicked:
        try:
            await popup.keyboard.press("Enter")
        except Exception:
            pass

    failed_requests: list[str] = []
    pending_requests: list[str] = []

    def on_request(request) -> None:
        url = request.url or ""
        if not url.startswith("chrome-extension://"):
            pending_requests.append(url[:120])

    def on_response(response) -> None:
        url = response.url or ""
        if url in pending_requests:
            pending_requests.remove(url)

    def on_request_failed(request) -> None:
        url = request.url or ""
        if url.startswith("chrome-extension://"):
            return
        try:
            failure = request.failure or ""
        except Exception:
            failure = ""
        failed_requests.append(f"{url[:100]} | {failure}")
        shortened = url[:120]
        if shortened in pending_requests:
            pending_requests.remove(shortened)

    listeners_registered = False
    try:
        popup.on("request", on_request)
        popup.on("response", on_response)
        popup.on("requestfailed", on_request_failed)
        listeners_registered = True
    except Exception:
        pass

    try:
        for _ in range(15):
            await asyncio.sleep(1)
            try:
                current_url = popup.url or ""
                if popup.is_closed():
                    _emit(log_func, account_id, "解锁弹窗已关闭")
                    return True
            except Exception:
                _emit(log_func, account_id, "解锁弹窗已关闭（页面不可访问）")
                return True
            if "unlock" not in current_url:
                _emit(log_func, account_id, "解锁成功（URL 已变化）")
                return True

        if failed_requests or pending_requests:
            _emit(log_func, account_id, "解锁网络请求超时，停止本次解锁")
        else:
            _emit(log_func, account_id, "解锁超时（扩展内部未完成）")
        try:
            await popup.close()
        except Exception:
            pass
        return False
    finally:
        if listeners_registered:
            try:
                popup.remove_listener("request", on_request)
                popup.remove_listener("response", on_response)
                popup.remove_listener("requestfailed", on_request_failed)
            except Exception:
                pass


async def _unlock_with_provider_popup(
    context: BrowserContext,
    account_id: str,
    password: str,
    extension_id: str,
    log_func: LogFunction | None,
) -> bool | str:
    page = None
    for candidate in context.pages:
        try:
            url = candidate.url or ""
            if url.startswith("http") and "chrome-extension://" not in url:
                page = candidate
                break
        except Exception:
            continue
    if page is None:
        page = await context.new_page()

    try:
        has_provider = bool(await page.evaluate("() => !!window.okxwallet"))
    except Exception:
        has_provider = False

    if not has_provider:
        _emit(log_func, account_id, "导航到外部网页以激活钱包内容脚本...")
        navigated = False
        for url in ("https://example.com", "https://www.google.com", "https://www.baidu.com"):
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=15_000)
                navigated = True
                break
            except Exception:
                continue
        if not navigated:
            _emit(log_func, account_id, "无法导航到任何外部网页")
            return False
        await asyncio.sleep(3)

    for attempt in range(8):
        try:
            has_provider = bool(
                await page.evaluate("() => !!(window.okxwallet || window.ethereum)")
            )
        except Exception:
            has_provider = False
        if has_provider:
            break
        if attempt == 4:
            _emit(log_func, account_id, "provider 仍未出现，刷新页面重试...")
            try:
                await page.reload(wait_until="domcontentloaded", timeout=15_000)
            except Exception:
                pass
            await asyncio.sleep(5)
        elif attempt < 7:
            _emit(log_func, account_id, f"未检测到钱包 provider，等待重试 ({attempt + 1}/8)...")
            await asyncio.sleep(3)
    if not has_provider:
        _emit(log_func, account_id, "钱包 provider 未找到 → 扩展可能未安装或未启用")
        return False

    try:
        lock_check = await page.evaluate(
            """async () => {
                const provider = window.okxwallet;
                if (!provider) return {known: false};
                try {
                    if (provider._metamask
                            && typeof provider._metamask.isUnlocked === 'function') {
                        return {
                            known: true,
                            unlocked: await provider._metamask.isUnlocked()
                        };
                    }
                } catch (_) {}
                try {
                    if (typeof provider.isUnlocked === 'function') {
                        return {known: true, unlocked: await provider.isUnlocked()};
                    }
                } catch (_) {}
                return {known: false};
            }"""
        )
    except Exception:
        lock_check = {"known": False}
    if lock_check.get("known") and lock_check.get("unlocked"):
        _emit(log_func, account_id, "钱包内部 API 确认已解锁")
        return True

    wallet_popup = None
    popup_ready = asyncio.Event()

    async def capture_popup(new_page) -> None:
        nonlocal wallet_popup
        if popup_ready.is_set():
            return
        for _ in range(20):
            try:
                url = new_page.url
                if url and url != "about:blank":
                    break
            except Exception:
                return
            await asyncio.sleep(0.2)
        try:
            url = new_page.url or ""
        except Exception:
            return
        if extension_id in url:
            wallet_popup = new_page
            popup_ready.set()
            _emit(log_func, account_id, f"捕获到钱包弹窗: {url[-60:]}")

    context.on("page", capture_popup)
    try:
        _emit(log_func, account_id, "触发 personal_sign 强制钱包弹窗...")
        try:
            await page.evaluate(
                """() => {
                    const provider = window.okxwallet;
                    if (!provider) return;
                    provider.request({method: 'eth_accounts'}).then(accounts => {
                        if (accounts && accounts.length > 0) {
                            provider.request({
                                method: 'personal_sign',
                                params: ['0x76657269667920756e6c6f636b', accounts[0]]
                            }).catch(() => {});
                        } else {
                            provider.request({method: 'eth_requestAccounts'}).catch(() => {});
                        }
                    }).catch(() => {
                        provider.request({method: 'eth_requestAccounts'}).catch(() => {});
                    });
                }"""
            )
        except Exception as exc:
            _emit(log_func, account_id, f"触发弹窗异常: {exc}")

        try:
            await asyncio.wait_for(popup_ready.wait(), timeout=15)
        except asyncio.TimeoutError:
            try:
                recheck = await page.evaluate(
                    """async () => {
                        const provider = window.okxwallet || window.ethereum;
                        if (!provider) return {known: false};
                        try {
                            if (provider._metamask
                                    && typeof provider._metamask.isUnlocked === 'function') {
                                return {
                                    known: true,
                                    unlocked: await provider._metamask.isUnlocked()
                                };
                            }
                        } catch (_) {}
                        try {
                            const accounts = await provider.request({method: 'eth_accounts'});
                            if (accounts && accounts.length > 0) {
                                return {known: true, unlocked: true};
                            }
                        } catch (_) {}
                        return {known: false};
                    }"""
                )
            except Exception:
                recheck = {"known": False}
            if recheck.get("known") and recheck.get("unlocked"):
                _emit(log_func, account_id, "钱包确认已解锁（无需弹窗）")
                return True
            _emit(log_func, account_id, "钱包状态不确定，将通过 dApp 触发解锁")
            return "NEED_DAPP"

        popup = wallet_popup
        if popup is None:
            _emit(log_func, account_id, "弹窗引用丢失")
            return False
        try:
            await popup.wait_for_load_state("domcontentloaded", timeout=10_000)
        except Exception:
            pass

        for check in range(10):
            await asyncio.sleep(2)
            password_count = 0
            button_count = 0
            for frame in popup.frames:
                try:
                    password_count += await frame.locator('input[type="password"]').count()
                    button_count += await frame.locator("button").count()
                except Exception:
                    continue
            _emit(
                log_func,
                account_id,
                f"弹窗渲染 #{check + 1}: password={password_count}, "
                f"button={button_count}, frames={len(popup.frames)}",
            )
            if password_count > 0 or button_count > 0:
                break
            if check == 9:
                _emit(log_func, account_id, "弹窗渲染超时（20秒）")
                return False

        if not await _find_and_fill_password(popup, account_id, password, log_func):
            _emit(log_func, account_id, "无密码框 → 钱包已解锁（关闭签名弹窗）")
            try:
                if not popup.is_closed():
                    await popup.close()
            except Exception:
                pass
            return True

        await asyncio.sleep(0.5)
        if not await _click_unlock_button(popup, account_id, log_func):
            return False
        await asyncio.sleep(3)
        remaining_passwords = 0
        for frame in popup.frames:
            try:
                remaining_passwords += await frame.locator('input[type="password"]').count()
            except Exception:
                continue
        if remaining_passwords > 0:
            _emit(log_func, account_id, "密码框仍在 → 解锁失败（可能密码错误）")
            return False
        _emit(log_func, account_id, "钱包解锁成功，关闭签名弹窗...")
        await asyncio.sleep(1)
        try:
            if not popup.is_closed():
                await popup.close()
        except Exception:
            pass
        return True
    finally:
        try:
            context.remove_listener("page", capture_popup)
        except Exception:
            pass


async def unlock_okx_wallet(
    context,
    account_id,
    password: str | None = None,
    extension_id: str = OKX_EXTENSION_ID,
    log_func=None,
) -> bool | str:
    password = password or get_wallet_password(PROJECT_DIR)
    if not password:
        _emit(log_func, account_id, "未配置 OKX 钱包密码")
        return False
    return await _unlock_with_provider_popup(
        context,
        account_id,
        password,
        extension_id,
        log_func,
    )


async def click_wallet_button(
    page,
    account_id,
    max_rounds: int = 5,
    log_func=None,
) -> bool:
    any_clicked = False
    for round_number in range(max_rounds):
        try:
            if page.is_closed():
                break
        except Exception:
            break
        clicked = False
        for wait_number in range(5):
            for frame in page.frames:
                for text in WALLET_BUTTON_KEYWORDS:
                    try:
                        button = frame.locator(f'button:has-text("{text}")')
                        if await button.count() > 0:
                            await button.first.click(timeout=3_000)
                            where = "iframe" if frame != page.main_frame else "主文档"
                            _emit(
                                log_func,
                                account_id,
                                f"[第{round_number + 1}轮] 在{where}点击 [{text}]",
                            )
                            clicked = True
                            break
                    except Exception:
                        continue
                if clicked:
                    break
                try:
                    button = frame.locator('button[type="submit"]')
                    if await button.count() > 0:
                        await button.first.click(timeout=3_000)
                        _emit(log_func, account_id, f"[第{round_number + 1}轮] 点击 submit 按钮")
                        clicked = True
                        break
                except Exception:
                    continue
            if clicked:
                break
            if wait_number < 4:
                await asyncio.sleep(1)

        if not clicked:
            for frame in page.frames:
                try:
                    clicked = bool(
                        await frame.evaluate(
                            """() => {
                                const keywords = [
                                    '确认', '连接', '签名',
                                    'Confirm', 'Connect', 'Sign', 'Approve'
                                ];
                                for (const button of document.querySelectorAll('button')) {
                                    const text = (button.innerText || '').trim();
                                    if (keywords.some(keyword => text.includes(keyword))) {
                                        button.click();
                                        return true;
                                    }
                                }
                                return false;
                            }"""
                        )
                    )
                    if clicked:
                        _emit(log_func, account_id, f"[第{round_number + 1}轮] JS 点击成功")
                        break
                except Exception:
                    continue
        if not clicked:
            break
        any_clicked = True
        await asyncio.sleep(2)
    return any_clicked
