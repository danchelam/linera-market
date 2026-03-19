"""
通用浏览器自动化底层模块 (Playwright 版本 2.0)
─────────────────────────────────────────────
- AdsPower 浏览器管理（启动/关闭）
- OKX 钱包解锁（Shadow DOM 自动穿透 + React 受控组件兼容）
- 钱包弹窗自动处理（事件驱动，连接/签名/确认）
- Excel/CSV 账号读取
- asyncio 并发调度

写新项目只需:
    from base_module import load_accounts, run_batch, log
    async def my_task(page, context, account_id, popup_handler, **kw) -> bool: ...
    asyncio.run(run_batch(accounts, my_task, max_workers=3))
"""

import asyncio
import datetime
import json
import os
import sys
import random
import threading
from typing import Optional, Dict, List, Callable

import pandas as pd
import requests
from playwright.async_api import (
    async_playwright, Browser, BrowserContext, Page, Playwright,
)

__version__ = "2026.03.19.8"

# ════════════════════════════════════════════════════════════
#  全局配置（可在调用 run_batch 时覆盖）
# ════════════════════════════════════════════════════════════

ADSPOWER_API_BASE_URL = "http://127.0.0.1:50325"
ADSPOWER_API_KEY = "5b9664bf3e65c5a0622d1b5d0d766eac"
OKX_EXTENSION_ID = "mcohilncbfahbmgdjkbpemcciiolgcge"
OKX_DEFAULT_PASSWORD = "DD112211"

STOP_FLAG = False
PERF_DEBUG = os.environ.get("PERF_DEBUG", "1").strip().lower() in (
    "1", "true", "yes", "on",
)

_print_lock = threading.Lock()
_file_lock = threading.Lock()
_logger_callback: Optional[Callable] = None


def _get_base_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


# ════════════════════════════════════════════════════════════
#  日志
# ════════════════════════════════════════════════════════════

def set_logger_callback(cb: Optional[Callable]):
    global _logger_callback
    _logger_callback = cb


def log(account_id: str, msg: str):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    full = f"[{ts}] [窗口 {account_id}] {msg}"
    with _print_lock:
        print(full)
    if _logger_callback:
        try:
            _logger_callback(full)
        except Exception:
            pass


def perf_log(account_id: str, msg: str):
    if PERF_DEBUG:
        log(account_id, f"[PERF] {msg}")


def stop_all_tasks():
    global STOP_FLAG
    STOP_FLAG = True


# ════════════════════════════════════════════════════════════
#  任务周期管理（每天 08:00 重置）
# ════════════════════════════════════════════════════════════

def _completed_path() -> str:
    return os.path.join(_get_base_dir(), "completed_tasks.json")


def load_completed_tasks() -> Dict[str, float]:
    p = _completed_path()
    if not os.path.exists(p):
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_completed_task(account_id: str):
    with _file_lock:
        data = load_completed_tasks()
        data[account_id] = datetime.datetime.now().timestamp()
        try:
            with open(_completed_path(), "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
        except Exception as e:
            print(f"保存完成记录失败: {e}")


def is_account_completed(account_id: str) -> bool:
    data = load_completed_tasks()
    ts = data.get(account_id)
    if not ts:
        return False
    now = datetime.datetime.now()
    today8 = now.replace(hour=8, minute=0, second=0, microsecond=0)
    cycle = today8 if now >= today8 else today8 - datetime.timedelta(days=1)
    return datetime.datetime.fromtimestamp(ts) > cycle


# ════════════════════════════════════════════════════════════
#  账号信息
# ════════════════════════════════════════════════════════════

class AccountInfo:
    def __init__(self, id: str, ua: str = "", proxy: str = ""):
        self.id = id
        self.ua = ua
        self.proxy = proxy


def load_accounts(excel_path: Optional[str] = None) -> List[AccountInfo]:
    if excel_path is None:
        excel_path = os.path.join(_get_base_dir(), "shuju.xlsx")

    base, _ = os.path.splitext(excel_path)
    chosen = excel_path
    if not os.path.exists(chosen):
        alt = base + ".csv"
        if os.path.exists(alt):
            chosen = alt

    accounts: List[AccountInfo] = []
    try:
        print(f"正在加载账号文件: {chosen}")
        if chosen.lower().endswith(".csv"):
            df = pd.read_csv(chosen, dtype=str, encoding="utf-8", keep_default_na=False)
        else:
            df = pd.read_excel(chosen, dtype=str).fillna("")
        print(f"前 5 行:\n{df.head()}")

        def sv(x):
            return str(x).strip() if x is not None else ""

        for _, row in df.iterrows():
            id_val = (
                sv(row.get("id", ""))
                or sv(row.get("user_id", ""))
                or sv(row.get("acc_id", ""))
            )
            if id_val:
                accounts.append(AccountInfo(
                    id=id_val,
                    ua=sv(row.get("ua", "")),
                    proxy=sv(row.get("proxy", "")) if "proxy" in row else "",
                ))
        print(f"加载账号数量: {len(accounts)}")
    except Exception as e:
        print(f"加载账号失败: {e}\n路径: {chosen}")
    return accounts


# ════════════════════════════════════════════════════════════
#  AdsPower 浏览器管理
# ════════════════════════════════════════════════════════════

class AdsPowerManager:
    def __init__(
        self,
        api_base_url: str = ADSPOWER_API_BASE_URL,
        api_key: str = "",
    ):
        self.api_base_url = api_base_url
        self._headers = {"X-Api-Key": api_key} if api_key else None

    def start_browser(self, user_id: str) -> Optional[str]:
        """启动浏览器，返回 CDP 地址（如 '127.0.0.1:xxxxx'）"""
        import time as _t
        url = f"{self.api_base_url}/api/v1/browser/start?user_id={user_id}"
        for attempt in range(5):
            try:
                resp = requests.get(url, timeout=60, headers=self._headers)
                data = resp.json()
                if data.get("code") == 0 and "debug_port" in data.get("data", {}):
                    port = data["data"]["debug_port"]
                    return port if ":" in str(port) else f"127.0.0.1:{port}"
                if "Too many request" in data.get("msg", ""):
                    wait = (attempt + 1) * 2
                    log(user_id, f"API 限速，{wait}s 后重试({attempt + 2}/5)...")
                    _t.sleep(wait)
                    continue
                log(user_id, f"启动失败: {data.get('msg', '未知')}")
                return None
            except Exception as e:
                log(user_id, f"启动异常: {e}")
                return None
        return None

    def close_browser(self, user_id: str) -> bool:
        url = f"{self.api_base_url}/api/v1/browser/stop?user_id={user_id}"
        try:
            resp = requests.get(url, timeout=30, headers=self._headers)
            data = resp.json()
            ok = data.get("code") == 0
            log(user_id, "关闭浏览器成功" if ok else f"关闭失败: {data.get('msg')}")
            return ok
        except Exception as e:
            log(user_id, f"关闭异常: {e}")
            return False


# ════════════════════════════════════════════════════════════
#  Playwright CDP 连接
# ════════════════════════════════════════════════════════════

async def connect_browser(pw: Playwright, cdp_addr: str) -> Optional[Browser]:
    try:
        return await pw.chromium.connect_over_cdp(f"http://{cdp_addr}")
    except Exception as e:
        print(f"CDP 连接失败 ({cdp_addr}): {e}")
        return None


# ════════════════════════════════════════════════════════════
#  CDP 辅助：穿透 Shadow DOM 查找 & 操作元素
#  ── DOM.getDocument(pierce=true) 可穿透 closed shadow root
#  ── 这是 Playwright locator 做不到的
# ════════════════════════════════════════════════════════════

async def _cdp_find_nodes(cdp, selector: str) -> List[int]:
    """用 CDP 穿透 Shadow DOM 查找匹配的节点 ID 列表"""
    try:
        doc = await cdp.send("DOM.getDocument", {"depth": -1, "pierce": True})
        result = await cdp.send("DOM.querySelectorAll", {
            "nodeId": doc["root"]["nodeId"],
            "selector": selector,
        })
        return result.get("nodeIds", [])
    except Exception:
        return []


async def _cdp_focus_and_type(cdp, page: Page, node_id: int, text: str):
    """CDP 聚焦到节点，然后用 Playwright keyboard 输入"""
    await cdp.send("DOM.focus", {"nodeId": node_id})
    await asyncio.sleep(0.1)
    await page.keyboard.type(text, delay=30)


async def _cdp_click_node(cdp, page: Page, node_id: int):
    """通过 CDP 获取节点坐标并模拟鼠标点击"""
    try:
        box = await cdp.send("DOM.getBoxModel", {"nodeId": node_id})
        quad = box["model"]["content"]
        x = (quad[0] + quad[2]) / 2
        y = (quad[1] + quad[5]) / 2
        await cdp.send("Input.dispatchMouseEvent", {
            "type": "mousePressed", "x": x, "y": y,
            "button": "left", "clickCount": 1,
        })
        await asyncio.sleep(0.05)
        await cdp.send("Input.dispatchMouseEvent", {
            "type": "mouseReleased", "x": x, "y": y,
            "button": "left", "clickCount": 1,
        })
        return True
    except Exception:
        await page.keyboard.press("Enter")
        return False


async def _cdp_get_full_html(cdp) -> str:
    """获取完整 HTML（穿透 Shadow DOM），用于判断页面状态"""
    try:
        doc = await cdp.send("DOM.getDocument", {"depth": -1, "pierce": True})
        result = await cdp.send("DOM.getOuterHTML", {
            "nodeId": doc["root"]["nodeId"],
        })
        return result.get("outerHTML", "")
    except Exception:
        return ""


# ════════════════════════════════════════════════════════════
#  OKX 钱包解锁
#  ── 通过 eth_requestAccounts 触发扩展自己弹出 notification.html
#  ── 在 notification 页面中查找密码框并填写
#  ── popup.html 在标签页中不会渲染 UI，所以不能直接打开
# ════════════════════════════════════════════════════════════

def _find_ses_frame(wp: Page):
    """在 notification 页面中找到 SES 沙盒 iframe（实际 UI 所在位置）"""
    for frame in wp.frames:
        if frame == wp.main_frame:
            continue
        try:
            url = frame.url or ""
            if "ses.html" in url or "ses-sandbox" in url:
                return frame
        except Exception:
            continue
    # 没有明确的 SES iframe，找任何包含 input 的 iframe
    for frame in wp.frames:
        if frame == wp.main_frame:
            continue
        try:
            if frame.locator("input").count() > 0:
                return frame
        except Exception:
            continue
    return None


async def _find_and_fill_password(
    wp: Page,
    context: BrowserContext,
    account_id: str,
    password: str,
) -> bool:
    """
    在钱包弹窗页面中查找密码框并填写。
    优先搜索 iframe（OKX 用 SES iframe 沙盒渲染 UI）。
    """
    # ── 优先：在所有 iframe 中搜索 ──────────────
    for frame in wp.frames:
        try:
            loc = frame.locator('input[type="password"]')
            if await loc.count() > 0:
                await loc.first.fill(password)
                where = "iframe" if frame != wp.main_frame else "主文档"
                log(account_id, f"在{where}中找到密码框并填写")
                return True
        except Exception:
            continue

    # ── 兜底：JS 递归（主文档 shadow DOM）──────
    try:
        js_ok = await wp.evaluate("""(pwd) => {
            function deep(root, sel) {
                if (!root) return null;
                try { let e = root.querySelector(sel); if (e) return e; } catch(x){}
                try { for (const el of root.querySelectorAll('*'))
                    if (el.shadowRoot) { let e = deep(el.shadowRoot, sel); if (e) return e; }
                } catch(x){}
                return null;
            }
            const input = deep(document, 'input[type="password"]');
            if (!input) return false;
            input.focus();
            const setter = Object.getOwnPropertyDescriptor(
                HTMLInputElement.prototype, 'value').set;
            setter.call(input, pwd);
            input.dispatchEvent(new Event('input', {bubbles: true}));
            input.dispatchEvent(new Event('change', {bubbles: true}));
            return true;
        }""", password)
        if js_ok:
            log(account_id, "JS 递归找到密码框")
            return True
    except Exception:
        pass

    return False


async def _click_unlock_button(
    wp: Page,
    context: BrowserContext,
    account_id: str,
) -> bool:
    """点击解锁/Unlock 按钮，优先搜索 iframe"""
    # ── 在所有 frame 中搜索按钮 ──────────────
    for frame in wp.frames:
        for text in ("解锁", "Unlock"):
            try:
                btn = frame.locator(f'button:has-text("{text}")')
                if await btn.count() > 0:
                    await btn.first.click()
                    log(account_id, f"已点击 [{text}]")
                    return True
            except Exception:
                continue
        for sel in ('button[type="submit"]', 'button[data-testid="okd-button"]'):
            try:
                btn = frame.locator(sel)
                if await btn.count() > 0:
                    await btn.first.click()
                    log(account_id, f"已点击 [{sel}]")
                    return True
            except Exception:
                continue

    # ── 兜底：JS 递归 ──────────────────────────
    for frame in wp.frames:
        try:
            js_ok = await frame.evaluate("""() => {
                const kw = ['解锁', 'Unlock'];
                for (const b of document.querySelectorAll('button')) {
                    const t = (b.innerText || '').trim();
                    if (kw.some(k => t.includes(k))) { b.click(); return true; }
                }
                const sub = document.querySelector('button[type="submit"]');
                if (sub) { sub.click(); return true; }
                return false;
            }""")
            if js_ok:
                log(account_id, "JS 点击解锁按钮成功")
                return True
        except Exception:
            continue

    await wp.keyboard.press("Enter")
    return True


async def unlock_okx_wallet(
    context: BrowserContext,
    account_id: str,
    password: str = OKX_DEFAULT_PASSWORD,
    extension_id: str = OKX_EXTENSION_ID,
) -> bool:
    """
    解锁 OKX 钱包。

    策略：
    1. 先用 _metamask.isUnlocked() 内部 API 检查真实锁定状态
    2. 若锁定/未知 → 用 personal_sign 触发弹窗（需要私钥，锁定必弹密码框）
       eth_accounts / eth_requestAccounts 返回缓存数据，不可靠
    3. 弹窗中填写密码 → 点击解锁 → 关闭残留签名弹窗
    """
    # 从 context.pages 中找到真正的网页（跳过扩展 offscreen 页面）
    page = None
    for p in context.pages:
        try:
            u = p.url or ""
            if u.startswith("http") and "chrome-extension://" not in u:
                page = p
                break
        except Exception:
            continue
    if not page:
        page = await context.new_page()

    # ── 1. 导航到外部网页（扩展不在 adspower 内部页面注入内容脚本）──
    # 先检查当前页是否已有 provider（比如已经在 dApp 上）
    has_provider = False
    try:
        has_provider = await page.evaluate(
            "() => !!(window.okxwallet)"
        )
    except Exception:
        pass

    if not has_provider:
        log(account_id, "导航到外部网页以激活钱包内容脚本...")
        navigated = False
        for url in ("https://example.com", "https://www.google.com", "https://www.baidu.com"):
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                navigated = True
                break
            except Exception:
                continue
        if not navigated:
            log(account_id, "无法导航到任何外部网页")
            return False
        await asyncio.sleep(3)

    # ── 2. 检查钱包 provider 是否存在 ────────────
    for attempt in range(8):
        try:
            has_provider = await page.evaluate(
                "() => !!(window.okxwallet || window.ethereum)"
            )
        except Exception:
            has_provider = False

        if has_provider:
            break

        if attempt == 4:
            log(account_id, "provider 仍未出现，刷新页面重试...")
            try:
                await page.reload(wait_until="domcontentloaded", timeout=15000)
            except Exception:
                pass
            await asyncio.sleep(5)
            continue

        if attempt < 7:
            log(account_id, f"未检测到钱包 provider，等待重试 ({attempt + 1}/8)...")
            await asyncio.sleep(3)

    if not has_provider:
        log(account_id, "钱包 provider 未找到 → 扩展可能未安装或未启用")
        return False

    # ── 3. 通过钱包内部 API 检查真实锁定状态 ──────
    #    eth_accounts / eth_requestAccounts 都会返回缓存地址，不可靠。
    #    _metamask.isUnlocked() 直接查询扩展后台，可信。
    try:
        lock_check = await page.evaluate("""async () => {
            const p = window.okxwallet;
            if (!p) return {known: false};
            try {
                if (p._metamask && typeof p._metamask.isUnlocked === 'function') {
                    return {known: true, unlocked: await p._metamask.isUnlocked()};
                }
            } catch(e) {}
            try {
                if (typeof p.isUnlocked === 'function') {
                    return {known: true, unlocked: await p.isUnlocked()};
                }
            } catch(e) {}
            return {known: false};
        }""")
    except Exception:
        lock_check = {"known": False}

    if lock_check.get("known") and lock_check.get("unlocked"):
        log(account_id, "钱包内部 API 确认已解锁")
        return True

    if lock_check.get("known") and not lock_check.get("unlocked"):
        log(account_id, "钱包内部 API 确认已锁定，需要密码解锁")
    else:
        log(account_id, "内部 API 不可用，触发弹窗验证锁定状态...")

    # ── 4. 设置弹窗捕获 ───────────────────────────
    wallet_popup: Optional[Page] = None
    popup_ready = asyncio.Event()

    async def _capture_popup(new_page: Page):
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
            log(account_id, f"捕获到钱包弹窗: {url[-60:]}")
            wallet_popup = new_page
            popup_ready.set()

    context.on("page", _capture_popup)

    try:
        # ── 5. 触发弹窗 ──────────────────────────
        #    personal_sign 需要私钥签名 → 钱包锁定时必弹密码框
        #    比 eth_requestAccounts 可靠（后者返回缓存不弹窗）
        log(account_id, "触发 personal_sign 强制钱包弹窗...")
        try:
            await page.evaluate("""() => {
                const p = window.okxwallet;
                if (!p) return;
                p.request({method: 'eth_accounts'}).then(accs => {
                    if (accs && accs.length > 0) {
                        p.request({
                            method: 'personal_sign',
                            params: ['0x76657269667920756e6c6f636b', accs[0]]
                        }).catch(() => {});
                    } else {
                        p.request({method: 'eth_requestAccounts'}).catch(() => {});
                    }
                }).catch(() => {
                    p.request({method: 'eth_requestAccounts'}).catch(() => {});
                });
            }""")
        except Exception as e:
            log(account_id, f"触发弹窗异常: {e}")

        # ── 6. 等待弹窗出现 ──────────────────────
        try:
            await asyncio.wait_for(popup_ready.wait(), timeout=15)
        except asyncio.TimeoutError:
            log(account_id, "等待钱包弹窗超时（15秒），再次检查锁定状态...")
            # 弹窗没出现 → 可能钱包已解锁（personal_sign 静默完成）
            try:
                recheck = await page.evaluate("""async () => {
                    const p = window.okxwallet || window.ethereum;
                    if (!p) return {known: false};
                    try {
                        if (p._metamask && typeof p._metamask.isUnlocked === 'function') {
                            return {known: true, unlocked: await p._metamask.isUnlocked()};
                        }
                    } catch(e) {}
                    try {
                        const accs = await p.request({method: 'eth_accounts'});
                        if (accs && accs.length > 0) return {known: true, unlocked: true};
                    } catch(e) {}
                    return {known: false};
                }""")
            except Exception:
                recheck = {"known": False}

            if recheck.get("known") and recheck.get("unlocked"):
                log(account_id, "钱包确认已解锁（无需弹窗）")
                return True

            # 仍不确定 → 返回 "NEED_DAPP" 让任务脚本通过网站触发解锁
            log(account_id, "钱包状态不确定，将通过 dApp 触发解锁")
            return "NEED_DAPP"

        # ── 7. 弹窗已出现，等待 React 渲染出表单元素 ──
        wp = wallet_popup
        if not wp:
            log(account_id, "弹窗引用丢失")
            return False

        try:
            await wp.wait_for_load_state("domcontentloaded", timeout=10000)
        except Exception:
            pass

        for check in range(10):
            await asyncio.sleep(2)
            total_pwd = 0
            total_btn = 0
            for frame in wp.frames:
                try:
                    total_pwd += await frame.locator('input[type="password"]').count()
                    total_btn += await frame.locator("button").count()
                except Exception:
                    continue

            log(account_id, f"弹窗渲染 #{check + 1}: "
                f"password={total_pwd}, button={total_btn}, frames={len(wp.frames)}")

            if total_pwd > 0 or total_btn > 0:
                break

            if check == 9:
                try:
                    wp_html = await wp.evaluate(
                        "document.documentElement ? document.documentElement.outerHTML : ''"
                    )
                except Exception:
                    wp_html = ""
                log(account_id, f"弹窗渲染超时（20秒），HTML: {wp_html[:300]}")
                return False

        # ── 8. 查找并填写密码 ─────────────────────
        found = await _find_and_fill_password(wp, context, account_id, password)

        if not found:
            # 没有密码框 → 钱包已解锁，弹窗是 personal_sign 签名确认
            log(account_id, "无密码框 → 钱包已解锁（关闭签名弹窗）")
            try:
                if wp and not wp.is_closed():
                    await wp.close()
            except Exception:
                pass
            return True

        # ── 9. 点击解锁按钮 ──────────────────────
        await asyncio.sleep(0.5)
        await _click_unlock_button(wp, context, account_id)
        await asyncio.sleep(3)

        # ── 10. 验证是否解锁成功（搜索所有 frame）──
        still = 0
        for frame in wp.frames:
            try:
                still += await frame.locator('input[type="password"]').count()
            except Exception:
                pass

        if still > 0:
            log(account_id, "密码框仍在 → 解锁失败（可能密码错误）")
            return False

        # 解锁成功 → 关闭残留的 personal_sign 弹窗
        log(account_id, "钱包解锁成功，关闭签名弹窗...")
        await asyncio.sleep(1)
        try:
            if wp and not wp.is_closed():
                await wp.close()
        except Exception:
            pass

        return True

    finally:
        try:
            context.remove_listener("page", _capture_popup)
        except Exception:
            pass


# ════════════════════════════════════════════════════════════
#  钱包弹窗自动处理（事件驱动）
#  ── 注册到 BrowserContext，新钱包弹窗出现时自动点击确认
# ════════════════════════════════════════════════════════════

WALLET_BUTTON_KEYWORDS = [
    "确认", "连接", "签名",
    "Confirm", "Connect", "Sign", "Approve",
]


async def _click_wallet_button(
    page: Page, account_id: str, max_rounds: int = 5,
) -> bool:
    """
    在钱包弹窗中点击确认/连接/签名按钮。
    OKX 钱包连接流程在同一 notification.html 内有多步确认，
    每轮点击一个按钮后等待页面切换到下一步，持续处理直到没有更多按钮。
    """
    any_clicked = False

    for round_num in range(max_rounds):
        try:
            if page.is_closed():
                break
        except Exception:
            break

        clicked = False

        # 等待按钮出现（最多 5 秒）
        for wait in range(5):
            for frame in page.frames:
                for text in WALLET_BUTTON_KEYWORDS:
                    try:
                        btn = frame.locator(f'button:has-text("{text}")')
                        if await btn.count() > 0:
                            await btn.first.click(timeout=3000)
                            where = "iframe" if frame != page.main_frame else "主文档"
                            log(account_id,
                                f"[第{round_num+1}轮] 在{where}点击 [{text}]")
                            clicked = True
                            break
                    except Exception:
                        continue
                if clicked:
                    break
                try:
                    btn = frame.locator('button[type="submit"]')
                    if await btn.count() > 0:
                        await btn.first.click(timeout=3000)
                        log(account_id, f"[第{round_num+1}轮] 点击 submit 按钮")
                        clicked = True
                        break
                except Exception:
                    continue
            if clicked:
                break
            if wait < 4:
                await asyncio.sleep(1)

        # Playwright 未找到 → JS 兜底
        if not clicked:
            for frame in page.frames:
                try:
                    js_ok = await frame.evaluate("""() => {
                        const kw = ['确认','连接','签名',
                                     'Confirm','Connect','Sign','Approve'];
                        for (const b of document.querySelectorAll('button')) {
                            const t = (b.innerText||'').trim();
                            if (kw.some(k => t.includes(k))) {
                                b.click(); return true;
                            }
                        }
                        return false;
                    }""")
                    if js_ok:
                        log(account_id, f"[第{round_num+1}轮] JS 点击成功")
                        clicked = True
                        break
                except Exception:
                    continue

        if clicked:
            any_clicked = True
            # 等待弹窗切换到下一步（或自动关闭）
            await asyncio.sleep(2)
        else:
            break

    return any_clicked


class WalletPopupHandler:
    def __init__(self, account_id: str, context: BrowserContext):
        self.account_id = account_id
        self.context = context
        self.enabled = True

    async def on_new_page(self, page: Page):
        if not self.enabled:
            return

        for _ in range(10):
            try:
                url = page.url
                if url and url != "about:blank":
                    break
            except Exception:
                return
            await asyncio.sleep(0.3)

        try:
            url = page.url
        except Exception:
            return
        if "chrome-extension://" not in url:
            return

        log(self.account_id, f"检测到钱包弹窗: {url[:80]}")

        try:
            await page.wait_for_load_state("domcontentloaded", timeout=10000)
        except Exception:
            pass

        await asyncio.sleep(random.uniform(1.0, 2.5))

        try:
            clicked = await _click_wallet_button(page, self.account_id)
            if clicked:
                log(self.account_id, "钱包弹窗已自动确认")
            else:
                log(self.account_id, "钱包弹窗未找到可点击按钮")
        except Exception as e:
            log(self.account_id, f"处理弹窗异常: {e}")


def setup_wallet_handler(
    context: BrowserContext, account_id: str,
) -> WalletPopupHandler:
    handler = WalletPopupHandler(account_id, context)
    context.on("page", handler.on_new_page)
    return handler


async def drain_existing_popups(
    context: BrowserContext,
    account_id: str,
    main_page: Page,
):
    """清理连接前就已存在的钱包弹窗"""
    for p in context.pages:
        if p == main_page:
            continue
        try:
            url = p.url or ""
        except Exception:
            continue
        if "chrome-extension://" not in url:
            continue
        log(account_id, f"清理残留弹窗: {url[:60]}")
        try:
            await _click_wallet_button(p, account_id)
        except Exception:
            pass


# ════════════════════════════════════════════════════════════
#  单账号运行 + 批量调度
# ════════════════════════════════════════════════════════════

async def run_single_account(
    pw: Playwright,
    ads: AdsPowerManager,
    account: AccountInfo,
    task_func,
    **task_kwargs,
):
    """
    完整流程：启动浏览器 → CDP → 弹窗处理器 → 解锁钱包 → 执行业务 → 关闭。

    task_func 签名:
        async def my_task(page, context, account_id, popup_handler, **kw) -> bool
    """
    aid = account.id
    if is_account_completed(aid):
        log(aid, "当前周期已完成，跳过。")
        return

    cdp_addr = await asyncio.to_thread(ads.start_browser, aid)
    if not cdp_addr:
        return
    await asyncio.sleep(1.5)

    browser: Optional[Browser] = None
    try:
        browser = await connect_browser(pw, cdp_addr)
        if not browser:
            return
        log(aid, f"已连接 ({cdp_addr})")

        context = browser.contexts[0] if browser.contexts else None
        if not context:
            log(aid, "无法获取浏览器上下文")
            return

        # 找到真正的网页（跳过扩展 offscreen 页面）
        page = None
        for p in context.pages:
            try:
                u = p.url or ""
                if u.startswith("http") and "chrome-extension://" not in u:
                    page = p
                    break
            except Exception:
                continue
        if not page:
            page = await context.new_page()

        handler = setup_wallet_handler(context, aid)

        # 清理残留弹窗
        await drain_existing_popups(context, aid, page)

        # 执行业务
        success = await task_func(page, context, aid, handler, **task_kwargs)

        if STOP_FLAG:
            log(aid, "收到停止信号，未记录。")
        elif success:
            save_completed_task(aid)
            log(aid, "任务全部完成，已记录。")
        else:
            log(aid, "任务未完整完成。")

    except Exception as e:
        log(aid, f"任务异常: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if browser:
            try:
                await browser.close()
            except Exception:
                pass
            await asyncio.to_thread(ads.close_browser, aid)


async def run_batch(
    accounts: List[AccountInfo],
    task_func,
    max_workers: int = 3,
    api_base_url: str = ADSPOWER_API_BASE_URL,
    api_key: str = ADSPOWER_API_KEY,
    **task_kwargs,
):
    """批量并发运行，自带两轮执行（第二轮补跑失败的）"""
    ads = AdsPowerManager(api_base_url=api_base_url, api_key=api_key)
    sem = asyncio.Semaphore(max_workers)

    async with async_playwright() as pw:

        async def _run(acc: AccountInfo):
            async with sem:
                if STOP_FLAG:
                    return
                await run_single_account(pw, ads, acc, task_func, **task_kwargs)

        # 第一轮
        log("SYSTEM", f"第一轮：{len(accounts)} 个账号")
        tasks = []
        for acc in accounts:
            tasks.append(asyncio.ensure_future(_run(acc)))
            await asyncio.sleep(3)
        await asyncio.gather(*tasks, return_exceptions=True)

        # 第二轮补跑
        remaining = [a for a in accounts if not is_account_completed(a.id)]
        if remaining and not STOP_FLAG:
            log("SYSTEM", f"第二轮补跑：{len(remaining)} 个")
            tasks = []
            for acc in remaining:
                tasks.append(asyncio.ensure_future(_run(acc)))
                await asyncio.sleep(3)
            await asyncio.gather(*tasks, return_exceptions=True)
