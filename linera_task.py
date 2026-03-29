"""
Linera Prediction Market 自动化任务 (Playwright 版本 2.0)
─────────────────────────────────────────────────────────
基于 base_module 通用底层，包含 Linera 预测市场业务逻辑：
  1. 打开网站 → 处理钱包签名弹窗完成连接
  2. 设置 1 minute 市场 + 金额
  3. 随机市场（BTC/ETH/SOL）+ 随机方向（HIGHER/LOWER）下注
  4. 下注后由后台 WalletPopupHandler 自动签名
  5. 等待成功标志确认下注有效
  6. 完成 15 次下注
"""

__version__ = "2026.03.28.7"

import asyncio
import random
import re
import sys
import os
import json as _json
from datetime import datetime

from playwright.async_api import Page, BrowserContext

from base_module import (
    WalletPopupHandler,
    _click_wallet_button,
    _find_and_fill_password,
    _click_unlock_button,
    OKX_DEFAULT_PASSWORD,
    load_accounts,
    run_batch,
    log,
    STOP_FLAG,
)

# ─── 页面配置 ─────────────────────────────────────────
DAPP_URL = "https://linera.market"
MARKETS = ["BTC", "ETH", "SOL"]
TARGET_BETS = 21

# 跨轮次进度记忆：account_id → 目标 Trades 总数（持久化到文件，跨重启继承）
ACCOUNT_TARGET_TRADES: dict[str, int] = {}
_TARGET_TRADES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "account_targets.json")


def _load_target_trades():
    global ACCOUNT_TARGET_TRADES
    if os.path.exists(_TARGET_TRADES_FILE):
        try:
            with open(_TARGET_TRADES_FILE, "r", encoding="utf-8") as f:
                data = _json.load(f)
            saved_date = data.get("_date", "")
            today_str = datetime.now().strftime("%Y-%m-%d")
            if saved_date != today_str:
                os.remove(_TARGET_TRADES_FILE)
                ACCOUNT_TARGET_TRADES = {}
                return
            data.pop("_date", None)
            ACCOUNT_TARGET_TRADES = data
        except Exception:
            ACCOUNT_TARGET_TRADES = {}


def _save_target_trades():
    try:
        data = dict(ACCOUNT_TARGET_TRADES)
        data["_date"] = datetime.now().strftime("%Y-%m-%d")
        with open(_TARGET_TRADES_FILE, "w", encoding="utf-8") as f:
            _json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass


_load_target_trades()

# 实时状态追踪：account_id → 状态字典（供 Web 前端展示，持久化到文件）
TASK_STATUS: dict[str, dict] = {}
_TASK_STATUS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "task_status.json")
_task_status_dirty = False


def _load_task_status():
    global TASK_STATUS
    if os.path.exists(_TASK_STATUS_FILE):
        try:
            with open(_TASK_STATUS_FILE, "r", encoding="utf-8") as f:
                data = _json.load(f)
            saved_date = data.get("_date", "")
            today_str = datetime.now().strftime("%Y-%m-%d")
            if saved_date != today_str:
                os.remove(_TASK_STATUS_FILE)
                TASK_STATUS = {}
                return
            data.pop("_date", None)
            TASK_STATUS = data
        except Exception:
            TASK_STATUS = {}


def _save_task_status():
    try:
        data = dict(TASK_STATUS)
        data["_date"] = datetime.now().strftime("%Y-%m-%d")
        with open(_TASK_STATUS_FILE, "w", encoding="utf-8") as f:
            _json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass


_load_task_status()

# ─── 失败截图开关（由 runner 传入） ─────────────────────
SCREENSHOT_ON_FAILURE = False
_SCREENSHOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "screenshots")


async def _take_failure_screenshot(page, account_id: str, label: str):
    """失败时自动截图，保存到 screenshots/{窗口号}/ 文件夹"""
    if not SCREENSHOT_ON_FAILURE:
        return
    try:
        acct_dir = os.path.join(_SCREENSHOT_DIR, account_id)
        os.makedirs(acct_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_label = label.replace("/", "-").replace("\\", "-").replace(" ", "_")
        filename = f"{safe_label}_{ts}.png"
        filepath = os.path.join(acct_dir, filename)
        await page.screenshot(path=filepath, full_page=False)
        log(account_id, f"【截图】已保存: {account_id}/{filename}")
    except Exception as e:
        log(account_id, f"【截图】截图失败: {e}")


# ─── 定时截图（全程录制）开关 ─────────────────────────
TIMELAPSE_ENABLED = False
TIMELAPSE_INTERVAL = 3


class TimelapseRecorder:
    """后台定时截图，成功删除、失败保留"""

    def __init__(self, page: Page, account_id: str):
        self.page = page
        self.account_id = account_id
        self.folder = os.path.join(_SCREENSHOT_DIR, account_id)
        self._task: asyncio.Task = None
        self._running = False
        self._count = 0

    async def start(self):
        if not TIMELAPSE_ENABLED or self._running:
            return
        self._running = True
        self._count = 0
        os.makedirs(self.folder, exist_ok=True)
        self._task = asyncio.create_task(self._loop())
        log(self.account_id, f"【录制】定时截图已启动（间隔 {TIMELAPSE_INTERVAL}s）")

    async def _loop(self):
        while self._running:
            try:
                ts = datetime.now().strftime("%H%M%S")
                filepath = os.path.join(self.folder, f"tl_{self._count:05d}_{ts}.png")
                await self.page.screenshot(path=filepath, full_page=False)
                self._count += 1
            except Exception:
                pass
            await asyncio.sleep(TIMELAPSE_INTERVAL)

    async def stop(self, success: bool):
        if not self._running:
            return
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if success:
            try:
                import shutil
                shutil.rmtree(self.folder, ignore_errors=True)
                log(self.account_id, f"【录制】任务成功，已删除 {self._count} 张截图")
            except Exception:
                pass
        else:
            log(self.account_id, f"【录制】任务失败，保留 {self._count} 张截图: {self.folder}")


def _update_status(account_id: str, **fields):
    """更新账号的实时运行状态"""
    if account_id not in TASK_STATUS:
        TASK_STATUS[account_id] = {
            "name": account_id, "status": "waiting",
            "initial_trades": -1, "target_trades": -1,
            "current_trades": -1, "bets_completed": 0,
            "bets_target": 0, "round": 0, "error": "", "updated_at": "",
        }
    TASK_STATUS[account_id].update(fields)
    TASK_STATUS[account_id]["updated_at"] = datetime.now().strftime("%H:%M:%S")
    _save_task_status()


def _is_wallet_popup(url: str) -> bool:
    """判断一个页面 URL 是否为 OKX 钱包弹窗（notification.html）"""
    return "chrome-extension://" in url and "notification.html" in url


# ════════════════════════════════════════════════════════
#  工具：RPC 恢复等待
# ════════════════════════════════════════════════════════

FATAL_ERROR_SEL = "span.text-danger"
FATAL_ERROR_TEXT = "An issue was detected"
CONNECTION_FAILED_TEXT = "Connection failed"
CLAIMING_CHAIN_SEL = "span:text-is('Claiming chain...')"


async def is_fatal_error(page: Page) -> bool:
    """检测不可恢复的 RPC 错误"""
    try:
        loc = page.locator(FATAL_ERROR_SEL)
        if await loc.count() > 0:
            text = await loc.first.inner_text(timeout=2000)
            if FATAL_ERROR_TEXT in text:
                return True
    except Exception:
        pass
    return False


async def is_connection_failed(page: Page) -> bool:
    """检测 Connection failed 错误"""
    try:
        loc = page.locator("span.text-danger:has-text('Connection failed')")
        return await loc.count() > 0
    except Exception:
        return False


async def wait_rpc_recovery(
    page: Page, account_id: str,
    context: BrowserContext = None,
    max_wait: int = 120, max_refresh: int = 3,
) -> bool:
    """
    页面跳转后等待 RPC 恢复。
    - Claiming chain... → 等最多 max_wait 秒，同时处理钱包弹窗
    - Connection failed → 刷新页面（最多 3 次，间隔 60s）
    - "An issue was detected..." → 不可恢复，返回 False
    返回 True 表示恢复正常，False 表示不可恢复需跳过。
    """
    for refresh_round in range(max_refresh + 1):
        if await is_fatal_error(page):
            log(account_id, "检测到 RPC 致命错误（local site storage），跳过该窗口")
            return False

        # 检查 Connection failed
        if await is_connection_failed(page):
            if refresh_round < max_refresh:
                log(account_id, f"检测到 Connection failed，60s 后刷新（第 {refresh_round+1}/{max_refresh} 次）")
                await asyncio.sleep(60)
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=30000)
                except Exception:
                    pass
                await asyncio.sleep(5)
                continue
            else:
                log(account_id, f"Connection failed 刷新 {max_refresh} 次后仍未恢复，跳过")
                return False

        claiming = page.locator(CLAIMING_CHAIN_SEL)
        if await claiming.count() == 0:
            return True

        log(account_id, f"检测到 Claiming chain...，等待恢复（第 {refresh_round + 1} 轮，最长 {max_wait}s）")
        for tick in range(max_wait):
            if STOP_FLAG:
                return False
            if await is_fatal_error(page):
                log(account_id, "等待中检测到 RPC 致命错误，跳过该窗口")
                return False

            # Claiming chain 期间处理钱包弹窗
            if context:
                for p in context.pages:
                    try:
                        if _is_wallet_popup(p.url or ""):
                            log(account_id, f"Claiming 期间发现钱包弹窗: {p.url[-60:]}")
                            try:
                                await p.wait_for_load_state("domcontentloaded", timeout=5000)
                            except Exception:
                                pass
                            await asyncio.sleep(2)
                            await _click_wallet_button(p, account_id)
                            log(account_id, "Claiming 期间弹窗已确认")
                            await asyncio.sleep(2)
                    except Exception:
                        continue

            if await claiming.count() == 0:
                # 恢复后检查是否变成了 Connection failed
                if await is_connection_failed(page):
                    log(account_id, "Claiming 结束后出现 Connection failed")
                    break
                log(account_id, "RPC 恢复正常")
                return True
            await asyncio.sleep(1)

        # Claiming 超时或 Connection failed，刷新重试
        if refresh_round < max_refresh:
            if await is_connection_failed(page):
                log(account_id, f"Connection failed，60s 后刷新（第 {refresh_round+1}/{max_refresh} 次）")
                await asyncio.sleep(60)
            else:
                log(account_id, f"Claiming chain 等待 {max_wait}s 超时，刷新页面重试...")
            try:
                await page.reload(wait_until="domcontentloaded", timeout=30000)
            except Exception:
                pass
            await asyncio.sleep(5)
        else:
            log(account_id, f"刷新 {max_refresh} 次后仍未恢复，跳过该窗口")
            return False

    return False


# ════════════════════════════════════════════════════════
#  工具：手动处理钱包弹窗（仅登录阶段使用）
# ════════════════════════════════════════════════════════

async def handle_wallet_popups_manual(
    context: BrowserContext, account_id: str, timeout: int = 30,
) -> bool:
    """
    主动搜索并处理钱包弹窗（仅在后台 handler 被禁用时使用）。
    支持多步确认。
    """
    for _ in range(timeout):
        for p in context.pages:
            try:
                url = p.url or ""
            except Exception:
                continue
            if not _is_wallet_popup(url):
                continue

            log(account_id, f"发现钱包弹窗: {url[-60:]}")
            try:
                await p.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass
            await asyncio.sleep(random.uniform(1.5, 2.5))
            try:
                clicked = await _click_wallet_button(p, account_id)
                if clicked:
                    await asyncio.sleep(2)
                    for _ in range(5):
                        still_open = False
                        for p2 in context.pages:
                            try:
                                u2 = p2.url or ""
                            except Exception:
                                continue
                            if _is_wallet_popup(u2):
                                await asyncio.sleep(1.5)
                                await _click_wallet_button(p2, account_id)
                                still_open = True
                                break
                        if not still_open:
                            break
                        await asyncio.sleep(1)
                    return True
            except Exception as e:
                log(account_id, f"处理弹窗异常: {e}")
            return False
        await asyncio.sleep(1)
    return False


# ════════════════════════════════════════════════════════
#  工具：结算状态
# ════════════════════════════════════════════════════════

async def is_settling(page: Page) -> bool:
    try:
        return await page.locator("svg.lucide-loader-circle.animate-spin").count() > 0
    except Exception:
        return False


async def wait_settlement_done(
    page: Page, account_id: str,
    context: BrowserContext = None,
    timeout: int = 120,
) -> bool:
    """等待结算完成。超时后自动刷新页面+RPC恢复。"""
    if not await is_settling(page):
        return True
    log(account_id, "市场结算中，等待...")
    for _ in range(timeout):
        if STOP_FLAG:
            return False
        if not await is_settling(page):
            log(account_id, "结算完成")
            return True
        await asyncio.sleep(1)

    log(account_id, f"结算超时（{timeout}s），刷新页面恢复...")
    await _take_failure_screenshot(page, account_id, "settlement_timeout")
    try:
        await page.reload(wait_until="domcontentloaded", timeout=30000)
    except Exception:
        pass
    await asyncio.sleep(5)
    if context:
        if not await wait_rpc_recovery(page, account_id, context):
            return False
    return True


# ════════════════════════════════════════════════════════
#  工具：页面卡住检测
# ════════════════════════════════════════════════════════

async def is_page_stuck(page: Page) -> bool:
    """
    检测页面是否卡住。正常页面应包含：
    - canvas（价格图表）
    - svg.lucide-flag（旗帜图标）
    如果都不存在，说明页面卡住了。
    """
    try:
        has_canvas = await page.locator("canvas").count() > 0
        has_flag = await page.locator("svg.lucide-flag").count() > 0
        return not has_canvas and not has_flag
    except Exception:
        return True


async def recover_from_stuck(
    page: Page, account_id: str, current_market: str = "",
) -> bool:
    """切换到其他市场来恢复卡住的页面，最多尝试所有市场"""
    log(account_id, "页面卡住，尝试切换市场恢复...")
    candidates = [m for m in MARKETS if m != current_market]
    random.shuffle(candidates)
    if current_market:
        candidates.append(current_market)

    for m in candidates:
        await switch_market(page, account_id, m)
        await asyncio.sleep(3)
        if not await is_page_stuck(page):
            log(account_id, f"切换到 {m} 后页面恢复正常")
            return True

    log(account_id, "所有市场均卡住，尝试刷新页面...")
    try:
        await page.reload(wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(5)
        if not await is_page_stuck(page):
            log(account_id, "刷新后页面恢复正常")
            return True
    except Exception:
        pass
    log(account_id, "页面无法恢复")
    return False


# ════════════════════════════════════════════════════════
#  工具：下注成功标志
# ════════════════════════════════════════════════════════

async def get_card_glass_count(page: Page) -> int:
    """获取当前页面上 card-glass 的数量"""
    try:
        return await page.locator("div.card-glass").count()
    except Exception:
        return 0


async def check_bet_success(page: Page, baseline_count: int = 0) -> bool:
    """
    检测下注成功：card-glass 数量比 baseline 增加。
    baseline 是点击下注按钮前记录的数量，避免误判旧卡片。
    """
    try:
        current = await page.locator("div.card-glass").count()
        if current > baseline_count:
            return True
    except Exception:
        pass
    return False


# ════════════════════════════════════════════════════════
#  工具：池子余额
# ════════════════════════════════════════════════════════

async def wait_countdown(page: Page, account_id: str, timeout: int = 90):
    """等待页面倒计时归零。倒计时在 span.text-foreground-muted 中显示数字。"""
    countdown_sel = "span.text-foreground-muted"
    loc = page.locator(countdown_sel)

    # 先检查是否有倒计时存在
    for _ in range(5):
        if await loc.count() > 0:
            break
        await asyncio.sleep(1)
    else:
        return

    last_value = ""
    for _ in range(timeout):
        if STOP_FLAG:
            return
        try:
            if await loc.count() == 0:
                log(account_id, "倒计时结束（元素消失）")
                return
            text = (await loc.first.inner_text(timeout=2000)).strip()
            if text != last_value:
                if text.isdigit():
                    val = int(text)
                    if val <= 0:
                        log(account_id, "倒计时归零")
                        return
                    if last_value == "" or val % 10 == 0:
                        log(account_id, f"倒计时: {val}s")
                last_value = text
        except Exception:
            pass
        await asyncio.sleep(1)
    log(account_id, "等待倒计时超时")


async def get_countdown_value(page: Page) -> int:
    """读取当前倒计时秒数，无倒计时返回 -1"""
    try:
        loc = page.locator("span.text-foreground-muted")
        if await loc.count() > 0:
            text = (await loc.first.inner_text(timeout=2000)).strip()
            if text.isdigit():
                return int(text)
    except Exception:
        pass
    return -1


async def get_pool_balance(page: Page) -> str:
    try:
        bal = page.locator("span.text-base.font-bold.text-foreground")
        if await bal.count() > 0:
            return (await bal.first.inner_text(timeout=2000)).strip()
    except Exception:
        pass
    return ""


# ════════════════════════════════════════════════════════
#  选择 1 minute 市场
# ════════════════════════════════════════════════════════

async def select_1_minute(page: Page, account_id: str, max_wait: int = 15) -> bool:
    """选择 1 minute 市场，等待按钮加载最多 max_wait 秒"""
    btn_loc = page.locator("button:text-is('1 minute')")

    for attempt in range(max_wait):
        try:
            if await btn_loc.count() > 0:
                await btn_loc.first.click(timeout=5000)
                log(account_id, "已选择 1 minute 市场")
                await asyncio.sleep(2)
                return True
        except Exception:
            pass

        # JS 兜底
        try:
            ok = await page.evaluate("""() => {
                for (const btn of document.querySelectorAll('button')) {
                    if (btn.textContent.trim() === '1 minute') {
                        btn.click();
                        return true;
                    }
                }
                return false;
            }""")
            if ok:
                log(account_id, "已选择 1 minute 市场 (JS)")
                await asyncio.sleep(2)
                return True
        except Exception:
            pass

        if attempt < max_wait - 1:
            if attempt == 0:
                log(account_id, "1 minute 按钮未加载，等待中...")
            await asyncio.sleep(1)

    log(account_id, "1 minute 按钮等待超时")
    return False


# ════════════════════════════════════════════════════════
#  切换市场 + 重新选 1 minute
# ════════════════════════════════════════════════════════

async def switch_market(page: Page, account_id: str, market: str) -> bool:
    try:
        tab = page.locator(f"img[alt='{market} icon']")
        if await tab.count() > 0:
            await tab.first.click(timeout=5000)
        else:
            await page.locator(f"text={market}").first.click(timeout=5000)
        log(account_id, f"切换到 {market}")
        await asyncio.sleep(2)
    except Exception as e:
        log(account_id, f"切换到 {market} 失败: {e}")
        return False
    ok = await select_1_minute(page, account_id)
    if not ok:
        # 1 minute 按钮加载失败，尝试切到另一个市场再回来
        other = [m for m in MARKETS if m != market]
        if other:
            alt = random.choice(other)
            log(account_id, f"1 minute 不可用，先切到 {alt} 再切回 {market}")
            try:
                alt_tab = page.locator(f"img[alt='{alt} icon']")
                if await alt_tab.count() > 0:
                    await alt_tab.first.click(timeout=5000)
                else:
                    await page.locator(f"text={alt}").first.click(timeout=5000)
                await asyncio.sleep(2)
                tab2 = page.locator(f"img[alt='{market} icon']")
                if await tab2.count() > 0:
                    await tab2.first.click(timeout=5000)
                else:
                    await page.locator(f"text={market}").first.click(timeout=5000)
                await asyncio.sleep(2)
            except Exception:
                pass
            ok = await select_1_minute(page, account_id)
    return ok


# ════════════════════════════════════════════════════════
#  清除浏览器缓存（Connection failed 反复出现时使用）
# ════════════════════════════════════════════════════════

async def _clear_browser_cache(page: Page, context: BrowserContext, account_id: str):
    """清除 localStorage、sessionStorage 和 Cookie"""
    try:
        await page.evaluate("try { localStorage.clear(); sessionStorage.clear(); } catch(e) {}")
        log(account_id, "已清除 localStorage / sessionStorage")
    except Exception as e:
        log(account_id, f"清除 storage 失败: {e}")
    try:
        await context.clear_cookies()
        log(account_id, "已清除 Cookies")
    except Exception as e:
        log(account_id, f"清除 Cookies 失败: {e}")


# ════════════════════════════════════════════════════════
#  登录流程（禁用后台 handler，手动处理弹窗）
# ════════════════════════════════════════════════════════

async def login(
    page: Page, context: BrowserContext, account_id: str,
    popup_handler: WalletPopupHandler,
) -> bool:
    """
    初始化：打开 History 页面 → 钱包解锁/签名 → 读 Trades 基线
    登录期间禁用后台 handler 避免冲突。
    """
    popup_handler.enabled = False

    try:
        # 直接进 History 页面（同时触发钱包连接 + 读取基线）
        history_url = f"{DAPP_URL}/history?market=BTC&duration=1"

        for attempt in range(3):
            try:
                await page.goto(history_url, wait_until="domcontentloaded", timeout=30000)
                break
            except Exception as e:
                if attempt < 2:
                    log(account_id, f"导航失败，重试 ({attempt+1}/3)...")
                    await asyncio.sleep(3)
                else:
                    log(account_id, f"导航彻底失败: {e}")
                    return False

        log(account_id, "History 页面已打开，等待加载...")
        await asyncio.sleep(8)

        # ── 预检：先处理可能已存在的钱包弹窗（如解锁弹窗） ──
        for pre_check in range(3):
            wallet_page = None
            for p in context.pages:
                try:
                    u = p.url or ""
                except Exception:
                    continue
                if _is_wallet_popup(u):
                    wallet_page = p
                    break

            if not wallet_page:
                break

            log(account_id, f"发现已有钱包弹窗: {wallet_page.url[-60:]}")
            try:
                await wallet_page.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass
            await asyncio.sleep(2)

            has_pwd = False
            for frame in wallet_page.frames:
                try:
                    if await frame.locator('input[type="password"]').count() > 0:
                        has_pwd = True
                        break
                except Exception:
                    continue

            if has_pwd:
                log(account_id, "弹窗含密码框，执行解锁...")
                await _find_and_fill_password(wallet_page, context, account_id, OKX_DEFAULT_PASSWORD)
                await asyncio.sleep(0.5)
                await _click_unlock_button(wallet_page, context, account_id)
                await asyncio.sleep(3)
                log(account_id, "钱包预解锁完成")
            else:
                clicked = await _click_wallet_button(wallet_page, account_id)
                if clicked:
                    log(account_id, "已处理预弹窗")
                    await asyncio.sleep(3)
                else:
                    break

        # 解锁后等待页面状态更新
        await asyncio.sleep(3)

        # ── 主登录循环：处理 Connect Wallet / Connection failed / 加载等待 ──
        # Connection failed 可能在任意阶段出现，点 Retry 后状态可能回到 Connect Wallet
        login_done = False
        conn_fail_count = 0
        cache_cleared_count = 0
        for main_attempt in range(20):

            # ── 连续 5 次 Connection failed → 清缓存重新加载 ──
            if conn_fail_count > 0 and conn_fail_count % 5 == 0 and conn_fail_count // 5 > cache_cleared_count:
                cache_cleared_count += 1
                if cache_cleared_count <= 2:
                    log(account_id, f"Connection failed 已达 {conn_fail_count} 次，清除缓存重新加载（第 {cache_cleared_count}/2 次）...")
                    await _take_failure_screenshot(page, account_id, f"conn_fail_{conn_fail_count}x")
                    await _clear_browser_cache(page, context, account_id)
                    try:
                        await page.goto(history_url, wait_until="domcontentloaded", timeout=30000)
                    except Exception:
                        pass
                    await asyncio.sleep(8)
                    continue

            # ── Phase A: 检查 Connection failed → 点 Retry ──
            retry_btn = page.locator("span.text-danger button")
            if await retry_btn.count() > 0:
                conn_fail_count += 1
                log(account_id, f"检测到 Connection failed，点击 Retry...（第 {conn_fail_count} 次）")
                try:
                    await retry_btn.first.click(timeout=5000)
                except Exception:
                    pass
                await asyncio.sleep(3)
                continue

            # ── Phase B: 检测是否需要 Connect Wallet ──
            connect_btn = page.locator("button:has-text('Connect Wallet')")
            if await connect_btn.count() > 0:
                popup_handler.enabled = False
                okx_selected = False
                for connect_try in range(5):
                    connect_btn = page.locator("button:has-text('Connect Wallet')")
                    if await connect_btn.count() == 0:
                        okx_selected = True
                        break

                    log(account_id, f"检测到 Connect Wallet 按钮，开始连接...（第 {connect_try+1} 次）")
                    await connect_btn.first.click(timeout=5000)
                    await asyncio.sleep(3)

                    okx_option = page.locator("button.wallet-list-item__tile:has(img[alt='okxwallet'])")
                    wallet_appeared = False
                    for _ in range(30):
                        if await okx_option.count() > 0:
                            break
                        for p in context.pages:
                            try:
                                if _is_wallet_popup(p.url or ""):
                                    wallet_appeared = True
                                    break
                            except Exception:
                                continue
                        if wallet_appeared:
                            break
                        await asyncio.sleep(0.5)

                    if wallet_appeared:
                        log(account_id, "Connect Wallet 后直接弹出钱包弹窗，跳过选择列表")
                        for p in context.pages:
                            try:
                                if _is_wallet_popup(p.url or ""):
                                    await p.wait_for_load_state("domcontentloaded", timeout=5000)
                                    await asyncio.sleep(2)
                                    has_pwd = False
                                    for frame in p.frames:
                                        try:
                                            if await frame.locator('input[type="password"]').count() > 0:
                                                has_pwd = True
                                                break
                                        except Exception:
                                            continue
                                    if has_pwd:
                                        await _find_and_fill_password(p, context, account_id, OKX_DEFAULT_PASSWORD)
                                        await asyncio.sleep(0.5)
                                        await _click_unlock_button(p, context, account_id)
                                        log(account_id, "弹窗钱包解锁完成")
                                    else:
                                        await _click_wallet_button(p, account_id)
                                        log(account_id, "弹窗已确认")
                                    await asyncio.sleep(3)
                                    break
                            except Exception:
                                continue
                        okx_selected = True
                        break

                    if await okx_option.count() > 0:
                        await okx_option.first.click(timeout=5000)
                        log(account_id, "已选择 OKX Wallet")
                        await asyncio.sleep(3)
                        okx_selected = True
                        break

                    okx_text = page.locator("text=OKX Wallet")
                    if await okx_text.count() > 0:
                        await okx_text.first.click(timeout=5000)
                        log(account_id, "已选择 OKX Wallet (文本匹配)")
                        await asyncio.sleep(3)
                        okx_selected = True
                        break

                    log(account_id, "OKX Wallet 未加载，刷新页面重试...")
                    await page.keyboard.press("Escape")
                    await asyncio.sleep(1)
                    try:
                        await page.reload(wait_until="domcontentloaded", timeout=30000)
                    except Exception:
                        pass
                    await asyncio.sleep(8)

                popup_handler.enabled = True
                if not okx_selected:
                    log(account_id, "5 次尝试后仍未找到 OKX Wallet，跳过此账号")
                    return False

                # 处理连接后的钱包弹窗（解锁/连接/确认）
                for round_num in range(5):
                    wallet_page = None
                    for p in context.pages:
                        try:
                            u = p.url or ""
                        except Exception:
                            continue
                        if _is_wallet_popup(u):
                            wallet_page = p
                            break

                    if not wallet_page:
                        if round_num == 0:
                            await asyncio.sleep(5)
                            continue
                        break

                    try:
                        log(account_id, f"发现钱包弹窗: {wallet_page.url[-60:]}")
                        try:
                            await wallet_page.wait_for_load_state("domcontentloaded", timeout=5000)
                        except Exception:
                            pass
                        await asyncio.sleep(2)

                        has_pwd = False
                        for frame in wallet_page.frames:
                            try:
                                if await frame.locator('input[type="password"]').count() > 0:
                                    has_pwd = True
                                    break
                            except Exception:
                                continue

                        if has_pwd:
                            log(account_id, "弹窗含密码框，执行解锁...")
                            await _find_and_fill_password(wallet_page, context, account_id, OKX_DEFAULT_PASSWORD)
                            await asyncio.sleep(0.5)
                            await _click_unlock_button(wallet_page, context, account_id)
                            await asyncio.sleep(3)
                            log(account_id, f"钱包解锁弹窗已处理（第 {round_num+1} 轮）")
                        else:
                            clicked = await _click_wallet_button(wallet_page, account_id)
                            if clicked:
                                log(account_id, f"钱包弹窗已处理（第 {round_num+1} 轮）")
                                await asyncio.sleep(3)
                            else:
                                break
                    except Exception:
                        log(account_id, f"弹窗处理中页面已关闭（第 {round_num+1} 轮），继续")
                        await asyncio.sleep(2)

                # 检测 Select Ethereum network
                await asyncio.sleep(2)
                try:
                    net_btn = page.locator("button[data-testid='SelectNetworkButton']")
                    if await net_btn.count() > 0:
                        await net_btn.first.click(timeout=5000)
                        log(account_id, "已点击 Select Ethereum network")
                        await asyncio.sleep(3)
                    else:
                        log(account_id, "未检测到 Select Network 按钮，跳过")
                except Exception as e:
                    log(account_id, f"Select Network 处理异常: {e}")

            elif main_attempt == 0:
                log(account_id, "已登录过，跳过 Connect Wallet 流程")

            # ── Phase C: 统一等待（弹窗 / Claiming chain / Connection failed / 转圈） ──
            await asyncio.sleep(2)
            claiming_loc = page.locator("span:text-is('Claiming chain...')")
            spinner_loc = page.locator("svg.animate-spin")
            conn_fail_loc = page.locator("span.text-danger button")
            popup_count = 0
            claiming_logged = False
            spinner_logged = False
            need_outer_retry = False

            for tick in range(180):
                # 钱包弹窗
                wallet_page = None
                for p in context.pages:
                    try:
                        u = p.url or ""
                    except Exception:
                        continue
                    if _is_wallet_popup(u):
                        wallet_page = p
                        break

                if wallet_page:
                    popup_count += 1
                    try:
                        log(account_id, f"发现钱包弹窗: {wallet_page.url[-60:]}")
                        try:
                            await wallet_page.wait_for_load_state("domcontentloaded", timeout=5000)
                        except Exception:
                            pass
                        await asyncio.sleep(2)

                        has_pwd = False
                        for frame in wallet_page.frames:
                            try:
                                if await frame.locator('input[type="password"]').count() > 0:
                                    has_pwd = True
                                    break
                            except Exception:
                                continue

                        if has_pwd:
                            log(account_id, "弹窗含密码框，执行解锁...")
                            await _find_and_fill_password(wallet_page, context, account_id, OKX_DEFAULT_PASSWORD)
                            await asyncio.sleep(0.5)
                            await _click_unlock_button(wallet_page, context, account_id)
                        else:
                            await _click_wallet_button(wallet_page, account_id)

                        log(account_id, f"弹窗已处理（第 {popup_count} 个）")
                    except Exception:
                        log(account_id, f"弹窗处理中页面已关闭（第 {popup_count} 个），继续")
                    await asyncio.sleep(3)
                    continue

                # Claiming chain
                if await claiming_loc.count() > 0:
                    if not claiming_logged:
                        log(account_id, "检测到 Claiming chain...，等待完成")
                        claiming_logged = True
                    await asyncio.sleep(1)
                    continue

                # Connection failed → 点 Retry，回外层重新判断状态
                if await conn_fail_loc.count() > 0:
                    conn_fail_count += 1
                    log(account_id, f"检测到 Connection failed，点击 Retry...（第 {conn_fail_count} 次）")
                    try:
                        await conn_fail_loc.first.click(timeout=5000)
                    except Exception:
                        pass
                    await asyncio.sleep(3)
                    need_outer_retry = True
                    break

                # 转圈（加载中）
                if await spinner_loc.count() > 0:
                    if not spinner_logged:
                        log(account_id, "页面加载中（转圈），等待...")
                        spinner_logged = True
                    await asyncio.sleep(1)
                    continue

                # 没弹窗、没 Claiming chain、没 Connection failed、没转圈 → 登录完成
                if tick > 3:
                    if claiming_logged:
                        log(account_id, "Claiming chain 完成")
                    if spinner_logged:
                        log(account_id, "页面加载完成")
                    login_done = True
                    break
                await asyncio.sleep(1)
            else:
                if claiming_logged:
                    log(account_id, "Claiming chain 等待超时，继续执行")
                if spinner_logged:
                    log(account_id, "页面加载等待超时，继续执行")
                login_done = True

            if need_outer_retry:
                continue
            if login_done:
                break

        if not login_done:
            log(account_id, f"登录重试 {main_attempt+1} 次后仍失败")
            await _take_failure_screenshot(page, account_id, "login_all_retries_failed")
            return False

        await asyncio.sleep(2)

        # ── 登录后验证：Connect Wallet 按钮应该消失 ──
        connect_btn = page.locator("button:has-text('Connect Wallet')")
        if await connect_btn.count() > 0:
            log(account_id, "登录验证失败：Connect Wallet 按钮仍存在")
            await _take_failure_screenshot(page, account_id, "login_wallet_btn_still_exists")
            return False

        # ── 在 History 页面读取 Trades 基线 ──
        initial_trades = await get_trades_count(page, account_id)
        if initial_trades >= 0:
            log(account_id, f"登录完成，Trades 基线: {initial_trades}")
        else:
            log(account_id, "登录完成，无法读取 Trades 基线（可能页面未完全加载）")
            initial_trades = -1

        # 存入 page 对象供后续使用
        page._initial_trades = initial_trades
        return True
    finally:
        popup_handler.enabled = True


# ════════════════════════════════════════════════════════
#  单次下注（依赖后台 handler 自动签名）
# ════════════════════════════════════════════════════════

NO_POPUP_FAILURE = "no_popup"

async def place_single_bet(
    page: Page,
    context: BrowserContext,
    account_id: str,
    bet_number: int,
    target_bets: int = TARGET_BETS,
):
    """
    单次下注流程，返回值：
      True  — 下注成功
      False — 普通失败（有弹窗但未成功等）
      "no_popup" — 60s 内钱包弹窗完全没出现
    """

    # 0. 检测 RPC 致命错误
    if await is_fatal_error(page):
        log(account_id, "检测到 RPC 致命错误，跳过该窗口")
        await _take_failure_screenshot(page, account_id, "rpc_fatal")
        return False

    # 0.5 检测页面是否卡住
    if await is_page_stuck(page):
        await _take_failure_screenshot(page, account_id, "page_stuck")
        recovered = await recover_from_stuck(page, account_id)
        if not recovered:
            return False

    # 1. 检查池子余额
    balance = await get_pool_balance(page)
    if balance in ("0", "0.00", "0.000", ""):
        log(account_id, f"池子余额为 '{balance}'，切换市场...")
        for m in random.sample(MARKETS, len(MARKETS)):
            await switch_market(page, account_id, m)
            await asyncio.sleep(2)
            if not await wait_rpc_recovery(page, account_id, context):
                return False
            new_bal = await get_pool_balance(page)
            if new_bal not in ("0", "0.00", "0.000", ""):
                log(account_id, f"{m} 池子余额: {new_bal}")
                break

    # 2. 等待结算完成（超时会自动刷新+RPC恢复）
    if not await wait_settlement_done(page, account_id, context):
        return False

    # 3. 确认按钮可用
    for wait in range(15):
        try:
            h = page.locator("button.btn-higher")
            l = page.locator("button.btn-lower")
            if (await h.count() > 0 and await l.count() > 0
                    and await h.get_attribute("disabled") is None
                    and await l.get_attribute("disabled") is None):
                break
        except Exception:
            pass
        if wait == 14:
            connect_btn = page.locator("button:has-text('Connect Wallet')")
            if await connect_btn.count() > 0:
                log(account_id, "HIGHER/LOWER 不可用：钱包已掉线")
            else:
                log(account_id, "HIGHER/LOWER 长时间不可用")
            await _take_failure_screenshot(page, account_id, "btn_unavailable")
            return False
        await asyncio.sleep(1)

    # 3.5 倒计时 < 8 秒则等新一轮
    cd = await get_countdown_value(page)
    if 0 < cd < 8:
        log(account_id, f"倒计时仅剩 {cd}s，等新一轮...")
        await wait_countdown(page, account_id)
        await asyncio.sleep(2)
        if not await wait_settlement_done(page, account_id, context):
            return False
        for wait2 in range(15):
            try:
                h2 = page.locator("button.btn-higher")
                l2 = page.locator("button.btn-lower")
                if (await h2.count() > 0 and await l2.count() > 0
                        and await h2.get_attribute("disabled") is None
                        and await l2.get_attribute("disabled") is None):
                    break
            except Exception:
                pass
            if wait2 == 14:
                log(account_id, "新一轮 HIGHER/LOWER 不可用")
                await _take_failure_screenshot(page, account_id, "newround_btn_unavailable")
                return False
            await asyncio.sleep(1)

    # 4. 记录当前 card-glass 数量（下注前基线）
    baseline = await get_card_glass_count(page)

    # 5. 随机方向并点击
    direction = random.choice(["HIGHER", "LOWER"])
    btn_cls = "btn-higher" if direction == "HIGHER" else "btn-lower"
    try:
        await page.locator(f"button.{btn_cls}").first.click(timeout=5000)
        log(account_id, f"[{bet_number}/{target_bets}] 点击 {direction}")
    except Exception as e:
        log(account_id, f"点击 {direction} 失败: {e}")
        await _take_failure_screenshot(page, account_id, f"click_{direction}_failed")
        return False

    # 6. 等待成功标志 + 跟踪弹窗是否出现
    log(account_id, f"[{bet_number}/{target_bets}] 等待钱包自动签名 + 成功标志...")
    success = False
    popup_seen = False
    for i in range(60):
        if STOP_FLAG:
            return False
        if await check_bet_success(page, baseline):
            success = True
            break
        if not popup_seen:
            for p in context.pages:
                try:
                    if _is_wallet_popup(p.url or ""):
                        popup_seen = True
                        break
                except Exception:
                    continue
        await asyncio.sleep(1)

    if success:
        log(account_id, f"[{bet_number}/{target_bets}] 下注成功")
        return True

    if not popup_seen:
        log(account_id, f"[{bet_number}/{target_bets}] 60s 内钱包弹窗未出现")
        await _take_failure_screenshot(page, account_id, f"no_popup_bet{bet_number}")
        return NO_POPUP_FAILURE

    log(account_id, f"[{bet_number}/{target_bets}] 有弹窗但 60s 内未检测到成功标志")
    await _take_failure_screenshot(page, account_id, f"popup_no_success_bet{bet_number}")
    return False


# ════════════════════════════════════════════════════════
#  钱包重连（下注期间掉线时使用）
# ════════════════════════════════════════════════════════

async def reconnect_wallet(
    page: Page, context: BrowserContext, account_id: str,
    popup_handler: WalletPopupHandler,
) -> bool:
    """刷新后检测钱包是否掉线，如掉线则重新连接。"""
    connect_btn = page.locator("button:has-text('Connect Wallet')")
    if await connect_btn.count() == 0:
        return True

    log(account_id, "检测到钱包掉线，重新连接...")
    popup_handler.enabled = False
    try:
        try:
            await connect_btn.first.click(timeout=5000)
            await asyncio.sleep(2)
        except Exception:
            return False

        okx = page.locator("button:has-text('OKX Wallet'), img[alt='OKX Wallet']")
        for _ in range(10):
            if await okx.count() > 0:
                break
            await asyncio.sleep(0.5)
        if await okx.count() > 0:
            await okx.first.click(timeout=5000)
            log(account_id, "已选择 OKX Wallet（重连）")
            await asyncio.sleep(3)

        # 处理钱包弹窗（解锁+签名）
        for tick in range(45):
            wallet_page = None
            for p in context.pages:
                try:
                    if _is_wallet_popup(p.url or ""):
                        wallet_page = p
                        break
                except Exception:
                    continue

            if not wallet_page:
                # 检查是否已连接成功
                if await connect_btn.count() == 0:
                    log(account_id, "钱包重连成功")
                    return True
                await asyncio.sleep(1)
                continue

            try:
                await wallet_page.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass
            await asyncio.sleep(2)

            has_pwd = False
            for frame in wallet_page.frames:
                try:
                    if await frame.locator('input[type="password"]').count() > 0:
                        has_pwd = True
                        break
                except Exception:
                    continue

            if has_pwd:
                await _find_and_fill_password(wallet_page, context, account_id, OKX_DEFAULT_PASSWORD)
                await asyncio.sleep(0.5)
                await _click_unlock_button(wallet_page, context, account_id)
                log(account_id, "钱包解锁完成（重连）")
            else:
                await _click_wallet_button(wallet_page, account_id)
                log(account_id, "钱包弹窗已处理（重连）")
            await asyncio.sleep(3)

        # 等加载完成
        spinner = page.locator("svg.animate-spin")
        for _ in range(30):
            if await spinner.count() == 0:
                break
            await asyncio.sleep(1)

        if await connect_btn.count() == 0:
            log(account_id, "钱包重连成功")
            return True
        else:
            log(account_id, "钱包重连失败")
            await _take_failure_screenshot(page, account_id, "reconnect_wallet_failed")
            return False
    finally:
        popup_handler.enabled = True


# ════════════════════════════════════════════════════════
#  下注主循环
# ════════════════════════════════════════════════════════

async def run_betting_loop(
    page: Page,
    context: BrowserContext,
    account_id: str,
    popup_handler: WalletPopupHandler,
    target_bets: int = TARGET_BETS,
) -> bool:
    completed = 0
    consecutive_failures = 0
    consecutive_no_popup = 0
    total_failures = 0
    max_total_failures = 10
    market_idx = 0

    log(account_id, f"开始下注，目标 {target_bets} 次")
    _update_status(account_id, status="betting", bets_target=target_bets, bets_completed=0)

    while completed < target_bets and not STOP_FLAG:
        if total_failures >= max_total_failures:
            log(account_id, f"累计失败 {total_failures} 次，放弃下注，等待第二轮重试")
            await _take_failure_screenshot(page, account_id, "max_failures_reached")
            _update_status(account_id, status="failed", error=f"累计失败{total_failures}次")
            return False

        # 连续 3 次无弹窗 → 页面卡住，刷新
        if consecutive_no_popup >= 3:
            log(account_id, f"连续 {consecutive_no_popup} 次无弹窗，判定页面卡住，刷新...")
            await _take_failure_screenshot(page, account_id, "stuck_no_popup_3x")
            popup_handler.enabled = False
            try:
                await page.reload(wait_until="domcontentloaded", timeout=30000)
            except Exception:
                pass
            await asyncio.sleep(5)
            if not await wait_rpc_recovery(page, account_id, context):
                popup_handler.enabled = True
                return False
            popup_handler.enabled = True
            if not await reconnect_wallet(page, context, account_id, popup_handler):
                await handle_wallet_popups_manual(context, account_id, timeout=15)
            await asyncio.sleep(3)
            consecutive_no_popup = 0
            consecutive_failures = 0
            continue

        if consecutive_failures >= 5:
            log(account_id, f"连续失败 {consecutive_failures} 次，刷新页面...")
            await _take_failure_screenshot(page, account_id, "consecutive_fail_5x")
            popup_handler.enabled = False
            try:
                await page.reload(wait_until="domcontentloaded", timeout=30000)
            except Exception:
                pass
            await asyncio.sleep(5)
            if not await wait_rpc_recovery(page, account_id, context):
                popup_handler.enabled = True
                return False
            popup_handler.enabled = True
            if not await reconnect_wallet(page, context, account_id, popup_handler):
                await handle_wallet_popups_manual(context, account_id, timeout=15)
            await asyncio.sleep(3)
            consecutive_failures = 0

        result = await place_single_bet(
            page, context, account_id, completed + 1, target_bets,
        )

        if result is True:
            completed += 1
            consecutive_failures = 0
            consecutive_no_popup = 0
            log(account_id, f"已完成 {completed}/{target_bets} 次下注")
            _update_status(account_id, bets_completed=completed, error="")

            # 切换到下一个市场
            if completed < target_bets:
                market_idx = (market_idx + 1) % len(MARKETS)
                next_market = MARKETS[market_idx]
                await switch_market(page, account_id, next_market)

                # 等待上一注的 card-glass 消失（上一轮结果清除）
                card_glass_loc = page.locator("div.card-glass")
                for cg_wait in range(30):
                    if await card_glass_loc.count() == 0:
                        break
                    await asyncio.sleep(1)

                if not await wait_rpc_recovery(page, account_id, context):
                    return False

        elif result == NO_POPUP_FAILURE:
            consecutive_no_popup += 1
            consecutive_failures += 1
            total_failures += 1
            log(account_id,
                f"无弹窗失败（连续无弹窗: {consecutive_no_popup}/3，累计: {total_failures}/{max_total_failures}）")
            _update_status(account_id, error=f"无弹窗{consecutive_no_popup}/3")
            await asyncio.sleep(3)
        else:
            consecutive_failures += 1
            consecutive_no_popup = 0
            total_failures += 1

            # 立即检测钱包掉线：不用等连续5次，直接刷新重连
            connect_btn = page.locator("button:has-text('Connect Wallet')")
            if await connect_btn.count() > 0:
                log(account_id, "检测到钱包掉线，立即刷新重连...")
                popup_handler.enabled = False
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=30000)
                except Exception:
                    pass
                await asyncio.sleep(5)
                if not await wait_rpc_recovery(page, account_id, context):
                    popup_handler.enabled = True
                    return False
                popup_handler.enabled = True
                if not await reconnect_wallet(page, context, account_id, popup_handler):
                    log(account_id, "钱包重连失败，放弃")
                    return False
                await asyncio.sleep(3)
                consecutive_failures = 0
                continue

            log(account_id,
                f"下注失败（连续: {consecutive_failures}，累计: {total_failures}/{max_total_failures}），等待后重试...")
            _update_status(account_id, error=f"连续失败{consecutive_failures}次")
            await asyncio.sleep(5)

    if STOP_FLAG:
        log(account_id, f"收到停止信号，已完成 {completed}/{target_bets} 次")
        return False

    log(account_id, f"全部 {target_bets} 次下注完成，等待 30s 让链上确认...")
    await asyncio.sleep(30)
    return True


# ════════════════════════════════════════════════════════
#  Leaderboard Trades 总数读取
# ════════════════════════════════════════════════════════

async def click_menu_button(page: Page, account_id: str) -> bool:
    """点击菜单按钮（三横线图标）"""
    try:
        menu_btn = page.locator("button:has(svg.lucide-menu)")
        if await menu_btn.count() == 0:
            menu_btn = page.locator("svg.lucide-menu").locator("..")
        if await menu_btn.count() > 0:
            await menu_btn.first.click(timeout=5000)
            log(account_id, "已点击菜单按钮")
            await asyncio.sleep(1.5)
            return True
        else:
            log(account_id, "未找到菜单按钮")
            return False
    except Exception as e:
        log(account_id, f"点击菜单按钮失败: {e}")
        return False


async def navigate_to_history(page: Page, account_id: str) -> bool:
    """从市场页面导航到 History 页面"""
    if not await click_menu_button(page, account_id):
        return False

    try:
        hist_link = page.locator("a[href*='/history']")
        if await hist_link.count() == 0:
            hist_link = page.locator("a:has(svg.lucide-clock)")
        if await hist_link.count() == 0:
            hist_link = page.locator("a:has-text('History')")
        if await hist_link.count() > 0:
            await hist_link.first.click(timeout=5000)
            log(account_id, "已点击 History")
            await asyncio.sleep(3)
            return True
        else:
            log(account_id, "未找到 History 链接")
            return False
    except Exception as e:
        log(account_id, f"点击 History 失败: {e}")
        return False


async def get_trades_count(page: Page, account_id: str) -> int:
    """读取 History 页面上 Trades 后面的数字，若检测到加载动画则等待其消失"""
    try:
        spinner = page.locator("i.animate-spinner-linear-spin")
        spinner_logged = False
        for _ in range(180):
            if await spinner.count() == 0:
                break
            if not spinner_logged:
                log(account_id, "Trades 数据加载中，等待...")
                spinner_logged = True
            await asyncio.sleep(1)
        else:
            log(account_id, "Trades 加载超时（180s）")
        if spinner_logged:
            await asyncio.sleep(1)

        no_predictions = page.locator("p.text-default-400:has-text('No predictions yet')")
        if await no_predictions.count() > 0:
            log(account_id, "当前 Trades 总数: 0（No predictions yet）")
            return 0

        trades_span = page.locator("span:has-text('Trades') >> span.font-semibold")
        for _ in range(10):
            if await trades_span.count() > 0:
                text = (await trades_span.first.inner_text(timeout=3000)).strip()
                if text.isdigit():
                    count = int(text)
                    log(account_id, f"当前 Trades 总数: {count}")
                    return count
            await asyncio.sleep(1)
    except Exception as e:
        log(account_id, f"读取 Trades 数量失败: {e}")
    return -1


async def navigate_back_to_market(page: Page, account_id: str) -> bool:
    """从 Leaderboard 返回市场页面"""
    market = random.choice(MARKETS)
    url = f"{DAPP_URL}/?market={market}"
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(5)
        log(account_id, "已返回市场页面")
        return True
    except Exception as e:
        log(account_id, f"返回市场页面失败: {e}")
        return False


# ════════════════════════════════════════════════════════
#  上传交易记录
# ════════════════════════════════════════════════════════

async def _wait_wallet_and_confirm(page: Page, context: BrowserContext, account_id: str, action: str, timeout: int = 30) -> bool:
    """等待钱包弹窗被后台 handler 自动处理，最多等 timeout 秒"""
    log(account_id, f"等待 {action} 签名确认...")
    no_action = page.locator("button:has-text('No Actions Needed')")
    for _ in range(timeout):
        if await no_action.count() > 0:
            return True
        await asyncio.sleep(1)
    return False


async def upload_trades(
    page: Page, context: BrowserContext, account_id: str,
) -> bool:
    """
    导航到 Leaderboard → Resolve Bets(签名) → Upload Trades(签名)
    → 等待变为 No Actions Needed
    """
    log(account_id, "开始上传交易记录...")

    if not await click_menu_button(page, account_id):
        return False

    try:
        lb_link = page.locator("a[href*='/leaderboard']")
        if await lb_link.count() == 0:
            lb_link = page.locator("a:has(svg.lucide-trophy)")
        if await lb_link.count() == 0:
            lb_link = page.locator("a:has-text('Leaderboard')")
        if await lb_link.count() > 0:
            await lb_link.first.click(timeout=5000)
            log(account_id, "已点击 Leaderboard")
            await asyncio.sleep(3)
        else:
            log(account_id, "未找到 Leaderboard 链接")
            return False
    except Exception as e:
        log(account_id, f"点击 Leaderboard 失败: {e}")
        return False

    # 如果已经是 No Actions Needed，直接返回
    no_action = page.locator("button:has-text('No Actions Needed')")
    if await no_action.count() > 0:
        log(account_id, "已是 No Actions Needed，无需操作")
        return True

    # ── Step 1: Resolve Bets ──
    resolve_btn = page.locator("button:has-text('Resolve Bets')")
    for _ in range(15):
        if await resolve_btn.count() > 0:
            break
        if await no_action.count() > 0:
            log(account_id, "已是 No Actions Needed，无需操作")
            return True
        await asyncio.sleep(1)

    if await resolve_btn.count() > 0:
        try:
            await resolve_btn.first.click(timeout=5000)
            log(account_id, "已点击 Resolve Bets")
            await asyncio.sleep(2)
        except Exception as e:
            log(account_id, f"点击 Resolve Bets 失败: {e}")

        # 等待钱包签名完成（后台 handler 自动处理）
        for _ in range(30):
            # Resolve 完成后按钮会消失或变成 Upload Trades / No Actions Needed
            if await resolve_btn.count() == 0:
                break
            if await no_action.count() > 0:
                break
            await asyncio.sleep(1)
        log(account_id, "Resolve Bets 完成")
        await asyncio.sleep(2)

    # 如果 Resolve 后直接变成 No Actions Needed
    if await no_action.count() > 0:
        log(account_id, "Resolve 后已是 No Actions Needed")
        return True

    # ── Step 2: Upload Trades ──
    upload_btn = page.locator("button:has-text('Upload Trades')")
    for _ in range(15):
        if await upload_btn.count() > 0:
            break
        if await no_action.count() > 0:
            log(account_id, "已是 No Actions Needed")
            return True
        await asyncio.sleep(1)

    if await upload_btn.count() > 0:
        try:
            await upload_btn.first.click(timeout=5000)
            log(account_id, "已点击 Upload Trades")
            await asyncio.sleep(2)
        except Exception as e:
            log(account_id, f"点击 Upload Trades 失败: {e}")
            return False

        # 等待签名完成 → 按钮变为 No Actions Needed
        for _ in range(30):
            if await no_action.count() > 0:
                break
            if await upload_btn.count() == 0:
                break
            await asyncio.sleep(1)

    if await no_action.count() > 0:
        log(account_id, "交易记录上传成功 (No Actions Needed)")
    else:
        log(account_id, "上传可能已完成（无法确认最终状态）")

    return True


# ════════════════════════════════════════════════════════
#  Claim Quest（Portal 领取任务奖励）
# ════════════════════════════════════════════════════════

PORTAL_QUEST_URL = "https://portal.linera.net/quests?taskGuid=f8ee1b19-e787-49d4-b523-7d5b3452e261"


async def _parse_cooldown(page: Page, account_id: str) -> int:
    """解析 Cooldown 倒计时文本（如 '1:14' → 74 秒），失败返回 90"""
    try:
        cd_el = page.locator("text=Cooldown active").locator("..")
        text = await cd_el.inner_text(timeout=5000)
        m = re.search(r'(\d+):(\d+)', text)
        if m:
            secs = int(m.group(1)) * 60 + int(m.group(2))
            log(account_id, f"Cooldown 剩余: {m.group(1)}:{m.group(2)} ({secs}s)")
            return secs
    except Exception:
        pass
    return 90


async def claim_quest(
    page: Page, context: BrowserContext, account_id: str,
    popup_handler: WalletPopupHandler,
) -> bool:
    """
    进入 Portal Quest 页面 → 检测登录状态 → 如需登录则走 OKX 签名 → 点 Claim → 签名
    """
    log(account_id, "开始 Claim Quest...")

    # ── 导航到 Quest 页面 ──
    for nav_try in range(3):
        try:
            await page.goto(PORTAL_QUEST_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(5)
            break
        except Exception as e:
            if nav_try < 2:
                log(account_id, f"导航到 Portal 失败，重试 ({nav_try+1}/3)...")
                await asyncio.sleep(3)
            else:
                log(account_id, f"导航到 Portal 彻底失败: {e}")
                return False

    # ── 检测登录状态 ──
    claim_btn = page.locator("button:has-text('Claim')")
    signin_btn = page.locator("button:has-text('Sign in')")

    for _ in range(15):
        if await claim_btn.count() > 0:
            break
        if await signin_btn.count() > 0:
            break
        await asyncio.sleep(1)

    # ── 未登录：点 Sign in → 选 OKX Wallet → 处理弹窗 → 重新进入 ──
    if await signin_btn.count() > 0 and await claim_btn.count() == 0:
        log(account_id, "Portal 未登录，开始签名登录...")
        clicked_signin = False
        try:
            await signin_btn.first.wait_for(state="visible", timeout=10000)
            await signin_btn.first.click(timeout=5000)
            clicked_signin = True
        except Exception:
            pass
        if not clicked_signin:
            try:
                await page.evaluate("""() => {
                    for (const btn of document.querySelectorAll('button')) {
                        if (btn.textContent.includes('Sign in')) {
                            btn.click();
                            return true;
                        }
                    }
                    return false;
                }""")
                clicked_signin = True
                log(account_id, "已通过 JS 点击 Sign in")
            except Exception as e:
                log(account_id, f"点击 Sign in 失败: {e}")
                return False
        await asyncio.sleep(3)

        # 选择 OKX Wallet（重试直到弹窗出现）
        okx_clicked = False
        for okx_try in range(5):
            okx_option = page.locator("button.wallet-list-item__tile:has(img[alt='okxwallet'])")
            okx_text = page.locator("text=OKX Wallet")

            for _ in range(20):
                if await okx_option.count() > 0 or await okx_text.count() > 0:
                    break
                await asyncio.sleep(0.5)

            if await okx_option.count() > 0:
                await okx_option.first.click(timeout=5000)
                log(account_id, f"已点击 OKX Wallet (Portal)（第 {okx_try+1} 次）")
            elif await okx_text.count() > 0:
                await okx_text.first.click(timeout=5000)
                log(account_id, f"已点击 OKX Wallet 文本（第 {okx_try+1} 次）")
            else:
                log(account_id, "Portal 未找到 OKX Wallet 选项")
                return False

            # 等待钱包弹窗出现，最多 8 秒
            popup_found = False
            for _ in range(16):
                for p in context.pages:
                    try:
                        if _is_wallet_popup(p.url or ""):
                            popup_found = True
                            break
                    except Exception:
                        continue
                if popup_found:
                    break
                await asyncio.sleep(0.5)

            if popup_found:
                okx_clicked = True
                break
            log(account_id, f"点击 OKX Wallet 后未弹窗，重试...")
            await asyncio.sleep(2)

        if not okx_clicked:
            log(account_id, "多次点击 OKX Wallet 均未弹窗，放弃")
            return False

        # 处理登录弹窗（解锁 + 签名），最多等 45 秒
        for tick in range(45):
            wallet_page = None
            for p in context.pages:
                try:
                    u = p.url or ""
                except Exception:
                    continue
                if _is_wallet_popup(u):
                    wallet_page = p
                    break

            if not wallet_page:
                await asyncio.sleep(1)
                continue

            log(account_id, f"Portal 登录弹窗: {wallet_page.url[-60:]}")
            try:
                await wallet_page.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass
            await asyncio.sleep(2)

            has_pwd = False
            for frame in wallet_page.frames:
                try:
                    if await frame.locator('input[type="password"]').count() > 0:
                        has_pwd = True
                        break
                except Exception:
                    continue

            if has_pwd:
                await _find_and_fill_password(wallet_page, context, account_id, OKX_DEFAULT_PASSWORD)
                await asyncio.sleep(0.5)
                await _click_unlock_button(wallet_page, context, account_id)
                log(account_id, "Portal 钱包解锁完成")
            else:
                await _click_wallet_button(wallet_page, account_id)
                log(account_id, "Portal 登录弹窗已处理")

            await asyncio.sleep(3)

        # 登录完成后重新进入 Quest 页面
        log(account_id, "Portal 登录完成，重新进入 Quest 页面...")
        try:
            await page.goto(PORTAL_QUEST_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(5)
        except Exception as e:
            log(account_id, f"重新进入 Quest 页面失败: {e}")
            return False

        # 等待 Claim 按钮出现
        for _ in range(15):
            if await claim_btn.count() > 0:
                break
            await asyncio.sleep(1)

    # ── 点击 Claim（带 Cooldown 重试，最多 3 轮） ──
    success_loc = page.locator("p.text-sm.text-gray-700:has-text('Quest completed successfully')")
    cooldown_loc = page.locator("text=Cooldown active")

    for claim_round in range(3):
        claim_btn = page.locator("button:has-text('Claim')")

        # 先检测是否已经成功（上轮可能延迟生效）
        try:
            if await success_loc.count() > 0:
                log(account_id, "检测到 Quest completed successfully!")
                return True
        except Exception:
            pass

        if await claim_btn.count() == 0:
            # 没有 Claim 按钮 → 可能在 Cooldown 或已完成
            if await cooldown_loc.count() > 0:
                cd_secs = await _parse_cooldown(page, account_id)
                if cd_secs > 0:
                    log(account_id, f"Cooldown 中，等待 {cd_secs+5}s 后重试（第 {claim_round+1}/3 轮）...")
                    await asyncio.sleep(cd_secs + 5)
                    try:
                        await page.goto(PORTAL_QUEST_URL, wait_until="domcontentloaded", timeout=30000)
                        await asyncio.sleep(5)
                    except Exception:
                        pass
                    continue
            log(account_id, "未找到 Claim 按钮，可能已经 Claim 过或未达标")
            return False

        try:
            await claim_btn.first.click(timeout=5000)
            log(account_id, f"已点击 Claim，等待签名...（第 {claim_round+1}/3 轮）")
            await asyncio.sleep(2)
        except Exception as e:
            log(account_id, f"点击 Claim 失败: {e}")
            return False

        # 等待成功文本或处理钱包弹窗（60s）
        claim_done = False
        for tick in range(60):
            try:
                if await success_loc.count() > 0:
                    claim_done = True
                    log(account_id, "检测到 Quest completed successfully!")
                    break
            except Exception:
                pass

            wallet_page = None
            for p in context.pages:
                try:
                    if _is_wallet_popup(p.url or ""):
                        wallet_page = p
                        break
                except Exception:
                    continue

            if wallet_page:
                log(account_id, f"Claim 签名弹窗: {wallet_page.url[-60:]}")
                try:
                    await wallet_page.wait_for_load_state("domcontentloaded", timeout=5000)
                except Exception:
                    pass
                await asyncio.sleep(2)
                await _click_wallet_button(wallet_page, account_id)
                log(account_id, "Claim 签名已确认")
                await asyncio.sleep(3)
                continue

            await asyncio.sleep(1)

        if claim_done:
            log(account_id, "Claim Quest 成功")
            return True

        # 未成功 → 截图 → 刷新检查 Cooldown
        log(account_id, f"60s 内未检测到成功提示（第 {claim_round+1}/3 轮）")
        await _take_failure_screenshot(page, account_id, f"claim_no_success_round{claim_round+1}")
        try:
            await page.goto(PORTAL_QUEST_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(5)
        except Exception:
            pass

        # 刷新后再次检测成功标志
        try:
            if await success_loc.count() > 0:
                log(account_id, "刷新后检测到 Quest completed successfully!")
                return True
        except Exception:
            pass

        # 检测 Cooldown → 等待后重试
        if await cooldown_loc.count() > 0:
            cd_secs = await _parse_cooldown(page, account_id)
            if cd_secs > 0 and claim_round < 2:
                log(account_id, f"Cooldown 中，等待 {cd_secs+5}s 后重试...")
                await asyncio.sleep(cd_secs + 5)
                try:
                    await page.goto(PORTAL_QUEST_URL, wait_until="domcontentloaded", timeout=30000)
                    await asyncio.sleep(5)
                except Exception:
                    pass
                continue

    log(account_id, "Claim Quest 失败：3 轮均未检测到成功标志")
    await _take_failure_screenshot(page, account_id, "claim_failed_final")
    return False


# ════════════════════════════════════════════════════════
#  主任务函数
# ════════════════════════════════════════════════════════

async def linera_task(
    page: Page,
    context: BrowserContext,
    account_id: str,
    popup_handler: WalletPopupHandler,
    **kwargs,
) -> bool:
    target_bets = kwargs.get("target_bets", TARGET_BETS)
    current_round = TASK_STATUS.get(account_id, {}).get("round", 0) + 1
    _update_status(account_id, status="logging_in", round=current_round, error="")

    recorder = TimelapseRecorder(page, account_id)
    await recorder.start()

    result = await _linera_task_inner(page, context, account_id, popup_handler, target_bets)
    if not result:
        cur = TASK_STATUS.get(account_id, {})
        if cur.get("status") not in ("done", "failed"):
            _update_status(account_id, status="failed", error=cur.get("error") or "任务异常退出")

    await recorder.stop(success=result)
    return result


async def _linera_task_inner(
    page: Page,
    context: BrowserContext,
    account_id: str,
    popup_handler: WalletPopupHandler,
    target_bets: int,
) -> bool:

    # ── Step 1: 登录（在 History 页面完成解锁 + 读基线） ──
    if not await login(page, context, account_id, popup_handler):
        log(account_id, "登录失败")
        _update_status(account_id, status="failed", error="登录失败")
        return False

    initial_trades = getattr(page, '_initial_trades', -1)

    # ── 跨轮次进度继承 ──
    if initial_trades >= 0:
        if account_id in ACCOUNT_TARGET_TRADES:
            target_total = ACCOUNT_TARGET_TRADES[account_id]
            remaining = target_total - initial_trades
            log(account_id, f"进度检查: 当前 Trades={initial_trades}，今日目标={target_total}，差={remaining}")
            if remaining <= 0:
                log(account_id, f"Trades 已达标: {initial_trades} >= {target_total}（今日进度继承），跳过下注")
                _update_status(account_id, status="uploading",
                               initial_trades=initial_trades, target_trades=target_total,
                               current_trades=initial_trades, bets_completed=0, bets_target=0)
                await upload_trades(page, context, account_id)
                _update_status(account_id, status="claiming")
                await claim_quest(page, context, account_id, popup_handler)
                _update_status(account_id, status="done")
                return True
            log(account_id, f"继承上轮进度: 当前 {initial_trades}，目标 {target_total}，还需 {remaining} 次")
            target_bets = remaining
        else:
            target_total = initial_trades + target_bets
            ACCOUNT_TARGET_TRADES[account_id] = target_total
            _save_target_trades()
            log(account_id, f"首次运行: Trades {initial_trades}，目标 {target_total}")
    else:
        target_total = -1

    _update_status(account_id, status="logged_in",
                   initial_trades=initial_trades, target_trades=target_total)

    # ── Step 2: 导航到市场页面 ──
    market = random.choice(MARKETS)
    market_url = f"{DAPP_URL}/?market={market}&duration=1"
    for nav_try in range(3):
        try:
            await page.goto(market_url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(5)
            break
        except Exception as e:
            if nav_try < 2:
                log(account_id, f"导航到市场失败，重试 ({nav_try+1}/3)...")
                await asyncio.sleep(3)
            else:
                log(account_id, f"导航到市场彻底失败: {e}")
                _update_status(account_id, status="failed", error="导航到市场失败")
                return False

    if not await wait_rpc_recovery(page, account_id, context):
        return False

    await select_1_minute(page, account_id)
    log(account_id, f"初始化完成，开始下注（目标 {target_bets} 次）")

    # ── Step 3: 下注 ──
    bet_ok = await run_betting_loop(
        page, context, account_id, popup_handler, target_bets,
    )

    if not bet_ok:
        return False

    # ── Step 4: 校验 History 笔数（轮询等上链），不足则补跑 ──
    _update_status(account_id, status="verifying")
    if target_total >= 0:
        # 先轮询等链上确认
        if not await navigate_to_history(page, account_id):
            log(account_id, "无法导航到 History，跳过校验")
        else:
            await asyncio.sleep(3)
            last_trades = -1
            stable_count = 0
            for poll in range(6):
                cur_trades = await get_trades_count(page, account_id)
                if cur_trades < 0:
                    log(account_id, "无法读取 Trades 数量")
                    break
                _update_status(account_id, current_trades=cur_trades)
                shortfall = target_total - cur_trades

                if shortfall <= 0:
                    log(account_id, f"Trades 校验通过: {cur_trades} >= {target_total}")
                    break

                if cur_trades == last_trades:
                    stable_count += 1
                else:
                    stable_count = 0

                if stable_count >= 2:
                    log(account_id, f"Trades 连续 {stable_count+1} 次未变化（{cur_trades}/{target_total}），判定上链完成")
                    break

                last_trades = cur_trades
                if cur_trades > (target_total - target_bets):
                    log(account_id, f"Trades {cur_trades}/{target_total}（差 {shortfall}），链上确认中，等 30s...")
                else:
                    log(account_id, f"Trades {cur_trades}/{target_total}（差 {shortfall}），等 30s...")
                await asyncio.sleep(30)
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=15000)
                except Exception:
                    pass
                await asyncio.sleep(3)

        # 轮询结束后，如果仍不足则补跑
        for verify_round in range(2):
            if not await navigate_to_history(page, account_id):
                log(account_id, "无法导航到 History，跳过校验")
                break
            await asyncio.sleep(3)
            final_trades = await get_trades_count(page, account_id)
            if final_trades < 0:
                log(account_id, "无法读取 Trades 数量")
                break

            shortfall = target_total - final_trades
            _update_status(account_id, current_trades=final_trades)
            if shortfall <= 0:
                log(account_id, f"Trades 校验通过: {final_trades} >= {target_total}")
                break

            log(account_id, f"Trades 确认不足: {final_trades}/{target_total}，还差 {shortfall} 次，补跑中...")
            if not await navigate_back_to_market(page, account_id):
                break
            await asyncio.sleep(3)
            if not await wait_rpc_recovery(page, account_id, context):
                return False
            await select_1_minute(page, account_id)
            await asyncio.sleep(2)

            extra_ok = await run_betting_loop(
                page, context, account_id, popup_handler, shortfall,
            )
            if not extra_ok:
                log(account_id, "补跑失败")
                break

        # 上传前最后确认（轮询等待链上确认）
        final_trades = -1
        for final_poll in range(6):
            if not await navigate_to_history(page, account_id):
                log(account_id, "上传前无法进入 History，中止上传")
                return False
            await asyncio.sleep(3)
            final_trades = await get_trades_count(page, account_id)
            if final_trades < 0:
                log(account_id, "无法读取 Trades 数量，中止上传")
                return False
            _update_status(account_id, current_trades=final_trades)
            if final_trades >= target_total:
                log(account_id, f"笔数已达标，开始上传：Trades {final_trades} >= {target_total}")
                break
            if final_poll < 5:
                log(account_id, f"Trades {final_trades}/{target_total}，等待链上确认（{final_poll+1}/6）...")
                await asyncio.sleep(30)
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=15000)
                except Exception:
                    pass
                await asyncio.sleep(3)
        else:
            log(account_id, f"Trades 仍不足 ({final_trades}/{target_total})，跳过上传")
            _update_status(account_id, status="failed", error=f"Trades不足 {final_trades}/{target_total}")
            return False
    else:
        log(account_id, "无 Trades 基线，跳过上传前校验")

    # ── Step 5: 上传 ──
    _update_status(account_id, status="uploading")
    await upload_trades(page, context, account_id)

    # ── Step 6: Claim Quest ──
    _update_status(account_id, status="claiming")
    claim_ok = await claim_quest(page, context, account_id, popup_handler)
    if not claim_ok:
        log(account_id, "Claim Quest 未成功，但下注和上传已完成")
        _update_status(account_id, status="done", error="Claim未成功")
    else:
        _update_status(account_id, status="done")
    return True


# ════════════════════════════════════════════════════════
#  入口
# ════════════════════════════════════════════════════════

def main():
    accounts = load_accounts()
    if not accounts:
        print("未读取到任何账号，请检查 hubshuju.xlsx")
        sys.exit(1)

    print(f"共读取到 {len(accounts)} 个账号。")
    print("1. 单窗口测试（第 1 个账号）")
    print("2. 批量运行")

    mode = input("请输入数字 (1/2): ").strip()

    if mode == "1":
        target = accounts[0]
        print(f"单窗口测试: {target.id}")
        asyncio.run(run_batch(
            [target], linera_task, max_workers=1,
        ))
    elif mode == "2":
        try:
            workers = int(input("请输入并发数（建议 1-3）: ").strip())
        except ValueError:
            workers = 1
        print(f"批量运行，并发: {workers}")
        asyncio.run(run_batch(
            accounts, linera_task, max_workers=workers,
        ))
    else:
        print("无效输入。")


if __name__ == "__main__":
    main()
