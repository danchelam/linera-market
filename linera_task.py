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

__version__ = "2026.05.22.1"

import asyncio
import random
import re
import sys
import os
import json as _json
from datetime import datetime, timedelta

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
TARGET_PAIRS = 15
TARGET_BETS = TARGET_PAIRS * 2  # 每对 = HIGHER + LOWER
MARKET_DURATION = 3  # 3 minute 市场

def _business_date() -> str:
    """返回业务日期字符串。每日任务在 UTC 0:00（北京时间 8:00）重置。"""
    return (datetime.now() - timedelta(hours=8)).strftime("%Y-%m-%d")


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
            today_str = _business_date()
            print(f"[日期检查] account_targets.json: _date={saved_date!r}, today={today_str!r}, 条目数={len(data)-1 if '_date' in data else len(data)}")
            if saved_date != today_str:
                os.remove(_TARGET_TRADES_FILE)
                ACCOUNT_TARGET_TRADES = {}
                print(f"[日期检查] 已清除 account_targets.json（过期）")
                return
            data.pop("_date", None)
            ACCOUNT_TARGET_TRADES = data
            print(f"[日期检查] 加载今日进度: {len(data)} 条")
        except Exception as e:
            print(f"[日期检查] 加载失败: {e}，清空数据")
            ACCOUNT_TARGET_TRADES = {}
    else:
        print(f"[日期检查] account_targets.json 不存在，无需清除")


def _save_target_trades():
    try:
        data = dict(ACCOUNT_TARGET_TRADES)
        data["_date"] = _business_date()
        with open(_TARGET_TRADES_FILE, "w", encoding="utf-8") as f:
            _json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass


_load_target_trades()


def reset_daily_data():
    """由 runner 在每次启动任务时调用，确保过期数据被清除"""
    global ACCOUNT_TARGET_TRADES, TASK_STATUS
    today_str = _business_date()

    for fpath, name in [(_TARGET_TRADES_FILE, "account_targets"), (_TASK_STATUS_FILE, "task_status")]:
        if not os.path.exists(fpath):
            continue
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = _json.load(f)
            if data.get("_date", "") != today_str:
                os.remove(fpath)
                print(f"[reset_daily_data] 已清除 {name}.json（_date={data.get('_date', '无')}）")
        except Exception:
            try:
                os.remove(fpath)
            except Exception:
                pass

    if not os.path.exists(_TARGET_TRADES_FILE):
        ACCOUNT_TARGET_TRADES = {}
    if not os.path.exists(_TASK_STATUS_FILE):
        TASK_STATUS = {}


# 实时状态追踪：account_id → 状态字典（供 Web 前端展示，持久化到文件）
TASK_STATUS: dict[str, dict] = {}
_TASK_STATUS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "task_status.json")
_task_status_dirty = False

# Weekly Reward 领取记录（account_id → ISO week string，如 "2026-W21"）
_WEEKLY_CLAIM_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "weekly_claim.json")
WEEKLY_CLAIM_RECORD: dict[str, str] = {}


def _current_iso_week() -> str:
    """返回当前 ISO 周字符串，如 '2026-W21'"""
    d = datetime.now()
    year, week, _ = d.isocalendar()
    return f"{year}-W{week:02d}"


def _load_weekly_claim():
    global WEEKLY_CLAIM_RECORD
    if os.path.exists(_WEEKLY_CLAIM_FILE):
        try:
            with open(_WEEKLY_CLAIM_FILE, "r", encoding="utf-8") as f:
                WEEKLY_CLAIM_RECORD = _json.load(f)
        except Exception:
            WEEKLY_CLAIM_RECORD = {}


def _save_weekly_claim():
    try:
        with open(_WEEKLY_CLAIM_FILE, "w", encoding="utf-8") as f:
            _json.dump(WEEKLY_CLAIM_RECORD, f, ensure_ascii=False)
    except Exception:
        pass


def _is_weekly_claimed(account_id: str) -> bool:
    return WEEKLY_CLAIM_RECORD.get(account_id, "") == _current_iso_week()


def _mark_weekly_claimed(account_id: str):
    WEEKLY_CLAIM_RECORD[account_id] = _current_iso_week()
    _save_weekly_claim()


_load_weekly_claim()


def _load_task_status():
    global TASK_STATUS
    if os.path.exists(_TASK_STATUS_FILE):
        try:
            with open(_TASK_STATUS_FILE, "r", encoding="utf-8") as f:
                data = _json.load(f)
            saved_date = data.get("_date", "")
            today_str = _business_date()
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
        data["_date"] = _business_date()
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
#  工具：下注金额控制
# ════════════════════════════════════════════════════════

BET_AMOUNT = "25"
MIN_BALANCE = 50  # 余额低于此值暂停下注，等待回款


async def get_user_balance(page: Page) -> float:
    """读取右上角用户余额（如 209.39）"""
    try:
        el = page.locator("span.font-bold.text-foreground").first
        if await el.count() > 0:
            text = (await el.inner_text(timeout=3000)).strip().replace(",", "")
            return float(text)
    except Exception:
        pass
    return -1


async def get_current_bet_amount(page: Page) -> str:
    """读取 NEXT MARKET BET 区域中间显示的下注金额（齿轮图标左侧的大号数字）"""
    # 方法1: JS 精确定位 — 齿轮 SVG 的前一个兄弟元素
    try:
        val = await page.evaluate("""() => {
            // 找齿轮图标
            const gears = document.querySelectorAll('svg');
            for (const svg of gears) {
                const cls = svg.getAttribute('class') || '';
                if (cls.includes('settings') || cls.includes('sliders')) {
                    // 齿轮前面的兄弟就是金额
                    let prev = svg.parentElement.previousElementSibling;
                    if (prev) {
                        const t = prev.textContent.trim();
                        if (/^[\\d.]+$/.test(t)) return t;
                    }
                    // 尝试齿轮父容器的前一个
                    prev = svg.parentElement.parentElement
                           ? svg.parentElement.parentElement.previousElementSibling
                           : null;
                    if (prev) {
                        const t = prev.textContent.trim();
                        if (/^[\\d.]+$/.test(t)) return t;
                    }
                }
            }
            return '';
        }""")
        if val:
            return val
    except Exception:
        pass

    # 方法2: 找 HIGHER 和 LOWER 按钮之间的区域里独立的金额数字
    try:
        val = await page.evaluate("""() => {
            const label = Array.from(document.querySelectorAll('*'))
                .find(el => el.textContent.trim() === 'NEXT MARKET BET' && el.children.length === 0);
            if (!label) return '';
            // label 的父容器下，找所有直接子元素中纯数字的
            const container = label.parentElement;
            if (!container) return '';
            const children = container.querySelectorAll('*');
            for (const c of children) {
                if (c.children.length === 0) {
                    const t = c.textContent.trim();
                    // 纯数字且不含 x（排除倍率）不含 %
                    if (/^\\d+(\\.\\d+)?$/.test(t) && !c.closest('button')) {
                        // 排除 HIGHER/LOWER 按钮内的数字和小数倍率
                        const fontSize = window.getComputedStyle(c).fontSize;
                        const size = parseFloat(fontSize);
                        if (size >= 18) return t;  // 金额数字通常比较大
                    }
                }
            }
            return '';
        }""")
        if val:
            return val
    except Exception:
        pass

    # 方法3: 输入框（面板已打开时）
    try:
        input_el = page.locator("input[type='number'], input[inputmode='decimal']").first
        if await input_el.count() > 0:
            val = await input_el.input_value(timeout=3000)
            if val:
                return val.strip()
    except Exception:
        pass
    return ""


async def open_amount_panel(page: Page) -> bool:
    """点击齿轮图标或金额区域打开修改面板"""
    # 先检查面板是否已经打开
    input_el = page.locator("input[type='number'], input[inputmode='decimal']").first
    if await input_el.count() > 0:
        return True

    # 点击齿轮图标
    selectors = [
        "svg.lucide-settings-2",
        "svg.lucide-sliders-horizontal",
    ]
    for sel in selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                parent_btn = el.locator("..")
                await parent_btn.click(timeout=3000)
                await asyncio.sleep(1)
                if await input_el.count() > 0:
                    return True
        except Exception:
            continue

    # 兜底：点击 NEXT MARKET BET 区域的金额数字
    try:
        bet_section = page.locator("text=NEXT MARKET BET").locator("..")
        if await bet_section.count() > 0:
            await bet_section.click(timeout=3000)
            await asyncio.sleep(1)
            if await input_el.count() > 0:
                return True
    except Exception:
        pass

    return False


async def set_bet_amount(page: Page, amount: str, account_id: str = "") -> bool:
    """设置下注金额"""
    if not await open_amount_panel(page):
        if account_id:
            log(account_id, "无法打开金额面板")
        return False

    try:
        input_el = page.locator("input[type='number'], input[inputmode='decimal']").first
        await input_el.click(timeout=3000)
        # 全选后删除再填入
        await input_el.press("Control+a")
        await asyncio.sleep(0.2)
        await input_el.fill(amount)
        await asyncio.sleep(0.5)

        await input_el.press("Enter")
        await asyncio.sleep(1)

        # 验证
        current = await get_current_bet_amount(page)
        target_num = float(amount)
        if current:
            try:
                current_num = float(current)
                if abs(current_num - target_num) < 0.01:
                    if account_id:
                        log(account_id, f"下注金额已设为 {amount}")
                    return True
            except ValueError:
                pass

        if account_id:
            log(account_id, f"金额设置后验证: 期望 {amount}, 实际 {current}")
        return current != ""
    except Exception as e:
        if account_id:
            log(account_id, f"设置金额失败: {e}")
        return False


async def ensure_bet_amount(page: Page, account_id: str, target_amount: str = "") -> bool:
    """检查并确保下注金额正确，不正确则自动修复"""
    if not target_amount:
        target_amount = BET_AMOUNT

    current = await get_current_bet_amount(page)
    if current:
        try:
            if abs(float(current) - float(target_amount)) < 0.01:
                return True
        except ValueError:
            pass
        log(account_id, f"当前金额 {current}，需要修改为 {target_amount}")

    return await set_bet_amount(page, target_amount, account_id)


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

async def select_duration(page: Page, account_id: str, duration: int = 0, max_wait: int = 15) -> bool:
    """选择市场时长（1/3/5 minute），默认使用全局 MARKET_DURATION"""
    if duration <= 0:
        duration = MARKET_DURATION
    label = f"{duration} minute"
    btn_loc = page.locator(f"button:text-is('{label}')")

    for attempt in range(max_wait):
        try:
            if await btn_loc.count() > 0:
                await btn_loc.first.click(timeout=5000)
                log(account_id, f"已选择 {label} 市场")
                await asyncio.sleep(2)
                return True
        except Exception:
            pass

        try:
            ok = await page.evaluate(f"""() => {{
                for (const btn of document.querySelectorAll('button')) {{
                    if (btn.textContent.trim() === '{label}') {{
                        btn.click();
                        return true;
                    }}
                }}
                return false;
            }}""")
            if ok:
                log(account_id, f"已选择 {label} 市场 (JS)")
                await asyncio.sleep(2)
                return True
        except Exception:
            pass

        if attempt < max_wait - 1:
            if attempt == 0:
                log(account_id, f"{label} 按钮未加载，等待中...")
            await asyncio.sleep(1)

    log(account_id, f"{label} 按钮等待超时")
    return False


async def select_1_minute(page: Page, account_id: str, max_wait: int = 15) -> bool:
    """兼容旧调用"""
    return await select_duration(page, account_id, duration=MARKET_DURATION, max_wait=max_wait)


# ════════════════════════════════════════════════════════
#  切换市场 + 重新选时长
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
    ok = await select_duration(page, account_id)
    if not ok:
        other = [m for m in MARKETS if m != market]
        if other:
            alt = random.choice(other)
            log(account_id, f"{MARKET_DURATION} minute 不可用，先切到 {alt} 再切回 {market}")
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
            ok = await select_duration(page, account_id)
    return ok


# ════════════════════════════════════════════════════════
#  清除浏览器缓存（Connection failed 反复出现时使用）
# ════════════════════════════════════════════════════════

async def _clear_browser_cache(page: Page, context: BrowserContext, account_id: str):
    """彻底清除站点数据（Cookie + Storage + IndexedDB + CacheStorage），等同于手动清除"""
    origins = [
        "https://linera.market",
        "https://app.dynamicauth.com",
    ]

    # 方式1: CDP 协议清除（最彻底，覆盖所有存储类型）
    cdp_ok = False
    try:
        cdp = await context.new_cdp_session(page)
        # 清除所有 Cookie
        await cdp.send("Network.clearBrowserCookies")
        log(account_id, "已通过 CDP 清除所有 Cookies")
        # 清除每个域名的完整站点数据
        for origin in origins:
            try:
                await cdp.send("Storage.clearDataForOrigin", {
                    "origin": origin,
                    "storageTypes": "cookies,local_storage,session_storage,indexeddb,cache_storage,websql",
                })
                log(account_id, f"已清除 {origin} 的全部站点数据")
            except Exception as e:
                log(account_id, f"清除 {origin} 站点数据失败: {e}")
        await cdp.detach()
        cdp_ok = True
    except Exception as e:
        log(account_id, f"CDP 清除失败: {e}，回退到常规方式")

    # 方式2: 常规 API 兜底
    if not cdp_ok:
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

    # 额外: JS 清除 IndexedDB 和 CacheStorage（针对当前域名）
    try:
        await page.evaluate("""async () => {
            try { localStorage.clear(); sessionStorage.clear(); } catch(e) {}
            try {
                const dbs = await indexedDB.databases();
                for (const db of dbs) { indexedDB.deleteDatabase(db.name); }
            } catch(e) {}
            try {
                const keys = await caches.keys();
                for (const k of keys) { await caches.delete(k); }
            } catch(e) {}
        }""")
    except Exception:
        pass


# ════════════════════════════════════════════════════════
#  OKX 钱包网络检测（确保在"所有网络"）
# ════════════════════════════════════════════════════════

async def _ensure_all_networks(wallet_page: Page, account_id: str, page: Page = None) -> bool:
    """解锁后检测 OKX 钱包是否在「所有网络」，如不是则切换。"""
    try:
        net_icon = wallet_page.locator('div[data-testid="home-page-networks-icon"]')
        for _ in range(10):
            if await net_icon.count() > 0:
                break
            await asyncio.sleep(0.5)
        if await net_icon.count() == 0:
            return True

        specific = net_icon.locator('div._wallet-icon__text_5gayk_73')
        if await specific.count() == 0:
            return True

        net_text = await specific.inner_text(timeout=3000)
        log(account_id, f"钱包当前网络: {net_text!r}，非「所有网络」，切换中...")

        await net_icon.first.click(timeout=5000)
        await asyncio.sleep(2)

        # 先点击「热门网络」标签页
        hot_tab = wallet_page.locator(
            'div[data-testid="network-management-page-chain-tabs-extension_wallet_network_tab_main_network"]'
        )
        for _ in range(10):
            if await hot_tab.count() > 0:
                break
            await asyncio.sleep(0.5)
        if await hot_tab.count() > 0:
            await hot_tab.first.click(timeout=5000)
            log(account_id, "已点击「热门网络」标签")
            await asyncio.sleep(2)

        # 再点击「所有网络」选项
        all_net = wallet_page.locator('text=所有网络')
        for _ in range(10):
            if await all_net.count() > 0:
                break
            await asyncio.sleep(0.5)

        if await all_net.count() > 0:
            await all_net.first.click(timeout=5000)
            log(account_id, "已切换到「所有网络」")
            await asyncio.sleep(2)
        else:
            log(account_id, "未找到「所有网络」选项")
            return False

        # 切换完成后，需要在 dApp 页面重新点击 OKX Wallet 触发签名
        if page:
            await asyncio.sleep(2)
            okx_btn = page.locator('button[data-testid="ListTile"]:has-text("OKX Wallet")')
            if await okx_btn.count() == 0:
                okx_btn = page.locator("button.wallet-list-item__tile:has(img[alt='okxwallet'])")
            if await okx_btn.count() > 0:
                await okx_btn.first.click(timeout=5000)
                log(account_id, "已重新点击 OKX Wallet（触发签名）")
                await asyncio.sleep(3)
            else:
                log(account_id, "dApp 页面未找到 OKX Wallet 按钮，跳过")

        return True
    except Exception as e:
        log(account_id, f"网络检测异常: {e}")
        return True


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
                await _ensure_all_networks(wallet_page, account_id, page)
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

            # ── 连续 3 次 Connection failed → 清缓存重新加载 ──
            if conn_fail_count > 0 and conn_fail_count % 3 == 0 and conn_fail_count // 3 > cache_cleared_count:
                cache_cleared_count += 1
                if cache_cleared_count <= 3:
                    log(account_id, f"Connection failed 已达 {conn_fail_count} 次，清除缓存重新加载（第 {cache_cleared_count}/3 次）...")
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
                # 先等转圈消失再点，否则会打断正在加载的连接
                spin_loc = page.locator("svg.animate-spin")
                if await spin_loc.count() > 0:
                    log(account_id, "页面加载中（转圈），等待加载完成后再连接...")
                    spin_wallet_handled = False
                    spin_cf_hit = False
                    for _sw in range(120):
                        if await spin_loc.count() == 0:
                            break
                        # 转圈过程中检测钱包弹窗（可能在等解锁/签名）
                        for p in context.pages:
                            try:
                                if _is_wallet_popup(p.url or ""):
                                    if not spin_wallet_handled:
                                        log(account_id, f"转圈中发现钱包弹窗: {p.url.split('/')[-1]}")
                                    try:
                                        await p.wait_for_load_state("domcontentloaded", timeout=5000)
                                    except Exception:
                                        pass
                                    await asyncio.sleep(1)
                                    has_pwd = False
                                    for frame in p.frames:
                                        try:
                                            if await frame.locator('input[type="password"]').count() > 0:
                                                has_pwd = True
                                                break
                                        except Exception:
                                            continue
                                    if has_pwd:
                                        log(account_id, "弹窗含密码框，执行解锁...")
                                        await _find_and_fill_password(p, context, account_id, OKX_DEFAULT_PASSWORD)
                                        await asyncio.sleep(0.5)
                                        await _click_unlock_button(p, context, account_id)
                                        await asyncio.sleep(3)
                                        try:
                                            await _ensure_all_networks(p, account_id, page)
                                        except Exception:
                                            pass
                                    else:
                                        await _click_wallet_button(p, account_id)
                                    spin_wallet_handled = True
                                    await asyncio.sleep(3)
                                    break
                            except Exception:
                                continue
                        # 转圈过程中检测 Connection failed → 点 Retry 后立即跳出回主循环
                        cf_btn = page.locator("span.text-danger button")
                        if await cf_btn.count() > 0:
                            conn_fail_count += 1
                            log(account_id, f"等待中检测到 Connection failed（第 {conn_fail_count} 次），跳出等待回主循环处理")
                            try:
                                await cf_btn.first.click(timeout=5000)
                            except Exception:
                                pass
                            await asyncio.sleep(3)
                            spin_cf_hit = True
                            break  # 跳出转圈循环，回主循环让清缓存逻辑判断
                        # Claiming chain
                        claiming_chk = page.locator("span:text-is('Claiming chain...')")
                        if await claiming_chk.count() > 0:
                            await asyncio.sleep(1)
                            continue
                        await asyncio.sleep(1)
                    # 回主循环
                    if not spin_cf_hit:
                        await asyncio.sleep(2)
                    continue

                popup_handler.enabled = False
                okx_selected = False
                for connect_try in range(5):
                    connect_btn = page.locator("button:has-text('Connect Wallet')")
                    if await connect_btn.count() == 0:
                        okx_selected = True
                        break

                    # 再次确认没有转圈（每次重试前都检查）
                    if await spin_loc.count() > 0:
                        log(account_id, "检测到转圈，等待加载完成...")
                        for _sw2 in range(60):
                            if await spin_loc.count() == 0:
                                break
                            # 同时处理钱包弹窗
                            for p in context.pages:
                                try:
                                    if _is_wallet_popup(p.url or ""):
                                        try:
                                            await p.wait_for_load_state("domcontentloaded", timeout=3000)
                                        except Exception:
                                            pass
                                        await asyncio.sleep(1)
                                        has_pwd = False
                                        for frame in p.frames:
                                            try:
                                                if await frame.locator('input[type="password"]').count() > 0:
                                                    has_pwd = True
                                                    break
                                            except Exception:
                                                continue
                                        if has_pwd:
                                            log(account_id, "转圈中发现钱包需解锁...")
                                            await _find_and_fill_password(p, context, account_id, OKX_DEFAULT_PASSWORD)
                                            await asyncio.sleep(0.5)
                                            await _click_unlock_button(p, context, account_id)
                                            await asyncio.sleep(3)
                                        else:
                                            await _click_wallet_button(p, account_id)
                                            await asyncio.sleep(2)
                                        break
                                except Exception:
                                    continue
                            await asyncio.sleep(1)
                        await asyncio.sleep(2)
                        # 转圈结束后 Connect Wallet 可能消失了
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
                    # OKX Wallet 5 次都找不到，可能是网络问题
                    # 检查是否处于 Connection failed 状态，如果是则回主循环继续处理
                    retry_btn_check = page.locator("span.text-danger button")
                    if await retry_btn_check.count() > 0:
                        conn_fail_count += 1
                        log(account_id, f"OKX Wallet 加载失败，检测到 Connection failed（累计 {conn_fail_count} 次），回主循环重试...")
                        continue
                    # 不是 Connection failed → 尝试清缓存后重试一次
                    if cache_cleared_count < 2:
                        cache_cleared_count += 1
                        conn_fail_count += 5  # 直接触发清缓存阈值
                        log(account_id, f"OKX Wallet 反复加载失败，清除缓存重新加载（第 {cache_cleared_count}/2 次）...")
                        await _take_failure_screenshot(page, account_id, f"okx_wallet_fail_{main_attempt}")
                        await _clear_browser_cache(page, context, account_id)
                        try:
                            await page.goto(history_url, wait_until="domcontentloaded", timeout=30000)
                        except Exception:
                            pass
                        await asyncio.sleep(8)
                        continue
                    log(account_id, "多次尝试后仍未找到 OKX Wallet，跳过此账号")
                    return False

                # ── 检测 QR 码界面（插件未注入） ──
                await asyncio.sleep(2)
                qr_detected = False
                try:
                    get_ext = page.locator("text='Get Extension'")
                    copy_qr = page.locator("text='Copy QR URI'")
                    if await get_ext.count() > 0 or await copy_qr.count() > 0:
                        qr_detected = True
                except Exception:
                    pass
                if qr_detected:
                    log(account_id, "检测到 QR 码界面（OKX 插件未注入），刷新页面重试...")
                    try:
                        await page.keyboard.press("Escape")
                        await asyncio.sleep(1)
                        await page.reload(wait_until="domcontentloaded", timeout=30000)
                    except Exception:
                        pass
                    await asyncio.sleep(8)
                    continue

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
                            await _ensure_all_networks(wallet_page, account_id, page)
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

            # ── Phase B½: 无论是否已登录，都要检查并处理可能存在的钱包弹窗 ──
            for _wallet_check in range(10):
                wp = None
                for p in context.pages:
                    try:
                        if _is_wallet_popup(p.url or ""):
                            wp = p
                            break
                    except Exception:
                        continue
                if not wp:
                    break
                log(account_id, f"检测到待处理钱包弹窗: {wp.url.split('/')[-1]}")
                try:
                    await wp.wait_for_load_state("domcontentloaded", timeout=5000)
                except Exception:
                    pass
                await asyncio.sleep(2)
                has_pwd = False
                for frame in wp.frames:
                    try:
                        if await frame.locator('input[type="password"]').count() > 0:
                            has_pwd = True
                            break
                    except Exception:
                        continue
                if has_pwd:
                    log(account_id, "弹窗含密码框，执行解锁...")
                    await _find_and_fill_password(wp, context, account_id, OKX_DEFAULT_PASSWORD)
                    await asyncio.sleep(0.5)
                    await _click_unlock_button(wp, context, account_id)
                    await asyncio.sleep(3)
                    log(account_id, "钱包已解锁")
                else:
                    await _click_wallet_button(wp, account_id)
                    log(account_id, "钱包弹窗已确认")
                    await asyncio.sleep(3)

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
                    if popup_count > 15:
                        log(account_id, f"弹窗确认超过 15 次仍有新弹窗，放弃当前轮次，刷新重试...")
                        try:
                            await wallet_page.close()
                        except Exception:
                            pass
                        need_outer_retry = True
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
                            await asyncio.sleep(2)
                            await _ensure_all_networks(wallet_page, account_id, page)
                        else:
                            await _click_wallet_button(wallet_page, account_id)

                        log(account_id, f"弹窗已处理（第 {popup_count} 个）")
                    except Exception:
                        log(account_id, f"弹窗处理中页面已关闭（第 {popup_count} 个），继续")
                    await asyncio.sleep(3)
                    continue

                # "Something went wrong" 错误 → 刷新重试
                try:
                    err_loc = page.locator("text='Something went wrong'")
                    if await err_loc.count() > 0:
                        log(account_id, "检测到 'Something went wrong'，刷新重试...")
                        try:
                            await page.keyboard.press("Escape")
                            await asyncio.sleep(1)
                            await page.reload(wait_until="domcontentloaded", timeout=30000)
                        except Exception:
                            pass
                        await asyncio.sleep(8)
                        need_outer_retry = True
                        break
                except Exception:
                    pass

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


async def get_round_id(page: Page) -> str:
    """读取当前轮次时间段（如 '17:18 - 17:21'），用于判断是否已在该轮下过注"""
    try:
        return await page.evaluate("""() => {
            const els = document.querySelectorAll('*');
            for (const el of els) {
                if (el.children.length === 0) {
                    const t = (el.textContent || '').trim();
                    if (/^\\d{2}:\\d{2}\\s*-\\s*\\d{2}:\\d{2}$/.test(t)) return t;
                }
            }
            return '';
        }""")
    except Exception:
        return ""


async def has_existing_bets(page: Page, account_id: str = "") -> bool:
    """检查当前轮次是否已下注：HIGHER/LOWER 按钮上方的金额 > 下注设置金额"""
    try:
        bet_amount = float(BET_AMOUNT)
        # 读取 HIGHER 和 LOWER 按钮上方显示的金额
        amounts = await page.evaluate("""() => {
            const results = [];
            // 方法1: 找 btn-higher 和 btn-lower 按钮附近的金额
            const btns = document.querySelectorAll('button.btn-higher, button.btn-lower');
            for (const btn of btns) {
                const parent = btn.parentElement;
                if (!parent) continue;
                // 向上找到包含金额和倍率的容器
                const container = parent.parentElement;
                if (!container) continue;
                const text = container.innerText || '';
                // 匹配 "4.00" 后面跟 "x1.79" 的模式
                const m = text.match(/([\\d.]+)\\s*x[\\d.]+/g);
                if (m) {
                    for (const match of m) {
                        const val = parseFloat(match);
                        if (!isNaN(val)) results.push(val);
                    }
                }
            }
            if (results.length > 0) return results;

            // 方法2: 直接搜索 NEXT MARKET BET 后面的所有 "数字 x数字" 模式
            const all = document.body.innerText || '';
            const idx = all.indexOf('NEXT MARKET BET');
            if (idx < 0) return [];
            const after = all.substring(idx, idx + 200);
            const matches = after.match(/([\\d.]+)\\s*x[\\d.]+/g);
            if (!matches) return [];
            return matches.map(m => parseFloat(m));
        }""")
        if not amounts or len(amounts) < 2:
            return False
        threshold = bet_amount * 0.8
        for val in amounts:
            if val >= threshold:
                if account_id:
                    log(account_id, f"[检测] 已有下注（金额 {val} >= 阈值 {threshold:.1f}）")
                return True
        return False
    except Exception:
        return False


async def _click_and_sign(
    page: Page, context: BrowserContext, account_id: str,
    direction: str, label: str,
) -> bool:
    """点击 HIGHER/LOWER 按钮并等待钱包签名完成（弹窗消失），不等链上确认。"""
    btn_cls = "btn-higher" if direction == "HIGHER" else "btn-lower"
    try:
        await page.locator(f"button.{btn_cls}").first.click(timeout=5000)
        log(account_id, f"{label} 点击 {direction}")
    except Exception as e:
        log(account_id, f"{label} 点击 {direction} 失败: {e}")
        return False

    # 等待钱包弹窗出现并被自动签名（最多 30 秒）
    popup_seen = False
    for i in range(30):
        if STOP_FLAG:
            return False
        has_popup = False
        wallet_p = None
        for p in context.pages:
            try:
                if _is_wallet_popup(p.url or ""):
                    has_popup = True
                    wallet_p = p
                    popup_seen = True
                    break
            except Exception:
                continue
        if popup_seen and not has_popup:
            log(account_id, f"{label} {direction} 签名完成")
            return True
        # 弹窗停留超过 5s，主动尝试处理
        if has_popup and wallet_p and i > 0 and i % 5 == 0:
            try:
                has_pwd = False
                for frame in wallet_p.frames:
                    try:
                        if await frame.locator('input[type="password"]').count() > 0:
                            has_pwd = True
                            break
                    except Exception:
                        continue
                if has_pwd:
                    log(account_id, "下注中检测到解锁弹窗，自动解锁...")
                    await _find_and_fill_password(wallet_p, context, account_id, OKX_DEFAULT_PASSWORD)
                    await asyncio.sleep(0.5)
                    await _click_unlock_button(wallet_p, context, account_id)
                    await asyncio.sleep(2)
                else:
                    await _click_wallet_button(wallet_p, account_id)
            except Exception:
                pass
        await asyncio.sleep(1)

    if not popup_seen:
        log(account_id, f"{label} {direction} 30s 内无弹窗")
        return False
    log(account_id, f"{label} {direction} 签名超时")
    return False


async def place_bet_pair(
    page: Page, context: BrowserContext, account_id: str,
    pair_num: int, target_pairs: int,
) -> bool | str:
    """
    在当前市场快速连续下 HIGHER + LOWER 一对：
    点击 HIGHER → 签名 → 点击 LOWER → 签名 → 统一等待链上确认。
    调用前需确保：按钮可用、倒计时充足、当前轮次未下注。
    """
    label = f"[{pair_num}/{target_pairs}]"

    # 最后一道防线：下注前检查是否已有下注
    if await has_existing_bets(page, account_id):
        log(account_id, f"{label} 下注前检测到已有下注，跳过")
        return "SIGNED"

    baseline = await get_card_glass_count(page)
    signed_count = 0  # 已完成签名的数量

    # ── 快速连续下两注：HIGHER → 签名 → LOWER → 签名 ──
    ok1 = await _click_and_sign(page, context, account_id, "HIGHER", label)
    if not ok1:
        await _take_failure_screenshot(page, account_id, f"pair{pair_num}_higher_fail")
        return NO_POPUP_FAILURE
    signed_count += 1

    await asyncio.sleep(1)

    # 等 LOWER 按钮再次可用
    for w3 in range(10):
        try:
            lb = page.locator("button.btn-lower")
            if await lb.count() > 0 and await lb.get_attribute("disabled") is None:
                break
        except Exception:
            pass
        if w3 == 9:
            log(account_id, f"{label} LOWER 按钮不可用")
            # HIGHER 已签名，标记为已签名以防重复下注
            return "SIGNED"
        await asyncio.sleep(1)

    ok2 = await _click_and_sign(page, context, account_id, "LOWER", label)
    if not ok2:
        await _take_failure_screenshot(page, account_id, f"pair{pair_num}_lower_fail")
        return "SIGNED"
    signed_count += 1

    # ── 统一等待链上确认（card-glass 增加 2） ──
    log(account_id, f"{label} 两注已签名，等待链上确认...")
    target_count = baseline + 2
    for tick in range(30):
        if STOP_FLAG:
            return "SIGNED"
        cur = await get_card_glass_count(page)
        if cur >= target_count:
            log(account_id, f"{label} 双向下注成功（card-glass {baseline} → {cur}）")
            return True
        await asyncio.sleep(1)

    final = await get_card_glass_count(page)
    if final > baseline:
        log(account_id, f"{label} 部分确认（card-glass {baseline} → {final}），视为成功")
        return True

    # 签名已完成但链上未确认，仍视为"已签名"防止重复下注
    log(account_id, f"{label} 30s 内链上未确认，但签名已完成")
    return "SIGNED"


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

async def _try_find_market(
    page: Page, account_id: str, context: BrowserContext,
    bet_records: dict, popup_handler: WalletPopupHandler,
) -> str | None:
    """
    遍历所有市场，找到一个可以下注的（未结算完成、倒计时充足、本轮未下注）。
    返回市场名或 None。
    """
    for market in MARKETS:
        await switch_market(page, account_id, market)
        await asyncio.sleep(1)

        # 检查结算
        if not await wait_settlement_done(page, account_id, context, timeout=5):
            continue

        # 检查按钮
        try:
            h = page.locator("button.btn-higher")
            l = page.locator("button.btn-lower")
            if not (await h.count() > 0 and await l.count() > 0
                    and await h.get_attribute("disabled") is None
                    and await l.get_attribute("disabled") is None):
                continue
        except Exception:
            continue

        # 检查倒计时
        cd = await get_countdown_value(page)
        if 0 < cd < 30:
            log(account_id, f"{market} 倒计时 {cd}s < 30s，跳过")
            continue

        # 检查本轮是否已下注
        # 方法1: 用轮次 ID 判断（内部记录）
        round_id = await get_round_id(page)
        if round_id and bet_records.get(market) == round_id:
            log(account_id, f"{market} 本轮 {round_id} 已下注（bet_records），跳过")
            continue
        if not round_id:
            log(account_id, f"{market} 未获取到 round_id，尝试 UI 检测")

        # 方法2: 检查 UI 上是否已有下注（HIGHER/LOWER 旁金额 >= 设置金额）
        if await has_existing_bets(page, account_id):
            if round_id:
                bet_records[market] = round_id
            log(account_id, f"{market} UI 检测到已有下注，跳过")
            continue

        return market

    return None


async def _do_refresh_recovery(
    page: Page, account_id: str, context: BrowserContext,
    popup_handler: WalletPopupHandler,
) -> bool:
    """刷新页面并恢复钱包连接，失败返回 False"""
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
    return True


async def run_betting_loop(
    page: Page,
    context: BrowserContext,
    account_id: str,
    popup_handler: WalletPopupHandler,
    target_bets: int = TARGET_BETS,
) -> bool:
    """
    下注主循环：轮转 BTC/ETH/SOL 寻找可下注市场。
    每对 = HIGHER + LOWER，下完一对立刻切到下一个市场继续。
    所有市场都在当前轮次下完注后，等待任一市场新轮次开始。
    """
    target_pairs = target_bets // 2 if target_bets > 1 else target_bets
    completed_pairs = 0
    consecutive_failures = 0
    consecutive_no_popup = 0
    total_failures = 0
    max_total_failures = 10
    bet_records: dict[str, str] = {}   # {market: round_id}

    log(account_id, f"开始下注，目标 {target_pairs} 对（{target_bets} 次），轮转 {'/'.join(MARKETS)}")
    _update_status(account_id, status="betting", bets_target=target_bets, bets_completed=0)

    while completed_pairs < target_pairs and not STOP_FLAG:
        # ── 余额检查：低于 MIN_BALANCE 暂停等待回款 ──
        balance = await get_user_balance(page)
        if balance >= 0 and balance < MIN_BALANCE:
            log(account_id, f"余额不足（{balance:.2f} < {MIN_BALANCE}），暂停下注等待回款...")
            _update_status(account_id, status="waiting_balance", error=f"余额{balance:.2f}<{MIN_BALANCE}")
            for _wait in range(60):
                if STOP_FLAG:
                    return False
                await asyncio.sleep(10)
                balance = await get_user_balance(page)
                if balance >= MIN_BALANCE:
                    log(account_id, f"余额已恢复（{balance:.2f}），继续下注")
                    _update_status(account_id, status="betting", error="")
                    break
            else:
                log(account_id, f"等待 10 分钟余额仍不足（{balance:.2f}），放弃")
                return False

        if total_failures >= max_total_failures:
            log(account_id, f"累计失败 {total_failures} 次，放弃下注")
            await _take_failure_screenshot(page, account_id, "max_failures_reached")
            _update_status(account_id, status="failed", error=f"累计失败{total_failures}次")
            return False

        if consecutive_no_popup >= 3:
            log(account_id, f"连续 {consecutive_no_popup} 次无弹窗，刷新...")
            await _take_failure_screenshot(page, account_id, "stuck_no_popup_3x")
            if not await _do_refresh_recovery(page, account_id, context, popup_handler):
                return False
            consecutive_no_popup = 0
            consecutive_failures = 0
            bet_records.clear()
            continue

        if consecutive_failures >= 5:
            log(account_id, f"连续失败 {consecutive_failures} 次，刷新...")
            await _take_failure_screenshot(page, account_id, "consecutive_fail_5x")
            if not await _do_refresh_recovery(page, account_id, context, popup_handler):
                return False
            consecutive_failures = 0
            bet_records.clear()

        # ── 寻找可下注市场 ──
        market = await _try_find_market(page, account_id, context, bet_records, popup_handler)

        if market is None:
            # 所有市场都在当前轮次下完了或不可用，等待新一轮
            log(account_id, "所有市场当前轮次已下注或不可用，等待新一轮...")
            await asyncio.sleep(10)
            bet_records.clear()
            continue

        log(account_id, f"在 {market} 下注（第 {completed_pairs+1}/{target_pairs} 对）")

        # ── 前置检查 ──
        if await is_fatal_error(page):
            log(account_id, "RPC 致命错误")
            return False
        if await is_page_stuck(page):
            await _take_failure_screenshot(page, account_id, "page_stuck")
            if not await recover_from_stuck(page, account_id):
                return False

        # ── 下注 ──
        result = await place_bet_pair(
            page, context, account_id, completed_pairs + 1, target_pairs,
        )

        # 只要签名完成（True 或 "SIGNED"），都记录 bet_records 防止重复下注
        if result is True or result == "SIGNED":
            round_id = await get_round_id(page)
            if round_id:
                bet_records[market] = round_id

        if result is True:
            completed_pairs += 1
            completed_bets = completed_pairs * 2
            consecutive_failures = 0
            consecutive_no_popup = 0
            log(account_id, f"已完成 {completed_pairs}/{target_pairs} 对（{completed_bets}/{target_bets} 次）")
            _update_status(account_id, bets_completed=completed_bets, error="")

        elif result == "SIGNED":
            # 签名完成但链上未确认，算作成功（交易已提交）
            completed_pairs += 1
            completed_bets = completed_pairs * 2
            consecutive_failures = 0
            consecutive_no_popup = 0
            log(account_id, f"已完成 {completed_pairs}/{target_pairs} 对（签名已提交，{completed_bets}/{target_bets} 次）")
            _update_status(account_id, bets_completed=completed_bets, error="")

        elif result == NO_POPUP_FAILURE:
            consecutive_no_popup += 1
            consecutive_failures += 1
            total_failures += 1
            log(account_id, f"无弹窗（连续: {consecutive_no_popup}/3，累计: {total_failures}/{max_total_failures}）")
            _update_status(account_id, error=f"无弹窗{consecutive_no_popup}/3")
            await asyncio.sleep(3)
        else:
            consecutive_failures += 1
            consecutive_no_popup = 0
            total_failures += 1

            connect_btn = page.locator("button:has-text('Connect Wallet')")
            if await connect_btn.count() > 0:
                log(account_id, "钱包掉线，刷新重连...")
                if not await _do_refresh_recovery(page, account_id, context, popup_handler):
                    return False
                consecutive_failures = 0
                bet_records.clear()
                continue

            log(account_id, f"下注失败（连续: {consecutive_failures}，累计: {total_failures}/{max_total_failures}）")
            _update_status(account_id, error=f"连续失败{consecutive_failures}次")
            await asyncio.sleep(5)

    if STOP_FLAG:
        log(account_id, f"收到停止信号，已完成 {completed_pairs}/{target_pairs} 对")
        return False

    log(account_id, f"全部 {target_pairs} 对下注完成，等待 30s 让链上确认...")
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
#  Claim Quest（Portal Archetype 领取 — 新版 UI）
# ════════════════════════════════════════════════════════

PORTAL_QUEST_URL = "https://portal.linera.net/quests?taskGuid=f8ee1b19-e787-49d4-b523-7d5b3452e261"
ARCHETYPE_NAMES = ["Achiever", "Socializer", "Killer", "Explorer"]


async def _portal_login(
    page: Page, context: BrowserContext, account_id: str,
) -> bool:
    """Portal 页面登录（Sign in → OKX Wallet → 处理弹窗）"""
    signin_btn = page.locator("button:has-text('Sign in')")
    connect_btn = page.locator("button:has-text('Connect')")

    need_login = False
    for btn_loc in [signin_btn, connect_btn]:
        if await btn_loc.count() > 0:
            need_login = True
            break
    if not need_login:
        return True

    log(account_id, "Portal 未登录，开始签名登录...")
    clicked = False
    for btn_loc in [signin_btn, connect_btn]:
        if await btn_loc.count() > 0:
            try:
                await btn_loc.first.click(timeout=5000)
                clicked = True
                break
            except Exception:
                pass
    if not clicked:
        try:
            await page.evaluate("""() => {
                for (const btn of document.querySelectorAll('button')) {
                    if (btn.textContent.includes('Sign in') || btn.textContent.includes('Connect')) {
                        btn.click(); return true;
                    }
                }
                return false;
            }""")
            log(account_id, "已通过 JS 点击 Sign in")
        except Exception as e:
            log(account_id, f"点击 Sign in 失败: {e}")
            return False
    await asyncio.sleep(3)

    # 选择 OKX Wallet
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
            log(account_id, f"Portal 未找到 OKX Wallet（第 {okx_try+1}/5 次），重试...")
            await asyncio.sleep(3)
            continue

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
        log(account_id, "点击 OKX Wallet 后未弹窗，重试...")
        await asyncio.sleep(2)

    if not okx_clicked:
        log(account_id, "多次点击 OKX Wallet 均未弹窗，放弃")
        return False

    # 处理钱包弹窗（解锁 + 签名）
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

    return True


async def _find_best_archetype(page: Page, account_id: str) -> str | None:
    """
    扫描页面上的 4 个 Archetype 卡片，找到可领取且分数最高的那个。
    返回 Archetype 名称（如 'Achiever'），无可领取时返回 None。
    """
    best_name = None
    best_score = -1

    for name in ARCHETYPE_NAMES:
        name_loc = page.locator(f"text={name}").first
        if await name_loc.count() == 0:
            continue

        try:
            card = name_loc.locator("..").locator("..").locator("..")
            card_text = await card.inner_text(timeout=3000)
        except Exception:
            continue

        if "Not eligible" in card_text:
            log(account_id, f"  {name}: Not eligible")
            continue

        card_claim = card.locator("button:has-text('Claim')")
        if await card_claim.count() == 0:
            log(account_id, f"  {name}: 无 Claim 按钮")
            continue

        score = -1
        m = re.search(r'REWARD\s*\n?\s*(\d+)', card_text)
        if m:
            score = int(m.group(1))
        log(account_id, f"  {name}: 可领取，分数 {score}")

        if score > best_score:
            best_score = score
            best_name = name

    return best_name


async def _click_archetype_claim(page: Page, context: BrowserContext, account_id: str, archetype: str) -> bool:
    """点击指定 Archetype 的 Claim 按钮，处理确认弹窗和钱包签名"""
    name_loc = page.locator(f"text={archetype}").first
    card = name_loc.locator("..").locator("..").locator("..")
    claim_btn = card.locator("button:has-text('Claim')")

    if await claim_btn.count() == 0:
        log(account_id, f"{archetype} 的 Claim 按钮已消失")
        return False

    log(account_id, f"点击 {archetype} Claim...")
    try:
        await claim_btn.first.click(timeout=5000)
    except Exception as e:
        log(account_id, f"点击 {archetype} Claim 失败: {e}")
        return False
    await asyncio.sleep(3)

    # 处理确认弹窗（"Claim XXX pts" 按钮）
    claim_pts_btn = page.locator("button").filter(has_text="Claim").filter(has_text="pts")
    for _ in range(10):
        if await claim_pts_btn.count() > 0:
            log(account_id, "检测到确认弹窗，点击 Claim pts...")
            try:
                await claim_pts_btn.first.click(timeout=5000)
            except Exception:
                pass
            break
        await asyncio.sleep(0.5)
    else:
        cancel_btn = page.locator("button:has-text('Cancel')")
        if await cancel_btn.count() > 0:
            sibling_claim = cancel_btn.locator("..").locator("button:has-text('Claim')")
            if await sibling_claim.count() > 0:
                log(account_id, "通过 Cancel 旁定位到确认按钮")
                try:
                    await sibling_claim.first.click(timeout=5000)
                except Exception:
                    pass
    await asyncio.sleep(3)

    # 处理钱包签名弹窗
    success_loc = page.locator("text=Quest completed successfully")
    for tick in range(60):
        try:
            if await success_loc.count() > 0:
                log(account_id, f"{archetype} Claim 成功！（Quest completed successfully）")
                return True
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
            log(account_id, f"Claim 签名弹窗: {wallet_page.url[-50:]}")
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

    log(account_id, f"{archetype} Claim 60s 未检测到成功")
    return False


async def claim_quest(
    page: Page, context: BrowserContext, account_id: str,
    popup_handler: WalletPopupHandler,
) -> bool:
    """
    新版 Claim：进入 Portal → 登录 → 扫描 Archetype → 领取分数最高的可领取项。
    每 UTC 日只能领一个 Archetype。
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

    # ── 检测登录状态，等待页面加载 ──
    for _ in range(15):
        for name in ARCHETYPE_NAMES:
            if await page.locator(f"text={name}").count() > 0:
                break
        else:
            signin_btn = page.locator("button:has-text('Sign in')")
            connect_btn = page.locator("button:has-text('Connect')")
            if await signin_btn.count() > 0 or await connect_btn.count() > 0:
                break
            await asyncio.sleep(1)
            continue
        break

    # ── 未登录则执行 Portal 登录 ──
    signin_btn = page.locator("button:has-text('Sign in')")
    connect_btn = page.locator("button:has-text('Connect')")
    if await signin_btn.count() > 0 or await connect_btn.count() > 0:
        if not await _portal_login(page, context, account_id):
            return False

        log(account_id, "Portal 登录完成，重新加载 Quest 页面...")
        try:
            await page.goto(PORTAL_QUEST_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(5)
        except Exception as e:
            log(account_id, f"重新进入 Quest 页面失败: {e}")
            return False

    # ── 等待 Archetype 卡片加载 ──
    for _ in range(15):
        for name in ARCHETYPE_NAMES:
            if await page.locator(f"text={name}").count() > 0:
                break
        else:
            await asyncio.sleep(1)
            continue
        break

    # ── 扫描 Archetype，找最高分可领取的 ──
    log(account_id, "扫描 Archetype 状态...")
    best = await _find_best_archetype(page, account_id)

    if best is None:
        log(account_id, "所有 Archetype 均 Not eligible 或无 Claim 按钮")
        return True

    # ── 领取最佳 Archetype（带 Cooldown 重试，最多 3 轮） ──
    for claim_round in range(3):
        ok = await _click_archetype_claim(page, context, account_id, best)
        if ok:
            log(account_id, f"Archetype Claim 成功: {best}")
            return True

        await _take_failure_screenshot(page, account_id, f"archetype_claim_round{claim_round+1}")

        # 刷新检查 Cooldown
        try:
            await page.goto(PORTAL_QUEST_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(5)
        except Exception:
            pass

        # 检查成功标志
        success_loc = page.locator("text=Quest completed successfully")
        try:
            if await success_loc.count() > 0:
                log(account_id, "刷新后检测到成功标志！")
                return True
        except Exception:
            pass

        # 检查 Cooldown
        cooldown_loc = page.locator("text=Cooldown active")
        if await cooldown_loc.count() > 0:
            try:
                cd_el = cooldown_loc.locator("..")
                text = await cd_el.inner_text(timeout=5000)
                m = re.search(r'(\d+):(\d+)', text)
                if m:
                    cd_secs = int(m.group(1)) * 60 + int(m.group(2))
                    log(account_id, f"Cooldown {m.group(1)}:{m.group(2)}，等待 {cd_secs+5}s（第 {claim_round+1}/3 轮）")
                    await asyncio.sleep(cd_secs + 5)
                else:
                    await asyncio.sleep(95)
            except Exception:
                await asyncio.sleep(95)
            try:
                await page.goto(PORTAL_QUEST_URL, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(5)
            except Exception:
                pass
            continue

        if claim_round < 2:
            log(account_id, f"第 {claim_round+1} 轮未成功，重试...")

    log(account_id, "Archetype Claim 失败：3 轮均未成功")
    await _take_failure_screenshot(page, account_id, "archetype_claim_failed_final")
    return False


# ════════════════════════════════════════════════════════
#  Weekly Reward 领取（每周一次）
# ════════════════════════════════════════════════════════

async def claim_weekly_reward(
    page: Page, context: BrowserContext, account_id: str,
    popup_handler: WalletPopupHandler,
) -> bool:
    """
    在 Portal Quest 页面领取 Weekly Tier Reward。
    每周只需领取一次，通过 weekly_claim.json 记录避免重复。
    假设调用前已在 Portal Quest 页面且已登录。
    """
    if _is_weekly_claimed(account_id):
        log(account_id, "Weekly Reward 本周已领取，跳过")
        return True

    log(account_id, "检查 Weekly Reward...")

    # 确保在 Quest 页面
    try:
        current_url = page.url or ""
        if "portal.linera.net" not in current_url:
            await page.goto(PORTAL_QUEST_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(5)
    except Exception:
        pass

    # 滚动到底部让 Weekly Reward 可见
    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await asyncio.sleep(1)

    # 查找 Weekly Reward 区域
    weekly_text = page.locator("text=Weekly Tier Reward")
    if await weekly_text.count() == 0:
        log(account_id, "未找到 Weekly Tier Reward 区域")
        return False

    # 检查是否已领取（显示 "CLAIMED THIS WEEK" 或 "Back next"）
    claimed_marker = page.locator("text=CLAIMED THIS WEEK")
    back_next = page.locator("text=Back next")
    if await claimed_marker.count() > 0 or await back_next.count() > 0:
        log(account_id, "Weekly Reward 已领取过（页面显示 Claimed）")
        _mark_weekly_claimed(account_id)
        return True

    # 找到 Weekly 区域的 Claim 按钮
    weekly_section = weekly_text.locator("..").locator("..")
    weekly_claim_btn = weekly_section.locator("button:has-text('Claim')")
    if await weekly_claim_btn.count() == 0:
        log(account_id, "Weekly Reward 区域未找到 Claim 按钮")
        return False

    log(account_id, "点击 Weekly Reward Claim...")
    try:
        await weekly_claim_btn.first.click(timeout=5000)
    except Exception as e:
        log(account_id, f"点击 Weekly Claim 失败: {e}")
        return False
    await asyncio.sleep(3)

    # 处理确认弹窗：点击 "Claim XXX pts" 红色按钮
    claim_pts_btn = page.locator("button").filter(has_text="Claim").filter(has_text="pts")
    for _ in range(10):
        if await claim_pts_btn.count() > 0:
            log(account_id, "检测到确认弹窗，点击 Claim pts...")
            try:
                await claim_pts_btn.first.click(timeout=5000)
            except Exception:
                pass
            break
        await asyncio.sleep(0.5)
    else:
        # 兜底：Cancel 按钮旁边的 Claim
        cancel_btn = page.locator("button:has-text('Cancel')")
        if await cancel_btn.count() > 0:
            sibling_claim = cancel_btn.locator("..").locator("button:has-text('Claim')")
            if await sibling_claim.count() > 0:
                log(account_id, "通过 Cancel 旁定位到 Claim 确认按钮")
                try:
                    await sibling_claim.first.click(timeout=5000)
                except Exception:
                    pass

    await asyncio.sleep(3)

    # 处理可能的钱包签名弹窗
    for tick in range(30):
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
            log(account_id, f"Weekly Claim 签名弹窗: {wallet_page.url[-50:]}")
            try:
                await wallet_page.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass
            await asyncio.sleep(2)
            await _click_wallet_button(wallet_page, account_id)
            log(account_id, "Weekly Claim 签名已确认")
            await asyncio.sleep(3)
            break

        # 检查是否已完成
        if await claimed_marker.count() > 0 or await back_next.count() > 0:
            break

        await asyncio.sleep(1)

    await asyncio.sleep(3)

    # 验证领取结果
    # 刷新页面确认
    try:
        await page.goto(PORTAL_QUEST_URL, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(5)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1)
    except Exception:
        pass

    claimed_marker = page.locator("text=CLAIMED THIS WEEK")
    back_next = page.locator("text=Back next")
    if await claimed_marker.count() > 0 or await back_next.count() > 0:
        log(account_id, "Weekly Reward 领取成功！")
        _mark_weekly_claimed(account_id)
        return True

    log(account_id, "Weekly Reward 领取结果不确定")
    await _take_failure_screenshot(page, account_id, "weekly_claim_uncertain")
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
                try:
                    await claim_weekly_reward(page, context, account_id, popup_handler)
                except Exception as e:
                    log(account_id, f"Weekly Reward 异常（不影响任务）: {e}")
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

    await select_duration(page, account_id)

    # ── Step 2.5: 启动时检查一次下注金额（全局生效，无需每次检查） ──
    await ensure_bet_amount(page, account_id)

    target_pairs = target_bets // 2 if target_bets > 1 else target_bets
    log(account_id, f"初始化完成，开始下注（目标 {target_pairs} 对 / {target_bets} 次）")

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
            await select_duration(page, account_id)
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
        log(account_id, "Claim Quest 未成功，等待下轮补跑重试")
        _update_status(account_id, status="failed", error="Claim未成功")
        return False

    # ── Step 7: Weekly Reward（每周一次，失败不影响整体任务） ──
    try:
        await claim_weekly_reward(page, context, account_id, popup_handler)
    except Exception as e:
        log(account_id, f"Weekly Reward 领取异常（不影响任务完成）: {e}")

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
