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

__version__ = "2026.03.24.4"

import asyncio
import random
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
                ACCOUNT_TARGET_TRADES = _json.load(f)
        except Exception:
            pass


def _save_target_trades():
    try:
        with open(_TARGET_TRADES_FILE, "w", encoding="utf-8") as f:
            _json.dump(ACCOUNT_TARGET_TRADES, f, ensure_ascii=False)
    except Exception:
        pass


_load_target_trades()

# 实时状态追踪：account_id → 状态字典（供 Web 前端展示）
TASK_STATUS: dict[str, dict] = {}


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


async def wait_settlement_done(page: Page, account_id: str, timeout: int = 120):
    if not await is_settling(page):
        return
    log(account_id, "市场结算中，等待...")
    for _ in range(timeout):
        if STOP_FLAG or not await is_settling(page):
            log(account_id, "结算完成")
            return
        await asyncio.sleep(1)
    log(account_id, "等待结算超时")


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

        # ── 检测是否需要 Connect Wallet（带重试） ──
        need_full_connect = False
        connect_btn = page.locator("button:has-text('Connect Wallet')")
        if await connect_btn.count() > 0:
            need_full_connect = True

        if need_full_connect:
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
                    # 检查是否直接弹出了钱包弹窗（跳过选择列表）
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
                    # 立即处理该弹窗
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

            # ── 首次连接：处理钱包弹窗（解锁/连接） ──
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

            # ── 检测 Select Ethereum network 按钮 ──
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
        else:
            log(account_id, "已登录过，跳过 Connect Wallet 流程")

        # ── 统一阶段：处理弹窗 + 等待 Claiming chain ──
        # 不管是首次连接还是已登录，都在这里统一处理所有剩余弹窗和 Claiming chain
        await asyncio.sleep(2)
        claiming_sel = "span:text-is('Claiming chain...')"
        claiming_loc = page.locator(claiming_sel)
        popup_count = 0
        claiming_logged = False

        for tick in range(90):
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
                await asyncio.sleep(3)
                continue

            if await claiming_loc.count() > 0:
                if not claiming_logged:
                    log(account_id, "检测到 Claiming chain...，等待完成")
                    claiming_logged = True
                await asyncio.sleep(1)
                continue

            # 没弹窗也没 Claiming chain → 登录完成
            if tick > 3:
                if claiming_logged:
                    log(account_id, "Claiming chain 完成")
                break
            await asyncio.sleep(1)
        else:
            if claiming_logged:
                log(account_id, "Claiming chain 等待超时，继续执行")

        await asyncio.sleep(2)

        # ── 在 History 页面读取 Trades 基线 ──
        initial_trades = await get_trades_count(page, account_id)
        if initial_trades >= 0:
            log(account_id, f"登录完成，Trades 基线: {initial_trades}")
        else:
            log(account_id, "登录完成，无法读取 Trades 基线")
            initial_trades = -1

        # 存入 page 对象供后续使用
        page._initial_trades = initial_trades
        return True
    finally:
        popup_handler.enabled = True


# ════════════════════════════════════════════════════════
#  单次下注（依赖后台 handler 自动签名）
# ════════════════════════════════════════════════════════

async def place_single_bet(
    page: Page,
    context: BrowserContext,
    account_id: str,
    bet_number: int,
    target_bets: int = TARGET_BETS,
) -> bool:
    """
    单次下注流程：
    0. 检测页面是否卡住 → 卡住则切换市场恢复
    1. 检查池子 → 切换市场(+1min)
    2. 等结算完成
    3. 点击 HIGHER/LOWER
    4. 后台 WalletPopupHandler 自动处理签名
    5. 等待成功标志确认
    """

    # 0. 检测 RPC 致命错误
    if await is_fatal_error(page):
        log(account_id, "检测到 RPC 致命错误，跳过该窗口")
        return False

    # 0.5 检测页面是否卡住
    if await is_page_stuck(page):
        recovered = await recover_from_stuck(page, account_id)
        if not recovered:
            return False

    # 1. 检查池子余额（仅在余额为 0 时才切换市场）
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

    # 2. 等待结算完成
    await wait_settlement_done(page, account_id)

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
            log(account_id, "HIGHER/LOWER 长时间不可用")
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
        return False

    # 6. 等待成功标志：card-glass 数量 > baseline
    log(account_id, f"[{bet_number}/{target_bets}] 等待钱包自动签名 + 成功标志...")
    success = False
    for i in range(60):
        if STOP_FLAG:
            return False
        if await check_bet_success(page, baseline):
            success = True
            break
        await asyncio.sleep(1)

    if success:
        log(account_id, f"[{bet_number}/{target_bets}] 下注成功")
    else:
        log(account_id, f"[{bet_number}/{target_bets}] 60s 内未检测到成功标志")
        return False

    # 6. 等待倒计时归零（本轮结束）
    await wait_countdown(page, account_id)
    await asyncio.sleep(random.uniform(1.5, 2.5))

    # 7. 等待结算完成
    await wait_settlement_done(page, account_id)
    return True


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
    total_failures = 0
    max_total_failures = 10

    log(account_id, f"开始下注，目标 {target_bets} 次")
    _update_status(account_id, status="betting", bets_target=target_bets, bets_completed=0)

    while completed < target_bets and not STOP_FLAG:
        if total_failures >= max_total_failures:
            log(account_id, f"累计失败 {total_failures} 次，放弃下注，等待第二轮重试")
            _update_status(account_id, status="failed", error=f"累计失败{total_failures}次")
            return False

        if consecutive_failures >= 5:
            log(account_id, f"连续失败 {consecutive_failures} 次，刷新页面...")
            popup_handler.enabled = False
            try:
                await page.reload(wait_until="domcontentloaded", timeout=30000)
            except Exception:
                pass
            await asyncio.sleep(5)
            if not await wait_rpc_recovery(page, account_id, context):
                popup_handler.enabled = True
                return False
            await handle_wallet_popups_manual(context, account_id, timeout=15)
            await asyncio.sleep(3)
            popup_handler.enabled = True
            consecutive_failures = 0

        success = await place_single_bet(
            page, context, account_id, completed + 1, target_bets,
        )

        if success:
            completed += 1
            consecutive_failures = 0
            log(account_id, f"已完成 {completed}/{target_bets} 次下注")
            _update_status(account_id, bets_completed=completed, error="")
        else:
            consecutive_failures += 1
            total_failures += 1
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
    """读取 Leaderboard 页面上 Trades 后面的数字"""
    try:
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

    # ── 点击 Claim ──
    if await claim_btn.count() == 0:
        log(account_id, "未找到 Claim 按钮，可能已经 Claim 过或未达标")
        return False

    try:
        await claim_btn.first.click(timeout=5000)
        log(account_id, "已点击 Claim，等待钱包签名...")
        await asyncio.sleep(2)
    except Exception as e:
        log(account_id, f"点击 Claim 失败: {e}")
        return False

    # 手动处理 Claim 的钱包签名弹窗
    claim_signed = False
    for tick in range(45):
        # 检查是否已完成（按钮消失或变 disabled）
        try:
            if await claim_btn.count() == 0:
                claim_signed = True
                break
            is_disabled = await claim_btn.first.get_attribute("disabled")
            if is_disabled is not None:
                claim_signed = True
                break
        except Exception:
            pass

        # 查找并处理钱包弹窗
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

    if claim_signed:
        log(account_id, "Claim Quest 完成")
    else:
        log(account_id, "Claim Quest 签名超时")
    return claim_signed


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
    result = await _linera_task_inner(page, context, account_id, popup_handler, target_bets)
    if not result:
        cur = TASK_STATUS.get(account_id, {})
        if cur.get("status") not in ("done", "failed"):
            _update_status(account_id, status="failed", error=cur.get("error") or "任务异常退出")
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
            if remaining <= 0:
                log(account_id, f"Trades 已达标: {initial_trades} >= {target_total}（上轮进度继承），跳过下注")
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

    # ── Step 4: 校验 History 笔数，不足则补跑 ──
    _update_status(account_id, status="verifying")
    if target_total >= 0:
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

            log(account_id, f"Trades 不足: {final_trades}/{target_total}，还差 {shortfall} 次，补跑中...")
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

        # 上传前最后确认
        if not await navigate_to_history(page, account_id):
            log(account_id, "上传前无法进入 History，中止上传")
            return False
        await asyncio.sleep(2)
        final_trades = await get_trades_count(page, account_id)
        if final_trades < 0:
            log(account_id, "无法读取 Trades 数量，中止上传")
            return False
        if final_trades < target_total:
            log(account_id, f"Trades 仍不足 ({final_trades}/{target_total})，跳过上传")
            _update_status(account_id, status="failed", error=f"Trades不足 {final_trades}/{target_total}")
            return False
        log(account_id, f"笔数已达标，开始上传：Trades {final_trades} >= {target_total}")
    else:
        log(account_id, "无 Trades 基线，跳过上传前校验")

    # ── Step 5: 上传 ──
    _update_status(account_id, status="uploading")
    await upload_trades(page, context, account_id)

    # ── Step 6: Claim Quest ──
    _update_status(account_id, status="claiming")
    await claim_quest(page, context, account_id, popup_handler)

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
