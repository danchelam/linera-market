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

__version__ = "2026.03.19.8"

import asyncio
import random
import sys

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
    ADSPOWER_API_KEY,
)

# ─── 页面配置 ─────────────────────────────────────────
DAPP_URL = "https://linera.market"
MARKETS = ["BTC", "ETH", "SOL"]
TARGET_BETS = 15
BET_AMOUNT = "1"


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
            if "chrome-extension://" not in url or "offscreen" in url:
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
                            if "chrome-extension://" in u2 and "offscreen" not in u2:
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

async def check_bet_success(page: Page) -> bool:
    """
    检测下注成功标志：页面出现 div.card-glass 中包含乘数（x1.78 / x2.07 等）。
    成功时有两个 card-glass（HIGHER 和 LOWER 各一个），内含 bet-arrow-icon。
    """
    try:
        cards = page.locator("div.card-glass")
        if await cards.count() == 0:
            return False
        # 检查 card 内是否包含乘数文本 (xN.NN)
        multiplier = page.locator("div.card-glass span[class*='text-danger'], div.card-glass span[class*='text-success']")
        if await multiplier.count() > 0:
            text = await multiplier.first.inner_text(timeout=2000)
            if text.strip().startswith("x"):
                return True
        # 兜底：检查 bet-arrow-icon 存在
        if await page.locator("div.bet-arrow-icon, div.bet-arrow-icon-danger").count() > 0:
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
#  设置下注金额（SAVE 后持久化，只需一次）
# ════════════════════════════════════════════════════════

async def set_bet_amount(page: Page, account_id: str, amount: str) -> bool:
    # 点击金额按钮（包含 Currency 图标的圆形按钮）
    try:
        ok = await page.evaluate("""() => {
            // 找包含 Currency 图标的按钮区域
            const img = document.querySelector('img[alt="Currency"]');
            if (img) {
                let el = img.parentElement;
                for (let i = 0; i < 3; i++) {
                    if (!el) break;
                    if (el.tagName === 'BUTTON' || getComputedStyle(el).cursor === 'pointer') {
                        el.click();
                        return true;
                    }
                    el = el.parentElement;
                }
                img.parentElement.click();
                return true;
            }
            // 兜底：找 rounded-full 且包含 img 的 div
            const divs = document.querySelectorAll('div.rounded-full');
            for (const d of divs) {
                if (d.querySelector('img')) {
                    d.click();
                    return true;
                }
            }
            return false;
        }""")
        if ok:
            log(account_id, "已点击金额按钮")
            await asyncio.sleep(1.5)
        else:
            log(account_id, "未找到金额按钮")
            return False
    except Exception as e:
        log(account_id, f"点击金额按钮失败: {e}")
        return False

    # 等待输入框出现
    input_loc = page.locator("input[inputmode='decimal']")
    for _ in range(10):
        if await input_loc.count() > 0 and await input_loc.first.is_visible():
            break
        await asyncio.sleep(0.5)
    else:
        log(account_id, "金额输入框未出现")
        await page.keyboard.press("Escape")
        return False

    # 填写金额
    try:
        target = input_loc.first
        await target.click(click_count=3, timeout=2000)
        await asyncio.sleep(0.2)
        await target.fill(amount)
        current = await target.input_value()
        if current != amount:
            await page.evaluate("""(amount) => {
                const inp = document.querySelector('input[inputmode="decimal"]');
                if (!inp) return;
                const setter = Object.getOwnPropertyDescriptor(
                    HTMLInputElement.prototype, 'value').set;
                setter.call(inp, amount);
                inp.dispatchEvent(new Event('input', {bubbles: true}));
                inp.dispatchEvent(new Event('change', {bubbles: true}));
            }""", amount)
        log(account_id, f"金额已填写: {amount}")
    except Exception as e:
        log(account_id, f"填写金额失败: {e}")
        await page.keyboard.press("Escape")
        return False

    # 点击 SAVE
    await asyncio.sleep(0.5)
    try:
        save_btn = page.locator("button:has-text('SAVE')")
        if await save_btn.count() > 0:
            await save_btn.first.click(timeout=3000)
            log(account_id, "已点击 SAVE，金额保存成功")
            await asyncio.sleep(1)
        else:
            log(account_id, "未找到 SAVE 按钮")
            await page.keyboard.press("Escape")
            return False
    except Exception as e:
        log(account_id, f"点击 SAVE 失败: {e}")
        await page.keyboard.press("Escape")
        return False

    # 确认弹窗已关闭
    for _ in range(10):
        if await page.locator("div[data-slot='wrapper']").count() == 0:
            break
        await asyncio.sleep(0.5)
    return True


# ════════════════════════════════════════════════════════
#  登录流程（禁用后台 handler，手动处理弹窗）
# ════════════════════════════════════════════════════════

async def login(
    page: Page, context: BrowserContext, account_id: str,
    popup_handler: WalletPopupHandler, bet_amount: str,
) -> bool:
    """
    初始化：打开网站 → 手动处理钱包签名 → 选 1 minute → 设金额
    登录期间禁用后台 handler 避免冲突。
    """
    # 禁用后台处理器，防止抢弹窗
    popup_handler.enabled = False

    try:
        market = random.choice(MARKETS)
        url = f"{DAPP_URL}/?market={market}"

        for attempt in range(3):
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                break
            except Exception as e:
                if attempt < 2:
                    log(account_id, f"导航失败，重试 ({attempt+1}/3)...")
                    await asyncio.sleep(3)
                else:
                    log(account_id, f"导航彻底失败: {e}")
                    return False

        log(account_id, "页面已打开，等待加载...")
        await asyncio.sleep(8)

        # ── 检测是否需要 Connect Wallet（带重试） ──
        okx_selected = False
        for connect_try in range(3):
            connect_btn = page.locator("button:has-text('Connect Wallet')")
            if await connect_btn.count() == 0:
                if connect_try == 0:
                    log(account_id, "未检测到 Connect Wallet 按钮（可能已连接）")
                okx_selected = True
                break

            log(account_id, f"检测到 Connect Wallet 按钮，开始连接...（第 {connect_try+1} 次）")
            await connect_btn.first.click(timeout=5000)
            await asyncio.sleep(2)

            # 等待 OKX Wallet 选项加载（最多 10 秒）
            okx_option = page.locator("button.wallet-list-item__tile:has(img[alt='okxwallet'])")
            for _ in range(20):
                if await okx_option.count() > 0:
                    break
                await asyncio.sleep(0.5)

            if await okx_option.count() > 0:
                await okx_option.first.click(timeout=5000)
                log(account_id, "已选择 OKX Wallet")
                await asyncio.sleep(3)
                okx_selected = True
                break

            # 文本匹配兜底
            okx_text = page.locator("text=OKX Wallet")
            if await okx_text.count() > 0:
                await okx_text.first.click(timeout=5000)
                log(account_id, "已选择 OKX Wallet (文本匹配)")
                await asyncio.sleep(3)
                okx_selected = True
                break

            # 没找到 → 关闭弹窗，刷新页面重试
            log(account_id, "OKX Wallet 未加载，刷新页面重试...")
            await page.keyboard.press("Escape")
            await asyncio.sleep(1)
            try:
                await page.reload(wait_until="domcontentloaded", timeout=20000)
            except Exception:
                pass
            await asyncio.sleep(5)

        if not okx_selected:
            log(account_id, "多次尝试后仍未找到 OKX Wallet，跳过此账号")
            return False

        # ── 处理钱包弹窗（可能是解锁弹窗或签名弹窗） ──
        for round_num in range(5):
            wallet_page = None
            for p in context.pages:
                try:
                    u = p.url or ""
                except Exception:
                    continue
                if "chrome-extension://" in u and "offscreen" not in u:
                    wallet_page = p
                    break

            if not wallet_page:
                if round_num == 0:
                    # 第一轮没弹窗，多等一下
                    await asyncio.sleep(5)
                    continue
                break

            log(account_id, f"发现钱包弹窗: {wallet_page.url[-60:]}")
            try:
                await wallet_page.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass
            await asyncio.sleep(2)

            # 检查是否有密码框（解锁弹窗）
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
                # 解锁后可能出现签名弹窗，继续循环处理
                log(account_id, f"钱包解锁弹窗已处理（第 {round_num+1} 轮）")
            else:
                # 普通签名/连接弹窗
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

        # ── 处理剩余弹窗 + 等待 Claiming chain（同时进行） ──
        # 弹窗可能在 Claiming chain 期间才弹出，所以需要边等边处理
        await asyncio.sleep(2)
        claiming_sel = "span:text-is('Claiming chain...')"
        claiming_loc = page.locator(claiming_sel)
        popup_count = 0
        claiming_logged = False

        for tick in range(90):
            # 先检查有没有钱包弹窗需要处理
            wallet_page = None
            for p in context.pages:
                try:
                    u = p.url or ""
                except Exception:
                    continue
                if "chrome-extension://" in u and "offscreen" not in u:
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

                # 检查是否是解锁弹窗
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

            # 没有弹窗，检查 Claiming chain
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

        # 选择 1 minute 市场
        await select_1_minute(page, account_id)

        # 处理选择市场可能触发的钱包弹窗
        await asyncio.sleep(2)
        handled = await handle_wallet_popups_manual(context, account_id, timeout=5)
        if handled:
            log(account_id, "市场切换后的钱包弹窗已处理")
            await asyncio.sleep(2)

        # 设置下注金额
        await set_bet_amount(page, account_id, bet_amount)

        # 验证按钮可用
        for i in range(10):
            try:
                higher = page.locator("button.btn-higher")
                if await higher.count() > 0:
                    if await higher.get_attribute("disabled") is None:
                        log(account_id, "初始化完成，按钮已激活")
                        return True
            except Exception:
                pass
            await asyncio.sleep(2)

        log(account_id, "按钮状态未确认，尝试继续...")
        return True
    finally:
        # 重新启用后台处理器
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

    # 0. 检测页面是否卡住
    if await is_page_stuck(page):
        recovered = await recover_from_stuck(page, account_id)
        if not recovered:
            return False
        await select_1_minute(page, account_id)

    # 1. 检查池子余额
    balance = await get_pool_balance(page)
    if balance in ("0", "0.00", "0.000", ""):
        log(account_id, f"池子余额为 '{balance}'，切换市场...")
        for m in random.sample(MARKETS, len(MARKETS)):
            await switch_market(page, account_id, m)
            await asyncio.sleep(2)
            new_bal = await get_pool_balance(page)
            if new_bal not in ("0", "0.00", "0.000", ""):
                log(account_id, f"{m} 池子余额: {new_bal}")
                break
    elif bet_number % 3 == 1:
        await switch_market(page, account_id, random.choice(MARKETS))

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

    # 4. 随机方向并点击
    direction = random.choice(["HIGHER", "LOWER"])
    btn_cls = "btn-higher" if direction == "HIGHER" else "btn-lower"
    try:
        await page.locator(f"button.{btn_cls}").first.click(timeout=5000)
        log(account_id, f"[{bet_number}/{target_bets}] 点击 {direction}")
    except Exception as e:
        log(account_id, f"点击 {direction} 失败: {e}")
        return False

    # 5. 等待成功标志（后台 handler 会自动处理钱包弹窗）
    #    不再手动抢弹窗，避免和后台 handler 冲突
    log(account_id, f"[{bet_number}/{target_bets}] 等待钱包自动签名 + 成功标志...")
    success = False
    for i in range(60):
        if STOP_FLAG:
            return False
        if await check_bet_success(page):
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
    bet_amount: str = BET_AMOUNT,
) -> bool:
    completed = 0
    consecutive_failures = 0

    log(account_id, f"开始下注，目标 {target_bets} 次")

    while completed < target_bets and not STOP_FLAG:
        if consecutive_failures >= 5:
            log(account_id, f"连续失败 {consecutive_failures} 次，刷新页面...")
            popup_handler.enabled = False
            try:
                await page.reload(wait_until="domcontentloaded", timeout=30000)
            except Exception:
                pass
            await asyncio.sleep(5)
            await handle_wallet_popups_manual(context, account_id, timeout=15)
            await select_1_minute(page, account_id)
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
        else:
            consecutive_failures += 1
            log(account_id,
                f"下注失败（连续失败: {consecutive_failures}），等待后重试...")
            await asyncio.sleep(5)

    if STOP_FLAG:
        log(account_id, f"收到停止信号，已完成 {completed}/{target_bets} 次")
        return False

    log(account_id, f"全部 {target_bets} 次下注完成")
    return True


# ════════════════════════════════════════════════════════
#  上传交易记录
# ════════════════════════════════════════════════════════

async def upload_trades(
    page: Page, context: BrowserContext, account_id: str,
) -> bool:
    """
    下注完成后上传交易：
    1. 点击菜单按钮（三横线图标）
    2. 点击 Leaderboard 链接
    3. 等待 Upload Trades 按钮出现并点击
    4. 处理签名弹窗
    """
    log(account_id, "开始上传交易记录...")

    # 1. 点击菜单按钮（lucide-menu 图标）
    try:
        menu_btn = page.locator("button:has(svg.lucide-menu)")
        if await menu_btn.count() == 0:
            menu_btn = page.locator("svg.lucide-menu").locator("..")
        if await menu_btn.count() > 0:
            await menu_btn.first.click(timeout=5000)
            log(account_id, "已点击菜单按钮")
            await asyncio.sleep(1.5)
        else:
            log(account_id, "未找到菜单按钮")
            return False
    except Exception as e:
        log(account_id, f"点击菜单按钮失败: {e}")
        return False

    # 2. 点击 Leaderboard 链接
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

    # 3. 等待 Upload Trades 按钮出现
    upload_btn = page.locator("button:has-text('Upload Trades')")
    for _ in range(30):
        if await upload_btn.count() > 0:
            break
        await asyncio.sleep(1)
    else:
        log(account_id, "Upload Trades 按钮未出现")
        return False

    # 4. 点击 Upload Trades
    try:
        await upload_btn.first.click(timeout=5000)
        log(account_id, "已点击 Upload Trades")
        await asyncio.sleep(2)
    except Exception as e:
        log(account_id, f"点击 Upload Trades 失败: {e}")
        return False

    # 5. 处理签名弹窗（由后台 handler 自动处理）
    log(account_id, "等待上传签名确认...")
    await asyncio.sleep(10)

    # 检查是否上传成功（按钮消失或文案变化）
    await asyncio.sleep(3)
    if await upload_btn.count() == 0 or await page.locator("text=Upload Trades").count() == 0:
        log(account_id, "交易记录上传成功")
    else:
        log(account_id, "上传可能已完成（无法确认状态）")

    return True


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
    bet_amount = str(kwargs.get("bet_amount", BET_AMOUNT))

    if not await login(page, context, account_id, popup_handler, bet_amount):
        log(account_id, "登录失败")
        return False

    bet_ok = await run_betting_loop(
        page, context, account_id, popup_handler, target_bets, bet_amount,
    )

    if bet_ok:
        await upload_trades(page, context, account_id)

    return bet_ok


# ════════════════════════════════════════════════════════
#  入口
# ════════════════════════════════════════════════════════

def main():
    accounts = load_accounts()
    if not accounts:
        print("未读取到任何账号，请检查 shuju.xlsx")
        sys.exit(1)

    print(f"共读取到 {len(accounts)} 个账号。")
    print("1. 单窗口测试（第 1 个账号）")
    print("2. 批量运行")

    mode = input("请输入数字 (1/2): ").strip()

    if mode == "1":
        target = accounts[0]
        print(f"单窗口测试: {target.id}")
        asyncio.run(run_batch(
            [target], linera_task, max_workers=1, api_key=ADSPOWER_API_KEY,
        ))
    elif mode == "2":
        try:
            workers = int(input("请输入并发数（建议 1-3）: ").strip())
        except ValueError:
            workers = 1
        print(f"批量运行，并发: {workers}")
        asyncio.run(run_batch(
            accounts, linera_task, max_workers=workers, api_key=ADSPOWER_API_KEY,
        ))
    else:
        print("无效输入。")


if __name__ == "__main__":
    main()
