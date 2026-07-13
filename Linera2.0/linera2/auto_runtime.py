from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from time import monotonic
from typing import Awaitable, Callable

from playwright.async_api import BrowserContext, Page

from .auto_page import AutoPage
from .auto_session import AutoSessionRecord, AutoSessionState, AutoSessionStore
from .auto_tracking import HistoryCounts, ResolutionKeyMonitor, RoundTracker
from .readiness import (
    ReadinessResult,
    check_account_ready,
    read_frontend_snapshot,
)


LogFunction = Callable[[str, str], None]


def _default_log(account_id: str, message: str) -> None:
    print(f"[{account_id}] {message}", flush=True)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _safe_reason(exc: BaseException | str) -> str:
    text = str(exc).replace("\r", " ").replace("\n", " ").strip()
    return text[:180] or type(exc).__name__


async def _wait_history_stable(
    adapter: AutoPage,
    *,
    deadline: float,
    clock: Callable[[], float],
    sleep_func: Callable[[float], Awaitable[None]],
    poll_interval: float,
) -> HistoryCounts:
    previous: HistoryCounts | None = None
    while clock() < deadline:
        current = await adapter.read_history_counts()
        if current == previous:
            return current
        previous = current
        await sleep_func(poll_interval)
    raise TimeoutError("等待 History 稳定超时")


def _has_active_pair(history: HistoryCounts) -> bool:
    return history.active_higher > 0 or history.active_lower > 0


async def _wait_active_positions_clear(
    adapter: AutoPage,
    initial: HistoryCounts,
    *,
    deadline: float,
    clock: Callable[[], float],
    sleep_func: Callable[[float], Awaitable[None]],
    poll_interval: float,
) -> HistoryCounts:
    current = initial
    while _has_active_pair(current):
        if clock() >= deadline:
            raise TimeoutError("等待遗留 Live/Open 仓位结算超时")
        await sleep_func(poll_interval)
        current = await adapter.read_history_counts()
    return current


async def run_auto_session(
    page: Page,
    context: BrowserContext,
    account_id: str,
    *,
    store: AutoSessionStore,
    readiness: ReadinessResult | None = None,
    timeout: int = 1_200,
    settle_timeout: int = 180,
    poll_interval: float = 2.0,
    target_override: int | None = None,
    log_func: LogFunction = _default_log,
    adapter_factory: Callable[[Page], AutoPage] = AutoPage,
    monitor_factory: Callable[[], ResolutionKeyMonitor] = ResolutionKeyMonitor,
    sleep_func: Callable[[float], Awaitable[None]] = asyncio.sleep,
    snapshot_reader=read_frontend_snapshot,
    readiness_checker=check_account_ready,
    clock: Callable[[], float] = monotonic,
) -> AutoSessionRecord:
    account_id = str(account_id)
    adapter = adapter_factory(page)
    record: AutoSessionRecord | None = None
    monitor: ResolutionKeyMonitor | None = None
    listener_attached = False
    stop_attempted = False

    async def persist_failure(reason: str, *, inspect_auto: bool = True) -> AutoSessionRecord:
        nonlocal record, stop_attempted
        if record is None:
            record = store.get_or_create_daily(
                account_id, target_override=target_override
            )
        still_running = False
        if inspect_auto:
            try:
                state = await adapter.read_state()
                still_running = state.running or state.auto_on_visible
                if still_running and not stop_attempted:
                    stop_attempted = True
                    await adapter.stop_once()
                    post_stop = await adapter.read_state()
                    still_running = post_stop.running or post_stop.auto_on_visible
            except Exception:
                still_running = True
        record.state = AutoSessionState.FAILED.value
        record.failure_reason = _safe_reason(reason)
        record.ended_at = _now()
        record.auto_still_running = still_running
        store.update(record)
        log_func(account_id, f"Auto 会话失败：{record.failure_reason}")
        return record

    try:
        if readiness is None:
            readiness = await readiness_checker(page, context, account_id)
        record = store.get_or_create_daily(
            account_id, target_override=target_override
        )
        if record.state == AutoSessionState.COMPLETED.value:
            log_func(account_id, "今天的 Auto 会话已完成，跳过")
            return record
        if not readiness.ready:
            return await persist_failure(
                f"账号未就绪：{readiness.state}", inspect_auto=False
            )
        if readiness.coins is None or readiness.coins < 2:
            return await persist_failure("Coins 少于 2，不能启动 Auto", inspect_auto=False)

        monitor = monitor_factory()
        page.on("request", monitor.on_request)
        listener_attached = True
        baseline_keys: set[int] = set()
        baseline_deadline = clock() + max(2, min(10, poll_interval * 5))
        while clock() < baseline_deadline and not baseline_keys:
            await sleep_func(poll_interval)
            baseline_keys = monitor.snapshot()
        if not baseline_keys:
            return await persist_failure("未取得后端轮次基线，未启动 Auto")

        initial_state = await adapter.read_state()
        if initial_state.running or initial_state.auto_on_visible:
            stop_attempted = True
            if not await adapter.stop_once():
                return await persist_failure("无法停止遗留 Auto")
            await _wait_history_stable(
                adapter,
                deadline=clock() + min(settle_timeout, 30),
                clock=clock,
                sleep_func=sleep_func,
                poll_interval=poll_interval,
            )
            stop_attempted = False
            baseline_keys = monitor.snapshot()
            if not baseline_keys:
                return await persist_failure("遗留 Auto 停止后轮次基线不可用")
        baseline_history = await adapter.read_history_counts()
        if _has_active_pair(baseline_history):
            baseline_history = await _wait_active_positions_clear(
                adapter,
                baseline_history,
                deadline=clock() + max(0.01, settle_timeout),
                clock=clock,
                sleep_func=sleep_func,
                poll_interval=poll_interval,
            )
            baseline_keys = monitor.snapshot()
            if not baseline_keys:
                return await persist_failure("遗留仓位结算后轮次基线不可用")
        record.state = AutoSessionState.CONFIGURING.value
        record.start_coins = readiness.coins
        record.current_coins = readiness.coins
        record.end_coins = None
        record.baseline_resolution_keys = sorted(baseline_keys)
        record.baseline_higher_rows = baseline_history.higher
        record.baseline_lower_rows = baseline_history.lower
        record.started_at = record.started_at or _now()
        record.ended_at = None
        record.failure_reason = None
        record.auto_still_running = False
        store.update(record)

        await adapter.open_configuration()
        await adapter.configure_one_plus_one()
        await adapter.start()
        record.state = AutoSessionState.RUNNING.value
        store.update(record)
        log_func(
            account_id,
            f"Auto 已启动：目标 {record.target_rounds} 轮，已完成 {record.completed_rounds} 轮",
        )

        tracker = RoundTracker(
            baseline_keys,
            baseline_history,
            already_counted=set(record.counted_resolution_keys),
        )
        deadline = clock() + max(1, timeout)
        while record.completed_rounds < record.target_rounds:
            if clock() >= deadline:
                raise TimeoutError("Auto 会话达到硬超时")
            keys = monitor.snapshot()
            history = await adapter.read_history_counts()
            added = tracker.observe(keys, history)
            if added:
                record.counted_resolution_keys.extend(added)
                record.counted_resolution_keys = sorted(
                    set(record.counted_resolution_keys)
                )
                record.completed_rounds += len(added)
                record.completed_rounds = min(
                    record.completed_rounds, record.target_rounds
                )
                record.nominal_stake = record.completed_rounds * 2
                store.update(record)
                log_func(
                    account_id,
                    f"Auto 轮次 {record.completed_rounds}/{record.target_rounds}",
                )
            if record.completed_rounds < record.target_rounds:
                await sleep_func(poll_interval)

        record.state = AutoSessionState.STOPPING.value
        store.update(record)
        stop_attempted = True
        if not await adapter.stop_once():
            return await persist_failure("达到目标后无法停止 Auto")

        record.state = AutoSessionState.SETTLING.value
        record.auto_still_running = False
        store.update(record)
        settle_deadline = clock() + max(0.01, settle_timeout)
        settling_history = await adapter.read_history_counts()
        await _wait_active_positions_clear(
            adapter,
            settling_history,
            deadline=settle_deadline,
            clock=clock,
            sleep_func=sleep_func,
            poll_interval=poll_interval,
        )
        await _wait_history_stable(
            adapter,
            deadline=settle_deadline,
            clock=clock,
            sleep_func=sleep_func,
            poll_interval=poll_interval,
        )
        snapshot = await snapshot_reader(page)
        record.current_coins = snapshot.coins
        record.end_coins = snapshot.coins
        record.state = AutoSessionState.COMPLETED.value
        record.ended_at = _now()
        record.failure_reason = None
        record.auto_still_running = False
        store.update(record)
        log_func(
            account_id,
            f"Auto 会话完成：{record.completed_rounds} 轮，名义 Stake {record.nominal_stake}",
        )
        return record
    except Exception as exc:
        return await persist_failure(_safe_reason(exc))
    finally:
        if listener_attached and monitor is not None:
            try:
                page.remove_listener("request", monitor.on_request)
            except Exception:
                pass
