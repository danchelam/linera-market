"""
完整流程测试：登录 → 下注30次 → 上传记录 → Claim Archetype → Weekly Reward
使用第一个账号，单窗口运行。Monkey-patch log 函数使输出到 stdout。
"""

import asyncio
import sys
import os
import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 先 patch log，让输出到终端可见
import base_module
_original_log = base_module.log
def _patched_log(account_id, msg):
    t = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{t}] [{account_id}] {msg}", flush=True)
    _original_log(account_id, msg)
base_module.log = _patched_log

import linera_task
linera_task.log = _patched_log
linera_task.TARGET_PAIRS = 15
linera_task.TARGET_BETS = linera_task.TARGET_PAIRS * 2
linera_task.MARKET_DURATION = 3
linera_task.BET_AMOUNT = "2"  # 测试用 $2，正式用 $25

from base_module import load_accounts, run_batch
from linera_task import linera_task as task_func


def main():
    accounts = load_accounts()
    if not accounts:
        print("未找到账号", flush=True)
        return

    target = accounts[7]
    print(f"[测试] 账号: {target.ua or target.id}, "
          f"TARGET_PAIRS={linera_task.TARGET_PAIRS}, "
          f"MARKET_DURATION={linera_task.MARKET_DURATION}min, "
          f"BET_AMOUNT=${linera_task.BET_AMOUNT}", flush=True)
    asyncio.run(run_batch([target], task_func, max_workers=1))


if __name__ == "__main__":
    main()
