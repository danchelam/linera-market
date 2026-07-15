from __future__ import annotations

import datetime
import threading
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


_print_lock = threading.Lock()


@dataclass(frozen=True)
class AccountInfo:
    id: str
    ua: str = ""
    proxy: str = ""


def load_accounts(path: Path) -> list[AccountInfo]:
    base = path.with_suffix("")
    chosen = path
    if not chosen.exists():
        alternate = base.with_suffix(".csv")
        if alternate.exists():
            chosen = alternate

    accounts: list[AccountInfo] = []
    try:
        if chosen.suffix.lower() == ".csv":
            frame = pd.read_csv(
                chosen,
                dtype=str,
                encoding="utf-8-sig",
                keep_default_na=False,
            )
        else:
            frame = pd.read_excel(chosen, header=1, dtype=str).fillna("")

        def string_value(value) -> str:
            return str(value).strip() if value is not None else ""

        for _, row in frame.iterrows():
            account_id = (
                string_value(row.get("环境ID", ""))
                or string_value(row.get("id", ""))
                or string_value(row.get("user_id", ""))
                or string_value(row.get("containerCode", ""))
            )
            if account_id:
                accounts.append(
                    AccountInfo(
                        id=account_id,
                        ua=string_value(row.get("环境名称", "")),
                    )
                )
    except Exception as exc:
        print(f"加载账号失败: {exc}\n路径: {chosen}", flush=True)
    return accounts


def log(account_id: str, message: str) -> None:
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    with _print_lock:
        print(f"[{timestamp}] [窗口 {account_id}] {message}", flush=True)
