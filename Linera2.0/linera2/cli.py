from __future__ import annotations

import argparse
import asyncio
import sys
import threading
from pathlib import Path

from .auto_session import AutoSessionStore
from .runtime import scan_accounts
from .store import ReadinessStore
from .webapp import create_app


PROJECT_DIR = Path(__file__).resolve().parents[1]
PARENT_DIR = PROJECT_DIR.parent
STATUS_FILE = PROJECT_DIR / "readiness_status.json"
AUTO_STATUS_FILE = PROJECT_DIR / "auto_sessions.json"


class LineraArgumentParser(argparse.ArgumentParser):
    def parse_args(self, args=None, namespace=None):
        parsed = super().parse_args(args, namespace)
        if parsed.integration_target is not None and not parsed.auto_session:
            self.error("--integration-target requires --auto-session")
        return parsed


def default_account_file() -> Path:
    return PARENT_DIR / "hubshuju.xlsx"


def build_parser() -> argparse.ArgumentParser:
    parser = LineraArgumentParser(description="Linera 2.0 账号就绪状态检测")
    parser.add_argument("--accounts", type=Path, default=default_account_file())
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--web", action="store_true", help="检测同时启动 Web 状态页")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5060)
    parser.add_argument(
        "--auto-session",
        action="store_true",
        help="就绪后执行每日测试网 Auto 会话",
    )
    parser.add_argument("--auto-timeout", type=int, default=1_200)
    parser.add_argument(
        "--integration-target",
        type=int,
        choices=[1],
        help="仅用于人工集成验证：固定目标 1 轮",
    )
    return parser


def _load_parent_base_module():
    parent = str(PARENT_DIR)
    if parent not in sys.path:
        sys.path.insert(0, parent)
    import base_module

    return base_module


def load_accounts(path: Path):
    base_module = _load_parent_base_module()
    return base_module.load_accounts(str(path))


def _parent_log(account_id: str, message: str) -> None:
    base_module = _load_parent_base_module()
    base_module.log(account_id, message)


async def run_scan(
    args,
    store: ReadinessStore,
    auto_store: AutoSessionStore | None = None,
):
    accounts = load_accounts(args.accounts)
    if not accounts:
        print(f"未读取到账号：{args.accounts}", flush=True)
        return []
    return await scan_accounts(
        accounts,
        max_workers=max(1, args.workers),
        store=store,
        log_func=_parent_log,
        timeout=max(1, args.timeout),
        auto_session_store=auto_store if args.auto_session else None,
        run_auto=args.auto_session,
        auto_timeout=max(1, args.auto_timeout),
        target_override=args.integration_target,
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    store = ReadinessStore(STATUS_FILE)
    auto_store = AutoSessionStore(AUTO_STATUS_FILE)

    if args.web:
        thread = threading.Thread(
            target=lambda: asyncio.run(run_scan(args, store, auto_store)),
            name="linera2-readiness-scan",
            daemon=True,
        )
        thread.start()
        app = create_app(store, auto_store, auto_enabled=args.auto_session)
        print(f"Web 状态页：http://{args.host}:{args.port}", flush=True)
        app.run(host=args.host, port=args.port, debug=False, use_reloader=False)
        return 0

    results = asyncio.run(run_scan(args, store, auto_store))
    return 0 if results else 1
