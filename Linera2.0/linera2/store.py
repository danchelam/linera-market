from __future__ import annotations

import json
import os
import tempfile
import threading
from datetime import datetime
from pathlib import Path

from .readiness import ReadinessResult


class ReadinessStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()
        self._accounts: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            accounts = payload.get("accounts", {})
            if isinstance(accounts, dict):
                self._accounts = accounts
        except (OSError, json.JSONDecodeError):
            self._accounts = {}

    def update(self, result: ReadinessResult) -> None:
        with self._lock:
            self._accounts[result.account_id] = result.to_dict()
            self._write_locked()

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                "accounts": dict(self._accounts),
            }

    def as_list(self) -> list[dict]:
        with self._lock:
            return [dict(value) for _, value in sorted(self._accounts.items())]

    def _write_locked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = self.snapshot()
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{self.path.name}.",
            suffix=".tmp",
            dir=self.path.parent,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
            os.replace(temporary_name, self.path)
        finally:
            if os.path.exists(temporary_name):
                os.remove(temporary_name)
