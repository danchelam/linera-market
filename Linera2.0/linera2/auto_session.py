from __future__ import annotations

import json
import os
import random
import tempfile
import threading
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path


class AutoSessionState(str, Enum):
    WAITING = "waiting"
    CONFIGURING = "configuring"
    RUNNING = "running"
    STOPPING = "stopping"
    SETTLING = "settling"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class AutoSessionRecord:
    account_id: str
    utc_date: str
    state: str
    target_rounds: int
    completed_rounds: int = 0
    start_coins: int | None = None
    current_coins: int | None = None
    end_coins: int | None = None
    nominal_stake: int = 0
    baseline_resolution_keys: list[int] = field(default_factory=list)
    counted_resolution_keys: list[int] = field(default_factory=list)
    baseline_higher_rows: int = 0
    baseline_lower_rows: int = 0
    started_at: str | None = None
    ended_at: str | None = None
    failure_reason: str | None = None
    auto_still_running: bool = False

    @property
    def net_change(self) -> int | None:
        latest = self.end_coins if self.end_coins is not None else self.current_coins
        if self.start_coins is None or latest is None:
            return None
        return latest - self.start_coins

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["net_change"] = self.net_change
        return payload

    @classmethod
    def from_dict(cls, payload: dict) -> "AutoSessionRecord":
        allowed = {item.name for item in fields(cls)}
        return cls(**{key: value for key, value in payload.items() if key in allowed})


class AutoSessionStore:
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

    def get(self, account_id: str) -> AutoSessionRecord | None:
        with self._lock:
            payload = self._accounts.get(str(account_id))
            return AutoSessionRecord.from_dict(dict(payload)) if payload else None

    def get_or_create_daily(
        self,
        account_id: str,
        *,
        now: datetime | None = None,
        rng: random.Random | None = None,
        target_override: int | None = None,
    ) -> AutoSessionRecord:
        if target_override is not None and target_override != 1:
            raise ValueError("integration target override must be 1")
        current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        utc_date = current.date().isoformat()
        account_id = str(account_id)
        with self._lock:
            existing = self.get(account_id)
            if existing is not None and existing.utc_date == utc_date:
                return existing
            generator = rng or random.SystemRandom()
            target = target_override if target_override is not None else generator.randint(4, 7)
            record = AutoSessionRecord(
                account_id=account_id,
                utc_date=utc_date,
                state=AutoSessionState.WAITING.value,
                target_rounds=target,
            )
            self._accounts[account_id] = record.to_dict()
            self._write_locked()
            return AutoSessionRecord.from_dict(record.to_dict())

    def update(self, record: AutoSessionRecord) -> None:
        with self._lock:
            self._accounts[record.account_id] = record.to_dict()
            self._write_locked()

    def as_dict(self) -> dict[str, dict]:
        with self._lock:
            return {key: dict(value) for key, value in self._accounts.items()}

    def _write_locked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "accounts": self._accounts,
        }
        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
            os.replace(temporary_name, self.path)
        finally:
            if os.path.exists(temporary_name):
                os.remove(temporary_name)
