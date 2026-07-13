from __future__ import annotations

import json
import re
from dataclasses import dataclass

from playwright.async_api import Request


WORKER_APPLICATION_MARKER = "worker.infra.linera.net/chains/"
_RESOLUTIONS_START = re.compile(r"\bresolutions\s*(?:\([^)]*\)\s*)?\{")
_ENTRY_KEY = re.compile(r"\bentry\s*\(\s*key\s*:\s*(\d+)\s*\)")


def _resolution_blocks(query: str):
    for match in _RESOLUTIONS_START.finditer(query):
        opening = query.find("{", match.start())
        depth = 0
        for index in range(opening, len(query)):
            char = query[index]
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    yield query[opening + 1:index]
                    break


def _queries(payload: str | dict | list | None) -> list[str]:
    if payload is None:
        return []
    if isinstance(payload, str):
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError:
            return [payload] if "resolutions" in payload else []
        return _queries(decoded)
    if isinstance(payload, dict):
        query = payload.get("query")
        return [query] if isinstance(query, str) else []
    if isinstance(payload, list):
        result: list[str] = []
        for item in payload:
            result.extend(_queries(item))
        return result
    return []


def extract_resolution_keys(payload: str | dict | list | None) -> set[int]:
    keys: set[int] = set()
    for query in _queries(payload):
        for block in _resolution_blocks(query):
            keys.update(int(match.group(1)) for match in _ENTRY_KEY.finditer(block))
    return keys


class ResolutionKeyMonitor:
    def __init__(self) -> None:
        self.keys: set[int] = set()

    def on_request(self, request: Request) -> None:
        url = request.url or ""
        if WORKER_APPLICATION_MARKER not in url or "/applications/" not in url:
            return
        try:
            payload = request.post_data
        except Exception:
            return
        self.keys.update(extract_resolution_keys(payload))

    def snapshot(self) -> set[int]:
        return set(self.keys)


@dataclass(frozen=True)
class HistoryCounts:
    higher: int
    lower: int
    active_higher: int = 0
    active_lower: int = 0


class RoundTracker:
    def __init__(
        self,
        baseline_keys: set[int],
        baseline_history: HistoryCounts,
        already_counted: set[int] | None = None,
    ) -> None:
        self.baseline_keys = set(baseline_keys)
        self.baseline_history = baseline_history
        self.baseline_max = max(self.baseline_keys, default=-1)
        self.counted = {
            key for key in (already_counted or set()) if key > self.baseline_max
        }
        self.observed = set(self.baseline_keys) | set(self.counted)
        self.pair_seen = False
        self.pending_key: int | None = None

    def observe(self, keys: set[int], history: HistoryCounts) -> list[int]:
        if history.active_higher > 0 and history.active_lower > 0:
            self.pair_seen = True
        unseen = sorted(
            key
            for key in keys
            if key > self.baseline_max and key not in self.observed
        )
        self.observed.update(unseen)
        if unseen:
            self.pending_key = unseen[-1]
        if not self.pair_seen or self.pending_key is None:
            return []
        key = self.pending_key
        self.pending_key = None
        self.pair_seen = False
        if key in self.counted:
            return []
        self.counted.add(key)
        return [key]
