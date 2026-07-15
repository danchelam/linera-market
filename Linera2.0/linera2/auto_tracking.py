from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass

from playwright.async_api import Request, Response


WORKER_APPLICATION_MARKER = "worker.infra.linera.net/chains/"
_RESOLUTIONS_START = re.compile(r"\bresolutions\s*(?:\([^)]*\)\s*)?\{")
_ENTRY_KEY = re.compile(r"\bentry\s*\(\s*key\s*:\s*(\d+)\s*\)")
_ALIASED_ENTRY_KEY = re.compile(
    r"\b([_A-Za-z][_0-9A-Za-z]*)\s*:\s*entry\s*\(\s*key\s*:\s*(\d+)\s*\)"
)


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


def _payload_items(payload: str | dict | list | None) -> list[dict]:
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return []
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    return []


def _resolution_aliases(query: str) -> dict[str, int]:
    aliases: dict[str, int] = {}
    for block in _resolution_blocks(query):
        aliases.update(
            (match.group(1), int(match.group(2)))
            for match in _ALIASED_ENTRY_KEY.finditer(block)
        )
    return aliases


def extract_resolved_resolution_keys(
    request_payload: str | dict | list | None,
    response_payload: str | dict | list | None,
) -> set[int]:
    request_items = _payload_items(request_payload)
    response_items = _payload_items(response_payload)
    resolved: set[int] = set()
    for request_item, response_item in zip(request_items, response_items):
        query = request_item.get("query")
        data = response_item.get("data")
        if not isinstance(query, str) or not isinstance(data, dict):
            continue
        resolutions = data.get("resolutions")
        if not isinstance(resolutions, dict):
            continue
        for alias, key in _resolution_aliases(query).items():
            entry = resolutions.get(alias)
            if isinstance(entry, dict) and entry.get("value") is not None:
                resolved.add(key)
    return resolved


class ResolutionKeyMonitor:
    def __init__(self) -> None:
        self.keys: set[int] = set()
        self._tasks: set[asyncio.Task] = set()

    def on_request(self, request: Request) -> None:
        url = request.url or ""
        if WORKER_APPLICATION_MARKER not in url or "/applications/" not in url:
            return
        try:
            payload = request.post_data
        except Exception:
            return
        self.keys.update(extract_resolution_keys(payload))

    def on_response(self, response: Response) -> None:
        url = response.url or ""
        if WORKER_APPLICATION_MARKER not in url or "/applications/" not in url:
            return
        task = asyncio.create_task(self._inspect_response(response))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _inspect_response(self, response: Response) -> None:
        if response.status != 200:
            return
        try:
            request_payload = response.request.post_data
            response_payload = await response.json()
        except Exception:
            return
        self.keys.update(
            extract_resolved_resolution_keys(request_payload, response_payload)
        )

    async def drain(self) -> None:
        if self._tasks:
            await asyncio.gather(*tuple(self._tasks), return_exceptions=True)

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

    def observe(self, keys: set[int], history: HistoryCounts) -> list[int]:
        if history.active_higher > 0 and history.active_lower > 0:
            self.pair_seen = True
        unseen = sorted(
            key
            for key in keys
            if key > self.baseline_max and key not in self.observed
        )
        self.observed.update(unseen)
        if not self.pair_seen or not unseen:
            return []
        key = unseen[-1]
        self.pair_seen = False
        if key in self.counted:
            return []
        self.counted.add(key)
        return [key]
