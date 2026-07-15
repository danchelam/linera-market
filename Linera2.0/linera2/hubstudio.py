from __future__ import annotations

import time
from typing import Any

import requests


DEFAULT_API_BASE_URL = "http://127.0.0.1:6873"


class HubstudioReadOnlyClient:
    """Starts environments and supports an explicit one-shot cold restart."""

    def __init__(
        self,
        api_base_url: str = DEFAULT_API_BASE_URL,
        *,
        session: Any | None = None,
        timeout: tuple[int, int] = (10, 60),
    ) -> None:
        self.api_base_url = api_base_url.rstrip("/")
        self.session = session or requests.Session()
        self.timeout = timeout
        self.last_error = ""

    def start_browser(self, container_code: str) -> str | None:
        self.last_error = ""
        payload = {
            "containerCode": str(container_code),
            "args": ["--remote-allow-origins=*"],
        }
        try:
            response = self.session.post(
                f"{self.api_base_url}/api/v1/browser/start",
                json=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
            body = response.json()
        except Exception as exc:
            self.last_error = str(exc)
            return None

        if body.get("code") != 0:
            self.last_error = body.get("msg") or f"HubStudio code={body.get('code')}"
            return None
        port = (body.get("data") or {}).get("debuggingPort")
        if not port:
            self.last_error = "HubStudio 未返回 debuggingPort"
            return None
        return f"127.0.0.1:{port}"

    def restart_browser_once(self, container_code: str) -> str | None:
        """Stop one environment once, then start it and return its CDP address."""
        self.last_error = ""
        try:
            response = self.session.post(
                f"{self.api_base_url}/api/v1/browser/stop",
                json={"containerCode": str(container_code)},
                timeout=self.timeout,
            )
            response.raise_for_status()
            body = response.json()
        except Exception as exc:
            self.last_error = f"重启 stop 阶段失败：{exc}"
            return None

        if body.get("code") != 0:
            detail = body.get("msg") or f"HubStudio code={body.get('code')}"
            self.last_error = f"重启 stop 阶段失败：{detail}"
            return None

        time.sleep(3)
        address = self.start_browser(container_code)
        if not address:
            self.last_error = f"重启 start 阶段失败：{self.last_error or '未取得调试端口'}"
        return address
