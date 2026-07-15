from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path


def load_local_config(project_dir: Path) -> dict[str, str]:
    path = project_dir / "local_config.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in data.items()
    ):
        return {}
    return data


def get_wallet_password(project_dir: Path) -> str | None:
    value = os.environ.get("OKX_WALLET_PASSWORD", "").strip()
    if value:
        return value
    stored = load_local_config(project_dir).get("wallet_password", "").strip()
    return stored or None


def _atomic_write_json(target: Path, data: dict[str, str]) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            delete=False,
            dir=target.parent,
        ) as temporary:
            json.dump(data, temporary, ensure_ascii=False)
            temporary_path = Path(temporary.name)
        os.replace(temporary_path, target)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def migrate_legacy_wallet_password(project_dir: Path, legacy_path: Path) -> bool:
    if get_wallet_password(project_dir):
        return True
    if not legacy_path.is_file():
        return False
    match = re.search(
        r'^OKX_DEFAULT_PASSWORD\s*=\s*["\']([^"\']+)["\']',
        legacy_path.read_text(encoding="utf-8"),
        re.M,
    )
    if not match:
        return False
    _atomic_write_json(
        project_dir / "local_config.json",
        {"wallet_password": match.group(1)},
    )
    return True
