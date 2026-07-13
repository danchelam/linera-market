from __future__ import annotations

from pathlib import Path

from flask import Flask, jsonify, render_template

from .auto_session import AutoSessionStore
from .store import ReadinessStore


SESSION_FIELDS = {
    "state": "session_state",
    "target_rounds": "target_rounds",
    "completed_rounds": "completed_rounds",
    "start_coins": "start_coins",
    "current_coins": "current_coins",
    "end_coins": "end_coins",
    "nominal_stake": "nominal_stake",
    "net_change": "net_change",
    "auto_still_running": "auto_still_running",
    "failure_reason": "session_failure_reason",
}


def create_app(
    store: ReadinessStore,
    auto_store: AutoSessionStore | None = None,
    *,
    auto_enabled: bool = False,
) -> Flask:
    template_folder = Path(__file__).resolve().parents[1] / "templates"
    app = Flask(__name__, template_folder=str(template_folder))

    @app.get("/")
    def index():
        return render_template("index.html", auto_enabled=auto_enabled)

    @app.get("/api/readiness")
    def readiness():
        sessions = auto_store.as_dict() if auto_store is not None else {}
        result = []
        for source in store.as_list():
            item = dict(source)
            session = sessions.get(str(item.get("account_id")), {})
            for source_name, public_name in SESSION_FIELDS.items():
                item[public_name] = session.get(source_name)
            result.append(item)
        return jsonify(result)

    return app
