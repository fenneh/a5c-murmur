from __future__ import annotations

import time
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from a5c_murmur.schema import Message

UI_DIR = Path(__file__).parent
TEMPLATES = Jinja2Templates(directory=str(UI_DIR / "templates"))


def mount_ui(app: FastAPI, *, get_bus, get_journal) -> None:
    app.mount(
        "/ui/static",
        StaticFiles(directory=str(UI_DIR / "static")),
        name="ui-static",
    )

    TEMPLATES.env.globals["fmt_ts"] = _fmt_ts
    TEMPLATES.env.globals["fmt_age"] = _fmt_age

    @app.get("/ui", response_class=HTMLResponse)
    def ui_index(request: Request):
        bus = get_bus()
        agents = _agents(bus)
        recent = _recent_debates(get_journal(), limit=10)
        return TEMPLATES.TemplateResponse(
            request,
            "index.html",
            {"agents": agents, "debates": recent},
        )

    @app.get("/ui/agents", response_class=HTMLResponse)
    def ui_agents(request: Request):
        return TEMPLATES.TemplateResponse(
            request,
            "agents.html",
            {"agents": _agents(get_bus())},
        )

    @app.get("/ui/debates", response_class=HTMLResponse)
    def ui_debates(request: Request):
        return TEMPLATES.TemplateResponse(
            request,
            "debates.html",
            {"debates": _recent_debates(get_journal(), limit=100)},
        )

    @app.get("/ui/debates/{task_id}", response_class=HTMLResponse)
    def ui_debate(request: Request, task_id: str):
        j = get_journal()
        bus = get_bus()
        journal_messages = j.messages_for(task_id)
        if journal_messages:
            messages = journal_messages
        else:
            # Fall back to live stream if the journal hasn't captured it yet.
            raw = bus.history(f"task:{task_id}:debate")
            messages = [Message.from_redis_fields(fields).to_dict() for _, fields in raw]
        if not messages:
            raise HTTPException(status_code=404)
        decision = j.get_decision(task_id)
        return TEMPLATES.TemplateResponse(
            request,
            "debate.html",
            {"task_id": task_id, "messages": messages, "decision": decision},
        )


def _agents(bus) -> list[dict]:
    keys = bus.keys("agent:*:status")
    out = []
    for key in sorted(keys):
        parts = key.split(":")
        if len(parts) < 3:
            continue
        role = parts[1]
        status = bus.hget_all(key)
        last_seen = float(status.get("last_seen", "0") or "0")
        out.append(
            {
                "role": role,
                "status": status.get("status", "unknown"),
                "pid": status.get("pid"),
                "last_seen": last_seen,
                "active_tasks": int(status.get("active_tasks", "0") or "0"),
                "last_error": status.get("last_error"),
                "stale": last_seen and (time.time() - last_seen) > 120,
            }
        )
    return out


def _recent_debates(journal, *, limit: int) -> list[dict]:
    tasks = journal.list_tasks(limit=limit)
    out = []
    for t in tasks:
        decision = journal.get_decision(t["task_id"])
        out.append({**t, "decision": decision})
    return out


def _fmt_ts(ts: float | None) -> str:
    if not ts:
        return "—"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def _fmt_age(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds / 60)}m ago"
    return f"{int(seconds / 3600)}h ago"
