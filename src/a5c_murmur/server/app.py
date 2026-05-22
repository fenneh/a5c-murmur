from __future__ import annotations

import os
import time

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse

from a5c_murmur.bus import Bus, BusAdapter
from a5c_murmur.journal import Journal
from a5c_murmur.schema import Message
from a5c_murmur.ui.routes import mount_ui


def _default_db_path() -> str:
    p = os.environ.get("A5C_MURMUR_DB")
    if p:
        return p
    from pathlib import Path

    base = Path.home() / ".a5c-murmur"
    base.mkdir(exist_ok=True)
    return str(base / "murmur.db")


_bus: BusAdapter | None = None
_journal: Journal | None = None


def get_bus() -> BusAdapter:
    global _bus
    if _bus is None:
        _bus = Bus.open()
    return _bus


def get_journal() -> Journal:
    global _journal
    if _journal is None:
        _journal = Journal(_default_db_path())
        _journal.init()
    return _journal


app = FastAPI(title="a5c-murmur", version="0.1.0")


@app.get("/")
def root() -> RedirectResponse:
    return RedirectResponse(url="/ui")


@app.get("/api/agents")
def api_agents():
    bus = get_bus()
    status_keys = bus.keys("agent:*:status")
    out = []
    for key in sorted(status_keys):
        parts = key.split(":")
        if len(parts) < 3:
            continue
        role = parts[1]
        status = bus.hget_all(key)
        last_seen = float(status.get("last_seen", "0") or "0")
        age = time.time() - last_seen if last_seen else None
        out.append(
            {
                "role": role,
                "status": status.get("status", "unknown"),
                "pid": status.get("pid"),
                "last_seen": last_seen,
                "age_seconds": age,
                "active_tasks": int(status.get("active_tasks", "0") or "0"),
                "last_error": status.get("last_error"),
                "stale": age is not None and age > 120,
            }
        )
    return out


@app.get("/api/debates")
def api_debates(limit: int = 50):
    j = get_journal()
    tasks = j.list_tasks(limit=limit)
    out = []
    for t in tasks:
        decision = j.get_decision(t["task_id"])
        out.append(
            {
                "task_id": t["task_id"],
                "label": t["label"],
                "created_at": t["created_at"],
                "closed_at": t["closed_at"],
                "decision": decision,
            }
        )
    return out


@app.get("/api/debates/{task_id}")
def api_debate(task_id: str):
    j = get_journal()
    messages = j.messages_for(task_id)
    if not messages:
        raise HTTPException(status_code=404, detail="no messages for task")
    decision = j.get_decision(task_id)
    return {"task_id": task_id, "messages": messages, "decision": decision}


@app.get("/api/debates/{task_id}/live")
def api_debate_live(task_id: str):
    """Live view: read directly from the bus stream rather than the journal."""
    bus = get_bus()
    stream = f"task:{task_id}:debate"
    raw = bus.history(stream)
    messages = [Message.from_redis_fields(fields).to_dict() for _, fields in raw]
    return {"task_id": task_id, "messages": messages}


mount_ui(app, get_bus=get_bus, get_journal=get_journal)


def main() -> None:
    import uvicorn

    host = os.environ.get("A5C_MURMUR_HOST", "127.0.0.1")
    port = int(os.environ.get("A5C_MURMUR_PORT", "8001"))
    uvicorn.run(app, host=host, port=port)
