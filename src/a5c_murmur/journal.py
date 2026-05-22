"""SQLite audit log for tasks, messages, decisions, and tool calls.

Anything that crosses the bus and matters is replayable from here."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from a5c_murmur.schema import Decision, Message

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    label TEXT,
    created_at REAL NOT NULL,
    closed_at REAL,
    extra TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS messages (
    msg_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    agent TEXT NOT NULL,
    kind TEXT NOT NULL,
    text TEXT NOT NULL DEFAULT '',
    action_json TEXT,
    action_hash TEXT,
    round INTEGER NOT NULL DEFAULT 0,
    in_reply_to TEXT,
    ts REAL NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);
CREATE INDEX IF NOT EXISTS idx_messages_task ON messages(task_id, ts);
CREATE INDEX IF NOT EXISTS idx_messages_agent ON messages(agent, ts);
CREATE INDEX IF NOT EXISTS idx_messages_action_hash ON messages(task_id, action_hash);

CREATE TABLE IF NOT EXISTS decisions (
    task_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    action_json TEXT,
    action_hash TEXT,
    signers_json TEXT NOT NULL,
    rationale TEXT NOT NULL DEFAULT '',
    decided_at REAL NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);

CREATE TABLE IF NOT EXISTS tool_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    agent TEXT NOT NULL,
    tool TEXT NOT NULL,
    args_json TEXT NOT NULL,
    result_preview TEXT,
    round INTEGER NOT NULL DEFAULT 0,
    ts REAL NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);
CREATE INDEX IF NOT EXISTS idx_tool_calls_task ON tool_calls(task_id, ts);
"""


class Journal:
    def __init__(self, path: str | Path = "a5c_murmur.db"):
        self.path = Path(path)
        self._conn: sqlite3.Connection | None = None

    def _connect(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.executescript(SCHEMA)
        self._conn = conn
        return conn

    def init(self) -> None:
        self._connect()

    # ---- tasks ----------------------------------------------------
    def open_task(
        self, task_id: str, *, label: str | None = None, extra: dict[str, Any] | None = None
    ) -> None:
        import time as _t

        conn = self._connect()
        conn.execute(
            "INSERT OR IGNORE INTO tasks(task_id, label, created_at, extra) VALUES (?, ?, ?, ?)",
            (task_id, label, _t.time(), json.dumps(extra or {})),
        )
        conn.commit()

    def close_task(self, task_id: str) -> None:
        import time as _t

        conn = self._connect()
        conn.execute("UPDATE tasks SET closed_at = ? WHERE task_id = ?", (_t.time(), task_id))
        conn.commit()

    def list_tasks(self, *, limit: int = 50) -> list[dict[str, Any]]:
        conn = self._connect()
        rows = conn.execute(
            "SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ---- messages -------------------------------------------------
    def record_message(self, msg: Message) -> None:
        conn = self._connect()
        conn.execute(
            """
            INSERT OR REPLACE INTO messages
            (msg_id, task_id, agent, kind, text, action_json, action_hash,
             round, in_reply_to, ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                msg.msg_id,
                msg.task_id,
                msg.agent,
                msg.kind.value,
                msg.text,
                json.dumps(msg.action) if msg.action else None,
                msg.action_hash,
                msg.round,
                msg.in_reply_to,
                msg.ts,
            ),
        )
        conn.commit()

    def messages_for(self, task_id: str) -> list[dict[str, Any]]:
        conn = self._connect()
        rows = conn.execute(
            "SELECT * FROM messages WHERE task_id = ? ORDER BY ts ASC", (task_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ---- decisions ------------------------------------------------
    def record_decision(self, dec: Decision) -> None:
        conn = self._connect()
        conn.execute(
            """
            INSERT OR REPLACE INTO decisions
            (task_id, status, action_json, action_hash, signers_json,
             rationale, decided_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dec.task_id,
                dec.status,
                json.dumps(dec.action) if dec.action else None,
                dec.action_hash,
                json.dumps(dec.signers),
                dec.rationale,
                dec.decided_at,
            ),
        )
        conn.commit()

    def get_decision(self, task_id: str) -> dict[str, Any] | None:
        conn = self._connect()
        row = conn.execute("SELECT * FROM decisions WHERE task_id = ?", (task_id,)).fetchone()
        return dict(row) if row else None

    # ---- tool calls -----------------------------------------------
    def record_tool_call(
        self,
        *,
        task_id: str,
        agent: str,
        tool: str,
        args: dict[str, Any],
        result_preview: str = "",
        round: int = 0,
    ) -> None:
        import time as _t

        conn = self._connect()
        conn.execute(
            "INSERT INTO tool_calls(task_id, agent, tool, args_json, result_preview, round, ts) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (task_id, agent, tool, json.dumps(args), result_preview, round, _t.time()),
        )
        conn.commit()

    def tool_calls_for(self, task_id: str) -> list[dict[str, Any]]:
        conn = self._connect()
        rows = conn.execute(
            "SELECT * FROM tool_calls WHERE task_id = ? ORDER BY ts ASC", (task_id,)
        ).fetchall()
        return [dict(r) for r in rows]
