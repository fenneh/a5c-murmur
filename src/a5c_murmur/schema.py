from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class MessageKind(StrEnum):
    INTRO = "intro"
    RESEARCH = "research"
    PROPOSE = "propose"
    CHALLENGE = "challenge"
    REVISE = "revise"
    AGREE = "agree"
    DECIDE = "decide"
    ABORT = "abort"
    SYSTEM = "system"
    USER = "user"


@dataclass
class Message:
    """One message on a debate channel.

    `action` carries the structured proposal when kind is propose / revise /
    agree. Two messages with the same `action_hash` count as the same proposal
    for consensus purposes."""

    msg_id: str
    task_id: str
    agent: str
    kind: MessageKind
    text: str = ""
    action: dict[str, Any] | None = None
    round: int = 0
    in_reply_to: str | None = None
    ts: float = field(default_factory=time.time)

    @classmethod
    def new(
        cls,
        *,
        task_id: str,
        agent: str,
        kind: MessageKind | str,
        text: str = "",
        action: dict[str, Any] | None = None,
        round: int = 0,
        in_reply_to: str | None = None,
    ) -> Message:
        return cls(
            msg_id=str(uuid.uuid4()),
            task_id=task_id,
            agent=agent,
            kind=MessageKind(kind),
            text=text,
            action=action,
            round=round,
            in_reply_to=in_reply_to,
        )

    @property
    def action_hash(self) -> str | None:
        return action_hash(self.action)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["kind"] = self.kind.value
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Message:
        d = dict(d)
        d["kind"] = MessageKind(d["kind"])
        return cls(**d)

    # Redis stream encoding. Streams only take string -> string maps.
    def to_redis_fields(self) -> dict[str, str]:
        return {
            "msg_id": self.msg_id,
            "task_id": self.task_id,
            "agent": self.agent,
            "kind": self.kind.value,
            "text": self.text,
            "action_json": json.dumps(self.action) if self.action else "",
            "round": str(self.round),
            "in_reply_to": self.in_reply_to or "",
            "ts": str(self.ts),
        }

    @classmethod
    def from_redis_fields(cls, fields: dict[str, str]) -> Message:
        action = json.loads(fields["action_json"]) if fields.get("action_json") else None
        return cls(
            msg_id=fields["msg_id"],
            task_id=fields["task_id"],
            agent=fields["agent"],
            kind=MessageKind(fields["kind"]),
            text=fields.get("text", ""),
            action=action,
            round=int(fields.get("round", "0")),
            in_reply_to=fields.get("in_reply_to") or None,
            ts=float(fields.get("ts", "0") or "0"),
        )


def action_hash(action: dict[str, Any] | None) -> str | None:
    """Stable hash over sorted keys so semantically-equal proposals collide.

    None / empty dict -> None (you can't agree on nothing)."""
    if not action:
        return None
    canonical = json.dumps(action, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


@dataclass
class Decision:
    """The outcome of a debate, written to the journal."""

    task_id: str
    status: str  # "agreed" | "no_action" | "timeout" | "aborted"
    action: dict[str, Any] | None
    action_hash: str | None
    signers: list[str]
    decided_at: float = field(default_factory=time.time)
    rationale: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
