"""Debate primitive.

A Debate is a per-task stream where named agents post messages with a kind
(propose / challenge / revise / agree / decide / abort / system / user).
Consensus is detected when `quorum` distinct agents have posted an `agree`
message whose action payload hashes to the same value."""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from a5c_murmur.bus import Bus, BusAdapter
from a5c_murmur.schema import Decision, Message, MessageKind, action_hash


@dataclass
class DebateOutcome:
    status: str  # "agreed" | "no_action" | "timeout" | "aborted"
    action: dict[str, Any] | None
    action_hash: str | None
    signers: list[str]
    messages: list[Message]
    decided_at: float = field(default_factory=time.time)

    def to_decision(self, task_id: str, rationale: str = "") -> Decision:
        return Decision(
            task_id=task_id,
            status=self.status,
            action=self.action,
            action_hash=self.action_hash,
            signers=self.signers,
            rationale=rationale,
            decided_at=self.decided_at,
        )


class Debate:
    """A single task's discussion. The stream key is `task:{task_id}:debate`."""

    def __init__(self, task_id: str, channel: BusAdapter, roster: list[str] | None = None):
        self.task_id = task_id
        self.bus = channel
        self.roster = list(roster) if roster else []
        self.stream = f"task:{task_id}:debate"

    @classmethod
    def open(
        cls,
        task_id: str,
        *,
        channel: BusAdapter | None = None,
        roster: list[str] | None = None,
    ) -> Debate:
        return cls(task_id, channel or Bus.open(), roster)

    # ------------------------------------------------------------------
    def post(
        self,
        *,
        agent: str,
        kind: MessageKind | str,
        text: str = "",
        action: dict[str, Any] | None = None,
        round: int = 0,
        in_reply_to: str | None = None,
    ) -> Message:
        msg = Message.new(
            task_id=self.task_id,
            agent=agent,
            kind=kind,
            text=text,
            action=action,
            round=round,
            in_reply_to=in_reply_to,
        )
        self.bus.publish(self.stream, msg.to_redis_fields())
        return msg

    def history(self) -> list[Message]:
        return [Message.from_redis_fields(fields) for _, fields in self.bus.history(self.stream)]

    # ------------------------------------------------------------------
    def wait_for_decision(
        self,
        *,
        quorum: int = 2,
        timeout_s: float = 180.0,
        poll_ms: int = 500,
    ) -> DebateOutcome:
        """Block until either:
        - `quorum` distinct agents have posted an `agree` for the same action
        - any agent posts an `abort`
        - `timeout_s` elapses
        """
        deadline = time.time() + timeout_s
        seen_msg_ids: set[str] = set()
        agree_signers: dict[str, set[str]] = defaultdict(set)
        agree_actions: dict[str, dict[str, Any]] = {}
        all_messages: list[Message] = []

        while time.time() < deadline:
            new_msgs = [m for m in self.history() if m.msg_id not in seen_msg_ids]
            for msg in new_msgs:
                seen_msg_ids.add(msg.msg_id)
                all_messages.append(msg)

                if msg.kind == MessageKind.ABORT:
                    return DebateOutcome(
                        status="aborted",
                        action=None,
                        action_hash=None,
                        signers=[msg.agent],
                        messages=all_messages,
                    )

                if msg.kind == MessageKind.AGREE and msg.action_hash:
                    agree_signers[msg.action_hash].add(msg.agent)
                    agree_actions[msg.action_hash] = msg.action
                    if len(agree_signers[msg.action_hash]) >= quorum:
                        return DebateOutcome(
                            status="agreed",
                            action=agree_actions[msg.action_hash],
                            action_hash=msg.action_hash,
                            signers=sorted(agree_signers[msg.action_hash]),
                            messages=all_messages,
                        )

            time.sleep(poll_ms / 1000.0)

        return DebateOutcome(
            status="timeout",
            action=None,
            action_hash=None,
            signers=[],
            messages=all_messages,
        )

    # ------------------------------------------------------------------
    @staticmethod
    def tally_agreements(messages: list[Message]) -> dict[str, set[str]]:
        """Helper for tests / inspectors: who agreed on what."""
        out: dict[str, set[str]] = defaultdict(set)
        for m in messages:
            if m.kind == MessageKind.AGREE:
                h = m.action_hash
                if h:
                    out[h].add(m.agent)
        return out


# Re-export for ergonomic imports.
__all__ = ["Debate", "DebateOutcome", "action_hash"]
