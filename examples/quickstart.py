"""Two agents agreeing on a config change. No Redis needed.

A planner agent picks up a task from a stream and proposes a change.
A reviewer agent watches the debate and signs off. The decision lands
in a throwaway journal and gets printed.

Run:
    uv run python examples/quickstart.py
"""

from __future__ import annotations

import tempfile
import threading
import time
from pathlib import Path

from a5c_murmur import Agent, Debate, Message
from a5c_murmur.bus import InMemoryBus
from a5c_murmur.journal import Journal

TASK_ID = "cfg-rollout-7"


class Planner(Agent):
    role = "planner"
    streams = ["bus:tasks"]

    def handle_message(self, stream, msg_id, fields):
        debate = Debate(fields["task_id"], self.bus)
        action = {"file": "service.toml", "set": {"workers": 8}}
        debate.post(
            agent=self.role,
            kind="propose",
            text="queue depth keeps climbing, double the workers",
            action=action,
        )
        debate.post(agent=self.role, kind="agree", action=action)


class Reviewer(Agent):
    role = "reviewer"
    streams = [f"task:{TASK_ID}:debate"]

    def handle_message(self, stream, msg_id, fields):
        msg = Message.from_redis_fields(fields)
        if msg.kind == "propose":
            Debate(msg.task_id, self.bus).post(
                agent=self.role,
                kind="agree",
                text="metrics back it up, go ahead",
                action=msg.action,
            )


def main() -> None:
    bus = InMemoryBus()
    agents = [Planner(bus=bus, block_ms=200), Reviewer(bus=bus, block_ms=200)]
    threads = [threading.Thread(target=a.run, daemon=True) for a in agents]
    for t in threads:
        t.start()
    time.sleep(0.2)  # let both agents subscribe

    bus.publish("bus:tasks", {"task_id": TASK_ID, "instruction": "tune the worker pool"})

    debate = Debate(TASK_ID, bus)
    outcome = debate.wait_for_decision(quorum=2, timeout_s=10, poll_ms=100)

    with tempfile.TemporaryDirectory() as tmp:
        journal = Journal(path=Path(tmp) / "journal.db")
        journal.init()
        journal.open_task(TASK_ID, label="config change")
        for msg in outcome.messages:
            journal.record_message(msg)
        journal.record_decision(outcome.to_decision(TASK_ID, rationale="quorum met"))
        decision = journal.get_decision(TASK_ID)

    for a in agents:
        a.stop()
    for t in threads:
        t.join(timeout=2)

    print(f"status:  {outcome.status}")
    print(f"signers: {outcome.signers}")
    print(f"action:  {outcome.action}")
    print(f"journal: {decision}")


if __name__ == "__main__":
    main()
