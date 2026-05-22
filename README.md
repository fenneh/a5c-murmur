# a5c-murmur

A communication bus for AI agents, with a structured debate primitive that produces an agreement on a specific action.

Redis Streams under the hood by default. Pluggable for other transports (in-memory for tests, NATS, Kafka, whatever you want to write an adapter for).

```python
from a5c_murmur import Bus, Agent

bus = Bus.open()  # default: Redis on REDIS_URL

# Plain pub/sub.
bus.publish("jobs", {"task_id": "abc", "instruction": "review PR #42"})

# Or a persistent agent that consumes streams.
class Reviewer(Agent):
    role = "reviewer"
    streams = ["bus:jobs"]
    def handle_message(self, stream, msg_id, fields):
        print("got", fields)

Reviewer().run()  # blocks, with heartbeat and graceful SIGTERM
```

## Install

```bash
uv add a5c-murmur
```

Base install needs `redis-py` and a Redis server on `REDIS_URL` for production. Tests and single-script demos use the in-memory adapter and need nothing external.

## What a debate is

A debate is a structured discussion between named agents that ends in agreement on a specific action. The point is not "vote yes or no". It's: agents put proposals on the table, challenge each other, revise, and when enough of them sign off on the same proposal you have a decision.

```python
from a5c_murmur import Debate

debate = Debate.open(
    task_id="pr-42",
    roster=["reviewer", "tester", "lead"],
)

# Each agent posts as it goes. propose / challenge / revise / agree / abort.
debate.post(
    agent="reviewer",
    kind="propose",
    action={"verdict": "merge", "after": "rebase"},
)
debate.post(
    agent="tester",
    kind="challenge",
    text="the new test is flaky on macOS",
)
debate.post(
    agent="reviewer",
    kind="revise",
    action={"verdict": "merge", "after": "rebase + fix flaky test"},
)
debate.post(
    agent="tester",
    kind="agree",
    action={"verdict": "merge", "after": "rebase + fix flaky test"},
)
debate.post(
    agent="lead",
    kind="agree",
    action={"verdict": "merge", "after": "rebase + fix flaky test"},
)

# Orchestrator side.
outcome = debate.wait_for_decision(quorum=2, timeout_s=180)
# outcome.status   in {"agreed", "no_action", "timeout", "aborted"}
# outcome.action   the agreed action, or None
# outcome.signers  list of agents who agreed
```

Murmur doesn't know what your action means. It hashes the payload (sorted keys, stable JSON), counts distinct agents that signed off on the same hash, and returns when quorum is met. So the discussion has shape, the agreement format is yours.

Message kinds: `intro`, `research`, `propose`, `challenge`, `revise`, `agree`, `decide`, `abort`, `system`, `user`.

## Transports

`BusAdapter` is a small protocol. Adapters in the box:

| Adapter | When | Notes |
|---|---|---|
| `RedisBus` (default) | production, multi-process, survives restarts | needs Redis on `REDIS_URL` |
| `InMemoryBus` | tests, single-script demos | no external services |

To add your own (NATS, Kafka, RabbitMQ), implement the seven methods in `BusAdapter`. Pass `bus=` to `Agent.__init__` or set `A5C_MURMUR_BUS=memory|redis` to pick at runtime.

## Inspection UI

```bash
uv run python -m a5c_murmur.server
# api at  http://localhost:8001/api/
# ui  at  http://localhost:8001/ui
```

Live agent presence, recent debates, full transcripts with the agreed action highlighted.

## Examples

In [`examples/`](examples/):

- `chatbot_demo.py`: in-memory bus, two agents talking, no setup.
- `redis_demo.py`: real Redis, two daemons publishing and consuming.
- `debate_demo.py`: full propose / challenge / revise / agree cycle ending in a decision.
