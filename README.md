# a5c-murmur

A communication bus for AI agents, with a structured debate primitive that produces an agreement on a specific action.

Redis Streams under the hood by default. Pluggable for other transports (in-memory for tests, NATS, Kafka, whatever you want to write an adapter for).

## 60-second quickstart

No Redis needed, the demo runs on the in-memory bus.

```bash
git clone https://github.com/fenneh/a5c-murmur && cd a5c-murmur
uv sync
uv run python examples/quickstart.py
```

Two agents: a planner proposes a config change, a reviewer signs off, the decision is written to a throwaway journal. You should see:

```
status:  agreed
signers: ['planner', 'reviewer']
action:  {'file': 'service.toml', 'set': {'workers': 8}}
journal: {'task_id': 'cfg-rollout-7', 'status': 'agreed', ...}
```

The library itself:

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

To add your own (NATS, Kafka, RabbitMQ), implement the methods in `BusAdapter`. Pass `bus=` to `Agent.__init__` or set `A5C_MURMUR_BUS=memory|redis` to pick at runtime.

Need an operation the protocol doesn't model? `bus.raw_client()` returns the underlying client (redis-py for `RedisBus`). Using it couples your code to that adapter; `InMemoryBus` raises `NotImplementedError`.

## Durable delivery

By default an agent subscribes from the stream tail: messages published while it's down are gone. Opt in to at-least-once delivery with `durable=True`:

```python
Reviewer(durable=True).run()
```

This consumes through a consumer group (group = the agent's `role`, consumer = `{role}-{pid}`) and acks each message only after `handle_message` returns without raising. What you get:

- messages published while no consumer is running are delivered when one starts
- a handler exception leaves the message pending; it gets redelivered (after `claim_min_idle_ms`, default 60s) to this or another consumer of the same role, including across restarts
- multiple processes with the same `role` share the group, so each message goes to one of them

At-least-once means exactly that: a crash between handling and acking causes a redelivery. Durable handlers must be idempotent.

The group is created on first use, starting at the stream tail. Messages published before any agent of that role has ever run are not delivered.

## Budget and kill-switch

Agents can carry a daily spend cap:

```python
class Researcher(Agent):
    role = "researcher"
    streams = ["bus:jobs"]

    def handle_message(self, stream, msg_id, fields):
        result = call_llm(fields)
        self.track_spend(result.cost)  # raises BudgetExceeded past the cap

Researcher(daily_budget=5.0, kill_switch_path="/tmp/murmur.kill").run()
```

The running total lives in a shared hash (`agent:{role}:spend`, keyed by date) and is incremented atomically, so multiple processes running the same role share one budget. It resets at midnight local time. On overrun the agent writes the kill-switch file and raises `BudgetExceeded`; every agent configured with the same `kill_switch_path` pauses its consume loop until the file is removed.

## Retention

Streams and the journal grow until you bound them:

- `bus.publish(stream, fields, maxlen=10_000)` trims on write (approximate `XADD MAXLEN` on Redis, cheap)
- `Debate.open(task_id, maxlen=1_000)` caps a debate stream the same way
- `bus.trim(stream, maxlen=...)` trims explicitly
- `journal.prune(before=ts)` deletes tasks created before a unix timestamp, plus their messages, decisions, and tool calls

Nothing prunes automatically. Run these from your own scheduler.

## Inspection UI

```bash
uv run python -m a5c_murmur.server
# api at  http://localhost:8001/api/
# ui  at  http://localhost:8001/ui
```

Live agent presence, recent debates, full transcripts with the agreed action highlighted.

## Examples

In [`examples/`](examples/):

- `quickstart.py`: two agents, a proposal, an agreement, a journaled decision. No setup.
- `chatbot_demo.py`: in-memory bus, two agents talking, no setup.
- `redis_demo.py`: real Redis, two daemons publishing and consuming.
- `debate_demo.py`: full propose / challenge / revise / agree cycle ending in a decision.
