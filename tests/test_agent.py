import threading
import time

from a5c_murmur.agent import Agent


def test_agent_handles_messages(bus):
    received: list[dict] = []

    class Sink(Agent):
        role = "sink"
        streams = ["bus:test"]

        def handle_message(self, stream, msg_id, fields):
            received.append(fields)
            if len(received) >= 2:
                self.stop()

    agent = Sink(bus=bus, block_ms=200)
    t = threading.Thread(target=agent.run, daemon=True)
    t.start()
    # Default last_ids="$" means new-only, so we publish AFTER the agent has
    # subscribed (give it a tick to enter the loop).
    time.sleep(0.1)
    bus.publish("bus:test", {"i": "1"})
    bus.publish("bus:test", {"i": "2"})
    t.join(timeout=3)
    assert [r["i"] for r in received] == ["1", "2"]


def test_agent_heartbeat_writes_status(bus):
    class Idle(Agent):
        role = "idle"
        streams = ["bus:idle"]

        def handle_message(self, stream, msg_id, fields):
            pass

    agent = Idle(bus=bus, block_ms=100, heartbeat_interval_s=0.1)
    t = threading.Thread(target=agent.run, daemon=True)
    t.start()
    time.sleep(0.4)
    agent.stop()
    t.join(timeout=2)
    status = bus.hget_all("agent:idle:status")
    assert "last_seen" in status
    assert status.get("status") in {"running", "stopped"}


def test_agent_heartbeat_survives_bus_error(bus):
    state = {"failed": False}
    real_hset = bus.hset

    def flaky_hset(key, fields):
        # Fail the first heartbeat write (the one carrying active_tasks).
        if "active_tasks" in fields and not state["failed"]:
            state["failed"] = True
            raise ConnectionError("blip")
        real_hset(key, fields)

    bus.hset = flaky_hset

    class Idle(Agent):
        role = "hb"
        streams = ["bus:hb"]

        def handle_message(self, stream, msg_id, fields):
            pass

    agent = Idle(bus=bus, block_ms=50, heartbeat_interval_s=0.05)
    t = threading.Thread(target=agent.run, daemon=True)
    t.start()
    time.sleep(0.4)
    agent.stop()
    t.join(timeout=2)

    assert state["failed"]
    status = bus.hget_all("agent:hb:status")
    assert "active_tasks" in status  # a later heartbeat landed


def test_agent_survives_handler_exception(bus):
    received: list[str] = []

    class Flaky(Agent):
        role = "flaky"
        streams = ["bus:flaky"]

        def handle_message(self, stream, msg_id, fields):
            if fields.get("crash"):
                raise RuntimeError("boom")
            received.append(fields["i"])
            if fields["i"] == "3":
                self.stop()

    agent = Flaky(bus=bus, block_ms=100)
    t = threading.Thread(target=agent.run, daemon=True)
    t.start()
    time.sleep(0.1)
    bus.publish("bus:flaky", {"i": "1"})
    bus.publish("bus:flaky", {"i": "2", "crash": "yes"})
    bus.publish("bus:flaky", {"i": "3"})
    t.join(timeout=3)

    assert "1" in received
    assert "3" in received  # didn't die on the crash in between

    status = bus.hget_all("agent:flaky:status")
    assert "last_error" in status
    assert "boom" in status["last_error"]


def _run_agent(agent):
    t = threading.Thread(target=agent.run, daemon=True)
    t.start()
    return t


def test_durable_agent_receives_messages_published_while_down(bus):
    received: list[dict] = []

    class Worker(Agent):
        role = "worker"
        streams = ["bus:jobs"]

        def handle_message(self, stream, msg_id, fields):
            received.append(fields)
            self.stop()

    # First run registers the consumer group, then goes down.
    a1 = Worker(bus=bus, durable=True, block_ms=100)
    t1 = _run_agent(a1)
    time.sleep(0.1)
    a1.stop()
    t1.join(timeout=2)

    # Published while nothing is running.
    bus.publish("bus:jobs", {"job": "alpha-42"})

    # Restarted agent picks it up.
    a2 = Worker(bus=bus, durable=True, block_ms=100)
    t2 = _run_agent(a2)
    t2.join(timeout=3)
    assert received == [{"job": "alpha-42"}]

    # Acked: a third run (claiming immediately) sees nothing.
    a3 = Worker(bus=bus, durable=True, block_ms=100, claim_min_idle_ms=0)
    t3 = _run_agent(a3)
    time.sleep(0.3)
    a3.stop()
    t3.join(timeout=2)
    assert received == [{"job": "alpha-42"}]


def test_durable_handler_exception_leaves_message_for_redelivery(bus):
    outcomes: list[str] = []

    class Crashy(Agent):
        role = "crashy"
        streams = ["bus:jobs"]

        def handle_message(self, stream, msg_id, fields):
            outcomes.append("crash")
            raise RuntimeError("boom")

    class Steady(Agent):
        role = "crashy"  # same group
        streams = ["bus:jobs"]

        def handle_message(self, stream, msg_id, fields):
            outcomes.append(f"ok:{fields['job']}")
            self.stop()

    a1 = Crashy(bus=bus, durable=True, block_ms=100)
    t1 = _run_agent(a1)
    time.sleep(0.1)
    bus.publish("bus:jobs", {"job": "alpha-42"})
    time.sleep(0.3)
    a1.stop()
    t1.join(timeout=2)
    assert "crash" in outcomes

    # Not acked, so a replacement consumer claims and handles it.
    a2 = Steady(bus=bus, durable=True, block_ms=100, claim_min_idle_ms=0)
    t2 = _run_agent(a2)
    t2.join(timeout=3)
    assert "ok:alpha-42" in outcomes


def test_non_durable_agent_misses_offline_messages(bus):
    received: list[dict] = []

    class Tail(Agent):
        role = "tail"
        streams = ["bus:jobs"]

        def handle_message(self, stream, msg_id, fields):
            received.append(fields)

    bus.publish("bus:jobs", {"job": "before"})  # published before subscribe
    agent = Tail(bus=bus, block_ms=100)
    t = _run_agent(agent)
    time.sleep(0.2)
    agent.stop()
    t.join(timeout=2)
    assert received == []


def test_bounded_dispatch_blocks_read_loop(bus):
    started: list[str] = []
    release = threading.Event()

    class Slow(Agent):
        role = "slow"
        streams = ["bus:work"]

        def handle_message(self, stream, msg_id, fields):
            started.append(fields["i"])
            release.wait(timeout=5)

    agent = Slow(bus=bus, max_concurrent=2, block_ms=100)
    t = _run_agent(agent)
    time.sleep(0.1)
    for i in range(6):
        bus.publish("bus:work", {"i": str(i)})
    time.sleep(0.6)
    # Two handlers in flight; the read loop is parked on the semaphore
    # instead of queueing the other four.
    assert len(started) == 2
    release.set()
    deadline = time.time() + 5
    while len(started) < 6 and time.time() < deadline:
        time.sleep(0.05)
    agent.stop()
    t.join(timeout=3)
    assert sorted(started) == [str(i) for i in range(6)]
