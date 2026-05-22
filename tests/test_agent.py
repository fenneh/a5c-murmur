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
    t = threading.Thread(target=agent.run)
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
    t = threading.Thread(target=agent.run)
    t.start()
    time.sleep(0.4)
    agent.stop()
    t.join(timeout=2)
    status = bus.hget_all("agent:idle:status")
    assert "last_seen" in status
    assert status.get("status") in {"running", "stopped"}


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
    t = threading.Thread(target=agent.run)
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
