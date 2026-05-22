"""Daily budget tracking with kill-switch on overrun."""

import threading
import time

import pytest

from a5c_murmur import Agent, BudgetExceeded
from a5c_murmur.agent import SPEND_KEY_FMT
from a5c_murmur.bus import InMemoryBus


class _NoopAgent(Agent):
    role = "noop"
    streams = ["bus:noop"]

    def handle_message(self, stream, msg_id, fields):
        pass


def test_no_budget_track_is_noop(bus):
    a = _NoopAgent(bus=bus)
    a.track_spend(0.5)
    a.track_spend(1000)
    assert a.spend_today == 0.0  # nothing tracked when daily_budget is unset


def test_budget_accumulates(bus):
    a = _NoopAgent(bus=bus, daily_budget=5.0)
    a.track_spend(1.0)
    a.track_spend(2.5)
    assert a.spend_today == pytest.approx(3.5)


def test_budget_persists_in_bus(bus):
    a = _NoopAgent(bus=bus, daily_budget=5.0)
    a.track_spend(2.0)
    today = time.strftime("%Y-%m-%d")
    stored = bus.hget(SPEND_KEY_FMT.format(role="noop"), today)
    assert float(stored) == pytest.approx(2.0)


def test_overrun_engages_kill_switch_and_raises(bus, tmp_path):
    ks = tmp_path / "kill"
    a = _NoopAgent(bus=bus, daily_budget=1.0, kill_switch_path=str(ks))
    a.track_spend(0.5)  # under
    assert not ks.exists()
    with pytest.raises(BudgetExceeded):
        a.track_spend(0.8)  # cumulative 1.3 > 1.0
    assert ks.exists()
    content = ks.read_text()
    assert "noop" in content
    assert "daily_budget_exceeded" in content


def test_consume_loop_pauses_when_kill_switch_engaged(tmp_path):
    received = []
    ks = tmp_path / "kill"
    bus = InMemoryBus()

    class Sink(Agent):
        role = "sink"
        streams = ["bus:t"]

        def handle_message(self, stream, msg_id, fields):
            received.append(fields["i"])
            if len(received) >= 1:
                self.stop()

    a = Sink(bus=bus, block_ms=100, kill_switch_path=str(ks))
    ks.write_text("trip")
    t = threading.Thread(target=a.run, daemon=True)
    t.start()
    time.sleep(0.1)
    bus.publish("bus:t", {"i": "1"})
    time.sleep(0.5)
    a.stop()
    t.join(timeout=2)
    assert received == []
    status = bus.hget_all("agent:sink:status")
    assert status.get("status") in {"paused", "stopped"}


def test_engage_kill_switch_writes_file(tmp_path, bus):
    ks = tmp_path / "subdir" / "kill"
    a = _NoopAgent(bus=bus, kill_switch_path=str(ks))
    a.engage_kill_switch(reason="manual test")
    assert ks.exists()
    assert "manual test" in ks.read_text()
    assert "noop" in ks.read_text()


def test_engage_kill_switch_noop_if_unset(bus):
    a = _NoopAgent(bus=bus)
    a.engage_kill_switch(reason="should not crash")
    assert a.kill_switch_path is None


def test_existing_spend_loaded_on_init(bus):
    today = time.strftime("%Y-%m-%d")
    bus.hset(SPEND_KEY_FMT.format(role="noop"), {today: "2.5"})
    a = _NoopAgent(bus=bus, daily_budget=5.0)
    assert a.spend_today == pytest.approx(2.5)
    a.track_spend(1.0)
    assert a.spend_today == pytest.approx(3.5)
