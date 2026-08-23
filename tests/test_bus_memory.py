import threading
import time


def test_publish_and_history(bus):
    mid = bus.publish("s", {"a": "1"})
    assert mid
    hist = bus.history("s")
    assert len(hist) == 1
    assert hist[0][1] == {"a": "1"}


def test_subscribe_yields_in_order(bus):
    bus.publish("s", {"i": "1"})
    bus.publish("s", {"i": "2"})
    seen = []

    def consume():
        for _stream, _msg_id, fields in bus.subscribe(["s"], last_ids={"s": "0"}, block_ms=200):
            seen.append(fields["i"])
            if len(seen) >= 2:
                break

    t = threading.Thread(target=consume, daemon=True)
    t.start()
    t.join(timeout=2)
    assert seen == ["1", "2"]


def test_hset_and_hget(bus):
    bus.hset("k", {"status": "running", "pid": "1"})
    got = bus.hget_all("k")
    assert got["status"] == "running"
    assert got["pid"] == "1"


def test_keys_glob(bus):
    bus.hset("agent:a:status", {"status": "running"})
    bus.hset("agent:b:status", {"status": "running"})
    bus.publish("bus:x", {"v": "1"})
    matches = bus.keys("agent:*:status")
    assert set(matches) == {"agent:a:status", "agent:b:status"}


def test_publish_maxlen_trims(bus):
    for i in range(10):
        bus.publish("s", {"i": str(i)}, maxlen=3)
    hist = bus.history("s")
    assert [f["i"] for _, f in hist] == ["7", "8", "9"]


def test_history_reverse(bus):
    for i in range(5):
        bus.publish("s", {"i": str(i)})
    out = bus.history("s", count=2, reverse=True)
    assert [f["i"] for _, f in out] == ["4", "3"]


def test_hincrby_float(bus):
    assert bus.hincrby_float("k", "f", 1.5) == 1.5
    assert bus.hincrby_float("k", "f", 2.0) == 3.5
    assert float(bus.hget("k", "f")) == 3.5


def test_trim(bus):
    for i in range(10):
        bus.publish("s", {"i": str(i)})
    removed = bus.trim("s", maxlen=3)
    assert removed == 7
    assert len(bus.history("s")) == 3


def test_subscribe_resumes_from_last_id(bus):
    bus.publish("s", {"i": "1"})
    bus.publish("s", {"i": "2"})

    # First read picks up both.
    first_seen = []

    def consume(seen, last):
        for _stream, msg_id, fields in bus.subscribe(["s"], last_ids=last, block_ms=100):
            seen.append((msg_id, fields["i"]))
            if len(seen) >= 2:
                break

    last1 = {"s": "0"}
    t = threading.Thread(target=consume, args=(first_seen, last1), daemon=True)
    t.start()
    t.join(timeout=2)
    assert [i for _, i in first_seen] == ["1", "2"]

    # Publishing again, second consumer with last_ids set to the previous max
    # only sees the new one.
    bus.publish("s", {"i": "3"})
    last_seen_id = first_seen[-1][0]
    last2 = {"s": last_seen_id}
    second_seen = []
    t2 = threading.Thread(target=consume, args=(second_seen, last2), daemon=True)
    t2.start()
    time.sleep(0.3)
    t2.join(timeout=2)
    assert any(i == "3" for _, i in second_seen)


def _create_group(bus, stream, group):
    stop = threading.Event()
    stop.set()
    list(bus.subscribe_group([stream], group, "bootstrap", block_ms=10, stop=stop))


def _consume_one(bus, stream, group, consumer, *, ack=True, min_idle_ms=60_000):
    stop = threading.Event()
    got = []
    for s, mid, fields in bus.subscribe_group(
        [stream], group, consumer, block_ms=100, min_idle_ms=min_idle_ms, stop=stop
    ):
        got.append((mid, fields))
        if ack:
            bus.ack(s, group, mid)
        stop.set()
    return got


def test_subscribe_group_delivers_offline_messages(bus):
    _create_group(bus, "bus:events", "workers")
    bus.publish("bus:events", {"job": "alpha-42"})  # nobody is consuming
    got = _consume_one(bus, "bus:events", "workers", "w-1")
    assert [f for _, f in got] == [{"job": "alpha-42"}]
    # Acked and cursor advanced: a second consumer sees nothing, even with
    # min_idle_ms=0.
    stop = threading.Event()
    seen = []
    t = threading.Thread(
        target=lambda: seen.extend(
            bus.subscribe_group(
                ["bus:events"], "workers", "w-2", block_ms=50, min_idle_ms=0, stop=stop
            )
        ),
        daemon=True,
    )
    t.start()
    time.sleep(0.3)
    stop.set()
    t.join(timeout=2)
    assert seen == []


def test_new_group_starts_at_tail(bus):
    bus.publish("bus:events", {"job": "old"})
    _create_group(bus, "bus:events", "workers")
    bus.publish("bus:events", {"job": "new"})
    got = _consume_one(bus, "bus:events", "workers", "w-1")
    assert [f["job"] for _, f in got] == ["new"]


def test_unacked_message_redelivered_via_claim(bus):
    _create_group(bus, "bus:events", "workers")
    bus.publish("bus:events", {"job": "alpha-42"})
    # First consumer reads but never acks (simulated crash).
    got1 = _consume_one(bus, "bus:events", "workers", "w-1", ack=False)
    assert len(got1) == 1
    # Second consumer claims the stale pending entry.
    got2 = _consume_one(bus, "bus:events", "workers", "w-2", min_idle_ms=0)
    assert got2 == got1
    # Now acked: a third consumer gets nothing back.
    stop = threading.Event()
    stop.set()
    got3 = list(
        bus.subscribe_group(["bus:events"], "workers", "w-3", block_ms=10, min_idle_ms=0, stop=stop)
    )
    assert got3 == []
