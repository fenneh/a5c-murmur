import threading
import time

from a5c_murmur.bus import InMemoryBus


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
        for stream, msg_id, fields in bus.subscribe(["s"], last_ids={"s": "0"}, block_ms=200):
            seen.append(fields["i"])
            if len(seen) >= 2:
                break

    t = threading.Thread(target=consume)
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
        for stream, msg_id, fields in bus.subscribe(["s"], last_ids=last, block_ms=100):
            seen.append((msg_id, fields["i"]))
            if len(seen) >= 2:
                break

    last1 = {"s": "0"}
    t = threading.Thread(target=consume, args=(first_seen, last1))
    t.start()
    t.join(timeout=2)
    assert [i for _, i in first_seen] == ["1", "2"]

    # Publishing again, second consumer with last_ids set to the previous max
    # only sees the new one.
    bus.publish("s", {"i": "3"})
    last_seen_id = first_seen[-1][0]
    last2 = {"s": last_seen_id}
    second_seen = []
    t2 = threading.Thread(target=consume, args=(second_seen, last2))
    t2.start()
    time.sleep(0.3)
    t2.join(timeout=2)
    assert any(i == "3" for _, i in second_seen)
