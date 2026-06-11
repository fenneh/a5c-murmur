"""RedisBus against fakeredis. Covers the adapter surface that differs
from InMemoryBus (xadd maxlen, xrevrange, hincrbyfloat)."""

import pytest

fakeredis = pytest.importorskip("fakeredis")

from a5c_murmur.bus.redis_bus import RedisBus  # noqa: E402


@pytest.fixture
def rbus():
    return RedisBus(client=fakeredis.FakeRedis(decode_responses=True))


def test_publish_and_history(rbus):
    mid = rbus.publish("s", {"a": "1"})
    assert mid
    hist = rbus.history("s")
    assert len(hist) == 1
    assert hist[0][1] == {"a": "1"}


def test_history_reverse_with_count(rbus):
    for i in range(5):
        rbus.publish("s", {"i": str(i)})
    out = rbus.history("s", count=2, reverse=True)
    assert [f["i"] for _, f in out] == ["4", "3"]


def test_publish_maxlen_bounds_stream(rbus):
    for i in range(50):
        rbus.publish("s", {"i": str(i)}, maxlen=5)
    # Approximate trimming may keep a few extra, but growth is bounded.
    assert len(rbus.history("s")) < 50


def test_hincrby_float(rbus):
    assert rbus.hincrby_float("k", "f", 1.5) == pytest.approx(1.5)
    assert rbus.hincrby_float("k", "f", 2.0) == pytest.approx(3.5)


def test_trim(rbus):
    for i in range(10):
        rbus.publish("s", {"i": str(i)})
    removed = rbus.trim("s", maxlen=3)
    assert removed == 7
    assert len(rbus.history("s")) == 3


def _create_group(rbus, stream, group):
    import threading

    stop = threading.Event()
    stop.set()
    list(rbus.subscribe_group([stream], group, "bootstrap", block_ms=10, stop=stop))


def _consume_one(rbus, stream, group, consumer, *, ack=True, min_idle_ms=60_000):
    import threading

    stop = threading.Event()
    got = []
    for s, mid, fields in rbus.subscribe_group(
        [stream], group, consumer, block_ms=100, min_idle_ms=min_idle_ms, stop=stop
    ):
        got.append((mid, fields))
        if ack:
            rbus.ack(s, group, mid)
        stop.set()
    return got


def test_subscribe_group_delivers_offline_messages(rbus):
    _create_group(rbus, "bus:events", "workers")
    rbus.publish("bus:events", {"job": "alpha-42"})
    got = _consume_one(rbus, "bus:events", "workers", "w-1")
    assert [f for _, f in got] == [{"job": "alpha-42"}]


def test_new_group_starts_at_tail(rbus):
    rbus.publish("bus:events", {"job": "old"})
    _create_group(rbus, "bus:events", "workers")
    rbus.publish("bus:events", {"job": "new"})
    got = _consume_one(rbus, "bus:events", "workers", "w-1")
    assert [f["job"] for _, f in got] == ["new"]


def test_unacked_message_redelivered_via_autoclaim(rbus):
    _create_group(rbus, "bus:events", "workers")
    rbus.publish("bus:events", {"job": "alpha-42"})
    got1 = _consume_one(rbus, "bus:events", "workers", "w-1", ack=False)
    assert len(got1) == 1
    # A replacement consumer claims the stale pending entry on startup.
    got2 = _consume_one(rbus, "bus:events", "workers", "w-2", min_idle_ms=0)
    assert got2 == got1
