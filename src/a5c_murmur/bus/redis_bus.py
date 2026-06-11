from __future__ import annotations

import os
import threading
from collections.abc import Iterator


class RedisBus:
    """Default production adapter. Wraps redis-py with the BusAdapter shape."""

    def __init__(self, url: str | None = None, *, decode_responses: bool = True, client=None):
        if client is not None:
            self._r = client
            return
        try:
            import redis
        except ImportError as e:
            raise ImportError("RedisBus needs the redis package. Install with: uv add redis") from e
        self._r = redis.from_url(
            url or os.environ.get("REDIS_URL", "redis://localhost:6379"),
            decode_responses=decode_responses,
        )

    def publish(self, stream: str, fields: dict[str, str], *, maxlen: int | None = None) -> str:
        if maxlen is not None:
            return self._r.xadd(stream, fields, maxlen=maxlen, approximate=True)
        return self._r.xadd(stream, fields)

    def history(
        self,
        stream: str,
        *,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
        reverse: bool = False,
    ) -> list[tuple[str, dict[str, str]]]:
        kwargs = {"count": count} if count else {}
        if reverse:
            return self._r.xrevrange(stream, end, start, **kwargs)
        return self._r.xrange(stream, start, end, **kwargs)

    def subscribe(
        self,
        streams: list[str],
        *,
        last_ids: dict[str, str] | None = None,
        block_ms: int = 5000,
        count: int = 10,
        stop: threading.Event | None = None,
    ) -> Iterator[tuple[str, str, dict[str, str]]]:
        last_ids = dict(last_ids or {s: "0" for s in streams})
        while True:
            if stop is not None and stop.is_set():
                return
            streams_to_read = {s: last_ids[s] for s in streams}
            result = self._r.xread(streams_to_read, count=count, block=block_ms)
            if stop is not None and stop.is_set():
                return
            if not result:
                continue
            for s, entries in result:
                for msg_id, fields in entries:
                    yield s, msg_id, fields
                    last_ids[s] = msg_id

    def _ensure_group(self, stream: str, group: str) -> None:
        try:
            self._r.xgroup_create(stream, group, id="$", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                raise

    def subscribe_group(
        self,
        streams: list[str],
        group: str,
        consumer: str,
        *,
        block_ms: int = 5000,
        count: int = 10,
        min_idle_ms: int = 60_000,
        stop: threading.Event | None = None,
    ) -> Iterator[tuple[str, str, dict[str, str]]]:
        for s in streams:
            self._ensure_group(s, group)
        # Claim entries left pending by dead consumers (XAUTOCLAIM walks the
        # PEL; entries idle >= min_idle_ms move to this consumer).
        for s in streams:
            cursor = "0-0"
            while True:
                reply = self._r.xautoclaim(
                    s, group, consumer, min_idle_time=min_idle_ms, start_id=cursor, count=count
                )
                next_cursor, claimed = reply[0], reply[1]
                for msg_id, fields in claimed:
                    yield s, msg_id, fields
                if not claimed or next_cursor in ("0-0", cursor):
                    break
                cursor = next_cursor
        while True:
            if stop is not None and stop.is_set():
                return
            result = self._r.xreadgroup(
                group, consumer, {s: ">" for s in streams}, count=count, block=block_ms
            )
            if stop is not None and stop.is_set():
                return
            if not result:
                continue
            for s, entries in result:
                for msg_id, fields in entries:
                    yield s, msg_id, fields

    def ack(self, stream: str, group: str, msg_id: str) -> None:
        self._r.xack(stream, group, msg_id)

    def hset(self, key: str, fields: dict[str, str]) -> None:
        self._r.hset(key, mapping=fields)

    def hget(self, key: str, field: str) -> str | None:
        v = self._r.hget(key, field)
        return v if v is not None else None

    def hget_all(self, key: str) -> dict[str, str]:
        return self._r.hgetall(key) or {}

    def hincrby_float(self, key: str, field: str, amount: float) -> float:
        return float(self._r.hincrbyfloat(key, field, amount))

    def expire(self, key: str, seconds: int) -> bool:
        return bool(self._r.expire(key, seconds))

    def keys(self, pattern: str) -> list[str]:
        return list(self._r.scan_iter(match=pattern))

    def delete(self, *keys: str) -> int:
        if not keys:
            return 0
        return self._r.delete(*keys)

    def trim(self, stream: str, maxlen: int) -> int:
        return self._r.xtrim(stream, maxlen=maxlen, approximate=False)

    def raw_client(self):
        return self._r
