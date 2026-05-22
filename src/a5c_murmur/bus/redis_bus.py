from __future__ import annotations

import os
import threading
from collections.abc import Iterator


class RedisBus:
    """Default production adapter. Wraps redis-py with the BusAdapter shape."""

    def __init__(self, url: str | None = None, *, decode_responses: bool = True):
        try:
            import redis
        except ImportError as e:
            raise ImportError(
                "RedisBus needs the redis package. Install with: uv add a5c-murmur"
            ) from e
        self._r = redis.from_url(
            url or os.environ.get("REDIS_URL", "redis://localhost:6379"),
            decode_responses=decode_responses,
        )

    def publish(self, stream: str, fields: dict[str, str]) -> str:
        return self._r.xadd(stream, fields)

    def history(
        self,
        stream: str,
        *,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, str]]]:
        kwargs = {"count": count} if count else {}
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

    def hset(self, key: str, fields: dict[str, str]) -> None:
        self._r.hset(key, mapping=fields)

    def hget_all(self, key: str) -> dict[str, str]:
        return self._r.hgetall(key) or {}

    def keys(self, pattern: str) -> list[str]:
        return list(self._r.scan_iter(match=pattern))

    def delete(self, *keys: str) -> int:
        if not keys:
            return 0
        return self._r.delete(*keys)

    def trim(self, stream: str, maxlen: int) -> int:
        return self._r.xtrim(stream, maxlen=maxlen, approximate=False)
