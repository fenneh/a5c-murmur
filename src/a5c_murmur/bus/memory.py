"""In-memory bus adapter. Useful for tests and single-script demos. Not
durable across processes."""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from collections.abc import Iterator


def _new_id(prev: str | None) -> str:
    """Redis-style ms-seq id. Always strictly increasing within a stream."""
    ms = int(time.time() * 1000)
    if prev is not None:
        prev_ms, prev_seq = (int(x) for x in prev.split("-"))
        if prev_ms >= ms:
            return f"{prev_ms}-{prev_seq + 1}"
    return f"{ms}-0"


class InMemoryBus:
    def __init__(self) -> None:
        self._streams: dict[str, list[tuple[str, dict[str, str]]]] = defaultdict(list)
        self._hashes: dict[str, dict[str, str]] = defaultdict(dict)
        self._cv = threading.Condition()

    def publish(self, stream: str, fields: dict[str, str]) -> str:
        with self._cv:
            prev_id = self._streams[stream][-1][0] if self._streams[stream] else None
            msg_id = _new_id(prev_id)
            self._streams[stream].append((msg_id, dict(fields)))
            self._cv.notify_all()
            return msg_id

    def history(
        self,
        stream: str,
        *,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, str]]]:
        with self._cv:
            entries = list(self._streams.get(stream, []))
        out = [e for e in entries if _between(e[0], start, end)]
        if count is not None:
            out = out[:count]
        return out

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
        # Resolve "$" (Redis idiom: from current tail) to whatever id is at the
        # tail right now.
        with self._cv:
            for s, cur in list(last_ids.items()):
                if cur == "$":
                    entries = self._streams.get(s, [])
                    last_ids[s] = entries[-1][0] if entries else "0"
        timeout = block_ms / 1000.0
        while True:
            if stop is not None and stop.is_set():
                return
            with self._cv:
                yielded = 0
                for s in streams:
                    entries = self._streams.get(s, [])
                    cursor = last_ids.get(s, "0")
                    for msg_id, fields in entries:
                        if _gt(msg_id, cursor):
                            yield s, msg_id, fields
                            last_ids[s] = msg_id
                            yielded += 1
                            if yielded >= count:
                                break
                    if yielded >= count:
                        break
                if yielded == 0:
                    self._cv.wait(timeout=timeout)
            if stop is not None and stop.is_set():
                return

    def hset(self, key: str, fields: dict[str, str]) -> None:
        with self._cv:
            self._hashes[key].update(fields)

    def hget_all(self, key: str) -> dict[str, str]:
        with self._cv:
            return dict(self._hashes.get(key, {}))

    def keys(self, pattern: str) -> list[str]:
        import fnmatch

        with self._cv:
            return [
                k
                for k in (*self._streams.keys(), *self._hashes.keys())
                if fnmatch.fnmatch(k, pattern)
            ]

    def delete(self, *keys: str) -> int:
        n = 0
        with self._cv:
            for k in keys:
                if k in self._streams:
                    del self._streams[k]
                    n += 1
                if k in self._hashes:
                    del self._hashes[k]
                    n += 1
        return n

    def trim(self, stream: str, maxlen: int) -> int:
        with self._cv:
            entries = self._streams.get(stream, [])
            removed = max(0, len(entries) - maxlen)
            if removed:
                self._streams[stream] = entries[-maxlen:]
            return removed


def _parse_id(s: str) -> tuple[int, int]:
    """Accept normal 'ms-seq' ids and the Redis specials '0', '-', '+', '$'.
    '$' is handled by the caller (means 'after current tail')."""
    if s in {"0", "-"}:
        return (0, 0)
    if s == "+":
        return (10**18, 10**18)
    if "-" in s:
        m, sq = s.split("-", 1)
        return (int(m), int(sq))
    return (int(s), 0)


def _gt(a: str, b: str) -> bool:
    return _parse_id(a) > _parse_id(b)


def _between(mid: str, start: str, end: str) -> bool:
    if start != "-":
        if not _gt(mid, _prev(start)):
            return False
    if end != "+":
        if _gt(mid, end):
            return False
    return True


def _prev(mid: str) -> str:
    if mid in {"0", "-"}:
        return "0"
    if mid == "+":
        return mid
    m, sq = _parse_id(mid)
    if sq > 0:
        return f"{m}-{sq - 1}"
    return f"{m - 1}-9999999"
