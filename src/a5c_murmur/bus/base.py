from __future__ import annotations

import threading
from collections.abc import Iterator
from typing import Protocol


class BusAdapter(Protocol):
    """Pluggable transport. Implement one of these to back murmur with
    something other than Redis (NATS, Kafka, RabbitMQ, etc)."""

    def publish(self, stream: str, fields: dict[str, str], *, maxlen: int | None = None) -> str:
        """Append a message to `stream`. Return the assigned message id.

        If `maxlen` is set, trim the stream to roughly that many entries on
        write (Redis uses approximate XADD MAXLEN trimming, which is cheap)."""

    def history(
        self,
        stream: str,
        *,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
        reverse: bool = False,
    ) -> list[tuple[str, dict[str, str]]]:
        """Return historical messages, oldest first. With `reverse=True`,
        newest first; `count` then caps from the newest end (XREVRANGE)."""

    def subscribe(
        self,
        streams: list[str],
        *,
        last_ids: dict[str, str] | None = None,
        block_ms: int = 5000,
        count: int = 10,
        stop: threading.Event | None = None,
    ) -> Iterator[tuple[str, str, dict[str, str]]]:
        """Yield (stream, msg_id, fields) tuples. Blocks for `block_ms` per
        empty poll. Adapters keep their own cursor (a copy of `last_ids`);
        the caller's dict is not mutated.

        If `stop` is set the iterator returns. Adapters should check `stop`
        both before blocking and after the block returns."""

    def hset(self, key: str, fields: dict[str, str]) -> None:
        """Write a presence / status hash. Used for agent heartbeats."""

    def hget(self, key: str, field: str) -> str | None:
        """Read a single field from a hash."""

    def hincrby_float(self, key: str, field: str, amount: float) -> float:
        """Atomically add `amount` to a float hash field. Return the new
        total. Used for shared counters like per-day spend."""

    def hget_all(self, key: str) -> dict[str, str]:
        """Read a presence / status hash."""

    def expire(self, key: str, seconds: int) -> bool:
        """Set a TTL on a key. Returns True if applied."""

    def keys(self, pattern: str) -> list[str]:
        """List keys matching a glob pattern."""

    def delete(self, *keys: str) -> int:
        """Delete keys. Returns number actually deleted."""

    def trim(self, stream: str, maxlen: int) -> int:
        """Trim a stream to at most `maxlen` entries. Returns entries removed."""

    def raw_client(self) -> object:
        """Return the underlying transport client (e.g. a redis-py client)
        for operations the adapter doesn't model. Escape hatch: callers
        couple themselves to the concrete adapter by using it. Adapters
        without a meaningful client should raise NotImplementedError."""
