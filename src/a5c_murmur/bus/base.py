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
        """Durable at-least-once delivery through a consumer group.

        The group is created on first use and starts at the stream tail
        ('$'): a brand-new group only sees messages published after it
        exists. Once the group exists, messages published while no consumer
        is running are retained and delivered when one starts.

        Before reading new messages, pending entries idle for at least
        `min_idle_ms` (e.g. left behind by a crashed consumer) are claimed
        by this consumer and re-yielded. Every yielded message stays pending
        until `ack`ed, so handlers must be idempotent: an unacked message
        will be delivered again."""

    def ack(self, stream: str, group: str, msg_id: str) -> None:
        """Acknowledge a message delivered via `subscribe_group`. Until a
        message is acked it stays pending and is eligible for redelivery."""

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
