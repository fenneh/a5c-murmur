from __future__ import annotations

import threading
from typing import Iterator, Protocol


class BusAdapter(Protocol):
    """Pluggable transport. Implement one of these to back murmur with
    something other than Redis (NATS, Kafka, RabbitMQ, etc)."""

    def publish(self, stream: str, fields: dict[str, str]) -> str:
        """Append a message to `stream`. Return the assigned message id."""

    def history(
        self,
        stream: str,
        *,
        start: str = "-",
        end: str = "+",
        count: int | None = None,
    ) -> list[tuple[str, dict[str, str]]]:
        """Return historical messages ordered oldest first."""

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
        empty poll. Update `last_ids` after each yield so resumption works.

        If `stop` is set the iterator returns. Adapters should check `stop`
        both before blocking and after the block returns."""

    def hset(self, key: str, fields: dict[str, str]) -> None:
        """Write a presence / status hash. Used for agent heartbeats."""

    def hget_all(self, key: str) -> dict[str, str]:
        """Read a presence / status hash."""

    def keys(self, pattern: str) -> list[str]:
        """List keys matching a glob pattern."""

    def delete(self, *keys: str) -> int:
        """Delete keys. Returns number actually deleted."""

    def trim(self, stream: str, maxlen: int) -> int:
        """Trim a stream to at most `maxlen` entries. Returns entries removed."""
