from __future__ import annotations

import os

from a5c_murmur.bus.base import BusAdapter


class Bus:
    """Entry point. `Bus.open()` picks an adapter from A5C_MURMUR_BUS or the
    redis vs in-memory default."""

    @staticmethod
    def open(*, kind: str | None = None, url: str | None = None) -> BusAdapter:
        kind = (kind or os.environ.get("A5C_MURMUR_BUS", "redis")).lower()
        if kind in {"memory", "in_memory", "fake"}:
            from a5c_murmur.bus.memory import InMemoryBus

            return InMemoryBus()
        if kind == "redis":
            from a5c_murmur.bus.redis_bus import RedisBus

            return RedisBus(url=url)
        raise ValueError(f"unknown bus kind: {kind!r}")
