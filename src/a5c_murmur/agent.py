"""Persistent agent base class. Subclass it, list streams, implement
handle_message. Run via Agent().run() — blocks with an XREAD loop, sends
heartbeats, and handles SIGTERM cleanly."""

from __future__ import annotations

import os
import signal
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor

from a5c_murmur.bus import Bus, BusAdapter

HEARTBEAT_INTERVAL_S = 30.0
STATUS_KEY_FMT = "agent:{role}:status"
SPEND_KEY_FMT = "agent:{role}:spend"


class BudgetExceeded(Exception):
    """Raised by `Agent.track_spend` when today's cumulative spend has gone
    past `daily_budget`. The agent has already tripped its kill-switch by
    the time this is raised."""


class Agent(ABC):
    """Subclasses set `role` and `streams` (class attrs) and implement
    `handle_message`. The XREAD loop reads from streams, dispatches each
    message to the handler, and updates a presence hash on Redis.

    Optional cost-cap behaviour: if `daily_budget` is non-zero, callers
    should invoke `self.track_spend(cost)` from inside `handle_message`
    each time they pay for an LLM call / API hit / whatever. When the
    daily total goes over budget the agent writes a kill-switch file
    (if `kill_switch_path` is set) and raises BudgetExceeded so the
    caller can short-circuit the rest of the handler."""

    role: str = "agent"
    streams: list[str] = []

    def __init__(
        self,
        *,
        bus: BusAdapter | None = None,
        max_concurrent: int = 1,
        block_ms: int = 5000,
        heartbeat_interval_s: float = HEARTBEAT_INTERVAL_S,
        kill_switch_path: str | None = None,
        daily_budget: float = 0.0,
    ):
        self.bus = bus or Bus.open()
        self.max_concurrent = max_concurrent
        self.block_ms = block_ms
        self.heartbeat_interval_s = heartbeat_interval_s
        self.kill_switch_path = kill_switch_path
        self.daily_budget = daily_budget
        self._stop = threading.Event()
        self._pool: ThreadPoolExecutor | None = None
        self._active_tasks: dict[str, float] = {}
        self._active_lock = threading.Lock()
        self._spend_today: float = self._read_today_spend() if daily_budget else 0.0

    # ------------------------------------------------------------------
    @abstractmethod
    def handle_message(self, stream: str, msg_id: str, fields: dict[str, str]) -> None: ...

    # ------------------------------------------------------------------
    def run(self) -> None:
        if not self.streams:
            raise RuntimeError(f"{type(self).__name__}.streams is empty")
        self._install_signal_handlers()
        heartbeat = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat.start()
        self._mark_status("running")

        if self.max_concurrent > 1:
            self._pool = ThreadPoolExecutor(max_workers=self.max_concurrent)

        try:
            self._consume_loop()
        finally:
            self._mark_status("stopped")
            if self._pool is not None:
                self._pool.shutdown(wait=True)

    def stop(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------------
    def _consume_loop(self) -> None:
        last_ids = {s: "$" for s in self.streams}
        for stream, msg_id, fields in self.bus.subscribe(
            self.streams,
            last_ids=last_ids,
            block_ms=self.block_ms,
            stop=self._stop,
        ):
            if self._stop.is_set():
                break
            if self._kill_switch_engaged():
                self._mark_status("paused")
                time.sleep(2)
                continue
            self._dispatch(stream, msg_id, fields)

    def _dispatch(self, stream: str, msg_id: str, fields: dict[str, str]) -> None:
        with self._active_lock:
            self._active_tasks[msg_id] = time.time()
        try:
            if self._pool is not None:
                self._pool.submit(self._safe_handle, stream, msg_id, fields)
            else:
                self._safe_handle(stream, msg_id, fields)
        finally:
            if self._pool is None:
                with self._active_lock:
                    self._active_tasks.pop(msg_id, None)

    def _safe_handle(self, stream: str, msg_id: str, fields: dict[str, str]) -> None:
        try:
            self.handle_message(stream, msg_id, fields)
        except Exception as e:
            # Stay alive on individual handler errors. Log to status.
            self.bus.hset(
                STATUS_KEY_FMT.format(role=self.role),
                {"last_error": f"{type(e).__name__}: {e}", "last_error_ts": str(time.time())},
            )
        finally:
            with self._active_lock:
                self._active_tasks.pop(msg_id, None)

    # ------------------------------------------------------------------
    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.heartbeat_interval_s):
            with self._active_lock:
                active = len(self._active_tasks)
            self.bus.hset(
                STATUS_KEY_FMT.format(role=self.role),
                {
                    "last_seen": str(time.time()),
                    "active_tasks": str(active),
                    "pid": str(os.getpid()),
                },
            )

    def _mark_status(self, status: str) -> None:
        self.bus.hset(
            STATUS_KEY_FMT.format(role=self.role),
            {"status": status, "last_seen": str(time.time()), "pid": str(os.getpid())},
        )

    def _install_signal_handlers(self) -> None:
        def _handler(signum, frame):
            self._stop.set()

        try:
            signal.signal(signal.SIGTERM, _handler)
            signal.signal(signal.SIGINT, _handler)
        except ValueError:
            # Not in main thread; signals not installable. Fine.
            pass

    def _kill_switch_engaged(self) -> bool:
        if not self.kill_switch_path:
            return False
        return os.path.exists(self.kill_switch_path)

    def engage_kill_switch(self, reason: str = "") -> None:
        """Touch the kill-switch file. The next iteration of the consume
        loop will see it and pause."""
        if not self.kill_switch_path:
            return
        from pathlib import Path

        path = Path(self.kill_switch_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = f"{time.time():.0f} {self.role} {reason}".strip()
        path.write_text(payload)

    # ---- budget tracking ---------------------------------------------
    def _read_today_spend(self) -> float:
        today = time.strftime("%Y-%m-%d")
        raw = self.bus.hget(SPEND_KEY_FMT.format(role=self.role), today)
        try:
            return float(raw) if raw else 0.0
        except (TypeError, ValueError):
            return 0.0

    def track_spend(self, cost: float) -> None:
        """Add ``cost`` to today's running total. If the total goes past
        ``daily_budget`` (non-zero), the kill-switch is engaged and
        ``BudgetExceeded`` is raised. Persisted to a per-role Redis hash
        ``agent:{role}:spend`` keyed by YYYY-MM-DD."""
        if self.daily_budget <= 0:
            return
        self._spend_today += cost
        today = time.strftime("%Y-%m-%d")
        spend_key = SPEND_KEY_FMT.format(role=self.role)
        self.bus.hset(spend_key, {today: f"{self._spend_today:.6f}"})
        self.bus.expire(spend_key, 60 * 60 * 24 * 7)
        if self._spend_today > self.daily_budget:
            self.engage_kill_switch(
                reason=f"daily_budget_exceeded ({self._spend_today:.2f}>{self.daily_budget:.2f})"
            )
            raise BudgetExceeded(
                f"{self.role}: spent {self._spend_today:.4f}, budget {self.daily_budget:.4f}"
            )

    @property
    def spend_today(self) -> float:
        return self._spend_today
