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
    caller can short-circuit the rest of the handler.

    Delivery modes:
    - default (`durable=False`): subscribe from the stream tail. Messages
      published while the agent is down are not seen. Unchanged behaviour.
    - `durable=True`: consume through a consumer group (group = `role`,
      consumer = `{role}-{pid}`). Messages published while the agent is
      down are delivered when it restarts, and each message is acked only
      after `handle_message` returns without raising. A handler exception
      leaves the message pending, so it is redelivered (after
      `claim_min_idle_ms`) to this or another consumer of the same role.
      Durable handlers must therefore be idempotent."""

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
        durable: bool = False,
        claim_min_idle_ms: int = 60_000,
    ):
        self.bus = bus or Bus.open()
        self.max_concurrent = max_concurrent
        self.block_ms = block_ms
        self.heartbeat_interval_s = heartbeat_interval_s
        self.kill_switch_path = kill_switch_path
        self.daily_budget = daily_budget
        self.durable = durable
        self.claim_min_idle_ms = claim_min_idle_ms
        self._stop = threading.Event()
        self._pool: ThreadPoolExecutor | None = None
        self._sem: threading.BoundedSemaphore | None = None
        self._active_tasks: dict[str, float] = {}
        self._active_lock = threading.Lock()
        self._spend_date: str = self._today()
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
            self._sem = threading.BoundedSemaphore(self.max_concurrent)

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
        if self.durable:
            source = self.bus.subscribe_group(
                self.streams,
                self.role,
                f"{self.role}-{os.getpid()}",
                block_ms=self.block_ms,
                min_idle_ms=self.claim_min_idle_ms,
                stop=self._stop,
            )
        else:
            last_ids = {s: "$" for s in self.streams}
            source = self.bus.subscribe(
                self.streams,
                last_ids=last_ids,
                block_ms=self.block_ms,
                stop=self._stop,
            )
        for stream, msg_id, fields in source:
            if self._stop.is_set():
                break
            if self._kill_switch_engaged():
                # Durable: the message stays pending (no ack) and is
                # redelivered once the kill switch clears.
                self._mark_status("paused")
                time.sleep(2)
                continue
            self._dispatch(stream, msg_id, fields)

    def _dispatch(self, stream: str, msg_id: str, fields: dict[str, str]) -> None:
        if self._pool is not None:
            # Bound in-flight work: block the read loop instead of letting
            # the executor queue grow without limit under a slow handler.
            while not self._sem.acquire(timeout=0.5):
                if self._stop.is_set():
                    return
            with self._active_lock:
                self._active_tasks[msg_id] = time.time()
            try:
                self._pool.submit(self._pooled_handle, stream, msg_id, fields)
            except BaseException:
                self._sem.release()
                raise
            return
        with self._active_lock:
            self._active_tasks[msg_id] = time.time()
        self._safe_handle(stream, msg_id, fields)

    def _pooled_handle(self, stream: str, msg_id: str, fields: dict[str, str]) -> None:
        try:
            self._safe_handle(stream, msg_id, fields)
        finally:
            self._sem.release()

    def _safe_handle(self, stream: str, msg_id: str, fields: dict[str, str]) -> None:
        try:
            self.handle_message(stream, msg_id, fields)
        except Exception as e:
            # Stay alive on individual handler errors. Log to status. No ack
            # in durable mode: the message stays pending for redelivery.
            self.bus.hset(
                STATUS_KEY_FMT.format(role=self.role),
                {"last_error": f"{type(e).__name__}: {e}", "last_error_ts": str(time.time())},
            )
        else:
            if self.durable:
                try:
                    self.bus.ack(stream, self.role, msg_id)
                except Exception:
                    # At-least-once: a failed ack means one redelivery, not
                    # a crashed agent.
                    pass
        finally:
            with self._active_lock:
                self._active_tasks.pop(msg_id, None)

    # ------------------------------------------------------------------
    def _heartbeat_loop(self) -> None:
        while not self._stop.wait(self.heartbeat_interval_s):
            with self._active_lock:
                active = len(self._active_tasks)
            try:
                self.bus.hset(
                    STATUS_KEY_FMT.format(role=self.role),
                    {
                        "last_seen": str(time.time()),
                        "active_tasks": str(active),
                        "pid": str(os.getpid()),
                    },
                )
            except Exception:
                # Transient bus error must not kill the heartbeat thread.
                continue

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
    @staticmethod
    def _today() -> str:
        return time.strftime("%Y-%m-%d")

    def _read_today_spend(self) -> float:
        raw = self.bus.hget(SPEND_KEY_FMT.format(role=self.role), self._today())
        try:
            return float(raw) if raw else 0.0
        except (TypeError, ValueError):
            return 0.0

    def track_spend(self, cost: float) -> None:
        """Add ``cost`` to today's running total. If the total goes past
        ``daily_budget`` (non-zero), the kill-switch is engaged and
        ``BudgetExceeded`` is raised.

        The total lives in a per-role hash ``agent:{role}:spend`` keyed by
        YYYY-MM-DD and is incremented atomically, so multiple processes
        running the same role share one budget. The counter resets at
        midnight (local time) even for long-running processes."""
        if self.daily_budget <= 0:
            return
        today = self._today()
        if today != self._spend_date:
            self._spend_date = today
        spend_key = SPEND_KEY_FMT.format(role=self.role)
        self._spend_today = self.bus.hincrby_float(spend_key, today, cost)
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
        if self._spend_date != self._today():
            self._spend_date = self._today()
            self._spend_today = self._read_today_spend()
        return self._spend_today
