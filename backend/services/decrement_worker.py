"""B-3's decrement on its own worker (plan ``docs/f7-mysql-adapter-plan.md`` §6
B-3, decided D23: *event + working-window net*; §3.3 rule 6).

678434 calls the trigger right after a scansia sale's CONSUME, the working-window
net (a Scheduler job) calls it every 15 min, and the app runs one pass at boot —
a Free instance woken by the call, a person or a restart applies what is pending.
Every one of them only *triggers*: the pass reads ``pending_deltas()`` itself
(the register is the queue), so a duplicate or late trigger finds the events
stamped and writes nothing.

:class:`DecrementWorker` runs the pass on a thread of its own — never on the
jobs' single-worker executor (it would queue behind a whole publish), never on
the event loop. A trigger arriving while a pass runs coalesces: at most one more
pass follows the running one, however many triggers arrive. A failing pass
(lease not handed over, the register refusing before the baseline, a store or
Shopify error) is logged and kept as the last result, never raised to the caller.
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("backend.services.decrement_worker")


def _spawn_thread(target: Callable[[], None]) -> None:
    threading.Thread(target=target, name="scansia-decrement", daemon=True).start()


class DecrementWorker:
    """One decrement pass at a time, triggers coalesced (at most one waiting)."""

    def __init__(
        self,
        run_pass: Callable[[], Any],
        *,
        spawn: Callable[[Callable[[], None]], None] = _spawn_thread,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._run_pass = run_pass
        self._spawn = spawn
        self._clock = clock
        self._lock = threading.Lock()
        self._running = False
        self._again = False
        self.runs = 0
        self.last: Optional[Dict[str, Any]] = None

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    def trigger(self, source: str) -> Dict[str, Any]:
        """Start a pass now, or coalesce into the one after the running pass.
        Returns at once (the caller — 678434 with a 5 s timeout — never waits)."""
        with self._lock:
            if self._running:
                self._again = True
                return {"accepted": True, "coalesced": True}
            self._running = True
        logger.info("decrement triggered (%s)", source)
        try:
            self._spawn(self._loop)
        except Exception:
            with self._lock:               # no thread: the next trigger must be able to start one
                self._running = self._again = False
            raise
        return {"accepted": True, "coalesced": False}

    def _loop(self) -> None:
        while True:
            self._one()
            with self._lock:
                if not self._again:
                    self._running = False
                    return
                self._again = False

    def _one(self) -> None:
        started = datetime.now(timezone.utc).isoformat()
        t0 = self._clock()
        try:
            report = self._run_pass()
            as_dict = getattr(report, "as_dict", None)
            self.last = {"started_at": started, "outcome": "done",
                         "report": as_dict() if callable(as_dict) else None}
        except Exception as e:  # noqa: BLE001 - a failed pass is logged and kept, never raised
            logger.warning("decrement pass failed: %s", type(e).__name__)
            self.last = {"started_at": started, "outcome": "error", "error": type(e).__name__}
        self.last["seconds"] = round(self._clock() - t0, 3)
        self.runs += 1


def make_run_pass(state: Any) -> Callable[[], Any]:
    """The production pass: a fresh register handle (one connection per pass,
    §3.7 rule 4) and transport, B-3's ``decrement_pass``."""
    from backend.services import passes  # local import: the worker module stays light

    def run() -> Any:
        register = state.register_factory(actor="decrement")
        try:
            return passes.decrement_pass(register, state.transport_factory(), state.promo_location_id)
        finally:
            close = getattr(register, "close", None)
            if callable(close):
                close()

    return run


__all__ = ["DecrementWorker", "make_run_pass"]
