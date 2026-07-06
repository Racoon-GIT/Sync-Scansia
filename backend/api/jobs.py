"""In-memory, single-slot background job store (PURE STDLIB — no FastAPI).

Scansia Manager runs on a SINGLE Render instance; a live-inventory JOIN is a slow
fan-out of throttled Shopify calls that must NOT block the ASGI event loop. This
module owns the tiny state machine that the API layer drives:

* :class:`JobStore` — a ``threading.Lock``-guarded dict of :class:`JobRecord`. It
  is SINGLE-SLOT: :meth:`JobStore.create` refuses (raises :class:`JobBusyError`,
  the API maps it to HTTP 409) while a job is still ``queued``/``running``, so at
  most one join is ever in flight. The submit PATH honours the lock; the blocking
  work itself runs OFF the event loop (a worker thread — see
  ``backend.api.read``), and the async poll endpoint only ever reads a record.

* :class:`SynchronousExecutor` — an ``executor.submit``-compatible shim that runs
  the callable INLINE. Injected in tests so ``POST`` then ``GET`` is deterministic
  with no sleeps/threads; production injects a real
  ``concurrent.futures.ThreadPoolExecutor(max_workers=1)``.

EPHEMERAL BY DESIGN: the store is process memory only. A restart LOSES every
in-flight and completed job — acceptable because a join is idempotent and cheap to
recompute, and nothing durable depends on it. (When a MySQL adapter lands, this is
the one module that gets a durable backend; the API talks only to the methods
below.)
"""
from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Optional
from zoneinfo import ZoneInfo

_ROME = ZoneInfo("Europe/Rome")

# Job kinds (only inventory today; publish/delete/price apply arrive M3-M5).
JOB_KIND_INVENTORY = "inventory"

# Job lifecycle states.
JOB_QUEUED = "queued"
JOB_RUNNING = "running"
JOB_DONE = "done"
JOB_FAILED = "failed"

_TERMINAL = frozenset({JOB_DONE, JOB_FAILED})
_ACTIVE = frozenset({JOB_QUEUED, JOB_RUNNING})


class JobBusyError(RuntimeError):
    """Single slot occupied — a job is already queued/running. API -> HTTP 409."""

    def __init__(self, active_job_id: str) -> None:
        self.active_job_id = active_job_id
        super().__init__(f"a job is already in flight: {active_job_id}")


@dataclass
class JobRecord:
    """One background job. Mutated ONLY under the store lock (never shared raw)."""

    job_id: str
    kind: str
    status: str
    created_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    result: Optional[Any] = None       # populated on JOB_DONE (the join payload)
    error_code: Optional[str] = None   # populated on JOB_FAILED (stable code only)


class JobStore:
    """Thread-safe, single-slot, in-memory job registry."""

    def __init__(self, *, now: Optional[Callable[[], datetime]] = None) -> None:
        self._lock = threading.Lock()
        self._jobs: Dict[str, JobRecord] = {}
        self._active_id: Optional[str] = None
        self._now = now or (lambda: datetime.now(_ROME))

    def _ts(self) -> str:
        return self._now().isoformat()

    def create(self, kind: str) -> JobRecord:
        """Reserve the single slot and return a fresh ``queued`` record.

        Raises :class:`JobBusyError` if a job is still ``queued``/``running``.
        """
        with self._lock:
            if self._active_id is not None:
                active = self._jobs.get(self._active_id)
                if active is not None and active.status in _ACTIVE:
                    raise JobBusyError(self._active_id)
                # Prior slot holder already terminal -> free the slot.
                self._active_id = None
            rec = JobRecord(
                job_id=uuid.uuid4().hex,
                kind=kind,
                status=JOB_QUEUED,
                created_at=self._ts(),
            )
            self._jobs[rec.job_id] = rec
            self._active_id = rec.job_id
            return rec

    def get(self, job_id: str) -> Optional[JobRecord]:
        with self._lock:
            return self._jobs.get(job_id)

    def mark_running(self, job_id: str) -> None:
        with self._lock:
            rec = self._jobs.get(job_id)
            if rec is not None and rec.status == JOB_QUEUED:
                rec.status = JOB_RUNNING
                rec.started_at = self._ts()

    def mark_done(self, job_id: str, result: Any) -> None:
        with self._lock:
            rec = self._jobs.get(job_id)
            if rec is not None:
                rec.status = JOB_DONE
                rec.finished_at = self._ts()
                rec.result = result
            if self._active_id == job_id:
                self._active_id = None

    def mark_failed(self, job_id: str, error_code: str) -> None:
        with self._lock:
            rec = self._jobs.get(job_id)
            if rec is not None:
                rec.status = JOB_FAILED
                rec.finished_at = self._ts()
                rec.error_code = error_code
            if self._active_id == job_id:
                self._active_id = None


class SynchronousExecutor:
    """``executor.submit``-compatible shim that runs the callable INLINE.

    Test-only: it makes ``POST`` (submit) block until the job has finished so a
    following ``GET`` sees a terminal state deterministically. Production uses a
    real ``ThreadPoolExecutor(max_workers=1)``. Any exception the callable raises
    is swallowed here (the job runner is expected to have its own top-level guard
    that records ``JOB_FAILED``), so a bug never surfaces as a 500 on submit.
    """

    def submit(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        try:
            fn(*args, **kwargs)
        except Exception:  # noqa: BLE001 - mirror ThreadPoolExecutor: never re-raise into submit()
            pass

    def shutdown(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - no-op
        pass


def job_record_to_dict(rec: JobRecord, *, serialize_result: Optional[Callable[[Any], Any]] = None) -> dict:
    """Serialize a :class:`JobRecord` for the poll endpoint.

    ``result`` is passed through ``serialize_result`` when present (the inventory
    join installs one that turns its dataclasses into JSON-safe dicts); a
    ``None`` result (queued/running/failed) stays ``None``.
    """
    result = rec.result
    if result is not None and serialize_result is not None:
        result = serialize_result(result)
    return {
        "job_id": rec.job_id,
        "kind": rec.kind,
        "status": rec.status,
        "created_at": rec.created_at,
        "started_at": rec.started_at,
        "finished_at": rec.finished_at,
        "error_code": rec.error_code,
        "result": result,
    }


__all__ = [
    "JOB_KIND_INVENTORY",
    "JOB_QUEUED",
    "JOB_RUNNING",
    "JOB_DONE",
    "JOB_FAILED",
    "JobBusyError",
    "JobRecord",
    "JobStore",
    "SynchronousExecutor",
    "job_record_to_dict",
]
