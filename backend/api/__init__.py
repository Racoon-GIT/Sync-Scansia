"""backend.api: HTTP surface for Scansia Manager.

The COMPUTE core (``errors``, ``jobs``, ``inventory``) is pure stdlib + the
backend library — no FastAPI — so it imports and unit-tests on a bare interpreter.
The FastAPI ROUTER (``read``) is imported lazily/guarded: importing this package
never requires FastAPI, and ``build_read_router`` is ``None`` where it is absent
(the app module, which does need FastAPI, imports ``read`` directly).
"""
from __future__ import annotations

from backend.api.errors import ApiError, map_exception
from backend.api.inventory import (
    CHIP_IN_SCANSIA,
    CHIP_MISMATCH,
    CHIP_OVERSELL,
    CHIP_PUBLISHED,
    CHIP_SOLD_OUT,
    JoinResult,
    join_group,
    run_inventory_join,
)
from backend.api.jobs import (
    JobBusyError,
    JobRecord,
    JobStore,
    SynchronousExecutor,
)

try:  # FastAPI is optional at import time: only the router needs it.
    from backend.api.read import build_read_router
except ImportError:  # pragma: no cover - exercised only where FastAPI is absent
    build_read_router = None  # type: ignore[assignment]

__all__ = [
    "ApiError",
    "map_exception",
    "JobStore",
    "JobRecord",
    "JobBusyError",
    "SynchronousExecutor",
    "JoinResult",
    "join_group",
    "run_inventory_join",
    "CHIP_IN_SCANSIA",
    "CHIP_PUBLISHED",
    "CHIP_SOLD_OUT",
    "CHIP_MISMATCH",
    "CHIP_OVERSELL",
    "build_read_router",
]
