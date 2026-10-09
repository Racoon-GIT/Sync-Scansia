"""Storage-agnostic persistence Protocols — the PORTING CONTRACT.

These Protocols are the ONLY thing the services know about persistence. They
are deliberately structural (:class:`typing.Protocol`): an adapter satisfies them
by SHAPE, never by inheritance.

* :class:`RegisterPort` — the register ``racoon.sc_returned_pair`` +
  ``sc_returned_pair_event`` and F7's own ``sm_outlet_audit`` / ``sm_outlet_lease``,
  named for the operations the services need (plan
  ``docs/f7-mysql-adapter-plan.md`` §3.1), not for the sheet it replaces. Its
  implementation is :class:`backend.persistence.register.Register` over a
  :class:`RegisterStore`: MySQL in production, an in-memory fake in tests.
* :class:`RegisterStore` — the small SQL surface a ``Register`` composes: plain
  non-locking reads, the three lease statements, and ONE fenced-write primitive
  (renew first, a list of guarded ops, the register ``UPDATE`` last — §3.3 rule 6,
  §3.7). Two implementations, proven equal by the same contract tests.
* :class:`AuditSink` — the durable audit boundary of the price / delete / init
  flows (the structural superset of the per-service sink Protocols, plus the
  three methods the services already call: ``write_init_before``,
  ``write_event``, ``read_recent``).
* :class:`TokenService` — stateless confirm tokens (no storage).
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Protocol, Sequence, Tuple, runtime_checkable

from backend.persistence.records import (
    AuditRow,
    Fence,
    Hold,
    InsertAudit,
    LeaseRow,
    PairRow,
    PendingDeltas,
    EventRow,
    Snapshot,
    WriteOp,
)

if TYPE_CHECKING:  # annotations only: the services import the register, never the reverse at runtime
    from backend.services.baseline_service import InitBeforeSnapshot
    from backend.services.delete_service import BeforeSnapshot, DeleteOutcomeEvent
    from backend.services.pricing_service import PriceIntent


@runtime_checkable
class FenceHolder(Protocol):
    """A lease holder as the writes see it: the fence to renew, its own monotonic
    clock (the renew's START is read on it), and the hook called after a renew
    succeeded (it restarts the local deadline, §3.3 rule 6)."""

    @property
    def fence(self) -> Fence: ...

    def monotonic(self) -> float: ...

    def mark_renewed(self, started_monotonic: float) -> None: ...


@runtime_checkable
class RegisterStore(Protocol):
    """The SQL surface of the register (one handle = one connection, one job)."""

    # --- plain, non-locking reads (autocommit; §3.7 rule 2) ----------------
    def read_pairs(
        self,
        *,
        states: Optional[Sequence[str]] = ...,
        sku: Optional[str] = None,
        pair_ids: Optional[Sequence[int]] = None,
        outlet_product_id: Optional[int] = None,
    ) -> List[PairRow]: ...

    def read_unstamped_events(self) -> List[EventRow]: ...

    def read_audit(
        self,
        kind: str,
        *,
        statuses: Optional[Sequence[str]] = None,
        row_ids: Optional[Sequence[int]] = None,
        group_id: Optional[int] = None,
        product_id: Optional[int] = None,
        intent_id: Optional[str] = None,
        last: Optional[int] = None,
    ) -> List[AuditRow]: ...

    def read_lease(self) -> LeaseRow: ...

    def read_snapshot(self, key: Optional[Tuple[str, str]] = None) -> Snapshot:
        """One READ ONLY consistent-snapshot transaction (the stated exception
        to §3.7 rule 1), closed before returning: shelf pairs, every unstamped
        event, each key's max event id — all keys, or one."""
        ...

    def read_import_payloads(self, pair_ids: Sequence[int]) -> List[Tuple[int, Any]]:
        """``(pair_id, PAYLOAD)`` of the pairs' ``IMPORTED`` events (B-2's J seed)."""
        ...

    def server_now(self) -> datetime: ...

    # --- the lease: one guarded statement each, atomic on its own ----------
    def lease_take(self, holder: str, ttl_ms: int) -> Optional[int]: ...

    def lease_release(self, fence: Fence) -> bool: ...

    def lease_wait(self, holder: str, wait_ms: int) -> bool: ...

    # --- the one write primitive ------------------------------------------
    def fenced_write(self, fence: Fence, ops: Sequence[WriteOp]) -> List[int]: ...

    def close(self) -> None: ...


@runtime_checkable
class RegisterPort(Protocol):
    """The register as the services use it (plan §3.1)."""

    # --- reads ---------------------------------------------------------------
    def shelf(self, sku: Optional[str] = None) -> List[PairRow]: ...

    def pending_deltas(self) -> PendingDeltas: ...

    def held_keys(self) -> List[Hold]: ...

    def baseline(self) -> Optional[AuditRow]: ...

    def snapshot(self, key: Optional[Tuple[str, str]] = None) -> Snapshot: ...

    def import_payloads(self, pair_ids: Sequence[int]) -> List[Tuple[int, Any]]: ...

    # --- fenced writes (each one transaction, renew first) -------------------
    def stamp(
        self,
        lease: FenceHolder,
        event_ids: Sequence[int],
        *,
        close_rows: Sequence[int] = (),
        carrier: Optional[InsertAudit] = None,
    ) -> List[int]: ...

    def set_outlet_price(
        self,
        lease: FenceHolder,
        sku: str,
        price: Optional[Decimal],
        discount: Optional[Decimal],
        *,
        only_missing: bool = False,
        ref: Optional[str] = None,
    ) -> List[int]: ...

    def restore_outlet_price(
        self,
        lease: FenceHolder,
        pair_ids: Sequence[int],
        price: Optional[Decimal],
        discount: Optional[Decimal],
        *,
        ref: str,
    ) -> List[int]: ...

    def set_outlet_product(self, lease: FenceHolder, pair_ids: Sequence[int], product_id: int) -> None: ...

    def clear_outlet_product(self, lease: FenceHolder, product_id: int) -> List[int]: ...

    def set_online(self, lease: FenceHolder, pair_ids: Sequence[int], value: bool) -> None: ...

    def record_baseline(
        self,
        lease: FenceHolder,
        *,
        scope: str,
        action: str,
        payload: Dict[str, Any],
        stamp_ids: Sequence[int],
        plan_hash: Optional[str] = None,
    ) -> int: ...


@runtime_checkable
class AuditSink(Protocol):
    """Durable, append-only audit boundary (dependency-injected).

    Structural superset of the per-service sink Protocols. ``capture_before`` /
    ``load`` serve the price revert flow; ``write_durable`` / ``write_outcome``
    the delete flow; ``write_init_before`` / ``write_event`` the init flow;
    ``read_recent`` the ``GET /audit`` page.
    """

    # --- pricing side ------------------------------------------------------
    def capture_before(self, intent: PriceIntent) -> str:
        """Persist the price before-snapshot durably; return its ``intent_id``.

        MUST persist durably before returning the id. ANY exception is caught
        by the service (nothing is pushed) and surfaced as a bounded ERROR.
        """
        ...

    def load(self, intent_id: str) -> PriceIntent:
        """Return the :class:`PriceIntent` captured under ``intent_id`` (for revert)."""
        ...

    # --- delete side -------------------------------------------------------
    def write_durable(self, snapshot: BeforeSnapshot) -> Optional[int]:
        """Persist the delete before-snapshot; RAISE on any failure — the delete
        ABORT GATE: if it raises, ``productDelete`` is never reached. Returns the
        ``DELETE`` row's id (terminal path (iii) names it)."""
        ...

    def write_outcome(self, event: DeleteOutcomeEvent) -> None:
        """Append a post-delete outcome event. Best-effort at the call site."""
        ...

    # --- init side ---------------------------------------------------------
    def write_init_before(self, snapshot: InitBeforeSnapshot) -> None:
        """Persist the init before-snapshot BEFORE any Shopify write; RAISE on
        any failure — the init ABORT GATE."""
        ...

    def write_event(
        self, *, action: str, target_gids: str = "", plan_hash: str = "", result: str = ""
    ) -> None:
        """Append a generic audit event. Best-effort at the call site."""
        ...

    # --- read side ---------------------------------------------------------
    def read_recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        """The most recent audit events, oldest of the window first (READ-ONLY)."""
        ...


@runtime_checkable
class TokenService(Protocol):
    """Stateless confirm-token boundary: mint at preview, verify at apply.

    ``kind`` (post-review HARDENING) is the id of the calling vertical
    (``"publish"``/``"cleanup"``/``"prices"``/...), SIGNED into the token so a
    token minted for one vertical never verifies for another.
    """

    def mint(self, plan_hash: str, ttl_s: int, *, kind: str) -> str:
        """Return a signed confirm-token binding ``plan_hash`` + ``kind`` for
        ``ttl_s`` seconds."""
        ...

    def verify(self, token: str, plan_hash: str, *, kind: str) -> bool:
        """True iff ``token`` is a valid, unexpired signature for ``plan_hash``
        AND ``kind`` matches the signed claim."""
        ...


__all__ = ["FenceHolder", "RegisterStore", "RegisterPort", "AuditSink", "TokenService"]
