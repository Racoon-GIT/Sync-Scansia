"""Storage-agnostic persistence Protocols — the PORTING CONTRACT.

These Protocols are the ONLY thing the confirm-gated services
(:mod:`backend.services.pricing_service`, :mod:`backend.services.delete_service`)
know about persistence. They are deliberately structural
(:class:`typing.Protocol`): an adapter satisfies them by SHAPE, never by
inheritance, so swapping today's sheet-centric implementation for a MySQL one
later is a NEW adapter module and ZERO change to the services.

:class:`AuditSink` is the STRUCTURAL SUPERSET of the two per-service sink
Protocols declared inline in the services:

* ``pricing_service.AuditSink``      -> ``capture_before`` + ``load``
* ``delete_service.DeleteAuditSink`` -> ``write_durable`` + ``write_outcome``

so ONE concrete object (:class:`~backend.persistence.gsheet_audit.GSheetAuditSink`)
can be injected into BOTH the price and the delete flows.

Note: ``load`` is part of this contract even though the delete flow never calls
it — the price *revert* flow (:func:`pricing_service.revert_prices`) does, and a
superset of ``pricing_service.AuditSink`` must include it. Any concrete adapter
that omits ``load`` would fail revert at runtime.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from backend.services.delete_service import BeforeSnapshot, DeleteOutcomeEvent
from backend.services.pricing_service import PriceIntent


@runtime_checkable
class AuditSink(Protocol):
    """Durable, append-only audit boundary (dependency-injected).

    Structural superset of the two inline service sink Protocols. The
    ``capture_before`` / ``load`` pair serves the price revert flow;
    ``write_durable`` / ``write_outcome`` serve the delete flow.
    """

    # --- pricing side ------------------------------------------------------
    def capture_before(self, intent: PriceIntent) -> str:
        """Persist the price before-snapshot durably; return its ``intent_id``.

        Contract (see :func:`pricing_service.prices_apply`): MUST persist durably
        before returning the id. ANY exception is caught by the service (nothing
        is pushed, no sheet cell is written) and surfaced as a bounded ERROR
        outcome — so raising here is safe, never propagated raw.
        """
        ...

    def load(self, intent_id: str) -> PriceIntent:
        """Return the :class:`PriceIntent` captured under ``intent_id`` (for revert)."""
        ...

    # --- delete side -------------------------------------------------------
    def write_durable(self, snapshot: BeforeSnapshot) -> None:
        """Persist the delete before-snapshot; RAISE on any failure.

        This raise is the delete ABORT GATE: if it raises, ``productDelete`` is
        never reached for that outlet (``SNAPSHOT_ABORTED``). The design target is
        TWO durable sinks; a single-sink adapter documents its reduced guarantee.
        """
        ...

    def write_outcome(self, event: DeleteOutcomeEvent) -> None:
        """Append a post-delete outcome event. Best-effort at the call site.

        The delete service wraps this call and swallows/logs any exception — a
        committed delete is never blocked by an outcome-log failure.
        """
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


__all__ = ["AuditSink", "TokenService"]
