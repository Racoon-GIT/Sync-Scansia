"""The audit boundary on the register — :class:`~backend.persistence.ports.AuditSink`
over ``sm_outlet_audit`` (plan ``docs/f7-mysql-adapter-plan.md`` §3.1, §3.6).

Replaces the sheet tabs (``AUDIT``, ``AUDIT_DELETE``, ``AUDIT_PRICE``,
``AUDIT_INIT`` — E7: never created on the production sheet, so nothing migrates):

* ``capture_before`` → a ``PRICE`` row keyed by a UNIQUE ``INTENT_ID``; ``load``
  is a unique-key lookup (today's "first matching row wins" goes, §3.5);
* ``write_durable`` → a ``DELETE`` row: the delete's ABORT GATE — any failure
  raises, so ``productDelete`` is never reached without its snapshot;
* ``write_init_before`` → an ``INIT_BEFORE`` row: the init's abort gate;
* ``write_outcome`` / ``write_event`` → ``AUDIT`` rows (best-effort at the call
  site); ``read_recent`` reads them back for ``GET /audit``.

Every write is an F7 write to the database, so it runs fenced under the pass
lease of the flow that writes (§3.3 rule 6): the sink is bound to that holder's
lease (``lease=`` or :meth:`bind`) and refuses to write unbound. ``read_recent``
needs no lease. Timestamps shown to a person are Europe/Rome (the stored
``CREATED_AT`` is UTC).
"""
from __future__ import annotations

import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from backend.persistence.records import (
    KIND_AUDIT,
    KIND_DELETE,
    KIND_INIT_BEFORE,
    KIND_PRICE,
    InsertAudit,
    StoreError,
)
from backend.services.delete_service import BeforeSnapshot, DeleteOutcomeEvent
from backend.services.baseline_service import InitBeforeSnapshot
from backend.services.pricing_service import PriceIntent, ProductPrior, VariantPrior

_ROME = ZoneInfo("Europe/Rome")
_PRODUCT_GID_PREFIX = "gid://shopify/Product/"


class AuditWriteUnfenced(StoreError):
    """An audit write with no pass lease bound — refused (every F7 write is fenced)."""


class IntentNotFound(StoreError):
    """No ``PRICE`` capture row carries that ``INTENT_ID``."""


def _product_number(gid: Optional[str]) -> Optional[int]:
    if gid and gid.startswith(_PRODUCT_GID_PREFIX) and gid[len(_PRODUCT_GID_PREFIX):].isdigit():
        return int(gid[len(_PRODUCT_GID_PREFIX):])
    return None


def _rome(ts: datetime) -> str:
    return ts.replace(tzinfo=timezone.utc).astimezone(_ROME).isoformat()


class RegisterAuditSink:
    """:class:`~backend.persistence.ports.AuditSink` on ``sm_outlet_audit``."""

    def __init__(self, register: Any, *, actor: Optional[str] = None, lease: Any = None) -> None:
        self._register = register
        self._actor = (actor or "").strip() or "unknown"
        self._lease = lease

    def bind(self, lease: Any) -> "RegisterAuditSink":
        """Bind the pass lease of the flow that will write through this sink."""
        self._lease = lease
        return self

    def _insert(self, row: InsertAudit) -> int:
        if self._lease is None:
            raise AuditWriteUnfenced("audit write outside the pass lease")
        return self._register.insert_audit(self._lease, row)

    # -- pricing side -----------------------------------------------------------
    def capture_before(self, intent: PriceIntent) -> str:
        intent_id = uuid.uuid4().hex  # 32 hex = INTENT_ID CHAR(32), UNIQUE
        self._insert(InsertAudit(
            kind=KIND_PRICE, actor=self._actor, action="capture_before", intent_id=intent_id,
            plan_hash=intent.plan_hash,
            payload={"mode": intent.mode, "priors": [asdict(p) for p in intent.priors]},
        ))
        return intent_id

    def load(self, intent_id: str) -> PriceIntent:
        rows = self._register.audit_rows(KIND_PRICE, intent_id=intent_id)
        if len(rows) != 1:
            raise IntentNotFound("price intent not found")
        row = rows[0]
        p = row.payload or {}
        priors = tuple(
            ProductPrior(
                product_gid=x["product_gid"], sku=x["sku"],
                variants=tuple(VariantPrior(v["variant_id"], v.get("price"), v.get("compare_at"))
                               for v in x.get("variants") or ()),
            )
            for x in p.get("priors") or ()
        )
        return PriceIntent(mode=p.get("mode") or "", plan_hash=(row.plan_hash or "").strip(), priors=priors)

    # -- delete side ------------------------------------------------------------
    def write_durable(self, snapshot: BeforeSnapshot) -> int:
        """The ``DELETE`` row (the abort gate); returns its id."""
        try:
            payload = asdict(snapshot)
        except TypeError as e:
            raise StoreError("delete snapshot is not serialisable") from e
        return self._insert(InsertAudit(
            kind=KIND_DELETE, actor=self._actor, action="before_snapshot",
            product_id=_product_number(snapshot.product_gid), payload=payload,
        ))

    def write_outcome(self, event: DeleteOutcomeEvent) -> None:
        self._insert(InsertAudit(
            kind=KIND_AUDIT, actor=self._actor, action="product_delete",
            product_id=_product_number(event.product_gid),
            payload={"target_gids": event.product_gid, "result": event.status,
                     "deleted_id": event.deleted_id},
        ))

    # -- init side ----------------------------------------------------------------
    def write_init_before(self, snapshot: InitBeforeSnapshot) -> None:
        try:
            payload = asdict(snapshot)
        except TypeError as e:
            raise StoreError("init snapshot is not serialisable") from e
        self._insert(InsertAudit(
            kind=KIND_INIT_BEFORE, actor=self._actor, action="init_before",
            plan_hash=snapshot.plan_hash or None, payload=payload,
        ))

    def write_event(
        self, *, action: str, target_gids: str = "", plan_hash: str = "", result: str = ""
    ) -> None:
        self._insert(InsertAudit(
            kind=KIND_AUDIT, actor=self._actor, action=action, plan_hash=plan_hash or None,
            payload={"target_gids": target_gids, "result": result},
        ))

    # -- read side ------------------------------------------------------------------
    def read_recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        last = int(limit) if limit and int(limit) > 0 else None  # 0 / negative: every row
        rows = self._register.audit_rows(KIND_AUDIT, last=last)
        return [
            {
                "ts": _rome(r.created_at),
                "actor": r.actor,
                "action": r.action,
                "target_gids": str((r.payload or {}).get("target_gids") or ""),
                "plan_hash": (r.plan_hash or "").strip(),
                "result": str((r.payload or {}).get("result") or ""),
            }
            for r in rows
        ]


__all__ = ["RegisterAuditSink", "AuditWriteUnfenced", "IntentNotFound"]
