"""PUBLISH vertical — the outlet publish/apply lifecycle behind the shared
preview-job / apply-job / confirm-token pattern (:mod:`backend.api.mutations`).

Wires :func:`backend.services.outlet_service.publish_preview` /
:func:`~backend.services.outlet_service.publish_apply` to four auth-gated routes:

* ``POST /outlet/publish/preview``           — enqueue the READ-ONLY plan job.
* ``GET  /outlet/publish/preview/{job_id}``  — poll: ``{plan, plan_hash, confirm_token}``.
* ``POST /outlet/publish/apply``             — confirm-gated (``{plan_hash, confirm_token}``).
* ``GET  /outlet/publish/apply/{job_id}``    — poll: apply outcomes (or VERIFY_FAILED).

Publish has NO audit sink and NO human-gesture gate (only ``mark_reconciled`` on
the sheet, done inside the service): the spec leaves ``make_audit`` /
``make_gesture`` unset. The aggregate ``plan_hash`` is derived from the per-SKU
``PlanAction.plan_hash`` values (the outlet service's own TOCTOU keys), sorted so
the hash is order-independent; ``publish_apply`` still re-verifies each SKU against
its own plan_hash internally, so this aggregate is the coarse batch-level drift
gate on top of that per-action guard.
"""
from __future__ import annotations

import hashlib
from typing import Any, Dict, List

from backend.api.mutations import MutationVertical, build_mutation_router
from backend.services import outlet_service

PREFIX = "/outlet/publish"
KIND_PREVIEW = "publish_preview"
KIND_APPLY = "publish_apply"


# =============================================================================
# Aggregate plan_hash — the batch-level TOCTOU key the confirm-token binds.
# =============================================================================
def publish_plan_hash(plan: Any) -> str:
    """Order-independent sha256 (16 hex) over the per-SKU ``PlanAction.plan_hash``.

    Each action already carries a ``_plan_hash(anchor, branch, live_status)`` that
    binds its live state; aggregating the sorted ``sku:plan_hash`` pairs yields a
    single batch key that changes iff any action's live state (or the action set)
    changed between preview and apply.
    """
    parts = sorted(f"{a.sku}:{a.plan_hash}" for a in plan.actions)
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


# =============================================================================
# JSON-safe serializers (explicit — no internal-only fields, JSON-safe scalars).
# =============================================================================
def _size_target_to_dict(st: Any) -> Dict[str, Any]:
    return {
        "raw_size": st.raw_size,
        "norm_size": st.norm_size,
        "row_uuids": list(st.row_uuids),
        "delta": st.delta,
        "frozen_pre": st.frozen_pre,
        "matched": st.matched,
        "variant_id": st.variant_id,
        "inventory_item_id": st.inventory_item_id,
    }


def _plan_action_to_dict(a: Any) -> Dict[str, Any]:
    return {
        "sku": a.sku,
        "branch": a.branch,
        "publishable": a.publishable,
        "reason": a.reason,
        "target_gid": a.target_gid,
        "source_gid": a.source_gid,
        "outlet_title": a.outlet_title,
        "live_status": a.live_status,
        "price": a.price,
        "compare_at": a.compare_at,
        "price_ok": a.price_ok,
        "size_targets": [_size_target_to_dict(st) for st in a.size_targets],
        "unmatched_sizes": list(a.unmatched_sizes),
        "warnings": list(a.warnings),
        "plan_hash": a.plan_hash,
    }


def serialize_plan(plan: Any) -> Dict[str, Any]:
    return {
        "dry_run": plan.dry_run,
        "actions": [_plan_action_to_dict(a) for a in plan.actions],
        "anomalies": list(plan.anomalies),
    }


def _outcome_to_dict(o: Any) -> Dict[str, Any]:
    return {
        "sku": o.sku,
        "branch": o.branch,
        "status": o.status,
        "target_gid": o.target_gid,
        "warnings": list(o.warnings),
        "reconciled_uuids": list(o.reconciled_uuids),
    }


def serialize_report(report: Any) -> Dict[str, Any]:
    return {"outcomes": [_outcome_to_dict(o) for o in report.outcomes]}


# =============================================================================
# Service closures over app.state (reached via the module so tests can monkeypatch
# outlet_service.publish_preview / publish_apply).
# =============================================================================
def _make_preview(state: Any):
    promo = state.promo_location_id

    def _preview(sheet: Any, transport: Any) -> Any:
        return outlet_service.publish_preview(sheet, transport, promo_location_id=promo)

    return _preview


def _make_apply(state: Any):
    promo = state.promo_location_id

    def _apply(sheet: Any, transport: Any, approved_plan: Any, audit_sink: Any) -> Any:
        # publish has no audit sink — the injected sink (always None here) is ignored.
        return outlet_service.publish_apply(
            sheet, transport, approved_plan, promo_location_id=promo
        )

    return _apply


PUBLISH_VERTICAL = MutationVertical(
    prefix=PREFIX,
    preview_kind=KIND_PREVIEW,
    apply_kind=KIND_APPLY,
    make_preview=_make_preview,
    make_apply=_make_apply,
    plan_hash_fn=publish_plan_hash,
    serialize_plan=serialize_plan,
    serialize_report=serialize_report,
    token_kind="publish",
)


def build_publish_router():
    """Construct the PUBLISH router (mount under the app's auth gate)."""
    return build_mutation_router(PUBLISH_VERTICAL)


__all__ = [
    "PREFIX",
    "PUBLISH_VERTICAL",
    "publish_plan_hash",
    "serialize_plan",
    "serialize_report",
    "build_publish_router",
]
