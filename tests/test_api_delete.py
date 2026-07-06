"""DELETE vertical — shared read-only/mutation workers + FastAPI TestClient tier.

Two tiers, mirroring ``test_api_publish``:

* WORKER tier (fastapi import only; no TestClient/network): the generic
  ``_run_readonly_job`` / ``_run_mutation_job`` and the ``token_binding_fn`` path
  of ``_run_preview_job`` in ``backend.api.mutations``, with pure fakes + the real
  ``JobStore``.
* TestClient tier (``importorskip`` fastapi+httpx — SKIPPED where the web deps are
  absent; runs on CI/deploy): the delete routes end-to-end with Basic Auth, the
  REAL HMAC confirm-token, and ``delete_service`` mocked so nothing touches Shopify
  or the sheet.

Safety-critical assertions: cleanup/apply WITHOUT ``CONFERMO`` or with a wrong
count -> 409 and ``cleanup_apply`` is NEVER called; a bad/expired token -> 409;
LIVE drift -> ``VERIFY_FAILED`` with no apply; the snapshot->delete gate is owned
by the service (the endpoint surfaces a ``SNAPSHOT_ABORTED`` verbatim, never
bypasses it).
"""
from __future__ import annotations

import base64
from types import SimpleNamespace
from typing import Any, List, Optional

import pytest

from backend.services.delete_service import (
    CONFIRM_WORD,
    STATUS_DELETED,
    STATUS_SNAPSHOT_ABORTED,
    CandidateOutlet,
    CleanupCandidate,
    CleanupPlan,
    CleanupReport,
    DeleteOutcome,
    PromoAnchorError,
    ReviewOutlet,
    SingleDeleteNotOutletError,
    ZeroStockReport,
)

PROMO = "gid://shopify/Location/PROMO"
_SECRET = "unit-test-signing-secret"


# ---------------------------------------------------------------------------
# Fakes + builders
# ---------------------------------------------------------------------------
class FakeSheet:
    """Opaque sheet handle exposing ``.ws.spreadsheet`` for the audit sink builder."""

    def __init__(self) -> None:
        self.ws = SimpleNamespace(spreadsheet=SimpleNamespace())


class FakeTransport:
    """Opaque transport handle — the service is mocked."""


def _cleanup_plan(plan_hash: str = "ph1raw", count: int = 3,
                  requires_second: bool = False, threshold: int = 25) -> CleanupPlan:
    cands = tuple(
        CleanupCandidate(f"gid://shopify/Product/{i}", f"Outlet {i}", "DRAFT")
        for i in range(count)
    )
    return CleanupPlan(
        dry_run=True, candidates=cands, review=(), count=count, threshold=threshold,
        archive_first=False, requires_second_confirm=requires_second, plan_hash=plan_hash,
    )


def _cleanup_report(status: str = STATUS_DELETED, deleted: int = 3) -> CleanupReport:
    outcomes = tuple(
        DeleteOutcome(f"gid://shopify/Product/{i}", status,
                      deleted_id=(f"gid://shopify/Product/{i}" if status == STATUS_DELETED else None))
        for i in range(3)
    )
    return CleanupReport(dry_run=False, verify_failed=False, plan_hash="ph1raw",
                         deleted=deleted, outcomes=outcomes)


def _zero_stock() -> ZeroStockReport:
    return ZeroStockReport(
        scanned=5,
        candidates=(CandidateOutlet("gid://shopify/Product/1", "Outlet 1", "DRAFT", ()),),
        review=(ReviewOutlet("gid://shopify/Product/2", "Outlet 2", "ACTIVE", ("promo_committed",)),),
        in_stock=3,
    )


class _MockDeleteService:
    """Records calls; ``plan`` is mutable to simulate LIVE drift preview->apply."""

    def __init__(self, *, plan: CleanupPlan, report: CleanupReport) -> None:
        self.plan = plan
        self.report = report
        self.zero = _zero_stock()
        self.single_outcome = DeleteOutcome("gid://shopify/Product/9", STATUS_DELETED,
                                            deleted_id="gid://shopify/Product/9")
        self.deny_count = 7
        self.preview_calls = 0
        self.apply_calls = 0
        self.single_calls = 0
        self.deny_calls = 0
        self.outlet_gate_calls = 0
        self.target_is_outlet = True   # default: pass the HARDENING gate

    def require_single_delete_target_is_outlet(self, transport: Any, product_gid: str) -> None:
        self.outlet_gate_calls += 1
        if not self.target_is_outlet:
            raise SingleDeleteNotOutletError(f"not an outlet: {product_gid}")

    def zero_stock_candidates(self, transport: Any, *, promo_location_id: str) -> ZeroStockReport:
        return self.zero

    def cleanup_preview(self, transport: Any, *, promo_location_id: str,
                        threshold: int = 25, archive_first: bool = False) -> CleanupPlan:
        self.preview_calls += 1
        return self.plan

    def cleanup_apply(self, transport: Any, sheet: Any, audit_sink: Any, approved_plan: Any,
                      **kwargs: Any) -> CleanupReport:
        self.apply_calls += 1
        return self.report

    def delete_single_apply(self, transport: Any, sheet: Any, audit_sink: Any,
                            product_gid: str, **kwargs: Any) -> DeleteOutcome:
        self.single_calls += 1
        return self.single_outcome

    def deny_normalize(self, transport: Any, product_gid: str) -> int:
        self.deny_calls += 1
        return self.deny_count


# ===========================================================================
# WORKER tier — generic read-only / mutation / preview workers
# ===========================================================================
def test_readonly_job_happy_runs_work_and_serializes():
    pytest.importorskip("fastapi")
    from backend.api.jobs import JobStore
    from backend.api.mutations import _run_readonly_job

    store = JobStore()
    rec = store.create("x_ro")
    _run_readonly_job(
        store, rec.job_id, lambda: FakeTransport(),
        lambda transport: {"scanned": 5}, lambda r: r,
    )
    got = store.get(rec.job_id)
    assert got.status == "done" and got.result == {"scanned": 5}


def test_readonly_job_maps_promo_anchor_error_to_stable_code():
    """A PromoAnchorError from the work_fn -> JOB_FAILED with a STABLE code."""
    pytest.importorskip("fastapi")
    from backend.api.jobs import JobStore
    from backend.api.mutations import _run_readonly_job

    store = JobStore()
    rec = store.create("x_ro")

    def boom(transport):
        raise PromoAnchorError("PROMO_LOCATION_ID missing")

    _run_readonly_job(store, rec.job_id, lambda: FakeTransport(), boom, lambda r: r)
    got = store.get(rec.job_id)
    assert got.status == "failed"
    assert got.error_code == "promo_anchor_missing"   # never internal_error / raw message
    assert got.result is None


def test_mutation_job_builds_audit_sink_and_calls_service():
    pytest.importorskip("fastapi")
    from backend.api.jobs import JobStore
    from backend.api.mutations import _run_mutation_job

    store = JobStore()
    rec = store.create("x_mut")
    seen: List[Any] = []

    def mutate(sheet, transport, audit_sink):
        seen.append(audit_sink)
        return {"ok": 1}

    _run_mutation_job(
        store, rec.job_id, lambda: FakeSheet(), lambda: FakeTransport(),
        lambda sheet: "SINK", mutate, lambda r: r,
    )
    got = store.get(rec.job_id)
    assert got.status == "done" and got.result == {"ok": 1}
    assert seen == ["SINK"]                            # actor-bound sink threaded in


def test_mutation_job_no_audit_builder_passes_none():
    pytest.importorskip("fastapi")
    from backend.api.jobs import JobStore
    from backend.api.mutations import _run_mutation_job

    store = JobStore()
    rec = store.create("x_mut")
    seen: List[Any] = []
    _run_mutation_job(
        store, rec.job_id, lambda: FakeSheet(), lambda: FakeTransport(),
        None, lambda s, t, sink: seen.append(sink) or {"n": 1}, lambda r: r,
    )
    assert seen == [None]


def test_preview_worker_token_binds_composite_but_stores_raw_hash():
    """token_binding_fn decouples the token binding from the stored plan_hash."""
    pytest.importorskip("fastapi")
    from backend.api.jobs import JobStore
    from backend.api.mutations import _run_preview_job

    class FakeToken:
        def mint(self, plan_hash: str, ttl_s: int, *, kind: str) -> str:
            return f"tok:{plan_hash}"

    store = JobStore()
    rec = store.create("x_prev")
    plan = SimpleNamespace(plan_hash="RAW", count=3)
    _run_preview_job(
        store, rec.job_id, lambda: FakeSheet(), lambda: FakeTransport(),
        lambda s, t: plan, lambda p: p.plan_hash, lambda p: {"plan_hash": p.plan_hash},
        FakeToken(), "cleanup", 900, lambda p: {"count": p.count},
        lambda p: f"{p.plan_hash}#{p.count}",
    )
    res = store.get(rec.job_id).result
    assert res["plan_hash"] == "RAW"                   # stored = raw TOCTOU key
    assert res["count"] == 3
    assert res["confirm_token"] == "tok:RAW#3"         # token bound to composite


# ===========================================================================
# TestClient tier — RUNS ONLY where fastapi/httpx are installed (importorskip).
# ===========================================================================
def _basic(user: str, pw: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{pw}".encode("utf-8")).decode("ascii")


def _auth():
    return {"Authorization": _basic("racoon", "s3cret-pw")}


def _make_client(monkeypatch, svc: _MockDeleteService, *, executor: Any = None):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from backend.app import create_app
    from backend.config import ShopifyConfig
    from backend.services import delete_service

    monkeypatch.delenv("APP_USERNAME", raising=False)
    monkeypatch.setenv("APP_PASSWORD", "s3cret-pw")
    monkeypatch.setenv("TOKEN_SIGNING_SECRET", _SECRET)

    for name in ("zero_stock_candidates", "cleanup_preview", "cleanup_apply",
                 "delete_single_apply", "deny_normalize",
                 "require_single_delete_target_is_outlet"):
        monkeypatch.setattr(delete_service, name, getattr(svc, name))

    cfg = ShopifyConfig("t.myshopify.com", "shpat_x", "2025-07", PROMO)
    from backend.api.jobs import SynchronousExecutor

    app = create_app(
        config=cfg,
        sheet_factory=lambda: FakeSheet(),
        transport_factory=lambda: FakeTransport(),
        audit_factory=lambda: None,
        executor=executor or SynchronousExecutor(),
        promo_location_id=PROMO,
    )
    return TestClient(app)


def _cleanup_preview_and_get(client) -> dict:
    sub = client.post("/outlet/cleanup/preview", headers=_auth(), json={})
    assert sub.status_code == 202
    job_id = sub.json()["job_id"]
    poll = client.get(f"/outlet/cleanup/preview/{job_id}", headers=_auth())
    assert poll.status_code == 200
    data = poll.json()
    assert data["status"] == "done"
    return data["result"]


# --- zero-stock ------------------------------------------------------------
def test_zero_stock_returns_candidates_and_review(monkeypatch):
    svc = _MockDeleteService(plan=_cleanup_plan(), report=_cleanup_report())
    client = _make_client(monkeypatch, svc)
    sub = client.post("/outlet/zero-stock", headers=_auth())
    assert sub.status_code == 202
    job_id = sub.json()["job_id"]
    res = client.get(f"/outlet/zero-stock/{job_id}", headers=_auth()).json()["result"]
    assert res["candidate_count"] == 1
    assert res["candidates"][0]["product_gid"] == "gid://shopify/Product/1"
    assert res["review"][0]["reasons"] == ["promo_committed"]


# --- cleanup preview -------------------------------------------------------
def test_cleanup_preview_returns_plan_hash_count_and_token(monkeypatch):
    svc = _MockDeleteService(plan=_cleanup_plan(plan_hash="ph1raw", count=3), report=_cleanup_report())
    client = _make_client(monkeypatch, svc)
    res = _cleanup_preview_and_get(client)
    assert res["plan_hash"] == "ph1raw"
    assert res["count"] == 3
    assert res["requires_second_confirm"] is False
    assert res["confirm_token"]
    assert svc.apply_calls == 0


# --- cleanup apply: happy --------------------------------------------------
def test_cleanup_apply_valid_gesture_and_token_executes(monkeypatch):
    svc = _MockDeleteService(plan=_cleanup_plan(plan_hash="ph1raw", count=3), report=_cleanup_report())
    client = _make_client(monkeypatch, svc)
    res = _cleanup_preview_and_get(client)
    ap = client.post("/outlet/cleanup/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"],
        "confirm": "CONFERMO", "count": 3,
    })
    assert ap.status_code == 202
    out = client.get(f"/outlet/cleanup/apply/{ap.json()['job_id']}", headers=_auth()).json()["result"]
    assert out["status"] == "APPLIED" and out["applied"] is True
    assert out["report"]["deleted"] == 3
    assert svc.apply_calls == 1


# --- cleanup apply: gesture / token failures (NO delete) -------------------
def test_cleanup_apply_without_confermo_is_409_and_skips_apply(monkeypatch):
    svc = _MockDeleteService(plan=_cleanup_plan(plan_hash="ph1raw", count=3), report=_cleanup_report())
    client = _make_client(monkeypatch, svc)
    res = _cleanup_preview_and_get(client)
    ap = client.post("/outlet/cleanup/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"], "count": 3,
    })
    assert ap.status_code == 409
    assert ap.json()["error"]["code"] == "gesture_required"
    assert svc.apply_calls == 0


def test_cleanup_apply_wrong_count_is_409_and_skips_apply(monkeypatch):
    """A mistyped count breaks the plan_hash#count token binding -> 409, no delete."""
    svc = _MockDeleteService(plan=_cleanup_plan(plan_hash="ph1raw", count=3), report=_cleanup_report())
    client = _make_client(monkeypatch, svc)
    res = _cleanup_preview_and_get(client)
    ap = client.post("/outlet/cleanup/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"],
        "confirm": "CONFERMO", "count": 4,   # wrong
    })
    assert ap.status_code == 409
    assert ap.json()["error"]["code"] == "confirm_invalid"
    assert svc.apply_calls == 0


def test_cleanup_apply_bad_token_is_409_and_skips_apply(monkeypatch):
    svc = _MockDeleteService(plan=_cleanup_plan(plan_hash="ph1raw", count=3), report=_cleanup_report())
    client = _make_client(monkeypatch, svc)
    res = _cleanup_preview_and_get(client)
    ap = client.post("/outlet/cleanup/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": "not-a-token",
        "confirm": "CONFERMO", "count": 3,
    })
    assert ap.status_code == 409
    assert ap.json()["error"]["code"] == "confirm_invalid"
    assert svc.apply_calls == 0


def test_cleanup_apply_drift_is_verify_failed_and_skips_apply(monkeypatch):
    svc = _MockDeleteService(plan=_cleanup_plan(plan_hash="ph1raw", count=3), report=_cleanup_report())
    client = _make_client(monkeypatch, svc)
    res = _cleanup_preview_and_get(client)
    # LIVE state moves: the apply re-preview yields a different raw plan_hash while
    # the token stays valid for the ORIGINAL submitted hash+count.
    svc.plan = _cleanup_plan(plan_hash="ph2raw", count=3)
    ap = client.post("/outlet/cleanup/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"],
        "confirm": "CONFERMO", "count": 3,
    })
    assert ap.status_code == 202
    out = client.get(f"/outlet/cleanup/apply/{ap.json()['job_id']}", headers=_auth()).json()["result"]
    assert out["status"] == "VERIFY_FAILED" and out["applied"] is False
    assert svc.apply_calls == 0


def test_cleanup_apply_over_cap_without_second_confirm_is_409(monkeypatch):
    svc = _MockDeleteService(plan=_cleanup_plan(plan_hash="phBIG", count=30, requires_second=True),
                             report=_cleanup_report())
    client = _make_client(monkeypatch, svc)
    res = _cleanup_preview_and_get(client)
    assert res["requires_second_confirm"] is True
    ap = client.post("/outlet/cleanup/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"],
        "confirm": "CONFERMO", "count": 30,   # 30 > cap(25), no second_confirm
    })
    assert ap.status_code == 409
    assert ap.json()["error"]["code"] == "gesture_required"
    assert svc.apply_calls == 0


def test_cleanup_apply_raised_threshold_cannot_skip_second_confirm(monkeypatch):
    """HARDENING (post-review): the second_confirm requirement is SIGNED into the
    preview token (plan_hash#count#requires_second_confirm) — an operator cannot
    inflate `threshold` at apply time to silently skip the speed-bump for a count
    that genuinely exceeded the threshold reviewed at preview."""
    svc = _MockDeleteService(
        plan=_cleanup_plan(plan_hash="ph1raw", count=30, requires_second=True, threshold=25),
        report=_cleanup_report(),
    )
    client = _make_client(monkeypatch, svc)
    res = _cleanup_preview_and_get(client)
    assert res["requires_second_confirm"] is True
    ap = client.post("/outlet/cleanup/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"],
        "confirm": "CONFERMO", "count": 30,
        "threshold": 50,   # raised: would (incorrectly) yield cap=50 >= count
    })
    assert ap.status_code == 409
    assert ap.json()["error"]["code"] == "gesture_required"
    assert svc.apply_calls == 0


def test_cleanup_apply_surfaces_service_snapshot_gate_verbatim(monkeypatch):
    """The endpoint never bypasses the service's snapshot->delete gate: a
    SNAPSHOT_ABORTED outcome (write_durable raised, no delete) is surfaced as-is."""
    svc = _MockDeleteService(
        plan=_cleanup_plan(plan_hash="ph1raw", count=3),
        report=_cleanup_report(status=STATUS_SNAPSHOT_ABORTED, deleted=0),
    )
    client = _make_client(monkeypatch, svc)
    res = _cleanup_preview_and_get(client)
    ap = client.post("/outlet/cleanup/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"],
        "confirm": "CONFERMO", "count": 3,
    })
    out = client.get(f"/outlet/cleanup/apply/{ap.json()['job_id']}", headers=_auth()).json()["result"]
    assert out["report"]["deleted"] == 0
    assert out["report"]["outcomes"][0]["status"] == STATUS_SNAPSHOT_ABORTED
    assert svc.apply_calls == 1   # only the gated service ran; endpoint issued no delete


# --- single delete ---------------------------------------------------------
def test_delete_single_without_confermo_is_409(monkeypatch):
    svc = _MockDeleteService(plan=_cleanup_plan(), report=_cleanup_report())
    client = _make_client(monkeypatch, svc)
    ap = client.post("/outlet/delete/apply", headers=_auth(),
                     json={"product_gid": "gid://shopify/Product/9", "count": 1})
    assert ap.status_code == 409
    assert svc.single_calls == 0


def test_delete_single_wrong_count_is_409(monkeypatch):
    svc = _MockDeleteService(plan=_cleanup_plan(), report=_cleanup_report())
    client = _make_client(monkeypatch, svc)
    ap = client.post("/outlet/delete/apply", headers=_auth(),
                     json={"product_gid": "gid://shopify/Product/9", "confirm": "CONFERMO", "count": 2})
    assert ap.status_code == 409
    assert svc.single_calls == 0


def test_delete_single_happy_calls_service(monkeypatch):
    svc = _MockDeleteService(plan=_cleanup_plan(), report=_cleanup_report())
    client = _make_client(monkeypatch, svc)
    ap = client.post("/outlet/delete/apply", headers=_auth(),
                     json={"product_gid": "gid://shopify/Product/9", "confirm": "CONFERMO", "count": 1})
    assert ap.status_code == 202
    out = client.get(f"/outlet/delete/apply/{ap.json()['job_id']}", headers=_auth()).json()["result"]
    assert out["status"] == STATUS_DELETED
    assert svc.single_calls == 1
    assert svc.outlet_gate_calls == 1   # HARDENING gate ran and passed (outlet target)


def test_delete_single_rejects_non_outlet_target(monkeypatch):
    """HARDENING (post-review): a mistyped GID that does not resolve to an
    outlet -> 409 single_delete_not_outlet, product_delete (delete_single_apply)
    NEVER called, no job even created."""
    svc = _MockDeleteService(plan=_cleanup_plan(), report=_cleanup_report())
    svc.target_is_outlet = False
    client = _make_client(monkeypatch, svc)
    ap = client.post("/outlet/delete/apply", headers=_auth(),
                     json={"product_gid": "gid://shopify/Product/999", "confirm": "CONFERMO", "count": 1})
    assert ap.status_code == 409
    assert ap.json()["error"]["code"] == "single_delete_not_outlet"
    assert svc.single_calls == 0
    assert svc.outlet_gate_calls == 1


# --- deny-normalize --------------------------------------------------------
def test_deny_normalize_without_confermo_is_409(monkeypatch):
    svc = _MockDeleteService(plan=_cleanup_plan(), report=_cleanup_report())
    client = _make_client(monkeypatch, svc)
    ap = client.post("/outlet/deny-normalize", headers=_auth(),
                     json={"product_gid": "gid://shopify/Product/9"})
    assert ap.status_code == 409
    assert svc.deny_calls == 0


def test_deny_normalize_happy_calls_service(monkeypatch):
    svc = _MockDeleteService(plan=_cleanup_plan(), report=_cleanup_report())
    client = _make_client(monkeypatch, svc)
    ap = client.post("/outlet/deny-normalize", headers=_auth(),
                     json={"product_gid": "gid://shopify/Product/9", "confirm": "CONFERMO"})
    assert ap.status_code == 202
    out = client.get(f"/outlet/deny-normalize/{ap.json()['job_id']}", headers=_auth()).json()["result"]
    assert out == {"normalized": 7}
    assert svc.deny_calls == 1


# --- auth ------------------------------------------------------------------
@pytest.mark.parametrize("method,path,body", [
    ("post", "/outlet/zero-stock", None),
    ("get", "/outlet/zero-stock/whatever", None),
    ("post", "/outlet/cleanup/preview", {}),
    ("get", "/outlet/cleanup/preview/whatever", None),
    ("post", "/outlet/cleanup/apply", {"plan_hash": "h", "confirm_token": "t", "confirm": "CONFERMO", "count": 1}),
    ("post", "/outlet/delete/apply", {"product_gid": "g", "confirm": "CONFERMO", "count": 1}),
    ("post", "/outlet/deny-normalize", {"product_gid": "g", "confirm": "CONFERMO"}),
    ("get", "/outlet/cleanup/apply/whatever", None),
])
def test_delete_routes_require_auth(monkeypatch, method, path, body):
    svc = _MockDeleteService(plan=_cleanup_plan(), report=_cleanup_report())
    client = _make_client(monkeypatch, svc)
    kwargs = {"json": body} if body is not None else {}
    r = getattr(client, method)(path, **kwargs)
    assert r.status_code == 401
    assert "www-authenticate" in {k.lower() for k in r.headers}


# --- single-slot job_busy (TEST-GAP) ----------------------------------------
class _StuckExecutor:
    """A single-slot executor that NEVER completes its submitted job — keeps the
    JobStore slot occupied deterministically (a real ThreadPoolExecutor would
    eventually finish, which isn't a reliable test signal)."""

    def submit(self, fn: Any, *args: Any, **kwargs: Any) -> None:
        pass  # swallow: the JobRecord stays QUEUED forever, the slot stays taken

    def shutdown(self, *args: Any, **kwargs: Any) -> None:
        pass


def test_single_slot_job_busy_is_409(monkeypatch):
    """TEST-GAP (post-review): a second request while the single slot is
    occupied -> 409 job_busy, asserted at the HTTP layer (not just JobStore)."""
    svc = _MockDeleteService(plan=_cleanup_plan(), report=_cleanup_report())
    client = _make_client(monkeypatch, svc, executor=_StuckExecutor())
    first = client.post("/outlet/zero-stock", headers=_auth())
    assert first.status_code == 202
    second = client.post("/outlet/zero-stock", headers=_auth())
    assert second.status_code == 409
    assert second.json()["error"]["code"] == "job_busy"
