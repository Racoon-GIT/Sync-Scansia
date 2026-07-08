"""INIT vertical — shared mutation helper + FastAPI TestClient integration.

Mirrors ``test_api_publish``/``test_api_delete``: the compute service
(``backend.services.init_service``) is mocked at the module boundary so nothing
touches Shopify or the sheet; only the HTTP/token/gesture machinery is exercised
here. Runs on a bare interpreter as an import-time no-op (``init_service`` has no
FastAPI dependency); every test that needs FastAPI/httpx calls
``pytest.importorskip`` locally, matching the sibling test modules.
"""
from __future__ import annotations

import base64
from types import SimpleNamespace
from typing import Any, List

from backend.services.init_service import (
    InitOutcome,
    InitPlan,
    InitReport,
    InitRowDecision,
)

PROMO = "gid://shopify/Location/PROMO"
_SECRET = "unit-test-signing-secret"


# ---------------------------------------------------------------------------
# Fakes + plan/report builders
# ---------------------------------------------------------------------------
class FakeSheet:
    """Opaque sheet handle — exposes ``.ws.spreadsheet`` (audit sink builder) and
    ``cutover_done()`` (the ``GET /init/status`` banner probe)."""

    def __init__(self, *, cutover_done: bool = False) -> None:
        self.ws = SimpleNamespace(spreadsheet=SimpleNamespace())
        self._cutover_done = cutover_done

    def cutover_done(self) -> bool:
        return self._cutover_done


class FakeTransport:
    """Opaque transport handle — the service is mocked."""


def _decision(sku="SKU1", size="42", bucket="demote:missing", ph="h1") -> InitRowDecision:
    return InitRowDecision(sku=sku, size=size, row_uuid="", bucket=bucket,
                            target_gid=None, live_status=None, plan_hash=ph)


def _plan(ph: str = "h1", *, demote_missing_count: int = 1) -> InitPlan:
    return InitPlan(
        dry_run=True, cutover_already_done=False, backfill_pending_rows=1,
        demote_missing=tuple(_decision(sku=f"SKU{i}", ph=f"{ph}-{i}") for i in range(demote_missing_count)),
    )


def _report() -> InitReport:
    return InitReport(
        backfill_stamped=1, backfill_already_done=False, demoted_rows=1,
        drafted_products=0, verify_failed_rows=0,
        outcomes=(InitOutcome("SKU1", "42", "u1", "demote:missing", "DEMOTED", None, ()),),
    )


class _MockInitService:
    """Stand-in for ``init_service`` — records calls; ``plan`` is mutable so a
    test can simulate LIVE drift between preview and the apply re-preview."""

    def __init__(self, *, plan: InitPlan, report: InitReport) -> None:
        self.plan = plan
        self.report = report
        self.preview_calls = 0
        self.apply_calls = 0
        self.last_approved_plan_hash: Any = None

    def init_preview(self, sheet: Any, transport: Any, *, promo_location_id: str) -> InitPlan:
        self.preview_calls += 1
        return self.plan

    def init_apply(self, sheet: Any, transport: Any, approved_plan: InitPlan, audit_sink: Any,
                   *, promo_location_id: str, now=None, approved_plan_hash=None) -> InitReport:
        self.apply_calls += 1
        self.last_approved_plan_hash = approved_plan_hash
        return self.report


# ===========================================================================
# init_plan_hash — aggregate TOCTOU key (API layer)
# ===========================================================================
def test_init_plan_hash_is_order_independent_and_excludes_row_uuid():
    import pytest
    pytest.importorskip("fastapi")
    from backend.api.init import init_plan_hash

    d1 = InitRowDecision(sku="A", size="42", row_uuid="uuid-preview-1", bucket="demote:missing",
                         target_gid=None, live_status=None, plan_hash="h1")
    d2 = InitRowDecision(sku="B", size="40", row_uuid="uuid-preview-2", bucket="kept-online",
                         target_gid="gid://p/1", live_status="ACTIVE", plan_hash="h2")
    plan_a = InitPlan(dry_run=True, cutover_already_done=False, backfill_pending_rows=0,
                      demote_missing=(d1,), kept_online=(d2,))
    plan_b = InitPlan(dry_run=True, cutover_already_done=False, backfill_pending_rows=0,
                      demote_missing=(d1,), kept_online=(d2,))
    assert init_plan_hash(plan_a) == init_plan_hash(plan_b)

    # Same rows, DIFFERENT row_uuid (as would happen between an ephemeral preview
    # read and a real post-backfill read) -> the aggregate hash is UNCHANGED.
    d1_new_uuid = InitRowDecision(sku="A", size="42", row_uuid="uuid-apply-REAL", bucket="demote:missing",
                                  target_gid=None, live_status=None, plan_hash="h1")
    plan_c = InitPlan(dry_run=True, cutover_already_done=False, backfill_pending_rows=0,
                      demote_missing=(d1_new_uuid,), kept_online=(d2,))
    assert init_plan_hash(plan_a) == init_plan_hash(plan_c)

    # A genuinely different row plan_hash flips the aggregate.
    d1_drift = InitRowDecision(sku="A", size="42", row_uuid="x", bucket="kept-online",
                               target_gid="gid://p/9", live_status="ACTIVE", plan_hash="h9")
    plan_d = InitPlan(dry_run=True, cutover_already_done=False, backfill_pending_rows=0,
                      demote_missing=(), kept_online=(d2, d1_drift))
    assert init_plan_hash(plan_a) != init_plan_hash(plan_d)


# ===========================================================================
# TestClient tier — RUNS ONLY where fastapi/httpx are installed (importorskip).
# ===========================================================================
def _basic(user: str, pw: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{pw}".encode("utf-8")).decode("ascii")


def _auth():
    return {"Authorization": _basic("racoon", "s3cret-pw")}


def _make_client(monkeypatch, svc: _MockInitService, *, cutover_done: bool = False):
    import pytest
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from backend.app import create_app
    from backend.config import ShopifyConfig
    from backend.services import init_service

    monkeypatch.delenv("APP_USERNAME", raising=False)
    monkeypatch.setenv("APP_PASSWORD", "s3cret-pw")
    monkeypatch.setenv("TOKEN_SIGNING_SECRET", _SECRET)

    monkeypatch.setattr(init_service, "init_preview", svc.init_preview)
    monkeypatch.setattr(init_service, "init_apply", svc.init_apply)

    cfg = ShopifyConfig("t.myshopify.com", "shpat_x", "2025-07", PROMO)
    from backend.api.jobs import SynchronousExecutor

    app = create_app(
        config=cfg,
        sheet_factory=lambda: FakeSheet(cutover_done=cutover_done),
        transport_factory=lambda: FakeTransport(),
        audit_factory=lambda: None,
        executor=SynchronousExecutor(),
        promo_location_id=PROMO,
    )
    return TestClient(app)


def _preview_and_get(client) -> dict:
    sub = client.post("/init/preview", headers=_auth())
    assert sub.status_code == 202
    job_id = sub.json()["job_id"]
    poll = client.get(f"/init/preview/{job_id}", headers=_auth())
    assert poll.status_code == 200
    data = poll.json()
    assert data["status"] == "done"
    return data["result"]


# --- status (banner gate) --------------------------------------------------
def test_init_status_reports_cutover_done(monkeypatch):
    svc = _MockInitService(plan=_plan(), report=_report())
    client = _make_client(monkeypatch, svc, cutover_done=False)
    res = client.get("/init/status", headers=_auth())
    assert res.status_code == 200
    assert res.json() == {"cutover_done": False}


def test_init_status_requires_auth(monkeypatch):
    svc = _MockInitService(plan=_plan(), report=_report())
    client = _make_client(monkeypatch, svc)
    res = client.get("/init/status")
    assert res.status_code == 401


# --- preview -----------------------------------------------------------
def test_init_preview_returns_plan_hash_and_token_and_does_not_mutate(monkeypatch):
    svc = _MockInitService(plan=_plan("h1"), report=_report())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    assert res["plan_hash"] and res["confirm_token"]
    assert res["plan"]["demote_missing"][0]["sku"] == "SKU0"
    assert svc.apply_calls == 0
    assert svc.preview_calls == 1


# --- apply: happy path ------------------------------------------------------
def test_init_apply_valid_token_and_confermo_executes(monkeypatch):
    svc = _MockInitService(plan=_plan("h1"), report=_report())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    ap = client.post("/init/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"], "confirm": "CONFERMO",
    })
    assert ap.status_code == 202
    out = client.get(f"/init/apply/{ap.json()['job_id']}", headers=_auth()).json()["result"]
    assert out["status"] == "APPLIED" and out["applied"] is True
    assert out["report"]["demoted_rows"] == 1
    assert svc.apply_calls == 1


def test_init_apply_threads_the_real_aggregate_plan_hash_into_the_service(monkeypatch):
    """HIGH-2 (post-review): the api layer must recompute + thread the REAL
    aggregate plan_hash into init_service.init_apply (used for the durable
    before-snapshot + AFTER audit event) — never leave it implicit/empty."""
    import pytest
    pytest.importorskip("fastapi")
    from backend.api.init import init_plan_hash

    svc = _MockInitService(plan=_plan("h1"), report=_report())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    ap = client.post("/init/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"], "confirm": "CONFERMO",
    })
    client.get(f"/init/apply/{ap.json()['job_id']}", headers=_auth())
    assert svc.last_approved_plan_hash == init_plan_hash(svc.plan)


# --- apply: gesture failures (NO service call) ------------------------------
def test_init_apply_without_confermo_is_gesture_required_and_skips_apply(monkeypatch):
    svc = _MockInitService(plan=_plan("h1"), report=_report())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    ap = client.post("/init/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"],
    })
    assert ap.status_code == 202  # token verified -> a job IS created
    out = client.get(f"/init/apply/{ap.json()['job_id']}", headers=_auth()).json()["result"]
    assert out["status"] == "gesture_required" and out["applied"] is False
    assert svc.apply_calls == 0


def test_init_apply_over_threshold_without_second_confirm_is_gesture_required(monkeypatch):
    svc = _MockInitService(plan=_plan("h1", demote_missing_count=26), report=_report())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    ap = client.post("/init/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"], "confirm": "CONFERMO",
    })
    out = client.get(f"/init/apply/{ap.json()['job_id']}", headers=_auth()).json()["result"]
    assert out["status"] == "gesture_required" and out["applied"] is False
    assert svc.apply_calls == 0


def test_init_apply_over_threshold_with_second_confirm_executes(monkeypatch):
    svc = _MockInitService(plan=_plan("h1", demote_missing_count=26), report=_report())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    ap = client.post("/init/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"],
        "confirm": "CONFERMO", "second_confirm": True,
    })
    out = client.get(f"/init/apply/{ap.json()['job_id']}", headers=_auth()).json()["result"]
    assert out["status"] == "APPLIED" and out["applied"] is True
    assert svc.apply_calls == 1


# --- apply: token failures (NO job, NO service call) ------------------------
def test_init_apply_bad_token_is_409_and_skips_apply(monkeypatch):
    svc = _MockInitService(plan=_plan("h1"), report=_report())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    ap = client.post("/init/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": "not-a-valid-token", "confirm": "CONFERMO",
    })
    assert ap.status_code == 409
    assert ap.json()["error"]["code"] == "confirm_invalid"
    assert svc.apply_calls == 0


def test_init_apply_drift_is_verify_failed_and_skips_apply(monkeypatch):
    svc = _MockInitService(plan=_plan("h1"), report=_report())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    ph, token = res["plan_hash"], res["confirm_token"]

    svc.plan = _plan("h2")  # LIVE state moved between preview and apply
    ap = client.post("/init/apply", headers=_auth(), json={
        "plan_hash": ph, "confirm_token": token, "confirm": "CONFERMO",
    })
    assert ap.status_code == 202
    out = client.get(f"/init/apply/{ap.json()['job_id']}", headers=_auth()).json()["result"]
    assert out["status"] == "VERIFY_FAILED" and out["applied"] is False
    assert svc.apply_calls == 0


def test_init_routes_require_auth(monkeypatch):
    import pytest
    svc = _MockInitService(plan=_plan("h1"), report=_report())
    client = _make_client(monkeypatch, svc)
    for method, path, kwargs in [
        ("post", "/init/preview", {}),
        ("get", "/init/preview/whatever", {}),
        ("post", "/init/apply", {"json": {"plan_hash": "h1", "confirm_token": "t"}}),
        ("get", "/init/apply/whatever", {}),
    ]:
        r = getattr(client, method)(path, **kwargs)
        assert r.status_code == 401
