"""PUBLISH vertical — shared mutation helper + FastAPI TestClient integration.

Two tiers, mirroring ``test_api_read``:

* WORKER tier (needs ``fastapi`` only for the module import — no TestClient/httpx):
  the generic apply/preview off-loop workers in ``backend.api.mutations`` with pure
  fakes and the real ``JobStore`` — the drift gate, the gesture gate, and the
  happy path, plus the preview mint. No network.
* TestClient tier (``importorskip`` fastapi+httpx — SKIPPED where the web deps are
  absent, e.g. this bare interpreter; runs on CI/deploy): the four publish routes
  end-to-end with Basic Auth, the REAL HMAC confirm-token, and ``outlet_service``
  mocked so nothing touches Shopify or the sheet.

Every mutation path asserts the service ``publish_apply`` is NOT called unless the
token verifies AND the live re-preview matches (no drift).
"""
from __future__ import annotations

import base64
from typing import Any, List, Optional

import pytest

from backend.services.outlet_service import (
    ApplyOutcome,
    ApplyReport,
    Plan,
    PlanAction,
)

PROMO = "gid://shopify/Location/PROMO"
_SECRET = "unit-test-signing-secret"


# ---------------------------------------------------------------------------
# Fakes + plan/report builders
# ---------------------------------------------------------------------------
class FakeSheet:
    """Opaque sheet handle — the service is mocked, so no method is exercised."""


class FakeTransport:
    """Opaque transport handle — the service is mocked."""


def _plan(action_hash: str = "h1", sku: str = "SKU1") -> Plan:
    """A minimal but real :class:`Plan` so ``publish_plan_hash`` / serializers run."""
    action = PlanAction(
        sku=sku, branch="ACTIVE", publishable=True, reason=None,
        target_gid="gid://shopify/Product/1", source_gid=None, outlet_title=None,
        live_status="ACTIVE", price="50.00", compare_at="100.00", price_ok=True,
        size_targets=(), unmatched_sizes=(), warnings=(), plan_hash=action_hash,
    )
    return Plan(dry_run=True, actions=(action,), anomalies=())


def _report() -> ApplyReport:
    return ApplyReport(outcomes=(
        ApplyOutcome(
            sku="SKU1", branch="ACTIVE", status="APPLIED",
            target_gid="gid://shopify/Product/1", warnings=(), reconciled_uuids=("u1",),
        ),
    ))


class _MockService:
    """Stand-in for ``outlet_service`` — records calls; ``plan`` is mutable so a
    test can simulate LIVE drift between the preview and the apply re-preview."""

    def __init__(self, *, plan: Plan, report: ApplyReport) -> None:
        self.plan = plan
        self.report = report
        self.preview_calls = 0
        self.apply_calls = 0

    def publish_preview(self, sheet: Any, transport: Any, *, promo_location_id: str) -> Plan:
        self.preview_calls += 1
        return self.plan

    def publish_apply(self, sheet: Any, transport: Any, approved_plan: Plan, *,
                      promo_location_id: str, publication_id: Optional[str] = None) -> ApplyReport:
        self.apply_calls += 1
        return self.report


# ===========================================================================
# WORKER tier — generic apply/preview workers (fastapi import only; no network)
# ===========================================================================
def test_apply_worker_happy_path_calls_apply():
    pytest.importorskip("fastapi")
    from backend.api.jobs import JobStore
    from backend.api.mutations import _run_apply_job

    store = JobStore()
    rec = store.create("x_apply")
    calls: List[Any] = []

    def apply_fn(s, t, plan, sink):
        calls.append((plan, sink))
        return {"ok": True}

    _run_apply_job(
        store, rec.job_id, "H1",
        lambda: FakeSheet(), lambda: FakeTransport(),
        None,                                   # no audit builder (publish-shaped)
        lambda s, t: {"h": "H1"},               # live re-preview -> same hash
        lambda p: p["h"], apply_fn, lambda r: r, None,
    )
    res = store.get(rec.job_id).result
    assert res["status"] == "APPLIED" and res["applied"] is True
    assert res["report"] == {"ok": True}
    assert len(calls) == 1 and calls[0][1] is None  # audit_sink None for publish


def test_apply_worker_drift_is_verify_failed_and_skips_apply():
    pytest.importorskip("fastapi")
    from backend.api.jobs import JobStore
    from backend.api.mutations import _run_apply_job

    store = JobStore()
    rec = store.create("x_apply")
    calls: List[Any] = []

    _run_apply_job(
        store, rec.job_id, "H1",
        lambda: FakeSheet(), lambda: FakeTransport(),
        None,
        lambda s, t: {"h": "H2"},               # live state moved -> hash differs
        lambda p: p["h"],
        lambda s, t, plan, sink: calls.append(1), lambda r: r, None,
    )
    res = store.get(rec.job_id).result
    assert res["status"] == "VERIFY_FAILED" and res["applied"] is False
    assert calls == []                          # apply NEVER reached on drift


def test_apply_worker_gesture_gate_blocks_apply():
    pytest.importorskip("fastapi")
    from backend.api.jobs import JobStore
    from backend.api.mutations import _run_apply_job

    store = JobStore()
    rec = store.create("x_apply")
    calls: List[Any] = []

    _run_apply_job(
        store, rec.job_id, "H1",
        lambda: FakeSheet(), lambda: FakeTransport(),
        None,
        lambda s, t: {"h": "H1"},               # no drift
        lambda p: p["h"],
        lambda s, t, plan, sink: calls.append(1), lambda r: r,
        lambda plan: "COUNT_MISMATCH",          # gesture gate fails
    )
    res = store.get(rec.job_id).result
    assert res["status"] == "COUNT_MISMATCH" and res["applied"] is False
    assert calls == []                          # apply NEVER reached when gesture fails


def test_apply_worker_service_crash_maps_to_stable_code():
    pytest.importorskip("fastapi")
    from backend.api.jobs import JobStore
    from backend.api.mutations import _run_apply_job

    store = JobStore()
    rec = store.create("x_apply")

    def boom(s, t, plan, sink):
        raise RuntimeError("RAW-INTERNAL-never-leak")

    _run_apply_job(
        store, rec.job_id, "H1",
        lambda: FakeSheet(), lambda: FakeTransport(),
        None, lambda s, t: {"h": "H1"}, lambda p: p["h"], boom, lambda r: r, None,
    )
    got = store.get(rec.job_id)
    assert got.status == "failed"
    assert got.error_code == "internal_error"   # bounded code, no raw message
    assert got.result is None


def test_preview_worker_mints_token_and_stores_plan():
    pytest.importorskip("fastapi")
    from backend.api.jobs import JobStore
    from backend.api.mutations import _run_preview_job

    class FakeToken:
        def mint(self, plan_hash: str, ttl_s: int, *, kind: str) -> str:
            return f"tok:{plan_hash}:{ttl_s}"

    store = JobStore()
    rec = store.create("x_preview")
    _run_preview_job(
        store, rec.job_id,
        lambda: FakeSheet(), lambda: FakeTransport(),
        lambda s, t: {"h": "HH"}, lambda p: p["h"], lambda p: {"plan": p},
        FakeToken(), "publish", 900, None,
    )
    res = store.get(rec.job_id).result
    assert res["plan_hash"] == "HH"
    assert res["confirm_token"] == "tok:HH:900"
    assert res["plan"] == {"plan": {"h": "HH"}}


def test_publish_plan_hash_is_order_independent():
    """Aggregate hash binds the SET of per-SKU plan_hashes, not their order."""
    pytest.importorskip("fastapi")
    from backend.api.publish import publish_plan_hash

    a = Plan(dry_run=True, actions=(_plan("h1", "A").actions[0], _plan("h2", "B").actions[0]))
    b = Plan(dry_run=True, actions=(_plan("h2", "B").actions[0], _plan("h1", "A").actions[0]))
    assert publish_plan_hash(a) == publish_plan_hash(b)
    # a changed live state on ANY action flips the aggregate.
    c = Plan(dry_run=True, actions=(_plan("h1", "A").actions[0], _plan("h9", "B").actions[0]))
    assert publish_plan_hash(a) != publish_plan_hash(c)


# ===========================================================================
# TestClient tier — RUNS ONLY where fastapi/httpx are installed (importorskip).
# ===========================================================================
def _basic(user: str, pw: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{pw}".encode("utf-8")).decode("ascii")


def _make_client(monkeypatch, svc: _MockService):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from backend.app import create_app
    from backend.config import ShopifyConfig
    from backend.services import outlet_service

    monkeypatch.delenv("APP_USERNAME", raising=False)
    monkeypatch.setenv("APP_PASSWORD", "s3cret-pw")
    monkeypatch.setenv("TOKEN_SIGNING_SECRET", _SECRET)

    # Mock the service at the module boundary — the router closures call
    # outlet_service.publish_preview / .publish_apply by attribute at call time.
    monkeypatch.setattr(outlet_service, "publish_preview", svc.publish_preview)
    monkeypatch.setattr(outlet_service, "publish_apply", svc.publish_apply)

    cfg = ShopifyConfig("t.myshopify.com", "shpat_x", "2025-07", PROMO)
    from backend.api.jobs import SynchronousExecutor

    app = create_app(
        config=cfg,
        sheet_factory=lambda: FakeSheet(),
        transport_factory=lambda: FakeTransport(),
        audit_factory=lambda: None,
        executor=SynchronousExecutor(),  # POST completes the job inline
        promo_location_id=PROMO,
    )
    return TestClient(app)


def _auth():
    return {"Authorization": _basic("racoon", "s3cret-pw")}


def _preview_and_get(client) -> dict:
    sub = client.post("/outlet/publish/preview", headers=_auth())
    assert sub.status_code == 202
    job_id = sub.json()["job_id"]
    poll = client.get(f"/outlet/publish/preview/{job_id}", headers=_auth())
    assert poll.status_code == 200
    data = poll.json()
    assert data["status"] == "done"
    return data["result"]


def test_publish_preview_returns_plan_hash_and_token_and_does_not_mutate(monkeypatch):
    svc = _MockService(plan=_plan("h1"), report=_report())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    assert res["plan_hash"] and res["confirm_token"]
    assert res["plan"]["actions"][0]["sku"] == "SKU1"
    assert res["plan"]["actions"][0]["plan_hash"] == "h1"
    # preview is READ-ONLY: the service apply must never have been called.
    assert svc.apply_calls == 0
    assert svc.preview_calls == 1


def test_publish_apply_valid_token_no_drift_executes_apply(monkeypatch):
    svc = _MockService(plan=_plan("h1"), report=_report())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    ph, token = res["plan_hash"], res["confirm_token"]

    ap = client.post("/outlet/publish/apply", headers=_auth(),
                     json={"plan_hash": ph, "confirm_token": token})
    assert ap.status_code == 202
    ajob = ap.json()["job_id"]
    out = client.get(f"/outlet/publish/apply/{ajob}", headers=_auth()).json()
    assert out["status"] == "done"
    ares = out["result"]
    assert ares["status"] == "APPLIED" and ares["applied"] is True
    assert ares["report"]["outcomes"][0]["status"] == "APPLIED"
    assert svc.apply_calls == 1


def test_publish_apply_bad_token_is_409_and_skips_apply(monkeypatch):
    svc = _MockService(plan=_plan("h1"), report=_report())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    ph = res["plan_hash"]

    ap = client.post("/outlet/publish/apply", headers=_auth(),
                     json={"plan_hash": ph, "confirm_token": "not-a-valid-token"})
    assert ap.status_code == 409
    assert ap.json()["error"]["code"] == "confirm_invalid"
    assert svc.apply_calls == 0  # publish_apply NEVER called on a bad token


def test_publish_apply_expired_token_is_409(monkeypatch):
    """A token minted with a NEGATIVE ttl is already expired -> 409, no apply."""
    svc = _MockService(plan=_plan("h1"), report=_report())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    ph = res["plan_hash"]

    from backend.persistence.tokens import HmacTokenService

    expired = HmacTokenService().mint(ph, -10, kind="publish")  # exp in the past
    ap = client.post("/outlet/publish/apply", headers=_auth(),
                     json={"plan_hash": ph, "confirm_token": expired})
    assert ap.status_code == 409
    assert ap.json()["error"]["code"] == "confirm_invalid"
    assert svc.apply_calls == 0


def test_publish_apply_missing_signing_secret_is_503_config_error(monkeypatch):
    """TEST-GAP (post-review): TOKEN_SIGNING_SECRET absent at apply time ->
    HmacTokenService.verify raises ConfigError -> the app error boundary maps it
    to 503 config_error (fail-closed) — never 409, never a leaked message."""
    svc = _MockService(plan=_plan("h1"), report=_report())
    client = _make_client(monkeypatch, svc)
    monkeypatch.delenv("TOKEN_SIGNING_SECRET", raising=False)
    ap = client.post("/outlet/publish/apply", headers=_auth(),
                     json={"plan_hash": "h1", "confirm_token": "whatever"})
    assert ap.status_code == 503
    assert ap.json()["error"]["code"] == "config_error"
    assert svc.apply_calls == 0


def test_publish_apply_drift_is_verify_failed_and_skips_apply(monkeypatch):
    svc = _MockService(plan=_plan("h1"), report=_report())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    ph, token = res["plan_hash"], res["confirm_token"]

    # LIVE state moves between preview and apply: the re-preview yields a different
    # aggregate hash while the token stays valid for the ORIGINAL submitted hash.
    svc.plan = _plan("h2")

    ap = client.post("/outlet/publish/apply", headers=_auth(),
                     json={"plan_hash": ph, "confirm_token": token})
    assert ap.status_code == 202  # token verified -> a job IS created
    ajob = ap.json()["job_id"]
    out = client.get(f"/outlet/publish/apply/{ajob}", headers=_auth()).json()
    ares = out["result"]
    assert ares["status"] == "VERIFY_FAILED" and ares["applied"] is False
    assert svc.apply_calls == 0  # publish_apply NEVER called on drift


def test_publish_apply_response_never_leaks_token_signing_secret(monkeypatch):
    svc = _MockService(plan=_plan("h1"), report=_report())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    import json as _json
    # the signing secret must never appear anywhere in the preview payload.
    assert _SECRET not in _json.dumps(res)


@pytest.mark.parametrize("method,path", [
    ("post", "/outlet/publish/preview"),
    ("get", "/outlet/publish/preview/whatever"),
    ("post", "/outlet/publish/apply"),
    ("get", "/outlet/publish/apply/whatever"),
])
def test_publish_routes_require_auth(monkeypatch, method, path):
    svc = _MockService(plan=_plan("h1"), report=_report())
    client = _make_client(monkeypatch, svc)
    kwargs = {}
    if method == "post" and path.endswith("/apply"):
        kwargs["json"] = {"plan_hash": "h1", "confirm_token": "t"}
    r = getattr(client, method)(path, **kwargs)
    assert r.status_code == 401
    assert "www-authenticate" in {k.lower() for k in r.headers}
