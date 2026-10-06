"""PRICES vertical — aggregate plan_hash + FastAPI TestClient integration.

WORKER tier (fastapi import only): the aggregate ``prices_plan_hash`` (order
independence + drift sensitivity). TestClient tier (``importorskip`` fastapi+httpx;
runs on CI/deploy): the price routes end-to-end with Basic Auth, the REAL HMAC
confirm-token, and ``pricing_service`` mocked so nothing touches Shopify or the
register. Every apply asserts ``prices_apply`` is reached ONLY when the token verifies
AND the live re-preview matches (no drift). The last tests run the REAL service on
the in-memory register and the fake Shopify through HTTP (preview → apply → revert).
"""
from __future__ import annotations

import base64
from typing import Any, List

import pytest

from backend.services.pricing_service import (
    DebtReport,
    PriceApplyReport,
    PriceDiff,
    PriceOutcome,
    PricePlan,
    RevertReport,
)

PROMO = "gid://shopify/Location/PROMO"
_SECRET = "unit-test-signing-secret"


# ---------------------------------------------------------------------------
# Fakes + builders
# ---------------------------------------------------------------------------
class FakeRegister:
    """What the mocked service receives: a handle the job closes."""

    def __init__(self, actor: str = "") -> None:
        self.actor = actor
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeTransport:
    pass


def _diff(sku: str = "SKU1", plan_hash: str = "d1", *, actionable: bool = True) -> PriceDiff:
    return PriceDiff(
        sku=sku, product_gid="gid://shopify/Product/1", status="OK", actionable=actionable,
        price="50.00", compare_at="100.00", discount="50.00", percent=0.5, register_price="50.00",
        register_changed=False, live_price="60.00", live_compare_at="100.00", live_changed=True,
        live_status="ACTIVE", pair_ids=(1,), warnings=(), plan_hash=plan_hash,
    )


def _plan(diffs=None, mode: str = "percent") -> PricePlan:
    return PricePlan(dry_run=True, mode=mode, diffs=tuple(diffs or (_diff(),)), anomalies=())


def _apply_report() -> PriceApplyReport:
    return PriceApplyReport(intent_id="intent-1",
                            outcomes=(PriceOutcome("SKU1", "gid://shopify/Product/1", "APPLIED", ()),))


def _revert_report() -> RevertReport:
    return RevertReport(intent_id="intent-1", reverted_products=1, reverted_variants=2,
                        outcomes=(PriceOutcome("SKU1", "gid://shopify/Product/1", "REVERTED", ()),))


def _debt_report() -> DebtReport:
    return DebtReport(scanned_products=10, broken_products=2, broken_variants=3,
                      broken_gids=("gid://shopify/Product/7", "gid://shopify/Product/8"))


class _MockPricingService:
    def __init__(self, *, plan: PricePlan) -> None:
        self.plan = plan
        self.apply_report = _apply_report()
        self.revert_report = _revert_report()
        self.debt = _debt_report()
        self.preview_calls = 0
        self.apply_calls = 0
        self.revert_calls = 0
        self.debt_calls = 0
        self.registers: List[Any] = []
        self.sinks: List[Any] = []

    def prices_preview(self, register: Any, transport: Any, mode: str, params: Any,
                       *, status_override: bool = False) -> PricePlan:
        self.preview_calls += 1
        self.registers.append(register)
        return self.plan

    def prices_apply(self, register: Any, transport: Any, mode: str, params: Any,
                     approved_plan: Any, audit_sink: Any,
                     *, promo_location_id: str, status_override: bool = False) -> PriceApplyReport:
        self.apply_calls += 1
        self.registers.append(register)
        self.sinks.append(audit_sink)
        return self.apply_report

    def revert_prices(self, register: Any, transport: Any, audit_sink: Any, intent_id: str,
                      *, promo_location_id: str) -> RevertReport:
        self.revert_calls += 1
        self.registers.append(register)
        self.sinks.append(audit_sink)
        return self.revert_report

    def discharge_debt_count(self, transport: Any, *, collection_gid: str = "") -> DebtReport:
        self.debt_calls += 1
        return self.debt


# ===========================================================================
# WORKER tier — aggregate plan_hash
# ===========================================================================
def test_prices_plan_hash_order_independent_and_drift_sensitive():
    pytest.importorskip("fastapi")
    from backend.api.prices import prices_plan_hash

    a = _plan((_diff("A", "h1"), _diff("B", "h2")))
    b = _plan((_diff("B", "h2"), _diff("A", "h1")))
    assert prices_plan_hash(a) == prices_plan_hash(b)
    c = _plan((_diff("A", "h1"), _diff("B", "h9")))
    assert prices_plan_hash(a) != prices_plan_hash(c)


# ===========================================================================
# TestClient tier — RUNS ONLY where fastapi/httpx are installed (importorskip).
# ===========================================================================
def _basic(user: str, pw: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{pw}".encode("utf-8")).decode("ascii")


def _auth():
    return {"Authorization": _basic("racoon", "s3cret-pw")}


def _make_client(monkeypatch, svc: _MockPricingService):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from backend.app import create_app
    from backend.config import ShopifyConfig
    from backend.services import pricing_service

    monkeypatch.delenv("APP_USERNAME", raising=False)
    monkeypatch.setenv("APP_PASSWORD", "s3cret-pw")
    monkeypatch.setenv("TOKEN_SIGNING_SECRET", _SECRET)

    for name in ("prices_preview", "prices_apply", "revert_prices", "discharge_debt_count"):
        monkeypatch.setattr(pricing_service, name, getattr(svc, name))

    cfg = ShopifyConfig("t.myshopify.com", "shpat_x", "2026-10", PROMO)
    from backend.api.jobs import SynchronousExecutor

    app = create_app(
        config=cfg,
        sheet_factory=lambda: None,
        register_factory=lambda actor="": FakeRegister(actor),
        transport_factory=lambda: FakeTransport(),
        audit_factory=lambda: None,
        executor=SynchronousExecutor(),
        promo_location_id=PROMO,
    )
    return TestClient(app)


def _preview_and_get(client, mode: str = "percent") -> dict:
    sub = client.post("/prices/preview", headers=_auth(),
                      json={"mode": mode, "params": {"percent_by_sku": {"SKU1": 0.5}}})
    assert sub.status_code == 202
    poll = client.get(f"/prices/preview/{sub.json()['job_id']}", headers=_auth())
    assert poll.status_code == 200
    data = poll.json()
    assert data["status"] == "done"
    return data["result"]


def test_prices_preview_returns_plan_hash_and_token(monkeypatch):
    svc = _MockPricingService(plan=_plan())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    assert res["plan_hash"] and res["confirm_token"]
    assert res["plan"]["diffs"][0]["sku"] == "SKU1"
    assert res["plan"]["diffs"][0]["plan_hash"] == "d1"
    assert svc.apply_calls == 0 and svc.preview_calls == 1


def test_prices_apply_valid_token_no_drift_executes(monkeypatch):
    svc = _MockPricingService(plan=_plan())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    ap = client.post("/prices/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"],
        "mode": "percent", "params": {"percent_by_sku": {"SKU1": 0.5}},
    })
    assert ap.status_code == 202
    out = client.get(f"/prices/apply/{ap.json()['job_id']}", headers=_auth()).json()["result"]
    assert out["status"] == "APPLIED" and out["applied"] is True
    assert out["report"]["intent_id"] == "intent-1"
    assert out["report"]["outcomes"][0]["status"] == "APPLIED"
    assert svc.apply_calls == 1
    # the apply's register is bound to the acting user, its sink too; the job closed it
    reg = svc.registers[-1]
    assert reg.actor == "racoon" and reg.closed
    from backend.persistence.audit_sink import RegisterAuditSink
    assert isinstance(svc.sinks[-1], RegisterAuditSink)


def test_prices_apply_bad_token_is_409_and_skips_apply(monkeypatch):
    svc = _MockPricingService(plan=_plan())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    ap = client.post("/prices/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": "not-a-token",
        "mode": "percent", "params": {"percent_by_sku": {"SKU1": 0.5}},
    })
    assert ap.status_code == 409
    assert ap.json()["error"]["code"] == "confirm_invalid"
    assert svc.apply_calls == 0


def test_prices_apply_drift_is_verify_failed_and_skips_apply(monkeypatch):
    svc = _MockPricingService(plan=_plan())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    svc.plan = _plan((_diff("SKU1", "d2"),))   # live target moved -> aggregate flips
    ap = client.post("/prices/apply", headers=_auth(), json={
        "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"],
        "mode": "percent", "params": {"percent_by_sku": {"SKU1": 0.5}},
    })
    assert ap.status_code == 202
    out = client.get(f"/prices/apply/{ap.json()['job_id']}", headers=_auth()).json()["result"]
    assert out["status"] == "VERIFY_FAILED" and out["applied"] is False
    assert svc.apply_calls == 0


def test_prices_preview_invalid_mode_is_422(monkeypatch):
    svc = _MockPricingService(plan=_plan())
    client = _make_client(monkeypatch, svc)
    r = client.post("/prices/preview", headers=_auth(), json={"mode": "nope", "params": {}})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_mode"
    assert svc.preview_calls == 0


def test_prices_apply_invalid_mode_is_422(monkeypatch):
    svc = _MockPricingService(plan=_plan())
    client = _make_client(monkeypatch, svc)
    r = client.post("/prices/apply", headers=_auth(),
                    json={"plan_hash": "h", "confirm_token": "t", "mode": "nope", "params": {}})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "invalid_mode"
    assert svc.apply_calls == 0


def test_prices_revert_without_confermo_is_409(monkeypatch):
    svc = _MockPricingService(plan=_plan())
    client = _make_client(monkeypatch, svc)
    r = client.post("/prices/revert", headers=_auth(), json={"intent_id": "intent-1"})
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "gesture_required"
    assert svc.revert_calls == 0


def test_prices_revert_confirm_gated_happy(monkeypatch):
    svc = _MockPricingService(plan=_plan())
    client = _make_client(monkeypatch, svc)
    ap = client.post("/prices/revert", headers=_auth(),
                     json={"intent_id": "intent-1", "confirm": "CONFERMO"})
    assert ap.status_code == 202
    out = client.get(f"/prices/revert/{ap.json()['job_id']}", headers=_auth()).json()["result"]
    assert out["reverted_products"] == 1 and out["reverted_variants"] == 2
    assert out["outcomes"][0]["status"] == "REVERTED"
    assert svc.revert_calls == 1
    assert svc.registers[-1].actor == "racoon" and svc.registers[-1].closed


def test_prices_discharge_debt_is_readonly_job(monkeypatch):
    svc = _MockPricingService(plan=_plan())
    client = _make_client(monkeypatch, svc)
    sub = client.post("/prices/discharge-debt", headers=_auth())
    assert sub.status_code == 202
    out = client.get(f"/prices/discharge-debt/{sub.json()['job_id']}", headers=_auth()).json()["result"]
    assert out["broken_products"] == 2 and out["broken_variants"] == 3
    assert out["broken_gids"] == ["gid://shopify/Product/7", "gid://shopify/Product/8"]
    assert svc.debt_calls == 1
    assert svc.apply_calls == 0   # read-only: no mutation path touched


def test_prices_apply_response_never_leaks_signing_secret(monkeypatch):
    import json as _json
    svc = _MockPricingService(plan=_plan())
    client = _make_client(monkeypatch, svc)
    res = _preview_and_get(client)
    assert _SECRET not in _json.dumps(res)


@pytest.mark.parametrize("method,path,body", [
    ("post", "/prices/preview", {"mode": "percent", "params": {}}),
    ("get", "/prices/preview/whatever", None),
    ("post", "/prices/apply", {"plan_hash": "h", "confirm_token": "t", "mode": "percent", "params": {}}),
    ("get", "/prices/apply/whatever", None),
    ("post", "/prices/revert", {"intent_id": "i", "confirm": "CONFERMO"}),
    ("get", "/prices/revert/whatever", None),
    ("post", "/prices/discharge-debt", None),
    ("get", "/prices/discharge-debt/whatever", None),
])
def test_prices_routes_require_auth(monkeypatch, method, path, body):
    svc = _MockPricingService(plan=_plan())
    client = _make_client(monkeypatch, svc)
    kwargs = {"json": body} if body is not None else {}
    r = getattr(client, method)(path, **kwargs)
    assert r.status_code == 401
    assert "www-authenticate" in {k.lower() for k in r.headers}


# ===========================================================================
# The REAL service through HTTP: the in-memory register, the fake Shopify
# ===========================================================================
def _real_client(monkeypatch):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from decimal import Decimal

    from fastapi.testclient import TestClient

    from backend.api.jobs import SynchronousExecutor
    from backend.app import create_app
    from backend.config import ShopifyConfig
    from backend.services import flows

    from tests.fake_shop import PROMO as SHOP_PROMO
    from tests.fake_shop import FakeShop
    from tests.register_fixtures import MemoryBackend

    monkeypatch.delenv("APP_USERNAME", raising=False)
    monkeypatch.setenv("APP_PASSWORD", "s3cret-pw")
    monkeypatch.setenv("TOKEN_SIGNING_SECRET", _SECRET)
    be, shop = MemoryBackend(), FakeShop()
    be.seed_baseline()
    out = shop.product("SKU1 - Outlet", sku="SKU1", sizes=("42",))
    out.variants[0].price, out.variants[0].compare_at = "90.00", "100.00"
    pid, _e = be.add_shelf_pair("SKU1", "42", reconciled=True, price_high=Decimal("100.00"),
                                outlet_product_id=flows.product_number(out.gid))
    app = create_app(config=ShopifyConfig("t.myshopify.com", "shpat_x", "2026-10", SHOP_PROMO),
                     sheet_factory=lambda: None, register_factory=lambda **kw: be.register(**kw),
                     transport_factory=lambda: shop, audit_factory=lambda: None,
                     executor=SynchronousExecutor(), promo_location_id=SHOP_PROMO)
    return TestClient(app), be, shop, out, pid


def test_prices_end_to_end_on_the_register_preview_apply_revert(monkeypatch):
    from decimal import Decimal

    client, be, shop, out, pid = _real_client(monkeypatch)
    body = {"mode": "percent", "params": {"percent_by_sku": {"SKU1": 0.3}}}
    sub = client.post("/prices/preview", headers=_auth(), json=body)
    res = client.get(f"/prices/preview/{sub.json()['job_id']}", headers=_auth()).json()["result"]
    (d,) = res["plan"]["diffs"]
    assert (d["price"], d["discount"], d["register_price"], d["live_price"]) == ("70.00", "30.00", None, "90.00")
    ap = client.post("/prices/apply", headers=_auth(),
                     json={**body, "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"]})
    out_job = client.get(f"/prices/apply/{ap.json()['job_id']}", headers=_auth()).json()["result"]
    assert out_job["status"] == "APPLIED", out_job
    report = out_job["report"]
    assert report["outcomes"][0]["status"] == "APPLIED" and len(report["intent_id"]) == 32
    (p,) = be.register().pairs(pair_ids=[pid])
    assert p.price_outlet == Decimal("70.00") and shop.products[out.gid].variants[0].price == "70.00"
    rows = be.register().audit_rows("PRICE")
    assert {r.actor for r in rows} == {"racoon"}                 # every audit row names the acting user
    rv = client.post("/prices/revert", headers=_auth(), json={"intent_id": report["intent_id"], "confirm": "CONFERMO"})
    rres = client.get(f"/prices/revert/{rv.json()['job_id']}", headers=_auth()).json()["result"]
    assert rres["outcomes"][0]["status"] == "REVERTED" and rres["reverted_pairs"] == 1
    (p,) = be.register().pairs(pair_ids=[pid])
    assert p.price_outlet is None and shop.products[out.gid].variants[0].price == "90.00"
    assert be.register().lease_state().holder is None


def test_prices_revert_of_an_unknown_intent_fails_the_job_with_a_stable_code(monkeypatch):
    client, *_ = _real_client(monkeypatch)
    rv = client.post("/prices/revert", headers=_auth(), json={"intent_id": "0" * 32, "confirm": "CONFERMO"})
    rec = client.get(f"/prices/revert/{rv.json()['job_id']}", headers=_auth()).json()
    assert rec["status"] == "failed" and rec["error_code"] == "intent_not_found"
