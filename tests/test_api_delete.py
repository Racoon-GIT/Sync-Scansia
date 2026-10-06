"""DELETE vertical + RECONCILE rows through HTTP (plan §3.5, §3.3 rule 2 (iv)).

Two tiers:

* WORKER tier (fastapi import only): the generic ``_run_readonly_job`` /
  ``_run_mutation_job`` and the ``token_binding_fn`` path of ``_run_preview_job``
  in ``backend.api.mutations``, with pure fakes + the real ``JobStore``.
* TestClient tier (``importorskip`` fastapi+httpx): every route end to end — Basic
  Auth, the REAL HMAC confirm-tokens, the REAL services on the in-memory register
  and the stateful fake Shopify. The gestures are asserted at the HTTP layer: a
  missing ``CONFERMO``, a wrong count, a bad / foreign token, a client-raised
  threshold → 409 and nothing deleted; live drift → ``VERIFY_FAILED``.
"""
from __future__ import annotations

import base64
from types import SimpleNamespace
from typing import Any, List

import pytest

from backend.persistence.records import HELD, KIND_AUDIT, KIND_DELETE, KIND_HOLD, OPEN
from backend.services import flows
from backend.services import ledger as L
from backend.services.delete_service import PromoAnchorError

from tests.fake_shop import PROMO, FakeShop
from tests.register_fixtures import MemoryBackend, seed_reconcile

_SECRET = "unit-test-signing-secret"


class FakeHandle:
    def close(self) -> None:
        pass


# ===========================================================================
# WORKER tier — generic read-only / mutation / preview workers
# ===========================================================================
def test_readonly_job_happy_runs_work_and_serializes():
    pytest.importorskip("fastapi")
    from backend.api.jobs import JobStore
    from backend.api.mutations import _run_readonly_job

    store = JobStore()
    rec = store.create("x_ro")
    _run_readonly_job(store, rec.job_id, lambda: object(), lambda transport: {"scanned": 5}, lambda r: r)
    got = store.get(rec.job_id)
    assert got.status == "done" and got.result == {"scanned": 5}


def test_readonly_job_maps_promo_anchor_error_to_stable_code():
    pytest.importorskip("fastapi")
    from backend.api.jobs import JobStore
    from backend.api.mutations import _run_readonly_job

    store = JobStore()
    rec = store.create("x_ro")

    def boom(transport):
        raise PromoAnchorError("PROMO_LOCATION_ID missing")

    _run_readonly_job(store, rec.job_id, lambda: object(), boom, lambda r: r)
    got = store.get(rec.job_id)
    assert got.status == "failed" and got.error_code == "promo_anchor_missing" and got.result is None


def test_mutation_job_builds_audit_sink_and_calls_service():
    pytest.importorskip("fastapi")
    from backend.api.jobs import JobStore
    from backend.api.mutations import _run_mutation_job

    store = JobStore()
    rec = store.create("x_mut")
    seen: List[Any] = []

    def mutate(handle, transport, audit_sink):
        seen.append(audit_sink)
        return {"ok": 1}

    _run_mutation_job(store, rec.job_id, FakeHandle, lambda: object(), lambda h: "SINK", mutate, lambda r: r)
    got = store.get(rec.job_id)
    assert got.status == "done" and got.result == {"ok": 1} and seen == ["SINK"]


def test_mutation_job_no_audit_builder_passes_none():
    pytest.importorskip("fastapi")
    from backend.api.jobs import JobStore
    from backend.api.mutations import _run_mutation_job

    store = JobStore()
    rec = store.create("x_mut")
    seen: List[Any] = []
    _run_mutation_job(store, rec.job_id, FakeHandle, lambda: object(), None,
                      lambda h, t, sink: seen.append(sink) or {"n": 1}, lambda r: r)
    assert seen == [None]


def test_preview_worker_token_binds_composite_but_stores_raw_hash():
    pytest.importorskip("fastapi")
    from backend.api.jobs import JobStore
    from backend.api.mutations import _run_preview_job

    class FakeToken:
        def mint(self, plan_hash: str, ttl_s: int, *, kind: str) -> str:
            return f"tok:{plan_hash}"

    store = JobStore()
    rec = store.create("x_prev")
    plan = SimpleNamespace(plan_hash="RAW", count=3)
    _run_preview_job(store, rec.job_id, FakeHandle, lambda: object(), lambda s, t: plan, lambda p: p.plan_hash,
                     lambda p: {"plan_hash": p.plan_hash}, FakeToken(), "cleanup", 900,
                     lambda p: {"count": p.count}, lambda p: f"{p.plan_hash}#{p.count}")
    res = store.get(rec.job_id).result
    assert res["plan_hash"] == "RAW" and res["count"] == 3 and res["confirm_token"] == "tok:RAW#3"


# ===========================================================================
# TestClient tier — the real services on the in-memory register
# ===========================================================================
def _auth():
    return {"Authorization": "Basic " + base64.b64encode(b"racoon:s3cret-pw").decode("ascii")}


class World:
    def __init__(self, monkeypatch, *, executor: Any = None) -> None:
        pytest.importorskip("fastapi")
        pytest.importorskip("httpx")
        from fastapi.testclient import TestClient

        from backend.api.jobs import SynchronousExecutor
        from backend.app import create_app
        from backend.config import ShopifyConfig

        monkeypatch.delenv("APP_USERNAME", raising=False)
        monkeypatch.setenv("APP_PASSWORD", "s3cret-pw")
        monkeypatch.setenv("TOKEN_SIGNING_SECRET", _SECRET)
        self.be = MemoryBackend()
        self.be.seed_baseline()
        self.shop = FakeShop()
        app = create_app(config=ShopifyConfig("t.myshopify.com", "shpat_x", "2026-10", PROMO),
                         register_factory=lambda **kw: self.be.register(**kw),
                         transport_factory=lambda: self.shop,
                         executor=executor or SynchronousExecutor(), promo_location_id=PROMO)
        self.client = TestClient(app)

    def register(self):
        return self.be.register(actor="t")

    def post(self, path, body=None):
        return self.client.post(path, headers=_auth(), json=body if body is not None else {})

    def job(self, path, sub):
        assert sub.status_code == 202, sub.json()
        rec = self.client.get(f"{path}/{sub.json()['job_id']}", headers=_auth()).json()
        assert rec["status"] == "done", rec
        return rec["result"]

    def preview(self, path, body=None):
        return self.job(f"{path}/preview", self.post(f"{path}/preview", body))


# --- zero-stock, cleanup --------------------------------------------------------------
def test_zero_stock_returns_candidates_and_review(monkeypatch):
    w = World(monkeypatch)
    cand = w.shop.product("A - Outlet", status="DRAFT", sku="A")
    w.shop.product("B - Outlet", status="DRAFT", sku="B", committed={"42": 1})
    res = w.job("/outlet/zero-stock", w.post("/outlet/zero-stock"))
    assert [c["product_gid"] for c in res["candidates"]] == [cand.gid]
    assert res["review"][0]["reasons"] == ["promo_committed"]


def test_cleanup_end_to_end_protected_listed_apart_deleted_with_count_and_confermo(monkeypatch):
    w = World(monkeypatch)
    keep = w.shop.product("A - Outlet", status="DRAFT", sku="A")
    gone = w.shop.product("B - Outlet", status="DRAFT", sku="B")
    w.be.add_shelf_pair("A", "42")
    res = w.preview("/outlet/cleanup")
    assert res["count"] == 1 and [p["product_gid"] for p in res["plan"]["protected"]] == [keep.gid]
    body = {"plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"], "count": 1, "confirm": "CONFERMO"}
    out = w.job("/outlet/cleanup/apply", w.post("/outlet/cleanup/apply", body))
    assert out["status"] == "APPLIED" and out["report"]["deleted"] == 1
    assert gone.gid not in w.shop.products and keep.gid in w.shop.products
    assert {r.actor for r in w.register().audit_rows(KIND_DELETE)} == {"racoon"}


@pytest.mark.parametrize("over,code", [
    ({"confirm": ""}, "gesture_required"),
    ({"count": 2}, "confirm_invalid"),                 # the count is part of the token binding
    ({"confirm_token": "nope"}, "confirm_invalid"),
    ({"count": None}, "gesture_required"),
])
def test_cleanup_apply_gestures_refuse_before_any_job(monkeypatch, over, code):
    w = World(monkeypatch)
    gone = w.shop.product("B - Outlet", status="DRAFT", sku="B")
    res = w.preview("/outlet/cleanup")
    body = {"plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"], "count": 1, "confirm": "CONFERMO",
            **over}
    r = w.post("/outlet/cleanup/apply", body)
    assert r.status_code == 409 and r.json()["error"]["code"] == code
    assert gone.gid in w.shop.products and w.shop.count("delete") == 0


def test_cleanup_raised_threshold_cannot_skip_the_signed_second_confirm(monkeypatch):
    w = World(monkeypatch)
    for i in range(3):
        w.shop.product(f"{i} - Outlet", status="DRAFT", sku=f"S{i}")
    res = w.preview("/outlet/cleanup", {"threshold": 2})
    assert res["requires_second_confirm"] is True
    r = w.post("/outlet/cleanup/apply", {"plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"],
                                         "count": 3, "confirm": "CONFERMO", "threshold": 50})
    assert r.status_code == 409 and w.shop.count("delete") == 0


def test_cleanup_drift_is_verify_failed(monkeypatch):
    w = World(monkeypatch)
    gone = w.shop.product("B - Outlet", status="DRAFT", sku="B")
    res = w.preview("/outlet/cleanup")
    w.shop.promo(gone.gid, "42").available = 1
    out = w.job("/outlet/cleanup/apply", w.post("/outlet/cleanup/apply", {
        "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"], "count": 1, "confirm": "CONFERMO"}))
    assert out["status"] == "VERIFY_FAILED" and gone.gid in w.shop.products


# --- single delete ----------------------------------------------------------------------
def test_single_delete_end_to_end_holds_deletes_and_names_the_actor(monkeypatch):
    w = World(monkeypatch)
    out = w.shop.product("A - Outlet", status="ACTIVE", sku="A")
    w.be.add_shelf_pair("A", "42", outlet_product_id=flows.product_number(out.gid))
    res = w.preview("/outlet/delete", {"product_gid": out.gid})
    assert res["plan"]["keys"] == [["A", "42"]]
    body = {"product_gid": out.gid, "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"],
            "confirm": "CONFERMO", "count": 1}
    rep = w.job("/outlet/delete/apply", w.post("/outlet/delete/apply", body))
    assert rep["status"] == "APPLIED" and rep["report"]["status"] == "DELETED"
    assert out.gid not in w.shop.products
    (hold,) = w.register().audit_rows(KIND_HOLD)
    assert hold.actor == "racoon" and hold.payload["reason"] == "outlet deleted"


def test_single_delete_refuses_a_non_outlet_at_once(monkeypatch):
    w = World(monkeypatch)
    src = w.shop.product("Full price", outlet=False)
    r = w.post("/outlet/delete/preview", {"product_gid": src.gid})
    assert r.status_code == 409 and r.json()["error"]["code"] == "single_delete_not_outlet"


@pytest.mark.parametrize("over,code", [
    ({"confirm": "no"}, "gesture_required"),
    ({"count": 2}, "gesture_required"),
    ({"confirm_token": "nope"}, "confirm_invalid"),
])
def test_single_delete_gestures(monkeypatch, over, code):
    w = World(monkeypatch)
    out = w.shop.product("A - Outlet", sku="A")
    res = w.preview("/outlet/delete", {"product_gid": out.gid})
    body = {"product_gid": out.gid, "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"],
            "confirm": "CONFERMO", "count": 1, **over}
    r = w.post("/outlet/delete/apply", body)
    assert r.status_code == 409 and r.json()["error"]["code"] == code and out.gid in w.shop.products


def test_a_token_of_another_vertical_never_deletes(monkeypatch):
    w = World(monkeypatch)
    out = w.shop.product("A - Outlet", status="DRAFT", sku="A")
    cleanup = w.preview("/outlet/cleanup")               # a cleanup token, bound to its own composite
    single = w.preview("/outlet/delete", {"product_gid": out.gid})
    r = w.post("/outlet/delete/apply", {"product_gid": out.gid, "plan_hash": single["plan_hash"],
                                        "confirm_token": cleanup["confirm_token"], "confirm": "CONFERMO", "count": 1})
    assert r.status_code == 409 and out.gid in w.shop.products


# --- the multi-outlet exit, deny-normalize ---------------------------------------------------
def test_exit_end_to_end(monkeypatch):
    w = World(monkeypatch)
    keep = w.shop.product("A - Outlet", sku="A", promo={"42": 1})
    other = w.shop.product("A - Outlet bis", sku="A", promo={"42": 1})
    res = w.preview("/outlet/exit", {"sku": "A", "keep_product_gid": keep.gid})
    assert res["plan"]["refused"] is None and [o["product_gid"] for o in res["plan"]["outlets"]] == [other.gid]
    body = {"sku": "A", "keep_product_gid": keep.gid, "plan_hash": res["plan_hash"],
            "confirm_token": res["confirm_token"], "confirm": "CONFERMO"}
    assert w.post("/outlet/exit/apply", {**body, "confirm": ""}).status_code == 409
    rep = w.job("/outlet/exit/apply", w.post("/outlet/exit/apply", body))
    assert rep["report"]["status"] == "APPLIED" and other.gid not in w.shop.products and keep.gid in w.shop.products


def test_deny_normalize_end_to_end(monkeypatch):
    w = World(monkeypatch)
    p = w.shop.product("A - Outlet", sku="A", policy="CONTINUE")
    res = w.preview("/outlet/deny-normalize", {"product_gids": [p.gid]})
    assert [x["product_gid"] for x in res["plan"]["products"]] == [p.gid]
    body = {"product_gids": [p.gid], "plan_hash": res["plan_hash"], "confirm_token": res["confirm_token"],
            "confirm": "CONFERMO"}
    assert w.post("/outlet/deny-normalize/apply", {**body, "confirm": "x"}).status_code == 409
    rep = w.job("/outlet/deny-normalize/apply", w.post("/outlet/deny-normalize/apply", body))
    assert rep["report"]["outcomes"][0]["status"] == "NORMALISED"
    assert all(v.policy == "DENY" for v in w.shop.products[p.gid].variants)


# --- the RECONCILE rows: open, a person's close ------------------------------------------------
def test_open_rows_and_a_persons_close(monkeypatch):
    w = World(monkeypatch)
    out = w.shop.product("A - Outlet", sku="A", sizes=("42",))
    row = seed_reconcile(w.register(), action=L.A_ADJUST, keys=(("A", "42"),), purpose=L.P_ADD, share=1,
                         inventory_item_id=out.variants[0].item, location_id=PROMO,
                         product_id=flows.product_number(out.gid), sent=True)
    listed = w.client.get("/outlet/reconcile/open", headers=_auth()).json()["rows"]
    assert [(r["row_id"], r["status"], r["closes_as"]) for r in listed] == [(row, OPEN, HELD)]
    assert w.post("/outlet/reconcile/close", {"row_id": row, "expect_status": OPEN, "why": "x",
                                              "confirm": "CONFERMO"}).status_code == 409          # a reason
    assert w.post("/outlet/reconcile/close", {"row_id": row, "expect_status": OPEN, "why": "checked",
                                              "confirm": "no"}).status_code == 409                # the word
    rep = w.job("/outlet/reconcile/close", w.post("/outlet/reconcile/close", {
        "row_id": row, "expect_status": OPEN, "why": "checked by hand", "confirm": "CONFERMO"}))
    assert rep["status"] == "CLOSED" and rep["closed_as"] == HELD
    (audit,) = [r for r in w.register().audit_rows(KIND_AUDIT) if r.action == "person_close"]
    assert audit.actor == "racoon" and audit.payload["why"] == "checked by hand"
    assert w.client.get("/outlet/reconcile/open", headers=_auth()).json()["rows"] == []


# --- auth, the single slot -----------------------------------------------------------------------
@pytest.mark.parametrize("method,path", [
    ("post", "/outlet/zero-stock"), ("post", "/outlet/cleanup/preview"), ("post", "/outlet/cleanup/apply"),
    ("post", "/outlet/delete/preview"), ("post", "/outlet/delete/apply"), ("post", "/outlet/exit/preview"),
    ("post", "/outlet/exit/apply"), ("post", "/outlet/deny-normalize/preview"),
    ("post", "/outlet/deny-normalize/apply"), ("get", "/outlet/reconcile/open"),
    ("post", "/outlet/reconcile/close"), ("get", "/outlet/cleanup/apply/whatever"),
    ("get", "/outlet/reconcile/close/whatever"),
])
def test_delete_routes_require_auth(monkeypatch, method, path):
    w = World(monkeypatch)
    r = getattr(w.client, method)(path, **({"json": {}} if method == "post" else {}))
    assert r.status_code == 401 and "www-authenticate" in {k.lower() for k in r.headers}


class _StuckExecutor:
    """Never completes its job: the single slot stays taken."""

    def submit(self, fn: Any, *args: Any, **kwargs: Any) -> None:
        pass

    def shutdown(self, *args: Any, **kwargs: Any) -> None:
        pass


def test_single_slot_job_busy_is_409(monkeypatch):
    w = World(monkeypatch, executor=_StuckExecutor())
    assert w.post("/outlet/zero-stock").status_code == 202
    second = w.post("/outlet/zero-stock")
    assert second.status_code == 409 and second.json()["error"]["code"] == "job_busy"
