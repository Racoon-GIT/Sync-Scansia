"""backend.api READ surface on the register — the pure compute core + the FastAPI
TestClient tier.

* PURE tier: the job store's single slot, the inventory join's chips (exact size
  match, twins surfaced, customisation-only out of every count), the failed /
  stale flags, the shelf projection — in-memory fakes, NO network.
* TestClient tier (``importorskip`` fastapi+httpx): ``/scansia``,
  ``/scansia/inventory``, ``/audit`` end to end on the in-memory register and the
  fake Shopify, with Basic Auth.
"""
from __future__ import annotations

import base64
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pytest

from backend.api.inventory import (
    CHIP_IN_SCANSIA,
    CHIP_MISMATCH,
    CHIP_OVERSELL,
    CHIP_PUBLISHED,
    CHIP_SOLD_OUT,
    join_group,
    join_result_to_dict,
    pair_to_dict,
    run_inventory_join,
)
from backend.api.jobs import JOB_DONE, JOB_KIND_INVENTORY, JobBusyError, JobStore, job_record_to_dict
from backend.persistence.records import KIND_AUDIT, InsertAudit
from backend.services import flows, resolvers
from backend.shopify import ops

from tests.fake_shop import PROMO, FakeShop
from tests.register_fixtures import MemoryBackend

_FIXED = datetime(2026, 7, 6, 12, 0, 0, tzinfo=timezone.utc)


def _clock():
    return _FIXED


def _pairs(be: MemoryBackend, *keys, **kw):
    for sku, size in keys:
        be.add_shelf_pair(sku, size, **kw)
    return be.register().shelf()


# ===========================================================================
# JobStore — single slot + lifecycle
# ===========================================================================
def test_jobstore_single_slot_rejects_second_job():
    store = JobStore()
    rec = store.create(JOB_KIND_INVENTORY)
    with pytest.raises(JobBusyError) as ei:
        store.create(JOB_KIND_INVENTORY)
    assert ei.value.active_job_id == rec.job_id


def test_jobstore_slot_frees_after_done_and_after_failed():
    store = JobStore()
    rec = store.create(JOB_KIND_INVENTORY)
    store.mark_running(rec.job_id)
    store.mark_done(rec.job_id, ["result"])
    rec2 = store.create(JOB_KIND_INVENTORY)
    assert store.get(rec.job_id).status == JOB_DONE and store.get(rec.job_id).result == ["result"]
    store.mark_failed(rec2.job_id, "register_error")
    assert store.create(JOB_KIND_INVENTORY).status == "queued"
    assert store.get(rec2.job_id).error_code == "register_error"


def test_job_record_to_dict_shape():
    rec = JobStore(now=_clock).create(JOB_KIND_INVENTORY)
    d = job_record_to_dict(rec)
    assert d["job_id"] == rec.job_id and d["status"] == "queued"
    assert d["result"] is None and d["error_code"] is None and d["created_at"] == _FIXED.isoformat()


# ===========================================================================
# GET /scansia projection
# ===========================================================================
def test_pair_to_dict_carries_the_key_prices_and_b2s_compare_at():
    be = MemoryBackend()
    be.add_shelf_pair("SKU1", "42", price_high=None, price=Decimal("159.00"), price_outlet=Decimal("99.00"),
                      brand="Nike")
    (p,) = be.register().shelf()
    d = pair_to_dict(p)
    assert (d["sku"], d["size"], d["brand"]) == ("SKU1", "42", "Nike")
    assert (d["price_high"], d["price"], d["compare_at"], d["price_outlet"]) == (None, "159.00", "159.00", "99.00")
    assert set(d) >= {"pair_id", "pair_uuid", "outlet_product_id", "online", "discount"}


# ===========================================================================
# The live join (the real ops documents on the fake Shopify)
# ===========================================================================
def test_join_active_with_stock_is_published():
    be, shop = MemoryBackend(), FakeShop()
    out = shop.product("SKU1 - Outlet", promo={"42": 3, "43": 0})
    r = join_group("SKU1", _pairs(be, ("SKU1", "42")), shop, PROMO, now=_clock)
    assert not r.failed and not r.stale and r.product_gid == out.gid
    assert set(r.chips) == {CHIP_IN_SCANSIA, CHIP_PUBLISHED}
    (s,) = r.sizes
    assert (s.size, s.n, s.matched, s.promo_available) == ("42", 1, True, 3)


def test_join_zero_promo_is_sold_out_and_customisation_only_never_counts():
    be, shop = MemoryBackend(), FakeShop()
    out = shop.product("SKU1 - Outlet", promo={"42": 0, "43": 0}, extra_variants=(("Solo Personalizzazione", "SKU1"),))
    shop.promo(out.gid, "Solo Personalizzazione").available = 4
    r = join_group("SKU1", _pairs(be, ("SKU1", "42")), shop, PROMO, now=_clock)
    assert CHIP_SOLD_OUT in r.chips


def test_join_continue_policy_is_oversell_and_suppresses_sold_out():
    be, shop = MemoryBackend(), FakeShop()
    shop.product("SKU1 - Outlet", promo={"42": 0, "43": 0}, policy="CONTINUE")
    r = join_group("SKU1", _pairs(be, ("SKU1", "42")), shop, PROMO, now=_clock)
    assert CHIP_OVERSELL in r.chips and CHIP_SOLD_OUT not in r.chips


def test_join_matches_sizes_exactly_and_surfaces_a_twin():
    be, shop = MemoryBackend(), FakeShop()
    shop.product("SKU1 - Outlet", sizes=("42 L",), promo={"42 L": 1})
    r = join_group("SKU1", _pairs(be, ("SKU1", "42L")), shop, PROMO, now=_clock)
    (s,) = r.sizes
    assert not s.matched and s.twins == ("42 L",) and s.promo_available is None
    assert CHIP_MISMATCH in r.chips and "size_twin:42L:42 L" in r.warnings


def test_join_prefers_the_pairs_outlet_product_id_among_several_outlets():
    be, shop = MemoryBackend(), FakeShop()
    shop.product("SKU1 - Outlet", promo={"42": 0})
    b = shop.product("SKU1 - Outlet bis", promo={"42": 2})
    shelf = _pairs(be, ("SKU1", "42"), outlet_product_id=flows.product_number(b.gid))
    r = join_group("SKU1", shelf, shop, PROMO, now=_clock)
    assert r.product_gid == b.gid and CHIP_MISMATCH not in r.chips


def test_join_several_outlets_and_no_id_is_mismatch_not_failed():
    be, shop = MemoryBackend(), FakeShop()
    shop.product("SKU1 - Outlet")
    shop.product("SKU1 - Outlet bis")
    r = join_group("SKU1", _pairs(be, ("SKU1", "42")), shop, PROMO, now=_clock)
    assert not r.failed and r.product_gid is None and "multi_outlet" in r.warnings and CHIP_MISMATCH in r.chips


def test_join_no_outlet_is_mismatch_not_failed():
    be, shop = MemoryBackend(), FakeShop()
    r = join_group("SKU1", _pairs(be, ("SKU1", "42")), shop, PROMO, now=_clock)
    assert not r.failed and CHIP_MISMATCH in r.chips and "no_outlet" in r.warnings


def test_join_a_raising_read_is_failed_with_no_chips(monkeypatch):
    be, shop = MemoryBackend(), FakeShop()
    shop.product("SKU1 - Outlet")
    shelf = _pairs(be, ("SKU1", "42"))

    def boom(t, gid):
        raise RuntimeError("boom")

    monkeypatch.setattr(ops, "read_variant_inventory", boom)
    r = join_group("SKU1", shelf, shop, PROMO, now=_clock)
    assert r.failed and r.stale and r.chips == () and r.warnings[0].startswith("error:")
    assert r.fetched_at == _FIXED.isoformat()


def test_join_truncated_levels_or_candidates_are_stale(monkeypatch):
    be, shop = MemoryBackend(), FakeShop()
    out = shop.product("SKU1 - Outlet", promo={"42": 0, "43": 0})
    shelf = _pairs(be, ("SKU1", "42"))
    shop.truncated.add(out.gid)
    r = join_group("SKU1", shelf, shop, PROMO, now=_clock)
    assert r.stale and not r.failed and CHIP_SOLD_OUT not in r.chips
    shop.truncated.clear()
    monkeypatch.setattr(resolvers, "outlet_resolver",
                        lambda t, sku: {"matches": [{"product_gid": out.gid, "status": "ACTIVE"}],
                                        "warning": "TRUNCATED", "truncated": True})
    r2 = join_group("SKU1", shelf, shop, PROMO, now=_clock)
    assert r2.stale and not r2.failed


def test_run_inventory_join_groups_the_shelf_by_sku_and_isolates_failures(monkeypatch):
    be, shop = MemoryBackend(), FakeShop()
    shop.product("OK - Outlet", sku="OK", promo={"42": 2})
    bad = shop.product("BAD - Outlet", sku="BAD")
    _pairs(be, ("OK", "42"), ("OK", "42"), ("BAD", "42"))
    real = ops.read_variant_inventory

    def read(t, gid):
        if gid == bad.gid:
            raise RuntimeError("boom")
        return real(t, gid)

    monkeypatch.setattr(ops, "read_variant_inventory", read)
    results = {r.sku: r for r in run_inventory_join(be.register(), shop, PROMO, now=_clock)}
    assert set(results) == {"OK", "BAD"}
    assert results["OK"].sizes[0].n == 2 and not results["OK"].failed
    assert results["BAD"].failed and results["BAD"].chips == ()


# ===========================================================================
# TestClient tier
# ===========================================================================
def _auth():
    return {"Authorization": "Basic " + base64.b64encode(b"racoon:s3cret-pw").decode("ascii")}


class _HoldingExecutor:
    def submit(self, fn: Any, *args: Any, **kwargs: Any) -> None:
        pass

    def shutdown(self, *args: Any, **kwargs: Any) -> None:
        pass


def _client(monkeypatch, *, executor=None):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from backend.api.jobs import SynchronousExecutor
    from backend.app import create_app
    from backend.config import ShopifyConfig

    monkeypatch.delenv("APP_USERNAME", raising=False)
    monkeypatch.setenv("APP_PASSWORD", "s3cret-pw")
    be, shop = MemoryBackend(), FakeShop()
    app = create_app(config=ShopifyConfig("t.myshopify.com", "shpat_x", "2026-10", PROMO),
                     register_factory=lambda **kw: be.register(**kw),
                     transport_factory=lambda: shop,
                     executor=executor or SynchronousExecutor(), promo_location_id=PROMO)
    return TestClient(app), be, shop


def test_endpoint_scansia_returns_the_shelf(monkeypatch):
    client, be, _shop = _client(monkeypatch)
    be.add_shelf_pair("SKU1", "42")
    be.add_pair("SKU1", "43", state="VENDUTO")
    data = client.get("/scansia", headers=_auth()).json()
    assert data["count"] == 1 and [(p["sku"], p["size"]) for p in data["pairs"]] == [("SKU1", "42")]


def test_endpoint_inventory_submit_then_poll(monkeypatch):
    client, be, shop = _client(monkeypatch)
    shop.product("SKU1 - Outlet", promo={"42": 1})
    be.add_shelf_pair("SKU1", "42")
    sub = client.post("/scansia/inventory", headers=_auth())
    assert sub.status_code == 202
    rec = client.get(f"/scansia/inventory/{sub.json()['job_id']}", headers=_auth()).json()
    assert rec["status"] == "done" and rec["result"]["count"] == 1
    (res,) = rec["result"]["results"]
    assert res["sizes"] == [{"size": "42", "n": 1, "matched": True, "promo_available": 1, "twins": [],
                              "foreign_sku": None}]


def test_endpoint_inventory_second_submit_while_busy_is_409(monkeypatch):
    client, _be, _shop = _client(monkeypatch, executor=_HoldingExecutor())
    assert client.post("/scansia/inventory", headers=_auth()).status_code == 202
    assert client.post("/scansia/inventory", headers=_auth()).status_code == 409


def test_endpoint_inventory_poll_unknown_job_is_404(monkeypatch):
    client, _be, _shop = _client(monkeypatch)
    assert client.get("/scansia/inventory/nope", headers=_auth()).status_code == 404


def test_endpoint_audit_reads_the_audit_rows_newest_window(monkeypatch):
    client, be, _shop = _client(monkeypatch)
    for i in range(3):
        be.seed_audit(InsertAudit(kind=KIND_AUDIT, actor="ale", action="product_delete",
                                  payload={"target_gids": f"gid://shopify/Product/{i}", "result": "DELETED"}))
    data = client.get("/audit?limit=2", headers=_auth()).json()
    assert data["count"] == 2 and data["events"][-1]["target_gids"] == "gid://shopify/Product/2"
    assert data["events"][0]["actor"] == "ale"
    assert datetime.fromisoformat(data["events"][0]["ts"]).utcoffset() is not None     # Europe/Rome, zoned


@pytest.mark.parametrize("method,path", [
    ("get", "/scansia"), ("post", "/scansia/inventory"), ("get", "/scansia/inventory/x"), ("get", "/audit"),
])
def test_read_endpoints_require_auth(monkeypatch, method, path):
    client, _be, _shop = _client(monkeypatch)
    assert getattr(client, method)(path).status_code == 401


def test_join_shows_a_title_match_on_a_variant_of_another_sku_f1():
    be, shop = MemoryBackend(), FakeShop()
    shop.product("SKU1 - Outlet", sizes=("43",), promo={"43": 0, "42": 1}, extra_variants=(("42", "OTHER"),))
    r = join_group("SKU1", _pairs(be, ("SKU1", "42")), shop, PROMO, now=_clock)
    (s,) = r.sizes
    assert (s.matched, s.foreign_sku) == (True, "OTHER")
    assert CHIP_MISMATCH in r.chips and "sku_mismatch_on_size:42:OTHER" in r.warnings
    assert join_result_to_dict(r)["sizes"][0]["foreign_sku"] == "OTHER"
