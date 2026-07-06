"""backend.api READ surface — pure compute core + FastAPI TestClient integration.

Two tiers, mirroring the auth foundation:

* PURE tier (always runs, no FastAPI): the job store single-slot guard, the
  inventory-join chip logic, the failed/stale freshness flags, the eligible-row
  projection, and ``GSheetAuditSink.read_recent`` — all with in-memory fakes, NO
  network.
* TestClient tier (``importorskip`` — skipped where FastAPI/httpx are absent,
  e.g. this bare interpreter; runs on the deploy target / CI): the four READ
  endpoints end-to-end with injected fakes and Basic Auth.
"""
from __future__ import annotations

import base64
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pytest

from backend.api.inventory import (
    CHIP_IN_SCANSIA,
    CHIP_MISMATCH,
    CHIP_OVERSELL,
    CHIP_PUBLISHED,
    CHIP_SOLD_OUT,
    canonrow_to_dict,
    join_group,
    read_eligible_rows,
    run_inventory_join,
)
from backend.api.jobs import (
    JOB_DONE,
    JOB_KIND_INVENTORY,
    JobBusyError,
    JobStore,
    SynchronousExecutor,
    job_record_to_dict,
)
from backend.gsheet.reader import CanonRead, CanonRow
from backend.persistence.gsheet_audit import TAB_AUDIT, GSheetAuditSink
from backend.services import resolvers
from backend.shopify import ops

# Reuse the persistence test's in-memory gspread fakes for the audit read.
from tests.test_persistence import FakeSpreadsheet

PROMO = "gid://shopify/Location/PROMO"
MAG = "gid://shopify/Location/MAG"

_FIXED = datetime(2026, 7, 6, 12, 0, 0, tzinfo=timezone.utc)


def _clock():
    return _FIXED


# ---------------------------------------------------------------------------
# Builders (aligned with test_outlet_service fixtures)
# ---------------------------------------------------------------------------
def _row(sku, size, qta, *, reconciled=False, product_id="", row_uuid=None) -> CanonRow:
    return CanonRow(
        row_uuid=row_uuid or f"u-{sku}-{size}",
        sku=sku, size=size, product_id=product_id,
        prezzo_high="100.00", prezzo_outlet="50.00",
        qta=qta, qta_raw=str(qta), online="SI", sconto="",
        reconciled=reconciled, row_index=0, anomalies=[], raw={},
    )


def _level(loc_id, available, committed=0, on_hand=None):
    return {"location_id": loc_id, "location_name": loc_id.split("/")[-1],
            "available": available, "committed": committed,
            "on_hand": available if on_hand is None else on_hand}


def _inv_variant(vid, size, item_id, levels, policy="DENY", truncated=False, sku="SKU1"):
    return {"id": vid, "sku": sku, "inventoryPolicy": policy,
            "selectedOptions": [{"name": "Size", "value": size}],
            "inventoryItemId": item_id, "levels": levels, "levels_truncated": truncated}


def _outlet_match(gid, status):
    return {"product_gid": gid, "title": "Nike - Outlet", "handle": "nike-outlet",
            "status": status, "is_outlet_member": True, "title_is_outlet": True,
            "review": False, "matched_variant_gids": [gid + "/v"]}


class FakeSheet:
    """Minimal ScansiaSheet stand-in: read_canonical returns canned rows."""

    def __init__(self, rows: List[CanonRow]):
        self._rows = rows
        self.reads: List[bool] = []

    def read_canonical(self, *, assign_uuids=True) -> CanonRead:
        self.reads.append(assign_uuids)
        return CanonRead(list(self._rows), {}, [])


class FakeTransport:
    """Not used directly — ops/resolvers are monkeypatched. Placeholder handle."""


# ===========================================================================
# JobStore — single-slot guard + lifecycle
# ===========================================================================
def test_jobstore_single_slot_rejects_second_job():
    store = JobStore()
    rec = store.create(JOB_KIND_INVENTORY)
    with pytest.raises(JobBusyError) as ei:
        store.create(JOB_KIND_INVENTORY)
    assert ei.value.active_job_id == rec.job_id


def test_jobstore_slot_frees_after_done():
    store = JobStore()
    rec = store.create(JOB_KIND_INVENTORY)
    store.mark_running(rec.job_id)
    store.mark_done(rec.job_id, ["result"])
    # slot freed -> a new job is allowed
    rec2 = store.create(JOB_KIND_INVENTORY)
    assert rec2.job_id != rec.job_id
    got = store.get(rec.job_id)
    assert got.status == JOB_DONE and got.result == ["result"]


def test_jobstore_slot_frees_after_failed():
    store = JobStore()
    rec = store.create(JOB_KIND_INVENTORY)
    store.mark_failed(rec.job_id, "sheet_io_error")
    rec2 = store.create(JOB_KIND_INVENTORY)  # must not raise
    assert store.get(rec.job_id).error_code == "sheet_io_error"
    assert rec2.status == "queued"


def test_job_record_to_dict_shape():
    store = JobStore(now=_clock)
    rec = store.create(JOB_KIND_INVENTORY)
    d = job_record_to_dict(rec)
    assert d["job_id"] == rec.job_id
    assert d["status"] == "queued"
    assert d["result"] is None and d["error_code"] is None
    assert d["created_at"] == _FIXED.isoformat()


# ===========================================================================
# GET /scansia projection — eligible rows only
# ===========================================================================
def test_read_eligible_rows_filters_and_uses_dry_read():
    rows = [
        _row("SKU1", "42", 1),                       # eligible
        _row("SKU2", "43", 0),                       # qta 0 -> excluded
        _row("SKU3", "44", 2, reconciled=True),      # reconciled still eligible (online=SI, qta>0)
    ]
    # Force online=NO on one to prove the filter runs.
    rows.append(CanonRow("u-x", "SKU4", "45", "", "100.00", "50.00", 3, "3", "NO", "",
                         False, 0, [], {}))
    sheet = FakeSheet(rows)
    eligible = read_eligible_rows(sheet)
    skus = {r.sku for r in eligible}
    assert skus == {"SKU1", "SKU3"}          # SKU2 (qta0) + SKU4 (online NO) filtered
    assert sheet.reads == [False]            # DRY read: assign_uuids=False (never mutates)


def test_canonrow_to_dict_omits_internal_fields():
    d = canonrow_to_dict(_row("SKU1", "42", 1, product_id="gid://shopify/Product/1"))
    assert d["sku"] == "SKU1" and d["qta"] == 1 and d["product_id"].endswith("/1")
    assert "row_index" not in d and "raw" not in d


# ===========================================================================
# join_group — chip logic + freshness/failed/stale
# ===========================================================================
def test_join_group_active_with_stock_is_published(monkeypatch):
    monkeypatch.setattr(resolvers, "outlet_resolver",
                        lambda t, sku: {"matches": [_outlet_match("gid://shopify/Product/1", "ACTIVE")], "warning": None})
    monkeypatch.setattr(ops, "read_variant_inventory",
                        lambda t, gid: [_inv_variant("v1", "42", "i1", [_level(PROMO, 3)])])
    r = join_group("SKU1", [_row("SKU1", "42", 1)], FakeTransport(), PROMO, now=_clock)
    assert r.failed is False and r.stale is False
    assert r.product_gid == "gid://shopify/Product/1"
    assert CHIP_IN_SCANSIA in r.chips and CHIP_PUBLISHED in r.chips
    assert CHIP_SOLD_OUT not in r.chips
    assert r.fetched_at == _FIXED.isoformat()


def test_join_group_zero_promo_is_sold_out(monkeypatch):
    monkeypatch.setattr(resolvers, "outlet_resolver",
                        lambda t, sku: {"matches": [_outlet_match("gid://shopify/Product/1", "ACTIVE")], "warning": None})
    monkeypatch.setattr(ops, "read_variant_inventory",
                        lambda t, gid: [_inv_variant("v1", "42", "i1", [_level(PROMO, 0)])])
    r = join_group("SKU1", [_row("SKU1", "42", 1)], FakeTransport(), PROMO, now=_clock)
    assert CHIP_SOLD_OUT in r.chips and r.failed is False


def test_join_group_continue_policy_is_oversell(monkeypatch):
    monkeypatch.setattr(resolvers, "outlet_resolver",
                        lambda t, sku: {"matches": [_outlet_match("gid://shopify/Product/1", "ACTIVE")], "warning": None})
    monkeypatch.setattr(ops, "read_variant_inventory",
                        lambda t, gid: [_inv_variant("v1", "42", "i1", [_level(PROMO, 0)], policy="CONTINUE")])
    r = join_group("SKU1", [_row("SKU1", "42", 1)], FakeTransport(), PROMO, now=_clock)
    assert CHIP_OVERSELL in r.chips
    assert CHIP_SOLD_OUT not in r.chips  # oversell suppresses the sold-out conclusion


def test_join_group_unmatched_size_is_mismatch(monkeypatch):
    monkeypatch.setattr(resolvers, "outlet_resolver",
                        lambda t, sku: {"matches": [_outlet_match("gid://shopify/Product/1", "ACTIVE")], "warning": None})
    monkeypatch.setattr(ops, "read_variant_inventory",
                        lambda t, gid: [_inv_variant("v1", "40", "i1", [_level(PROMO, 5)])])
    r = join_group("SKU1", [_row("SKU1", "99", 1)], FakeTransport(), PROMO, now=_clock)  # size 99 not present
    assert CHIP_MISMATCH in r.chips


def test_join_group_no_outlet_is_mismatch_not_failed(monkeypatch):
    monkeypatch.setattr(resolvers, "outlet_resolver",
                        lambda t, sku: {"matches": [], "warning": None})
    r = join_group("SKU1", [_row("SKU1", "42", 1)], FakeTransport(), PROMO, now=_clock)
    assert r.failed is False           # resolve succeeded (found none) -> authoritative
    assert CHIP_MISMATCH in r.chips and r.product_gid is None


def test_join_group_unresolvable_stock_is_failed_non_authoritative(monkeypatch):
    """A live read that RAISES -> failed=True, NO chips (never authoritative)."""
    monkeypatch.setattr(resolvers, "outlet_resolver",
                        lambda t, sku: {"matches": [_outlet_match("gid://shopify/Product/1", "ACTIVE")], "warning": None})

    def _boom(t, gid):
        raise RuntimeError("Product not found for GID")

    monkeypatch.setattr(ops, "read_variant_inventory", _boom)
    r = join_group("SKU1", [_row("SKU1", "42", 1)], FakeTransport(), PROMO, now=_clock)
    assert r.failed is True and r.stale is True
    assert r.chips == ()               # non-authoritative: no sold_out/in_scansia asserted
    assert r.fetched_at == _FIXED.isoformat()
    assert r.warnings and r.warnings[0].startswith("error:")


def test_join_group_truncated_levels_is_stale(monkeypatch):
    monkeypatch.setattr(resolvers, "outlet_resolver",
                        lambda t, sku: {"matches": [_outlet_match("gid://shopify/Product/1", "ACTIVE")], "warning": None})
    monkeypatch.setattr(ops, "read_variant_inventory",
                        lambda t, gid: [_inv_variant("v1", "42", "i1", [_level(PROMO, 0)], truncated=True)])
    r = join_group("SKU1", [_row("SKU1", "42", 1)], FakeTransport(), PROMO, now=_clock)
    assert r.stale is True and r.failed is False
    assert CHIP_SOLD_OUT not in r.chips  # unknown inventory -> no sold-out conclusion


def test_join_group_truncated_resolver_candidates_is_stale(monkeypatch):
    """A truncated resolver candidate set (>100 matches, hasNextPage=true) means a
    second/further outlet page was silently ignored — a single-outlet, 0-stock
    read must NOT be reported as authoritative even though the chip logic itself
    has nothing wrong with it."""
    monkeypatch.setattr(
        resolvers, "outlet_resolver",
        lambda t, sku: {
            "matches": [_outlet_match("gid://shopify/Product/1", "ACTIVE")],
            "warning": "TRUNCATED: set candidati >100",
            "truncated": True,
        },
    )
    monkeypatch.setattr(ops, "read_variant_inventory",
                        lambda t, gid: [_inv_variant("v1", "42", "i1", [_level(PROMO, 0)])])
    r = join_group("SKU1", [_row("SKU1", "42", 1)], FakeTransport(), PROMO, now=_clock)
    assert r.failed is False
    assert r.stale is True                # non-authoritative despite a clean read
    assert CHIP_SOLD_OUT in r.chips       # the chip is still derived, just flagged stale


def test_join_group_truncated_resolver_with_no_chosen_outlet_is_stale(monkeypatch):
    """Truncation also taints the early 'no single outlet' mismatch branch: a
    hidden further page could have resolved the ambiguity."""
    monkeypatch.setattr(
        resolvers, "outlet_resolver",
        lambda t, sku: {"matches": [], "warning": None, "truncated": True},
    )
    r = join_group("SKU1", [_row("SKU1", "42", 1)], FakeTransport(), PROMO, now=_clock)
    assert r.failed is False
    assert CHIP_MISMATCH in r.chips
    assert r.stale is True


def test_run_inventory_join_groups_and_isolates(monkeypatch):
    """Two SKUs: one resolves clean, one raises -> per-group isolation holds."""
    def _resolver(t, sku):
        return {"matches": [_outlet_match(f"gid://shopify/Product/{sku}", "ACTIVE")], "warning": None}

    def _read(t, gid):
        if gid.endswith("BAD"):
            raise RuntimeError("boom")
        return [_inv_variant("v1", "42", "i1", [_level(PROMO, 2)])]

    monkeypatch.setattr(resolvers, "outlet_resolver", _resolver)
    monkeypatch.setattr(ops, "read_variant_inventory", _read)
    sheet = FakeSheet([_row("OK", "42", 1), _row("BAD", "42", 1)])
    results = run_inventory_join(sheet, FakeTransport(), PROMO, now=_clock)
    assert len(results) == 2
    by_sku = {r.sku: r for r in results}
    assert by_sku["OK"].failed is False and CHIP_IN_SCANSIA in by_sku["OK"].chips
    assert by_sku["BAD"].failed is True and by_sku["BAD"].chips == ()
    assert sheet.reads == [False]  # join also uses the DRY read


# ===========================================================================
# GSheetAuditSink.read_recent
# ===========================================================================
def test_audit_read_recent_returns_header_keyed_dicts():
    ss = FakeSpreadsheet()
    sink = GSheetAuditSink(ss, actor="racoon")
    from backend.services.delete_service import DeleteOutcomeEvent
    for i in range(3):
        sink.write_outcome(DeleteOutcomeEvent(f"gid://shopify/Product/{i}", f"gid://shopify/Product/{i}", "DELETED"))
    events = sink.read_recent(limit=2)
    assert len(events) == 2                       # last 2 of 3
    assert events[-1]["target_gids"] == "gid://shopify/Product/2"
    assert events[0]["action"] == "product_delete"


def test_audit_read_recent_empty_tab_returns_empty():
    ss = FakeSpreadsheet()
    sink = GSheetAuditSink(ss)
    # No events yet: the tab is created empty-with-header on first _tab() touch.
    assert sink.read_recent() == []


# ===========================================================================
# FastAPI TestClient tier — RUNS ONLY when `fastapi`/`httpx` are installed in
# the interpreter running pytest (``pytest.importorskip`` inside `_make_client`).
# On a bare local interpreter without those deps, every test below is reported
# as SKIPPED, not failed/passed — this is an intentional, documented local skip,
# not masked. They run for real on the deploy target / CI where the web deps
# are installed.
# ===========================================================================
def _basic(user: str, pw: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{pw}".encode("utf-8")).decode("ascii")


class _HoldingExecutor:
    """``executor.submit``-compatible shim that records the callable WITHOUT ever
    running it — keeps the JobStore's single slot permanently occupied. Used to
    drive a second ``POST /scansia/inventory`` into the 409 ``JobBusyError`` path,
    which ``SynchronousExecutor`` can't exercise (it completes inline and frees
    the slot before a second request could see it busy)."""

    def __init__(self) -> None:
        self.submitted: List[Any] = []

    def submit(self, fn, *args, **kwargs) -> None:
        self.submitted.append((fn, args, kwargs))

    def shutdown(self, *args, **kwargs) -> None:
        pass


def _make_client(monkeypatch, *, rows=None, resolver=None, read_inv=None, audit_ss=None, executor=None):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from backend.app import create_app
    from backend.config import ShopifyConfig

    monkeypatch.delenv("APP_USERNAME", raising=False)
    monkeypatch.setenv("APP_PASSWORD", "s3cret-pw")

    if resolver is not None:
        monkeypatch.setattr(resolvers, "outlet_resolver", resolver)
    if read_inv is not None:
        monkeypatch.setattr(ops, "read_variant_inventory", read_inv)

    cfg = ShopifyConfig("t.myshopify.com", "shpat_x", "2025-07", PROMO)
    sheet = FakeSheet(rows if rows is not None else [])
    audit = GSheetAuditSink(audit_ss if audit_ss is not None else FakeSpreadsheet(), actor="racoon")
    app = create_app(
        config=cfg,
        sheet_factory=lambda: sheet,
        transport_factory=lambda: FakeTransport(),
        audit_factory=lambda: audit,
        # inline by default: POST completes the job before returning; pass
        # executor=_HoldingExecutor() to keep the single slot busy instead.
        executor=executor if executor is not None else SynchronousExecutor(),
        promo_location_id=PROMO,
    )
    return TestClient(app), sheet


def test_endpoint_scansia_returns_eligible_rows(monkeypatch):
    client, _ = _make_client(monkeypatch, rows=[_row("SKU1", "42", 1), _row("SKU2", "43", 0)])
    r = client.get("/scansia", headers={"Authorization": _basic("racoon", "s3cret-pw")})
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1 and body["rows"][0]["sku"] == "SKU1"


def test_endpoint_scansia_requires_auth(monkeypatch):
    client, _ = _make_client(monkeypatch, rows=[_row("SKU1", "42", 1)])
    assert client.get("/scansia").status_code == 401


def test_endpoint_inventory_submit_then_poll(monkeypatch):
    resolver = lambda t, sku: {"matches": [_outlet_match("gid://shopify/Product/1", "ACTIVE")], "warning": None}
    read_inv = lambda t, gid: [_inv_variant("v1", "42", "i1", [_level(PROMO, 0)])]
    client, _ = _make_client(monkeypatch, rows=[_row("SKU1", "42", 1)], resolver=resolver, read_inv=read_inv)
    auth = {"Authorization": _basic("racoon", "s3cret-pw")}
    sub = client.post("/scansia/inventory", headers=auth)
    assert sub.status_code == 202
    job_id = sub.json()["job_id"]
    poll = client.get(f"/scansia/inventory/{job_id}", headers=auth)
    assert poll.status_code == 200
    data = poll.json()
    assert data["status"] == "done"
    res = data["result"]
    assert res["count"] == 1 and res["failed_count"] == 0
    one = res["results"][0]
    assert one["sku"] == "SKU1" and one["failed"] is False
    assert CHIP_SOLD_OUT in one["chips"]
    assert one["fetched_at"]  # freshness present


def test_endpoint_inventory_second_submit_while_busy_is_409(monkeypatch):
    """The single-slot guard end-to-end: with an executor that HOLDS the slot
    (never completes the job inline, unlike ``SynchronousExecutor``), a second
    ``POST /scansia/inventory`` while the first is still queued must 409."""
    client, _ = _make_client(monkeypatch, rows=[_row("SKU1", "42", 1)], executor=_HoldingExecutor())
    auth = {"Authorization": _basic("racoon", "s3cret-pw")}
    first = client.post("/scansia/inventory", headers=auth)
    assert first.status_code == 202
    second = client.post("/scansia/inventory", headers=auth)
    assert second.status_code == 409


def test_endpoint_inventory_failed_outlet_is_non_authoritative(monkeypatch):
    resolver = lambda t, sku: {"matches": [_outlet_match("gid://shopify/Product/1", "ACTIVE")], "warning": None}

    def read_inv(t, gid):
        raise RuntimeError("unresolvable stock")

    client, _ = _make_client(monkeypatch, rows=[_row("SKU1", "42", 1)], resolver=resolver, read_inv=read_inv)
    auth = {"Authorization": _basic("racoon", "s3cret-pw")}
    job_id = client.post("/scansia/inventory", headers=auth).json()["job_id"]
    data = client.get(f"/scansia/inventory/{job_id}", headers=auth).json()
    res = data["result"]
    assert res["failed_count"] == 1
    one = res["results"][0]
    assert one["failed"] is True and one["stale"] is True and one["chips"] == []
    assert one["fetched_at"]
    # never leak the raw RuntimeError message anywhere in the response body
    import json
    assert "unresolvable stock" not in json.dumps(data)


def test_endpoint_inventory_poll_unknown_job_404(monkeypatch):
    client, _ = _make_client(monkeypatch)
    auth = {"Authorization": _basic("racoon", "s3cret-pw")}
    assert client.get("/scansia/inventory/nope", headers=auth).status_code == 404


def test_endpoint_inventory_requires_auth(monkeypatch):
    client, _ = _make_client(monkeypatch)
    assert client.post("/scansia/inventory").status_code == 401


def test_endpoint_audit_reads_mocked_tab(monkeypatch):
    ss = FakeSpreadsheet()
    seed = GSheetAuditSink(ss, actor="racoon")
    from backend.services.delete_service import DeleteOutcomeEvent
    seed.write_outcome(DeleteOutcomeEvent("gid://shopify/Product/7", "gid://shopify/Product/7", "DELETED"))
    client, _ = _make_client(monkeypatch, audit_ss=ss)
    auth = {"Authorization": _basic("racoon", "s3cret-pw")}
    r = client.get("/audit", headers=auth)
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1
    assert body["events"][0]["target_gids"] == "gid://shopify/Product/7"


def test_endpoint_audit_requires_auth(monkeypatch):
    client, _ = _make_client(monkeypatch)
    assert client.get("/audit").status_code == 401
