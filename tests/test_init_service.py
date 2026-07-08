"""backend.services.init_service — the "Inizializza" (init / cutover reconciliation)
vertical (locked spec ``docs/init-reconcile-design.md``).

Every collaborator is an in-memory fake (FakeSheet, resolvers.outlet_resolver,
ops.read_variant_inventory, ops.product_update_status) — NO HTTP, NO live store,
NO live Sheet, per project rules.

Coverage: the "truly online" predicate (all 5 buckets, incl. every false branch:
missing / draft / sold-out-size-zero / sold-out-size-unmatched / multi-match);
online=no rows ignored entirely; preview is genuinely READ-ONLY pre-cutover;
apply ordering (backfill BEFORE read_canonical(assign_uuids=True)); DRAFT dedup
per target_gid; sheet write-back shape (online=NO + Vendute il=today, Europe/Rome,
via an injected clock); kept-online/review rows left untouched; per-row TOCTOU
verify-skip on drift; idempotency (second run against now-offline rows mutates
nothing); baseline stamps reconciled=true on every row via backfill_cutover.

Post-review additions: HIGH-1 (raw-vs-normalized size plan_hash collision);
HIGH-2 (durable BEFORE-snapshot abort gate, ordering before any mutation);
LOW-a (per-SKU error isolation in ``_decide_all``); LOW-b (broadened DRAFT-loop
catch); LOW-d (classify reads ``available``, never ``on_hand``).
"""
from __future__ import annotations

import uuid as _uuidlib
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import pytest

from backend.gsheet.reader import CanonRead, CanonRow, CutoverNotDoneError
from backend.gsheet.writer import BackfillReport, WriteResult
from backend.services import init_service as svc
from backend.services import resolvers
from backend.shopify import ops
from backend.shopify.transport import ShopifyTransportError

PROMO = "gid://shopify/Location/PROMO"
_ROME = ZoneInfo("Europe/Rome")


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------
def _level(loc_id, available, committed=0):
    return {"location_id": loc_id, "location_name": loc_id.split("/")[-1],
            "available": available, "committed": committed, "on_hand": available}


def _variant(vid, size, levels, policy="DENY", truncated=False, sku=None):
    return {"id": vid, "sku": sku, "inventoryPolicy": policy,
            "selectedOptions": [{"name": "Size", "value": size}],
            "inventoryItemId": vid + "/i", "levels": levels, "levels_truncated": truncated}


def _match(gid, status, *, review=False):
    return {"product_gid": gid, "title": "Nike - Outlet", "handle": "nike-outlet",
            "status": status, "is_outlet_member": True, "title_is_outlet": True,
            "review": review, "matched_variant_gids": [gid + "/v"]}


class FakeSheet:
    """In-memory sheet: rows are plain dicts so tests can mutate ``online`` in
    place to model a demotion actually landing, exactly like the real gspread
    write-back would. ``row_uuid`` starts EMPTY for a never-cut sheet (mirrors
    Make's append range) — pre-cutover reads mint an EPHEMERAL, never-persisted
    uuid; ``backfill_cutover`` mints + PERSISTS a real one."""

    def __init__(self, rows: List[Dict[str, Any]]):
        self._rows = [dict(r) for r in rows]
        self.cutover = False
        self.backfill_calls: List[None] = []
        self.read_calls: List[Dict[str, Any]] = []
        self.write_back_calls: List[tuple] = []

    def cutover_done(self) -> bool:
        return self.cutover

    def backfill_cutover(self) -> BackfillReport:
        self.backfill_calls.append(None)
        already_done = self.cutover
        if already_done:
            return BackfillReport(0, already_done=True)
        stamped = 0
        for r in self._rows:
            if not r.get("row_uuid"):
                r["row_uuid"] = _uuidlib.uuid4().hex
            r["reconciled"] = True
            stamped += 1
        self.cutover = True
        return BackfillReport(stamped, already_done=False)

    def read_canonical(self, *, assign_uuids: bool = True, require_cutover: bool = True) -> CanonRead:
        self.read_calls.append({"assign_uuids": assign_uuids, "require_cutover": require_cutover})
        if require_cutover and not self.cutover:
            raise CutoverNotDoneError("fake: cutover not done")
        rows: List[CanonRow] = []
        for r in self._rows:
            row_uuid = r.get("row_uuid") or ""
            if not row_uuid:
                row_uuid = _uuidlib.uuid4().hex  # ephemeral (assign_uuids=False) or minted-not-persisted
                if assign_uuids:
                    r["row_uuid"] = row_uuid
            rows.append(CanonRow(
                row_uuid=row_uuid, sku=r["sku"], size=r["size"],
                product_id=r.get("product_id", ""),
                prezzo_high=r.get("prezzo_high", "100.00"),
                prezzo_outlet=r.get("prezzo_outlet", "50.00"),
                qta=r.get("qta", 1), qta_raw=str(r.get("qta", 1)),
                online=r.get("online", "SI"), sconto="",
                reconciled=bool(r.get("reconciled", False)),
                row_index=0, anomalies=[], raw={},
            ))
        return CanonRead(rows, {}, [])

    def write_back(self, row_uuid, fields, *, expected_sku, product_id_guard=None) -> WriteResult:
        self.write_back_calls.append((row_uuid, dict(fields), expected_sku))
        for r in self._rows:
            if r.get("row_uuid") == row_uuid:
                if r["sku"] != expected_sku:
                    return WriteResult(False, None, "sku_mismatch")
                r.update(fields)
                return WriteResult(True, 1, None)
        return WriteResult(False, None, "row_not_found")


class FakeAudit:
    def __init__(self):
        self.events: List[Dict[str, Any]] = []
        self.init_before_calls: List[Any] = []

    def write_event(self, *, action, target_gids="", plan_hash="", result="") -> None:
        self.events.append({"action": action, "target_gids": target_gids,
                             "plan_hash": plan_hash, "result": result})

    def write_init_before(self, snapshot: Any) -> None:
        self.init_before_calls.append(snapshot)


class RaisingBeforeAudit(FakeAudit):
    """FakeAudit whose ``write_init_before`` raises AFTER recording the call —
    simulates a durable-sink failure (HIGH-2 abort-gate regression)."""

    def write_init_before(self, snapshot: Any) -> None:
        super().write_init_before(snapshot)
        raise RuntimeError("simulated durable before-snapshot write failure")


def _patch_ops(
    monkeypatch, *, outlet_by_sku=None, inv_by_gid=None, draft_raises_for=None,
    resolver_raises_for=None, draft_raises_generic_for=None,
):
    """Patch resolvers.outlet_resolver / ops.read_variant_inventory / ops.product_update_status.

    Returns a call-recorder dict: {"resolver": [...skus...], "inv": [...gids...],
    "draft": [...gids...]}.
    """
    calls = {"resolver": [], "inv": [], "draft": []}
    outlet_by_sku = outlet_by_sku or {}
    inv_by_gid = inv_by_gid or {}
    draft_raises_for = draft_raises_for or set()
    resolver_raises_for = resolver_raises_for or set()
    draft_raises_generic_for = draft_raises_generic_for or set()

    def _resolver(transport, sku):
        calls["resolver"].append(sku)
        if sku in resolver_raises_for:
            raise ShopifyTransportError(f"simulated transport failure for {sku}")
        return outlet_by_sku.get(sku, {"matches": [], "warning": None})

    def _read_inv(transport, gid):
        calls["inv"].append(gid)
        return [dict(v) for v in inv_by_gid.get(gid, [])]

    def _status(transport, gid, status):
        calls["draft"].append((gid, status))
        if gid in draft_raises_for:
            raise ops.ShopifyUserError("productUpdate", [{"field": ["status"], "message": "boom"}])
        if gid in draft_raises_generic_for:
            raise RuntimeError(f"simulated unexpected failure for {gid}")
        return {}

    monkeypatch.setattr(resolvers, "outlet_resolver", _resolver)
    monkeypatch.setattr(ops, "read_variant_inventory", _read_inv)
    monkeypatch.setattr(ops, "product_update_status", _status)
    return calls


# =============================================================================
# _classify — the "truly online" predicate (pure, no I/O)
# =============================================================================
def test_classify_kept_online_when_active_and_size_available():
    idx = {"42": _variant("v1", "42", [_level(PROMO, 3)])}
    inv = [idx["42"]]
    bucket, gid, status = svc._classify(
        "SKU1", "42", [_match("gid://p/1", "ACTIVE")], idx, inv, PROMO
    )
    assert (bucket, gid, status) == (svc.BUCKET_KEPT_ONLINE, "gid://p/1", "ACTIVE")


def test_classify_demote_missing_when_zero_outlet_matches():
    bucket, gid, status = svc._classify("SKU1", "42", [], {}, [], PROMO)
    assert (bucket, gid, status) == (svc.BUCKET_DEMOTE_MISSING, None, None)


@pytest.mark.parametrize("status", ["DRAFT", "ARCHIVED"])
def test_classify_demote_draft_when_status_not_active(status):
    bucket, gid, live_status = svc._classify(
        "SKU1", "42", [_match("gid://p/1", status)], {}, [], PROMO
    )
    assert (bucket, gid, live_status) == (svc.BUCKET_DEMOTE_DRAFT, "gid://p/1", status)


def test_classify_demote_sold_out_size_when_active_but_zero_available():
    idx = {"42": _variant("v1", "42", [_level(PROMO, 0)])}
    inv = [idx["42"]]
    bucket, gid, status = svc._classify(
        "SKU1", "42", [_match("gid://p/1", "ACTIVE")], idx, inv, PROMO
    )
    assert (bucket, gid, status) == (svc.BUCKET_DEMOTE_SOLD_OUT_SIZE, "gid://p/1", "ACTIVE")


def test_classify_demote_sold_out_size_when_size_unmatched():
    idx = {"43": _variant("v1", "43", [_level(PROMO, 5)])}  # only size 43 exists
    inv = [idx["43"]]
    bucket, gid, status = svc._classify(
        "SKU1", "42", [_match("gid://p/1", "ACTIVE")], idx, inv, PROMO  # row wants size 42
    )
    assert (bucket, gid, status) == (svc.BUCKET_DEMOTE_SOLD_OUT_SIZE, "gid://p/1", "ACTIVE")


def test_classify_demote_sold_out_size_when_promo_level_absent():
    idx = {"42": _variant("v1", "42", [])}  # no Promo level at all -> absent -> None
    inv = [idx["42"]]
    bucket, gid, status = svc._classify(
        "SKU1", "42", [_match("gid://p/1", "ACTIVE")], idx, inv, PROMO
    )
    assert (bucket, gid, status) == (svc.BUCKET_DEMOTE_SOLD_OUT_SIZE, "gid://p/1", "ACTIVE")


def test_classify_demote_sold_out_size_reads_available_not_on_hand():
    """LOW-d regression: the _level() builder always sets on_hand == available,
    so no other test pins that the predicate reads 'available'. Here on_hand is
    POSITIVE (physical stock present) but available is 0 (fully committed) —
    classify MUST still demote (reads 'available', never 'on_hand')."""
    level = {"location_id": PROMO, "location_name": PROMO.split("/")[-1],
             "available": 0, "committed": 5, "on_hand": 5}
    idx = {"42": _variant("v1", "42", [level])}
    inv = [idx["42"]]
    bucket, gid, status = svc._classify(
        "SKU1", "42", [_match("gid://p/1", "ACTIVE")], idx, inv, PROMO
    )
    assert (bucket, gid, status) == (svc.BUCKET_DEMOTE_SOLD_OUT_SIZE, "gid://p/1", "ACTIVE")


def test_classify_review_multi_match_when_more_than_one_outlet():
    matches = [_match("gid://p/1", "ACTIVE"), _match("gid://p/2", "ACTIVE")]
    bucket, gid, status = svc._classify("SKU1", "42", matches, {}, [], PROMO)
    assert (bucket, gid, status) == (svc.BUCKET_REVIEW_MULTI_MATCH, None, None)


def test_row_plan_hash_excludes_row_uuid():
    """plan_hash is a function of (sku,size,bucket,target_gid,live_status) only —
    NOT row_uuid (see module docstring: preview's uuid may be ephemeral)."""
    h1 = svc._row_plan_hash("SKU1", "42", svc.BUCKET_KEPT_ONLINE, "gid://p/1", "ACTIVE")
    h2 = svc._row_plan_hash("SKU1", "42", svc.BUCKET_KEPT_ONLINE, "gid://p/1", "ACTIVE")
    assert h1 == h2  # deterministic, and by construction row_uuid never enters it
    h3 = svc._row_plan_hash("SKU1", "42", svc.BUCKET_DEMOTE_DRAFT, "gid://p/1", "DRAFT")
    assert h3 != h1


def test_sold_at_utc_matches_make_iso_format():
    # 10:00 Europe/Rome (CEST, +2) == 08:00 UTC -> Make's created_at format.
    fixed = datetime(2026, 7, 8, 10, 0, tzinfo=_ROME)
    assert svc._sold_at_utc(lambda: fixed) == "2026-07-08T08:00:00.000Z"


# =============================================================================
# init_preview — READ-ONLY, groups online=si rows, ignores online=no
# =============================================================================
def test_preview_online_no_rows_are_ignored_entirely(monkeypatch):
    calls = _patch_ops(monkeypatch)
    sheet = FakeSheet([
        {"sku": "SKU1", "size": "42", "online": "NO"},
    ])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    assert plan.kept_online == plan.demote_missing == plan.demote_draft == ()
    assert plan.demote_sold_out_size == plan.review_multi_match == ()
    assert calls["resolver"] == []  # never even resolved


def test_preview_is_read_only_and_uses_require_cutover_false(monkeypatch):
    _patch_ops(monkeypatch, outlet_by_sku={"SKU1": {"matches": [], "warning": None}})
    sheet = FakeSheet([{"sku": "SKU1", "size": "42", "online": "SI"}])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    assert sheet.backfill_calls == []  # backfill NEVER called by preview
    assert sheet.write_back_calls == []  # no sheet mutation
    assert sheet.read_calls == [{"assign_uuids": False, "require_cutover": False}]
    assert plan.cutover_already_done is False
    assert plan.backfill_pending_rows == 1
    assert len(plan.demote_missing) == 1


def test_preview_buckets_multiple_rows_by_classification(monkeypatch):
    calls = _patch_ops(
        monkeypatch,
        outlet_by_sku={
            "SKU-KEEP": {"matches": [_match("gid://p/keep", "ACTIVE")], "warning": None},
            "SKU-MISS": {"matches": [], "warning": None},
            "SKU-DRAFT": {"matches": [_match("gid://p/draft", "DRAFT")], "warning": None},
            "SKU-MULTI": {"matches": [_match("gid://p/a", "ACTIVE"), _match("gid://p/b", "ACTIVE")], "warning": "MULTI_OUTLET: 2 distinti"},
        },
        inv_by_gid={"gid://p/keep": [_variant("v1", "42", [_level(PROMO, 2)])]},
    )
    sheet = FakeSheet([
        {"sku": "SKU-KEEP", "size": "42", "online": "SI"},
        {"sku": "SKU-MISS", "size": "40", "online": "si"},
        {"sku": "SKU-DRAFT", "size": "41", "online": "SI"},
        {"sku": "SKU-MULTI", "size": "39", "online": "SI"},
    ])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    assert [d.sku for d in plan.kept_online] == ["SKU-KEEP"]
    assert [d.sku for d in plan.demote_missing] == ["SKU-MISS"]
    assert [d.sku for d in plan.demote_draft] == ["SKU-DRAFT"]
    assert [d.sku for d in plan.review_multi_match] == ["SKU-MULTI"]
    assert any("MULTI_OUTLET" in a for a in plan.anomalies)
    # inventory is only fetched for the ACTIVE single-match SKU.
    assert calls["inv"] == ["gid://p/keep"]


def test_decide_all_isolates_sku_error_others_still_classified(monkeypatch):
    """LOW-a regression: one SKU whose resolver raises (known
    ShopifyTransportError OR unexpected) must never abort the whole
    preview/apply — its rows are skipped (absent from every bucket), surfaced
    into anomalies, and every OTHER sku is still classified normally."""
    calls = _patch_ops(
        monkeypatch,
        outlet_by_sku={"SKU-OK": {"matches": [], "warning": None}},
        resolver_raises_for={"SKU-BOOM"},
    )
    sheet = FakeSheet([
        {"sku": "SKU-BOOM", "size": "42", "online": "SI"},
        {"sku": "SKU-OK", "size": "40", "online": "SI"},
    ])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    assert [d.sku for d in plan.demote_missing] == ["SKU-OK"]
    assert plan.kept_online == plan.demote_draft == plan.demote_sold_out_size == ()
    assert plan.review_multi_match == ()
    assert any("SKU-BOOM:sku_error:ShopifyTransportError" in a for a in plan.anomalies)
    assert calls["resolver"] == ["SKU-BOOM", "SKU-OK"]  # SKU-OK still resolved


# =============================================================================
# init_apply — ordering, dedup, write-back shape, kept/review untouched
# =============================================================================
def _fresh_clock():
    fixed = datetime(2026, 7, 8, 9, 30, tzinfo=_ROME)
    return lambda: fixed


def test_apply_calls_backfill_before_read_canonical(monkeypatch):
    _patch_ops(monkeypatch, outlet_by_sku={"SKU1": {"matches": [], "warning": None}})
    sheet = FakeSheet([{"sku": "SKU1", "size": "42", "online": "SI"}])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)

    order: List[str] = []
    orig_backfill = sheet.backfill_cutover
    orig_read = sheet.read_canonical

    def spy_backfill():
        order.append("backfill")
        return orig_backfill()

    def spy_read(**kwargs):
        order.append("read")
        return orig_read(**kwargs)

    sheet.backfill_cutover = spy_backfill
    sheet.read_canonical = spy_read

    svc.init_apply(sheet, transport=object(), approved_plan=plan, audit_sink=None,
                    promo_location_id=PROMO, now=_fresh_clock())
    assert order == ["backfill", "read"]


def test_apply_write_back_shape_and_demote_missing_no_draft(monkeypatch):
    _patch_ops(monkeypatch, outlet_by_sku={"SKU1": {"matches": [], "warning": None}})
    sheet = FakeSheet([{"sku": "SKU1", "size": "42", "online": "SI"}])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    audit = FakeAudit()

    report = svc.init_apply(sheet, transport=object(), approved_plan=plan, audit_sink=audit,
                             promo_location_id=PROMO, now=_fresh_clock())

    assert report.demoted_rows == 1
    assert report.drafted_products == 0  # nothing to draft: product doesn't exist
    assert report.verify_failed_rows == 0
    assert len(sheet.write_back_calls) == 1
    row_uuid, fields, expected_sku = sheet.write_back_calls[0]
    assert expected_sku == "SKU1"
    assert fields == {"online": "NO", "Vendute il": "2026-07-08T07:30:00.000Z"}  # 09:30 Rome == 07:30 UTC
    assert audit.events[0]["action"] == "init_reconcile"


def test_apply_kept_online_and_review_rows_are_untouched(monkeypatch):
    calls = _patch_ops(
        monkeypatch,
        outlet_by_sku={
            "SKU-KEEP": {"matches": [_match("gid://p/keep", "ACTIVE")], "warning": None},
            "SKU-MULTI": {"matches": [_match("gid://p/a", "ACTIVE"), _match("gid://p/b", "ACTIVE")], "warning": None},
        },
        inv_by_gid={"gid://p/keep": [_variant("v1", "42", [_level(PROMO, 2)])]},
    )
    sheet = FakeSheet([
        {"sku": "SKU-KEEP", "size": "42", "online": "SI"},
        {"sku": "SKU-MULTI", "size": "39", "online": "SI"},
    ])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    report = svc.init_apply(sheet, transport=object(), approved_plan=plan, audit_sink=None,
                             promo_location_id=PROMO, now=_fresh_clock())

    assert report.demoted_rows == 0
    assert report.drafted_products == 0
    assert sheet.write_back_calls == []  # NEITHER kept-online NOR review is written
    assert calls["draft"] == []          # multi-match is NEVER drafted
    statuses = {o.sku: o.status for o in report.outcomes}
    assert statuses["SKU-KEEP"] == "KEPT"
    assert statuses["SKU-MULTI"] == "REVIEW"


def test_apply_dedupes_draft_call_per_target_gid_across_sizes(monkeypatch):
    calls = _patch_ops(
        monkeypatch,
        outlet_by_sku={"SKU1": {"matches": [_match("gid://p/1", "ACTIVE")], "warning": None}},
        inv_by_gid={"gid://p/1": [
            _variant("v42", "42", [_level(PROMO, 0)]),   # sold-out
            _variant("v43", "43", [_level(PROMO, 0)]),   # sold-out too — SAME product
        ]},
    )
    sheet = FakeSheet([
        {"sku": "SKU1", "size": "42", "online": "SI"},
        {"sku": "SKU1", "size": "43", "online": "SI"},
    ])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    assert len(plan.demote_sold_out_size) == 2

    report = svc.init_apply(sheet, transport=object(), approved_plan=plan, audit_sink=None,
                             promo_location_id=PROMO, now=_fresh_clock())
    assert report.demoted_rows == 2
    assert report.drafted_products == 1     # ONE product_update_status call...
    assert calls["draft"] == [("gid://p/1", "DRAFT")]  # ...for the ONE distinct gid
    assert len(sheet.write_back_calls) == 2


def test_apply_draft_failure_skips_writeback_for_that_gids_rows(monkeypatch):
    _patch_ops(
        monkeypatch,
        outlet_by_sku={"SKU1": {"matches": [_match("gid://p/1", "DRAFT")], "warning": None}},
        draft_raises_for={"gid://p/1"},
    )
    sheet = FakeSheet([{"sku": "SKU1", "size": "42", "online": "SI"}])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    report = svc.init_apply(sheet, transport=object(), approved_plan=plan, audit_sink=None,
                             promo_location_id=PROMO, now=_fresh_clock())
    assert report.drafted_products == 0
    assert report.demoted_rows == 0
    assert sheet.write_back_calls == []  # sheet NOT touched when the Shopify side failed
    assert report.outcomes[0].status == "DRAFT_FAILED"


def test_apply_draft_unexpected_exception_is_draft_failed_not_propagated(monkeypatch):
    """LOW-b regression: an UNEXPECTED exception (not ShopifyUserError/
    ShopifyTransportError) from product_update_status must NOT propagate — it
    is recorded into failed_gids (DRAFT_FAILED) exactly like a known Shopify
    error, and the sheet cell is left untouched."""
    _patch_ops(
        monkeypatch,
        outlet_by_sku={"SKU1": {"matches": [_match("gid://p/1", "DRAFT")], "warning": None}},
        draft_raises_generic_for={"gid://p/1"},
    )
    sheet = FakeSheet([{"sku": "SKU1", "size": "42", "online": "SI"}])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    report = svc.init_apply(sheet, transport=object(), approved_plan=plan, audit_sink=None,
                             promo_location_id=PROMO, now=_fresh_clock())
    assert report.drafted_products == 0
    assert report.demoted_rows == 0
    assert sheet.write_back_calls == []
    assert report.outcomes[0].status == "DRAFT_FAILED"
    assert "RuntimeError" in report.outcomes[0].warnings[0]


def test_apply_backfill_stamps_reconciled_true_on_every_row(monkeypatch):
    """Part A baseline DoD: backfill_cutover marks EVERY row reconciled=true,
    regardless of the online-reconcile outcome."""
    _patch_ops(monkeypatch, outlet_by_sku={
        "SKU1": {"matches": [], "warning": None},
        "SKU2": {"matches": [], "warning": None},
    })
    sheet = FakeSheet([
        {"sku": "SKU1", "size": "42", "online": "SI"},
        {"sku": "SKU2", "size": "40", "online": "NO"},  # untouched by init, still gets baselined
    ])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    report = svc.init_apply(sheet, transport=object(), approved_plan=plan, audit_sink=None,
                             promo_location_id=PROMO, now=_fresh_clock())
    assert report.backfill_stamped == 2
    assert report.backfill_already_done is False
    assert all(r["reconciled"] is True for r in sheet._rows)


def test_apply_idempotent_second_run_after_demotion_mutates_nothing(monkeypatch):
    calls = _patch_ops(monkeypatch, outlet_by_sku={"SKU1": {"matches": [], "warning": None}})
    sheet = FakeSheet([{"sku": "SKU1", "size": "42", "online": "SI"}])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    svc.init_apply(sheet, transport=object(), approved_plan=plan, audit_sink=None,
                    promo_location_id=PROMO, now=_fresh_clock())
    assert sheet._rows[0]["online"] == "NO"  # demoted

    # Second run: re-preview + re-apply against the NOW-OFFLINE sheet state.
    calls["resolver"].clear()
    plan2 = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    assert plan2.demote_missing == () and plan2.kept_online == ()  # row is online=NO -> ignored
    before_writebacks = len(sheet.write_back_calls)
    before_drafts = len(calls["draft"])
    report2 = svc.init_apply(sheet, transport=object(), approved_plan=plan2, audit_sink=None,
                              promo_location_id=PROMO, now=_fresh_clock())
    assert report2.demoted_rows == 0
    assert report2.drafted_products == 0
    assert len(sheet.write_back_calls) == before_writebacks   # zero NEW mutations
    assert len(calls["draft"]) == before_drafts
    assert report2.backfill_already_done is True              # sentinel already present -> no-op


def test_apply_per_row_toctou_verify_skip_on_drift(monkeypatch):
    """A row approved as demote:missing that has since gained an ACTIVE outlet
    match (live state moved between preview and apply) is VERIFY-skipped, not
    silently demoted with stale assumptions."""
    calls = _patch_ops(monkeypatch, outlet_by_sku={"SKU1": {"matches": [], "warning": None}})
    sheet = FakeSheet([{"sku": "SKU1", "size": "42", "online": "SI"}])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    assert len(plan.demote_missing) == 1

    # Live state moves: an ACTIVE outlet now exists for SKU1, with stock at size 42.
    calls_after = _patch_ops(
        monkeypatch,
        outlet_by_sku={"SKU1": {"matches": [_match("gid://p/new", "ACTIVE")], "warning": None}},
        inv_by_gid={"gid://p/new": [_variant("v1", "42", [_level(PROMO, 5)])]},
    )
    report = svc.init_apply(sheet, transport=object(), approved_plan=plan, audit_sink=None,
                             promo_location_id=PROMO, now=_fresh_clock())
    assert report.verify_failed_rows == 1
    assert report.demoted_rows == 0
    assert sheet.write_back_calls == []
    assert calls_after["draft"] == []
    assert report.outcomes[0].status == "VERIFY_FAILED"


# =============================================================================
# HIGH-1 regression: raw-vs-normalized size plan_hash collision
# =============================================================================
def test_apply_demotes_both_rows_when_raw_size_differs_but_normalizes_equal(monkeypatch):
    """Two same-SKU rows with raw sizes '42' and '42.0' (equal after
    _norm_size) resolving to 0 outlets must BOTH demote cleanly — before the
    fix, ``approved_by_key`` (keyed on (sku, normalized size)) collapsed the
    two approved decisions into one, and the row whose raw-size hash lost the
    collision came back as a false VERIFY_FAILED."""
    _patch_ops(monkeypatch, outlet_by_sku={"SKU1": {"matches": [], "warning": None}})
    sheet = FakeSheet([
        {"sku": "SKU1", "size": "42", "online": "SI"},
        {"sku": "SKU1", "size": "42.0", "online": "SI"},
    ])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    assert len(plan.demote_missing) == 2

    report = svc.init_apply(sheet, transport=object(), approved_plan=plan, audit_sink=None,
                             promo_location_id=PROMO, now=_fresh_clock())
    assert report.demoted_rows == 2
    assert report.verify_failed_rows == 0
    assert all(r["online"] == "NO" for r in sheet._rows)


# =============================================================================
# HIGH-2 regression: durable BEFORE-snapshot abort gate
# =============================================================================
def test_apply_writes_before_snapshot_before_any_mutation(monkeypatch):
    """The durable BEFORE-snapshot must be persisted BEFORE the first
    product_update_status(DRAFT) call and BEFORE the first sheet write_back."""
    _patch_ops(
        monkeypatch,
        outlet_by_sku={"SKU1": {"matches": [_match("gid://p/1", "DRAFT")], "warning": None}},
    )
    sheet = FakeSheet([{"sku": "SKU1", "size": "42", "online": "SI"}])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    audit = FakeAudit()

    order: List[str] = []
    orig_write_init_before = audit.write_init_before
    orig_write_back = sheet.write_back

    def spy_write_init_before(snapshot):
        order.append("init_before")
        return orig_write_init_before(snapshot)

    def spy_write_back(*a, **kw):
        order.append("write_back")
        return orig_write_back(*a, **kw)

    audit.write_init_before = spy_write_init_before
    sheet.write_back = spy_write_back

    from backend.shopify import ops as ops_module
    orig_status = ops_module.product_update_status

    def spy_status(transport, gid, status):
        order.append("draft")
        return orig_status(transport, gid, status)

    monkeypatch.setattr(ops_module, "product_update_status", spy_status)

    report = svc.init_apply(sheet, transport=object(), approved_plan=plan, audit_sink=audit,
                             promo_location_id=PROMO, now=_fresh_clock())

    assert order == ["init_before", "draft", "write_back"]
    assert report.demoted_rows == 1
    assert report.drafted_products == 1
    assert len(audit.init_before_calls) == 1
    snap = audit.init_before_calls[0]
    assert snap.rows[0].sku == "SKU1"
    assert snap.rows[0].prior_online == "SI"
    assert snap.targets[0].gid == "gid://p/1"
    assert snap.targets[0].prior_status == "DRAFT"


def test_apply_aborts_when_before_snapshot_write_fails(monkeypatch):
    """A raising ``write_init_before`` must ABORT the apply entirely: zero
    drafts, zero write-backs — the row is reported SNAPSHOT_ABORTED, not
    silently demoted or left in an ambiguous state."""
    calls = _patch_ops(
        monkeypatch,
        outlet_by_sku={"SKU1": {"matches": [_match("gid://p/1", "DRAFT")], "warning": None}},
    )
    sheet = FakeSheet([{"sku": "SKU1", "size": "42", "online": "SI"}])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    audit = RaisingBeforeAudit()

    report = svc.init_apply(sheet, transport=object(), approved_plan=plan, audit_sink=audit,
                             promo_location_id=PROMO, now=_fresh_clock())

    assert report.drafted_products == 0
    assert report.demoted_rows == 0
    assert calls["draft"] == []               # product_update_status NEVER called
    assert sheet.write_back_calls == []        # sheet NEVER written
    assert report.outcomes[0].status == "SNAPSHOT_ABORTED"
    assert len(audit.init_before_calls) == 1   # the attempt WAS made (and recorded) before raising


def test_apply_threads_approved_plan_hash_into_before_snapshot_and_after_event(monkeypatch):
    """The AFTER audit event and the before-snapshot must both record the REAL
    aggregate plan_hash the caller threads in — never the hardcoded ''."""
    _patch_ops(monkeypatch, outlet_by_sku={"SKU1": {"matches": [], "warning": None}})
    sheet = FakeSheet([{"sku": "SKU1", "size": "42", "online": "SI"}])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    audit = FakeAudit()

    report = svc.init_apply(
        sheet, transport=object(), approved_plan=plan, audit_sink=audit,
        promo_location_id=PROMO, now=_fresh_clock(), approved_plan_hash="agg-hash-xyz",
    )
    assert report.demoted_rows == 1
    assert audit.init_before_calls[0].plan_hash == "agg-hash-xyz"
    assert audit.events[-1]["plan_hash"] == "agg-hash-xyz"


def test_apply_kept_online_only_never_calls_before_snapshot(monkeypatch):
    """No demoted rows -> nothing to snapshot -> write_init_before is never
    called (avoids a pointless AUDIT_INIT row when nothing will be mutated)."""
    _patch_ops(
        monkeypatch,
        outlet_by_sku={"SKU-KEEP": {"matches": [_match("gid://p/keep", "ACTIVE")], "warning": None}},
        inv_by_gid={"gid://p/keep": [_variant("v1", "42", [_level(PROMO, 2)])]},
    )
    sheet = FakeSheet([{"sku": "SKU-KEEP", "size": "42", "online": "SI"}])
    plan = svc.init_preview(sheet, transport=object(), promo_location_id=PROMO)
    audit = FakeAudit()
    report = svc.init_apply(sheet, transport=object(), approved_plan=plan, audit_sink=audit,
                             promo_location_id=PROMO, now=_fresh_clock())
    assert report.demoted_rows == 0
    assert audit.init_before_calls == []
