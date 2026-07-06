"""backend.services.pricing_service — FIX_PRICES successor (preview/apply/revert).

Every collaborator is mocked in-memory (ScansiaSheet, ops, audit_sink) — NO HTTP,
NO live store, NO live Sheet, per project rules. ops.* is patched on the module
object (pricing_service calls through the module), and a single shared call log
records the exact order so the "capture_before BEFORE every push" invariant is
asserted mechanically.

Coverage: mode-1 arithmetic; mode-2 repair (push + skip-Q-empty + fill-missing
REFUSE); mode-3 precedence (per-product override > last bulk rule); price
validation (0 / >=compareAt); revert-capture-before-push + revert re-push;
row-eligibility + ACTIVE-only; skip-if-correct.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import pytest

from backend.gsheet.reader import CanonRead, CanonRow
from backend.gsheet.writer import WriteResult
from backend.services import pricing_service as ps
from backend.shopify import ops

ACTIVE_GID = "gid://shopify/Product/ACTIVE1"
DRAFT_GID = "gid://shopify/Product/DRAFT1"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------

def _row(sku, size, *, qta=1, online="SI", product_id="", prezzo_high="100.00",
         prezzo_outlet="50.00", sconto="", row_uuid=None, raw=None) -> CanonRow:
    return CanonRow(
        row_uuid=row_uuid or f"u-{sku}-{size}",
        sku=sku, size=size, product_id=product_id,
        prezzo_high=prezzo_high, prezzo_outlet=prezzo_outlet,
        qta=qta, qta_raw=str(qta), online=online, sconto=sconto,
        reconciled=False, row_index=0, anomalies=[],
        raw=raw if raw is not None else {"sku": sku, "size": size},
    )


def _variant(vid, price="50.00", compare="100.00"):
    return {"id": vid, "sku": "SKU", "title": "42", "price": price,
            "compareAtPrice": compare, "inventoryItem": {"id": vid + "/i"},
            "selectedOptions": [{"name": "Size", "value": "42"}]}


class FakeSheet:
    def __init__(self, rows: List[CanonRow]):
        self.rows = rows
        self.reads: List[bool] = []
        self.writes: List[tuple] = []

    def read_canonical(self, *, assign_uuids=True) -> CanonRead:
        self.reads.append(assign_uuids)
        return CanonRead(list(self.rows), {}, [])

    def write_back(self, row_uuid, fields, *, expected_sku, product_id_guard=None) -> WriteResult:
        self.writes.append((row_uuid, dict(fields), expected_sku))
        return WriteResult(True, 1, None)


class FakeAudit:
    """Injected audit sink; shares the ops call log so ordering is assertable."""

    def __init__(self, log: List[tuple]):
        self.log = log
        self.store: Dict[str, ps.PriceIntent] = {}
        self._n = 0

    def capture_before(self, intent: ps.PriceIntent) -> str:
        self._n += 1
        intent_id = f"intent-{self._n}"
        self.store[intent_id] = intent
        self.log.append(("capture_before", intent_id, len(intent.priors)))
        return intent_id

    def load(self, intent_id: str) -> ps.PriceIntent:
        return self.store[intent_id]


class OpsMock:
    """Patch ops.* used by pricing_service; record calls into a shared log."""

    def __init__(self, monkeypatch, *, status_by_gid=None, variants_by_gid=None,
                 members=None, log=None):
        self.log: List[tuple] = log if log is not None else []
        self.status_by_gid: Dict[str, str] = status_by_gid or {}
        self.variants_by_gid: Dict[str, list] = variants_by_gid or {}
        # enumerate members: default derived from status_by_gid.
        self.members = members if members is not None else [
            {"id": g, "title": g, "status": s} for g, s in self.status_by_gid.items()
        ]
        monkeypatch.setattr(ops, "enumerate_outlet_products", self._enumerate)
        monkeypatch.setattr(ops, "get_product_variants", self._get_variants)
        monkeypatch.setattr(ops, "product_variants_bulk_update", self._bulk)

    def _enumerate(self, t, collection_gid=ops.OUTLET_COLLECTION_GID):
        self.log.append(("enumerate", collection_gid))
        return list(self.members)

    def _get_variants(self, t, gid):
        self.log.append(("get_variants", gid))
        return [dict(v) for v in self.variants_by_gid.get(gid, [])]

    def _bulk(self, t, gid, variants):
        self.log.append(("push", gid, variants))
        return {}

    def names(self) -> List[str]:
        return [c[0] for c in self.log]


# ---------------------------------------------------------------------------
# Mode 1 — % per product: arithmetic
# ---------------------------------------------------------------------------

def test_mode1_percent_arithmetic(monkeypatch):
    rows = [_row("SKU1", "42", product_id=ACTIVE_GID, prezzo_high="100.00", prezzo_outlet="")]
    OpsMock(monkeypatch, status_by_gid={ACTIVE_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1", price="0.00", compare="0.00")]})
    params = ps.PriceParams(percent_by_sku={"SKU1": 0.30})
    plan = ps.prices_preview(FakeSheet(rows), object(), ps.MODE_PERCENT, params)
    (d,) = plan.diffs
    assert d.price == "70.00"          # round(100 * (1 - 0.30), 2)
    assert d.compare_at == "100.00"    # col H
    assert d.percent == 0.30
    assert d.actionable and d.status == ps.STATUS_OK


def test_mode1_percent_non_round(monkeypatch):
    rows = [_row("SKU1", "42", product_id=ACTIVE_GID, prezzo_high="129.90", prezzo_outlet="")]
    OpsMock(monkeypatch, status_by_gid={ACTIVE_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1", price="1.00", compare="1.00")]})
    params = ps.PriceParams(percent_by_sku={"SKU1": 0.15})
    (d,) = ps.prices_preview(FakeSheet(rows), object(), ps.MODE_PERCENT, params).diffs
    assert d.price == "110.42"         # round(129.90 * 0.85, 2) == 110.415 -> 110.42


# ---------------------------------------------------------------------------
# Mode 2 — direct/repair: push + skip-Q-empty + fill-missing REFUSE
# ---------------------------------------------------------------------------

def test_mode2_skip_q_empty_and_fill_missing_refused(monkeypatch):
    rows = [
        _row("SKU_OK", "42", product_id=ACTIVE_GID, prezzo_high="100.00", prezzo_outlet="50.00"),
        _row("SKU_NOQ", "42", product_id="", prezzo_high="100.00", prezzo_outlet="50.00"),
        _row("SKU_NOPRICE", "42", product_id=ACTIVE_GID, prezzo_high="100.00", prezzo_outlet=None),
    ]
    OpsMock(monkeypatch, status_by_gid={ACTIVE_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1", price="0.00", compare="100.00")]})
    plan = ps.prices_preview(FakeSheet(rows), object(), ps.MODE_DIRECT, ps.PriceParams())
    by_sku = {d.sku: d for d in plan.diffs}

    assert by_sku["SKU_NOQ"].status == ps.STATUS_NO_PRODUCT_ID
    # fill-missing REFUSE: empty prezzo_outlet is NOT back-filled from prezzo_high.
    assert by_sku["SKU_NOPRICE"].status == ps.STATUS_MISSING_PRICE
    assert by_sku["SKU_NOPRICE"].price != "100.00"
    assert by_sku["SKU_OK"].actionable and by_sku["SKU_OK"].price == "50.00"


def test_mode2_repair_apply_pushes_sheet_values(monkeypatch):
    rows = [_row("SKU1", "42", product_id=ACTIVE_GID, prezzo_high="100.00", prezzo_outlet="50.00")]
    log: List[tuple] = []
    m = OpsMock(monkeypatch, log=log, status_by_gid={ACTIVE_GID: "ACTIVE"},
                variants_by_gid={ACTIVE_GID: [_variant("v1", price="0.00", compare="0.00")]})
    sheet = FakeSheet(rows)
    audit = FakeAudit(log)
    plan = ps.prices_preview(sheet, object(), ps.MODE_DIRECT, ps.PriceParams())
    report = ps.prices_apply(sheet, object(), ps.MODE_DIRECT, ps.PriceParams(), plan, audit)

    assert [o.status for o in report.outcomes] == [ps.STATUS_APPLIED]
    pushes = [c for c in log if c[0] == "push"]
    assert pushes and pushes[0][2] == [{"id": "v1", "price": "50.00", "compareAtPrice": "100.00"}]


# ---------------------------------------------------------------------------
# Mode 3 — precedence: per-product override > last matching bulk rule
# ---------------------------------------------------------------------------

def test_mode3_bulk_precedence(monkeypatch):
    rows = [
        _row("SKU_RULE", "42", product_id=ACTIVE_GID, prezzo_high="100.00", prezzo_outlet="",
             raw={"sku": "SKU_RULE", "brand": "Nike"}),
        _row("SKU_OVR", "43", product_id=DRAFT_GID, prezzo_high="100.00", prezzo_outlet="",
             raw={"sku": "SKU_OVR", "brand": "Nike"}),
    ]
    OpsMock(monkeypatch, status_by_gid={ACTIVE_GID: "ACTIVE", DRAFT_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1", "1.00", "1.00")],
                             DRAFT_GID: [_variant("v2", "1.00", "1.00")]})
    params = ps.PriceParams(
        rules=(
            ps.BulkRule(percent=0.20, scope={"brand": ("Nike",)}),
            ps.BulkRule(percent=0.50, scope={"brand": ("Nike",)}),  # overlaps -> last wins
        ),
        override_percent_by_sku={"SKU_OVR": 0.10},                  # override wins over rules
    )
    by_sku = {d.sku: d for d in
              ps.prices_preview(FakeSheet(rows), object(), ps.MODE_BULK, params).diffs}
    assert by_sku["SKU_RULE"].percent == 0.50 and by_sku["SKU_RULE"].price == "50.00"
    assert by_sku["SKU_OVR"].percent == 0.10 and by_sku["SKU_OVR"].price == "90.00"


def test_mode3_out_of_scope_dropped(monkeypatch):
    rows = [_row("SKU_X", "42", product_id=ACTIVE_GID, raw={"sku": "SKU_X", "brand": "Adidas"})]
    OpsMock(monkeypatch, status_by_gid={ACTIVE_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1")]})
    params = ps.PriceParams(rules=(ps.BulkRule(percent=0.5, scope={"brand": ("Nike",)}),))
    plan = ps.prices_preview(FakeSheet(rows), object(), ps.MODE_BULK, params)
    assert plan.diffs == ()  # no rule matched, no override -> not in this operation


# ---------------------------------------------------------------------------
# Validation — reject price 0 / price >= compareAt
# ---------------------------------------------------------------------------

def test_validation_reject_price_ge_compare(monkeypatch):
    rows = [_row("SKU1", "42", product_id=ACTIVE_GID, prezzo_high="100.00", prezzo_outlet="100.00")]
    OpsMock(monkeypatch, status_by_gid={ACTIVE_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1")]})
    (d,) = ps.prices_preview(FakeSheet(rows), object(), ps.MODE_DIRECT, ps.PriceParams()).diffs
    assert d.status == ps.STATUS_PRICE_INVALID and not d.actionable
    assert any("price_ge_compare_at" in w for w in d.warnings)


def test_validation_reject_price_zero(monkeypatch):
    rows = [_row("SKU1", "42", product_id=ACTIVE_GID, prezzo_high="100.00", prezzo_outlet="0.00")]
    OpsMock(monkeypatch, status_by_gid={ACTIVE_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1")]})
    (d,) = ps.prices_preview(FakeSheet(rows), object(), ps.MODE_DIRECT, ps.PriceParams()).diffs
    assert d.status == ps.STATUS_PRICE_INVALID
    assert any("price_zero" in w for w in d.warnings)


def test_apply_never_pushes_invalid(monkeypatch):
    rows = [_row("SKU1", "42", product_id=ACTIVE_GID, prezzo_high="100.00", prezzo_outlet="0.00")]
    log: List[tuple] = []
    OpsMock(monkeypatch, log=log, status_by_gid={ACTIVE_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1")]})
    sheet, audit = FakeSheet(rows), FakeAudit(log)
    plan = ps.prices_preview(sheet, object(), ps.MODE_DIRECT, ps.PriceParams())
    report = ps.prices_apply(sheet, object(), ps.MODE_DIRECT, ps.PriceParams(), plan, audit)
    assert report.intent_id is None
    assert not any(c[0] == "push" for c in log)
    assert not any(c[0] == "capture_before" for c in log)


# ---------------------------------------------------------------------------
# Row-eligibility + status ACTIVE-only
# ---------------------------------------------------------------------------

def test_eligibility_online_and_qty(monkeypatch):
    rows = [
        _row("SKU_ON", "42", online="SI", qta=1, product_id=ACTIVE_GID),
        _row("SKU_OFF", "42", online="NO", qta=5, product_id=ACTIVE_GID),
        _row("SKU_ZERO", "42", online="SI", qta=0, product_id=ACTIVE_GID),
    ]
    OpsMock(monkeypatch, status_by_gid={ACTIVE_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1", "1.00", "1.00")]})
    skus = {d.sku for d in
            ps.prices_preview(FakeSheet(rows), object(), ps.MODE_DIRECT, ps.PriceParams()).diffs}
    assert skus == {"SKU_ON"}          # online=NO and qta=0 excluded


def test_status_active_only_skips_draft(monkeypatch):
    rows = [_row("SKU1", "42", product_id=DRAFT_GID, prezzo_high="100.00", prezzo_outlet="50.00")]
    OpsMock(monkeypatch, status_by_gid={DRAFT_GID: "DRAFT"},
            variants_by_gid={DRAFT_GID: [_variant("v1")]})
    (d,) = ps.prices_preview(FakeSheet(rows), object(), ps.MODE_DIRECT, ps.PriceParams()).diffs
    assert d.status == ps.STATUS_DRAFT and not d.actionable


def test_status_override_includes_draft(monkeypatch):
    rows = [_row("SKU1", "42", product_id=DRAFT_GID, prezzo_high="100.00", prezzo_outlet="50.00")]
    OpsMock(monkeypatch, status_by_gid={DRAFT_GID: "DRAFT"},
            variants_by_gid={DRAFT_GID: [_variant("v1", "0.00", "100.00")]})
    (d,) = ps.prices_preview(FakeSheet(rows), object(), ps.MODE_DIRECT, ps.PriceParams(),
                             status_override=True).diffs
    assert d.actionable and d.status == ps.STATUS_OK


def test_row_override_ignores_eligibility(monkeypatch):
    rows = [_row("SKU_OFF", "42", online="NO", qta=0, product_id=ACTIVE_GID,
                 prezzo_high="100.00", prezzo_outlet="50.00")]
    OpsMock(monkeypatch, status_by_gid={ACTIVE_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1", "0.00", "100.00")]})
    plan = ps.prices_preview(FakeSheet(rows), object(), ps.MODE_DIRECT, ps.PriceParams(),
                             row_override=True)
    assert [d.sku for d in plan.diffs] == ["SKU_OFF"]


# ---------------------------------------------------------------------------
# skip-if-correct (B2)
# ---------------------------------------------------------------------------

def test_skip_if_correct_no_push(monkeypatch):
    rows = [_row("SKU1", "42", product_id=ACTIVE_GID, prezzo_high="100.00", prezzo_outlet="50.00")]
    log: List[tuple] = []
    OpsMock(monkeypatch, log=log, status_by_gid={ACTIVE_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1", price="50.00", compare="100.00")]})
    sheet, audit = FakeSheet(rows), FakeAudit(log)
    plan = ps.prices_preview(sheet, object(), ps.MODE_DIRECT, ps.PriceParams())
    (d,) = plan.diffs
    assert d.status == ps.STATUS_ALREADY_CORRECT and not d.actionable
    report = ps.prices_apply(sheet, object(), ps.MODE_DIRECT, ps.PriceParams(), plan, audit)
    assert report.intent_id is None
    assert not any(c[0] == "push" for c in log)


# ---------------------------------------------------------------------------
# Revert parity — capture BEFORE every push, then re-push priors
# ---------------------------------------------------------------------------

def test_capture_before_precedes_push_and_snapshots_prior(monkeypatch):
    rows = [_row("SKU1", "42", product_id=ACTIVE_GID, prezzo_high="100.00", prezzo_outlet="50.00")]
    log: List[tuple] = []
    OpsMock(monkeypatch, log=log, status_by_gid={ACTIVE_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1", price="0.00", compare="0.00")]})
    sheet, audit = FakeSheet(rows), FakeAudit(log)
    plan = ps.prices_preview(sheet, object(), ps.MODE_DIRECT, ps.PriceParams())
    report = ps.prices_apply(sheet, object(), ps.MODE_DIRECT, ps.PriceParams(), plan, audit)

    ops_names = [c[0] for c in log]
    assert "capture_before" in ops_names and "push" in ops_names
    assert ops_names.index("capture_before") < ops_names.index("push")  # capture BEFORE push
    # captured prior == the live values before the push (0.00 / 0.00)
    intent = audit.store[report.intent_id]
    prior = intent.priors[0].variants[0]
    assert (prior.price, prior.compare_at) == ("0.00", "0.00")


class _BoomAudit:
    """audit_sink whose capture_before raises something OUTSIDE pricing_service's
    ``_CAUGHT`` tuple — e.g. a sink-specific error, not a RuntimeError."""

    def capture_before(self, intent: ps.PriceIntent) -> str:
        raise ValueError("sink exploded")

    def load(self, intent_id: str) -> ps.PriceIntent:
        raise NotImplementedError


def test_apply_capture_before_failure_no_sheet_drift_no_raw_propagation(monkeypatch):
    """A capture_before failure (even one outside _CAUGHT) must not propagate raw
    and must not leave the sheet updated-but-not-pushed: the sheet write for a
    push_set row is DEFERRED until after a successful capture_before."""
    rows = [_row("SKU1", "42", product_id=ACTIVE_GID, prezzo_high="100.00", prezzo_outlet="50.00")]
    log: List[tuple] = []
    OpsMock(monkeypatch, log=log, status_by_gid={ACTIVE_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1", price="0.00", compare="0.00")]})
    sheet = FakeSheet(rows)
    plan = ps.prices_preview(sheet, object(), ps.MODE_DIRECT, ps.PriceParams())

    # does not raise before this point; the ValueError is raised only inside
    # prices_apply's capture_before call, which pricing_service must catch.
    report = ps.prices_apply(sheet, object(), ps.MODE_DIRECT, ps.PriceParams(), plan, _BoomAudit())

    assert report.intent_id is None
    assert report.outcomes[0].status == ps.STATUS_ERROR
    assert "capture_before_failed:ValueError" in report.outcomes[0].warnings[0]
    assert sheet.writes == []                       # no sheet-updated-but-not-pushed drift
    assert not any(c[0] == "push" for c in log)      # nothing pushed


def test_revert_repushes_prior_values(monkeypatch):
    rows = [_row("SKU1", "42", product_id=ACTIVE_GID, prezzo_high="100.00", prezzo_outlet="50.00")]
    log: List[tuple] = []
    OpsMock(monkeypatch, log=log, status_by_gid={ACTIVE_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1", price="0.00", compare="0.00")]})
    sheet, audit = FakeSheet(rows), FakeAudit(log)
    plan = ps.prices_preview(sheet, object(), ps.MODE_DIRECT, ps.PriceParams())
    report = ps.prices_apply(sheet, object(), ps.MODE_DIRECT, ps.PriceParams(), plan, audit)

    log.clear()
    rev = ps.revert_prices(object(), audit, report.intent_id)
    assert rev.reverted_products == 1 and rev.reverted_variants == 1
    pushes = [c for c in log if c[0] == "push"]
    assert pushes[0][2] == [{"id": "v1", "price": "0.00", "compareAtPrice": "0.00"}]


# ---------------------------------------------------------------------------
# TOCTOU — plan_hash drift -> VERIFY_FAILED
# ---------------------------------------------------------------------------

def test_apply_verify_failed_on_status_drift_skips_as_draft(monkeypatch):
    rows = [_row("SKU1", "42", product_id=ACTIVE_GID, prezzo_high="100.00", prezzo_outlet="50.00")]
    m = OpsMock(monkeypatch, status_by_gid={ACTIVE_GID: "ACTIVE"},
                variants_by_gid={ACTIVE_GID: [_variant("v1", "0.00", "0.00")]})
    sheet, audit = FakeSheet(rows), FakeAudit([])
    plan = ps.prices_preview(sheet, object(), ps.MODE_DIRECT, ps.PriceParams())
    # live drifts ACTIVE -> DRAFT between preview and apply.
    m.status_by_gid[ACTIVE_GID] = "DRAFT"
    m.members = [{"id": ACTIVE_GID, "title": "x", "status": "DRAFT"}]
    report = ps.prices_apply(sheet, object(), ps.MODE_DIRECT, ps.PriceParams(), plan, audit)
    # DRAFT is non-actionable now -> surfaced as its skip status, nothing pushed.
    # This drift never reaches the real TOCTOU guard (plan_hash comparison) below
    # because `not d.actionable` short-circuits first — see the two tests after
    # this one for the guard that DOES get exercised (VERIFY_FAILED / NOT_IN_PLAN).
    assert report.intent_id is None
    assert report.outcomes[0].status == ps.STATUS_DRAFT


def test_apply_verify_failed_when_still_actionable_plan_hash_drift(monkeypatch):
    """The REAL TOCTOU guard: SKU stays ACTIVE/actionable but the target price
    (hence plan_hash) drifted between preview and apply — must abort as
    VERIFY_FAILED, never push, never capture_before."""
    rows = [_row("SKU1", "42", product_id=ACTIVE_GID, prezzo_high="100.00", prezzo_outlet="50.00")]
    log: List[tuple] = []
    OpsMock(monkeypatch, log=log, status_by_gid={ACTIVE_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1", price="0.00", compare="0.00")]})
    sheet, audit = FakeSheet(rows), FakeAudit(log)
    plan = ps.prices_preview(sheet, object(), ps.MODE_DIRECT, ps.PriceParams())

    # operator edits col J (prezzo_outlet) on the sheet between preview and apply:
    # SKU stays ACTIVE/actionable, but the computed target price (and plan_hash)
    # drifted from 50.00 to 60.00.
    sheet.rows[0] = sheet.rows[0]._replace(prezzo_outlet="60.00")
    report = ps.prices_apply(sheet, object(), ps.MODE_DIRECT, ps.PriceParams(), plan, audit)

    assert report.intent_id is None
    assert report.outcomes[0].status == ps.STATUS_VERIFY_FAILED
    assert not any(c[0] == "push" for c in log)
    assert not any(c[0] == "capture_before" for c in log)


def test_apply_not_in_plan_when_sku_absent_from_approved(monkeypatch):
    """A fresh, actionable SKU absent from the approved plan -> NOT_IN_PLAN,
    never pushed, never captured."""
    rows = [_row("SKU1", "42", product_id=ACTIVE_GID, prezzo_high="100.00", prezzo_outlet="50.00")]
    log: List[tuple] = []
    OpsMock(monkeypatch, log=log, status_by_gid={ACTIVE_GID: "ACTIVE"},
            variants_by_gid={ACTIVE_GID: [_variant("v1", price="0.00", compare="0.00")]})
    sheet, audit = FakeSheet(rows), FakeAudit(log)
    empty_plan = ps.PricePlan(dry_run=True, mode=ps.MODE_DIRECT, diffs=())

    report = ps.prices_apply(sheet, object(), ps.MODE_DIRECT, ps.PriceParams(), empty_plan, audit)

    assert report.intent_id is None
    assert report.outcomes[0].status == ps.STATUS_NOT_IN_PLAN
    assert not any(c[0] == "push" for c in log)
    assert not any(c[0] == "capture_before" for c in log)


# ---------------------------------------------------------------------------
# Discharge-debt recon (READ-ONLY)
# ---------------------------------------------------------------------------

def test_discharge_debt_count(monkeypatch):
    good, broke_zero, broke_ge, drafted = (
        "gid://shopify/Product/G", "gid://shopify/Product/Z",
        "gid://shopify/Product/GE", "gid://shopify/Product/D",
    )
    OpsMock(
        monkeypatch,
        members=[
            {"id": good, "title": "g", "status": "ACTIVE"},
            {"id": broke_zero, "title": "z", "status": "ACTIVE"},
            {"id": broke_ge, "title": "ge", "status": "ACTIVE"},
            {"id": drafted, "title": "d", "status": "DRAFT"},   # non-ACTIVE -> not scanned
        ],
        variants_by_gid={
            good: [_variant("v1", "50.00", "100.00")],
            broke_zero: [_variant("v2", "0.00", "100.00")],      # price<=0
            broke_ge: [_variant("v3", "100.00", "100.00")],      # price>=compareAt
            drafted: [_variant("v4", "0.00", "0.00")],
        },
    )
    debt = ps.discharge_debt_count(object())
    assert debt.scanned_products == 3
    assert debt.broken_products == 2
    assert debt.broken_variants == 2
    assert set(debt.broken_gids) == {broke_zero, broke_ge}


def test_bad_mode_raises():
    with pytest.raises(ValueError):
        ps.prices_preview(FakeSheet([]), object(), "nope", ps.PriceParams())
