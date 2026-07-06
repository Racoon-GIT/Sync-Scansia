"""backend.persistence: HMAC confirm-tokens + sheet-centric audit sink.

All fakes are in-memory (a gspread ``Spreadsheet``/``Worksheet`` stand-in) — NO
network, NO real gspread. Secrets come only from ``monkeypatch``-ed env vars and
are never asserted-on as plaintext beyond the round-trip they enable.
"""
from __future__ import annotations

import json
import types

import pytest

from backend.config import ConfigError
from backend.gsheet.reader import SheetIOError
from backend.persistence.gsheet_audit import (
    TAB_AUDIT,
    TAB_DELETE,
    TAB_PRICE,
    GSheetAuditSink,
)
from backend.persistence.tokens import HmacTokenService
from backend.services.delete_service import (
    BeforeSnapshot,
    DeleteOutcomeEvent,
    SnapshotCollection,
    SnapshotVariant,
)
from backend.services.pricing_service import PriceIntent, ProductPrior, VariantPrior

_SECRET_ENV = "TOKEN_SIGNING_SECRET"


# ===========================================================================
# HmacTokenService
# ===========================================================================
@pytest.fixture
def signing_secret(monkeypatch):
    monkeypatch.setenv(_SECRET_ENV, "unit-test-signing-secret")


def test_token_mint_verify_happy(signing_secret):
    svc = HmacTokenService()
    tok = svc.mint("planhash-abc", ttl_s=300)
    assert isinstance(tok, str) and tok.count(".") == 1
    assert svc.verify(tok, "planhash-abc") is True


def test_token_verify_altered_signature_false(signing_secret):
    svc = HmacTokenService()
    tok = svc.mint("planhash-abc", ttl_s=300)
    payload_b64, sig_b64 = tok.split(".")
    flipped = sig_b64[:-1] + ("A" if sig_b64[-1] != "A" else "B")
    assert svc.verify(f"{payload_b64}.{flipped}", "planhash-abc") is False


def test_token_verify_tampered_payload_false(signing_secret):
    """Forging a longer TTL (rewriting exp) invalidates the signature."""
    import base64

    svc = HmacTokenService()
    tok = svc.mint("planhash-abc", ttl_s=1)
    payload_b64, sig_b64 = tok.split(".")
    pad = "=" * (-len(payload_b64) % 4)
    data = json.loads(base64.urlsafe_b64decode(payload_b64 + pad))
    data["exp"] = data["exp"] + 10_000_000  # attacker extends validity
    forged = base64.urlsafe_b64encode(
        json.dumps(data, separators=(",", ":"), sort_keys=True).encode()
    ).rstrip(b"=").decode()
    assert svc.verify(f"{forged}.{sig_b64}", "planhash-abc") is False


def test_token_verify_expired_false(signing_secret):
    minted = HmacTokenService(now=lambda: 1_000.0)
    tok = minted.mint("planhash-abc", ttl_s=10)  # exp == 1010
    later = HmacTokenService(now=lambda: 5_000.0)
    assert later.verify(tok, "planhash-abc") is False


def test_token_verify_not_yet_expired_true(signing_secret):
    minted = HmacTokenService(now=lambda: 1_000.0)
    tok = minted.mint("planhash-abc", ttl_s=100)  # exp == 1100
    still = HmacTokenService(now=lambda: 1_050.0)
    assert still.verify(tok, "planhash-abc") is True


def test_token_verify_wrong_plan_hash_false(signing_secret):
    svc = HmacTokenService()
    tok = svc.mint("planhash-abc", ttl_s=300)
    assert svc.verify(tok, "planhash-DIFFERENT") is False


def test_token_verify_malformed_false(signing_secret):
    svc = HmacTokenService()
    assert svc.verify("no-dot-here", "planhash-abc") is False
    assert svc.verify("!!!.$$$", "planhash-abc") is False
    assert svc.verify("", "planhash-abc") is False


def test_token_mint_missing_secret_raises(monkeypatch):
    monkeypatch.delenv(_SECRET_ENV, raising=False)
    with pytest.raises(ConfigError, match=_SECRET_ENV):
        HmacTokenService().mint("planhash-abc", ttl_s=300)


def test_token_verify_missing_secret_raises(monkeypatch):
    """Fail-closed on verify too: no secret -> ConfigError, not a silent False."""
    monkeypatch.delenv(_SECRET_ENV, raising=False)
    with pytest.raises(ConfigError, match=_SECRET_ENV):
        HmacTokenService().verify("a.b", "planhash-abc")


def test_token_blank_secret_is_missing(monkeypatch):
    monkeypatch.setenv(_SECRET_ENV, "   ")
    with pytest.raises(ConfigError, match=_SECRET_ENV):
        HmacTokenService().mint("planhash-abc", ttl_s=300)


# ===========================================================================
# GSheetAuditSink — fake gspread Spreadsheet / Worksheet
# ===========================================================================
class FakeAuditWorksheet:
    def __init__(self, title, *, fail_append=False, fail_insert=False):
        self.title = title
        self._rows = []
        self.fail_append = fail_append
        self.fail_insert = fail_insert
        self.append_calls = []
        self.insert_calls = []

    def append_row(self, values):
        self.append_calls.append(list(values))
        if self.fail_append:
            raise RuntimeError("simulated gspread APIError on append")
        self._rows.append(list(values))

    def insert_row(self, values, index=1):
        self.insert_calls.append((list(values), index))
        if self.fail_insert:
            raise RuntimeError("simulated gspread APIError on insert_row")
        self._rows.insert(index - 1, list(values))

    def get_all_values(self):
        w = max((len(r) for r in self._rows), default=0)
        return [list(r) + [""] * (w - len(r)) for r in self._rows]


class FakeSpreadsheet:
    def __init__(self, worksheets=None, *, fail_list=False, fail_add=False, add_fail_append=False):
        self._ws = list(worksheets or [])
        self.fail_list = fail_list
        self.fail_add = fail_add
        self.add_fail_append = add_fail_append
        self.added = []

    def worksheets(self):
        if self.fail_list:
            raise RuntimeError("simulated gspread APIError on worksheets()")
        return list(self._ws)

    def add_worksheet(self, title, rows, cols):
        if self.fail_add:
            raise RuntimeError("simulated gspread APIError on add_worksheet")
        ws = FakeAuditWorksheet(title, fail_append=self.add_fail_append)
        self._ws.append(ws)
        self.added.append((title, rows, cols))
        return ws

    def titles(self):
        return [w.title for w in self._ws]

    def tab(self, title):
        return {w.title: w for w in self._ws}.get(title)


def _snapshot(gid="gid://shopify/Product/1"):
    return BeforeSnapshot(
        product_gid=gid,
        title="Outlet Nike",
        handle="outlet-nike",
        status="ACTIVE",
        tags=("outlet",),
        variants=(
            SnapshotVariant("gid://shopify/ProductVariant/11", "SKU1", "42",
                            "129.00", "199.00", "gid://shopify/InventoryItem/111"),
        ),
        image_srcs=("https://cdn/img.jpg",),
        metafields=({"namespace": "custom", "key": "x", "value": "1"},),
        collections=(SnapshotCollection("gid://shopify/Collection/9", "Outlet", "outlet", False),),
    )


def _intent():
    return PriceIntent(
        mode="percent",
        plan_hash="ph-123",
        priors=(
            ProductPrior(
                product_gid="gid://shopify/Product/1",
                sku="SKU1",
                variants=(
                    VariantPrior("gid://shopify/ProductVariant/11", "199.00", "199.00"),
                    VariantPrior("gid://shopify/ProductVariant/12", None, "199.00"),
                ),
            ),
            ProductPrior(
                product_gid="gid://shopify/Product/2",
                sku="SKU2",
                variants=(VariantPrior("gid://shopify/ProductVariant/21", "89.00", "150.00"),),
            ),
        ),
    )


# --- create-tab-if-missing + write_outcome append --------------------------
def test_write_outcome_creates_tab_and_appends():
    ss = FakeSpreadsheet()  # no tabs yet
    sink = GSheetAuditSink(ss, actor="racoon")
    sink.write_outcome(DeleteOutcomeEvent("gid://shopify/Product/1",
                                          "gid://shopify/Product/1", "DELETED"))
    assert TAB_AUDIT in ss.titles()  # created on first use
    rows = ss.tab(TAB_AUDIT).get_all_values()
    assert rows[0] == ["ts", "actor", "action", "target_gids", "plan_hash", "result"]
    body = rows[1]
    assert body[1] == "racoon"
    assert body[2] == "product_delete"
    assert body[3] == "gid://shopify/Product/1"
    assert "DELETED" in body[5]


def test_write_outcome_default_actor_when_missing():
    ss = FakeSpreadsheet()
    GSheetAuditSink(ss).write_outcome(
        DeleteOutcomeEvent("gid://shopify/Product/1", None, "DELETE_FAILED")
    )
    assert ss.tab(TAB_AUDIT).get_all_values()[1][1] == "unknown"


# --- write_durable append + RAISE gate -------------------------------------
def test_write_durable_appends_snapshot_json():
    ss = FakeSpreadsheet()
    GSheetAuditSink(ss, actor="racoon").write_durable(_snapshot())
    assert TAB_DELETE in ss.titles()
    rows = ss.tab(TAB_DELETE).get_all_values()
    assert rows[0] == ["ts", "product_gid", "snapshot_json"]
    body = rows[1]
    assert body[1] == "gid://shopify/Product/1"
    parsed = json.loads(body[2])
    assert parsed["title"] == "Outlet Nike"
    assert parsed["variants"][0]["sku"] == "SKU1"


def test_write_durable_raises_sheetioerror_on_append_failure():
    """The abort gate: a failed durable write MUST raise (delete never proceeds)."""
    failing = FakeAuditWorksheet(TAB_DELETE, fail_append=True)
    ss = FakeSpreadsheet(worksheets=[failing])
    with pytest.raises(SheetIOError):
        GSheetAuditSink(ss).write_durable(_snapshot())


def test_write_durable_raises_when_add_worksheet_fails():
    ss = FakeSpreadsheet(fail_add=True)  # tab missing AND cannot be created
    with pytest.raises(SheetIOError):
        GSheetAuditSink(ss).write_durable(_snapshot())


def test_write_durable_raises_when_worksheets_listing_fails():
    ss = FakeSpreadsheet(fail_list=True)
    with pytest.raises(SheetIOError):
        GSheetAuditSink(ss).write_durable(_snapshot())


def test_write_durable_raises_sheetioerror_on_unserializable_snapshot():
    """A pre-append serialization failure (asdict/json.dumps) must surface as
    SheetIOError too — not a raw TypeError/ValueError — so the abort gate is
    robust for any future consumer that catches only SheetIOError."""
    ss = FakeSpreadsheet()
    with pytest.raises(SheetIOError):
        GSheetAuditSink(ss).write_durable(object())  # not a dataclass -> asdict() TypeError


# --- capture_before append + load round-trip -------------------------------
def test_capture_before_appends_and_returns_intent_id():
    ss = FakeSpreadsheet()
    sink = GSheetAuditSink(ss)
    intent_id = sink.capture_before(_intent())
    assert isinstance(intent_id, str) and intent_id
    assert TAB_PRICE in ss.titles()
    rows = ss.tab(TAB_PRICE).get_all_values()
    assert rows[0] == ["ts", "intent_id", "mode", "plan_hash", "priors_json"]
    assert rows[1][1] == intent_id
    assert rows[1][2] == "percent"
    assert rows[1][3] == "ph-123"


def test_capture_before_then_load_round_trip():
    ss = FakeSpreadsheet()
    sink = GSheetAuditSink(ss)
    original = _intent()
    intent_id = sink.capture_before(original)
    loaded = sink.load(intent_id)
    assert loaded == original  # frozen dataclasses compare by value (incl. None price)


def test_load_unknown_intent_raises():
    ss = FakeSpreadsheet()
    sink = GSheetAuditSink(ss)
    sink.capture_before(_intent())
    with pytest.raises(SheetIOError):
        sink.load("nonexistent-id")


def test_capture_before_raises_on_append_failure():
    failing = FakeAuditWorksheet(TAB_PRICE, fail_append=True)
    ss = FakeSpreadsheet(worksheets=[failing])
    with pytest.raises(SheetIOError):
        GSheetAuditSink(ss).capture_before(_intent())


# --- reuse existing tab (no duplicate creation) ----------------------------
def test_existing_tab_is_reused_not_recreated():
    existing = FakeAuditWorksheet(TAB_AUDIT)
    existing.append_row(list(("ts", "actor", "action", "target_gids", "plan_hash", "result")))
    ss = FakeSpreadsheet(worksheets=[existing])
    sink = GSheetAuditSink(ss, actor="racoon")
    sink.write_outcome(DeleteOutcomeEvent("gid://shopify/Product/1", "x", "DELETED"))
    sink.write_outcome(DeleteOutcomeEvent("gid://shopify/Product/2", "y", "DELETED"))
    assert ss.added == []  # never called add_worksheet
    assert existing.title == TAB_AUDIT
    # header + two outcome rows
    assert len(existing.get_all_values()) == 3


# --- header repair on a pre-existing, header-less tab ---------------------
def test_capture_before_then_load_repairs_missing_header_on_empty_preexisting_tab():
    """Reproduces the race: add_worksheet succeeded but append_row(header) failed,
    so a retry finds AUDIT_PRICE already listed and EMPTY (no header). Without the
    repair, the first captured intent's row would land at row 0, get mistaken for
    the header by load(), and the revert of that intent would fail silently."""
    empty_price_tab = FakeAuditWorksheet(TAB_PRICE)  # pre-existing, zero rows, no header
    ss = FakeSpreadsheet(worksheets=[empty_price_tab])
    sink = GSheetAuditSink(ss)

    intent_id = sink.capture_before(_intent())
    loaded = sink.load(intent_id)  # must NOT raise "not found"
    assert loaded == _intent()

    rows = ss.tab(TAB_PRICE).get_all_values()
    assert rows[0] == ["ts", "intent_id", "mode", "plan_hash", "priors_json"]  # header restored
    assert rows[1][1] == intent_id          # real data preserved at row 1


def test_read_recent_repairs_missing_header_with_preexisting_data_preserved():
    """A tab created out-of-band already holds a real data row but no header.
    The repair must INSERT the header (shifting existing data down), never
    overwrite/discard it."""
    ws = FakeAuditWorksheet(TAB_AUDIT)
    ws.append_row(["2024-01-01T00:00:00+01:00", "racoon", "product_delete",
                   "gid://shopify/Product/9", "", "DELETED"])  # no header row above it
    ss = FakeSpreadsheet(worksheets=[ws])
    sink = GSheetAuditSink(ss, actor="racoon")

    events = sink.read_recent()
    assert len(events) == 1
    assert events[0]["target_gids"] == "gid://shopify/Product/9"
    assert ws.get_all_values()[0] == ["ts", "actor", "action", "target_gids", "plan_hash", "result"]


def test_tab_with_already_correct_header_is_left_untouched():
    """No-op path: a pre-existing tab whose row 0 already matches the header must
    not be touched (idempotent — no spurious insert_row call)."""
    ws = FakeAuditWorksheet(TAB_AUDIT)
    ws.append_row(["ts", "actor", "action", "target_gids", "plan_hash", "result"])
    ss = FakeSpreadsheet(worksheets=[ws])
    GSheetAuditSink(ss, actor="racoon").read_recent()
    assert ws.insert_calls == []


def test_header_repair_raises_sheetioerror_on_insert_failure():
    failing = FakeAuditWorksheet(TAB_AUDIT, fail_insert=True)  # empty -> header missing
    ss = FakeSpreadsheet(worksheets=[failing])
    with pytest.raises(SheetIOError):
        GSheetAuditSink(ss).read_recent()


# --- from_scansia_sheet reaches sheet.ws.spreadsheet -----------------------
def test_from_scansia_sheet_reuses_ws_spreadsheet():
    ss = FakeSpreadsheet()
    fake_sheet = types.SimpleNamespace(ws=types.SimpleNamespace(spreadsheet=ss))
    sink = GSheetAuditSink.from_scansia_sheet(fake_sheet, actor="racoon")
    sink.write_outcome(DeleteOutcomeEvent("gid://shopify/Product/1", "x", "DELETED"))
    assert TAB_AUDIT in ss.titles()


# --- structural conformance to the ports.AuditSink Protocol ----------------
def test_gsheet_sink_satisfies_ports_protocol():
    from backend.persistence.ports import AuditSink

    sink = GSheetAuditSink(FakeSpreadsheet())
    assert isinstance(sink, AuditSink)  # runtime_checkable structural match
