"""backend.gsheet: canonical GSheet layer.

The worksheet is a fake in-memory grid (``FakeWorksheet``) — NO network, NO real
gspread. It implements the slice of the gspread Worksheet API the layer uses
(``get_all_values`` / ``update_cell``) plus ``append_row`` / ``update`` for
completeness, and records ``update_cell`` calls so tests can assert that a
CAS-aborted write performs no mutation.
"""
from __future__ import annotations

import pytest

from backend.gsheet import (
    ANOMALY_MISSING_UUID_ASSIGNED,
    ANOMALY_PRICE_INCOMPLETE,
    ANOMALY_QTA_FRACTION,
    DEFAULT_SI_SET,
    RECONCILED_HEADER,
    SENTINEL_HEADER,
    UUID_HEADER,
    CutoverNotDoneError,
    ScansiaSheet,
)


# ---------------------------------------------------------------------------
# Fake Worksheet
# ---------------------------------------------------------------------------
class FakeWorksheet:
    """In-memory gspread-Worksheet stand-in. Rows stored ragged; get_all_values
    returns a rectangular (padded) grid like real gspread does."""

    def __init__(self, rows):
        self._grid = [list(r) for r in rows]
        self.update_cell_calls = []  # spy: (row, col, value)

    def _max_width(self):
        return max((len(r) for r in self._grid), default=0)

    def get_all_values(self):
        w = self._max_width()
        return [list(r) + [""] * (w - len(r)) for r in self._grid]

    def update_cell(self, row, col, value):
        self.update_cell_calls.append((row, col, value))
        while len(self._grid) < row:
            self._grid.append([])
        r = self._grid[row - 1]
        while len(r) < col:
            r.append("")
        r[col - 1] = "" if value is None else str(value)

    def append_row(self, values):
        self._grid.append(["" if v is None else str(v) for v in values])

    def update(self, *args, **kwargs):  # not used by the layer; present for parity
        raise NotImplementedError("range update not used by backend.gsheet")


# Base header includes unknown columns (Brand/Modello) to prove preservation,
# plus the H/J/Q labelled columns and Qta/online.
BASE_HEADER = [
    "Brand",
    "Modello",
    "SKU",
    "Prezzo High",     # col H -> compareAtPrice
    "Prezzo Outlet",   # col J -> price
    "Size",
    "Qta",
    "online",
    "Product_Id",      # col Q
]


def make_ws(data_rows):
    return FakeWorksheet([BASE_HEADER] + [list(r) for r in data_rows])


def uuid_by_sku(sheet, sku, size=None):
    """Read canonical and return the row_uuid of the (first) row matching sku[/size]."""
    for r in sheet.read_canonical().rows:
        if r.sku == sku and (size is None or r.size == size):
            return r.row_uuid
    raise AssertionError(f"no row for sku={sku} size={size}")


# ---------------------------------------------------------------------------
# parse_qta (pure)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "raw,value,anomaly",
    [
        ("1/3", 1, True),
        ("3/5", 3, True),
        ("2", 2, False),
        ("", 0, False),
        ("nan", 0, False),
        ("None", 0, False),
        ("abc", 0, True),
        ("1,0", 1, False),
        ("1.0", 1, False),
        (None, 0, False),
    ],
)
def test_parse_qta_table(raw, value, anomaly):
    qp = ScansiaSheet.parse_qta(raw)
    assert (qp.value, qp.anomaly) == (value, anomaly)


# ---------------------------------------------------------------------------
# CI-1 — backfill DoD: no re-inflate
# ---------------------------------------------------------------------------
def test_backfill_then_iter_unreconciled_is_empty():
    """Published outlet rows -> backfill_cutover() -> iter_unreconciled() is EMPTY."""
    ws = make_ws(
        [
            ["Nike", "Air", "SKU1", "129,90", "99,90", "42", "1", "SI", "gid://shopify/Product/111"],
            ["Vans", "Era", "SKU2", "89,00", "69,00", "40", "2", "SI", "gid://shopify/Product/222"],
        ]
    )
    sheet = ScansiaSheet(ws)
    report = sheet.backfill_cutover()
    assert report.rows_stamped == 2
    assert report.already_done is False
    # THE DoD: zero pending rows -> zero stock delta.
    assert list(sheet.iter_unreconciled()) == []


def test_backfill_is_idempotent():
    ws = make_ws(
        [["Nike", "Air", "SKU1", "129,90", "99,90", "42", "1", "SI", "gid://shopify/Product/111"]]
    )
    sheet = ScansiaSheet(ws)
    sheet.backfill_cutover()
    second = sheet.backfill_cutover()
    assert second.already_done is True
    assert second.rows_stamped == 0
    assert list(sheet.iter_unreconciled()) == []


# ---------------------------------------------------------------------------
# CI-1 — fail-closed before cutover
# ---------------------------------------------------------------------------
def test_read_canonical_without_sentinel_raises():
    ws = make_ws(
        [["Nike", "Air", "SKU1", "129,90", "99,90", "42", "1", "SI", "gid://shopify/Product/111"]]
    )
    sheet = ScansiaSheet(ws)
    with pytest.raises(CutoverNotDoneError):
        sheet.read_canonical()
    with pytest.raises(CutoverNotDoneError):
        list(sheet.iter_unreconciled())


def test_read_canonical_empty_sheet_raises():
    sheet = ScansiaSheet(FakeWorksheet([]))
    with pytest.raises(CutoverNotDoneError):
        sheet.read_canonical()


# ---------------------------------------------------------------------------
# CI-2 — write-back CAS
# ---------------------------------------------------------------------------
def test_write_product_id_happy_when_q_empty():
    ws = make_ws(
        [["Nike", "Air", "SKU1", "129,90", "99,90", "42", "1", "SI", ""]]  # Q empty
    )
    sheet = ScansiaSheet(ws)
    sheet.backfill_cutover()
    ru = uuid_by_sku(sheet, "SKU1")
    gid = "gid://shopify/Product/999"
    res = sheet.write_product_id(ru, gid, expected_sku="SKU1")
    assert res.ok is True
    # Q cell now holds the GID.
    row = next(r for r in sheet.read_canonical().rows if r.row_uuid == ru)
    assert row.product_id == gid


def test_write_back_aborts_on_sku_mismatch_without_writing():
    ws = make_ws(
        [["Nike", "Air", "SKU1", "129,90", "99,90", "42", "1", "SI", ""]]
    )
    sheet = ScansiaSheet(ws)
    sheet.backfill_cutover()
    ru = uuid_by_sku(sheet, "SKU1")
    before = len(ws.update_cell_calls)
    res = sheet.write_product_id(ru, "gid://shopify/Product/999", expected_sku="WRONG_SKU")
    assert res.ok is False
    assert res.reason == "sku_mismatch"
    # No mutation performed on abort.
    assert len(ws.update_cell_calls) == before


def test_write_product_id_aborts_when_q_already_different_gid():
    ws = make_ws(
        [["Nike", "Air", "SKU1", "129,90", "99,90", "42", "1", "SI", "gid://shopify/Product/111"]]
    )
    sheet = ScansiaSheet(ws)
    sheet.backfill_cutover()
    ru = uuid_by_sku(sheet, "SKU1")
    before = len(ws.update_cell_calls)
    res = sheet.write_product_id(ru, "gid://shopify/Product/222", expected_sku="SKU1")
    assert res.ok is False
    assert res.reason == "product_id_conflict"
    assert len(ws.update_cell_calls) == before


def test_write_back_row_not_found():
    ws = make_ws(
        [["Nike", "Air", "SKU1", "129,90", "99,90", "42", "1", "SI", ""]]
    )
    sheet = ScansiaSheet(ws)
    sheet.backfill_cutover()
    res = sheet.write_product_id("does-not-exist", "gid://x/1", expected_sku="SKU1")
    assert res.ok is False
    assert res.reason == "row_not_found"


# ---------------------------------------------------------------------------
# CI-5 + CI-4 — duplicate (SKU,Size) = distinct returns; one-shot reconcile
# ---------------------------------------------------------------------------
def _cut_sheet_with_two_new_dupes():
    """A cut sheet, then two NEW rows with the same (SKU,Size) appended by 'Make'
    (no uuid, no reconciled)."""
    ws = make_ws(
        [["Nike", "Air", "SKU1", "129,90", "99,90", "42", "1", "SI", "gid://shopify/Product/111"]]
    )
    sheet = ScansiaSheet(ws)
    sheet.backfill_cutover()  # existing row -> reconciled=true
    # Two distinct return events, same SKU+Size (legitimate per CI-5).
    ws.append_row(["Nike", "Air", "SKU9", "129,90", "99,90", "44", "1", "SI", ""])
    ws.append_row(["Nike", "Air", "SKU9", "129,90", "99,90", "44", "3", "SI", ""])
    return sheet


def test_duplicate_sku_size_get_distinct_uuids():
    sheet = _cut_sheet_with_two_new_dupes()
    dupes = [r for r in sheet.read_canonical().rows if r.sku == "SKU9" and r.size == "44"]
    assert len(dupes) == 2
    assert dupes[0].row_uuid != dupes[1].row_uuid  # distinct identity
    assert all(a == [ANOMALY_MISSING_UUID_ASSIGNED] or ANOMALY_MISSING_UUID_ASSIGNED in a
               for a in (dupes[0].anomalies, dupes[1].anomalies))


def test_delta_one_shot_idempotent():
    sheet = _cut_sheet_with_two_new_dupes()
    pending = list(sheet.iter_unreconciled())
    assert {r.sku for r in pending} == {"SKU9"}  # only the two new rows
    assert len(pending) == 2
    first = pending[0]
    res = sheet.mark_reconciled(first.row_uuid, expected_sku=first.sku)
    assert res.ok is True
    # After reconciling ONE, only the other remains — the reconciled row never re-emits.
    remaining = list(sheet.iter_unreconciled())
    assert len(remaining) == 1
    assert remaining[0].row_uuid != first.row_uuid
    # Second read is stable (idempotent).
    assert [r.row_uuid for r in sheet.iter_unreconciled()] == [remaining[0].row_uuid]


# ---------------------------------------------------------------------------
# assign-on-read
# ---------------------------------------------------------------------------
def test_assign_on_read_mints_uuid_and_marks_unreconciled():
    ws = make_ws(
        [["Nike", "Air", "SKU1", "129,90", "99,90", "42", "1", "SI", "gid://shopify/Product/111"]]
    )
    sheet = ScansiaSheet(ws)
    sheet.backfill_cutover()
    ws.append_row(["Nike", "New", "SKU5", "129,90", "99,90", "41", "2", "SI", ""])
    rows = sheet.read_canonical().rows
    new = next(r for r in rows if r.sku == "SKU5")
    assert new.row_uuid  # a uuid was minted
    assert new.reconciled is False
    assert ANOMALY_MISSING_UUID_ASSIGNED in new.anomalies
    # Persisted: a second read sees the SAME uuid (no re-mint).
    again = next(r for r in sheet.read_canonical().rows if r.sku == "SKU5")
    assert again.row_uuid == new.row_uuid


def test_assign_on_read_preserves_existing_reconciled_when_uuid_missing():
    """Anti re-inflate: a row already reconciled=true but with an empty uuid
    (e.g. a manual edit / partial migration) must NOT be forced back to
    pending just because its identity is missing."""
    ws = make_ws(
        [["Nike", "Air", "SKU1", "129,90", "99,90", "42", "1", "SI", "gid://shopify/Product/111"]]
    )
    sheet = ScansiaSheet(ws)
    sheet.backfill_cutover()  # widens header: ... row_uuid(10) reconciled(11) sentinel(12)
    # Simulate a row that already carries reconciled=true but lost/never had a uuid.
    ws.append_row(
        ["Nike", "Old", "SKU7", "129,90", "99,90", "43", "1", "SI", "", "", "true", ""]
    )
    rows = sheet.read_canonical().rows
    row = next(r for r in rows if r.sku == "SKU7")
    assert row.row_uuid  # identity still minted
    assert row.reconciled is True  # NOT resurrected to pending
    assert "SKU7" not in {r.sku for r in sheet.iter_unreconciled()}


# ---------------------------------------------------------------------------
# DRY_RUN — read_canonical(assign_uuids=False) must not mutate the live Sheet
# ---------------------------------------------------------------------------
def test_read_canonical_assign_uuids_false_does_not_mutate_sheet():
    ws = make_ws(
        [["Nike", "Air", "SKU1", "129,90", "99,90", "42", "1", "SI", "gid://shopify/Product/111"]]
    )
    sheet = ScansiaSheet(ws)
    sheet.backfill_cutover()
    ws.append_row(["Nike", "New", "SKU5", "129,90", "99,90", "41", "2", "SI", ""])
    before = len(ws.update_cell_calls)
    result = sheet.read_canonical(assign_uuids=False)
    # No write of any kind performed by the dry-run read.
    assert len(ws.update_cell_calls) == before
    new = next(r for r in result.rows if r.sku == "SKU5")
    assert new.row_uuid  # ephemeral in-memory uuid, still usable by the caller
    assert new.reconciled is False
    # A second dry-run read mints a DIFFERENT ephemeral uuid (never persisted).
    again = next(r for r in sheet.read_canonical(assign_uuids=False).rows if r.sku == "SKU5")
    assert again.row_uuid != new.row_uuid
    assert len(ws.update_cell_calls) == before


# ---------------------------------------------------------------------------
# CI-7 — price incomplete = flagged, not dropped
# ---------------------------------------------------------------------------
def test_price_incomplete_is_flagged_not_dropped():
    ws = make_ws(
        [
            ["Nike", "Air", "SKU1", "129,90", "99,90", "42", "1", "SI", "gid://shopify/Product/111"],
            ["Vans", "Era", "SKU2", "", "69,00", "40", "2", "SI", "gid://shopify/Product/222"],  # H empty
        ]
    )
    sheet = ScansiaSheet(ws)
    sheet.backfill_cutover()
    rows = sheet.read_canonical().rows
    incomplete = next(r for r in rows if r.sku == "SKU2")
    assert ANOMALY_PRICE_INCOMPLETE in incomplete.anomalies
    assert incomplete.prezzo_high is None
    assert incomplete.prezzo_outlet == "69.00"  # J preserved as 2-decimal string
    # Row is present (not dropped).
    assert {r.sku for r in rows} == {"SKU1", "SKU2"}


# ---------------------------------------------------------------------------
# H->compareAt / J->price mapping preserved
# ---------------------------------------------------------------------------
def test_price_mapping_h_compareat_j_price():
    ws = make_ws(
        [["Nike", "Air", "SKU1", "€ 129", "99,90", "42", "1", "SI", "gid://shopify/Product/111"]]
    )
    sheet = ScansiaSheet(ws)
    sheet.backfill_cutover()
    row = sheet.read_canonical().rows[0]
    assert row.prezzo_high == "129.00"   # H -> compareAtPrice, currency stripped
    assert row.prezzo_outlet == "99.90"  # J -> price, IT decimal comma


# ---------------------------------------------------------------------------
# eligibility parity vs legacy inline filter (row-eligibility)
# ---------------------------------------------------------------------------
def _legacy_eligible_uuids(rows):
    """Replicate the live legacy inline filter (sync.py:919-936 / fix_prices.py:218-235):
    online in SI-set AND int(float(qta.replace(',','.'))) > 0 (fraction -> ValueError -> 0)."""
    out = []
    for r in rows:
        if str(r.online).strip().lower() not in DEFAULT_SI_SET:
            continue
        try:
            q = int(float(str(r.qta_raw).replace(",", ".")))
        except Exception:
            q = 0
        if q > 0:
            out.append(r.row_uuid)
    return out


def test_eligible_rows_superset_of_legacy_diff_is_fractions():
    ws = make_ws(
        [
            ["Nike", "A", "SKU1", "129,90", "99,90", "42", "2", "SI", ""],     # eligible both
            ["Nike", "B", "SKU2", "129,90", "99,90", "40", "5", "NO", ""],     # online=NO -> neither
            ["Nike", "C", "SKU3", "129,90", "99,90", "39", "0", "SI", ""],     # qta 0 -> neither
            ["Nike", "D", "SKU4", "129,90", "99,90", "38", "1/3", "SI", ""],   # fraction: new-only
        ]
    )
    sheet = ScansiaSheet(ws)
    sheet.backfill_cutover()
    rows = sheet.read_canonical().rows
    new_eligible = {r.row_uuid for r in sheet.eligible_rows(rows)}
    legacy = set(_legacy_eligible_uuids(rows))
    # Legacy selection is a strict subset; the ONLY difference is the fraction row.
    assert legacy <= new_eligible
    diff = new_eligible - legacy
    frac_uuids = {r.row_uuid for r in rows if ANOMALY_QTA_FRACTION in r.anomalies}
    assert diff == frac_uuids
    # And the plain-eligible row is in both.
    sku1 = next(r for r in rows if r.sku == "SKU1")
    assert sku1.row_uuid in legacy and sku1.row_uuid in new_eligible


def test_eligible_rows_override_returns_all():
    ws = make_ws(
        [["Nike", "B", "SKU2", "129,90", "99,90", "40", "0", "NO", ""]]
    )
    sheet = ScansiaSheet(ws)
    sheet.backfill_cutover()
    rows = sheet.read_canonical().rows
    assert len(sheet.eligible_rows(rows, override=True)) == len(rows)
    assert sheet.eligible_rows(rows) == []  # default filter drops it


# ---------------------------------------------------------------------------
# low-level read_rows retro-compat + unknown-column preservation
# ---------------------------------------------------------------------------
def test_read_rows_retro_compat_shape():
    ws = make_ws(
        [["Nike", "Air", "SKU1", "129,90", "99,90", "42", "1", "SI", "gid://shopify/Product/111"]]
    )
    sheet = ScansiaSheet(ws)
    rows, col_index, out_ws = sheet.read_rows()
    assert out_ws is ws
    assert col_index["product_id"] == 9  # 1-based
    assert rows[0]["_row_index"] == 2    # header=row1, first data row=2
    assert rows[0]["brand"] == "Nike"    # unknown column preserved
    assert rows[0]["prezzo_high"] == "129,90"


def test_control_columns_appended_to_the_right():
    ws = make_ws(
        [["Nike", "Air", "SKU1", "129,90", "99,90", "42", "1", "SI", "gid://shopify/Product/111"]]
    )
    sheet = ScansiaSheet(ws)
    sheet.backfill_cutover()
    header = ws.get_all_values()[0]
    # Control columns live to the RIGHT of the original append range (CI-6).
    assert header[:9] == BASE_HEADER
    for name in (UUID_HEADER, RECONCILED_HEADER, SENTINEL_HEADER):
        assert name in header
        assert header.index(name) >= 9
