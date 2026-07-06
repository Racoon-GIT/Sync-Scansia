"""Canonical GSheet reader for the Scansia Manager backend.

This module is the *foundation* of the ``backend.gsheet`` package: it holds the
pure helpers, the single ``_CANON`` synonym map, the dataclasses/errors, and the
read-side of ``ScansiaSheet`` (as ``ReaderMixin``). The write-side lives in
``writer.py``; the concrete ``ScansiaSheet`` composes both in ``__init__.py``.

Derived (behaviour-for-behaviour, verified against source — not a summary) from
the only *live* legacy GSheet code, ``src/sync.py``:
  - ``_norm_key``      -> ``sync.py:43-45``
  - ``_clean_price``   -> ``sync.py:47-60``  (returns a 2-decimal *string*)
  - ``_truthy_si``     -> ``sync.py:62-70``  (synonym set for online=SI)
  - ``read_rows``      -> ``sync.py:108-128`` (``gs_read_rows``, header-name
                          addressing, 1-based ``_row_index`` for write-back)
  - ``parse_qta``      -> compat with the dead-but-referenced ``utils.parse_qty``
                          (``src/utils.py:97-108``) / ``sanitize_quantity``
                          (``:199-231``): ``"1/3"`` -> numerator ``1`` + anomaly.

No pandas / CSV. No network at import time (``gspread`` is imported lazily in the
``ScansiaSheet.open()`` factory). Columns are addressed by *normalized header
name*, never by fixed index.
"""
from __future__ import annotations

import re
import uuid
from typing import Any, Dict, Iterator, List, NamedTuple, Optional

# ---------------------------------------------------------------------------
# Control columns (pinned to the RIGHT of Make's append range — see CI-6).
# ---------------------------------------------------------------------------
UUID_HEADER = "row_uuid"
RECONCILED_HEADER = "reconciled"
SENTINEL_HEADER = "_scansia_cutover"  # header-cell presence == cutover done

# Anomaly kinds surfaced on CanonRow.anomalies / CanonRead.anomalies.
ANOMALY_QTA_FRACTION = "qta_fraction"
ANOMALY_QTA_UNPARSEABLE = "qta_unparseable"
ANOMALY_PRICE_INCOMPLETE = "price_incomplete"
ANOMALY_MISSING_UUID_ASSIGNED = "missing_uuid_assigned"

# online=SI synonym set — verbatim from sync._truthy_si (sync.py:69).
DEFAULT_SI_SET = frozenset({"si", "sì", "true", "1", "x", "ok", "yes"})


# ---------------------------------------------------------------------------
# Pure helpers (no I/O, unit-testable in isolation).
# ---------------------------------------------------------------------------
def _norm_key(k: Any) -> str:
    """Normalize a header/key. Verbatim from sync._norm_key (sync.py:43-45)."""
    return (k or "").strip().lower().replace("-", "_").replace(" ", "_")


def _clean_price(v: Any) -> Optional[str]:
    """'€ 129', '129,90' -> '129.90'. Verbatim behaviour of sync._clean_price.

    Returns a 2-decimal *string* (not float) or ``None`` when empty/unparseable.
    """
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    s2 = re.sub(r"[^\d,\.]", "", s)
    if s2.count(",") == 1 and s2.count(".") == 0:
        s2 = s2.replace(",", ".")
    try:
        return f"{float(s2):.2f}"
    except Exception:
        return None


def _truthy_si(v: Any, allowed: frozenset = DEFAULT_SI_SET) -> bool:
    """Is ``v`` truthy-'SI'? Verbatim from sync._truthy_si (sync.py:62-70).

    ``allowed`` overrides the string synonym set (used by ``eligible_rows`` to let
    an operator pass a custom online-value set); bool/int semantics are preserved.
    """
    if v is True:
        return True
    if isinstance(v, bool):  # v is False
        return False
    if isinstance(v, (int, float)):
        return int(v) == 1
    if isinstance(v, str):
        return v.strip().lower() in allowed
    return False


# ---------------------------------------------------------------------------
# Single synonym map: normalized-header -> canonical field.
# Derived from sync._norm_key raw keys (the live reader exposes NO synonyms) +
# the dead gsheets.py/utils.py alt-spellings, unified here on purpose.
# ---------------------------------------------------------------------------
_CANON: Dict[str, str] = {
    # Prezzo High (col H) -> Shopify compareAtPrice
    "prezzo_high": "prezzo_high",
    "prezzo_pieno": "prezzo_high",
    "full_price": "prezzo_high",
    # Prezzo Outlet (col J) -> Shopify price
    "prezzo_outlet": "prezzo_outlet",
    "prezzo_scontato": "prezzo_outlet",
    "sale_price": "prezzo_outlet",
    # Product_Id (col Q) -> product GID target / write-back target
    "product_id": "product_id",
    # variant match key (Size primary, SKU secondary)
    "size": "size",
    "taglia": "size",
    # single-event return quantity (delta, NOT absolute stock)
    "qta": "qta",
    "qty": "qta",
    # eligibility flag
    "online": "online",
    # discount %
    "sconto": "sconto",
    "discount": "sconto",
    # SKU (grouping + CAS identity guard)
    "sku": "sku",
    # control columns (identity + reconciliation state)
    UUID_HEADER: "row_uuid",
    RECONCILED_HEADER: "reconciled",
}


# ---------------------------------------------------------------------------
# Result / row types (tuple-unpackable AND attribute-accessible).
# ---------------------------------------------------------------------------
class QtaParse(NamedTuple):
    value: int
    anomaly: bool
    raw: str


class Anomaly(NamedTuple):
    row_uuid: str
    sku: str
    kind: str
    detail: str


class RawRead(NamedTuple):
    rows: List[Dict[str, Any]]
    col_index: Dict[str, int]
    ws: Any


class CanonRow(NamedTuple):
    row_uuid: str
    sku: str
    size: str
    product_id: str            # Q value: full GID, handle, or "" (not yet published)
    prezzo_high: Optional[str]  # cleaned 2-decimal str or None -> compareAtPrice
    prezzo_outlet: Optional[str]  # cleaned 2-decimal str or None -> price
    qta: int                   # parse_qta value (numerator for fractions)
    qta_raw: str
    online: str                # raw online cell
    sconto: str
    reconciled: bool
    row_index: int             # 1-based sheet row — WRITE-BACK ADDRESS ONLY, never identity
    anomalies: List[str]
    raw: Dict[str, Any]        # full normalized-header dict (preserves unknown cols)


class CanonRead(NamedTuple):
    rows: List[CanonRow]
    col_index: Dict[str, int]
    anomalies: List[Anomaly]


# ---------------------------------------------------------------------------
# Typed errors — never leak raw gspread/Exception at the API boundary.
# ---------------------------------------------------------------------------
class GSheetError(RuntimeError):
    """Base for backend.gsheet errors."""


class CutoverNotDoneError(GSheetError):
    """Raised by read_canonical/iter_unreconciled when the cutover sentinel is
    absent — fail-closed so historical rows are NEVER auto-treated as pending."""


class RowNotFoundError(GSheetError):
    """A row_uuid could not be located on re-read."""


class CASMismatchError(GSheetError):
    """A compare-and-swap guard failed (surfaced as WriteResult.ok=False)."""


class SheetIOError(GSheetError):
    """Underlying worksheet I/O failed."""


def _new_uuid() -> str:
    return str(uuid.uuid4())


def _build_col_index(header: List[str]) -> Dict[str, int]:
    """normalized-header -> 1-based column index (verbatim from gs_read_rows)."""
    return {_norm_key(h): i + 1 for i, h in enumerate(header)}


def _cell(row: List[str], col_1based: Optional[int]) -> str:
    """Safe positional cell fetch (row may be shorter than the header)."""
    if not col_1based:
        return ""
    i = col_1based - 1
    return row[i] if 0 <= i < len(row) else ""


class ReaderMixin:
    """Read-side of ``ScansiaSheet``. Requires ``self.ws`` (a gspread Worksheet
    or a compatible object exposing ``get_all_values``/``update_cell``)."""

    ws: Any  # set by ScansiaSheet.__init__

    # -- pure API (static; testable without a worksheet) -------------------
    @staticmethod
    def parse_qta(raw: Any) -> QtaParse:
        """Parse a quantity cell. Compat with legacy utils.parse_qty/sanitize_quantity.

        - ""/"nan"/"None"        -> (0, False, raw)
        - "N/M" (contains '/')   -> (int(float(N)), True, raw)   # numerator + anomaly
        - "2" / "1.0" / "1,0"    -> (int(float(v.replace(',','.'))), False, raw)
        - unparseable ("abc")    -> (0, True, raw)                # flagged, never raises
        """
        s = "" if raw is None else str(raw).strip()
        if s == "" or s.lower() == "nan" or s == "None":
            return QtaParse(0, False, s)
        try:
            if "/" in s:
                num, _rest = s.split("/", 1)
                return QtaParse(int(float(num.strip())), True, s)
            return QtaParse(int(float(s.replace(",", "."))), False, s)
        except Exception:
            return QtaParse(0, True, s)

    @staticmethod
    def eligible_rows(
        rows: List[CanonRow],
        *,
        online_values: frozenset = DEFAULT_SI_SET,
        min_qty: int = 0,
        override: bool = False,
    ) -> List[CanonRow]:
        """Legacy-parity eligibility filter: online=SI AND qta > min_qty.

        ``override=True`` returns rows unfiltered (explicit operator choice).
        NOTE (deliberate divergence, SPEC §5.1): a ``qta_fraction`` row ("1/3")
        canonicalizes to value=1 and IS eligible here, whereas the legacy inline
        filter dropped it (int(float("1/3")) -> ValueError -> 0). So
        ``legacy_selected ⊆ eligible_rows`` with the diff being exactly the
        flagged fraction rows.
        """
        if override:
            return list(rows)
        out: List[CanonRow] = []
        for r in rows:
            if not _truthy_si(r.online, online_values):
                continue
            if r.qta <= min_qty:
                continue
            out.append(r)
        return out

    # -- low-level read (retro-compat with gs_read_rows) -------------------
    def read_rows(self) -> RawRead:
        """Low-level read: (rows, col_index, ws). No canonicalization, no writes.

        Faithful to sync.gs_read_rows: each row dict is keyed by normalized header
        (fallback ``col{n}`` for cells past the header) plus 1-based ``_row_index``.
        """
        try:
            values = self.ws.get_all_values()
        except Exception as e:  # pragma: no cover - passthrough as typed error
            raise SheetIOError(f"get_all_values failed: {e}") from e
        if not values:
            return RawRead([], {}, self.ws)
        header = values[0]
        col_index = _build_col_index(header)
        rows: List[Dict[str, Any]] = []
        for row_idx, row in enumerate(values[1:], start=2):
            m: Dict[str, Any] = {}
            for i, cell in enumerate(row):
                key = _norm_key(header[i]) if i < len(header) else f"col{i + 1}"
                m[key] = cell
            m["_row_index"] = row_idx
            rows.append(m)
        return RawRead(rows, col_index, self.ws)

    # -- canonical read (fail-closed on cutover) ---------------------------
    def read_canonical(self, *, assign_uuids: bool = True) -> CanonRead:
        """Canonical read. FAIL-CLOSED: raises ``CutoverNotDoneError`` if the
        cutover sentinel is absent (never auto-marks historical rows pending).

        Side-effect (when ``assign_uuids=True``, the default): assigns a
        ``row_uuid`` (and explicit ``reconciled=false``, unless the ``reconciled``
        cell already carries a value — see below) to any row lacking one — the
        stable id is minted at first read (SPEC §5.2).

        ``assign_uuids=False`` (DRY_RUN / read-only mode): NO write is performed.
        Rows lacking a ``row_uuid`` get an *ephemeral*, in-memory-only uuid (never
        persisted to the sheet) so iteration still works without mutating the
        live Sheet. Callers MUST pass ``assign_uuids=False`` whenever DRY_RUN is
        active (project default — see CLAUDE.md rule 2).

        Anti-re-inflate guard: a row whose ``reconciled`` cell is already
        populated (e.g. ``true``) but whose ``row_uuid`` is empty is NEVER forced
        back to ``reconciled=false`` — only a genuinely empty ``reconciled`` cell
        is stamped ``false``. The missing uuid is still minted (or, in
        ``assign_uuids=False`` mode, generated ephemerally) either way.
        """
        try:
            values = self.ws.get_all_values()
        except Exception as e:  # pragma: no cover
            raise SheetIOError(f"get_all_values failed: {e}") from e
        if not values:
            raise CutoverNotDoneError(
                "empty sheet: cutover sentinel absent — run backfill_cutover() first"
            )
        header = values[0]
        col_index = _build_col_index(header)
        if not col_index.get(SENTINEL_HEADER):
            raise CutoverNotDoneError(
                "cutover sentinel absent — run backfill_cutover() before read_canonical()"
            )
        # Post-cutover these exist; ensure defensively (creates to the right).
        # Skipped entirely in assign_uuids=False mode — no writes allowed there.
        if assign_uuids and (
            not col_index.get(UUID_HEADER) or not col_index.get(RECONCILED_HEADER)
        ):
            col_index = self._ensure_columns([UUID_HEADER, RECONCILED_HEADER])
            values = self.ws.get_all_values()  # header widened
            header = values[0]
        uuid_col = col_index.get(UUID_HEADER)
        rec_col = col_index.get(RECONCILED_HEADER)

        canon_rows: List[CanonRow] = []
        anomalies: List[Anomaly] = []
        for row_idx, row in enumerate(values[1:], start=2):
            raw: Dict[str, Any] = {}
            canon: Dict[str, str] = {}
            for i, cell in enumerate(row):
                nk = _norm_key(header[i]) if i < len(header) else f"col{i + 1}"
                raw[nk] = cell
                cf = _CANON.get(nk)
                # first non-empty synonym wins (later non-empty fills an empty)
                if cf and (cf not in canon or not canon[cf]):
                    canon[cf] = cell
            raw["_row_index"] = row_idx

            sku = (canon.get("sku") or "").strip()
            size = (canon.get("size") or "").strip()
            product_id = (canon.get("product_id") or "").strip()
            prezzo_high = _clean_price(canon.get("prezzo_high"))
            prezzo_outlet = _clean_price(canon.get("prezzo_outlet"))
            qp = self.parse_qta(canon.get("qta"))
            online = canon.get("online") or ""
            sconto = canon.get("sconto") or ""
            reconciled = _truthy_si(canon.get("reconciled") or "")
            row_uuid = (canon.get("row_uuid") or "").strip()

            row_anoms: List[str] = []
            if qp.anomaly:
                kind = ANOMALY_QTA_FRACTION if "/" in qp.raw else ANOMALY_QTA_UNPARSEABLE
                row_anoms.append(kind)
            if not prezzo_high or not prezzo_outlet:
                row_anoms.append(ANOMALY_PRICE_INCOMPLETE)

            # assign-on-read: mint a stable id for rows lacking one (post-cutover
            # rows appended by Make). Guarded write: only the empty uuid cell —
            # and ONLY if the reconciled cell is ALSO empty (anti re-inflate: a
            # row already reconciled=true must never be forced back to pending
            # just because its uuid is missing).
            if not row_uuid:
                row_uuid = _new_uuid()
                if assign_uuids:
                    reconciled_cell_empty = not (canon.get("reconciled") or "").strip()
                    try:
                        self.ws.update_cell(row_idx, uuid_col, row_uuid)
                        if reconciled_cell_empty:
                            self.ws.update_cell(row_idx, rec_col, "false")
                            reconciled = False
                    except Exception as e:  # pragma: no cover
                        raise SheetIOError(
                            f"assign-on-read update_cell failed: {e}"
                        ) from e
                # assign_uuids=False: row_uuid above is ephemeral (in-memory only,
                # never written back); reconciled keeps the value already derived
                # from the existing cell content at line ~343.
                row_anoms.append(ANOMALY_MISSING_UUID_ASSIGNED)

            for kind in row_anoms:
                anomalies.append(Anomaly(row_uuid, sku, kind, qp.raw if "qta" in kind else ""))

            canon_rows.append(
                CanonRow(
                    row_uuid=row_uuid,
                    sku=sku,
                    size=size,
                    product_id=product_id,
                    prezzo_high=prezzo_high,
                    prezzo_outlet=prezzo_outlet,
                    qta=qp.value,
                    qta_raw=qp.raw,
                    online=online,
                    sconto=sconto,
                    reconciled=reconciled,
                    row_index=row_idx,
                    anomalies=row_anoms,
                    raw=raw,
                )
            )
        return CanonRead(canon_rows, col_index, anomalies)

    def iter_unreconciled(self, *, assign_uuids: bool = True) -> Iterator[CanonRow]:
        """PULL ingestion (MVP default): yield canonical rows whose ``reconciled``
        is falsy. Idempotent by construction — a reconciled row is never emitted.

        ``assign_uuids`` is forwarded verbatim to ``read_canonical`` — pass
        ``assign_uuids=False`` in DRY_RUN so pulling pending rows never mutates
        the live Sheet."""
        for row in self.read_canonical(assign_uuids=assign_uuids).rows:
            if not row.reconciled:
                yield row
