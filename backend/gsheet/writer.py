"""Write-side of ``ScansiaSheet`` (``WriterMixin``).

Everything here mutates the worksheet through ``update_cell`` only (never a bulk
range rewrite), so unknown columns preserved by the append-oriented sheet are
left untouched. GSheet has no native compare-and-swap, so every write is guarded
by an *immediate re-read* (SPEC CI-2, TOCTOU).

Foundation (helpers, ``_CANON``, dataclasses, errors) lives in ``reader.py``;
this module imports from it and is composed into the concrete ``ScansiaSheet``
in ``__init__.py``. The read-side may call ``self._ensure_columns`` — resolved at
runtime because both mixins share the same ``self`` (no import cycle).
"""
from __future__ import annotations

from typing import Any, Dict, List, NamedTuple, Optional

from backend.gsheet.reader import (
    RECONCILED_HEADER,
    SENTINEL_HEADER,
    UUID_HEADER,
    SheetIOError,
    _build_col_index,
    _cell,
    _new_uuid,
    _norm_key,
)


class WriteResult(NamedTuple):
    ok: bool
    row_index: Optional[int]
    reason: Optional[str]


class BackfillReport(NamedTuple):
    rows_stamped: int
    already_done: bool


def _serialize(value: Any) -> str:
    """Sheet-cell serialization. bool -> 'true'/'false' (round-trips through
    ``_truthy_si``); everything else stringified (GID passes through unchanged)."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return "" if value is None else str(value)


class WriterMixin:
    """Write-side of ``ScansiaSheet``. Requires ``self.ws``."""

    ws: Any  # set by ScansiaSheet.__init__

    # -- schema-column management (control cols pinned to the RIGHT) --------
    def _ensure_columns(self, names: List[str]) -> Dict[str, int]:
        """Ensure each header name exists; append missing ones to the RIGHT (so
        Make's header-name appends stay aligned — CI-6). Returns a fresh
        col_index. Idempotent: existing headers are left in place."""
        try:
            values = self.ws.get_all_values()
        except Exception as e:  # pragma: no cover
            raise SheetIOError(f"get_all_values failed: {e}") from e
        if not values:
            # Bare sheet: lay the requested headers down at row 1.
            header: List[str] = []
        else:
            header = list(values[0])
        col_index = _build_col_index(header)
        next_col = len(header) + 1
        for name in names:
            if not col_index.get(_norm_key(name)):
                try:
                    self.ws.update_cell(1, next_col, name)
                except Exception as e:  # pragma: no cover
                    raise SheetIOError(f"header update_cell failed: {e}") from e
                col_index[_norm_key(name)] = next_col
                next_col += 1
        return col_index

    # -- guarded write-back (CI-2) -----------------------------------------
    def write_back(
        self,
        row_uuid: str,
        fields: Dict[str, Any],
        *,
        expected_sku: str,
        product_id_guard: Optional[str] = None,
    ) -> WriteResult:
        """Compare-and-swap write-back keyed by ``row_uuid`` (identity — NEVER
        (SKU,Size)). Does ONE immediate re-read and verifies, in that same read,
        that: the row_uuid exists AND its sku == ``expected_sku`` AND (when
        ``product_id_guard`` given) the Q cell is empty OR == the guard. Any
        mismatch -> abort with ``ok=False`` and NO write."""
        try:
            values = self.ws.get_all_values()
        except Exception as e:  # pragma: no cover
            raise SheetIOError(f"get_all_values failed: {e}") from e
        if not values:
            return WriteResult(False, None, "empty_sheet")
        header = values[0]
        col_index = _build_col_index(header)
        uuid_col = col_index.get(UUID_HEADER)
        if not uuid_col:
            return WriteResult(False, None, "no_row_uuid_column")
        sku_col = col_index.get("sku")
        pid_col = col_index.get("product_id")

        target_idx: Optional[int] = None
        target_row: List[str] = []
        for r_idx, row in enumerate(values[1:], start=2):
            if _cell(row, uuid_col) == row_uuid:
                target_idx = r_idx
                target_row = row
                break
        if target_idx is None:
            return WriteResult(False, None, "row_not_found")

        # CAS guards — all evaluated against the SAME read used to locate the row.
        sku_val = _cell(target_row, sku_col).strip()
        if sku_val != (expected_sku or "").strip():
            return WriteResult(False, target_idx, "sku_mismatch")
        if product_id_guard is not None:
            q_val = _cell(target_row, pid_col).strip()
            if q_val != "" and q_val != product_id_guard:
                return WriteResult(False, target_idx, "product_id_conflict")

        # Passed → write each field. Control columns are created if absent.
        for fname, fval in fields.items():
            col = col_index.get(_norm_key(fname))
            if not col:
                col_index = self._ensure_columns([fname])
                col = col_index[_norm_key(fname)]
            try:
                self.ws.update_cell(target_idx, col, _serialize(fval))
            except Exception as e:  # pragma: no cover
                raise SheetIOError(f"write_back update_cell failed: {e}") from e
        return WriteResult(True, target_idx, None)

    # -- thin sugar over write_back (same CAS) ------------------------------
    def write_product_id(self, row_uuid: str, gid: str, *, expected_sku: str) -> WriteResult:
        """SYNC's write-back of the outlet GID into col Q, CAS-guarded so Q is
        only written when empty or already equal to this GID."""
        return self.write_back(
            row_uuid, {"product_id": gid}, expected_sku=expected_sku, product_id_guard=gid
        )

    def mark_reconciled(self, row_uuid: str, *, expected_sku: str) -> WriteResult:
        """Mark a row's delta as applied (reconciled=true). CI-4 idempotency."""
        return self.write_back(row_uuid, {"reconciled": True}, expected_sku=expected_sku)

    def write_delete_state(
        self, row_uuid: str, gid: str, *, expected_sku: str, field: str, value: Any
    ) -> WriteResult:
        """Record a delete outcome. Disambiguated by Q==gid (product_id_guard).
        The target ``field`` is a PARAMETER, never hardcoded ``online`` — CI-6:
        if Make reads the ``online`` column, callers pass a tool-private field."""
        return self.write_back(
            row_uuid, {field: value}, expected_sku=expected_sku, product_id_guard=gid
        )

    # -- one-time cutover backfill (CI-1) ----------------------------------
    def backfill_cutover(self) -> BackfillReport:
        """ONE-TIME, idempotent. Stamps a ``row_uuid`` on every row lacking one,
        marks EVERY pre-existing row ``reconciled=true`` (their stock is already
        live on Shopify), then sets the cutover sentinel LAST. A second call is a
        no-op (sentinel present). This is what makes ``iter_unreconciled`` return
        ZERO rows on a freshly-cut sheet (anti re-inflate)."""
        try:
            values = self.ws.get_all_values()
        except Exception as e:  # pragma: no cover
            raise SheetIOError(f"get_all_values failed: {e}") from e

        header = list(values[0]) if values else []
        col_index = _build_col_index(header)
        if col_index.get(SENTINEL_HEADER):
            return BackfillReport(0, already_done=True)

        col_index = self._ensure_columns([UUID_HEADER, RECONCILED_HEADER])
        uuid_col = col_index[UUID_HEADER]
        rec_col = col_index[RECONCILED_HEADER]

        # Re-read: header widened by _ensure_columns.
        values = self.ws.get_all_values()
        stamped = 0
        for r_idx, row in enumerate(values[1:], start=2):
            if not _cell(row, uuid_col).strip():
                try:
                    self.ws.update_cell(r_idx, uuid_col, _new_uuid())
                except Exception as e:  # pragma: no cover
                    raise SheetIOError(f"backfill update_cell (uuid) failed: {e}") from e
            try:
                self.ws.update_cell(r_idx, rec_col, "true")
            except Exception as e:  # pragma: no cover
                raise SheetIOError(f"backfill update_cell (reconciled) failed: {e}") from e
            stamped += 1

        # Sentinel LAST — its presence is the idempotency + fail-closed marker.
        self._ensure_columns([SENTINEL_HEADER])
        return BackfillReport(stamped, already_done=False)
