"""Sheet-centric durable audit sink (:class:`~backend.persistence.ports.AuditSink`).

Writes to three dedicated tabs of the SAME spreadsheet that backs
:class:`backend.gsheet.ScansiaSheet`, reusing its already-authorized gspread
client (reached via ``sheet.ws.spreadsheet`` in :meth:`from_scansia_sheet` — no
new credential read, no new env var, no new ``open()`` affordance). Each tab is
created on first use if absent:

* ``AUDIT``        — general outcome/event log (``write_outcome``).
* ``AUDIT_DELETE`` — delete before-snapshots, written BEFORE ``productDelete``.
* ``AUDIT_PRICE``  — price before-snapshots (priors), read back by ``load`` for revert.

DURABILITY CAVEAT (deliberate, documented). The ``DeleteAuditSink`` contract asks
``write_durable`` to persist to TWO durable sinks. This adapter persists to ONE
(the GSheet tab); the second durable sink (e.g. Cloudflare R2 / object storage) is
an OPTIONAL follow-on. Until it lands, ``write_durable`` still provides single-sink
durability AND remains a hard abort gate — it RAISES ``SheetIOError`` on any
failure, so the delete never proceeds when the snapshot could not be stored. It is
just a reduced-redundancy guarantee. Acceptable for the single-operator tool;
revisit before any high-volume batch delete.

No sensitive payload is ever logged: snapshots/priors live only in the sheet, and
log lines carry tab titles and exception TYPE names only (never messages/values).
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from backend.gsheet.reader import SheetIOError
from backend.services.delete_service import BeforeSnapshot, DeleteOutcomeEvent
from backend.services.pricing_service import PriceIntent, ProductPrior, VariantPrior

logger = logging.getLogger("backend.persistence.gsheet_audit")

_ROME = ZoneInfo("Europe/Rome")

TAB_AUDIT = "AUDIT"
TAB_DELETE = "AUDIT_DELETE"
TAB_PRICE = "AUDIT_PRICE"

_AUDIT_HEADER: Tuple[str, ...] = ("ts", "actor", "action", "target_gids", "plan_hash", "result")
_DELETE_HEADER: Tuple[str, ...] = ("ts", "product_gid", "snapshot_json")
_PRICE_HEADER: Tuple[str, ...] = ("ts", "intent_id", "mode", "plan_hash", "priors_json")

# Provisioning size for a freshly created audit tab (append-only; grows as needed).
_NEW_TAB_ROWS = 1000
_NEW_TAB_COLS = 12


def _intent_from_json(mode: str, plan_hash: str, priors_json: str) -> PriceIntent:
    """Reconstruct a :class:`PriceIntent` from a stored ``priors_json`` cell."""
    data = json.loads(priors_json)
    priors = tuple(
        ProductPrior(
            product_gid=p["product_gid"],
            sku=p["sku"],
            variants=tuple(
                VariantPrior(v["variant_id"], v.get("price"), v.get("compare_at"))
                for v in p.get("variants", [])
            ),
        )
        for p in data
    )
    return PriceIntent(mode=mode, plan_hash=plan_hash, priors=priors)


class GSheetAuditSink:
    """Concrete :class:`~backend.persistence.ports.AuditSink` over gspread tabs.

    Inject a gspread ``Spreadsheet`` handle (tests inject a fake — no network).
    The Protocol is satisfied STRUCTURALLY; this class subclasses it only for
    documentation and an explicit ``isinstance`` check via ``runtime_checkable``.
    """

    def __init__(
        self,
        spreadsheet: Any,
        *,
        actor: Optional[str] = None,
        now: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self._ss = spreadsheet
        self._actor = (actor or "").strip() or "unknown"
        self._now = now or (lambda: datetime.now(_ROME))
        self._tabs: Dict[str, Any] = {}

    @classmethod
    def from_scansia_sheet(
        cls, sheet: Any, *, actor: Optional[str] = None,
        now: Optional[Callable[[], datetime]] = None,
    ) -> "GSheetAuditSink":
        """Build a sink reusing ScansiaSheet's authorized client via ``sheet.ws.spreadsheet``."""
        return cls(sheet.ws.spreadsheet, actor=actor, now=now)

    # -- tab management (create-if-missing) ---------------------------------
    def _tab(self, title: str, header: Tuple[str, ...]) -> Any:
        cached = self._tabs.get(title)
        if cached is not None:
            return cached
        try:
            existing = {w.title: w for w in self._ss.worksheets()}
        except Exception as e:  # noqa: BLE001 - any gspread failure -> bounded SheetIOError
            raise SheetIOError(f"audit list worksheets failed: {type(e).__name__}") from e
        ws = existing.get(title)
        if ws is None:
            try:
                ws = self._ss.add_worksheet(title=title, rows=_NEW_TAB_ROWS, cols=_NEW_TAB_COLS)
                ws.append_row(list(header))
            except Exception as e:  # noqa: BLE001
                raise SheetIOError(f"audit create tab {title} failed: {type(e).__name__}") from e
            logger.info("created audit tab %s", title)  # title only, never payload
        else:
            # Pre-existing tab (resolved via worksheets()): it may be missing its
            # header — created out-of-band, or a partial-creation race where
            # add_worksheet succeeded but the follow-up append_row(header) failed;
            # on retry the tab is already listed and would otherwise be cached
            # header-less. load()/read_recent() unconditionally treat row 0 as the
            # header, so repair it now, once, before any row is trusted as data.
            self._ensure_header(title, ws, header)
        self._tabs[title] = ws
        return ws

    def _ensure_header(self, title: str, ws: Any, header: Tuple[str, ...]) -> None:
        """Idempotently repair ``ws``'s header row if absent or mismatched.

        Compares row 0 (truncated to the header's own width, since a freshly
        provisioned tab has more columns than the header) against the expected
        header tuple. If it doesn't match — tab empty, or row 0 is actually real
        data that predates any header — insert the header at row 1, which shifts
        any existing data down rather than overwriting it. No-op when the header
        is already correct (the common case, cheap and safe to re-check).
        """
        try:
            values = ws.get_all_values()
        except Exception as e:  # noqa: BLE001
            raise SheetIOError(f"audit read {title} header failed: {type(e).__name__}") from e
        current = tuple(values[0][: len(header)]) if values else ()
        if current == header:
            return
        try:
            ws.insert_row(list(header), 1)
        except Exception as e:  # noqa: BLE001
            raise SheetIOError(f"audit header repair {title} failed: {type(e).__name__}") from e
        logger.info("repaired missing/mismatched header on audit tab %s", title)  # title only

    def _append(self, title: str, header: Tuple[str, ...], row: List[Any]) -> None:
        ws = self._tab(title, header)
        try:
            ws.append_row([("" if v is None else str(v)) for v in row])
        except Exception as e:  # noqa: BLE001
            raise SheetIOError(f"audit append to {title} failed: {type(e).__name__}") from e

    def _ts(self) -> str:
        return self._now().isoformat()

    # -- AuditSink: pricing side (capture_before / load) --------------------
    def capture_before(self, intent: PriceIntent) -> str:
        """Append the price priors to ``AUDIT_PRICE`` and return a fresh ``intent_id``.

        Persists ONE row per intent: ``(ts, intent_id, mode, plan_hash, priors_json)``.
        Raises ``SheetIOError`` on failure (the service catches it -> nothing is
        pushed and no sheet cell is written).
        """
        intent_id = uuid.uuid4().hex
        priors_json = json.dumps(
            [asdict(p) for p in intent.priors],
            separators=(",", ":"), ensure_ascii=False, default=str,
        )
        self._append(
            TAB_PRICE, _PRICE_HEADER,
            [self._ts(), intent_id, intent.mode, intent.plan_hash, priors_json],
        )
        return intent_id

    def load(self, intent_id: str) -> PriceIntent:
        """Read back the priors captured under ``intent_id`` (for revert)."""
        ws = self._tab(TAB_PRICE, _PRICE_HEADER)
        try:
            values = ws.get_all_values()
        except Exception as e:  # noqa: BLE001
            raise SheetIOError(f"audit read AUDIT_PRICE failed: {type(e).__name__}") from e
        for row in values[1:]:  # row[0] is the header
            if len(row) >= 5 and row[1] == intent_id:
                return _intent_from_json(row[2], row[3], row[4])
        raise SheetIOError(f"audit intent not found: {intent_id}")

    # -- AuditSink: delete side (write_durable / write_outcome) -------------
    def write_durable(self, snapshot: BeforeSnapshot) -> None:
        """Append the delete before-snapshot to ``AUDIT_DELETE`` — the ABORT GATE.

        RAISES ``SheetIOError`` on any failure so that ``productDelete`` is never
        reached when the snapshot could not be durably stored (single-sink; see the
        module durability caveat). This includes a pre-append serialization
        failure (``asdict``/``json.dumps`` raising ``TypeError``/``ValueError``),
        which is normalized to ``SheetIOError`` too — the docstring's "on any
        failure" must hold for any future consumer that catches only
        ``SheetIOError``.
        """
        try:
            snap_json = json.dumps(
                asdict(snapshot), separators=(",", ":"), ensure_ascii=False, default=str,
            )
        except (TypeError, ValueError) as e:
            raise SheetIOError(f"audit snapshot serialization failed: {type(e).__name__}") from e
        self._append(TAB_DELETE, _DELETE_HEADER, [self._ts(), snapshot.product_gid, snap_json])

    def write_outcome(self, event: DeleteOutcomeEvent) -> None:
        """Append a delete outcome event to ``AUDIT`` (best-effort at the call site)."""
        result = event.status + (f":deleted_id={event.deleted_id}" if event.deleted_id else "")
        self._append(
            TAB_AUDIT, _AUDIT_HEADER,
            [self._ts(), self._actor, "product_delete", event.product_gid, "", result],
        )

    # -- read side (GET /audit) ---------------------------------------------
    def read_recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Return the most recent ``AUDIT`` rows as header-keyed dicts (READ-ONLY).

        Reads the whole ``AUDIT`` tab (create-if-missing) and returns the LAST
        ``limit`` data rows in sheet order (oldest-of-the-window first). Never
        returns the header row. Raises ``SheetIOError`` on any gspread failure —
        the boundary translates it; no raw error text is exposed.
        """
        ws = self._tab(TAB_AUDIT, _AUDIT_HEADER)
        try:
            values = ws.get_all_values()
        except Exception as e:  # noqa: BLE001 - any gspread failure -> bounded SheetIOError
            raise SheetIOError(f"audit read {TAB_AUDIT} failed: {type(e).__name__}") from e
        if not values:
            return []
        header = list(values[0])
        body = values[1:]
        if limit and limit > 0:
            body = body[-limit:]
        return [dict(zip(header, row)) for row in body]


__all__ = ["GSheetAuditSink", "TAB_AUDIT", "TAB_DELETE", "TAB_PRICE"]
