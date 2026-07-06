"""Canonical GSheet layer for the Scansia Manager backend.

Single public class ``ScansiaSheet`` over a gspread ``Worksheet`` — the sole live
legacy GSheet code (``src/sync.py`` ``gs_read_rows``/``gs_write_product_id``)
consolidated with the append-oriented row_uuid/reconciled model.

  - ``ScansiaSheet(ws=...)`` — inject a Worksheet (tests mock it; NO network).
  - ``ScansiaSheet.open()``  — production factory: opens the worksheet via
    ``gspread`` from env (``GSPREAD_SHEET_ID``, ``GSPREAD_WORKSHEET_TITLE``,
    ``GOOGLE_CREDENTIALS_JSON`` or ``GOOGLE_APPLICATION_CREDENTIALS``). gspread /
    google-auth are imported lazily here so importing this package (and the tests)
    never requires credentials or the network.

Env-only config, fail-closed (reuses ``backend.config.ConfigError``). No secret
is ever logged.
"""
from __future__ import annotations

from typing import Any, Optional

from backend.config import ConfigError
from backend.gsheet.reader import (
    ANOMALY_MISSING_UUID_ASSIGNED,
    ANOMALY_PRICE_INCOMPLETE,
    ANOMALY_QTA_FRACTION,
    ANOMALY_QTA_UNPARSEABLE,
    DEFAULT_SI_SET,
    RECONCILED_HEADER,
    SENTINEL_HEADER,
    UUID_HEADER,
    Anomaly,
    CanonRead,
    CanonRow,
    CASMismatchError,
    CutoverNotDoneError,
    GSheetError,
    QtaParse,
    RawRead,
    ReaderMixin,
    RowNotFoundError,
    SheetIOError,
    _clean_price,
    _norm_key,
    _truthy_si,
)
from backend.gsheet.writer import BackfillReport, WriteResult, WriterMixin

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class ScansiaSheet(ReaderMixin, WriterMixin):
    """Canonical view over one worksheet. Compose read + write behaviour."""

    def __init__(self, ws: Any) -> None:
        if ws is None:
            raise ValueError("ScansiaSheet requires a worksheet (use .open() in production)")
        self.ws = ws

    @classmethod
    def open(cls) -> "ScansiaSheet":
        """Open the configured worksheet via gspread (env-only, fail-closed)."""
        import json
        import os

        import gspread  # lazy: keeps import-time + tests network/credential-free
        from google.oauth2.service_account import Credentials

        sheet_id = os.environ.get("GSPREAD_SHEET_ID")
        title = os.environ.get("GSPREAD_WORKSHEET_TITLE")
        if not sheet_id or not sheet_id.strip():
            raise ConfigError("Missing required environment variable: GSPREAD_SHEET_ID")
        if not title or not title.strip():
            raise ConfigError("Missing required environment variable: GSPREAD_WORKSHEET_TITLE")

        cred_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
        if cred_json and cred_json.strip():
            creds = Credentials.from_service_account_info(json.loads(cred_json), scopes=_SCOPES)
        else:
            path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if not path or not path.strip():
                raise ConfigError(
                    "Missing Google credentials: set GOOGLE_CREDENTIALS_JSON "
                    "or GOOGLE_APPLICATION_CREDENTIALS"
                )
            creds = Credentials.from_service_account_file(path, scopes=_SCOPES)

        client = gspread.authorize(creds)
        ws = client.open_by_key(sheet_id).worksheet(title)
        return cls(ws)


__all__ = [
    "ScansiaSheet",
    # result / row types
    "QtaParse",
    "Anomaly",
    "RawRead",
    "CanonRow",
    "CanonRead",
    "WriteResult",
    "BackfillReport",
    # errors
    "GSheetError",
    "CutoverNotDoneError",
    "RowNotFoundError",
    "CASMismatchError",
    "SheetIOError",
    "ConfigError",
    # anomaly kinds
    "ANOMALY_QTA_FRACTION",
    "ANOMALY_QTA_UNPARSEABLE",
    "ANOMALY_PRICE_INCOMPLETE",
    "ANOMALY_MISSING_UUID_ASSIGNED",
    # control columns + defaults
    "UUID_HEADER",
    "RECONCILED_HEADER",
    "SENTINEL_HEADER",
    "DEFAULT_SI_SET",
    # helpers (canonical, reused downstream)
    "_clean_price",
    "_norm_key",
    "_truthy_si",
]
