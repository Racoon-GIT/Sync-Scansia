"""Persistence adapters + the porting contract for Scansia Manager.

The Protocols in :mod:`backend.persistence.ports` are the storage-agnostic
boundary the confirm-gated services depend on (structurally). Today's concrete
adapters are sheet-centric
(:class:`~backend.persistence.gsheet_audit.GSheetAuditSink`) and stateless-HMAC
(:class:`~backend.persistence.tokens.HmacTokenService`); a future MySQL port is a
NEW adapter satisfying the SAME Protocols — the services never change.

Only the Protocols are re-exported here. The concrete adapters are imported from
their own modules so importing this package stays free of gspread / crypto setup.
"""
from backend.persistence.ports import AuditSink, TokenService

__all__ = ["AuditSink", "TokenService"]
