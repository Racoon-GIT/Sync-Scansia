"""Persistence adapters + the porting contract for Scansia Manager.

The Protocols in :mod:`backend.persistence.ports` are the storage-agnostic
boundary the services depend on (structurally). The concrete adapters: the
register (:class:`~backend.persistence.register.Register` over a
``RegisterStore`` — :mod:`~backend.persistence.mysql_store` in production,
:mod:`~backend.persistence.memory_store` in the tests), its audit sink
(:class:`~backend.persistence.audit_sink.RegisterAuditSink`) and the
stateless-HMAC confirm-tokens (:class:`~backend.persistence.tokens.HmacTokenService`).

Only the Protocols are re-exported here. The concrete adapters are imported from
their own modules so importing this package stays free of driver / crypto setup.
"""
from backend.persistence.ports import AuditSink, TokenService

__all__ = ["AuditSink", "TokenService"]
