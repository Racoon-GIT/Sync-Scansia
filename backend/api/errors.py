"""Error boundary translation — the single map from an internal exception to a
bounded, client-safe ``(status_code, error_code, message)`` triple.

This module is PURE STDLIB (no FastAPI): the mapping is a plain function so it is
unit-testable on a bare interpreter, and it is reused both by the FastAPI
exception handlers (``backend.app``) and by the background job runner
(``backend.api.read._run_inventory_job``) to derive a stable ``error_code`` for a
crashed job without leaking anything.

Cross-project invariant (parent CLAUDE.md "Errors at the API boundary"): NEVER
leak a raw ``Error.message``, stack trace, GraphQL ``userErrors`` payload, or
gspread error text to the client. Every branch below returns a FIXED, generic
safe message keyed by a stable ``error_code``; the only thing logged internally is
the exception TYPE name + the error_code (never ``str(exc)``, which could carry a
SKU search string, a GraphQL fragment, or a sheet cell value).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from backend.auth.basic_auth import AuthError, AuthNotConfigured
from backend.config import ConfigError
from backend.gsheet.reader import CutoverNotDoneError, GSheetError, SheetIOError
from backend.shopify.ops import ShopifyUserError
from backend.shopify.transport import ShopifyTransportError

logger = logging.getLogger("backend.api.errors")

# Stable error codes (part of the API contract — safe to expose to the client).
CODE_AUTH_NOT_CONFIGURED = "auth_not_configured"
CODE_UNAUTHORIZED = "unauthorized"
CODE_CONFIG_ERROR = "config_error"
CODE_CUTOVER_NOT_DONE = "cutover_not_done"
CODE_SHEET_IO_ERROR = "sheet_io_error"
CODE_SHEET_ERROR = "sheet_error"
CODE_SHOPIFY_USER_ERROR = "shopify_user_error"
CODE_SHOPIFY_TRANSPORT_ERROR = "shopify_transport_error"
CODE_INTERNAL_ERROR = "internal_error"


@dataclass(frozen=True)
class ApiError:
    """A bounded, client-safe error. ``message`` is FIXED per code — never a raw
    exception string. ``as_body`` is the exact JSON body the boundary returns."""

    status_code: int
    error_code: str
    message: str

    def as_body(self) -> dict:
        return {"error": {"code": self.error_code, "message": self.message}}


# Fixed, generic, secret-free messages. Order of the isinstance checks in
# :func:`map_exception` matters: the most specific subclass is tested first
# (SheetIOError / CutoverNotDoneError before their GSheetError base).
_INTERNAL = ApiError(500, CODE_INTERNAL_ERROR, "Internal server error.")


def map_exception(exc: BaseException) -> ApiError:
    """Translate an internal exception into a bounded :class:`ApiError`.

    Unknown/unexpected exceptions collapse to a generic 500 — the fallback is
    fail-safe (never re-raises, never inspects the message). Auth errors are
    included for completeness even though the auth dependency maps them to
    ``HTTPException`` first; a service that re-raises one still lands here safely.
    """
    if isinstance(exc, AuthNotConfigured):
        return ApiError(503, CODE_AUTH_NOT_CONFIGURED, "Authentication not configured.")
    if isinstance(exc, AuthError):
        return ApiError(401, CODE_UNAUTHORIZED, "Unauthorized.")
    if isinstance(exc, ConfigError):
        return ApiError(503, CODE_CONFIG_ERROR, "Service configuration error.")
    if isinstance(exc, CutoverNotDoneError):
        return ApiError(409, CODE_CUTOVER_NOT_DONE, "Sheet cutover not completed.")
    if isinstance(exc, SheetIOError):
        return ApiError(502, CODE_SHEET_IO_ERROR, "Spreadsheet I/O error.")
    if isinstance(exc, GSheetError):
        return ApiError(502, CODE_SHEET_ERROR, "Spreadsheet error.")
    if isinstance(exc, ShopifyUserError):
        return ApiError(422, CODE_SHOPIFY_USER_ERROR, "Shopify rejected the operation.")
    if isinstance(exc, ShopifyTransportError):
        return ApiError(502, CODE_SHOPIFY_TRANSPORT_ERROR, "Shopify is unavailable.")
    return _INTERNAL


def log_boundary(exc: BaseException, err: ApiError) -> None:
    """Log a boundary hit with TYPE name + error_code ONLY (never the message).

    Deliberately avoids ``str(exc)``: a raw exception string can carry a SKU
    search token, a GraphQL fragment, or a sheet cell value. A 5xx is logged at
    ERROR, a client-side 4xx at INFO.
    """
    level = logging.ERROR if err.status_code >= 500 else logging.INFO
    logger.log(level, "api boundary: %s -> %s (%d)", type(exc).__name__, err.error_code, err.status_code)


__all__ = [
    "ApiError",
    "map_exception",
    "log_boundary",
    "CODE_AUTH_NOT_CONFIGURED",
    "CODE_UNAUTHORIZED",
    "CODE_CONFIG_ERROR",
    "CODE_CUTOVER_NOT_DONE",
    "CODE_SHEET_IO_ERROR",
    "CODE_SHEET_ERROR",
    "CODE_SHOPIFY_USER_ERROR",
    "CODE_SHOPIFY_TRANSPORT_ERROR",
    "CODE_INTERNAL_ERROR",
]
