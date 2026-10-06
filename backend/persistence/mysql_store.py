"""MySQL :class:`~backend.persistence.ports.RegisterStore` on ``mysql-connector-python``.

The production store behind :class:`~backend.persistence.register.Register`
(plan ``docs/f7-mysql-adapter-plan.md`` §3.6, §3.7; ``docs/f7-build-notes.md``
§Storage). Its contract with the in-memory fake is ``tests/test_register_store.py``,
run here on ``racoon_staging`` only.

Connection discipline (§3.7), each rule enforced in code:

1. **Autocommit on**, asserted at connect (the driver starts with it off, PEP 249);
   an explicit transaction only in :meth:`MySQLStore.fenced_write` (renew first,
   the ops, ``COMMIT``) — never across a Shopify call: the store has no API that
   leaves a transaction open between two calls.
2. **No locking read, no SELECT inside a write**: every read is a plain SELECT;
   every write is ``UPDATE … SET … WHERE`` / ``INSERT … VALUES`` with no subquery.
3. **Every UPDATE by PRIMARY KEY** (``WHERE ID = …`` / ``ID IN (…)``), the register
   ``UPDATE`` last before ``COMMIT`` (``records.check_op_order``).
4. **No connection outlives a job** (the caller ``close()``s the store); session
   ``wait_timeout`` 60 s — and since a job may sit in Shopify calls longer than
   that between two statements, a handle idle for more than ``IDLE_REOPEN_S``
   re-opens its session before the next statement (outside any transaction).
5. Session ``innodb_lock_wait_timeout`` 5 s: the app's own give-up.
6. ``CLIENT_FOUND_ROWS``: a guarded statement's count is rows MATCHED.
7. No literal ``%`` in any statement — the driver would eat a bound parameter
   (``SVILUPPO/docs/shared-database.md`` §mysql-connector-python); a test greps
   every statement template here.

TLS: the server presents the in-house CA's certificate, SAN ``IP:78.46.244.227``;
the client pins that CA and verifies the identity BY IP (``ssl_verify_identity``,
``shared-database.md`` §TLS). Time: every instant is the SERVER's
``UTC_TIMESTAMP(3)`` (no client clock in a fence); the session ``time_zone`` is
pinned to UTC as the shared database requires of every consumer.

Errors: driver errors become the bounded ``records`` errors (type + MySQL errno
only — a driver message can carry SQL with values, so it is never re-raised).
"""
from __future__ import annotations

import base64
import json
import logging
import os
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from backend.config import ConfigError
from backend.persistence.records import (
    KIND_RECONCILE,
    SHELF,
    TERMINAL,
    AuditRow,
    CommitUnknown,
    DatabaseUnavailable,
    Deadlock,
    DuplicateKey,
    EventRow,
    Fence,
    GuardMismatch,
    InsertAudit,
    LeaseLost,
    LeaseRow,
    LockWaitTimeout,
    PairRow,
    Snapshot,
    StampEvents,
    StoreClosed,
    StoreError,
    UpdateAudit,
    UpdatePairs,
    WriteOp,
    check_op_order,
    resolve_inserted,
)

logger = logging.getLogger("backend.persistence.mysql_store")

SESSION_WAIT_TIMEOUT_S = 60
SESSION_LOCK_WAIT_TIMEOUT_S = 5
IDLE_REOPEN_S = 45  # < SESSION_WAIT_TIMEOUT_S: re-open before the server drops the session
CONNECT_TIMEOUT_S = 10
# One retry of a connect that never reached the server (TCP timeouts, errno 60, seen
# on staging 2026-10-06) — never of a refusal: a wrong password is not retried.
CONNECT_ATTEMPTS = 2

# MySQL / client error numbers (mysql.connector.errorcode).
_ER_DUP_ENTRY = 1062
_ER_LOCK_WAIT_TIMEOUT = 1205
_ER_LOCK_DEADLOCK = 1213
_CONNECTION_LOST = frozenset({2003, 2006, 2013, 2055})

# ---------------------------------------------------------------------------
# Statements (no literal % anywhere: only %s placeholders)
# ---------------------------------------------------------------------------
SQL_SESSION = (
    "SET SESSION time_zone = '+00:00', "
    f"SESSION wait_timeout = {SESSION_WAIT_TIMEOUT_S}, "
    f"SESSION innodb_lock_wait_timeout = {SESSION_LOCK_WAIT_TIMEOUT_S}"
)
SQL_SESSION_CHECK = "SELECT @@SESSION.autocommit, DATABASE()"
SQL_NOW = "SELECT UTC_TIMESTAMP(3)"

_PAIR_COLS = (
    "ID, PAIR_UUID, SKU, SIZE, STATE, PRICE_HIGH, PRICE, PRICE_OUTLET, DISCOUNT, "
    "OUTLET_PRODUCT_ID, ONLINE, UPDATED_AT, PRODUCT_TITLE"
)
SQL_PAIRS = f"SELECT {_PAIR_COLS} FROM sc_returned_pair"
_UNSTAMPED_WHERE = (
    "SELECT e.ID, e.PAIR_ID, e.EVENT_TYPE, e.ACTOR, e.OUTLET_DELTA, e.CREATED_AT, "
    "e.OUTLET_RECONCILED_AT, p.SKU, p.SIZE, p.STATE "
    "FROM sc_returned_pair_event e LEFT JOIN sc_returned_pair p ON p.ID = e.PAIR_ID "
    "WHERE e.OUTLET_RECONCILED_AT IS NULL"
)
SQL_UNSTAMPED = _UNSTAMPED_WHERE + " ORDER BY e.ID"
_AUDIT_COLS = (
    "ID, KIND, ACTOR, ACTION, CREATED_AT, PLAN_HASH, INTENT_ID, LEASE_GEN, SENT_AT, "
    "SENT_AVAILABLE, IDEMPOTENCY_KEY, PRODUCT_ID, STEP, GROUP_ID, REQUEST, STATUS, "
    "OUTCOME, PAYLOAD, CLOSED_AT"
)
SQL_AUDIT = f"SELECT {_AUDIT_COLS} FROM sm_outlet_audit WHERE KIND = %s"
SQL_SNAP_PAIRS = f"SELECT {_PAIR_COLS} FROM sc_returned_pair WHERE STATE = 'IN_SCANSIA'"
SQL_SNAP_MAX = (
    "SELECT p.SKU, p.SIZE, MAX(e.ID) FROM sc_returned_pair_event e "
    "JOIN sc_returned_pair p ON p.ID = e.PAIR_ID"
)
SQL_IMPORT_PAYLOADS = (
    "SELECT e.PAIR_ID, e.PAYLOAD FROM sc_returned_pair_event e WHERE e.EVENT_TYPE = 'IMPORTED' AND e.PAIR_ID IN "
)
SQL_LEASE = (
    "SELECT HOLDER, GENERATION, EXPIRES_AT, WAITER, WAITER_EXPIRES_AT, UPDATED_AT "
    "FROM sm_outlet_lease WHERE ID = 1"
)

# The lease statements — docs/f7-build-notes.md §Storage, verbatim in shape.
SQL_LEASE_TAKE = (
    "UPDATE sm_outlet_lease SET HOLDER = %s, GENERATION = LAST_INSERT_ID(GENERATION + 1), "
    "EXPIRES_AT = UTC_TIMESTAMP(3) + INTERVAL %s MICROSECOND, "
    "WAITER = IF(WAITER = %s, NULL, WAITER), UPDATED_AT = UTC_TIMESTAMP(3) "
    "WHERE ID = 1 AND (HOLDER IS NULL OR EXPIRES_AT < UTC_TIMESTAMP(3)) "
    "AND (WAITER IS NULL OR WAITER_EXPIRES_AT < UTC_TIMESTAMP(3) OR WAITER = %s)"
)
SQL_LAST_ID = "SELECT LAST_INSERT_ID()"
SQL_LEASE_RENEW = (
    "UPDATE sm_outlet_lease SET EXPIRES_AT = UTC_TIMESTAMP(3) + INTERVAL %s MICROSECOND, "
    "UPDATED_AT = UTC_TIMESTAMP(3) "
    "WHERE ID = 1 AND HOLDER = %s AND GENERATION = %s AND EXPIRES_AT > UTC_TIMESTAMP(3)"
)
SQL_LEASE_RELEASE = (
    "UPDATE sm_outlet_lease SET HOLDER = NULL, EXPIRES_AT = NULL, UPDATED_AT = UTC_TIMESTAMP(3) "
    "WHERE ID = 1 AND HOLDER = %s AND GENERATION = %s"
)
SQL_LEASE_WAIT = (
    "UPDATE sm_outlet_lease SET WAITER = %s, "
    "WAITER_EXPIRES_AT = UTC_TIMESTAMP(3) + INTERVAL %s MICROSECOND, "
    "UPDATED_AT = UTC_TIMESTAMP(3) "
    "WHERE ID = 1 AND (WAITER IS NULL OR WAITER_EXPIRES_AT < UTC_TIMESTAMP(3) OR WAITER = %s)"
)

SQL_INSERT_AUDIT = (
    "INSERT INTO sm_outlet_audit (KIND, ACTOR, ACTION, PLAN_HASH, INTENT_ID, LEASE_GEN, "
    "IDEMPOTENCY_KEY, PRODUCT_ID, STEP, GROUP_ID, STATUS, OUTCOME, PAYLOAD, CREATED_AT, CLOSED_AT) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP(3), "
    "IF(%s, UTC_TIMESTAMP(3), NULL))"
)
SQL_STAMP_HEAD = (
    "UPDATE sc_returned_pair_event "
    "SET OUTLET_RECONCILED_AT = COALESCE(OUTLET_RECONCILED_AT, UTC_TIMESTAMP(3)) WHERE ID IN "
)

STATEMENTS = (
    SQL_SESSION, SQL_SESSION_CHECK, SQL_NOW, SQL_PAIRS, SQL_UNSTAMPED, SQL_AUDIT, SQL_LEASE,
    SQL_SNAP_PAIRS, SQL_SNAP_MAX, SQL_IMPORT_PAYLOADS,
    SQL_LEASE_TAKE, SQL_LAST_ID, SQL_LEASE_RENEW, SQL_LEASE_RELEASE, SQL_LEASE_WAIT,
    SQL_INSERT_AUDIT, SQL_STAMP_HEAD,
)


def _in_list(n: int) -> str:
    return "(" + ", ".join(["%s"] * n) + ")"


# ---------------------------------------------------------------------------
# Configuration (env-only, fail-closed)
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    database: str
    ca_path: str
    user: Optional[str] = None
    password: Optional[str] = None
    option_file: Optional[str] = None  # a MySQL option file ([client] user/password/…), tests only

    def __repr__(self) -> str:  # never print a password
        return f"DBConfig(host={self.host!r}, port={self.port}, database={self.database!r})"


_CA_LOCK = threading.Lock()
_CA_FILE: Dict[str, str] = {}


def _ca_file_from_b64(b64: str) -> str:
    """Materialise the CA PEM given as base64 in the environment, once per process."""
    with _CA_LOCK:
        if b64 in _CA_FILE:
            return _CA_FILE[b64]
        try:
            pem = base64.b64decode(b64, validate=True)
        except Exception as e:  # noqa: BLE001
            raise ConfigError("SCANSIA_DB_CA_B64 is not valid base64") from e
        fd, path = tempfile.mkstemp(prefix="scansia-ca-", suffix=".pem")
        with os.fdopen(fd, "wb") as fh:
            fh.write(pem)
        os.chmod(path, 0o600)
        _CA_FILE[b64] = path
        return path


def load_db_config(env: Optional[Mapping[str, str]] = None) -> DBConfig:
    """The register database from the environment (fail-closed, no defaults that
    could point elsewhere): ``SCANSIA_DB_HOST``, ``SCANSIA_DB_USER``,
    ``SCANSIA_DB_PASSWORD``, ``SCANSIA_DB_NAME``, and the CA as
    ``SCANSIA_DB_CA_PATH`` (a file) or ``SCANSIA_DB_CA_B64`` (its PEM, base64);
    ``SCANSIA_DB_PORT`` defaults to 3306."""
    e = os.environ if env is None else env

    def need(name: str) -> str:
        v = e.get(name)
        if v is None or not str(v).strip():
            raise ConfigError(f"Missing required environment variable: {name}")
        return str(v).strip()

    ca_path = (e.get("SCANSIA_DB_CA_PATH") or "").strip()
    ca_b64 = (e.get("SCANSIA_DB_CA_B64") or "").strip()
    if not ca_path and not ca_b64:
        raise ConfigError("Missing the register CA: SCANSIA_DB_CA_PATH or SCANSIA_DB_CA_B64")
    port_raw = (e.get("SCANSIA_DB_PORT") or "3306").strip()
    if not port_raw.isdigit():
        raise ConfigError("SCANSIA_DB_PORT is not a number")
    return DBConfig(
        host=need("SCANSIA_DB_HOST"), port=int(port_raw), database=need("SCANSIA_DB_NAME"),
        ca_path=ca_path or _ca_file_from_b64(ca_b64),
        user=need("SCANSIA_DB_USER"), password=need("SCANSIA_DB_PASSWORD"),
    )


# ---------------------------------------------------------------------------
# Row mapping
# ---------------------------------------------------------------------------
def _json(value: Any) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        return json.loads(value)
    return value


def _text(value: Any) -> Optional[str]:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    return value


def _dumps(value: Optional[Mapping[str, Any]]) -> Optional[str]:
    return None if value is None else json.dumps(dict(value), default=str, separators=(",", ":"))


def _pair(r: Sequence[Any]) -> PairRow:
    return PairRow(
        pair_id=int(r[0]), pair_uuid=_text(r[1]), sku=_text(r[2]), size=_text(r[3]), state=_text(r[4]),
        price_high=r[5], price=r[6], price_outlet=r[7], discount=r[8],
        outlet_product_id=None if r[9] is None else int(r[9]), online=bool(r[10]),
        updated_at=r[11], product_title=_text(r[12]),
    )


def _event(r: Sequence[Any]) -> EventRow:
    return EventRow(
        event_id=int(r[0]), pair_id=None if r[1] is None else int(r[1]), event_type=_text(r[2]),
        actor=_text(r[3]), delta=int(r[4]), created_at=r[5], reconciled_at=r[6],
        sku=_text(r[7]), size=_text(r[8]), pair_state=_text(r[9]),
    )


def _audit(r: Sequence[Any]) -> AuditRow:
    return AuditRow(
        row_id=int(r[0]), kind=_text(r[1]), actor=_text(r[2]), action=_text(r[3]), created_at=r[4],
        plan_hash=_text(r[5]), intent_id=_text(r[6]), lease_gen=None if r[7] is None else int(r[7]),
        sent_at=r[8], sent_available=None if r[9] is None else int(r[9]),
        idempotency_key=_text(r[10]), product_id=None if r[11] is None else int(r[11]),
        step=_text(r[12]), group_id=None if r[13] is None else int(r[13]), request=_text(r[14]),
        status=_text(r[15]), outcome=_json(r[16]), payload=_json(r[17]), closed_at=r[18],
    )


def _errno(e: BaseException) -> Optional[int]:
    return getattr(e, "errno", None)


def _translate(e: BaseException) -> StoreError:
    """A bounded store error from a driver error: type + errno, never its message."""
    n = _errno(e)
    if n == _ER_DUP_ENTRY:
        return DuplicateKey(f"duplicate key (errno {n})")
    if n == _ER_LOCK_WAIT_TIMEOUT:
        return LockWaitTimeout("lock wait timeout (errno 1205)")
    if n == _ER_LOCK_DEADLOCK:
        return Deadlock("deadlock victim (errno 1213)")
    if n in _CONNECTION_LOST or type(e).__name__ in ("InterfaceError", "OperationalError"):
        return DatabaseUnavailable(f"connection failed ({type(e).__name__}, errno {n})")
    return StoreError(f"database error ({type(e).__name__}, errno {n})")


# ---------------------------------------------------------------------------
# The store
# ---------------------------------------------------------------------------
class MySQLStore:
    """One connection (one job) onto the register database."""

    def __init__(
        self,
        cfg: DBConfig,
        *,
        connect: Optional[Callable[..., Any]] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.cfg = cfg
        if connect is None:
            import mysql.connector  # lazy: the import stays off the test interpreter's path

            connect = mysql.connector.connect
        self._connect_fn = connect
        self._clock = clock
        self._conn: Any = None
        self._last_used = 0.0
        self._closed = False
        self._open()

    # -- session --------------------------------------------------------------
    def _connect_kwargs(self) -> Dict[str, Any]:
        from mysql.connector.constants import ClientFlag

        kw: Dict[str, Any] = dict(
            host=self.cfg.host, port=self.cfg.port, database=self.cfg.database,
            ssl_ca=self.cfg.ca_path, ssl_verify_cert=True, ssl_verify_identity=True,
            client_flags=[ClientFlag.FOUND_ROWS], autocommit=True, charset="utf8mb4",
            connection_timeout=CONNECT_TIMEOUT_S,
        )
        if self.cfg.option_file:
            kw["option_files"] = self.cfg.option_file
            kw["option_groups"] = ["client"]
        else:
            kw["user"] = self.cfg.user
            kw["password"] = self.cfg.password
        return kw

    def _open(self) -> None:
        for attempt in range(1, CONNECT_ATTEMPTS + 1):
            try:
                conn = self._connect_fn(**self._connect_kwargs())
                break
            except Exception as e:  # noqa: BLE001 - bounded, never the driver's text
                err = _translate(e)
                if not (isinstance(err, DatabaseUnavailable) and attempt < CONNECT_ATTEMPTS):
                    raise err from None
        try:
            cur = conn.cursor()
            cur.execute(SQL_SESSION)
            cur.execute(SQL_SESSION_CHECK)
            autocommit, database = cur.fetchone()
            cur.close()
        except Exception as e:  # noqa: BLE001
            _quiet_close(conn)
            raise _translate(e) from None
        if not getattr(conn, "autocommit", False) or int(autocommit) != 1:
            _quiet_close(conn)
            raise StoreError("autocommit is off on the register connection (§3.7 rule 1)")
        if _text(database) != self.cfg.database:
            _quiet_close(conn)
            raise StoreError("connected to an unexpected schema")
        self._conn = conn
        self._last_used = self._clock()

    def _reopen(self) -> None:
        _quiet_close(self._conn)
        self._conn = None
        self._open()

    def _ready(self) -> Any:
        if self._closed:
            raise StoreClosed("store handle used after close()")
        if self._conn is None or self._clock() - self._last_used > IDLE_REOPEN_S:
            self._reopen()
        return self._conn

    def close(self) -> None:
        self._closed = True
        _quiet_close(self._conn)
        self._conn = None

    # -- autocommit statements (one retry on a fresh session if the old one died) --
    def _run(self, fn: Callable[[Any], Any], *, retry: bool = True) -> Any:
        """Run ``fn(cursor)``; a lost connection is retried once on a fresh
        session when ``retry`` (reads and idempotent statements only)."""
        for attempt in (1, 2):
            conn = self._ready()
            cur = conn.cursor()
            try:
                out = fn(cur)
                self._last_used = self._clock()
                return out
            except Exception as e:  # noqa: BLE001
                err = _translate(e)
                if isinstance(err, DatabaseUnavailable):
                    self._conn_lost()
                    if retry and attempt == 1:
                        continue
                raise err from None
            finally:
                _quiet_close(cur)
        raise DatabaseUnavailable("unreachable")  # pragma: no cover

    def _conn_lost(self) -> None:
        _quiet_close(self._conn)
        self._conn = None

    def _select(self, sql: str, params: Sequence[Any] = ()) -> List[Tuple[Any, ...]]:
        def fn(cur: Any) -> List[Tuple[Any, ...]]:
            cur.execute(sql, tuple(params))
            return list(cur.fetchall())

        return self._run(fn)

    # -- reads ----------------------------------------------------------------
    def read_pairs(
        self,
        *,
        states: Optional[Sequence[str]] = (SHELF,),
        sku: Optional[str] = None,
        pair_ids: Optional[Sequence[int]] = None,
        outlet_product_id: Optional[int] = None,
    ) -> List[PairRow]:
        where: List[str] = []
        params: List[Any] = []
        if states is not None:
            if not states:
                return []
            where.append("STATE IN " + _in_list(len(states)))
            params.extend(states)
        if sku is not None:
            where.append("SKU = %s")
            params.append(sku)
        if pair_ids is not None:
            ids = sorted({int(i) for i in pair_ids})
            if not ids:
                return []
            where.append("ID IN " + _in_list(len(ids)))
            params.extend(ids)
        if outlet_product_id is not None:
            where.append("OUTLET_PRODUCT_ID = %s")
            params.append(int(outlet_product_id))
        sql = SQL_PAIRS + (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY ID"
        return [_pair(r) for r in self._select(sql, params)]

    def read_unstamped_events(self) -> List[EventRow]:
        return [_event(r) for r in self._select(SQL_UNSTAMPED)]

    def read_audit(
        self,
        kind: str,
        *,
        statuses: Optional[Sequence[str]] = None,
        row_ids: Optional[Sequence[int]] = None,
        group_id: Optional[int] = None,
        product_id: Optional[int] = None,
        intent_id: Optional[str] = None,
        last: Optional[int] = None,
    ) -> List[AuditRow]:
        """``last``: only the newest ``last`` rows, still returned oldest first."""
        sql = SQL_AUDIT
        params: List[Any] = [kind]
        if statuses is not None:
            if not statuses:
                return []
            sql += " AND STATUS IN " + _in_list(len(statuses))
            params.extend(statuses)
        if row_ids is not None:
            ids = sorted({int(i) for i in row_ids})
            if not ids:
                return []
            sql += " AND ID IN " + _in_list(len(ids))
            params.extend(ids)
        if group_id is not None:
            sql += " AND GROUP_ID = %s"
            params.append(int(group_id))
        if product_id is not None:
            sql += " AND PRODUCT_ID = %s"
            params.append(int(product_id))
        if intent_id is not None:
            sql += " AND INTENT_ID = %s"
            params.append(intent_id)
        if last is not None:
            if last <= 0:
                return []
            sql += " ORDER BY ID DESC LIMIT %s"
            params.append(int(last))
            return [_audit(r) for r in reversed(self._select(sql, params))]
        sql += " ORDER BY ID"
        return [_audit(r) for r in self._select(sql, params)]

    def read_lease(self) -> LeaseRow:
        rows = self._select(SQL_LEASE)
        if not rows:
            raise StoreError("sm_outlet_lease has no row ID = 1 (the DDL seeds it)")
        h, g, exp, w, wexp, upd = rows[0]
        return LeaseRow(holder=_text(h), generation=int(g), expires_at=exp, waiter=_text(w),
                        waiter_expires_at=wexp, updated_at=upd)

    def server_now(self) -> datetime:
        return self._select(SQL_NOW)[0][0]

    def read_snapshot(self, key: Optional[Tuple[str, str]] = None) -> Snapshot:
        """``START TRANSACTION READ ONLY WITH CONSISTENT SNAPSHOT`` — three plain
        SELECTs — ``COMMIT`` (a read: nothing to roll back, nothing locked)."""
        for attempt in (1, 2):
            conn = self._ready()
            cur = conn.cursor()
            try:
                conn.start_transaction(consistent_snapshot=True, readonly=True)
                kp: Tuple[Any, ...] = tuple(key) if key is not None else ()
                cur.execute(SQL_SNAP_PAIRS + (" AND SKU = %s AND SIZE = %s" if key else "") + " ORDER BY ID", kp)
                pairs = tuple(_pair(r) for r in cur.fetchall())
                cur.execute(_UNSTAMPED_WHERE + (" AND p.SKU = %s AND p.SIZE = %s" if key else "")
                            + " ORDER BY e.ID", kp)
                unstamped = tuple(_event(r) for r in cur.fetchall())
                cur.execute(SQL_SNAP_MAX + (" WHERE p.SKU = %s AND p.SIZE = %s" if key else "")
                            + " GROUP BY p.SKU, p.SIZE", kp)
                maxima = {(_text(s), _text(z)): int(m) for s, z, m in cur.fetchall()}
                conn.commit()
                self._last_used = self._clock()
                return Snapshot(pairs=pairs, unstamped=unstamped, max_event_id=maxima)
            except Exception as e:  # noqa: BLE001
                _quiet_rollback(conn)
                err = _translate(e)
                if isinstance(err, DatabaseUnavailable):
                    self._conn_lost()
                    if attempt == 1:
                        continue
                raise err from None
            finally:
                _quiet_close(cur)
        raise DatabaseUnavailable("unreachable")  # pragma: no cover

    def read_import_payloads(self, pair_ids: Sequence[int]) -> List[Tuple[int, Any]]:
        ids = sorted({int(i) for i in pair_ids})
        if not ids:
            return []
        rows = self._select(SQL_IMPORT_PAYLOADS + _in_list(len(ids)) + " ORDER BY e.ID", ids)
        out = []
        for pid, payload in rows:
            try:
                out.append((int(pid), _json(payload)))
            except ValueError:
                out.append((int(pid), None))   # an unparseable payload: no seed for that pair
        return out

    # -- the lease statements ---------------------------------------------------
    def lease_take(self, holder: str, ttl_ms: int) -> Optional[int]:
        """One guarded take. Never retried blind: if the connection dies, the
        take may have landed — the lease is read back, and a row naming this
        holder IS the take (its generation adopted)."""

        def fn(cur: Any) -> Optional[int]:
            cur.execute(SQL_LEASE_TAKE, (holder, int(ttl_ms) * 1000, holder, holder))
            if cur.rowcount != 1:
                return None
            cur.execute(SQL_LAST_ID)
            return int(cur.fetchone()[0])

        try:
            return self._run(fn, retry=False)
        except DatabaseUnavailable:
            lease = self.read_lease()
            if lease.holder == holder:
                return lease.generation
            return None

    def lease_release(self, fence: Fence) -> bool:
        def fn(cur: Any) -> bool:
            cur.execute(SQL_LEASE_RELEASE, (fence.holder, fence.generation))
            return cur.rowcount == 1

        return self._run(fn)

    def lease_wait(self, holder: str, wait_ms: int) -> bool:
        def fn(cur: Any) -> bool:
            cur.execute(SQL_LEASE_WAIT, (holder, int(wait_ms) * 1000, holder))
            return cur.rowcount == 1

        return self._run(fn)

    # -- the one write primitive ---------------------------------------------------
    def fenced_write(self, fence: Fence, ops: Sequence[WriteOp]) -> List[int]:
        """Renew first, the ops, ``COMMIT`` — one transaction. A connection lost
        before ``COMMIT`` was sent rolled everything back: retried once on a fresh
        session. A failure DURING ``COMMIT`` is :class:`CommitUnknown`."""
        check_op_order(ops)
        for attempt in (1, 2):
            conn = self._ready()
            cur = conn.cursor()
            in_commit = False
            try:
                conn.start_transaction()
                cur.execute(SQL_LEASE_RENEW, (int(fence.ttl_ms) * 1000, fence.holder, fence.generation))
                if cur.rowcount != 1:
                    raise LeaseLost("renew matched no row")
                inserted: List[int] = []
                for i, op in enumerate(ops):
                    new_id = self._apply(cur, i, op, fence, inserted)
                    if new_id is not None:
                        inserted.append(new_id)
                in_commit = True
                conn.commit()
                self._last_used = self._clock()
                return inserted
            except (LeaseLost, GuardMismatch, ValueError, TypeError):
                _quiet_rollback(conn)
                raise
            except BaseException as e:  # noqa: BLE001 - every exit rolls back
                _quiet_rollback(conn)
                if not isinstance(e, Exception):
                    raise
                if in_commit:
                    self._conn_lost()
                    raise CommitUnknown(f"COMMIT outcome unknown ({type(e).__name__}, errno {_errno(e)})") from None
                err = _translate(e)
                if isinstance(err, DatabaseUnavailable):
                    self._conn_lost()
                    if attempt == 1:
                        continue
                raise err from None
            finally:
                _quiet_close(cur)
        raise DatabaseUnavailable("unreachable")  # pragma: no cover

    def _apply(self, cur: Any, i: int, op: WriteOp, fence: Fence, inserted: Sequence[int] = ()) -> Optional[int]:
        if isinstance(op, InsertAudit):
            cur.execute(SQL_INSERT_AUDIT, (
                op.kind, op.actor, op.action, op.plan_hash, op.intent_id,
                fence.generation if op.kind == KIND_RECONCILE else None,
                op.idempotency_key, op.product_id, op.step, op.group_id, op.status,
                _dumps(op.outcome), _dumps(op.payload), 1 if op.status in TERMINAL else 0,
            ))
            return int(cur.lastrowid)
        if isinstance(op, UpdateAudit):
            sets: List[str] = []
            params: List[Any] = []
            if op.mark_sent:
                sets.append("SENT_AT = UTC_TIMESTAMP(3)")
            if op.first_send:
                sets.extend(["SENT_AVAILABLE = %s", "REQUEST = %s"])
                params.extend([op.sent_available, op.request])
            if op.product_id is not None:
                sets.append("PRODUCT_ID = %s")
                params.append(op.product_id)
            if op.step is not None:
                sets.append("STEP = %s")
                params.append(op.step)
            if op.outcome is not None:
                sets.append("OUTCOME = %s")
                params.append(_dumps(resolve_inserted(op.outcome, inserted)))
            if op.status is not None:
                sets.append("STATUS = %s")
                params.append(op.status)
                if op.status in TERMINAL:
                    sets.append("CLOSED_AT = UTC_TIMESTAMP(3)")
            sql = (
                "UPDATE sm_outlet_audit SET " + ", ".join(sets)
                + " WHERE ID = %s AND KIND = 'RECONCILE' AND STATUS = %s"
                + (" AND SENT_AT IS NULL" if op.first_send else "")
            )
            cur.execute(sql, tuple(params) + (op.row_id, op.expect_status))
            if cur.rowcount != 1:
                raise GuardMismatch(i, 1, max(cur.rowcount, 0))
            return None
        if isinstance(op, StampEvents):
            ids = sorted({int(x) for x in op.event_ids})
            cur.execute(SQL_STAMP_HEAD + _in_list(len(ids)), tuple(ids))
            if cur.rowcount != len(ids):
                raise GuardMismatch(i, len(ids), max(cur.rowcount, 0))
            return None
        if isinstance(op, UpdatePairs):
            ids = sorted({int(x) for x in op.pair_ids})
            cols = sorted(op.values)
            sets = [f"{c} = %s" for c in cols] + ["UPDATED_AT = UTC_TIMESTAMP(3)"]
            params = [_pair_value(c, op.values[c]) for c in cols]
            sql = "UPDATE sc_returned_pair SET " + ", ".join(sets) + " WHERE ID IN " + _in_list(len(ids))
            params.extend(ids)
            if op.require_state is not None:
                sql += " AND STATE = %s"
                params.append(op.require_state)
            if op.require_outlet_product_id is not None:
                sql += " AND OUTLET_PRODUCT_ID = %s"
                params.append(int(op.require_outlet_product_id))
            cur.execute(sql, tuple(params))
            if cur.rowcount != len(ids):
                raise GuardMismatch(i, len(ids), max(cur.rowcount, 0))
            return None
        raise TypeError(f"unknown op {type(op).__name__}")


def _pair_value(col: str, value: Any) -> Any:
    if col == "ONLINE":
        return 1 if value else 0
    if col == "OUTLET_PRODUCT_ID":
        return None if value is None else int(value)
    return value


def _quiet_close(obj: Any) -> None:
    if obj is None:
        return
    try:
        obj.close()
    except Exception:  # noqa: BLE001
        pass


def _quiet_rollback(conn: Any) -> None:
    try:
        conn.rollback()
    except Exception:  # noqa: BLE001 - a dead connection has rolled back already
        pass


def make_register_factory(
    load: Callable[[], DBConfig] = load_db_config,
) -> Callable[..., Any]:
    """``app.state.register_factory``: a new connection per job, the config read
    at the first call (fail-closed: ``ConfigError`` → the job's ``config_error``)."""
    from backend.persistence.register import DEFAULT_ACTOR, Register

    def factory(actor: str = DEFAULT_ACTOR) -> Any:
        return Register(MySQLStore(load()), actor=actor)

    return factory


__all__ = [
    "DBConfig", "MySQLStore", "load_db_config", "make_register_factory", "STATEMENTS",
    "SESSION_WAIT_TIMEOUT_S", "SESSION_LOCK_WAIT_TIMEOUT_S", "IDLE_REOPEN_S",
]
