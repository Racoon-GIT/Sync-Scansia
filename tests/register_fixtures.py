"""Test fixtures for the register: one API over the in-memory fake (always) and,
from §4.3, ``racoon_staging`` (never ``racoon``).

A *fixture backend* exposes:

* ``store()``    — a fresh app-side store handle (what production builds);
* ``register()`` — a :class:`~backend.persistence.register.Register` on it;
* ``advance(ms)`` — move the SERVER clock forward (memory: the fake clock;
  MySQL: a real sleep — the server clock cannot be moved);
* DML standing for the register's OTHER writers (STOCK_Manager, the SPs):
  ``add_pair``, ``add_event``, ``add_shelf_pair``, ``consume``,
  ``leave_shelf``, ``seed_audit``.
"""
from __future__ import annotations

from typing import Any, Optional

from backend.persistence.memory_store import FakeClock, MemoryDB
from backend.persistence.records import (
    DONE,
    KIND_BASELINE,
    SCOPE_REGISTER,
    InsertAudit,
)
from backend.persistence.register import Register

PROMO = "gid://shopify/Location/PROMO"


class MemoryBackend:
    """The in-memory fake behind the fixture API."""

    name = "memory"
    scale = 1   # time margins of the liveness tests

    def __init__(self) -> None:
        self.clock = FakeClock()
        self.db = MemoryDB(clock=self.clock)

    def store(self) -> Any:
        return self.db.connect()

    def register(self, actor: str = "test") -> Register:
        return Register(self.store(), actor=actor)

    def advance(self, ms: int) -> None:
        self.clock.advance(milliseconds=ms)

    # DML of the other writers
    def add_pair(self, sku: str, size: str, **kw: Any) -> int:
        return self.db.add_pair(sku, size, **kw)

    def add_event(self, pair_id: Optional[int], delta: int, **kw: Any) -> int:
        return self.db.add_event(pair_id, delta, **kw)

    def add_shelf_pair(self, sku: str, size: str, **kw: Any):
        return self.db.add_shelf_pair(sku, size, **kw)

    def consume(self, sku: str, size: str, *, outlet_line: bool = False) -> int:
        return self.db.consume(sku, size, outlet_line=outlet_line)

    def leave_shelf(self, pair_id: int, to_state: str, **kw: Any) -> int:
        return self.db.leave_shelf(pair_id, to_state, **kw)

    def seed_audit(self, row: InsertAudit, *, generation: Optional[int] = None) -> int:
        return self.db.seed_audit(row, generation=generation)

    def seed_baseline(self, *, held=(), stamped=()) -> int:
        """A whole-register BASELINE row as 'Inizializza' leaves it."""
        return self.seed_audit(InsertAudit(
            kind=KIND_BASELINE, actor="test", action="baseline",
            payload={"scope": SCOPE_REGISTER, "held": list(held), "stamped": list(stamped)},
        ))


class MySQLBackend:
    """``racoon_staging`` (F7.1, SERVER) behind the same fixture API — NEVER ``racoon``.

    Opt-in: ``SCANSIA_STAGING=1`` and both credential files present
    (``~/.config/racoon-scansia/<user>.cred``, MySQL option files written by
    SERVER — read by the driver, never opened, never printed). The app side is
    ``scansia_mgr_stg`` (the grant of §3.6); the other writers' DML and
    ``SP_SCANSIA_CONSUME`` run as ``scansia_fixture_stg``. Every row the suite
    writes is marked (``INTAKE_KEY`` / ``DEDUP_KEY`` prefix ``f7test:``, order ids
    ``73570…``, audit ``ACTOR`` prefix ``f7test``) and deleted at each start, so
    SERVER's own fixtures (A10) are left alone and assertions stay scoped.
    """

    name = "mysql"
    scale = 5   # WAN round trips (a TLS connect took 0.3–1 s, 2026-10-06): liveness margins ×5
    HOST = "78.46.244.227"
    PORT = 3306
    DATABASE = "racoon_staging"
    ACTOR = "f7test"
    _ORDER_PREFIX = "73570"

    @classmethod
    def cred(cls, user: str) -> str:
        import os

        return os.path.expanduser(f"~/.config/racoon-scansia/{user}.cred")

    @classmethod
    def ca_path(cls) -> str:
        import os
        from pathlib import Path

        return os.environ.get("SCANSIA_TEST_CA") or str(
            Path(__file__).resolve().parents[3] / "SERVER" / "racoon-internal-ca.pem"
        )

    @classmethod
    def available(cls) -> Optional[str]:
        """``None`` when usable, else why not (the skip reason)."""
        import os

        if os.environ.get("SCANSIA_STAGING") != "1":
            return "staging tests are opt-in: SCANSIA_STAGING=1 (racoon_staging only)"
        for user in ("scansia_mgr_stg", "scansia_fixture_stg"):
            if not os.path.exists(cls.cred(user)):
                return f"missing option file for {user}"
        if not os.path.exists(cls.ca_path()):
            return "missing the in-house CA"
        import importlib.util

        if importlib.util.find_spec("mysql") is None:
            return "mysql-connector-python not installed"
        return None

    def __init__(self) -> None:
        import mysql.connector
        from mysql.connector.constants import ClientFlag

        for attempt in (1, 2):  # one retry of a lost connect, as the store does (CONNECT_ATTEMPTS)
            try:
                self._fx = mysql.connector.connect(
                    option_files=self.cred("scansia_fixture_stg"), option_groups=["client"],
                    host=self.HOST, port=self.PORT, database=self.DATABASE,
                    ssl_ca=self.ca_path(), ssl_verify_cert=True, ssl_verify_identity=True,
                    client_flags=[ClientFlag.FOUND_ROWS], autocommit=True, charset="utf8mb4",
                    connection_timeout=10,
                )
                break
            except mysql.connector.errors.DatabaseError as e:
                if e.errno != 2003 or attempt == 2:
                    raise
        db = self._q("SELECT DATABASE()")[0][0]
        if db != self.DATABASE:  # the fixture refuses to run anywhere else
            self._fx.close()
            raise RuntimeError("the staging fixture is connected to the wrong schema")
        self.reset()

    # -- plumbing ---------------------------------------------------------------
    def _q(self, sql: str, params: tuple = ()) -> list:
        cur = self._fx.cursor()
        try:
            cur.execute(sql, params)
            return cur.fetchall() if cur.with_rows else []
        finally:
            cur.close()

    def _x(self, sql: str, params: tuple = ()) -> int:
        cur = self._fx.cursor()
        try:
            cur.execute(sql, params)
            return int(cur.lastrowid or 0)
        finally:
            cur.close()

    def reset(self) -> None:
        mark = "f7test:%"
        self._x("DELETE e FROM sc_returned_pair_event e JOIN sc_returned_pair p ON p.ID = e.PAIR_ID "
                "WHERE p.INTAKE_KEY LIKE %s", (mark,))
        self._x("DELETE FROM sc_returned_pair_event WHERE DEDUP_KEY LIKE %s OR DEDUP_KEY LIKE %s",
                (mark, f"consume-miss:ord:{self._ORDER_PREFIX}%"))
        self._x("DELETE FROM sc_returned_pair WHERE INTAKE_KEY LIKE %s", (mark,))
        self._x("DELETE FROM sm_outlet_audit WHERE ACTOR LIKE %s", (f"{self.ACTOR}%",))
        self._x("UPDATE sm_outlet_lease SET HOLDER = NULL, EXPIRES_AT = NULL, WAITER = NULL, "
                "WAITER_EXPIRES_AT = NULL, UPDATED_AT = UTC_TIMESTAMP(3) WHERE ID = 1")

    def close(self) -> None:
        self._fx.close()

    # -- the fixture API ------------------------------------------------------------
    def store(self, tap: Optional["Tap"] = None) -> Any:
        from backend.persistence.mysql_store import DBConfig, MySQLStore

        cfg = DBConfig(host=self.HOST, port=self.PORT, database=self.DATABASE,
                       ca_path=self.ca_path(), option_file=self.cred("scansia_mgr_stg"))
        if tap is None:
            return MySQLStore(cfg)
        import mysql.connector

        return MySQLStore(cfg, connect=lambda **kw: tap.wrap(mysql.connector.connect(**kw)))

    def event_type(self, event_id: int) -> str:
        return self._q("SELECT EVENT_TYPE FROM sc_returned_pair_event WHERE ID = %s", (event_id,))[0][0]

    def register(self, actor: str = ACTOR) -> Register:
        return Register(self.store(), actor=actor if actor.startswith(self.ACTOR) else self.ACTOR)

    def advance(self, ms: int) -> None:
        import time

        time.sleep(ms / 1000.0 + 0.05)

    def add_pair(self, sku: str, size: str, *, state: str = "IN_SCANSIA", price_high: Any = None,
                 price: Any = None, price_outlet: Any = None, discount: Any = None,
                 outlet_product_id: Optional[int] = None, online: bool = False,
                 product_title: Optional[str] = None, brand: Optional[str] = None,
                 pair_uuid: Optional[str] = None) -> int:
        import uuid

        return self._x(
            "INSERT INTO sc_returned_pair (PAIR_UUID, SKU, SIZE, STATE, PRICE_HIGH, PRICE, PRICE_OUTLET, "
            "DISCOUNT, OUTLET_PRODUCT_ID, ONLINE, PRODUCT_TITLE, BRAND, SOURCE, INTAKE_KEY, SELLABLE_AT, "
            "CREATED_AT, UPDATED_AT) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'MANUAL', %s, "
            "IF(%s, UTC_TIMESTAMP(3), NULL), UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))",
            (pair_uuid or str(uuid.uuid4()), sku, size, state, price_high, price, price_outlet, discount,
             outlet_product_id, 1 if online else 0, product_title, brand, f"f7test:{uuid.uuid4().hex}",
             1 if state == "IN_SCANSIA" else 0),
        )

    def add_event(self, pair_id: Optional[int], delta: int, *, event_type: str = "IMPORTED",
                  actor: str = "IMPORT", reconciled: bool = False, payload: Optional[dict] = None) -> int:
        import json
        import uuid

        return self._x(
            "INSERT INTO sc_returned_pair_event (PAIR_ID, EVENT_TYPE, DEDUP_KEY, ACTOR, OUTLET_DELTA, "
            "OUTLET_RECONCILED_AT, PAYLOAD, CREATED_AT) VALUES (%s, %s, %s, %s, %s, "
            "IF(%s, UTC_TIMESTAMP(3), NULL), %s, UTC_TIMESTAMP(3))",
            (pair_id, event_type, f"f7test:{uuid.uuid4().hex}", actor, int(delta), 1 if reconciled else 0,
             None if payload is None else json.dumps(payload)),
        )

    def add_shelf_pair(self, sku: str, size: str, *, event_type: str = "ARRIVE", actor: str = "GATE",
                       reconciled: bool = False, **pair_kw: Any):
        pid = self.add_pair(sku, size, state="IN_SCANSIA", **pair_kw)
        return pid, self.add_event(pid, +1, event_type=event_type, actor=actor, reconciled=reconciled)

    def leave_shelf(self, pair_id: int, to_state: str, *, event_type: str, actor: str = "OPERATOR",
                    delta: int = -1) -> int:
        self._x("UPDATE sc_returned_pair SET STATE = %s, UPDATED_AT = UTC_TIMESTAMP(3) WHERE ID = %s",
                (to_state, pair_id))
        return self.add_event(pair_id, delta, event_type=event_type, actor=actor)

    def consume(self, sku: str, size: str, *, outlet_line: bool = False) -> int:
        """The real ``SP_SCANSIA_CONSUME`` (staging copy of the live text)."""
        import random

        order = self._ORDER_PREFIX + "".join(random.choice("0123456789") for _ in range(10))
        line = "".join(random.choice("0123456789") for _ in range(9))
        cur = self._fx.cursor()
        try:
            cur.callproc("SP_SCANSIA_CONSUME", (order, f"#F7T{order[-6:]}", line, sku, size,
                                                "Test - Outlet" if outlet_line else "Test",
                                                "2026-10-06T12:00:00Z"))
            for res in cur.stored_results():
                res.fetchall()
        finally:
            cur.close()
        key = f"ord:{order}:{line}"
        rows = self._q("SELECT ID FROM sc_returned_pair_event WHERE DEDUP_KEY IN (%s, %s)",
                       (f"consume:{key}", f"consume-miss:{key}"))
        return int(rows[0][0])

    def seed_audit(self, row: InsertAudit, *, generation: Optional[int] = None) -> int:
        import json

        from backend.persistence.records import KIND_RECONCILE, TERMINAL

        dumps = (lambda v: None if v is None else json.dumps(dict(v), default=str))
        return self._x(
            "INSERT INTO sm_outlet_audit (KIND, ACTOR, ACTION, PLAN_HASH, INTENT_ID, LEASE_GEN, "
            "IDEMPOTENCY_KEY, PRODUCT_ID, STEP, GROUP_ID, STATUS, OUTCOME, PAYLOAD, CREATED_AT, CLOSED_AT) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP(3), "
            "IF(%s, UTC_TIMESTAMP(3), NULL))",
            (row.kind, self.ACTOR, row.action, row.plan_hash, row.intent_id,
             generation if row.kind == KIND_RECONCILE else None, row.idempotency_key, row.product_id,
             row.step, row.group_id, row.status, dumps(row.outcome), dumps(row.payload),
             1 if row.status in TERMINAL else 0),
        )

    def seed_baseline(self, *, held=(), stamped=()) -> int:
        return self.seed_audit(InsertAudit(
            kind=KIND_BASELINE, actor=self.ACTOR, action="baseline",
            payload={"scope": SCOPE_REGISTER, "held": list(held), "stamped": list(stamped)},
        ))


class Tap:
    """Two seams on the app's real connection (staging only): every statement the
    store sends is recorded, and an armed COMMIT pauses with the transaction's
    locks still held — what a concurrent ``SP_SCANSIA_CONSUME`` would meet."""

    def __init__(self) -> None:
        import threading

        self.statements: list = []
        self.pause_s = 0.0
        self.commit_entered = threading.Event()
        self.commit_sent = 0.0
        self.conn: Any = None

    def arm(self, pause_s: float) -> None:
        self.pause_s = pause_s
        self.commit_entered.clear()

    def wrap(self, conn: Any) -> Any:
        self.conn = conn
        return _TapConnection(conn, self)

    def explain(self, sql: str, params: tuple) -> list:
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute("EXPLAIN " + sql, params)
            return cur.fetchall()
        finally:
            cur.close()


class _TapCursor:
    def __init__(self, cur: Any, tap: Tap) -> None:
        self._cur, self._tap = cur, tap

    def __getattr__(self, name: str) -> Any:
        return getattr(self._cur, name)

    def execute(self, sql: str, params: Any = ()) -> Any:
        self._tap.statements.append((sql, tuple(params or ())))
        return self._cur.execute(sql, params)


class _TapConnection:
    def __init__(self, conn: Any, tap: Tap) -> None:
        self._conn, self._tap = conn, tap

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    def cursor(self, *a: Any, **k: Any) -> Any:
        return _TapCursor(self._conn.cursor(*a, **k), self._tap)

    def commit(self) -> None:
        import time

        if self._tap.pause_s > 0:
            pause, self._tap.pause_s = self._tap.pause_s, 0.0
            self._tap.commit_entered.set()
            time.sleep(pause)
        self._tap.commit_sent = time.monotonic()
        self._conn.commit()


class StubLease:
    """A held lease without the client rules: fence + renew hook (for store /
    register tests that are not about the lease client)."""

    def __init__(self, register: Register, holder: str = "test-holder", ttl_ms: int = 300_000) -> None:
        from backend.persistence.records import Fence

        gen = register.lease_take(holder, ttl_ms)
        assert gen is not None, "lease busy in fixture"
        self._fence = Fence(holder, gen, ttl_ms)
        self.renewals = 0

    @property
    def fence(self):
        return self._fence

    def monotonic(self) -> float:
        import time

        return time.monotonic()

    def mark_renewed(self, started: float) -> None:
        self.renewals += 1


def stamp_carrier(event_ids, keys=(), action="stamp-only") -> InsertAudit:
    """A RECONCILE row closed DONE that carries a stamp with no Shopify write."""
    from backend.persistence.records import KIND_RECONCILE

    return InsertAudit(
        kind=KIND_RECONCILE, actor="test", action=action, status=DONE,
        payload={"event_ids": sorted(event_ids), "keys": [list(k) for k in keys]},
    )


__all__ = ["MemoryBackend", "MySQLBackend", "StubLease", "stamp_carrier", "PROMO"]


def seed_reconcile(register: Register, *, sent: bool = False, status: str = "OPEN",
                   outcome: Optional[dict] = None, group_id: int = 7, **spec: Any) -> int:
    """A ``RECONCILE`` row as an earlier pass would have left it: written ahead
    (``ledger.RowSpec(**spec)``), optionally sent, optionally given an outcome —
    under a lease taken and released for the purpose."""
    from backend.services import ledger as L

    stub = StubLease(register, holder="seed")
    try:
        (rid,) = register.write_ahead(stub, [L.RowSpec(**spec).to_insert(actor="seed", group_id=group_id)])
        if sent:
            register.mark_sent(stub, rid, first=True, sent_available=0, request='{"query": "x", "variables": {}}')
        if status != "OPEN":
            register.record_outcome(stub, rid, expect="OPEN", status=status, outcome=outcome or {})
    finally:
        register.lease_release(stub)
    return rid
