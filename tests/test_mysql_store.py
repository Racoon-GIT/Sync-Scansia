"""MySQL store — unit level, offline (plan §3.7; the contract on ``racoon_staging``
is ``tests/test_register_store.py``).

Every statement goes through the driver's own parameter substitution
(``tests/mysql_offline.py``), so a literal ``%`` or a placeholder/parameter
mismatch fails here, not in production. Covered: the connection discipline
(TLS by IP against the pinned CA, ``CLIENT_FOUND_ROWS``, autocommit asserted,
the session settings, the schema check), the fenced transaction (renew first,
the ops, the register UPDATE last, ROLLBACK on every failure), the error
mapping, the idle re-open, the take read back after a lost connection, and the
env configuration (fail-closed, the CA from base64, no password in ``repr``).
"""
from __future__ import annotations

import base64
import os
import re
import stat
from decimal import Decimal

import pytest

from tests.mysql_offline import OfflineDriver, driver_error, substitute

from backend.config import ConfigError  # noqa: E402
from backend.persistence import mysql_store  # noqa: E402
from backend.persistence.mysql_store import (  # noqa: E402
    STATEMENTS,
    DBConfig,
    MySQLStore,
    load_db_config,
    make_register_factory,
)
from backend.persistence.records import (  # noqa: E402
    APPLIED,
    DONE,
    KIND_AUDIT,
    KIND_RECONCILE,
    OPEN,
    SHELF,
    CommitUnknown,
    DatabaseUnavailable,
    DuplicateKey,
    Fence,
    GuardMismatch,
    InsertAudit,
    LeaseLost,
    LockWaitTimeout,
    StampEvents,
    StoreClosed,
    UpdateAudit,
    UpdatePairs,
)

CFG = DBConfig(host="78.46.244.227", port=3306, database="racoon_staging",
               ca_path="/ca.pem", user="u", password="p")
FENCE = Fence("h", 7, 300_000)


class Mono:
    def __init__(self) -> None:
        self.t = 100.0

    def __call__(self) -> float:
        return self.t


def ok(rowcount=1, lastrowid=None, rows=None):
    return rowcount, lastrowid, rows or []


def _in_count(sql):
    m = re.search(r"WHERE ID IN \(([^)]*)\)", sql)
    return len(m.group(1).split(",")) if m else 1


def default_handler(sql, conn):
    """Every guarded statement matches all the rows it names."""
    if sql.startswith("INSERT INTO sm_outlet_audit"):
        return ok(1, 41)
    return ok(_in_count(sql))


def _store(handler=default_handler, **kw):
    drv = OfflineDriver(handler, **kw)
    mono = Mono()
    return drv, mono, MySQLStore(CFG, connect=drv.connect, clock=mono)


# ---------------------------------------------------------------------------
# Statement hygiene (§3.7 rule 7)
# ---------------------------------------------------------------------------

def test_no_literal_percent_in_any_statement_template():
    for sql in STATEMENTS:
        assert "%" not in sql.replace("%s", ""), sql


def test_the_driver_trap_is_real_negative_control():
    with pytest.raises(Exception, match="Not enough parameters"):
        substitute("SELECT DATE_FORMAT(x, '%%Y-%%s') FROM t WHERE a = %s", (5,))


def test_every_dynamic_statement_substitutes_cleanly():
    drv, _, s = _store()
    s.fenced_write(FENCE, [
        InsertAudit(kind=KIND_RECONCILE, actor="a", action="x", status=OPEN, payload={"k": "50%"}),
        UpdateAudit(9, OPEN, mark_sent=True, first_send=True, sent_available=2, request='{"q":"%s"}'),
        UpdateAudit(9, OPEN, status=APPLIED, outcome={"a": 1}, product_id=5, step="s1"),
        UpdatePairs((1, 2), {"PRICE_OUTLET": Decimal("59.00"), "DISCOUNT": None}, require_state=SHELF),
    ])
    s.read_pairs(states=(SHELF, "VENDUTO"), sku="A%B", pair_ids=[3], outlet_product_id=4)
    s.read_audit(KIND_RECONCILE, statuses=[OPEN], row_ids=[1], group_id=2, product_id=3, intent_id="i")
    sent = "\n".join(drv.statements)
    assert r"""'{\"k\":\"50%\"}'""" in sent         # a % inside a bound VALUE is data, not syntax
    assert r"""REQUEST = '{\"q\":\"%s\"}'""" in sent
    assert "SKU = 'A%B'" in sent


# ---------------------------------------------------------------------------
# Connection discipline
# ---------------------------------------------------------------------------

def test_connect_pins_tls_by_ip_found_rows_and_autocommit():
    from mysql.connector.constants import ClientFlag

    drv, _, s = _store()
    kw = drv.connections[0].kwargs
    assert kw["host"] == "78.46.244.227" and kw["ssl_ca"] == "/ca.pem"
    assert kw["ssl_verify_cert"] is True and kw["ssl_verify_identity"] is True
    assert kw["client_flags"] == [ClientFlag.FOUND_ROWS] and kw["autocommit"] is True
    first = drv.connections[0].log[0]
    assert "time_zone = '+00:00'" in first and "wait_timeout = 60" in first
    assert "innodb_lock_wait_timeout = 5" in first


def test_refuses_a_session_with_autocommit_off():
    with pytest.raises(Exception, match="autocommit"):
        _store(autocommit_reply=0)


def test_refuses_an_unexpected_schema():
    with pytest.raises(Exception, match="unexpected schema"):
        _store(database="racoon")


def test_connect_failure_is_bounded():
    def boom(**kw):
        raise driver_error(2003, "InterfaceError")

    with pytest.raises(DatabaseUnavailable) as ei:
        MySQLStore(CFG, connect=boom)
    assert "simulated" not in str(ei.value)          # never the driver's text


def test_an_idle_session_is_reopened_before_the_next_statement():
    drv, mono, s = _store(lambda sql, c: ok(1, rows=[(1, "x", "S", "42", SHELF, None, None, None,
                                                      None, None, 0, None, None)]))
    s.read_pairs()
    assert len(drv.connections) == 1
    mono.t += mysql_store.IDLE_REOPEN_S + 1
    s.read_pairs()
    assert len(drv.connections) == 2 and drv.connections[0].closed


def test_close_ends_the_connection_and_refuses_reuse():
    drv, _, s = _store()
    s.close()
    assert drv.connections[0].closed
    with pytest.raises(StoreClosed):
        s.read_lease()


# ---------------------------------------------------------------------------
# The fenced transaction
# ---------------------------------------------------------------------------

def test_fenced_write_is_renew_then_ops_then_commit_register_last():
    drv, _, s = _store()
    ids = s.fenced_write(FENCE, [
        InsertAudit(kind=KIND_RECONCILE, actor="a", action="add", status=DONE,
                    payload={"event_ids": [5]}),
        StampEvents((5, 6, 5)),
    ])
    assert ids == [41]
    log = drv.connections[0].log[2:]          # after the two session statements
    assert log[0] == "START TRANSACTION"
    assert log[1].startswith("UPDATE sm_outlet_lease SET EXPIRES_AT")
    assert "HOLDER = 'h' AND GENERATION = 7 AND EXPIRES_AT > UTC_TIMESTAMP(3)" in log[1]
    assert "300000000 MICROSECOND" in log[1]
    assert log[2].startswith("INSERT INTO sm_outlet_audit")
    assert log[3].endswith("WHERE ID IN (5, 6)")       # distinct ids, by PRIMARY KEY
    assert "COALESCE(OUTLET_RECONCILED_AT, UTC_TIMESTAMP(3))" in log[3]
    assert log[4] == "COMMIT"


def test_reconcile_insert_carries_the_generation_and_closes_terminal_rows():
    drv, _, s = _store()
    s.fenced_write(FENCE, [InsertAudit(kind=KIND_RECONCILE, actor="a", action="x", status=DONE)])
    s.fenced_write(FENCE, [InsertAudit(kind=KIND_AUDIT, actor="a", action="y")])
    inserts = [x for x in drv.statements if x.startswith("INSERT")]
    assert ", 7, " in inserts[0] and inserts[0].endswith("IF(1, UTC_TIMESTAMP(3), NULL))")
    assert ", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, UTC_TIMESTAMP(3), IF(0," in inserts[1]


def test_a_lost_renew_is_lease_lost_and_rolled_back():
    def h(sql, conn):
        if sql.startswith("UPDATE sm_outlet_lease SET EXPIRES_AT"):
            return ok(0)
        return ok(1, 1)

    drv, _, s = _store(h)
    with pytest.raises(LeaseLost):
        s.fenced_write(FENCE, [InsertAudit(kind=KIND_AUDIT, actor="a", action="x")])
    conn = drv.connections[0]
    assert conn.rollbacks == 1 and conn.commits == 0
    assert not any(x.startswith("INSERT") for x in conn.log)


@pytest.mark.parametrize("op,matched", [
    (StampEvents((1, 2, 3)), 2),
    (UpdatePairs((1, 2), {"ONLINE": True}, require_state=SHELF), 1),
    (UpdateAudit(4, APPLIED, status=DONE), 0),
])
def test_a_short_match_rolls_the_whole_transaction_back(op, matched):
    def h(sql, conn):
        if sql.startswith("UPDATE sm_outlet_lease"):
            return ok(1)
        if sql.startswith("INSERT"):
            return ok(1, 3)
        return ok(matched)

    drv, _, s = _store(h)
    with pytest.raises(GuardMismatch):
        s.fenced_write(FENCE, [InsertAudit(kind=KIND_AUDIT, actor="a", action="x"), op])
    conn = drv.connections[0]
    assert conn.rollbacks == 1 and conn.commits == 0


def test_audit_update_guards_status_kind_and_first_send():
    drv, _, s = _store()
    s.fenced_write(FENCE, [UpdateAudit(9, OPEN, mark_sent=True, first_send=True, sent_available=3, request="{}")])
    upd = [x for x in drv.statements if x.startswith("UPDATE sm_outlet_audit")][0]
    assert upd.endswith("WHERE ID = 9 AND KIND = 'RECONCILE' AND STATUS = 'OPEN' AND SENT_AT IS NULL")
    assert "SENT_AT = UTC_TIMESTAMP(3), SENT_AVAILABLE = 3, REQUEST = '{}'" in upd


def test_pair_update_sets_updated_at_and_its_guards():
    drv, _, s = _store()
    s.fenced_write(FENCE, [UpdatePairs((3, 1), {"OUTLET_PRODUCT_ID": None}, require_outlet_product_id=9)])
    upd = [x for x in drv.statements if x.startswith("UPDATE sc_returned_pair ")][0]
    assert upd == ("UPDATE sc_returned_pair SET OUTLET_PRODUCT_ID = NULL, UPDATED_AT = UTC_TIMESTAMP(3) "
                   "WHERE ID IN (1, 3) AND OUTLET_PRODUCT_ID = 9")


def test_duplicate_intent_is_duplicate_key():
    drv, _, s = _store()
    drv.fail_on = lambda sql: driver_error(1062, "IntegrityError") if sql.startswith("INSERT") else None
    with pytest.raises(DuplicateKey):
        s.fenced_write(FENCE, [InsertAudit(kind=KIND_AUDIT, actor="a", action="x", intent_id="i" * 32)])
    assert drv.connections[0].rollbacks == 1


def test_lock_wait_timeout_is_bounded_and_rolled_back():
    drv, _, s = _store()
    drv.fail_on = lambda sql: driver_error(1205, "DatabaseError") if sql.startswith("UPDATE sc_returned") else None
    with pytest.raises(LockWaitTimeout):
        s.fenced_write(FENCE, [UpdatePairs((1,), {"ONLINE": False})])
    assert drv.connections[0].rollbacks == 1 and drv.connections[0].commits == 0


def test_a_failed_commit_is_commit_unknown_never_retried():
    drv, _, s = _store()
    drv.fail_commit = True
    with pytest.raises(CommitUnknown):
        s.fenced_write(FENCE, [InsertAudit(kind=KIND_AUDIT, actor="a", action="x")])
    assert sum(c.transactions for c in drv.connections) == 1


def test_a_connection_lost_before_commit_is_retried_once_on_a_fresh_session():
    seen = {"n": 0}
    drv, _, s = _store()

    def lose_first_insert(sql):
        if sql.startswith("INSERT"):
            seen["n"] += 1
            if seen["n"] == 1:
                return driver_error(2013, "OperationalError")
        return None

    drv.fail_on = lose_first_insert
    assert s.fenced_write(FENCE, [InsertAudit(kind=KIND_AUDIT, actor="a", action="x")]) == [41]
    assert len(drv.connections) == 2
    assert drv.connections[0].rollbacks == 1 and drv.connections[1].commits == 1


# ---------------------------------------------------------------------------
# The lease statements
# ---------------------------------------------------------------------------

def test_take_reads_the_generation_from_last_insert_id():
    def h(sql, conn):
        if sql.startswith("UPDATE sm_outlet_lease SET HOLDER = 'pub:1'"):
            return ok(1)
        if sql == "SELECT LAST_INSERT_ID()":
            return ok(1, rows=[(12,)])
        return ok(0)

    drv, _, s = _store(h)
    assert s.lease_take("pub:1", 300_000) == 12
    take = [x for x in drv.statements if x.startswith("UPDATE sm_outlet_lease")][0]
    assert "GENERATION = LAST_INSERT_ID(GENERATION + 1)" in take
    assert "WAITER = IF(WAITER = 'pub:1', NULL, WAITER)" in take
    assert "(WAITER IS NULL OR WAITER_EXPIRES_AT < UTC_TIMESTAMP(3) OR WAITER = 'pub:1')" in take


def test_a_take_whose_answer_was_lost_is_read_back():
    drv, _, s = _store()
    drv.user_handler = lambda sql, c: (ok(1, rows=[("pub:1", 13, None, None, None, None)])
                                       if sql.startswith("SELECT HOLDER") else ok(1))
    drv.fail_on = lambda sql: (driver_error(2013, "OperationalError")
                               if sql.startswith("UPDATE sm_outlet_lease SET HOLDER") else None)
    assert s.lease_take("pub:1", 1000) == 13           # the row names us: that WAS the take


def test_a_lost_take_that_did_not_land_is_not_ours():
    drv, _, s = _store()
    drv.user_handler = lambda sql, c: (ok(1, rows=[("other", 13, None, None, None, None)])
                                       if sql.startswith("SELECT HOLDER") else ok(1))
    drv.fail_on = lambda sql: (driver_error(2013, "OperationalError")
                               if sql.startswith("UPDATE sm_outlet_lease SET HOLDER") else None)
    assert s.lease_take("pub:1", 1000) is None


# ---------------------------------------------------------------------------
# Row mapping
# ---------------------------------------------------------------------------

def test_audit_rows_parse_json_from_text_and_bytes():
    from datetime import datetime

    now = datetime(2026, 10, 6, 12)
    rows = [(1, "RECONCILE", "a", "add", now, None, None, 3, None, None, None, 5, None, None, None,
             "OPEN", '{"x": 1}', b'{"keys": [["S", "42"]], "event_ids": [7]}', None)]
    drv, _, s = _store(lambda sql, c: ok(1, rows=rows))
    (r,) = s.read_audit(KIND_RECONCILE)
    assert r.outcome == {"x": 1} and r.keys == (("S", "42"),) and r.event_ids == {7}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ENV = {"SCANSIA_DB_HOST": "78.46.244.227", "SCANSIA_DB_USER": "scansia_mgr",
       "SCANSIA_DB_PASSWORD": "pw", "SCANSIA_DB_NAME": "racoon", "SCANSIA_DB_CA_PATH": "/x.pem"}


@pytest.mark.parametrize("missing", ["SCANSIA_DB_HOST", "SCANSIA_DB_USER", "SCANSIA_DB_PASSWORD",
                                     "SCANSIA_DB_NAME", "SCANSIA_DB_CA_PATH"])
def test_config_is_fail_closed(missing):
    env = dict(ENV)
    env.pop(missing)
    with pytest.raises(ConfigError):
        load_db_config(env)


def test_config_ca_from_base64_lands_in_a_private_file_and_repr_hides_the_password():
    pem = b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"
    env = dict(ENV)
    env.pop("SCANSIA_DB_CA_PATH")
    env["SCANSIA_DB_CA_B64"] = base64.b64encode(pem).decode()
    cfg = load_db_config(env)
    with open(cfg.ca_path, "rb") as fh:
        assert fh.read() == pem
    assert stat.S_IMODE(os.stat(cfg.ca_path).st_mode) == 0o600
    assert "pw" not in repr(cfg) and cfg.port == 3306
    env["SCANSIA_DB_CA_B64"] = "not base64!!"
    with pytest.raises(ConfigError):
        load_db_config(env)


def test_register_factory_reads_the_config_at_call_time():
    def missing():
        raise ConfigError("no db")

    factory = make_register_factory(missing)       # building it never fails
    with pytest.raises(ConfigError):
        factory(actor="x")


@pytest.mark.parametrize("values", [{"PRICE_OUTLET": Decimal("1.00"), "DISCOUNT": None},
                                    {"OUTLET_PRODUCT_ID": 5}, {"ONLINE": False}])
def test_a_pair_update_sets_only_granted_columns(values):
    """§3.6: the grant forbids STATE, CONSUMED_AT, CONSUME_KEY; the statements
    the store sends name only the column(s) asked + UPDATED_AT in their SET."""
    drv, _, s = _store()
    s.fenced_write(FENCE, [UpdatePairs((1,), values, require_state=SHELF)])
    upd = [x for x in drv.statements if x.startswith("UPDATE sc_returned_pair ")][0]
    set_clause = upd.split(" SET ", 1)[1].split(" WHERE ", 1)[0]
    named = {part.split(" = ")[0] for part in set_clause.split(", ")}
    assert named == set(values) | {"UPDATED_AT"}
