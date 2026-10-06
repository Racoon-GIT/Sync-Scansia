"""F7's writes beside ``SP_SCANSIA_CONSUME`` on ``racoon_staging`` (plan §3.6, §4 item 4):
the stamp and the pair update by PRIMARY KEY only (EXPLAIN); a CONSUME beside an open
stamp never waits; a CONSUME on a pair F7 is writing waits one F7 commit at most."""
from __future__ import annotations

import threading
import time

import pytest

from backend.persistence.records import SHELF, Fence, StampEvents, UpdatePairs
from tests.register_fixtures import MySQLBackend, Tap

PAUSE_S = 3.0          # F7's COMMIT held back, its locks held
WAN_MARGIN_S = 2.0     # round trips Mac → staging (a TLS connect took 0.3–1 s, 2026-10-06)


@pytest.fixture
def be():
    why = MySQLBackend.available()
    if why:
        pytest.skip(why)
    b = MySQLBackend()
    yield b
    b.reset()
    b.close()


def _consume_at_f7_commit(be, tap, sku, size):
    """CONSUME from the fixture's own session once F7 reaches COMMIT."""
    out = {}

    def run():
        assert tap.commit_entered.wait(30)
        out["t0"] = time.monotonic()
        out["event"] = be.consume(sku, size)
        out["t1"] = time.monotonic()

    th = threading.Thread(target=run, daemon=True)
    th.start()
    return th, out


def _f7_write(be, tap, ops):
    s = be.store(tap=tap)
    gen = s.lease_take("f7test-lock", 300_000)
    assert gen is not None
    fence = Fence("f7test-lock", gen, 300_000)
    tap.arm(PAUSE_S)
    s.fenced_write(fence, ops)
    done = time.monotonic()
    s.lease_release(fence)
    return s, done


def _explained(tap, table):
    stmts = [(q, p) for q, p in tap.statements if q.startswith(f"UPDATE {table} ")]
    assert stmts
    return [row for q, p in stmts for row in tap.explain(q, p)]


def test_a_consume_beside_an_open_stamp_never_waits_and_the_stamp_is_by_primary_key(be):
    p1, e1 = be.add_shelf_pair("F7T-LOCK", "42")                 # F7 stamps e1
    be.add_shelf_pair("F7T-LOCK", "42", reconciled=True)
    tap = Tap()
    th, out = _consume_at_f7_commit(be, tap, "F7T-LOCK", "42")
    s, done = _f7_write(be, tap, [StampEvents((e1,))])
    th.join(30)
    assert out["t1"] < done                                      # done while F7 held its locks
    assert out["t1"] - out["t0"] < PAUSE_S / 2
    assert be.event_type(out["event"]) == "CONSUME"
    plan = _explained(tap, "sc_returned_pair_event")
    assert {r["key"] for r in plan} == {"PRIMARY"}
    s.close()


def test_a_consume_on_a_pair_f7_is_writing_waits_one_f7_commit_at_most(be):
    p, _ = be.add_shelf_pair("F7T-LOCK", "43", reconciled=True)  # N 1: CONSUME must take p
    tap = Tap()
    th, out = _consume_at_f7_commit(be, tap, "F7T-LOCK", "43")
    s, done = _f7_write(be, tap, [UpdatePairs((p,), {"ONLINE": 1}, require_state=SHELF)])
    th.join(30)
    assert out["t1"] >= tap.commit_sent                          # it waited for F7's COMMIT
    assert out["t1"] - out["t0"] <= PAUSE_S + WAN_MARGIN_S       # and no longer
    assert be.event_type(out["event"]) == "CONSUME"
    plan = _explained(tap, "sc_returned_pair")
    assert {r["key"] for r in plan} == {"PRIMARY"}
    s.close()
