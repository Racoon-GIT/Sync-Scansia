"""A person's close of a RECONCILE row — plan §3.3 rule 2's terminal path (iv): under
the pass lease (a decrement-class holder, its recovery first), guarded on the
status the person read, ``HELD`` (``FAILED`` for a tail / normalise row), recorded
(who, when, why); the group settled; the key released only by a re-level (AB4) or
the N-0 release. End to end on the fake Shopify and the in-memory register."""
from __future__ import annotations

from typing import List

from backend.persistence.records import (
    APPLIED,
    CARRIED,
    FAILED,
    HELD,
    HOLD_RECOVERY,
    KIND_AUDIT,
    OPEN,
)
from backend.services import close_service as cs
from backend.services import delete_service as ds
from backend.services import flows
from backend.services import ledger as L
from backend.services.lease import PassLease

from tests.fake_shop import PROMO, FakeShop
from tests.register_fixtures import MemoryBackend, seed_reconcile

K = ("SKU1", "42")


class Env:
    def __init__(self) -> None:
        self.be = MemoryBackend()
        self.be.seed_baseline()
        self.shop = FakeShop()
        self.leases: List[PassLease] = []

    def register(self):
        return self.be.register(actor="ale")

    def close(self, row_id, expect=OPEN, why="answer lost; level checked by hand"):
        reg = self.register()
        lease = PassLease(reg, "close", sleep=lambda s: None)
        self.leases.append(lease)
        return cs.close_row(reg, self.shop, row_id, expect_status=expect, why=why, promo_location_id=PROMO,
                            lease=lease, sleep=lambda s: None)

    def row(self, row_id):
        return self.register().reconcile_rows(row_ids=[row_id])[0]


def add_with_activation(env: Env, product, event_id: int):
    """A +Δ group whose activation applied and whose add's answer was lost."""
    reg = env.register()
    item = product.variants[0].item
    act = seed_reconcile(reg, action=L.A_ACTIVATE, keys=(K,), event_ids=(event_id,), inventory_item_id=item,
                         location_id=PROMO, product_id=flows.product_number(product.gid), sent=True,
                         status=APPLIED, outcome={"moved": 0}, group_id=21)
    add = seed_reconcile(reg, action=L.A_ADJUST, keys=(K,), event_ids=(event_id,), purpose=L.P_ADD, share=1,
                         inventory_item_id=item, location_id=PROMO, product_id=flows.product_number(product.gid),
                         sent=True, group_id=21)
    return act, add


def test_a_persons_close_holds_the_add_records_who_and_why_and_carries_its_applied_activation():
    env = Env()
    out = env.shop.product("SKU1 - Outlet", sku="SKU1", sizes=("42",))
    _pid, e = env.be.add_shelf_pair(*K)
    act, add = add_with_activation(env, out, e)
    rep = env.close(add)
    assert rep.status == cs.ST_CLOSED and rep.closed_as == HELD and rep.settled == "carried"
    closed = env.row(add)
    assert closed.status == HELD and closed.closed_at is not None
    assert closed.outcome["person_close"] == {"by": "ale", "why": "answer lost; level checked by hand"}
    assert env.row(act).status == CARRIED                                   # AB1: beside a HELD sibling
    (audit,) = [r for r in env.register().audit_rows(KIND_AUDIT) if r.action == "person_close"]
    assert audit.actor == "ale" and audit.payload == {"row_id": add, "from": OPEN, "to": HELD,
                                                      "why": "answer lost; level checked by hand",
                                                      "keys": [list(K)]}
    holds = [h for h in env.register().held_keys() if h.key == K]
    assert [h.kind for h in holds] == [HOLD_RECOVERY] and holds[0].event_ids == frozenset({e})
    assert e not in {x.event_id for x in env.register().pending_deltas().events}   # never re-added, never stamped
    assert env.shop.count("adjust") == 0 and env.shop.count("activate") == 0


def test_a_close_against_a_status_changed_meanwhile_closes_nothing():
    env = Env()
    out = env.shop.product("SKU1 - Outlet", sku="SKU1", sizes=("42",))
    _pid, e = env.be.add_shelf_pair(*K)
    _act, add = add_with_activation(env, out, e)
    rep = env.close(add, expect=APPLIED)                                    # the person read a stale status
    assert rep.status == cs.ST_CHANGED and rep.reason == "status_now:OPEN" and env.row(add).status == OPEN
    assert [r for r in env.register().audit_rows(KIND_AUDIT) if r.action == "person_close"] == []


def test_the_holders_recovery_runs_first_and_may_close_the_row_itself():
    env = Env()
    out = env.shop.product("SKU1 - Outlet", sku="SKU1", sizes=("42",))
    never = seed_reconcile(env.register(), action=L.A_ADJUST, keys=(K,), purpose=L.P_ADD, share=1,
                           inventory_item_id=out.variants[0].item, location_id=PROMO,
                           product_id=flows.product_number(out.gid))
    rep = env.close(never)
    assert rep.status == cs.ST_CHANGED and rep.reason == "status_now:FAILED"   # closed "never sent" by recovery
    assert env.row(never).outcome.get("never_sent") is True


def test_a_persons_close_of_a_normalise_row_fails_it_and_drafts_an_unsafe_active_outlet():
    env = Env()
    out = env.shop.product("SKU1 - Outlet", sku="SKU1", policy="CONTINUE")
    norm = seed_reconcile(env.register(), action=L.A_NORMALISE, keys=(K,), product_id=flows.product_number(out.gid),
                          sent=True)
    rep = env.close(norm)
    assert rep.status == cs.ST_CLOSED and rep.closed_as == FAILED and env.row(norm).status == FAILED   # AB11
    assert env.shop.products[out.gid].status == "DRAFT"                                                # AB2


def test_a_persons_close_of_a_tail_row_fails_it_surfaced():
    env = Env()
    out = env.shop.product("SKU1 - Outlet", sku="SKU1", status="DRAFT", promo={"42": 1})
    tail = seed_reconcile(env.register(), action=L.A_TAIL, keys=(K,), product_id=flows.product_number(out.gid),
                          sent=True)
    rep = env.close(tail)
    assert rep.closed_as == FAILED and env.row(tail).status == FAILED
    assert any(i["kind"] == "tail_closed_by_person" for i in rep.surfaced)
    assert env.shop.products[out.gid].status == "DRAFT"                     # left as the tail left it


def test_a_reason_is_required_and_a_closed_row_cannot_be_closed_again():
    env = Env()
    assert env.close(1, why=" ").status == cs.ST_REFUSED
    assert env.close(1, expect=HELD).status == cs.ST_REFUSED
    assert env.close(999).status == cs.ST_NOT_FOUND


def test_an_orphan_unknown_create_whose_orphan_was_deleted_is_closed_by_a_person():
    """AB15: (iii) finds rows by product id — an "orphan unknown" CREATE has none —
    and once the orphan is gone the search finds nothing: (iv) is its close."""
    from backend.persistence.audit_sink import RegisterAuditSink

    env = Env()
    source = env.shop.product("Nike Air", sku="SKU1", outlet=False)
    orphan = env.shop.product("Nike Air - Outlet", sku="SKU1", status="DRAFT")
    create = seed_reconcile(env.register(), action=L.A_CREATE, keys=(K,), event_ids=(), sent=True,
                            extra={"source_gid": source.gid, "title": "Nike Air - Outlet", "deltas": {"42": 1}})
    reg = env.register()
    plan = ds.single_delete_preview(reg, env.shop, orphan.gid)
    out = ds.delete_single_apply(reg, env.shop, RegisterAuditSink(reg, actor="ale"), plan, human_gesture="CONFERMO",
                                 promo_location_id=PROMO, lease=PassLease(reg, "delete", sleep=lambda s: None),
                                 sleep=lambda s: None)
    assert out.status == ds.STATUS_DELETED and out.closed_rows == () and env.row(create).status == OPEN
    rep = env.close(create, why="orphan deleted by hand")
    assert rep.closed_as == HELD and env.row(create).status == HELD
    assert K in {h.key for h in env.register().held_keys()}


def test_the_releasing_relevel_stamps_the_held_rows_events():
    """AB4's tail: the key a person's close holds is released by a per-key re-level,
    which levels it to N and stamps the HELD row's events."""
    from tests.test_baseline import Env as BaselineEnv
    from tests.test_relevel import relevel

    env = BaselineEnv()
    out = env.shop.product("Nike - Outlet", promo={"42": 1})
    env.imported(size="42", price_outlet="50.00", price_high="100.00")
    env.run()
    _pid, e = env.be.add_shelf_pair("SKU1", "42", price_outlet="50.00", price_high="100.00")
    reg = env.register()
    add = seed_reconcile(reg, action=L.A_ADJUST, keys=(K,), event_ids=(e,), purpose=L.P_ADD, share=1,
                         inventory_item_id=out.variants[0].item, location_id=PROMO,
                         product_id=flows.product_number(out.gid), sent=True)
    lease = PassLease(reg, "close", sleep=lambda s: None)
    rep = cs.close_row(reg, env.shop, add, expect_status=OPEN, why="checked by hand", promo_location_id=PROMO,
                       lease=lease, sleep=lambda s: None)
    assert rep.closed_as == HELD and e in env.unstamped()
    _plan, rr = relevel(env, [K])
    assert [o.result for o in rr.outcomes] == ["released"]
    assert e not in env.unstamped() and env.shop.available(out.gid, "42") == 2
    assert K not in {h.key for h in env.register().held_keys()}
