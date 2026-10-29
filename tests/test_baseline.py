""""Inizializza" on MySQL — the baseline (plan §3.4, B-1 Level (D20), B-2 (D20 +
D25)), end to end: the real ``ops`` documents against the stateful fake Shopify
(``tests/fake_shop.py``), the register on the in-memory store. Every test that
applies ends with §3.4's tested invariant on every key: live Promo (real-size,
exact title, clamped at 0) + Σ pending Δ = N, or the key is held."""
from __future__ import annotations

from datetime import timedelta, timezone
from typing import Dict, List, Tuple

import pytest

from backend.persistence.audit_sink import RegisterAuditSink
from backend.persistence.memory_store import SimulatedCrash
from backend.persistence.records import (
    DONE,
    KIND_BASELINE,
    KIND_HOLD,
    KIND_INIT_BEFORE,
    KIND_PRICE,
    KIND_RECONCILE,
    KIND_REGISTER,
    OPEN,
    SCOPE_REGISTER,
    BaselineMissing,
    InsertAudit,
)
from backend.services import baseline_service as B
from backend.services import ledger as L
from backend.services import outlet_service, passes, resolvers, variants
from backend.services.lease import PassLease

from tests.fake_shop import PROMO, FakeShop
from tests.register_fixtures import MemoryBackend

MUTATIONS = {"duplicate", "adjust", "activate", "deactivate", "bulk", "status", "publish", "delete"}


class Env:
    def __init__(self) -> None:
        self.be = MemoryBackend()
        self.shop = FakeShop()

    def register(self):
        return self.be.register(actor="ale")

    # -- the register's other writers ------------------------------------------------
    def imported(self, sku="SKU1", size="42", *, j=None, **kw) -> Tuple[int, int]:
        """A pair the legacy import put on the shelf: its IMPORTED +1 (raw.J = ``j``)."""
        pid = self.be.add_pair(sku, size, **kw)
        eid = self.be.add_event(pid, +1, event_type="IMPORTED", actor="IMPORT",
                                payload={"raw": {"J": j}} if j is not None else {"raw": {}})
        return pid, eid

    def departed(self, sku="SKU1", size="42") -> Tuple[int, int]:
        """An imported pair since sold at full price: +1 then −1, off the shelf."""
        pid, e1 = self.imported(sku, size)
        e2 = self.be.leave_shelf(pid, "VENDUTO", event_type="CONSUME", actor="SP")
        return e1, e2

    # -- the operation ---------------------------------------------------------------
    def preview(self) -> B.BaselinePlan:
        return B.baseline_preview(self.register(), self.shop, promo_location_id=PROMO)

    def dlq(self, plan, *, minutes_before: float = 1, count: int = 0) -> B.DlqCheck:
        at = plan.previewed_at.replace(tzinfo=timezone.utc) - timedelta(minutes=minutes_before)
        return B.DlqCheck(at, count, at.isoformat())

    def apply(self, plan, *, dlq="default", sink=None, **kw) -> B.BaselineReport:
        reg = self.register()
        lease = kw.pop("lease", None) or PassLease(reg, "baseline", sleep=lambda s: None)
        return B.baseline_apply(
            reg, self.shop, plan, sink or RegisterAuditSink(reg, actor="ale"), promo_location_id=PROMO,
            dlq=self.dlq(plan) if dlq == "default" else dlq, lease=lease, sleep=lambda s: None, **kw)

    def run(self) -> B.BaselineReport:
        return self.apply(self.preview())

    # -- reading back ----------------------------------------------------------------------
    def unstamped(self) -> List[int]:
        return [e.event_id for e in self.be.store().read_unstamped_events()]

    def baseline_row(self):
        return self.register().baseline()

    def held(self) -> Dict[Tuple[str, str], str]:
        return {(h["key"][0], h["key"][1]): h["reason"] for h in (self.baseline_row().payload or {})["held"]}

    def mutations(self) -> List[str]:
        return [c[0] for c in self.shop.calls if c[0] in MUTATIONS]

    def invariant(self) -> Dict[Tuple[str, str], int]:
        """§3.4's tested invariant on every key: the keys where clamp(P) + Σ
        pending Δ ≠ N and that are not held (must be empty)."""
        reg = self.register()
        pend = reg.pending_deltas()
        held = {h.key for h in pend.holds}
        n: Dict[Tuple[str, str], int] = {}
        for p in reg.shelf():
            n[p.key] = n.get(p.key, 0) + 1
        sigma: Dict[Tuple[str, str], int] = {}
        for e in pend.events:
            sigma[e.key] = sigma.get(e.key, 0) + e.delta
        keys = set(n) | set(sigma)
        promo: Dict[Tuple[str, str], int] = {}
        for prod in self.shop.products.values():
            if not (prod.outlet_member or "outlet" in prod.title.casefold()):
                continue
            for v in prod.variants:
                if "personalizzazione" in v.title.casefold():
                    continue
                lvl = self.shop.levels.get((v.item, PROMO))
                k = ((v.sku or "").strip(), v.title)
                keys.add(k)
                promo[k] = promo.get(k, 0) + max(lvl.available if lvl else 0, 0)
        return {k: promo.get(k, 0) + sigma.get(k, 0) - n.get(k, 0)
                for k in keys if k not in held and promo.get(k, 0) + sigma.get(k, 0) != n.get(k, 0)}


def by_key(plan: B.BaselinePlan) -> Dict[Tuple[str, str], B.KeyPlan]:
    return {kp.key: kp for kp in plan.keys}


def outcome(rep: B.BaselineReport, sku: str, size: str) -> B.KeyOutcome:
    return next(o for o in rep.outcomes if (o.sku, o.size) == (sku, size))


# ---------------------------------------------------------------------------
# Level (a): one ACTIVE outlet with the exact size
# ---------------------------------------------------------------------------

def test_a_raises_an_active_outlet_to_n_and_stamps_every_event_of_the_key():
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 0, "43": 0})
    _, e1 = env.imported(size="42")
    _, e2 = env.imported(size="42")
    plan = env.preview()
    kp = by_key(plan)[("SKU1", "42")]
    assert (kp.action, kp.n, kp.target, kp.delta, kp.activate) == (B.ACT_LEVEL, 2, 2, 2, False)
    rep = env.apply(plan)
    assert rep.status == B.ST_APPLIED and outcome(rep, "SKU1", "42").result == "levelled"
    assert env.shop.available(out.gid, "42") == 2
    (adj,) = [c for c in env.shop.calls if c[0] == "adjust"]
    assert adj[1]["input"]["changes"][0]["delta"] == 2 and adj[1]["input"]["changes"][0]["changeFromQuantity"] == 0
    assert env.unstamped() == []
    base = env.baseline_row()
    assert base.payload["scope"] == SCOPE_REGISTER and base.payload["stamped"] == [e1, e2]
    assert base.payload["rule"] == "level" and base.payload["held"] == []
    (row,) = [r for r in env.register().reconcile_rows() if r.action == L.A_ADJUST]
    assert row.status == DONE and L.purpose(row) == L.P_RELEVEL and row.event_ids == frozenset()
    assert env.invariant() == {}


def test_a_lowers_an_over_exposed_key_to_n_from_any_level():
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 3, "43": -1}, sizes=("42", "43"))
    env.imported(size="42")
    env.imported(size="43")
    env.run()
    assert env.shop.available(out.gid, "42") == 1          # 3 → N 1
    assert env.shop.available(out.gid, "43") == 1          # −1 → N 1: a re-level sets the level (V8)
    assert env.invariant() == {}


def test_a_activates_an_absent_or_inactive_level_with_explicit_zeros_before_raising():
    env = Env()
    out = env.shop.product("Nike - Outlet", sizes=("42", "43"), absent=("42",), inactive=("43",),
                           promo={"43": 2})
    env.imported(size="42")
    env.imported(size="43")
    plan = env.preview()
    assert by_key(plan)[("SKU1", "42")].activate and by_key(plan)[("SKU1", "43")].activate
    env.apply(plan)
    acts = [c for c in env.shop.calls if c[0] == "activate"]
    assert len(acts) == 2 and all(a[1]["available"] == 0 and "onHand" not in a[1] for a in acts)
    names = env.mutations()
    assert names.index("activate") < names.index("adjust")
    assert env.shop.available(out.gid, "42") == 1 and env.shop.promo(out.gid, "42").active
    assert env.shop.available(out.gid, "43") == 1          # the inactive level's 2 dropped by the explicit 0 (fewer)
    assert env.invariant() == {}


def test_a_level_that_is_already_n_writes_nothing():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 1})
    env.imported(size="42")
    rep = env.run()
    assert env.mutations() == [] and outcome(rep, "SKU1", "42").result == "levelled"
    assert env.unstamped() == [] and env.invariant() == {}


def test_a_upward_write_meeting_committed_holds_the_key_and_writes_nothing():
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 0}, committed={"42": 1})
    _, e = env.imported(size="42")
    plan = env.preview()
    assert by_key(plan)[("SKU1", "42")].reason == B.R_COMMITTED
    env.apply(plan)
    assert env.mutations() == [] and env.shop.available(out.gid, "42") == 0
    assert env.held() == {("SKU1", "42"): B.R_COMMITTED}
    assert env.baseline_row().payload["held"][0]["event_ids"] == [e] and env.unstamped() == [e]
    assert env.invariant() == {}


# ---------------------------------------------------------------------------
# Level (b) and (c), and keys with no shelf pair
# ---------------------------------------------------------------------------

def test_b_a_draft_parked_key_is_left_at_n_not_held():
    env = Env()
    out = env.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 1})
    env.be.add_shelf_pair("SKU1", "42", reconciled=True)          # N 1, its unit parked, nothing pending
    plan = env.preview()
    kp = by_key(plan)[("SKU1", "42")]
    assert (kp.action, kp.delta) == (B.ACT_PARKED, 0) and plan.counts()["parked"] == 1   # listed: a re-level each
    rep = env.apply(plan)
    assert outcome(rep, "SKU1", "42").result == "parked"
    assert env.mutations() == [] and env.shop.available(out.gid, "42") == 1 and env.held() == {}
    assert env.invariant() == {}


def test_a_key_with_no_shelf_pair_and_a_committed_unit_keeps_its_events_held():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0}, committed={"42": 1})
    gone = env.departed(size="42")
    plan = env.preview()
    assert by_key(plan)[("SKU1", "42")].reason == B.R_UNIT_HELD      # rule 4, D33: the −1 waits
    env.apply(plan)
    assert set(env.unstamped()) == set(gone)


def test_b_zeroes_a_draft_outlet_leaves_the_shelf_events_pending_and_the_revive_restores_n():
    env = Env()
    out = env.shop.product("Nike - Outlet", status="DRAFT", promo={"42": 2})
    _, keep = env.imported(size="42", price_outlet="50.00", price_high="100.00")
    gone = env.departed(size="42")
    rep = env.run()
    assert outcome(rep, "SKU1", "42").result == "zeroed"
    assert env.shop.available(out.gid, "42") == 0
    assert env.unstamped() == [keep]                        # the departed pair's events stamped
    assert set(env.baseline_row().payload["stamped"]) == set(gone)
    assert env.invariant() == {}
    reg = env.register()                                    # the normal publish revives with exactly N
    plan = outlet_service.publish_preview(reg, env.shop, promo_location_id=PROMO)
    outlet_service.publish_apply(reg, env.shop, plan, promo_location_id=PROMO,
                                 lease=PassLease(reg, "publish", sleep=lambda s: None))
    assert env.shop.available(out.gid, "42") == 1 and env.shop.products[out.gid].status == "ACTIVE"
    assert env.invariant() == {}


def test_c_no_outlet_writes_nothing_and_stamps_only_the_departed_pairs():
    env = Env()
    env.shop.product("Nike Air", outlet=False, sizes=("43",))      # the source, which lacks 42
    _, keep = env.imported(size="42")
    gone = env.departed(size="42")
    plan = env.preview()
    assert by_key(plan)[("SKU1", "42")].action == B.ACT_NONE
    assert [(c.sku, c.size, c.note) for c in plan.catalogue] == [("SKU1", "42", "size_absent_on_source")]
    env.apply(plan)
    assert env.mutations() == [] and env.unstamped() == [keep]
    assert set(env.baseline_row().payload["stamped"]) == set(gone)
    assert env.invariant() == {}


def test_c_a_sku_not_in_the_catalogue_or_with_two_sources_is_listed():
    env = Env()
    env.imported(sku="NOPE", size="40")
    env.shop.product("Two A", sku="TWO", outlet=False)
    env.shop.product("Two B", sku="TWO", outlet=False)
    env.imported(sku="TWO", size="42")
    plan = env.preview()
    assert [(c.sku, c.note) for c in plan.catalogue] == [("NOPE", "not_in_catalogue"), ("TWO", "multi_source")]


def test_a_key_with_no_shelf_pair_and_no_variant_titled_size_is_stamped_only():
    env = Env()
    env.shop.product("Nike - Outlet", sizes=("43",), promo={"43": 0})
    gone = env.departed(size="42")
    rep = env.run()
    assert outcome(rep, "SKU1", "42").result == "stamped" and env.unstamped() == []
    assert set(env.baseline_row().payload["stamped"]) == set(gone)


def test_every_level_branch_stamps_the_events_of_pairs_no_longer_on_the_shelf():
    env = Env()
    env.shop.product("A - Outlet", sku="A", promo={"42": 0})
    env.shop.product("B - Outlet", sku="B", status="DRAFT", promo={"42": 0})
    gone = []
    for sku in ("A", "B", "C"):
        env.imported(sku=sku, size="42")
        gone += env.departed(sku=sku, size="42")
    env.run()
    stamped = set(env.baseline_row().payload["stamped"])
    assert set(gone) <= stamped
    assert env.invariant() == {}


def test_a_stale_unit_is_lowered_to_zero_on_any_outlet_never_held():
    env = Env()
    active = env.shop.product("Nike - Outlet", promo={"42": 1, "44": 2}, sizes=("42", "44"))
    foreign = env.shop.product("Other - Outlet", sku="ZZ", status="DRAFT", promo={"40": 1}, sizes=("40",))
    env.imported(size="42")
    plan = env.preview()
    keys = by_key(plan)
    assert keys[("SKU1", "44")].action == B.ACT_LOWER and keys[("ZZ", "40")].action == B.ACT_LOWER
    rep = env.apply(plan)
    assert env.shop.available(active.gid, "44") == 0 and env.shop.available(foreign.gid, "40") == 0
    assert env.shop.available(active.gid, "42") == 1
    assert outcome(rep, "ZZ", "40").result == "lowered" and env.held() == {}
    assert env.invariant() == {}


# ---------------------------------------------------------------------------
# Held keys (§3.4): listed, excluded, lowered never raised
# ---------------------------------------------------------------------------

def test_a_spelling_twin_holds_the_shelf_key_and_its_twin_unit_is_lowered():
    env = Env()
    out = env.shop.product("AR - Outlet", sku="AR-K-33PC", status="DRAFT", sizes=("36 L", "42 L"),
                           promo={"36 L": 0, "42 L": 1})
    _, e = env.imported(sku="AR-K-33PC", size="42L")
    plan = env.preview()
    kp = by_key(plan)[("AR-K-33PC", "42L")]
    assert (kp.action, kp.reason, kp.twins) == (B.ACT_HOLD, B.R_SIZE_ABSENT, ("42 L",))
    assert by_key(plan)[("AR-K-33PC", "42 L")].action == B.ACT_LOWER
    env.apply(plan)
    assert env.shop.available(out.gid, "42 L") == 0          # never matched by normalisation (§3.2)
    assert env.held() == {("AR-K-33PC", "42L"): B.R_SIZE_ABSENT} and env.unstamped() == [e]
    assert env.invariant() == {}


def test_a_multi_outlet_key_is_held_and_lowered_by_its_excess_at_confirm_active_first():
    env = Env()
    a = env.shop.product("Nike - Outlet A", promo={"42": 1})
    d = env.shop.product("Nike - Outlet B", status="DRAFT", promo={"42": 1})
    env.imported(size="42")
    plan = env.preview()
    kp = by_key(plan)[("SKU1", "42")]
    assert (kp.reason, kp.delta) == (B.R_MULTI_OUTLET, -1)
    env.apply(plan)
    assert env.shop.available(a.gid, "42") == 0 and env.shop.available(d.gid, "42") == 1
    assert env.held() == {("SKU1", "42"): B.R_MULTI_OUTLET}
    assert env.invariant() == {}


def test_a_held_key_short_of_n_is_never_raised():
    env = Env()
    env.shop.product("Nike - Outlet A", promo={"42": 0})
    env.shop.product("Nike - Outlet B", status="DRAFT", promo={"42": 0})
    env.imported(size="42")
    env.run()
    assert env.mutations() == []


def test_the_blank_sku_pair_is_held_and_nothing_resolves_a_blank_sku():
    env = Env()
    _, e = env.imported(sku="", size="Solo Personalizzazione")
    plan = env.preview()
    assert by_key(plan)[("", "Solo Personalizzazione")].reason == B.R_BLANK_SKU
    assert not any(c[0] == "resolve" and c[1]["q"] == "sku:" for c in env.shop.calls)
    env.apply(plan)
    assert env.held() == {("", "Solo Personalizzazione"): B.R_BLANK_SKU} and env.unstamped() == [e]


def test_an_unclean_resolution_holds_the_key(monkeypatch):
    env = Env()
    env.imported(size="42")
    monkeypatch.setattr(resolvers, "outlet_resolver",
                        lambda t, sku: {"matches": [], "warning": "TRUNCATED", "truncated": True})
    plan = env.preview()
    assert by_key(plan)[("SKU1", "42")].reason == B.R_UNRESOLVED


def test_a_key_already_held_stays_held_by_the_baseline():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0})
    env.imported(size="42")
    env.be.seed_audit(InsertAudit(kind=KIND_HOLD, actor="t", action="hold",
                                  payload={"keys": [["SKU1", "42"]], "reason": "outlet deleted", "event_ids": []}))
    plan = env.preview()
    assert by_key(plan)[("SKU1", "42")].reason == f"{B.R_HELD_BEFORE}:hold"
    env.apply(plan)
    assert env.mutations() == []


def test_an_unexpected_outlet_status_holds_the_key():
    env = Env()
    env.shop.product("Nike - Outlet", status="ARCHIVED", promo={"42": 1})
    env.imported(size="42")
    assert by_key(env.preview())[("SKU1", "42")].reason == f"{B.R_STATUS}:ARCHIVED"


# ---------------------------------------------------------------------------
# The confirm (§3.4): a key that moved is held, nothing written
# ---------------------------------------------------------------------------

def test_a_new_event_between_preview_and_confirm_holds_the_key_its_plus_one_never_added():
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 0})
    _, e1 = env.imported(size="42")
    plan = env.preview()
    _, late = env.be.add_shelf_pair("SKU1", "42")            # an arrival after the snapshot
    rep = env.apply(plan)
    assert outcome(rep, "SKU1", "42").reason == B.R_MOVED
    assert env.mutations() == [] and env.shop.available(out.gid, "42") == 0
    assert env.baseline_row().payload["held"][0]["event_ids"] == [e1]
    pend = env.register().pending_deltas()
    assert [e.event_id for e in pend.events] == [late] and ("SKU1", "42") in pend.held_keys
    reg = env.register()                                    # a held key's +Δ is never added by a publish
    plan2 = outlet_service.publish_preview(reg, env.shop, promo_location_id=PROMO)
    assert any(a.startswith("held_positive:SKU1:42") for a in plan2.anomalies)
    assert env.invariant() == {}


def test_a_live_level_that_moved_holds_the_key_and_the_confirm_lowers_its_excess():
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 1})
    env.imported(size="42")
    plan = env.preview()
    env.shop.levels[(env.shop.variant(out.gid, "42").item, PROMO)].available = 3   # a hand edit
    env.shop.levels[(env.shop.variant(out.gid, "42").item, PROMO)].on_hand = 3
    rep = env.apply(plan)
    assert outcome(rep, "SKU1", "42").reason == B.R_MOVED
    assert env.shop.available(out.gid, "42") == 1                 # lowered by its excess, never set
    assert env.invariant() == {}


def test_an_outlet_whose_status_changed_since_the_preview_holds_the_key():
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 0})
    env.imported(size="42")
    plan = env.preview()
    env.shop.products[out.gid].status = "DRAFT"
    rep = env.apply(plan)
    assert outcome(rep, "SKU1", "42").reason == B.R_MOVED and env.mutations() == []


def test_a_level_write_with_an_unknown_outcome_holds_the_key_and_stays_in_flight():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0})
    _, e = env.imported(size="42")
    env.shop.fault("adjust", "timeout_before")
    rep = env.apply(env.preview())
    assert outcome(rep, "SKU1", "42").reason.startswith(B.R_WRITE)
    assert env.unstamped() == [e]
    (row,) = env.register().reconcile_rows(statuses=(OPEN,))
    assert L.purpose(row) == L.P_RELEVEL and row.sent_at is not None
    kinds = {h.kind for h in env.register().held_keys()}
    assert kinds == {"baseline", "in_flight"}


def test_a_refused_level_write_holds_the_key():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0})
    env.imported(size="42")
    env.shop.fault("adjust", "user_error:INVALID_QUANTITY_DOCUMENT")
    rep = env.apply(env.preview())
    assert outcome(rep, "SKU1", "42").reason == f"{B.R_WRITE}:FAILED"
    assert env.invariant() == {}


# ---------------------------------------------------------------------------
# Rule 7: normalisation before any raise; customisation-only variants
# ---------------------------------------------------------------------------

def test_continue_and_untracked_variants_are_normalised_before_any_raise():
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 0, "Solo Personalizzazione": 2, "Sola personalizzazione": -1},
                           extra_variants=(("Solo Personalizzazione", "SKU1"), ("Sola personalizzazione", None)))
    env.shop.variant(out.gid, "43").policy = "CONTINUE"
    env.shop.variant(out.gid, "Solo Personalizzazione").tracked = False
    env.imported(size="42")
    plan = env.preview()
    (fix,) = plan.products
    assert set(fix.unsafe_variant_ids) == {env.shop.variant(out.gid, "43").id,
                                           env.shop.variant(out.gid, "Solo Personalizzazione").id}
    assert [c[2] for c in fix.custom_lowerings] == [2]
    env.apply(plan)
    calls = [c for c in env.shop.calls if c[0] in MUTATIONS]
    raise_at = next(i for i, c in enumerate(calls) if c[0] == "adjust" and c[1]["input"]["changes"][0]["delta"] > 0)
    assert [c[0] for c in calls].index("bulk") < raise_at          # DENY + tracked before the unit is exposed
    assert all(v.policy == "DENY" and v.tracked for v in env.shop.products[out.gid].variants)
    assert env.shop.available(out.gid, "Solo Personalizzazione") == 0          # a positive lowered
    assert env.shop.available(out.gid, "Sola personalizzazione") == -1         # a negative left
    assert env.shop.available(out.gid, "42") == 1
    assert env.invariant() == {}


def test_a_refused_normalisation_drafts_the_unsafe_outlet_and_holds_its_keys():
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 0}, policy="CONTINUE")
    env.imported(size="42")
    env.shop.fault("bulk", "user_error:INVALID")
    rep = env.apply(env.preview())
    assert outcome(rep, "SKU1", "42").reason == B.R_NORMALISE
    assert env.shop.products[out.gid].status == "DRAFT"          # rule 7's forbidden case (AB2)
    assert env.shop.available(out.gid, "42") == 0


# ---------------------------------------------------------------------------
# Audit: INIT_BEFORE first, BASELINE last; a crash re-runs from a fresh preview
# ---------------------------------------------------------------------------

def test_init_before_is_written_before_any_shopify_write():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0})
    env.imported(size="42", online=True)
    seen: List[int] = []

    class Spy(RegisterAuditSink):
        def write_init_before(self, snapshot):
            seen.append(len(env.mutations()))
            super().write_init_before(snapshot)

    reg = env.register()
    env.apply(env.preview(), sink=Spy(reg, actor="ale"))
    assert seen == [0]
    (row,) = env.register().audit_rows(KIND_INIT_BEFORE)
    assert row.payload["online"] == [[1, True]] and row.payload["keys"][0]["action"] == B.ACT_LEVEL


def test_an_init_before_failure_aborts_with_nothing_written():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0})
    env.imported(size="42")

    class Broken(RegisterAuditSink):
        def write_init_before(self, snapshot):
            raise RuntimeError("disk full")

    reg = env.register()
    rep = env.apply(env.preview(), sink=Broken(reg, actor="ale"))
    assert rep.status == B.ST_ABORTED and env.mutations() == [] and env.baseline_row() is None


def test_a_crash_before_the_baseline_row_leaves_no_baseline_and_a_fresh_preview_completes():
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 0})
    _, e = env.imported(size="42")

    def crash(i, op):
        if isinstance(op, InsertAudit) and op.kind == KIND_BASELINE:
            raise SimulatedCrash("killed before the BASELINE commit")

    env.be.db.crash_hook = crash
    with pytest.raises(SimulatedCrash):
        env.run()
    env.be.db.crash_hook = None
    assert env.baseline_row() is None and env.unstamped() == [e]
    with pytest.raises(BaselineMissing):
        env.register().pending_deltas()                        # the reader keeps refusing
    assert env.shop.available(out.gid, "42") == 1
    adjusts = env.shop.count("adjust")
    rep = env.run()                                             # re-run from a fresh preview
    assert rep.status == B.ST_APPLIED and env.shop.count("adjust") == adjusts   # nothing written twice
    assert env.shop.available(out.gid, "42") == 1 and env.unstamped() == []
    assert env.invariant() == {}


def test_the_baseline_runs_once_then_the_preview_reports_it_done():
    env = Env()
    env.run()
    plan = env.preview()
    assert plan.already_done and env.apply(plan).status == B.ST_ALREADY_DONE


def test_recovery_runs_first_at_the_take_and_a_key_in_flight_at_the_preview_stays_held():
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 0})
    env.imported(size="42")
    env.shop.fault("adjust", "lose_after")                    # landed, its answer lost

    def crash(i, op):
        if isinstance(op, InsertAudit) and op.kind == KIND_BASELINE:
            raise SimulatedCrash("killed before the BASELINE commit")

    env.be.db.crash_hook = crash
    with pytest.raises(SimulatedCrash):
        env.run()
    env.be.db.crash_hook = None
    (row,) = env.register().reconcile_rows(statuses=(OPEN,))   # sent, outcome unknown
    plan = env.preview()
    assert by_key(plan)[("SKU1", "42")].reason == f"{B.R_HELD_BEFORE}:in_flight"
    closed_before_init: List[str] = []

    class Spy(RegisterAuditSink):
        def write_init_before(self, snapshot):
            closed_before_init.append(env.register().reconcile_rows(row_ids=[row.row_id])[0].status)
            super().write_init_before(snapshot)

    rep = env.apply(plan, sink=Spy(env.register(), actor="ale"))
    assert rep.status == B.ST_APPLIED and closed_before_init == [DONE]   # its stored request re-sent: cached
    assert env.shop.available(out.gid, "42") == 1                        # +1 once, never twice
    assert env.held() == {("SKU1", "42"): f"{B.R_HELD_BEFORE}:in_flight"}
    assert env.invariant() == {}


def test_the_baseline_row_is_the_last_write_of_the_run():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0})
    env.imported(size="42", j={"t": "n", "v": 50})
    env.imported(sku="C", size="40", online=True)
    env.run()
    base = env.baseline_row()
    reg = env.register()
    ids = [r.row_id for kind in (KIND_INIT_BEFORE, KIND_RECONCILE, KIND_REGISTER, KIND_PRICE)
           for r in reg.audit_rows(kind)]
    assert ids and max(ids) < base.row_id


def test_the_decrement_is_refused_until_the_baseline_row_exists():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 1})
    env.imported(size="42")
    reg = env.register()
    with pytest.raises(BaselineMissing):
        passes.decrement_pass(reg, env.shop, PROMO, lease=PassLease(reg, "decrement", sleep=lambda s: None),
                              sleep=lambda s: None, wait_bound_s=0)
    env.run()
    env.be.consume("SKU1", "42")
    reg = env.register()
    rep = passes.decrement_pass(reg, env.shop, PROMO, lease=PassLease(reg, "decrement", sleep=lambda s: None),
                                sleep=lambda s: None, wait_bound_s=0)
    assert rep.applied == {"SKU1|42": "stamped"}


def test_the_baseline_keeps_the_lease_throughout_and_waits_for_a_held_one():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0, "43": 0})
    env.imported(size="42")
    env.imported(size="43")
    other = PassLease(env.register(), "publish", sleep=lambda s: None)
    other.take()
    plan = env.preview()
    assert env.apply(plan, take_bound_s=0).status == B.ST_LEASE_UNAVAILABLE
    other.release()
    takes = []
    real = env.be.db.connect

    def spy_connect():
        store = real()
        orig = store.lease_take

        def take(holder, ttl_ms):
            takes.append(holder)
            return orig(holder, ttl_ms)

        store.lease_take = take
        return store

    env.be.db.connect = spy_connect
    env.apply(plan)
    assert len(takes) == 1                                      # never released mid-run


# ---------------------------------------------------------------------------
# §3.4's precondition (the /dlqs GET), kept in the BASELINE row
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("count,minutes,why", [
    (1, 1, "dlq_unresolved_entries"),
    (0, 16, "dlq_check_too_old"),
    (0, -3, "dlq_check_in_the_future"),
])
def test_the_precondition_refuses_and_writes_nothing(count, minutes, why):
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0})
    env.imported(size="42")
    plan = env.preview()
    rep = env.apply(plan, dlq=env.dlq(plan, minutes_before=minutes, count=count))
    assert (rep.status, rep.reason) == (B.ST_PRECONDITION_REFUSED, why)
    assert env.mutations() == [] and env.register().audit_rows(KIND_INIT_BEFORE) == []


def test_the_precondition_missing_is_refused_and_kept_when_given():
    env = Env()
    plan = env.preview()
    assert env.apply(plan, dlq=None).reason == "dlq_check_missing"
    env.apply(plan)
    dlq = env.baseline_row().payload["dlq"]
    assert dlq["count"] == 0 and dlq["checked_at"].startswith(plan.previewed_at.strftime("%Y-%m-%d"))


def test_parse_dlq_reads_an_offset_or_europe_rome():
    utc, _ = B.parse_dlq("2026-10-08T07:40:00Z", 0)
    rome, _ = B.parse_dlq("2026-10-08T09:40", 0)
    assert utc.checked_at == rome.checked_at                  # 09:40 CEST = 07:40 UTC
    assert B.parse_dlq("ieri", 0) == (None, "dlq_check_unparseable")
    assert B.parse_dlq("", 0) == (None, "dlq_check_missing")


# ---------------------------------------------------------------------------
# B-2: one price per SKU seeded from J; conflicts listed, never picked
# ---------------------------------------------------------------------------

def test_j_seeds_one_price_per_sku_and_lists_what_it_cannot_pick():
    env = Env()
    for sku in ("S1", "S2", "S3", "S4", "S5"):
        env.shop.product(f"{sku} - Outlet", sku=sku, promo={"42": 0})
    env.imported(sku="S1", j={"t": "n", "v": 149, "w": "149"})
    env.imported(sku="S1", j={"t": "s", "v": "€ 149,00"})
    env.be.add_shelf_pair("S1", "42")                          # a post-cutover return: no J, priced with its SKU
    env.imported(sku="S2", j={"t": "n", "v": 149})
    env.imported(sku="S2", j={"t": "n", "v": 159})
    env.imported(sku="S3", j={"t": "s", "v": "149 euro"})
    env.imported(sku="S4", j={"t": "n", "v": 99}, price_outlet="89.00")
    env.imported(sku="S5", j={"t": "n", "v": 0})
    plan = env.preview()
    seeds = {s.sku: (s.status, s.price) for s in plan.prices}
    assert seeds == {"S1": (B.SEED, "149.00"), "S2": (B.SEED_CONFLICT, None), "S3": (B.SEED_UNPARSEABLE, None),
                     "S4": (B.SEED_PRICED, None), "S5": (B.SEED_INVALID, None)}
    rep = env.apply(plan)
    assert rep.prices_seeded == ("S1",)
    reg = env.register()
    assert {str(p.price_outlet) for p in reg.shelf("S1")} == {"149.00"}
    assert {p.price_outlet for p in reg.shelf("S2")} == {None}
    (price_row,) = reg.audit_rows(KIND_PRICE)
    assert price_row.payload["sku"] == "S1" and len(price_row.payload["pairs"]) == 3


@pytest.mark.parametrize("cell,expected", [
    ({"t": "n", "v": 149}, ("149.00", False)),
    ({"t": "n", "v": 89.905}, ("89.91", False)),
    ({"t": "s", "v": "89,9"}, ("89.90", False)),
    ({"t": "s", "v": " € 1 234 "}, ("1234.00", False)),
    ({"t": "s", "v": "1.234,50"}, (None, True)),
    ({"t": "e", "v": "#N/A"}, (None, True)),
    ({"t": "s", "v": "  "}, (None, False)),
    (None, (None, False)),
])
def test_the_j_cell_parses_by_the_registers_own_price_rule(cell, expected):
    value, flag = B.parse_sheet_price(cell)
    assert (None if value is None else f"{value:.2f}", flag) == expected


# ---------------------------------------------------------------------------
# ONLINE from the result; Part B listed, never written
# ---------------------------------------------------------------------------

def test_online_is_set_from_the_result_one_register_row_per_write():
    env = Env()
    env.shop.product("A - Outlet", sku="A", promo={"42": 0})
    env.shop.product("M - Outlet 1", sku="M", promo={"42": 0})
    env.shop.product("M - Outlet 2", sku="M", promo={"42": 0})
    a, _ = env.imported(sku="A", size="42")
    m, _ = env.imported(sku="M", size="42", online=True)       # held (multi-outlet) → 0
    c, _ = env.imported(sku="C", size="42", online=True)       # no outlet → 0
    env.run()
    reg = env.register()
    assert {p.pair_id: p.online for p in reg.shelf()} == {a: True, m: False, c: False}
    rows = [r for r in reg.audit_rows(KIND_REGISTER) if r.payload["column"] == "ONLINE"]
    assert len(rows) == 2


def test_a_sku_resolving_to_one_outlet_gets_its_id_on_the_shelf_pairs_that_lack_it():
    env = Env()
    a = env.shop.product("A - Outlet", sku="A", promo={"42": 0})
    env.shop.product("M - Outlet 1", sku="M", promo={"42": 0})
    env.shop.product("M - Outlet 2", sku="M", promo={"42": 0})
    pa, _ = env.imported(sku="A", size="42")
    pm, _ = env.imported(sku="M", size="42")
    env.run()
    reg = env.register()
    ids = {p.pair_id: p.outlet_product_id for p in reg.shelf()}
    assert ids == {pa: int(a.gid.rsplit("/", 1)[1]), pm: None}       # never on a multi-outlet SKU
    (row,) = [r for r in reg.audit_rows(KIND_REGISTER) if r.payload["column"] == "OUTLET_PRODUCT_ID"]
    assert row.payload["pairs"] == [{"pair_id": pa, "old": None, "new": int(a.gid.rsplit("/", 1)[1])}]


def test_part_b_lists_the_online_pairs_against_shopify_and_writes_nothing():
    env = Env()
    env.shop.product("A - Outlet", sku="A", promo={"42": 1})
    env.shop.product("B - Outlet", sku="B", status="DRAFT", promo={"42": 1})
    env.shop.product("D - Outlet", sku="D", promo={"42": 0})
    for sku in ("A", "B", "C", "D"):
        env.imported(sku=sku, size="42", online=True)
    plan = env.preview()
    assert {r.sku: r.bucket for r in plan.part_b} == {"A": B.B_KEPT, "B": B.B_DRAFT, "C": B.B_MISSING,
                                                      "D": B.B_SOLD_OUT}
    assert env.mutations() == []


# ---------------------------------------------------------------------------
# Never stamped: the keyless CONSUME_MISS; the invariant across a mixed register
# ---------------------------------------------------------------------------

def test_a_keyless_consume_miss_is_never_stamped():
    env = Env()
    miss = env.be.consume("SKU9", "40")                         # no pair: CONSUME_MISS, Δ 0
    plan = env.preview()
    assert plan.keyless_event_ids == (miss,)
    env.apply(plan)
    assert env.unstamped() == [miss] and env.baseline_row().payload["keyless"] == [miss]


def test_the_invariant_holds_on_every_key_of_a_mixed_register():
    env = Env()
    env.shop.product("A - Outlet", sku="A", sizes=("40", "41", "42"), promo={"40": 0, "41": 5, "42": 1})
    env.shop.product("B - Outlet", sku="B", status="DRAFT", sizes=("40", "41"), promo={"40": 1, "41": 1})
    env.shop.product("M - Outlet 1", sku="M", promo={"42": 2})
    env.shop.product("M - Outlet 2", sku="M", status="DRAFT", promo={"42": 1})
    env.shop.product("Z - Outlet", sku="Z", promo={"38": 1}, sizes=("38",))
    for sku, size in [("A", "40"), ("A", "40"), ("A", "41"), ("B", "40"), ("M", "42"), ("C", "39")]:
        env.imported(sku=sku, size=size)
    env.departed(sku="A", size="42")
    env.departed(sku="B", size="41")
    env.imported(sku="", size="Solo Personalizzazione")
    rep = env.run()
    assert rep.status == B.ST_APPLIED
    assert env.invariant() == {}
    assert set(env.held()) == {("M", "42"), ("", "Solo Personalizzazione")}


def test_a_customisation_only_variant_never_counts_as_a_key():
    env = Env()
    env.shop.product("Nike - Outlet", promo={"42": 0, "Solo Personalizzazione": 1},
                     extra_variants=(("Solo Personalizzazione", "SKU1"),))
    env.imported(size="42")
    plan = env.preview()
    assert ("SKU1", "Solo Personalizzazione") not in by_key(plan)
    assert variants.is_customisation_only({"title": "SOLO  personalizzazione"})


# ---------------------------------------------------------------------------
# The confirm's re-read is per key; what the baseline cannot establish is held
# ---------------------------------------------------------------------------

def test_the_confirm_re_reads_each_key_not_once_per_sku():
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 0, "43": 0})
    env.imported(size="42")
    env.imported(size="43")
    plan = env.preview()

    def hand_edit_43(shop):        # while 42 is written, someone edits 43 by hand
        lvl = shop.promo(out.gid, "43")
        lvl.available, lvl.on_hand = 2, 2

    env.shop.on("adjust", hand_edit_43)
    rep = env.apply(plan)
    assert outcome(rep, "SKU1", "42").result == "levelled"
    assert outcome(rep, "SKU1", "43").reason == B.R_MOVED
    assert env.shop.available(out.gid, "43") == 1          # held, then lowered by its excess
    assert env.invariant() == {}


def test_shelf_events_that_do_not_sum_to_n_hold_the_key():
    env = Env()
    pid, _ = env.imported(sku="C", size="40")
    env.be.add_event(pid, -1, event_type="IMPORT_SYNC", actor="IMPORT")   # an I3 violation: on the shelf, net 0
    plan = env.preview()
    assert by_key(plan)[("C", "40")].reason == B.R_PENDING
    env.apply(plan)
    assert env.invariant() == {}


def test_an_outlet_whose_levels_read_truncated_holds_its_keys():
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 0})
    env.shop.truncated.add(out.gid)
    env.imported(size="42")
    assert by_key(env.preview())[("SKU1", "42")].reason == B.R_UNREADABLE


def test_a_collection_outlet_the_resolver_did_not_return_is_never_guessed(monkeypatch):
    env = Env()
    out = env.shop.product("Nike - Outlet", promo={"42": 1, "43": 1})
    env.imported(size="42")
    monkeypatch.setattr(resolvers, "outlet_resolver", lambda t, sku: {"matches": [], "warning": None, "truncated": False})
    plan = env.preview()
    assert by_key(plan)[("SKU1", "42")].reason == B.R_UNRESOLVED
    assert any(a.startswith("collection_product_not_resolved:") for a in plan.anomalies)
    assert ("SKU1", "43") not in by_key(plan)                 # its units are not treated as stale
    env.apply(plan)
    assert env.shop.available(out.gid, "43") == 1


def test_a_shopify_failure_inside_one_key_holds_that_key_and_the_run_goes_on():
    env = Env()
    a = env.shop.product("A - Outlet", sku="A", promo={"42": 0})
    b = env.shop.product("B - Outlet", sku="B", promo={"42": 0})
    env.imported(sku="A", size="42")
    env.imported(sku="B", size="42")
    plan = env.preview()
    env.shop.fault("read_level", "timeout_before", when=lambda v: v["id"] == env.shop.variant(a.gid, "42").item)
    rep = env.apply(plan)
    assert rep.status == B.ST_APPLIED
    assert outcome(rep, "A", "42").reason == "shopify_error:ShopifyOutcomeUnknown"
    assert outcome(rep, "B", "42").result == "levelled" and env.shop.available(b.gid, "42") == 1
    assert env.invariant() == {}


# ---------------------------------------------------------------------------
# F1 (F7.4 baseline review): no level-up on a title match of another SKU
# ---------------------------------------------------------------------------
def _foreign_outlet(env: Env, *, status="ACTIVE", foreign="OTHER", promo=None, absent=()):
    """SKU1's outlet (it resolves by its 43) whose variant «42» carries ``foreign``."""
    return env.shop.product("Nike - Outlet", status=status, sizes=("43",), promo=promo or {"43": 0, "42": 0},
                            absent=absent, extra_variants=(("42", foreign),))


@pytest.mark.parametrize("absent", [(), ("42",)])
def test_f1_an_active_level_up_on_a_variant_of_another_sku_is_held(absent):
    env = Env()
    out = _foreign_outlet(env, absent=absent)
    _, e = env.imported(size="42")
    plan = env.preview()
    kp = by_key(plan)[("SKU1", "42")]
    assert (kp.action, kp.reason, kp.delta) == (B.ACT_HOLD, f"{B.R_SKU_MISMATCH}:OTHER", 0)
    assert kp.outlets[0].foreign_sku == "OTHER"
    env.apply(plan)
    assert env.shop.available(out.gid, "42") == (None if absent else 0)
    assert not [c for c in env.shop.calls if c[0] in ("adjust", "activate")]
    assert env.held() == {("SKU1", "42"): f"{B.R_SKU_MISMATCH}:OTHER"} and env.unstamped() == [e]
    assert env.invariant() == {}


@pytest.mark.parametrize("promo, lowered_to", [(2, 1), (1, 1)])
def test_f1_a_level_down_or_equal_on_a_variant_of_another_sku_is_held_its_excess_lowered(promo, lowered_to):
    """Not the key's variant: never levelled, stamped or set ONLINE — held and listed;
    the hold still lowers the excess (fewer, §3.3)."""
    env = Env()
    out = _foreign_outlet(env, promo={"43": 0, "42": promo})
    pid, e = env.imported(size="42")
    plan = env.preview()
    kp = by_key(plan)[("SKU1", "42")]
    assert (kp.action, kp.reason, kp.delta) == (B.ACT_HOLD, f"{B.R_SKU_MISMATCH}:OTHER", lowered_to - promo)
    env.apply(plan)
    assert env.shop.available(out.gid, "42") == lowered_to
    assert env.held() == {("SKU1", "42"): f"{B.R_SKU_MISMATCH}:OTHER"} and env.unstamped() == [e]
    assert not env.register().shelf("SKU1")[0].online


def test_f1_a_draft_key_on_a_variant_of_another_sku_is_still_zeroed():
    env = Env()
    out = _foreign_outlet(env, status="DRAFT", promo={"43": 0, "42": 1})
    _, e = env.imported(size="42")
    plan = env.preview()
    assert by_key(plan)[("SKU1", "42")].action == B.ACT_ZERO
    env.apply(plan)
    assert env.shop.available(out.gid, "42") == 0 and env.unstamped() == [e]   # its +1 withheld by the publish


@pytest.mark.parametrize("blank", [None, ""])
def test_f1_a_blank_variant_sku_is_levelled_as_ever(blank):
    env = Env()
    out = _foreign_outlet(env, foreign=blank)
    env.imported(size="42")
    plan = env.preview()
    kp = by_key(plan)[("SKU1", "42")]
    assert (kp.action, kp.delta) == (B.ACT_LEVEL, 1) and kp.outlets[0].foreign_sku is None
    env.apply(plan)
    assert env.shop.available(out.gid, "42") == 1 and env.held() == {}


def test_f1_a_source_variant_of_another_sku_is_listed_for_the_create():
    env = Env()
    env.shop.product("Nike Air", outlet=False, sizes=("43",), extra_variants=(("42", "OTHER"),))
    env.imported(size="42")
    plan = env.preview()
    assert by_key(plan)[("SKU1", "42")].action == B.ACT_NONE
    assert [(c.sku, c.size, c.note) for c in plan.catalogue] == [("SKU1", "42", "sku_mismatch_on_source:OTHER")]
