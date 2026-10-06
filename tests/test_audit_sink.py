"""``RegisterAuditSink`` — the audit boundary on ``sm_outlet_audit`` (plan §3.1,
§3.6), on the in-memory store: fenced writes only, the price intent's unique-key
round trip, the delete / init abort gates, and the ``GET /audit`` read."""
from __future__ import annotations

from datetime import datetime

import pytest

from backend.persistence.audit_sink import AuditWriteUnfenced, IntentNotFound, RegisterAuditSink
from backend.persistence.records import (
    KIND_AUDIT,
    KIND_DELETE,
    KIND_INIT_BEFORE,
    KIND_PRICE,
    LeaseLost,
)
from backend.persistence.memory_store import SimulatedCrash
from backend.services.delete_service import BeforeSnapshot, DeleteOutcomeEvent
from backend.services.init_service import InitBeforeSnapshot, InitDemotedRowSnapshot, InitTargetSnapshot
from backend.services.pricing_service import PriceIntent, ProductPrior, VariantPrior

from tests.register_fixtures import MemoryBackend, StubLease


@pytest.fixture
def be():
    return MemoryBackend()


def _sink(be, lease=True, actor="ale"):
    reg = be.register()
    return reg, RegisterAuditSink(reg, actor=actor, lease=StubLease(reg) if lease else None)


def _snapshot(gid="gid://shopify/Product/77"):
    return BeforeSnapshot(product_gid=gid, title="X - Outlet", handle="x-outlet", status="DRAFT",
                          tags=("a",), variants=(), image_srcs=(), metafields=(), collections=())


def test_every_write_needs_the_pass_lease(be):
    _, sink = _sink(be, lease=False)
    with pytest.raises(AuditWriteUnfenced):
        sink.write_durable(_snapshot())
    with pytest.raises(AuditWriteUnfenced):
        sink.write_event(action="x")
    assert be.register().audit_rows(KIND_DELETE) == []


def test_price_intent_round_trip_by_unique_id(be):
    reg, sink = _sink(be)
    intent = PriceIntent(mode="percent", plan_hash="abc123", priors=(
        ProductPrior("gid://shopify/Product/1", "SKU1", (VariantPrior("v1", "50.00", "100.00"),)),))
    i1 = sink.capture_before(intent)
    i2 = sink.capture_before(intent)
    assert i1 != i2 and len(i1) == 32
    assert sink.load(i1) == intent
    (row,) = reg.audit_rows(KIND_PRICE, intent_id=i1)
    assert row.actor == "ale" and row.action == "capture_before"
    with pytest.raises(IntentNotFound):
        sink.load("0" * 32)


def test_write_durable_is_the_delete_abort_gate(be):
    reg, sink = _sink(be)
    sink.write_durable(_snapshot())
    (row,) = reg.audit_rows(KIND_DELETE)
    assert row.product_id == 77 and row.payload["product_gid"] == "gid://shopify/Product/77"

    def crash(i, op):
        raise SimulatedCrash("db down")

    be.db.crash_hook = crash
    with pytest.raises(SimulatedCrash):          # raises: the caller never reaches productDelete
        sink.write_durable(_snapshot("gid://shopify/Product/78"))
    be.db.crash_hook = None
    assert len(reg.audit_rows(KIND_DELETE)) == 1


def test_a_lost_lease_refuses_the_snapshot(be):
    reg = be.register()
    lease = StubLease(reg, ttl_ms=1000)
    sink = RegisterAuditSink(reg, actor="ale", lease=lease)
    be.advance(1001)
    with pytest.raises(LeaseLost):
        sink.write_durable(_snapshot())


def test_init_before_snapshot(be):
    reg, sink = _sink(be)
    sink.write_init_before(InitBeforeSnapshot(
        plan_hash="ph", rows=(InitDemotedRowSnapshot("u", "SKU", "42", "1", ""),),
        targets=(InitTargetSnapshot("gid://shopify/Product/9", "ACTIVE"),)))
    (row,) = reg.audit_rows(KIND_INIT_BEFORE)
    assert row.plan_hash == "ph" and row.payload["targets"][0]["prior_status"] == "ACTIVE"


def test_events_read_back_newest_window_oldest_first_in_rome_time(be):
    reg, sink = _sink(be)
    for i in range(5):
        sink.write_event(action=f"a{i}", target_gids=f"g{i}", plan_hash=f"h{i}", result=f"r{i}")
        be.advance(1000)
    sink.write_outcome(DeleteOutcomeEvent("gid://shopify/Product/5", "gid://shopify/DeletedProduct/5", "DELETED"))
    recent = sink.read_recent(3)
    assert [e["action"] for e in recent] == ["a3", "a4", "product_delete"]
    assert recent[0] == {"ts": recent[0]["ts"], "actor": "ale", "action": "a3", "target_gids": "g3",
                         "plan_hash": "h3", "result": "r3"}
    assert datetime.fromisoformat(recent[0]["ts"]).utcoffset().total_seconds() == 7200  # CEST
    assert len(sink.read_recent(0)) == 6
    assert reg.audit_rows(KIND_AUDIT)[-1].product_id == 5
