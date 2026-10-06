"""B-3's trigger, boot pass and worker (plan §6 B-3, D23) — the build tests B-3
lists: the endpoint answers at once and never runs the pass on the caller's time,
a duplicate and a late trigger no-op, a trigger during a pass coalesces, the boot
pass applies a pending −Δ; the trigger's own bearer, fail-closed."""
from __future__ import annotations

import base64
import threading

import pytest

from backend.services.decrement_worker import DecrementWorker, make_run_pass

from tests.fake_shop import PROMO, FakeShop
from tests.register_fixtures import MemoryBackend

SECRET = "trigger-unit-test-secret"   # synthetic test fixture, no live value — guard-secrets: allow


def _inline(fn):
    fn()


# ---------------------------------------------------------------------------
# The worker (no web layer)
# ---------------------------------------------------------------------------

def test_a_trigger_during_a_pass_coalesces_into_at_most_one_more():
    gate = threading.Event()
    started = threading.Event()
    calls = []

    def run_pass():
        calls.append(1)
        started.set()
        gate.wait(5)

    w = DecrementWorker(run_pass)
    assert w.trigger("a") == {"accepted": True, "coalesced": False}
    assert started.wait(5)
    assert [w.trigger(x)["coalesced"] for x in ("b", "c", "d")] == [True, True, True]
    gate.set()
    for _ in range(500):
        if not w.running:
            break
        threading.Event().wait(0.01)
    assert not w.running and len(calls) == 2 and w.runs == 2


def test_a_failing_pass_is_kept_never_raised_and_the_worker_goes_on():
    outcomes = iter([RuntimeError("boom"), None])

    def run_pass():
        e = next(outcomes)
        if e:
            raise e

    w = DecrementWorker(run_pass, spawn=_inline)
    w.trigger("a")
    assert w.last["outcome"] == "error" and w.last["error"] == "RuntimeError"
    w.trigger("b")
    assert w.last["outcome"] == "done" and w.runs == 2


# ---------------------------------------------------------------------------
# The app: the boot pass, the route
# ---------------------------------------------------------------------------

class World:
    def __init__(self, monkeypatch, *, secret: str = SECRET) -> None:
        pytest.importorskip("fastapi")
        pytest.importorskip("httpx")
        from backend.api.jobs import SynchronousExecutor
        from backend.app import create_app
        from backend.config import ShopifyConfig

        monkeypatch.setenv("APP_PASSWORD", "s3cret-pw")
        monkeypatch.delenv("APP_USERNAME", raising=False)
        if secret:
            monkeypatch.setenv("TRIGGER_SECRET", secret)
        else:
            monkeypatch.delenv("TRIGGER_SECRET", raising=False)
        self.be = MemoryBackend()
        self.be.seed_baseline()
        self.shop = FakeShop()
        self.out = self.shop.product("Nike - Outlet", promo={"42": 1, "43": 1})
        self.be.add_shelf_pair("SKU1", "42", reconciled=True)
        self.be.add_shelf_pair("SKU1", "42", reconciled=True)
        self.app = create_app(
            config=ShopifyConfig("t.myshopify.com", "shpat_x", "2026-10", PROMO),
            sheet_factory=lambda: None, register_factory=lambda **kw: self.be.register(**kw),
            transport_factory=lambda: self.shop, audit_factory=lambda: None,
            executor=SynchronousExecutor(), promo_location_id=PROMO,
        )
        self.worker = DecrementWorker(make_run_pass(self.app.state), spawn=_inline)
        self.app.state.decrement_worker = self.worker

    def client(self):
        from fastapi.testclient import TestClient

        return TestClient(self.app)


def _basic() -> dict:
    return {"Authorization": "Basic " + base64.b64encode(b"racoon:s3cret-pw").decode()}


def test_the_boot_pass_applies_a_pending_minus_one(monkeypatch):
    w = World(monkeypatch)
    w.be.consume("SKU1", "42")                                # a sale while the instance slept
    from fastapi.testclient import TestClient

    with TestClient(w.app):                                   # startup: the boot pass
        pass
    assert w.worker.runs == 1 and w.shop.available(w.out.gid, "42") == 0


@pytest.mark.parametrize("headers", [{"Authorization": f"Bearer {SECRET}"}, {"X-Trigger-Secret": SECRET}])
def test_the_trigger_runs_a_pass_and_a_duplicate_no_ops(monkeypatch, headers):
    w = World(monkeypatch)
    c = w.client()
    w.be.consume("SKU1", "42")
    r = c.post("/outlet/decrement/trigger", headers=headers)
    assert r.status_code == 202 and r.json() == {"accepted": True, "coalesced": False}
    assert w.shop.available(w.out.gid, "42") == 0
    adjusts = w.shop.count("adjust")
    assert c.post("/outlet/decrement/trigger", headers=headers).status_code == 202   # a duplicate / late one
    assert w.shop.count("adjust") == adjusts and w.worker.runs == 2


def test_the_trigger_needs_its_own_bearer_not_the_app_password(monkeypatch):
    w = World(monkeypatch)
    c = w.client()
    for headers in ({}, _basic(), {"Authorization": "Bearer nope"}, {"X-Trigger-Secret": "nope"}):
        r = c.post("/outlet/decrement/trigger", headers=headers)
        assert r.status_code == 401 and r.json()["error"]["code"] == "trigger_unauthorized"
    assert w.worker.runs == 0
    assert c.post(f"/outlet/decrement/trigger?secret={SECRET}").status_code == 401   # never URL-borne


def test_an_unset_trigger_secret_fails_closed(monkeypatch):
    w = World(monkeypatch, secret="")
    r = w.client().post("/outlet/decrement/trigger", headers={"Authorization": "Bearer "})
    assert r.status_code == 503 and r.json()["error"]["code"] == "trigger_not_configured"
    assert w.worker.runs == 0


def test_the_trigger_never_waits_for_the_jobs_slot(monkeypatch):
    w = World(monkeypatch)
    w.app.state.job_store.create("publish_apply")              # a publish holds the single job slot
    r = w.client().post("/outlet/decrement/trigger", headers={"Authorization": f"Bearer {SECRET}"})
    assert r.status_code == 202 and w.worker.runs == 1


def test_the_status_shows_the_last_pass_behind_basic_auth(monkeypatch):
    w = World(monkeypatch)
    c = w.client()
    assert c.get("/outlet/decrement/status").status_code == 401
    c.post("/outlet/decrement/trigger", headers={"Authorization": f"Bearer {SECRET}"})
    st = c.get("/outlet/decrement/status", headers=_basic()).json()
    assert st["runs"] == 1 and st["last"]["outcome"] == "done" and st["running"] is False


def test_a_thread_that_cannot_start_leaves_the_worker_able_to_start_the_next():
    def no_thread(fn):
        raise RuntimeError("can't start new thread")

    w = DecrementWorker(lambda: None, spawn=no_thread)
    with pytest.raises(RuntimeError):
        w.trigger("a")
    assert not w.running
    w._spawn = _inline
    assert w.trigger("b") == {"accepted": True, "coalesced": False} and w.runs == 1
