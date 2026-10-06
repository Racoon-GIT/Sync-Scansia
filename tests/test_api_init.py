"""INIT vertical — "Inizializza" on MySQL behind the confirm-token machinery
(plan §3.4), end to end through the HTTP layer: the real baseline service on the
in-memory register and the stateful fake Shopify. Every test needs FastAPI/httpx
and calls ``pytest.importorskip`` locally, like the sibling API test modules."""
from __future__ import annotations

import base64
from datetime import datetime, timedelta

import pytest

from backend.persistence.records import KIND_BASELINE, KIND_INIT_BEFORE

from tests.fake_shop import PROMO, FakeShop
from tests.register_fixtures import MemoryBackend

_SECRET = "unit-test-signing-secret"
MUTATIONS = {"duplicate", "adjust", "activate", "deactivate", "bulk", "status", "publish", "delete"}


def _auth(user: str = "racoon", pw: str = "s3cret-pw") -> dict:
    return {"Authorization": "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()}


class World:
    def __init__(self, monkeypatch) -> None:
        pytest.importorskip("fastapi")
        pytest.importorskip("httpx")
        monkeypatch.delenv("APP_USERNAME", raising=False)
        monkeypatch.setenv("APP_PASSWORD", "s3cret-pw")
        monkeypatch.setenv("TOKEN_SIGNING_SECRET", _SECRET)
        self.be = MemoryBackend()
        self.shop = FakeShop()
        self.out = self.shop.product("Nike - Outlet", promo={"42": 0})
        pid = self.be.add_pair("SKU1", "42")
        self.event = self.be.add_event(pid, +1, event_type="IMPORTED", actor="IMPORT", payload={"raw": {}})
        self.client = self.new_client()

    def new_client(self):
        from fastapi.testclient import TestClient

        from backend.api.jobs import SynchronousExecutor
        from backend.app import create_app
        from backend.config import ShopifyConfig

        app = create_app(
            config=ShopifyConfig("t.myshopify.com", "shpat_x", "2026-10", PROMO),
            sheet_factory=lambda: None,
            register_factory=lambda **kw: self.be.register(**kw),
            transport_factory=lambda: self.shop,
            audit_factory=lambda: None,
            executor=SynchronousExecutor(),
            promo_location_id=PROMO,
        )
        return TestClient(app)

    def preview(self) -> dict:
        sub = self.client.post("/init/preview", headers=_auth())
        assert sub.status_code == 202
        data = self.client.get(f"/init/preview/{sub.json()['job_id']}", headers=_auth()).json()
        assert data["status"] == "done", data
        return data["result"]

    def body(self, result: dict, **over) -> dict:
        # one minute before the snapshot, on the (fake) database clock the service checks against
        at = datetime.fromisoformat(result["plan"]["previewed_at"]) - timedelta(minutes=1)
        return {"plan_hash": result["plan_hash"], "confirm_token": result["confirm_token"], "confirm": "CONFERMO",
                "dlq_checked_at": at.isoformat(), "dlq_count": 0, **over}

    def apply(self, body: dict):
        return self.client.post("/init/apply", headers=_auth(), json=body)

    def apply_relevel(self, result: dict):
        return self.client.post("/relevel/apply", headers=_auth(), json=self.body(result))

    def mutations(self):
        return [c[0] for c in self.shop.calls if c[0] in MUTATIONS]


def test_status_reports_the_baseline_and_requires_auth(monkeypatch):
    w = World(monkeypatch)
    assert w.client.get("/init/status").status_code == 401
    assert w.client.get("/init/status", headers=_auth()).json() == {"baseline_done": False}
    res = w.preview()
    sub = w.apply(w.body(res))
    assert sub.status_code == 202
    assert w.client.get("/init/status", headers=_auth()).json()["baseline_done"] is True


def test_the_preview_returns_the_plan_and_a_token_and_writes_nothing(monkeypatch):
    w = World(monkeypatch)
    res = w.preview()
    plan = res["plan"]
    assert res["plan_hash"] and res["confirm_token"]
    assert plan["counts"]["level"] == 1 and plan["keys"][0]["sku"] == "SKU1" and plan["previewed_at"].endswith("+00:00")
    assert w.mutations() == [] and w.be.register().audit_rows(KIND_INIT_BEFORE) == []


def test_a_confirmed_apply_levels_and_writes_the_baseline_row_as_the_acting_user(monkeypatch):
    w = World(monkeypatch)
    res = w.preview()
    sub = w.apply(w.body(res))
    assert sub.status_code == 202
    out = w.client.get(f"/init/apply/{sub.json()['job_id']}", headers=_auth()).json()
    assert out["status"] == "done" and out["result"]["status"] == "APPLIED", out
    assert out["result"]["report"]["stamped"] == 1
    base = w.be.register().baseline()
    assert base.actor == "racoon" and base.payload["dlq"]["count"] == 0
    assert w.shop.available(w.out.gid, "42") == 1


@pytest.mark.parametrize("over,code,reason", [
    ({"confirm_token": "nope"}, "confirm_invalid", None),
    ({"confirm": "confermo?"}, "gesture_required", None),
    ({"dlq_count": 2}, "gesture_required", "dlq_unresolved_entries"),
    ({"dlq_checked_at": None}, "gesture_required", "dlq_check_missing"),
    ({"dlq_checked_at": "2020-01-01T00:00:00Z"}, "gesture_required", "dlq_check_too_old"),
])
def test_a_refused_confirm_creates_no_job_and_writes_nothing(monkeypatch, over, code, reason):
    w = World(monkeypatch)
    res = w.preview()
    resp = w.apply(w.body(res, **over))
    assert resp.status_code == 409
    err = resp.json()["error"]
    assert err["code"] == code and err.get("reason") == reason
    assert w.mutations() == [] and w.be.register().audit_rows(KIND_BASELINE) == []


def test_a_plan_this_process_no_longer_keeps_is_refused(monkeypatch):
    w = World(monkeypatch)
    res = w.preview()
    w.client = w.new_client()                                  # a restart: the kept plans are gone
    resp = w.apply(w.body(res))
    assert resp.status_code == 409 and resp.json()["error"]["code"] == "preview_expired"
    assert w.mutations() == []


def test_an_applied_plan_cannot_be_confirmed_twice(monkeypatch):
    w = World(monkeypatch)
    res = w.preview()
    assert w.apply(w.body(res)).status_code == 202
    resp = w.apply(w.body(res))
    assert resp.status_code == 409 and resp.json()["error"]["code"] == "preview_expired"


def test_the_preview_after_the_baseline_reports_it_done(monkeypatch):
    w = World(monkeypatch)
    w.apply(w.body(w.preview()))
    assert w.preview()["plan"]["already_done"] is True


@pytest.mark.parametrize("method,path", [
    ("post", "/init/preview"), ("get", "/init/preview/x"), ("post", "/init/apply"), ("get", "/init/apply/x"),
])
def test_init_routes_require_auth(monkeypatch, method, path):
    w = World(monkeypatch)
    assert getattr(w.client, method)(path).status_code == 401


# ---------------------------------------------------------------------------
# /relevel — the per-key re-level behind the same custody and confirm gate
# ---------------------------------------------------------------------------

def _job(w, path: str, sub) -> dict:
    assert sub.status_code == 202, sub.json()
    return w.client.get(f"{path}/{sub.json()['job_id']}", headers=_auth()).json()


def test_relevel_lists_previews_and_releases_a_held_key(monkeypatch):
    w = World(monkeypatch)
    w.shop.products[w.out.gid].status = "ARCHIVED"            # the baseline holds SKU1/42 (unexpected status)
    w.apply(w.body(w.preview()))
    held = w.client.get("/relevel/held", headers=_auth()).json()["held"]
    assert [h["key"] for h in held] == [["SKU1", "42"]]
    w.shop.products[w.out.gid].status = "ACTIVE"
    for p in w.be.register().shelf():                       # FIX4 needs the SKU's price and compare-at
        w.be.db.state.pairs[p.pair_id].update(PRICE_OUTLET=50, PRICE_HIGH=100)
    res = _job(w, "/relevel/preview", w.client.post("/relevel/preview", headers=_auth(),
                                                    json={"keys": [["SKU1", "42"]]}))["result"]
    assert res["plan"]["keys"][0]["branch"] == "active"
    out = _job(w, "/relevel/apply", w.apply_relevel(res))
    assert out["result"]["status"] == "APPLIED", out
    assert out["result"]["report"]["outcomes"][0]["result"] == "released"
    assert w.client.get("/relevel/held", headers=_auth()).json()["held"] == []
    assert w.shop.available(w.out.gid, "42") == 1


def test_relevel_refuses_bad_keys_and_an_init_token(monkeypatch):
    w = World(monkeypatch)
    bad = w.client.post("/relevel/preview", headers=_auth(), json={"keys": [["SKU1"]]})
    assert bad.status_code == 422 and bad.json()["error"]["code"] == "invalid_keys"
    res = w.preview()                                          # an INIT token never confirms a re-level
    resp = w.client.post("/relevel/apply", headers=_auth(), json=w.body(res))
    assert resp.status_code == 409 and resp.json()["error"]["code"] == "confirm_invalid"
