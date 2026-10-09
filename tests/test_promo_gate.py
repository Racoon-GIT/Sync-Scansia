"""The Promo anchor gate (plan E18): ``PROMO_LOCATION_ID`` must resolve, live, to the
location named "Promo" before any pass writes — checked before the lease is taken,
so a wrong id writes nothing."""
from __future__ import annotations

import pytest

from backend.services import passes
from backend.services.lease import PassLease
from backend.services.promo_gate import PromoAnchorInvalid, require_promo_location

from tests.fake_shop import MAG, PROMO, FakeShop
from tests.register_fixtures import MemoryBackend


def test_the_gate_accepts_promo_and_refuses_another_or_no_location():
    shop = FakeShop()
    require_promo_location(shop, PROMO)
    with pytest.raises(PromoAnchorInvalid, match="another location"):
        require_promo_location(shop, MAG)                          # the README's "Magazzino"
    with pytest.raises(PromoAnchorInvalid, match="no location"):
        require_promo_location(shop, "gid://shopify/Location/999")


def test_a_pass_with_a_wrong_anchor_writes_nothing_not_even_the_lease():
    be, shop = MemoryBackend(), FakeShop()
    be.seed_baseline()
    shop.locations[PROMO] = "Magazzino"
    reg = be.register()
    gen = reg.lease_state().generation
    with pytest.raises(PromoAnchorInvalid):
        passes.decrement_pass(reg, shop, PROMO, lease=PassLease(reg, "decrement", sleep=lambda s: None),
                              sleep=lambda s: None)
    with pytest.raises(PromoAnchorInvalid):
        passes.Holder(reg, shop, PROMO, "pricing", sleep=lambda s: None).start()
    assert reg.lease_state().generation == gen and reg.lease_state().holder is None
    assert shop.names() == ["locations", "locations"]


def test_the_api_maps_it_to_a_stable_code():
    from backend.api.errors import map_exception

    err = map_exception(PromoAnchorInvalid("x"))
    assert (err.status_code, err.error_code) == (503, "promo_anchor_invalid")
