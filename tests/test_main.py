"""``main.py`` — the cron entry point: ``RUN_MODE=REORDER`` only since F7 §3.8
(``SYNC`` and ``FIX_PRICES`` read the sheet: retired, they stop with exit 1 and run
nothing); ``DRY_RUN`` fail-closed."""
from __future__ import annotations

import sys

import pytest

import main
from src import reorder_collection

COLLECTION = "262965428289"


def test_main_resolve_dry_run_fail_closed(monkeypatch):
    monkeypatch.delenv("DRY_RUN", raising=False)
    assert main._resolve_dry_run() is True
    for tok in ["", "true", "1", "yes", "garbage", "dry", "TRUE "]:
        monkeypatch.setenv("DRY_RUN", tok)
        assert main._resolve_dry_run() is True, tok
    for tok in ["false", "0", "no", "apply", "APPLY", "Off"]:
        monkeypatch.setenv("DRY_RUN", tok)
        assert main._resolve_dry_run() is False, tok


@pytest.mark.parametrize("mode", ["SYNC", "FIX_PRICES", "sync", None, "", "PUBLISH"])
def test_a_retired_missing_or_unknown_mode_exits_non_zero_and_runs_nothing(monkeypatch, mode):
    ran = []
    monkeypatch.setattr(main, "run_reorder", lambda: ran.append(1))
    if mode is None:
        monkeypatch.delenv("RUN_MODE", raising=False)
    else:
        monkeypatch.setenv("RUN_MODE", mode)
    with pytest.raises(SystemExit) as exc:
        main.main()
    assert exc.value.code == 1 and ran == []


def test_reorder_is_dispatched(monkeypatch):
    ran = []
    monkeypatch.setattr(main, "run_reorder", lambda: ran.append(1))
    monkeypatch.setenv("RUN_MODE", "reorder")
    main.main()
    assert ran == [1]


@pytest.mark.parametrize("dry_run, applies", [(None, False), ("true", False), ("apply", True)])
def test_reorder_applies_only_on_an_explicit_token_and_restores_argv(monkeypatch, dry_run, applies):
    seen = []
    monkeypatch.setattr(reorder_collection, "main", lambda: seen.append(list(sys.argv)))
    monkeypatch.setenv("COLLECTION_ID", COLLECTION)
    if dry_run is None:
        monkeypatch.delenv("DRY_RUN", raising=False)
    else:
        monkeypatch.setenv("DRY_RUN", dry_run)
    before = list(sys.argv)
    main.run_reorder()
    assert seen == [["reorder_collection.py", "--collection-id", COLLECTION] + (["--apply"] if applies else [])]
    assert sys.argv == before


@pytest.mark.parametrize("collection", [None, "", "gid://shopify/Collection/262965428289"])
def test_reorder_refuses_a_missing_or_non_numeric_collection(monkeypatch, collection):
    ran = []
    monkeypatch.setattr(reorder_collection, "main", lambda: ran.append(1))
    if collection is None:
        monkeypatch.delenv("COLLECTION_ID", raising=False)
    else:
        monkeypatch.setenv("COLLECTION_ID", collection)
    with pytest.raises(SystemExit) as exc:
        main.run_reorder()
    assert exc.value.code == 1 and ran == []
