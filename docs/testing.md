# Testing — local pytest, the FastAPI skip gap, the staging opt-in

This is a house pattern, not a CI pipeline: `pytest` is run locally (pre-commit)
in the developer's own environment. There is no configured CI service for this
repo today.

## Why some tests are skipped locally

The Scansia Manager web layer (`backend/api/*`, `backend/app.py`) imports
`fastapi`/`httpx`. Those two packages are listed in `requirements.txt` but are
NOT required by the compute core (`backend/services/*`, `backend/shopify/*`,
`backend/persistence/*` — the MySQL driver is imported lazily, at the first
connect), which is deliberately importable and testable on a bare interpreter
(see the module docstrings — e.g. `backend/api/mutations.py`: "imports FastAPI,
so it loads ONLY where the web deps are installed").

Every test module that exercises the web layer gates itself with
`pytest.importorskip("fastapi")` / `pytest.importorskip("httpx")`. If those
packages are not installed in the interpreter running `pytest`, those tests
report as **SKIPPED**, not failed — this is intentional, not a masked failure.
Measured 2026-10-08 on a bare Python 3.13 (pytest + requests, no fastapi): 664
passed, 131 skipped for `fastapi`, 28 for the staging opt-in below. The full venv
run: 795 passed, 28 skipped (the staging opt-in). "Bare" is the `python3` on PATH
(`/usr/local/bin`, 3.13); the system `/usr/bin/python3` (3.9) splits the same 823
tests differently (677 passed, 146 skipped) — compare counts on one interpreter.

## Running the full offline suite (no fastapi skips)

Use a throwaway virtualenv OUTSIDE the repo so the project's own
`requirements.txt` workflow (no `pip install` inside the repo — see root
`CLAUDE.md`) is respected:

```bash
python3 -m venv /tmp/scansia-test-venv
source /tmp/scansia-test-venv/bin/activate
pip install -r requirements.txt
cd /path/to/Sync-Scansia
pytest -q
deactivate
rm -rf /tmp/scansia-test-venv
```

With `fastapi`/`httpx` present, every previously-skipped web test executes for
real. A partial local run (fastapi/httpx absent) is still a valid signal for
everything OUTSIDE the web layer — it is just not a complete signal for the
API routes themselves.

`python3` here is `/usr/local/bin`'s 3.13 (2026-10-09 on `ff0894f`: 795 passed,
28 skipped): the Mac has no 3.12 (no pyenv, uv or python.org build), while the
Render pin is 3.12.4 (`.python-version`; `runtime.txt` agrees). Check that
parity statically: `pip install vermin` in the venv, then, from the repo root,
`vermin -t=3.12- --violations --no-tips backend main.py` (no violation at
`ff0894f`, minimum 3.9).

## The staging opt-in (28 tests)

The register store's contract (`tests/test_register_store.py`) runs on the
in-memory fake always, and on MySQL `racoon_staging` only when
`SCANSIA_STAGING=1` is set; `tests/test_consume_beside_f7.py` runs on staging
only. The fixture refuses any schema but `racoon_staging`, marks its rows
(`f7test`) and deletes them at each start; credentials are SERVER's option files
(`~/.config/racoon-scansia/<user>.cred`), read by the driver only. Harness and
facts: `docs/f7-build-notes.md` §Code layering.

```bash
SCANSIA_STAGING=1 pytest tests/test_register_store.py tests/test_consume_beside_f7.py -k "mysql or consume_beside"
```
