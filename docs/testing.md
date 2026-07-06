# Testing — local pytest and the FastAPI skip gap

This is a house pattern, not a CI pipeline: `pytest` is run locally (pre-commit)
in the developer's own environment. There is no configured CI service for this
repo today.

## Why some tests are skipped locally

The Scansia Manager web layer (`backend/api/*`, `backend/app.py`) imports
`fastapi`/`httpx`. Those two packages are listed in `requirements.txt` but are
NOT required by the compute core (`backend/services/*`, `backend/shopify/*`,
`backend/gsheet/*`), which is deliberately importable and testable on a bare
interpreter (see the module docstrings — e.g. `backend/api/mutations.py`:
"imports FastAPI, so it loads ONLY where the web deps are installed").

Every test module that exercises the web layer gates itself with
`pytest.importorskip("fastapi")` / `pytest.importorskip("httpx")`. If those
packages are not installed in the interpreter running `pytest`, those tests
report as **SKIPPED**, not failed — this is intentional, not a masked failure.
As of this writing that gap is roughly 80 tests (TestClient tier of
`test_api_delete.py`, `test_api_prices.py`, `test_api_publish.py`, `test_app.py`,
plus the worker-tier tests in those same files that only need `fastapi` for the
import).

## Running the full suite (0 skips)

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

With `fastapi`/`httpx` present, every previously-skipped test executes for
real. A partial local run (fastapi/httpx absent) is still a valid signal for
everything OUTSIDE the web layer — it is just not a complete signal for the
API routes themselves.
