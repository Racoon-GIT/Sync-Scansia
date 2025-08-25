# Render deploy fix (force Python 3.12)

Your deploy is failing because Render is using Python 3.13 and tries to **build pandas** from source.
Pin Python to **3.12.4** using one (or more) of these, then clear the build cache and redeploy.

## Option 1 — render.yaml (preferred)
Commit this `render.yaml` at repo root. It sets `pythonVersion: 3.12.4` for the cron service.

## Option 2 — Environment variable (UI)
Add env var `PYTHON_VERSION=3.12.4` in Render → Service → Environment.

## Option 3 — .python-version (optional)
Add `.python-version` with `3.12.4` at repo root.

After committing:
1. Clear build cache (Render → Settings → Clear build cache)
2. Deploy again and check logs show `Using Python version 3.12.x`