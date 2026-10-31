# TRACKER — Scansia Manager (refactor Sync-Scansia → web utility interna)

<!-- RULES (parent CLAUDE.md rule 5 — Current-state protocol):
     - Agents read ONLY "Current state" at session start (Read with limit ~45 lines).
     - REWRITE "Current state" entirely at session end (max 45 lines). Never append to it.
     - Superseded detail goes to TRACKER-ARCHIVE.md (or git log for committed work).
     - Keep "Tried & discarded" entries while still relevant: they prevent re-proposing dead ends. -->

## Current state

Pipeline: build · item: W5 OUTLET_PRODUCT_ID backfill (close-out wave 2) · plan: IT/_scansia-programme/closeout-plan.md (parent Racoon-LAB repo) · gate-1: Ale 2026-10-30 (D64, rev. 2) · gate-2: pending

**W5 backfill BUILT, NOT PUSHED (2026-10-31)** — brief `IT/_scansia-programme/briefs/sync-scansia-W5-backfill-fix.md`, spec = W5 report Q4 (`~/.claude/racoon-bus/msg/scansia-closeout/w5-report.md`, sha `949c3942699f…`). Cause: a publish resolves the outlet seconds after DRAFT→ACTIVE, the smart collection has not re-indexed → `review` match → `OUTLET_PRODUCT_ID` not written, and no later pass fills it. Fix `6cdd5af`: `passes._backfill_outlet_products`, called in `decrement_pass` after `n0_release`, runs `flows.maybe_set_outlet_product` for each SKU with a shelf pair `ONLINE` 1 and id NULL (same §3.1 rule, errors isolated per SKU, surfaced `outlet_product_backfill_error`). Tests `6cdd5af` + `876b82f` (4 in `tests/test_decrement.py`; coverage row M21a). Round 1 (coordinator `scansia-programme-98`, `~/.claude/racoon-bus/msg/scansia-closeout/w5-change-request-1.md`): `3001bb7` adds `lease.keepalive()` before each `ctx.resolve` in the backfill + 3 tests (renewal per SKU, `LeaseLost` aborts and releases, `StoreError` on one write isolated) — 7 backfill tests in all. Suite 886 passed / 28 skipped (was 879). Gate-2: 5 PASS rows on `876b82f` + 3 PASS on `3001bb7` in `docs/gate-2-log.md`. Awaiting `GATE:2+push` (coordinator asks Ale); origin/main still `2d02350`. The `plan:` field names the parent-repo plan: `guard-build` resolves a plan only inside this nested repo, so the push may need `SKIP_PIPELINE_GUARD` (reason line in the docs commit message) — only if the guard refuses on that field alone.

**Before F7.4**: F7.4 publish-poll fix pushed 2026-10-30 (D63, `501cc65..2d02350`, code `8cf77d9`; `git log`); the deploy check was the coordinator's.

**Before that**: the F1 SKU guard is live on `501cc65` (D62, deploy verified by `sviluppo-2d`); F7 live since `1a161cc` (2026-10-09, D51); baseline APPLIED 2026-10-29 (D58). Residual 1 closed with evidence 2026-10-30 (census `IT/_scansia-programme/measures/2026-10-30-residual1/`: 0 breaches, 0 units on a foreign/blank match; the coordinator's per-publish count is the monitor; Ale may overrule). Full text of F1's push, guard and re-grade: `TRACKER-ARCHIVE.md`. F1's cost left stated, not fixed: D1 zeroes units on a mixed-SKU DRAFT outlet's other variant; `app.js` `sizeCell` untested. The plan is the contract (sha `6dadcdd7…`), never edited here.

**Next**:
- [ ] W5: wait for the coordinator's push instruction or change request. Otherwise idle for the coordinator. The first F7.4 publish (preview A) is the coordinator's; F2 (93 SKUs without `PRICE_OUTLET`) is Ale's, not decided (D59).
- [ ] Not granted, each its own gate: the suspended cron's deletion and the `GSPREAD_*` / Google env vars' removal (SERVER / Ale).
- [ ] Deferred (runner branch): `runner.py:353`'s re-run hint and S4.7's `if live:`; list 2 §5 "Writes at `M`"; a later list's `A` borrows no live product's files.

**Active constraints & decisions**:
- No write to `racoon`, Shopify, Make, production Trello, the Scheduler or Render outside a recorded gate; this lane runs nothing live. Integration tests on `racoon_staging` only (option files `~/.config/racoon-scansia/<user>.cred`, read by the driver only — never opened, never printed).
- Render `scansia-manager` deploys **`main` on commit** (`IT/SERVER/infra-inventory.md:47`): every push to `main` is a deploy — `GATE:push`, except a docs-only diff (`IT/session-protocol.md` §7). `render.yaml` is the suspended `Sync-Scansia` cron: it stays until SERVER / Ale delete the cron.
- Branches: `origin/main` = `2d02350`; local `main` carries `693a893` (hook-registration removal, `.claude/settings.json` only) and the W5 commits on top, not pushed. `fix/f7-f1-sku-guard`, `fix/f7-create-order`, `fix/f7-outlet-media`, `feat/scansia-manager` are contained in `main`. Local only: `f73-live-runner` `dc06b32`, `f73-live-check` `602823f` — their runner is pinned to `bbc1cfb`'s `ops.py`, so it refuses to start on `main` (F1 changed `ops.py` docstrings).
- E18: `PROMO_LOCATION_ID` may be the GID or the numeric id; every writing pass refuses (503 `promo_anchor_invalid`) unless it names the location called "Promo". D28: staging has its own app user. D33: the N-0 release waits while an exact-size Promo level has `committed > 0`.
- One connect retry (`mysql_store.CONNECT_ATTEMPTS`): from the Mac a connect to `78.46.244.227:3306` fails now and then (errno 2003, OS 60).
- No personal data in the repo (ids, SKUs, counts only). DRY-RUN stays the default (project `CLAUDE.md` rule 2).
- Working tree: clean (the `.claude/settings.json` change is committed as `693a893`, Ale-approved, not pushed).

**Tried & discarded (still relevant)**:
- The sheet as store (Q-persistenza, 2026-07-06): voided by the F6 cutover — the register is MySQL.
- Level-setting sweeps ("set Promo to the shelf count"): they put back units sold during an intake stall (plan §3.3).
- A Shopify-only price revert: the next publish re-sends the register's price on every variant, undoing it.
- Bypassing `mutate-verify.sh`'s thermal refusal: a throttled run forges kills. Under load, narrow `--suite` to the killing modules and pre-check each mutant on the full suite.
- A revive keeping a foreign-SKU unit whose own SKU has the pair (v2's `no_own_pair`): two units, one pair (D1).
- Dropping only a withheld size from a CREATE: its write-ahead row carries every target's ids — abort instead.
- `git push` prefixed with `cd <repo>;`: `guard-build` resolves no repo and refuses — push from the session cwd.
- `git stash push` in a command: `guard-build` reads it as a push and blocks — use `git stash -- <path>`. A dirty `.claude/settings.json` stamps `-dirty` on gate-2 rows: stash it, earn the row, pop.
- Writing `.md` via a shell redirect: `guard-bash-channel` refuses — use Write/Edit.

**Key refs**: plan `docs/f7-mysql-adapter-plan.md`; `docs/f7-coverage-map.md` (gate-2's checklist; §15 = F1); `docs/f7-build-notes.md`; receipts `docs/gate-2-log.md` (over the 40 KB ceiling — the cut is Ale's); gate-2 tools `~/.claude/racoon-bus/msg/sync-scansia-gate2/` (`drive_f1.sh` takes the suite as its 4th argument; run it under `/usr/bin/lockf -k /tmp/stockmgr-suite.lock`; mutant files `mut_f1*.py` are never edited once a row cites them); `docs/testing.md` (the venv lives in a session scratchpad: rebuild it from `/usr/local/bin/python3`, 3.13); history: `TRACKER-ARCHIVE.md`.

## Archive
Full history: `TRACKER-ARCHIVE.md` (same folder). Keep appending superseded material there — never here.
