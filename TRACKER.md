# TRACKER — Scansia Manager (refactor Sync-Scansia → web utility interna)

<!-- RULES (parent CLAUDE.md rule 5 — Current-state protocol):
     - Agents read ONLY "Current state" at session start (Read with limit ~45 lines).
     - REWRITE "Current state" entirely at session end (max 45 lines). Never append to it.
     - Superseded detail goes to TRACKER-ARCHIVE.md (or git log for committed work).
     - Keep "Tried & discarded" entries while still relevant: they prevent re-proposing dead ends. -->

## Current state

Pipeline: shipped · item: F7 — Scansia Manager on MySQL · plan: docs/f7-mysql-adapter-plan.md · gate-1: Ale 2026-10-06 · gate-2: Ale 2026-10-08

**F7.4 publish-poll fix PUSHED (2026-10-30)** under `GATE:push` D63 (PROGRAM line `gate: Ale 2026-10-30 GATE:push`, `501cc65..2d02350`; brief `IT/_scansia-programme/briefs/sync-scansia-F7.4-publish-poll.md`). `origin/main` = **`2d02350`** (`ls-remote`, 2026-10-30). Code only in `8cf77d9`: `backend/static/app.js` passes `INIT_POLL` (30 min, 3 s) to both publish `pollJob` calls (the live preview ran 175 s past the 120 s default), plus `tests/test_api_ui.py::test_publish_polls_use_the_long_budget`, a source-text pin (no JS harness exists; red on the pre-fix `app.js`, seen). Suite 879 passed / 28 skipped. Gate-2: 2 PASS rows on `8cf77d9` (apply loses `INIT_POLL`; preview budget cut to 60 s) in `docs/gate-2-log.md`, + 1 earlier `-dirty` row. Pushed with Ale's «confermo» for `guard-foreign-push` over 5 commits (`FOREIGN_PUSH_OK`). **Only «pushed» is claimed: the Render deploy check is the coordinator's (`sviluppo-1d`).**

**Before that**: the F1 SKU guard is live on `501cc65` (D62, deploy verified by `sviluppo-2d`); F7 live since `1a161cc` (2026-10-09, D51); baseline APPLIED 2026-10-29 (D58). Residual 1 closed with evidence 2026-10-30 (census `IT/_scansia-programme/measures/2026-10-30-residual1/`: 0 breaches, 0 units on a foreign/blank match; the coordinator's per-publish count is the monitor; Ale may overrule). Full text of F1's push, guard and re-grade: `TRACKER-ARCHIVE.md`. F1's cost left stated, not fixed: D1 zeroes units on a mixed-SKU DRAFT outlet's other variant; `app.js` `sizeCell` untested. The plan is the contract (sha `6dadcdd7…`), never edited here.

**Next**:
- [ ] Nothing open for this lane: idle for the coordinator. The first F7.4 publish (preview A) is the coordinator's; F2 (93 SKUs without `PRICE_OUTLET`) is Ale's, not decided (D59).
- [ ] Not granted, each its own gate: the suspended cron's deletion and the `GSPREAD_*` / Google env vars' removal (SERVER / Ale).
- [ ] Deferred (runner branch): `runner.py:353`'s re-run hint and S4.7's `if live:`; list 2 §5 "Writes at `M`"; a later list's `A` borrows no live product's files.

**Active constraints & decisions**:
- No write to `racoon`, Shopify, Make, production Trello, the Scheduler or Render outside a recorded gate; this lane runs nothing live. Integration tests on `racoon_staging` only (option files `~/.config/racoon-scansia/<user>.cred`, read by the driver only — never opened, never printed).
- Render `scansia-manager` deploys **`main` on commit** (`IT/SERVER/infra-inventory.md:47`): every push to `main` is a deploy — `GATE:push`, except a docs-only diff (`IT/session-protocol.md` §7). `render.yaml` is the suspended `Sync-Scansia` cron: it stays until SERVER / Ale delete the cron.
- Branches: `origin/main` = `2d02350`; local `main` may carry this TRACKER's docs-only commits on top, not pushed. `fix/f7-f1-sku-guard`, `fix/f7-create-order`, `fix/f7-outlet-media`, `feat/scansia-manager` are contained in `main`. Local only: `f73-live-runner` `dc06b32`, `f73-live-check` `602823f` — their runner is pinned to `bbc1cfb`'s `ops.py`, so it refuses to start on `main` (F1 changed `ops.py` docstrings).
- E18: `PROMO_LOCATION_ID` may be the GID or the numeric id; every writing pass refuses (503 `promo_anchor_invalid`) unless it names the location called "Promo". D28: staging has its own app user. D33: the N-0 release waits while an exact-size Promo level has `committed > 0`.
- One connect retry (`mysql_store.CONNECT_ATTEMPTS`): from the Mac a connect to `78.46.244.227:3306` fails now and then (errno 2003, OS 60).
- No personal data in the repo (ids, SKUs, counts only). DRY-RUN stays the default (project `CLAUDE.md` rule 2).
- Working tree: the uncommitted `.claude/settings.json` (sha `638e2594…`) is not this programme's — left as found, out of every commit.

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
