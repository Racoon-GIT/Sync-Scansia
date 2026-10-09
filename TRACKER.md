# TRACKER — Scansia Manager (refactor Sync-Scansia → web utility interna)

<!-- RULES (parent CLAUDE.md rule 5 — Current-state protocol):
     - Agents read ONLY "Current state" at session start (Read with limit ~45 lines).
     - REWRITE "Current state" entirely at session end (max 45 lines). Never append to it.
     - Superseded detail goes to TRACKER-ARCHIVE.md (or git log for committed work).
     - Keep "Tried & discarded" entries while still relevant: they prevent re-proposing dead ends. -->

## Current state

Pipeline: shipped · item: F7 — Scansia Manager on MySQL · plan: docs/f7-mysql-adapter-plan.md · gate-1: Ale 2026-10-06 · gate-2: Ale 2026-10-08

**Phase**: F7 **live on `1a161cc`** (2026-10-09) — the F7.3 fixes shipped under `GATE:push` D51 (Ale), after `/ship` (steps 0–6 pass; lint, lockfile n/a): `main` = merge `1a161cc` (parents `538c234` + `fix/f7-create-order` `65ca5fb`; its tree is `65ca5fb`'s), pushed `619e6ae..1a161cc` (13 commits; `FOREIGN_PUSH_OK` typed by the orchestrator on Ale's behalf, D51's text) — `ls-remote` `main` = `1a161cc` at 20:40:13Z. The deploy, verified by the orchestrator (its DONE): Render `dep-db4l2augekts73fmejmg` live 20:41:21Z on `1a161cc`; boot log «decrement pass failed: BaselineMissing» (the expected stop until «Inizializza»); `/health` 200. Programme state: `IT/_scansia-programme/PROGRAM.md`. The plan is the contract (sha `6dadcdd7…`): never edited here; F7.3's results are the orchestrator's to fold in (PROGRAM 1623e44 names the lines).

**What shipped** (D49, context `57d770c7`; reviewed by the orchestrator; GATE-2 self-grade 9.5/10, its reasons in `65ca5fb`'s TRACKER): F3 — D1 copies the images (`includeImages`, `b4a4c35`); S3.8 — CREATE activates Promo on every variant lacking it first, re-reads, then FIX1's order; no flow deactivates a variant's last active level (`flows.zero_rows_for` keeps it, zeroed, surfaced); R053 — the activation body sends `available` 0 only; S2.10 — an unconfirmed publish is surfaced and recorded on its row. Suite on `1a161cc` (venv 3.13): 828 passed, 28 skipped (the staging opt-in). Gate-2: 10 PASS on `bbc1cfb` + F3's on `b4a4c35` (`docs/gate-2-log.md`; mechanisms `docs/f7-coverage-map.md` §13–§14).

**The live checks** (both `shopify-operator`, test products only, residue empty): run `202610091404` (D45, list 1, `f73-live-runner` @ `dc06b32`) and run `202610092018` (D50, list 2 `docs/f7.3-fixes-live-check.md`, `f73-live-check` @ `602823f`; 63 calls, 17 mutations) — judged by the orchestrator, HOLDS: CREATE's new order on a Magazzino-only duplicate ends Promo-only, nothing refused; a deactivation KEEPS units; `includeInactive` returns an inactive level; a write to an inactive level is accepted; the `available` 0 activation of an inactive level holding units DROPS them (3 → 0); the old 0/0 body is refused on an absent level too (input validation); a key re-sent when `changeFromQuantity` matches again answers the cached result, not applied; D1 answers `imageJob: null` and the duplicate references the source's own image files (F7 tolerates a null job), a delete removes only the reference. Logs: `~/.claude/racoon-bus/msg/f73-live-<RUN_ID>/`.

**Next**:
- [ ] Not granted, each its own gate: F7.4's baseline («Inizializza») — until then every decrement pass stops at `BaselineMissing` and no CREATE runs; the suspended cron's deletion and the `GSPREAD_*` / Google env vars' removal (SERVER / Ale).
- [ ] Deferred to the next code change and its brief (wording, none blocking): the `imageJob` text (`ops.py:105`, `:130-136`; `outlet_service.py:555-556`; `tests/fake_shop.py:26`, `:332`; `test_outlet_service.py:232`'s comment); `ops.py`'s module docstring ~:20-23 (R053 refused `available` + `onHand` together, not an active level); the runner's `runner.py:353` re-run hint (no `--list fixes`) and S4.7's `if live:`; list 2 §5 "Writes at `M`" (S1.1 creates `A`'s `M` levels at 0); a later list's `A` borrows no live product's files.

**Active constraints & decisions**:
- No write to `racoon`, Shopify, Make, production Trello, the Scheduler or Render outside a recorded gate; this lane runs nothing live. Integration tests on `racoon_staging` only (`scansia_mgr_stg` / `scansia_fixture_stg`, option files `~/.config/racoon-scansia/<user>.cred`, read by the driver only — never opened, never printed).
- Render `scansia-manager` deploys **`main` on commit** (`IT/SERVER/infra-inventory.md:47`): every push to `main` is a deploy — `GATE:push`, except a docs-only diff (`IT/session-protocol.md` §7). `render.yaml` is the suspended `Sync-Scansia` cron: it stays until SERVER / Ale delete the cron.
- Branches: `origin/main` = `1a161cc`; this TRACKER commit sits on local `main` only (docs, not pushed). Local only: `f73-live-runner` `dc06b32` (list 1), `f73-live-check` `602823f` (list 2; runner `--list f73|fixes` required, pinned to `bbc1cfb`'s `ops.py` — any later `ops.py` change makes it refuse to start). `fix/f7-create-order`, `fix/f7-outlet-media`, `feat/scansia-manager` are contained in `main`.
- E18: `PROMO_LOCATION_ID` may be the GID or the numeric id; every writing pass refuses (503 `promo_anchor_invalid`) unless it names the location called "Promo". D28: staging has its own app user; `scansia_mgr` is `racoon` only. D33: the N-0 release waits while an exact-size Promo level has `committed > 0`.
- One connect retry (`mysql_store.CONNECT_ATTEMPTS`): from the Mac a connect to `78.46.244.227:3306` fails now and then (errno 2003, OS 60).
- No personal data in the repo (ids, SKUs, counts only). DRY-RUN stays the default (project `CLAUDE.md` rule 2).
- Working tree: the uncommitted `.claude/settings.json` (sha `638e2594…`) is not this programme's — left as found, out of every commit.

**Tried & discarded (still relevant)**:
- The sheet as store (Q-persistenza, 2026-07-06): voided by the F6 cutover — the register is MySQL.
- Level-setting sweeps ("set Promo to the shelf count"): they put back units sold during an intake stall (plan §3.3).
- A Shopify-only price revert: the next publish re-sends the register's price on every variant, undoing it.
- Bypassing `mutate-verify.sh`'s thermal refusal: a throttled run forges kills. Under other lanes' load even the full suite's baseline throttles the Mac below 100 before the first mutant (D49: 23 aborts) — narrow `--suite` to the modules that kill the queue (a kill by a subset is one by the whole suite) and pre-check each mutant on the full suite.

**Key refs**: plan `docs/f7-mysql-adapter-plan.md`; `docs/f7-coverage-map.md` (gate-2's checklist; §14 = D49); `docs/f7-build-notes.md`; receipts `docs/gate-2-log.md` (47 KB, over the 40 KB ceiling — the cut is Ale's); gate-2 tools `~/.claude/racoon-bus/msg/sync-scansia-gate2/` (D49's: `mut_f73fix.py`, `queue_f73fix.tsv`, `drive_f73fix.sh` — the suite its 4th argument); `docs/testing.md` (the venv lives in a session scratchpad and a reboot empties it — rebuild it there from `/usr/local/bin/python3`, 3.13); F1 history: `TRACKER-ARCHIVE.md`.

## Archive
Full history: `TRACKER-ARCHIVE.md` (same folder). Keep appending superseded material there — never here.
