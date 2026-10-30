# TRACKER — Scansia Manager (refactor Sync-Scansia → web utility interna)

<!-- RULES (parent CLAUDE.md rule 5 — Current-state protocol):
     - Agents read ONLY "Current state" at session start (Read with limit ~45 lines).
     - REWRITE "Current state" entirely at session end (max 45 lines). Never append to it.
     - Superseded detail goes to TRACKER-ARCHIVE.md (or git log for committed work).
     - Keep "Tried & discarded" entries while still relevant: they prevent re-proposing dead ends. -->

## Current state

Pipeline: shipped · item: F7 — Scansia Manager on MySQL · plan: docs/f7-mysql-adapter-plan.md · gate-1: Ale 2026-10-06 · gate-2: Ale 2026-10-08

**Phase**: the **F1 SKU guard is PUSHED** (2026-10-30) under `GATE:2+push` D62 (Ale, 08:02Z; PROGRAM `73774af`), after `/ship` (steps 0–6 pass: receipts carried, suite 878/28, lint not configured, vermin 3.12 no violation, no lockfile, secret scan 0). `origin/main` = merge **`501cc65`**: its parents are `9f16b9f` + `fix/f7-f1-sku-guard` `3fca954`, and its tree is `3fca954`'s. Pushed `1a161cc..501cc65` (18 commits) at 08:12:26Z (`ls-remote`). `guard-foreign-push` refused 16 commits, and Ale typed `FOREIGN_PUSH_OK` in the lane's pane. **Deploy NOT verified by this lane.** Render deploys `main` on commit, and the F7.4 coordinator `sviluppo-2d` verifies it (`list_deploys`): until its DONE, «pushed» is the only claim. Before F1: F7 live on `1a161cc` (2026-10-09, D51; detail in `TRACKER-ARCHIVE.md`), and the baseline APPLIED 2026-10-29 (D58, `BASELINE` row 222). Programme state: `IT/_scansia-programme/PROGRAM.md`. The plan is the contract (sha `6dadcdd7…`) and is never edited here. Folding in §3.2's narrowing (title AND CONSUME-equal SKU) is the coordinator's job.

**F1 guard (what shipped)**: a title match whose variant SKU is not the pair's as CONSUME compares them (`SKU = p_sku`, `utf8mb4_bin` PAD SPACE: code points, only spaces pad), or is blank (D59), is not the key's variant (`variants.foreign_sku`); a blank pair SKU never matches. On such a variant the publish withholds the size (pending, surfaced), a revive zeroes it whatever its own SKU's shelf holds (D1), a CREATE aborts (`create_aborted:size_withheld`) on a returned size its duplicate cannot raise, the baseline holds the key, and a re-level refuses it (a CREATE whose source was respelled since the preview is *moved*). Lowerings still read it by title, except a re-level's ACTIVE level-down onto it, refused (D2).
Code `211d7a4` · `de2c5b9` · `924c8a2` · `819b763` · `960ff4f` (+ test-only `131e679`); receipts 16 + 20 + 6 + 8 + 1 PASS (`a271ab3`, `d8b073f`, `aaf0b43`, `11673d4`, `e9bf920`); verify rounds and the independent review in `~/.claude/racoon-bus/msg/f74-f1-verify/`; map `docs/f7-coverage-map.md` §15; design, residuals and payload `DONE.md` in `~/.claude/racoon-bus/msg/sync-scansia-f1-guard/`.

**GATE-2 re-grade of the verify fixes (effort max, 2026-10-30): 9.5/10.** For: every finding answered with a test red first; m1 closed on two independent lines, each with its own killed mutant; existing abort / hold paths reused, never a stamp. Against: the re-grade found the absent-size class unpinned (`131e679` pins it). Missing for 10/10: (1) D1's cost unsurfaced — on a mixed-SKU DRAFT outlet, A's revive zeroes units on B's own variant even when they back B's pair; (2) an abort after `zero_all` surfaces `last_active_level_kept` twice (cosmetic); (3) no store read that `productDuplicate` copies SKUs at call time; (4) `app.js` `sizeCell` untested.

**Next**:
- [ ] The coordinator's verification of the deploy on `501cc65` (its DONE). The first F7.4 publish is the coordinator's; F2 (93 SKUs without `PRICE_OUTLET`) is Ale's, not decided (D59).
- [ ] Residual 1 (D2): units already on a foreign-SKU variant of an ACTIVE outlet. This is a separate item, decided after the coordinator's per-publish count. D1's cost is stated in D62's Open rows.
- [ ] Not granted, each its own gate: the suspended cron's deletion and the `GSPREAD_*` / Google env vars' removal (SERVER / Ale).
- [ ] Deferred (runner branch): `runner.py:353`'s re-run hint and S4.7's `if live:`; list 2 §5 "Writes at `M`"; a later list's `A` borrows no live product's files.

**Active constraints & decisions**:
- No write to `racoon`, Shopify, Make, production Trello, the Scheduler or Render outside a recorded gate; this lane runs nothing live. Integration tests on `racoon_staging` only (option files `~/.config/racoon-scansia/<user>.cred`, read by the driver only — never opened, never printed).
- Render `scansia-manager` deploys **`main` on commit** (`IT/SERVER/infra-inventory.md:47`): every push to `main` is a deploy — `GATE:push`, except a docs-only diff (`IT/session-protocol.md` §7). `render.yaml` is the suspended `Sync-Scansia` cron: it stays until SERVER / Ale delete the cron.
- Branches: `origin/main` = `501cc65`; local `main` carries this TRACKER's docs-only commit on top, not pushed. `fix/f7-f1-sku-guard`, `fix/f7-create-order`, `fix/f7-outlet-media`, `feat/scansia-manager` are contained in `main`. Local only: `f73-live-runner` `dc06b32`, `f73-live-check` `602823f` — their runner is pinned to `bbc1cfb`'s `ops.py`, so it refuses to start on `main` (F1 changed `ops.py` docstrings).
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

**Key refs**: plan `docs/f7-mysql-adapter-plan.md`; `docs/f7-coverage-map.md` (gate-2's checklist; §15 = F1); `docs/f7-build-notes.md`; receipts `docs/gate-2-log.md` (over the 40 KB ceiling — the cut is Ale's); gate-2 tools `~/.claude/racoon-bus/msg/sync-scansia-gate2/` (`drive_f1.sh` takes the suite as its 4th argument; run it under `/usr/bin/lockf -k /tmp/stockmgr-suite.lock`; mutant files `mut_f1*.py` are never edited once a row cites them); `docs/testing.md` (the venv lives in a session scratchpad: rebuild it from `/usr/local/bin/python3`, 3.13); history: `TRACKER-ARCHIVE.md`.

## Archive
Full history: `TRACKER-ARCHIVE.md` (same folder). Keep appending superseded material there — never here.
