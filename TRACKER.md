# TRACKER — Scansia Manager (refactor Sync-Scansia → web utility interna)

<!-- RULES (parent CLAUDE.md rule 5 — Current-state protocol):
     - Agents read ONLY "Current state" at session start (Read with limit ~45 lines).
     - REWRITE "Current state" entirely at session end (max 45 lines). Never append to it.
     - Superseded detail goes to TRACKER-ARCHIVE.md (or git log for committed work).
     - Keep "Tried & discarded" entries while still relevant: they prevent re-proposing dead ends. -->

## Current state

Pipeline: gate-2 · item: F7 — Scansia Manager on MySQL · plan: docs/f7-mysql-adapter-plan.md · gate-1: Ale 2026-10-06 · gate-2: Ale 2026-10-08

**Phase**: F7.3, the go-live of F7.2 — `feat/scansia-manager` (`7b2c18d`) merged into `main` as **`ff0894f`** (a merge commit, parents `00c7682` + `7b2c18d`; `TRACKER.md` the only conflict, the branch's side kept; its tree is `7b2c18d`'s, and `1356027..7b2c18d` touches `TRACKER.md` + `docs/` only: the code is gate-2's). D42's one push to `main` (`GATE:push`, Ale 2026-10-09) carries `ff0894f` and this TRACKER commit, after SERVER set Render's env vars (the orchestrator's INFO 2026-10-09: SERVER `8473b21`, re-read by it; the keys' own oracle is the first boot): **pushed = go-live** (Render `scansia-manager` deploys `main` on commit); **deploy checks by the orchestrator** `scansia-f6`. Programme "Scansia → MySQL": state `IT/_scansia-programme/PROGRAM.md`, mandate `briefs/sync-scansia-F7.3.md`. The plan is the contract (sha `6dadcdd7…`, `71dfc51`): never edited here — a statement that looks wrong goes to the orchestrator. Register: `racoon.sc_returned_pair` + `sc_returned_pair_event` (owner STOCK_Manager) since the F6 cutover (2026-09-21); `Scarpe_in_Scansia` frozen, read by no code. Suite on `ff0894f` (2026-10-09, venv on Python 3.13): 795 passed, 28 skipped — all 28 the staging opt-in; Render runs 3.12.4 (`runtime.txt`): `vermin -t=3.12-` on `backend/` + `main.py`, no violation (minimum 3.9).

**Next**:
- [x] F7.2 — the engine and every §3 vertical on the register, the sheet removed (`7d6f001`); gate-2 D41 on `1356027` (receipts `docs/gate-2-log.md`; GATE-2 self-grade 10/10, its reasons in the TRACKER at `1356027`).
- [x] F7.3 — the merge `ff0894f` and the suite on it (above). The stage's GATE-2 prompt, run on this context's work: no code written here — the merge's tree is `7b2c18d`'s, so gate-2's 10/10 stands; the merge verified by tree hash, parents and suite. Residual, not closable under D42: runtime parity with 3.12.4 is static only (no 3.12 on the Mac) — Render's first boot is its oracle.
- [ ] The orchestrator's deploy checks on D42's push: `/health` 200; `GET /outlet/decrement/status` → `last.error` = `BaselineMissing` (the boot pass reached Shopify's Promo location and MySQL, wrote nothing to Shopify); `GET /scansia` served from MySQL. Then `Pipeline: shipped` (a docs-only commit) — not before they pass.
- [ ] Not granted by D42, each its own gate: the live write tests (`f7-plan.md:1024`, Ale's `confermo`); F7.4's baseline ("Inizializza"); the suspended cron's deletion and the `GSPREAD_*` / Google env vars' removal (SERVER / Ale).

**Active constraints & decisions**:
- No write to `racoon`, Shopify, Make, production Trello, the Scheduler or Render outside a recorded gate; the live write tests need Ale's `confermo`. Integration tests on `racoon_staging` only (`scansia_mgr_stg` / `scansia_fixture_stg`, option files `~/.config/racoon-scansia/<user>.cred`, read by the driver only — never opened, never printed).
- Render `scansia-manager` deploys **`main` on commit** (`IT/SERVER/infra-inventory.md:47`): every push to `main` is a deploy — `GATE:push`, except a docs-only diff (`IT/session-protocol.md` §7). `render.yaml` is the suspended `Sync-Scansia` cron: it stays until SERVER / Ale delete the cron.
- Branches: `main` holds the whole F7 (`ff0894f`); the local `feat/scansia-manager` (`7b2c18d`) is 27 ahead of `origin/feat/scansia-manager` (`e3c2bff`) and not pushed (D42: one push, `main` only) — contained in `main`, no work left on it.
- E18: `PROMO_LOCATION_ID` may be the GID or the numeric id; every writing pass refuses (503 `promo_anchor_invalid`) unless it names the location called "Promo". D28: staging has its own app user; `scansia_mgr` is `racoon` only. D33: the N-0 release waits while an exact-size Promo level has `committed > 0`.
- One connect retry (`mysql_store.CONNECT_ATTEMPTS`): from the Mac a connect to `78.46.244.227:3306` fails now and then (errno 2003, OS 60); the Render → VPS path is measured at the first boot.
- No personal data in the repo (ids, SKUs, counts only). DRY-RUN stays the default (project `CLAUDE.md` rule 2).
- Working tree: the uncommitted `.claude/settings.json` (sha `638e2594…`) is not this programme's — left as found (carried across the switch to `main`: same committed blob on both sides, sha and mtime unchanged).

**Reads made in F7.3**: none on `racoon`, `racoon_staging` or Shopify.

**Tried & discarded (still relevant)**:
- The sheet as store (Q-persistenza, 2026-07-06): voided by the F6 cutover — the register is MySQL.
- Level-setting sweeps ("set Promo to the shelf count"): they put back units sold during an intake stall (plan §3.3).
- A Shopify-only price revert: the next publish re-sends the register's price on every variant, undoing it.
- Bypassing `mutate-verify.sh`'s thermal refusal: a throttled run forges kills — the Mac drops to `CPU_Speed_Limit` 81–97 under the suite; `drive.sh` waits for 100 and stops after 40 attempts (exit 9: resume at the `--start` its last log names).

**Key refs**: plan `docs/f7-mysql-adapter-plan.md`; `docs/f7-coverage-map.md` (gate-2's checklist; §12 = set V's survivors); `docs/f7-build-notes.md`; receipts `docs/gate-2-log.md`; gate-2 tools `~/.claude/racoon-bus/msg/sync-scansia-gate2/` (`mut.py`, `build_queue.py`, `drive.sh`, `author_mutants.py`, `verifier-mandate.txt`, `verifier_mutants.jsonl`, the `run-*` logs); `docs/testing.md` (the venv lives in a session scratchpad and a reboot empties it — rebuild it there from `/usr/local/bin/python3`, 3.13, then point `drive.sh`'s `SUITE` at it); F1 history: `TRACKER-ARCHIVE.md`.

## Archive
Full history: `TRACKER-ARCHIVE.md` (same folder). Keep appending superseded material there — never here.
