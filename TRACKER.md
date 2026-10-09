# TRACKER — Scansia Manager (refactor Sync-Scansia → web utility interna)

<!-- RULES (parent CLAUDE.md rule 5 — Current-state protocol):
     - Agents read ONLY "Current state" at session start (Read with limit ~45 lines).
     - REWRITE "Current state" entirely at session end (max 45 lines). Never append to it.
     - Superseded detail goes to TRACKER-ARCHIVE.md (or git log for committed work).
     - Keep "Tried & discarded" entries while still relevant: they prevent re-proposing dead ends. -->

## Current state

Pipeline: gate-2 · item: F7 — Scansia Manager on MySQL · plan: docs/f7-mysql-adapter-plan.md · gate-1: Ale 2026-10-06 · gate-2: Ale 2026-10-08

**Phase**: F7.2 (build) closed — gate-2 D41 (recorded in `9b48d0b`), the lane's DONE sent 2026-10-08 00:51Z. Next: **F7.3, the go-live** — `GATE:push` D42 (2026-10-09), this lane's fresh context D43, mandate `IT/_scansia-programme/briefs/sync-scansia-F7.3.md`. Programme "Scansia → MySQL": orchestrator `scansia-f6`, state `IT/_scansia-programme/PROGRAM.md`; gate-1 D25 (`gate1-receipts.log` line 661). The plan is the contract (sha `6dadcdd7…`, `71dfc51`): never edited here — a statement that looks wrong goes to the orchestrator. Register: `racoon.sc_returned_pair` + `sc_returned_pair_event` (owner STOCK_Manager) since the F6 cutover (2026-09-21); `Scarpe_in_Scansia` frozen, read by no code. Suite at `1356027`: 795 offline green (venv) + 28 on `racoon_staging` (opt-in; last staging run 2026-10-06 19:45Z, 28/28 — nothing since touched store code); bare interpreter 664 + 159 skipped (`docs/testing.md`).

**Next**:
- [x] F7.2 §4.1–§4.6 — the engine and every §3 vertical on the register, the sheet removed (`7d6f001`; mechanisms `docs/f7-build-notes.md`); gate-2 receipts in `docs/gate-2-log.md`: set A 15/15 PASS on `7d6f001`, set V 60 PASS + 10 SURVIVOR on `0f18084`, the 10 pinned (`49399fd`, map §12) and re-run with a control, 11/11 PASS on `49399fd`. GATE-2 self-grade 9 → 10/10 (its reasons: TRACKER at `1356027`).
- [ ] F7.3 in D42's order: SERVER sets Render's env vars first — the lane waits for the orchestrator's word; then the lane merges `feat/scansia-manager` into `main` (a merge commit; `TRACKER.md` the only conflict, keep this branch's side) and pushes it, the orchestrator typing `FOREIGN_PUSH_OK` in this pane; the orchestrator checks the deploy. Not granted by D42: the live write tests (Ale's `confermo`), F7.4's baseline, the cron's and the `GSPREAD_*` env vars' removal.

**Active constraints & decisions**:
- No write to `racoon`, Shopify, Make, production Trello, the Scheduler or Render outside a recorded gate: D42 grants F7.3's merge + push only, after SERVER's env vars; the live write tests need Ale's `confermo`. Integration tests on `racoon_staging` only (`scansia_mgr_stg` / `scansia_fixture_stg`, option files `~/.config/racoon-scansia/<user>.cred`, read by the driver only — never opened, never printed).
- Render `scansia-manager` deploys **`main` on commit** (`IT/SERVER/infra-inventory.md:47`) — `main` moves only in D42's order, never before SERVER's env vars are confirmed. The push carries earlier contexts' commits (`guard-foreign-push` needs `FOREIGN_PUSH_OK` typed in this pane — D42: the orchestrator types it). `origin/main` = `00c7682` (TRACKER-only) is not in this branch: a merge conflicts on `TRACKER.md` — keep this branch's side. A merge carrying `gate-2-log.md` receipts is a merge commit, never a rebase or squash (the receipts name pre-merge shas).
- Later, SERVER / Ale (not in D42): delete the suspended `Sync-Scansia` cron (`render.yaml` stays until then) and the web service's `GSPREAD_*` / Google env vars — no code reads them. E18: `PROMO_LOCATION_ID` may be the GID or the numeric id; every writing pass refuses (503 `promo_anchor_invalid`) unless it names the location called "Promo".
- D28: staging has its own app user; `scansia_mgr` is `racoon` only. D33: the N-0 release waits while an exact-size Promo level has `committed > 0`.
- One connect retry (`mysql_store.CONNECT_ATTEMPTS`): from the Mac a connect to `78.46.244.227:3306` fails now and then (errno 2003, OS 60); the Render → VPS path is F7.3's to measure.
- No personal data in the repo (ids, SKUs, counts only). DRY-RUN stays the default (project `CLAUDE.md` rule 2).
- Working tree: the uncommitted `.claude/settings.json` (sha `638e2594…`) is not this programme's — left as found; stashed only while `mutate-verify.sh` runs (receipts are `-dirty` otherwise), restored byte-identical (backup in the gate-2 folder).

**Reads made in F7.2 (facts for build steps)**: `racoon` none. `racoon_staging` 2026-10-06 (as the staging users): server status and `sm_outlet_lease` column types (probe), the post-run counts of `f7test` rows; facts in `docs/f7-build-notes.md` §Facts.

**Tried & discarded (still relevant)**:
- The sheet as store (Q-persistenza, 2026-07-06): voided by the F6 cutover — the register is MySQL.
- Level-setting sweeps ("set Promo to the shelf count"): they put back units sold during an intake stall (plan §3.3).
- A Shopify-only price revert: the next publish re-sends the register's price on every variant, undoing it.
- Bypassing `mutate-verify.sh`'s thermal refusal: a throttled run forges kills — the Mac drops to `CPU_Speed_Limit` 81–97 under the suite; `drive.sh` waits for 100 and stops after 40 attempts (exit 9: resume at the `--start` its last log names).

**Key refs**: plan `docs/f7-mysql-adapter-plan.md`; `docs/f7-coverage-map.md` (gate-2's checklist; §12 = set V's survivors); `docs/f7-build-notes.md`; receipts `docs/gate-2-log.md`; gate-2 tools `~/.claude/racoon-bus/msg/sync-scansia-gate2/` (`mut.py`, `build_queue.py`, `drive.sh`, `author_mutants.py`, `verifier-mandate.txt`, `verifier_mutants.jsonl`, the `run-*` logs); `docs/testing.md` (the venv lives in a session scratchpad and a reboot empties it — rebuild it there, then point `drive.sh`'s `SUITE` at it); F1 history: `TRACKER-ARCHIVE.md`.

## Archive
Full history: `TRACKER-ARCHIVE.md` (same folder). Keep appending superseded material there — never here.
