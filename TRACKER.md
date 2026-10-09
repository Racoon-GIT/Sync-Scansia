# TRACKER — Scansia Manager (refactor Sync-Scansia → web utility interna)

<!-- RULES (parent CLAUDE.md rule 5 — Current-state protocol):
     - Agents read ONLY "Current state" at session start (Read with limit ~45 lines).
     - REWRITE "Current state" entirely at session end (max 45 lines). Never append to it.
     - Superseded detail goes to TRACKER-ARCHIVE.md (or git log for committed work).
     - Keep "Tried & discarded" entries while still relevant: they prevent re-proposing dead ends. -->

## Current state

Pipeline: shipped · item: F7 — Scansia Manager on MySQL · plan: docs/f7-mysql-adapter-plan.md · gate-1: Ale 2026-10-06 · gate-2: Ale 2026-10-08

**Phase**: F7 **live** on `619e6ae` (D42, 2026-10-09; Render `dep-db4amps9v7es73af6qgg`; every pass stops at `BaselineMissing` until F7.4). F7.3's live run `202610091404` (D45, runner `f73-live-runner` @ `dc06b32`, log `~/.claude/racoon-bus/msg/f73-live-202610091404/`): COMPLETED, residue empty; the orchestrator judged its five FAILs (`IT/_scansia-programme/PROGRAM.md`, 2026-10-09): S3.4, S3.5, S2.10, S2.14 recorded facts; **S3.8 a defect** — the store refuses to deactivate an item's last active level (R061) and CREATE deactivated every non-Promo level before any Promo activation, so all 70 CREATE candidates (927 of their 932 variants active only at Magazzino) would abort. The plan is the contract (sha `6dadcdd7…`): never edited here — its E15 / rule 2 "explicit 0/0" activation is wrong at the pin (R053), the orchestrator folds it.

**D49 (context `57d770c7`, 2026-10-09): F7.3's tail WRITTEN — nothing pushed, nothing run live:**
- `fix/f7-create-order` (from `fix/f7-outlet-media` `3352fcb`: one push carries F3 and these): `e2105bc` code + tests, `bbc1cfb` docs (`docs/f7-coverage-map.md` §14, build-note facts), `2e13fb9` gate-2 receipts (10 PASS on `bbc1cfb`), this TRACKER. CREATE activates Promo on every variant lacking it first (stops at the first not `APPLIED`), re-reads, requires every variant Promo-active, then FIX1's order composed from the re-read; `flows.zero_rows_for` never plans the deactivation of a variant's last active level (zeroed, kept, surfaced `last_active_level_kept`) — CREATE and the orphan alike; activation body `available` 0 only (R053, the orchestrator's decision); a publish answered without `publishedOnPublication` true is surfaced (`publish_unconfirmed`) and recorded on its row, no abort (S2.10). Suite 828 passed, 28 skipped (venv 3.13); `vermin -t=3.12-` clean.
- `f73-live-check` (from `dc06b32`; `fix/f7-create-order` merged at `b4133aa`): the list `docs/f7.3-fixes-live-check.md` (`56169a0`, sha `a12bf704…`) — C1 F3, C2 CREATE's phase on a Magazzino-only duplicate, C3 E15, C4 the old body on an absent level, C5 S3.4's open case; Magazzino at 0 throughout (the orchestrator's decision, `gid://shopify/Location/8251572336`); 17–19 mutations, 63–125 calls. Its runner (`--list fixes`, now required) pinned to `bbc1cfb`'s files, `602823f`; worktree suite 1174 passed, 28 skipped; offline run on the fake COMPLETED (17 W / 68 calls).
- Delivery: `~/.claude/racoon-bus/msg/20261009-1858-sync-scansia-scansia-f73-fixes.md`.

**Next**:
- [ ] The orchestrator reviews D49's delivery, then asks Ale one `GATE:bulk` (the live check, branch `f73-live-check`, run by `shopify-operator`) + `GATE:push` (`fix/f7-create-order` into `main`: the merge conflicts on `TRACKER.md` only). F7.4's first CREATE waits for both.
- [ ] Not granted, each its own gate: F7.4's baseline ("Inizializza"); the suspended cron's deletion and the `GSPREAD_*` / Google env vars' removal (SERVER / Ale).
- [ ] Open, the list's §8 note 1: an activation of an INACTIVE Promo level holding units followed by an add (ACTIVE branch, revive) puts those units on sale if the store keeps them — C3 (S4.6) measures it; 0 such levels on the store today (the orchestrator's read).

**Active constraints & decisions**:
- No write to `racoon`, Shopify, Make, production Trello, the Scheduler or Render outside a recorded gate; this lane runs nothing live. Integration tests on `racoon_staging` only (`scansia_mgr_stg` / `scansia_fixture_stg`, option files `~/.config/racoon-scansia/<user>.cred`, read by the driver only — never opened, never printed).
- Render `scansia-manager` deploys **`main` on commit** (`IT/SERVER/infra-inventory.md:47`): every push to `main` is a deploy — `GATE:push`, except a docs-only diff (`IT/session-protocol.md` §7). `render.yaml` is the suspended `Sync-Scansia` cron: it stays until SERVER / Ale delete the cron.
- Branches, all local, none pushed: `main` `538c234` (docs only on `origin/main` `619e6ae`; untouched by D49, so its TRACKER is older than this one); `fix/f7-outlet-media` `3352fcb` (contained here); `fix/f7-create-order` (this); `f73-live-runner` `dc06b32` (list 1, run); `f73-live-check` `602823f`. `feat/scansia-manager` is contained in `main`.
- E18: `PROMO_LOCATION_ID` may be the GID or the numeric id; every writing pass refuses (503 `promo_anchor_invalid`) unless it names the location called "Promo". D28: staging has its own app user; `scansia_mgr` is `racoon` only. D33: the N-0 release waits while an exact-size Promo level has `committed > 0`.
- One connect retry (`mysql_store.CONNECT_ATTEMPTS`): from the Mac a connect to `78.46.244.227:3306` fails now and then (errno 2003, OS 60).
- No personal data in the repo (ids, SKUs, counts only). DRY-RUN stays the default (project `CLAUDE.md` rule 2).
- Working tree: the uncommitted `.claude/settings.json` (sha `638e2594…`) is not this programme's — left as found; D49's gate-2 driver put the committed blob in place around each run and restored it byte for byte (sha re-read).

**Reads made in D49**: none on `racoon`, `racoon_staging` or Shopify; shopify.dev `2026-10` `inventoryActivate.md` (public docs).

**Tried & discarded (still relevant)**:
- The sheet as store (Q-persistenza, 2026-07-06): voided by the F6 cutover — the register is MySQL.
- Level-setting sweeps ("set Promo to the shelf count"): they put back units sold during an intake stall (plan §3.3).
- A Shopify-only price revert: the next publish re-sends the register's price on every variant, undoing it.
- Bypassing `mutate-verify.sh`'s thermal refusal: a throttled run forges kills. Under other lanes' load even the full suite's baseline throttles the Mac below 100 before the first mutant (D49: 23 aborts) — narrow `--suite` to the modules that kill the queue (a kill by a subset is one by the whole suite) and pre-check each mutant on the full suite.

**Key refs**: plan `docs/f7-mysql-adapter-plan.md`; `docs/f7-coverage-map.md` (gate-2's checklist; §14 = D49); `docs/f7-build-notes.md`; receipts `docs/gate-2-log.md`; gate-2 tools `~/.claude/racoon-bus/msg/sync-scansia-gate2/` (D49's: `mut_f73fix.py`, `queue_f73fix.tsv`, `drive_f73fix.sh` — the suite its 4th argument); `docs/testing.md` (the venv lives in a session scratchpad and a reboot empties it — rebuild it there from `/usr/local/bin/python3`, 3.13); F1 history: `TRACKER-ARCHIVE.md`.

## Archive
Full history: `TRACKER-ARCHIVE.md` (same folder). Keep appending superseded material there — never here.
