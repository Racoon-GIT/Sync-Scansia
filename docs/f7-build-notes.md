# F7.2 build notes — the lane's mechanisms and facts (Sync-Scansia)

Companion to `docs/f7-mysql-adapter-plan.md` (the contract — never edited here). This file records what the plan
leaves to the lane ("the lane's", "proposed") as the lane fixes it, and facts read during the build. A note here
that contradicts the plan is a defect: it goes to the orchestrator (`scansia-f6`), not into code.

## Storage (F7.1, SERVER's DDL: `IT/SERVER/_deliverables/scansia-f7-2026-10.{md,sql}`)

- `sm_outlet_audit` — SERVER's A08/B01 as written: names from plan §3.6, types SERVER's (`KIND VARCHAR(16)`,
  `STATUS VARCHAR(12)`, `IDEMPOTENCY_KEY VARCHAR(255)`, `PLAN_HASH CHAR(64)`, `INTENT_ID CHAR(32)` unique,
  `PAYLOAD`/`OUTCOME` JSON). Indexes: `(KIND, STATUS, ID)`, `(PRODUCT_ID, ID)`, `GROUP_ID`, `CREATED_AT`.
  A row is immutable after its INSERT except `SENT_AT, SENT_AVAILABLE, REQUEST, PRODUCT_ID, STEP, STATUS, OUTCOME,
  CLOSED_AT` (the grant). So everything a row needs later that never changes — event ids, keys served, variant /
  inventory item / location, Δ or share, the write's kind, the group's planned row count — goes into `PAYLOAD` at
  the write-ahead.
- `sm_outlet_lease` (D29, the lane's choice under §3.6): one row `ID = 1`, seeded by the DDL; app grant SELECT +
  UPDATE on `HOLDER, GENERATION, EXPIRES_AT, WAITER, WAITER_EXPIRES_AT, UPDATED_AT`; no INSERT / DELETE. Statements,
  all `WHERE ID = 1`, `now` = `UTC_TIMESTAMP(3)` (the server's clock: no client clock in a fence):
  - **take** (first start, take-over, re-take): `SET HOLDER = h, GENERATION = LAST_INSERT_ID(GENERATION + 1),
    EXPIRES_AT = now + ttl, WAITER = IF(WAITER = h, NULL, WAITER), UPDATED_AT = now WHERE (HOLDER IS NULL OR
    EXPIRES_AT < now) AND (WAITER IS NULL OR WAITER_EXPIRES_AT < now OR WAITER = h)` — one guarded statement, never
    check-then-insert; the new generation is `LAST_INSERT_ID()` of the same connection (no SELECT inside a write).
  - **renew** (opens every F7 write transaction, §3.3 rule 6): `SET EXPIRES_AT = now + ttl, UPDATED_AT = now WHERE
    HOLDER = h AND GENERATION = g AND EXPIRES_AT > now` — matched count (CLIENT_FOUND_ROWS) 1, else ROLLBACK, stop.
  - **release**: `SET HOLDER = NULL, EXPIRES_AT = NULL, UPDATED_AT = now WHERE HOLDER = h AND GENERATION = g`.
  - **wait** (the decrement only — the hand-off): `SET WAITER = h, WAITER_EXPIRES_AT = now + w, UPDATED_AT = now
    WHERE WAITER IS NULL OR WAITER_EXPIRES_AT < now OR WAITER = h`.
  - "Another holder held the lease in between" (rule 6: recovery + held-key sweep again on a re-take): the re-take's
    generation ≠ this holder's previous generation + 1.
- Users (D28, D29): `scansia_mgr` on `racoon` only; `scansia_mgr_stg` on `racoon_staging` only;
  `scansia_fixture_stg` (staging DML + EXECUTE on the two staging SPs) writes the integration tests' fixtures and
  calls `SP_SCANSIA_CONSUME` for the race test. Credentials: local 600 files SERVER names — read at runtime by the
  test fixture, never printed, never opened with a tool. The fixture refuses to run unless `DATABASE()` is
  `racoon_staging`. Server: `78.46.244.227:3306`, TLS `VERIFY_IDENTITY` against `IT/SERVER/racoon-internal-ca.pem`.

## Parameters the plan proposes, fixed here (each one tested)

- Re-send window margin 1 h (§3.3 rule 2): the window is the first `SENT_AT` + 23 h.
- Session `wait_timeout` 60 s, `innodb_lock_wait_timeout` 5 s (§3.7 rules 4–5).
- `GROUP_ID` = `LEASE_GEN × 10^6 + n` (the n-th group of that take): unique with no sequence table; a
  `SUPERSEDED` row's successor inherits its group's id (§3.3 rule 2); a remainder is a new group of its pass.
- Lease TTL and the local-deadline margin: fixed at §4.3 — the expiry exceeds the longest single Shopify call with
  its retries (rule 6), measured on `transport.py`'s timeouts.
- Admin API pin: the newest stable version on the build day (`2026-10` per E15) — re-read
  shopify.dev/docs/api/usage/versioning on the day it is pinned.

## Facts read during the build (source, date)

- `sc_returned_pair_event.PAIR_ID` is `INT UNSIGNED NULL` (SERVER's A04 = STOCK_Manager's DDL at `92f6f90`,
  2026-10-06): an event with no pair has no (SKU, SIZE) key. Open: which event types can carry it NULL (design
  §2.2) — until read, `pending_deltas()` surfaces such an event and never stamps it.
- Render `scansia-manager` deploys `main` on commit (`scansia-f6`, 2026-10-06 14:23Z): this branch deploys nothing;
  `main` is production and stays out of F7.2.
