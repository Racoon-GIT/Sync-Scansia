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

## Code layering (§4.2, the lane's)

- `RegisterPort` (§3.1's operations) is implemented ONCE, by `backend/persistence/register.py` `Register`, over a
  small `RegisterStore` (`ports.py`): plain non-locking reads, the three lease statements, and ONE write primitive
  `fenced_write(fence, ops)` — the guarded renew, then ops from a closed algebra (`records.py`: `InsertAudit`,
  `UpdateAudit`, `StampEvents`, `UpdatePairs`), at most one register op and it is last (§3.7 rule 1), every guard
  counting rows MATCHED (`GuardMismatch` rolls the whole transaction back). Two stores: `memory_store.py` (the test
  fake, MySQL semantics: atomic transactions, server clock, JSON keys re-ordered as MySQL does) and the MySQL one
  (§4.3); `tests/test_register_store.py` is their shared contract.
- Holds are derived from audit rows, never stored: the whole-register `BASELINE` row's `PAYLOAD.held`, `RECONCILE`
  rows `HELD`, `HOLD` rows, and every `RECONCILE` row `OPEN`/`APPLIED` (the in-flight hold). A per-key `BASELINE`
  row releases that key's baseline / recovery / `HOLD` holds with a LOWER row id, never an in-flight one. Holds a
  re-level's confirm makes are `HOLD` rows; `BASELINE` rows only define (whole-register) or release (per key).
- `PAYLOAD` conventions: `keys` [[SKU, SIZE], …] and `event_ids` on `RECONCILE` / `HOLD` rows; `scope`
  (`register` | `key`), `key`, `held` [{key, reason, event_ids}] on `BASELINE` rows. A stamp is always carried by
  an audit row in its transaction: a new `RECONCILE` row (a stamp with no Shopify write: `STATUS DONE` at insert)
  or the close of `APPLIED` rows; the N-0 release writes its per-key `BASELINE` row even on a key not held.
- The lease client (`backend/services/lease.py`) reads the renew's START on the holder's own monotonic clock.
- MySQL store (`mysql_store.py`, §4.3): env `SCANSIA_DB_HOST`, `SCANSIA_DB_PORT` (3306), `SCANSIA_DB_USER`,
  `SCANSIA_DB_PASSWORD`, `SCANSIA_DB_NAME`, CA as `SCANSIA_DB_CA_PATH` or `SCANSIA_DB_CA_B64` (written once to a
  0600 temp file) — F7.3 sets them on Render. Driver `mysql-connector-python==9.6.0` (the workspace pin). A handle
  idle > 45 s re-opens its session before the next statement (session `wait_timeout` 60 s; a job can sit longer in
  Shopify calls); a connection lost before `COMMIT` rolled back → the fenced transaction is retried once on a fresh
  session; lost DURING `COMMIT` → `CommitUnknown`, never retried (the caller re-reads); a take whose answer was lost
  is read back (a row naming the holder is the take). Driver errors surface as type + errno, never their text.
- Audit sink (`audit_sink.py`): every write fenced under the writer's pass lease (bound at construction or
  `bind()`; refused unbound); `PRICE` capture rows keyed by a fresh `uuid4().hex` `INTENT_ID`.
- Staging tests (`tests/register_fixtures.py` `MySQLBackend`): opt-in `SCANSIA_STAGING=1`; app side
  `scansia_mgr_stg`, other writers' DML and `SP_SCANSIA_CONSUME` as `scansia_fixture_stg`, both through the option
  files SERVER wrote (`~/.config/racoon-scansia/<user>.cred`, read by the driver only); refuses any schema but
  `racoon_staging`; its rows are marked (`f7test:` keys, order ids `73570…`, audit actor `f7test`) and deleted at
  each start — SERVER's A10 fixtures are left alone. Liveness margins ×5 on MySQL (`scale`: WAN round trips).
  `tests/test_consume_beside_f7.py` (staging only) holds F7's COMMIT back 3 s through a connection tap
  (`register_fixtures.Tap`) while `SP_SCANSIA_CONSUME` runs from the fixture's session, and EXPLAINs the exact
  statements the store sent.
- Baseline reads (§3.4): `read_snapshot(key=None)` is the one stated read transaction (`START TRANSACTION READ
  ONLY WITH CONSISTENT SNAPSHOT`, three plain SELECTs, `COMMIT`): shelf pairs, every unstamped event, each key's
  max event id over all its events; `read_import_payloads(pair_ids)` returns each pair's `IMPORTED` `PAYLOAD`
  (B-2's J seed at `raw.J.v`, E14) — outside the snapshot, safely: events are append-only
  (`STOCK_Manager/docs/scansia-design.md` §2.2).

## The reconciliation engine (§4.4, the lane's mechanism)

- `services/ledger.py` — one `RECONCILE` row per Shopify write. `ACTION`: `adjust` (relative `available`
  write; `PAYLOAD.purpose` `add` / `lower` carry a signed `share` of the group's Δ; `zero`, `excess`, `custom`,
  `orphan`, `nonpromo`, `relevel` carry an absolute `target` and compute the delta at the send from the live
  read), `activate` (explicit 0/0, only on an absent or inactive level), `deactivate` (no key), the state rows
  `create`, `revive-tail`, `normalise` (`SENT_AT` + `STEP` before every call, `STEP` `<step>:ok` after), and
  `stamp-only` (`DONE` at insert). First send: write-ahead (group) → rule 8 read (`ops.read_level`,
  `includeInactive`) → premise check (else `FAILED` never sent) → `SENT_AT` + `SENT_AVAILABLE` + `REQUEST`
  → send → outcome. Settle: all non-`SUPERSEDED` rows `APPLIED` → stamp (distinct union of the rows' event ids)
  + `DONE`; beside `FAILED` / `HELD` → `APPLIED` rows `CARRIED` (`OUTCOME.moved` = what they moved).
- Interpretation recorded: terminal path (ii) on a row never sent closes it `FAILED` "never sent", whatever its
  kind (nothing sent → nothing landed; (ii)'s `HELD` protects a landed +Δ, which a never-sent row cannot be).
- `referenceDocumentUri` of every inventory write: `gid://scansia-manager/OutletReconcile/<row id>` (Shopify's
  inventory history names the row). Reason `correction`.
- Transport (rule 9): a mutation is re-sent only after HTTP 429 or a `THROTTLED` top-level error (refused
  before execution — shopify.dev GraphQL Admin rate limits); timeout / connection error / 5xx / unparseable /
  `INTERNAL_SERVER_ERROR` / data beside errors → `ShopifyOutcomeUnknown`; other 4xx and top-level errors →
  `ShopifyRefused` (nothing ran). Throttle waits capped at 10 s.
- Admin API pin `2026-10` (`config.PINNED_API_VERSION`; `SHOPIFY_API_VERSION` may only restate it, else no boot),
  read 2026-10-06 at shopify.dev/docs/api/usage/versioning ("Latest stable", accessible until 2027-10-16 15:00
  UTC). Migrated at the pin: `productUpdate(product:)`; `Collection.sources` (`smart` = a sub-collections
  source or a conditions source with inclusion conditions); inventory reads with `includeInactive: true`,
  `isActive`, `tracked`, variant `title`. Raw doc pages were read as `.md` (curl), not through a summariser.

## "Inizializza" — the baseline (§3.4, the lane's mechanism)

- `services/baseline_service.py`, Level only (B-1, D20); Parity — the option not taken — is not built. Preview:
  `register.snapshot()` (the one consistent read) and `held_keys()`; per register SKU the resolver (as every pass
  resolves) and one inventory read per outlet; the outlet collection (`enumerate_outlet_products`) for products of
  no register SKU. A collection product carrying a register SKU the resolver did not return makes that SKU unclean
  (anomaly listed, its keys held `unresolved`, the product untouched).
- Keys: shelf keys ∪ keys of unstamped events ∪ real-size variants with Promo > 0 and no pair, no event, on any
  outlet (a stale unit: `lower` to 0, never held — a hold with no event keeps nothing, and the N-0 release never
  sees a key with no event). Decision: `level` (a), `zero` (b), `none` (c), `stamp` (no shelf pair, no variant
  titled SIZE: the events stamped), `hold`. Hold reasons: `blank_sku`; `held_before_baseline:<kinds>` (a key
  already held — in flight, recovery `HELD`, a `HOLD` row — stays held, all its snapshot events; a re-level
  releases it); `unresolved`; `outlet_unreadable` (levels truncated, a quantity unread); `multi_outlet`;
  `size_absent` (+ its twins); `committed_on_upward_adjust`; `unexpected_status:<s>`; `shelf_events_not_n` ((b)
  and (c) leave the shelf pairs' events pending: §3.4's tested invariant needs them to sum to N, else the key is
  held). At confirm: `moved_at_confirm`, `normalise_not_applied`, `level_write_not_applied:<statuses>`,
  `shopify_error:<type>`.
- (a): `n − live` from any level, a negative included (V8); an upward write to an absent or inactive level first
  activates it, explicit 0 / 0 (an inactive level's units dropped: fewer); a lowering never activates.
- Rows: (a) `adjust`/`relevel` target N (+ `activate`); (b) and stale units `adjust`/`zero` target 0;
  customisation-only Promo > 0 `adjust`/`custom` target 0 on any outlet (a negative left); a `normalise` row on
  every ACTIVE outlet with a CONTINUE or untracked variant, before any level write (DRAFT outlets: the revive's
  tail). The baseline's rows carry NO event ids: its stamp is the `BASELINE` row's explicit list, one
  transaction, written last. A zero row closed `FAILED` "nothing_to_lower" (live ≤ 0 at the send) is at target.
- Confirm: `api/init.py` keeps each previewed plan by hash in process memory (4 kept) and the confirm applies THAT
  plan — no re-preview + aggregate hash gate as the other verticals: §3.4 holds a key that moved, it never refuses
  the whole confirm. Per key, right before its writes: `snapshot(key)` (N, max event id, the unstamped id set),
  the SKU's resolution (once per SKU per run), a fresh inventory read of its outlets — any difference holds it. A
  product whose normalisation did not apply holds its keys (checked before the re-read: the refusal may have
  drafted it). The held keys then go through `flows.held_key_lowering` (P > N lowered, active first, their
  post-hold events counted).
- Order: take (bound 120 s; never released mid-run) → whole-register `BASELINE` absent → recovery (publish class)
  → `INIT_BEFORE` through the audit sink (the abort gate) → normalisations → keys → the held keys' lowering →
  `OUTLET_PRODUCT_ID` (§3.1: one non-`review` outlet) → J seeds → `ONLINE` (1 on the shelf pairs of keys levelled
  with N > 0, 0 on every other shelf pair at 1; changed pairs only, one `REGISTER` row per value) → `BASELINE`
  (`rule`, `dlq` {checked_at, typed, count}, `held` [{key, reason, event_ids}], `stamped`, `keyless`, `counts`,
  `PLAN_HASH`) + its stamp. A Shopify failure inside one key holds that key; a store error or a lost lease ends
  the run with no `BASELINE` (re-run from a fresh preview: every write is a target re-read live).
- Part B under Level: the `ONLINE` = 1 shelf pairs classified (kept-online, demote:missing, demote:draft,
  demote:sold-out-size, review:multi-match; exact size) — listed, never written.
- B-2's seed: J = `IMPORTED.PAYLOAD.raw.J` (`{t, v, w}`), parsed by the register's own H/I rule (STOCK_Manager
  `parsePrice`, `src/lib/returned-pair-legacy.ts` at `92f6f90`). Per SKU: already priced → left; an unparseable J,
  or two values → listed, never picked; one value ≤ 0 → listed; one value > 0 → `set_outlet_price` on every shelf
  pair of the SKU (post-cutover returns, which have no J, included). `DISCOUNT` is not seeded (B-2 names J only).
- Catalogue notes for (c) keys with N > 0: the source resolver and its variants → `not_in_catalogue`,
  `multi_source`, `size_absent_on_source` (+ twins) — a person.

## Parameters the plan proposes, fixed here (each one tested)

- The `/dlqs` precondition (§3.4): the typed instant (ISO 8601; no offset → Europe/Rome) and `dlqCount`, which must
  be 0; the instant no later than now + 2 min and no earlier than the snapshot − 15 min (one confirm-token TTL,
  `api/mutations.py`) — checked by the API on the app clock, again by the service on the database clock, kept in
  the `BASELINE` row. The baseline takes the lease within 120 s or refuses.

- Re-send window margin 1 h (§3.3 rule 2): the window is the first `SENT_AT` + 23 h.
- Session `wait_timeout` 60 s, `innodb_lock_wait_timeout` 5 s (§3.7 rules 4–5).
- `GROUP_ID` = `LEASE_GEN × 10^6 + n` (the n-th group of that take): unique with no sequence table; a
  `SUPERSEDED` row's successor inherits its group's id (§3.3 rule 2); a remainder is a new group of its pass.
- Lease (`lease.py`, tested in `tests/test_lease.py`): the longest single Shopify call with its retries, on
  `transport.py`'s constants, is a read — 5 × 30 s timeout + backoffs 1+2+4+8+8 s + 5 × 0.7 s ≈ 177 s (a mutation is
  never re-posted blind, rule 9). TTL 300 s; keepalive renews before a call once 60 s have passed since the last
  renew (every call starts with ≥ 240 s left > 177 s); a send must start within TTL − 60 s of its renew. The
  decrement's waiter flag lives 60 s, refreshed every 2 s poll; it waits at most TTL + 60 s (a crashed holder's
  expiry) before ending its run, surfaced; a releasing holder waits up to 600 s to re-take.
- Customisation-only (rule 7): an option value or variant title containing "personalizzazione", case-folded — the
  three measured spellings (`measures/2026-09-30-f7/c7_join_sku_size.csv:2272-2274`) and any other.
- Exact size (§3.2): `SIZE` == the variant's `title` (what an order sends as `variant_title`); a twin is a title
  equal after case-fold, comma → dot and inner-space removal — §3.2's list, nothing more.
- Admin API pin: the newest stable version on the build day (`2026-10` per E15) — re-read
  shopify.dev/docs/api/usage/versioning on the day it is pinned.

## Facts read during the build (source, date)

- `sc_returned_pair_event.PAIR_ID` is `INT UNSIGNED NULL` (SERVER's A04 = STOCK_Manager's DDL at `92f6f90`,
  2026-10-06): an event with no pair has no (SKU, SIZE) key. Read 2026-10-06: NULL **only** for `CONSUME_MISS`
  (design §2.2 at `92f6f90`), whose `OUTLET_DELTA` is always 0 (SP spec step 4, `scansia-sp-spec.md:200-202` at
  `92f6f90`; the live CONSUME text, SERVER A07). `pending_deltas()` returns such events apart (`keyless`):
  surfaced, never grouped, never stamped.
- `racoon_staging` (2026-10-06, after `scansia-f6`'s staging INFO; reads as `scansia_mgr_stg` /
  `scansia_fixture_stg`): `sm_outlet_lease` time columns are `datetime(3)`; MySQL 8.0.45, `max_connections` 151,
  uptime 496112 s at the probe; a TLS connect from the Mac took 0.3–1.0 s (15 sequential); across five suite
  runs a connect failed 5 times (errno 2003; OS error 60, a TCP timeout, in the 3 runs that logged it), each
  test opening ≥ 2 connections — hence one connect retry (`CONNECT_ATTEMPTS`). The Render → VPS path is not measured (F7.3). After the runs: 0 rows
  marked `f7test`, the lease free.
- Render `scansia-manager` deploys `main` on commit (`scansia-f6`, 2026-10-06 14:23Z): this branch deploys nothing;
  `main` is production and stays out of F7.2.
