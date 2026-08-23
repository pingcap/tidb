# The narrow session serves Go's stale reads

Status: in progress (2026-08-23). Keep `Progress`, `Surprises & Discoveries`,
and `Decision Log` current while implementing.

## Purpose / Big Picture

The behavioural corpus is down to ONE divergence of 9,674:
`executor/stale_txn`'s `select @@tidb_current_ts = CAST(@last_commit_ts AS
UNSIGNED)` answers 1 on TiDB and NULL here. It is the visible tip of a
missing FAMILY: the narrow in-process session (the tier the differ drives)
keeps no commit history, so `@@tidb_last_txn_info` is never populated,
`AS OF TIMESTAMP` reads are refused, and `START TRANSACTION READ ONLY AS OF
TIMESTAMP` cannot pin `@@tidb_current_ts`. Eight further statements sit
OutOfDomain (we error where TiDB succeeds) for the same reason -- and
OutOfDomain is not a skip list: supporting the feature moves them into
comparison automatically.

Closing this takes the corpus to ZERO recorded divergences.

## Go contracts being ported (all read from source)

- `@@tidb_last_txn_info` is client-go's `TxnInfo` JSON
  (`.oracle/client-go/txnkv/transaction/txn.go:1054-1092`): `txn_scope`,
  `start_ts`, `commit_ts`, `txn_commit_mode` ("2pc" unless 1pc/async),
  `async_commit_fallback`, `one_pc_fallback`, `error` (omitempty),
  `pipelined`, `flush_wait_ms`. Written by the commit callback; the
  no-commit variant (`setLastTxnInfoBeforeTxnEnd`,
  `pkg/session/session.go:1056-1069`) marshals the same struct with only
  `TxnScope`+`StartTS` set -- zeros included -- and a txn that never
  activated (`StartTS == 0`, e.g. `SELECT 1`) does not overwrite at all.
- AS-OF evaluation is `CalculateAsOfTsExpr`
  (`pkg/sessiontxn/staleread/util.go:41-123`): evaluate the expression;
  NULL is 8135 "as of timestamp cannot be NULL"; try DATETIME first
  (convert to TIMESTAMP fsp 3 in the session zone, then
  `oracle.GoTimeToTS` = unix-ms << 18); fall back to a raw TSO (positive
  int, or a string that parses as u64); neither is 8135 "cannot parse AS
  OF TIMESTAMP expression as datetime or TSO"; a TSO whose physical half
  is before 2013-01-01 is 8135 "invalid TSO timestamp: TSO is before
  2013-01-01". 8135 renders as `ErrAsOf`: "invalid as of timestamp: %s".
- A stale transaction's `StartTS` IS the as-of timestamp, which is what
  `@@tidb_current_ts` reports (`sysvar.go`'s `TxnCtx.StartTS` read) --
  the corpus's exact assertion.
- The state "as of ts" is the newest commit at or below ts: the corpus
  reads at `commit_ts` (sees the row) and `commit_ts - 1` (does not).

## Implementation steps

1. `tidb-executor` `driver/catalog.rs`: `CommitHistory` behind an
   `Arc<Mutex<..>>` INSIDE `Catalog` (clones share it): a monotonic
   TSO-shaped allocator (`now_ms << 18`, strictly increasing) and a ring
   (cap 8) of `(commit_ts, Catalog)` snapshots; `state_as_of(ts)` is the
   floor lookup.
2. `tidb-session`: `last_txn_info` cell + the `@@tidb_last_txn_info`
   session-read hook (beside the existing `tidb_current_ts` hook); the
   two JSON writers; commit points allocate TSOs, record history, and
   write the JSON -- explicit COMMIT, autocommit DML/DDL (full JSON),
   table-reading autocommit SELECT and rollback/read-only end
   (start-only JSON), `SELECT 1` untouched.
3. `resolve_as_of_ts`: the `CalculateAsOfTsExpr` port over
   `eval_value`, with `DriverError::AsOf` rendering 8135.
4. Statement-level interception: a visitor finds and strips table-ref
   `as_of` exprs; all must resolve to one ts; the stripped statement
   runs against the floor snapshot through the existing catalog-stage
   swap. `START TRANSACTION READ ONLY AS OF TIMESTAMP` opens the
   transaction WITH the snapshot as its working copy, start_ts = the
   as-of ts, published to `current_tso`; its COMMIT publishes nothing.
5. Receipt: `executor/stale_txn` replays 0 diverged with the eight
   OutOfDomain statements compared; the full ratchet drops
   `KNOWN_DIVERGENCES` to 0; per-crate gates hold.

## Progress

- [x] (2026-08-23) Contracts read; seams mapped (eval_value, the
  TableRef visitor pattern, both refusal sites, the differ's
  OutOfDomain semantics).
- [x] (2026-08-23) Steps 1-4 implemented.
- [x] (2026-08-23) Step 5: `executor/stale_txn` replays 39 matched / 0
  diverged / 0 out-of-domain (was 30 / 1 / 8); the full ratchet holds at
  KNOWN_DIVERGENCES = 0 -- zero of 9,685 compared statements diverge; the
  two old refusal pins are rewritten as positive pins of the live
  contract and pass; per-crate gates show only the eight known baseline
  reds.

## Surprises & Discoveries

- `OutOfDomain` in the differ means "this tier errored where TiDB
  succeeded" -- not a curated skip list. Feature support alone widens
  the compared set.

## Outcomes & Retrospective

The corpus is at ZERO: 9,685 compared statements, none diverge. The
last divergence was not a bug in a comparison but a missing FAMILY, and
the "MVCC-blocked" label had quietly discouraged attempts at it -- the
unblocking observation was that the block was about the STORE, not the
contract, and a bounded commit history is enough store for every
behaviour the family specifies. The ratchet's own bound now enforces
zero forever: any future divergence is a regression by definition.

## Decision Log

- Decision: fabricated TSOs are REAL-SHAPED (`now_ms << 18`, monotonic)
  rather than small integers. Rationale: the corpus does TSO arithmetic
  (`CAST(@ts AS UNSIGNED) - 1`) and Go validates the physical half
  against 2013-01-01; small integers would fail Go's own validation
  rules ported here. Date/Author: 2026-08-23 / session c4d12b28.
- Decision: history ring cap is 8 full-catalog snapshots. Rationale:
  the corpus needs the last two commits; the narrow tier serves tests
  (no wire server runs it for general SQL), so per-commit clone cost is
  bounded by test-sized catalogs -- measured against the ratchet's
  runtime in step 5. Date/Author: 2026-08-23 / session c4d12b28.
- Decision: a statement mixing DIFFERENT as-of timestamps stays
  refused. Rationale: Go errors on it too (preprocess), the corpus
  never exercises it, and refusing is the honest bound until its exact
  message is needed. Date/Author: 2026-08-23 / session c4d12b28.
