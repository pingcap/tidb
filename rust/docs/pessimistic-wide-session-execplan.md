# Wire the wide session's explicit transaction onto the pessimistic machinery

This ExecPlan is a living document per `PLANS.md`. Keep `Progress`,
`Surprises & Discoveries`, and `Decision Log` current while implementing.

## Purpose / Big Picture

`BEGIN ... COMMIT` in the wide cluster session (`--cluster-session`, the
surface sysbench drives) runs OPTIMISTIC end to end, so under write
concurrency the first transaction to `COMMIT` after a competitor's commit
fails with 9007 where Go — whose `tidb_txn_mode` defaults to
`pessimistic` — blocks the competitor at lock time and lets the first
transaction win. Observable success: `sysbench oltp_write_only` /
`oltp_read_write --threads=4` complete with zero errors against
`tidb-server --store unistore --cluster-session`, and the named gap in
`cluster_session_node/transactions.rs`'s module doc is retired.

The machinery below the seam is already complete and proven by the narrow
session driver: `RealPessimisticTransaction` (acquire_locks with
fair-locking and `locked_with_conflict`, `advance_for_update_ts`,
pessimistic-constraint commit) and `multi_statement_transaction.rs`'s
statement loop (lock → conflict → advance for_update_ts → re-plan). The
work is wiring the WIDE session's `SessionTransaction` thread and
statement lifecycle onto it.

## Go contract being ported

- `pkg/executor/adapter.go` `handlePessimisticDML`: execute the DML, lock
  the statement's membuffer-delta keys at the session's `for_update_ts`
  (blocking, `@@innodb_lock_wait_timeout`), and on a write conflict from
  locking: advance `for_update_ts`, roll the STATEMENT back, and re-execute
  it reading at the new `for_update_ts`. Statement-scoped: 1205/1213 end
  the statement, never the transaction.
- Which keys lock: Go `KeyNeedToLock` (`pkg/session/txn.go`) over the
  statement's staging delta — record-key puts (created rows included),
  record-key deletes, and UNIQUE index entries; non-unique index entries
  skip. The wide buffer carries no membuffer flags, so the flag arms are
  inlined by entry shape (`pessimistic_lock_delta`).
- `COMMIT`: `RealPessimisticTransaction::commit` sets the pessimistic
  prewrite plan (locked keys, for_update_ts constraints) — no optimistic
  conflict for locked keys.
- `BEGIN` chooses the mode from `tidb_txn_mode` (default pessimistic,
  `vardef` already carries `DefTiDBTxnMode`).

## Implementation steps

1. `tidb-exec/cluster_table_storage.rs`
   - [x] `TransactionOpen::WritablePessimistic`; the worker thread hosts an
     enum of `RealOptimisticTransaction` | `RealPessimisticTransaction`
     (opener.begin_pessimistic).
   - [x] `TransactionRequest::LockKeys { keys, reply }`: worker runs the
     acquire/fair-locking/advance loop (mirror
     `multi_statement_transaction::lock_keys`), replying one of
     `Locked { for_update_ts }`, `RetryStatement { for_update_ts }`
     (locks HELD, statement must re-run at the new ts),
     `StatementError(LockSqlError)` (1205/1213 family),
     `TransactionError(...)`.
   - [x] `TransactionRequest::GetAt/BatchGetAt/ScanAt { ts }` or a read-ts
     field on the existing arms, for the retried statement's reads at
     `for_update_ts` (the coordinator's snapshot reads already take an
     explicit `read_ts`).
   - [x] Commit arm dispatches to the pessimistic commit; Finish rolls
     held locks back first (`pessimistic_rollback`).
   - [x] `SessionTransaction::begin_pessimistic`, `lock_keys`,
     `snapshot_at(ts)`, `is_pessimistic()`.
2. `tidb-server/cluster_session_node/transactions.rs`
   - [x] `ClusterTransactions::begin` reads the session's txn mode (plumb
     from session vars at the call site) and opens the matching
     `SessionTransaction`.
   - [x] `OpenClusterTransaction` gains `lock_staged_keys(...)`,
     `snapshot_at(ts)`, `is_pessimistic()`; retire/update the module doc's
     named gap.
3. `tidb-server/cluster_session_node/mod.rs`
   - [x] In `attempt_statement`'s explicit-transaction success arm: collect
     the buffer delta since the statement savepoint (rewrites + deletes),
     call `lock_staged_keys`; on `RetryStatement` restore the savepoint,
     rebind `snapshot_at(for_update_ts)`, and re-run the statement (loop);
     on `StatementError` restore the savepoint and report it
     statement-scoped.
   - [x] `MutationBuffer` delta: keys staged after the savepoint image,
     split by mutation kind (needs a small accessor if none exists).
4. Receipts
   - [x] Regression test in `cluster_session_node/tests` racing two
     sessions' `BEGIN; UPDATE same row; COMMIT` — pre-wiring the loser
     9007s at COMMIT, post-wiring both commit and the value is the serial
     result.
   - [x] Live: `sysbench oltp_write_only`/`oltp_read_write --threads=4`
     zero errors; suites hold the 12-failure baseline; ratchet re-measured.

## Progress

- [x] (2026-08-22) Design settled; machinery inventory confirmed (this doc).
- [x] (2026-08-22) Step 1: `TransactionOpen::WritablePessimistic`, the
  pessimistic worker (`serve_pessimistic_transaction` +
  `acquire_statement_locks`), `LockKeysOutcome`, read-ts-parameterized
  Get/BatchGet/Scan requests, `SessionTransaction::{begin_pessimistic,
  lock_keys, snapshot_at, is_pessimistic}`; txnkv gained
  `snapshot_batch_get_at`/`snapshot_scan_at` and a public
  `snapshot_get_at`.
- [x] (2026-08-22) Step 2: `ClusterTransactions::begin(pessimistic)`,
  `OpenClusterTransaction::{is_pessimistic, snapshot_at,
  lock_staged_keys}` with optimistic defaults; module-doc gap rewritten
  (SELECT FOR UPDATE remains named).
- [x] (2026-08-22) Step 3: `attempt_statement`'s pessimistic loop
  (`lock_pessimistic_statement_keys` + `PessimisticStep`), buffer delta via
  `pessimistic_lock_delta` filtering with Go `KeyNeedToLock` reduced to the
  flagless buffer; `open_explicit` resolves the mode from
  `Session::txn_mode()` / `@@tidb_txn_mode`.
- [x] (2026-08-22) Step 4 receipts: `racing_pessimistic_updates_both_commit_
  with_serial_effect` (both commit, value carries BOTH increments -- the
  retry re-read) and `racing_optimistic_updates_still_conflict_at_commit`
  (the pre-wiring behavior stays reachable by keyword, loser 9007).

## Surprises & Discoveries

- The wide session's `execute("BEGIN")` does NOT route transaction control:
  the wire layer calls `QuerySession::control_transaction` separately, so a
  test driving only `execute` never opens `self.explicit`. The race tests
  call `control_transaction` exactly as the wire does.
- `KeyNeedToLock`'s membuffer flags do not exist at the wide buffer; the
  port keys off the entry shape alone (record vs index, tombstone, the
  single-byte non-unique index value), which locks INSERTED row keys too --
  Go's own base behavior once the flag arms are inlined.

- Two follow-on Go behaviors surfaced by the live battery: the resolver's
  missing `LockNotExistDoNothing` arm (aborted transactions 1105 where Go
  cleans the stale lock), and `BEGIN`'s implicit commit running AFTER the
  schema refresh (phantom 9007 at `BEGIN`, and the abandoned buffer was
  DISCARDED where Go commits it). Both fixed with their own receipts.
- WATCH: one occurrence of 1062 (`Duplicate entry` at sysbench's
  DELETE-then-INSERT pair, binary protocol, 4 threads) in the earliest
  post-wiring battery round; not reproduced in fifteen runs since,
  including eight threads over 100-row tables. Suspect set: the statement
  retry's savepoint/rebind ordering, or a uniqueness read bypassing the
  buffer overlay on the retry path. Task chip filed.

## Decision Log

- Lock scope is `KeyNeedToLock` verbatim minus flags (created rows lock
  too; the narrow driver's plan-level rewritten-rows filter is ITS shape,
  not the buffer-level contract). Safe against prewrite because the wide
  commit's mutations are unasserted; duplicates stay read-detected.
- Autocommit DML keeps its existing replay loop (already Go-shaped for the
  single-statement case); only explicit transactions change.
- `SELECT ... FOR UPDATE` row locking rides on this wiring later; it is
  NOT in this unit and stays a named gap until then.
