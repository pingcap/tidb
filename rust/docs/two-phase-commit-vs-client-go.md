# Two-phase commit: `tidb-txnkv` against client-go, file by file

Comparison target: `github.com/tikv/client-go/v2 v2.0.8-0.20260708122311-01bd8f99f4da`
(the version `go.mod:125` pins), files `txnkv/transaction/{2pc,prewrite,commit,cleanup,txn}.go`,
against `rust/crates/tidb-txnkv/src/transaction/**` plus its exec-layer callers.

Method: source reading only. **Nothing here was executed.** No `cargo test`, no
`nextest`, no cluster, no mock run — this machine cannot launch a freshly built
binary. Every claim below is a claim about what the two sources say, and every
"sequence" is a hypothesis about a real cluster that has *not* been observed.
That limitation is the whole reason this document exists: the lost-update bug
fixed on this path today was found on a real cluster and not by any test,
because the mock's snapshot is a clone of the committed store and cannot express
a read-ts/write-ts split.

Counts: **9 divergences** (3 in consequence class 1, 2 in class 2, 2 in class 3,
2 in class 4) and **14 verified-equal behaviours**.

---

## 0. The undetermined-commit answer, first

**Question: when a COMMIT of the PRIMARY key returns a network error rather than
a verdict, what does each side do?**

**client-go.** `commit.go:127-135`: after `sender.SendReq`, if
`batch.isPrimary && sender.GetRPCError() != nil && !c.isAsyncCommit()`, it calls
`c.setUndeterminedErr(...)`. `commit.go:179-180`: the flag is cleared only when a
real `CommitResponse` body comes back for the primary batch. `2pc.go:2062-2069`
(`commitTxn`) converts any error into `tikverr.ErrResultUndetermined` when the
flag is set. `2pc.go:1738-1749` (`execute`'s defer) then **suppresses cleanup**:
`if !committed && !undetermined { c.cleanup(ctx) }`. In TiDB the error becomes
`terror.ErrResultUndetermined` (`pkg/store/driver/error/error.go:203`) and
`pkg/server/conn.go:1288` **closes the client connection** rather than reporting
either outcome. `commit.go:151-158` gives the same treatment to an explicit
`UndeterminedResult` region error on the primary batch.

**Ours — and this part matches.** `commit.rs:540-556`:
`PublishedCommand::AfterPublication` on the primary Commit (published, no
verdict) returns `PrimaryResult::Undetermined`. `commit.rs:561-575`: a region
error carrying `undetermined_result` (`commit.rs:888-890`) likewise. `commit.rs:401-411`
transitions the coordinator to `CoordinatorState::Undetermined` and returns
`OptimisticCommitOutcome::Undetermined`, **without** calling
`rollback_after_failure`. The suppression of cleanup, the ambiguity marker in the
receipt (`TransactionAttemptResult::Ambiguous`), and the distinction between
"failed before publication" (definitive) and "failed after publication"
(ambiguous) are all faithful, and are in some ways sharper than client-go's,
which reasons from a mutable `sender.GetRPCError()` rather than from the
publication boundary.

**Where it stops matching is one layer up — and that is finding D2.** The
coordinator produces a truthful `Undetermined`, and then every exec-layer
consumer flattens it into an ordinary "your transaction failed" SQL error. There
is no path in this repository that closes the connection, and no path that tells
the caller the outcome is unknown.

So: the hard part is right, the last mile is wrong.

---

## Consequence class 1 — data lost, corrupted, or a half-committed transaction made visible

### D1. Prewrite silently re-designates the primary key of a pessimistic transaction

- Ours: `rust/crates/tidb-txnkv/src/transaction/coordinator/commit.rs:86`
  — `let primary_key = mutations[0].key().to_vec();` unconditionally, after
  `validate_and_sort` (`transaction/mutation.rs:239-240`) sorts by key. The
  prewrite primary is therefore always the transaction's globally smallest key.
- Ours: `rust/crates/tidb-txnkv/src/transaction/pessimistic.rs:398-404` pins
  `primary_key = sorted[0]` of the **first locking statement** and stores it at
  `:441`; `:530-543` `commit()` hands the two-phase engine a
  `PessimisticPrewritePlan { for_update_ts, locked_keys, for_update_ts_constraints }`
  that **has no `primary_key` field at all**. The pinned primary is dropped on
  the floor.
- Ours: `rust/crates/tidb-exec/src/multi_statement_transaction.rs:483-496` starts
  the TTL heartbeat on `acquired.primary_key` — the *lock-phase* primary.
- client-go: `2pc.go:779-787` `primary()` returns the pinned `c.primaryKey`
  whenever it is set, and only falls back to `mutations.GetKey(0)` when it is
  empty. `txn.go:1794-1808` (`LockKeys`) pins it on the first lock.
  `2pc.go:697-698` (`initKeysAndMutations`) sets it **only if still empty**
  (`if len(c.primaryKey) == 0 && ...`), so mutation order can never override a
  pessimistically chosen primary. One transaction, one primary, for its whole
  life. That invariant is the entire basis of lock recovery.

Concrete sequence (two regions, keys `k3 < k5`):

1. `BEGIN;`
2. `UPDATE t SET v=1 WHERE id=5;` → pessimistic lock written on `k5` with
   `primary_lock = k5`. `self.primary_key := k5`. The heartbeat thread starts on
   `k5`.
3. `UPDATE t SET v=1 WHERE id=3;` → pessimistic lock on `k3` with
   `primary_lock = k5`. Correct so far: `k5` is the primary of record.
4. `COMMIT;` → `commit.rs:86` sets the prewrite primary to `k3`. Prewrite of
   `k3`'s region is published first and succeeds: `k3` now carries a prewrite
   lock claiming `primary_lock = k3`, and (async commit being on, see §5) naming
   `k5` as a secondary.
5. `k5`'s region is slow/partitioned. During that window the transaction has
   **two keys each claiming to be its primary**: `k3` (new prewrite lock) and
   `k5` (still the old pessimistic lock, `primary_lock = k5`).
6. Concurrent transaction T2 blocks on `k5`. It reads `k5`'s lock, sees
   `primary = k5`, and runs `check_txn_status` against `k5` itself. Meanwhile the
   heartbeat is refreshing `k5` — so T2 waits — but the *real* primary `k3` is
   being refreshed by nobody. Its TTL is whatever `transaction_lock_ttl_ms`
   (`coordinator/mod.rs:421-436`, 3 s–20 s + elapsed) granted at prewrite.
7. A transaction T3 blocked on `k3` after that TTL expires resolves `k3` as
   abandoned: `check_txn_status(primary = k3, lock_ts = start_ts)` finds the lock
   expired and rolls the transaction back, writing a Rollback record at
   `start_ts`.
8. Our prewrite of `k5` finally lands, or our Commit of `k3` lands, against a
   transaction TiKV has already rolled back. Either the commit fails after we
   have reported nothing, or — if `k3`'s Commit races ahead of the resolution —
   `k3` commits at `commit_ts` while a Rollback record for the same `start_ts`
   exists. That is a torn transaction: two contradictory statuses for one
   `start_ts`, and part of the write set visible.

The heartbeat aiming at the wrong key (step 6-7) is on its own the failure mode
that item 2 of the brief names: *a missing TTL heartbeat means other transactions
resolve our live locks as abandoned*. Here it is not missing — it is pointed at a
key that stopped being the primary the moment prewrite began.

**Not fixed here.** The fix is to add `primary_key` to `PessimisticPrewritePlan`
and have `commit()` honour it, which also means `commit_primary`'s
"regroup lost deterministic primary key" refusal (`commit.rs:499-506`) has to
tolerate a primary that is not `mutations[0]`, and `commit_secondaries`'
`holds_primary` test has to follow. That is more than a small-and-certain change,
it cannot be tested here, and getting it half-right is worse than leaving it
described.

### D2. An async-commit or 1PC prewrite whose response is lost is rolled back instead of declared undetermined

- client-go: `prewrite.go:352-361` (`prewrite1BatchReqHandler.drop`) — if the
  committer is async-commit **or** 1PC and `sender.GetRPCError() != nil` and the
  prewrite was not cancelled, it calls `setUndeterminedErr`. The comment states
  the reason plainly: for these protocols a prewrite that got no answer may
  already have committed the transaction.
- client-go: `2pc.go:1717-1737` — for 1PC and for async commit, `execute`'s defer
  runs `c.cleanup(ctx)` **only** when `c.getUndeterminedErr() == nil`. A lost
  async-commit/1PC prewrite response therefore leaves the locks alone for the
  lock resolver, which can read the primary lock's secondary list and decide
  correctly.
- Ours: `commit.rs:290-311` — `PublishedCommand::AfterPublication` on *any*
  prewrite becomes a `TransactionCause::Transport` and goes straight to
  `rollback_after_failure(receipt, &possibly_prewrite_keys, cause)`. There is no
  test of `protocol.use_async_commit` or `protocol.use_one_pc` anywhere on this
  path.

Concrete sequence (1PC, single region — and 1PC is on by default, see §5):

1. `INSERT INTO t VALUES (1);` autocommit. `attempted_protocol`
   (`prewrite.rs:58-89`) sets `use_one_pc = true`.
2. The Prewrite is published with `try_one_pc = true`. TiKV executes it and
   **commits the transaction** at a `one_pc_commit_ts`.
3. The response is lost (connection reset, timeout after the write landed).
   `publish_prewrite` returns `AfterPublication`.
4. `commit.rs:306` rolls back: `BatchRollback{ start_version: start_ts }` for the
   same keys, and `commit.rs:98` returns `RolledBack` (or `CleanupFailed`) to the
   caller.
5. The caller reports failure. The row is committed and visible. A retry of the
   `INSERT` now hits a duplicate key, or an idempotent retry double-applies.

Under async commit the same lost-response shape is worse: the completed prewrite
*is* the commit point, and issuing `BatchRollback` against a transaction whose
commit point may have passed is precisely the operation client-go refuses to
perform. Whether TiKV accepts or refuses that rollback is a TiKV-internal
question I did not verify and do not claim; what is provable at source level is
that client-go deliberately does not send it and we unconditionally do.

**Not fixed here.** The correct change is to route `AfterPublication` on prewrite
to `PrimaryResult::Undetermined`-equivalent handling when
`protocol.use_async_commit || protocol.use_one_pc`, which requires a new
`OptimisticCommitOutcome::Undetermined` return from the prewrite loop and a
matching skip of `rollback_after_failure`. Small in lines, but it changes what
the caller is told about live data, and it is untestable here.

### D3. A prewrite `undetermined_result` region error is not treated as undetermined under async commit or 1PC

- client-go: `prewrite.go:436-443` (`handleRegionErr`) — `if
  regionErr.GetUndeterminedResult() != nil && (isAsyncCommit || isOnePC)`, return
  `tikverr.ErrResultUndetermined` immediately, before any backoff or relocation.
- Ours: `commit.rs:137-187` — the prewrite region-error path goes straight into
  `recover_region_error` (`coordinator/mod.rs:344-382`), which maps an
  unrecognised/terminal region error to `TransactionCause::Region` and then
  `rollback_after_failure`. `primary_region_response_is_ambiguous`
  (`commit.rs:888-890`) exists but is called from exactly one place,
  `commit.rs:562`, on the primary **Commit** — never on prewrite.

Same sequence as D2 with step 3 replaced by "TiKV's region layer answers
`UndeterminedResult`", which is TiKV explicitly saying *I do not know whether
this write applied*. We answer that by rolling back and reporting a definite
outcome.

---

## Consequence class 2 — an outcome reported to the client that may not be true

### D4. `Undetermined` is flattened into an ordinary failure at every exec-layer consumer

- Ours: `rust/crates/tidb-exec/src/real_tikv_ddl.rs:239-256` matches
  `Committed` / `RolledBack` / `CleanupFailed` and sends everything else,
  `Undetermined` included, to `ClusterDdlError::NotCommitted(...)` — a name that
  asserts the very thing that is unknown.
- Ours: `rust/crates/tidb-exec/src/real_tikv_dml.rs:1352-1357` — `Committed`
  is `Ok`, `other => Err(ConfiguredWriteError::NotCommitted(...))`.
- Ours: `rust/crates/tidb-exec/src/real_tikv_analyze.rs:203`,
  `rust/crates/tidb-exec/src/cluster_auto_id.rs:185-190` — same shape.
- Ours: `rust/crates/tidb-exec/src/pessimistic_lock_error.rs:121` is the only
  consumer that names `Undetermined` explicitly, and it too returns a plain
  `LockSqlError`.
- client-go / TiDB: `2pc.go:2062-2069` → `tikverr.ErrResultUndetermined` →
  `pkg/store/driver/error/error.go:203` → `terror.ErrResultUndetermined` →
  `pkg/server/conn.go:1288`, which **closes the connection**. The connection is
  closed precisely because no SQL error code can express "unknown", and because a
  client that receives an error is entitled to retry — which, if the commit
  actually landed, double-applies.

Concrete sequence: an `UPDATE accounts SET balance = balance - 100 WHERE id = 1`
whose primary Commit is published and then loses its response. The coordinator
correctly says `Undetermined`. `real_tikv_dml.rs:1357` reports
`NotCommitted`. The application retries. The debit applies twice.

This is the cheapest of the class-1/2 findings to fix correctly, but "correctly"
means introducing a connection-kill path at the protocol layer, which does not
exist in this repository yet. Written up, not changed.

### D5. No maximum-transaction-lifetime or commit-ts upper-bound check before committing

- client-go: `2pc.go:1972-1983`, after the commit timestamp is obtained and
  before any Commit is published: `if c.store.GetOracle().IsExpired(c.startTS,
  MaxTxnTimeUse, ...)` → error `"txn takes too much time"`; and
  `if c.txn.commitTSUpperBoundCheck != nil && !c.txn.commitTSUpperBoundCheck(commitTS)`
  → error. `2pc.go:1964-1969` additionally re-validates the schema version at
  `commitTS` for the non-async path.
- Ours: `commit.rs:378-384` obtains the commit timestamp and proceeds. The only
  validation is `commit_timestamp` (`commit.rs:447-479`): `timestamp >
  self.start_ts && timestamp >= minimum`. There is no lifetime bound, no
  upper-bound hook, and no schema re-validation.

Concrete sequence: a long-running multi-statement transaction whose `start_ts`
has fallen behind the GC safepoint. Its snapshot reads
(`snapshot_read.rs:86,229`, all at `self.start_ts`) may already be reading
partially garbage-collected history, and we commit on top of them. client-go
refuses the commit outright. Whether GC has actually collected at that point
depends on cluster configuration, so I rank this class 2 rather than class 1:
the failure is that we report success for a commit client-go declares invalid.

The `commit_ts` validation asked for in item 6 of the brief is otherwise
**stronger** than client-go's — see V9.

---

## Consequence class 3 — locks left behind or resolved wrongly

### D6. Rollback covers only the keys prewrite actually reached, not the whole mutation set

- client-go: `2pc.go:1689-1698` — `c.cleanupMutations(bo, c.mutations)`. **All**
  mutations, unconditionally, regardless of how far prewrite got. For a
  pessimistic 1PC txn, `2pc.go:1699-1708` uses `pessimisticRollbackMutations`
  over the same full set.
- Ours: `commit.rs:95` builds `possibly_prewrite_keys` and extends it only for
  batches that were actually published (`commit.rs:133`, `:291`);
  `cleanup.rs:109-131` rolls back exactly those.

Concrete sequence: a pessimistic transaction holding locks on regions A, B and C.
Prewrite of region A fails definitively (write conflict). `possibly_prewrite_keys`
holds only A's keys. We roll back A and return. The pessimistic locks on B and C
stay until their TTL expires — and the heartbeat has stopped, because
`multi_statement_transaction.rs:528-529` closes it when the transaction ends.
Every reader of B and C blocks for the remaining TTL. client-go's
`BatchRollback` over the full mutation set clears them immediately.

This is a liveness/lock-hygiene defect, not a correctness one: the lock resolver
does eventually clean them, and the transaction's status at the primary is
consistent. Ranked 3 for that reason.

### D7. Rollback and secondary commit run inside the caller's per-call timeout, not their own budget

- client-go: `2pc.go:1651` spawns cleanup on its own goroutine with
  `retry.NewBackofferWithVars(cleanupKeysCtx, cleanupMaxBackoff=20000, ...)` and,
  critically, `cleanupKeysCtx` is derived from `c.store.Ctx()` — **not** from the
  statement context, so a cancelled statement still gets its locks cleaned.
  Secondary commits get `CommitSecondaryMaxBackoff = 41000` (`2pc.go:967,1054`).
- Ours: `cleanup.rs:117` and `commit.rs:692` both build
  `UnaryCallContext::with_timeout(self.timeout)` — the same per-call timeout the
  failed operation was already running under — and `wait_with_call`
  (`coordinator/mod.rs:438-458`) refuses any backoff longer than that timeout.

Concrete sequence: a statement with a short timeout whose prewrite times out. The
rollback is issued with the same short timeout, its first region-error backoff
exceeds it, `wait_with_call` returns `Transport`, and the rollback is abandoned
with `CleanupFailed`. Under client-go the same failure gets a fresh 20-second
budget on a context that outlives the statement.

---

## Consequence class 4 — retries and performance

### D8. Prewrite lock resolution is bounded by a fixed attempt count, not a time budget

`commit.rs:118,194,206` — `lock_attempts` against `MAX_LOCK_ATTEMPTS = 4`
(`coordinator/mod.rs:69`), after which the transaction fails with
`"Prewrite lock retry budget exhausted"`. client-go retries as long as the
backoffer's budget lasts (`prewrite.go` loop under `PrewriteMaxBackoff`), and
`resolveLocks` failure is what ends it. A transaction contending with four
successive short-lived lock holders fails here where client-go would succeed.
The failure is loud and the transaction rolls back, so nothing is lost.

### D9. Commit and rollback batches are not split by size

`region_batches.rs:166-180` (`group_keys`) produces exactly one batch per region.
client-go's `batchBuilder.appendBatchMutationsBySize` (`2pc.go:996-997`) splits
each region group at `kv.TxnCommitBatchSize`. A single region holding a very
large key set produces one oversized Commit/BatchRollback RPC. Note that this
also means `forgetPrimary`'s equivalent — `commit.rs:417-425`, excluding the
primary batch's keys — excludes a whole region rather than one size-bounded
batch, which is the same set as long as no split happens.

---

## Verified equal

Each of these was read on both sides and found to agree. This list is the
point of the exercise as much as the divergences are.

- **V1. Commit-sequence shape.** Primary committed first and alone, then the
  remaining keys. client-go `2pc.go:1037-1049` (`firstIsPrimary && actionIsCommit
  && !isAsyncCommit` → `doActionOnBatches(primaryBatch)`, then `forgetPrimary()`).
  Ours `commit.rs:390-437`: `commit_primary`, then `secondary_keys` computed by
  excluding the primary batch's keys, then `commit_secondaries`. No path can
  publish a secondary Commit before the primary's verdict.
- **V2. The primary batch's other keys ride with the primary.** Both sides commit
  the whole region batch containing the primary in the primary round and exclude
  exactly that set from the secondary round (`2pc.go:1048` `forgetPrimary` /
  `commit.rs:417-425`).
- **V3. Secondary commit failures do not un-commit the transaction.** client-go
  swallows them in the spawned goroutine (`2pc.go:1076-1083`); ours collects them
  into `CommittedTransaction::secondary_failures` (`commit.rs:441-444`) and still
  reports `Committed`. Ours additionally preserves the evidence, which client-go
  only logs.
- **V4. `CommitTsExpired` on the primary is the one commit KeyError that is
  retried, and it changes `commit_ts`.** client-go `commit.go:184-221`; ours
  `commit.rs:608-649`. Both re-take a timestamp and re-publish with the same
  keys. Both refuse when the key is not the primary (`commit.go:191-198` /
  `commit.rs:900-905`'s `expired.key != primary_key`). Both cap the retry at
  1 hour of TSO drift: client-go's literal `943718400000` is exactly
  `MAX_COMMIT_TS_DRIFT_MS (3_600_000) << TSO_LOGICAL_BITS (18)`
  (`coordinator/mod.rs:70-71`, `commit.rs:898-899`).
- **V5. Every other commit KeyError is fatal.** client-go `commit.go:224-254`
  returns the extracted error with no retry; ours `commit.rs:651-660`
  `classify_key_error` → `DefinitiveFailure` → rollback. Neither retries
  `AlreadyExist`, `WriteConflict`, `TxnNotFound`, `Abort`, or `CommitTsTooLarge`
  at commit time.
- **V6. Prewrite `KeyIsLocked` is the one prewrite KeyError that is retried, and
  only after resolution.** client-go `prewrite.go:411-420` (`extractKeyErrs` then
  `resolveLocks` then `retryable = true`); ours `prewrite.rs:179-263` then
  `commit.rs:188-245`. Both retry at the **same `start_ts`** — which is the shape
  of the bug already found, and it is correct on both sides.
- **V7. An optimistic prewrite meeting a lock with a larger `start_ts` fails with
  WriteConflict without paying for resolution.** client-go
  `prewrite.go:538-548`; ours `prewrite.rs:217-226`. Same short circuit, same
  reasoning, and ours carries the Go citation in its comment.
- **V8. `AlreadyExist` on prewrite is fatal and never resolved as a lock.**
  client-go `prewrite.go:481-485` returns before the lock extraction; ours
  `prewrite.rs:187-189` returns `classify_key_error` for any KeyError without a
  `locked` field, and `classify_key_error` (`coordinator/mod.rs:393-398`) puts
  `already_exist` first.
- **V9. `commit_ts` validation.** client-go relies on PD monotonicity and does not
  re-check `commitTS > startTS` on the ordinary path. Ours
  (`commit.rs:469`) requires `timestamp > self.start_ts && timestamp >= minimum`
  where `minimum` is `max(start_ts + 1, max over batches of
  response.min_commit_ts)` (`commit.rs:96,261`), and fails loudly after
  `MAX_LOCK_ATTEMPTS` if PD will not produce one. **Stronger than client-go**,
  in the safe direction.
- **V10. TTL derivation from transaction size.** client-go `2pc.go:804-823`:
  `ttlFactor = 6000`, `defaultLockTTL = 3000`, `ttl = 6000 * sqrt(sizeInMiB)`
  above a threshold, floored at `defaultLockTTL`, plus the elapsed time since the
  transaction started. Ours `coordinator/mod.rs:421-436`: identical factor,
  identical default, identical `sqrt(MiB)` formula, `SIZE_THRESHOLD_BYTES =
  16 * 1024` matching Go's threshold, clamped to `[3000, 20000]`, plus
  `opened_at.elapsed()`. Go clamps the upper bound elsewhere via
  `MaxTxnTTL`; the ceiling of `ManagedLockTTL = 20000` is the same number.
- **V11. Heartbeat interval and lifetime.** client-go `2pc.go:1303-1304`: ticker
  at `ManagedLockTTL / 2`. Ours `coordinator/opener.rs:302-308`:
  `MANAGED_LOCK_TTL_MS / 2`, with the same rationale in the comment.
  `maxConsecutiveFailure = 10` (`2pc.go:1293`) matches
  `MAX_CONSECUTIVE_FAILURES = 10` (`transaction/ttl.rs:40`). Go's
  `MaxTxnTTL` lifetime cap (`2pc.go:1330-1355`) matches
  `MAX_TXN_TTL_MS = 60 * 60 * 1000` (`transaction/ttl.rs:38`), and both stop the
  loop when it is reached rather than heartbeating forever.
- **V12. The heartbeat only runs for pessimistic transactions.** client-go starts
  `ttlManager.run` from the pessimistic-lock path and the pipelined path, never
  for a plain optimistic 2PC. Ours starts it only at
  `multi_statement_transaction.rs:483-496`, on a successful pessimistic lock
  acquisition. An optimistic autocommit statement has no heartbeat on either
  side, which is correct: its lock TTL is sized to outlive the commit.
- **V13. `start_ts` is single-sourced.** The transaction's `start_ts` is fixed at
  open (`coordinator/opener.rs:249-265`), and every snapshot read
  (`snapshot_read.rs:86,229`) and every Prewrite (`prewrite.rs:121`) and Commit
  (`commit.rs:509,712`) uses that same field. Nothing re-takes a timestamp
  between reading and prewriting. This is the invariant the lost-update bug
  violated, and it now holds structurally rather than by discipline.
- **V14. No `Op_CheckNotExists` mutations exist here**, so client-go's
  `stripNoNeedCommitKeys` (`2pc.go:2083`) and the `CheckNotExists` exclusion in
  `asyncSecondaries` (`2pc.go:795`) have no counterpart to diverge from.
  `transaction/mutation.rs:142-148` emits only `Insert`, `Put` and `Del`.

---

## §5. One-PC and async commit specifically

**We do take them, on a real cluster.**
`rust/crates/tidb-exec/src/session_commit_protocol.rs:35-46` derives both flags
from `global_system_variable_initial_value` with `store_is_tikv: true`, which
returns `ON` for both `tidb_enable_async_commit` and `tidb_enable_1pc` — the file
says so, and its own test at `:55-57` asserts it. So the async-commit and 1PC
paths in `commit.rs:326-376` are live, which is what makes D2 and D3 class-1
findings rather than notes about dead code.

Checked and equal:

- **1PC admission and fallback.** `prewrite.rs:283-287` `observe_batch_count`
  turns 1PC off the moment the mutations need more than one region, including
  when a mid-prewrite region split discovers it — this is client-go's
  `checkOnePCFallBack` (`2pc.go:1006,1629`). `prewrite.rs:299-320` treats a
  zeroed `one_pc_commit_ts` as TiKV declining, and additionally refuses a
  response that declines 1PC while still offering a `min_commit_ts`, and refuses
  a second 1PC commit ts from a second batch. `commit.rs:326-336` refuses to
  report `OnePc` with a zero commit ts — client-go's `2pc.go:1926-1929`.
  `commit.rs:324-325` correctly publishes **no** Commit at all for 1PC.
- **The 1PC/non-1PC cross-check.** client-go `2pc.go:1938-1941` `Fatal`s if a
  non-1PC transaction came back with a 1PC commit ts. Ours `prewrite.rs:322-329`
  returns `InvalidResponse` for the same condition. Same refusal, softer landing.
- **Async-commit admission.** `prewrite.rs:63-69`: `<= 256` keys
  (`ASYNC_COMMIT_KEYS_LIMIT`) and `<= 4 KiB` of total key bytes
  (`ASYNC_COMMIT_TOTAL_KEY_SIZE_LIMIT`), matching client-go's
  `checkAsyncCommit` limits (`2pc.go:1551`).
- **The secondary-lock list.** `prewrite.rs:79-87` collects every key except the
  primary, and `prewrite.rs:133-140` attaches it **only** to the batch containing
  the primary. That is client-go's `asyncSecondaries` (`2pc.go:790-801`) plus
  `buildPrewriteRequest`'s primary-only attachment, and the reason is stated
  correctly in the comment: only the primary lock names the secondaries, which is
  what makes the primary the single recovery entry point. Given D1, note that
  this list is built from `commit.rs:86`'s primary, not the pessimistic one — the
  two defects compound.
- **`min_commit_ts` derivation.** `commit.rs:96` seeds `start_ts + 1`,
  `commit.rs:261` takes the max over every batch's returned `min_commit_ts`, and
  `commit.rs:357` uses it as `commit_ts`. client-go `2pc.go:1943-1947` reads
  `c.minCommitTSMgr.get()` and **errors if it is zero**. Ours cannot reach that
  state: `prewrite.rs:330-332` turns async commit off for any batch that returns
  `min_commit_ts == 0`, so reaching `commit.rs:356` implies every batch returned
  a nonzero value greater than `start_ts`. The seed can therefore never be the
  winning value. Equal by construction rather than by check — worth knowing,
  because a future change to `observe_prewrite_response` would silently make the
  seed load-bearing.
- **`max_commit_ts`.** `prewrite.rs:97-101` derives a synthetic "now" from
  `opened_at.elapsed()` shifted into TSO units and adds a 2 s safe window,
  matching `calculateMaxCommitTS` (`2pc.go:2143`). No PD round trip on either
  side.
- **Async commit does not split primary from secondaries at Commit time.**
  client-go `2pc.go:1037-1038` excludes async commit from the primary-first
  branch; ours `commit.rs:362-368` passes `all_mutation_keys` in one call with
  `use_async_commit = true`, letting `holds_primary` (`commit.rs:710`) set the
  role per batch. Equal.
- **Divergence, ranked 4:** client-go spawns the async-commit Commit round on a
  goroutine and returns immediately (`2pc.go:2032-2043`); we run it inline
  (`commit.rs:362`). Latency only — the commit point has already passed, and the
  failures are reported as `secondary_failures`, not as commit failures. Safe
  direction.

---

## §8. Idempotence and retry safety of every RPC we retry

- **Prewrite retried after a region error** (`commit.rs:169-186`): same
  `start_ts` (`prewrite.rs:121`), same mutations, `is_retry_request = true`
  (`prewrite.rs:170`). Idempotent. client-go additionally sets
  `txnSize = MaxUint64` on a retry to suppress TiKV's "resolve lock lite"
  (`prewrite.go:332-336`); we always send the whole transaction's mutation count
  (`prewrite.rs:123`), which is never smaller than client-go's per-region count,
  so we are never more aggressive than client-go here. Safe divergence.
- **Prewrite retried after lock resolution** (`commit.rs:194-208`): same
  `start_ts`. Idempotent, and this is the exact shape the found bug violated.
- **Primary Commit retried after a region error** (`commit.rs:605-606`) and after
  `CommitTsExpired` (`commit.rs:641-649`): the first keeps `commit_ts`
  unchanged; the second is the one intended `commit_ts` change and is gated by
  `validate_commit_ts_expired`, which pins `start_ts`, the attempted commit ts,
  and the key to the primary. Idempotent under TiKV's commit semantics.
- **Secondary Commit retried after a region error** (`commit.rs:789-807`): same
  `commit_ts`. Idempotent.
- **BatchRollback retried** (`cleanup.rs:208-226`): same `start_ts`, no ts.
  Idempotent.
- **The one non-idempotent retry** is D2/D3's: re-publishing nothing, but
  *rolling back* after a possibly-successful async-commit/1PC prewrite. That is
  not a retry of the same operation, it is a contradicting operation.

---

## What is unverified

Everything empirical. Specifically:

- No test was run, no binary was built and executed, no cluster was contacted.
  `cargo check` and `cargo clippy` are the only tools available on this machine
  and this document changes no code, so neither was needed.
- Every "concrete sequence" above is a reading of two sources, not an observed
  interleaving. In particular D1's steps 6-8 depend on TiKV's `check_txn_status`
  behaviour against a pessimistic lock whose primary is itself, and D2's
  consequences depend on TiKV's handling of `BatchRollback` against an
  async-commit or 1PC-committed record. I read neither TiKV source nor ran
  either; the source-level claim — that client-go deliberately does not issue
  those operations and we do — stands independently of how TiKV answers them.
- I did not compare `pipelined_flush.go` (no counterpart exists here),
  `pessimistic.go`'s lock-acquisition RPC handling (another unit is on that
  surface), or `txn.go`'s statement-level buffer management.
- The mock cannot express a read-ts/write-ts split, so none of the class-1
  findings can be reproduced by any test in this repository today. Reproducing
  D1 needs a two-region pessimistic transaction whose first-locked key is not its
  smallest key; reproducing D2 needs a prewrite whose response is dropped after
  the write applied.

## What was deliberately not changed

All nine. Three of them (D1, D2, D3) sit directly on live data with async commit
and 1PC enabled by default, and none of them can be tested here. A wrong change
on this surface does not raise an error — it loses or tears data — so the
refusal to patch blind is itself the finding's safest disposition. D4 is the
cheapest correct fix but needs a connection-termination path that does not exist
in this repository yet.
