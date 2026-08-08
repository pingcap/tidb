# TiDB Design Documents

- Author(s): [takaidohigasi](https://github.com/takaidohigasi)
- Discussion PR: https://github.com/pingcap/tidb/pull/XXX
- Tracking Issue: https://github.com/pingcap/tidb/issues/18207

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
    * [Semantics](#semantics)
    * [Protocol](#protocol)
    * [TiKV](#tikv)
    * [Transaction Client (client-go)](#transaction-client-client-go)
    * [TiDB](#tidb)
    * [Compatibility and Rollout](#compatibility-and-rollout)
* [Test Design](#test-design)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This document proposes an implementation of MySQL 8.0's `SELECT ... FOR UPDATE SKIP
LOCKED` for TiDB: a locking read that never waits for row locks and silently removes
rows locked by other transactions from the result set.

It supersedes the unmerged 2022 proposal (#32625), which predates the modern
pessimistic lock protocol (`wake_up_mode`, per-key `PessimisticLockKeyResult`,
resumable lock requests).

## Motivation or Background

Since MySQL 8.0.1, `SELECT ... FOR UPDATE [NOWAIT | SKIP LOCKED]` is supported
([WL#8919](https://dev.mysql.com/worklog/task/?id=8919)). `NOWAIT` fails the statement
with `ER_LOCK_NOWAIT` (3572) on the first conflicting row; `SKIP LOCKED` skips
conflicting rows without error. The canonical use case is queue-like tables and hot-row
sharding: multiple workers `SELECT ... FOR UPDATE SKIP LOCKED LIMIT n` and each obtains
disjoint unlocked rows.

TiDB supports `NOWAIT` and `WAIT n`, but `SKIP LOCKED` is parsed and then **silently
degrades to a plain non-locking read** (#69720), and on nonclustered-PK tables it can
even fail planning with a `_tidb_rowid` column resolution error (#61621, #67715). MySQL
documents SKIP LOCKED as returning "an inconsistent view of the data", making it
"not suitable for general transactional work", but appropriate for queue-like access —
which is exactly the workload TiDB users request it for (#18207 is `feature/accepted`).

## Detailed Design

### Semantics

1. A key is **skipped** iff, at lock-acquisition time in TiKV, it holds a lock owned by
   another transaction (exclusive or, for an exclusive request, shared) — the
   `KeyIsLocked` condition. No lock is written for a skipped key, the key never enters
   the lock waiting queue, and no deadlock-detection entry is created.
2. A **write conflict on an unlocked key** (`latest commit_ts > for_update_ts`) is NOT
   a lock and is NOT skipped: it still fails the request with `WriteConflict`, and
   TiDB's existing pessimistic statement retry re-executes the statement at a newer
   `for_update_ts`, recomputing the skip set. The result of SKIP LOCKED is documented
   by MySQL as nondeterministic, so recomputation on retry is acceptable.
3. Keys already locked by the same transaction are never skipped.
4. Rows whose keys were skipped are removed from the statement's result set. Snapshot
   reads in the same transaction still see those rows (same as MySQL InnoDB).
5. `SELECT ... FOR SHARE SKIP LOCKED` is out of scope for v1: when FOR SHARE is a noop
   (default), it stays a noop with the usual warning; when shared locks are actually in
   effect (`tidb_enable_shared_lock_promotion`), it returns `ErrNotSupportedYet`.
   Shared-lock skipping interacts with the shrink-only lock conversion and deserves a
   separate design.
6. Fair (aggressive) locking is disabled for skip-locked statements: its
   `WakeUpModeForceLock` path forces locking through write conflicts, which is
   incompatible with (2), and it is a hot-single-row optimization while SKIP LOCKED
   targets the opposite access pattern.

Known v1 divergences from MySQL (documented, to be narrowed later):

- Multi-table `FOR UPDATE SKIP LOCKED` (joins) returns `ErrNotSupportedYet` in v1.
- `LIMIT n` above the lock operator locks what was scanned and then filters, so it may
  return fewer than `n` rows even when further unlocked rows exist, whereas MySQL's
  scan continues past skipped rows to fill the limit. Fixing this requires lock-during-
  scan (coprocessor participation) and is explicitly future work.
- MySQL skips on gap/next-key lock conflicts; TiDB's pessimistic model has no gap
  locks, so only record locks are considered.

### Protocol

kvproto changes (`proto/kvrpcpb.proto`):

```protobuf
message PessimisticLockRequest {
  // ...
  PessimisticLockWakeUpMode wake_up_mode = 14;
  // If set to true, keys locked by other transactions are skipped instead of waited
  // for or reported as errors. Only allowed with WakeUpModeNormal; incompatible with
  // allow_lock_with_conflict. The response carries per-key results in `results`.
  bool skip_locked = 15;
}

enum PessimisticLockKeyResultType {
  LockResultNormal = 0;
  LockResultLockedWithConflict = 1;
  LockResultFailed = 2;
  // The key was locked by another transaction and was skipped. Only used when
  // skip_locked is set.
  LockResultSkipped = 3;
}
```

Contract: when `skip_locked = true`, `wake_up_mode` must be `WakeUpModeNormal`, and the
response carries exactly one `PessimisticLockKeyResult` per mutation (in request order)
in `results`; the legacy `values`/`not_founds` fields stay empty. `skip_locked`
together with `allow_lock_with_conflict`/`WakeUpModeForceLock` is rejected. Unlike
`WakeUpModeForceLock`, multi-key batches are allowed: since skipped keys neither wait
nor abandon the request, each key's disposition is independent (the same key-by-key
model as `AcquirePessimisticLockResumed`).

Two contract details implementations must uphold:

- **Request-order mapping**: `results[i]` corresponds to `mutations[i]` of the request,
  so implementations must not reorder mutations while processing skip-locked requests
  (TiKV processes keys in request order; the unistore mockstore disables its
  `sortMutations` optimization in skip-locked mode). Multi-key requests with unsorted
  keys and mixed skipped/acquired dispositions are covered by tests.
- **Errors dominate partial results**: if a non-skip error (e.g. `WriteConflict`,
  `AlreadyExist`) is hit for any key, the whole request fails: the response carries
  `errors`, no lock of this request is written, and the `results` field must be left
  empty and ignored by the client. client-go processes `errors` before looking at
  `results`, and validates `len(results) == len(mutations)` otherwise.

### TiKV

In `AcquirePessimisticLock::process_write`:

- On `Err(KeyIsLocked)` for a key: push `PessimisticLockKeyResult::Skipped` and
  **continue** the loop — do not clear previously succeeded keys, do not push the lock
  into `encountered_locks`, do not break. Locks already acquired in the same request
  stay acquired.
- On `Err(NotInShrinkMode)` (exclusive request meets shared locks): push `Skipped` and
  continue **without** converting the shared locks to shrink-only — a skip-locked
  reader must not mutate other transactions' lock state.
- All other errors (e.g. `WriteConflict`) fail the request unchanged.
- Because nothing enters `encountered_locks`, the scheduler's lock-waiting paths
  (`on_wait_for_lock`, waiter manager, deadlock detector) are never involved, and the
  resumable command needs no changes.

`PessimisticLockKeyResult` gains a `Skipped` variant, mapped to `LockResultSkipped` in
`into_pb`. The gRPC layer routes skip-locked responses through the per-key `results`
field (as `WakeUpModeForceLock` does today).

### Transaction Client (client-go)

- `kv.LockCtx` gains `SkipLocked bool`; `kv.ReturnedValue` gains `SkipLocked bool`.
  Skipped keys are reported through `LockCtx.Values` (the same surface
  `LockOnlyIfExists` uses to report not-locked keys), plus an `IterateSkippedKeys`
  helper.
- The pessimistic lock action sets the request field and consumes the per-key
  `Results` with a dedicated response handler. Key errors are terminal (no lock
  resolving / waiting); a legacy-shaped response (no `results`) is converted into a
  hard "TiKV does not support skip locked" error — the backstop against old TiKV
  versions that ignore the unknown field.
- Accounting: skipped keys are not flagged as locked in the memory buffer and are not
  counted into `lockedCnt` (mirrors `LockOnlyIfExists`).
- **Primary key safety**: secondary pessimistic locks refer to the primary for
  transaction status, so a skipped primary with locked secondaries would let other
  transactions resolve those locks as rolled back. When the primary is tentatively
  selected among the keys of the current call, the client locks candidate primaries
  one by one (skip-locked, single-key requests) until one is acquired, then locks the
  remaining keys in one batch; if all keys are skipped, the primary selection is
  reverted and nothing is locked. The TTL keepalive is only started when the primary
  key was actually locked.
- New public API `KVTxn.PessimisticRollbackKeys(ctx, keys)`: statement-scope rollback
  of locks on specific keys (async, like the internal failure cleanup), reverting
  membuffer lock flags and `lockedCnt`. Needed by TiDB's unique-index executors: if
  the index key is locked but the row key is then skipped, the index lock must be
  released, otherwise concurrent skip-locked consumers of other rows would be blocked
  by a stale index-key lock.

### TiDB

Planner:

- Add the SkipLocked lock types to `ast.IsReadOnly`, and
  `SelectLockForUpdateSkipLocked` to `isSelectForUpdateLockType` /
  `IsSupportedSelectLockType` and `isForUpdateReadSelectLock`, so the statement is
  planned as a locking for-update read.
- `getLockWaitTime` treats SkipLocked as `lock = true` (the skip behavior travels via
  the lock type, not the wait time).
- Plan-build gates: multi-table `FOR UPDATE SKIP LOCKED` → `ErrNotSupportedYet`;
  sysvar disabled → `ErrNotSupportedYet`; TiKV min-version check (below).
- Bug fixes shipped ahead of the feature (PR 0): SKIP LOCKED errors deterministically
  instead of silently not locking (#69720), and `LogicalLock.PruneColumns` preserves
  handle columns for unsupported lock types, fixing the `_tidb_rowid` resolution crash
  (#61621, #67715).

Executor:

- `SelectLockExec` in skip-locked mode buffers child rows, issues one `LockKeys` with
  `LockCtx.SkipLocked`, then emits only rows whose keys were acquired. (Today it
  streams rows up before locking at end-of-stream, which cannot filter.)
  `runPessimisticSelectForUpdate` already drains the executor before returning results
  to the client, so no skipped row can escape before locks resolve. The buffer is not
  unbounded: it is attached to the statement's memory tracker, so it counts against
  `tidb_mem_quota_query` and exceeding the quota triggers the configured OOM action
  (by default the query is cancelled) — i.e. explicit rejection rather than unbounded
  growth. This matches the bound that `runPessimisticSelectForUpdate`'s own row
  buffering already has. Disk spilling for the buffer is possible follow-up work, but
  note the target workload is `LIMIT n` queue pops with small buffered sets.
- `PointGet`/`BatchPointGet`: a skipped row/index key produces no output row and does
  not enter the pessimistic lock cache. For unique-index access, the executor forms
  (index key → row key) pairs: if the index key locks but the row key skips,
  `PessimisticRollbackKeys` releases the index key and the row is dropped.
- The pessimistic-retry loop is unchanged: skip-locked statements produce no
  lock-wait errors; `WriteConflict` retries recompute the skip set.

The unistore mockstore implements the same protocol contract so executor/planner unit
tests run without real TiKV.

### Compatibility and Rollout

Three defense layers, because an old TiKV silently ignores the unknown proto field and
**waits** (the worst failure mode: wrong semantics, no error):

1. Sysvar `tidb_enable_select_skip_locked` (SESSION | GLOBAL), default `OFF` in the
   first release. When OFF, SKIP LOCKED returns `ErrNotSupportedYet` — the same
   deterministic behavior as PR 0, never a silent no-op.
2. When enabled and a skip-locked statement executes, TiDB verifies that **every** TiKV
   store in the cluster (i.e. the minimum store version, so any store the request may
   route to is covered) supports skip-locked, via the store-info path used by
   `TIKV_STORE_STATUS`, cached with a TTL; otherwise `ErrNotSupportedYet` with a
   version hint.
3. client-go backstop: a skip-locked response without per-key `results` fails hard
   with "TiKV does not support skip locked".

Upgrade order is the usual TiKV-before-TiDB. **Downgrading** a TiKV below the minimum
skip-locked version while the feature is in use is not safe to leave to the backstop
alone (an incompatible store would wait before responding), so it must be prevented:
the version check in layer 2 re-validates per statement against the store list (the
TTL cache bounds the window after a downgraded store joins), and the operational
guidance is to turn `tidb_enable_select_skip_locked` OFF before any TiKV downgrade
below the minimum version — consistent with the general rule that in-place component
downgrades below a feature's minimum version require disabling the feature first.

Statement-based replication caveats do not apply (TiDB Binlog is deprecated; TiCDC is
row-based), but the nondeterminism note is added to the docs, mirroring MySQL's
unsafe-for-SBR warning.

## Test Design

### Functional Tests

- TiKV unit tests: multi-key partial skip ([Normal, Skipped, Normal] with only
  unskipped locks written), same-txn re-lock not skipped, optimistic-lock conflicts
  skipped, shared locks skipped without shrink-only conversion, `return_values` on
  skipped keys, WriteConflict still failing the request, illegal combination rejected.
- client-go integration tests (unistore + real TiKV): two-session skip scenarios,
  partial skip, commit excludes skipped keys, `PessimisticRollbackKeys` releases only
  targeted keys, skipped-primary handling (primary reselection; all-skipped resets
  primary), old-TiKV backstop via mock RPC.
- TiDB `tests/realtikvtest/pessimistictest`: the queue-worker pattern (N sessions
  locking disjoint rows), RR + RC, clustered and nonclustered PK, unique-index
  PointGet/BatchPointGet including index-lock rollback verification, snapshot-read
  visibility of skipped rows, write-conflict retry recomputing the skip set, sysvar
  and FOR SHARE matrices, EXPLAIN output.
- Port the scenario catalog from MySQL's own MTR suites (`locking_clause.test`,
  `innodb/skip_locked_nowait.test`, `skip_locked_nowait_isolation.test`,
  `locking_part.test`) where applicable.

### Compatibility Tests

- Old-TiKV behavior: statement fails with a clear error (never waits silently).
- Interoperation with fair locking (other statements in the same txn), RC read
  consistency, plan cache (lock type is part of the plan), prepared statements.

### Benchmark Tests

- Queue-pop workload (N workers, SKIP LOCKED LIMIT 1) throughput vs. NOWAIT-with-retry
  and plain FOR UPDATE baselines; no regression for non-skip-locked pessimistic
  workloads (the new field defaults to false everywhere).

## Impacts & Risks

- `SelectLockExec` buffering changes the memory profile of large skip-locked scans;
  mitigated by memory tracking and the typical `LIMIT n` usage.
- RPC-retry idempotency: a re-sent skip-locked request may skip a key it had locked in
  a lost response, leaving an owned-but-unreported lock until TTL expiry — the same
  exposure class as the existing `LockedWithConflict` lost-response TODO in client-go.
- The per-key `Skipped` variant adds match arms in TiKV; Rust exhaustiveness covers
  the audit.

## Investigation & Alternatives

- MySQL implements skip-locked in the InnoDB lock system (`lock_rec_lock_slow`
  returns `DB_SKIP_LOCKED` before enqueueing a waiter; `row_search_mvcc` treats it as
  "go to next record"). TiDB's equivalent decision point is TiKV's
  `acquire_pessimistic_lock` action, and the "next record" behavior maps to client-side
  result filtering, since TiKV has no scan-and-lock primitive (locking is always
  key-by-key from client-provided keys; tikv#10920).
- Alternative considered: reusing `WakeUpModeForceLock` + `LockResultFailed` instead
  of a new field. Rejected: force-lock implicitly enables `allow_lock_with_conflict`
  (wrong write-conflict semantics for skip-locked) and is restricted to single-key
  requests; "wake-up mode" is also semantically wrong for requests that never sleep.
- Alternative considered: client-side emulation with per-key NOWAIT requests.
  Rejected: N round trips, statement-level failure semantics of NOWAIT, and no way to
  distinguish "locked" from other failures reliably.

## Unresolved Questions

- Whether TiKV's in-memory pessimistic lock optimization needs a separate audit for
  skipped keys (no lock is written, so presumably not).
- Exact minimum TiKV version constant (set when the TiKV PR lands in a release).
