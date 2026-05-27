# Lease Lock Business Review Notes

## 2026-05-14 business call-site review

Context: branch `feat/objstore-lease-based-lock-expiration` adds lease expiration,
renewal, and stale-lock cleanup to `pkg/objstore`. The low-level lock mechanics
have been reviewed separately. This note tracks business-layer review decisions
for BR callers that acquire object-store locks.

### Review principle

Each business caller must be reviewed as an independent critical section:

1. Identify the lock or locks acquired.
2. Identify the exact business operations protected by the lock.
3. Confirm renewal starts before any operation that may outlive `LeaseTTL`.
4. Confirm lease-loss cancellation stops subsequent remote writes/deletes.
5. Confirm lease-loss cancellation prevents subsequent protected remote
   writes/deletes from continuing under a lost lease. If the business flow is an
   existing best-effort/retryable warning model, document why warning-only
   propagation is acceptable instead of converting it to a hard error.
6. Confirm unlock happens once, late enough, and with a context that can still
   perform cleanup.

Scope refinement for restore review:

- This review is about adding leases to the existing migration lock, not
  redesigning restore's business use of migration data.
- Treat the pre-existing read-lock critical section as intentional. Do not
  reopen whether every restore sub-step should or should not use the migration
  view unless it newly affects lease correctness.
- In restore, the main lease-lock concern is whether code that directly reads
  `v1/migrations/` is covered by renewal, and whether lock-layer renewal,
  cleanup, and stale reclaim are safe.
- General restore cancellation semantics are out of scope. `context.CancelFunc`
  is a shared cancellation mechanism and not exclusively a migration-lease-loss
  signal.

### Known findings

#### `br/pkg/task/stream.go`: truncate lock cleanup and acquire

- Status: resolved in current branch for the missing-file cleanup behavior and
  for the fixed-path stale cleanup race.
- `RunStreamTruncate` uses a standalone truncate lock at `truncateLockPath`
  rather than the migration read/write lock family.
- The original acquire behavior was preserved: cleanup once, acquire once, and
  return on conflict. It does not use `LockWithRetry`.
- Preserve that acquire behavior unless the user-visible retry semantics are
  intentionally changed.
- The current code uses `CleanUpStaleTruncateLock` and
  `TryLockRemoteTruncate`. Truncate acquire now creates
  `truncating.lock.<32hex>` physical instances, and cleanup only reclaims stale
  new-format truncate instances.
- Legacy fixed-path truncate locks, malformed new-format candidates, intents,
  and unknown protected-prefix objects are treated conservatively: they are not
  auto-deleted by cleanup.

#### `br/pkg/task/stream.go`: truncate lease loss and warnings

- Status: reviewed and kept as the existing best-effort/retryable warning
  semantics.
- `RunStreamTruncate` keeps the existing cleanup-once/acquire-once behavior, but
  now acquires via `LockRemoteTruncate(ctx, ..., cancelFn)`, so renewal starts
  inside the lifecycle acquire API rather than through a business-layer
  `StartRenewal` call.
- The `--clean-up-compactions` path calls `MergeAndMigrateTo` and prints
  returned warnings before returning `nil`.
- This warning-only result handling matches `origin/master`. The reviewed
  business model is that `MergeAndMigrateTo` is a best-effort flow: unfinished
  operations are represented in the new BASE/migration state for a later retry.
  The current branch does not convert those warnings to command-level hard
  errors.

#### `br/pkg/stream/stream_metas.go`: `MergeAndMigrateTo`

- Status: renewal ownership and lease-loss warning semantics reviewed in current
  branch.
- `MergeAndMigrateTo` acquires a write lock on `lockPrefix` through
  `LockWithRetry(workCtx, ..., cancel)`. The lifecycle acquire API starts
  renewal before returning the lock handle.
- The protected region can write a new BASE migration, delete merged migration
  files, execute `migrateTo`, and write BASE again.
- Lease loss cancels the child work context used by the destructive critical
  section. Internal review confirmed the protected remote reads/writes/deletes
  use that child context rather than an uncanceled outer context.
- Context propagation note: `context.WithCancel(ctx)` creates an internal child
  context for the critical section. Calling that child cancel function does not
  cancel the caller's parent context. This is acceptable here because
  `MergeAndMigrateTo` already uses a best-effort/retryable warning model: failed
  operations are preserved for a later retry rather than treated as an atomic
  command failure.
- Interactive confirmation may block on user input after the write lock is
  acquired. The current branch keeps that behavior, but `MergeAndMigrateTo`
  explicitly checks `ctx.Err()` after the interactive check and before any BASE
  write/delete/migrate operation, so lease loss during the prompt cannot proceed
  into the destructive phase.

#### `br/pkg/stream/stream_metas.go`: `BASE_TMP` recovery

- Status: resolved in current branch for retry correctness.
- `writeBase` writes `v1/migrations/BASE_TMP` and then renames it to
  `v1/migrations/BASE`.
- Object-store `Rename` implementations are copy/write/delete style rather
  than atomic filesystem renames. If the process exits, storage fails, or the
  work context is canceled between writing `BASE_TMP` and deleting it, a
  `BASE_TMP` object can remain under `v1/migrations/`.
- `MigrationExt.Load` now ignores `BASE_TMP`, so a leftover temp file should not
  make later `Load` calls fail and block normal retry.
- This is not introduced by lease renewal, but lease-loss cancellation makes
  the interruption window more important.
- Optional cleanup may still try to delete stale `BASE_TMP`, but retry
  correctness should not depend on that deletion succeeding.

#### `br/pkg/stream/stream_metas.go`: `AppendMigration`

- Status: renewal ownership resolved in current branch.
- `AppendMigration` acquires a main read lock and an append write lock through
  `lockForAppend`.
- The protected region loads migrations, computes the next sequence number, and
  writes a new migration file.
- `AppendMigration` now creates a child work context and passes its cancel
  function to both lock acquisitions. `LockWithRetry` starts renewal for both
  the main read lock and append write lock before returning.

#### `br/pkg/restore/log_client/client.go`: `GetLockedMigrations`

- Status: renewal ownership resolved in current branch.
- `GetLockedMigrations(ctx, onLeaseLost)` acquires a read lock and starts
  renewal through `ext.GetReadLock` / `LockWithRetry` before loading
  migrations.
- `restoreStream` now passes its lease-loss cancel function into
  `GetLockedMigrations` and no longer calls `StartRenewal` directly.
- The returned `LockedMigrations` value hides the internal read lock and exposes
  `Unlock(ctx)`, so callers release the locked resource without controlling the
  renewal goroutine directly.
- Review decision on immediate renew: do not add an acquire-time immediate
  `tryRenew` or verify. Acquire already writes a fresh `ExpireAt`; the
  correctness issue was late ownership start, not the first renewal cadence.

#### `br/pkg/task/stream.go`: restore lease-loss context propagation

- Status: narrowed. Most restore sub-steps are pre-existing business logic and
  are not part of this lease-lock review unless they directly access
  `v1/migrations/` or change lock lifetime.
- `restoreStream` creates a child context and passes its cancel function to
  migration read-lock renewal. Lease loss is expected to cancel this child
  context and stop subsequent non-cleanup remote writes.
- `LoadDDLFiles`, DDL/meta restore, DML restore, SST restore, checkpoint
  flushing, and GC changes may have their own restore cancellation semantics,
  but they are not migration-lock lease issues unless they directly access
  `v1/migrations/` or alter lock ownership.
- `DisableGC` runs after the read lock is acquired, but it does not depend on
  the migration directory or migration-derived file view. Its background
  context behavior is a general restore cancellation / cleanup semantics issue,
  not a migration-lock correctness issue for this review.
- Lower-risk or expected exceptions:
  - `RestorePostWork` deliberately switches to a background context when the
    child context is canceled so it can restore TiKV normal mode and PD
    schedulers.
  - Read-lock cleanup uses a background timeout and is expected cleanup.
  - KV log restore and PD split/scatter paths appear to use the child context.
  - Post-restore SQL/DDL calls mostly use the child context, though DDL already
    accepted by TiDB may continue server-side.

Automatic review follow-up, after scope refinement:

- Out of scope for this lease-lock review: SST import uses a construction-time
  context from `createLogClient` / `InitClients`, not the lease-cancelable child
  context passed to `RestoreSSTFiles`. This is a restore cancellation question,
  not a direct migration-directory access question.
- Out of scope for this lease-lock review: DDL/meta discovery and file
  filtering may be migration-blind. That is a restore business-semantics
  question and predates lease-based expiration.
- Out of scope for this lease-lock review: checkpoint runners are created with
  the outer context before the lease-cancelable child context exists. Close-time
  flushing should be evaluated as checkpoint/restore cleanup semantics.
- Out of scope for this migration-lock review: `DisableGC` uses GC helpers that
  rely on background contexts. This may be a restore cancellation semantics
  issue, but it is not caused by or specific to migration-lock protection.
- Out of scope for this migration-lock review: restore registry heartbeat is
  started before `restoreStream` and is not stopped by migration read-lock lease
  loss. This is registry liveness behavior, not direct migration-directory
  access.

#### `pkg/objstore/locking.go`: unlock waits for renewal goroutine

- Status: reviewed and kept as-is in the current branch.
- `Unlock` stops renewal and waits for `done` before deleting the lock file.
- This prevents a late renewal write from recreating a deleted lock.
- The wait is not itself bounded by the cleanup context; if a storage operation
  inside the renewal goroutine blocks indefinitely, cleanup can block too.
- This is an infrastructure risk exposed by business callers that start renewal.
- Review decision: do not change this now. Keeping cleanup ordered behind
  renewal teardown is more important for the current lease-lock correctness
  work; bounded wait behavior can be revisited only with a separate cleanup
  semantics design.

#### `pkg/objstore/locking.go`: stale cleanup race

- Status: resolved in current branch by the instance/generation-based lock
  naming change. See `lease-lock-instance-generation-design.md`,
  `lease-lock-family-acquire-protocol.md`, and
  `lease-lock-instance-generation-implementation-plan.md`.
- Automatic review raised this as a high-risk infrastructure issue.
- `CleanUpStaleLock` reads stale lock metadata and later deletes the lock path
  with an unconditional `DeleteFile`; the current storage interface does not
  expose compare-and-delete.
- `LockWithRetry` can call stale cleanup from multiple waiters after failed
  acquire attempts. For fixed-path locks, two waiters may both observe an old
  stale lock; one deletes it and acquires a fresh lock, then the other deletes
  the fresh lock using its stale cleanup decision.
- If confirmed, mutual exclusion can be broken. A safe fix likely needs a
  conditional delete / CAS-style reclaim protocol, not only a best-effort
  re-read before delete.
- The same fixed-path risk applies to standalone locks such as the truncate
  lock if they use `TryLockRemote` directly and then rely on automatic stale
  cleanup. Fixing only migration `*.WRIT` locks would leave truncate with the
  same cleanup race.
- Final fix direction: make fixed locks instance/generation based instead of
  rewriting and deleting one stable object path. A random suffix, similar to
  read locks, prevents stale cleanup from deleting a newly acquired lock at the
  same path.
- Implemented behavior: acquire writes a new physical instance path such as
  `v1/LOCK.WRIT.<32hex>`, `v1/LOCK.READ.<32hex>`,
  `v1/APPEND_LOCK.WRIT.<32hex>`, or `truncating.lock.<32hex>`. Renewal and
  unlock operate on the acquired physical instance path.
- `LockWithRetry` now performs family-aware cleanup and only deletes stale
  cleanup-eligible new 32 hex committed instances. Acquire verification remains
  separate from cleanup: it scans the lock family for conflicts but does not
  read lock metadata, check `ExpireAt`, or delete objects.
- Superseded earlier hypothesis: renewal should not move to a new generation.
  The finalized design generates a new physical instance path only during
  acquire; renewal refreshes `ExpireAt` in place on the current instance. See
  `lease-lock-instance-generation-design.md` and
  `lease-lock-family-acquire-protocol.md`.
- Compatibility note: old fixed-path lock files, old 16 hex read locks,
  intents, malformed new-format candidates, and unknown protected-prefix
  objects are not auto-reclaimed. New conflict scans still treat old fixed-path
  write locks as valid conflicting locks.
- Follow-up, out of scope for the immediate file-name fix: all current lease
  expiration decisions use the local process clock. Review later whether BR
  should obtain lock lease time from PD, or at least make part of the lease
  protocol use a shared PD-derived time source, to reduce cross-host clock-skew
  ambiguity. This is a separate lease-correctness discussion and should not
  block the fixed-path stale-cleanup race fix.

### Business call sites to review

| Caller | File | Lock type | Current concern |
| --- | --- | --- | --- |
| `RunStreamTruncate` | `br/pkg/task/stream.go` | standalone truncate instance lock | fixed-path stale cleanup race resolved; warning-only truncate retry semantics reviewed and kept |
| `restoreStream` / `GetLockedMigrations` | `br/pkg/task/stream.go`, `br/pkg/restore/log_client/client.go` | migration read lock | renewal ownership resolved; restore sub-step cancellation is out of scope |
| `AppendMigration` | `br/pkg/stream/stream_metas.go` | migration read lock + append write lock | renewal ownership resolved |
| `MergeAndMigrateTo` | `br/pkg/stream/stream_metas.go` | migration write lock | renewal ownership resolved; best-effort warning/retry semantics reviewed and kept |

### Suggested review order

1. Done: fixed the fixed-path stale cleanup race in the lock layer with
   instance/generation-based physical paths. Standalone truncate locks are
   included in this design.
2. Done: moved restore read-lock renewal ownership into
   `GetLockedMigrations`, with a caller-provided lease-loss callback.
3. Done: reviewed immediate renew/verify and kept the existing interval
   behavior. Acquire writes a fresh `ExpireAt`; lifecycle ownership now starts
   immediately after acquire.
4. Done: moved `MergeAndMigrateTo` write-lock renewal into `LockWithRetry` and
   tied lease loss to the child work context.
5. Done: moved `AppendMigration` read-lock and append-write-lock renewal into
   `LockWithRetry` and tied lease loss to the child work context.
6. Done: keep `Unlock` waiting for renewal goroutine teardown. Do not add a
   bounded wait in the current branch.
7. Done: reviewed `MergeAndMigrateTo` / truncate warning-only behavior against
   `origin/master`. Keep the existing best-effort retry semantics instead of
   converting lease-loss-induced warnings into command-level hard errors.
