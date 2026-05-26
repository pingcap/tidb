# Lease Lock Business Review Notes

## 2026-05-14 business call-site review

Context: branch `feat/objstore-lease-based-lock-expiration` adds lease expiration,
renewal, and stale-lock cleanup to `pkg/objstore`. The low-level lock mechanics
have been reviewed separately. This note tracks remaining business-layer review
items for BR callers that acquire object-store locks.

### Review principle

Each business caller must be reviewed as an independent critical section:

1. Identify the lock or locks acquired.
2. Identify the exact business operations protected by the lock.
3. Confirm renewal starts before any operation that may outlive `LeaseTTL`.
4. Confirm lease-loss cancellation stops subsequent remote writes/deletes.
5. Confirm errors caused by lease loss are returned as failures, not only logged
   or collected as warnings.
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

- `RunStreamTruncate` starts renewal with `lock.StartRenewal(ctx, cancelFn)`.
  This is directionally correct because most truncate work uses the same `ctx`.
- The `--clean-up-compactions` path calls `MergeAndMigrateTo` and prints
  returned warnings before returning `nil`.
- Review whether `context.Canceled` caused by lease loss can be captured only as
  a warning and reported as command success. If so, convert lease-loss-induced
  cancellation into a returned error.

#### `br/pkg/stream/stream_metas.go`: `MergeAndMigrateTo`

- Status: confirmed issue; should be fixed, not merely reviewed.
- `MergeAndMigrateTo` acquires a write lock on `lockPrefix` but currently does
  not start renewal.
- The protected region can write a new BASE migration, delete merged migration
  files, execute `migrateTo`, and write BASE again.
- If that region exceeds the lease lifetime, another process may reclaim and
  acquire the lock while this caller continues writing/deleting.
- The risk is high because `migrateTo` may scan metadata, delete prefixes,
  delete log files, apply metadata edits, and rewrite BASE. This can outlive
  `LeaseTTL` on large log backup archives.
- Fix direction: start renewal immediately after acquiring the migration write
  lock, use lease loss to cancel the work context for the critical section, and
  ensure cancellation is surfaced to callers instead of being reported only as
  a warning.

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

- `AppendMigration` acquires a main read lock and an append write lock through
  `lockForAppend`.
- Neither lock currently starts renewal.
- The protected region loads migrations, computes the next sequence number, and
  writes a new migration file.
- Review should decide whether this region is short enough to avoid renewal. If
  not, both locks need renewal or a smaller critical section.

#### `br/pkg/restore/log_client/client.go`: `GetLockedMigrations`

- `GetLockedMigrations` acquires a read lock, then loads migrations, then
  returns the lock to `restoreStream`.
- `restoreStream` starts renewal only after `GetLockedMigrations` returns.
- This leaves the migration load phase outside renewal coverage.
- The returned `LockedMigrations` value contains both migration data and a lock
  resource that must be renewed and released. Naming the call-site variable
  `migs` makes this lifecycle obligation easy to miss.
- Review should either start renewal immediately after acquiring the lock or
  justify why the load phase cannot exceed the lease risk window.
- Review should also decide whether renewal ownership belongs inside
  `GetLockedMigrations`, with the business layer passing only a lease-loss
  callback such as the restore context cancel function.
- Review decision: renewal should start inside `GetLockedMigrations`
  immediately after `ext.GetReadLock` succeeds. Business callers should pass
  the lease-loss callback/cancel function and should not receive a locked
  migration value that still requires manual `StartRenewal`.
- Automatic review also flagged that `StartRenewal` waits until the first
  renewal interval before calling `tryRenew`. With the current layering, the
  first actual renewal can happen at roughly:
  `lock acquire + migration load + BuildMigrations + renew interval`.
  Even if renewal ownership is moved into `GetLockedMigrations`, consider an
  immediate renew/verify attempt before the interval loop.

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

- `Unlock` stops renewal and waits for `done` before deleting the lock file.
- This prevents a late renewal write from recreating a deleted lock.
- The wait is not itself bounded by the cleanup context; if a storage operation
  inside the renewal goroutine blocks indefinitely, cleanup can block too.
- This is an infrastructure risk exposed by business callers that start renewal.

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
| `RunStreamTruncate` | `br/pkg/task/stream.go` | standalone truncate instance lock | fixed-path stale cleanup race resolved; lease-loss warning/success behavior still needs review |
| `restoreStream` / `GetLockedMigrations` | `br/pkg/task/stream.go`, `br/pkg/restore/log_client/client.go` | migration read lock | renewal should start inside `GetLockedMigrations`; restore sub-step cancellation is out of scope |
| `AppendMigration` | `br/pkg/stream/stream_metas.go` | migration read lock + append write lock | no renewal during append critical section |
| `MergeAndMigrateTo` | `br/pkg/stream/stream_metas.go` | migration write lock | no renewal during destructive critical section |

### Suggested review order

1. Done: fixed the fixed-path stale cleanup race in the lock layer with
   instance/generation-based physical paths. Standalone truncate locks are
   included in this design.
2. Move restore read-lock renewal ownership into `GetLockedMigrations`, with a
   caller-provided lease-loss callback. Start renewal immediately after
   `ext.GetReadLock` succeeds and before reading `v1/migrations/`.
3. Review whether `StartRenewal` should immediately renew or verify before
   waiting for the first interval. This is lock-layer behavior and complements
   item 2.
4. Review `MergeAndMigrateTo`; it has the largest destructive migration write
   surface and still needs renewal.
5. Review `AppendMigration`; it protects sequence allocation and writing a new
   migration with a main read lock plus append write lock.
6. Keep `Unlock` waiting for renewal goroutine teardown, but separately decide
   whether the wait needs a bounded context/deadline. This is lower priority
   cleanup robustness.
