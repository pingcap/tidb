# Lease Lock Instance Generation Design

## Context

The lease-lock change introduces automatic stale-lock cleanup. Fixed-path lock
files can be unsafe with that cleanup model because stale cleanup reads lock
metadata and later deletes the same path without a compare-and-delete primitive.

For a fixed-path lock, two waiters can observe the same old stale file. One
waiter can delete the stale file and acquire a fresh lock at the same path. The
other waiter can then continue its stale-cleanup flow and delete the fresh lock.

The design below keeps business callers on logical lock paths while making the
physical lock files instance based.

## Goals

- Keep business-facing lock APIs mostly unchanged.
- Prevent stale cleanup from deleting a freshly acquired fixed-path lock.
- Keep read-lock file naming close to the existing format.
- Cover standalone exclusive locks such as `truncating.lock`, not only
  migration `READ` and `WRIT` locks.
- Keep old lock files compatible.
- Avoid introducing a new storage API such as compare-and-delete.

## Non-Goals

- Do not redesign restore, truncate, or append business semantics.
- Do not require object stores to provide stronger primitives than the current
  `Storage` interface.
- Do not solve cross-host clock-skew semantics in this change. Current lease
  decisions use local process time; a future review may consider using PD time
  or another shared time source.

## File Naming

Callers continue to pass logical lock paths:

- `v1/LOCK`
- `v1/APPEND_LOCK`
- `truncating.lock`

The lock layer writes physical instance files by appending one generation
suffix field.

Suggested generation format:

```text
<unix_nano_hex><short_random_hex>
```

For example, with 16 hex chars for UnixNano and 8 hex chars for random bits:

```text
%016x%08x
```

This keeps the suffix as one field while making it time-sortable enough for
humans and collision-resistant enough for concurrent processes.

Physical names:

```text
READ lock       v1/LOCK.READ.<generation>
WRITE lock      v1/LOCK.WRIT.<generation>
APPEND lock     v1/APPEND_LOCK.WRIT.<generation>
Standalone lock truncating.lock.<generation>
```

No extra `EXCL` field is required for standalone locks. The logical path itself
is already the lock namespace, and adding only `<generation>` keeps the file-name
change minimal.

## Business API Surface

Most business code should continue to use the existing APIs:

- `TryLockRemote(ctx, storage, logicalPath, hint)`
- `TryLockRemoteRead(ctx, storage, logicalPath, hint)`
- `TryLockRemoteWrite(ctx, storage, logicalPath, hint)`
- `LockWithRetry(ctx, locker, storage, logicalPath, hint)`
- `RemoteLock.StartRenewal(ctx, onLeaseLost)`
- `RemoteLock.Unlock(ctx)`
- `RemoteLock.UnlockOnCleanUp(ctx)`

`RemoteLock.path` should store the current physical instance path. After acquire,
renewal and unlock operate on that physical path and do not require the business
caller to know the generated suffix.

The only business-facing API needing careful semantics is:

```go
CleanUpStaleLock(ctx, storage, logicalPath)
```

The current truncate path already calls `CleanUpStaleLock` with the logical
`truncating.lock` path. To avoid adding a new exported API unless necessary,
`CleanUpStaleLock` can be made family-aware:

- If passed a logical path, it scans and cleans stale instances in that logical
  lock family.
- It remains compatible with old exact fixed-path files.

Internal helpers may still split the implementation into "list lock family" and
"clean one physical lock file" operations.

## Lock Family Rules

A lock family is the set of physical files that represent one logical lock.

For a standalone logical path such as `truncating.lock`:

```text
truncating.lock              old fixed-path lock
truncating.lock.<generation> new instance lock
```

For an RW logical path such as `v1/LOCK`:

```text
v1/LOCK                     old fixed-path exclusive lock, if any
v1/LOCK.WRIT                old fixed-path write lock
v1/LOCK.WRIT.<generation>   new write lock
v1/LOCK.READ.<generation>   read lock
```

Intent files such as `.INTENT.` are not normal lock holders and should be
skipped by stale-lock family cleanup. They remain handled by the existing
conditional-put intent protocol.

Conflict checks should scan the whole family:

- A standalone lock conflicts with any live lock in its standalone family.
- A write lock conflicts with live read or write locks in the RW family.
- A read lock conflicts with live write locks in the RW family.
- During renewal migration, the caller's own current instance is allowed, but
  any other holder's live instance is a conflict.

## Acquire Flow

Standalone acquire:

1. Caller passes logical path, for example `truncating.lock`.
2. Lock layer generates `truncating.lock.<generation>`.
3. `conditionalPut` writes that physical target.
4. Its verify callback scans the logical family and rejects conflicting live
   locks.
5. Returned `RemoteLock.path` is the generated physical target.

Read and write lock acquire use the same pattern with `READ.<generation>` and
`WRIT.<generation>` physical names.

## Stale Cleanup

Stale cleanup should delete only physical instance paths that it has read and
classified as reclaimable. Because fresh holders use new paths, stale cleanup no
longer targets the same path a new holder just acquired.

Compatibility rules:

- Old lock files without `ExpireAt` are never auto-reclaimed.
- Old fixed-path lock files with `ExpireAt` may be reclaimed if they pass the
  existing stale threshold.
- New instance files use the same stale threshold.

`LockWithRetry` should use family-aware cleanup for its logical lock path.
`RunStreamTruncate` can keep calling `CleanUpStaleLock(ctx, storage,
truncateLockPath)` if that function becomes family-aware.

## Renewal Flow

Renewal should migrate to a new generation instead of overwriting the current
physical file in place.

Recommended flow:

1. Lock `RemoteLock.mu`.
2. Read `RemoteLock.path` and the remote metadata at that physical path.
3. Verify the remote `TxnID` matches the local lock.
4. Verify the lease has not already expired.
5. Generate a new physical path with a fresh generation.
6. Write the new physical lock file with `conditionalPut`.
7. In the verify callback, scan the family and allow only the caller's current
   old instance as an existing holder.
8. Update `RemoteLock.path` to the new physical path.
9. Best-effort delete the old physical path.
10. Unlock `RemoteLock.mu`.

If deleting the old path fails after the new path is written, renewal should log
the failure but keep the renewed lock. Later stale cleanup can reclaim the old
instance after it expires.

`Unlock` should also coordinate with `RemoteLock.mu` so it deletes the current
physical path and does not race with renewal path migration.

## S3 and Object Store Implications

The design only changes object keys. It does not require new S3 operations.

Expected changes:

- More operations rely on prefix listing for lock-family conflict checks and
  stale cleanup.
- Standalone locks such as `truncating.lock` move from one exact key to
  generated instance keys.
- Manual inspection should search by logical prefix rather than a single exact
  file name.

Migration read locks already use generated `READ` files, so the RW lock family
already relies on prefix-style listing. The main new listing impact is for
standalone locks.

## Compatibility

The lock layer must continue recognizing old lock files:

- `truncating.lock`
- `v1/LOCK.WRIT`
- any existing `v1/LOCK.READ.*`

Old files without `ExpireAt` are treated as old-client locks and remain valid
until manually removed. New cleanup should not delete them automatically.

Old files with `ExpireAt` follow the current lease cleanup rule. This covers
intermediate locks written by the current branch before the instance-generation
change.

## Tests To Add Or Update

- Fixed-path stale cleanup must not delete a fresh fixed-path holder. This is
  already represented by
  `TestCleanUpStaleLockDoesNotDeleteFreshFixedPathLock`.
- Standalone `TryLockRemote` should create `path.<generation>` and unlock that
  physical path.
- `CleanUpStaleLock(ctx, storage, "truncating.lock")` should reclaim stale
  `truncating.lock.<generation>` files.
- `TryLockRemoteWrite` should create `base.WRIT.<generation>` and still conflict
  with old `base.WRIT`.
- `TryLockRemoteRead` should conflict with both old `base.WRIT` and new
  `base.WRIT.<generation>`.
- Renewal should move a lock to a new generation and leave no live old instance
  after successful cleanup.
- Renewal should keep the new generation valid even if deleting the old
  generation fails.
