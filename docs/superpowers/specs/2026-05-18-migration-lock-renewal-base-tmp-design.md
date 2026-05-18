# Migration Lock Renewal and BASE_TMP Recovery Design

Date: 2026-05-18

## Context

This branch adds lease expiration and renewal to object-store based locks. During the business review of log-backup truncate and restore paths, two confirmed issues were found in the migration maintenance path used by `br log truncate --clean-up-compactions`.

First, `MigrationExt.MergeAndMigrateTo` acquires a migration write lock but does not renew it. The function may run for a long time while loading migration state, writing `BASE`, deleting merged migration layers, truncating log files, deleting expired compaction artifacts, and writing the final `BASE`. If the write lock lease expires while these remote mutations are still running, another task can consider the lock stale and enter the same migration critical section.

Second, `writeBase` writes `v1/migrations/BASE_TMP` and then renames it to `v1/migrations/BASE`. Object-store rename is implemented as a non-atomic operation on some backends, usually equivalent to copying or writing the destination and deleting the source. If the destination is written but deleting `BASE_TMP` fails, a later `MigrationExt.Load` scans `BASE_TMP`, tries to parse it as a migration id, and fails.

## Goals

- Keep the migration write lock alive for the full `MergeAndMigrateTo` critical section.
- Stop migration remote mutations promptly if the migration write lock is lost.
- Make `MigrationExt.Load` tolerate leftover `BASE_TMP` files.
- Keep the existing warning-based `MergeAndMigrateTo` result semantics.
- Keep the fix narrow and avoid unrelated migration or truncate behavior changes.

## Non-Goals

- Do not change `MergeAndMigrateTo` to return an error instead of warnings.
- Do not change truncate CLI success or failure semantics.
- Do not add read-path deletion of `BASE_TMP`.
- Do not refactor migration load, merge, or truncate algorithms.
- Do not address the restore read-lock renewal window in this change.

## Design

### Migration Write Lock Renewal

After `MergeAndMigrateTo` acquires the migration write lock, it will derive a work context from the caller context:

```go
workCtx, cancel := context.WithCancel(ctx)
defer cancel()
lock.StartRenewal(workCtx, cancel)
ctx = workCtx
```

The rest of `MergeAndMigrateTo` continues to use `ctx`, so `Load`, `writeBase`, `DeleteFile`, `migrateTo`, and nested remote storage operations observe cancellation when renewal determines the lease is lost.

The outer truncate lock and the inner migration write lock do not directly unlock each other. They coordinate through context cancellation and existing deferred cleanup:

- If the outer truncate lock is lost, `RunStreamTruncate` cancels its context, and `MergeAndMigrateTo` receives cancellation through its parent context.
- If the inner migration write lock is lost, `MergeAndMigrateTo` cancels its work context and returns through the existing warning path.
- Function exit then runs the existing deferred lock cleanup.

`UnlockOnCleanUp` remains the cleanup mechanism for the migration write lock. It already switches to a background timeout when the supplied context has been canceled, so cleanup remains best effort even after lease-loss cancellation.

### BASE_TMP Recovery

`MigrationExt.Load` will ignore `BASE_TMP` while scanning `v1/migrations`.

This makes `Load` robust when a previous `writeBase` finished writing `BASE` but failed to remove `BASE_TMP`. The temporary object is not a migration layer and should not participate in ordering or parsing.

`writeBase` will not explicitly delete old `BASE_TMP` before writing a new one. The `Storage.WriteFile` contract is equivalent to an atomic `os.WriteFile` and overwrites complete file contents. Existing backends such as local, memory, S3-like, GCS, and Azure follow this overwrite behavior. The next `writeBase` naturally overwrites any stale `BASE_TMP`.

This keeps the read path side-effect-free and avoids adding extra remote operations to the write path.

## Testing Plan

Add focused regression coverage in the existing stream migration tests.

1. `MergeAndMigrateTo` renews its migration write lock:
   - Configure a short lease TTL.
   - Start `MergeAndMigrateTo`.
   - Block it after the write lock is acquired, for longer than one lease TTL.
   - Attempt to acquire the same migration write lock from another goroutine.
   - Verify the second acquisition still fails while the first operation is alive, proving renewal keeps the lock valid.

2. `Load` ignores leftover `BASE_TMP`:
   - Write a valid `BASE`.
   - Write a valid but temporary `BASE_TMP`.
   - Call `MigrationExt.Load`.
   - Verify `Load` succeeds and returns the expected base/layers without treating `BASE_TMP` as a migration.

Run targeted stream migration tests after implementation. If package failpoints are required for the selected test package, use the repository failpoint test runner workflow.

## Risks

- If renewal cancellation is observed only after a remote operation returns, one in-flight operation may still finish. This matches the existing context-based cancellation model and is still safer than continuing the whole critical section after lease loss.
- `MergeAndMigrateTo` currently reports most failures as warnings. This design preserves that behavior, so callers may still print warnings and return nil. Changing that is a separate business decision.
- Ignoring `BASE_TMP` assumes it is always a temporary write artifact and never a valid migration layer. This matches the existing constants and filename parser expectations.

## Acceptance Criteria

- `MergeAndMigrateTo` starts renewal for the migration write lock immediately after acquiring it.
- Lease loss on the migration write lock cancels the work context used by the rest of `MergeAndMigrateTo`.
- `MigrationExt.Load` ignores `v1/migrations/BASE_TMP`.
- Existing behavior for result warnings and truncate CLI return values is preserved.
- Regression tests cover both the write-lock renewal and `BASE_TMP` recovery behavior.
