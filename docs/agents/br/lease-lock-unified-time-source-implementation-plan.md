# Lease Lock Unified Time Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make BR lease-lock correctness paths use PD-backed lease time while keeping `pkg/objstore` independent of PD.

**Architecture:** BR upper layers construct a small PD-backed lease clock object and pass it down through stream/lock call paths. `pkg/objstore` consumes only an explicit `LeaseClock` interface and uses it for acquire, renewal, and cleanup lease decisions. Temporary local-time compatibility is explicit via `objstore.NewLocalLeaseClock()`.

**Tech Stack:** Go, `pkg/objstore`, `br/pkg/stream`, `br/pkg/restore/log_client`, `br/pkg/restore/snap_client`, `br/pkg/task`, PD client `GetTS`, `oracle.GetTimeFromTS`, failpoint-enabled objstore tests, targeted BR package tests, `make bazel_prepare`.

---

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

BR uses object-store lock files to coordinate migration metadata operations. The lock metadata contains `LockedAt` and `ExpireAt`, and today those lease decisions are based on local wall-clock time in `pkg/objstore`. Different BR processes on different hosts can therefore make lease decisions from skewed clocks.

After this plan, BR correctness paths obtain lease time from PD TSO physical time and pass that lease clock down to the lock layer. The lock layer remains a storage protocol package and does not import PD packages. This phase removes cross-host clock skew from lock timestamps and lease-expiration decisions. It does not implement cleanup tombstones, local freshness watchdogs, or the full delayed renewal-write protocol.

## Progress

- [x] (2026-05-28) Preserved older broad WIP on `temp/lease-lock-current-reference-20260528`.
- [x] (2026-05-28) Reset main branch to `origin/feat/objstore-lease-based-lock-expiration` and re-applied only the delayed-write analysis document.
- [x] (2026-05-28) Wrote draft unified time source spec in `docs/agents/br/lease-lock-unified-time-source-spec.md`.
- [x] (2026-05-28) Independent review found the plan feasible and recommended adding PiTR collector coverage, PD TSO timing caveats, and explicit `LockWithRetry` coupling notes.
- [x] (2026-05-28) Revised the design from a function-shaped time source to a small `LeaseClock` interface with a BR-side PD clock object.
- [ ] Commit the unified time source spec and this implementation plan as the planning baseline.
- [x] (2026-05-28) Implemented Milestone 1: objstore acquire lease-clock API and post-acquire proof. Commit is pending review.
- [x] (2026-05-28) Addressed Milestone 1 review: detached post-acquire cleanup from canceled caller contexts, kept `LeaseClock` as a stable one-method interface, kept the lease-time meta constructor internal, and added read/truncate explicit-clock coverage.
- [x] (2026-05-28) Implemented Milestone 2: objstore renewal uses the same lease clock. Commit is pending review.
- [x] (2026-05-28) Addressed Milestone 2 review: renewal now treats a missing `RemoteLock` lease clock as invalid construction and fails closed instead of silently falling back to local time.
- [x] (2026-05-28) Implemented Milestone 2.5: pre-M3 explicit-clock API boundary hardening.
- [x] (2026-05-28) Implemented Milestone 3: objstore stale cleanup and retry acquisition use the explicit lease clock.
- [x] (2026-05-28) Implemented Milestone 4: BR PD lease clock wrapper reuses existing `GetTSWithRetry`.
- [x] (2026-05-28) Implemented Milestone 4.5: collapsed objstore clock APIs into the original function names with explicit clock parameters.
- [x] (2026-05-28) Implemented Milestone 5: `MigrationExt`, `LogClient`, and PiTR collector propagation.
- [x] (2026-05-28) Implemented Milestone 6: stream truncate propagation.
- [ ] Run Ready validation before claiming the phase is complete.

## Pause Snapshot: Unified-Time Work Before TTL Re-evaluation

Status captured on 2026-05-28 before discussing whether to raise `LeaseTTL` from 5 minutes to 30-40 minutes.

Current branch state:

- Branch: `feat/objstore-lease-based-lock-expiration`.
- Status at snapshot time: clean worktree, ahead of `origin/feat/objstore-lease-based-lock-expiration` by 6 commits.
- Relevant commits:

        99daa16a0b docs: analyze lease lock delayed renewal writes
        282ed9ead9 pkg/objstore: add lease clock acquire path
        4f3f8d2f6d pkg/objstore: renew locks with lease clock
        9d88146e59 pkg/objstore: reject missing lease clock on renew
        5165027848 pkg/objstore: harden lease clock acquire boundary
        a78f5a3c1e pkg/objstore: clean stale locks with lease clock

Completed scope:

- `pkg/objstore` now has explicit `LeaseClock` acquire APIs for write/read/truncate and append write paths.
- Acquired locks store the same `LeaseClock` on `RemoteLock`.
- Renewal uses `RemoteLock.leaseClock` for both the old `ExpireAt` check and refreshed `ExpireAt`.
- Renewal fails closed if a lock reaches renewal without a lease clock.
- Stale cleanup and retry acquisition have explicit-clock entry points.
- After Milestone 4.5, exported objstore lock APIs require an explicit `LeaseClock`; temporarily unmigrated callers pass `objstore.NewLocalLeaseClock()` explicitly.

Not yet implemented:

- BR-side PD-backed `LeaseClock` helper.
- Propagation through `MigrationExt`, `LogClient`, PiTR collector, and stream truncate.
- Any production caller migration to the explicit-clock APIs.
- Renewal post-write proof or a full delayed renewal-write protocol.

Important open correctness notes:

- Acquire has post-acquire proof; renewal does not yet have post-write proof.
- Even with a longer TTL, S3 writes do not have a lock-layer hard timeout today. A stuck `PutObject` can still exceed the lease window unless callers provide a deadline or the lock layer adds one.
- A longer TTL simplifies the PD-clock retry design, but it also lengthens crash recovery because cleanup currently waits until `ExpireAt.Add(LeaseTTL)`.

## TTL Policy Decision

Accepted policy:

    LeaseTTL = 60 * time.Minute
    renewInterval = LeaseTTL / 3
    stale cleanup threshold = ExpireAt + LeaseTTL

Expected implications:

- A newly acquired or renewed lock is valid for about 60 minutes.
- Renewal runs about every 20 minutes.
- A single PD `GetTS` retry sequence using `utils.NewAggressivePDBackoffStrategy()` has about 57.1 seconds of worst-case sleep time, plus RPC time. Under a 60 minute TTL this is much less dangerous than under the original 5 minute TTL.
- A crashed holder's lock becomes automatically reclaimable only after about 120 minutes in the worst case.
- The longer TTL reduces pressure to implement complicated local freshness watchdogs or renewal post-write proof before the first PD-clock migration, but it does not eliminate the delayed-write correctness question.

Follow-up questions after accepting the TTL change:

- Should cleanup keep using `ExpireAt + LeaseTTL`, or should the reclaim grace become a separate constant if `LeaseTTL` grows?
- Should lock-layer storage operations get their own operation timeout, since S3 retry backoff is bounded but a single slow request is not bounded here?

## Surprises & Discoveries

- Observation: `conditionalPut.Content` cannot return an error, so acquire must obtain time before constructing the content closure.
  Evidence: `pkg/objstore/locking.go` defines `Content func(txnID uuid.UUID) []byte`.
- Observation: `LockWithRetry` owns more than acquire. It also runs stale cleanup and starts renewal, so a lease clock cannot be wrapped around only the `Locker` closure.
  Evidence: `pkg/objstore/locking.go` calls stale cleanup inside the retry loop and starts renewal after acquire.
- Observation: PiTR collector append-migration path also uses `MigrationExt` locks and was missing from the first spec draft.
  Evidence: `br/pkg/restore/snap_client/pitr_collector.go` has `PiTRCollDep.PDCli` and calls `MigrationExtension(...).AppendMigration`.
- Observation: PD TSO physical time is allocated during `GetTS`; it is not exactly the local receipt-time instant of the RPC.
  Evidence: independent design review; use it as a shared lease timestamp, not as object-storage linearization proof.
- Observation: `tryRenew` must not repair a nil `RemoteLock.leaseClock` with `localLeaseClock`; that hides invalid construction and can silently reintroduce local time into migrated paths.
  Evidence: review of `RemoteLock.tryRenew` after Milestone 2 and `TestTryRenewWithoutLeaseClockFails`.
- Observation: migrated BR paths need explicit-clock retry and cleanup entry points, not only explicit-clock single-attempt acquire APIs.
  Evidence: independent review found current `LockWithRetry` still owns cleanup and starts renewal while accepting only the legacy `Locker` shape.
- Observation: renewal may need a post-write lease proof similar to acquire if delayed object-store writes are in scope for this phase.
  Evidence: independent review noted `tryRenew` obtains lease time before `WriteFile` and currently returns success immediately after the write.
- Observation: `LogClient.GetLockedMigrations` should not construct a PD clock ad hoc from `rc.pdClient` inside the method.
  Evidence: storage-only log-client tests intentionally build `LogClient` without a PD client; direct construction from `rc.pdClient` caused `PDLeaseClock.Now` to call `GetTSWithRetry` with a nil client and panic. `LogClient` now carries an explicit `LeaseClock`, set to PD time in `NewLogClient` and local time only in test helpers.

## Decision Log

- Decision: Use a minimal explicit `LeaseClock` instead of `LockOption`, a lock manager/client, or strong-consistency gating.
  Rationale: Current lease-time call sites only need to obtain current lease time, and a stable one-method interface keeps the dependency narrow. Future capabilities should use a new interface, optional extension interface, or separate parameter rather than adding methods to `LeaseClock`.
  Date/Author: 2026-05-28 / design discussion.
- Decision: Keep `pkg/objstore` free of PD dependencies.
  Rationale: BR upper layers already have PD clients or PD/TLS config. The object-store package should consume time but not own cluster connectivity.
  Date/Author: 2026-05-28 / design discussion.
- Decision: Exclude manual `br operator migrate-to` from this phase.
  Rationale: It is a manual storage-only operator command whose config currently has no PD/TLS fields. Migrating it would create a separate command-design discussion.
  Date/Author: 2026-05-28 / design discussion.
- Decision: Collapse the temporary `WithLeaseClock` objstore APIs back into the original function names and require a `LeaseClock` parameter.
  Rationale: Once acquire, renewal, retry cleanup, and stale cleanup all use the explicit clock, two parallel public APIs add confusion. Compatibility paths should pass `objstore.NewLocalLeaseClock()` explicitly instead of relying on hidden fallback behavior.
  Date/Author: 2026-05-28 / design discussion.
- Decision: Put the PD lease-clock wrapper in `br/pkg/restore` and make it call the existing `GetTSWithRetry`.
  Rationale: BR already has `GetTS` and `GetTSWithRetry` in `br/pkg/restore`; wrapping that logic avoids a new package and avoids duplicating aggressive PD retry behavior. Lower-level `br/pkg/stream` will still only receive `objstore.LeaseClock` and will not import PD or restore helpers.
  Date/Author: 2026-05-28 / design discussion.

## Outcomes & Retrospective

Milestone 1 delivered `objstore.LeaseClock`, an internal lock-meta constructor that accepts lease time, explicit acquire APIs with the `WithLeaseClock` suffix, generation timestamps based on the lease clock, and post-acquire proof that deletes the just-written lock if the lease is already expired before returning. Legacy single-attempt acquire APIs now wrap the explicit-clock variants with a local compatibility clock.

Milestone 1 validation:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteWrite' -count=1
    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteWriteCleansUpWhenPostAcquireClockFailsWithCanceledContext' -count=1
    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteWrite|TestTryLockRemoteWrite|TestTryLockRemoteRead|TestTryLockRemoteTruncate|TestMakeLockMeta' -count=1
    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemote|TestConflictLock|TestConcurrentLock' -count=1
    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemote.*WithLeaseClock|TestTryLockRemoteWrite|TestTryLockRemoteRead|TestTryLockRemoteTruncate|TestMakeLockMeta|TestConflictLock|TestConcurrentLock' -count=1
    make bazel_prepare

Remaining gaps after Milestone 1: renewal and stale cleanup still use local time until Milestones 2 and 3.

Milestone 2 delivered `RemoteLock.leaseClock` and changed `tryRenew` to call that clock once per renewal attempt. The same lease time is used for both the existing `ExpireAt` check and the refreshed `ExpireAt = leaseNow.Add(LeaseTTL)`. Clock errors are returned as transient renewal errors and do not rewrite lock metadata.

Milestone 2 review tightened fail-closed behavior: `tryRenew` now returns `lease clock is required` when a `RemoteLock` has no lease clock instead of silently installing `localLeaseClock{}`. Legacy acquire wrappers still construct locks with `localLeaseClock{}` for compatibility; the invalid case is a lock object that reaches renewal without any clock at all.

Milestone 2 validation:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryRenewWithLeaseClock' -count=1
    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryRenewWithoutLeaseClockFails' -count=1
    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryRenew|TestStartRenewal|TestLockWithRetryStartsRenewal|TestLockRemoteTruncate' -count=1
    make bazel_prepare

Remaining gaps after Milestone 2: stale cleanup still uses local time until Milestone 3, `LockWithRetry` still has only the legacy locker shape, append explicit-clock acquire needs targeted coverage, and renewal post-write proof needs a separate decision before production migration.

Milestone 2.5 hardened the explicit-clock acquire boundary before cleanup migration. The internal exact-acquire helper now rejects nil lease clocks before writing any lock file, append write acquire has explicit-clock metadata and generation coverage, and Milestone 3 now has a concrete `LockerWithLeaseClock` / `LockWithRetry` API shape to implement together with cleanup clock threading.

Milestone 2.5 validation:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteExactWithLeaseClockRejectsNilClock' -count=1
    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteExactWithLeaseClockRejectsNilClock|TestTryLockRemoteAppendWriteWithLeaseClockUsesClockForMetaAndGeneration|TestTryLockRemoteWrite' -count=1
    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemote.*WithLeaseClock|TestTryLockRemoteWrite|TestTryLockRemoteRead|TestTryLockRemoteTruncate|TestMakeLockMeta' -count=1
    make bazel_prepare

Remaining gaps after Milestone 2.5: stale cleanup still uses local time until Milestone 3, `LockWithRetry` is designed but not implemented, and renewal post-write proof still needs a separate delayed-write correctness decision before production migration.

Milestone 3 delivered `CleanUpStaleTruncateLock`, `LockerWithLeaseClock`, `LockWithRetry`, and `LockRemoteTruncateWithLeaseClock`. At that point, legacy cleanup/retry/truncate APIs remained local-clock wrappers until Milestone 4.5 removed the parallel public API shape. Stale cleanup now uses the supplied lease clock for `ExpireAt.Add(LeaseTTL)` decisions; clock failure does not delete the candidate and is returned by the explicit cleanup API. Retry cleanup logs candidate errors and continues the retry loop.

Milestone 3 validation:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestCleanUpStaleTruncateLock|TestLockWithRetry' -count=1
    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestCleanUpStaleTruncateLock|TestLockWithRetry|TestLockRemoteTruncate' -count=1
    make bazel_prepare

Remaining gaps after Milestone 3: BR callers still need explicit clock propagation before PD time can replace local compatibility behavior, and renewal post-write proof remains a separate delayed-write correctness decision before production migration.

Milestone 4 delivered `restore.NewPDLeaseClock`, which returns an `objstore.LeaseClock` backed by the existing `restore.GetTSWithRetry`. The helper converts the returned TSO with `oracle.GetTimeFromTS` and returns PD retry failure without falling back to local time.

Milestone 4 validation:

    go test -run 'TestPDLeaseClock' -tags=intest,deadlock ./br/pkg/restore -count=1
    go test -run 'TestPDLeaseClock|TestGetTSWithRetry' -tags=intest,deadlock ./br/pkg/restore -count=1
    make bazel_prepare

Remaining gaps after Milestone 4: BR callers still need to construct this clock and pass it through `MigrationExt`, `LogClient`, PiTR collector, and stream truncate.

Milestone 4.5 collapsed the temporary objstore `WithLeaseClock` public APIs back into the original function names. The exported acquire, retry, truncate, and stale-cleanup APIs now require an explicit `LeaseClock` parameter and reject nil clocks. Existing non-PD callers were updated to pass `objstore.NewLocalLeaseClock()` explicitly until the next milestones replace those clocks with PD-backed clocks.

Milestone 4.5 validation:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemote|TestTryRenew|TestCleanUpStaleTruncateLock|TestLockWithRetry|TestLockRemoteTruncate' -count=1
    ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'TestMergeAndMigrateTo|TestAppendMigration' -count=1
    go test -run 'Test.*StreamTruncate|TestGetLockedMigrations|Test.*PiTR' -tags=intest,deadlock ./br/pkg/task ./br/pkg/restore/log_client ./br/pkg/restore/snap_client -count=1
    go test -run TestNonExistent -tags=intest,deadlock ./pkg/objstore/s3store -count=1

The third command ran matching `log_client` tests and served as compile
coverage for `br/pkg/task` and `br/pkg/restore/snap_client`, where the selected
patterns had no matching tests in this milestone.

Remaining gaps after Milestone 4.5: `MigrationExt`, `LogClient`, PiTR collector, and stream truncate still need production PD-clock propagation. The temporary local-clock calls added in this milestone are intentional migration placeholders.

Milestone 5 delivered `MigrationExt.clock` and changed all migration read,
write, and append-write lock acquisitions owned by `MigrationExt` to use that
clock. `LogClient` now stores a `LeaseClock` constructed from its PD client in
production and passes it to `MigrationExtension` for locked migration reads.
PiTR collector now stores a PD-backed lease clock from `PiTRCollDep.PDCli` and
uses it for append-migration writes. Manual operator paths and the stream
truncate command-level placeholder still pass `objstore.NewLocalLeaseClock()`
explicitly until Milestone 6 or a separate operator design.

Milestone 5 validation:

    ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'Test.*LeaseClock|TestMergeAndMigrateToRenewsWriteLock|TestLockForAppendRenewsBothLocks' -count=1
    ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'Test.*LeaseClock|TestMergeAndMigrateTo|TestAppendMigration|TestLockForAppendRenewsBothLocks' -count=1
    ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'TestUserAbort|TestLockForAppendRenewsBothLocks|TestMergeAndMigrateToRenewsWriteLock' -count=1
    go test -run 'TestGetLockedMigrations|Test.*PiTR' -tags=intest,deadlock ./br/pkg/restore/log_client ./br/pkg/restore/snap_client -count=1
    go test -run TestNonExistent -tags=intest,deadlock ./br/pkg/task ./br/pkg/task/operator -count=1
    rg -n "MigrationExtension\\([^,\\n]+\\)" br/pkg --glob '*.go'
    rg -n "MigrationExtension\\(" br/pkg --glob '*.go'
    git diff --check

The first stream command was the intended RED check and failed before
implementation with compile errors around the new `MigrationExtension(...,
clock)` shape and `MigrationExt.clock`. The first restore command also exposed
the nil-PD-client test helper issue described above; it passed after
`LogClient` carried an explicit lease clock.

`make bazel_prepare` was not required for Milestone 5: no Go source files were
added, removed, renamed, or moved; no new top-level Go test functions were
added; no Bazel metadata or module files changed.

Code review follow-up: the supplemental stream command above was added after
review noted that `TestUserAbort` is the test covering `DryRun` clock
preservation and was not matched by the original M5 GREEN regex.

Remaining gaps after Milestone 5: stream truncate still uses explicit local
time for its command-level truncate lock and temporary `MigrationExtension`
placeholder until Milestone 6.

Milestone 6 delivered PD-backed lease-clock propagation for the
`RunStreamTruncate` production path. The command now creates a `conn.Mgr`
inside the function, defers `mgr.Close()`, builds `restore.NewPDLeaseClock` from
`mgr.GetPDClient()`, and reuses that one clock for truncate stale cleanup,
truncate lock acquire/renewal, `MigrationExtension`, and
`RemoveDataFilesAndUpdateMetadataInBatch`. The stream metadata cleanup helper
now requires an explicit clock parameter instead of constructing a local clock
internally. Manual operator commands remain explicit local-clock paths.

Milestone 6 validation:

    ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'Test.*Truncate|TestWithSimpleTruncate' -count=1
    go test -run TestNonExistent -tags=intest,deadlock ./br/pkg/task -count=1
    rg -n "NewLocalLeaseClock\\(" br/pkg/task/stream.go br/pkg/stream/stream_metas.go
    rg -n "RemoveDataFilesAndUpdateMetadataInBatch\\(" br/pkg br/tests --glob '*.go'
    git diff --check

The RED check for Milestone 6 used the same stream command after updating test
call sites first; it failed with the expected compile errors because
`RemoveDataFilesAndUpdateMetadataInBatch` did not yet accept a clock.

`make bazel_prepare` was not required for Milestone 6: no Go source files were
added, removed, renamed, or moved; no new top-level Go test functions were
added; no Bazel metadata or module files changed.

## Context and Orientation

The lock implementation lives in `pkg/objstore/locking.go` and `pkg/objstore/locking_helper.go`. Tests live mostly in `pkg/objstore/locking_test.go`, with test-only exports in `pkg/objstore/export_test.go`.

A lease lock is stored as an object-storage file. Current physical paths include:

- `v1/LOCK.WRIT.<generation>` for migration write locks;
- `v1/LOCK.READ.<generation>` for migration read locks;
- `v1/APPEND_LOCK.WRIT.<generation>` for append migration locks;
- `truncating.lock.<generation>` for truncate locks.

`LockMeta` in `pkg/objstore/locking.go` stores `LockedAt` and `ExpireAt`. `LeaseTTL` is the lease duration. `renewInterval` and retry backoff are local scheduling controls and are not lease-validity proofs.

Before this plan, the lock-layer local-time call sites to replace were:

- lock generation timestamp construction in `pkg/objstore/locking_helper.go`;
- `MakeLockMeta()` in `pkg/objstore/locking.go`, which remains only for explicit local-clock compatibility and tests;
- `(*RemoteLock).tryRenew()` in `pkg/objstore/locking.go`;
- stale cleanup decisions in `pkg/objstore/locking_helper.go`.

After Milestone 4.5, exported objstore lock APIs require an explicit
`LeaseClock`; local-time behavior is visible only through callers that pass
`objstore.NewLocalLeaseClock()`.

BR upper-layer paths that can provide PD time:

- `br/pkg/restore/log_client/client.go`: `LogClient` already has `pdClient`.
- `br/pkg/restore/snap_client/pitr_collector.go`: `PiTRCollDep` has `PDCli`.
- `br/pkg/task/stream.go`: `StreamConfig` embeds `task.Config`, which has `PD` and `TLS`; `RunStreamTruncate` can create or reuse a manager to obtain `pd.Client`.

`br/pkg/stream/stream_metas.go` currently only has `storeapi.Storage` in `MigrationExt`; it should receive a lease clock from its creator and forward it to lock calls.

## Interfaces and Dependencies

In `pkg/objstore`, define the minimal lease-clock interface:

    type LeaseClock interface {
        Now(ctx context.Context) (time.Time, error)
    }

The exact name may change only if all plan references are updated. Keep the first version to this one method; future clock metadata or capability should use a new interface, optional extension interface, or separate parameter rather than adding methods to `LeaseClock`.

Provide an exported local compatibility clock for tests, manual paths, and
temporarily unmigrated callers that must make local-time behavior explicit:

    type localLeaseClock struct{}

    func (localLeaseClock) Now(context.Context) (time.Time, error) {
        return nowFunc(), nil
    }

    func NewLocalLeaseClock() LeaseClock

After Milestone 4.5, do not keep parallel `WithLeaseClock` APIs. The exported
lock APIs should require an explicit `clock LeaseClock` parameter directly:

    func TryLockRemoteWrite(ctx context.Context, storage storeapi.Storage, path, hint string, clock LeaseClock) (*RemoteLock, error)
    func TryLockRemoteRead(ctx context.Context, storage storeapi.Storage, path, hint string, clock LeaseClock) (*RemoteLock, error)
    func TryLockRemoteTruncate(ctx context.Context, storage storeapi.Storage, hint string, clock LeaseClock) (*RemoteLock, error)

    type Locker func(ctx context.Context, storage storeapi.Storage, path, hint string, clock LeaseClock) (*RemoteLock, error)

    func LockWithRetry(ctx context.Context, locker Locker, storage storeapi.Storage, lockPath, hint string, onLeaseLost func(), clock LeaseClock) (*RemoteLock, error)
    func LockRemoteTruncate(ctx context.Context, storage storeapi.Storage, hint string, onLeaseLost func(), clock LeaseClock) (*RemoteLock, error)
    func CleanUpStaleTruncateLock(ctx context.Context, storage storeapi.Storage, clock LeaseClock) (bool, error)

All these functions must reject nil clocks instead of silently falling back to
local time. Existing local-time behavior is preserved only where the caller
passes `NewLocalLeaseClock()` explicitly.

`RemoteLock` stores the clock so renewal uses the same lease clock as acquire.
Do not migrate BR callers to PD time until acquire, renewal, retry cleanup, and
stale cleanup all use the same explicit clock through the single public API
shape.

BR PD helper should live in `br/pkg/restore`, next to the existing `GetTS`
and `GetTSWithRetry` helpers:

    type PDLeaseClock struct {
        pdClient pd.Client
    }

    func NewPDLeaseClock(pdClient pd.Client) objstore.LeaseClock
    func (c PDLeaseClock) Now(ctx context.Context) (time.Time, error)

`PDLeaseClock.Now` should call the existing `GetTSWithRetry`, convert with
`oracle.GetTimeFromTS`, and return an error without local-time fallback when PD
time cannot be obtained after retry.

## Plan of Work

Milestone 1 adds the minimal lease-clock interface and explicit-clock single-attempt acquire variants. It changes acquire metadata and generation construction to use the clock and adds post-acquire proof before returning a lock. In that milestone, legacy acquire functions remain and keep current local-time behavior until Milestone 4.5 consolidates the API.

Milestone 2 stores the lease clock on `RemoteLock` and converts renewal to use it for expiry checks and refreshed `ExpireAt`. This milestone proves that `LockWithRetry` can start renewal without falling back to local time.

Milestone 2.5 hardens the explicit-clock boundary before cleanup work. It rejects nil clocks at the internal exact-acquire helper, adds append-lock explicit-clock acquire coverage, and records the retry API shape that Milestone 3 must implement.

Milestone 3 converts stale cleanup decisions to the explicit lease clock. It must carefully preserve the existing difference between public cleanup, which may return candidate errors, and retry-loop cleanup, which logs candidate errors and continues.

Milestone 4 adds the BR PD lease-clock wrapper in `br/pkg/restore`. It reuses
the existing `GetTSWithRetry` implementation instead of duplicating PD retry
logic in a new package.

Milestone 4.5 consolidates the objstore public API. It removes the temporary
`WithLeaseClock` variants and `LockerWithLeaseClock`, adds a public local clock
constructor for explicit compatibility, and updates current callers to pass
either the local compatibility clock or, in later milestones, the PD-backed
clock.

Milestone 5 propagates the lease clock through `MigrationExt`, `LogClient`, and PiTR collector. `MigrationExt.DryRun` and value-copy methods must preserve the clock.

Milestone 6 wires stream truncate. This is last because it touches command-level config, storage setup, PD/TLS manager setup, truncate lock cleanup/acquire, and migration cleanup.

## Concrete Steps

### Milestone 1: Objstore Acquire Lease Clock

Files:

- Modify `pkg/objstore/locking.go`.
- Modify `pkg/objstore/locking_helper.go`.
- Modify `pkg/objstore/locking_test.go`.

TDD steps:

- [x] Add tests in `pkg/objstore/locking_test.go`:

    `TestTryLockRemoteWriteUsesClockForMetaAndGeneration`

    This test should create a deterministic fake clock returning `leaseNow` for metadata and `leaseNow.Add(time.Millisecond)` for post-acquire proof. It should call `objstore.TryLockRemoteWrite`, read back the written `LockMeta`, and assert:

    - the physical path prefix contains `fmt.Sprintf("%016x", leaseNow.UnixNano())`;
    - `meta.LockedAt == leaseNow`;
    - `meta.ExpireAt == leaseNow.Add(objstore.LeaseTTL)`.

    `TestTryLockRemoteWriteFailsWhenInitialTimeFails`

    This test should provide a clock returning `expectedErr`, call `TryLockRemoteWrite`, and assert:

    - returned lock is nil;
    - error wraps `expectedErr`;
    - no `v1/LOCK.WRIT.` path exists.

    `TestTryLockRemoteWriteFailsIfAcquireReturnsAfterExpireAt`

    This test should provide a clock returning `leaseNow` first and `leaseNow.Add(objstore.LeaseTTL).Add(time.Nanosecond)` second. It should call `TryLockRemoteWrite` and assert:

    - returned lock is nil;
    - error contains `lease expired before acquire returned`;
    - no `v1/LOCK.WRIT.` path remains after best-effort cleanup.

- [x] Run the RED command from repository root:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteWrite' -count=1

  Expected before implementation: compile failure because `LeaseClock` and `TryLockRemoteWrite` do not exist.

- [x] Implement:

    In `pkg/objstore/locking.go`, add:

        type LeaseClock interface {
            Now(ctx context.Context) (time.Time, error)
        }

        type localLeaseClock struct{}

        func (localLeaseClock) Now(context.Context) (time.Time, error) {
            return nowFunc(), nil
        }

        func makeLockMetaAt(hint string, lockedAt time.Time) LockMeta {
            hname, err := os.Hostname()
            if err != nil {
                hname = fmt.Sprintf("UnknownHost(err=%s)", err)
            }
            return LockMeta{
                LockedAt:   lockedAt,
                LockerHost: hname,
                Hint:       hint,
                LockerPID:  os.Getpid(),
                ExpireAt:   lockedAt.Add(LeaseTTL),
            }
        }

    Change `MakeLockMeta` to call `makeLockMetaAt(hint, nowFunc())`.

    In `pkg/objstore/locking_helper.go`, change `newLockGeneration()` to `newLockGeneration(leaseNow time.Time)`.

    Change `makeLockContent` so it accepts a precomputed `LockMeta` without `TxnID`, and the `conditionalPut.Content` closure only fills `TxnID` and marshals.

    Add explicit-clock acquire variants. `TryLockRemoteWrite` should:

    - reject nil clock with an error before writing;
    - call `clock.Now(ctx)` once before generation and metadata construction;
    - use that time for generation and `makeLockMetaAt`;
    - call a helper such as `tryLockRemoteExactWithClock` that performs `conditionalPut.CommitTo`;
    - after commit and family verification succeed, call `clock.Now(ctx)` again;
    - if the second clock call errors, best-effort delete the physical path and return an acquire error;
    - if the second time is after `meta.ExpireAt`, best-effort delete the physical path and return an acquire error containing `lease expired before acquire returned`;
    - return `RemoteLock` only after the post-acquire proof succeeds.

    Add matching `TryLockRemoteRead` and `TryLockRemoteTruncate` by following the existing read/truncate acquire code. Keep existing `TryLockRemoteWrite`, `TryLockRemoteRead`, and `TryLockRemoteTruncate` as wrappers that call the explicit-clock variants with `localLeaseClock{}`.

- [x] Run the GREEN command:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteWrite|TestTryLockRemoteWrite|TestTryLockRemoteRead|TestTryLockRemoteTruncate|TestMakeLockMeta' -count=1

  Expected after implementation: selected tests pass.

- [x] Run `make bazel_prepare` because this milestone adds new top-level Go test functions.

- [x] Commit:

    git add pkg/objstore/locking.go pkg/objstore/locking_helper.go pkg/objstore/locking_test.go docs/agents/br/lease-lock-unified-time-source-spec.md docs/agents/br/lease-lock-unified-time-source-implementation-plan.md
    git commit -m "pkg/objstore: add explicit lease clock for acquire"

### Milestone 2: Objstore Renewal Lease Clock

Files:

- Modify `pkg/objstore/locking.go`.
- Modify `pkg/objstore/locking_test.go`.
- Possibly modify `pkg/objstore/export_test.go` for test-only helpers.

TDD steps:

- [x] Add tests proving renewal uses the clock stored on `RemoteLock`:

    `TestTryRenewWithLeaseClockUsesOneTimeForExpiryAndRefresh`

    Acquire with an explicit fake clock, then set the clock to a later deterministic time before `TESTTryRenew`. Assert refreshed `ExpireAt` equals that clock time plus `LeaseTTL`.

    `TestTryRenewWithLeaseClockErrorIsTransient`

    The clock returns an error during renewal. Assert `TESTTryRenew` returns that error or an annotated error and does not write a new `ExpireAt`.

- [x] Run RED:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryRenewWithLeaseClock' -count=1

  Expected before implementation: tests fail because renewal still uses local time or the clock cannot be controlled.

- [x] Implement:

    Store `leaseClock LeaseClock` on `RemoteLock`. Explicit-clock acquire variants set it to the provided clock. Legacy wrappers set it to `localLeaseClock{}`.

    Change `tryRenew` to call `l.leaseClock.Now(ctx)` once before checking `ExpireAt`; use that value for both the old-lease expired predicate and refreshed `ExpireAt`.

    Ensure `startRenewal` and `LockRemoteTruncateWithLeaseClock` use locks whose clock is already stored on `RemoteLock`.

- [x] Run GREEN:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryRenew|TestLockWithRetryStartsRenewal|TestLockRemoteTruncateStartsRenewal' -count=1

- [x] Run `make bazel_prepare` if new top-level test functions were added.

- [x] Commit:

    git add pkg/objstore/locking.go pkg/objstore/export_test.go pkg/objstore/locking_test.go docs/agents/br/lease-lock-unified-time-source-implementation-plan.md
    git commit -m "pkg/objstore: renew locks with explicit lease clock"

### Milestone 2.5: Pre-M3 Explicit-Clock Boundary Hardening

Files:

- Modify `pkg/objstore/locking_helper.go`.
- Modify `pkg/objstore/export_test.go`.
- Modify `pkg/objstore/locking_test.go`.
- Modify this plan.

TDD steps:

- [x] Add `TestTryLockRemoteExactWithLeaseClockRejectsNilClock`.

    This test should call a test-only wrapper around the exact-lock helper with a nil clock and assert:

    - returned lock is nil;
    - error contains `lease clock is required`;
    - no target lock file is created.

- [x] Run RED:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteExactWithLeaseClockRejectsNilClock' -count=1

  Expected before implementation: panic or failure because the helper tries to use a nil clock after writing.

- [x] Implement:

    Add a nil-clock check at the start of the exact-lock helper, before constructing or committing the conditional write.

- [x] Add append explicit-clock acquire coverage:

    `TestTryLockRemoteAppendWriteWithLeaseClockUsesClockForMetaAndGeneration`

    This test should call `TryLockRemoteWrite(ctx, storage, "v1/APPEND_LOCK", hint, clock)`, read back the `v1/APPEND_LOCK.WRIT.<generation>` file, and assert the generation prefix, `LockedAt`, and `ExpireAt` come from the fake clock.

- [x] Record API shape for Milestone 3:

        type LockerWithLeaseClock = func(ctx context.Context, storage storeapi.Storage, path, hint string, clock LeaseClock) (*RemoteLock, error)

        func LockWithRetry(ctx context.Context, locker LockerWithLeaseClock, storage storeapi.Storage, lockPath, hint string, onLeaseLost func(), clock LeaseClock) (*RemoteLock, error)

    Milestone 3 should implement this together with explicit-clock cleanup, because retry acquisition and cleanup must use the same lease clock.

- [x] Run GREEN:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteExactWithLeaseClockRejectsNilClock|TestTryLockRemoteAppendWriteWithLeaseClockUsesClockForMetaAndGeneration|TestTryLockRemoteWrite' -count=1

- [x] Run `make bazel_prepare` because this milestone adds new top-level Go test functions.

- [x] Commit:

    git add pkg/objstore/locking_helper.go pkg/objstore/export_test.go pkg/objstore/locking_test.go docs/agents/br/lease-lock-unified-time-source-implementation-plan.md
    git commit -m "pkg/objstore: harden lease clock acquire boundary"

### Milestone 3: Objstore Cleanup Lease Clock

Files:

- Modify `pkg/objstore/locking.go`.
- Modify `pkg/objstore/locking_helper.go`.
- Modify `pkg/objstore/locking_test.go`.

TDD steps:

- [x] Add tests:

    `TestCleanUpStaleTruncateLockUsesClockForStaleDecision`

    Create one stale truncate instance and one alive instance. Use a clock value after `ExpireAt.Add(LeaseTTL)` for the stale instance. Assert only the stale instance is deleted.

    `TestCleanUpStaleTruncateLockFailureDoesNotDelete`

    Use a clock returning an error. Assert cleanup returns false with the error and the candidate still exists.

- [x] Run RED:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestCleanUpStaleTruncateLock' -count=1

- [x] Implement:

    Add `CleanUpStaleTruncateLock(ctx, storage, clock)`.

    Thread `clock` through `cleanUpStaleLockFamily`, `tryCleanUpStaleLockFamily`, and `cleanUpStaleLockInstance`. Keep legacy cleanup functions as wrappers using `localLeaseClock{}`.

    Decide and document candidate-error handling:

    - public cleanup with `returnCandidateErrors=false` keeps existing suppression of candidate errors;
    - retry cleanup with `returnCandidateErrors=true` logs candidate errors and returns reclaimed status;
    - a clock failure for a candidate must not delete that candidate.

- [x] Run GREEN:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestCleanUpStaleTruncateLock|TestCleanup|TestLockWithRetry' -count=1

- [x] Run `make bazel_prepare` if new top-level tests were added.

- [x] Commit:

    git add pkg/objstore/locking.go pkg/objstore/locking_helper.go pkg/objstore/locking_test.go docs/agents/br/lease-lock-unified-time-source-implementation-plan.md
    git commit -m "pkg/objstore: clean stale locks with explicit lease clock"

### Milestone 4: BR PD Lease Clock Helper

Files:

- Create `br/pkg/restore/pd_lease_clock.go`.
- Create or extend `br/pkg/restore/*_test.go` with focused lease-clock tests.

TDD steps:

- [x] Add tests using a tiny fake PD client:

        type fakeTSClient struct {
            physical  int64
            logical   int64
            err       error
            callCount int
        }

        func (c *fakeTSClient) GetTS(context.Context) (int64, int64, error) {
            c.callCount++
            return c.physical, c.logical, c.err
        }

    `TestPDLeaseClockConvertsRetriedTSOTime` should assert
    `PDLeaseClock.Now` returns `oracle.GetTimeFromTS(ts)` where `ts` comes
    from the existing `GetTSWithRetry` path.

    `TestPDLeaseClockNowReturnsGetTSError` should assert no fallback to local
    time when `GetTSWithRetry` fails.

    Do not reimplement or broadly retest `GetTSWithRetry`; use its existing
    behavior. The wrapper tests only need to prove that `PDLeaseClock.Now`
    delegates to it and converts the resulting TSO to `time.Time`.

- [x] Run RED:

    go test -run 'TestPDLeaseClock' -tags=intest,deadlock ./br/pkg/restore -count=1

- [x] Implement `NewPDLeaseClock(pdClient pd.Client) objstore.LeaseClock`
  and `PDLeaseClock.Now(ctx context.Context) (time.Time, error)` by calling
  `GetTSWithRetry` and converting the returned TSO with `oracle.GetTimeFromTS`.

- [x] Run GREEN:

    go test -run 'TestPDLeaseClock' -tags=intest,deadlock ./br/pkg/restore -count=1

- [x] Run `make bazel_prepare` because this milestone adds Go source and may add new top-level tests.

- [x] Commit:

    git add br/pkg/restore docs/agents/br/lease-lock-unified-time-source-implementation-plan.md
    git commit -m "br: add PD-backed lease clock"

### Milestone 4.5: Objstore API Consolidation

Files:

- Modify `pkg/objstore/locking.go`.
- Modify `pkg/objstore/locking_helper.go`.
- Modify `pkg/objstore/export_test.go`.
- Modify `pkg/objstore/locking_test.go`.
- Modify current direct callers such as `br/pkg/stream/stream_metas.go`,
  `br/pkg/task/stream.go`, and package tests that call objstore lock APIs.

TDD steps:

- [x] Add or adjust tests so the original API names require an explicit clock:

        clock := objstore.NewLocalLeaseClock()
        lock, err := objstore.TryLockRemoteWrite(ctx, storage, "v1/LOCK", "hint", clock)

    For nil-clock coverage, use the original names instead of the temporary
    `WithLeaseClock` names. For example:

        _, err := objstore.TryLockRemoteWrite(ctx, storage, "v1/LOCK", "hint", nil)
        require.ErrorContains(t, err, "lease clock is required")

- [x] Run RED targeted objstore API tests. Expected result: compile failures
  or nil-clock assertion failures until the API is consolidated.

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemote.*LeaseClock|TestLockWithRetry.*LeaseClock|TestCleanUpStaleTruncateLock.*LeaseClock' -count=1

    Note: this milestone was resumed from already partially consolidated code, so
    the fresh verification evidence for this session is the GREEN suite below
    plus the public API residual scan.

- [x] Add `NewLocalLeaseClock() LeaseClock` in `pkg/objstore` and keep
  `localLeaseClock` unexported.

- [x] Change the original exported functions to require `clock LeaseClock`
  directly and reject nil clocks:

        func TryLockRemoteWrite(ctx context.Context, storage storeapi.Storage, path, hint string, clock LeaseClock) (*RemoteLock, error)
        func TryLockRemoteRead(ctx context.Context, storage storeapi.Storage, path, hint string, clock LeaseClock) (*RemoteLock, error)
        func TryLockRemoteTruncate(ctx context.Context, storage storeapi.Storage, hint string, clock LeaseClock) (*RemoteLock, error)
        func CleanUpStaleTruncateLock(ctx context.Context, storage storeapi.Storage, clock LeaseClock) (bool, error)
        func LockRemoteTruncate(ctx context.Context, storage storeapi.Storage, hint string, onLeaseLost func(), clock LeaseClock) (*RemoteLock, error)

- [x] Change `Locker` to include the clock parameter and delete
  `LockerWithLeaseClock`:

        type Locker func(ctx context.Context, storage storeapi.Storage, path, hint string, clock LeaseClock) (*RemoteLock, error)

        func LockWithRetry(ctx context.Context, locker Locker, storage storeapi.Storage, lockPath, hint string, onLeaseLost func(), clock LeaseClock) (*RemoteLock, error)

- [x] Delete the temporary public `WithLeaseClock` variants and update internal
  helper names so the package has one public API shape.

- [x] Update current non-PD call sites to pass `objstore.NewLocalLeaseClock()`
  explicitly. This includes current stream/task callers until Milestone 5
  replaces those local clocks with PD clocks.

- [x] Run GREEN targeted tests:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemote|TestTryRenew|TestCleanUpStaleTruncateLock|TestLockWithRetry|TestLockRemoteTruncate' -count=1

- [x] Run targeted compile/tests for adjusted BR callers:

    ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'TestMergeAndMigrateTo|TestAppendMigration' -count=1
    go test -run 'Test.*StreamTruncate|TestGetLockedMigrations|Test.*PiTR' -tags=intest,deadlock ./br/pkg/task ./br/pkg/restore/log_client ./br/pkg/restore/snap_client -count=1

- [x] Run `make bazel_prepare` if new top-level tests or generated Bazel
  metadata changes are required.

    Not required for Milestone 4.5: no Go files were added/removed/renamed,
    no new top-level Go tests were added, and no Bazel metadata changed.

- [x] Commit:

    git add pkg/objstore br/pkg/stream br/pkg/task br/pkg/restore docs/agents/br/lease-lock-unified-time-source-implementation-plan.md
    git commit -m "pkg/objstore: require explicit lease clock"

### Milestone 5: MigrationExt, LogClient, and PiTR Propagation

Scope:

- Wire PD-backed lease time into the `MigrationExt` read/write/append-write
  lock paths.
- Keep the repository compileable by updating every `MigrationExtension(...)`
  caller in the same commit.
- Do not wire the stream-truncate command's own truncate lock to PD time in
  this milestone; that command-level PD/TLS manager lifecycle remains Milestone
  6.
- Do not migrate manual operator commands to PD time in this phase; make their
  local-time behavior explicit with `objstore.NewLocalLeaseClock()`.

Files:

- Modify `br/pkg/stream/stream_metas.go`.
- Modify `br/pkg/stream/stream_metas_test.go`.
- Modify `br/pkg/task/stream.go` only enough to keep the
  `MigrationExtension` constructor call compiling with an explicit local
  compatibility clock. Production PD-clock wiring for this path remains
  Milestone 6.
- Modify `br/pkg/task/operator/migrate_to.go` and
  `br/pkg/task/operator/list_migration.go` to pass
  `objstore.NewLocalLeaseClock()` explicitly, because the manual operator paths
  are excluded from PD-clock migration in this phase.
- Modify `br/pkg/restore/log_client/client.go`.
- Modify `br/pkg/restore/log_client/export_test.go`.
- Modify `br/pkg/restore/snap_client/pitr_collector.go`.
- Modify related PiTR collector tests if needed.

TDD steps:

- [x] Add or extend stream tests so `MigrationExt` proves it uses the supplied
  clock for each lock family it owns. Prefer extending existing test functions
  instead of adding top-level tests where that keeps the diff focused.

    Test cases to cover:

    - `GetReadLock` writes a `v1/LOCK.READ.<generation>` file whose
      `LockedAt` and `ExpireAt` use the deterministic clock.
    - `lockForAppend` writes both:
      - `v1/LOCK.READ.<generation>` for the read phase;
      - `v1/APPEND_LOCK.WRIT.<generation>` for the append-write phase.
    - `MergeAndMigrateTo` writes `v1/LOCK.WRIT.<generation>` with the
      deterministic clock when locking is not skipped.
    - `DryRun` preserves the same clock when copying the batch
      `MigrationExt`, so later lock acquisition cannot silently fall back to a
      different clock.

    Reuse helper style from existing tests:

        meta, _, ok := readSingleLockMetaWithPrefix(t, ctx, s, lockPrefix+".READ.")
        require.True(t, ok)
        require.Equal(t, leaseNow, meta.LockedAt)
        require.Equal(t, leaseNow.Add(objstore.LeaseTTL), meta.ExpireAt)

    Use a simple deterministic clock in `stream_metas_test.go`, for example:

        type fixedLeaseClock struct {
            now time.Time
        }

        func (c fixedLeaseClock) Now(context.Context) (time.Time, error) {
            return c.now, nil
        }

- [x] Run RED targeted stream tests before implementation:

        ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'Test.*LeaseClock|TestMergeAndMigrateToRenewsWriteLock|TestLockForAppendRenewsBothLocks' -count=1

    Expected: compile failures because `MigrationExtension` does not yet accept
    a clock, or assertion failures because `MigrationExt` still passes
    `objstore.NewLocalLeaseClock()` internally.

- [x] In `br/pkg/stream/stream_metas.go`, make `MigrationExt` carry a lease
  clock supplied by its constructor. Change the constructor to:

        func MigrationExtension(s storeapi.Storage, clock objstore.LeaseClock) MigrationExt

    The constructor should store the clock without fallback. If a nil clock is
    passed, later lock acquisition must fail through the objstore nil-clock
    checks rather than silently installing local time. Call sites must pass
    either `restore.NewPDLeaseClock(...)` or `objstore.NewLocalLeaseClock()`.
    `DryRun` must preserve the clock.

- [x] Implement `MigrationExt` propagation and pass `m.clock` into every
  read/write lock call owned by `MigrationExt`:

    - `GetReadLock`: pass `m.clock` to `objstore.LockWithRetry` with
      `objstore.TryLockRemoteRead`.
    - `lockForAppend` read phase: pass `m.clock` to `objstore.LockWithRetry`
      with `objstore.TryLockRemoteRead`.
    - `lockForAppend` append-write phase: pass `m.clock` to
      `objstore.LockWithRetry` with `objstore.TryLockRemoteWrite`.
    - `MergeAndMigrateTo`: pass `m.clock` to `objstore.LockWithRetry` with
      `objstore.TryLockRemoteWrite`.
    - `DryRun`: copy `clock: m.clock` into the batch `MigrationExt`.

- [x] Update every current `MigrationExtension(...)` caller in the same
  milestone so the tree stays compileable:

    - `br/pkg/stream/stream_metas.go`: internal helper call near
      `RemoveMetadataByMigrations` or equivalent stream-internal use should pass
      `objstore.NewLocalLeaseClock()` unless the caller already has a clock.
    - `br/pkg/stream/stream_metas_test.go`: pass deterministic clocks in new
      clock tests and `objstore.NewLocalLeaseClock()` in unrelated existing
      tests.
    - `br/pkg/task/stream.go`: pass `objstore.NewLocalLeaseClock()` only as a
      temporary compile-preserving placeholder for
      `MigrationExtension(extStorage, clock)`. Do not claim stream truncate uses
      PD time until Milestone 6.
    - `br/pkg/task/operator/migrate_to.go`: pass
      `objstore.NewLocalLeaseClock()` because this manual operator path is out
      of scope for PD migration.
    - `br/pkg/task/operator/list_migration.go`: pass
      `objstore.NewLocalLeaseClock()` for the same manual-operator reason.
    - `br/pkg/restore/log_client/client.go`: pass
      `restore.NewPDLeaseClock(rc.pdClient)`.
    - `br/pkg/restore/snap_client/pitr_collector.go`: pass a PD clock derived
      from `deps.PDCli` or a clock stored on `pitrCollector`.
    - `br/pkg/restore/snap_client/pitr_collector_test.go`: pass
      `objstore.NewLocalLeaseClock()` or a deterministic test clock, depending
      on what the test asserts.

- [x] Update `LogClient.GetLockedMigrations` to pass an explicit lease clock
  to `MigrationExtension(rc.storage, clock)`. `LogClient` stores this clock so
  storage-only test helpers can provide a local test clock without nil PD-client
  panics, while production `NewLogClient` still initializes it from
  `restore.NewPDLeaseClock(pdClient)`.

- [x] Update PiTR collector to use `restore.NewPDLeaseClock(deps.PDCli)` for
  append migrations. If constructing once is cleaner, add an
  `objstore.LeaseClock` field to `pitrCollector`; otherwise construct the clock
  at the `MigrationExtension(c.taskStorage, clock)` call site. Do not introduce
  local-time fallback on PD errors.

- [x] Run GREEN targeted tests:

    ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'Test.*LeaseClock|TestMergeAndMigrateTo|TestAppendMigration' -count=1
    ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'TestUserAbort|TestLockForAppendRenewsBothLocks|TestMergeAndMigrateToRenewsWriteLock' -count=1
    go test -run 'TestGetLockedMigrations|Test.*PiTR' -tags=intest,deadlock ./br/pkg/restore/log_client ./br/pkg/restore/snap_client -count=1
    go test -run TestNonExistent -tags=intest,deadlock ./br/pkg/task ./br/pkg/task/operator -count=1

- [x] Run an API residual scan:

    rg -n "MigrationExtension\\([^,\\n]+\\)" br/pkg --glob '*.go'

    Expected: no one-argument `MigrationExtension(...)` calls remain. If this
    regex is noisy, manually inspect every `rg -n "MigrationExtension\\("`
    result and confirm each call passes a clock.

- [x] Run `make bazel_prepare` if new top-level tests or Bazel metadata changes are required.

    Not required for Milestone 5: existing stream tests were extended, but no
    new top-level Go test functions were added.

- [x] Commit:

    git add br/pkg/stream br/pkg/task br/pkg/restore/log_client br/pkg/restore/snap_client docs/agents/br/lease-lock-unified-time-source-implementation-plan.md
    git commit -m "br: propagate PD lease time through migration locks"

### Milestone 6: Stream Truncate Propagation

Files:

- Modify `br/pkg/task/stream.go`.
- Modify `br/pkg/stream/stream_metas.go`.
- Modify `br/pkg/stream/stream_metas_test.go`.
- Modify this plan.

Scope:

- Wire PD-backed lease time into the `RunStreamTruncate` production path.
- Use one `leaseClock` value for the truncate stale cleanup, truncate lock
  acquire/renewal, migration cleanup, and stream metadata cleanup helper.
- Keep manual operator commands on explicit local time.
- Do not add a broad interactive command test unless a small helper naturally
  falls out of the implementation. `RunStreamTruncate` combines storage,
  prompts, progress bars, safe-point writes, and metadata deletion; broad tests
  would be brittle for this wiring-only milestone.

Execution steps:

- [x] Update `StreamMetadataSet.RemoveDataFilesAndUpdateMetadataInBatch` to
  require an explicit `clock objstore.LeaseClock` parameter:

        func (ms *StreamMetadataSet) RemoveDataFilesAndUpdateMetadataInBatch(
            ctx context.Context,
            from uint64,
            st storeapi.Storage,
            clock objstore.LeaseClock,
            updateFn func(num int64),
        ) ([]string, error)

    The implementation should pass `clock` to `MigrationExtension(hst, clock)`
    instead of constructing `objstore.NewLocalLeaseClock()` internally. This
    helper currently calls `doTruncateLogs` directly and does not acquire a
    migration lock, so the primary validation is signature-level propagation and
    residual local-clock scanning rather than lock-metadata assertions.

- [x] Update all existing
  `RemoveDataFilesAndUpdateMetadataInBatch(...)` callers:

    - `br/pkg/task/stream.go`: pass the `leaseClock` constructed in
      `RunStreamTruncate`.
    - `br/pkg/stream/stream_metas_test.go`: pass
      `objstore.NewLocalLeaseClock()` in existing tests.

- [x] Run RED compile check after the test/call-site signature changes but
  before production implementation is complete:

        ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'Test.*Truncate|TestWithSimpleTruncate' -count=1

    Expected: compile failure around the new
    `RemoveDataFilesAndUpdateMetadataInBatch` signature until the production
    implementation and task caller are updated.

- [x] In `RunStreamTruncate`, create a PD-backed lease clock inside the
  function after storage creation and before lock cleanup/acquire:

        mgr, err := NewMgr(ctx, g, cfg.PD, cfg.TLS, GetKeepalive(&cfg.Config),
            cfg.CheckRequirements, false, conn.StreamVersionChecker)
        if err != nil {
            return err
        }
        defer mgr.Close()
        leaseClock := restore.NewPDLeaseClock(mgr.GetPDClient())

    This keeps the PD client lifetime aligned with the truncate command
    invocation.

- [x] Replace the `RunStreamTruncate` local-clock placeholders with the same
  `leaseClock`:

    - `objstore.CleanUpStaleTruncateLock(ctx, extStorage, leaseClock)`
    - `objstore.LockRemoteTruncate(ctx, extStorage, hintOnTruncateLock, cancelFn, leaseClock)`
    - `stream.MigrationExtension(extStorage, leaseClock)`
    - `metas.RemoveDataFilesAndUpdateMetadataInBatch(ctx, shiftUntilTS, extStorage, leaseClock, p.IncBy)`

- [x] Run GREEN targeted checks:

        ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'Test.*Truncate|TestWithSimpleTruncate' -count=1
        go test -run TestNonExistent -tags=intest,deadlock ./br/pkg/task -count=1

- [x] Run residual scans:

        rg -n "NewLocalLeaseClock\\(" br/pkg/task/stream.go br/pkg/stream/stream_metas.go
        rg -n "RemoveDataFilesAndUpdateMetadataInBatch\\(" br/pkg br/tests --glob '*.go'

    Expected: no `NewLocalLeaseClock()` remains in
    `br/pkg/task/stream.go` or `br/pkg/stream/stream_metas.go`. Every
    `RemoveDataFilesAndUpdateMetadataInBatch` call passes an explicit clock.

- [x] Run `make bazel_prepare` only if new top-level Go tests are added,
  Go files are added/removed/renamed, or Bazel metadata changes are required.

- [x] Commit:

    git add br/pkg/task/stream.go br/pkg/stream/stream_metas.go br/pkg/stream/stream_metas_test.go docs/agents/br/lease-lock-unified-time-source-implementation-plan.md
    git commit -m "br: use PD lease time for stream truncate locks"

## Validation and Acceptance

The phase is complete when all of the following are true:

- BR production lock paths covered by this plan pass a PD-backed lease clock to `pkg/objstore`.
- `pkg/objstore` acquire, renewal, and cleanup decisions use the explicit clock for migrated paths.
- Explicit local-clock compatibility placeholders pass their existing targeted tests until production callers are migrated to PD time.
- PD lease-clock helper tests prove no local-time fallback on `GetTSWithRetry` error.
- The manual `br operator migrate-to` path remains unchanged.

Required final validation profile is `Ready` per `AGENTS.md`, because code changes are included. Run at least:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemote|TestTryRenew|TestCleanUpStaleTruncateLock|TestCleanup|TestLockWithRetry' -count=1
    go test -run 'TestPDLeaseClock' -tags=intest,deadlock ./br/pkg/restore -count=1
    ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'TestMergeAndMigrateTo|TestAppendMigration|Test.*LeaseClock' -count=1
    go test -run 'TestGetLockedMigrations|Test.*PiTR' -tags=intest,deadlock ./br/pkg/restore/log_client ./br/pkg/restore/snap_client -count=1
    make bazel_prepare
    make lint

If sandbox permissions block Go build cache or Bazel output base writes, rerun the same command with elevated permissions and record both attempts.

## Idempotence and Recovery

Each milestone is intended to be a separate commit. If a milestone becomes too large, stop after a passing targeted test, update `Progress`, and split the milestone.

If a test fails after implementation, do not weaken the test until confirming whether the spec was wrong. Record any necessary design change in `Decision Log`.

If `make bazel_prepare` changes generated Bazel metadata, include those files in the milestone commit. Do not hand-edit generated Bazel outputs.

If BR propagation reveals a call path without PD client or PD/TLS config, do not force it into this phase. Record the path in `Surprises & Discoveries`, update `Non-Goals` in the spec if appropriate, and keep the path on legacy local-time behavior until explicitly designed.

## Artifacts and Notes

Current planning artifacts:

- `docs/agents/br/lease-lock-unified-time-source-spec.md`
- `docs/agents/br/lease-lock-renewal-delayed-write-analysis.md`
- `temp/lease-lock-current-reference-20260528`, which preserves older broad WIP for reference only.

Independent design reviews concluded that this phase is feasible and useful as infrastructure, with these caveats:

- Include PiTR collector propagation.
- Do not treat PD TSO physical time as object-storage linearization.
- Ensure `LockWithRetry` covers acquire, cleanup, and renewal together before migrating BR callers.
- Keep stream truncate for last because it has the broadest command-level wiring.
