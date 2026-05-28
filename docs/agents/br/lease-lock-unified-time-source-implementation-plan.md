# Lease Lock Unified Time Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make BR lease-lock correctness paths use PD-backed lease time while keeping `pkg/objstore` independent of PD.

**Architecture:** BR upper layers construct a small PD-backed lease clock object and pass it down through stream/lock call paths. `pkg/objstore` consumes only an explicit `LeaseClock` interface, uses it for acquire, renewal, and cleanup lease decisions, and keeps legacy storage-only entry points on their existing local-time behavior until callers migrate.

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
- [ ] Implement Milestone 3: objstore stale cleanup uses the explicit lease clock.
- [ ] Implement Milestone 4: BR PD time helper.
- [ ] Implement Milestone 5: `MigrationExt`, `LogClient`, and PiTR collector propagation.
- [ ] Implement Milestone 6: stream truncate propagation.
- [ ] Run Ready validation before claiming the phase is complete.

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
- Decision: Legacy storage-only lock functions may keep local-time behavior while new explicit-clock variants are introduced for BR correctness paths.
  Rationale: This keeps migration incremental and avoids forcing all tests/manual tools through PD time at once.
  Date/Author: 2026-05-28 / design discussion.

## Outcomes & Retrospective

Milestone 1 delivered `objstore.LeaseClock`, an internal lock-meta constructor that accepts lease time, explicit acquire APIs with the `WithLeaseClock` suffix, generation timestamps based on the lease clock, and post-acquire proof that deletes the just-written lock if the lease is already expired before returning. Legacy single-attempt acquire APIs now wrap the explicit-clock variants with a local compatibility clock.

Milestone 1 validation:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteWriteWithLeaseClock' -count=1
    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteWriteWithLeaseClockCleansUpWhenPostAcquireClockFailsWithCanceledContext' -count=1
    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteWriteWithLeaseClock|TestTryLockRemoteWrite|TestTryLockRemoteRead|TestTryLockRemoteTruncate|TestMakeLockMeta' -count=1
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

Milestone 2.5 hardened the explicit-clock acquire boundary before cleanup migration. The internal exact-acquire helper now rejects nil lease clocks before writing any lock file, append write acquire has explicit-clock metadata and generation coverage, and Milestone 3 now has a concrete `LockerWithLeaseClock` / `LockWithRetryWithLeaseClock` API shape to implement together with cleanup clock threading.

Milestone 2.5 validation:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteExactWithLeaseClockRejectsNilClock' -count=1
    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteExactWithLeaseClockRejectsNilClock|TestTryLockRemoteAppendWriteWithLeaseClockUsesClockForMetaAndGeneration|TestTryLockRemoteWriteWithLeaseClock' -count=1
    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemote.*WithLeaseClock|TestTryLockRemoteWrite|TestTryLockRemoteRead|TestTryLockRemoteTruncate|TestMakeLockMeta' -count=1
    make bazel_prepare

Remaining gaps after Milestone 2.5: stale cleanup still uses local time until Milestone 3, `LockWithRetryWithLeaseClock` is designed but not implemented, and renewal post-write proof still needs a separate delayed-write correctness decision before production migration.

## Context and Orientation

The lock implementation lives in `pkg/objstore/locking.go` and `pkg/objstore/locking_helper.go`. Tests live mostly in `pkg/objstore/locking_test.go`, with test-only exports in `pkg/objstore/export_test.go`.

A lease lock is stored as an object-storage file. Current physical paths include:

- `v1/LOCK.WRIT.<generation>` for migration write locks;
- `v1/LOCK.READ.<generation>` for migration read locks;
- `v1/APPEND_LOCK.WRIT.<generation>` for append migration locks;
- `truncating.lock.<generation>` for truncate locks.

`LockMeta` in `pkg/objstore/locking.go` stores `LockedAt` and `ExpireAt`. `LeaseTTL` is the lease duration. `renewInterval` and retry backoff are local scheduling controls and are not lease-validity proofs.

The current local-time call sites to replace in explicit-clock paths are:

- `newLockGeneration()` in `pkg/objstore/locking_helper.go`, which uses `nowFunc().UnixNano()` for the generation time prefix.
- `MakeLockMeta()` in `pkg/objstore/locking.go`, which uses `nowFunc()` for `LockedAt` and `ExpireAt`.
- `(*RemoteLock).tryRenew()` in `pkg/objstore/locking.go`, which uses `nowFunc()` for expiry check and refreshed `ExpireAt`.
- `cleanUpStaleLockInstance()` in `pkg/objstore/locking_helper.go`, which uses `nowFunc()` to decide whether `ExpireAt.Add(LeaseTTL)` has passed.

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

Provide an internal local compatibility clock:

    type localLeaseClock struct{}

    func (localLeaseClock) Now(context.Context) (time.Time, error) {
        return nowFunc(), nil
    }

Keep existing exported legacy functions such as `TryLockRemoteWrite`, `TryLockRemoteRead`, `TryLockRemoteTruncate`, `LockWithRetry`, `LockRemoteTruncate`, and `CleanUpStaleTruncateLock` as wrappers around explicit-clock implementations using `localLeaseClock{}`.

Add explicit-clock variants for migrated BR paths. Use the `WithLeaseClock` suffix consistently:

    func TryLockRemoteWriteWithLeaseClock(ctx context.Context, storage storeapi.Storage, path, hint string, clock LeaseClock) (*RemoteLock, error)
    func TryLockRemoteReadWithLeaseClock(ctx context.Context, storage storeapi.Storage, path, hint string, clock LeaseClock) (*RemoteLock, error)
    func TryLockRemoteTruncateWithLeaseClock(ctx context.Context, storage storeapi.Storage, hint string, clock LeaseClock) (*RemoteLock, error)

Later milestones add:

    func LockWithRetryWithLeaseClock(ctx context.Context, locker LockerWithLeaseClock, storage storeapi.Storage, lockPath, hint string, onLeaseLost func(), clock LeaseClock) (*RemoteLock, error)
    func LockRemoteTruncateWithLeaseClock(ctx context.Context, storage storeapi.Storage, hint string, onLeaseLost func(), clock LeaseClock) (*RemoteLock, error)
    func CleanUpStaleTruncateLockWithLeaseClock(ctx context.Context, storage storeapi.Storage, clock LeaseClock) (bool, error)

`RemoteLock` must eventually store the clock so renewal uses the same lease clock as acquire. Do not migrate BR callers to `LockWithRetryWithLeaseClock` until renewal and cleanup are covered.

BR PD helper should live outside `pkg/objstore`, for example `br/pkg/leaseclock`:

    type tsoClient interface {
        GetTS(ctx context.Context) (int64, int64, error)
    }

    type PDClock struct {
        client tsoClient
    }

    func NewPDClock(client tsoClient) *PDClock
    func (c *PDClock) Now(ctx context.Context) (time.Time, error)

`PDClock.Now` should call `GetTS`, compose with `oracle.ComposeTS`, convert with `oracle.GetTimeFromTS`, and return an error without local-time fallback when `GetTS` fails.

## Plan of Work

Milestone 1 adds the minimal lease-clock interface and explicit-clock single-attempt acquire variants. It changes acquire metadata and generation construction to use the clock and adds post-acquire proof before returning a lock. Legacy acquire functions remain and keep current local-time behavior.

Milestone 2 stores the lease clock on `RemoteLock` and converts renewal to use it for expiry checks and refreshed `ExpireAt`. This milestone proves that `LockWithRetryWithLeaseClock` can start renewal without falling back to local time.

Milestone 2.5 hardens the explicit-clock boundary before cleanup work. It rejects nil clocks at the internal exact-acquire helper, adds append-lock explicit-clock acquire coverage, and records the retry API shape that Milestone 3 must implement.

Milestone 3 converts stale cleanup decisions to the explicit lease clock. It must carefully preserve the existing difference between public cleanup, which may return candidate errors, and retry-loop cleanup, which logs candidate errors and continues.

Milestone 4 adds the BR PD time helper and focused tests using a tiny `GetTS` fake instead of a full PD client fake.

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

    `TestTryLockRemoteWriteWithLeaseClockUsesClockForMetaAndGeneration`

    This test should create a deterministic fake clock returning `leaseNow` for metadata and `leaseNow.Add(time.Millisecond)` for post-acquire proof. It should call `objstore.TryLockRemoteWriteWithLeaseClock`, read back the written `LockMeta`, and assert:

    - the physical path prefix contains `fmt.Sprintf("%016x", leaseNow.UnixNano())`;
    - `meta.LockedAt == leaseNow`;
    - `meta.ExpireAt == leaseNow.Add(objstore.LeaseTTL)`.

    `TestTryLockRemoteWriteWithLeaseClockFailsWhenInitialTimeFails`

    This test should provide a clock returning `expectedErr`, call `TryLockRemoteWriteWithLeaseClock`, and assert:

    - returned lock is nil;
    - error wraps `expectedErr`;
    - no `v1/LOCK.WRIT.` path exists.

    `TestTryLockRemoteWriteWithLeaseClockFailsIfAcquireReturnsAfterExpireAt`

    This test should provide a clock returning `leaseNow` first and `leaseNow.Add(objstore.LeaseTTL).Add(time.Nanosecond)` second. It should call `TryLockRemoteWriteWithLeaseClock` and assert:

    - returned lock is nil;
    - error contains `lease expired before acquire returned`;
    - no `v1/LOCK.WRIT.` path remains after best-effort cleanup.

- [x] Run the RED command from repository root:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteWriteWithLeaseClock' -count=1

  Expected before implementation: compile failure because `LeaseClock` and `TryLockRemoteWriteWithLeaseClock` do not exist.

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

    Add explicit-clock acquire variants. `TryLockRemoteWriteWithLeaseClock` should:

    - reject nil clock with an error before writing;
    - call `clock.Now(ctx)` once before generation and metadata construction;
    - use that time for generation and `makeLockMetaAt`;
    - call a helper such as `tryLockRemoteExactWithLeaseClock` that performs `conditionalPut.CommitTo`;
    - after commit and family verification succeed, call `clock.Now(ctx)` again;
    - if the second clock call errors, best-effort delete the physical path and return an acquire error;
    - if the second time is after `meta.ExpireAt`, best-effort delete the physical path and return an acquire error containing `lease expired before acquire returned`;
    - return `RemoteLock` only after the post-acquire proof succeeds.

    Add matching `TryLockRemoteReadWithLeaseClock` and `TryLockRemoteTruncateWithLeaseClock` by following the existing read/truncate acquire code. Keep existing `TryLockRemoteWrite`, `TryLockRemoteRead`, and `TryLockRemoteTruncate` as wrappers that call the explicit-clock variants with `localLeaseClock{}`.

- [x] Run the GREEN command:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteWriteWithLeaseClock|TestTryLockRemoteWrite|TestTryLockRemoteRead|TestTryLockRemoteTruncate|TestMakeLockMeta' -count=1

  Expected after implementation: selected tests pass.

- [x] Run `make bazel_prepare` because this milestone adds new top-level Go test functions.

- [ ] Commit:

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

- [ ] Commit:

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

    This test should call a test-only wrapper around `tryLockRemoteExactWithLeaseClock` with a nil clock and assert:

    - returned lock is nil;
    - error contains `lease clock is required`;
    - no target lock file is created.

- [x] Run RED:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteExactWithLeaseClockRejectsNilClock' -count=1

  Expected before implementation: panic or failure because the helper tries to use a nil clock after writing.

- [x] Implement:

    Add a nil-clock check at the start of `tryLockRemoteExactWithLeaseClock`, before constructing or committing the conditional write.

- [x] Add append explicit-clock acquire coverage:

    `TestTryLockRemoteAppendWriteWithLeaseClockUsesClockForMetaAndGeneration`

    This test should call `TryLockRemoteWriteWithLeaseClock(ctx, storage, "v1/APPEND_LOCK", hint, clock)`, read back the `v1/APPEND_LOCK.WRIT.<generation>` file, and assert the generation prefix, `LockedAt`, and `ExpireAt` come from the fake clock.

- [x] Record API shape for Milestone 3:

        type LockerWithLeaseClock = func(ctx context.Context, storage storeapi.Storage, path, hint string, clock LeaseClock) (*RemoteLock, error)

        func LockWithRetryWithLeaseClock(ctx context.Context, locker LockerWithLeaseClock, storage storeapi.Storage, lockPath, hint string, onLeaseLost func(), clock LeaseClock) (*RemoteLock, error)

    Milestone 3 should implement this together with explicit-clock cleanup, because retry acquisition and cleanup must use the same lease clock.

- [x] Run GREEN:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteExactWithLeaseClockRejectsNilClock|TestTryLockRemoteAppendWriteWithLeaseClockUsesClockForMetaAndGeneration|TestTryLockRemoteWriteWithLeaseClock' -count=1

- [x] Run `make bazel_prepare` because this milestone adds new top-level Go test functions.

- [ ] Commit:

    git add pkg/objstore/locking_helper.go pkg/objstore/export_test.go pkg/objstore/locking_test.go docs/agents/br/lease-lock-unified-time-source-implementation-plan.md
    git commit -m "pkg/objstore: harden lease clock acquire boundary"

### Milestone 3: Objstore Cleanup Lease Clock

Files:

- Modify `pkg/objstore/locking.go`.
- Modify `pkg/objstore/locking_helper.go`.
- Modify `pkg/objstore/locking_test.go`.

TDD steps:

- [ ] Add tests:

    `TestCleanUpStaleTruncateLockWithLeaseClockUsesClockForStaleDecision`

    Create one stale truncate instance and one alive instance. Use a clock value after `ExpireAt.Add(LeaseTTL)` for the stale instance. Assert only the stale instance is deleted.

    `TestCleanUpStaleTruncateLockWithLeaseClockFailureDoesNotDelete`

    Use a clock returning an error. Assert cleanup returns false with the error and the candidate still exists.

- [ ] Run RED:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestCleanUpStaleTruncateLockWithLeaseClock' -count=1

- [ ] Implement:

    Add `CleanUpStaleTruncateLockWithLeaseClock(ctx, storage, clock)`.

    Thread `clock` through `cleanUpStaleLockFamily`, `tryCleanUpStaleLockFamily`, and `cleanUpStaleLockInstance`. Keep legacy cleanup functions as wrappers using `localLeaseClock{}`.

    Decide and document candidate-error handling:

    - public cleanup with `returnCandidateErrors=false` keeps existing suppression of candidate errors;
    - retry cleanup with `returnCandidateErrors=true` logs candidate errors and returns reclaimed status;
    - a clock failure for a candidate must not delete that candidate.

- [ ] Run GREEN:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestCleanUpStaleTruncateLock|TestCleanup|TestLockWithRetry' -count=1

- [ ] Run `make bazel_prepare` if new top-level tests were added.

- [ ] Commit:

    git add pkg/objstore/locking.go pkg/objstore/locking_helper.go pkg/objstore/locking_test.go docs/agents/br/lease-lock-unified-time-source-implementation-plan.md
    git commit -m "pkg/objstore: clean stale locks with explicit lease clock"

### Milestone 4: BR PD Lease Clock Helper

Files:

- Create `br/pkg/leaseclock/lease_clock.go`.
- Create `br/pkg/leaseclock/lease_clock_test.go`.

TDD steps:

- [ ] Add tests using a tiny fake:

        type fakeTSClient struct {
            physical int64
            logical  int64
            err      error
        }

        func (c fakeTSClient) GetTS(context.Context) (int64, int64, error) {
            return c.physical, c.logical, c.err
        }

    `TestNewPDClockConvertsTSOPhysicalTime` should assert `PDClock.Now` returns `oracle.GetTimeFromTS(oracle.ComposeTS(physical, logical))`.

    `TestPDClockNowReturnsGetTSError` should assert no fallback to local time.

- [ ] Run RED:

    ./tools/check/failpoint-go-test.sh br/pkg/leaseclock -run 'Test(NewPDClock|PDClockNow)' -count=1

  If the package has no failpoints, plain `go test -run 'Test(NewPDClock|PDClockNow)' -tags=intest,deadlock ./br/pkg/leaseclock` is also acceptable after checking failpoint rules.

- [ ] Implement `NewPDClock(client tsoClient) *PDClock` and `(*PDClock).Now(ctx context.Context) (time.Time, error)`.

- [ ] Run GREEN:

    go test -run 'Test(NewPDClock|PDClockNow)' -tags=intest,deadlock ./br/pkg/leaseclock -count=1

- [ ] Run `make bazel_prepare` because this milestone adds a Go package and new top-level tests.

- [ ] Commit:

    git add br/pkg/leaseclock docs/agents/br/lease-lock-unified-time-source-implementation-plan.md
    git commit -m "br: add PD-backed lease clock"

### Milestone 5: MigrationExt, LogClient, and PiTR Propagation

Files:

- Modify `br/pkg/stream/stream_metas.go`.
- Modify `br/pkg/stream/stream_metas_test.go`.
- Modify `br/pkg/restore/log_client/client.go`.
- Modify `br/pkg/restore/log_client/client_test.go`.
- Modify `br/pkg/restore/snap_client/pitr_collector.go`.
- Modify related PiTR collector tests if needed.

TDD steps:

- [ ] In `br/pkg/stream/stream_metas.go`, plan for `MigrationExt` to carry a lease clock. The constructor `MigrationExtension(s storeapi.Storage)` keeps legacy behavior. Add a method:

        func (m MigrationExt) WithLeaseClock(clock objstore.LeaseClock) MigrationExt

    This method should copy the value and set the clock. `DryRun` must preserve the clock.

- [ ] Add tests in `stream_metas_test.go` that create a deterministic clock and verify `GetReadLock`, `lockForAppend`, and `MergeAndMigrateTo` write lock metadata with clock time. Include a `DryRun` test that proves the clock is not dropped.

- [ ] Run RED targeted stream tests.

- [ ] Implement `MigrationExt` propagation and use explicit-clock lock functions.

- [ ] Update `LogClient.GetLockedMigrations` to pass `leaseclock.NewPDClock(rc.pdClient)` to `MigrationExtension(rc.storage)`.

- [ ] Update PiTR collector to store a clock built from `PiTRCollDep.PDCli` and pass it into `MigrationExtension(c.taskStorage)` before `AppendMigration`.

- [ ] Run GREEN targeted tests:

    ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'Test.*LeaseClock|TestMergeAndMigrateTo|TestAppendMigration' -count=1
    go test -run 'TestGetLockedMigrations|Test.*PiTR' -tags=intest,deadlock ./br/pkg/restore/log_client ./br/pkg/restore/snap_client -count=1

- [ ] Run `make bazel_prepare` if new top-level tests or Bazel metadata changes are required.

- [ ] Commit:

    git add br/pkg/stream br/pkg/restore/log_client br/pkg/restore/snap_client docs/agents/br/lease-lock-unified-time-source-implementation-plan.md
    git commit -m "br: propagate PD lease time through migration locks"

### Milestone 6: Stream Truncate Propagation

Files:

- Modify `br/pkg/task/stream.go`.
- Modify targeted task tests or extract a helper for focused testing.

TDD steps:

- [ ] Extract a small helper if needed so tests do not need to exercise the entire interactive command. The helper should accept storage and a lease clock, then perform truncate stale cleanup, truncate lock acquire, and optional migration cleanup setup.

- [ ] Add tests proving `RunStreamTruncate` or the helper passes the PD-backed lease clock to `CleanUpStaleTruncateLockWithLeaseClock`, `LockRemoteTruncateWithLeaseClock`, and `MigrationExtension(...).WithLeaseClock(...)`.

- [ ] Implement manager/client creation from `cfg.PD` and `cfg.TLS`, following existing `NewMgr` cleanup patterns in `br/pkg/task/stream.go`.

- [ ] Run targeted tests. If no focused helper seam exists, add one before broad command tests.

- [ ] Run `make bazel_prepare` if needed.

- [ ] Commit:

    git add br/pkg/task/stream.go docs/agents/br/lease-lock-unified-time-source-implementation-plan.md
    git commit -m "br: use PD lease time for stream truncate locks"

## Validation and Acceptance

The phase is complete when all of the following are true:

- BR production lock paths covered by this plan pass a PD-backed lease clock to `pkg/objstore`.
- `pkg/objstore` acquire, renewal, and cleanup decisions use the explicit clock for migrated paths.
- Existing legacy storage-only APIs still pass their existing tests.
- PD time helper tests prove no local-time fallback on `GetTS` error.
- The manual `br operator migrate-to` path remains unchanged.

Required final validation profile is `Ready` per `AGENTS.md`, because code changes are included. Run at least:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemote|TestTryRenew|TestCleanUpStaleTruncateLock|TestCleanup|TestLockWithRetry' -count=1
    go test -run 'Test(NewPDClock|PDClockNow)' -tags=intest,deadlock ./br/pkg/leaseclock -count=1
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
