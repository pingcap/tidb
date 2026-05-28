# Lease Lock Renewal Delayed Write Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the delayed renewal-write race in the lease-based object-store lock by using trusted lease time, terminal renewal state, local watchdogs, and cleanup tombstones.

**Architecture:** `pkg/objstore` owns the lock protocol and exposes a small `LeaseClock` abstraction plus lock options. BR code injects a direct PD-backed clock through a new BR adapter package, while non-BR compatibility paths may continue using a local-clock default without claiming the delayed-renewal correctness proof. Cleanup becomes tombstone-first, renewal proves its own physical instance only, and acquire remains fail-closed.

**Tech Stack:** Go, `pkg/objstore`, `pkg/objstore/storeapi`, `br/pkg/stream`, `br/pkg/restore/log_client`, `br/pkg/task`, `br/pkg/task/operator`, PD client `GetTS`, failpoint-enabled package tests, `make bazel_prepare`, `make lint`.

---

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

The current lease lock uses generation-based physical lock files, so stale cleanup no longer races against a fixed lock path. One race remains: a holder can begin renewal while its old lease is valid, block inside the final storage `WriteFile`, get cleaned up after expiration, allow another holder to acquire and finish work, and then have the old delayed write recreate a self-consistent old generation. After this implementation, a live holder cannot accept that delayed write as successful, cleanup decisions survive as tombstones, and business work is stopped by a local watchdog if renewal I/O is stuck.

The observable result is that the preserved regression test in `pkg/objstore/locking_test.go` for delayed final renewal writes changes from expected failure to pass. Additional tests prove acquire/renewal timing, tombstone cleanup, PD-clock failure, and terminal lease-lost behavior.

## Progress

- [x] (2026-05-27) Wrote the design analysis in `docs/agents/br/lease-lock-renewal-delayed-write-analysis.md`.
- [x] (2026-05-27) Independent review found and the analysis recorded acquire return-time proof, strong-consistency scope, fixed 32-hex generation wording, and delete-not-found classification.
- [ ] Add the `LeaseClock` abstraction, lock options, and strict proof checks.
- [ ] Convert acquire to use trusted lease time and return-time proof.
- [ ] Implement terminal renewal state, remaining-window scheduling, and watchdog cancellation.
- [ ] Implement cleanup tombstones and renewal exact tombstone checks.
- [ ] Inject PD-backed lease clocks into BR lock paths.
- [ ] Add and run the detailed test matrix.
- [ ] Run `make bazel_prepare` after adding the new `br/pkg/leaseclock` Go
  package and top-level Go tests.
- [ ] Run Ready validation before claiming implementation completion.

## Surprises & Discoveries

- Observation: `conditionalPut.Content` cannot return an error, so acquire must obtain trusted lease time before constructing the `conditionalPut`.
  Evidence: `pkg/objstore/locking.go` defines `Content func(txnID uuid.UUID) []byte`.
- Observation: `operator migrate-to` currently has no PD configuration in `MigrateToConfig`.
  Evidence: `br/pkg/task/operator/config.go` defines `MigrateToConfig` with `objstore.BackendOptions` and `StorageURI`, but no `PD` or `TLS`.
- Observation: `storeapi.WalkOption.IncludeTombstone` is storage-internal deleted-object behavior, not the protocol tombstone object introduced by this plan.
  Evidence: `pkg/objstore/storeapi/storage.go` documents `IncludeTombstone` as emitting removed files during walking.

## Decision Log

- Decision: `pkg/objstore` defines `LeaseClock`; BR implements PD-backed time outside `pkg/objstore`.
  Rationale: The lock layer should not depend on PD packages. Tests can inject fake clocks, and BR can adapt PD `GetTS`.
  Date/Author: 2026-05-27 / design session.
- Decision: BR correctness paths pass an explicit PD-backed clock and strict strong-consistency option.
  Rationale: Tombstone proof requires trusted time and strong visibility. Compatibility paths may keep local-clock behavior but cannot claim the BR delayed-renewal proof.
  Date/Author: 2026-05-27 / design session.
- Decision: Local filesystem storage should implement `storeapi.StrongConsistency` for the lease-lock proof path.
  Rationale: BR commands may use local `file://` storage in tests or workflows, and local filesystem read/write/list/existence visibility is strong enough for this protocol. Strict mode should fail closed for genuinely non-strong backends, not for local storage.
  Date/Author: 2026-05-27 / final plan review.
- Decision: Acquire must perform a synchronous post-acquire PD-time proof before exposing a lock.
  Rationale: Acquire writes can also return after the lease window, and business code must not begin protected work with an already-expired lock.
  Date/Author: 2026-05-27 / independent review follow-up.
- Decision: Renewal anchors refreshed `ExpireAt` at pre-write PD time and computes next delay/watchdog from remaining lease time at post-write PD time.
  Rationale: Slow writes should not extend the lease window or sleep a full interval after consuming much of the lease.
  Date/Author: 2026-05-27 / design session.
- Decision: Cleanup tombstones are singleton empty files under `v1/LOCK_CLEANUP_TOMBSTONE/...` and are written with normal `WriteFile`.
  Rationale: The tombstone is a durable cleanup decision. It is idempotent and should not extend the acquire `conditionalPut` protocol.
  Date/Author: 2026-05-27 / design session.
- Decision: Acquire does not read tombstones.
  Rationale: Visible tombstoned zombies block acquire until cleanup deletes them. This is fail-closed and avoids adding a second tombstone-read path to acquire.
  Date/Author: 2026-05-27 / design session.
- Decision: Legacy or malformed lock names are not automatically deleted by the tombstone protocol.
  Rationale: This branch has not shipped; compatibility with intermediate names is not required. Unknown files remain fail-closed and diagnostic.
  Date/Author: 2026-05-27 / design session.

## Outcomes & Retrospective

No implementation has been performed from this plan yet. Update this section after each milestone with commands run, test output summaries, and any remaining gaps.

## Context and Orientation

The lock implementation lives in `pkg/objstore/locking.go` and `pkg/objstore/locking_helper.go`. The test-only exports live in `pkg/objstore/export_test.go`. The main tests are in `pkg/objstore/locking_test.go`.

Important terms:

- Lease clock: an interface returning trusted lease time. For BR correctness paths this means PD-backed time, not local wall-clock time.
- Local clock default: a compatibility fallback used when callers do not pass a lease clock. It preserves old behavior but does not provide cross-process clock-skew protection.
- Strong proof mode: a lock option that requires storage to implement `storeapi.StrongConsistency` before claiming the tombstone-based proof.
- Physical lock instance: a concrete object path such as `v1/LOCK.WRIT.<generation>` or `truncating.lock.<generation>`.
- Logical lock family: the family path used by callers, such as `v1/LOCK`, `v1/APPEND_LOCK`, or `truncating.lock`.
- Cleanup tombstone: a protocol-owned empty object recording that cleanup has committed to deleting one physical lock instance.
- Terminal lost state: once renewal observes any proof failure, the process-local `RemoteLock` is lost forever. Later successful storage calls cannot make it active again.

Current production lock call paths:

- `br/pkg/stream/stream_metas.go::MigrationExt.GetReadLock` calls `objstore.LockWithRetry` for migration read locks.
- `br/pkg/stream/stream_metas.go::MigrationExt.lockForAppend` calls `objstore.LockWithRetry` for migration read and append write locks.
- `br/pkg/stream/stream_metas.go::MigrationExt.MergeAndMigrateTo` calls `objstore.LockWithRetry` for migration write locks.
- `br/pkg/restore/log_client/client.go::LogClient.GetLockedMigrations` uses `MigrationExt.GetReadLock`; `LogClient` already has `pdClient`.
- `br/pkg/task/stream.go::RunStreamTruncate` uses `objstore.CleanUpStaleTruncateLock` and `objstore.LockRemoteTruncate`; `StreamConfig` embeds `task.Config`, so it has PD and TLS settings.
- `br/pkg/task/operator/migrate_to.go::RunMigrateTo` uses `MigrationExt.MergeAndMigrateTo`; `MigrateToConfig` currently has storage settings but no PD/TLS settings.

## Interfaces and Dependencies

In `pkg/objstore/locking.go`, add:

    type LeaseClock interface {
        Now(ctx context.Context) (time.Time, error)
    }

    type localLeaseClock struct{}

    func (localLeaseClock) Now(context.Context) (time.Time, error) {
        return nowFunc(), nil
    }

    type LockOption func(*lockOptions)

    type lockOptions struct {
        leaseClock            LeaseClock
        requireStrongStorage  bool
    }

    func WithLeaseClock(clock LeaseClock) LockOption
    func WithRequireStrongConsistency() LockOption

The option resolver should default to `localLeaseClock{}` and `requireStrongStorage=false`. It should reject a nil explicit clock:

    func resolveLockOptions(opts []LockOption) (lockOptions, error)

In strict mode, acquire/renewal/cleanup should call a helper before relying on lock or tombstone visibility:

    func requireStrongConsistencyStorage(storage storeapi.Storage) error {
        if _, ok := storage.(storeapi.StrongConsistency); !ok {
            return errors.Errorf("lease lock proof requires strong-consistency storage, got %T", storage)
        }
        return nil
    }

Change the primitive locker type to accept options:

    type Locker = func(
        ctx context.Context,
        storage storeapi.Storage,
        path string,
        hint string,
        opts ...LockOption,
    ) (*RemoteLock, error)

Change public lock APIs to accept options:

    func LockWithRetry(
        ctx context.Context,
        locker Locker,
        storage storeapi.Storage,
        lockPath string,
        hint string,
        onLeaseLost func(),
        opts ...LockOption,
    ) (*RemoteLock, error)

    func TryLockRemoteWrite(ctx context.Context, storage storeapi.Storage, path, hint string, opts ...LockOption) (*RemoteLock, error)

    func TryLockRemoteRead(ctx context.Context, storage storeapi.Storage, path, hint string, opts ...LockOption) (*RemoteLock, error)

    func TryLockRemoteTruncate(ctx context.Context, storage storeapi.Storage, hint string, opts ...LockOption) (*RemoteLock, error)

    func LockRemoteTruncate(ctx context.Context, storage storeapi.Storage, hint string, onLeaseLost func(), opts ...LockOption) (*RemoteLock, error)

Change cleanup APIs and helpers to accept the same options:

    func CleanUpStaleTruncateLock(ctx context.Context, storage storeapi.Storage, opts ...LockOption) (bool, error)

    func tryCleanUpStaleLockFamily(ctx context.Context, storage storeapi.Storage, logicalPath string, options lockOptions) bool

    func cleanUpStaleLockFamily(ctx context.Context, storage storeapi.Storage, logicalPath string, options lockOptions) (bool, error)

    func cleanUpStaleLockInstance(ctx context.Context, storage storeapi.Storage, path string, options lockOptions, tombstoned bool) (bool, error)

`LockWithRetry` must resolve `opts` once and pass the same resolved options to
both acquire and opportunistic cleanup. A caller that asks acquire to use a
PD-backed `LeaseClock` and strong-consistency proof must not have retry cleanup
fall back to local time or non-strict visibility.

`RemoteLock` should store the resolved clock and strict flag:

    type RemoteLock struct {
        txnID   uuid.UUID
        storage storeapi.Storage
        path    string

        leaseClock           LeaseClock
        requireStrongStorage bool
        acquiredLockedAt     time.Time
        acquiredExpireAt     time.Time

        mu           sync.Mutex
        renewalState renewalState
        stopCh       chan struct{}
        done         chan struct{}
        attemptCancel context.CancelFunc
        watchdog      *time.Timer
        onLeaseLost   func()
    }

The exact field layout can change during implementation, but the state machine must support `idle`, `running`, `lost`, and `stopped` semantics.

In `br/pkg/leaseclock/lease_clock.go`, add a small adapter:

    package leaseclock

    type PDLeaseClock struct {
        Client pd.Client
    }

    func NewPDLeaseClock(client pd.Client) objstore.LeaseClock

    func (c PDLeaseClock) Now(ctx context.Context) (time.Time, error) {
        physical, logical, err := c.Client.GetTS(ctx)
        if err != nil {
            return time.Time{}, errors.Trace(err)
        }
        return oracle.GetTimeFromTS(oracle.ComposeTS(physical, logical)), nil
    }

Use direct PD time. Do not add a PD-time cache in this implementation.

In `br/pkg/stream/stream_metas.go`, let `MigrationExt` carry lock options without knowing PD:

    type MigrationExt struct {
        s storeapi.Storage
        Hooks MigrationHooks
        lockOptions []objstore.LockOption
    }

    func (m MigrationExt) WithLockOptions(opts ...objstore.LockOption) MigrationExt {
        m.lockOptions = append(append([]objstore.LockOption{}, m.lockOptions...), opts...)
        return m
    }

Every `LockWithRetry` call from `MigrationExt` should pass `m.lockOptions...`.

## Plan of Work

Milestone 1 adds clocks, options, and the local-storage strong-consistency marker
without changing behavior for default local-clock callers. This de-risks API
plumbing.

Milestone 2 rewrites acquire to create metadata from trusted time and to fail before returning if the acquired lease window is already expired.

Milestone 3 rewrites renewal state and scheduling: terminal lost state, exact own tombstone check, remaining-window scheduling, and watchdog cancellation.

Milestone 4 rewrites stale cleanup to use singleton tombstones before physical deletion.

Milestone 5 wires PD-backed clocks through BR paths, including the operator migrate-to command.

Milestone 6 adds the full race-focused test matrix and runs validation.

Each milestone that adds new top-level Go test functions or a new Go package
must run `make bazel_prepare` before treating that milestone's validation as
complete. Milestone 6 runs it again as the final metadata check.

## Milestone 1: LeaseClock and Lock Options

Scope: `pkg/objstore/locking.go`, `pkg/objstore/locking_helper.go`, `pkg/objstore/local.go`, `pkg/objstore/export_test.go`, `pkg/objstore/locking_test.go`.

After this milestone, all lock APIs accept `LockOption`, but existing callers without options keep the local-clock compatibility behavior.

- [ ] Add `LeaseClock`, `localLeaseClock`, `LockOption`, `lockOptions`, `WithLeaseClock`, `WithRequireStrongConsistency`, and `resolveLockOptions`.
- [ ] Add `func (*LocalStorage) MarkStrongConsistency() {}` in `pkg/objstore/local.go`, with a short comment that local filesystem semantics are strong enough for lease-lock proof checks.
- [ ] Add `TESTLeaseClock` helpers in `pkg/objstore/export_test.go` so external package tests can create a deterministic fake clock:

      type TESTLeaseClock struct {
          mu    sync.Mutex
          now   time.Time
          err   error
      }

      func NewTESTLeaseClock(now time.Time) *TESTLeaseClock
      func (c *TESTLeaseClock) Now(context.Context) (time.Time, error)
      func (c *TESTLeaseClock) Set(now time.Time)
      func (c *TESTLeaseClock) SetError(err error)

- [ ] Extend `Locker`, `TryLockRemoteWrite`, `TryLockRemoteRead`, `TryLockRemoteTruncate`, `LockRemoteTruncate`, and `LockWithRetry` signatures with variadic `LockOption`.
- [ ] Extend `CleanUpStaleTruncateLock` and the internal stale-cleanup helpers with `LockOption` / resolved `lockOptions`.
- [ ] In `LockWithRetry`, resolve `opts` once and pass the same options to the locker and to `tryCleanUpStaleLockFamily`.
- [ ] Update all current callers to compile by either passing no options or forwarding `opts...`.
- [ ] Add tests:

      TestLockOptionsRejectNilExplicitClock
      TestLockOptionsRequireStrongConsistency
      TestLocalStorageSatisfiesStrongConsistency
      TestCleanupOptionsRequireStrongConsistency
      TestTryLockRemoteWriteUsesLocalClockByDefault

- [ ] Run:

      ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLockOptions|TestLocalStorageSatisfiesStrongConsistency|TestCleanupOptions|TestTryLockRemoteWriteUsesLocalClockByDefault' -count=1

  Expected: all selected tests pass.

- [ ] Run `make bazel_prepare` because this milestone adds new top-level tests in an existing `*_test.go` file.

- [ ] Commit:

      git add pkg/objstore/locking.go pkg/objstore/locking_helper.go pkg/objstore/local.go pkg/objstore/export_test.go pkg/objstore/locking_test.go
      git commit -m "pkg/objstore: add lease clock options"

## Milestone 2: Trusted-Time Acquire Proof

Scope: `pkg/objstore/locking.go`, `pkg/objstore/locking_helper.go`, `pkg/objstore/locking_test.go`.

After this milestone, acquire metadata uses the resolved lease clock, generation remains 32 hex characters, and lifecycle acquire fails before returning if the acquired lease is already expired.

- [ ] Change `newLockGeneration` to accept `leaseNow time.Time`:

      func newLockGeneration(leaseNow time.Time) (string, error) {
          var randomBytes [8]byte
          if _, err := cryptorand.Read(randomBytes[:]); err != nil {
              return "", errors.Annotate(err, "generate lock instance id")
          }
          return fmt.Sprintf("%016x%016x", leaseNow.UnixNano(), binary.BigEndian.Uint64(randomBytes[:])), nil
      }

- [ ] Replace `MakeLockMeta(hint string)` use in lock content creation with a helper that accepts `lockedAt time.Time`:

      func MakeLockMetaAt(hint string, lockedAt time.Time) LockMeta

  Keep `MakeLockMeta(hint string)` as compatibility wrapper:

      func MakeLockMeta(hint string) LockMeta {
          return MakeLockMetaAt(hint, nowFunc())
      }

- [ ] Change `makeLockContent` to accept a precomputed `LockMeta` without `TxnID`, then fill `TxnID` in the `conditionalPut.Content` closure.
- [ ] In each `TryLockRemote*`, resolve options, obtain `leaseNow := clock.Now(ctx)` before generation/content construction, and pass both into `tryLockRemoteExact`.
- [ ] In strict mode, call `requireStrongConsistencyStorage(storage)` before acquire writes or acquire verification. If storage is not strong-consistent, return an acquire error and do not create a lock object.
- [ ] In `tryLockRemoteExact`, after `CommitTo` succeeds, call `clock.Now(ctx)` again. If this post-acquire time read fails, best-effort delete the physical lock path and return an acquire error. Do not return a `RemoteLock`.
- [ ] In `tryLockRemoteExact`, if `nowAfterAcquire.After(meta.ExpireAt)` under the existing predicate, best-effort delete the physical lock path and return an acquire error. Do not return a `RemoteLock`.
- [ ] Store `leaseClock`, strict option, acquired `LockedAt`, and acquired `ExpireAt` on the returned `RemoteLock`.
- [ ] Add a helper for initial renewal scheduling from acquired metadata:

      func (l *RemoteLock) initialRenewalSchedule(ctx context.Context) (remainingLease time.Duration, firstDelay time.Duration, err error) {
          now, err := l.leaseClock.Now(ctx)
          if err != nil {
              return 0, 0, errors.Trace(err)
          }
          if now.After(l.acquiredExpireAt) {
              return 0, 0, errRenewLeaseExpired
          }
          remainingLease = l.acquiredExpireAt.Sub(now)
          firstDelay = maxDuration(0, l.acquiredLockedAt.Add(renewInterval).Sub(now))
          return remainingLease, firstDelay, nil
      }

- [ ] Add tests:

      TestTryLockRemoteWriteUsesLeaseClockForMetaAndGeneration
      TestTryLockRemoteWriteFailsWhenPostAcquireClockFails
      TestTryLockRemoteWriteFailsIfAcquireReturnsAfterExpireAt
      TestTryLockRemoteWriteKeepsGeneration32Hex
      TestInitialRenewalScheduleUsesAcquiredLeaseWindow

- [ ] Update existing tests that assert `MakeLockMeta` uses `TESTSetNow`; keep those tests passing for the local-clock compatibility wrapper.
- [ ] Run:

      ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLockRemoteWrite|TestTryLockRemoteRead|TestTryLockRemoteTruncate|TestLockWithRetry|TestMakeLockMeta|TestTESTSetNow' -count=1

  Expected: selected tests pass.

- [ ] Run `make bazel_prepare` if this milestone adds any new top-level Go test functions.

- [ ] Commit:

      git add pkg/objstore/locking.go pkg/objstore/locking_helper.go pkg/objstore/locking_test.go
      git commit -m "pkg/objstore: prove acquired lease before returning"

## Milestone 3: Terminal Renewal State, Remaining Scheduling, and Watchdog

Scope: `pkg/objstore/locking.go`, `pkg/objstore/export_test.go`, `pkg/objstore/locking_test.go`.

After this milestone, renewal cannot be revived after any failure source, stuck renewal I/O is bounded by a local watchdog, and next renewal timing is based on remaining lease time.

- [ ] Add renewal state values for running, lost, and stopped. Keep the zero state idle.
- [ ] Add a helper that transitions `active -> lost` exactly once and invokes `onLeaseLost` once.
- [ ] Add in-flight attempt context tracking:

      attemptCtx, attemptCancel := context.WithCancel(parentCtx)

  Store the cancel function under `RemoteLock.mu` so watchdog and normal stop can cancel it.
- [ ] Change `tryRenew` to return a result:

      type renewalResult struct {
          remainingLease time.Duration
          nextDelay      time.Duration
      }

      func (l *RemoteLock) tryRenew(ctx context.Context) (renewalResult, error)

- [ ] Add the exact tombstone path helper before `tryRenew` starts using it:

      const cleanupTombstoneRoot = "v1/LOCK_CLEANUP_TOMBSTONE"

      func tombstonePathForLockPath(lockPath string) (string, bool)

  The helper maps only current 32-hex generation lock paths:

      truncating.lock.<generation> -> v1/LOCK_CLEANUP_TOMBSTONE/TRUNCATE/WRIT/<generation>
      v1/LOCK.WRIT.<generation> -> v1/LOCK_CLEANUP_TOMBSTONE/LOCK/WRIT/<generation>
      v1/LOCK.READ.<generation> -> v1/LOCK_CLEANUP_TOMBSTONE/LOCK/READ/<generation>
      v1/APPEND_LOCK.WRIT.<generation> -> v1/LOCK_CLEANUP_TOMBSTONE/APPEND_LOCK/WRIT/<generation>

  Return `false` for legacy or malformed paths. Renewal treats `false` as no
  tombstone path for local-clock compatibility tests; cleanup later reports
  malformed cleanup candidates as errors.

- [ ] In `tryRenew`, distinguish read errors:

      ReadFile succeeds: continue.
      ReadFile fails and FileExists returns false: return errRenewLockMissing.
      ReadFile fails and FileExists returns true or FileExists fails: return transient error.

- [ ] In `tryRenew`, use `l.leaseClock.Now(ctx)` before write, set `ExpireAt = nowBeforeWrite.Add(LeaseTTL)`, write the full metadata, then use `l.leaseClock.Now(ctx)` after write.
- [ ] Keep the existing expiration boundary predicate: `now.After(meta.ExpireAt)`.
- [ ] In strict mode, call `requireStrongConsistencyStorage(l.storage)` before relying on the own-lock read, own-lock existence check, or own tombstone existence check. A strict-mode visibility failure is a renewal error and must not report success.
- [ ] After post-write PD time, check exact own tombstone with `FileExists(tombstonePathFor(l.path))`. If it exists, return permanent lease lost. If existence check errors, return transient renewal error.
- [ ] Compute:

      remainingLease = newExpireAt.Sub(nowAfterWrite)
      nextDelay = maxDuration(0, nowBeforeWrite.Add(renewInterval).Sub(nowAfterWrite))

- [ ] Update `startRenewal` to call `initialRenewalSchedule(ctx)` before starting the loop. If the first target time has already passed, renew immediately. If this initial schedule proof fails, transition to lost and do not start a healthy renewal loop.
- [ ] Add and manage a watchdog timer. Reset it to the remaining proven lease window after acquire and after each successful renewal.
- [ ] When watchdog fires, transition to lost and cancel the in-flight renewal attempt. Treat cancellation only as resource cleanup; delayed successful I/O must observe lost state and must not reset timers.
- [ ] Ensure `Unlock` / `stopRenewalIfStarted` transitions active to stopped, stops watchdog, cancels in-flight renewal, waits for the loop, and never overwrites a lost state back to stopped.
- [ ] Preserve best-effort `Unlock` behavior for lost locks. It may delete the physical object, but delete success or failure does not change lost state.
- [ ] Add tests:

      TestTryRenewConfirmedMissingLockReportsLost
      TestTryRenewPostWriteClockAfterOldExpireReportsLost
      TestTryRenewPostWriteClockFailureIsTransient
      TestTryRenewStrictModeRequiresStrongConsistency
      TestTryRenewOwnTombstoneReportsLost
      TestStartRenewalWatchdogFiresWhenWriteBlocks
      TestStartRenewalDelayedSuccessAfterWatchdogDoesNotRevive
      TestStartRenewalSchedulesImmediateRenewWhenRemainingIntervalConsumed
      TestStopRenewalStopsWatchdogWithoutOnLeaseLost
      TestUnlockAfterLostDoesNotReviveState

- [ ] Run:

      ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryRenew|TestStartRenewal|TestStopRenewal|TestUnlock' -count=1

  Expected: selected tests pass. The preserved delayed-final-write subtest does
  not need to pass until Milestone 4 adds cleanup tombstone writes.

- [ ] Run `make bazel_prepare` if this milestone adds any new top-level Go test functions.

- [ ] Commit:

      git add pkg/objstore/locking.go pkg/objstore/export_test.go pkg/objstore/locking_test.go
      git commit -m "pkg/objstore: make renewal terminal and watchdog bounded"

## Milestone 4: Cleanup Tombstones

Scope: `pkg/objstore/locking_helper.go`, `pkg/objstore/locking.go`, `pkg/objstore/locking_test.go`.

After this milestone, stale cleanup writes a singleton tombstone before deleting a generation lock instance, and renewal checks its own tombstone exactly.

- [ ] Reuse the exact `tombstonePathForLockPath` helper added in Milestone 3. Do not add a second mapping or a cleanup-only variant with different parsing rules.
- [ ] Make `CleanUpStaleTruncateLock` resolve `opts ...LockOption` and pass the resolved options through `tryCleanUpStaleLockFamily`, `cleanUpStaleLockFamily`, and per-instance cleanup helpers.
- [ ] In cleanup strict mode, call `requireStrongConsistencyStorage(storage)` before relying on lock-family lists, tombstone lists, `FileExists`, or delete-not-found classification. If the check fails, return a cleanup error and do not write tombstones or delete candidates.

- [ ] Add list helpers for tombstones by logical family:

      listCleanupTombstones(ctx, storage, logicalPath) (map[string]struct{}, error)

  The map key should be the physical lock instance path reconstructed from the tombstone path, so cleanup can compare against family candidates.

- [ ] Change `cleanUpStaleLockFamily` flow:

      list family candidates
      classify candidates
      list tombstones for the logical family
      for each candidate:
          if unknown committed non-intent:
              append candidate cleanup error and continue
          if not cleanup eligible:
              continue
          if tombstoned:
              DeleteFile(candidate)
              treat delete success or confirmed absence as reclaimed/success
              continue
          read latest metadata
          if read/parse/LeaseClock time fails:
              append candidate cleanup error and continue
          if existingStalePredicate(now, ExpireAt, LeaseTTL):
              WriteFile(tombstonePath, empty)
              if tombstone write fails: append error, do not delete
              DeleteFile(candidate)

- [ ] Keep stale predicate unchanged: `now.After(meta.ExpireAt.Add(LeaseTTL))`.
- [ ] If `DeleteFile` after tombstone fails and no typed not-found exists, call `FileExists`. Confirmed absence is success; unknown existence remains cleanup error.
- [ ] If tombstone list fails, do not write new tombstones or delete candidates in that family attempt.
- [ ] Keep acquire verification unchanged with respect to tombstones: acquire does not read tombstones and does not ignore visible lock files.
- [ ] Add tests:

      TestCleanupWritesTombstoneBeforeDeletingStaleLock
      TestCleanupDoesNotDeleteWhenTombstoneWriteFails
      TestCleanupDeletesAlreadyTombstonedInstanceWithoutReadingMeta
      TestCleanupDeleteNotFoundAfterTombstoneIsSuccess
      TestCleanupTombstoneListFailureDoesNotDeleteCandidates
      TestCleanupStrictModeRequiresStrongConsistency
      TestCleanupReportsLegacyOrMalformedCandidateErrorButContinues
      TestAcquireDoesNotIgnoreTombstonedZombie
      TestTryRenewOwnTombstoneBlocksDelayedWriteRevival
      TestTryRenewSuccess/blocks_competing_acquire_while_final_renew_write_is_pending

- [ ] Run:

      ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestCleanup|TestAcquireDoesNotIgnoreTombstonedZombie|TestTryRenew' -count=1

  Expected: all selected tests pass. The preserved delayed-final-write subtest should pass here.

- [ ] Run `make bazel_prepare` if this milestone adds any new top-level Go test functions.

- [ ] Commit:

      git add pkg/objstore/locking.go pkg/objstore/locking_helper.go pkg/objstore/locking_test.go
      git commit -m "pkg/objstore: add cleanup tombstones for lease locks"

## Milestone 5: BR PD Clock Injection

Scope: `br/pkg/leaseclock/lease_clock.go`, `br/pkg/stream/stream_metas.go`, `br/pkg/restore/log_client/client.go`, `br/pkg/task/stream.go`, `br/pkg/task/operator/config.go`, `br/pkg/task/operator/migrate_to.go`, related tests.

After this milestone, BR production lock paths inject direct PD-backed lease time and strict strong-consistency proof options.

- [ ] Create `br/pkg/leaseclock/lease_clock.go` with `PDLeaseClock` using `pd.Client.GetTS` and `oracle.GetTimeFromTS`.
- [ ] Add tests in `br/pkg/leaseclock/lease_clock_test.go` using a fake PD client or a small interface wrapper if full `pd.Client` is too large. If `pd.Client` is too large for a simple fake, make `PDLeaseClock` depend on a local minimal interface:

      type tsoClient interface {
          GetTS(context.Context) (int64, int64, error)
      }

  Then `pd.Client` satisfies it.

- [ ] Add `MigrationExt.WithLockOptions(opts ...objstore.LockOption) MigrationExt` and pass `m.lockOptions...` to all `LockWithRetry` calls in `GetReadLock`, `lockForAppend`, and `MergeAndMigrateTo`.
- [ ] In `br/pkg/restore/log_client/client.go`, make `LogClient.GetLockedMigrations` build options from `rc.pdClient`:

      opts := []objstore.LockOption{
          objstore.WithLeaseClock(leaseclock.NewPDLeaseClock(rc.pdClient)),
          objstore.WithRequireStrongConsistency(),
      }
      ext := stream.MigrationExtension(rc.storage).WithLockOptions(opts...)

- [ ] In `br/pkg/task/stream.go::RunStreamTruncate`, create or reuse a PD manager before lock cleanup/acquire. Use `cfg.PD`, `cfg.TLS`, and existing keepalive config. Build the same two lock options and apply them to `CleanUpStaleTruncateLock`, `LockRemoteTruncate`, and the `MigrationExtension` used for `CleanUpCompactions`. Close the PD manager/client on every return path with the package's existing cleanup pattern.
- [ ] In `br/pkg/task/operator/config.go`, extend `MigrateToConfig` so the command has PD and TLS configuration. The concrete path is:

      type MigrateToConfig struct {
          task.Config
          Recent    bool
          MigrateTo int
          Base      bool
          Yes       bool
          DryRun    bool
      }

  Update `DefineFlagsForMigrateToConfig` to call `task.DefineCommonFlags(flags)` instead of manually defining storage and backend flags. Keep the existing migrate-to specific flags. Update `ParseFromFlags` to call `cfg.Config.ParseFromFlags(flags)` and then parse migrate-to specific flags.

- [ ] In `br/pkg/task/operator/migrate_to.go`, use `cfg.Storage` and `cfg.BackendOptions` from embedded `task.Config`, dial PD with existing `dialPD(ctx, &cfg.Config)`, build lock options, and apply them to `stream.MigrationExtension(st).WithLockOptions(...)`. Close the PD manager/client on every return path with the package's existing cleanup pattern.
- [ ] For `operator migrate-to`, verify and document in tests that the existing storage flag behavior is preserved. The command must still accept the original storage input, must still parse backend options, and must now reject or fail clearly when PD is required for lease-lock correctness but not configured.
- [ ] Update tests for `MigrationExt` so test-only paths either pass fake lock options or use local-clock compatibility intentionally.
- [ ] Add tests:

      TestMigrationExtWithLockOptionsPassesClockToReadLock
      TestMergeAndMigrateToUsesInjectedLeaseClock
      TestAppendMigrationUsesInjectedLeaseClockForBothLocks
      TestGetLockedMigrationsUsesPDLeaseClock
      TestRunMigrateToParsesPDAndTLSFlags
      TestRunMigrateToPreservesStorageAndBackendFlagParsing
      TestRunMigrateToRequiresPDForLeaseLockClock

- [ ] Run:

      ./tools/check/failpoint-go-test.sh br/pkg/leaseclock -run 'Test.*PDLeaseClock|Test.*LeaseClock' -count=1

      ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'TestMergeAndMigrateTo|Test.*AppendMigration|TestMigrationExtWithLockOptions' -count=1

      ./tools/check/failpoint-go-test.sh br/pkg/restore/log_client -run 'TestGetLockedMigrations' -count=1

      ./tools/check/failpoint-go-test.sh br/pkg/task/operator -run 'Test.*MigrateTo' -count=1

      ./tools/check/failpoint-go-test.sh br/pkg/task -run 'Test.*StreamTruncate' -count=1

  Expected: selected tests pass. If a package has no matching tests, the command should report no tests to run and exit successfully.

- [ ] Run `make bazel_prepare` because this milestone adds `br/pkg/leaseclock` and new top-level Go tests.

- [ ] Commit:

      git add br/pkg/leaseclock br/pkg/stream br/pkg/restore/log_client br/pkg/task pkg/objstore
      git commit -m "br: inject PD lease clocks for object locks"

## Milestone 6: Full Test Matrix and Validation

Scope: tests and final verification.

After this milestone, the implementation has targeted coverage for every correctness boundary in the analysis document.

- [ ] Ensure `pkg/objstore/locking_test.go` includes race-shaped tests for:

      delayed renewal write after cleanup and replacement acquire
      acquire write/verification returning after acquired lease expiration
      watchdog firing while renewal WriteFile is blocked
      successful renewal with little remaining lease time schedules immediate renewal
      confirmed missing own lock object during renewal
      tombstoned zombie blocks acquire but cleanup direct-deletes it
      PD clock failure in acquire, renewal, and cleanup
      malformed/legacy lock names are not auto-deleted but do not stop cleanup of other parseable candidates

- [ ] Run the focused objstore package test command:

      ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryLock|TestLockWithRetry|TestLockRemoteTruncate|TestTryRenew|TestStartRenewal|TestCleanup|TestAcquire|TestUnlock' -count=1

  Expected: pass.

- [ ] Run focused BR package tests:

      ./tools/check/failpoint-go-test.sh br/pkg/stream -run 'TestMergeAndMigrateTo|Test.*AppendMigration|TestMigrationExtWithLockOptions|TestLockForAppend' -count=1

      ./tools/check/failpoint-go-test.sh br/pkg/restore/log_client -run 'TestGetLockedMigrations' -count=1

      ./tools/check/failpoint-go-test.sh br/pkg/task -run 'Test.*StreamTruncate' -count=1

      ./tools/check/failpoint-go-test.sh br/pkg/task/operator -run 'Test.*MigrateTo' -count=1

  Expected: pass or no tests to run for packages with no matching tests.

- [ ] Run `make bazel_prepare` because this plan adds a new Go package and new
  top-level Go tests in existing `*_test.go` files.

      make bazel_prepare

  Expected: command succeeds. Include any resulting Bazel metadata changes if generated.

- [ ] Run docs checks because this plan and likely review notes under `docs/agents/` changed:

      git diff --check

      rg -n -P '\x60(MUST(?: NOT)?|SHOULD|MAY)\x60' AGENTS.md docs/agents/agents-review-guide.md docs/agents/br

  Expected: no output from `git diff --check`; no wrapped normative keyword matches.

- [ ] Run Ready validation before claiming completion:

      make lint

  Expected: pass.

- [ ] Commit any final test/docs adjustments:

      git add .
      git commit -m "test: cover lease lock delayed renewal races"

## Validation and Acceptance

The implementation is accepted when all of the following are true:

- A delayed final renewal write cannot be accepted after cleanup tombstoned its physical instance.
- If renewal storage I/O blocks longer than the remaining proven lease window, `onLeaseLost` is called once and later successful I/O cannot revive the lock.
- Acquire does not return an active lock if acquire-time writes and verification consumed the acquired lease window.
- Renewal scheduling and watchdog duration use remaining lease time, not a fresh full `LeaseTTL` after the operation returns.
- Cleanup writes a singleton tombstone before deleting stale generation instances.
- Acquire remains fail-closed and does not ignore tombstoned zombie lock files.
- BR production lock paths inject a PD-backed clock and strict strong-consistency proof options.
- Legacy or malformed lock names are not automatically deleted by the tombstone protocol.
- Targeted tests, `make bazel_prepare`, docs checks, and `make lint` have passing evidence recorded in this plan.

## Idempotence and Recovery

All test commands are safe to rerun. If failpoint-based tests leave rewritten files behind after interruption, run:

    make failpoint-disable

Then inspect `git status --short` and restore only generated failpoint artifacts that are unrelated to the implementation. Do not reset the worktree wholesale.

If `make bazel_prepare` changes Bazel metadata, keep those changes. If it fails because of dependency or toolchain state, record the error in `Surprises & Discoveries` and fix the underlying Bazel metadata or environment issue before continuing.

If a milestone becomes too large, stop after a passing targeted test, update `Progress`, commit the completed subset, and add a short note in `Outcomes & Retrospective`.

## Artifacts and Notes

Primary design reference:

    docs/agents/br/lease-lock-renewal-delayed-write-analysis.md

Related prior implementation plan:

    docs/agents/br/lease-lock-renewal-ownership-implementation-plan.md

Preserved regression test that should pass after tombstone and terminal renewal state are implemented:

    pkg/objstore/locking_test.go:
    TestTryRenewSuccess/blocks_competing_acquire_while_final_renew_write_is_pending
