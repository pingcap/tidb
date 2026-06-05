# Lease Lock Cleanup and HA Model Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic `pkg/objstore` model tests for stale-cleanup/reacquire races, multi-contender cleanup retry interleavings, and high-value protected-work death / renewal-lost hang semantics.

**Architecture:** Extend the existing `pkg/objstore/locking_model_concurrency_test.go` harness with narrowly scoped storage barriers and worker helpers. Use production lock APIs wherever possible; test-only wrappers only control operation ordering and record observable side effects. Keep `protected work hung while renewal alive` as a lower-priority follow-up because existing live-holder cleanup and successful-renewal tests already cover most of that behavior.

**Tech Stack:** Go tests in package `objstore_test`, TiDB failpoint-aware test runner, local object storage test doubles, `testify/require`, existing `criticalSectionAudit` and `protectedWorker` helpers.

---

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

This work proves that the lease-lock stale cleanup and HA model behavior remains safe under the most important remaining interleavings. After implementation, a developer can run targeted `pkg/objstore` tests and observe that an old stale cleanup operation does not delete a newly acquired physical lock, that multiple contenders cleaning and retrying around the same stale blocker do not both enter protected work, and that protected work cannot resume after renewal has declared lease loss.

The user-visible safety property is `业务临界区安全`: a holder may run protected business operations only while it owns a valid lease. Availability is also covered for dead holders: if a holder dies and stops renewal without unlocking, later contenders must be able to reclaim after the stale boundary.

## Progress

- [x] (2026-06-04) User approved the high-priority scope: cleanup/reacquire, contender retry cleanup, and protected-work death / hung-after-loss semantics.
- [x] (2026-06-04) User clarified that `contender retry cleanup` is required and that `protected work hung while renewal alive` is lower value for this round.
- [x] (2026-06-04) Drafted this implementation plan.
- [x] (2026-06-04) Implement storage barrier helpers.
- [x] (2026-06-04) Implement `TestLeaseLockCleanupDoesNotDeleteReacquiredInstance` migration-write case.
- [x] (2026-06-04) Implement `TestLeaseLockContenderRetryCleanupInterleaving`.
- [x] (2026-06-04) Implement `TestLeaseLockProtectedWorkDeathAndHangSemantics` high-priority subcases.
- [x] (2026-06-04) Run WIP validation for Task 1 through Task 4.
- [x] (2026-06-04) Run `make bazel_prepare` after adding new top-level `TestXxx` functions.
- [x] (2026-06-04) Run Ready validation before claiming PR readiness, including `make lint`.
- [x] (2026-06-05) Implement read/write mixed cleanup interleaving.
- [x] (2026-06-05) Implement intent lifecycle interleaving.
- [x] (2026-06-05) Implement one append-write family smoke case.
- [x] (2026-06-05) Run Task 9 WIP validation, Bazel metadata preparation, and final hygiene checks.

## Surprises & Discoveries

- Observation: A new holder cannot acquire a conflicting lock while the old stale physical instance still exists.
  Evidence: `verifyLockFamily` lists lock-family candidates and rejects conflicting committed instances before conditional put commit. Therefore `cleanup/reacquire` cannot block before `DeleteFile(A)` and still acquire B through production APIs. The deterministic harness must either block after deleting A but before cleanup returns, or explicitly simulate a backend where the delete is externally visible before the cleanup caller resumes. This plan uses the first approach.

- Observation: `LockWithRetry` has production-scale retry backoff and jitter.
  Evidence: `pkg/objstore/locking.go` creates `utils.InitialRetryState(lockRetryTimes, 1*time.Second, 60*time.Second)` and adds a 2.5s-7.5s jitter. Contender tests should cancel losing contenders once one holder succeeds, instead of waiting for full retry exhaustion.

- Observation: `lateWriteStorage` starts a background late-commit goroutine after returning the renewal write timeout.
  Evidence: `lateWriteStorage.WriteFile` waits for `releaseLate` and then writes with `context.Background()`. Tests that release late commit must also wait for `lateCommitted`, otherwise `t.TempDir()` cleanup can race with the background write.

## Decision Log

- Decision: Keep `contender retry cleanup` as a high-priority test group.
  Rationale: It exercises the real contender workflow: conflict, stale cleanup, retry, acquire, and protected work. It is the closest mock-level representation of multiple BR instances trying to recover from the same stale blocker.
  Date/Author: 2026-06-04 / Codex + user.

- Decision: Lower the priority of `protected work hung while renewal alive`.
  Rationale: A hung business operation with live renewal mostly repeats existing live-holder cleanup and successful-renewal coverage. The higher-value protected-work cases are dead holder stale reclaim and hung work after renewal lost.
  Date/Author: 2026-06-04 / Codex + user.

- Decision: Use storage wrappers only for ordering and observation.
  Rationale: The tests should keep production lock APIs in charge of acquire, verify, cleanup, renewal, and unlock semantics. Wrappers may block a specific storage operation and record paths, but they must not silently change lock-family conflict rules.
  Date/Author: 2026-06-04 / Codex.

- Decision: Do not treat truncate / read / append path expansion as the next P0.
  Rationale: The migration-write cleanup/retry tests already exercise the main stale committed-instance path. Copying that shape across families has lower proof value than testing the read/write compatibility matrix and `.INTENT.` lifecycle interleavings. Keep one append-write smoke as a later representative family check, not a full matrix.
  Date/Author: 2026-06-05 / Codex + user.

## Outcomes & Retrospective

Task 1 helper implementation is complete. It added `cleanupDeleteBarrierStorage`, `requireDeleteStarted`, and `requireNoDeleteCallForPath` in `pkg/objstore/locking_model_concurrency_test.go`.

Task 2 migration-write implementation is complete. It added `TestLeaseLockCleanupDoesNotDeleteReacquiredInstance`, which holds stale cleanup after physical lock A has been deleted, reacquires live lock B through production acquire, proves B can perform protected work before and after cleanup returns, and verifies cleanup does not delete or mutate B.

Task 3 migration-write implementation is complete. It added `TestLeaseLockContenderRetryCleanupInterleaving`, which forces two production `LockWithRetry` contenders to observe and attempt cleanup of the same stale A, then proves exactly one contender enters protected work and that winner remains locked while the loser is canceled.

Task 4 implementation is complete. It added `TestLeaseLockProtectedWorkDeathAndHangSemantics`, covering dead-holder reclaim only after `ExpireAt + staleReclaimGrace` and hung protected work returning `ok=false` after renewal loss. During validation, the hung-after-loss subtest exposed a cleanup race with `lateWriteStorage`'s background late commit; the test now waits for `lateCommitted` before temp directory cleanup.

Task 6 implementation is complete. It generalized `startLockWithRetryContender` to accept the locker, logical path, and operation label, then added `writer cleanup of stale read does not break live reader`. The subtest proves stale read A and live read B can coexist while writer cleanup is paused, old cleanup removes only A, live B keeps its `TxnID`, writers remain blocked while B is live, and a writer succeeds after B unlocks.

Task 7 implementation is complete. It added `intentWriteBarrierStorage`, `requireIntentStarted`, `failTargetWriteOnceStorage`, and `TestLeaseLockIntentLifecycleInterleaving`. The subtests cover observable in-flight read intent blocking a writer without creating a committed owner, own-intent cleanup after target commit failure, and foreign intent not being reclaimed as a stale committed lock.

Task 8 implementation is complete. It added the `append write` cleanup/reacquire smoke under `TestLeaseLockCleanupDoesNotDeleteReacquiredInstance`, proving the physical-path cleanup safety property for the append lock family without expanding into a full low-value family matrix.

Validation so far:

- `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockRenewalObservationHangIsTransient/low_remaining_lease_skips_renewal_attempt' -count=1` exited 0.
- `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockCleanupDoesNotDeleteReacquiredInstance' -count=1` exited 0.
- `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockContenderRetryCleanupInterleaving' -count=1` exited 0.
- `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockProtectedWorkDeathAndHangSemantics' -count=1` exited 0.
- `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(CleanupDoesNotDeleteReacquiredInstance|ContenderRetryCleanupInterleaving|ProtectedWorkDeathAndHangSemantics)' -count=1` exited 0.
- `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(TerminalReasonStableAfterLeaseLost|NormalUnlockWinsInFlightRenewal|RenewalObservationHangIsTransient|RenewalAmbiguousWriteAndProofFailureStopProtectedWork)' -count=1` exited 0.
- `make bazel_prepare` exited 0 and produced no tracked Bazel / DEPS diff.
- `make lint` exited 0.
- `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(ContenderRetryCleanupInterleaving|IntentLifecycleInterleaving|CleanupDoesNotDeleteReacquiredInstance)' -count=1` exited 0 on 2026-06-05 after Task 6-8.
- `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(TerminalReasonStableAfterLeaseLost|NormalUnlockWinsInFlightRenewal|RenewalObservationHangIsTransient|RenewalAmbiguousWriteAndProofFailureStopProtectedWork|ProtectedWorkDeathAndHangSemantics)' -count=1` exited 0 on 2026-06-05 after Task 6-8.
- `make bazel_prepare` exited 0 on 2026-06-05 after adding `TestLeaseLockIntentLifecycleInterleaving`; it produced no tracked Bazel / DEPS diff.
- The docs/agents policy-keyword grep exited 1 with no output.
- `git diff --check` and `git diff --cached --check` exited 0.
- `make lint` exited 0 on 2026-06-05 after Task 6-8.

Review so far:

Task 1 and Task 2 both passed independent spec review and code-quality review. Later Task 9 cleanup fixed legacy no-intent assertions to scan real physical prefixes such as `v1/LOCK.READ.` and `v1/LOCK.WRIT.`.

Task 3 passed spec re-review after moving protected-work audit into the successful contender path and directly proving the winner still blocks a third acquire before unlock. Code-quality review found the original `8s` first-success timeout could flake because production retry is `1s` plus `2.5s..7.5s` jitter; the timeout was raised to `10s`.

Task 4 passed independent spec review and code-quality review. Code-quality review suggested stronger early-timeout and late-commit path assertions; both were added.

Task 6 plan review initially blocked on a false single-read-path assertion and wrong intent prefixes. The plan was fixed to select the live read path by excluding stale A and to scan real physical prefixes (`v1/LOCK.READ.`, `v1/LOCK.WRIT.`). Task 6 implementation then passed focused spec and code-quality review.

Task 7 plan review initially blocked on weak false-owner proof and wrong intent prefixes. The plan was fixed to assert the in-flight intent file exists, the committed target does not exist, and the blocked reader has not returned before release. Task 7 implementation passed focused spec and code-quality review; code-quality review suggested tightening the foreign-intent error assertion to `context.DeadlineExceeded`, which was applied during Task 9 cleanup.

Task 8 implementation passed focused spec/code review. The append smoke remained scoped to cleanup/reacquire family routing.

Residual gaps: `br_lease_lock` integration remains an independent final verification item and was not rerun in this pass.

## Context and Orientation

The relevant package is `pkg/objstore`. The implementation under test is split across:

- `pkg/objstore/locking.go`: production acquire, retry, renewal, and unlock logic.
- `pkg/objstore/locking_helper.go`: lock-family classification and stale cleanup helpers.
- `pkg/objstore/locking_concurrency_test.go`: first-phase concurrency helpers, including `criticalSectionAudit`, `protectedWorker`, `requireNoIntentWithPrefix`, and stale cleanup fixtures.
- `pkg/objstore/locking_model_concurrency_test.go`: current Phase 2 model tests and controlled storage / clock helpers.
- `pkg/objstore/export_test.go`: test-only exports for renewal hooks and timing constants.

Definitions:

`physical lock path` means a concrete object path such as `v1/LOCK.WRIT.<32hex>`. A new acquire attempt creates a new physical path.

`lock family` means the logical conflict group rooted at `v1/LOCK`, `v1/APPEND_LOCK`, or `truncating.lock`. Family verification prevents incompatible physical instances from coexisting as active holders.

`contender` means an actor that does not currently hold the lock and is trying to acquire it, usually through `LockWithRetry`.

`protected work` means the business operation guarded by the lock. In these tests, `protectedWorker` or a barrier variant records when a holder enters or attempts a protected step.

`stale blocker A` means a physical lock instance with an `ExpireAt` older than `staleReclaimGrace`, so cleanup may reclaim it.

`new holder B` means a later lock holder that successfully acquired a different physical path in the same family after stale cleanup made the family available.

## Plan of Work

The implementation should keep all new tests in `pkg/objstore/locking_model_concurrency_test.go` unless a helper clearly belongs in `locking_concurrency_test.go`. Do not add production APIs. Test-only exports are acceptable only if a production helper is otherwise unreachable and the alternative would be weaker or much slower.

First add a storage barrier helper that can block deletion of one stale physical path either before the delete reaches the base storage or after the base storage delete succeeds but before the caller resumes. The before-delete mode is for contender retry cleanup. The after-delete mode is for cleanup/reacquire, because a conflicting new holder cannot acquire until A is actually gone.

Then add `TestLeaseLockCleanupDoesNotDeleteReacquiredInstance`. It should write stale A, run cleanup until `DeleteFile(A)` has succeeded but is paused before return, acquire B, prove B can run protected work, release cleanup, and prove cleanup did not delete B.

Next add `TestLeaseLockContenderRetryCleanupInterleaving`. It should write stale A, run two real `LockWithRetry` contenders around a before-delete cleanup barrier, release the cleanup, cancel losers once one succeeds, and assert at most one holder enters protected work.

Finally add `TestLeaseLockProtectedWorkDeathAndHangSemantics` with high-priority subcases only: dead holder with renewal stopped is reclaimable only after the stale boundary, and hung protected work cannot continue after renewal loss.

## Concrete Steps

### Task 1: Add Cleanup Delete Barrier Storage

**Files:**

- Modify: `pkg/objstore/locking_model_concurrency_test.go`

- [ ] **Step 1: Write helper skeleton near existing storage wrappers**

Add a helper type after `deleteRecordingStorage` and before `operationBlockingStorage`.

    type cleanupDeleteBarrierMode int

    const (
        cleanupDeleteBarrierBefore cleanupDeleteBarrierMode = iota
        cleanupDeleteBarrierAfter
    )

    type cleanupDeleteBarrierStorage struct {
        storeapi.Storage

        mu          sync.Mutex
        path        string
        mode        cleanupDeleteBarrierMode
        blocked     int
        deleteCalls []string
        started     chan interceptedOperation
        released    chan interceptedOperation
        release     chan struct{}
    }

    func newCleanupDeleteBarrierStorage(base storeapi.Storage, path string, mode cleanupDeleteBarrierMode) *cleanupDeleteBarrierStorage {
        return &cleanupDeleteBarrierStorage{
            Storage:  base,
            path:     path,
            mode:     mode,
            started:  make(chan interceptedOperation, 8),
            released: make(chan interceptedOperation, 8),
            release:  make(chan struct{}),
        }
    }

    func (s *cleanupDeleteBarrierStorage) releaseDelete() {
        s.mu.Lock()
        old := s.release
        s.release = make(chan struct{})
        s.mu.Unlock()
        close(old)
    }

    func (s *cleanupDeleteBarrierStorage) DeleteFile(ctx context.Context, name string) error {
        s.mu.Lock()
        shouldBlock := name == s.path
        if shouldBlock {
            s.blocked++
        }
        s.deleteCalls = append(s.deleteCalls, name)
        release := s.release
        mode := s.mode
        s.mu.Unlock()

        if !shouldBlock {
            return s.Storage.DeleteFile(ctx, name)
        }

        var err error
        if mode == cleanupDeleteBarrierBefore {
            s.started <- interceptedOperation{Path: name}
            select {
            case <-ctx.Done():
                err = ctx.Err()
            case <-release:
                err = s.Storage.DeleteFile(ctx, name)
            case <-time.After(time.Second):
                err = errors.New("guard timeout waiting to release cleanup DeleteFile before base delete")
            }
        } else {
            err = s.Storage.DeleteFile(ctx, name)
            if err == nil {
                // In after mode, started means the target path is already absent from base storage.
                s.started <- interceptedOperation{Path: name}
                select {
                case <-ctx.Done():
                    err = ctx.Err()
                case <-release:
                case <-time.After(time.Second):
                    err = errors.New("guard timeout waiting to release cleanup DeleteFile after base delete")
                }
            }
        }
        s.released <- interceptedOperation{Path: name, Err: err}
        return err
    }

    func (s *cleanupDeleteBarrierStorage) blockedDeleteCount() int {
        s.mu.Lock()
        defer s.mu.Unlock()
        return s.blocked
    }

    func (s *cleanupDeleteBarrierStorage) deleteCallSnapshot() []string {
        s.mu.Lock()
        defer s.mu.Unlock()
        return append([]string(nil), s.deleteCalls...)
    }

- [ ] **Step 2: Add helper assertions**

Add these helpers near `requireDeletedPath` / `requireNoDeleteSoon`.

    func requireDeleteStarted(t *testing.T, ch <-chan interceptedOperation, expected string) interceptedOperation {
        t.Helper()
        op := waitOperationFinished(t, ch, "cleanup DeleteFile start")
        require.Equal(t, expected, op.Path)
        return op
    }

    func requireNoDeleteCallForPath(t *testing.T, calls []string, forbidden string) {
        t.Helper()
        for _, p := range calls {
            require.NotEqual(t, forbidden, p, "cleanup must not delete live holder path")
        }
    }

- [ ] **Step 3: Run compile-focused test command**

Run from repository root:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockRenewalObservationHangIsTransient/low_remaining_lease_skips_renewal_attempt' -count=1

Expected before any new tests are added: PASS. This confirms the helper compiles and did not break existing model tests.

### Task 2: Add Cleanup Does Not Delete Reacquired Instance Test

**Files:**

- Modify: `pkg/objstore/locking_model_concurrency_test.go`

- [ ] **Step 1: Write the failing test with one family first**

Add a new top-level test. Start with migration write because it is the main BR restore path.

    func TestLeaseLockCleanupDoesNotDeleteReacquiredInstance(t *testing.T) {
        t.Run("migration write", func(t *testing.T) {
            parentCtx, cancel := context.WithCancel(context.Background())
            defer cancel()
            defer objstore.TESTSetStaleReclaimGrace(30 * time.Millisecond)()

            base, pth := createMockStorage(t)
            now := time.Date(2030, 6, 4, 15, 0, 0, 0, time.UTC)
            stalePath := "v1/LOCK.WRIT.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            writeLockMeta(t, base, stalePath, objstore.LockMeta{
                LockedAt: now.Add(-time.Hour),
                ExpireAt: now.Add(-time.Hour),
                TxnID:    []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
                Hint:     "old-stale",
            })

            storage := newCleanupDeleteBarrierStorage(base, stalePath, cleanupDeleteBarrierAfter)
            cleanupCtx, cleanupCancel := context.WithCancel(parentCtx)
            cleanupDone := make(chan error, 1)
            go func() {
                lock, err := objstore.LockWithRetry(cleanupCtx, objstore.TryLockRemoteWrite, storage, "v1/LOCK", "cleanup-contender", func() {}, localLeaseClock())
                if err == nil {
                    _ = lock.Unlock(context.Background())
                }
                cleanupDone <- err
            }()
            requireDeleteStarted(t, storage.started, stalePath)
            requireFileNotExists(t, filepath.Join(pth, stalePath))

            liveLock, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "new-holder", localLeaseClock())
            require.NoError(t, err)
            livePath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
            require.NotEqual(t, stalePath, livePath)
            liveMeta := readLockMeta(t, base, livePath)

            audit := newCriticalSectionAudit(t.Name())
            worker := startProtectedWorker(t, parentCtx, audit, "owner-b", "migration-write", liveLock.String())
            step, ok := worker.requestStep(t)
            require.True(t, ok)
            require.Positive(t, step)

            cleanupCancel()
            storage.releaseDelete()
            _ = waitOperationFinished(t, storage.released, "cleanup DeleteFile release")
            select {
            case <-cleanupDone:
            case <-time.After(time.Second):
                t.Fatal("timed out waiting for cleanup contender to exit")
            }

            require.Equal(t, liveMeta.TxnID, readLockMeta(t, base, livePath).TxnID)
            requireNoDeleteCallForPath(t, storage.deleteCallSnapshot(), livePath)
            step, ok = worker.requestStep(t)
            require.True(t, ok)
            require.Positive(t, step)
            require.NoError(t, liveLock.Unlock(parentCtx))
            worker.stop(workerStopTestStop)
            requireFileNotExists(t, filepath.Join(pth, livePath))
            next, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "next-holder", localLeaseClock())
            require.NoError(t, err)
            require.NoError(t, next.Unlock(parentCtx))
            requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")
        })
    }

Run from repository root:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockCleanupDoesNotDeleteReacquiredInstance/migration_write' -count=1

Expected at this point: the test may fail while helper details are refined. It must fail for a real assertion or timeout related to the new behavior, not for missing imports or compilation errors.

- [ ] **Step 2: Make the migration write case pass**

Adjust only the helper/test code. Do not change production cleanup behavior unless the test reveals cleanup deletes the live path or leaves unavoidable stale state. If `LockWithRetry` cleanup goroutine sleeps after cleanup, canceling `cleanupCtx` before releasing the after-delete barrier is the intended exit path.

Run:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockCleanupDoesNotDeleteReacquiredInstance/migration_write' -count=1

Expected: PASS.

- [ ] **Step 3: Extend table coverage**

Turn the test into a table with these cases:

    type cleanupReacquireCase struct {
        name          string
        logicalPath   string
        stalePath     string
        acquireNew    func(context.Context, storeapi.Storage, string) (*objstore.RemoteLock, error)
        cleanupStart  func(context.Context, storeapi.Storage) error
        intentPrefixes []string
    }

Use these rows:

- `truncate`: stale path `truncating.lock.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`, acquire with `objstore.TryLockRemoteTruncate`, cleanup with `objstore.CleanUpStaleTruncateLock`.
- `migration write`: stale path `v1/LOCK.WRIT.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`, acquire with `objstore.TryLockRemoteWrite(ctx, storage, "v1/LOCK", hint, localLeaseClock())`, cleanup via `LockWithRetry` write contender.
- `migration read`: stale path `v1/LOCK.READ.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`, acquire new read with `objstore.TryLockRemoteRead`, cleanup triggered by a write contender through `LockWithRetry`.
- `append write`: stale path `v1/APPEND_LOCK.WRIT.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`, acquire with `objstore.TryLockRemoteWrite(ctx, storage, "v1/APPEND_LOCK", hint, localLeaseClock())`, cleanup via `LockWithRetry` append contender.

Each row must assert:

- stale A is gone after the after-delete barrier starts.
- new B has a different path from A.
- B's `TxnID` remains unchanged after cleanup returns.
- B records at least one protected step before and after cleanup release.
- B unlocks successfully.
- follow-up acquire succeeds.
- no `.INTENT.` remains for the relevant physical prefixes.

Run:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockCleanupDoesNotDeleteReacquiredInstance' -count=1

Expected: PASS.

### Task 3: Add Contender Retry Cleanup Interleaving Test

**Files:**

- Modify: `pkg/objstore/locking_model_concurrency_test.go`

- [ ] **Step 1: Add contender result helper**

Add this helper near the cleanup test helpers:

    type lockContenderResult struct {
        ownerID string
        lock    *objstore.RemoteLock
        err     error
    }

    func startLockWithRetryContender(ctx context.Context, ownerID string, storage storeapi.Storage, lockPath string, locker objstore.Locker) <-chan lockContenderResult {
        ch := make(chan lockContenderResult, 1)
        go func() {
            lock, err := objstore.LockWithRetry(ctx, locker, storage, lockPath, ownerID, func() {}, localLeaseClock())
            ch <- lockContenderResult{ownerID: ownerID, lock: lock, err: err}
        }()
        return ch
    }

- [ ] **Step 2: Write the migration write vs write failing test**

Add:

    func TestLeaseLockContenderRetryCleanupInterleaving(t *testing.T) {
        t.Run("migration write contenders", func(t *testing.T) {
            ctx := context.Background()
            defer objstore.TESTSetStaleReclaimGrace(30 * time.Millisecond)()
            base, _ := createMockStorage(t)
            now := time.Date(2030, 6, 4, 16, 0, 0, 0, time.UTC)
            stalePath := "v1/LOCK.WRIT.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            writeLockMeta(t, base, stalePath, objstore.LockMeta{
                LockedAt: now.Add(-time.Hour),
                ExpireAt: now.Add(-time.Hour),
                TxnID:    []byte{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2},
                Hint:     "old-stale",
            })

            storage := newCleanupDeleteBarrierStorage(base, stalePath, cleanupDeleteBarrierBefore)
            c1Ctx, c1Cancel := context.WithCancel(ctx)
            defer c1Cancel()
            c2Ctx, c2Cancel := context.WithCancel(ctx)
            defer c2Cancel()

            c1 := startLockWithRetryContender(c1Ctx, "contender-1", storage, "v1/LOCK", objstore.TryLockRemoteWrite)
            requireDeleteStarted(t, storage.started, stalePath)
            c2 := startLockWithRetryContender(c2Ctx, "contender-2", storage, "v1/LOCK", objstore.TryLockRemoteWrite)

            storage.releaseDelete()
            first := waitFirstSuccessfulContender(t, c1, c2)
            if first.ownerID == "contender-1" {
                c2Cancel()
            } else {
                c1Cancel()
            }

            audit := newCriticalSectionAudit(t.Name())
            worker := startProtectedWorker(t, ctx, audit, first.ownerID, "migration-write", first.lock.String())
            step, ok := worker.requestStep(t)
            require.True(t, ok)
            require.Positive(t, step)
            require.NoError(t, first.lock.Unlock(ctx))
            worker.stop(workerStopTestStop)
            requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")
        })
    }

Also add:

    func waitFirstSuccessfulContender(t *testing.T, c1, c2 <-chan lockContenderResult) lockContenderResult {
        t.Helper()
        deadline := time.After(8 * time.Second)
        var lastErr error
        for c1 != nil || c2 != nil {
            select {
            case result := <-c1:
                c1 = nil
                if result.err == nil {
                    return result
                }
                lastErr = result.err
            case result := <-c2:
                c2 = nil
                if result.err == nil {
                    return result
                }
                lastErr = result.err
            case <-deadline:
                t.Fatalf("timed out waiting for successful contender, last error: %v", lastErr)
            }
        }
        t.Fatalf("no contender acquired lock, last error: %v", lastErr)
        return lockContenderResult{}
    }

Run:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockContenderRetryCleanupInterleaving/migration_write_contenders' -count=1

Expected: PASS after helper refinement. Runtime may include one production retry backoff; keep the timeout at 8 seconds for this subtest.

- [ ] **Step 3: Add remaining contender pairs**

Extend the test table with:

- `truncate contenders`: two goroutines call truncate acquisition. Since `LockRemoteTruncate` is not a `Locker`, use a small adapter with signature `func(ctx context.Context, storage storeapi.Storage, _ string, hint string, clock objstore.LeaseClock) (*objstore.RemoteLock, error) { return objstore.TryLockRemoteTruncate(ctx, storage, hint, clock) }`.
- `migration write blocks migration read`: one contender uses `TryLockRemoteWrite`, one contender uses `TryLockRemoteRead`; the write contender should be the one expected to clean stale read/write blockers. Use the same success-count assertion: at most one conflicting protected worker enters.
- `append write contenders`: two goroutines use `TryLockRemoteWrite` with `v1/APPEND_LOCK`.

For every case:

- Write stale blocker A.
- Block cleanup `DeleteFile(A)` before base delete.
- Start two contenders.
- Release cleanup.
- Wait for one success.
- Cancel the loser.
- Start protected worker only for the successful contender.
- Assert only that worker records a step.
- Unlock the winner.
- Assert no intent residue for the family.

Run:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockContenderRetryCleanupInterleaving' -count=1

Expected: PASS. If runtime is too high, keep only two highest-value cases in the default test and record the tradeoff in this plan before dropping coverage.

### Task 4: Add Protected Work Death and Hung-After-Loss Semantics

**Files:**

- Modify: `pkg/objstore/locking_model_concurrency_test.go`

- [ ] **Step 1: Add barrier protected worker helper**

Add near `startProtectedWorker`-related helpers in `locking_concurrency_test.go` only if reuse across files is clean; otherwise add a local model helper in `locking_model_concurrency_test.go`.

    type barrierProtectedWorker struct {
        worker *protectedWorker
        entered chan struct{}
        release chan struct{}
        done    chan workerStepResult
    }

    func startBarrierProtectedStep(t *testing.T, worker *protectedWorker) *barrierProtectedWorker {
        t.Helper()
        b := &barrierProtectedWorker{
            worker:  worker,
            entered: make(chan struct{}),
            release: make(chan struct{}),
            done:    make(chan workerStepResult, 1),
        }
        go func() {
            close(b.entered)
            <-b.release
            step, ok := worker.requestStep(t)
            b.done <- workerStepResult{step: step, ok: ok}
        }()
        return b
    }

This helper models business work paused before attempting its next protected step. It does not record a step until the test releases the barrier.

- [ ] **Step 2: Add dead holder stale reclaim subtest**

Inside a new top-level test `TestLeaseLockProtectedWorkDeathAndHangSemantics`, add:

    t.Run("dead holder is reclaimed only after stale boundary", func(t *testing.T) {
        ctx := context.Background()
        defer objstore.TESTSetStaleReclaimGrace(30 * time.Millisecond)()
        base, pth := createMockStorage(t)
        leaseNow := time.Date(2030, 6, 4, 17, 0, 0, 0, time.UTC)
        clock := fixedLeaseClock{now: leaseNow}

        lock, err := objstore.TryLockRemoteWrite(ctx, base, "v1/LOCK", "dead-holder", clock)
        require.NoError(t, err)
        physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")

        beforeStale := fixedLeaseClock{now: leaseNow.Add(objstore.LeaseTTL + 10*time.Millisecond)}
        _, err = objstore.LockWithRetry(contextWithShortDeadline(t, 150*time.Millisecond), objstore.TryLockRemoteWrite, base, "v1/LOCK", "early-contender", func() {}, beforeStale)
        require.Error(t, err)
        requireFileExists(t, filepath.Join(pth, physicalPath))

        afterStale := fixedLeaseClock{now: leaseNow.Add(objstore.LeaseTTL + 31*time.Millisecond)}
        next, err := objstore.LockWithRetry(ctx, objstore.TryLockRemoteWrite, base, "v1/LOCK", "late-contender", func() {}, afterStale)
        require.NoError(t, err)
        requireFileNotExists(t, filepath.Join(pth, physicalPath))
        require.NoError(t, next.Unlock(ctx))
        _ = lock
        requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")
    })

Implement small helpers if missing:

    type fixedLeaseClock struct {
        now time.Time
    }

    func (c fixedLeaseClock) Now(context.Context) (time.Time, error) {
        return c.now, nil
    }

    func contextWithShortDeadline(t *testing.T, d time.Duration) context.Context {
        t.Helper()
        ctx, cancel := context.WithTimeout(context.Background(), d)
        t.Cleanup(cancel)
        return ctx
    }

Run:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockProtectedWorkDeathAndHangSemantics/dead_holder_is_reclaimed_only_after_stale_boundary' -count=1

Expected: PASS. The early contender should fail because `now` is before `ExpireAt + staleReclaimGrace`; the late contender should reclaim and acquire.

- [ ] **Step 3: Add hung protected work then renewal lost subtest**

Add:

    t.Run("hung protected work stops after renewal lost", func(t *testing.T) {
        parentCtx, cancel := context.WithCancel(context.Background())
        defer objstore.TESTSetLeaseConstants(200*time.Millisecond, 15*time.Millisecond, 5, 5*time.Millisecond)()
        defer objstore.TESTSetRenewalProofConstants(30*time.Millisecond, time.Millisecond)()

        base, _ := createMockStorage(t)
        storage := newLateWriteStorage(base)
        lock, err := objstore.TryLockRemoteWrite(parentCtx, storage, "v1/LOCK", "owner", localLeaseClock())
        require.NoError(t, err)
        physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
        storage.blockNextWrites(physicalPath, 1)

        audit := newCriticalSectionAudit(t.Name())
        worker := startProtectedWorker(t, parentCtx, audit, "owner-a", "migration-write", lock.String())
        barrier := startBarrierProtectedStep(t, worker)
        waitClosed(t, barrier.entered, "barrier protected work entered")
        startModelRenewal(t, parentCtx, lock, worker)
        t.Cleanup(func() {
            stopModelRenewal(cancel, lock, storage.releaseLateCommit)
        })

        _ = waitOperationFinished(t, storage.writeStarted, "renewal WriteFile start")
        returned := waitOperationFinished(t, storage.writeReturned, "renewal WriteFile return")
        require.ErrorIs(t, returned.Err, context.DeadlineExceeded)
        waitClosed(t, worker.lostCh(), "lease lost while protected work was hung")

        close(barrier.release)
        select {
        case result := <-barrier.done:
            require.False(t, result.ok)
        case <-time.After(time.Second):
            t.Fatal("timed out waiting for hung protected work to observe terminal state")
        }
        requireNoStepAfterAction(t, audit.snapshot(), "owner-a", "lost")
    })

Run:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockProtectedWorkDeathAndHangSemantics/hung_protected_work_stops_after_renewal_lost' -count=1

Expected: PASS. The barrier release must not produce a protected step after renewal loss.

- [ ] **Step 4: Run the full protected-work group**

Run:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockProtectedWorkDeathAndHangSemantics' -count=1

Expected: PASS.

### Task 5: Update Design Docs and Bazel Metadata

**Files:**

- Modify: `docs/agents/br/lease-lock-model-concurrency-test-design.md`
- Modify if generated: `pkg/objstore/BUILD.bazel`

- [ ] **Step 1: Mark implemented plan sections**

Update `docs/agents/br/lease-lock-model-concurrency-test-design.md` to mention which of the three high-priority groups landed and which lower-priority groups remain. Do not reintroduce the old unified timeout formula wording.

- [ ] **Step 2: Run Bazel metadata preparation**

Because this plan adds new top-level Go tests in an existing `*_test.go`, run:

    make bazel_prepare

Expected: command exits 0. If it changes `pkg/objstore/BUILD.bazel` or related Bazel metadata, include those files in the final diff. If it produces no diff, record that fact in `Outcomes & Retrospective`.

- [ ] **Step 3: Run docs/agents review-guide lightweight checks**

Run:

    rg -n -P '`(MUST(?: NOT)?|SHOULD|MAY)`' AGENTS.md docs/agents/agents-review-guide.md docs/agents/br/lease-lock-model-concurrency-test-design.md docs/agents/br/lease-lock-cleanup-ha-model-tests-implementation-plan.md

Expected: exit 1 with no output, or only intentional non-policy examples that are not introduced by this plan.

Run:

    test -e docs/agents/br/lease-lock-model-concurrency-test-design.md
    test -e docs/agents/br/lease-lock-cleanup-ha-model-tests-implementation-plan.md
    test -e pkg/objstore/locking_model_concurrency_test.go

Expected: all exit 0.

### Task 6: Add Read/Write Mixed Cleanup Interleaving Test

**Why this is next:** This is the highest-value follow-up because it exercises the migration lock compatibility matrix, not just another physical-path family. The core interleaving is: a writer starts cleanup of stale read A, a live reader B acquires while cleanup is still in flight, and the writer must neither delete B nor enter protected work while B is live.

**Files:**

- Modify: `pkg/objstore/locking_model_concurrency_test.go`
- Update: `docs/agents/br/lease-lock-cleanup-ha-model-tests-implementation-plan.md`
- Update: `docs/agents/br/lease-lock-model-concurrency-test-design.md`

- [ ] **Step 1: Generalize the contender helper**

Replace the current write-only `startLockWithRetryContender` helper with a parameterized helper. Preserve existing callers by updating them to pass `objstore.TryLockRemoteWrite`, `"v1/LOCK"`, and `"migration-write"`.

```go
func startLockWithRetryContender(
	parentCtx context.Context,
	storage storeapi.Storage,
	locker objstore.Locker,
	lockPath string,
	ownerID string,
	operation string,
	audit *criticalSectionAudit,
) (context.CancelFunc, <-chan lockContenderResult) {
	ctx, cancel := context.WithCancel(parentCtx)
	resultCh := make(chan lockContenderResult, 1)
	go func() {
		lock, err := objstore.LockWithRetry(ctx, locker, storage, lockPath, ownerID, func() {}, localLeaseClock())
		if err == nil {
			audit.recordEnter(ownerID, operation, lock.String(), "enter")
			audit.recordStep(ownerID, operation, lock.String(), "step-1")
		}
		resultCh <- lockContenderResult{ownerID: ownerID, lock: lock, err: err}
	}()
	return cancel, resultCh
}
```

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockContenderRetryCleanupInterleaving/migration_write_contenders' -count=1
```

Expected: PASS. This step must be a pure helper refactor with no behavior change.

- [ ] **Step 2: Add the read/write mixed subtest**

Add this helper near the other path assertion helpers. It is needed because a stale read A and a live read B intentionally coexist before the writer cleanup barrier is released.

```go
func requireSinglePathWithPrefixExcept(t *testing.T, storage storeapi.Storage, prefix string, excluded ...string) string {
	t.Helper()
	excludedSet := make(map[string]struct{}, len(excluded))
	for _, path := range excluded {
		excludedSet[path] = struct{}{}
	}
	var matches []string
	for _, path := range requireListedPathsWithPrefix(t, storage, prefix) {
		if strings.Contains(path, ".INTENT.") {
			continue
		}
		if _, ok := excludedSet[path]; ok {
			continue
		}
		matches = append(matches, path)
	}
	require.Len(t, matches, 1)
	return matches[0]
}
```

Add this subtest under `TestLeaseLockContenderRetryCleanupInterleaving`:

```go
t.Run("writer cleanup of stale read does not break live reader", func(t *testing.T) {
	parentCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer objstore.TESTSetStaleReclaimGrace(30 * time.Millisecond)()

	base, pth := createMockStorage(t)
	now := time.Now()
	stalePath := "v1/LOCK.READ.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	writeLockMeta(t, base, stalePath, objstore.LockMeta{
		LockedAt: now.Add(-time.Hour),
		ExpireAt: now.Add(-time.Hour),
		TxnID:    []byte{3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3},
		Hint:     "old-stale-reader",
	})

	storage := newCleanupDeleteBarrierStorage(base, stalePath, cleanupDeleteBarrierBefore)
	audit := newCriticalSectionAudit(t.Name())

	cancelWriter, writerResults := startLockWithRetryContender(
		parentCtx, storage, objstore.TryLockRemoteWrite, "v1/LOCK", "writer-contender", "migration-write", audit,
	)
	defer cancelWriter()
	requireDeleteStarted(t, storage.started, stalePath)
	requireFileExists(t, filepath.Join(pth, stalePath))

	readerLock, err := objstore.TryLockRemoteRead(parentCtx, storage, "v1/LOCK", "live-reader", localLeaseClock())
	require.NoError(t, err)
	readerPath := requireSinglePathWithPrefixExcept(t, base, "v1/LOCK.READ.", stalePath)
	require.NotEqual(t, stalePath, readerPath)
	readerMeta := readLockMeta(t, base, readerPath)
	requireFileExists(t, filepath.Join(pth, stalePath))
	requireFileExists(t, filepath.Join(pth, readerPath))

	readerWorker := startProtectedWorker(t, parentCtx, audit, "live-reader", "migration-read", readerLock.String())
	step, ok := readerWorker.requestStep(t)
	require.True(t, ok)
	require.Positive(t, step)

	storage.releaseDelete()
	released := waitOperationFinished(t, storage.released, "writer cleanup DeleteFile release")
	require.Equal(t, stalePath, released.Path)
	requireFileNotExists(t, filepath.Join(pth, stalePath))
	requireFileExists(t, filepath.Join(pth, readerPath))
	requireNoDeleteCallForPath(t, storage.deleteCallSnapshot(), readerPath)
	require.Equal(t, readerMeta.TxnID, readLockMeta(t, base, readerPath).TxnID)

	third, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "third-writer", localLeaseClock())
	require.ErrorContains(t, err, "conflict file")
	require.Nil(t, third)

	cancelWriter()
	writerResult := waitLockContenderResult(t, writerResults, "writer blocked by live reader", time.Second)
	require.Error(t, writerResult.err)
	require.Nil(t, writerResult.lock)

	step, ok = readerWorker.requestStep(t)
	require.True(t, ok)
	require.Positive(t, step)
	require.NoError(t, readerLock.Unlock(parentCtx))
	readerWorker.stop(workerStopTestStop)

	next, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "next-writer", localLeaseClock())
	require.NoError(t, err)
	require.NoError(t, next.Unlock(parentCtx))
	requireNoIntentWithPrefix(t, base, "v1/LOCK.READ.")
	requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")
})
```

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockContenderRetryCleanupInterleaving/writer_cleanup_of_stale_read_does_not_break_live_reader' -count=1
```

Expected: PASS. The important proof is that writer cleanup removes stale read A, preserves live read B, and writer acquisition remains blocked while B is live.

- [ ] **Step 3: Run the full contender group**

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockContenderRetryCleanupInterleaving' -count=1
```

Expected: PASS.

### Task 7: Add Intent Lifecycle Interleaving Tests

**Why this matters:** `.INTENT.` objects are the acquire two-phase boundary. These tests should prove intent objects do not create false owners, failed own intents are cleaned up, and foreign intents are treated conservatively without being reclaimed as stale committed locks.

**Files:**

- Modify: `pkg/objstore/locking_model_concurrency_test.go`
- Update: `docs/agents/br/lease-lock-cleanup-ha-model-tests-implementation-plan.md`
- Update: `docs/agents/br/lease-lock-model-concurrency-test-design.md`

- [ ] **Step 1: Add a post-write barrier helper for intent paths**

Add a local helper near the existing storage wrappers. It should block only writes whose object name contains `.INTENT.`, but it must block after the intent has already been written to the base storage. Blocking before the base write would make the test meaningless because other contenders could not observe the in-flight intent. It must record `started`, `released`, and the path. Use the same guard-timeout style as `cleanupDeleteBarrierStorage`.

```go
type intentWriteBarrierStorage struct {
	storeapi.Storage

	mu       sync.Mutex
	blocked  int
	started chan interceptedOperation
	released chan interceptedOperation
	release  chan struct{}
}

func newIntentWriteBarrierStorage(base storeapi.Storage) *intentWriteBarrierStorage {
	return &intentWriteBarrierStorage{
		Storage:  base,
		started:  make(chan interceptedOperation, 8),
		released: make(chan interceptedOperation, 8),
		release:  make(chan struct{}),
	}
}

func (s *intentWriteBarrierStorage) releaseWrites() {
	s.mu.Lock()
	old := s.release
	s.release = make(chan struct{})
	s.mu.Unlock()
	close(old)
}

func (s *intentWriteBarrierStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	if !strings.Contains(name, ".INTENT.") {
		return s.Storage.WriteFile(ctx, name, data)
	}

	if err := s.Storage.WriteFile(ctx, name, data); err != nil {
		return err
	}

	s.mu.Lock()
	s.blocked++
	release := s.release
	s.mu.Unlock()

	s.started <- interceptedOperation{Path: name}
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-release:
	case <-time.After(time.Second):
		err = errors.New("guard timeout waiting to release intent WriteFile")
	}
	s.released <- interceptedOperation{Path: name, Err: err}
	return err
}
```

Also add:

```go
func requireIntentStarted(t *testing.T, ch <-chan interceptedOperation) interceptedOperation {
	t.Helper()
	op := waitOperationFinished(t, ch, "intent WriteFile start")
	require.Contains(t, op.Path, ".INTENT.")
	return op
}
```

Every subtest that uses `intentWriteBarrierStorage` must register `t.Cleanup(storage.releaseWrites)` immediately after creating the helper. Do not use a canceled context while the helper is paused as proof of own-intent cleanup; if the helper returns an error after the base intent write but before `conditionalPut.CommitTo` installs its deferred cleanup, the persisted intent may intentionally remain as an ambiguous storage outcome.

- [ ] **Step 2: Add subtest for in-flight foreign intent blocking conflicting writer**

Add a new top-level test:

```go
func TestLeaseLockIntentLifecycleInterleaving(t *testing.T) {
	t.Run("in-flight read intent blocks writer and does not create owner", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		base, pth := createMockStorage(t)
		storage := newIntentWriteBarrierStorage(base)
		t.Cleanup(storage.releaseWrites)

		readerDone := make(chan lockContenderResult, 1)
		go func() {
			lock, err := objstore.TryLockRemoteRead(parentCtx, storage, "v1/LOCK", "reader-with-blocked-intent", localLeaseClock())
			readerDone <- lockContenderResult{ownerID: "reader-with-blocked-intent", lock: lock, err: err}
		}()
		intent := requireIntentStarted(t, storage.started)
		committedTarget := strings.Split(intent.Path, ".INTENT.")[0]
		requireFileExists(t, filepath.Join(pth, intent.Path))
		requireFileNotExists(t, filepath.Join(pth, committedTarget))
		select {
		case result := <-readerDone:
			t.Fatalf("reader acquired before intent barrier was released: lock=%v err=%v", result.lock, result.err)
		default:
		}

		writer, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "writer-blocked-by-read-intent", localLeaseClock())
		require.ErrorContains(t, err, "conflict file")
		require.Nil(t, writer)
		requireFileExists(t, filepath.Join(pth, intent.Path))
		requireFileNotExists(t, filepath.Join(pth, committedTarget))

		storage.releaseWrites()
		released := waitOperationFinished(t, storage.released, "intent WriteFile release")
		require.Equal(t, intent.Path, released.Path)
		require.NoError(t, released.Err)
		reader := waitLockContenderResult(t, readerDone, "reader intent commit", time.Second)
		require.NoError(t, reader.err)
		require.NotNil(t, reader.lock)
		requireFileExists(t, filepath.Join(pth, committedTarget))
		require.NoError(t, reader.lock.Unlock(parentCtx))
		requireNoIntentWithPrefix(t, base, "v1/LOCK.READ.")
		requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")
	})
}
```

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockIntentLifecycleInterleaving/in-flight_read_intent_blocks_writer_and_does_not_create_owner' -count=1
```

Expected: PASS.

- [ ] **Step 3: Add subtest for target commit failure cleaning own intent**

Use the same `intentWriteBarrierStorage`, plus a small wrapper that fails the committed target write once after the intent phase has returned successfully. This proves a failed acquire after intent creation does not leave an own-intent orphan that blocks future owners.

```go
type failTargetWriteOnceStorage struct {
	storeapi.Storage

	mu     sync.Mutex
	failed bool
}

func (s *failTargetWriteOnceStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	if strings.Contains(name, ".INTENT.") {
		return s.Storage.WriteFile(ctx, name, data)
	}
	s.mu.Lock()
	shouldFail := !s.failed && strings.HasPrefix(name, "v1/LOCK.WRIT.")
	if shouldFail {
		s.failed = true
	}
	s.mu.Unlock()
	if shouldFail {
		return errors.New("injected target commit failure")
	}
	return s.Storage.WriteFile(ctx, name, data)
}
```

Then add:

```go
t.Run("target commit failure cleans own intent and later writer succeeds", func(t *testing.T) {
	parentCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base, _ := createMockStorage(t)
	failTarget := &failTargetWriteOnceStorage{Storage: base}
	storage := newIntentWriteBarrierStorage(failTarget)
	t.Cleanup(storage.releaseWrites)

	done := make(chan lockContenderResult, 1)
	go func() {
		lock, err := objstore.TryLockRemoteWrite(parentCtx, storage, "v1/LOCK", "writer-with-failed-commit", localLeaseClock())
		done <- lockContenderResult{ownerID: "writer-with-failed-commit", lock: lock, err: err}
	}()
	intent := requireIntentStarted(t, storage.started)
	storage.releaseWrites()
	released := waitOperationFinished(t, storage.released, "failed-commit intent WriteFile release")
	require.Equal(t, intent.Path, released.Path)
	require.NoError(t, released.Err)

	result := waitLockContenderResult(t, done, "failed-commit writer", time.Second)
	require.Error(t, result.err)
	require.Nil(t, result.lock)
	requireNoIntentWithPrefix(t, base, "v1/LOCK.READ.")
	requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")

	next, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "next-writer", localLeaseClock())
	require.NoError(t, err)
	require.NoError(t, next.Unlock(parentCtx))
})
```

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockIntentLifecycleInterleaving/target_commit_failure_cleans_own_intent_and_later_writer_succeeds' -count=1
```

Expected: PASS.

- [ ] **Step 4: Add subtest for stale cleanup not reclaiming foreign intent as committed lock**

Write a foreign intent directly, then run a cleanup attempt through `LockWithRetry`. It should fail or time out because the foreign intent is a conservative blocker, and the intent file should still exist.

```go
t.Run("foreign intent is not reclaimed as stale committed lock", func(t *testing.T) {
	parentCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer objstore.TESTSetStaleReclaimGrace(30 * time.Millisecond)()

	base, pth := createMockStorage(t)
	intentPath := "v1/LOCK.WRIT.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.INTENT.foreign"
	require.NoError(t, base.WriteFile(parentCtx, intentPath, []byte{}))

	shortCtx, shortCancel := context.WithTimeout(parentCtx, 150*time.Millisecond)
	defer shortCancel()
	lock, err := objstore.LockWithRetry(shortCtx, objstore.TryLockRemoteWrite, base, "v1/LOCK", "writer-blocked-by-foreign-intent", func() {}, localLeaseClock())
	require.Error(t, err)
	require.Nil(t, lock)
	requireFileExists(t, filepath.Join(pth, intentPath))

	require.NoError(t, base.DeleteFile(parentCtx, intentPath))
	next, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "next-writer", localLeaseClock())
	require.NoError(t, err)
	require.NoError(t, next.Unlock(parentCtx))
})
```

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockIntentLifecycleInterleaving/foreign_intent_is_not_reclaimed_as_stale_committed_lock' -count=1
```

Expected: PASS.

- [ ] **Step 5: Run the full intent group**

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockIntentLifecycleInterleaving' -count=1
```

Expected: PASS. If any subtest relies on production retry timing, prefer a shorter direct `TryLockRemote*` assertion over waiting through `LockWithRetry`.

### Task 8: Add Append Write Family Smoke for Cleanup Interleaving

**Why this is lower priority:** This is a representative family-routing check. Do not expand this into a full truncate/read/write/append matrix unless a previous task exposes a family-specific bug.

**Files:**

- Modify: `pkg/objstore/locking_model_concurrency_test.go`
- Update: `docs/agents/br/lease-lock-cleanup-ha-model-tests-implementation-plan.md`
- Update: `docs/agents/br/lease-lock-model-concurrency-test-design.md`

- [ ] **Step 1: Add append write subtest to cleanup/reacquire**

Add a subtest to `TestLeaseLockCleanupDoesNotDeleteReacquiredInstance` using the existing after-delete barrier:

```go
t.Run("append write", func(t *testing.T) {
	parentCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer objstore.TESTSetStaleReclaimGrace(30 * time.Millisecond)()

	base, pth := createMockStorage(t)
	now := time.Now()
	stalePath := "v1/APPEND_LOCK.WRIT.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	writeLockMeta(t, base, stalePath, objstore.LockMeta{
		LockedAt: now.Add(-time.Hour),
		ExpireAt: now.Add(-time.Hour),
		TxnID:    []byte{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4},
		Hint:     "old-stale-append",
	})

	storage := newCleanupDeleteBarrierStorage(base, stalePath, cleanupDeleteBarrierAfter)
	cleanupCtx, cleanupCancel := context.WithCancel(parentCtx)
	defer cleanupCancel()
	cleanupDone := make(chan error, 1)
	go func() {
		lock, err := objstore.LockWithRetry(cleanupCtx, objstore.TryLockRemoteWrite, storage, "v1/APPEND_LOCK", "cleanup-append-contender", func() {}, localLeaseClock())
		if err == nil {
			_ = lock.Unlock(context.Background())
		}
		cleanupDone <- err
	}()
	requireDeleteStarted(t, storage.started, stalePath)
	requireFileNotExists(t, filepath.Join(pth, stalePath))

	liveLock, err := objstore.TryLockRemoteWrite(parentCtx, storage, "v1/APPEND_LOCK", "live-append", localLeaseClock())
	require.NoError(t, err)
	livePath := requireSinglePathWithPrefix(t, base, "v1/APPEND_LOCK.WRIT.")
	liveMeta := readLockMeta(t, base, livePath)

	cleanupCancel()
	storage.releaseDelete()
	released := waitOperationFinished(t, storage.released, "append cleanup DeleteFile release")
	require.True(t, released.Err == nil || errors.Is(released.Err, context.Canceled), "unexpected cleanup release error: %v", released.Err)
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for append cleanup contender to exit")
	}

	require.Equal(t, liveMeta.TxnID, readLockMeta(t, base, livePath).TxnID)
	requireNoDeleteCallForPath(t, storage.deleteCallSnapshot(), livePath)
	requireFileExists(t, filepath.Join(pth, livePath))
	require.NoError(t, liveLock.Unlock(parentCtx))
	next, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/APPEND_LOCK", "next-append", localLeaseClock())
	require.NoError(t, err)
	require.NoError(t, next.Unlock(parentCtx))
	requireNoIntentWithPrefix(t, base, "v1/APPEND_LOCK.WRIT.")
})
```

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockCleanupDoesNotDeleteReacquiredInstance/append_write' -count=1
```

Expected: PASS.

- [ ] **Step 2: Run the expanded cleanup/reacquire group**

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockCleanupDoesNotDeleteReacquiredInstance' -count=1
```

Expected: PASS.

### Task 9: Update Follow-Up Docs and Validation Evidence

**Files:**

- Modify: `docs/agents/br/lease-lock-cleanup-ha-model-tests-implementation-plan.md`
- Modify: `docs/agents/br/lease-lock-model-concurrency-test-design.md`
- Modify if generated: `pkg/objstore/BUILD.bazel`

- [ ] **Step 1: Update design status**

In `docs/agents/br/lease-lock-model-concurrency-test-design.md`, mark these as landed when their tests pass:

- read/write mixed cleanup interleaving;
- `.INTENT.` lifecycle interleaving;
- append-write cleanup/reacquire smoke.

Keep these as remaining lower-priority follow-ups:

- truncate family smoke if a future change touches truncate classification;
- fixed-seed random model;
- `protected work hung while renewal alive`;
- BR integration validation.

- [ ] **Step 2: Update this ExecPlan**

Mark Task 6 through Task 8 progress checkboxes and add exact command summaries to `Outcomes & Retrospective`. Do not claim full Ready unless Ready validation has actually run.

- [ ] **Step 3: Run WIP validation**

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(ContenderRetryCleanupInterleaving|IntentLifecycleInterleaving|CleanupDoesNotDeleteReacquiredInstance)' -count=1
```

Expected: PASS.

Run adjacent regression:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(TerminalReasonStableAfterLeaseLost|NormalUnlockWinsInFlightRenewal|RenewalObservationHangIsTransient|RenewalAmbiguousWriteAndProofFailureStopProtectedWork|ProtectedWorkDeathAndHangSemantics)' -count=1
```

Expected: PASS.

- [ ] **Step 4: Run metadata and hygiene checks**

Because Task 7 adds a new top-level `TestXxx`, run:

```bash
make bazel_prepare
```

Expected: exits 0. Include generated Bazel metadata only if it changes.

Run:

```bash
gofmt -w pkg/objstore/locking_model_concurrency_test.go
git diff --check
git diff --cached --check
```

Expected: all exit 0.

- [ ] **Step 5: Run Ready validation only before final readiness claim**

Run:

```bash
make lint
```

Expected: exits 0. If this is not run, final output must say Ready validation was not completed.

## Validation and Acceptance

Use WIP validation during implementation:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(CleanupDoesNotDeleteReacquiredInstance|ContenderRetryCleanupInterleaving|IntentLifecycleInterleaving|ProtectedWorkDeathAndHangSemantics)' -count=1

Run the adjacent already-existing regression group:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(TerminalReasonStableAfterLeaseLost|NormalUnlockWinsInFlightRenewal|RenewalObservationHangIsTransient|RenewalAmbiguousWriteAndProofFailureStopProtectedWork)' -count=1

Run formatting and whitespace checks:

    gofmt -w pkg/objstore/locking_model_concurrency_test.go
    git diff --check

Run `make bazel_prepare` after the new top-level tests are added.

Before claiming Ready or PR readiness, follow the Ready profile and run:

    make lint

Acceptance criteria:

- `TestLeaseLockCleanupDoesNotDeleteReacquiredInstance` proves stale cleanup does not delete a new physical instance acquired while cleanup is still in flight.
- `TestLeaseLockContenderRetryCleanupInterleaving` proves two contenders cleaning and retrying around the same stale blocker do not both enter protected work.
- `TestLeaseLockProtectedWorkDeathAndHangSemantics` proves a dead holder is reclaimable only after stale boundary, and a hung protected operation cannot continue after renewal loss.
- The read/write mixed subtest proves writer cleanup of stale read A does not delete live read B, and writer acquisition remains blocked while B is live.
- `TestLeaseLockIntentLifecycleInterleaving` proves observable `.INTENT.` files are conservative blockers, do not create false committed owners, own intents are removed after target commit failure, and foreign intents are not reclaimed as stale committed locks.
- The append-write smoke proves the cleanup/reacquire physical-path safety property also holds for the append lock family without expanding into a low-value full family matrix.
- All new tests leave no `.INTENT.` residue for their lock families.
- Existing renewal and terminal tests continue passing.

## Idempotence and Recovery

All tests use `t.TempDir()`-backed local storage through `createMockStorage(t)` and should be safe to rerun. Every helper that blocks on a channel must have a guard timeout and a cleanup path that releases the barrier. If a test times out during development, rerun only the failing subtest first.

If a cleanup contender goroutine is still running, cancel its context and release any delete barrier before test cleanup returns. Do not use `git reset --hard` or remove temp directories manually.

If `make bazel_prepare` changes unrelated files, inspect the diff before keeping it. Only include Bazel metadata required by the new tests.

## Interfaces and Dependencies

Use existing production APIs:

- `objstore.TryLockRemoteWrite`
- `objstore.TryLockRemoteRead`
- `objstore.TryLockRemoteTruncate`
- `objstore.LockWithRetry`
- `objstore.CleanUpStaleTruncateLock`
- `(*objstore.RemoteLock).Unlock`

Use existing test helpers:

- `createMockStorage`
- `writeLockMeta`
- `readLockMeta`
- `requireSinglePathWithPrefix`
- `requireListedPathsWithPrefix`
- `requireFileExists`
- `requireFileNotExists`
- `requireNoIntentWithPrefix`
- `startProtectedWorker`
- `requireNoStepAfterAction`
- `startModelRenewal`
- `stopModelRenewal`

Add only test-local helper types in `pkg/objstore/locking_model_concurrency_test.go` unless a helper must be shared with `pkg/objstore/locking_concurrency_test.go`.

## Artifacts and Notes

When each milestone completes, add the exact command output summary here. Keep evidence short: command, exit code, and the specific PASS/FAIL line or unexpected error.

Initial plan creation changed only documentation. No Go tests were run for this plan creation step.
