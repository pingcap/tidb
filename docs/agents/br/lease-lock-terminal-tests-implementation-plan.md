# Lease Lock Terminal Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add deterministic `pkg/objstore` tests that prove `RemoteLock` terminal reason semantics are stable when `lease_lost` is followed by `Unlock`, and when normal `Unlock` races with an in-flight renewal operation.

**Architecture:** Reuse the existing model/concurrency test harness in `pkg/objstore/locking_model_concurrency_test.go` and `pkg/objstore/locking_concurrency_test.go`. Add only small test helpers needed to record the first business terminal reason. If the normal-unlock race exposes a real implementation gap, make the minimal production change in `pkg/objstore/locking.go` so a renewal shutdown signal observed after an in-flight `tryRenew` returns cannot be reclassified as business `lease_lost`.

**Tech Stack:** Go tests in package `objstore_test`, TiDB failpoint-aware test runner, local object storage test doubles, `testify/require`.

---

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

This work makes the lease-lock terminal semantics explicit. After implementation, a developer can run a targeted `pkg/objstore` test command and observe two properties:

First, once a holder has already reached business terminal reason `lease_lost`, a later `Unlock` is only cleanup or diagnostic work. It must not make the protected business loop run again, and it must not overwrite the business terminal reason with normal unlock.

Second, when normal `Unlock` has already won the terminal race while a renewal write is still in flight, a later renewal timeout must not invoke the business `onLeaseLost` callback. This prevents noisy or misleading cancellation of business work after the user has already chosen the normal release path.

The important user-visible safety property remains the glossary term `业务临界区安全` in `CONTEXT.md`: a holder may continue protected work only while it can prove a valid lease window. These tests do not require object storage to be free of delayed renewal artifacts; they require protected work to stop and terminal reason to be interpreted correctly.

## Progress

- [x] (2026-06-04) Split terminal test design in `docs/agents/br/lease-lock-model-concurrency-test-design.md` into terminal reason stability and normal-unlock-wins race cases.
- [x] (2026-06-04) Drafted this implementation plan.
- [x] (2026-06-04) Implemented terminal reason and physical-delete recorder helpers in `pkg/objstore/locking_model_concurrency_test.go`.
- [x] (2026-06-04) Added test-only renewal stop observation helper in `pkg/objstore/export_test.go`.
- [x] (2026-06-04) Added proof-strength test `TestLeaseLockTerminalReasonStableAfterLeaseLost`.
- [x] (2026-06-04) Added red test `TestLeaseLockNormalUnlockWinsInFlightRenewal`.
- [x] (2026-06-04) Normal-unlock race failed for the expected `onLeaseLost` callback reason; implemented the minimal production change in `pkg/objstore/locking.go`.
- [x] (2026-06-04) Ran targeted WIP validation and recorded evidence below.
- [x] (2026-06-04) Ran `make bazel_prepare` because this plan adds new top-level `TestXxx` functions; no Bazel metadata diff was produced.
- [x] (2026-06-04) Reviewed proof strength with independent subagents; blocking feedback found a post-write-proof cancellation gap.
- [x] (2026-06-04) Ran `docs/agents/agents-review-guide.md` lightweight checks because this plan changes docs under `docs/agents`.
- [x] (2026-06-04) Addressed blocking review: renewal stop now cancels the renewal-owned context inherited by in-flight attempts, stop suppression applies only to success/non-permanent shutdown results, and permanent renewal loss is not suppressed by a later stop signal.
- [x] (2026-06-04) Addressed final proof-strength review: normal unlock cancellation during post-write proof now returns a transient shutdown result instead of permanent `lease_lost`, and `TestLeaseLockNormalUnlockWinsInFlightRenewal/post-write proof cancellation` covers it.
- [x] (2026-06-04) Simplified the implementation from a per-attempt stop watcher to a renewal-owned cancellable context.
- [ ] For Ready / PR-readiness claims after code changes, run the Ready profile, including `make lint`.

## Surprises & Discoveries

- Observation: Before this plan, `RemoteLock.Unlock` stopped the renewal loop before deleting the physical lock path, but it did not cancel the context of a `tryRenew` call that was already executing.
  Evidence: `pkg/objstore/locking.go` called `l.stopRenewalIfStarted()` at the start of `Unlock`; `stopRenewalIfStarted` closed `stopCh` and waited for `done`, while the in-flight `tryRenew` context was not derived from a renewal-owned cancellation source.
- Observation: Existing `TestLeaseLockUnlockWaitsForInFlightRenewal` proves `Unlock` waits for an in-flight renewal write to exit before `DeleteFile`, but it passes `nil` as `onLeaseLost`.
  Evidence: `pkg/objstore/locking_concurrency_test.go` starts renewal with `objstore.TESTStartRenewal(ctx, lock, nil)`, so the test does not prove whether a real business callback would be invoked after normal unlock has begun.
- Observation: `Unlock` return value is not the same concept as business terminal reason.
  Evidence: `Unlock` reads remote metadata, checks `TxnID`, and may delete the holder's physical path. A `lease_lost` holder can later see either a mismatch error or a successful cleanup depending on the cause of lease loss.
- Observation: The first draft of this plan made `lease_lost then Unlock` terminal stability a tautology because only `onLeaseLost` wrote to the recorder.
  Evidence: A later `Unlock` path does not know about the test recorder, so the test must explicitly attempt `recorder.record(terminalNormalUnlock)` after `Unlock` and assert that the second terminal reason is rejected.
- Observation: The first draft of the normal-unlock race could mask a bad production `onLeaseLost` callback if the callback only signaled after `recorder.record(terminalLeaseLost)` succeeded.
  Evidence: Once `terminalNormalUnlock` is already recorded, `record(terminalLeaseLost)` returns false by design. The callback must unconditionally signal a separate `leaseLostCalled` channel before attempting to record or stop the worker.
- Observation: A normal-unlock race test must prove `Unlock` has requested renewal stop before the in-flight renewal result is classified.
  Evidence: The proposed production fix keys off `stopCh` closure. A test that only records `terminalNormalUnlock` before calling `Unlock` could let renewal timeout first under scheduling delay, making `lease_lost` a legitimate first terminal reason.
- Observation: `TestLeaseLockTerminalReasonStableAfterLeaseLost` passed before production changes, as expected for a proof-strength test.
  Evidence: `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockTerminalReasonStableAfterLeaseLost' -count=1` exited 0 after rerunning outside the sandbox's read-only Go build cache.
- Observation: `TestLeaseLockNormalUnlockWinsInFlightRenewal` failed before production changes for the planned reason.
  Evidence: `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockNormalUnlockWinsInFlightRenewal' -count=1` failed with `locking_model_concurrency_test.go:684: onLeaseLost must not be called after normal unlock wins` after renewal write timeout logged `Lock renewal detected lease lost; calling onLeaseLost.`
- Observation: The sandbox cannot write the default Go and Bazel caches used by the validation commands.
  Evidence: the first failpoint test attempt failed with `open /root/.cache/go-build/...: read-only file system`; the first `make bazel_prepare` attempt failed with `Output base directory '/root/.cache/bazel/...' must be readable and writable.`
- Observation: The first production fix was too broad because it checked `stopCh` before permanent-loss classification.
  Evidence: review identified that an already-formed `errRenewWriteTimeout` or post-write proof failure could be suppressed if `Unlock` closed `stopCh` after `tryRenew` returned. A new subtest, `write timeout remains lease lost when unlock closes stop signal`, proves a renewal write timeout that has already reached `context.DeadlineExceeded` still records `lease_lost` even when `Unlock` closes the stop signal before `WriteFile` returns.
- Observation: `renewalState == renewalStopped` is weaker than proving the renewal stop signal is closed.
  Evidence: `stopRenewalIfStarted` sets `renewalState = renewalStopped` while holding `l.mu`, then unlocks and closes `stopCh`. The test-only helper was changed to `TESTRenewalStopSignalClosed`, which copies `stopCh` under lock and non-blockingly observes the channel closure.
- Observation: Normal unlock can cancel an in-flight renewal after the write has succeeded but before post-write lease proof returns.
  Evidence: final proof-strength review found that renewal stop cancellation also reaches the `postWriteClockCtx`. If that cancellation was wrapped as `errRenewPostWriteProofFailed`, `renewalLoop` would invoke `onLeaseLost` even though normal unlock had already closed the stop signal. The normal-unlock race test now has a `post-write proof cancellation` subtest for this interleaving.

## Decision Log

- Decision: Split terminal tests into one sequential test and one true race test.
  Rationale: `lease_lost then Unlock` can be tested deterministically for multiple lease-lost causes without adding interleaving complexity. The only first-phase race that needs strict ordering is normal unlock winning while renewal is in flight.
  Date/Author: 2026-06-04 / Codex + user.
- Decision: Cover three representative lease-lost causes in the sequential test: `TxnID` mismatch, renewal `WriteFile` timeout, and post-write proof failure.
  Rationale: These represent remote ownership loss, ambiguous mutation outcome, and post-mutation proof failure. Other lease-lost causes such as expired lease, remaining lease too small, retry exhaustion, and proven-window exhaustion are already covered by renewal/proven-window tests or are timing variants.
  Date/Author: 2026-06-04 / Codex + user.
- Decision: Do not treat `Unlock` success after `lease_lost` as normal release.
  Rationale: Cleanup success only says the holder deleted a physical path it could still prove was its own. It does not restore a valid lease window or change the protected business loop terminal reason.
  Date/Author: 2026-06-04 / Codex + user.
- Decision: Keep the normal-unlock race acceptance strict.
  Rationale: If normal unlock wins first, a later renewal shutdown result is a shutdown artifact and must not trigger business `onLeaseLost`. Do not weaken this assertion merely to match current implementation.
  Date/Author: 2026-06-04 / Codex + user.
- Decision: Use a test-only renewal stop observation helper to make the normal-unlock race deterministic.
  Rationale: The test must use real `lock.Unlock`, but it also needs to prove `Unlock` has closed the renewal stop signal before the blocked renewal write exits. Observing internal test-only state through `export_test.go` avoids relying on scheduler timing and does not change production APIs.
  Date/Author: 2026-06-04 / subagent review + Codex.
- Decision: Treat `make bazel_prepare` as mandatory for this plan.
  Rationale: `AGENTS.md` requires it when a code change adds a new top-level `TestXxx` function in an existing Go test file, even if the test file is already listed in `BUILD.bazel`.
  Date/Author: 2026-06-04 / subagent review + Codex.
- Decision: Apply the production fix only after preserving the normal-unlock race failure, but do not let a later stop signal suppress permanent renewal loss.
  Rationale: The red test proved that a renewal write timeout after `Unlock` had requested stop still invoked `onLeaseLost`, but review showed that checking `stopCh` before permanent classification can hide real lease loss. The corrected implementation cancels a renewal-owned context when `stopCh` closes, accepts success only if stop is not closed, invokes permanent renewal loss regardless of stop state, and suppresses only non-permanent shutdown/transient results after stop.
  Date/Author: 2026-06-04 / Codex.
- Decision: Use a renewal-owned cancellable context instead of a per-attempt stop watcher.
  Rationale: `stopRenewalIfStarted` is the single place that issues the renewal stop signal, so it can also call the stored `renewalCancel`. This naturally propagates normal unlock cancellation into `tryRenew` without spawning a watcher goroutine for every attempt.
  Date/Author: 2026-06-04 / Codex + user.
- Decision: Treat post-write proof `context.Canceled` as a non-permanent shutdown result, while preserving post-write proof timeout and unsafe-time results as permanent lease loss.
  Rationale: When normal `Unlock` closes the renewal stop signal, cancellation of an in-flight post-write proof is a shutdown artifact. A deadline or clock result proving the new lease window unsafe still means the holder cannot prove safety after a completed write and must stop protected work.
  Date/Author: 2026-06-04 / subagent review + Codex.

## Outcomes & Retrospective

Implemented the terminal recorder, physical delete recorder, `TESTRenewalStopSignalClosed`, and both terminal tests. The normal-unlock race produced the expected red failure before the production change. After blocking review, the production change uses a renewal-owned cancellable context to cancel in-flight attempts when normal unlock closes `stopCh`, and classifies permanent renewal losses before stop-based suppression. Shutdown cancellation results are suppressed after stop; real permanent losses such as renewal write timeout, post-write proof timeout, and unsafe post-write proof still invoke `onLeaseLost`.

Validation evidence:

- `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockTerminalReasonStableAfterLeaseLost' -count=1` initially failed in the sandbox because `/root/.cache/go-build` was read-only; rerun with writable cache access exited 0.
- `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockNormalUnlockWinsInFlightRenewal' -count=1` failed before the production fix with `onLeaseLost must not be called after normal unlock wins`.
- `gofmt -w pkg/objstore/locking.go pkg/objstore/locking_model_concurrency_test.go pkg/objstore/export_test.go` exited 0.
- `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(TerminalReasonStableAfterLeaseLost|NormalUnlockWinsInFlightRenewal)' -count=1` exited 0 after the production fix.
- `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(UnlockWaitsForInFlightRenewal|RenewalLostStopsCriticalSection|RenewalAmbiguousWriteAndProofFailureStopProtectedWork)' -count=1` exited 0.
- `make bazel_prepare` initially failed in the sandbox because `/root/.cache/bazel` was not writable; rerun with writable cache access exited 0 and produced no Bazel metadata diff.
- `git diff --check` exited 0.
- `rg -n -P '`(MUST(?: NOT)?|SHOULD|MAY)`' AGENTS.md docs/agents/agents-review-guide.md docs/agents/br/lease-lock-model-concurrency-test-design.md docs/agents/br/lease-lock-terminal-tests-implementation-plan.md` exited 1 with no output, meaning no backticked normative keyword matches were found.
- `test -e docs/agents/br/lease-lock-terminal-tests-implementation-plan.md` exited 0.
- `test -e pkg/objstore/locking_model_concurrency_test.go` exited 0.
- Blocking review follow-up: `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(TerminalReasonStableAfterLeaseLost|NormalUnlockWinsInFlightRenewal)' -count=1` exited 0 after adding the `context.Canceled` normal-unlock assertion and the permanent-timeout-while-stop-closed subtest.
- Blocking review follow-up: `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(UnlockWaitsForInFlightRenewal|RenewalLostStopsCriticalSection|RenewalAmbiguousWriteAndProofFailureStopProtectedWork)' -count=1` exited 0.
- Final review follow-up rerun after `TESTRenewalStopSignalClosed` lock-scope cleanup: `gofmt -w pkg/objstore/locking.go pkg/objstore/locking_model_concurrency_test.go pkg/objstore/export_test.go` exited 0.
- Final review follow-up rerun: `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(TerminalReasonStableAfterLeaseLost|NormalUnlockWinsInFlightRenewal)' -count=1` exited 0.
- Final review follow-up rerun: `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(UnlockWaitsForInFlightRenewal|RenewalLostStopsCriticalSection|RenewalAmbiguousWriteAndProofFailureStopProtectedWork)' -count=1` exited 0.
- Final review follow-up rerun: `make bazel_prepare` exited 0 and produced no Bazel metadata diff.
- Final review follow-up rerun: `git diff --check` exited 0.
- Final review follow-up rerun: `rg -n -P '`(MUST(?: NOT)?|SHOULD|MAY)`' AGENTS.md docs/agents/agents-review-guide.md docs/agents/br/lease-lock-model-concurrency-test-design.md docs/agents/br/lease-lock-terminal-tests-implementation-plan.md` exited 1 with no output, meaning no backticked normative keyword matches were found.
- Final review follow-up rerun: `test -e docs/agents/br/lease-lock-terminal-tests-implementation-plan.md` and `test -e pkg/objstore/locking_model_concurrency_test.go` both exited 0.
- Final independent proof-strength review found one blocking gap: normal unlock cancellation while post-write proof was in flight could be wrapped as permanent `errRenewPostWriteProofFailed`.
- Final independent concurrency review of the first fix found no blocking issues in the then-current renewal stop propagation helpers; it noted parent context cancellation semantics as a non-blocking question.
- Post-write proof cancellation fix rerun: `gofmt -w pkg/objstore/locking.go pkg/objstore/locking_model_concurrency_test.go pkg/objstore/export_test.go` exited 0.
- Post-write proof cancellation fix rerun: `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(TerminalReasonStableAfterLeaseLost|NormalUnlockWinsInFlightRenewal)' -count=1` exited 0.
- Post-write proof cancellation fix rerun: `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(UnlockWaitsForInFlightRenewal|RenewalLostStopsCriticalSection|RenewalAmbiguousWriteAndProofFailureStopProtectedWork)' -count=1` exited 0.
- Post-write proof cancellation fix rerun: `make bazel_prepare` exited 0 and produced no Bazel metadata diff.
- Post-write proof cancellation fix rerun: `git diff --check` exited 0.
- Post-write proof cancellation fix rerun: `rg -n -P '`(MUST(?: NOT)?|SHOULD|MAY)`' AGENTS.md docs/agents/agents-review-guide.md docs/agents/br/lease-lock-model-concurrency-test-design.md docs/agents/br/lease-lock-terminal-tests-implementation-plan.md` exited 1 with no output, meaning no backticked normative keyword matches were found.
- Post-write proof cancellation fix rerun: `test -e docs/agents/br/lease-lock-terminal-tests-implementation-plan.md` exited 0.
- Final post-comment rerun: `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(TerminalReasonStableAfterLeaseLost|NormalUnlockWinsInFlightRenewal)' -count=1` exited 0.
- Final narrow re-review found no blocking findings; it confirmed post-write proof `context.Canceled` is now a transient shutdown result, while post-write proof timeout / unsafe proof remains permanent lease loss.
- Renewal context simplification rerun: `gofmt -w pkg/objstore/locking.go pkg/objstore/locking_helper.go` exited 0.
- Renewal context simplification rerun: `git diff --check` exited 0.
- Renewal context simplification rerun: `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(TerminalReasonStableAfterLeaseLost|NormalUnlockWinsInFlightRenewal)' -count=1` exited 0.
- Renewal context simplification rerun: `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(UnlockWaitsForInFlightRenewal|RenewalLostStopsCriticalSection|RenewalAmbiguousWriteAndProofFailureStopProtectedWork)' -count=1` exited 0.
- Attempt-timeout simplification: removed the outer `renewalAttemptTimeout` wrapper so `tryRenew` receives the renewal-owned context directly. Per-operation timeouts inside `tryRenew` now carry the operation-specific classification.
- Timeout helper refactor was reverted for readability. `tryRenew` now keeps explicit per-operation `context.WithTimeout` calls so the timeout and error classification stay local to each renewal phase.
- Low remaining lease guard follow-up: `renewalLoop` now checks the current proven lease window before starting a new `tryRenew`; if it cannot cover one bounded renewal operation, it calls `onLeaseLost` without issuing another storage read. The targeted low-remaining subtest and related renewal / terminal regressions exited 0.

Remaining gaps: Ready profile was intentionally not run, so this plan does not claim Ready or PR-readiness.

## Context and Orientation

The relevant package is `pkg/objstore`. The implementation under test is `pkg/objstore/locking.go`. The test files are:

- `pkg/objstore/locking_test.go`, which contains basic lock, renewal, cleanup, and helper tests.
- `pkg/objstore/locking_concurrency_test.go`, which contains first-phase deterministic concurrency helpers such as `criticalSectionAudit`, `protectedWorker`, `renewalBlockingStorage`, `requireNoIntentWithPrefix`, and `TestLeaseLockUnlockWaitsForInFlightRenewal`.
- `pkg/objstore/locking_model_concurrency_test.go`, which contains Phase 2 model tests and helpers such as `lateWriteStorage`, `blockingLeaseClock`, `readLockMeta`, `startModelRenewal`, and `stopModelRenewal`.
- `pkg/objstore/export_test.go`, which exposes internal renewal hooks to external-package tests, including `objstore.TESTStartRenewal`, `objstore.TESTStopRenewal`, `objstore.TESTTryRenew`, and sentinel renewal errors.

Terminology:

`holder` means the actor that acquired a `RemoteLock`.

`protected work` means the business operation guarded by the lock. In tests, `protectedWorker` models this work and records `step`, `lost`, and `stopped` events into `criticalSectionAudit`.

`terminal reason` means the business-level final reason for the holder. The relevant reasons for this plan are `normal_unlock` and `lease_lost`. This is distinct from whether the renewal goroutine has exited.

`physical lock path` means the concrete object path created by one acquire attempt, for example `v1/LOCK.WRIT.<32hex>`. Different acquire attempts use different physical paths.

Current behavior to keep in mind:

`RemoteLock.Unlock(ctx)` calls `stopRenewalIfStarted()`, then reads metadata from `l.path`, verifies `TxnID`, and deletes `l.path`. If the remote `TxnID` differs from `l.txnID`, `Unlock` returns a mismatch error and does not delete.

Before this plan, `renewalLoop` invoked `onLeaseLost` when `tryRenew` returned a permanent renewal loss, when the proven lease window was exhausted, when retry backoff would exceed that window, or when retries were exhausted. It checked `stopCh` before starting an attempt and during retry backoff sleeps, but not immediately after an in-flight `tryRenew` returned.

After this plan, each renewal attempt uses a context that is canceled when `stopCh` closes. The loop accepts successful renewal only if the stop signal is not closed, invokes permanent renewal loss before stop-based suppression, and suppresses non-permanent shutdown/transient results after stop. Post-write proof `context.Canceled` is a non-permanent shutdown result; post-write proof timeout or an unsafe proof result remains permanent.

## Plan of Work

### Milestone 1: Add Terminal and Delete Recorder Helpers

Scope: Add small test-only helpers in `pkg/objstore/locking_model_concurrency_test.go` near the other helper types. The terminal helper records the first business terminal reason and optionally drives `protectedWorker.stop`. The delete helper records which storage paths `Unlock` actually deletes, so tests can prove cause-specific cleanup behavior without inferring it only from final file state.

After this milestone, tests can assert `first terminal reason wins` without relying on `Unlock` return values. Tests must use the helper both to record the first terminal reason and to attempt the losing terminal reason, otherwise the assertion becomes tautological.

Implementation sketch:

    type terminalReason string

    const (
        terminalNone         terminalReason = ""
        terminalNormalUnlock terminalReason = "normal_unlock"
        terminalLeaseLost    terminalReason = "lease_lost"
    )

    type terminalRecorder struct {
        mu     sync.Mutex
        reason terminalReason
        ch     chan terminalReason
    }

    func newTerminalRecorder() *terminalRecorder {
        return &terminalRecorder{ch: make(chan terminalReason, 1)}
    }

    func (r *terminalRecorder) record(reason terminalReason) bool {
        r.mu.Lock()
        defer r.mu.Unlock()
        if r.reason != terminalNone {
            return false
        }
        r.reason = reason
        r.ch <- reason
        return true
    }

    func (r *terminalRecorder) reasonNow() terminalReason {
        r.mu.Lock()
        defer r.mu.Unlock()
        return r.reason
    }

    func waitTerminalReason(t *testing.T, r *terminalRecorder, name string) terminalReason {
        t.Helper()
        select {
        case reason := <-r.ch:
            return reason
        case <-time.After(time.Second):
            t.Fatalf("timed out waiting for terminal reason %s", name)
            return terminalNone
        }
    }

Add a delete recorder wrapper:

    type deleteRecordingStorage struct {
        storeapi.Storage
        deleted chan string
    }

    func newDeleteRecordingStorage(base storeapi.Storage) *deleteRecordingStorage {
        return &deleteRecordingStorage{
            Storage: base,
            deleted: make(chan string, 8),
        }
    }

    func (s *deleteRecordingStorage) DeleteFile(ctx context.Context, name string) error {
        err := s.Storage.DeleteFile(ctx, name)
        if err == nil {
            s.deleted <- name
        }
        return err
    }

    func requireDeletedPath(t *testing.T, ch <-chan string, expected string) {
        t.Helper()
        select {
        case got := <-ch:
            require.Equal(t, expected, got)
        case <-time.After(time.Second):
            t.Fatalf("timed out waiting for DeleteFile %s", expected)
        }
    }

    func requireNoDeleteSoon(t *testing.T, ch <-chan string, d time.Duration) {
        t.Helper()
        select {
        case got := <-ch:
            t.Fatalf("unexpected DeleteFile %s", got)
        case <-time.After(d):
        }
    }

Acceptance:

- The helper is test-only and does not change production code.
- The terminal helper records exactly one reason.
- Calling `record` after a first terminal reason returns false and does not send a second event.
- `lease_lost then Unlock` tests must call `recorder.record(terminalNormalUnlock)` after `Unlock` and assert it returns false.
- `normal unlock wins` tests must call `recorder.record(terminalLeaseLost)` from `onLeaseLost`, but callback occurrence must be tracked by a separate channel before this record attempt.
- Delete recorder assertions must be cause-specific: mismatch deletes no path; own-`TxnID` cleanup deletes exactly the holder's physical path.

### Milestone 2: Add a Test-Only Renewal Stop Observation Helper

Scope: Modify `pkg/objstore/export_test.go` to expose whether `Unlock` has closed the renewal stop signal. This is test-only code compiled only for tests in package `objstore`.

Add:

    func TESTRenewalStopSignalClosed(l *RemoteLock) bool {
        l.mu.Lock()
        stopCh := l.stopCh
        l.mu.Unlock()
        if stopCh == nil {
            return false
        }
        select {
        case <-stopCh:
            return true
        default:
            return false
        }
    }

Acceptance:

- This helper does not change production APIs or production behavior.
- `TestLeaseLockNormalUnlockWinsInFlightRenewal` must use it to wait until real `lock.Unlock(ctx)` has closed the renewal stop signal channel before the blocked renewal write exits.
- Do not use this helper to force state changes. It is observation-only.

### Milestone 3: Add `TestLeaseLockTerminalReasonStableAfterLeaseLost`

Scope: Add a top-level test in `pkg/objstore/locking_model_concurrency_test.go`. This test is sequential and table-driven. It should reuse existing helpers and add only small trigger-specific setup code.

Subtest 1: `txn id mismatch then unlock`

Scenario:

- Set short lease constants with `objstore.TESTSetLeaseConstants(200*time.Millisecond, 15*time.Millisecond, 5, 5*time.Millisecond)` and proof constants with `objstore.TESTSetRenewalProofConstants(30*time.Millisecond, time.Millisecond)`.
- Create local object storage with `createMockStorage(t)` and wrap it in `deleteRecordingStorage`.
- Acquire a migration write lock with `objstore.TryLockRemoteWrite(parentCtx, recorderStorage, "v1/LOCK", "owner", localLeaseClock())`.
- Find its physical path with `requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")`.
- Start `protectedWorker` and confirm it can record one step before terminal state.
- Start renewal with an `onLeaseLost` callback that calls `recorder.record(terminalLeaseLost)` and then `worker.stop(workerStopLeaseLost)` only if record returned true.
- Overwrite the same physical path with lock metadata containing a different `TxnID`.
- Wait for terminal reason and assert it is `terminalLeaseLost`.
- Call `lock.Unlock(parentCtx)` after lease loss.
- After `Unlock`, simulate the normal business release path by calling `recorder.record(terminalNormalUnlock)` and assert it returns false.

Expected assertions:

- `lock.Unlock(parentCtx)` returns an error containing `Txn ID mismatch`.
- `recorder.reasonNow()` remains `terminalLeaseLost`.
- `recorder.record(terminalNormalUnlock)` after `Unlock` returns false.
- `worker.stopReason()` is `workerStopLeaseLost`.
- `worker.requestStep(t)` returns `ok=false`.
- `requireNoStepAfterAction(t, audit.snapshot(), "owner-a", "lost")` passes.
- Reading remote metadata from the physical path still returns the hijacker `TxnID`.
- `requireNoDeleteSoon(t, recorderStorage.deleted, 30*time.Millisecond)` passes because mismatch must not delete the hijacker metadata.
- `requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")` passes.

Subtest 2: `write timeout late commit then unlock`

Scenario:

- Use `deleteRecordingStorage` wrapping a local base storage, and `lateWriteStorage` wrapping the delete recorder.
- Acquire a migration write lock with `objstore.TryLockRemoteWrite`.
- Find physical path, record original `ExpireAt`, and configure `storage.blockNextWrites(physicalPath, 1)`.
- Start `protectedWorker`, `terminalRecorder`, and renewal.
- Wait for `storage.writeStarted`, then wait for `storage.writeReturned` and assert `context.DeadlineExceeded`.
- Wait for terminal reason `terminalLeaseLost` and worker stop.
- Read captured renewal metadata with `storage.capturedMeta(t)` and assert `ExpireAt` advanced.
- Release late commit with `storage.releaseLateCommit()` and assert the late commit succeeded.
- Call `lock.Unlock(parentCtx)` after lease loss.
- After `Unlock`, simulate the normal business release path by calling `recorder.record(terminalNormalUnlock)` and assert it returns false.

Expected assertions:

- `lock.Unlock(parentCtx)` returns nil because the late committed metadata still carries the holder's own `TxnID`.
- `requireDeletedPath(t, deleteRecorder.deleted, physicalPath)` observes exactly the holder physical path.
- The physical path no longer exists after unlock.
- `recorder.reasonNow()` remains `terminalLeaseLost`.
- `recorder.record(terminalNormalUnlock)` after `Unlock` returns false.
- `worker.requestStep(t)` returns `ok=false`.
- `requireNoStepAfterAction(t, audit.snapshot(), "owner-a", "lost")` passes.
- A later `objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "next-owner", localLeaseClock())` succeeds and can unlock.
- `requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")` passes.

Subtest 3: `post write proof failed then unlock`

Scenario:

- Use a `blockingLeaseClock` with acquire pre/post times, one renewal pre-write time, and a renewal post-write time that is after the new `ExpireAt`, for example `leaseNow.Add(objstore.LeaseTTL+time.Second)`.
- Acquire a migration write lock against `deleteRecordingStorage` wrapping base storage.
- Start worker, recorder, and renewal.
- Wait for terminal reason `terminalLeaseLost`.
- Call `lock.Unlock(parentCtx)` after lease loss.
- After `Unlock`, simulate the normal business release path by calling `recorder.record(terminalNormalUnlock)` and assert it returns false.

Expected assertions:

- `lock.Unlock(parentCtx)` returns nil because the remote metadata still belongs to the holder.
- `requireDeletedPath(t, deleteRecorder.deleted, physicalPath)` observes exactly the holder physical path.
- The physical path no longer exists after unlock.
- `recorder.reasonNow()` remains `terminalLeaseLost`.
- `recorder.record(terminalNormalUnlock)` after `Unlock` returns false.
- `worker.requestStep(t)` returns `ok=false`.
- `requireNoStepAfterAction(t, audit.snapshot(), "owner-a", "lost")` passes.
- A later acquire succeeds and can unlock.
- `requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")` passes.

Expected proof-strength behavior:

These subtests may pass against current production code because current code already prevents protected-worker steps after `onLeaseLost`, and `Unlock` already checks `TxnID` before deletion. They are proof-strength tests, not expected red tests. They are still required because they bind cause-specific `Unlock` outcomes to the business terminal reason contract and explicitly prove a later normal-unlock terminal attempt is rejected.

### Milestone 4: Add `TestLeaseLockNormalUnlockWinsInFlightRenewal`

Scope: Add one top-level test in `pkg/objstore/locking_model_concurrency_test.go`. The test covers both shutdown interleavings where normal unlock can win while renewal is already inside an attempt: cancellation while blocked in renewal `WriteFile`, and cancellation after a successful write while blocked in post-write lease proof.

Scenario:

- Set lease constants similar to existing `TestLeaseLockUnlockWaitsForInFlightRenewal`: `objstore.TESTSetLeaseConstants(2*time.Second, 10*time.Millisecond, 3, 5*time.Millisecond)` and `objstore.TESTSetRenewalProofConstants(1500*time.Millisecond, time.Millisecond)`.
- Create local storage and wrap it with a storage helper that blocks renewal `WriteFile` until its context is canceled, records the returned context error, and tracks whether `DeleteFile` starts before the blocked write exits.
- Acquire a migration write lock with `objstore.TryLockRemoteWrite(ctx, wrapped, "v1/LOCK", "owner", localLeaseClock())`.
- Find the physical path in base storage and configure the wrapper to block the next write to that physical path.
- Start `protectedWorker` and confirm one protected step can happen.
- Create `terminalRecorder` and a `leaseLostCalled` channel.
- Start renewal with `objstore.TESTStartRenewal(ctx, lock, onLeaseLost)`. The callback must first unconditionally signal `leaseLostCalled`, then attempt `recorder.record(terminalLeaseLost)`, then stop the worker only if appropriate. Do not guard the channel signal behind the recorder result.
- Wait for the wrapper's write-started signal, proving the renewal write is inside `WriteFile`.
- Record `terminalNormalUnlock` in the terminal recorder and stop the worker with `workerStopTestStop` to model business normal unlock winning before the storage operation returns.
- Start `lock.Unlock(ctx)` in a goroutine.
- Wait until `objstore.TESTRenewalStopSignalClosed(lock)` returns true, proving real `Unlock` has closed the renewal stop signal before the blocked renewal write exits.
- While unlock is blocked, assert `wrapped.requireNoDeleteBeforeWriteExit(t)`.
- Wait for the blocked write to return and assert it returned `context.Canceled`, proving normal unlock canceled the in-flight attempt instead of waiting for a lease timeout.
- Wait for `lock.Unlock(ctx)` to return.

Add a sibling subtest for post-write proof cancellation:

- Use a `blockingLeaseClock` with acquire pre/post times, one renewal pre-write time, and a blocked renewal post-write proof call.
- Wait for the blocked post-write proof call to start; this proves the renewal write already returned successfully.
- Record `terminalNormalUnlock`, stop the worker with `workerStopTestStop`, and call real `lock.Unlock(ctx)` in a goroutine.
- Wait until `objstore.TESTRenewalStopSignalClosed(lock)` returns true, then wait for the blocked clock call to finish with `context.Canceled`.
- Assert `onLeaseLost` was not called, the recorder remains `terminalNormalUnlock`, the physical path is deleted by normal unlock, and the next owner can acquire.

Expected assertions:

- `recorder.reasonNow()` is `terminalNormalUnlock`.
- `onLeaseLost` is not called after normal unlock has won. The test must assert this by checking a buffered `leaseLostCalled` channel after `unlockDone`; because callback signaling is unconditional, this detects callback invocation even when the terminal recorder rejects `terminalLeaseLost`.
- `recorder.record(terminalLeaseLost)` returns false if the test explicitly attempts it after `Unlock`.
- `worker.requestStep(t)` returns `ok=false`.
- `wrapped.requireNoDeleteBeforeWriteExit(t)` passes.
- The physical path no longer exists after unlock.
- A later `objstore.TryLockRemoteWrite(ctx, base, "v1/LOCK", "next-owner", localLeaseClock())` succeeds and can unlock.
- `requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")` passes.

Expected red-test behavior:

This test is expected to fail against current production code if an in-flight renewal write returns `context.DeadlineExceeded` after `Unlock` has closed `stopCh`, or if an in-flight post-write proof returns `context.Canceled` after `Unlock` has closed `stopCh` and is still classified as permanent `lease_lost`. The likely failure is that `onLeaseLost` is called before the renewal loop observes `stopCh` again, or that shutdown cancellation is wrapped as post-write proof failure. Do not weaken the test to accept this callback. This is the planned red test for normal-unlock terminal precedence. If the test does not fail before production changes, record the evidence and treat it as proof-strength rather than forcing a production edit.

### Milestone 5: Implement Minimal Production Fix If Needed

Scope: Modify `pkg/objstore/locking.go` only if `TestLeaseLockNormalUnlockWinsInFlightRenewal` fails as expected.

Candidate implementation:

Give `RemoteLock` a renewal-owned cancellable context. `startRenewal` creates `renewalCtx, renewalCancel := context.WithCancel(ctx)`, stores `renewalCancel`, and launches `renewalLoop(renewalCtx, onLeaseLost)`. `stopRenewalIfStarted` remains the single stop issuer: when it closes `stopCh`, it also calls the stored `renewalCancel`. Inside `renewalLoop`, pass this context directly into `tryRenew`; per-operation timeout contexts inside `tryRenew` keep the error classification tied to the operation that timed out.

After `tryRenew` returns, classify results in this order: successful renewals are accepted only if the stop signal is not closed; permanent renewal loss still invokes `onLeaseLost`; non-permanent shutdown or transient results are suppressed if the stop signal is closed; otherwise retry logic proceeds as before.

The intended shape is:

    result, err := l.tryRenew(ctx)

    if err == nil {
        if l.renewalStopSignalClosed() {
            return
        }
        nextDelay = result.nextDelay
        leaseDeadline = time.Now().Add(result.remainingLease)
        renewSucceeded = true
        break
    }
    if isPermanentRenewalLoss(err) {
        invokeLost()
        return
    }
    if l.renewalStopSignalClosed() {
        return
    }

Acceptance:

- `stopCh` closure must also call `renewalCancel`, so context-respecting storage and clock operations return promptly during normal unlock.
- Permanent renewal loss classification must happen before stop-based suppression.
- Successful renewal results must not be accepted after `stopCh` is closed.
- Non-permanent shutdown or transient results may be suppressed after `stopCh` is closed.
- Do not change `tryRenew` error classification.
- Do not make `Unlock` delete before the in-flight write exits.
- Do not suppress genuine lease-lost callbacks, including already-formed write timeout and post-write proof failure, even if `stopCh` closes before classification.

Expected passing behavior:

- `TestLeaseLockNormalUnlockWinsInFlightRenewal` passes.
- Existing `TestLeaseLockUnlockWaitsForInFlightRenewal` still passes, proving delete ordering remains intact.
- Existing renewal-lost tests still pass, proving real lease-lost still invokes the callback when normal unlock has not won.
- `TestLeaseLockTerminalReasonStableAfterLeaseLost/write timeout remains lease lost when unlock closes stop signal` passes, proving permanent timeout is not hidden by a later stop signal.

### Milestone 6: Run Validation and Update the Plan

Run targeted tests from repository root. Because `pkg/objstore` uses failpoints, use the failpoint-aware runner:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(TerminalReasonStableAfterLeaseLost|NormalUnlockWinsInFlightRenewal)' -count=1

Expected before production fix:

- The terminal reason stability test may pass.
- The normal-unlock race should fail if current code invokes `onLeaseLost` after normal unlock has won.

Expected after production fix:

- Both new tests pass.

Run nearby regression tests:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(UnlockWaitsForInFlightRenewal|RenewalLostStopsCriticalSection|RenewalAmbiguousWriteAndProofFailureStopProtectedWork)' -count=1

Expected:

- Existing unlock ordering, lease-lost stop, and ambiguous-write proof tests pass.

Run formatting before collecting final pass evidence:

    gofmt -w pkg/objstore/locking.go pkg/objstore/locking_model_concurrency_test.go pkg/objstore/export_test.go

Expected:

- Go files are formatted before final test runs.

Run `make bazel_prepare`. This is mandatory because this plan adds new top-level `TestXxx` functions in an existing Go test file and modifies test hooks:

    make bazel_prepare

Expected:

- No unexpected Bazel metadata drift. If `pkg/objstore/BUILD.bazel` changes, inspect and keep only relevant generated metadata.

Run whitespace check:

    git diff --check

Expected:

- No output and exit code 0.

Because this plan changes docs under `docs/agents`, run the relevant checks from `docs/agents/agents-review-guide.md`:

    rg -n -P '`(MUST(?: NOT)?|SHOULD|MAY)`' AGENTS.md docs/agents/agents-review-guide.md docs/agents/br/lease-lock-model-concurrency-test-design.md docs/agents/br/lease-lock-terminal-tests-implementation-plan.md
    test -e docs/agents/br/lease-lock-terminal-tests-implementation-plan.md
    test -e pkg/objstore/locking_model_concurrency_test.go

Expected:

- No backticked normative keyword matches.
- Referenced paths exist.

For local WIP completion, record the targeted test, regression test, `make bazel_prepare`, docs checks, and `git diff --check` evidence. For Ready / PR-readiness claims after code changes, run the Ready profile, including:

    make lint

Expected:

- `make lint` exits 0 before claiming Ready or PR readiness.

Update this ExecPlan:

- Mark completed progress items.
- Add any red-test output to `Surprises & Discoveries`.
- Add final validation commands and results to `Outcomes & Retrospective`.

## Concrete Steps

- [ ] **Step 1: Add terminal and delete recorder helpers**

  Edit `pkg/objstore/locking_model_concurrency_test.go` near the helper section before the first top-level test. Add `terminalReason`, `terminalRecorder`, `waitTerminalReason`, `deleteRecordingStorage`, `requireDeletedPath`, and `requireNoDeleteSoon` as described in Milestone 1.

- [ ] **Step 2: Add test-only renewal stop observation helper**

  Edit `pkg/objstore/export_test.go` and add `TESTRenewalStopSignalClosed` as described in Milestone 2.

- [ ] **Step 3: Add lease-lost then unlock test**

  In `pkg/objstore/locking_model_concurrency_test.go`, add a top-level test named
  `TestLeaseLockTerminalReasonStableAfterLeaseLost`. It must contain these exact subtests and implement the scenarios from
  Milestone 3:

  - `txn id mismatch then unlock`
  - `write timeout late commit then unlock`
  - `post write proof failed then unlock`
  - `write timeout remains lease lost when unlock closes stop signal`

  Use the exact scenario and assertions from Milestone 3. Each subtest must attempt `recorder.record(terminalNormalUnlock)` after `Unlock` and assert it returns false.

- [ ] **Step 4: Run the targeted new test before production changes**

  Run:

      ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockTerminalReasonStableAfterLeaseLost' -count=1

  Expected:

  The test may pass. It is a proof-strength test. If it fails, inspect whether the failure is a real safety issue or a test setup bug. Do not adjust assertions that protect terminal reason stability.

- [ ] **Step 5: Add normal-unlock race red test**

  In `pkg/objstore/locking_model_concurrency_test.go`, add a top-level test named
  `TestLeaseLockNormalUnlockWinsInFlightRenewal`.

  Use the exact scenario and assertions from Milestone 4. The test must wait for `objstore.TESTRenewalStopSignalClosed(lock)` before the blocked renewal write or blocked post-write proof exits, assert the blocked operation returns `context.Canceled`, and `onLeaseLost` must unconditionally signal `leaseLostCalled`.

- [ ] **Step 6: Run the normal-unlock race test and preserve red evidence**

  Run:

      ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLockNormalUnlockWinsInFlightRenewal' -count=1

  Expected:

  The test is expected to fail if current code invokes `onLeaseLost` after normal unlock has won. Record the failure summary in `Surprises & Discoveries`.

- [ ] **Step 7: Implement the minimal production fix only if Step 6 fails for the expected reason**

  Edit `pkg/objstore/locking.go` in `renewalLoop` as described in Milestone 5. Do not change unrelated renewal behavior.

- [ ] **Step 8: Format changed Go files**

  Run:

      gofmt -w pkg/objstore/locking.go pkg/objstore/locking_model_concurrency_test.go pkg/objstore/export_test.go

  Expected:

  Go formatting completes with no output.

- [ ] **Step 9: Re-run new tests**

  Run:

      ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(TerminalReasonStableAfterLeaseLost|NormalUnlockWinsInFlightRenewal)' -count=1

  Expected:

  Both tests pass.

- [ ] **Step 10: Re-run nearby regression tests**

  Run:

      ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(UnlockWaitsForInFlightRenewal|RenewalLostStopsCriticalSection|RenewalAmbiguousWriteAndProofFailureStopProtectedWork)' -count=1

  Expected:

  The existing tests pass.

- [ ] **Step 11: Run mandatory Bazel metadata preparation and diff checks**

  Run:

      make bazel_prepare
      git diff --check

  Expected:

  `make bazel_prepare` exits 0. `git diff --check` exits 0.

- [ ] **Step 12: Run docs/agents review checks**

  Run:

      rg -n -P '`(MUST(?: NOT)?|SHOULD|MAY)`' AGENTS.md docs/agents/agents-review-guide.md docs/agents/br/lease-lock-model-concurrency-test-design.md docs/agents/br/lease-lock-terminal-tests-implementation-plan.md
      test -e docs/agents/br/lease-lock-terminal-tests-implementation-plan.md
      test -e pkg/objstore/locking_model_concurrency_test.go

  Expected:

  No backticked normative keyword matches, and path checks exit 0.

- [ ] **Step 13: Update this ExecPlan and request review**

  Mark progress items complete, record command evidence, and ask a subagent to review proof strength. The review should focus on false positives, whether normal-unlock really wins before renewal returns, and whether `Unlock` return assertions are cause-specific.

- [ ] **Step 14: Run Ready profile only before Ready / PR-readiness claims**

  If the work is being declared ready, run:

      make lint

  Expected:

  `make lint` exits 0. Do not claim Ready without this evidence.

## Validation and Acceptance

This plan is complete when all of these are true:

- `TestLeaseLockTerminalReasonStableAfterLeaseLost` proves `lease_lost` is stable across a later `Unlock` for `TxnID` mismatch, write timeout with late commit, and post-write proof failure.
- `TestLeaseLockNormalUnlockWinsInFlightRenewal` proves normal unlock suppresses a later renewal-loss callback from an in-flight renewal operation.
- `TestLeaseLockUnlockWaitsForInFlightRenewal` still proves `Unlock` waits before deleting the physical path.
- `TestLeaseLockRenewalLostStopsCriticalSection` still proves genuine renewal loss stops protected work.
- `TestLeaseLockRenewalAmbiguousWriteAndProofFailureStopProtectedWork` still proves ambiguous write and post-write proof failure stop protected work when normal unlock has not won.
- `make bazel_prepare` has been run and any relevant generated metadata is included.
- `docs/agents/agents-review-guide.md` checks have been run for the changed `docs/agents` files.
- `git diff --check` passes.
- The ExecPlan records exact commands and outcomes.
- If Ready or PR readiness is claimed, `make lint` has been run and exits 0.

Use WIP validation for local iteration. Do not claim Ready or PR readiness without the repository Ready profile, including `make lint` for code changes.

## Idempotence and Recovery

The tests use temporary local object storage, scoped lease timing overrides, and cleanup callbacks. They should be safe to rerun.

If a test hangs, first check for a blocked storage or clock helper whose release function was not called in cleanup. Cancel the parent context and call `objstore.TESTStopRenewal(lock)` before retrying.

If `make bazel_prepare` changes unrelated files, inspect `git status --short` and do not include unrelated metadata churn without understanding why.

If the normal-unlock race does not fail before production changes, do not force a production edit. Record that current implementation already satisfies the strict terminal-precedence test and keep the proof-strength test.

## Artifacts and Notes

Planned target test command:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(TerminalReasonStableAfterLeaseLost|NormalUnlockWinsInFlightRenewal)' -count=1

Planned regression command:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(UnlockWaitsForInFlightRenewal|RenewalLostStopsCriticalSection|RenewalAmbiguousWriteAndProofFailureStopProtectedWork)' -count=1

## Interfaces and Dependencies

No production API changes are planned.

The only likely production implementation change is internal to `pkg/objstore/locking.go` in `(*RemoteLock).renewalLoop`.

Tests depend on these existing helpers:

- `createMockStorage(t)` from `pkg/objstore/locking_test.go`.
- `requireSinglePathWithPrefix(t, strg, prefix)` from `pkg/objstore/locking_test.go`.
- `requireFileNotExists(t, path)` from `pkg/objstore/locking_test.go`.
- `newCriticalSectionAudit`, `startProtectedWorker`, `requireNoStepAfterAction`, and `requireNoIntentWithPrefix` from `pkg/objstore/locking_concurrency_test.go`.
- `lateWriteStorage`, `contextDoneWriteStorage`, `blockingLeaseClock`, `clockAt`, `readLockMeta`, `waitOperationFinished`, `startModelRenewal`, and `stopModelRenewal` from `pkg/objstore/locking_model_concurrency_test.go`.
- `TESTRenewalStopSignalClosed`, added to `pkg/objstore/export_test.go` by this plan.

When editing tests, keep package `objstore_test` and do not add new dependencies.
