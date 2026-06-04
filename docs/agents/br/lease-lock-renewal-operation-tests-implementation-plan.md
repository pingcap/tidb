# Lease Lock Renewal Operation Tests Implementation Plan

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan must be maintained according to it.

## Purpose / Big Picture

This work adds deterministic `pkg/objstore` tests for lease-lock renewal operation hangs and ambiguous outcomes. After implementation, a developer can verify that a holder whose renewal observation fails once does not immediately lose the lease, while a holder whose renewal write outcome or post-write proof is ambiguous stops protected work before safety can be violated.

The user-visible outcome is stronger confidence in BR lease-lock safety under object-store and lease-clock stalls. The observable proof is a targeted `pkg/objstore` test run that exercises read hang, pre-write clock hang, write timeout with late write, ordinary write error, and post-write proof failure.

## Progress

- [x] (2026-06-04) Captured renewal operation failure semantics in `docs/agents/br/lease-lock-model-concurrency-test-design.md`.
- [x] (2026-06-04) Drafted this implementation plan.
- [x] (2026-06-04) Reviewed plan with sub-agent Noether for feasibility and proof strength.
- [x] (2026-06-04) Updated this plan with review findings before coding.
- [x] (2026-06-04) Implemented red tests in `pkg/objstore/locking_model_concurrency_test.go`.
- [x] (2026-06-04) Ran targeted tests and confirmed the intended red failures: read/pre-clock/post-clock hangs saw guard timeout instead of `context.DeadlineExceeded`.
- [x] (2026-06-04) Implemented the minimal production behavior needed to pass the tests.
- [x] (2026-06-04) Ran targeted WIP validation for new tests and nearby renewal tests.
- [x] (2026-06-04) Ran `make bazel_prepare`; `pkg/objstore/BUILD.bazel` now includes the new test file.
- [x] (2026-06-04) Recorded final WIP validation evidence and reviewer findings.

## Surprises & Discoveries

- Current code already treats renewal `WriteFile` timeout as permanent renewal loss through `errRenewWriteTimeout`.
  Evidence: `pkg/objstore/locking.go` includes `errRenewWriteTimeout` in `isPermanentRenewalLoss`.
- Current code treats renewal `ReadFile`, pre-write `LeaseClock.Now`, and non-timeout `WriteFile` errors as transient because they are not included in `isPermanentRenewalLoss`.
  Evidence: `tryRenew` wraps those errors without a permanent sentinel.
- Current code does not apply a bounded context to renewal `ReadFile`, pre-write lease clock, or post-write proof clock.
  Evidence: `tryRenew` calls `l.storage.ReadFile(ctx, l.path)`, `leaseClock.Now(ctx)`, and post-write `leaseClock.Now(ctx)` with the parent context.
- Sub-agent review found that blocking helpers can create false positives if guard timeout returns an ordinary error to production code. A test must assert that the blocked operation observed a bounded context deadline; otherwise current code can accidentally pass by treating the guard error as transient or permanent proof failure.
  Evidence: `tryRenew` currently classifies read/pre-clock ordinary errors as transient and post-write clock ordinary errors as permanent proof failure.
- Sub-agent review found that `renewalLoop` tracks the proven retry window with local `time.Now()`, not only the injected lease clock. Tests for "repeated observation failures exhaust proven window" must therefore use wall-clock bounds and avoid confusing retry exhaustion with proven-window exhaustion.
  Evidence: `renewalLoop` updates `leaseDeadline := time.Now().Add(result.remainingLease)` and checks `time.Until(leaseDeadline)` before retry backoff.
- During implementation, the pre-write clock retry test needed to capture the renewal `WriteFile` payload directly instead of racing against final file state.
  Evidence: a scripted clock can be exhausted while the renewal loop is still alive; capturing the specific renewal write proves the retry advanced `ExpireAt` without relying on later final state.

## Decision Log

- Decision: Scope this plan to `pkg/objstore` deterministic tests plus the minimal production behavior required to satisfy them.
  Rationale: BR shell HA tests are higher cost and should follow after lock-layer proof is stronger.
  Date/Author: 2026-06-04 / Codex + user.
- Decision: Treat mutation-before observation failures (`ReadFile`, pre-write clock) as transient unless repeated failures exhaust the proven lease window.
  Rationale: These failures do not mutate remote lock state, create late writes, or revive a physical lock path.
  Date/Author: 2026-06-04 / Codex + user.
- Decision: Treat renewal `WriteFile` timeout and post-write proof failure as requiring protected work to stop.
  Rationale: A timed-out write may still land later, and a post-write proof failure means the holder cannot prove a safe lease window after mutating remote metadata.
  Date/Author: 2026-06-04 / Codex + user.
- Decision: Keep the next step focused on test design and proof strength, not production helper naming.
  Rationale: The user explicitly asked to avoid getting stuck in implementation-code details before tests are designed and reviewed.
  Date/Author: 2026-06-04 / user.
- Decision: Blocking helper guard timeout is a test failure signal, not a simulated production error.
  Rationale: Returning guard timeout as an ordinary storage/clock error can make current code accidentally pass without proving bounded operation contexts.
  Date/Author: 2026-06-04 / sub-agent review + Codex.
- Decision: Some full-loop tests are proof-strength tests rather than red tests.
  Rationale: Write timeout, ordinary write error, and expired post-write proof already have helper-level coverage and may pass before new implementation. They remain valuable because they bind existing semantics to protected-worker stop behavior and late-write safety.
  Date/Author: 2026-06-04 / sub-agent review + Codex.

## Outcomes & Retrospective

- Outcome: New deterministic renewal operation tests were added and passed after minimal implementation.
  Evidence:
  `./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(RenewalObservationHangIsTransient|RenewalAmbiguousWriteAndProofFailureStopProtectedWork)' -count=1`
  passed after first confirming the intended red failures; nearby renewal tests also passed with
  `./tools/check/failpoint-go-test.sh pkg/objstore -run 'Test(TryRenew|StartRenewal|LeaseLockUnlockWaitsForInFlightRenewal)' -count=1`.
  `make bazel_prepare` completed and added `locking_model_concurrency_test.go` to `pkg/objstore/BUILD.bazel`.
  Remaining gaps: Ready-profile validation such as `make lint` has not been run in this WIP pass.
  Date/Author: 2026-06-04 / Codex.

## Context and Orientation

The relevant package is `pkg/objstore`. Lease locks are implemented in `pkg/objstore/locking.go`; tests live in `pkg/objstore/locking_test.go` and `pkg/objstore/locking_concurrency_test.go`.

A holder is a process or goroutine that acquired a lease lock and is allowed to run protected work. Protected work means the business operation guarded by the lock. The renewal loop is the background goroutine started by `RemoteLock.startRenewal` through `objstore.TESTStartRenewal` in tests.

The current renewal sequence in `RemoteLock.tryRenew` is:

    ReadFile(lock path)
    unmarshal and verify TxnID
    leaseClock.Now before write
    WriteFile refreshed metadata with a bounded context
    leaseClock.Now after write to prove the new ExpireAt is still safe

The existing tests already provide useful helpers:

- `createMockStorage` in `pkg/objstore/locking_test.go` creates a local object storage.
- `sequenceLeaseClock` in `pkg/objstore/locking_test.go` scripts lease clock timestamps and errors.
- `protectedWorker` and `criticalSectionAudit` in `pkg/objstore/locking_concurrency_test.go` model protected business work and terminal events.
- `objstore.TESTStartRenewal`, `objstore.TESTStopRenewal`, and timing override helpers in `pkg/objstore/export_test.go` expose renewal internals to tests.

New tests should be placed in `pkg/objstore/locking_model_concurrency_test.go` using package `objstore_test`. This keeps the model/concurrency phase separate from the existing first-phase deterministic tests while reusing package-level helpers.

Because the package uses failpoints, local unit validation must use:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(RenewalObservationHangIsTransient|RenewalAmbiguousWriteAndProofFailureStopProtectedWork)' -count=1

Because adding a new Go test file and new top-level `TestXxx` functions changes Bazel metadata requirements, run:

    make bazel_prepare

after implementing the tests.

## Plan of Work

### Milestone 1: Add Test Harness Helpers

Create `pkg/objstore/locking_model_concurrency_test.go`.

Add focused test helpers in that file:

- `operationBlockingStorage`, embedding `storeapi.Storage`, with configurable interception for renewal `ReadFile` on a specific physical lock path.
- `lateWriteStorage`, embedding `storeapi.Storage`, with configurable interception for renewal `WriteFile` on a specific physical lock path and an explicit test-controlled late commit.
- `blockingLeaseClock`, implementing `objstore.LeaseClock`, with scripted calls that can return a timestamp, return an error, or block until the passed context is canceled.
- small wait/assert helpers only if existing `waitClosed`, `protectedWorker`, and `criticalSectionAudit` helpers are insufficient.

Acceptance for this milestone:

- The new helpers do not alter production code.
- Blocking helpers have guard timeouts so a missing bounded context fails the test quickly with an explanatory error instead of hanging.
- Blocking helpers must expose whether the intercepted operation observed `context.DeadlineExceeded` from the context passed by production code. Tests must assert this explicitly for read/pre-clock/post-clock hang cases.
- Guard timeout must not be returned to production code as if it were an ordinary storage or clock error unless the subtest is intentionally testing ordinary transient errors.
- Helper event channels identify the operation and path being blocked.
- Full renewal-loop tests should acquire with `objstore.TryLockRemoteWrite`, create the `protectedWorker`, then call `objstore.TESTStartRenewal`. This ordering avoids an `onLeaseLost` callback racing ahead of worker setup.
- Every subtest cleanup must release storage/clock barriers and cancel the parent context before calling `objstore.TESTStopRenewal`; otherwise current code paths without bounded contexts can hang cleanup.

Expected red-test behavior before production changes:

- A renewal `ReadFile` hang test should fail because current code passes the parent context and the helper reports that no bounded context deadline was observed.
- A pre-write or post-write clock hang test should fail for the same reason.
- Write-timeout and ordinary-write-error full-loop tests may already pass before production changes; they are retained as proof-strength tests, not mandatory red tests.

### Milestone 2: Add Observation-Hang Tests

Add top-level test:

    func TestLeaseLockRenewalObservationHangIsTransient(t *testing.T)

Use subtests for these behavior cases:

1. `read timeout then retry succeeds`

   Scenario:

   - Acquire a write lock with scripted lease-clock times.
   - Start a `protectedWorker`.
   - Start renewal with `objstore.TESTStartRenewal`.
   - Configure `operationBlockingStorage` so the first renewal `ReadFile` on the physical path blocks until its context is canceled.
   - Assert the storage helper observed `context.DeadlineExceeded` on the passed context; if a guard timeout fired instead, fail the test.
   - Assert `onLeaseLost` is not called for that single observation timeout.
   - Release or disable the read block so the next retry succeeds.
   - Assert the remote lock metadata `ExpireAt` advances after the successful retry.
   - Assert protected work can step before terminal state and the lock can unlock normally.

   Proof:

   This proves a single non-mutating read stall is not treated like a permanent lease loss.

2. `pre-write clock timeout then retry succeeds`

   Scenario:

   - Use `blockingLeaseClock` so acquire and post-acquire proof calls return normally.
   - Block the renewal pre-write `Now` call until its context is canceled.
   - Assert the clock helper observed `context.DeadlineExceeded` on the passed context; if a guard timeout fired instead, fail the test.
   - Assert no immediate `lease_lost` event.
   - Allow the next renewal attempt to receive normal pre-write and post-write proof times.
   - Assert the remote lock metadata `ExpireAt` advances after the successful retry.
   - Assert protected work has no step after any terminal event and can unlock normally.

   Proof:

   This proves a single pre-mutation lease-clock stall is treated as transient.

3. `repeated observation failures exhaust proven window`

   Scenario:

   - Configure repeated renewal `ReadFile` or pre-write clock failures.
   - Use short but not razor-thin wall-clock constants, for example TTL around several hundred milliseconds, renew interval in tens of milliseconds, operation timeout cap in tens of milliseconds, max retries high enough not to be the limiting factor, and base backoff large enough that a later retry cannot fit in the remaining proven window.
   - Record first observation failure time, each blocked operation attempt, and lease-lost time.
   - Assert `onLeaseLost` fires after at least one retry attempt and before `renewMaxRetries` could be exhausted, so the observed terminal reason is the proven-window boundary rather than plain retry exhaustion.
   - Assert releasing the blocked operation after terminal state does not let the old holder record protected steps.

   Proof:

   This proves observation failures are recoverable only while the last proven lease window still justifies continued protected work.

4. `observation hang before detecting hijack`

   Scenario:

   - Acquire a lock with owner A and start renewal.
   - Block A's renewal `ReadFile`.
   - Overwrite A's physical lock path with a different `TxnID` while A is blocked. This intentionally models the same-path hijack branch that `tryRenew` can observe directly; stale cleanup with a later new physical instance is covered by the cleanup/reacquire tests in `locking_concurrency_test.go` and the model design doc.
   - Release A's read block.
   - Assert A detects TxnID mismatch or expired lease and enters terminal state.
   - Assert A cannot record a protected step after terminal state.

   Proof:

   This proves a read hang cannot hide a later hijack and let the old holder continue after its proven lease window.

### Milestone 3: Add Ambiguous-Write and Proof-Failure Tests

Add top-level test:

    func TestLeaseLockRenewalAmbiguousWriteAndProofFailureStopProtectedWork(t *testing.T)

Use subtests for these behavior cases:

1. `write timeout stops worker even if write lands late`

   Scenario:

   - Acquire a write lock and start a `protectedWorker`.
   - Start renewal with `lateWriteStorage`.
   - On the first renewal `WriteFile`, capture the payload and block until the write context is canceled.
   - Assert `onLeaseLost` fires and the worker stops.
   - Release the storage helper so it commits the captured payload after returning timeout to the caller.
   - Assert the late committed metadata matches the captured renewal payload and contains an advanced `ExpireAt`.
   - Assert the old holder still cannot record protected steps after late commit.

   Proof:

   This proves safety is preserved even under ambiguous object-store write outcome.

2. `ordinary write error remains transient`

   Scenario:

   - Configure the first renewal `WriteFile` to return a non-timeout storage error.
   - Assert `onLeaseLost` is not called immediately.
   - Let the next retry succeed.
   - Assert the remote lock metadata `ExpireAt` advances after retry and no `lost` audit event was recorded before success.
   - Assert protected work can continue only before terminal state and the lock can unlock normally.

   Proof:

   This proves the test suite distinguishes ordinary write failure from ambiguous write timeout.

3. `post-write proof clock hang stops worker`

   Scenario:

   - Configure renewal pre-write clock to return normally.
   - Let renewal `WriteFile` return success.
   - Block the post-write proof clock call until its context is canceled.
   - Assert the clock helper observed `context.DeadlineExceeded`; if a guard timeout returned an ordinary error, fail the test because that would only prove ordinary post-clock error handling.
   - Assert `onLeaseLost` fires and the worker stops.
   - Assert no protected step can be recorded after terminal state.

   Proof:

   This proves a holder cannot continue protected work merely because the write returned; it also needs proof that the renewed lease window is safe.

4. `post-write proof says lease already unsafe`

   Scenario:

   - Let renewal pre-write clock and `WriteFile` succeed.
   - Return a post-write proof time later than the new `ExpireAt`.
   - Assert `onLeaseLost` fires and the worker stops.

   Proof:

   This covers the non-hanging proof failure path already represented at helper level, but now checks the full renewal loop and protected-worker stop behavior.

### Milestone 4: Minimal Production Behavior to Satisfy Tests

Do not start this milestone until the sub-agent review accepts the plan and the red tests have been observed.

Modify `pkg/objstore/locking.go` only as much as needed to make the tests pass:

- Apply lease-bounded contexts to renewal `ReadFile`, pre-write lease clock `Now`, renewal `WriteFile`, and post-write proof lease clock `Now`.
- Use the same timeout policy for each operation: single operation timeout is capped by 10 minutes in production and by the existing test timing override in tests.
- Preserve current transient classification for ordinary `ReadFile`, pre-write clock, and non-timeout `WriteFile` errors.
- Preserve or add permanent protected-work stop behavior for write timeout and post-write proof failure.
- Ensure repeated transient failures still stop renewal before retry/backoff would exceed the proven lease window.

If test-only exported timing helpers need to be renamed or extended in `pkg/objstore/export_test.go`, keep backward compatibility with existing tests unless all call sites are updated in the same change.

### Milestone 5: Validation and Documentation Update

Run targeted WIP validation:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestLeaseLock(RenewalObservationHangIsTransient|RenewalAmbiguousWriteAndProofFailureStopProtectedWork)' -count=1

Run nearby renewal tests:

    ./tools/check/failpoint-go-test.sh pkg/objstore -run 'Test(TryRenew|StartRenewal|LeaseLockUnlockWaitsForInFlightRenewal)' -count=1

Run Bazel metadata generation because this plan adds top-level tests / likely a new Go test file:

    make bazel_prepare

Before claiming Ready, follow repository policy and run the Ready profile, including:

    make lint

If behavior discoveries change the test design, update:

- `docs/agents/br/lease-lock-model-concurrency-test-design.md`
- this implementation plan's `Decision Log` and `Surprises & Discoveries`

## Concrete Steps

1. Create `pkg/objstore/locking_model_concurrency_test.go` with license header and package `objstore_test`.
2. Add `operationBlockingStorage` and a small unit-style test path inside `TestLeaseLockRenewalObservationHangIsTransient/read timeout then retry succeeds`.
3. Run the targeted test and confirm it fails because renewal read does not receive a bounded context.
4. Add `blockingLeaseClock` and the pre-write clock observation subtest.
5. Run the targeted test and confirm it fails because pre-write clock does not receive a bounded context.
6. Add repeated observation failure and hijack subtests.
7. Add `lateWriteStorage` and the write-timeout late-write subtest.
8. Add ordinary write error and post-write proof subtests.
9. Run the full new-test regex and record the expected red failures.
   Expected red failures before production changes: read/pre-clock/post-clock hang subtests fail because no bounded context deadline is observed. Write-timeout and ordinary-write-error subtests may pass because they exercise behavior already present at helper level.
10. Implement the minimal production behavior in `pkg/objstore/locking.go`.
11. Run the new-test regex until it passes.
12. Run nearby renewal tests.
13. Run `make bazel_prepare` and review generated Bazel diffs.
14. Update docs with any discovered behavior changes.

## Validation and Acceptance

The implementation is acceptable when:

- `TestLeaseLockRenewalObservationHangIsTransient` proves single read/pre-clock observation timeout does not directly call `onLeaseLost`.
- The same test proves repeated observation failure cannot let protected work continue past the last proven lease window.
- `TestLeaseLockRenewalAmbiguousWriteAndProofFailureStopProtectedWork` proves write timeout stops protected work even if the object-store write lands late.
- The same test proves ordinary write errors remain transient.
- The same test proves post-write proof hang/error stops protected work.
- Targeted failpoint-aware package tests pass.
- Bazel metadata is current after adding tests.

## Idempotence and Recovery

The test commands are safe to rerun. Timing overrides must be restored through `defer` or `t.Cleanup`; if a test flakes or hangs, inspect helper guard timeout messages before changing production code.

If `make bazel_prepare` changes unrelated files, inspect the diff and do not revert user changes. Only include Bazel metadata required by the new Go test file or new top-level tests.

If the red tests do not fail before production changes, update the test harness so it proves the missing bounded context or missing protected-worker stop explicitly.

## Artifacts and Notes

Current related design documents:

- `docs/agents/br/lease-lock-model-concurrency-test-design.md`
- `docs/agents/br/lease-lock-concurrency-and-ha-test-direction.md`
- `docs/agents/br/lease-lock-objstore-concurrency-test-implementation-plan.md`

Current closest code references:

- `pkg/objstore/locking.go`
- `pkg/objstore/locking_test.go`
- `pkg/objstore/locking_concurrency_test.go`
- `pkg/objstore/export_test.go`

## Interfaces and Dependencies

No new production API should be required. Tests may use existing `objstore.TEST*` helpers in `pkg/objstore/export_test.go`.

If production code needs a new internal helper to apply bounded operation contexts, keep it unexported in `pkg/objstore/locking.go`. Tests should assert externally observable behavior: `onLeaseLost`, worker terminal state, lock metadata state, and retry behavior.
