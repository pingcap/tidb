# Lease Lock Renewal Bounded Write Proof Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the first phase of bounded renewal proof so BR lease-lock holders stop protected work when renewal can no longer prove a safe lease window.

**Architecture:** Keep the change local to `pkg/objstore` renewal and stale-cleanup logic. Renewal writes get a lease-window-bounded context, successful writes get a post-write `LeaseClock` proof, next renewal scheduling uses the proven remaining window, and stale cleanup uses a separate 30 minute reclaim grace. This phase deliberately does not refactor the full `RemoteLock` lifecycle or add own-lock cleanup after loss.

**Tech Stack:** Go, `pkg/objstore`, `pkg/objstore/storeapi`, failpoint-enabled unit tests via `./tools/check/failpoint-go-test.sh`, `make bazel_prepare`, `make lint`.

---

This plan implements only the first phase from `docs/agents/br/lease-lock-renewal-bounded-write-proof-design.md`.

Second-stage lifecycle ideas are intentionally out of scope:

- do not add best-effort own-lock cleanup after loss;
- do not redesign `Unlock` around a unified `finish(reason)` path;
- do not split terminal outcome from renewal loop control in this phase;
- do not add cleanup tombstones or strict strong-consistency options.

## File Map

- `pkg/objstore/locking.go`: renewal constants, sentinel errors, `tryRenew` proof logic, and renewal-loop scheduling.
- `pkg/objstore/locking_helper.go`: stale cleanup threshold and any small helper functions shared by cleanup/renewal.
- `pkg/objstore/export_test.go`: test-only setters and exports for new renewal proof constants or sentinel errors.
- `pkg/objstore/locking_test.go`: focused tests for cleanup grace, bounded renewal write timeout, post-write proof, minimum remaining lease, and next-delay calculation.
- `docs/agents/br/lease-lock-renewal-bounded-write-proof-implementation-plan.md`: living implementation plan and validation record.

## Task 1: Split Stale Reclaim Grace From LeaseTTL

**Files:**

- Modify: `pkg/objstore/locking.go`
- Modify: `pkg/objstore/locking_helper.go`
- Modify: `pkg/objstore/export_test.go`
- Modify: `pkg/objstore/locking_test.go`
- Modify: `docs/agents/br/lease-lock-renewal-bounded-write-proof-implementation-plan.md`

- [x] **Step 1: Write failing cleanup-grace tests**

In `pkg/objstore/locking_test.go`, add focused coverage near the existing `TestCleanUpStaleTruncateLock*` tests:

```go
func TestCleanUpStaleTruncateLockUsesStaleReclaimGrace(t *testing.T) {
	ctx := context.Background()
	strg, pth := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()
	defer objstore.TESTSetStaleReclaimGrace(30 * time.Millisecond)()

	leaseNow := time.Date(2030, 5, 28, 10, 11, 12, 123456789, time.UTC)
	stalePath := "truncating.lock.0123456789abcdef0123456789abcdef"
	alivePath := "truncating.lock.33333333333333333333333333333333"
	writeLockMeta(t, strg, stalePath, objstore.LockMeta{
		LockedAt: leaseNow.Add(-time.Hour),
		ExpireAt: leaseNow.Add(-30*time.Millisecond).Add(-time.Nanosecond),
		TxnID:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Hint:     "past-reclaim-grace",
	})
	writeLockMeta(t, strg, alivePath, objstore.LockMeta{
		LockedAt: leaseNow.Add(-time.Hour),
		ExpireAt: leaseNow.Add(-30*time.Millisecond).Add(time.Nanosecond),
		TxnID:    []byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		Hint:     "inside-reclaim-grace",
	})

	clock := &sequenceLeaseClock{times: []time.Time{leaseNow, leaseNow}}
	reclaimed, err := objstore.CleanUpStaleTruncateLock(ctx, strg, clock)
	require.NoError(t, err)
	require.True(t, reclaimed)
	requireFileNotExists(t, filepath.Join(pth, stalePath))
	requireFileExists(t, filepath.Join(pth, alivePath))
}
```

Update nearby comments/assertions that still say `ExpireAt+LeaseTTL` so they use `staleReclaimGrace`.

- [x] **Step 2: Run RED cleanup test**

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestCleanUpStaleTruncateLockUsesStaleReclaimGrace' -count=1
```

Expected before implementation: compile failure because `objstore.TESTSetStaleReclaimGrace` does not exist, or behavior failure because cleanup still uses `LeaseTTL`.

- [x] **Step 3: Implement `staleReclaimGrace`**

In `pkg/objstore/locking.go`, add the new variable next to `LeaseTTL`:

```go
	// staleReclaimGrace is the extra delay after ExpireAt before stale cleanup
	// may reclaim a physical lock instance. It is separate from LeaseTTL so a
	// long lease does not force an equally long crash-reclaim delay.
	staleReclaimGrace = 30 * time.Minute
```

In `pkg/objstore/locking_helper.go`, change the cleanup threshold:

```go
	reclaimAfter := meta.ExpireAt.Add(staleReclaimGrace)
```

In `pkg/objstore/export_test.go`, add a test-only setter:

```go
// TESTSetStaleReclaimGrace overrides the stale cleanup grace for tests.
func TESTSetStaleReclaimGrace(grace time.Duration) (restore func()) {
	old := staleReclaimGrace
	staleReclaimGrace = grace
	return func() { staleReclaimGrace = old }
}
```

- [x] **Step 4: Run GREEN cleanup tests**

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestCleanUpStaleTruncateLock(UsesStaleReclaimGrace|OverdueWithinTTL|OverduePastTTL|RefreshedDuringWait|UsesClockForStaleDecision)' -count=1
```

Expected after implementation: selected cleanup tests pass. Update old test names only if needed; keep behavior-based names.

- [x] **Step 5: Commit Task 1**

Run:

```bash
git add pkg/objstore/locking.go pkg/objstore/locking_helper.go pkg/objstore/export_test.go pkg/objstore/locking_test.go docs/agents/br/lease-lock-renewal-bounded-write-proof-implementation-plan.md
git commit -m "pkg/objstore: split lease lock reclaim grace"
```

## Task 2: Add Bounded Renewal Write and Post-Write Proof

**Files:**

- Modify: `pkg/objstore/locking.go`
- Modify: `pkg/objstore/export_test.go`
- Modify: `pkg/objstore/locking_test.go`
- Modify: `docs/agents/br/lease-lock-renewal-bounded-write-proof-implementation-plan.md`

- [x] **Step 1: Add renewal proof tests and storage wrapper**

In `pkg/objstore/locking_test.go`, add `sync` to imports and add this helper near the other test storage wrappers:

```go
type controlledWriteStorage struct {
	storeapi.Storage
	blockPath   string
	block       atomic.Bool
	started     chan struct{}
	startedOnce sync.Once
}

func (s *controlledWriteStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	if s.block.Load() && name == s.blockPath {
		s.startedOnce.Do(func() { close(s.started) })
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return errors.New("renewal write did not receive bounded context")
		}
	}
	return s.Storage.WriteFile(ctx, name, data)
}
```

Add a timeout test:

```go
func TestTryRenewWriteTimeoutIsLeaseLost(t *testing.T) {
	ctx := context.Background()
	base, _ := createMockStorage(t)
	storage := &controlledWriteStorage{Storage: base, started: make(chan struct{})}
	defer objstore.TESTSetRenewalProofConstants(20*time.Millisecond, time.Millisecond)()

	leaseNow := time.Date(2030, 5, 28, 10, 11, 12, 0, time.UTC)
	renewNow := leaseNow.Add(time.Millisecond)
	clock := &sequenceLeaseClock{
		times: []time.Time{
			leaseNow,
			leaseNow.Add(time.Millisecond),
			renewNow,
		},
	}
	lock, err := objstore.TryLockRemoteWrite(ctx, storage, "v1/LOCK", "owner", clock)
	require.NoError(t, err)
	storage.blockPath = requireSinglePathWithPrefix(t, storage, "v1/LOCK.WRIT.")
	storage.block.Store(true)

	err = objstore.TESTTryRenew(ctx, lock)
	require.ErrorIs(t, err, objstore.TESTRenewWriteTimeout)
	select {
	case <-storage.started:
	case <-time.After(time.Second):
		t.Fatal("renewal write did not start")
	}
}
```

Add post-write proof tests:

```go
func TestTryRenewPostWriteProofRejectsExpiredLease(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	defer objstore.TESTSetRenewalProofConstants(5*time.Minute, time.Minute)()

	leaseNow := time.Date(2030, 5, 28, 10, 11, 12, 0, time.UTC)
	renewNow := leaseNow.Add(time.Minute)
	clock := &sequenceLeaseClock{
		times: []time.Time{
			leaseNow,
			leaseNow.Add(time.Millisecond),
			renewNow,
			renewNow.Add(objstore.LeaseTTL).Add(time.Nanosecond),
		},
	}
	lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "owner", clock)
	require.NoError(t, err)

	err = objstore.TESTTryRenew(ctx, lock)
	require.ErrorIs(t, err, objstore.TESTRenewPostWriteProofFailed)
}

func TestTryRenewPostWriteProofRejectsTinyRemainingLease(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	defer objstore.TESTSetRenewalProofConstants(5*time.Minute, time.Minute)()

	leaseNow := time.Date(2030, 5, 28, 10, 11, 12, 0, time.UTC)
	renewNow := leaseNow.Add(time.Minute)
	clock := &sequenceLeaseClock{
		times: []time.Time{
			leaseNow,
			leaseNow.Add(time.Millisecond),
			renewNow,
			renewNow.Add(objstore.LeaseTTL).Add(-30 * time.Second),
		},
	}
	lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "owner", clock)
	require.NoError(t, err)

	err = objstore.TESTTryRenew(ctx, lock)
	require.ErrorIs(t, err, objstore.TESTRenewRemainingLeaseTooSmall)
}
```

- [x] **Step 2: Update existing renewal-clock test expectations**

Rename `TestTryRenewUsesLeaseClockOneTimeForExpiryAndRefresh` to
`TestTryRenewUsesLeaseClockForExpiryRefreshAndPostWriteProof`, then update its
fake clock sequence so renewal has a post-write proof time:

```go
clock := &sequenceLeaseClock{
	times: []time.Time{
		leaseNow,
		leaseNow.Add(time.Millisecond),
		renewNow,
		renewNow.Add(time.Millisecond),
	},
}
```

Update the final assertion:

```go
require.Equal(t, 4, clock.idx)
```

Keep the existing `ExpireAt == renewNow.Add(objstore.LeaseTTL)` assertion.

- [x] **Step 3: Run RED renewal proof tests**

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryRenew(WriteTimeoutIsLeaseLost|PostWriteProofRejectsExpiredLease|PostWriteProofRejectsTinyRemainingLease)' -count=1
```

Expected before implementation: compile failure because `TESTSetRenewalProofConstants` and new sentinel exports do not exist.

- [x] **Step 4: Add constants and sentinel errors**

In `pkg/objstore/locking.go`, add constants near the renewal timing vars:

```go
	// renewWriteTimeoutCap bounds a single renewal WriteFile call. The actual
	// timeout is also capped by the remaining old lease window.
	renewWriteTimeoutCap = 5 * time.Minute

	// minRenewRemainingLease is the minimum proven lease window required after
	// a renewal write returns successfully.
	minRenewRemainingLease = time.Minute
```

Add sentinel errors near the existing renewal errors:

```go
var errRenewWriteTimeout = errors.New("renewal: write timed out")
var errRenewPostWriteProofFailed = errors.New("renewal: post-write lease proof failed")
var errRenewRemainingLeaseTooSmall = errors.New("renewal: remaining lease too small after write")
```

In `pkg/objstore/export_test.go`, export them and add the test setter:

```go
	TESTRenewWriteTimeout            = errRenewWriteTimeout
	TESTRenewPostWriteProofFailed    = errRenewPostWriteProofFailed
	TESTRenewRemainingLeaseTooSmall  = errRenewRemainingLeaseTooSmall
```

```go
// TESTSetRenewalProofConstants overrides renewal proof timing knobs for tests.
func TESTSetRenewalProofConstants(writeTimeoutCap, minRemaining time.Duration) (restore func()) {
	oldCap, oldMin := renewWriteTimeoutCap, minRenewRemainingLease
	renewWriteTimeoutCap = writeTimeoutCap
	minRenewRemainingLease = minRemaining
	return func() {
		renewWriteTimeoutCap = oldCap
		minRenewRemainingLease = oldMin
	}
}
```

- [x] **Step 5: Change `tryRenew` to return next delay**

Change the internal signature in `pkg/objstore/locking.go`:

```go
func (l *RemoteLock) tryRenew(ctx context.Context) (time.Duration, error)
```

Keep the external test helper compatible in `pkg/objstore/export_test.go`:

```go
func TESTTryRenew(ctx context.Context, l *RemoteLock) error {
	_, err := l.tryRenew(ctx)
	return err
}

func TESTTryRenewWithDelay(ctx context.Context, l *RemoteLock) (time.Duration, error) {
	return l.tryRenew(ctx)
}
```

Add helper functions in `pkg/objstore/locking.go`:

```go
func renewalWriteTimeout(expireAt, leaseNow time.Time) (time.Duration, error) {
	if expireAt.IsZero() {
		return renewWriteTimeoutCap, nil
	}
	remaining := expireAt.Sub(leaseNow)
	if remaining <= 0 {
		return 0, errRenewLeaseExpired
	}
	timeout := remaining / 2
	if timeout > renewWriteTimeoutCap {
		timeout = renewWriteTimeoutCap
	}
	if timeout <= 0 {
		return 0, errRenewLeaseExpired
	}
	return timeout, nil
}

func nextRenewDelay(remaining time.Duration) (time.Duration, error) {
	if remaining <= minRenewRemainingLease {
		return 0, errRenewRemainingLeaseTooSmall
	}
	delay := remaining / 3
	if delay > renewInterval {
		delay = renewInterval
	}
	if delay < 0 {
		delay = 0
	}
	return delay, nil
}
```

- [x] **Step 6: Implement bounded write and post-write proof**

In `tryRenew`, after old-lease validation and before `WriteFile`, compute the bounded timeout:

```go
	writeTimeout, err := renewalWriteTimeout(meta.ExpireAt, leaseNow)
	if err != nil {
		return 0, err
	}
	newExpireAt := leaseNow.Add(LeaseTTL)
	meta.ExpireAt = newExpireAt
```

Use a bounded context for `WriteFile`:

```go
	writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	if err := l.storage.WriteFile(writeCtx, l.path, newData); err != nil {
		if stderrors.Is(writeCtx.Err(), context.DeadlineExceeded) {
			return 0, errors.Annotate(errRenewWriteTimeout, err.Error())
		}
		return 0, errors.Annotatef(err, "tryRenew: WriteFile %s", l.path)
	}
```

Immediately prove the refreshed lease:

```go
	nowAfterWrite, err := leaseClock.Now(ctx)
	if err != nil {
		return 0, errors.Annotate(errRenewPostWriteProofFailed, err.Error())
	}
	if nowAfterWrite.After(newExpireAt) {
		return 0, errors.Annotatef(errRenewPostWriteProofFailed,
			"now=%s expire_at=%s", nowAfterWrite, newExpireAt)
	}
	delay, err := nextRenewDelay(newExpireAt.Sub(nowAfterWrite))
	if err != nil {
		return 0, err
	}
	return delay, nil
```

Keep existing `TxnID` mismatch, missing clock, and old lease expiration behavior.

- [x] **Step 7: Mark new renewal proof failures as permanent lost in `renewalLoop`**

Add a helper near the renewal sentinel errors:

```go
func isPermanentRenewalLoss(err error) bool {
	return stderrors.Is(err, errRenewTxnIDMismatch) ||
		stderrors.Is(err, errRenewLeaseExpired) ||
		stderrors.Is(err, errRenewWriteTimeout) ||
		stderrors.Is(err, errRenewPostWriteProofFailed) ||
		stderrors.Is(err, errRenewRemainingLeaseTooSmall)
}
```

Use it from `renewalLoop` so every new proof failure calls `onLeaseLost` and exits instead of entering the transient retry path.

- [x] **Step 8: Run GREEN renewal proof tests**

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryRenew(WriteTimeoutIsLeaseLost|PostWriteProofRejectsExpiredLease|PostWriteProofRejectsTinyRemainingLease|UsesLeaseClockForExpiryRefreshAndPostWriteProof|LeaseClockErrorIsTransient|TxnIDMismatch|LeaseExpired|WriteError)' -count=1
```

Expected after implementation: selected tests pass.

- [x] **Step 9: Commit Task 2**

Run:

```bash
git add pkg/objstore/locking.go pkg/objstore/export_test.go pkg/objstore/locking_test.go docs/agents/br/lease-lock-renewal-bounded-write-proof-implementation-plan.md
git commit -m "pkg/objstore: prove renewed lease after bounded write"
```

## Task 3: Schedule Renewal From Proven Remaining Window

**Files:**

- Modify: `pkg/objstore/locking.go`
- Modify: `pkg/objstore/locking_test.go`
- Modify: `docs/agents/br/lease-lock-renewal-bounded-write-proof-implementation-plan.md`

- [ ] **Step 1: Add direct next-delay test**

In `pkg/objstore/locking_test.go`, add:

```go
func TestTryRenewReturnsDelayFromRemainingLease(t *testing.T) {
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	defer objstore.TESTSetLeaseConstants(90*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()
	defer objstore.TESTSetRenewalProofConstants(20*time.Millisecond, time.Millisecond)()

	leaseNow := time.Date(2030, 5, 28, 10, 11, 12, 0, time.UTC)
	renewNow := leaseNow.Add(10 * time.Millisecond)
	clock := &sequenceLeaseClock{
		times: []time.Time{
			leaseNow,
			leaseNow.Add(time.Millisecond),
			renewNow,
			renewNow.Add(75 * time.Millisecond),
		},
	}
	lock, err := objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "owner", clock)
	require.NoError(t, err)

	delay, err := objstore.TESTTryRenewWithDelay(ctx, lock)
	require.NoError(t, err)
	require.Equal(t, 5*time.Millisecond, delay)
}
```

- [ ] **Step 2: Run RED next-delay test**

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryRenewReturnsDelayFromRemainingLease' -count=1
```

Expected before renewal-loop update: compile failure until `TESTTryRenewWithDelay` exists, then pass once Task 2 helper is present. Keep this test as the direct proof for delay calculation.

- [ ] **Step 3: Update renewal loop to use returned delay**

In `renewalLoop`, replace the fixed `time.After(renewInterval)` top-level sleep with a mutable delay. Use this complete shape and preserve the existing log message text:

```go
func (l *RemoteLock) renewalLoop(ctx context.Context, onLeaseLost func()) {
	defer close(l.done)

	invokeLost := func() {
		if onLeaseLost != nil {
			onLeaseLost()
		}
	}

	nextDelay := renewInterval
	for {
		timer := time.NewTimer(nextDelay)
		select {
		case <-l.stopCh:
			timer.Stop()
			return
		case <-timer.C:
		}

		var lastErr error
		renewSucceeded := false
		for attempt := 0; attempt <= renewMaxRetries; attempt++ {
			delay, err := l.tryRenew(ctx)
			if err == nil {
				nextDelay = delay
				renewSucceeded = true
				break
			}
			if isPermanentRenewalLoss(err) {
				log.Warn("Lock renewal detected lease lost; calling onLeaseLost.",
					zap.Stringer("lock", l),
					zap.Int("attempt", attempt),
					logutil.ShortError(err))
				invokeLost()
				return
			}

			lastErr = err
			if attempt == renewMaxRetries {
				break
			}

			retryBackoff := renewBaseBackoff * time.Duration(1<<attempt)
			log.Warn("Lock renewal hit transient error; will retry with exponential backoff.",
				zap.Stringer("lock", l),
				zap.Int("attempt", attempt),
				zap.Duration("backoff", retryBackoff),
				logutil.ShortError(err))
			timer := time.NewTimer(retryBackoff)
			select {
			case <-l.stopCh:
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		if !renewSucceeded {
			log.Warn("Lock renewal retries exhausted; calling onLeaseLost.",
				zap.Stringer("lock", l),
				zap.Int("max_retries", renewMaxRetries),
				logutil.ShortError(lastErr))
			invokeLost()
			return
		}
	}
}
```

- [ ] **Step 4: Add loop-level smoke coverage**

Update `TestStartRenewalRefreshesLeasePeriodically` only if it becomes flaky under remaining-window scheduling. Keep the assertion that `ExpireAt` advances; do not add broad sleeps beyond what existing tests use.

- [ ] **Step 5: Run GREEN renewal scheduling tests**

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryRenewReturnsDelayFromRemainingLease|TestStartRenewalRefreshesLeasePeriodically|TestStartRenewalCallsOnLeaseLostAfterRetryExhaustion|TestLockWithRetryStartsRenewal' -count=1
```

Expected after implementation: selected tests pass.

- [ ] **Step 6: Commit Task 3**

Run:

```bash
git add pkg/objstore/locking.go pkg/objstore/locking_test.go docs/agents/br/lease-lock-renewal-bounded-write-proof-implementation-plan.md
git commit -m "pkg/objstore: schedule renewal from proven lease window"
```

## Task 4: Final Validation and Documentation Record

**Files:**

- Modify: `docs/agents/br/lease-lock-renewal-bounded-write-proof-implementation-plan.md`
- Possibly modify: `pkg/objstore/BUILD.bazel` if `make bazel_prepare` changes metadata

- [ ] **Step 1: Run focused objstore checks**

Run:

```bash
./tools/check/failpoint-go-test.sh pkg/objstore -run 'TestTryRenew|TestStartRenewal|TestLockWithRetryStartsRenewal|TestCleanUpStaleTruncateLock' -count=1
```

Expected: selected tests pass.

- [ ] **Step 2: Run Bazel metadata generation**

Run `make bazel_prepare` because this plan adds new top-level `func TestXxx(t *testing.T)` functions in an existing `*_test.go` file.

```bash
make bazel_prepare
```

Expected: command succeeds. If it changes `pkg/objstore/BUILD.bazel` or related generated metadata, include those changes in the final commit.

- [ ] **Step 3: Run docs and diff hygiene checks**

Run:

```bash
git diff --check
rg -n -P '\x60(MUST(?: NOT)?|SHOULD|MAY)\x60' AGENTS.md docs/agents/agents-review-guide.md docs/agents/br/lease-lock-renewal-bounded-write-proof-design.md docs/agents/br/lease-lock-renewal-bounded-write-proof-implementation-plan.md CONTEXT.md
rg -n 'TO(DO)|TB(D)' docs/agents/br/lease-lock-renewal-bounded-write-proof-design.md docs/agents/br/lease-lock-renewal-bounded-write-proof-implementation-plan.md CONTEXT.md
```

Expected: no output from the `rg` checks and no diff whitespace errors.

- [ ] **Step 4: Run Ready validation**

Because code changes are included, use the `Ready` profile before claiming completion.

Run at least:

```bash
make lint
```

Expected: lint passes. If local environment cannot run lint, record the exact failure and do not claim Ready.

- [ ] **Step 5: Commit final metadata/docs updates**

Run:

```bash
git status --short
git add docs/agents/br/lease-lock-renewal-bounded-write-proof-implementation-plan.md pkg/objstore/BUILD.bazel
git commit -m "docs: record bounded renewal proof validation"
```

If `pkg/objstore/BUILD.bazel` is unchanged, omit it from `git add`.

## Completion Criteria

- Renewal `WriteFile` has a timeout of `min(5min, remainingOldLease / 2)`.
- A successful renewal write is followed by a `LeaseClock` post-write proof.
- Renewal enters lease-lost handling when post-write proof fails or remaining lease is `<= 1min`.
- Successful renewal returns a next delay based on proven remaining lease.
- Stale cleanup uses `ExpireAt + staleReclaimGrace`, where `staleReclaimGrace = 30min`.
- First-stage tests pass through the failpoint test runner.
- `make bazel_prepare` has been run because new top-level Go tests are added.
- Ready validation has been run before reporting completion.
