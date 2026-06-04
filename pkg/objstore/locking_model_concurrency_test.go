// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

const modelOperationGuardTimeout = 250 * time.Millisecond

type interceptedOperation struct {
	Path string
	Err  error
}

type operationBlockingStorage struct {
	storeapi.Storage

	mu           sync.Mutex
	readPath     string
	blockReads   int
	blockedReads int
	readStarted  chan interceptedOperation
	readFinished chan interceptedOperation
	releaseRead  chan struct{}
}

func newOperationBlockingStorage(base storeapi.Storage) *operationBlockingStorage {
	return &operationBlockingStorage{
		Storage:      base,
		readStarted:  make(chan interceptedOperation, 8),
		readFinished: make(chan interceptedOperation, 8),
		releaseRead:  make(chan struct{}),
	}
}

func (s *operationBlockingStorage) blockNextReads(path string, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readPath = path
	s.blockReads = n
}

func (s *operationBlockingStorage) releaseReads() {
	s.mu.Lock()
	old := s.releaseRead
	s.releaseRead = make(chan struct{})
	s.mu.Unlock()
	close(old)
}

func (s *operationBlockingStorage) ReadFile(ctx context.Context, name string) ([]byte, error) {
	var release <-chan struct{}
	s.mu.Lock()
	shouldBlock := name == s.readPath && s.blockReads > 0
	if shouldBlock {
		s.blockReads--
		s.blockedReads++
		release = s.releaseRead
	}
	s.mu.Unlock()
	if !shouldBlock {
		return s.Storage.ReadFile(ctx, name)
	}

	s.readStarted <- interceptedOperation{Path: name}
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-release:
		return s.Storage.ReadFile(ctx, name)
	case <-time.After(modelOperationGuardTimeout):
		err = errors.New("guard timeout waiting for renewal ReadFile context deadline")
	}
	s.readFinished <- interceptedOperation{Path: name, Err: err}
	return nil, err
}

func (s *operationBlockingStorage) blockedReadCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.blockedReads
}

type lateWriteStorage struct {
	storeapi.Storage

	mu              sync.Mutex
	path            string
	blockWrites     int
	writeStarted    chan interceptedOperation
	writeReturned   chan interceptedOperation
	lateCommitted   chan interceptedOperation
	releaseLate     chan struct{}
	capturedPayload []byte
}

func newLateWriteStorage(base storeapi.Storage) *lateWriteStorage {
	return &lateWriteStorage{
		Storage:       base,
		writeStarted:  make(chan interceptedOperation, 8),
		writeReturned: make(chan interceptedOperation, 8),
		lateCommitted: make(chan interceptedOperation, 8),
		releaseLate:   make(chan struct{}),
	}
}

func (s *lateWriteStorage) blockNextWrites(path string, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.path = path
	s.blockWrites = n
}

func (s *lateWriteStorage) releaseLateCommit() {
	s.mu.Lock()
	old := s.releaseLate
	s.releaseLate = make(chan struct{})
	s.mu.Unlock()
	close(old)
}

func (s *lateWriteStorage) capturedMeta(t *testing.T) objstore.LockMeta {
	t.Helper()
	s.mu.Lock()
	data := append([]byte(nil), s.capturedPayload...)
	s.mu.Unlock()
	var meta objstore.LockMeta
	require.NoError(t, json.Unmarshal(data, &meta))
	return meta
}

func (s *lateWriteStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	var release <-chan struct{}
	s.mu.Lock()
	shouldBlock := name == s.path && s.blockWrites > 0
	if shouldBlock {
		s.blockWrites--
		s.capturedPayload = append([]byte(nil), data...)
		release = s.releaseLate
	}
	s.mu.Unlock()
	if !shouldBlock {
		return s.Storage.WriteFile(ctx, name, data)
	}

	s.writeStarted <- interceptedOperation{Path: name}
	<-ctx.Done()
	err := ctx.Err()
	s.writeReturned <- interceptedOperation{Path: name, Err: err}

	go func(payload []byte, release <-chan struct{}) {
		<-release
		commitErr := s.Storage.WriteFile(context.Background(), name, payload)
		s.lateCommitted <- interceptedOperation{Path: name, Err: commitErr}
	}(append([]byte(nil), data...), release)
	return err
}

type recordingWriteStorage struct {
	storeapi.Storage

	mu     sync.Mutex
	path   string
	writes chan objstore.LockMeta
}

func newRecordingWriteStorage(base storeapi.Storage) *recordingWriteStorage {
	return &recordingWriteStorage{
		Storage: base,
		writes:  make(chan objstore.LockMeta, 8),
	}
}

func (s *recordingWriteStorage) recordPath(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.path = path
}

func (s *recordingWriteStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	err := s.Storage.WriteFile(ctx, name, data)
	s.mu.Lock()
	shouldRecord := err == nil && s.path != "" && name == s.path
	s.mu.Unlock()
	if shouldRecord {
		var meta objstore.LockMeta
		if json.Unmarshal(data, &meta) == nil {
			s.writes <- meta
		}
	}
	return err
}

func waitRecordedWrite(t *testing.T, ch <-chan objstore.LockMeta, name string) objstore.LockMeta {
	t.Helper()
	select {
	case meta := <-ch:
		return meta
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
		return objstore.LockMeta{}
	}
}

type blockingLeaseClock struct {
	mu      sync.Mutex
	steps   []blockingLeaseClockStep
	started chan interceptedClockCall
	done    chan interceptedClockCall
}

type blockingLeaseClockStep struct {
	at       time.Time
	err      error
	block    bool
	release  chan struct{}
	label    string
	fallback time.Time
}

type interceptedClockCall struct {
	Label string
	Err   error
}

func newBlockingLeaseClock(steps ...blockingLeaseClockStep) *blockingLeaseClock {
	for i := range steps {
		if steps[i].block && steps[i].release == nil {
			steps[i].release = make(chan struct{})
		}
	}
	return &blockingLeaseClock{
		steps:   steps,
		started: make(chan interceptedClockCall, 16),
		done:    make(chan interceptedClockCall, 16),
	}
}

func clockAt(label string, at time.Time) blockingLeaseClockStep {
	return blockingLeaseClockStep{label: label, at: at}
}

func clockBlock(label string, fallback time.Time) blockingLeaseClockStep {
	return blockingLeaseClockStep{label: label, block: true, release: make(chan struct{}), fallback: fallback}
}

func (c *blockingLeaseClock) releaseAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.steps {
		if c.steps[i].block && c.steps[i].release != nil {
			close(c.steps[i].release)
			c.steps[i].release = nil
		}
	}
}

func (c *blockingLeaseClock) Now(ctx context.Context) (time.Time, error) {
	c.mu.Lock()
	if len(c.steps) == 0 {
		c.mu.Unlock()
		return time.Now(), nil
	}
	step := c.steps[0]
	c.steps = c.steps[1:]
	c.mu.Unlock()

	if step.err != nil {
		c.done <- interceptedClockCall{Label: step.label, Err: step.err}
		return time.Time{}, step.err
	}
	if !step.block {
		c.done <- interceptedClockCall{Label: step.label}
		return step.at, nil
	}

	c.started <- interceptedClockCall{Label: step.label}
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case <-step.release:
		c.done <- interceptedClockCall{Label: step.label}
		return step.fallback, nil
	case <-time.After(modelOperationGuardTimeout):
		err = errors.New("guard timeout waiting for renewal lease-clock context deadline")
	}
	c.done <- interceptedClockCall{Label: step.label, Err: err}
	return time.Time{}, err
}

func waitOperationFinished(t *testing.T, ch <-chan interceptedOperation, name string) interceptedOperation {
	t.Helper()
	select {
	case op := <-ch:
		return op
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
		return interceptedOperation{}
	}
}

func waitClockLabel(t *testing.T, ch <-chan interceptedClockCall, label string) interceptedClockCall {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case call := <-ch:
			if call.Label == label {
				return call
			}
		case <-deadline:
			t.Fatalf("timed out waiting for lease-clock call %s", label)
			return interceptedClockCall{}
		}
	}
}

func requireNoLeaseLostSoon(t *testing.T, lost <-chan struct{}, window time.Duration) {
	t.Helper()
	select {
	case <-lost:
		t.Fatal("lease lost was invoked too early")
	case <-time.After(window):
	}
}

func readLockMeta(t *testing.T, strg storeapi.Storage, path string) objstore.LockMeta {
	t.Helper()
	data, err := strg.ReadFile(context.Background(), path)
	require.NoError(t, err)
	var meta objstore.LockMeta
	require.NoError(t, json.Unmarshal(data, &meta))
	return meta
}

func waitExpireAtAdvanced(t *testing.T, strg storeapi.Storage, path string, original time.Time) objstore.LockMeta {
	t.Helper()
	var meta objstore.LockMeta
	require.Eventually(t, func() bool {
		meta = readLockMeta(t, strg, path)
		return meta.ExpireAt.After(original)
	}, time.Second, 10*time.Millisecond, "expected ExpireAt to advance past %s, got %s", original, meta.ExpireAt)
	return meta
}

func startModelRenewal(t *testing.T, ctx context.Context, lock *objstore.RemoteLock, worker *protectedWorker) {
	t.Helper()
	objstore.TESTStartRenewal(ctx, lock, func() {
		worker.stop(workerStopLeaseLost)
	})
}

func stopModelRenewal(cancel context.CancelFunc, lock *objstore.RemoteLock, release func()) {
	if release != nil {
		release()
	}
	if cancel != nil {
		cancel()
	}
	objstore.TESTStopRenewal(lock)
}

func TestLeaseLockRenewalObservationHangIsTransient(t *testing.T) {
	t.Run("read timeout then retry succeeds", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer objstore.TESTSetLeaseConstants(180*time.Millisecond, 15*time.Millisecond, 5, 5*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(30*time.Millisecond, time.Millisecond)()

		base, _ := createMockStorage(t)
		storage := newOperationBlockingStorage(base)
		lock, err := objstore.TryLockRemoteWrite(parentCtx, storage, "v1/LOCK", "owner", localLeaseClock())
		require.NoError(t, err)
		physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
		original := readLockMeta(t, base, physicalPath).ExpireAt
		storage.blockNextReads(physicalPath, 1)

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, parentCtx, audit, "owner-a", "migration-write", lock.String())
		startModelRenewal(t, parentCtx, lock, worker)
		t.Cleanup(func() {
			stopModelRenewal(cancel, lock, storage.releaseReads)
		})

		_ = waitOperationFinished(t, storage.readStarted, "renewal ReadFile start")
		finished := waitOperationFinished(t, storage.readFinished, "renewal ReadFile finish")
		require.ErrorIs(t, finished.Err, context.DeadlineExceeded)
		requireNoLeaseLostSoon(t, worker.lostCh(), 30*time.Millisecond)

		waitExpireAtAdvanced(t, base, physicalPath, original)
		step, ok := worker.requestStep(t)
		require.True(t, ok)
		require.Positive(t, step)
		require.NoError(t, lock.Unlock(parentCtx))
		worker.stop(workerStopTestStop)
	})

	t.Run("pre-write clock timeout then retry succeeds", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer objstore.TESTSetLeaseConstants(500*time.Millisecond, 20*time.Millisecond, 5, 5*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(40*time.Millisecond, time.Millisecond)()

		base, _ := createMockStorage(t)
		storage := newRecordingWriteStorage(base)
		leaseNow := time.Date(2030, 6, 4, 10, 0, 0, 0, time.UTC)
		clock := newBlockingLeaseClock(
			clockAt("acquire-pre-clock", leaseNow),
			clockAt("acquire-post-clock", leaseNow.Add(time.Millisecond)),
			clockBlock("renewal-pre-clock-timeout", leaseNow.Add(20*time.Millisecond)),
			clockAt("renewal-pre-clock-retry", leaseNow.Add(30*time.Millisecond)),
			clockAt("renewal-post-clock-retry", leaseNow.Add(31*time.Millisecond)),
		)
		lock, err := objstore.TryLockRemoteWrite(parentCtx, storage, "v1/LOCK", "owner", clock)
		require.NoError(t, err)
		physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
		original := readLockMeta(t, base, physicalPath).ExpireAt
		storage.recordPath(physicalPath)

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, parentCtx, audit, "owner-a", "migration-write", lock.String())
		startModelRenewal(t, parentCtx, lock, worker)
		t.Cleanup(func() {
			stopModelRenewal(cancel, lock, clock.releaseAll)
		})

		_ = waitClockLabel(t, clock.started, "renewal-pre-clock-timeout")
		finished := waitClockLabel(t, clock.done, "renewal-pre-clock-timeout")
		require.ErrorIs(t, finished.Err, context.DeadlineExceeded)
		requireNoLeaseLostSoon(t, worker.lostCh(), 30*time.Millisecond)
		_ = waitClockLabel(t, clock.done, "renewal-pre-clock-retry")
		_ = waitClockLabel(t, clock.done, "renewal-post-clock-retry")
		objstore.TESTStopRenewal(lock)

		renewed := waitRecordedWrite(t, storage.writes, "renewal write after pre-clock retry")
		require.True(t, renewed.ExpireAt.After(original), "expected captured renewal ExpireAt %s to advance past %s", renewed.ExpireAt, original)
		step, ok := worker.requestStep(t)
		require.True(t, ok)
		require.Positive(t, step)
		require.NoError(t, lock.Unlock(parentCtx))
		worker.stop(workerStopTestStop)
	})

	t.Run("repeated observation failures exhaust proven window", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer objstore.TESTSetLeaseConstants(700*time.Millisecond, 10*time.Millisecond, 8, 100*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(50*time.Millisecond, 30*time.Millisecond)()

		base, _ := createMockStorage(t)
		storage := newOperationBlockingStorage(base)
		lock, err := objstore.TryLockRemoteWrite(parentCtx, storage, "v1/LOCK", "owner", localLeaseClock())
		require.NoError(t, err)
		physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
		storage.blockNextReads(physicalPath, 8)

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, parentCtx, audit, "owner-a", "migration-write", lock.String())
		startModelRenewal(t, parentCtx, lock, worker)
		t.Cleanup(func() {
			stopModelRenewal(cancel, lock, storage.releaseReads)
		})

		_ = waitOperationFinished(t, storage.readStarted, "first renewal ReadFile start")
		firstFailureAt := time.Now()
		_ = waitOperationFinished(t, storage.readFinished, "first renewal ReadFile finish")
		_ = waitOperationFinished(t, storage.readStarted, "retry renewal ReadFile start")

		waitClosed(t, worker.lostCh(), "lease lost after proven window exhaustion")
		require.Less(t, storage.blockedReadCount(), 8, "loss must happen before max retry exhaustion")
		require.Greater(t, time.Since(firstFailureAt), 200*time.Millisecond)
		_, ok := worker.requestStep(t)
		require.False(t, ok)
		requireNoStepAfterAction(t, audit.snapshot(), "owner-a", "lost")
	})

	t.Run("observation hang before detecting hijack", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer objstore.TESTSetLeaseConstants(100*time.Millisecond, 10*time.Millisecond, 8, 20*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(20*time.Millisecond, time.Millisecond)()

		base, _ := createMockStorage(t)
		storage := newOperationBlockingStorage(base)
		lock, err := objstore.TryLockRemoteWrite(parentCtx, storage, "v1/LOCK", "owner-a", localLeaseClock())
		require.NoError(t, err)
		physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
		storage.blockNextReads(physicalPath, 1)

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, parentCtx, audit, "owner-a", "migration-write", lock.String())
		startModelRenewal(t, parentCtx, lock, worker)
		t.Cleanup(func() {
			stopModelRenewal(cancel, lock, storage.releaseReads)
		})

		_ = waitOperationFinished(t, storage.readStarted, "renewal ReadFile start")
		hijacked := objstore.MakeLockMeta("hijacker")
		hijacked.TxnID = []byte{99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99}
		hijackedData, err := json.Marshal(hijacked)
		require.NoError(t, err)
		require.NoError(t, base.WriteFile(parentCtx, physicalPath, hijackedData))

		storage.releaseReads()
		waitClosed(t, worker.lostCh(), "lease lost after hijack")
		_, ok := worker.requestStep(t)
		require.False(t, ok)
		requireNoStepAfterAction(t, audit.snapshot(), "owner-a", "lost")
	})
}

func TestLeaseLockRenewalAmbiguousWriteAndProofFailureStopProtectedWork(t *testing.T) {
	t.Run("write timeout stops worker even if write lands late", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer objstore.TESTSetLeaseConstants(200*time.Millisecond, 15*time.Millisecond, 5, 5*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(30*time.Millisecond, time.Millisecond)()

		base, _ := createMockStorage(t)
		storage := newLateWriteStorage(base)
		lock, err := objstore.TryLockRemoteWrite(parentCtx, storage, "v1/LOCK", "owner", localLeaseClock())
		require.NoError(t, err)
		physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
		original := readLockMeta(t, base, physicalPath).ExpireAt
		storage.blockNextWrites(physicalPath, 1)

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, parentCtx, audit, "owner-a", "migration-write", lock.String())
		startModelRenewal(t, parentCtx, lock, worker)
		t.Cleanup(func() {
			stopModelRenewal(cancel, lock, storage.releaseLateCommit)
		})

		_ = waitOperationFinished(t, storage.writeStarted, "renewal WriteFile start")
		returned := waitOperationFinished(t, storage.writeReturned, "renewal WriteFile return")
		require.ErrorIs(t, returned.Err, context.DeadlineExceeded)
		waitClosed(t, worker.lostCh(), "lease lost after ambiguous write timeout")
		worker.waitStopped(t)

		captured := storage.capturedMeta(t)
		require.True(t, captured.ExpireAt.After(original))
		storage.releaseLateCommit()
		committed := waitOperationFinished(t, storage.lateCommitted, "late renewal commit")
		require.NoError(t, committed.Err)
		require.Equal(t, captured.ExpireAt, readLockMeta(t, base, physicalPath).ExpireAt)
		_, ok := worker.requestStep(t)
		require.False(t, ok)
		requireNoStepAfterAction(t, audit.snapshot(), "owner-a", "lost")
	})

	t.Run("ordinary write error remains transient", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer objstore.TESTSetLeaseConstants(180*time.Millisecond, 15*time.Millisecond, 5, 5*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(30*time.Millisecond, time.Millisecond)()

		base, _ := createMockStorage(t)
		failOnce := &ordinaryWriteErrorOnceStorage{
			Storage: base,
			err:     errors.New("ordinary renewal write error"),
			failed:  make(chan struct{}, 1),
		}
		lock, err := objstore.TryLockRemoteWrite(parentCtx, failOnce, "v1/LOCK", "owner", localLeaseClock())
		require.NoError(t, err)
		physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
		original := readLockMeta(t, base, physicalPath).ExpireAt
		failOnce.path = physicalPath

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, parentCtx, audit, "owner-a", "migration-write", lock.String())
		startModelRenewal(t, parentCtx, lock, worker)
		t.Cleanup(func() {
			stopModelRenewal(cancel, lock, nil)
		})

		waitClosed(t, failOnce.failed, "ordinary renewal write failure")
		requireNoLeaseLostSoon(t, worker.lostCh(), 30*time.Millisecond)
		waitExpireAtAdvanced(t, base, physicalPath, original)
		select {
		case <-worker.lostCh():
			t.Fatal("ordinary write failure should not stop protected work after retry success")
		default:
		}
		step, ok := worker.requestStep(t)
		require.True(t, ok)
		require.Positive(t, step)
		require.NoError(t, lock.Unlock(parentCtx))
		worker.stop(workerStopTestStop)
	})

	t.Run("post-write proof clock hang stops worker", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer objstore.TESTSetLeaseConstants(200*time.Millisecond, 15*time.Millisecond, 5, 5*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(30*time.Millisecond, time.Millisecond)()

		base, _ := createMockStorage(t)
		leaseNow := time.Date(2030, 6, 4, 11, 0, 0, 0, time.UTC)
		clock := newBlockingLeaseClock(
			clockAt("acquire-pre-clock", leaseNow),
			clockAt("acquire-post-clock", leaseNow.Add(time.Millisecond)),
			clockAt("renewal-pre-clock", leaseNow.Add(20*time.Millisecond)),
			clockBlock("renewal-post-clock-timeout", leaseNow.Add(21*time.Millisecond)),
		)
		lock, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "owner", clock)
		require.NoError(t, err)

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, parentCtx, audit, "owner-a", "migration-write", lock.String())
		startModelRenewal(t, parentCtx, lock, worker)
		t.Cleanup(func() {
			stopModelRenewal(cancel, lock, clock.releaseAll)
		})

		_ = waitClockLabel(t, clock.started, "renewal-post-clock-timeout")
		finished := waitClockLabel(t, clock.done, "renewal-post-clock-timeout")
		require.ErrorIs(t, finished.Err, context.DeadlineExceeded)
		waitClosed(t, worker.lostCh(), "lease lost after post-write proof clock timeout")
		_, ok := worker.requestStep(t)
		require.False(t, ok)
		requireNoStepAfterAction(t, audit.snapshot(), "owner-a", "lost")
	})

	t.Run("post-write proof says lease already unsafe", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer objstore.TESTSetLeaseConstants(200*time.Millisecond, 15*time.Millisecond, 5, 5*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(30*time.Millisecond, time.Millisecond)()

		base, _ := createMockStorage(t)
		leaseNow := time.Date(2030, 6, 4, 12, 0, 0, 0, time.UTC)
		clock := newBlockingLeaseClock(
			clockAt("acquire-pre-clock", leaseNow),
			clockAt("acquire-post-clock", leaseNow.Add(time.Millisecond)),
			clockAt("renewal-pre-clock", leaseNow.Add(20*time.Millisecond)),
			clockAt("renewal-post-clock-expired", leaseNow.Add(objstore.LeaseTTL+time.Second)),
		)
		lock, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "owner", clock)
		require.NoError(t, err)

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, parentCtx, audit, "owner-a", "migration-write", lock.String())
		startModelRenewal(t, parentCtx, lock, worker)
		t.Cleanup(func() {
			stopModelRenewal(cancel, lock, nil)
		})

		waitClosed(t, worker.lostCh(), "lease lost after expired post-write proof")
		_, ok := worker.requestStep(t)
		require.False(t, ok)
		requireNoStepAfterAction(t, audit.snapshot(), "owner-a", "lost")
	})
}

type ordinaryWriteErrorOnceStorage struct {
	storeapi.Storage

	mu       sync.Mutex
	path     string
	err      error
	failed   chan struct{}
	failOnce sync.Once
}

func (s *ordinaryWriteErrorOnceStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	s.mu.Lock()
	shouldFail := name == s.path && s.path != ""
	if shouldFail {
		s.path = ""
	}
	s.mu.Unlock()
	if shouldFail {
		s.failOnce.Do(func() { close(s.failed) })
		return s.err
	}
	return s.Storage.WriteFile(ctx, name, data)
}
