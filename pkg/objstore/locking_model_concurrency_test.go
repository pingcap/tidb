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
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

const modelOperationGuardTimeout = 250 * time.Millisecond

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
	if err == nil && !strings.Contains(name, ".INTENT.") {
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

func requireSinglePathWithPrefixExcept(t *testing.T, storage storeapi.Storage, prefix string, excluded ...string) string {
	t.Helper()
	excludedSet := make(map[string]struct{}, len(excluded))
	for _, path := range excluded {
		excludedSet[path] = struct{}{}
	}
	paths := requireListedPathsWithPrefix(t, storage, prefix)
	matches := make([]string, 0, len(paths))
	for _, path := range paths {
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

type interceptedOperation struct {
	Path string
	Err  error
}

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

type intentWriteBarrierStorage struct {
	storeapi.Storage

	mu       sync.Mutex
	blocked  int
	started  chan interceptedOperation
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

func requireIntentStarted(t *testing.T, ch <-chan interceptedOperation) interceptedOperation {
	t.Helper()
	op := waitOperationFinished(t, ch, "intent WriteFile start")
	require.Contains(t, op.Path, ".INTENT.")
	return op
}

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

type contextDoneWriteStorage struct {
	storeapi.Storage

	mu                    sync.Mutex
	path                  string
	blockWrites           int
	pauseAfterContextDone bool
	writeExited           bool
	deleteBeforeWriteExit bool
	writeStarted          chan interceptedOperation
	contextDone           chan interceptedOperation
	writeReturned         chan interceptedOperation
	releaseReturn         chan struct{}
	releaseOnce           sync.Once
}

func newContextDoneWriteStorage(base storeapi.Storage) *contextDoneWriteStorage {
	return &contextDoneWriteStorage{
		Storage:       base,
		writeStarted:  make(chan interceptedOperation, 8),
		contextDone:   make(chan interceptedOperation, 8),
		writeReturned: make(chan interceptedOperation, 8),
		releaseReturn: make(chan struct{}),
	}
}

func (s *contextDoneWriteStorage) blockNextWriteUntilContextDone(path string, pauseAfterContextDone bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.path = path
	s.blockWrites = 1
	s.pauseAfterContextDone = pauseAfterContextDone
}

func (s *contextDoneWriteStorage) releaseWriteReturn() {
	s.releaseOnce.Do(func() {
		close(s.releaseReturn)
	})
}

func (s *contextDoneWriteStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	s.mu.Lock()
	shouldBlock := name == s.path && s.blockWrites > 0
	if shouldBlock {
		s.blockWrites--
	}
	pauseAfterContextDone := s.pauseAfterContextDone
	s.mu.Unlock()
	if !shouldBlock {
		return s.Storage.WriteFile(ctx, name, data)
	}

	s.writeStarted <- interceptedOperation{Path: name}
	<-ctx.Done()
	err := ctx.Err()
	s.contextDone <- interceptedOperation{Path: name, Err: err}
	if pauseAfterContextDone {
		select {
		case <-s.releaseReturn:
		case <-time.After(time.Second):
			err = errors.New("guard timeout waiting to release context-done WriteFile")
		}
	}
	s.mu.Lock()
	s.writeExited = true
	s.mu.Unlock()
	s.writeReturned <- interceptedOperation{Path: name, Err: err}
	return err
}

func (s *contextDoneWriteStorage) DeleteFile(ctx context.Context, name string) error {
	s.mu.Lock()
	trackDelete := name == s.path && s.path != ""
	if trackDelete && !s.writeExited {
		s.deleteBeforeWriteExit = true
	}
	s.mu.Unlock()
	return s.Storage.DeleteFile(ctx, name)
}

func (s *contextDoneWriteStorage) requireNoDeleteBeforeWriteExit(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	require.False(t, s.deleteBeforeWriteExit, "DeleteFile for %s happened before blocked renewal WriteFile exited", s.path)
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

type fixedLeaseClock struct {
	now time.Time
}

func (c fixedLeaseClock) Now(context.Context) (time.Time, error) {
	return c.now, nil
}

func barrierProtectedStep(w *protectedWorker, release <-chan struct{}) <-chan workerStepResult {
	resultCh := make(chan workerStepResult, 1)
	go func() {
		select {
		case <-release:
		case <-w.terminating:
			resultCh <- workerStepResult{ok: false}
			return
		case <-w.stopped:
			resultCh <- workerStepResult{ok: false}
			return
		}
		reply := make(chan workerStepResult, 1)
		select {
		case <-w.terminating:
			resultCh <- workerStepResult{ok: false}
			return
		case <-w.stopped:
			resultCh <- workerStepResult{ok: false}
			return
		case w.stepReq <- reply:
		}
		select {
		case result := <-reply:
			resultCh <- result
		case <-w.terminating:
			resultCh <- workerStepResult{ok: false}
		case <-w.stopped:
			resultCh <- workerStepResult{ok: false}
		}
	}()
	return resultCh
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

//revive:disable-next-line:context-as-argument
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

func startTerminalRenewal(ctx context.Context, lock *objstore.RemoteLock, recorder *terminalRecorder, worker *protectedWorker) {
	objstore.TESTStartRenewal(ctx, lock, func() {
		if recorder.record(terminalLeaseLost) {
			worker.stop(workerStopLeaseLost)
		}
	})
}

type lockContenderResult struct {
	ownerID string
	lock    *objstore.RemoteLock
	err     error
}

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

func waitLockContenderResult(t *testing.T, ch <-chan lockContenderResult, name string, timeout time.Duration) lockContenderResult {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for lock contender %s", name)
		return lockContenderResult{}
	}
}

func TestLeaseLockCleanupDoesNotDeleteReacquiredInstance(t *testing.T) {
	t.Run("migration write", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		defer objstore.TESTSetStaleReclaimGrace(30 * time.Millisecond)()

		base, pth := createMockStorage(t)
		now := time.Now()
		stalePath := "v1/LOCK.WRIT.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		writeLockMeta(t, base, stalePath, objstore.LockMeta{
			LockedAt: now.Add(-time.Hour),
			ExpireAt: now.Add(-time.Hour),
			TxnID:    []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			Hint:     "old-stale",
		})

		storage := newCleanupDeleteBarrierStorage(base, stalePath, cleanupDeleteBarrierAfter)
		cleanupCtx, cleanupCancel := context.WithCancel(parentCtx)
		defer cleanupCancel()
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

		liveLock, err := objstore.TryLockRemoteWrite(parentCtx, storage, "v1/LOCK", "new-holder", localLeaseClock())
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
		released := waitOperationFinished(t, storage.released, "cleanup DeleteFile release")
		require.True(t, released.Err == nil || errors.Is(released.Err, context.Canceled), "unexpected cleanup release error: %v", released.Err)
		select {
		case <-cleanupDone:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for cleanup contender to exit")
		}

		require.Equal(t, liveMeta.TxnID, readLockMeta(t, base, livePath).TxnID)
		requireNoDeleteCallForPath(t, storage.deleteCallSnapshot(), livePath)
		requireFileExists(t, filepath.Join(pth, livePath))
		step, ok = worker.requestStep(t)
		require.True(t, ok)
		require.Positive(t, step)

		require.NoError(t, liveLock.Unlock(parentCtx))
		worker.stop(workerStopTestStop)
		requireFileNotExists(t, filepath.Join(pth, livePath))
		next, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "next-holder", localLeaseClock())
		require.NoError(t, err)
		require.NoError(t, next.Unlock(parentCtx))
		requireNoIntentWithPrefix(t, base, "v1/LOCK.READ.")
		requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")
	})

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
		require.NotEqual(t, stalePath, livePath)
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
}

func TestLeaseLockContenderRetryCleanupInterleaving(t *testing.T) {
	t.Run("migration write contenders", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		defer objstore.TESTSetStaleReclaimGrace(30 * time.Millisecond)()

		base, pth := createMockStorage(t)
		now := time.Now()
		stalePath := "v1/LOCK.WRIT.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		writeLockMeta(t, base, stalePath, objstore.LockMeta{
			LockedAt: now.Add(-time.Hour),
			ExpireAt: now.Add(-time.Hour),
			TxnID:    []byte{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			Hint:     "old-stale",
		})

		storage := newCleanupDeleteBarrierStorage(base, stalePath, cleanupDeleteBarrierBefore)
		audit := newCriticalSectionAudit(t.Name())
		cancelContender1, contender1Results := startLockWithRetryContender(
			parentCtx, storage, objstore.TryLockRemoteWrite, "v1/LOCK", "contender-1", "migration-write", audit,
		)
		defer cancelContender1()
		requireDeleteStarted(t, storage.started, stalePath)
		requireFileExists(t, filepath.Join(pth, stalePath))

		cancelContender2, contender2Results := startLockWithRetryContender(
			parentCtx, storage, objstore.TryLockRemoteWrite, "v1/LOCK", "contender-2", "migration-write", audit,
		)
		defer cancelContender2()
		requireDeleteStarted(t, storage.started, stalePath)

		storage.releaseDelete()
		released1 := waitOperationFinished(t, storage.released, "first cleanup DeleteFile release")
		released2 := waitOperationFinished(t, storage.released, "second cleanup DeleteFile release")
		require.Equal(t, stalePath, released1.Path)
		require.Equal(t, stalePath, released2.Path)
		requireFileNotExists(t, filepath.Join(pth, stalePath))
		require.Equal(t, 2, storage.blockedDeleteCount())

		var winner lockContenderResult
		var loserCancel context.CancelFunc
		var loserResults <-chan lockContenderResult
		select {
		case result := <-contender1Results:
			winner = result
			loserCancel = cancelContender2
			loserResults = contender2Results
		case result := <-contender2Results:
			winner = result
			loserCancel = cancelContender1
			loserResults = contender1Results
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for first lock contender success")
		}
		require.NoError(t, winner.err)
		require.NotNil(t, winner.lock)
		loserCancel()
		loser := waitLockContenderResult(t, loserResults, "loser cancellation", time.Second)
		require.Error(t, loser.err)
		require.Nil(t, loser.lock)

		events := audit.snapshot()
		require.Len(t, events, 2)
		require.Equal(t, winner.ownerID, events[0].OwnerID)
		require.Equal(t, "enter", events[0].Action)
		require.Equal(t, winner.ownerID, events[1].OwnerID)
		require.Equal(t, "step", events[1].Action)

		winnerPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
		requireFileExists(t, filepath.Join(pth, winnerPath))
		third, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "third-holder", localLeaseClock())
		require.ErrorContains(t, err, "conflict file")
		require.Nil(t, third)
		require.NoError(t, winner.lock.Unlock(parentCtx))
		next, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "next-holder", localLeaseClock())
		require.NoError(t, err)
		require.NoError(t, next.Unlock(parentCtx))
		requireNoIntentWithPrefix(t, base, "v1/LOCK.READ.")
		requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")
		requireFileNotExists(t, filepath.Join(pth, stalePath))
	})

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
}

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
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, lock)
		requireFileExists(t, filepath.Join(pth, intentPath))

		require.NoError(t, base.DeleteFile(parentCtx, intentPath))
		next, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "next-writer", localLeaseClock())
		require.NoError(t, err)
		require.NoError(t, next.Unlock(parentCtx))
	})
}

func TestLeaseLockProtectedWorkDeathAndHangSemantics(t *testing.T) {
	t.Run("dead holder is reclaimed only after stale boundary", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		defer objstore.TESTSetStaleReclaimGrace(30 * time.Millisecond)()
		defer objstore.TESTSetLeaseConstants(100*time.Millisecond, 30*time.Millisecond, 3, 5*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(30*time.Millisecond, time.Millisecond)()

		base, pth := createMockStorage(t)
		leaseStart := time.Date(2030, 6, 4, 15, 0, 0, 0, time.UTC)
		deadLock, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "dead-holder", fixedLeaseClock{now: leaseStart})
		require.NoError(t, err)
		require.NotNil(t, deadLock)
		deadPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
		deadMeta := readLockMeta(t, base, deadPath)

		beforeBoundary := deadMeta.ExpireAt.Add(15 * time.Millisecond)
		earlyCtx, earlyCancel := context.WithTimeout(parentCtx, 50*time.Millisecond)
		defer earlyCancel()
		early, err := objstore.LockWithRetry(earlyCtx, objstore.TryLockRemoteWrite, base, "v1/LOCK", "early-contender", func() {}, fixedLeaseClock{now: beforeBoundary})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, early)
		requireFileExists(t, filepath.Join(pth, deadPath))
		require.Equal(t, deadMeta.TxnID, readLockMeta(t, base, deadPath).TxnID)

		afterBoundary := deadMeta.ExpireAt.Add(31 * time.Millisecond)
		reclaimCtx, reclaimCancel := context.WithTimeout(parentCtx, 10*time.Second)
		defer reclaimCancel()
		reclaimed, err := objstore.LockWithRetry(reclaimCtx, objstore.TryLockRemoteWrite, base, "v1/LOCK", "reclaim-contender", func() {}, fixedLeaseClock{now: afterBoundary})
		require.NoError(t, err)
		require.NotNil(t, reclaimed)
		reclaimedPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
		require.NotEqual(t, deadPath, reclaimedPath)
		requireFileNotExists(t, filepath.Join(pth, deadPath))
		requireFileExists(t, filepath.Join(pth, reclaimedPath))

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, parentCtx, audit, "owner-b", "migration-write", reclaimed.String())
		step, ok := worker.requestStep(t)
		require.True(t, ok)
		require.Positive(t, step)
		require.NoError(t, reclaimed.Unlock(parentCtx))
		worker.stop(workerStopTestStop)
		requireNoIntentWithPrefix(t, base, "v1/LOCK.READ.")
		requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")
	})

	t.Run("hung protected work stops after renewal lost", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer objstore.TESTSetLeaseConstants(200*time.Millisecond, 15*time.Millisecond, 5, 5*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(30*time.Millisecond, time.Millisecond)()

		base, _ := createMockStorage(t)
		storage := newLateWriteStorage(base)
		lock, err := objstore.TryLockRemoteWrite(parentCtx, storage, "v1/LOCK", "owner-a", localLeaseClock())
		require.NoError(t, err)
		physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
		storage.blockNextWrites(physicalPath, 1)

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, parentCtx, audit, "owner-a", "migration-write", lock.String())
		releaseBusinessStep := make(chan struct{})
		businessStep := barrierProtectedStep(worker, releaseBusinessStep)

		startModelRenewal(t, parentCtx, lock, worker)
		t.Cleanup(func() {
			stopModelRenewal(cancel, lock, storage.releaseLateCommit)
		})

		_ = waitOperationFinished(t, storage.writeStarted, "renewal WriteFile start")
		returned := waitOperationFinished(t, storage.writeReturned, "renewal WriteFile return")
		require.ErrorIs(t, returned.Err, context.DeadlineExceeded)
		lateCommitReleased := false
		releaseAndWaitLateCommit := func() {
			if lateCommitReleased {
				return
			}
			lateCommitReleased = true
			storage.releaseLateCommit()
			committed := waitOperationFinished(t, storage.lateCommitted, "late renewal commit")
			require.Equal(t, physicalPath, committed.Path)
			require.NoError(t, committed.Err)
		}
		t.Cleanup(releaseAndWaitLateCommit)
		waitClosed(t, worker.lostCh(), "lease lost after ambiguous renewal write")

		close(releaseBusinessStep)
		select {
		case result := <-businessStep:
			require.False(t, result.ok)
			require.Zero(t, result.step)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for hung business operation to observe lease loss")
		}
		requireNoStepAfterAction(t, audit.snapshot(), "owner-a", "lost")
		releaseAndWaitLateCommit()
	})
}

func TestLeaseLockTerminalReasonStableAfterLeaseLost(t *testing.T) {
	t.Run("txn id mismatch then unlock", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer objstore.TESTSetLeaseConstants(200*time.Millisecond, 15*time.Millisecond, 5, 5*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(30*time.Millisecond, time.Millisecond)()

		base, _ := createMockStorage(t)
		deleteRecorder := newDeleteRecordingStorage(base)
		storage := newOperationBlockingStorage(deleteRecorder)
		lock, err := objstore.TryLockRemoteWrite(parentCtx, storage, "v1/LOCK", "owner", localLeaseClock())
		require.NoError(t, err)
		physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
		storage.blockNextReads(physicalPath, 1)

		// This is a defensive same-file corruption/hijack check. A real
		// reclaim/re-acquire writes a new generated lock path instead.
		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, parentCtx, audit, "owner-a", "migration-write", lock.String())
		step, ok := worker.requestStep(t)
		require.True(t, ok)
		require.Positive(t, step)
		recorder := newTerminalRecorder()
		startTerminalRenewal(parentCtx, lock, recorder, worker)
		t.Cleanup(func() {
			stopModelRenewal(cancel, lock, nil)
		})

		_ = waitOperationFinished(t, storage.readStarted, "renewal ReadFile start")
		hijackerTxn := []byte{99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99, 99}
		hijacked := objstore.MakeLockMeta("hijacker")
		hijacked.TxnID = hijackerTxn
		hijackedData, err := json.Marshal(hijacked)
		require.NoError(t, err)
		require.NoError(t, base.WriteFile(parentCtx, physicalPath, hijackedData))
		storage.releaseReads()

		require.Equal(t, terminalLeaseLost, waitTerminalReason(t, recorder, "txn id mismatch"))
		unlockErr := lock.Unlock(parentCtx)
		require.ErrorContains(t, unlockErr, "Txn ID mismatch")
		require.False(t, recorder.record(terminalNormalUnlock))
		require.Equal(t, terminalLeaseLost, recorder.reasonNow())
		require.Equal(t, workerStopLeaseLost, worker.stopReason())
		_, ok = worker.requestStep(t)
		require.False(t, ok)
		requireNoStepAfterAction(t, audit.snapshot(), "owner-a", "lost")
		require.Equal(t, hijackerTxn, readLockMeta(t, base, physicalPath).TxnID)
		requireNoDeleteSoon(t, deleteRecorder.deleted, 30*time.Millisecond)
		requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")
	})

	t.Run("write timeout late commit then unlock", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer objstore.TESTSetLeaseConstants(200*time.Millisecond, 15*time.Millisecond, 5, 5*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(30*time.Millisecond, time.Millisecond)()

		base, pth := createMockStorage(t)
		deleteRecorder := newDeleteRecordingStorage(base)
		storage := newLateWriteStorage(deleteRecorder)
		lock, err := objstore.TryLockRemoteWrite(parentCtx, storage, "v1/LOCK", "owner", localLeaseClock())
		require.NoError(t, err)
		physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
		original := readLockMeta(t, base, physicalPath).ExpireAt
		storage.blockNextWrites(physicalPath, 1)

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, parentCtx, audit, "owner-a", "migration-write", lock.String())
		step, ok := worker.requestStep(t)
		require.True(t, ok)
		require.Positive(t, step)
		recorder := newTerminalRecorder()
		startTerminalRenewal(parentCtx, lock, recorder, worker)
		t.Cleanup(func() {
			stopModelRenewal(cancel, lock, storage.releaseLateCommit)
		})

		_ = waitOperationFinished(t, storage.writeStarted, "renewal WriteFile start")
		returned := waitOperationFinished(t, storage.writeReturned, "renewal WriteFile return")
		require.ErrorIs(t, returned.Err, context.DeadlineExceeded)
		require.Equal(t, terminalLeaseLost, waitTerminalReason(t, recorder, "ambiguous renewal write"))
		worker.waitStopped(t)

		captured := storage.capturedMeta(t)
		require.True(t, captured.ExpireAt.After(original))
		storage.releaseLateCommit()
		committed := waitOperationFinished(t, storage.lateCommitted, "late renewal commit")
		require.NoError(t, committed.Err)
		require.Equal(t, captured.ExpireAt, readLockMeta(t, base, physicalPath).ExpireAt)

		require.NoError(t, lock.Unlock(parentCtx))
		requireDeletedPath(t, deleteRecorder.deleted, physicalPath)
		requireNoDeleteSoon(t, deleteRecorder.deleted, 30*time.Millisecond)
		requireFileNotExists(t, filepath.Join(pth, physicalPath))
		require.False(t, recorder.record(terminalNormalUnlock))
		require.Equal(t, terminalLeaseLost, recorder.reasonNow())
		_, ok = worker.requestStep(t)
		require.False(t, ok)
		requireNoStepAfterAction(t, audit.snapshot(), "owner-a", "lost")

		next, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "next-owner", localLeaseClock())
		require.NoError(t, err)
		require.NoError(t, next.Unlock(parentCtx))
		requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")
	})

	t.Run("post write proof failed then unlock", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer objstore.TESTSetLeaseConstants(200*time.Millisecond, 15*time.Millisecond, 5, 5*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(30*time.Millisecond, time.Millisecond)()

		base, pth := createMockStorage(t)
		storage := newDeleteRecordingStorage(base)
		leaseNow := time.Date(2030, 6, 4, 13, 0, 0, 0, time.UTC)
		clock := newBlockingLeaseClock(
			clockAt("acquire-pre-clock", leaseNow),
			clockAt("acquire-post-clock", leaseNow.Add(time.Millisecond)),
			clockAt("renewal-pre-clock", leaseNow.Add(20*time.Millisecond)),
			clockAt("renewal-post-clock-expired", leaseNow.Add(objstore.LeaseTTL+time.Second)),
		)
		lock, err := objstore.TryLockRemoteWrite(parentCtx, storage, "v1/LOCK", "owner", clock)
		require.NoError(t, err)
		physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, parentCtx, audit, "owner-a", "migration-write", lock.String())
		step, ok := worker.requestStep(t)
		require.True(t, ok)
		require.Positive(t, step)
		recorder := newTerminalRecorder()
		startTerminalRenewal(parentCtx, lock, recorder, worker)
		t.Cleanup(func() {
			stopModelRenewal(cancel, lock, nil)
		})

		require.Equal(t, terminalLeaseLost, waitTerminalReason(t, recorder, "post-write proof failed"))
		require.NoError(t, lock.Unlock(parentCtx))
		requireDeletedPath(t, storage.deleted, physicalPath)
		requireNoDeleteSoon(t, storage.deleted, 30*time.Millisecond)
		requireFileNotExists(t, filepath.Join(pth, physicalPath))
		require.False(t, recorder.record(terminalNormalUnlock))
		require.Equal(t, terminalLeaseLost, recorder.reasonNow())
		_, ok = worker.requestStep(t)
		require.False(t, ok)
		requireNoStepAfterAction(t, audit.snapshot(), "owner-a", "lost")

		next, err := objstore.TryLockRemoteWrite(parentCtx, base, "v1/LOCK", "next-owner", localLeaseClock())
		require.NoError(t, err)
		require.NoError(t, next.Unlock(parentCtx))
		requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")
	})

	t.Run("write timeout remains lease lost when unlock closes stop signal", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer objstore.TESTSetLeaseConstants(200*time.Millisecond, 15*time.Millisecond, 5, 5*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(30*time.Millisecond, time.Millisecond)()

		base, pth := createMockStorage(t)
		deleteRecorder := newDeleteRecordingStorage(base)
		storage := newContextDoneWriteStorage(deleteRecorder)
		lock, err := objstore.TryLockRemoteWrite(parentCtx, storage, "v1/LOCK", "owner", localLeaseClock())
		require.NoError(t, err)
		physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
		storage.blockNextWriteUntilContextDone(physicalPath, true)

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, parentCtx, audit, "owner-a", "migration-write", lock.String())
		step, ok := worker.requestStep(t)
		require.True(t, ok)
		require.Positive(t, step)
		recorder := newTerminalRecorder()
		startTerminalRenewal(parentCtx, lock, recorder, worker)
		t.Cleanup(func() {
			stopModelRenewal(cancel, lock, storage.releaseWriteReturn)
		})

		_ = waitOperationFinished(t, storage.writeStarted, "renewal WriteFile start")
		deadline := waitOperationFinished(t, storage.contextDone, "renewal WriteFile deadline")
		require.ErrorIs(t, deadline.Err, context.DeadlineExceeded)

		unlockDone := make(chan error, 1)
		go func() {
			unlockDone <- lock.Unlock(parentCtx)
		}()
		require.Eventually(t, func() bool {
			return objstore.TESTRenewalStopSignalClosed(lock)
		}, time.Second, time.Millisecond, "Unlock should close renewal stop signal before releasing timed-out WriteFile")
		storage.releaseWriteReturn()
		returned := waitOperationFinished(t, storage.writeReturned, "renewal WriteFile return")
		require.ErrorIs(t, returned.Err, context.DeadlineExceeded)
		require.Equal(t, terminalLeaseLost, waitTerminalReason(t, recorder, "write timeout after unlock stop signal"))
		select {
		case err := <-unlockDone:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for Unlock after permanent renewal timeout")
		}

		requireDeletedPath(t, deleteRecorder.deleted, physicalPath)
		requireNoDeleteSoon(t, deleteRecorder.deleted, 30*time.Millisecond)
		requireFileNotExists(t, filepath.Join(pth, physicalPath))
		require.False(t, recorder.record(terminalNormalUnlock))
		require.Equal(t, terminalLeaseLost, recorder.reasonNow())
		require.Equal(t, workerStopLeaseLost, worker.stopReason())
		_, ok = worker.requestStep(t)
		require.False(t, ok)
		requireNoStepAfterAction(t, audit.snapshot(), "owner-a", "lost")
		requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")
	})
}

func TestLeaseLockNormalUnlockWinsInFlightRenewal(t *testing.T) {
	t.Run("write cancellation", func(t *testing.T) {
		ctx := context.Background()
		defer objstore.TESTSetLeaseConstants(2*time.Second, 10*time.Millisecond, 3, 5*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(1500*time.Millisecond, time.Millisecond)()

		base, pth := createMockStorage(t)
		wrapped := newContextDoneWriteStorage(base)
		lock, err := objstore.TryLockRemoteWrite(ctx, wrapped, "v1/LOCK", "owner", localLeaseClock())
		require.NoError(t, err)
		physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
		wrapped.blockNextWriteUntilContextDone(physicalPath, false)

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, ctx, audit, "owner-a", "migration-write", lock.String())
		step, ok := worker.requestStep(t)
		require.True(t, ok)
		require.Positive(t, step)
		recorder := newTerminalRecorder()
		leaseLostCalled := make(chan struct{}, 1)
		objstore.TESTStartRenewal(ctx, lock, func() {
			select {
			case leaseLostCalled <- struct{}{}:
			default:
			}
			if recorder.record(terminalLeaseLost) {
				worker.stop(workerStopLeaseLost)
			}
		})
		t.Cleanup(func() {
			objstore.TESTStopRenewal(lock)
		})

		_ = waitOperationFinished(t, wrapped.writeStarted, "renewal write started")
		require.True(t, recorder.record(terminalNormalUnlock))
		worker.stop(workerStopTestStop)
		worker.waitStopped(t)

		unlockDone := make(chan error, 1)
		go func() {
			unlockDone <- lock.Unlock(ctx)
		}()
		require.Eventually(t, func() bool {
			return objstore.TESTRenewalStopSignalClosed(lock)
		}, time.Second, time.Millisecond, "Unlock should request renewal stop before renewal write exits")
		wrapped.requireNoDeleteBeforeWriteExit(t)
		returned := waitOperationFinished(t, wrapped.writeReturned, "blocked renewal WriteFile exited")
		require.ErrorIs(t, returned.Err, context.Canceled)
		wrapped.requireNoDeleteBeforeWriteExit(t)
		select {
		case err := <-unlockDone:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for Unlock after renewal write exit")
		}

		require.Equal(t, terminalNormalUnlock, recorder.reasonNow())
		select {
		case <-leaseLostCalled:
			t.Fatal("onLeaseLost must not be called after normal unlock wins")
		default:
		}
		require.False(t, recorder.record(terminalLeaseLost))
		_, ok = worker.requestStep(t)
		require.False(t, ok)
		requireFileNotExists(t, filepath.Join(pth, physicalPath))

		next, err := objstore.TryLockRemoteWrite(ctx, base, "v1/LOCK", "next-owner", localLeaseClock())
		require.NoError(t, err)
		require.NoError(t, next.Unlock(ctx))
		requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")
	})

	t.Run("post-write proof cancellation", func(t *testing.T) {
		ctx := context.Background()
		defer objstore.TESTSetLeaseConstants(2*time.Second, 10*time.Millisecond, 3, 5*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(1500*time.Millisecond, time.Millisecond)()

		base, pth := createMockStorage(t)
		leaseNow := time.Date(2030, 6, 4, 14, 0, 0, 0, time.UTC)
		clock := newBlockingLeaseClock(
			clockAt("acquire-pre-clock", leaseNow),
			clockAt("acquire-post-clock", leaseNow.Add(time.Millisecond)),
			clockAt("renewal-pre-clock", leaseNow.Add(20*time.Millisecond)),
			clockBlock("renewal-post-clock-block", leaseNow.Add(21*time.Millisecond)),
		)
		lock, err := objstore.TryLockRemoteWrite(ctx, base, "v1/LOCK", "owner", clock)
		require.NoError(t, err)
		physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, ctx, audit, "owner-a", "migration-write", lock.String())
		step, ok := worker.requestStep(t)
		require.True(t, ok)
		require.Positive(t, step)
		recorder := newTerminalRecorder()
		leaseLostCalled := make(chan struct{}, 1)
		objstore.TESTStartRenewal(ctx, lock, func() {
			select {
			case leaseLostCalled <- struct{}{}:
			default:
			}
			if recorder.record(terminalLeaseLost) {
				worker.stop(workerStopLeaseLost)
			}
		})
		t.Cleanup(func() {
			objstore.TESTStopRenewal(lock)
		})

		waitClockLabel(t, clock.started, "renewal-post-clock-block")
		require.True(t, recorder.record(terminalNormalUnlock))
		worker.stop(workerStopTestStop)
		worker.waitStopped(t)

		unlockDone := make(chan error, 1)
		go func() {
			unlockDone <- lock.Unlock(ctx)
		}()
		require.Eventually(t, func() bool {
			return objstore.TESTRenewalStopSignalClosed(lock)
		}, time.Second, time.Millisecond, "Unlock should request renewal stop before post-write proof exits")
		done := waitClockLabel(t, clock.done, "renewal-post-clock-block")
		require.ErrorIs(t, done.Err, context.Canceled)
		select {
		case err := <-unlockDone:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for Unlock after post-write proof exit")
		}

		require.Equal(t, terminalNormalUnlock, recorder.reasonNow())
		select {
		case <-leaseLostCalled:
			t.Fatal("onLeaseLost must not be called after normal unlock cancels post-write proof")
		default:
		}
		require.False(t, recorder.record(terminalLeaseLost))
		_, ok = worker.requestStep(t)
		require.False(t, ok)
		requireFileNotExists(t, filepath.Join(pth, physicalPath))

		next, err := objstore.TryLockRemoteWrite(ctx, base, "v1/LOCK", "next-owner", localLeaseClock())
		require.NoError(t, err)
		require.NoError(t, next.Unlock(ctx))
		requireNoIntentWithPrefix(t, base, "v1/LOCK.WRIT.")
	})
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

	t.Run("low remaining lease skips renewal attempt", func(t *testing.T) {
		parentCtx, cancel := context.WithCancel(context.Background())
		defer objstore.TESTSetLeaseConstants(60*time.Millisecond, 25*time.Millisecond, 5, 5*time.Millisecond)()
		defer objstore.TESTSetRenewalProofConstants(40*time.Millisecond, time.Millisecond)()

		base, _ := createMockStorage(t)
		storage := newOperationBlockingStorage(base)
		lock, err := objstore.TryLockRemoteWrite(parentCtx, storage, "v1/LOCK", "owner", localLeaseClock())
		require.NoError(t, err)
		physicalPath := requireSinglePathWithPrefix(t, base, "v1/LOCK.WRIT.")
		storage.blockNextReads(physicalPath, 1)

		audit := newCriticalSectionAudit(t.Name())
		worker := startProtectedWorker(t, parentCtx, audit, "owner-a", "migration-write", lock.String())
		startModelRenewal(t, parentCtx, lock, worker)
		t.Cleanup(func() {
			stopModelRenewal(cancel, lock, storage.releaseReads)
		})

		select {
		case op := <-storage.readStarted:
			t.Fatalf("renewal should stop before ReadFile when remaining lease cannot cover operation cap: %s", op.Path)
		case <-worker.lostCh():
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for lease lost after low remaining lease guard")
		}
		_, ok := worker.requestStep(t)
		require.False(t, ok)
		requireNoStepAfterAction(t, audit.snapshot(), "owner-a", "lost")
		require.Zero(t, storage.blockedReadCount())
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
