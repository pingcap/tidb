// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objstore_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

type criticalSectionEvent struct {
	Seq          int
	At           time.Time
	Interleaving string
	OwnerID      string
	Family       string
	LockInfo     string
	Action       string
	Detail       string
}

type criticalSectionAudit struct {
	mu           sync.Mutex
	interleaving string
	nextSeq      int
	events       []criticalSectionEvent
}

func newCriticalSectionAudit(interleaving string) *criticalSectionAudit {
	return &criticalSectionAudit{interleaving: interleaving}
}

func (a *criticalSectionAudit) record(action, ownerID, family, lockInfo, detail string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nextSeq++
	a.events = append(a.events, criticalSectionEvent{
		Seq:          a.nextSeq,
		At:           time.Now(),
		Interleaving: a.interleaving,
		OwnerID:      ownerID,
		Family:       family,
		LockInfo:     lockInfo,
		Action:       action,
		Detail:       detail,
	})
}

func (a *criticalSectionAudit) recordEnter(ownerID, family, lockInfo, detail string) {
	a.record("enter", ownerID, family, lockInfo, detail)
}

func (a *criticalSectionAudit) recordStep(ownerID, family, lockInfo, detail string) {
	a.record("step", ownerID, family, lockInfo, detail)
}

func (a *criticalSectionAudit) recordExit(ownerID, family, lockInfo, detail string) {
	a.record("exit", ownerID, family, lockInfo, detail)
}

func (a *criticalSectionAudit) recordLost(ownerID, family, lockInfo, detail string) {
	a.record("lost", ownerID, family, lockInfo, detail)
}

func (a *criticalSectionAudit) recordStopped(ownerID, family, lockInfo, detail string) {
	a.record("stopped", ownerID, family, lockInfo, detail)
}

func (a *criticalSectionAudit) snapshot() []criticalSectionEvent {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]criticalSectionEvent, len(a.events))
	copy(out, a.events)
	return out
}

type workerStopReason string

const (
	workerStopTestStop  workerStopReason = "test_stop"
	workerStopLeaseLost workerStopReason = "lease_lost"
)

type protectedWorker struct {
	cancel context.CancelFunc
	audit  *criticalSectionAudit

	ownerID  string
	family   string
	lockInfo string

	stepReq     chan chan workerStepResult
	stopped     chan struct{}
	lost        chan struct{}
	terminating chan struct{}

	stopOnce sync.Once
	stateMu  sync.Mutex
	reason   workerStopReason
	terminal bool
	steps    int
}

type workerStepResult struct {
	step int
	ok   bool
}

//revive:disable-next-line:context-as-argument
func startProtectedWorker(t *testing.T, ctx context.Context, audit *criticalSectionAudit, ownerID, family, lockInfo string) *protectedWorker {
	t.Helper()
	workerCtx, cancel := context.WithCancel(ctx)
	w := &protectedWorker{
		cancel:      cancel,
		audit:       audit,
		ownerID:     ownerID,
		family:      family,
		lockInfo:    lockInfo,
		stepReq:     make(chan chan workerStepResult),
		stopped:     make(chan struct{}),
		lost:        make(chan struct{}),
		terminating: make(chan struct{}),
		reason:      workerStopTestStop,
	}
	go w.loop(workerCtx)
	t.Cleanup(func() {
		w.stop(workerStopTestStop)
		w.waitStopped(t)
	})
	return w
}

func (w *protectedWorker) loop(ctx context.Context) {
	defer close(w.stopped)
	w.audit.recordEnter(w.ownerID, w.family, w.lockInfo, "enter")
	for {
		select {
		case <-ctx.Done():
			w.audit.recordStopped(w.ownerID, w.family, w.lockInfo, string(w.stopReason()))
			return
		case reply := <-w.stepReq:
			w.stateMu.Lock()
			if w.terminal {
				w.stateMu.Unlock()
				reply <- workerStepResult{ok: false}
				continue
			}
			w.steps++
			w.audit.recordStep(w.ownerID, w.family, w.lockInfo, fmt.Sprintf("step-%d", w.steps))
			step := w.steps
			w.stateMu.Unlock()
			reply <- workerStepResult{step: step, ok: true}
		}
	}
}

func (w *protectedWorker) requestStep(t *testing.T) (int, bool) {
	t.Helper()
	reply := make(chan workerStepResult, 1)
	select {
	case <-w.terminating:
		return 0, false
	case <-w.stopped:
		return 0, false
	case w.stepReq <- reply:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out requesting protected worker step")
	}
	select {
	case result := <-reply:
		return result.step, result.ok
	case <-w.terminating:
		return 0, false
	case <-w.stopped:
		return 0, false
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for protected worker step")
	}
	return 0, false
}

func (w *protectedWorker) stop(reason workerStopReason) {
	w.stopOnce.Do(func() {
		w.stateMu.Lock()
		w.reason = reason
		w.terminal = true
		if reason == workerStopLeaseLost {
			w.audit.recordLost(w.ownerID, w.family, w.lockInfo, string(reason))
			close(w.lost)
		}
		close(w.terminating)
		w.stateMu.Unlock()
		w.cancel()
	})
}

func (w *protectedWorker) lostCh() <-chan struct{} {
	return w.lost
}

func (w *protectedWorker) waitStopped(t *testing.T) {
	t.Helper()
	waitClosed(t, w.stopped, "protected worker stopped")
}

func (w *protectedWorker) stopReason() workerStopReason {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()
	return w.reason
}

func waitClosed(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func requireEventAction(t *testing.T, events []criticalSectionEvent, action string) {
	t.Helper()
	for _, event := range events {
		if event.Action == action {
			return
		}
	}
	t.Fatalf("missing audit action %q in %#v", action, events)
}

func requireActionBeforeAction(t *testing.T, events []criticalSectionEvent, ownerID, beforeAction, afterAction string) {
	t.Helper()
	beforeSeq := 0
	afterSeq := 0
	for _, event := range events {
		if event.OwnerID != ownerID {
			continue
		}
		if event.Action == beforeAction {
			beforeSeq = event.Seq
		}
		if event.Action == afterAction {
			afterSeq = event.Seq
		}
	}
	require.NotZero(t, beforeSeq, "missing %s event for %s", beforeAction, ownerID)
	require.NotZero(t, afterSeq, "missing %s event for %s", afterAction, ownerID)
	require.Less(t, beforeSeq, afterSeq, "%s should be recorded before %s for %s", beforeAction, afterAction, ownerID)
}

func requireNoStepAfterAction(t *testing.T, events []criticalSectionEvent, ownerID, action string) {
	t.Helper()
	actionSeq := 0
	for _, event := range events {
		if event.OwnerID == ownerID && event.Action == action {
			actionSeq = event.Seq
			break
		}
	}
	require.NotZero(t, actionSeq, "missing %s event for %s", action, ownerID)
	for _, event := range events {
		require.False(t, event.OwnerID == ownerID && event.Action == "step" && event.Seq > actionSeq,
			"step after %s: %#v", action, event)
	}
}

func TestLeaseLockCriticalSectionAuditSelfCheck(t *testing.T) {
	audit := newCriticalSectionAudit("self-check")

	audit.recordEnter("owner-a", "migration-write", "lock-a", "enter")
	audit.recordStep("owner-a", "migration-write", "lock-a", "step-1")
	audit.recordExit("owner-a", "migration-write", "lock-a", "exit")

	events := audit.snapshot()
	require.Len(t, events, 3)
	require.Equal(t, 1, events[0].Seq)
	require.Equal(t, "self-check", events[0].Interleaving)
	require.Equal(t, "owner-a", events[0].OwnerID)
	require.Equal(t, "migration-write", events[0].Family)
	require.Equal(t, "lock-a", events[0].LockInfo)
	require.Equal(t, "enter", events[0].Action)
	require.Equal(t, "enter", events[0].Detail)

	events[0].OwnerID = "mutated"
	require.Equal(t, "owner-a", audit.snapshot()[0].OwnerID)

	const goroutines = 8
	const perGoroutine = 5
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		ownerID := fmt.Sprintf("owner-%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				audit.recordStep(ownerID, "migration-read", "lock", fmt.Sprintf("step-%d", j))
			}
		}()
	}
	wg.Wait()

	events = audit.snapshot()
	require.Len(t, events, 3+goroutines*perGoroutine)
	ownerSteps := make(map[string]int)
	for i, event := range events {
		require.Equal(t, i+1, event.Seq)
		require.False(t, event.At.IsZero())
		if event.Family == "migration-read" && event.Action == "step" {
			ownerSteps[event.OwnerID]++
		}
	}
	for i := 0; i < goroutines; i++ {
		require.Equal(t, perGoroutine, ownerSteps[fmt.Sprintf("owner-%d", i)])
	}
}

func TestLeaseLockProtectedWorkerSelfCheck(t *testing.T) {
	ctx := context.Background()
	audit := newCriticalSectionAudit("worker-self-check")
	worker := startProtectedWorker(t, ctx, audit, "owner-a", "migration-write", "lock-a")

	step, ok := worker.requestStep(t)
	require.True(t, ok)
	require.Equal(t, 1, step)
	step, ok = worker.requestStep(t)
	require.True(t, ok)
	require.Equal(t, 2, step)

	worker.stop(workerStopLeaseLost)
	waitClosed(t, worker.lostCh(), "lost")
	worker.waitStopped(t)
	require.Equal(t, workerStopLeaseLost, worker.stopReason())

	_, ok = worker.requestStep(t)
	require.False(t, ok)

	events := audit.snapshot()
	requireEventAction(t, events, "enter")
	requireEventAction(t, events, "lost")
	requireEventAction(t, events, "stopped")
	requireActionBeforeAction(t, events, "owner-a", "lost", "stopped")
	requireNoStepAfterAction(t, events, "owner-a", "lost")
	requireNoStepAfterAction(t, events, "owner-a", "stopped")

	testStopAudit := newCriticalSectionAudit("worker-test-stop-self-check")
	testStopWorker := startProtectedWorker(t, ctx, testStopAudit, "owner-b", "migration-read", "lock-b")
	step, ok = testStopWorker.requestStep(t)
	require.True(t, ok)
	require.Equal(t, 1, step)
	testStopWorker.stop(workerStopTestStop)
	testStopWorker.stop(workerStopLeaseLost)
	testStopWorker.waitStopped(t)
	require.Equal(t, workerStopTestStop, testStopWorker.stopReason())
	select {
	case <-testStopWorker.lostCh():
		t.Fatal("test stop should not close lost channel")
	default:
	}
	requireNoStepAfterAction(t, testStopAudit.snapshot(), "owner-b", "stopped")
}

func requireNoIntentWithPrefix(t *testing.T, strg storeapi.Storage, prefixes ...string) {
	t.Helper()
	ctx := context.Background()
	for _, prefix := range prefixes {
		dirName := ""
		fileName := prefix
		if idx := strings.LastIndex(prefix, "/"); idx >= 0 {
			dirName = prefix[:idx]
			fileName = prefix[idx+1:]
		}
		require.NoError(t, strg.WalkDir(ctx, &storeapi.WalkOption{
			SubDir:           dirName,
			ObjPrefix:        fileName,
			IncludeTombstone: true,
		}, func(p string, _ int64) error {
			require.NotContains(t, p, ".INTENT.")
			return nil
		}))
	}
}

type lockAttemptResult struct {
	ownerID string
	lock    *objstore.RemoteLock
	err     error
}

type lockAttemptFunc func(context.Context, storeapi.Storage) (*objstore.RemoteLock, error)

func testConcurrentAcquirePair(
	t *testing.T,
	interleaving string,
	lockerA lockAttemptFunc,
	lockerB lockAttemptFunc,
	intentPrefixes []string,
	expectedSuccesses int,
) {
	t.Helper()
	ctx := context.Background()
	strg, _ := createMockStorage(t)
	audit := newCriticalSectionAudit(interleaving)

	waitSignal := func(ch <-chan struct{}, name string) {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s", name)
		}
	}

	phase1 := make(chan struct{}, 1)
	releasePhase1 := make(chan struct{})
	var releasePhase1Once sync.Once
	releaseFirstAcquire := func() {
		releasePhase1Once.Do(func() {
			close(releasePhase1)
		})
	}
	t.Cleanup(releaseFirstAcquire)
	var phase1Mu sync.Mutex
	phase1Count := 0
	getPhase1Count := func() int {
		phase1Mu.Lock()
		defer phase1Mu.Unlock()
		return phase1Count
	}
	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/objstore/exclusive-write-commit-to-1",
		func() {
			phase1Mu.Lock()
			phase1Count++
			first := phase1Count == 1
			phase1Mu.Unlock()
			if first {
				phase1 <- struct{}{}
				<-releasePhase1
			}
		}))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/objstore/exclusive-write-commit-to-1"))
	})

	phase2 := make(chan struct{}, 1)
	releasePhase2 := make(chan struct{})
	var releasePhase2Once sync.Once
	releaseSecondAcquire := func() {
		releasePhase2Once.Do(func() {
			close(releasePhase2)
		})
	}
	t.Cleanup(releaseSecondAcquire)
	var phase2Once sync.Once
	var phase2Mu sync.Mutex
	phase2Count := 0
	getPhase2Count := func() int {
		phase2Mu.Lock()
		defer phase2Mu.Unlock()
		return phase2Count
	}
	require.NoError(t, failpoint.EnableCall("github.com/pingcap/tidb/pkg/objstore/exclusive-write-commit-to-2",
		func() {
			phase2Mu.Lock()
			phase2Count++
			phase2Mu.Unlock()
			phase2Once.Do(func() {
				phase2 <- struct{}{}
				<-releasePhase2
			})
		}))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/objstore/exclusive-write-commit-to-2"))
	})

	resultCh := make(chan lockAttemptResult, 2)
	go func() {
		lock, err := lockerA(ctx, strg)
		resultCh <- lockAttemptResult{ownerID: "owner-a", lock: lock, err: err}
	}()
	go func() {
		lock, err := lockerB(ctx, strg)
		resultCh <- lockAttemptResult{ownerID: "owner-b", lock: lock, err: err}
	}()

	waitSignal(phase1, "first acquire at phase 1")
	waitSignal(phase2, "second acquire at phase 2")
	releaseFirstAcquire()
	releaseSecondAcquire()

	results := make([]lockAttemptResult, 0, 2)
	for len(results) < 2 {
		select {
		case result := <-resultCh:
			results = append(results, result)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for acquire results: interleaving=%s phase1=%d phase2=%d partial=%#v audit=%#v",
				interleaving, getPhase1Count(), getPhase2Count(), results, audit.snapshot())
		}
	}
	successes := 0
	for _, result := range results {
		if result.err != nil {
			require.Nil(t, result.lock)
			require.ErrorContains(t, result.err, "conflict file")
			continue
		}
		successes++
		require.NotNil(t, result.lock)
		lockInfo := result.lock.String()
		worker := startProtectedWorker(t, ctx, audit, result.ownerID, interleaving, lockInfo)
		_, ok := worker.requestStep(t)
		require.True(t, ok)
		worker.stop(workerStopTestStop)
		worker.waitStopped(t)
		require.NoError(t, result.lock.Unlock(ctx))
		audit.recordExit(result.ownerID, interleaving, lockInfo, "unlock")
	}
	require.Equal(t, expectedSuccesses, successes, "%s results=%#v", interleaving, results)
	requireNoIntentWithPrefix(t, strg, intentPrefixes...)
}

func TestLeaseLockConcurrentAcquireExclusion(t *testing.T) {
	t.Run("migration read and write", func(t *testing.T) {
		testConcurrentAcquirePair(t,
			"migration read and write",
			func(ctx context.Context, strg storeapi.Storage) (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteRead(ctx, strg, "v1/LOCK", "reader", localLeaseClock())
			},
			func(ctx context.Context, strg storeapi.Storage) (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "writer", localLeaseClock())
			},
			[]string{"v1/LOCK.READ.", "v1/LOCK.WRIT."},
			1,
		)
	})
	t.Run("truncate writers", func(t *testing.T) {
		testConcurrentAcquirePair(t,
			"truncate",
			func(ctx context.Context, strg storeapi.Storage) (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteTruncate(ctx, strg, "truncate-a", localLeaseClock())
			},
			func(ctx context.Context, strg storeapi.Storage) (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteTruncate(ctx, strg, "truncate-b", localLeaseClock())
			},
			[]string{"truncating.lock."},
			1,
		)
	})
	t.Run("migration writers", func(t *testing.T) {
		testConcurrentAcquirePair(t,
			"migration-write",
			func(ctx context.Context, strg storeapi.Storage) (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "writer-a", localLeaseClock())
			},
			func(ctx context.Context, strg storeapi.Storage) (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteWrite(ctx, strg, "v1/LOCK", "writer-b", localLeaseClock())
			},
			[]string{"v1/LOCK.WRIT."},
			1,
		)
	})
	t.Run("append writers", func(t *testing.T) {
		testConcurrentAcquirePair(t,
			"append-write",
			func(ctx context.Context, strg storeapi.Storage) (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteWrite(ctx, strg, "v1/APPEND_LOCK", "append-a", localLeaseClock())
			},
			func(ctx context.Context, strg storeapi.Storage) (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteWrite(ctx, strg, "v1/APPEND_LOCK", "append-b", localLeaseClock())
			},
			[]string{"v1/APPEND_LOCK.WRIT."},
			1,
		)
	})
	t.Run("migration readers", func(t *testing.T) {
		testConcurrentAcquirePair(t,
			"migration-read",
			func(ctx context.Context, strg storeapi.Storage) (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteRead(ctx, strg, "v1/LOCK", "reader-a", localLeaseClock())
			},
			func(ctx context.Context, strg storeapi.Storage) (*objstore.RemoteLock, error) {
				return objstore.TryLockRemoteRead(ctx, strg, "v1/LOCK", "reader-b", localLeaseClock())
			},
			[]string{"v1/LOCK.READ."},
			2,
		)
	})
}

func TestLeaseLockRenewalLostStopsCriticalSection(t *testing.T) {
	t.Run("migration write block", func(t *testing.T) {
		testRenewalLostStopsCriticalSection(t, "migration-write-block", true, false)
	})
	t.Run("migration write error", func(t *testing.T) {
		testRenewalLostStopsCriticalSection(t, "migration-write-error", true, true)
	})
	t.Run("truncate block", func(t *testing.T) {
		testRenewalLostStopsCriticalSection(t, "truncate-block", false, false)
	})
	t.Run("truncate error", func(t *testing.T) {
		testRenewalLostStopsCriticalSection(t, "truncate-error", false, true)
	})
}

func testRenewalLostStopsCriticalSection(t *testing.T, name string, migrationWrite bool, writeError bool) {
	t.Helper()
	defer objstore.TESTSetLeaseConstants(120*time.Millisecond, 10*time.Millisecond, 3, 5*time.Millisecond)()
	defer objstore.TESTSetRenewalProofConstants(15*time.Millisecond, time.Millisecond)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	strg, _ := createMockStorage(t)
	audit := newCriticalSectionAudit(name)

	var leaseLostOnce sync.Once
	workerReady := make(chan *protectedWorker, 1)
	var releaseWorkerOnce sync.Once
	releaseWorkerForLeaseLost := func(worker *protectedWorker) {
		releaseWorkerOnce.Do(func() {
			workerReady <- worker
		})
	}
	t.Cleanup(func() {
		releaseWorkerForLeaseLost(nil)
	})
	onLeaseLost := func() {
		leaseLostOnce.Do(func() {
			worker := <-workerReady
			if worker == nil {
				return
			}
			worker.stop(workerStopLeaseLost)
		})
	}

	if writeError {
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/objstore/lease-lock-renewal-write-error",
			fmt.Sprintf(`return("signal-dir=%s")`, t.TempDir())))
		t.Cleanup(func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/objstore/lease-lock-renewal-write-error"))
		})
	} else {
		marker := filepath.Join(t.TempDir(), "blocked")
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/objstore/lease-lock-renewal-write-block",
			fmt.Sprintf(`return("signal=%s")`, marker)))
		t.Cleanup(func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/objstore/lease-lock-renewal-write-block"))
		})
	}

	var lock *objstore.RemoteLock
	var err error
	if migrationWrite {
		lock, err = objstore.LockWithRetry(ctx, objstore.TryLockRemoteWrite, strg, "v1/LOCK", "lost-owner", onLeaseLost, localLeaseClock())
	} else {
		lock, err = objstore.LockRemoteTruncate(ctx, strg, "lost-owner", onLeaseLost, localLeaseClock())
	}
	require.NoError(t, err)
	defer objstore.TESTStopRenewal(lock)

	lockInfo := lock.String()
	createdWorker := startProtectedWorker(t, ctx, audit, "owner-a", name, lockInfo)
	step, ok := createdWorker.requestStep(t)
	require.True(t, ok)
	require.Equal(t, 1, step)
	releaseWorkerForLeaseLost(createdWorker)

	select {
	case <-createdWorker.lostCh():
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for lease lost: name=%s migrationWrite=%t writeError=%t lock=%s audit=%#v",
			name, migrationWrite, writeError, lockInfo, audit.snapshot())
	}
	createdWorker.waitStopped(t)
	require.Equal(t, workerStopLeaseLost, createdWorker.stopReason())
	_, ok = createdWorker.requestStep(t)
	require.False(t, ok)

	events := audit.snapshot()
	requireActionBeforeAction(t, events, "owner-a", "lost", "stopped")
	requireNoStepAfterAction(t, events, "owner-a", "lost")
	requireNoStepAfterAction(t, events, "owner-a", "stopped")
}
