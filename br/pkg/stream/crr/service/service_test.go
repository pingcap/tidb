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

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
	streamhelperconfig "github.com/pingcap/tidb/br/pkg/streamhelper/config"
	testutil "github.com/pingcap/tidb/br/pkg/utiltest/crr"
	"github.com/stretchr/testify/require"
)

func TestServiceTracksSuccessfulCheckpoint(t *testing.T) {
	ctx := context.Background()
	h := newServiceHarness(ctx, t)
	initialCheckpoint := h.requireInitialCheckpointByTick()

	svc, err := New(
		Deps{
			PD:       h.PDSim,
			Watcher:  h.PDSim,
			Upstream: h.Upstream,
			Sync:     checkpoint.NewExistenceSyncChecker(h.Downstream),
		},
		Config{
			CalculatorConfig: CalculatorConfig{
				TaskName:     "drr_test_task",
				PollInterval: 5 * time.Millisecond,
			},
			RetryInterval: 5 * time.Millisecond,
		},
	)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(runCtx)
	}()

	upstreamCheckpoint := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, upstreamCheckpoint)
	h.requireReplicateAllPending()

	require.Eventually(t, func() bool {
		snapshot := svc.Status()
		return snapshot.SafeCheckpoint == upstreamCheckpoint &&
			snapshot.SyncedTS > 0 &&
			snapshot.Statistic.UpstreamReadMetaFileCount == 1 &&
			snapshot.Statistic.EstimatedSyncLogFileCount == 1 &&
			snapshot.Statistic.DownstreamCheckFileCount >= 2 &&
			snapshot.ConsecutiveFailures == 0 &&
			!snapshot.LastSuccessTime.IsZero()
	}, 5*time.Second, 20*time.Millisecond)

	cancel()
	require.NoError(t, <-done)

	snapshot := svc.Status()
	require.Equal(t, stateStopped, snapshot.State)
	require.False(t, snapshot.Live)
	require.False(t, snapshot.Ready)
}

func TestServiceTracksFailures(t *testing.T) {
	ctx := context.Background()
	h := newServiceHarness(ctx, t)
	initialCheckpoint := h.requireInitialCheckpointByTick()

	svc, err := New(
		Deps{
			PD:       h.PDSim,
			Watcher:  h.PDSim,
			Upstream: h.Upstream,
			Sync:     checkpoint.NewExistenceSyncChecker(failingDownstreamChecker{}),
		},
		Config{
			CalculatorConfig: CalculatorConfig{
				TaskName:     "drr_test_task",
				PollInterval: 5 * time.Millisecond,
			},
			RetryInterval: 5 * time.Millisecond,
		},
	)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(runCtx)
	}()

	upstreamCheckpoint := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, upstreamCheckpoint)

	require.Eventually(t, func() bool {
		snapshot := svc.Status()
		return snapshot.State == stateDegraded &&
			!snapshot.Ready &&
			snapshot.ConsecutiveFailures > 0 &&
			snapshot.LastError != "" &&
			!snapshot.LastErrorTime.IsZero() &&
			!snapshot.LastEventTime.IsZero()
	}, 5*time.Second, 20*time.Millisecond)

	cancel()
	require.NoError(t, <-done)
}

func TestServiceDoesNotDoubleCountCalculatorFailure(t *testing.T) {
	ctx := context.Background()
	h := newServiceHarness(ctx, t)
	initialCheckpoint := h.requireInitialCheckpointByTick()

	runCtx, cancel := context.WithCancel(ctx)
	checker := &cancelingFailingDownstreamChecker{cancel: cancel}
	svc, err := New(
		Deps{
			PD:       h.PDSim,
			Watcher:  h.PDSim,
			Upstream: h.Upstream,
			Sync:     checkpoint.NewExistenceSyncChecker(checker),
		},
		Config{
			CalculatorConfig: CalculatorConfig{
				TaskName:     "drr_test_task",
				PollInterval: 5 * time.Millisecond,
			},
			RetryInterval: 5 * time.Millisecond,
		},
	)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- svc.Run(runCtx)
	}()

	upstreamCheckpoint := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, upstreamCheckpoint)

	require.NoError(t, <-done)

	snapshot := svc.Status()
	require.Equal(t, uint64(1), snapshot.ConsecutiveFailures)
	require.Contains(t, snapshot.LastError, "boom for")
	require.False(t, snapshot.LastErrorTime.IsZero())
	require.False(t, snapshot.LastEventTime.IsZero())
}

func TestServiceWaitsForCheckpointWatch(t *testing.T) {
	ctx := context.Background()
	h := newServiceHarness(ctx, t)
	initialCheckpoint := h.requireInitialCheckpointByTick()

	svc, err := New(
		Deps{
			PD:       h.PDSim,
			Watcher:  h.PDSim,
			Upstream: h.Upstream,
			Sync:     checkpoint.NewExistenceSyncChecker(h.Downstream),
		},
		Config{
			CalculatorConfig: CalculatorConfig{
				TaskName:     "drr_test_task",
				PollInterval: 5 * time.Millisecond,
			},
			RetryInterval: 5 * time.Millisecond,
		},
	)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(runCtx)
	}()

	var idleRound uint64
	require.Eventually(t, func() bool {
		snapshot := svc.Status()
		if snapshot.SafeCheckpoint != initialCheckpoint {
			return false
		}
		idleRound = snapshot.CurrentRound
		return idleRound >= 2
	}, 5*time.Second, 20*time.Millisecond)

	time.Sleep(100 * time.Millisecond)
	require.Equal(t, idleRound, svc.Status().CurrentRound)

	upstreamCheckpoint := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, upstreamCheckpoint)
	h.requireReplicateAllPending()

	require.Eventually(t, func() bool {
		snapshot := svc.Status()
		return snapshot.SafeCheckpoint == upstreamCheckpoint && snapshot.CurrentRound > idleRound
	}, 5*time.Second, 20*time.Millisecond)

	cancel()
	require.NoError(t, <-done)
}

func TestServiceRecoversFromCheckpointWatchError(t *testing.T) {
	ctx := context.Background()
	h := newServiceHarness(ctx, t)
	initialCheckpoint := h.requireInitialCheckpointByTick()

	pd := &watchErrorPDSim{
		PDSim:     h.PDSim,
		failNext:  true,
		errorText: "watch boom",
	}
	svc, err := New(
		Deps{
			PD:       pd,
			Watcher:  pd,
			Upstream: h.Upstream,
			Sync:     checkpoint.NewExistenceSyncChecker(h.Downstream),
		},
		Config{
			CalculatorConfig: CalculatorConfig{
				TaskName:     "drr_test_task",
				PollInterval: 5 * time.Millisecond,
			},
			RetryInterval: 5 * time.Millisecond,
		},
	)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(runCtx)
	}()

	require.Eventually(t, func() bool {
		snapshot := svc.Status()
		return snapshot.SafeCheckpoint == initialCheckpoint &&
			snapshot.State == stateDegraded &&
			snapshot.LastError == "watch boom" &&
			snapshot.ConsecutiveFailures == 1 &&
			!snapshot.LastErrorTime.IsZero() &&
			!snapshot.LastEventTime.IsZero()
	}, 5*time.Second, 20*time.Millisecond)

	upstreamCheckpoint := h.flushRoundsAndGetCheckpoint([]uint64{1}, 1)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, upstreamCheckpoint)
	h.requireReplicateAllPending()

	require.Eventually(t, func() bool {
		snapshot := svc.Status()
		return snapshot.SafeCheckpoint == upstreamCheckpoint &&
			snapshot.State == stateRunning &&
			snapshot.ConsecutiveFailures == 0 &&
			snapshot.LastError == ""
	}, 5*time.Second, 20*time.Millisecond)

	cancel()
	require.NoError(t, <-done)
}

func TestServiceLoadsPersistedResumeState(t *testing.T) {
	ctx := context.Background()
	h := newServiceHarness(ctx, t)
	initialCheckpoint := h.requireInitialCheckpointByTick()

	firstRecord, err := h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, firstRecord.CheckpointTS)
	h.requireReplicateAllPending()

	secondRecord, err := h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	h.requireCheckpointAdvancedByTick(firstRecord.CheckpointTS, secondRecord.CheckpointTS)

	stateStore := &inMemoryResumeStateStore{
		state: &PersistentState{
			LastCheckpoint: firstRecord.CheckpointTS,
			SyncedTS:       firstRecord.FlushTS,
			SyncedByStore:  map[uint64]uint64{1: firstRecord.FlushTS},
		},
	}
	svc, err := New(
		Deps{
			PD:       h.PDSim,
			Watcher:  h.PDSim,
			Upstream: h.Upstream,
			Sync:     checkpoint.NewExistenceSyncChecker(h.Downstream),
			State:    stateStore,
		},
		Config{
			CalculatorConfig: CalculatorConfig{
				TaskName:     "drr_test_task",
				PollInterval: 5 * time.Millisecond,
			},
			RetryInterval: 5 * time.Millisecond,
		},
	)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(runCtx)
	}()

	require.Eventually(t, func() bool {
		snapshot := svc.Status()
		return snapshot.SafeCheckpoint == firstRecord.CheckpointTS &&
			snapshot.SyncedTS == firstRecord.FlushTS &&
			snapshot.PendingFileCount > 0 &&
			snapshot.Statistic.UpstreamReadMetaFileCount == 1 &&
			snapshot.Statistic.EstimatedSyncLogFileCount == 1
	}, 5*time.Second, 20*time.Millisecond)

	h.requireReplicateAllPending()
	require.Eventually(t, func() bool {
		snapshot := svc.Status()
		return snapshot.SafeCheckpoint == secondRecord.CheckpointTS &&
			snapshot.SyncedTS == secondRecord.FlushTS &&
			stateStore.savedState().LastCheckpoint == secondRecord.CheckpointTS &&
			stateStore.savedState().SyncedTS == secondRecord.FlushTS &&
			stateStore.savedState().SyncedByStore[1] == secondRecord.FlushTS
	}, 5*time.Second, 20*time.Millisecond)

	cancel()
	require.NoError(t, <-done)
}

func TestServiceRetriesFailedResumeStatePersist(t *testing.T) {
	ctx := context.Background()
	h := newServiceHarness(ctx, t)
	initialCheckpoint := h.requireInitialCheckpointByTick()
	stateStore := &inMemoryResumeStateStore{saveFailuresLeft: 1}

	svc, err := New(
		Deps{
			PD:       h.PDSim,
			Watcher:  h.PDSim,
			Upstream: h.Upstream,
			Sync:     checkpoint.NewExistenceSyncChecker(h.Downstream),
			State:    stateStore,
		},
		Config{
			CalculatorConfig: CalculatorConfig{
				TaskName:     "drr_test_task",
				PollInterval: 5 * time.Millisecond,
			},
			RetryInterval: 5 * time.Millisecond,
		},
	)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(runCtx)
	}()

	record, err := h.FlushSim.FlushStore(ctx, 1)
	require.NoError(t, err)
	h.requireCheckpointAdvancedByTick(initialCheckpoint, record.CheckpointTS)
	h.requireReplicateAllPending()

	require.Eventually(t, func() bool {
		snapshot := svc.Status()
		state := stateStore.savedState()
		return state.LastCheckpoint == record.CheckpointTS &&
			state.SyncedTS == record.FlushTS &&
			stateStore.saveCount() >= 2 &&
			snapshot.Ready &&
			snapshot.State == stateRunning &&
			snapshot.ConsecutiveFailures == 0 &&
			snapshot.LastError == ""
	}, 5*time.Second, 20*time.Millisecond)

	cancel()
	require.NoError(t, <-done)
}

func TestServiceRetriesFailedResumeStateLoad(t *testing.T) {
	ctx := context.Background()
	h := newServiceHarness(ctx, t)
	stateStore := &inMemoryResumeStateStore{loadFailuresLeft: 1}

	svc, err := New(
		Deps{
			PD:       h.PDSim,
			Watcher:  h.PDSim,
			Upstream: h.Upstream,
			Sync:     checkpoint.NewExistenceSyncChecker(h.Downstream),
			State:    stateStore,
		},
		Config{
			CalculatorConfig: CalculatorConfig{
				TaskName:     "drr_test_task",
				PollInterval: 5 * time.Millisecond,
			},
			RetryInterval: 5 * time.Millisecond,
		},
	)
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- svc.Run(runCtx)
	}()

	require.Eventually(t, func() bool {
		snapshot := svc.Status()
		return stateStore.loadCount() >= 2 &&
			snapshot.State == stateRunning &&
			snapshot.Ready &&
			snapshot.ConsecutiveFailures == 0 &&
			snapshot.LastError == ""
	}, 5*time.Second, 20*time.Millisecond)

	cancel()
	require.NoError(t, <-done)
}

func TestServiceStatusEndpoints(t *testing.T) {
	status := newStatusStore("task")
	svc := &Service{status: status}
	mux := http.NewServeMux()
	svc.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/livez", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)

	status.start()
	status.applyEvent(checkpoint.CheckpointEvent{
		Type:               checkpoint.EventCheckpointAdvanced,
		Time:               time.Now(),
		UpstreamCheckpoint: 42,
		SafeCheckpoint:     42,
		SyncedTS:           42,
		Statistic: &checkpoint.FileStatistic{
			UpstreamReadMetaFileCount:       3,
			EstimatedSyncLogFileCount:       7,
			DownstreamCheckFileCount:        11,
			PlannedFileSuffixCounts:         map[string]int{".log": 7, ".meta": 3},
			DownstreamCheckFileSuffixCounts: map[string]int{".log": 8, ".meta": 3},
		},
	})

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	status.applyEvent(checkpoint.CheckpointEvent{
		Type: checkpoint.EventCalculationFailed,
		Time: time.Now(),
		Err:  fmt.Errorf("boom"),
	})

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var snapshot StatusSnapshot
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &snapshot))
	require.Equal(t, uint64(42), snapshot.SafeCheckpoint)
	require.Equal(t, uint64(42), snapshot.SyncedTS)
	require.Equal(t, 3, snapshot.Statistic.UpstreamReadMetaFileCount)
	require.Equal(t, 7, snapshot.Statistic.EstimatedSyncLogFileCount)
	require.Equal(t, 11, snapshot.Statistic.DownstreamCheckFileCount)
	require.Equal(t, map[string]int{".log": 7, ".meta": 3}, snapshot.Statistic.PlannedFileSuffixCounts)
	require.Equal(t, map[string]int{".log": 8, ".meta": 3}, snapshot.Statistic.DownstreamCheckFileSuffixCounts)
}

func TestStatusStoreTracksFileStatistic(t *testing.T) {
	status := newStatusStore("task")
	status.start()
	status.beginRound()
	status.applyEvent(checkpoint.CheckpointEvent{
		Type:             checkpoint.EventRoundPlanned,
		Time:             time.Now(),
		PendingFileCount: 2,
		Statistic: &checkpoint.FileStatistic{
			UpstreamReadMetaFileCount: 1,
			EstimatedSyncLogFileCount: 1,
			PlannedFileSuffixCounts:   map[string]int{".log": 1, ".meta": 1},
		},
	})
	status.applyEvent(checkpoint.CheckpointEvent{
		Type: checkpoint.EventCheckpointAdvanced,
		Time: time.Now(),
		Statistic: &checkpoint.FileStatistic{
			UpstreamReadMetaFileCount:       1,
			EstimatedSyncLogFileCount:       1,
			DownstreamCheckFileCount:        2,
			PlannedFileSuffixCounts:         map[string]int{".log": 1, ".meta": 1},
			DownstreamCheckFileSuffixCounts: map[string]int{".log": 1, ".meta": 1},
		},
	})

	snapshot := status.snapshotCopy()
	require.Equal(t, 1, snapshot.Statistic.UpstreamReadMetaFileCount)
	require.Equal(t, 1, snapshot.Statistic.EstimatedSyncLogFileCount)
	require.Equal(t, 2, snapshot.Statistic.DownstreamCheckFileCount)
	require.Equal(t, map[string]int{".log": 1, ".meta": 1}, snapshot.Statistic.PlannedFileSuffixCounts)
	require.Equal(t, map[string]int{".log": 1, ".meta": 1}, snapshot.Statistic.DownstreamCheckFileSuffixCounts)

	snapshot.Statistic.PlannedFileSuffixCounts[".txt"] = 99
	require.Equal(t, map[string]int{".log": 1, ".meta": 1}, status.snapshotCopy().Statistic.PlannedFileSuffixCounts)
}

func TestStatusStorePreservesFailureStoreCountAndTracksZeroAliveStores(t *testing.T) {
	status := newStatusStore("task")
	status.start()
	status.applyEvent(checkpoint.CheckpointEvent{
		Type:            checkpoint.EventRoundPlanned,
		Time:            time.Now(),
		AliveStoreCount: 3,
	})
	status.applyEvent(checkpoint.CheckpointEvent{
		Type: checkpoint.EventCalculationFailed,
		Time: time.Now(),
		Err:  fmt.Errorf("boom"),
	})

	snapshot := status.snapshotCopy()
	require.Equal(t, 3, snapshot.AliveStoreCount)
	require.False(t, snapshot.Ready)

	status.applyEvent(checkpoint.CheckpointEvent{
		Type:            checkpoint.EventRoundPlanned,
		Time:            time.Now(),
		AliveStoreCount: 0,
	})

	snapshot = status.snapshotCopy()
	require.Equal(t, 0, snapshot.AliveStoreCount)
}

func TestGetStatusFileName(t *testing.T) {
	testCases := []struct {
		name      string
		subDir    string
		expect    string
		errSubstr string
	}{
		{
			name:   "valid subdir",
			subDir: "state/subdir",
			expect: "state/subdir/resume-state.json",
		},
		{
			name:   "leading and trailing slashes",
			subDir: "/state/subdir/",
			expect: "state/subdir/resume-state.json",
		},
		{
			name:   "clean dot segments",
			subDir: "state/./subdir/../subdir2",
			expect: "state/subdir2/resume-state.json",
		},
		{
			name:      "empty subdir",
			subDir:    "",
			errSubstr: "must not be empty",
		},
		{
			name:      "only slash",
			subDir:    "/",
			errSubstr: "must not be empty",
		},
		{
			name:      "parent directory",
			subDir:    "../state",
			errSubstr: "must stay within upstream storage",
		},
		{
			name:      "clean to parent directory",
			subDir:    "state/../../escape",
			errSubstr: "must stay within upstream storage",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := GetStatusFileName(tc.subDir)
			if tc.errSubstr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.errSubstr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expect, actual)
		})
	}
}

func TestServiceRegisterPanicsOnNilMux(t *testing.T) {
	svc := &Service{}
	require.PanicsWithValue(t, "service: nil mux", func() {
		svc.Register(nil)
	})
}

type serviceHarness struct {
	t   *testing.T
	ctx context.Context
	*testutil.TestHarness
}

func newServiceHarness(ctx context.Context, t *testing.T) *serviceHarness {
	t.Helper()

	boundaries, err := testutil.BuildRegionLayout(
		testutil.AddRoundRobinRegions(1, 1),
	)
	require.NoError(t, err)

	h, err := testutil.NewLocalTestHarnessWithTestContext(ctx, testutil.NewTestContext(t), boundaries)
	require.NoError(t, err)
	h.Advancer.UpdateConfig(&streamhelperconfig.CommandConfig{
		BackoffTime:            streamhelperconfig.DefaultBackOffTime,
		TickDuration:           streamhelperconfig.DefaultTickInterval,
		TryAdvanceThreshold:    10 * 365 * 24 * time.Hour,
		CheckPointLagLimit:     streamhelperconfig.DefaultCheckPointLagLimit,
		OwnershipCycleInterval: streamhelperconfig.DefaultOwnershipCycleInterval,
	})
	return &serviceHarness{
		t:           t,
		ctx:         ctx,
		TestHarness: h,
	}
}

func (h *serviceHarness) requireInitialCheckpointByTick() uint64 {
	_, err := h.Tick(h.ctx)
	require.ErrorContains(h.t, err, "GetLastFlushTSOfRegion is legacy and disabled in DRR harness")
	require.True(h.t, h.Advancer.HasSubscriptions())

	checkpoint := h.PDSim.GlobalCheckpoint()
	require.Greater(h.t, checkpoint, uint64(0))
	return checkpoint
}

func (h *serviceHarness) flushRoundsAndGetCheckpoint(stores []uint64, rounds int) uint64 {
	var roundCheckpoint uint64
	for range rounds {
		roundCheckpoint = ^uint64(0)
		for _, storeID := range stores {
			record, err := h.FlushSim.FlushStore(h.ctx, storeID)
			require.NoError(h.t, err)
			if record.CheckpointTS < roundCheckpoint {
				roundCheckpoint = record.CheckpointTS
			}
		}
	}
	require.NotEqual(h.t, ^uint64(0), roundCheckpoint)
	return roundCheckpoint
}

func (h *serviceHarness) requireCheckpointAdvancedByTick(before uint64, expected uint64) {
	require.Greater(h.t, expected, before)

	var checkpoint uint64
	var tickErr error
	require.Eventually(h.t, func() bool {
		checkpoint, tickErr = h.Tick(h.ctx)
		return tickErr == nil && checkpoint == expected
	}, time.Second, 10*time.Millisecond)
	require.NoError(h.t, tickErr)
	require.Greater(h.t, checkpoint, before)
	require.Equal(h.t, expected, checkpoint)
	require.Equal(h.t, expected, h.PDSim.GlobalCheckpoint())
}

func (h *serviceHarness) requireReplicateAllPending() {
	pulled := h.PullMessages(0)
	require.Greater(h.t, pulled, 0)
	replicated, err := h.Replicate(h.ctx, 0)
	require.NoError(h.t, err)
	require.Greater(h.t, replicated, 0)
}

type failingDownstreamChecker struct{}

func (failingDownstreamChecker) FileExists(ctx context.Context, name string) (bool, error) {
	return false, fmt.Errorf("boom for %s", name)
}

type cancelingFailingDownstreamChecker struct {
	once   sync.Once
	cancel context.CancelFunc
}

func (c *cancelingFailingDownstreamChecker) FileExists(ctx context.Context, name string) (bool, error) {
	c.once.Do(c.cancel)
	return false, fmt.Errorf("boom for %s", name)
}

type watchErrorPDSim struct {
	*testutil.PDSim
	mu        sync.Mutex
	failNext  bool
	errorText string
}

func (p *watchErrorPDSim) WaitGlobalCheckpointAdvance(ctx context.Context, taskName string, current uint64) error {
	p.mu.Lock()
	if p.failNext {
		p.failNext = false
		errText := p.errorText
		p.mu.Unlock()
		return fmt.Errorf("%s", errText)
	}
	p.mu.Unlock()
	return p.PDSim.WaitGlobalCheckpointAdvance(ctx, taskName, current)
}

type inMemoryResumeStateStore struct {
	mu               sync.Mutex
	state            *PersistentState
	loadFailuresLeft int
	loads            int
	saveFailuresLeft int
	saves            int
}

func (s *inMemoryResumeStateStore) LoadState(ctx context.Context) (*PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loads++
	if s.loadFailuresLeft > 0 {
		s.loadFailuresLeft--
		return nil, fmt.Errorf("load boom")
	}
	if s.state == nil {
		return nil, nil
	}
	state := *s.state
	state.SyncedByStore = copyUint64Map(state.SyncedByStore)
	return &state, nil
}

func (s *inMemoryResumeStateStore) SaveState(ctx context.Context, state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	if s.saveFailuresLeft > 0 {
		s.saveFailuresLeft--
		return fmt.Errorf("persist boom")
	}
	copied := state
	copied.SyncedByStore = copyUint64Map(state.SyncedByStore)
	s.state = &copied
	return nil
}

func (s *inMemoryResumeStateStore) savedState() PersistentState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == nil {
		return PersistentState{}
	}
	state := *s.state
	state.SyncedByStore = copyUint64Map(state.SyncedByStore)
	return state
}

func (s *inMemoryResumeStateStore) saveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saves
}

func (s *inMemoryResumeStateStore) loadCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loads
}

func copyUint64Map(input map[uint64]uint64) map[uint64]uint64 {
	if input == nil {
		return nil
	}
	cloned := make(map[uint64]uint64, len(input))
	for k, v := range input {
		cloned[k] = v
	}
	return cloned
}
