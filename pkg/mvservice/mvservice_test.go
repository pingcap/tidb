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

//go:build intest

package mvservice

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestNewMVServiceConfig(t *testing.T) {
	t.Run("applied", func(t *testing.T) {
		cfg := DefaultMVServiceConfig()
		cfg.TaskMaxConcurrency = 3
		cfg.RefreshTaskConcurrencyRatio = 0.6
		cfg.RefreshTaskTimeout = 42 * time.Second
		cfg.PurgeTaskTimeout = 84 * time.Second
		cfg.FetchInterval = 37 * time.Second
		cfg.BasicInterval = 2 * time.Second
		cfg.ServerRefreshInterval = 9 * time.Second
		cfg.RetryBaseDelay = 3 * time.Second
		cfg.RetryMaxDelay = 21 * time.Second
		cfg.ServerConsistentHashReplicas = 17
		cfg.TaskBackpressure = TaskBackpressureConfig{
			CPUThreshold: 0.7,
			MemThreshold: 0.8,
			Delay:        500 * time.Millisecond,
		}

		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, cfg)
		refreshConcurrency, purgeConcurrency := splitTaskConcurrency(cfg.TaskMaxConcurrency, cfg.RefreshTaskConcurrencyRatio)
		require.Equal(t, refreshConcurrency, int(svc.refreshExecutor.maxConcurrency.Load()))
		require.Equal(t, purgeConcurrency, int(svc.purgeExecutor.maxConcurrency.Load()))
		require.Equal(t, cfg.RefreshTaskConcurrencyRatio, svc.GetRefreshTaskConcurrencyRatio())
		require.Equal(t, cfg.RefreshTaskTimeout, time.Duration(svc.refreshExecutor.timeoutNanos.Load()))
		require.Equal(t, cfg.PurgeTaskTimeout, time.Duration(svc.purgeExecutor.timeoutNanos.Load()))

		baseDelay, maxDelay := svc.retryDelayConfig()
		require.Equal(t, cfg.RetryBaseDelay, baseDelay)
		require.Equal(t, cfg.RetryMaxDelay, maxDelay)

		backpressureCfg := svc.GetTaskBackpressureConfig()
		require.Equal(t, cfg.TaskBackpressure, backpressureCfg)
		require.NotNil(t, svc.refreshExecutor.backpressure.Load())
		require.NotNil(t, svc.purgeExecutor.backpressure.Load())

		require.Equal(t, cfg.FetchInterval, svc.fetchInterval())
		require.Equal(t, cfg.BasicInterval, svc.basicInterval)
		require.Equal(t, cfg.ServerRefreshInterval, svc.serverRefreshInterval)
		require.Equal(t, cfg.ServerConsistentHashReplicas, svc.sch.chash.replicas)
	})

	t.Run("normalized_defaults", func(t *testing.T) {
		cfg := DefaultMVServiceConfig()
		cfg.TaskMaxConcurrency = -1
		cfg.RefreshTaskConcurrencyRatio = -1

		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, cfg)

		baseDelay, maxDelay := svc.retryDelayConfig()
		require.Equal(t, cfg.RetryBaseDelay, baseDelay)
		require.Equal(t, cfg.RetryMaxDelay, maxDelay)

		backpressureCfg := svc.GetTaskBackpressureConfig()
		require.Equal(t, cfg.TaskBackpressure, backpressureCfg)
		require.Equal(t, defaultMVRefreshTaskConcurrencyRatio, svc.GetRefreshTaskConcurrencyRatio())
		require.Nil(t, svc.refreshExecutor.backpressure.Load())
		require.Nil(t, svc.purgeExecutor.backpressure.Load())
	})

	t.Run("invalid_retry_panics", func(t *testing.T) {
		cfg := DefaultMVServiceConfig()
		cfg.RetryBaseDelay = 10 * time.Second
		cfg.RetryMaxDelay = 2 * time.Second
		require.Panics(t, func() {
			_ = NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, cfg)
		})
	})

	t.Run("invalid_backpressure_panics", func(t *testing.T) {
		cfg := DefaultMVServiceConfig()
		cfg.TaskBackpressure = TaskBackpressureConfig{
			CPUThreshold: -1,
			MemThreshold: 0.8,
		}
		require.Panics(t, func() {
			_ = NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, cfg)
		})
	})
}

func TestMVServiceUpdateConfigs(t *testing.T) {
	t.Run("fetch_interval", func(t *testing.T) {
		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, DefaultMVServiceConfig())

		err := svc.setFetchInterval(2 * time.Second)
		require.NoError(t, err)
		require.Equal(t, 2*time.Second, svc.fetchInterval())

		err = svc.setFetchInterval(0)
		require.Error(t, err)

		err = svc.setFetchInterval(500 * time.Microsecond)
		require.Error(t, err)
	})

	t.Run("task_backpressure", func(t *testing.T) {
		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, DefaultMVServiceConfig())

		err := svc.SetTaskBackpressureConfig(TaskBackpressureConfig{
			CPUThreshold: 0.7,
			MemThreshold: 0.8,
			Delay:        time.Second,
		})
		require.NoError(t, err)

		cfg := svc.GetTaskBackpressureConfig()
		require.Equal(t, 0.7, cfg.CPUThreshold)
		require.Equal(t, 0.8, cfg.MemThreshold)
		require.Equal(t, time.Second, cfg.Delay)
		require.NotNil(t, svc.refreshExecutor.backpressure.Load())
		require.NotNil(t, svc.purgeExecutor.backpressure.Load())

		err = svc.SetTaskBackpressureConfig(TaskBackpressureConfig{})
		require.NoError(t, err)
		require.Equal(t, TaskBackpressureConfig{}, svc.GetTaskBackpressureConfig())
		require.Nil(t, svc.refreshExecutor.backpressure.Load())
		require.Nil(t, svc.purgeExecutor.backpressure.Load())

		err = svc.SetTaskBackpressureConfig(TaskBackpressureConfig{
			CPUThreshold: -1,
			MemThreshold: 0.8,
		})
		require.Error(t, err)
	})

	t.Run("retry_delay", func(t *testing.T) {
		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, DefaultMVServiceConfig())

		baseDelay, maxDelay := svc.retryDelayConfig()
		require.Equal(t, defaultMVTaskRetryBase, baseDelay)
		require.Equal(t, defaultMVTaskRetryMax, maxDelay)

		err := svc.setRetryDelayConfig(2*time.Second, 10*time.Second)
		require.NoError(t, err)
		baseDelay, maxDelay = svc.retryDelayConfig()
		require.Equal(t, 2*time.Second, baseDelay)
		require.Equal(t, 10*time.Second, maxDelay)
		require.Equal(t, 2*time.Second, svc.retryDelay(0))
		require.Equal(t, 4*time.Second, svc.retryDelay(2))
		require.Equal(t, 10*time.Second, svc.retryDelay(10))

		err = svc.setRetryDelayConfig(0, 10*time.Second)
		require.Error(t, err)
		err = svc.setRetryDelayConfig(10*time.Second, 2*time.Second)
		require.Error(t, err)
	})

	t.Run("task_concurrency_split", func(t *testing.T) {
		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, DefaultMVServiceConfig())

		svc.SetTaskMaxConcurrency(10)
		svc.SetRefreshTaskConcurrencyRatio(0.6)
		require.Equal(t, 6, int(svc.refreshExecutor.maxConcurrency.Load()))
		require.Equal(t, 4, int(svc.purgeExecutor.maxConcurrency.Load()))

		svc.SetRefreshTaskConcurrencyRatio(0.9)
		require.Equal(t, 9, int(svc.refreshExecutor.maxConcurrency.Load()))
		require.Equal(t, 1, int(svc.purgeExecutor.maxConcurrency.Load()))

		svc.SetTaskMaxConcurrency(2)
		require.Equal(t, 1, int(svc.refreshExecutor.maxConcurrency.Load()))
		require.Equal(t, 1, int(svc.purgeExecutor.maxConcurrency.Load()))
	})
}

type fullChainMVServiceHelper struct {
	*mockMVServiceHelper

	pendingMu   sync.RWMutex
	pendingLogs map[int64]time.Time
	pendingMVs  map[int64]time.Time

	refreshSignal chan int64
	purgeSignal   chan int64
}

type mvServiceTestHarness struct {
	t      *testing.T
	svc    *MVService
	helper *fullChainMVServiceHelper

	cancel  context.CancelFunc
	runDone chan struct{}
}

func newFullChainMVServiceHelper() *fullChainMVServiceHelper {
	return &fullChainMVServiceHelper{
		mockMVServiceHelper: &mockMVServiceHelper{
			refreshNext: mvsNow().Add(30 * time.Second),
			purgeNext:   mvsNow().Add(30 * time.Second),
		},
		pendingLogs:   make(map[int64]time.Time),
		pendingMVs:    make(map[int64]time.Time),
		refreshSignal: make(chan int64, 16),
		purgeSignal:   make(chan int64, 16),
	}
}

func (h *fullChainMVServiceHelper) drainSignals() {
	drainInt64Chan(h.refreshSignal)
	drainInt64Chan(h.purgeSignal)
}

func (h *fullChainMVServiceHelper) setPending(logs map[int64]time.Time, mvs map[int64]time.Time) {
	h.pendingMu.Lock()
	defer h.pendingMu.Unlock()

	h.pendingLogs = make(map[int64]time.Time, len(logs))
	for id, next := range logs {
		h.pendingLogs[id] = next
	}
	h.pendingMVs = make(map[int64]time.Time, len(mvs))
	for id, next := range mvs {
		h.pendingMVs[id] = next
	}
}

func (h *fullChainMVServiceHelper) LoadAllTiDBMVLogPurge(context.Context, basic.SessionPool) (map[int64]*mlogPurgeTask, error) {
	h.fetchLogsCalls.Add(1)
	if h.fetchLogsErr != nil {
		return nil, h.fetchLogsErr
	}

	h.pendingMu.RLock()
	defer h.pendingMu.RUnlock()
	ret := make(map[int64]*mlogPurgeTask, len(h.pendingLogs))
	for id, next := range h.pendingLogs {
		logTask := &mlogPurgeTask{
			ID:        id,
			nextPurge: next,
		}
		logTask.orderTs = next.UnixMilli()
		ret[id] = logTask
	}
	return ret, nil
}

func (h *fullChainMVServiceHelper) LoadAllTiDBMVRefresh(context.Context, basic.SessionPool) (map[int64]*mviewTask, error) {
	h.fetchViewCalls.Add(1)
	if h.fetchViewsErr != nil {
		return nil, h.fetchViewsErr
	}

	h.pendingMu.RLock()
	defer h.pendingMu.RUnlock()
	ret := make(map[int64]*mviewTask, len(h.pendingMVs))
	for id, next := range h.pendingMVs {
		mvTask := &mviewTask{
			ID:          id,
			nextRefresh: next,
		}
		mvTask.orderTs = next.UnixMilli()
		ret[id] = mvTask
	}
	return ret, nil
}

func (h *fullChainMVServiceHelper) RefreshMV(_ context.Context, _ basic.SessionPool, mviewID int64) (nextRefresh time.Time, err error) {
	h.lastRefreshID = mviewID
	emitInt64(h.refreshSignal, mviewID)
	if h.refreshErr != nil {
		return time.Time{}, h.refreshErr
	}
	return h.refreshNext, nil
}

func (h *fullChainMVServiceHelper) PurgeMVLog(_ context.Context, _ basic.SessionPool, mlogID int64) (nextPurge time.Time, err error) {
	h.lastPurgeID = mlogID
	emitInt64(h.purgeSignal, mlogID)
	if h.purgeErr != nil {
		return time.Time{}, h.purgeErr
	}
	return h.purgeNext, nil
}

func (*fullChainMVServiceHelper) TryBackoffRefreshManualCancel(context.Context, basic.SessionPool, int64, time.Time) (bool, time.Time, error) {
	return false, time.Time{}, nil
}

func (*fullChainMVServiceHelper) TryBackoffPurgeManualCancel(context.Context, basic.SessionPool, int64, time.Time) (bool, time.Time, error) {
	return false, time.Time{}, nil
}

func pendingTaskCounts(svc *MVService) (mvLogCount int, mvCount int) {
	svc.mlogPurgeMu.Lock()
	mvLogCount = len(svc.mlogPurgeMu.pending)
	svc.mlogPurgeMu.Unlock()

	svc.mviewRefreshMu.Lock()
	mvCount = len(svc.mviewRefreshMu.pending)
	svc.mviewRefreshMu.Unlock()
	return mvLogCount, mvCount
}

func newMVServiceTestHarness(t *testing.T) *mvServiceTestHarness {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	helper := newFullChainMVServiceHelper()
	helper.setPending(nil, nil)

	return &mvServiceTestHarness{
		t:       t,
		svc:     NewMVService(ctx, mockSessionPool{}, helper, DefaultMVServiceConfig()),
		helper:  helper,
		cancel:  cancel,
		runDone: make(chan struct{}),
	}
}

func (h *mvServiceTestHarness) start() {
	h.t.Helper()
	go func() {
		h.svc.Run()
		close(h.runDone)
	}()
}

func (h *mvServiceTestHarness) stop() {
	h.t.Helper()
	h.cancel()
	select {
	case <-h.runDone:
	case <-time.After(testEventuallyWait):
		h.t.Fatal("mv service did not stop in time")
	}
}

func (h *mvServiceTestHarness) setMeta(logs map[int64]time.Time, mvs map[int64]time.Time) {
	h.t.Helper()
	h.helper.setPending(logs, mvs)
}

func (h *mvServiceTestHarness) triggerDDL() {
	h.t.Helper()
	h.svc.NotifyDDLChange()
}

func (h *mvServiceTestHarness) fetchCallCounts() (logCalls int32, viewCalls int32) {
	h.t.Helper()
	return h.helper.fetchLogsCalls.Load(), h.helper.fetchViewCalls.Load()
}

func (h *mvServiceTestHarness) waitFetchCycleSince(logCalls, viewCalls int32) {
	h.t.Helper()
	require.Eventually(h.t, func() bool {
		return h.helper.fetchLogsCalls.Load() > logCalls && h.helper.fetchViewCalls.Load() > viewCalls
	}, testEventuallyWait, testEventuallyTick)
}

func (h *mvServiceTestHarness) waitRefreshTask(mviewID int64) {
	h.t.Helper()
	waitInt64SignalReal(h.t, h.helper.refreshSignal, mviewID, testEventuallyWait, "refresh task")
}

func (h *mvServiceTestHarness) waitPurgeTask(mlogID int64) {
	h.t.Helper()
	waitInt64SignalReal(h.t, h.helper.purgeSignal, mlogID, testEventuallyWait, "purge task")
}

func (h *mvServiceTestHarness) assertNoPending() {
	h.t.Helper()
	require.Eventually(h.t, func() bool {
		mvLogCount, mvCount := pendingTaskCounts(h.svc)
		return mvLogCount == 0 && mvCount == 0
	}, testEventuallyWait, testEventuallyTick)
}

func TestMVServiceFullChainSimulation(t *testing.T) {
	module := installMockTimeForTest(t)

	h := newMVServiceTestHarness(t)
	startupLogCalls, startupViewCalls := h.fetchCallCounts()
	h.start()
	defer h.stop()

	// Wait for startup fetch cycle.
	h.waitFetchCycleSince(startupLogCalls, startupViewCalls)

	t.Run("schedule_and_execute", func(t *testing.T) {
		h.helper.drainSignals()
		baseLogCalls, baseViewCalls := h.fetchCallCounts()

		now := mvsNow()
		h.setMeta(
			map[int64]time.Time{2001: now.Add(-time.Second)},
			map[int64]time.Time{1001: now.Add(-time.Second)},
		)
		h.triggerDDL()

		h.waitFetchCycleSince(baseLogCalls, baseViewCalls)
		h.waitPurgeTask(2001)
		h.waitRefreshTask(1001)

		require.Eventually(t, func() bool {
			return h.svc.combinedTaskExecutorMetrics().finishedCount == 2
		}, testEventuallyWait, testEventuallyTick)
		assertCombinedTaskExecutorMetrics(t, h.svc, 2, 2, 0, 0, 0, 0, 0, 0)
		require.Equal(t, 1, h.helper.taskDurationCount(mvTaskDurationTypeRefresh, mvDurationResultSuccess))
		require.Equal(t, 1, h.helper.taskDurationCount(mvTaskDurationTypePurge, mvDurationResultSuccess))
	})

	t.Run("retry_then_success", func(t *testing.T) {
		h.helper.drainSignals()
		h.helper.refreshErr = errors.New("refresh failed")
		h.helper.purgeErr = errors.New("purge failed")

		baseMetrics := h.svc.combinedTaskExecutorMetrics()
		finishedBase := baseMetrics.finishedCount
		failedBase := baseMetrics.failedCount
		submittedBase := baseMetrics.submittedCount
		baseLogCalls, baseViewCalls := h.fetchCallCounts()

		now := mvsNow()
		h.setMeta(
			map[int64]time.Time{2002: now.Add(-time.Second)},
			map[int64]time.Time{1002: now.Add(-time.Second)},
		)
		h.triggerDDL()

		h.waitFetchCycleSince(baseLogCalls, baseViewCalls)
		h.waitPurgeTask(2002)
		h.waitRefreshTask(1002)
		require.Eventually(t, func() bool {
			metrics := h.svc.combinedTaskExecutorMetrics()
			return metrics.finishedCount >= finishedBase+2 &&
				metrics.failedCount >= failedBase+2 &&
				metrics.submittedCount >= submittedBase+2
		}, testEventuallyWait, testEventuallyTick)

		h.helper.refreshErr = nil
		h.helper.purgeErr = nil

		module.Advance(defaultMVTaskRetryBase + time.Millisecond)

		h.waitPurgeTask(2002)
		h.waitRefreshTask(1002)
		require.Eventually(t, func() bool {
			metrics := h.svc.combinedTaskExecutorMetrics()
			return metrics.finishedCount >= finishedBase+4 &&
				metrics.failedCount >= failedBase+2 &&
				metrics.submittedCount >= submittedBase+4
		}, testEventuallyWait, testEventuallyTick)

		require.Equal(t, 2, h.helper.taskDurationCount(mvTaskDurationTypeRefresh, mvDurationResultSuccess))
		require.Equal(t, 2, h.helper.taskDurationCount(mvTaskDurationTypePurge, mvDurationResultSuccess))
		require.Equal(t, 1, h.helper.taskDurationCount(mvTaskDurationTypeRefresh, mvDurationResultFailed))
		require.Equal(t, 1, h.helper.taskDurationCount(mvTaskDurationTypePurge, mvDurationResultFailed))
	})

	t.Run("remove_after_meta_deleted", func(t *testing.T) {
		h.helper.drainSignals()
		baseLogCalls, baseViewCalls := h.fetchCallCounts()
		h.setMeta(nil, nil)
		h.triggerDDL()

		h.waitFetchCycleSince(baseLogCalls, baseViewCalls)
		h.assertNoPending()
	})
}

func waitInt64SignalReal(t *testing.T, ch <-chan int64, expected int64, timeout time.Duration, hint string) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case got := <-ch:
			if got == expected {
				return
			}
		case <-deadline:
			t.Fatalf("timeout waiting for %s=%d", hint, expected)
		}
	}
}

func emitInt64(ch chan int64, v int64) {
	select {
	case ch <- v:
	default:
	}
}

func drainInt64Chan(ch chan int64) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func assertCombinedTaskExecutorMetrics(
	t *testing.T,
	svc *MVService,
	submitted, finished, failed, timeout, rejected int64,
	running, waiting, timedOutRunning int64,
) {
	t.Helper()
	metrics := svc.combinedTaskExecutorMetrics()
	require.Equal(t, submitted, metrics.submittedCount)
	require.Equal(t, finished, metrics.finishedCount)
	require.Equal(t, failed, metrics.failedCount)
	require.Equal(t, timeout, metrics.timeoutCount)
	require.Equal(t, rejected, metrics.rejectedCount)
	require.Equal(t, running, metrics.runningCount)
	require.Equal(t, waiting, metrics.waitingCount)
	require.Equal(t, timedOutRunning, metrics.timedOutRunningCount)
}
