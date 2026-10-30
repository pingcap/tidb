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
		cfg.MViewRefreshHistRetention = 10 * 24 * time.Hour
		cfg.MLogPurgeHistRetention = 20 * 24 * time.Hour
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

		historyGCInterval := svc.historyGCInterval()
		require.Equal(t, deriveHistoryGCInterval(cfg.MViewRefreshHistRetention, cfg.MLogPurgeHistRetention), historyGCInterval)
		mviewRefreshRetention, mlogPurgeRetention := svc.historyGCRetentionConfig()
		require.Equal(t, cfg.MViewRefreshHistRetention, mviewRefreshRetention)
		require.Equal(t, cfg.MLogPurgeHistRetention, mlogPurgeRetention)

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
		cfg.MViewRefreshHistRetention = -time.Hour
		cfg.MLogPurgeHistRetention = -time.Hour

		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, cfg)

		baseDelay, maxDelay := svc.retryDelayConfig()
		require.Equal(t, cfg.RetryBaseDelay, baseDelay)
		require.Equal(t, cfg.RetryMaxDelay, maxDelay)

		historyGCInterval := svc.historyGCInterval()
		require.Equal(t, defaultMVHistoryGCInterval, historyGCInterval)
		mviewRefreshRetention, mlogPurgeRetention := svc.historyGCRetentionConfig()
		require.Equal(t, defaultMVHistoryGCRetention, mviewRefreshRetention)
		require.Equal(t, defaultMVHistoryGCRetention, mlogPurgeRetention)

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

	t.Run("history_gc", func(t *testing.T) {
		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, DefaultMVServiceConfig())

		interval := svc.historyGCInterval()
		require.Equal(t, defaultMVHistoryGCInterval, interval)
		mviewRefreshRetention, mlogPurgeRetention := svc.historyGCRetentionConfig()
		require.Equal(t, defaultMVHistoryGCRetention, mviewRefreshRetention)
		require.Equal(t, defaultMVHistoryGCRetention, mlogPurgeRetention)

		err := svc.SetMViewRefreshHistRetention(10 * 24 * time.Hour)
		require.NoError(t, err)
		err = svc.SetMLogPurgeHistRetention(20 * 24 * time.Hour)
		require.NoError(t, err)
		interval = svc.historyGCInterval()
		require.Equal(t, 20*time.Minute, interval)
		mviewRefreshRetention, mlogPurgeRetention = svc.historyGCRetentionConfig()
		require.Equal(t, 10*24*time.Hour, mviewRefreshRetention)
		require.Equal(t, 20*24*time.Hour, mlogPurgeRetention)

		err = svc.SetMViewRefreshHistRetention(30 * time.Second)
		require.NoError(t, err)
		err = svc.SetMLogPurgeHistRetention(90 * time.Second)
		require.NoError(t, err)
		interval = svc.historyGCInterval()
		require.Equal(t, time.Second, interval)

		err = svc.SetMViewRefreshHistRetention(0)
		require.Error(t, err)
		err = svc.SetMLogPurgeHistRetention(0)
		require.Error(t, err)
	})

	t.Run("history_gc_reschedule_earlier_only", func(t *testing.T) {
		installMockTimeForTest(t)
		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, DefaultMVServiceConfig())
		now := mvsNow()

		svc.nextHistoryGCAtMillis.Store(now.Add(10 * time.Minute).UnixMilli())
		require.NoError(t, svc.SetMViewRefreshHistRetention(2*time.Minute))
		require.NoError(t, svc.SetMLogPurgeHistRetention(2*time.Minute))

		afterShrink := time.UnixMilli(svc.nextHistoryGCAtMillis.Load())
		require.True(t, afterShrink.Sub(now) <= time.Minute+time.Second)
		require.True(t, afterShrink.Sub(now) >= 0)

		svc.nextHistoryGCAtMillis.Store(now.Add(5 * time.Second).UnixMilli())
		require.NoError(t, svc.SetMViewRefreshHistRetention(12*time.Hour))
		require.NoError(t, svc.SetMLogPurgeHistRetention(12*time.Hour))
		afterGrow := time.UnixMilli(svc.nextHistoryGCAtMillis.Load())
		require.Equal(t, now.Add(1200*time.Millisecond).UnixMilli(), afterGrow.UnixMilli())
	})

	t.Run("history_gc_retry_cap", func(t *testing.T) {
		installMockTimeForTest(t)
		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, DefaultMVServiceConfig())
		now := mvsNow()
		interval := svc.historyGCInterval()
		svc.historyGCRetryCount.Store(historyGCRetryMaxAttempts)

		svc.scheduleHistoryGCFailure(now, interval)
		require.Equal(t, int64(0), svc.historyGCRetryCount.Load())
		require.Equal(t, now.Add(interval).UnixMilli(), svc.nextHistoryGCAtMillis.Load())
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

func TestMVServiceCollectRefreshAlertTasksByAlertLevel(t *testing.T) {
	svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, DefaultMVServiceConfig())
	defer svc.closeTaskExecutors()

	now := mvsNow()

	warnTask := &mviewTask{
		ID:              101,
		nextRefresh:     now.Add(-2 * time.Minute),
		lastSuccessTime: now.Add(-40 * time.Second),
		alertWarningSec: 30,
		alertOverdueSec: 120,
	}

	overdueTask := &mviewTask{
		ID:              102,
		nextRefresh:     now.Add(-2 * time.Minute),
		lastSuccessTime: now.Add(-70 * time.Second),
		alertWarningSec: 30,
		alertOverdueSec: 60,
	}
	overdueTask.orderTs = maxNextScheduleTs // simulate running task

	disabledTask := &mviewTask{
		ID:              103,
		nextRefresh:     now.Add(-2 * time.Minute),
		lastSuccessTime: now.Add(-5 * time.Minute),
	}
	addMVRefreshAlertTasksForTest(svc, warnTask, overdueTask, disabledTask)
	addMVRefreshExecutionTasksForTest(svc, warnTask, overdueTask, disabledTask)

	got, warningCount, overdueCount := svc.collectRefreshAlertTasks(now)
	require.Len(t, got, 2)
	require.Equal(t, int64(1), warningCount)
	require.Equal(t, int64(1), overdueCount)

	byID := make(map[int64]refreshAlertTask, len(got))
	for _, task := range got {
		byID[task.mviewID] = task
	}
	require.False(t, byID[101].overdue)
	require.Equal(t, "queued", byID[101].taskState)
	require.Equal(t, int64(30), byID[101].alertWarningSec)
	require.Equal(t, int64(120), byID[101].alertOverdueSec)
	require.True(t, byID[102].overdue)
	require.Equal(t, "running", byID[102].taskState)
	require.Equal(t, int64(30), byID[102].alertWarningSec)
	require.Equal(t, int64(60), byID[102].alertOverdueSec)
}

func TestMVServiceRefreshAlertScanInterval(t *testing.T) {
	svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, DefaultMVServiceConfig())
	defer svc.closeTaskExecutors()

	now := mvsNow()

	svc.mviewRefreshMu.Lock()
	if svc.mviewRefreshMu.pending == nil {
		svc.mviewRefreshMu.pending = make(map[int64]mviewTaskItem)
	}
	task := &mviewTask{
		ID:              1,
		nextRefresh:     now.Add(-2 * time.Minute),
		lastSuccessTime: now.Add(-2 * time.Minute),
		alertWarningSec: 30,
	}
	task.orderTs = task.nextRefresh.UnixMilli()
	svc.mviewRefreshMu.pending[task.ID] = svc.mviewRefreshMu.prio.Push(task)
	svc.mviewRefreshMu.Unlock()

	svc.nextRefreshAlertScanMillis.Store(0)
	svc.maybeLogRefreshAlertTasks(now)
	require.Equal(t, now.Add(mvRefreshAlertScanInterval).UnixMilli(), svc.nextRefreshAlertScanMillis.Load())

	// Within scan interval: should not trigger another scan.
	svc.maybeLogRefreshAlertTasks(now.Add(mvRefreshAlertScanInterval / 2))
	require.Equal(t, now.Add(mvRefreshAlertScanInterval).UnixMilli(), svc.nextRefreshAlertScanMillis.Load())

	// After interval: one more full scan and next scan time moves forward.
	svc.maybeLogRefreshAlertTasks(now.Add(mvRefreshAlertScanInterval))
	require.Equal(t, now.Add(2*mvRefreshAlertScanInterval).UnixMilli(), svc.nextRefreshAlertScanMillis.Load())
}

func TestMVServiceCollectRefreshAlertTasksDedupByLastSuccessReadTSO(t *testing.T) {
	type step struct {
		atOffset        time.Duration
		beforeCollect   func(task *mviewTask, now time.Time)
		expectedCount   int
		expectedID      int64
		expectedOverdue bool
		warningCount    int64
		overdueCount    int64
	}
	type testCase struct {
		name  string
		task  *mviewTask
		steps []step
	}

	now := mvsNow()
	testCases := []testCase{
		{
			name: "dedup_on_same_tso",
			task: &mviewTask{
				ID:                 201,
				nextRefresh:        now.Add(-2 * time.Minute),
				lastSuccessReadTSO: 123456,
				lastSuccessTime:    now.Add(-2 * time.Minute),
				alertWarningSec:    30,
			},
			steps: []step{
				{atOffset: 0, expectedCount: 1, expectedID: 201, expectedOverdue: false, warningCount: 1, overdueCount: 0},
				{atOffset: 10 * time.Second, expectedCount: 0, warningCount: 1, overdueCount: 0},
				{
					atOffset: 20 * time.Second,
					beforeCollect: func(task *mviewTask, now time.Time) {
						task.lastSuccessReadTSO = 223456
						task.lastSuccessTime = now.Add(-2 * time.Minute)
					},
					expectedCount: 1, expectedID: 201, expectedOverdue: false, warningCount: 1, overdueCount: 0,
				},
			},
		},
		{
			name: "dedup_by_alert_level",
			task: &mviewTask{
				ID:                 202,
				nextRefresh:        now.Add(-2 * time.Minute),
				lastSuccessReadTSO: 123456,
				lastSuccessTime:    now.Add(-40 * time.Second),
				alertWarningSec:    30,
				alertOverdueSec:    60,
			},
			steps: []step{
				{atOffset: 0, expectedCount: 1, expectedID: 202, expectedOverdue: false, warningCount: 1, overdueCount: 0},
				{atOffset: 10 * time.Second, expectedCount: 0, warningCount: 1, overdueCount: 0},
				{atOffset: 30 * time.Second, expectedCount: 1, expectedID: 202, expectedOverdue: true, warningCount: 0, overdueCount: 1},
				{atOffset: 40 * time.Second, expectedCount: 0, warningCount: 0, overdueCount: 1},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, DefaultMVServiceConfig())
			defer svc.closeTaskExecutors()

			task := tc.task
			task.orderTs = task.nextRefresh.UnixMilli()
			addMVRefreshAlertTasksForTest(svc, task)

			for _, s := range tc.steps {
				at := now.Add(s.atOffset)
				if s.beforeCollect != nil {
					s.beforeCollect(task, at)
				}
				got, warningCount, overdueCount := svc.collectRefreshAlertTasks(at)
				require.Len(t, got, s.expectedCount)
				require.Equal(t, s.warningCount, warningCount)
				require.Equal(t, s.overdueCount, overdueCount)
				if s.expectedCount > 0 {
					require.Equal(t, s.expectedID, got[0].mviewID)
					require.Equal(t, s.expectedOverdue, got[0].overdue)
				}
			}
		})
	}
}

func TestMVServiceMaybeLogRefreshAlertTasksSyncsAlertStates(t *testing.T) {
	helper := &mockMVServiceHelper{}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	defer svc.closeTaskExecutors()
	setRefreshAlertCleanupOwnerForTest(svc, 10)

	now := mvsNow()

	warnTask := &mviewTask{
		ID:                 301,
		schemaName:         "test",
		mviewName:          "mv_warn",
		nextRefresh:        now.Add(-2 * time.Minute),
		lastSuccessReadTSO: 12345,
		lastSuccessTime:    now.Add(-40 * time.Second),
		alertWarningSec:    30,
		alertOverdueSec:    120,
	}

	overdueTask := &mviewTask{
		ID:                 302,
		schemaName:         "test",
		mviewName:          "mv_overdue",
		nextRefresh:        now.Add(-2 * time.Minute),
		lastSuccessReadTSO: 22345,
		lastSuccessTime:    now.Add(-90 * time.Second),
		alertWarningSec:    30,
		alertOverdueSec:    60,
	}

	healthyTask := &mviewTask{
		ID:                 303,
		schemaName:         "test",
		mviewName:          "mv_healthy",
		nextRefresh:        now.Add(-2 * time.Minute),
		lastSuccessReadTSO: 32345,
		lastSuccessTime:    now.Add(-10 * time.Second),
		alertWarningSec:    30,
		alertOverdueSec:    60,
	}
	helper.fetchViews = map[int64]*mviewTask{
		warnTask.ID:    warnTask,
		overdueTask.ID: overdueTask,
		healthyTask.ID: healthyTask,
	}

	svc.nextRefreshAlertScanMillis.Store(0)
	svc.maybeLogRefreshAlertTasks(now)

	require.Equal(t, int32(1), helper.syncRefreshAlertCalls.Load())
	require.Equal(t, int64(1), svc.metrics.alertWarningCount.Load())
	require.Equal(t, int64(1), svc.metrics.alertOverdueCount.Load())

	helper.syncRefreshAlertMu.Lock()
	syncedAt := helper.lastRefreshAlertAt
	synced := append([]refreshAlertTask(nil), helper.lastRefreshAlertSync...)
	helper.syncRefreshAlertMu.Unlock()

	require.True(t, syncedAt.Equal(now))
	require.Len(t, synced, 3)

	byID := make(map[int64]refreshAlertTask, len(synced))
	for _, state := range synced {
		byID[state.mviewID] = state
	}
	require.Equal(t, mvRefreshAlertLevelWarning, byID[301].alertLevel)
	require.Equal(t, mvRefreshAlertLevelOverdue, byID[302].alertLevel)
	require.Empty(t, byID[303].alertLevel)

	svc.nextRefreshAlertScanMillis.Store(0)
	svc.maybeLogRefreshAlertTasks(now)
	require.Equal(t, int32(1), helper.syncRefreshAlertCalls.Load())
	require.Equal(t, int32(2), helper.cleanupStaleRefreshAlertCalls.Load())
}

func TestMVServiceMaybeLogRefreshAlertTasksCleansUpStaleRowsWithoutLocalPendingTasks(t *testing.T) {
	helper := &mockMVServiceHelper{}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	defer svc.closeTaskExecutors()
	setRefreshAlertCleanupOwnerForTest(svc, 10)

	now := mvsNow()

	svc.nextRefreshAlertScanMillis.Store(0)
	svc.maybeLogRefreshAlertTasks(now)
	require.Equal(t, int32(1), helper.cleanupStaleRefreshAlertCalls.Load())
	require.Equal(t, int32(0), helper.syncRefreshAlertCalls.Load())

	svc.maybeLogRefreshAlertTasks(now.Add(mvRefreshAlertScanInterval / 2))
	require.Equal(t, int32(1), helper.cleanupStaleRefreshAlertCalls.Load())

	svc.maybeLogRefreshAlertTasks(now.Add(mvRefreshAlertScanInterval))
	require.Equal(t, int32(2), helper.cleanupStaleRefreshAlertCalls.Load())
}

func TestMVServiceBuildMVRefreshTasksDoesNotClearRemovedAlertStates(t *testing.T) {
	helper := &mockMVServiceHelper{}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	defer svc.closeTaskExecutors()

	now := mvsNow()
	m := &mviewTask{
		ID:          401,
		nextRefresh: now.Add(time.Minute),
	}
	m.orderTs = m.nextRefresh.UnixMilli()

	svc.buildMViewRefreshTasks(map[int64]*mviewTask{m.ID: m})
	require.Equal(t, int32(0), helper.syncRefreshAlertCalls.Load())

	svc.buildMViewRefreshTasks(map[int64]*mviewTask{})
	require.Equal(t, int32(0), helper.syncRefreshAlertCalls.Load())
}

func TestMVServiceMaybeLogRefreshAlertTasksResyncsWhenLastSuccessReadTSOChanges(t *testing.T) {
	helper := &mockMVServiceHelper{}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	defer svc.closeTaskExecutors()
	setRefreshAlertCleanupOwnerForTest(svc, 10)

	now := mvsNow()
	task := &mviewTask{
		ID:                 501,
		schemaName:         "test",
		mviewName:          "mv_warn",
		nextRefresh:        now.Add(-2 * time.Minute),
		lastSuccessReadTSO: 12345,
		lastSuccessTime:    now.Add(-40 * time.Second),
		alertWarningSec:    30,
	}
	task.orderTs = task.nextRefresh.UnixMilli()
	helper.fetchViews = map[int64]*mviewTask{task.ID: task}

	svc.nextRefreshAlertScanMillis.Store(0)
	svc.maybeLogRefreshAlertTasks(now)
	require.Equal(t, int32(1), helper.syncRefreshAlertCalls.Load())

	task.lastSuccessReadTSO = 22345
	task.lastSuccessTime = now.Add(-35 * time.Second)

	svc.nextRefreshAlertScanMillis.Store(0)
	svc.maybeLogRefreshAlertTasks(now)
	require.Equal(t, int32(2), helper.syncRefreshAlertCalls.Load())
}

func TestMVServiceMaybeLogRefreshAlertTasksSkipsUnresolvedMetadata(t *testing.T) {
	helper := &mockMVServiceHelper{}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	defer svc.closeTaskExecutors()
	setRefreshAlertCleanupOwnerForTest(svc, 10)

	now := mvsNow()
	task := &mviewTask{
		ID:                 601,
		nextRefresh:        now.Add(-2 * time.Minute),
		lastSuccessReadTSO: 12345,
		lastSuccessTime:    now.Add(-40 * time.Second),
		alertWarningSec:    30,
		metadataUnresolved: true,
	}
	task.orderTs = task.nextRefresh.UnixMilli()
	helper.fetchViews = map[int64]*mviewTask{task.ID: task}

	svc.nextRefreshAlertScanMillis.Store(0)
	svc.maybeLogRefreshAlertTasks(now)
	require.Equal(t, int32(0), helper.syncRefreshAlertCalls.Load())
	require.Equal(t, int32(1), helper.cleanupStaleRefreshAlertCalls.Load())
	require.Equal(t, int64(0), svc.metrics.alertWarningCount.Load())
	require.Equal(t, int64(0), svc.metrics.alertOverdueCount.Load())
}

func TestMVServiceBuildMVRefreshTasksDoesNotDeleteAlertsOnTemporaryMetadataMiss(t *testing.T) {
	helper := &mockMVServiceHelper{}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	defer svc.closeTaskExecutors()
	setRefreshAlertCleanupOwnerForTest(svc, 10)

	now := mvsNow()
	existing := &mviewTask{
		ID:                     602,
		nextRefresh:            now.Add(-2 * time.Minute),
		schemaName:             "test",
		mviewName:              "mv",
		lastSuccessReadTSO:     12345,
		lastSuccessTime:        now.Add(-40 * time.Second),
		alertWarningSec:        30,
		lastSyncedAlertLevel:   mvRefreshAlertLevelWarning,
		lastSyncedAlertReadTSO: 12345,
		alertStateInitialized:  true,
	}
	existing.orderTs = existing.nextRefresh.UnixMilli()
	addMVRefreshAlertTasksForTest(svc, existing)
	helper.fetchViews = map[int64]*mviewTask{
		existing.ID: {
			ID:                 existing.ID,
			nextRefresh:        existing.nextRefresh,
			lastSuccessReadTSO: existing.lastSuccessReadTSO,
			lastSuccessTime:    existing.lastSuccessTime,
			metadataUnresolved: true,
		},
	}

	svc.nextRefreshAlertScanMillis.Store(0)
	svc.maybeLogRefreshAlertTasks(now)
	require.Equal(t, int32(0), helper.syncRefreshAlertCalls.Load())
	require.Equal(t, int32(1), helper.cleanupStaleRefreshAlertCalls.Load())

	svc.mviewRefreshAlertMu.Lock()
	preserved, ok := svc.mviewRefreshAlertMu.pending[existing.ID]
	svc.mviewRefreshAlertMu.Unlock()
	require.True(t, ok)
	require.NotNil(t, preserved)
	require.True(t, preserved.alertStateInitialized)
	require.Equal(t, mvRefreshAlertLevelWarning, preserved.lastSyncedAlertLevel)
	require.Equal(t, uint64(12345), preserved.lastSyncedAlertReadTSO)
}

func TestMVServiceMaybeLogRefreshAlertTasksSkipsCleanupWhenNotOwner(t *testing.T) {
	helper := &mockMVServiceHelper{}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	defer svc.closeTaskExecutors()
	setRefreshAlertCleanupOwnerForTest(svc, 20)

	now := mvsNow()

	svc.nextRefreshAlertScanMillis.Store(0)
	svc.maybeLogRefreshAlertTasks(now)
	require.Equal(t, int32(0), helper.cleanupStaleRefreshAlertCalls.Load())
	require.Equal(t, int32(0), helper.syncRefreshAlertCalls.Load())
}

func TestMVServiceRefreshAlertCheckerOwnersScanGlobalTasks(t *testing.T) {
	now := mvsNow()
	newTask := func() *mviewTask {
		return &mviewTask{
			ID:                 701,
			schemaName:         "test",
			mviewName:          "mv_overdue",
			nextRefresh:        now.Add(-2 * time.Minute),
			lastSuccessReadTSO: 12345,
			lastSuccessTime:    now.Add(-90 * time.Second),
			alertWarningSec:    30,
			alertOverdueSec:    60,
		}
	}
	testCases := []struct {
		name         string
		selfID       string
		checkerOwner bool
		metricsOwner bool
		fetchCalls   int32
		syncCalls    int32
		overdueCount int64
		taskState    string
	}{
		{
			name:         "first_server_reports_metrics",
			selfID:       "nodeA",
			checkerOwner: true,
			metricsOwner: true,
			fetchCalls:   1,
			syncCalls:    1,
			overdueCount: 1,
			taskState:    mvRefreshAlertTaskStateUnknown,
		},
		{
			name:         "second_server_does_not_report_metrics",
			selfID:       "nodeB",
			checkerOwner: true,
			fetchCalls:   1,
			syncCalls:    1,
			taskState:    mvRefreshAlertTaskStateUnknown,
		},
		{
			name:   "third_server_is_not_checker_owner",
			selfID: "nodeC",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			task := newTask()
			helper := &mockMVServiceHelper{
				fetchViews: map[int64]*mviewTask{task.ID: task},
			}
			svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
			defer svc.closeTaskExecutors()

			setThreeNodeRefreshAlertOwnersForTest(svc, tc.selfID)
			require.Equal(t, tc.checkerOwner, svc.isRefreshAlertCheckerOwner())
			require.Equal(t, tc.metricsOwner, svc.isRefreshAlertMetricsOwner())
			svc.metrics.alertWarningCount.Store(7)
			svc.metrics.alertOverdueCount.Store(7)
			reportedWarning, reportedOverdue := svc.refreshAlertMetricCounts()
			if tc.metricsOwner {
				require.Equal(t, int64(7), reportedWarning)
				require.Equal(t, int64(7), reportedOverdue)
			} else {
				require.Equal(t, int64(0), reportedWarning)
				require.Equal(t, int64(0), reportedOverdue)
			}
			svc.nextRefreshAlertScanMillis.Store(0)
			svc.maybeLogRefreshAlertTasks(now)
			require.Equal(t, tc.fetchCalls, helper.fetchViewCalls.Load())
			require.Equal(t, tc.syncCalls, helper.syncRefreshAlertCalls.Load())
			require.Equal(t, int64(0), svc.metrics.alertWarningCount.Load())
			require.Equal(t, tc.overdueCount, svc.metrics.alertOverdueCount.Load())
			if tc.syncCalls == 0 {
				return
			}
			helper.syncRefreshAlertMu.Lock()
			synced := append([]refreshAlertTask(nil), helper.lastRefreshAlertSync...)
			helper.syncRefreshAlertMu.Unlock()
			require.Len(t, synced, 1)
			require.Equal(t, task.ID, synced[0].mviewID)
			require.Equal(t, mvRefreshAlertLevelOverdue, synced[0].alertLevel)
			require.Equal(t, tc.taskState, synced[0].taskState)
		})
	}
}

func TestMVServiceRefreshAlertCheckerOwnerSingleServer(t *testing.T) {
	svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, DefaultMVServiceConfig())
	defer svc.closeTaskExecutors()

	svc.sch.mu.Lock()
	svc.sch.ID = "nodeA"
	svc.sch.rebuildLocked(map[string]serverInfo{
		"nodeA": {ID: "nodeA"},
	})
	svc.sch.mu.Unlock()

	require.True(t, svc.isRefreshAlertCheckerOwner())
	require.True(t, svc.isRefreshAlertMetricsOwner())

	svc.sch.mu.Lock()
	svc.sch.ID = "nodeB"
	svc.sch.mu.Unlock()

	require.False(t, svc.isRefreshAlertCheckerOwner())
	require.False(t, svc.isRefreshAlertMetricsOwner())
}

func TestMVServiceMaybeScanMVLogAccumulationAlertsFiltersUnownedTasks(t *testing.T) {
	helper := &mockMVServiceHelper{
		fetchAccumulationTasks: map[int64]*mlogAccumulationTask{
			101: {schemaName: "test", mlogName: "mlog_101", alertRows: 1000},
			102: {schemaName: "test", mlogName: "mlog_102", alertRows: 1000},
		},
		fetchAccumulationRowCounts: map[int64]uint64{
			101: 1500,
			102: 2500,
		},
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	defer svc.closeTaskExecutors()
	setTaskOwnersForTest(svc, map[int64]uint32{
		101: 10,
		102: 30,
	})

	now := mvsNow()
	svc.nextMVLogAccumulationScanMillis.Store(0)
	svc.maybeScanMVLogAccumulationAlerts(now)
	require.Eventually(t, func() bool {
		return !svc.mvLogAccumulationScanRunning.Load() && helper.fetchAccumulationTaskCalls.Load() == 1 &&
			helper.fetchAccumulationRowCountCalls.Load() == 1 &&
			svc.metrics.mvLogAccumulationCount.Load() == 1
	}, testEventuallyWait, testEventuallyTick)
	require.Equal(t, []int64{101}, helper.lastAccumulationRowCountTaskIDs)

	svc.maybeScanMVLogAccumulationAlerts(now.Add(mvLogAccumulationAlertScanInterval / 2))
	require.Equal(t, int32(1), helper.fetchAccumulationTaskCalls.Load())

	svc.nextMVLogAccumulationScanMillis.Store(0)
	helper.fetchAccumulationTasks = map[int64]*mlogAccumulationTask{
		101: {schemaName: "test", mlogName: "mlog_101", alertRows: 1000},
		102: {schemaName: "test", mlogName: "mlog_102", alertRows: 1000},
	}
	helper.fetchAccumulationRowCounts = map[int64]uint64{
		101: 1800,
		102: 2800,
	}
	svc.maybeScanMVLogAccumulationAlerts(now.Add(mvLogAccumulationAlertScanInterval))
	require.Eventually(t, func() bool {
		return !svc.mvLogAccumulationScanRunning.Load() && helper.fetchAccumulationTaskCalls.Load() == 2 &&
			helper.fetchAccumulationRowCountCalls.Load() == 2 &&
			svc.metrics.mvLogAccumulationCount.Load() == 1
	}, testEventuallyWait, testEventuallyTick)
	require.Equal(t, []int64{101}, helper.lastAccumulationRowCountTaskIDs)
}

func TestMVServiceMaybeScanMVLogAccumulationAlertsPreservesLastValueOnError(t *testing.T) {
	helper := &mockMVServiceHelper{
		fetchAccumulationTasks: map[int64]*mlogAccumulationTask{
			101: {schemaName: "test", mlogName: "mlog_101", alertRows: 1000},
		},
		fetchAccumulationRowCounts: map[int64]uint64{
			101: 1500,
		},
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	defer svc.closeTaskExecutors()
	setTaskOwnersForTest(svc, map[int64]uint32{
		101: 10,
	})

	now := mvsNow()
	svc.nextMVLogAccumulationScanMillis.Store(0)
	svc.maybeScanMVLogAccumulationAlerts(now)
	require.Eventually(t, func() bool {
		return !svc.mvLogAccumulationScanRunning.Load() && svc.metrics.mvLogAccumulationCount.Load() == 1
	}, testEventuallyWait, testEventuallyTick)

	helper.fetchAccumulationRowCountsErr = errors.New("mock accumulation scan failed")
	svc.nextMVLogAccumulationScanMillis.Store(0)
	svc.maybeScanMVLogAccumulationAlerts(now.Add(mvLogAccumulationAlertScanInterval))
	require.Eventually(t, func() bool {
		return !svc.mvLogAccumulationScanRunning.Load() && helper.fetchAccumulationRowCountCalls.Load() == 2
	}, testEventuallyWait, testEventuallyTick)
	require.Equal(t, int64(1), svc.metrics.mvLogAccumulationCount.Load())
}

func TestMVServiceMaybeScanMVLogAccumulationAlertsUsesStrictGreaterThan(t *testing.T) {
	helper := &mockMVServiceHelper{
		fetchAccumulationTasks: map[int64]*mlogAccumulationTask{
			101: {schemaName: "test", mlogName: "mlog_101", alertRows: 50},
		},
		fetchAccumulationRowCounts: map[int64]uint64{
			101: 50,
		},
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	defer svc.closeTaskExecutors()
	setTaskOwnersForTest(svc, map[int64]uint32{
		101: 10,
	})

	now := mvsNow()
	svc.nextMVLogAccumulationScanMillis.Store(0)
	svc.maybeScanMVLogAccumulationAlerts(now)
	require.Eventually(t, func() bool {
		return !svc.mvLogAccumulationScanRunning.Load() && helper.fetchAccumulationRowCountCalls.Load() == 1
	}, testEventuallyWait, testEventuallyTick)
	require.Equal(t, int64(0), svc.metrics.mvLogAccumulationCount.Load())
	require.Equal(t, []int64{101}, helper.lastAccumulationRowCountTaskIDs)
}

func TestMVServiceMaybeScanMVLogAccumulationAlertsAsyncAndNonOverlapping(t *testing.T) {
	helper := &mockMVServiceHelper{
		fetchAccumulationTasks: map[int64]*mlogAccumulationTask{
			101: {schemaName: "test", mlogName: "mlog_101", alertRows: 1000},
		},
		fetchAccumulationRowCounts: map[int64]uint64{
			101: 1500,
		},
		accumulationRowCountEntered: make(chan struct{}, 2),
		blockAccumulationRowCount:   make(chan struct{}),
	}
	svc := NewMVService(context.Background(), mockSessionPool{}, helper, DefaultMVServiceConfig())
	defer svc.closeTaskExecutors()
	setTaskOwnersForTest(svc, map[int64]uint32{
		101: 10,
	})

	now := mvsNow()
	svc.nextMVLogAccumulationScanMillis.Store(0)
	done := make(chan struct{})
	go func() {
		svc.maybeScanMVLogAccumulationAlerts(now)
		close(done)
	}()

	waitForSignal(t, helper.accumulationRowCountEntered, testEventuallyWait)
	waitForSignal(t, done, testEventuallyWait)
	require.Equal(t, int64(0), svc.metrics.mvLogAccumulationCount.Load())

	svc.nextMVLogAccumulationScanMillis.Store(0)
	svc.maybeScanMVLogAccumulationAlerts(now.Add(mvLogAccumulationAlertScanInterval))
	require.Equal(t, int32(1), helper.fetchAccumulationTaskCalls.Load())
	require.Equal(t, int32(1), helper.fetchAccumulationRowCountCalls.Load())

	close(helper.blockAccumulationRowCount)
	require.Eventually(t, func() bool {
		return !svc.mvLogAccumulationScanRunning.Load() && svc.metrics.mvLogAccumulationCount.Load() == 1 &&
			helper.fetchAccumulationTaskCalls.Load() == 1 &&
			helper.fetchAccumulationRowCountCalls.Load() == 1
	}, testEventuallyWait, testEventuallyTick)
}

func (h *fullChainMVServiceHelper) LoadAllTiDBMVLogAccumulationTasks(context.Context, basic.SessionPool) (map[int64]*mlogAccumulationTask, error) {
	h.fetchAccumulationTaskCalls.Add(1)
	if h.fetchAccumulationTasksErr != nil {
		return nil, h.fetchAccumulationTasksErr
	}
	if h.fetchAccumulationTasks == nil {
		return map[int64]*mlogAccumulationTask{}, nil
	}
	ret := make(map[int64]*mlogAccumulationTask, len(h.fetchAccumulationTasks))
	for id, task := range h.fetchAccumulationTasks {
		if task == nil {
			ret[id] = nil
			continue
		}
		taskCopy := *task
		ret[id] = &taskCopy
	}
	return ret, nil
}

func (h *fullChainMVServiceHelper) LoadTiDBMVLogAccumulationRowCounts(_ context.Context, _ basic.SessionPool, tasks map[int64]*mlogAccumulationTask) (map[int64]uint64, error) {
	h.fetchAccumulationRowCountCalls.Add(1)
	if h.fetchAccumulationRowCountsErr != nil {
		return nil, h.fetchAccumulationRowCountsErr
	}
	ret := make(map[int64]uint64, len(tasks))
	for id := range tasks {
		if rowCount, ok := h.fetchAccumulationRowCounts[id]; ok {
			ret[id] = rowCount
		}
	}
	return ret, nil
}

func (*fullChainMVServiceHelper) SyncMVRefreshAlertStates(context.Context, basic.SessionPool, time.Time, []refreshAlertTask) error {
	return nil
}

func (*fullChainMVServiceHelper) CleanupStaleMVRefreshAlerts(context.Context, basic.SessionPool) error {
	return nil
}
