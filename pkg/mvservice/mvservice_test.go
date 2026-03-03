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

package mvservice

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestTaskExecutorMaxConcurrency(t *testing.T) {
	installMockTimeForTest(t)
	exec := NewTaskExecutor(1, 0)
	require.True(t, exec.Run())
	require.False(t, exec.Run()) // idempotent
	defer exec.Close()

	started := make(chan string, 2)
	block := make(chan struct{})

	task := func(name string) func() error {
		return func() error {
			started <- name
			<-block
			return nil
		}
	}

	exec.Submit("t1", task("t1"))
	exec.Submit("t2", task("t2"))

	first := waitForStart(t, started, time.Hour)
	require.True(t, first == "t1" || first == "t2")

	select {
	case got := <-started:
		t.Fatalf("unexpected concurrent start: %s", got)
	default:
	}

	assertTaskExecutorMetrics(t, exec, 2, 0, 0, 0, 0, 1, 1, 0)

	close(block)

	second := waitForStart(t, started, time.Hour)
	require.NotEqual(t, first, second)

	waitForCount(t, exec.metrics.counters.finishedCount.Load, 2)
	assertTaskExecutorMetrics(t, exec, 2, 2, 0, 0, 0, 0, 0, 0)
}

func TestTaskExecutorUpdateMaxConcurrency(t *testing.T) {
	installMockTimeForTest(t)
	exec := NewTaskExecutor(1, 0)
	exec.Run()
	defer exec.Close()

	started := make(chan struct{}, 2)
	block := make(chan struct{})

	task := func() error {
		started <- struct{}{}
		<-block
		return nil
	}

	exec.Submit("t1", task)
	exec.Submit("t2", task)

	waitForSignal(t, started, time.Hour)

	select {
	case got := <-started:
		t.Fatalf("unexpected concurrent start: %v", got)
	default:
	}

	assertTaskExecutorMetrics(t, exec, 2, 0, 0, 0, 0, 1, 1, 0)

	exec.setMaxConcurrency(2)

	waitForSignal(t, started, time.Hour)

	assertTaskExecutorMetrics(t, exec, 2, 0, 0, 0, 0, 2, 0, 0)

	close(block)

	waitForCount(t, exec.metrics.counters.finishedCount.Load, 2)
	assertTaskExecutorMetrics(t, exec, 2, 2, 0, 0, 0, 0, 0, 0)
}

func TestTaskExecutorTimeoutReleasesSlot(t *testing.T) {
	module := installMockTimeForTest(t)
	exec := NewTaskExecutor(1, 50*time.Millisecond)
	exec.Run()
	defer exec.Close()

	started := make(chan string, 2)
	block := make(chan struct{})
	longStarted := make(chan struct{})

	exec.Submit("long", func() error {
		started <- "long"
		close(longStarted)
		<-block
		return nil
	})

	waitForSignal(t, longStarted, time.Hour)

	assertTaskExecutorMetrics(t, exec, 1, 0, 0, 0, 0, 1, 0, 0)

	module.Advance(500 * time.Millisecond)

	waitForCount(t, exec.metrics.counters.timeoutCount.Load, 1)
	waitForCount(t, exec.metrics.gauges.runningCount.Load, 0)
	waitForCount(t, exec.metrics.gauges.timedOutRunningCount.Load, 1)

	exec.Submit("short", func() error {
		started <- "short"
		return nil
	})

	waitForNamedStart(t, started, "short", time.Hour)

	close(block)

	waitForCount(t, exec.metrics.counters.finishedCount.Load, 2)
	assertTaskExecutorMetrics(t, exec, 2, 2, 0, 1, 0, 0, 0, 0)
}

func TestTaskExecutorUpdateTimeout(t *testing.T) {
	module := installMockTimeForTest(t)
	exec := NewTaskExecutor(1, 0)
	exec.Run()
	defer exec.Close()

	exec.setTimeout(40 * time.Millisecond)

	started := make(chan string, 2)
	block := make(chan struct{})
	longStarted := make(chan struct{})

	exec.Submit("long", func() error {
		started <- "long"
		close(longStarted)
		<-block
		return nil
	})

	waitForSignal(t, longStarted, time.Hour)

	assertTaskExecutorMetrics(t, exec, 1, 0, 0, 0, 0, 1, 0, 0)

	exec.Submit("short", func() error {
		started <- "short"
		return nil
	})

	module.Advance(40 * time.Millisecond)
	waitForNamedStart(t, started, "short", 300*time.Millisecond)
	waitForCount(t, exec.metrics.gauges.timedOutRunningCount.Load, 1)

	close(block)

	waitForCount(t, exec.metrics.counters.timeoutCount.Load, 1)
	waitForCount(t, exec.metrics.counters.finishedCount.Load, 2)
	assertTaskExecutorMetrics(t, exec, 2, 2, 0, 1, 0, 0, 0, 0)
}

func TestTaskExecutorRejectAfterClose(t *testing.T) {
	exec := NewTaskExecutor(1, 0)
	require.True(t, exec.Close())
	require.False(t, exec.Close())

	exec.Submit("rejected", func() error { return nil })

	assertTaskExecutorMetrics(t, exec, 0, 0, 0, 0, 1, 0, 0, 0)
}

type toggleBackpressureController struct {
	blocked atomic.Bool
	delay   time.Duration
}

func (c *toggleBackpressureController) ShouldBackpressure() (bool, time.Duration) {
	if c.blocked.Load() {
		return true, c.delay
	}
	return false, 0
}

type blockingBackpressureController struct {
	entered chan struct{}
	release chan struct{}
}

func (c *blockingBackpressureController) ShouldBackpressure() (bool, time.Duration) {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-c.release
	return false, 0
}

type signalBackpressureController struct {
	entered chan struct{}
	delay   time.Duration
}

func (c *signalBackpressureController) ShouldBackpressure() (bool, time.Duration) {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	return true, c.delay
}

func TestTaskExecutorBackpressure(t *testing.T) {
	module := installMockTimeForTest(t)
	exec := NewTaskExecutor(1, 0)
	exec.Run()
	defer exec.Close()

	controller := &toggleBackpressureController{
		delay: time.Second,
	}
	controller.blocked.Store(true)
	exec.SetBackpressureController(controller)

	started := make(chan struct{}, 1)
	done := make(chan struct{}, 1)

	exec.Submit("blocked", func() error {
		started <- struct{}{}
		done <- struct{}{}
		return nil
	})

	select {
	case <-started:
		t.Fatalf("task should not start under backpressure")
	default:
	}

	module.Advance(5 * time.Second)
	select {
	case <-started:
		t.Fatalf("task should still be blocked")
	default:
	}

	controller.blocked.Store(false)
	module.Advance(time.Second)
	waitForSignal(t, done, time.Hour)
}

func TestTaskExecutorBackpressureCheckDoesNotBlockSubmit(t *testing.T) {
	exec := NewTaskExecutor(1, 0)
	exec.Run()
	defer exec.Close()

	controller := &blockingBackpressureController{
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	exec.SetBackpressureController(controller)

	started := make(chan string, 2)
	exec.Submit("t1", func() error {
		started <- "t1"
		return nil
	})

	select {
	case <-controller.entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not reach backpressure controller")
	}

	submitDone := make(chan struct{})
	go func() {
		exec.Submit("t2", func() error {
			started <- "t2"
			return nil
		})
		close(submitDone)
	}()

	select {
	case <-submitDone:
	case <-time.After(time.Second):
		t.Fatal("submit should not be blocked by backpressure check")
	}

	close(controller.release)

	waitStarted := func(want string) {
		t.Helper()
		timeout := time.After(time.Second)
		for {
			select {
			case got := <-started:
				if got == want {
					return
				}
			case <-timeout:
				t.Fatalf("timeout waiting for %s to start", want)
			}
		}
	}
	waitStarted("t1")
	waitStarted("t2")
	require.Eventually(t, func() bool {
		return exec.metrics.counters.finishedCount.Load() == 2
	}, time.Second, 10*time.Millisecond)
}

func TestTaskExecutorCloseInterruptsBackpressureWait(t *testing.T) {
	exec := NewTaskExecutor(1, 0)
	exec.Run()

	controller := &signalBackpressureController{
		entered: make(chan struct{}, 1),
		delay:   time.Hour,
	}
	exec.SetBackpressureController(controller)

	exec.Submit("blocked", func() error {
		t.Fatal("task should not run under persistent backpressure")
		return nil
	})

	select {
	case <-controller.entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not reach backpressure check")
	}

	closed := make(chan struct{})
	go func() {
		exec.Close()
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close should interrupt backpressure wait")
	}
}

func TestCPUMemBackpressureController(t *testing.T) {
	controller := NewCPUMemBackpressureController(0.7, 0.8, 0)
	controller.getCPUUsage = func() (float64, bool) {
		return 0.9, false
	}
	controller.getMemTotal = func() uint64 { return 100 }
	controller.getMemUsed = func() (uint64, error) { return 20, nil }

	blocked, delay := controller.ShouldBackpressure()
	require.True(t, blocked)
	require.Equal(t, defaultTaskBackpressureDelay, delay)

	controller.getCPUUsage = func() (float64, bool) {
		return 0.1, false
	}
	controller.getMemUsed = func() (uint64, error) { return 90, nil }
	controller.Delay = 200 * time.Millisecond
	blocked, delay = controller.ShouldBackpressure()
	require.True(t, blocked)
	require.Equal(t, 200*time.Millisecond, delay)

	controller.getMemUsed = func() (uint64, error) { return 10, nil }
	blocked, delay = controller.ShouldBackpressure()
	require.False(t, blocked)
	require.Equal(t, time.Duration(0), delay)
}

func TestNewMVServiceConfig(t *testing.T) {
	t.Run("applied", func(t *testing.T) {
		cfg := DefaultMVServiceConfig()
		cfg.TaskMaxConcurrency = 3
		cfg.TaskTimeout = 42 * time.Second
		cfg.FetchInterval = 37 * time.Second
		cfg.BasicInterval = 2 * time.Second
		cfg.ServerRefreshInterval = 9 * time.Second
		cfg.RetryBaseDelay = 3 * time.Second
		cfg.RetryMaxDelay = 21 * time.Second
		cfg.HistoryGCInterval = 3 * time.Hour
		cfg.HistoryGCRetention = 15 * 24 * time.Hour
		cfg.ServerConsistentHashReplicas = 17
		cfg.TaskBackpressure = TaskBackpressureConfig{
			CPUThreshold: 0.7,
			MemThreshold: 0.8,
			Delay:        500 * time.Millisecond,
		}

		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, cfg)
		maxConcurrency, timeout := svc.GetTaskExecConfig()
		require.Equal(t, cfg.TaskMaxConcurrency, maxConcurrency)
		require.Equal(t, cfg.TaskTimeout, timeout)

		baseDelay, maxDelay := svc.GetRetryDelayConfig()
		require.Equal(t, cfg.RetryBaseDelay, baseDelay)
		require.Equal(t, cfg.RetryMaxDelay, maxDelay)

		historyGCInterval, historyGCRetention := svc.GetHistoryGCConfig()
		require.Equal(t, cfg.HistoryGCInterval, historyGCInterval)
		require.Equal(t, cfg.HistoryGCRetention, historyGCRetention)

		backpressureCfg := svc.GetTaskBackpressureConfig()
		require.Equal(t, cfg.TaskBackpressure, backpressureCfg)
		require.NotNil(t, svc.executor.backpressure.Load())

		require.Equal(t, cfg.FetchInterval, svc.fetchInterval)
		require.Equal(t, cfg.BasicInterval, svc.basicInterval)
		require.Equal(t, cfg.ServerRefreshInterval, svc.serverRefreshInterval)
		require.Equal(t, cfg.ServerConsistentHashReplicas, svc.sch.chash.replicas)
	})

	t.Run("normalized_defaults", func(t *testing.T) {
		cfg := DefaultMVServiceConfig()
		cfg.HistoryGCInterval = 0
		cfg.HistoryGCRetention = -time.Hour

		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, cfg)

		baseDelay, maxDelay := svc.GetRetryDelayConfig()
		require.Equal(t, cfg.RetryBaseDelay, baseDelay)
		require.Equal(t, cfg.RetryMaxDelay, maxDelay)

		historyGCInterval, historyGCRetention := svc.GetHistoryGCConfig()
		require.Equal(t, defaultMVHistoryGCInterval, historyGCInterval)
		require.Equal(t, defaultMVHistoryGCRetention, historyGCRetention)

		backpressureCfg := svc.GetTaskBackpressureConfig()
		require.Equal(t, cfg.TaskBackpressure, backpressureCfg)
		require.Nil(t, svc.executor.backpressure.Load())
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
		require.NotNil(t, svc.executor.backpressure.Load())

		err = svc.SetTaskBackpressureConfig(TaskBackpressureConfig{})
		require.NoError(t, err)
		require.Equal(t, TaskBackpressureConfig{}, svc.GetTaskBackpressureConfig())
		require.Nil(t, svc.executor.backpressure.Load())

		err = svc.SetTaskBackpressureConfig(TaskBackpressureConfig{
			CPUThreshold: -1,
			MemThreshold: 0.8,
		})
		require.Error(t, err)
	})

	t.Run("retry_delay", func(t *testing.T) {
		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, DefaultMVServiceConfig())

		baseDelay, maxDelay := svc.GetRetryDelayConfig()
		require.Equal(t, defaultMVTaskRetryBase, baseDelay)
		require.Equal(t, defaultMVTaskRetryMax, maxDelay)

		err := svc.SetRetryDelayConfig(2*time.Second, 10*time.Second)
		require.NoError(t, err)
		baseDelay, maxDelay = svc.GetRetryDelayConfig()
		require.Equal(t, 2*time.Second, baseDelay)
		require.Equal(t, 10*time.Second, maxDelay)
		require.Equal(t, 2*time.Second, svc.retryDelay(0))
		require.Equal(t, 4*time.Second, svc.retryDelay(2))
		require.Equal(t, 10*time.Second, svc.retryDelay(10))

		err = svc.SetRetryDelayConfig(0, 10*time.Second)
		require.Error(t, err)
		err = svc.SetRetryDelayConfig(10*time.Second, 2*time.Second)
		require.Error(t, err)
	})

	t.Run("history_gc", func(t *testing.T) {
		svc := NewMVService(context.Background(), mockSessionPool{}, &mockMVServiceHelper{}, DefaultMVServiceConfig())

		interval, retention := svc.GetHistoryGCConfig()
		require.Equal(t, defaultMVHistoryGCInterval, interval)
		require.Equal(t, defaultMVHistoryGCRetention, retention)

		err := svc.SetHistoryGCConfig(2*time.Hour, 14*24*time.Hour)
		require.NoError(t, err)

		interval, retention = svc.GetHistoryGCConfig()
		require.Equal(t, 2*time.Hour, interval)
		require.Equal(t, 14*24*time.Hour, retention)

		err = svc.SetHistoryGCConfig(0, 7*24*time.Hour)
		require.Error(t, err)
		err = svc.SetHistoryGCConfig(time.Hour, 0)
		require.Error(t, err)
	})
}

func TestTaskQueueRingBufferFIFO(t *testing.T) {
	var q taskQueue
	mkReq := func(i int) taskRequest {
		return taskRequest{
			name: fmt.Sprintf("t%d", i),
			task: func() error { return nil },
		}
	}

	for i := 1; i <= 4; i++ {
		q.push(mkReq(i))
	}
	for i := 1; i <= 2; i++ {
		req, ok := q.pop()
		require.True(t, ok)
		require.Equal(t, fmt.Sprintf("t%d", i), req.name)
	}

	for i := 5; i <= 12; i++ {
		q.push(mkReq(i))
	}
	require.Equal(t, 10, q.length())

	for i := 3; i <= 12; i++ {
		req, ok := q.pop()
		require.True(t, ok)
		require.Equal(t, fmt.Sprintf("t%d", i), req.name)
	}
	require.Equal(t, 0, q.length())

	_, ok := q.pop()
	require.False(t, ok)
}

func TestTaskQueueClearsReferences(t *testing.T) {
	var q taskQueue
	fn := func() error { return nil }
	q.push(taskRequest{name: "a", task: fn})

	_, ok := q.pop()
	require.True(t, ok)
	require.Equal(t, 0, q.length())
	require.GreaterOrEqual(t, len(q.buf), 1)
	require.Empty(t, q.buf[0].name)
	require.Nil(t, q.buf[0].task)

	q.push(taskRequest{name: "b", task: fn})
	q.push(taskRequest{name: "c", task: fn})
	pending := q.clear()
	require.Equal(t, 2, pending)
	require.Equal(t, 0, q.length())
	require.Nil(t, q.buf)
	require.Equal(t, 0, q.head)
}

type fullChainMVServiceHelper struct {
	*mockMVServiceHelper

	pendingMu   sync.RWMutex
	pendingLogs map[int64]time.Time
	pendingMVs  map[int64]time.Time

	refreshSignal chan int64
	purgeSignal   chan int64
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

func (h *fullChainMVServiceHelper) fetchAllTiDBMVLogPurge(context.Context, basic.SessionPool) (map[int64]*mvLog, error) {
	h.fetchLogsCalls.Add(1)
	if h.fetchLogsErr != nil {
		return nil, h.fetchLogsErr
	}

	h.pendingMu.RLock()
	defer h.pendingMu.RUnlock()
	ret := make(map[int64]*mvLog, len(h.pendingLogs))
	for id, next := range h.pendingLogs {
		logTask := &mvLog{
			ID:        id,
			nextPurge: next,
		}
		logTask.orderTs = next.UnixMilli()
		ret[id] = logTask
	}
	return ret, nil
}

func (h *fullChainMVServiceHelper) fetchAllTiDBMVRefresh(context.Context, basic.SessionPool) (map[int64]*mv, error) {
	h.fetchViewCalls.Add(1)
	if h.fetchViewsErr != nil {
		return nil, h.fetchViewsErr
	}

	h.pendingMu.RLock()
	defer h.pendingMu.RUnlock()
	ret := make(map[int64]*mv, len(h.pendingMVs))
	for id, next := range h.pendingMVs {
		mvTask := &mv{
			ID:          id,
			nextRefresh: next,
		}
		mvTask.orderTs = next.UnixMilli()
		ret[id] = mvTask
	}
	return ret, nil
}

func (h *fullChainMVServiceHelper) RefreshMV(_ context.Context, _ basic.SessionPool, mvID int64) (nextRefresh time.Time, err error) {
	h.lastRefreshID = mvID
	emitInt64(h.refreshSignal, mvID)
	if h.refreshErr != nil {
		return time.Time{}, h.refreshErr
	}
	return h.refreshNext, nil
}

func (h *fullChainMVServiceHelper) PurgeMVLog(_ context.Context, _ basic.SessionPool, mvLogID int64) (nextPurge time.Time, err error) {
	h.lastPurgeID = mvLogID
	emitInt64(h.purgeSignal, mvLogID)
	if h.purgeErr != nil {
		return time.Time{}, h.purgeErr
	}
	return h.purgeNext, nil
}

func pendingTaskCounts(svc *MVService) (mvLogCount int, mvCount int) {
	svc.mvLogPurgeMu.Lock()
	mvLogCount = len(svc.mvLogPurgeMu.pending)
	svc.mvLogPurgeMu.Unlock()

	svc.mvRefreshMu.Lock()
	mvCount = len(svc.mvRefreshMu.pending)
	svc.mvRefreshMu.Unlock()
	return mvLogCount, mvCount
}

type mvServiceTestHarness struct {
	t      *testing.T
	svc    *MVService
	helper *fullChainMVServiceHelper

	cancel  context.CancelFunc
	runDone chan struct{}
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
	case <-time.After(time.Second):
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
	}, time.Second, 10*time.Millisecond)
}

func (h *mvServiceTestHarness) waitRefreshTask(mvID int64) {
	h.t.Helper()
	waitInt64SignalReal(h.t, h.helper.refreshSignal, mvID, time.Second, "refresh task")
}

func (h *mvServiceTestHarness) waitPurgeTask(mvLogID int64) {
	h.t.Helper()
	waitInt64SignalReal(h.t, h.helper.purgeSignal, mvLogID, time.Second, "purge task")
}

func (h *mvServiceTestHarness) assertNoPending() {
	h.t.Helper()
	require.Eventually(h.t, func() bool {
		mvLogCount, mvCount := pendingTaskCounts(h.svc)
		return mvLogCount == 0 && mvCount == 0
	}, time.Second, 10*time.Millisecond)
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
			return h.svc.executor.metrics.counters.finishedCount.Load() == 2
		}, time.Second, 10*time.Millisecond)
		assertTaskExecutorMetrics(t, h.svc.executor, 2, 2, 0, 0, 0, 0, 0, 0)
		require.Equal(t, 1, h.helper.taskDurationCount(mvTaskDurationTypeRefresh, mvDurationResultSuccess))
		require.Equal(t, 1, h.helper.taskDurationCount(mvTaskDurationTypePurge, mvDurationResultSuccess))
	})

	t.Run("retry_then_success", func(t *testing.T) {
		h.helper.drainSignals()
		h.helper.refreshErr = errors.New("refresh failed")
		h.helper.purgeErr = errors.New("purge failed")

		finishedBase := h.svc.executor.metrics.counters.finishedCount.Load()
		failedBase := h.svc.executor.metrics.counters.failedCount.Load()
		submittedBase := h.svc.executor.metrics.counters.submittedCount.Load()
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
			return h.svc.executor.metrics.counters.finishedCount.Load() >= finishedBase+2 &&
				h.svc.executor.metrics.counters.failedCount.Load() >= failedBase+2 &&
				h.svc.executor.metrics.counters.submittedCount.Load() >= submittedBase+2
		}, time.Second, 10*time.Millisecond)

		h.helper.refreshErr = nil
		h.helper.purgeErr = nil

		module.Advance(defaultMVTaskRetryBase + time.Millisecond)

		h.waitPurgeTask(2002)
		h.waitRefreshTask(1002)
		require.Eventually(t, func() bool {
			return h.svc.executor.metrics.counters.finishedCount.Load() >= finishedBase+4 &&
				h.svc.executor.metrics.counters.failedCount.Load() >= failedBase+2 &&
				h.svc.executor.metrics.counters.submittedCount.Load() >= submittedBase+4
		}, time.Second, 10*time.Millisecond)

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

func waitForSignal(t *testing.T, ch <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-ch:
		return
	case <-mvsAfter(timeout):
		t.Fatalf("timeout waiting for signal")
	}
}

func assertTaskExecutorMetrics(
	t *testing.T,
	exec *TaskExecutor,
	submitted, finished, failed, timeout, rejected int64,
	running, waiting, timedOutRunning int64,
) {
	t.Helper()
	require.Equal(t, submitted, exec.metrics.counters.submittedCount.Load())
	require.Equal(t, finished, exec.metrics.counters.finishedCount.Load())
	require.Equal(t, failed, exec.metrics.counters.failedCount.Load())
	require.Equal(t, timeout, exec.metrics.counters.timeoutCount.Load())
	require.Equal(t, rejected, exec.metrics.counters.rejectedCount.Load())
	require.Equal(t, running, exec.metrics.gauges.runningCount.Load())
	require.Equal(t, waiting, exec.metrics.gauges.waitingCount.Load())
	require.Equal(t, timedOutRunning, exec.metrics.gauges.timedOutRunningCount.Load())
}

func waitForStart(t *testing.T, ch <-chan string, timeout time.Duration) string {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-mvsAfter(timeout):
		t.Fatalf("timeout waiting for start")
		return ""
	}
}

func waitForNamedStart(t *testing.T, ch <-chan string, want string, timeout time.Duration) {
	t.Helper()
	deadline := mvsAfter(timeout)
	for {
		select {
		case got := <-ch:
			if got == want {
				return
			}
		case <-deadline:
			t.Fatalf("timeout waiting for %s to start", want)
		}
	}
}

func waitForCount(t *testing.T, get func() int64, want int64) {
	t.Helper()
	for {
		if got := get(); got == want {
			return
		}
		mvsSleep(time.Hour)
	}
}
