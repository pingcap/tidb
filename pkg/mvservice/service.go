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
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type mlogPurgeTaskItem = Item[*mlogPurgeTask]
type mviewTaskItem = Item[*mviewTask]

// MVMetricsReporter reports MV service runtime metrics.
type MVMetricsReporter interface {
	reportMetrics(*MVService)
	observeTaskDuration(taskType, result string, duration time.Duration)
	observeRunEvent(eventType string)
}

// Helper provides all external dependencies required by MVService.
type Helper interface {
	ServerDiscovery
	MVTaskHandler
	MVMetricsReporter
}

// MVService is the in-memory scheduler and executor for MV refresh/purge tasks.
type MVService struct {
	sysSessionPool      basic.SessionPool
	lastMetaFetchMillis atomic.Int64
	ctx                 context.Context
	sch                 *ServerConsistentHash

	refreshExecutor *TaskExecutor
	purgeExecutor   *TaskExecutor

	refreshTaskConcurrencyRatioBits atomic.Uint64
	notifier                        Notifier
	ddlDirty                        atomic.Bool

	mh Helper

	fetchIntervalMillis   atomic.Int64
	basicInterval         time.Duration
	serverRefreshInterval time.Duration

	retryBaseDelayMillis atomic.Int64
	retryMaxDelayMillis  atomic.Int64
	backpressureMu       sync.RWMutex
	backpressureCfg      TaskBackpressureConfig

	metrics struct {
		mvCount                atomic.Int64
		mvLogCount             atomic.Int64
		runningMVRefreshCount  atomic.Int64
		runningMVLogPurgeCount atomic.Int64
	}

	mviewRefreshMu struct {
		sync.Mutex
		pending map[int64]mviewTaskItem
		prio    PriorityQueue[*mviewTask]
	}
	mlogPurgeMu struct {
		sync.Mutex
		pending map[int64]mlogPurgeTaskItem
		prio    PriorityQueue[*mlogPurgeTask]
	}
}

// DefaultMVRefreshTaskTimeout and DefaultMVPurgeTaskTimeout define the default
// per-task timeout budget for MV refresh and MV log purge.
const (
	DefaultMVRefreshTaskTimeout          = 5 * time.Minute
	DefaultMVPurgeTaskTimeout            = 10 * time.Minute
	defaultMVRefreshTaskConcurrencyRatio = 0.6
	defaultMVFetchInterval               = 30 * time.Second
	defaultMVBasicInterval               = time.Second
	defaultServerRefreshInterval         = 5 * time.Second
	defaultMVTaskRetryBase               = 10 * time.Second
	defaultMVTaskRetryMax                = 120 * time.Second
	manualCancelBackoffDelay             = 2 * time.Minute
	maxNextScheduleTs                    = 9e18

	defaultCHReplicas = 100

	mvTaskDurationTypeRefresh = "mv_refresh"
	mvTaskDurationTypePurge   = "mvlog_purge"

	mvFetchTypeMLogPurge    = "fetch_mlog"
	mvFetchTypeMViewRefresh = "fetch_mviews"

	mvDurationResultSuccess = "success"
	mvDurationResultFailed  = "failed"

	mvRunEventInitFailed         = "init_failed"
	mvRunEventServerChanged      = "server_changed"
	mvRunEventServerRefreshError = "server_refresh_error"
	mvRunEventFetchByDDL         = "fetch_meta_by_ddl"
	mvRunEventFetchByInterval    = "fetch_meta_by_interval"
)

type mviewTask struct {
	ID          int64
	nextRefresh time.Time

	orderTs    int64 // unix timestamp in milliseconds
	retryCount atomic.Int64
}

// mlogPurgeTask tracks scheduling state for one MV log purge task.
type mlogPurgeTask struct {
	ID        int64
	nextPurge time.Time

	orderTs    int64 // unix timestamp in milliseconds
	retryCount atomic.Int64
}

// TaskBackpressureConfig is the runtime config for task backpressure.
type TaskBackpressureConfig struct {
	CPUThreshold float64
	MemThreshold float64
	Delay        time.Duration
}

type taskExecutorMetricsSnapshot struct {
	submittedCount       int64
	finishedCount        int64
	failedCount          int64
	timeoutCount         int64
	rejectedCount        int64
	backpressureCount    int64
	runningCount         int64
	waitingCount         int64
	timedOutRunningCount int64
	backpressureBlocked  int64
}

func snapshotTaskExecutorMetrics(exec *TaskExecutor) taskExecutorMetricsSnapshot {
	if exec == nil {
		return taskExecutorMetricsSnapshot{}
	}
	return taskExecutorMetricsSnapshot{
		submittedCount:       exec.metrics.counters.submittedCount.Load(),
		finishedCount:        exec.metrics.counters.finishedCount.Load(),
		failedCount:          exec.metrics.counters.failedCount.Load(),
		timeoutCount:         exec.metrics.counters.timeoutCount.Load(),
		rejectedCount:        exec.metrics.counters.rejectedCount.Load(),
		backpressureCount:    exec.metrics.counters.backpressureCount.Load(),
		runningCount:         exec.metrics.gauges.runningCount.Load(),
		waitingCount:         exec.metrics.gauges.waitingCount.Load(),
		timedOutRunningCount: exec.metrics.gauges.timedOutRunningCount.Load(),
		backpressureBlocked:  exec.metrics.gauges.backpressureBlocked.Load(),
	}
}

func (m taskExecutorMetricsSnapshot) add(other taskExecutorMetricsSnapshot) taskExecutorMetricsSnapshot {
	return taskExecutorMetricsSnapshot{
		submittedCount:       m.submittedCount + other.submittedCount,
		finishedCount:        m.finishedCount + other.finishedCount,
		failedCount:          m.failedCount + other.failedCount,
		timeoutCount:         m.timeoutCount + other.timeoutCount,
		rejectedCount:        m.rejectedCount + other.rejectedCount,
		backpressureCount:    m.backpressureCount + other.backpressureCount,
		runningCount:         m.runningCount + other.runningCount,
		waitingCount:         m.waitingCount + other.waitingCount,
		timedOutRunningCount: m.timedOutRunningCount + other.timedOutRunningCount,
		backpressureBlocked:  m.backpressureBlocked + other.backpressureBlocked,
	}
}

func (m *mviewTask) Less(other *mviewTask) bool {
	return m.orderTs < other.orderTs
}

func (m *mlogPurgeTask) Less(other *mlogPurgeTask) bool {
	return m.orderTs < other.orderTs
}

func (t *MVService) combinedTaskExecutorMetrics() taskExecutorMetricsSnapshot {
	return snapshotTaskExecutorMetrics(t.refreshExecutor).
		add(snapshotTaskExecutorMetrics(t.purgeExecutor))
}

func loadTaskExecutorWaitingCount(exec *TaskExecutor) int64 {
	if exec == nil {
		return 0
	}
	return exec.metrics.gauges.waitingCount.Load()
}

func (t *MVService) runTaskExecutors() {
	if t.refreshExecutor != nil {
		t.refreshExecutor.Run()
	}
	if t.purgeExecutor != nil {
		t.purgeExecutor.Run()
	}
}

func (t *MVService) closeTaskExecutors() {
	if t.refreshExecutor != nil {
		t.refreshExecutor.Close()
	}
	if t.purgeExecutor != nil {
		t.purgeExecutor.Close()
	}
}

// fetchExecTasks collects due tasks from both queues and marks them as running.
func (t *MVService) fetchExecTasks(now time.Time) (mlogsToPurge []*mlogPurgeTask, mviewsToRefresh []*mviewTask) {
	nowTs := now.UnixMilli()
	{
		t.mlogPurgeMu.Lock() // guard mvlog purge queue
		for t.mlogPurgeMu.prio.Len() > 0 {
			it := t.mlogPurgeMu.prio.Front()
			l := it.Value
			if l.orderTs == maxNextScheduleTs {
				break
			}
			if l.orderTs > nowTs {
				break
			}
			mlogsToPurge = append(mlogsToPurge, l)
			l.orderTs = maxNextScheduleTs // set to max to avoid being picked again before reschedule
			t.mlogPurgeMu.prio.Update(it, l)
		}
		t.mlogPurgeMu.Unlock() // release mvlog purge queue guard
	}
	{
		t.mviewRefreshMu.Lock() // guard mv refresh queue
		for t.mviewRefreshMu.prio.Len() > 0 {
			it := t.mviewRefreshMu.prio.Front()
			m := it.Value
			if m.orderTs == maxNextScheduleTs {
				break
			}
			if m.orderTs > nowTs {
				break
			}
			mviewsToRefresh = append(mviewsToRefresh, m)
			m.orderTs = maxNextScheduleTs // set to max to avoid being picked again before reschedule
			t.mviewRefreshMu.prio.Update(it, m)
		}
		t.mviewRefreshMu.Unlock() // release mv refresh queue guard
	}
	return
}

// refreshMViews submits due refresh tasks to the task executor.
func (t *MVService) refreshMViews(mviewsToRefresh []*mviewTask) {
	if len(mviewsToRefresh) == 0 {
		return
	}
	for _, task := range mviewsToRefresh {
		mvTask := task
		mviewID := mvTask.ID
		t.refreshExecutor.Submit("mv-refresh/"+strconv.FormatInt(mviewID, 10), func() error {
			return t.executeRefreshTask(mvTask)
		})
	}
}

// purgeMLogs submits purge jobs to the task executor.
func (t *MVService) purgeMLogs(mlogsToPurge []*mlogPurgeTask) {
	if len(mlogsToPurge) == 0 {
		return
	}
	for _, l := range mlogsToPurge {
		t.purgeExecutor.Submit("mvlog-purge/"+strconv.FormatInt(l.ID, 10), func() error {
			return t.executePurgeTask(l)
		})
	}
}

func (t *MVService) executeRefreshTask(m *mviewTask) (err error) {
	if !t.hasPendingMViewTask(m) {
		return nil
	}

	t.metrics.runningMVRefreshCount.Add(1)
	defer t.metrics.runningMVRefreshCount.Add(-1)

	taskStart := mvsNow()
	var nextRefresh time.Time
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error(
				"refresh MV task panicked",
				zap.Int64("mview_id", m.ID),
				zap.Any("panic", r),
				zap.ByteString("stack", debug.Stack()),
			)
			err = fmt.Errorf("refresh MV task panicked: %v", r)
		}
		t.observeTaskDuration(mvTaskDurationTypeRefresh, taskStart, err)
		t.handleRefreshTaskResult(m, nextRefresh, err)
	}()

	nextRefresh, err = t.mh.RefreshMV(t.ctx, t.sysSessionPool, m.ID)
	return err
}

func (t *MVService) executePurgeTask(l *mlogPurgeTask) (err error) {
	if !t.hasPendingMLogPurgeTask(l) {
		return nil
	}

	t.metrics.runningMVLogPurgeCount.Add(1)
	defer t.metrics.runningMVLogPurgeCount.Add(-1)

	taskStart := mvsNow()
	var nextPurge time.Time
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error(
				"purge MV log task panicked",
				zap.Int64("mvlog_id", l.ID),
				zap.Any("panic", r),
				zap.ByteString("stack", debug.Stack()),
			)
			err = fmt.Errorf("purge MV log task panicked: %v", r)
		}
		t.observeTaskDuration(mvTaskDurationTypePurge, taskStart, err)
		t.handlePurgeTaskResult(l, nextPurge, err)
	}()

	nextPurge, err = t.mh.PurgeMVLog(t.ctx, t.sysSessionPool, l.ID)
	return err
}

// hasPendingMViewTask reports whether this exact refresh task is still tracked.
func (t *MVService) hasPendingMViewTask(m *mviewTask) bool {
	t.mviewRefreshMu.Lock()
	defer t.mviewRefreshMu.Unlock()

	it, ok := t.mviewRefreshMu.pending[m.ID]
	return ok && it.Value == m
}

// hasPendingMLogPurgeTask reports whether this exact purge task is still tracked.
func (t *MVService) hasPendingMLogPurgeTask(l *mlogPurgeTask) bool {
	t.mlogPurgeMu.Lock()
	defer t.mlogPurgeMu.Unlock()

	it, ok := t.mlogPurgeMu.pending[l.ID]
	return ok && it.Value == l
}

func (t *MVService) observeTaskDuration(taskType string, taskStart time.Time, err error) {
	result := mvDurationResultSuccess
	if err != nil {
		result = mvDurationResultFailed
	}
	t.mh.observeTaskDuration(taskType, result, mvsSince(taskStart))
}

func (t *MVService) runtimeLogFields() []zap.Field {
	fields := []zap.Field{
		zap.String("server_id", t.sch.ID),
		zap.Int64("mv_count", t.metrics.mvCount.Load()),
		zap.Int64("mvlog_count", t.metrics.mvLogCount.Load()),
		zap.Int64("running_refresh_count", t.metrics.runningMVRefreshCount.Load()),
		zap.Int64("running_purge_count", t.metrics.runningMVLogPurgeCount.Load()),
		zap.Int64("waiting_count", loadTaskExecutorWaitingCount(t.refreshExecutor)+loadTaskExecutorWaitingCount(t.purgeExecutor)),
	}
	return fields
}

func (t *MVService) handleRefreshTaskResult(m *mviewTask, nextRefresh time.Time, err error) {
	defer t.notifier.Wake()
	if err != nil {
		if isMVTaskCanceledManually(err) {
			m.retryCount.Store(0)
			nextRetryAt := mvsNow().Add(manualCancelBackoffDelay)
			applied, appliedNext, backoffErr := t.mh.TryBackoffRefreshManualCancel(t.ctx, t.sysSessionPool, m.ID, nextRetryAt)
			if backoffErr != nil {
				fields := append(t.runtimeLogFields(),
					zap.Int64("mview_id", m.ID),
					zap.Time("manual_cancel_backoff_at", nextRetryAt),
					zap.Error(backoffErr),
				)
				logutil.BgLogger().Warn("refresh MV manual cancel backoff persist failed", fields...)
			}
			if applied {
				if appliedNext.IsZero() {
					t.removeMViewTask(m)
					return
				}
				t.rescheduleMViewSuccess(m, appliedNext)
				return
			}
			t.rescheduleMView(m, nextRetryAt.UnixMilli())
			return
		}
		retryCount := m.retryCount.Add(1)
		retryDelay := t.retryDelay(retryCount)
		nextRetryAt := mvsNow().Add(retryDelay)
		t.rescheduleMView(m, nextRetryAt.UnixMilli())
		fields := append(t.runtimeLogFields(),
			zap.Int64("mview_id", m.ID),
			zap.Int64("failed_retry_count", retryCount),
			zap.Duration("retry_delay", retryDelay),
			zap.Time("next_retry_at", nextRetryAt),
			zap.Error(err),
		)
		logutil.BgLogger().Warn("refresh MV task failed, rescheduled for retry", fields...)
		return
	}
	if nextRefresh.IsZero() {
		m.retryCount.Store(0)
		t.removeMViewTask(m)
		return
	}
	m.retryCount.Store(0)
	t.rescheduleMViewSuccess(m, nextRefresh)
}

func (t *MVService) handlePurgeTaskResult(l *mlogPurgeTask, nextPurge time.Time, err error) {
	defer t.notifier.Wake()
	if err != nil {
		if isMVTaskCanceledManually(err) {
			l.retryCount.Store(0)
			nextRetryAt := mvsNow().Add(manualCancelBackoffDelay)
			applied, appliedNext, backoffErr := t.mh.TryBackoffPurgeManualCancel(t.ctx, t.sysSessionPool, l.ID, nextRetryAt)
			if backoffErr != nil {
				fields := append(t.runtimeLogFields(),
					zap.Int64("mvlog_id", l.ID),
					zap.Time("manual_cancel_backoff_at", nextRetryAt),
					zap.Error(backoffErr),
				)
				logutil.BgLogger().Warn("purge MV log manual cancel backoff persist failed", fields...)
			}
			if applied {
				if appliedNext.IsZero() {
					t.removeMLogPurgeTask(l)
					return
				}
				t.rescheduleMLogPurgeSuccess(l, appliedNext)
				return
			}
			t.rescheduleMLogPurge(l, nextRetryAt.UnixMilli())
			return
		}
		retryCount := l.retryCount.Add(1)
		retryDelay := t.retryDelay(retryCount)
		nextRetryAt := mvsNow().Add(retryDelay)
		t.rescheduleMLogPurge(l, nextRetryAt.UnixMilli())
		fields := append(t.runtimeLogFields(),
			zap.Int64("mvlog_id", l.ID),
			zap.Int64("failed_retry_count", retryCount),
			zap.Duration("retry_delay", retryDelay),
			zap.Time("next_retry_at", nextRetryAt),
			zap.Error(err),
		)
		logutil.BgLogger().Warn("purge MV log task failed, rescheduled for retry", fields...)
		return
	}
	if nextPurge.IsZero() {
		l.retryCount.Store(0)
		t.removeMLogPurgeTask(l)
		return
	}
	l.retryCount.Store(0)
	t.rescheduleMLogPurgeSuccess(l, nextPurge)
}

// removeMLogPurgeTask removes a purge task from the scheduler after completion.
func (t *MVService) removeMLogPurgeTask(l *mlogPurgeTask) {
	t.mlogPurgeMu.Lock() // guard mvlog purge queue
	if it, ok := t.mlogPurgeMu.pending[l.ID]; ok && it.Value == l {
		delete(t.mlogPurgeMu.pending, l.ID)
		t.mlogPurgeMu.prio.Remove(it)
	}
	t.metrics.mvLogCount.Store(int64(len(t.mlogPurgeMu.pending)))
	t.mlogPurgeMu.Unlock() // release mvlog purge queue guard
}

// removeMViewTask removes a refresh task from the scheduler after completion.
func (t *MVService) removeMViewTask(m *mviewTask) {
	t.mviewRefreshMu.Lock() // guard mv refresh queue
	if it, ok := t.mviewRefreshMu.pending[m.ID]; ok && it.Value == m {
		delete(t.mviewRefreshMu.pending, m.ID)
		t.mviewRefreshMu.prio.Remove(it)
	}
	t.metrics.mvCount.Store(int64(len(t.mviewRefreshMu.pending)))
	t.mviewRefreshMu.Unlock() // release mv refresh queue guard
}

// rescheduleMView reschedules a refresh task using a millisecond unix timestamp.
func (t *MVService) rescheduleMView(m *mviewTask, next int64) {
	t.mviewRefreshMu.Lock() // guard mv refresh queue
	if it, ok := t.mviewRefreshMu.pending[m.ID]; ok && it.Value == m {
		m.orderTs = next
		t.mviewRefreshMu.prio.Update(it, m)
	}
	t.mviewRefreshMu.Unlock() // release mv refresh queue guard
}

// rescheduleMViewSuccess applies the next refresh time from a successful execution.
func (t *MVService) rescheduleMViewSuccess(m *mviewTask, nextRefresh time.Time) {
	orderTs := nextRefresh.UnixMilli()

	t.mviewRefreshMu.Lock() // guard mv refresh queue
	if it, ok := t.mviewRefreshMu.pending[m.ID]; ok && it.Value == m {
		m.nextRefresh = nextRefresh
		m.orderTs = orderTs
		t.mviewRefreshMu.prio.Update(it, m)
	}
	t.mviewRefreshMu.Unlock() // release mv refresh queue guard
}

// rescheduleMLogPurge reschedules a purge task using a millisecond unix timestamp.
func (t *MVService) rescheduleMLogPurge(l *mlogPurgeTask, next int64) {
	t.mlogPurgeMu.Lock() // guard mvlog purge queue
	if it, ok := t.mlogPurgeMu.pending[l.ID]; ok && it.Value == l {
		l.orderTs = next
		t.mlogPurgeMu.prio.Update(it, l)
	}
	t.mlogPurgeMu.Unlock() // release mvlog purge queue guard
}

// rescheduleMLogPurgeSuccess applies the next purge time from a successful execution.
func (t *MVService) rescheduleMLogPurgeSuccess(l *mlogPurgeTask, nextPurge time.Time) {
	orderTs := nextPurge.UnixMilli()

	t.mlogPurgeMu.Lock() // guard mvlog purge queue
	if it, ok := t.mlogPurgeMu.pending[l.ID]; ok && it.Value == l {
		l.nextPurge = nextPurge
		l.orderTs = orderTs
		t.mlogPurgeMu.prio.Update(it, l)
	}
	t.mlogPurgeMu.Unlock() // release mvlog purge queue guard
}

// buildMLogPurgeTasks rebuilds purge task states from fetched metadata.
//
// For each item in newPending:
// 1. Update mutable metadata fields (nextPurge).
// 2. If nextPurge changed and the task is not currently running, update orderTs and heap position.
// 3. If the task is currently running (orderTs == maxNextScheduleTs), defer heap adjustment until task completion.
func (t *MVService) buildMLogPurgeTasks(newPending map[int64]*mlogPurgeTask) {
	t.mlogPurgeMu.Lock()         // guard mvlog purge queue
	defer t.mlogPurgeMu.Unlock() // release mvlog purge queue guard

	if t.mlogPurgeMu.pending == nil {
		t.mlogPurgeMu.pending = make(map[int64]mlogPurgeTaskItem, len(newPending))
	}
	for id, nl := range newPending {
		if ol, ok := t.mlogPurgeMu.pending[id]; ok {
			changed := ol.Value.nextPurge != nl.nextPurge
			ol.Value.nextPurge = nl.nextPurge
			if ol.Value.orderTs != maxNextScheduleTs { // not running
				if changed {
					ol.Value.orderTs = ol.Value.nextPurge.UnixMilli()
					t.mlogPurgeMu.prio.Update(ol, ol.Value)
				}
			}
			continue
		}
		t.mlogPurgeMu.pending[id] = t.mlogPurgeMu.prio.Push(nl)
	}
	for id, item := range t.mlogPurgeMu.pending {
		if _, ok := newPending[id]; ok {
			continue
		}
		delete(t.mlogPurgeMu.pending, id)
		t.mlogPurgeMu.prio.Remove(item)
	}

	t.metrics.mvLogCount.Store(int64(len(t.mlogPurgeMu.pending)))
}

// buildMViewRefreshTasks rebuilds refresh task states from fetched metadata.
func (t *MVService) buildMViewRefreshTasks(newPending map[int64]*mviewTask) {
	t.mviewRefreshMu.Lock()         // guard mv refresh queue
	defer t.mviewRefreshMu.Unlock() // release mv refresh queue guard

	if t.mviewRefreshMu.pending == nil {
		t.mviewRefreshMu.pending = make(map[int64]mviewTaskItem, len(newPending))
	}
	for id, nm := range newPending {
		if om, ok := t.mviewRefreshMu.pending[id]; ok {
			changed := om.Value.nextRefresh != nm.nextRefresh
			om.Value.nextRefresh = nm.nextRefresh
			if om.Value.orderTs != maxNextScheduleTs { // not running
				if changed {
					om.Value.orderTs = om.Value.nextRefresh.UnixMilli()
					t.mviewRefreshMu.prio.Update(om, om.Value)
				}
			}
		} else {
			t.mviewRefreshMu.pending[id] = t.mviewRefreshMu.prio.Push(nm)
		}
	}
	for id, item := range t.mviewRefreshMu.pending {
		if _, ok := newPending[id]; ok {
			continue
		}
		delete(t.mviewRefreshMu.pending, id)
		t.mviewRefreshMu.prio.Remove(item)
	}

	t.metrics.mvCount.Store(int64(len(t.mviewRefreshMu.pending)))
}

// filterUnownedTasks removes tasks that are not owned by this server.
// It checks all IDs under one hash-ring read lock to reduce lock contention.
func filterUnownedTasks[T any](sch *ServerConsistentHash, newPending map[int64]T) {
	if len(newPending) == 0 {
		return
	}
	sch.mu.RLock()
	for id := range newPending {
		if sch.chash.GetNode(int64KeyToBinaryBytes(id)) != sch.ID {
			delete(newPending, id)
		}
	}
	sch.mu.RUnlock()
}

// fetchAllTiDBMVLogPurge fetches purge metadata and filters out tasks not owned by this node.
func (t *MVService) fetchAllTiDBMVLogPurge() (map[int64]*mlogPurgeTask, error) {
	start := mvsNow()
	result := mvDurationResultSuccess
	defer func() {
		t.mh.observeTaskDuration(mvFetchTypeMLogPurge, result, mvsSince(start))
	}()

	newPending, err := t.mh.LoadAllTiDBMVLogPurge(t.ctx, t.sysSessionPool)
	if err != nil {
		result = mvDurationResultFailed
		fields := append(t.runtimeLogFields(), zap.Error(err))
		logutil.BgLogger().Warn("fetch all mvlog purge tasks failed", fields...)
		return nil, err
	}
	filterUnownedTasks(t.sch, newPending)
	return newPending, nil
}

// fetchAllTiDBMVRefresh fetches refresh metadata and filters out tasks not owned by this node.
func (t *MVService) fetchAllTiDBMVRefresh() (map[int64]*mviewTask, error) {
	start := mvsNow()
	result := mvDurationResultSuccess
	defer func() {
		t.mh.observeTaskDuration(mvFetchTypeMViewRefresh, result, mvsSince(start))
	}()

	newPending, err := t.mh.LoadAllTiDBMVRefresh(t.ctx, t.sysSessionPool)
	if err != nil {
		result = mvDurationResultFailed
		fields := append(t.runtimeLogFields(), zap.Error(err))
		logutil.BgLogger().Warn("fetch all materialized view refresh tasks failed", fields...)
		return nil, err
	}
	filterUnownedTasks(t.sch, newPending)
	return newPending, nil
}

// fetchAllMVMeta refreshes both purge and refresh task queues from metadata tables.
func (t *MVService) fetchAllMVMeta() error {
	newMLogPending, err := t.fetchAllTiDBMVLogPurge()
	if err != nil {
		return fmt.Errorf("fetch mvlog purge metadata failed: %w", err)
	}
	newMViewPending, err := t.fetchAllTiDBMVRefresh()
	if err != nil {
		return fmt.Errorf("fetch mview refresh metadata failed: %w", err)
	}
	t.buildMLogPurgeTasks(newMLogPending)
	t.buildMViewRefreshTasks(newMViewPending)

	t.lastMetaFetchMillis.Store(mvsNow().UnixMilli())
	return nil
}

// resetTimer safely resets timer to delay, draining the channel when needed.
func resetTimer(timer *mvsTimer, delay time.Duration) {
	if delay < 0 {
		delay = 0
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}

// NotifyDDLChange marks MV metadata as dirty and wakes the service loop.
func (t *MVService) NotifyDDLChange() {
	t.ddlDirty.Store(true)
	t.notifier.Wake()
}

// Run is the main scheduler loop for MVService.
// It refreshes server topology, fetches metadata, dispatches due tasks, and reports metrics.
func (t *MVService) Run() {
	if !t.sch.init() {
		t.mh.observeRunEvent(mvRunEventInitFailed)
		return
	}
	t.runTaskExecutors()
	timer := mvsNewTimer(0)
	maintenanceTimer := mvsNewTimer(t.basicInterval)

	defer func() {
		timer.Stop()
		maintenanceTimer.Stop()
		t.closeTaskExecutors()
		t.mh.reportMetrics(t)
	}()

	lastSrvRefresh := time.Time{}
	sawInitialServerRefresh := false
	for {
		ddlDirty := false
		maintenanceTick := false
		select {
		case <-timer.C:
		case <-maintenanceTimer.C:
			maintenanceTick = true
		case <-t.notifier.C:
			t.notifier.clear()
			ddlDirty = t.ddlDirty.Swap(false)
		case <-t.ctx.Done():
			return
		}

		now := mvsNow()
		if maintenanceTick {
			t.mh.reportMetrics(t)
			resetTimer(maintenanceTimer, t.basicInterval)
		}

		serverChanged := false
		if now.Sub(lastSrvRefresh) >= t.serverRefreshInterval {
			changed, err := t.sch.refresh()
			if err != nil {
				t.mh.observeRunEvent(mvRunEventServerRefreshError)
				fields := append(t.runtimeLogFields(), zap.Error(err))
				logutil.BgLogger().Warn("refresh all TiDB server info failed", fields...)
			} else {
				if sawInitialServerRefresh && changed {
					serverChanged = true
					t.mh.observeRunEvent(mvRunEventServerChanged)
				}
				sawInitialServerRefresh = true
			}
			lastSrvRefresh = now
		}

		needFetch := t.shouldFetchMVMeta(now)
		if ddlDirty || serverChanged || needFetch {
			if ddlDirty {
				t.mh.observeRunEvent(mvRunEventFetchByDDL)
			}
			if needFetch {
				t.mh.observeRunEvent(mvRunEventFetchByInterval)
			}
			// Fetch metadata on demand; errors are throttled via lastMetaFetchMillis update below.
			if err := t.fetchAllMVMeta(); err != nil {
				fields := append(t.runtimeLogFields(),
					zap.Bool("ddl_dirty", ddlDirty),
					zap.Bool("server_changed", serverChanged),
					zap.Bool("periodic_fetch", needFetch),
					zap.Error(err),
				)
				logutil.BgLogger().Warn("fetch materialized view metadata failed", fields...)
				// Keep retries bounded:
				// - periodic fetch failure keeps existing fetchInterval throttling.
				// - DDL/topology-triggered failure retries sooner to reduce stale-window.
				t.markFetchFailure(now, ddlDirty || serverChanged)
			}
		}

		mlogsToPurge, mviewsToRefresh := t.fetchExecTasks(now)
		t.purgeMLogs(mlogsToPurge)
		t.refreshMViews(mviewsToRefresh)

		next := t.nextScheduleTime(now)
		resetTimer(timer, mvsUntil(next))
	}
}

// markFetchFailure records a synthetic lastMetaFetchMillis to control next fetch time.
func (t *MVService) markFetchFailure(now time.Time, urgent bool) {
	fetchInterval := t.fetchInterval()
	if !urgent {
		t.lastMetaFetchMillis.Store(now.UnixMilli())
		return
	}

	retryDelay := t.basicInterval
	if retryDelay <= 0 {
		retryDelay = defaultMVBasicInterval
	}
	if retryDelay > fetchInterval {
		retryDelay = fetchInterval
	}
	// next fetch time = lastMetaFetchMillis + fetchInterval = now + retryDelay
	t.lastMetaFetchMillis.Store(now.Add(retryDelay - fetchInterval).UnixMilli())
}

// shouldFetchMVMeta reports whether a periodic metadata refresh is due.
func (t *MVService) shouldFetchMVMeta(now time.Time) bool {
	last := t.lastMetaFetchMillis.Load()
	if last == 0 {
		return true
	}
	return now.Sub(mvsUnixMilli(last)) >= t.fetchInterval()
}

// nextFetchTime returns the next periodic metadata refresh time.
func (t *MVService) nextFetchTime(now time.Time) time.Time {
	fetchInterval := t.fetchInterval()
	last := t.lastMetaFetchMillis.Load()
	if last == 0 {
		return now
	}
	next := mvsUnixMilli(last).Add(fetchInterval)
	if next.Before(now) {
		return now
	}
	return next
}

// nextDueTime returns the earliest due time among refresh and purge task queues.
func (t *MVService) nextDueTime() (time.Time, bool) {
	next := time.Time{}
	has := false
	{
		t.mviewRefreshMu.Lock()
		if item := t.mviewRefreshMu.prio.Front(); item != nil {
			next = mvsUnixMilli(item.Value.orderTs)
			has = true
		}
		t.mviewRefreshMu.Unlock()
	}

	{
		t.mlogPurgeMu.Lock()
		if item := t.mlogPurgeMu.prio.Front(); item != nil {
			due := mvsUnixMilli(item.Value.orderTs)
			if !has || due.Before(next) {
				next = due
				has = true
			}
		}
		t.mlogPurgeMu.Unlock()
	}
	return next, has
}

// nextScheduleTime returns the next wake-up time for the scheduler loop.
func (t *MVService) nextScheduleTime(now time.Time) time.Time {
	next := t.nextFetchTime(now)
	if due, ok := t.nextDueTime(); ok && due.Before(next) {
		next = due
	}
	if next.Before(now) {
		return now
	}
	return next
}
