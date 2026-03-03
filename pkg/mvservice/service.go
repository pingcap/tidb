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

type mvLogItem = Item[*mvLog]
type mvItem = Item[*mv]

// MVMetricsReporter reports MV service runtime metrics.
type MVMetricsReporter interface {
	reportMetrics(*MVService)
	observeTaskDuration(taskType, result string, duration time.Duration)
	observeFetchDuration(fetchType, result string, duration time.Duration)
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

	executor *TaskExecutor
	notifier Notifier
	ddlDirty atomic.Bool

	mh Helper

	fetchInterval            time.Duration
	basicInterval            time.Duration
	serverRefreshInterval    time.Duration
	historyGCIntervalMillis  atomic.Int64
	historyGCRetentionMillis atomic.Int64
	lastHistoryGCMillis      atomic.Int64

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

	mvRefreshMu struct {
		sync.Mutex
		pending map[int64]mvItem
		prio    PriorityQueue[*mv]
	}
	mvLogPurgeMu struct {
		sync.Mutex
		pending map[int64]mvLogItem
		prio    PriorityQueue[*mvLog]
	}
}

const (
	defaultMVTaskTimeout         = 5 * time.Minute
	defaultMVFetchInterval       = 30 * time.Second
	defaultMVBasicInterval       = time.Second
	defaultServerRefreshInterval = 5 * time.Second
	defaultMVHistoryGCInterval   = time.Hour
	defaultMVHistoryGCRetention  = 7 * 24 * time.Hour
	defaultMVTaskRetryBase       = 10 * time.Second
	defaultMVTaskRetryMax        = 120 * time.Second
	maxNextScheduleTs            = 9e18

	defaultCHReplicas = 20

	mvTaskDurationTypeRefresh = "mv_refresh"
	mvTaskDurationTypePurge   = "mvlog_purge"

	mvFetchTypeMLogPurge    = "fetch_mlog"
	mvFetchTypeMViewRefresh = "fetch_mviews"

	mvTaskDurationTypeHistoryGC = "history_gc"

	mvDurationResultSuccess = "success"
	mvDurationResultFailed  = "failed"

	mvRunEventInitFailed         = "init_failed"
	mvRunEventRecoveredPanic     = "mv_service_panic"
	mvRunEventServerRefreshOK    = "server_refresh_ok"
	mvRunEventServerRefreshError = "server_refresh_error"
	mvRunEventFetchByDDL         = "fetch_meta_trigger_ddl"
	mvRunEventFetchByInterval    = "fetch_meta_trigger_interval"
	mvRunEventFetchMLogOK        = "fetch_mlog_ok"
	mvRunEventFetchMLogErr       = "fetch_mlog_error"
	mvRunEventFetchMViewOK       = "fetch_mviews_ok"
	mvRunEventFetchMViewErr      = "fetch_mviews_error"
	mvRunEventHistoryGCGetTSOErr = "history_gc_get_tso_error"

	mvHistoryGCOwnerKey = "mv-history-gc"
)

type mv struct {
	ID              int64
	refreshInterval time.Duration
	nextRefresh     time.Time

	orderTs    int64 // unix timestamp in milliseconds
	retryCount atomic.Int64
}

// mvLog tracks scheduling state for one MV log purge task.
type mvLog struct {
	ID            int64
	purgeInterval time.Duration
	nextPurge     time.Time

	orderTs    int64 // unix timestamp in milliseconds
	retryCount atomic.Int64
}

// TaskBackpressureConfig is the runtime config for task backpressure.
type TaskBackpressureConfig struct {
	CPUThreshold float64
	MemThreshold float64
	Delay        time.Duration
}

func (m *mv) Less(other *mv) bool {
	return m.orderTs < other.orderTs
}

func (m *mvLog) Less(other *mvLog) bool {
	return m.orderTs < other.orderTs
}

// fetchExecTasks collects due tasks from both queues and marks them as running.
func (t *MVService) fetchExecTasks(now time.Time) (mvLogToPurge []*mvLog, mvToRefresh []*mv) {
	nowTs := now.UnixMilli()
	{
		t.mvLogPurgeMu.Lock() // guard mvlog purge queue
		for t.mvLogPurgeMu.prio.Len() > 0 {
			it := t.mvLogPurgeMu.prio.Front()
			l := it.Value
			if l.orderTs == maxNextScheduleTs {
				break
			}
			if l.orderTs > nowTs {
				break
			}
			mvLogToPurge = append(mvLogToPurge, l)
			l.orderTs = maxNextScheduleTs // set to max to avoid being picked again before reschedule
			t.mvLogPurgeMu.prio.Update(it, l)
		}
		t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard
	}
	{
		t.mvRefreshMu.Lock() // guard mv refresh queue
		for t.mvRefreshMu.prio.Len() > 0 {
			it := t.mvRefreshMu.prio.Front()
			m := it.Value
			if m.orderTs == maxNextScheduleTs {
				break
			}
			if m.orderTs > nowTs {
				break
			}
			mvToRefresh = append(mvToRefresh, m)
			m.orderTs = maxNextScheduleTs // set to max to avoid being picked again before reschedule
			t.mvRefreshMu.prio.Update(it, m)
		}
		t.mvRefreshMu.Unlock() // release mv refresh queue guard
	}
	return
}

func (t *MVService) refreshMV(mvToRefresh []*mv) {
	if len(mvToRefresh) == 0 {
		return
	}
	for _, m := range mvToRefresh {
		t.executor.Submit("mv-refresh/"+strconv.FormatInt(m.ID, 10), func() error {
			return t.executeRefreshTask(m)
		})
	}
}

// purgeMVLog submits purge jobs to the task executor.
func (t *MVService) purgeMVLog(mvLogToPurge []*mvLog) {
	if len(mvLogToPurge) == 0 {
		return
	}
	for _, l := range mvLogToPurge {
		t.executor.Submit("mvlog-purge/"+strconv.FormatInt(l.ID, 10), func() error {
			return t.executePurgeTask(l)
		})
	}
}

func (t *MVService) executeRefreshTask(m *mv) error {
	if !t.hasPendingMVTask(m) {
		return nil
	}

	t.metrics.runningMVRefreshCount.Add(1)
	defer t.metrics.runningMVRefreshCount.Add(-1)

	taskStart := mvsNow()
	nextRefresh, err := t.mh.RefreshMV(t.ctx, t.sysSessionPool, m.ID)
	t.observeTaskDuration(mvTaskDurationTypeRefresh, taskStart, err)
	return t.handleRefreshTaskResult(m, nextRefresh, err)
}

func (t *MVService) executePurgeTask(l *mvLog) error {
	if !t.hasPendingMVLogTask(l) {
		return nil
	}

	t.metrics.runningMVLogPurgeCount.Add(1)
	defer t.metrics.runningMVLogPurgeCount.Add(-1)

	taskStart := mvsNow()
	nextPurge, err := t.mh.PurgeMVLog(t.ctx, t.sysSessionPool, l.ID)
	t.observeTaskDuration(mvTaskDurationTypePurge, taskStart, err)
	return t.handlePurgeTaskResult(l, nextPurge, err)
}

// hasPendingMVTask reports whether this exact refresh task is still tracked.
func (t *MVService) hasPendingMVTask(m *mv) bool {
	t.mvRefreshMu.Lock()
	defer t.mvRefreshMu.Unlock()

	it, ok := t.mvRefreshMu.pending[m.ID]
	return ok && it.Value == m
}

// hasPendingMVLogTask reports whether this exact purge task is still tracked.
func (t *MVService) hasPendingMVLogTask(l *mvLog) bool {
	t.mvLogPurgeMu.Lock()
	defer t.mvLogPurgeMu.Unlock()

	it, ok := t.mvLogPurgeMu.pending[l.ID]
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
		zap.Int64("mv_pending_count", t.metrics.mvCount.Load()),
		zap.Int64("mvlog_pending_count", t.metrics.mvLogCount.Load()),
		zap.Int64("running_refresh_count", t.metrics.runningMVRefreshCount.Load()),
		zap.Int64("running_purge_count", t.metrics.runningMVLogPurgeCount.Load()),
		zap.Int64("executor_running_count", t.executor.metrics.gauges.runningCount.Load()),
		zap.Int64("executor_waiting_count", t.executor.metrics.gauges.waitingCount.Load()),
	}
	if lastMetaFetchTS := t.lastMetaFetchMillis.Load(); lastMetaFetchTS > 0 {
		fields = append(fields, zap.Time("last_meta_fetch_time", mvsUnixMilli(lastMetaFetchTS)))
	}
	return fields
}

func (t *MVService) handleRefreshTaskResult(m *mv, nextRefresh time.Time, err error) error {
	defer t.notifier.Wake()
	if err != nil {
		retryCount := m.retryCount.Add(1)
		retryDelay := t.retryDelay(retryCount)
		nextRetryAt := mvsNow().Add(retryDelay)
		t.rescheduleMV(m, nextRetryAt.UnixMilli())
		fields := append(t.runtimeLogFields(),
			zap.Int64("mview_id", m.ID),
			zap.Int64("retry_count", retryCount),
			zap.Duration("retry_delay", retryDelay),
			zap.Time("next_retry_at", nextRetryAt),
			zap.Error(err),
		)
		logutil.BgLogger().Warn("refresh MV task failed, rescheduled for retry", fields...)
		return err
	}
	if nextRefresh.IsZero() {
		m.retryCount.Store(0)
		t.removeMVTask(m)
		return nil
	}
	m.retryCount.Store(0)
	t.rescheduleMVSuccess(m, nextRefresh)
	return nil
}

func (t *MVService) handlePurgeTaskResult(l *mvLog, nextPurge time.Time, err error) error {
	defer t.notifier.Wake()
	if err != nil {
		retryCount := l.retryCount.Add(1)
		retryDelay := t.retryDelay(retryCount)
		nextRetryAt := mvsNow().Add(retryDelay)
		t.rescheduleMVLog(l, nextRetryAt.UnixMilli())
		fields := append(t.runtimeLogFields(),
			zap.Int64("mvlog_id", l.ID),
			zap.Int64("retry_count", retryCount),
			zap.Duration("retry_delay", retryDelay),
			zap.Time("next_retry_at", nextRetryAt),
			zap.Error(err),
		)
		logutil.BgLogger().Warn("purge MV log task failed, rescheduled for retry", fields...)
		return err
	}
	if nextPurge.IsZero() {
		l.retryCount.Store(0)
		t.removeMVLogTask(l)
		return nil
	}
	l.retryCount.Store(0)
	t.rescheduleMVLogSuccess(l, nextPurge)
	return nil
}

// removeMVLogTask removes a purge task from the scheduler after completion.
func (t *MVService) removeMVLogTask(l *mvLog) {
	t.mvLogPurgeMu.Lock() // guard mvlog purge queue
	if it, ok := t.mvLogPurgeMu.pending[l.ID]; ok && it.Value == l {
		delete(t.mvLogPurgeMu.pending, l.ID)
		t.mvLogPurgeMu.prio.Remove(it)
	}
	t.metrics.mvLogCount.Store(int64(len(t.mvLogPurgeMu.pending)))
	t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard
}

// removeMVTask removes a refresh task from the scheduler after completion.
func (t *MVService) removeMVTask(m *mv) {
	t.mvRefreshMu.Lock() // guard mv refresh queue
	if it, ok := t.mvRefreshMu.pending[m.ID]; ok && it.Value == m {
		delete(t.mvRefreshMu.pending, m.ID)
		t.mvRefreshMu.prio.Remove(it)
	}
	t.metrics.mvCount.Store(int64(len(t.mvRefreshMu.pending)))
	t.mvRefreshMu.Unlock() // release mv refresh queue guard
}

// rescheduleMV reschedules a refresh task using a millisecond unix timestamp.
func (t *MVService) rescheduleMV(m *mv, next int64) {
	t.mvRefreshMu.Lock() // guard mv refresh queue
	if it, ok := t.mvRefreshMu.pending[m.ID]; ok && it.Value == m {
		m.orderTs = next
		t.mvRefreshMu.prio.Update(it, m)
	}
	t.mvRefreshMu.Unlock() // release mv refresh queue guard
}

// rescheduleMVSuccess applies the next refresh time from a successful execution.
func (t *MVService) rescheduleMVSuccess(m *mv, nextRefresh time.Time) {
	orderTs := nextRefresh.UnixMilli()

	t.mvRefreshMu.Lock() // guard mv refresh queue
	if it, ok := t.mvRefreshMu.pending[m.ID]; ok && it.Value == m {
		m.nextRefresh = nextRefresh
		m.orderTs = orderTs
		t.mvRefreshMu.prio.Update(it, m)
	}
	t.mvRefreshMu.Unlock() // release mv refresh queue guard
}

// rescheduleMVLog reschedules a purge task using a millisecond unix timestamp.
func (t *MVService) rescheduleMVLog(l *mvLog, next int64) {
	t.mvLogPurgeMu.Lock() // guard mvlog purge queue
	if it, ok := t.mvLogPurgeMu.pending[l.ID]; ok && it.Value == l {
		l.orderTs = next
		t.mvLogPurgeMu.prio.Update(it, l)
	}
	t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard
}

// rescheduleMVLogSuccess applies the next purge time from a successful execution.
func (t *MVService) rescheduleMVLogSuccess(l *mvLog, nextPurge time.Time) {
	orderTs := nextPurge.UnixMilli()

	t.mvLogPurgeMu.Lock() // guard mvlog purge queue
	if it, ok := t.mvLogPurgeMu.pending[l.ID]; ok && it.Value == l {
		l.nextPurge = nextPurge
		l.orderTs = orderTs
		t.mvLogPurgeMu.prio.Update(it, l)
	}
	t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard
}

// buildMVLogPurgeTasks rebuilds purge task states from fetched metadata.
//
// For each item in newPending:
// 1. Update mutable metadata fields (purgeInterval, nextPurge).
// 2. If nextPurge changed and the task is not currently running, update orderTs and heap position.
// 3. If the task is currently running (orderTs == maxNextScheduleTs), defer heap adjustment until task completion.
func (t *MVService) buildMVLogPurgeTasks(newPending map[int64]*mvLog) {
	t.mvLogPurgeMu.Lock()         // guard mvlog purge queue
	defer t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard

	if t.mvLogPurgeMu.pending == nil {
		t.mvLogPurgeMu.pending = make(map[int64]mvLogItem, len(newPending))
	}
	for id, nl := range newPending {
		if ol, ok := t.mvLogPurgeMu.pending[id]; ok {
			ol.Value.purgeInterval = nl.purgeInterval
			changed := ol.Value.nextPurge != nl.nextPurge
			ol.Value.nextPurge = nl.nextPurge
			if ol.Value.orderTs != maxNextScheduleTs { // not running
				if changed {
					ol.Value.orderTs = ol.Value.nextPurge.UnixMilli()
					t.mvLogPurgeMu.prio.Update(ol, ol.Value)
				}
			}
			continue
		}
		t.mvLogPurgeMu.pending[id] = t.mvLogPurgeMu.prio.Push(nl)
	}
	for id, item := range t.mvLogPurgeMu.pending {
		if _, ok := newPending[id]; ok {
			continue
		}
		delete(t.mvLogPurgeMu.pending, id)
		t.mvLogPurgeMu.prio.Remove(item)
	}

	t.metrics.mvLogCount.Store(int64(len(t.mvLogPurgeMu.pending)))
}

// buildMVRefreshTasks rebuilds refresh task states from fetched metadata.
func (t *MVService) buildMVRefreshTasks(newPending map[int64]*mv) {
	t.mvRefreshMu.Lock()         // guard mv refresh queue
	defer t.mvRefreshMu.Unlock() // release mv refresh queue guard

	if t.mvRefreshMu.pending == nil {
		t.mvRefreshMu.pending = make(map[int64]mvItem, len(newPending))
	}
	for id, nm := range newPending {
		if om, ok := t.mvRefreshMu.pending[id]; ok {
			om.Value.refreshInterval = nm.refreshInterval
			changed := om.Value.nextRefresh != nm.nextRefresh
			om.Value.nextRefresh = nm.nextRefresh
			if om.Value.orderTs != maxNextScheduleTs { // not running
				if changed {
					om.Value.orderTs = om.Value.nextRefresh.UnixMilli()
					t.mvRefreshMu.prio.Update(om, om.Value)
				}
			}
		} else {
			t.mvRefreshMu.pending[id] = t.mvRefreshMu.prio.Push(nm)
		}
	}
	for id, item := range t.mvRefreshMu.pending {
		if _, ok := newPending[id]; ok {
			continue
		}
		delete(t.mvRefreshMu.pending, id)
		t.mvRefreshMu.prio.Remove(item)
	}

	t.metrics.mvCount.Store(int64(len(t.mvRefreshMu.pending)))
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
func (t *MVService) fetchAllTiDBMVLogPurge() (map[int64]*mvLog, error) {
	start := mvsNow()
	result := mvDurationResultSuccess
	defer func() {
		t.mh.observeFetchDuration(mvFetchTypeMLogPurge, result, mvsSince(start))
	}()

	newPending, err := t.mh.fetchAllTiDBMVLogPurge(t.ctx, t.sysSessionPool)
	if err != nil {
		result = mvDurationResultFailed
		t.mh.observeRunEvent(mvRunEventFetchMLogErr)
		fields := append(t.runtimeLogFields(), zap.Error(err))
		logutil.BgLogger().Warn("fetch all mvlog purge tasks failed", fields...)
		return nil, err
	}
	t.mh.observeRunEvent(mvRunEventFetchMLogOK)
	filterUnownedTasks(t.sch, newPending)
	return newPending, nil
}

// fetchAllTiDBMVRefresh fetches refresh metadata and filters out tasks not owned by this node.
func (t *MVService) fetchAllTiDBMVRefresh() (map[int64]*mv, error) {
	start := mvsNow()
	result := mvDurationResultSuccess
	defer func() {
		t.mh.observeFetchDuration(mvFetchTypeMViewRefresh, result, mvsSince(start))
	}()

	newPending, err := t.mh.fetchAllTiDBMVRefresh(t.ctx, t.sysSessionPool)
	if err != nil {
		result = mvDurationResultFailed
		t.mh.observeRunEvent(mvRunEventFetchMViewErr)
		fields := append(t.runtimeLogFields(), zap.Error(err))
		logutil.BgLogger().Warn("fetch all materialized view refresh tasks failed", fields...)
		return nil, err
	}
	t.mh.observeRunEvent(mvRunEventFetchMViewOK)
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
	t.buildMVLogPurgeTasks(newMLogPending)
	t.buildMVRefreshTasks(newMViewPending)

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

func (t *MVService) maybeGCOperationHistory(now time.Time) {
	historyGCInterval, historyGCRetention := t.GetHistoryGCConfig()
	nowMillis := now.UnixMilli()
	lastMillis := t.lastHistoryGCMillis.Load()
	if lastMillis > 0 && nowMillis-lastMillis < historyGCInterval.Milliseconds() {
		return
	}
	if !t.sch.AvailableString(mvHistoryGCOwnerKey) {
		return
	}
	// Throttle retries by interval even when GC fails.
	t.lastHistoryGCMillis.Store(nowMillis)
	startAt := mvsNow()
	result := mvDurationResultSuccess
	defer func() {
		t.mh.observeTaskDuration(mvTaskDurationTypeHistoryGC, result, mvsSince(startAt))
	}()

	currentTSO, err := t.mh.GetCurrentTSO(t.ctx, t.sysSessionPool)
	if err != nil {
		result = mvDurationResultFailed
		t.mh.observeRunEvent(mvRunEventHistoryGCGetTSOErr)
		fields := append(t.runtimeLogFields(), zap.Error(err))
		logutil.BgLogger().Warn("get current tso failed when GC MV/MVLOG operation history", fields...)
		return
	}
	if err := t.mh.PurgeMVHistoryBeforeTSO(t.ctx, t.sysSessionPool, currentTSO, historyGCRetention); err != nil {
		result = mvDurationResultFailed
		fields := append(t.runtimeLogFields(),
			zap.Uint64("current_tso", currentTSO),
			zap.Duration("history_gc_retention", historyGCRetention),
			zap.Error(err),
		)
		logutil.BgLogger().Warn("GC MV/MVLOG operation history failed", fields...)
		return
	}
}

// NotifyDDLChange marks MV metadata as dirty and wakes the service loop.
func (t *MVService) NotifyDDLChange() {
	t.ddlDirty.Store(true)
	t.notifier.Wake()
}

// Run is the main scheduler loop for MVService.
// It refreshes server topology, fetches metadata, dispatches due tasks, and reports metrics.
func (t *MVService) Run() {
	defer func() {
		if r := recover(); r != nil {
			t.mh.observeRunEvent(mvRunEventRecoveredPanic)
			fields := append(t.runtimeLogFields(), zap.Any("panic", r), zap.ByteString("stack", debug.Stack()))
			logutil.BgLogger().Error("MVService panicked", fields...)
		}
	}()
	if !t.sch.init() {
		t.mh.observeRunEvent(mvRunEventInitFailed)
		return
	}
	t.executor.Run()
	timer := mvsNewTimer(0)
	maintenanceTimer := mvsNewTimer(t.basicInterval)

	defer func() {
		timer.Stop()
		maintenanceTimer.Stop()
		t.executor.Close()
		t.mh.reportMetrics(t)
	}()

	lastSrvRefresh := time.Time{}
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
			t.maybeGCOperationHistory(now)
			resetTimer(maintenanceTimer, t.basicInterval)
		}

		if now.Sub(lastSrvRefresh) >= t.serverRefreshInterval {
			if err := t.sch.refresh(); err != nil {
				t.mh.observeRunEvent(mvRunEventServerRefreshError)
				fields := append(t.runtimeLogFields(), zap.Error(err))
				logutil.BgLogger().Warn("refresh all TiDB server info failed", fields...)
			} else {
				t.mh.observeRunEvent(mvRunEventServerRefreshOK)
			}
			lastSrvRefresh = now
		}

		needFetch := t.shouldFetchMVMeta(now)
		if ddlDirty || needFetch {
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
					zap.Bool("periodic_fetch", needFetch),
					zap.Error(err),
				)
				logutil.BgLogger().Warn("fetch materialized view metadata failed", fields...)
				// Keep retries bounded:
				// - periodic fetch failure keeps existing fetchInterval throttling.
				// - DDL-triggered failure retries sooner to reduce stale-window.
				t.markFetchFailure(now, ddlDirty)
			}
		}

		mvLogToPurge, mvToRefresh := t.fetchExecTasks(now)
		t.purgeMVLog(mvLogToPurge)
		t.refreshMV(mvToRefresh)

		next := t.nextScheduleTime(now)
		resetTimer(timer, mvsUntil(next))
	}
}

// markFetchFailure records a synthetic lastMetaFetchMillis to control next fetch time.
func (t *MVService) markFetchFailure(now time.Time, ddlTriggered bool) {
	if !ddlTriggered {
		t.lastMetaFetchMillis.Store(now.UnixMilli())
		return
	}

	retryDelay := t.basicInterval
	if retryDelay <= 0 {
		retryDelay = defaultMVBasicInterval
	}
	if retryDelay > t.fetchInterval {
		retryDelay = t.fetchInterval
	}
	// next fetch time = lastMetaFetchMillis + fetchInterval = now + retryDelay
	t.lastMetaFetchMillis.Store(now.Add(retryDelay - t.fetchInterval).UnixMilli())
}

// shouldFetchMVMeta reports whether a periodic metadata refresh is due.
func (t *MVService) shouldFetchMVMeta(now time.Time) bool {
	last := t.lastMetaFetchMillis.Load()
	if last == 0 {
		return true
	}
	return now.Sub(mvsUnixMilli(last)) >= t.fetchInterval
}

// nextFetchTime returns the next periodic metadata refresh time.
func (t *MVService) nextFetchTime(now time.Time) time.Time {
	last := t.lastMetaFetchMillis.Load()
	if last == 0 {
		return now
	}
	next := mvsUnixMilli(last).Add(t.fetchInterval)
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
		t.mvRefreshMu.Lock()
		if item := t.mvRefreshMu.prio.Front(); item != nil {
			next = mvsUnixMilli(item.Value.orderTs)
			has = true
		}
		t.mvRefreshMu.Unlock()
	}

	{
		t.mvLogPurgeMu.Lock()
		if item := t.mvLogPurgeMu.prio.Front(); item != nil {
			due := mvsUnixMilli(item.Value.orderTs)
			if !has || due.Before(next) {
				next = due
				has = true
			}
		}
		t.mvLogPurgeMu.Unlock()
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
