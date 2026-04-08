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

	nextRefreshAlertScanMillis atomic.Int64

	executor *TaskExecutor
	notifier Notifier
	ddlDirty atomic.Bool

	mh Helper

	fetchIntervalMillis             atomic.Int64
	basicInterval                   time.Duration
	serverRefreshInterval           time.Duration
	mviewRefreshHistRetentionMillis atomic.Int64
	mlogPurgeHistRetentionMillis    atomic.Int64
	nextHistoryGCAtMillis           atomic.Int64
	historyGCRetryCount             atomic.Int64
	historyGCRunning                atomic.Bool

	retryBaseDelayMillis atomic.Int64
	retryMaxDelayMillis  atomic.Int64
	backpressureMu       sync.RWMutex
	backpressureCfg      TaskBackpressureConfig

	metrics struct {
		mvCount                atomic.Int64
		mvLogCount             atomic.Int64
		runningMVRefreshCount  atomic.Int64
		runningMVLogPurgeCount atomic.Int64
		alertWarningCount      atomic.Int64
		alertOverdueCount      atomic.Int64
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
	defaultMVHistoryGCRetention  = 365 * 24 * time.Hour
	defaultMVTaskRetryBase       = 10 * time.Second
	defaultMVTaskRetryMax        = 120 * time.Second
	mvRefreshAlertScanInterval   = 30 * time.Second
	maxNextScheduleTs            = 9e18

	defaultCHReplicas = 100

	mvTaskDurationTypeRefresh = "mv_refresh"
	mvTaskDurationTypePurge   = "mvlog_purge"

	mvFetchTypeMLogPurge    = "fetch_mlog"
	mvFetchTypeMViewRefresh = "fetch_mviews"

	mvTaskDurationTypeHistoryGC = "history_gc"

	mvDurationResultSuccess = "success"
	mvDurationResultFailed  = "failed"

	mvRunEventInitFailed         = "init_failed"
	mvRunEventRecoveredPanic     = "mv_service_panic"
	mvRunEventServerChanged      = "server_changed"
	mvRunEventServerRefreshError = "server_refresh_error"
	mvRunEventFetchByDDL         = "fetch_meta_by_ddl"
	mvRunEventFetchByInterval    = "fetch_meta_by_interval"
	mvRunEventHistoryGCGetTSOErr = "get_tso_error"

	mvHistoryGCOwnerKey = "gc-mv-op-hist"

	historyGCRetryMaxAttempts = 8
)

type mv struct {
	ID          int64
	nextRefresh time.Time
	schemaName  string
	mviewName   string

	lastSuccessReadTSO uint64
	lastSuccessTime    time.Time
	alertWarningSec    int64
	alertOverdueSec    int64
	// Suppress repeated overdue logs for the same lastSuccessReadTSO.
	lastLoggedWarningTSO uint64
	lastLoggedOverdueTSO uint64

	orderTs    int64 // unix timestamp in milliseconds
	retryCount atomic.Int64
}

// mvLog tracks scheduling state for one MV log purge task.
type mvLog struct {
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

// refreshMV submits due refresh tasks to the task executor.
func (t *MVService) refreshMV(mvToRefresh []*mv) {
	if len(mvToRefresh) == 0 {
		return
	}
	for _, task := range mvToRefresh {
		mvTask := task
		mviewID := mvTask.ID
		t.executor.Submit("mv-refresh/"+strconv.FormatInt(mviewID, 10), func() error {
			return t.executeRefreshTask(mvTask)
		})
	}
}

type refreshAlertTask struct {
	mviewID         int64
	schemaName      string
	mviewName       string
	nextRefresh     time.Time
	lastSuccessTime time.Time
	alertWarningSec int64
	alertOverdueSec int64
	retryCount      int64
	taskState       string
	overdue         bool
}

// maybeLogRefreshAlertTasks scans pending refresh tasks and logs warning/overdue alerts.
// The scan runs on maintenanceTick and is rate-limited to once per minute.
func (t *MVService) maybeLogRefreshAlertTasks(now time.Time) {
	nowMillis := now.UnixMilli()
	if next := t.nextRefreshAlertScanMillis.Load(); next > nowMillis {
		return
	}
	t.nextRefreshAlertScanMillis.Store(now.Add(mvRefreshAlertScanInterval).UnixMilli())

	alertTasks, warningCount, overdueCount := t.collectRefreshAlertTasks(now)
	t.metrics.alertWarningCount.Store(warningCount)
	t.metrics.alertOverdueCount.Store(overdueCount)
	if len(alertTasks) == 0 {
		return
	}
	t.logMVRefreshAlerts(alertTasks)
}

func (t *MVService) collectRefreshAlertTasks(now time.Time) ([]refreshAlertTask, int64, int64) {
	t.mvRefreshMu.Lock()
	defer t.mvRefreshMu.Unlock()

	alertTasks := make([]refreshAlertTask, 0)
	var warningCount int64
	var overdueCount int64
	for mviewID, item := range t.mvRefreshMu.pending {
		if item.Value == nil || item.Value.nextRefresh.IsZero() {
			continue
		}
		lastSuccessTime := item.Value.lastSuccessTime
		if lastSuccessTime.IsZero() {
			continue
		}
		if now.Before(lastSuccessTime) {
			continue
		}
		overdueDuration := now.Sub(lastSuccessTime)
		overdue := false
		if item.Value.alertOverdueSec > 0 && overdueDuration >= time.Duration(item.Value.alertOverdueSec)*time.Second {
			overdue = true
		} else if item.Value.alertWarningSec > 0 && overdueDuration >= time.Duration(item.Value.alertWarningSec)*time.Second {
			overdue = false
		} else {
			continue
		}
		if overdue {
			overdueCount++
		} else {
			warningCount++
		}
		if tso := item.Value.lastSuccessReadTSO; tso > 0 {
			if overdue {
				if item.Value.lastLoggedOverdueTSO == tso {
					continue
				}
				item.Value.lastLoggedOverdueTSO = tso
			} else {
				// If overdue has already been logged for this tso, skip warning downgrade logs.
				if item.Value.lastLoggedWarningTSO == tso || item.Value.lastLoggedOverdueTSO == tso {
					continue
				}
				item.Value.lastLoggedWarningTSO = tso
			}
		}
		taskState := "queued"
		if item.Value.orderTs == maxNextScheduleTs {
			taskState = "running"
		}
		alertTasks = append(alertTasks, refreshAlertTask{
			mviewID:         mviewID,
			schemaName:      item.Value.schemaName,
			mviewName:       item.Value.mviewName,
			nextRefresh:     item.Value.nextRefresh,
			lastSuccessTime: lastSuccessTime,
			alertWarningSec: item.Value.alertWarningSec,
			alertOverdueSec: item.Value.alertOverdueSec,
			retryCount:      item.Value.retryCount.Load(),
			taskState:       taskState,
			overdue:         overdue,
		})
	}
	return alertTasks, warningCount, overdueCount
}

// logMVRefreshAlerts logs refresh alert task details with warning/overdue levels.
func (t *MVService) logMVRefreshAlerts(alertTasks []refreshAlertTask) {
	for _, task := range alertTasks {
		fields := append(t.runtimeLogFields(),
			zap.Int64("mview_id", task.mviewID),
			zap.String("schema", task.schemaName),
			zap.String("mview", task.mviewName),
			zap.Time("next_refresh", task.nextRefresh),
			zap.Time("last_success_time", task.lastSuccessTime),
			zap.Int64("alert_warning_sec", task.alertWarningSec),
			zap.Int64("alert_overdue_sec", task.alertOverdueSec),
			zap.Int64("failed_retry_count", task.retryCount),
			zap.String("state", task.taskState),
		)
		if task.overdue {
			logutil.BgLogger().Error("Materialized_view_refresh_time_overdue", fields...)
		} else {
			logutil.BgLogger().Warn("Materialized_view_refresh_time_warning", fields...)
		}
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

func (t *MVService) executeRefreshTask(m *mv) (err error) {
	if !t.hasPendingMVTask(m) {
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

func (t *MVService) executePurgeTask(l *mvLog) (err error) {
	if !t.hasPendingMVLogTask(l) {
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
		zap.Int64("mv_count", t.metrics.mvCount.Load()),
		zap.Int64("mvlog_count", t.metrics.mvLogCount.Load()),
		zap.Int64("running_refresh_count", t.metrics.runningMVRefreshCount.Load()),
		zap.Int64("running_purge_count", t.metrics.runningMVLogPurgeCount.Load()),
		zap.Int64("waiting_count", t.executor.metrics.gauges.waitingCount.Load()),
	}
	return fields
}

func (t *MVService) handleRefreshTaskResult(m *mv, nextRefresh time.Time, err error) {
	defer t.notifier.Wake()
	if err != nil {
		retryCount := m.retryCount.Add(1)
		retryDelay := t.retryDelay(retryCount)
		nextRetryAt := mvsNow().Add(retryDelay)
		t.rescheduleMV(m, nextRetryAt.UnixMilli())
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
		t.removeMVTask(m)
		return
	}
	m.retryCount.Store(0)
	t.rescheduleMVSuccess(m, nextRefresh)
}

func (t *MVService) handlePurgeTaskResult(l *mvLog, nextPurge time.Time, err error) {
	defer t.notifier.Wake()
	if err != nil {
		retryCount := l.retryCount.Add(1)
		retryDelay := t.retryDelay(retryCount)
		nextRetryAt := mvsNow().Add(retryDelay)
		t.rescheduleMVLog(l, nextRetryAt.UnixMilli())
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
		t.removeMVLogTask(l)
		return
	}
	l.retryCount.Store(0)
	t.rescheduleMVLogSuccess(l, nextPurge)
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
// 1. Update mutable metadata fields (nextPurge).
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
			om.Value.schemaName = nm.schemaName
			om.Value.mviewName = nm.mviewName
			om.Value.lastSuccessReadTSO = nm.lastSuccessReadTSO
			om.Value.lastSuccessTime = nm.lastSuccessTime
			om.Value.alertWarningSec = nm.alertWarningSec
			om.Value.alertOverdueSec = nm.alertOverdueSec
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
		t.mh.observeTaskDuration(mvFetchTypeMLogPurge, result, mvsSince(start))
	}()

	newPending, err := t.mh.loadAllTiDBMVLogPurge(t.ctx, t.sysSessionPool)
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
func (t *MVService) fetchAllTiDBMVRefresh() (map[int64]*mv, error) {
	start := mvsNow()
	result := mvDurationResultSuccess
	defer func() {
		t.mh.observeTaskDuration(mvFetchTypeMViewRefresh, result, mvsSince(start))
	}()

	newPending, err := t.mh.loadAllTiDBMVRefresh(t.ctx, t.sysSessionPool)
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
	historyGCInterval := t.historyGCInterval()
	nowMillis := now.UnixMilli()
	nextAtMillis := t.nextHistoryGCAtMillis.Load()
	if nextAtMillis > nowMillis {
		return
	}
	if !t.historyGCRunning.CompareAndSwap(false, true) {
		return
	}

	if !t.sch.AvailableString(mvHistoryGCOwnerKey) {
		t.historyGCRetryCount.Store(0)
		t.scheduleNextHistoryGC(now, t.serverRefreshInterval)
		t.historyGCRunning.Store(false)
		return
	}
	go t.runGCOperationHistory(now, historyGCInterval)
}

func (t *MVService) runGCOperationHistory(now time.Time, historyGCInterval time.Duration) {
	defer t.historyGCRunning.Store(false)
	mviewRefreshRetention, mlogPurgeRetention := t.historyGCRetentionConfig()
	startAt := mvsNow()
	result := mvDurationResultSuccess
	defer func() {
		t.mh.observeTaskDuration(mvTaskDurationTypeHistoryGC, result, mvsSince(startAt))
	}()
	defer func() {
		if r := recover(); r != nil {
			result = mvDurationResultFailed
			t.scheduleHistoryGCFailure(now, historyGCInterval)
			t.mh.observeRunEvent(mvRunEventRecoveredPanic)
			fields := append(t.runtimeLogFields(), zap.Any("panic", r), zap.ByteString("stack", debug.Stack()))
			logutil.BgLogger().Error("MVService history GC panicked", fields...)
		}
	}()

	currentTSO, err := t.mh.GetCurrentTSO(t.ctx, t.sysSessionPool)
	if err != nil {
		result = mvDurationResultFailed
		t.scheduleHistoryGCFailure(now, historyGCInterval)
		t.mh.observeRunEvent(mvRunEventHistoryGCGetTSOErr)
		fields := append(t.runtimeLogFields(), zap.Error(err))
		logutil.BgLogger().Warn("get current tso failed when GC MV/MVLOG operation history", fields...)
		return
	}
	if err := t.mh.PurgeMVHistoryBeforeTSO(t.ctx, t.sysSessionPool, currentTSO, mviewRefreshRetention, mlogPurgeRetention); err != nil {
		result = mvDurationResultFailed
		t.scheduleHistoryGCFailure(now, historyGCInterval)
		fields := append(t.runtimeLogFields(),
			zap.Uint64("current_tso", currentTSO),
			zap.Duration("mview_refresh_hist_retention", mviewRefreshRetention),
			zap.Duration("mlog_purge_hist_retention", mlogPurgeRetention),
			zap.Error(err),
		)
		logutil.BgLogger().Warn("GC MV/MVLOG operation history failed", fields...)
		return
	}
	t.scheduleHistoryGCSuccess(now, historyGCInterval)
}

func (t *MVService) scheduleNextHistoryGC(now time.Time, delay time.Duration) {
	if delay <= 0 {
		delay = defaultMVBasicInterval
	}
	t.nextHistoryGCAtMillis.Store(now.Add(delay).UnixMilli())
}

func (t *MVService) scheduleHistoryGCSuccess(now time.Time, historyGCInterval time.Duration) {
	t.historyGCRetryCount.Store(0)
	t.scheduleNextHistoryGC(now, historyGCInterval)
}

func (t *MVService) scheduleHistoryGCFailure(now time.Time, historyGCInterval time.Duration) {
	if t.ctx.Err() != nil {
		t.historyGCRetryCount.Store(0)
		t.scheduleNextHistoryGC(now, historyGCInterval)
		return
	}
	retryCount := t.historyGCRetryCount.Add(1)
	if retryCount > historyGCRetryMaxAttempts {
		t.historyGCRetryCount.Store(0)
		t.scheduleNextHistoryGC(now, historyGCInterval)
		return
	}
	baseDelay := t.basicInterval
	if baseDelay <= 0 {
		baseDelay = defaultMVBasicInterval
	}
	maxDelay := time.Minute
	if historyGCInterval < maxDelay {
		maxDelay = historyGCInterval
	}
	if maxDelay < baseDelay {
		maxDelay = baseDelay
	}
	t.scheduleNextHistoryGC(now, calcRetryDelay(retryCount, baseDelay, maxDelay))
}

func (t *MVService) rescheduleHistoryGCEarlier(now time.Time, interval time.Duration) {
	if interval <= 0 {
		interval = defaultMVHistoryGCInterval
	}
	desired := now.Add(interval).UnixMilli()
	for {
		current := t.nextHistoryGCAtMillis.Load()
		if current > 0 && current <= desired {
			return
		}
		if t.nextHistoryGCAtMillis.CompareAndSwap(current, desired) {
			return
		}
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
			t.maybeGCOperationHistory(now)
			t.maybeLogRefreshAlertTasks(now)
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

		mvLogToPurge, mvToRefresh := t.fetchExecTasks(now)
		t.purgeMVLog(mvLogToPurge)
		t.refreshMV(mvToRefresh)

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
