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

	nextRefreshAlertScanMillis      atomic.Int64
	nextMVLogAccumulationScanMillis atomic.Int64

	refreshExecutor                 *TaskExecutor
	purgeExecutor                   *TaskExecutor
	mlogAnalyzeExecutor             *TaskExecutor
	refreshTaskConcurrencyRatioBits atomic.Uint64
	notifier                        Notifier
	ddlDirty                        atomic.Bool

	mh Helper

	fetchIntervalMillis             atomic.Int64
	basicInterval                   time.Duration
	serverRefreshInterval           time.Duration
	mviewRefreshHistRetentionMillis atomic.Int64
	mlogPurgeHistRetentionMillis    atomic.Int64
	nextHistoryGCAtMillis           atomic.Int64
	historyGCRetryCount             atomic.Int64
	historyGCRunning                atomic.Bool
	mvLogAccumulationScanRunning    atomic.Bool
	nextMLogAnalyzeScanMillis       atomic.Int64
	mlogAnalyzeScanRunning          atomic.Bool

	retryBaseDelayMillis atomic.Int64
	retryMaxDelayMillis  atomic.Int64
	backpressureMu       sync.RWMutex
	backpressureCfg      TaskBackpressureConfig

	metrics struct {
		mvCount                  atomic.Int64
		mvLogCount               atomic.Int64
		runningMVRefreshCount    atomic.Int64
		runningMVLogPurgeCount   atomic.Int64
		runningMVLogAnalyzeCount atomic.Int64
		alertWarningCount        atomic.Int64
		alertOverdueCount        atomic.Int64
		mvLogAccumulationCount   atomic.Int64
		mvLogAnalyzeTaskCount    atomic.Int64
	}

	mviewRefreshMu struct {
		sync.Mutex
		pending map[int64]mviewTaskItem
		prio    PriorityQueue[*mviewTask]
	}
	mviewRefreshAlertMu struct {
		sync.Mutex
		pending map[int64]*mviewTask
	}
	mlogPurgeMu struct {
		sync.Mutex
		pending map[int64]mlogPurgeTaskItem
		prio    PriorityQueue[*mlogPurgeTask]
	}
	mlogAnalyzeTasks struct {
		sync.Mutex
		running map[int64]struct{}
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
	defaultMVHistoryGCInterval           = 20 * time.Minute
	defaultMVHistoryGCRetention          = 365 * 24 * time.Hour
	defaultMVHistoryGCMaxRecords         = uint64(1000000)
	defaultMVTaskRetryBase               = 10 * time.Second
	defaultMVTaskRetryMax                = 120 * time.Second
	manualCancelBackoffDelay             = 2 * time.Minute
	mvRefreshAlertScanInterval           = 30 * time.Second
	mvLogAccumulationAlertScanInterval   = 20 * time.Minute
	mlogAnalyzeScanInterval              = 3 * time.Minute
	defaultMLogAnalyzeTaskConcurrency    = 2
	maxNextScheduleTs                    = 9e18

	defaultCHReplicas = 100

	mvTaskDurationTypeRefresh     = "mv_refresh"
	mvTaskDurationTypePurge       = "mvlog_purge"
	mvTaskDurationTypeMLogAnalyze = "mvlog_analyze"

	mvFetchTypeMLogPurge        = "fetch_mlog"
	mvFetchTypeMLogAccumulation = "fetch_mlog_accumulation"
	mvFetchTypeMViewRefresh     = "fetch_mviews"
	mvFetchTypeMLogAnalyze      = "fetch_mlog_analyze"

	mvTaskDurationTypeHistoryGC = "history_gc"

	mvDurationResultSuccess = "success"
	mvDurationResultFailed  = "failed"

	mvRunEventInitFailed         = "init_failed"
	mvRunEventServerChanged      = "server_changed"
	mvRunEventServerRefreshError = "server_refresh_error"
	mvRunEventFetchByDDL         = "fetch_meta_by_ddl"
	mvRunEventFetchByInterval    = "fetch_meta_by_interval"
	mvRunEventGetTSOErr          = "get_tso_error"

	mvHistoryGCOwnerKey = "gc-mv-op-hist"
	// A single hash-ring owner performs stale refresh-alert cleanup to avoid
	// repeating the same global delete on every TiDB node.
	mvRefreshAlertCleanupOwnerKey = "gc-mv-refresh-alert"

	historyGCRetryMaxAttempts  = 8
	mvRefreshAlertLevelWarning = "warning"
	mvRefreshAlertLevelOverdue = "overdue"

	mvRefreshAlertTaskStateRunning = "running"
	mvRefreshAlertTaskStateQueued  = "queued"
	mvRefreshAlertTaskStateUnknown = "unknown"
)

type mviewTask struct {
	ID          int64
	nextRefresh time.Time
	schemaName  string
	mviewName   string

	metadataUnresolved bool

	lastSuccessReadTSO uint64
	lastSuccessTime    time.Time
	alertWarningSec    int64
	alertOverdueSec    int64
	// Suppress repeated overdue logs for the same lastSuccessReadTSO.
	lastLoggedWarningTSO   uint64
	lastLoggedOverdueTSO   uint64
	lastSyncedAlertLevel   string
	lastSyncedAlertReadTSO uint64
	alertStateInitialized  bool

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
		add(snapshotTaskExecutorMetrics(t.purgeExecutor)).
		add(snapshotTaskExecutorMetrics(t.mlogAnalyzeExecutor))
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
	if t.mlogAnalyzeExecutor != nil {
		t.mlogAnalyzeExecutor.Run()
	}
}

func (t *MVService) closeTaskExecutors() {
	if t.refreshExecutor != nil {
		t.refreshExecutor.Close()
	}
	if t.purgeExecutor != nil {
		t.purgeExecutor.Close()
	}
	if t.mlogAnalyzeExecutor != nil {
		t.mlogAnalyzeExecutor.Close()
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

type refreshAlertTask struct {
	mviewID            int64
	schemaName         string
	mviewName          string
	nextRefresh        time.Time
	metadataUnresolved bool
	lastSuccessTime    time.Time
	lastSuccessReadTSO uint64
	alertWarningSec    int64
	alertOverdueSec    int64
	retryCount         int64
	taskState          string
	overdue            bool
	alertLevel         string
}

type refreshAlertExecutionState struct {
	running    bool
	retryCount int64
}

// maybeLogRefreshAlertTasks scans pending refresh tasks, persists alert states, and logs warning/overdue alerts.
// The scan runs on maintenanceTick and is rate-limited by mvRefreshAlertScanInterval.
func (t *MVService) maybeLogRefreshAlertTasks(now time.Time) {
	nowMillis := now.UnixMilli()
	if next := t.nextRefreshAlertScanMillis.Load(); next > nowMillis {
		return
	}
	t.nextRefreshAlertScanMillis.Store(now.Add(mvRefreshAlertScanInterval).UnixMilli())

	if !t.isRefreshAlertCheckerOwner() {
		t.clearRefreshAlertTasks()
		t.clearRefreshAlertMetrics()
		t.cleanupStaleRefreshAlerts()
		return
	}
	if !t.isRefreshAlertMetricsOwner() {
		t.clearRefreshAlertMetrics()
	}
	if err := t.refreshAllMVRefreshAlertTasks(); err != nil {
		t.cleanupStaleRefreshAlerts()
		return
	}

	alertStates, warningCount, overdueCount := t.collectRefreshAlertStates(now)
	t.updateRefreshAlertMetrics(warningCount, overdueCount)
	if t.syncRefreshAlertStates(now, alertStates) {
		t.markRefreshAlertStatesSynced(alertStates)
	}
	t.cleanupStaleRefreshAlerts()

	alertTasks, _, _ := t.collectRefreshAlertTasks(now)
	if len(alertTasks) == 0 {
		return
	}
	t.logMVRefreshAlerts(alertTasks)
}

func (t *MVService) refreshAllMVRefreshAlertTasks() error {
	newPending, err := t.mh.LoadAllTiDBMVRefresh(t.ctx, t.sysSessionPool)
	if err != nil {
		fields := append(t.runtimeLogFields(), zap.Error(err))
		logutil.BgLogger().Warn("fetch all materialized view refresh alert tasks failed", fields...)
		return err
	}
	t.buildMViewRefreshAlertTasks(newPending)
	return nil
}

func (t *MVService) snapshotMVRefreshExecutionStates() map[int64]refreshAlertExecutionState {
	t.mviewRefreshMu.Lock()
	defer t.mviewRefreshMu.Unlock()
	if len(t.mviewRefreshMu.pending) == 0 {
		return nil
	}
	states := make(map[int64]refreshAlertExecutionState, len(t.mviewRefreshMu.pending))
	for id, item := range t.mviewRefreshMu.pending {
		if item.Value == nil {
			continue
		}
		states[id] = refreshAlertExecutionState{
			running:    item.Value.orderTs == maxNextScheduleTs,
			retryCount: item.Value.retryCount.Load(),
		}
	}
	return states
}

func getRefreshAlertExecutionState(
	execStates map[int64]refreshAlertExecutionState,
	mviewID int64,
) (string, int64) {
	if state, ok := execStates[mviewID]; ok {
		if state.running {
			return mvRefreshAlertTaskStateRunning, state.retryCount
		}
		return mvRefreshAlertTaskStateQueued, state.retryCount
	}
	return mvRefreshAlertTaskStateUnknown, 0
}

func (t *MVService) maybeScanMVLogAccumulationAlerts(now time.Time) {
	nowMillis := now.UnixMilli()
	if next := t.nextMVLogAccumulationScanMillis.Load(); next > nowMillis {
		return
	}
	if !t.mvLogAccumulationScanRunning.CompareAndSwap(false, true) {
		return
	}

	go t.runMVLogAccumulationAlertScan()
}

func (t *MVService) runMVLogAccumulationAlertScan() {
	defer t.mvLogAccumulationScanRunning.Store(false)
	defer func() {
		if r := recover(); r != nil {
			fields := append(t.runtimeLogFields(), zap.Any("panic", r), zap.ByteString("stack", debug.Stack()))
			logutil.BgLogger().Error("MVService mvlog accumulation scan panicked", fields...)
		}
	}()

	alertedCount, err := t.fetchAllMVLogAccumulationAlerts()
	if err != nil {
		return
	}
	t.metrics.mvLogAccumulationCount.Store(int64(alertedCount))
	t.nextMVLogAccumulationScanMillis.Store(mvsNow().Add(mvLogAccumulationAlertScanInterval).UnixMilli())
}

func (t *MVService) maybeScanMLogAnalyze(now time.Time) {
	nowMillis := now.UnixMilli()
	if next := t.nextMLogAnalyzeScanMillis.Load(); next > nowMillis {
		return
	}
	if !t.mlogAnalyzeScanRunning.CompareAndSwap(false, true) {
		return
	}
	t.nextMLogAnalyzeScanMillis.Store(now.Add(mlogAnalyzeScanInterval).UnixMilli())

	go t.runMLogAnalyzeScan()
}

func (t *MVService) runMLogAnalyzeScan() {
	defer t.mlogAnalyzeScanRunning.Store(false)
	defer func() {
		if r := recover(); r != nil {
			fields := append(t.runtimeLogFields(), zap.Any("panic", r), zap.ByteString("stack", debug.Stack()))
			logutil.BgLogger().Error("MVService mlog analyze scan panicked", fields...)
		}
	}()

	mlogIDs, err := t.fetchMLogAnalyzeTasks()
	if err != nil {
		return
	}
	t.analyzeMVLog(mlogIDs)
}

func (t *MVService) collectRefreshAlertStates(now time.Time) ([]refreshAlertTask, int64, int64) {
	execStates := t.snapshotMVRefreshExecutionStates()

	t.mviewRefreshAlertMu.Lock()
	defer t.mviewRefreshAlertMu.Unlock()

	alertStates := make([]refreshAlertTask, 0, len(t.mviewRefreshAlertMu.pending))
	var warningCount int64
	var overdueCount int64
	for mviewID, task := range t.mviewRefreshAlertMu.pending {
		if task == nil || task.nextRefresh.IsZero() || task.metadataUnresolved {
			continue
		}
		alertLevel := classifyRefreshAlertLevel(now, task.lastSuccessTime, task.alertWarningSec, task.alertOverdueSec)
		switch alertLevel {
		case mvRefreshAlertLevelOverdue:
			overdueCount++
		case mvRefreshAlertLevelWarning:
			warningCount++
		}
		if task.alertStateInitialized &&
			task.lastSyncedAlertLevel == alertLevel &&
			task.lastSyncedAlertReadTSO == task.lastSuccessReadTSO {
			continue
		}
		taskState, retryCount := getRefreshAlertExecutionState(execStates, mviewID)
		alertStates = append(alertStates, refreshAlertTask{
			mviewID:            mviewID,
			schemaName:         task.schemaName,
			mviewName:          task.mviewName,
			nextRefresh:        task.nextRefresh,
			lastSuccessTime:    task.lastSuccessTime,
			lastSuccessReadTSO: task.lastSuccessReadTSO,
			alertWarningSec:    task.alertWarningSec,
			alertOverdueSec:    task.alertOverdueSec,
			retryCount:         retryCount,
			taskState:          taskState,
			overdue:            alertLevel == mvRefreshAlertLevelOverdue,
			alertLevel:         alertLevel,
		})
	}
	return alertStates, warningCount, overdueCount
}

func (t *MVService) markRefreshAlertStatesSynced(states []refreshAlertTask) {
	if len(states) == 0 {
		return
	}
	t.mviewRefreshAlertMu.Lock()
	defer t.mviewRefreshAlertMu.Unlock()
	for _, state := range states {
		task, ok := t.mviewRefreshAlertMu.pending[state.mviewID]
		if !ok || task == nil {
			continue
		}
		task.lastSyncedAlertLevel = state.alertLevel
		task.lastSyncedAlertReadTSO = state.lastSuccessReadTSO
		task.alertStateInitialized = true
	}
}

func (t *MVService) collectRefreshAlertTasks(now time.Time) ([]refreshAlertTask, int64, int64) {
	execStates := t.snapshotMVRefreshExecutionStates()

	t.mviewRefreshAlertMu.Lock()
	defer t.mviewRefreshAlertMu.Unlock()

	alertTasks := make([]refreshAlertTask, 0)
	var warningCount int64
	var overdueCount int64
	for mviewID, task := range t.mviewRefreshAlertMu.pending {
		if task == nil || task.nextRefresh.IsZero() || task.metadataUnresolved {
			continue
		}
		lastSuccessTime := task.lastSuccessTime
		alertLevel := classifyRefreshAlertLevel(now, lastSuccessTime, task.alertWarningSec, task.alertOverdueSec)
		if alertLevel == "" {
			continue
		}
		overdue := alertLevel == mvRefreshAlertLevelOverdue
		if overdue {
			overdueCount++
		} else {
			warningCount++
		}
		if tso := task.lastSuccessReadTSO; tso > 0 {
			if overdue {
				if task.lastLoggedOverdueTSO == tso {
					continue
				}
				task.lastLoggedOverdueTSO = tso
			} else {
				// If overdue has already been logged for this tso, skip warning downgrade logs.
				if task.lastLoggedWarningTSO == tso || task.lastLoggedOverdueTSO == tso {
					continue
				}
				task.lastLoggedWarningTSO = tso
			}
		}
		taskState, retryCount := getRefreshAlertExecutionState(execStates, mviewID)
		alertTasks = append(alertTasks, refreshAlertTask{
			mviewID:            mviewID,
			schemaName:         task.schemaName,
			mviewName:          task.mviewName,
			nextRefresh:        task.nextRefresh,
			lastSuccessTime:    lastSuccessTime,
			lastSuccessReadTSO: task.lastSuccessReadTSO,
			alertWarningSec:    task.alertWarningSec,
			alertOverdueSec:    task.alertOverdueSec,
			retryCount:         retryCount,
			taskState:          taskState,
			overdue:            overdue,
			alertLevel:         alertLevel,
		})
	}
	return alertTasks, warningCount, overdueCount
}

func classifyRefreshAlertLevel(now time.Time, lastSuccessTime time.Time, alertWarningSec, alertOverdueSec int64) string {
	if lastSuccessTime.IsZero() || now.Before(lastSuccessTime) {
		return ""
	}
	overdueDuration := now.Sub(lastSuccessTime)
	if alertOverdueSec > 0 && overdueDuration >= time.Duration(alertOverdueSec)*time.Second {
		return mvRefreshAlertLevelOverdue
	}
	if alertWarningSec > 0 && overdueDuration >= time.Duration(alertWarningSec)*time.Second {
		return mvRefreshAlertLevelWarning
	}
	return ""
}

func (t *MVService) syncRefreshAlertStates(updatedAt time.Time, states []refreshAlertTask) bool {
	if len(states) == 0 {
		return true
	}
	if err := t.mh.SyncMVRefreshAlertStates(t.ctx, t.sysSessionPool, updatedAt, states); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return false
		}
		fields := append(
			t.runtimeLogFields(),
			zap.Int("alert_state_count", len(states)),
			zap.Time("updated_at", updatedAt),
			zap.Error(err),
		)
		logutil.BgLogger().Warn("sync mv refresh alert states failed", fields...)
		return false
	}
	return true
}

func (t *MVService) updateRefreshAlertMetrics(warningCount, overdueCount int64) {
	if !t.isRefreshAlertMetricsOwner() {
		t.clearRefreshAlertMetrics()
		return
	}
	t.metrics.alertWarningCount.Store(warningCount)
	t.metrics.alertOverdueCount.Store(overdueCount)
}

func (t *MVService) refreshAlertMetricCounts() (int64, int64) {
	if !t.isRefreshAlertMetricsOwner() {
		return 0, 0
	}
	return t.metrics.alertWarningCount.Load(), t.metrics.alertOverdueCount.Load()
}

func (t *MVService) clearRefreshAlertMetrics() {
	t.metrics.alertWarningCount.Store(0)
	t.metrics.alertOverdueCount.Store(0)
}

func (t *MVService) cleanupStaleRefreshAlerts() {
	if !t.isRefreshAlertCleanupOwner() {
		return
	}
	if err := t.mh.CleanupStaleMVRefreshAlerts(t.ctx, t.sysSessionPool); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		fields := append(t.runtimeLogFields(), zap.Error(err))
		logutil.BgLogger().Warn("cleanup stale mv refresh alerts failed", fields...)
	}
}

func (t *MVService) isRefreshAlertCheckerOwner() bool {
	t.sch.mu.RLock()
	defer t.sch.mu.RUnlock()

	n := min(2, len(t.sch.serverIDs))
	for _, id := range t.sch.serverIDs[:n] {
		if id == t.sch.ID {
			return true
		}
	}
	return false
}

func (t *MVService) isRefreshAlertMetricsOwner() bool {
	t.sch.mu.RLock()
	defer t.sch.mu.RUnlock()

	if len(t.sch.serverIDs) == 0 {
		return false
	}
	return t.sch.serverIDs[0] == t.sch.ID
}

func (t *MVService) isRefreshAlertCleanupOwner() bool {
	t.sch.mu.RLock()
	defer t.sch.mu.RUnlock()
	if t.sch.ID == "" || len(t.sch.servers) == 0 {
		return false
	}
	return t.sch.chash.GetNode([]byte(mvRefreshAlertCleanupOwnerKey)) == t.sch.ID
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
			logutil.BgLogger().Error("Materialized_view_refresh_time_warning", fields...)
		}
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

// analyzeMVLog submits mlog analyze tasks to the task executor.
func (t *MVService) analyzeMVLog(mlogIDs []int64) {
	if len(mlogIDs) == 0 {
		return
	}
	for _, id := range mlogIDs {
		mlogID := id
		if !t.markMLogAnalyzeTaskSubmitted(mlogID) {
			continue
		}
		submitted := t.mlogAnalyzeExecutor.Submit("mvlog-analyze/"+strconv.FormatInt(mlogID, 10), func() error {
			return t.executeMLogAnalyzeTask(mlogID)
		})
		if !submitted {
			t.unmarkMLogAnalyzeTaskSubmitted(mlogID)
		}
	}
}

func (t *MVService) executeMLogAnalyzeTask(mlogID int64) (err error) {
	t.metrics.runningMVLogAnalyzeCount.Add(1)
	defer t.metrics.runningMVLogAnalyzeCount.Add(-1)
	defer t.unmarkMLogAnalyzeTaskSubmitted(mlogID)

	taskStart := mvsNow()
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error(
				"analyze MV log task panicked",
				zap.Int64("mvlog_id", mlogID),
				zap.Any("panic", r),
				zap.ByteString("stack", debug.Stack()),
			)
			err = fmt.Errorf("analyze MV log task panicked: %v", r)
		}
		t.observeTaskDuration(mvTaskDurationTypeMLogAnalyze, taskStart, err)
		if err != nil {
			fields := append(t.runtimeLogFields(), zap.Int64("mvlog_id", mlogID), zap.Error(err))
			logutil.BgLogger().Warn("analyze MV log task failed", fields...)
		}
	}()

	err = t.mh.AnalyzeMVLog(t.ctx, t.sysSessionPool, mlogID)
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

func (t *MVService) markMLogAnalyzeTaskSubmitted(mlogID int64) bool {
	t.mlogAnalyzeTasks.Lock()
	defer t.mlogAnalyzeTasks.Unlock()
	if t.mlogAnalyzeTasks.running == nil {
		t.mlogAnalyzeTasks.running = make(map[int64]struct{})
	}
	if _, ok := t.mlogAnalyzeTasks.running[mlogID]; ok {
		return false
	}
	t.mlogAnalyzeTasks.running[mlogID] = struct{}{}
	t.metrics.mvLogAnalyzeTaskCount.Store(int64(len(t.mlogAnalyzeTasks.running)))
	return true
}

func (t *MVService) unmarkMLogAnalyzeTaskSubmitted(mlogID int64) {
	t.mlogAnalyzeTasks.Lock()
	defer t.mlogAnalyzeTasks.Unlock()
	delete(t.mlogAnalyzeTasks.running, mlogID)
	t.metrics.mvLogAnalyzeTaskCount.Store(int64(len(t.mlogAnalyzeTasks.running)))
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
		zap.Int64("running_mlog_analyze_count", t.metrics.runningMVLogAnalyzeCount.Load()),
		zap.Int64("waiting_count", loadTaskExecutorWaitingCount(t.refreshExecutor)+loadTaskExecutorWaitingCount(t.purgeExecutor)+loadTaskExecutorWaitingCount(t.mlogAnalyzeExecutor)),
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
			om.Value.metadataUnresolved = nm.metadataUnresolved
			if !nm.metadataUnresolved {
				om.Value.schemaName = nm.schemaName
				om.Value.mviewName = nm.mviewName
				om.Value.alertWarningSec = nm.alertWarningSec
				om.Value.alertOverdueSec = nm.alertOverdueSec
			}
			om.Value.lastSuccessReadTSO = nm.lastSuccessReadTSO
			om.Value.lastSuccessTime = nm.lastSuccessTime
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

// buildMViewRefreshAlertTasks rebuilds global refresh alert state from fetched metadata.
func (t *MVService) buildMViewRefreshAlertTasks(newPending map[int64]*mviewTask) {
	t.mviewRefreshAlertMu.Lock()
	defer t.mviewRefreshAlertMu.Unlock()

	if t.mviewRefreshAlertMu.pending == nil {
		t.mviewRefreshAlertMu.pending = make(map[int64]*mviewTask, len(newPending))
	}
	for id, nm := range newPending {
		if nm == nil {
			continue
		}
		if nm.orderTs == 0 && !nm.nextRefresh.IsZero() {
			nm.orderTs = nm.nextRefresh.UnixMilli()
		}
		if om, ok := t.mviewRefreshAlertMu.pending[id]; ok && om != nil {
			om.metadataUnresolved = nm.metadataUnresolved
			if !nm.metadataUnresolved {
				om.schemaName = nm.schemaName
				om.mviewName = nm.mviewName
				om.alertWarningSec = nm.alertWarningSec
				om.alertOverdueSec = nm.alertOverdueSec
			}
			om.lastSuccessReadTSO = nm.lastSuccessReadTSO
			om.lastSuccessTime = nm.lastSuccessTime
			om.nextRefresh = nm.nextRefresh
			om.orderTs = nm.orderTs
			continue
		}
		t.mviewRefreshAlertMu.pending[id] = nm
	}
	for id := range t.mviewRefreshAlertMu.pending {
		if _, ok := newPending[id]; !ok {
			delete(t.mviewRefreshAlertMu.pending, id)
		}
	}
}

func (t *MVService) clearRefreshAlertTasks() {
	t.mviewRefreshAlertMu.Lock()
	t.mviewRefreshAlertMu.pending = nil
	t.mviewRefreshAlertMu.Unlock()
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

// fetchAllMVLogAccumulationAlerts counts MV logs whose row count exceeds the configured alert threshold.
func (t *MVService) fetchAllMVLogAccumulationAlerts() (int, error) {
	start := mvsNow()
	result := mvDurationResultSuccess
	defer func() {
		t.mh.observeTaskDuration(mvFetchTypeMLogAccumulation, result, mvsSince(start))
	}()

	candidates, err := t.fetchAllTiDBMVLogAccumulationTasks()
	if err != nil {
		result = mvDurationResultFailed
		return 0, err
	}
	rowCounts, err := t.mh.LoadTiDBMVLogAccumulationRowCounts(t.ctx, t.sysSessionPool, candidates)
	if err != nil {
		result = mvDurationResultFailed
		fields := append(t.runtimeLogFields(), zap.Error(err))
		logutil.BgLogger().Warn("fetch mvlog accumulation row counts failed", fields...)
		return 0, err
	}
	alertedCount := 0
	for mlogID, rowCount := range rowCounts {
		task, ok := candidates[mlogID]
		if !ok || task == nil {
			continue
		}
		if rowCount > task.alertRows {
			alertedCount++
		}
	}
	return alertedCount, nil
}

// fetchAllTiDBMVLogAccumulationTasks fetches accumulation metadata and filters out tasks not owned by this node.
func (t *MVService) fetchAllTiDBMVLogAccumulationTasks() (map[int64]*mlogAccumulationTask, error) {
	newPending, err := t.mh.LoadAllTiDBMVLogAccumulationTasks(t.ctx, t.sysSessionPool)
	if err != nil {
		fields := append(t.runtimeLogFields(), zap.Error(err))
		logutil.BgLogger().Warn("fetch all mvlog accumulation tasks failed", fields...)
		return nil, err
	}
	filterUnownedTasks(t.sch, newPending)
	return newPending, nil
}

// fetchMLogAnalyzeTasks fetches mlog analyze candidates and filters out tasks not owned by this node.
func (t *MVService) fetchMLogAnalyzeTasks() ([]int64, error) {
	start := mvsNow()
	result := mvDurationResultSuccess
	defer func() {
		t.mh.observeTaskDuration(mvFetchTypeMLogAnalyze, result, mvsSince(start))
	}()

	candidates, err := t.mh.LoadAllTiDBMVLogAnalyzeTasks(t.ctx, t.sysSessionPool)
	if err != nil {
		result = mvDurationResultFailed
		fields := append(t.runtimeLogFields(), zap.Error(err))
		logutil.BgLogger().Warn("fetch mvlog analyze tasks failed", fields...)
		return nil, err
	}
	filterUnownedTasks(t.sch, candidates)
	mlogIDs := make([]int64, 0, len(candidates))
	for mlogID := range candidates {
		mlogIDs = append(mlogIDs, mlogID)
	}
	return mlogIDs, nil
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
			fields := append(t.runtimeLogFields(), zap.Any("panic", r), zap.ByteString("stack", debug.Stack()))
			logutil.BgLogger().Error("MVService history GC panicked", fields...)
		}
	}()

	currentTSO, err := t.mh.GetCurrentTSO(t.ctx, t.sysSessionPool)
	if err != nil {
		result = mvDurationResultFailed
		t.scheduleHistoryGCFailure(now, historyGCInterval)
		t.mh.observeRunEvent(mvRunEventGetTSOErr)
		fields := append(t.runtimeLogFields(), zap.Error(err))
		logutil.BgLogger().Warn("get current tso failed when GC MV/MVLOG operation history", fields...)
		return
	}
	if err := t.mh.PurgeMVHistoryBeforeTSO(
		t.ctx,
		t.sysSessionPool,
		currentTSO,
		mviewRefreshRetention,
		mlogPurgeRetention,
	); err != nil {
		result = mvDurationResultFailed
		t.scheduleHistoryGCFailure(now, historyGCInterval)
		fields := append(t.runtimeLogFields(),
			zap.Uint64("current_tso", currentTSO),
			zap.Duration("mview_refresh_hist_retention", mviewRefreshRetention),
			zap.Duration("mlog_purge_hist_retention", mlogPurgeRetention),
			zap.Uint64("mview_refresh_hist_max_records", defaultMVHistoryGCMaxRecords),
			zap.Uint64("mlog_purge_hist_max_records", defaultMVHistoryGCMaxRecords),
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
			t.maybeGCOperationHistory(now)
			t.maybeLogRefreshAlertTasks(now)
			t.maybeScanMVLogAccumulationAlerts(now)
			t.maybeScanMLogAnalyze(now)
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
