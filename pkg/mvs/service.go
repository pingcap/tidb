package mvs

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	basic "github.com/pingcap/tidb/pkg/util"
)

type mvLogItem Item[*mvLog]
type mvItem Item[*mv]

// MVMetricsReporter reports MV service runtime metrics.
type MVMetricsReporter interface {
	reportMetrics(*MVService)
	observeTaskDuration(taskType, result string, duration time.Duration)
	observeFetchDuration(fetchType, result string, duration time.Duration)
	observeRunEvent(eventType string)
}

// MVServiceHelper provides all external dependencies required by MVService.
type MVServiceHelper interface {
	ServerHelper
	MVTaskHandler
	MVMetricsReporter
}

// MVService is the in-memory scheduler and executor for MV refresh/purge tasks.
type MVService struct {
	sysSessionPool basic.SessionPool
	lastRefresh    atomic.Int64
	ctx            context.Context
	sch            *ServerConsistentHash

	executor *TaskExecutor
	notifier Notifier
	ddlDirty atomic.Bool

	mh MVServiceHelper

	fetchInterval         time.Duration
	basicInterval         time.Duration
	serverRefreshInterval time.Duration

	retryBaseDelayNanos atomic.Int64
	retryMaxDelayNanos  atomic.Int64
	backpressureMu      sync.RWMutex
	backpressureCfg     TaskBackpressureConfig

	metrics struct {
		mvCount                atomic.Int64
		mvLogCount             atomic.Int64
		runningMVRefreshCount  atomic.Int64
		runningMVLogPurgeCount atomic.Int64
	}

	mvRefreshMu struct {
		sync.Mutex
		pending map[string]mvItem
		prio    PriorityQueue[*mv]
	}
	mvLogPurgeMu struct {
		sync.Mutex
		pending map[string]mvLogItem
		prio    PriorityQueue[*mvLog]
	}
}

const (
	defaultMVTaskMaxConcurrency      = 10
	defaultMVTaskTimeout             = 60 * time.Second
	defaultMVFetchInterval           = 30 * time.Second
	defaultMVBasicInterval           = time.Second
	defaultTiDBServerRefreshInterval = 5 * time.Second
	defaultMVTaskRetryBase           = 10 * time.Second
	defaultMVTaskRetryMax            = 120 * time.Second
	maxNextScheduleTs                = 9e18

	defaultServerConsistentHashReplicas = 10

	mvTaskDurationTypeRefresh = "mv_refresh"
	mvTaskDurationTypePurge   = "mvlog_purge"
	mvTaskDurationResultOK    = "success"
	mvTaskDurationResultErr   = "failed"

	mvFetchDurationTypeMLogPurge = "fetch_mlog"
	mvFetchDurationTypeMViews    = "fetch_mviews"
	mvFetchDurationResultOK      = "success"
	mvFetchDurationResultErr     = "failed"

	mvRunEventInitFailed         = "init_failed"
	mvRunEventRecoveredPanic     = "mv_service_panic"
	mvRunEventServerRefreshOK    = "server_refresh_ok"
	mvRunEventServerRefreshError = "server_refresh_error"
	mvRunEventFetchByDDL         = "fetch_meta_trigger_ddl"
	mvRunEventFetchByInterval    = "fetch_meta_trigger_interval"
	mvRunEventFetchMLogOK        = "fetch_mlog_ok"
	mvRunEventFetchMLogError     = "fetch_mlog_error"
	mvRunEventFetchMViewsOK      = "fetch_mviews_ok"
	mvRunEventFetchMViewsError   = "fetch_mviews_error"
)

type mv struct {
	ID              string
	lastRefresh     time.Time
	refreshInterval time.Duration
	nextRefresh     time.Time

	orderTs    int64 // unix timestamp in milliseconds
	retryCount atomic.Int64
}

// mvLog tracks scheduling state for one MV log purge task.
type mvLog struct {
	ID            string
	baseTableID   string
	lastPurge     time.Time
	purgeInterval time.Duration
	nextPurge     time.Time

	orderTs    int64 // unix timestamp in milliseconds
	retryCount atomic.Int64
}

// TaskBackpressureConfig is the runtime config for task backpressure.
type TaskBackpressureConfig struct {
	Enabled      bool
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
	{
		t.mvLogPurgeMu.Lock() // guard mvlog purge queue
		for t.mvLogPurgeMu.prio.Len() > 0 {
			it := t.mvLogPurgeMu.prio.Front()
			l := it.Value
			if l.orderTs == maxNextScheduleTs {
				break
			}
			if mvsUnixMilli(l.orderTs).After(now) {
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
			if mvsUnixMilli(m.orderTs).After(now) {
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
		t.executor.Submit("mv-refresh/"+m.ID, func() error {
			t.metrics.runningMVRefreshCount.Add(1)
			defer t.metrics.runningMVRefreshCount.Add(-1)
			taskStart := mvsNow()
			nextRefresh, err := t.mh.RefreshMV(t.ctx, t.sysSessionPool, m.ID)
			result := mvTaskDurationResultOK
			if err != nil {
				result = mvTaskDurationResultErr
			}
			t.mh.observeTaskDuration(mvTaskDurationTypeRefresh, result, mvsSince(taskStart))
			if err != nil {
				retryCount := m.retryCount.Add(1)
				t.rescheduleMV(m, mvsNow().Add(t.retryDelay(retryCount)).UnixMilli())
				t.notifier.Wake()
				return err
			}
			if nextRefresh.IsZero() {
				m.retryCount.Store(0)
				t.removeMVTask(m)
				t.notifier.Wake()
				return nil
			}
			m.retryCount.Store(0)
			t.rescheduleMVSuccess(m, nextRefresh)
			t.notifier.Wake()
			return nil
		})
	}
}

// purgeMVLog submits purge jobs to the task executor.
func (t *MVService) purgeMVLog(mvLogToPurge []*mvLog) {
	if len(mvLogToPurge) == 0 {
		return
	}
	for _, l := range mvLogToPurge {
		t.executor.Submit("mvlog-purge/"+l.ID, func() error {
			t.metrics.runningMVLogPurgeCount.Add(1)
			defer t.metrics.runningMVLogPurgeCount.Add(-1)
			taskStart := mvsNow()
			nextPurge, err := t.mh.PurgeMVLog(t.ctx, t.sysSessionPool, l.ID)
			result := mvTaskDurationResultOK
			if err != nil {
				result = mvTaskDurationResultErr
			}
			t.mh.observeTaskDuration(mvTaskDurationTypePurge, result, mvsSince(taskStart))
			if err != nil {
				retryCount := l.retryCount.Add(1)
				t.rescheduleMVLog(l, mvsNow().Add(t.retryDelay(retryCount)).UnixMilli())
				t.notifier.Wake()
				return err
			}
			if nextPurge.IsZero() {
				l.retryCount.Store(0)
				t.removeMVLogTask(l)
				t.notifier.Wake()
				return nil
			}
			l.retryCount.Store(0)
			t.rescheduleMVLogSuccess(l, nextPurge)
			t.notifier.Wake()
			return nil
		})
	}
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

// buildMLogPurgeTasks rebuilds purge task states from fetched metadata.
//
// For each item in newPending:
// 1. Update mutable metadata fields (purgeInterval, nextPurge).
// 2. If nextPurge changed and the task is not currently running, update orderTs and heap position.
// 3. If the task is currently running (orderTs == maxNextScheduleTs), defer heap adjustment until task completion.
func (t *MVService) buildMLogPurgeTasks(newPending map[string]*mvLog) error {
	t.mvLogPurgeMu.Lock()         // guard mvlog purge queue
	defer t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard

	if t.mvLogPurgeMu.pending == nil {
		t.mvLogPurgeMu.pending = make(map[string]mvLogItem, len(newPending))
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

	return nil
}

// buildMVRefreshTasks rebuilds refresh task states from fetched metadata.
func (t *MVService) buildMVRefreshTasks(newPending map[string]*mv) error {
	t.mvRefreshMu.Lock()         // guard mv refresh queue
	defer t.mvRefreshMu.Unlock() // release mv refresh queue guard

	if t.mvRefreshMu.pending == nil {
		t.mvRefreshMu.pending = make(map[string]mvItem, len(newPending))
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

	return nil
}

// fetchAllTiDBMLogPurge fetches purge metadata and filters out tasks not owned by this node.
func (t *MVService) fetchAllTiDBMLogPurge() (map[string]*mvLog, error) {
	start := mvsNow()
	result := mvFetchDurationResultOK
	defer func() {
		t.mh.observeFetchDuration(mvFetchDurationTypeMLogPurge, result, mvsSince(start))
	}()

	newPending, err := t.mh.fetchAllTiDBMLogPurge(t.ctx, t.sysSessionPool)
	if err != nil {
		result = mvFetchDurationResultErr
		t.mh.observeRunEvent(mvRunEventFetchMLogError)
		return nil, err
	}
	t.mh.observeRunEvent(mvRunEventFetchMLogOK)
	for id := range newPending {
		if id == "" || !t.sch.Available(id) {
			delete(newPending, id)
		}
	}
	return newPending, nil
}

// fetchAllTiDBMViews fetches refresh metadata and filters out tasks not owned by this node.
func (t *MVService) fetchAllTiDBMViews() (map[string]*mv, error) {
	start := mvsNow()
	result := mvFetchDurationResultOK
	defer func() {
		t.mh.observeFetchDuration(mvFetchDurationTypeMViews, result, mvsSince(start))
	}()

	newPending, err := t.mh.fetchAllTiDBMViews(t.ctx, t.sysSessionPool)
	if err != nil {
		result = mvFetchDurationResultErr
		t.mh.observeRunEvent(mvRunEventFetchMViewsError)
		return nil, err
	}
	t.mh.observeRunEvent(mvRunEventFetchMViewsOK)
	for id := range newPending {
		if id == "" || !t.sch.Available(id) {
			delete(newPending, id)
		}
	}
	return newPending, nil
}

// fetchAllMVMeta refreshes both purge and refresh task queues from metadata tables.
func (t *MVService) fetchAllMVMeta() error {
	newMLogPending, err := t.fetchAllTiDBMLogPurge()
	if err != nil {
		return err
	}
	newMViewPending, err := t.fetchAllTiDBMViews()
	if err != nil {
		return err
	}
	if err = t.buildMLogPurgeTasks(newMLogPending); err != nil {
		return err
	}
	if err = t.buildMVRefreshTasks(newMViewPending); err != nil {
		return err
	}

	t.lastRefresh.Store(mvsNow().UnixMilli())
	return nil
}
