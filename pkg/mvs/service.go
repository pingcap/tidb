package utils

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	basic "github.com/pingcap/tidb/pkg/util"
)

type mvLogItem Item[*mvLog]
type mvItem Item[*mv]

type MVServiceHelper interface {
	ServerHelper
	MVTaskHandler
}

type MVService struct {
	sysSessionPool basic.SessionPool
	lastRefresh    atomic.Int64
	running        atomic.Bool
	runWg          sync.WaitGroup
	ctx            context.Context
	cancel         context.CancelFunc
	sch            *ServerConsistentHash

	executor *TaskExecutor
	notifier Notifier
	ddlDirty atomic.Bool

	mh MVServiceHelper

	metrics struct {
		mvCount                atomic.Int64
		mvLogCount             atomic.Int64
		pendingMVRefreshCount  atomic.Int64
		pendingMVLogPurgeCount atomic.Int64
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
	defaultMVTaskMaxConcurrency = 10
	defaultMVTaskTimeout        = 60 * time.Second
	defaultMVFetchInterval      = 30 * time.Second
	defaultMVTaskRetryBase      = 5 * time.Second
	defaultMVTaskRetryMax       = 5 * time.Minute
	maxNextScheduleTs           = 9e18 // corresponds to year 5138
)

// NewMVJobsManager creates a MVJobsManager with a SQL executor.
func NewMVJobsManager(se basic.SessionPool, helper MVServiceHelper) *MVService {
	if helper == nil || se == nil {
		panic("invalid arguments")
	}
	ctx, cancel := context.WithCancel(context.Background())
	mgr := &MVService{
		sysSessionPool: se,
		sch:            NewServerConsistentHash(10, helper),
		executor:       NewTaskExecutor(ctx, defaultMVTaskMaxConcurrency, defaultMVTaskTimeout),

		notifier: NewNotifier(),
		ctx:      ctx,
		cancel:   cancel,
		mh:       helper,
	}
	return mgr
}

// SetTaskExecConfig sets the execution config for MV tasks.
func (t *MVService) SetTaskExecConfig(maxConcurrency int, timeout time.Duration) {
	t.executor.UpdateConfig(maxConcurrency, timeout)
}

type mv struct {
	ID              string
	lastRefresh     time.Time
	refreshInterval time.Duration
	dependentMLogs  map[string]struct{}
	nextRefresh     time.Time

	orderTs    int64 // unix timestamp in milliseconds
	retryCount atomic.Int64
}

type mvLog struct {
	ID            string
	baseTableID   string
	lastPurge     time.Time
	purgeInterval time.Duration
	dependentMVs  map[string]struct {
		tso int64
	}
	nextPurge time.Time

	orderTs    int64 // unix timestamp in milliseconds
	retryCount atomic.Int64
}

func (m *mv) Less(other *mv) bool {
	return m.orderTs < other.orderTs
}

func (m *mvLog) Less(other *mvLog) bool {
	return m.orderTs < other.orderTs
}

func retryDelay(retryCount int) time.Duration {
	if retryCount <= 0 {
		return defaultMVTaskRetryBase
	}
	delay := defaultMVTaskRetryBase
	for i := 1; i < retryCount && delay < defaultMVTaskRetryMax; i++ {
		delay *= 2
		if delay >= defaultMVTaskRetryMax {
			delay = defaultMVTaskRetryMax
			break
		}
	}
	return delay
}

func resetTimer(timer *time.Timer, delay time.Duration) {
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

/*
// CreateTiDBMViewsTable is a table to store materialized view metadata.
CreateTiDBMViewsTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_mviews (
    TABLE_CATALOG varchar(512) NOT NULL,
    TABLE_SCHEMA varchar(64) NOT NULL,
    MVIEW_ID varchar(64) NOT NULL,
    MVIEW_NAME varchar(64) NOT NULL,
    MVIEW_DEFINITION longtext NOT NULL,
    MVIEW_COMMENT varchar(128) DEFAULT NULL,
    MVIEW_TIFLASH_REPLICAS int DEFAULT 0,
    MVIEW_MODIFY_TIME datetime NOT NULL,
    REFRESH_METHOD varchar(32) DEFAULT NULL,
    REFRESH_MODE varchar(256) DEFAULT NULL,
    REFRESH_START datetime NOT NULL,
    REFRESH_INTERVAL bigint NOT NULL,
	RELATED_MVLOG varchar(256) DEFAULT NULL,
    PRIMARY KEY(MVIEW_ID),
    UNIQUE KEY uniq_mview_name(TABLE_SCHEMA, MVIEW_NAME))`

// CreateTiDBMLogsTable is a table to store materialized view log metadata.
CreateTiDBMLogsTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_mlogs (
    TABLE_CATALOG varchar(512) NOT NULL,
    TABLE_SCHEMA varchar(64) NOT NULL,
    MLOG_ID varchar(64) NOT NULL,
    MLOG_NAME varchar(64) NOT NULL,
    MLOG_COLUMNS longtext NOT NULL,
    BASE_TABLE_CATALOG varchar(512) NOT NULL,
    BASE_TABLE_SCHEMA varchar(64) NOT NULL,
    BASE_TABLE_ID varchar(64) NOT NULL,
    BASE_TABLE_NAME varchar(64) NOT NULL,
    PURGE_METHOD varchar(32) NOT NULL,
    PURGE_START datetime NOT NULL,
    PURGE_INTERVAL bigint NOT NULL,
	RELATED_MV varchar(256) DEFAULT NULL,
    PRIMARY KEY(MLOG_ID),
    UNIQUE KEY uniq_base_table(BASE_TABLE_ID),
    UNIQUE KEY uniq_mlog_name(TABLE_SCHEMA, MLOG_NAME))`

// CreateTiDBMViewRefreshHistTable is a table to store mview refresh history.
// Note: REFRESH_JOB_ID is auto-increment BIGINT (internal), while information_schema expects varchar.
CreateTiDBMViewRefreshHistTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_mview_refresh_hist (
    MVIEW_ID varchar(64) NOT NULL,
    MVIEW_NAME varchar(64) NOT NULL,
    REFRESH_JOB_ID bigint NOT NULL AUTO_INCREMENT,
    IS_NEWEST_REFRESH varchar(3) NOT NULL,
    REFRESH_METHOD varchar(32) NOT NULL,
    REFRESH_TIME datetime DEFAULT NULL,
    REFRESH_ENDTIME datetime DEFAULT NULL,
    REFRESH_STATUS varchar(16) DEFAULT NULL,
    PRIMARY KEY(REFRESH_JOB_ID),
    KEY idx_mview_newest(MVIEW_ID, IS_NEWEST_REFRESH))`

// CreateTiDBMLogPurgeHistTable is a table to store mlog purge history.
// Note: PURGE_JOB_ID is auto-increment BIGINT (internal), while information_schema expects varchar.
CreateTiDBMLogPurgeHistTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_mlog_purge_hist (
    MLOG_ID varchar(64) NOT NULL,
    MLOG_NAME varchar(64) NOT NULL,
    PURGE_JOB_ID bigint NOT NULL AUTO_INCREMENT,
    IS_NEWEST_PURGE varchar(3) NOT NULL,
    PURGE_METHOD varchar(32) NOT NULL,
    PURGE_TIME datetime DEFAULT NULL,
    PURGE_ENDTIME datetime DEFAULT NULL,
    PURGE_ROWS bigint NOT NULL,
    PURGE_STATUS varchar(16) DEFAULT NULL,
    PRIMARY KEY(PURGE_JOB_ID),
    KEY idx_mlog_newest(MLOG_ID, IS_NEWEST_PURGE))`
*/

func (t *MVService) Start() {
	if t.running.Swap(true) {
		return
	}
	t.sch.init(t.ctx)
	t.ddlDirty.Store(true)
	t.notifier.Wake()

	t.runWg.Add(1)
	go t.scheduleLoop()
}

func (t *MVService) scheduleLoop() {
	timer := time.NewTimer(0)
	defer func() {
		timer.Stop()
		t.runWg.Done()
	}()

	for {
		forceFetch := false
		select {
		case <-timer.C:
		case <-t.notifier.C:
			t.notifier.clear()
			forceFetch = t.ddlDirty.Swap(false)
		case <-t.ctx.Done():
			return
		}

		if !t.running.Load() {
			return
		}

		now := time.Now()

		if forceFetch || t.shouldFetch(now) {
			t.sch.Refresh(t.ctx)
			if err := t.FetchAllMVMeta(t.ctx); err != nil {
				// avoid tight retries on fetch failures
				t.lastRefresh.Store(now.UnixMilli())
			}
		}

		mvLogToPurge, mvToRefresh := t.fetchExecTasks(now)
		t.purgeMVLog(mvLogToPurge)
		t.refreshMV(mvToRefresh)

		next := t.nextScheduleTime(now)
		resetTimer(timer, time.Until(next))
	}
}

// Close stops the background refresh loop.
func (t *MVService) Close() {
	t.cancel()
	if !t.running.Swap(false) {
		return
	}
	t.runWg.Wait()
	t.executor.Close()
}

func (t *MVService) shouldFetch(now time.Time) bool {
	last := t.lastRefresh.Load()
	if last == 0 {
		return true
	}
	return now.Sub(time.UnixMilli(last)) >= defaultMVFetchInterval
}

func (t *MVService) nextFetchTime(now time.Time) time.Time {
	last := t.lastRefresh.Load()
	if last == 0 {
		return now
	}
	next := time.UnixMilli(last).Add(defaultMVFetchInterval)
	if next.Before(now) {
		return now
	}
	return next
}

func (t *MVService) nextDueTime() (time.Time, bool) {
	next := time.Time{}
	has := false
	{
		t.mvRefreshMu.Lock()
		if item := t.mvRefreshMu.prio.Front(); item != nil {
			next = time.UnixMilli(item.Value.orderTs)
			has = true
		}
		t.mvRefreshMu.Unlock()
	}

	{
		t.mvLogPurgeMu.Lock()
		if item := t.mvLogPurgeMu.prio.Front(); item != nil {
			due := time.UnixMilli(item.Value.orderTs)
			if !has || due.Before(next) {
				next = due
				has = true
			}
		}
		t.mvLogPurgeMu.Unlock()
	}
	return next, has
}

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

func (t *MVService) fetchExecTasks(now time.Time) (mvLogToPurge []*mvLog, mvToRefresh []*mv) {
	{
		t.mvLogPurgeMu.Lock() // guard mvlog purge queue
		for t.mvLogPurgeMu.prio.Len() > 0 {
			it := t.mvLogPurgeMu.prio.Front()
			l := it.Value
			if l.orderTs != maxNextScheduleTs && l.nextPurge.Compare(now) <= 0 {
				mvLogToPurge = append(mvLogToPurge, l)
				l.orderTs = maxNextScheduleTs // set to max to avoid being picked again before reschedule
				t.mvLogPurgeMu.prio.Update(it, l)
			} else {
				break
			}
		}
		t.metrics.pendingMVLogPurgeCount.Store(int64(t.mvLogPurgeMu.prio.Len()))
		t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard
	}
	{
		t.mvRefreshMu.Lock() // guard mv refresh queue
		for t.mvRefreshMu.prio.Len() > 0 {
			it := t.mvRefreshMu.prio.Front()
			m := it.Value
			if m.orderTs != maxNextScheduleTs && m.nextRefresh.Compare(now) <= 0 {
				mvToRefresh = append(mvToRefresh, m)
				m.orderTs = maxNextScheduleTs // set to max to avoid being picked again before reschedule
				t.mvRefreshMu.prio.Update(it, m)
			} else {
				break
			}
		}
		t.metrics.pendingMVRefreshCount.Store(int64(t.mvRefreshMu.prio.Len()))
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
			_, nextRefresh, err := t.mh.RefreshMV(t.ctx, t.sysSessionPool, m.ID)
			if err != nil {
				retryCount := m.retryCount.Add(1)
				t.rescheduleMV(m, time.Now().Add(retryDelay(int(retryCount))).UnixMilli())
				t.notifier.Wake()
				return err
			}
			m.retryCount.Store(0)
			t.rescheduleMV(m, nextRefresh.UnixMilli())
			t.notifier.Wake()
			return nil
		})
	}
}

func (t *MVService) purgeMVLog(mvLogToPurge []*mvLog) {
	if len(mvLogToPurge) == 0 {
		return
	}
	for _, l := range mvLogToPurge {
		t.executor.Submit("mvlog-purge/"+l.ID, func() error {
			t.metrics.runningMVLogPurgeCount.Add(1)
			defer t.metrics.runningMVLogPurgeCount.Add(-1)
			nextPurge, err := t.mh.PurgeMVLog(t.ctx, t.sysSessionPool, l.ID)
			if err != nil {
				retryCount := l.retryCount.Add(1)
				t.rescheduleMVLog(l, time.Now().Add(retryDelay(int(retryCount))).UnixMilli())
				t.notifier.Wake()
				return err
			}
			l.retryCount.Store(0)
			t.rescheduleMVLog(l, nextPurge.UnixMilli())
			t.notifier.Wake()
			return nil
		})
	}
}

func (t *MVService) rescheduleMV(m *mv, next int64) {
	t.mvRefreshMu.Lock() // guard mv refresh queue
	if it, ok := t.mvRefreshMu.pending[m.ID]; ok && it.Value == m {
		m.orderTs = next
		t.mvRefreshMu.prio.Update(it, m)
	}
	t.metrics.pendingMVRefreshCount.Store(int64(t.mvRefreshMu.prio.Len()))
	t.mvRefreshMu.Unlock() // release mv refresh queue guard
}

func (t *MVService) rescheduleMVLog(l *mvLog, next int64) {
	t.mvLogPurgeMu.Lock() // guard mvlog purge queue
	if it, ok := t.mvLogPurgeMu.pending[l.ID]; ok && it.Value == l {
		l.orderTs = next
		t.mvLogPurgeMu.prio.Update(it, l)
	}
	t.metrics.pendingMVLogPurgeCount.Store(int64(t.mvLogPurgeMu.prio.Len()))
	t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard
}

func (t *MVService) buildMLogPurgeTasks(newPending map[string]*mvLog) error {
	t.mvLogPurgeMu.Lock()         // guard mvlog purge queue
	defer t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard

	if t.mvLogPurgeMu.pending == nil {
		t.mvLogPurgeMu.pending = make(map[string]mvLogItem, len(newPending))
	}
	for id, nl := range newPending {
		if ol, ok := t.mvLogPurgeMu.pending[id]; ok {
			{ // copy fields that may be updated by purge task execution to avoid overwriting them
				ol.Value.purgeInterval = nl.purgeInterval
			}
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
	t.metrics.pendingMVLogPurgeCount.Store(int64(t.mvLogPurgeMu.prio.Len()))

	return nil
}

func (t *MVService) buildMVRefreshTasks(newPending map[string]*mv) error {
	t.mvRefreshMu.Lock()         // guard mv refresh queue
	defer t.mvRefreshMu.Unlock() // release mv refresh queue guard

	if t.mvRefreshMu.pending == nil {
		t.mvRefreshMu.pending = make(map[string]mvItem, len(newPending))
	}
	for id, nm := range newPending {
		if om, ok := t.mvRefreshMu.pending[id]; ok {
			{ // copy fields that may be updated by refresh task execution to avoid overwriting them
				om.Value.refreshInterval = nm.refreshInterval
			}
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
	t.metrics.pendingMVRefreshCount.Store(int64(t.mvRefreshMu.prio.Len()))

	return nil
}

func (t *MVService) fetchAllTiDBMLogPurge(ctx context.Context) error {
	newPending, err := t.mh.fetchAllTiDBMLogPurge(ctx, t.sysSessionPool)
	if err != nil {
		return err
	}
	for id := range newPending {
		if id == "" || !t.sch.Available(id) {
			delete(newPending, id)
		}
	}
	return t.buildMLogPurgeTasks(newPending)
}

func (t *MVService) fetchAllTiDBMViews(ctx context.Context) error {
	newPending, err := t.mh.fetchAllTiDBMViews(ctx, t.sysSessionPool)
	if err != nil {
		return err
	}
	for id := range newPending {
		if id == "" || !t.sch.Available(id) {
			delete(newPending, id)
		}
	}
	return t.buildMVRefreshTasks(newPending)
}

func (t *MVService) FetchAllMVMeta(ctx context.Context) error {
	if err := t.fetchAllTiDBMLogPurge(ctx); err != nil {
		return err
	}
	if err := t.fetchAllTiDBMViews(ctx); err != nil {
		return err
	}

	t.lastRefresh.Store(time.Now().UnixMilli())
	return nil
}
