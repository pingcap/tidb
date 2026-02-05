package utils

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

type mvLogItem Item[*mvLog]
type mvItem Item[*mv]

type MVJobsManager struct {
	sysSessionPool basic.SessionPool
	lastRefresh    time.Time
	running        atomic.Bool
	sch            *ServerConsistentHash
	maxConcurrency int
	taskTimeout    time.Duration
	executor       *TaskExecutor
	mvDDLEventCh   Notifier

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
)

// NewMVJobsManager creates a MVJobsManager with a SQL executor.
func NewMVJobsManager(se basic.SessionPool) *MVJobsManager {
	mgr := &MVJobsManager{
		sysSessionPool: se,
		sch:            NewServerConsistentHash(10),
		maxConcurrency: defaultMVTaskMaxConcurrency,
		taskTimeout:    defaultMVTaskTimeout,
		mvDDLEventCh:   NewNotifier(),
	}
	return mgr
}

// SetTaskExecConfig sets the execution config for MV tasks.
// It should be called before Start.
func (t *MVJobsManager) SetTaskExecConfig(maxConcurrency int, timeout time.Duration) {
	t.executor.UpdateConfig(maxConcurrency, timeout)
}

type mv struct {
	ID              string
	refreshInterval time.Duration

	nextRefresh time.Time
}

type mvLog struct {
	ID            string
	purgeInterval time.Duration

	nextPurge time.Time
}

func (m *mv) Less(other *mv) bool {
	return m.nextRefresh.Before(other.nextRefresh)
}

func (m *mvLog) Less(other *mvLog) bool {
	return m.nextPurge.Before(other.nextPurge)
}

/*
TODO: 实现 purge 逻辑，伪代码如下：

BEGIN;
SELECT * FROM mysql.tidb_mlog_purge WHERE MLOG_ID = ? FOR UPDATE;

find_min_mv_tso

	找出所有依赖的 MV，返回这些 MV 中最小的 LAST_REFRESH_READ_TSO

DELETE FROM find_mvlog_by_base_table() WHERE COMMIT_TSO IN (0, find_min_mv_tso()];

UPDATE mysql.tidb_mlog_purge SET LAST_PURGE_TIME = now(), LAST_PURGE_ERR = NULL WHERE MLOG_ID = ?;

COMMIT;
*/
func (m *mvLog) purge() (nextPurge time.Time, err error) {
	return
}

/*
TODO: implement the refresh logic, the pseudo code is as follows:

BEGIN;

SELECT * FROM mysql.tidb_mview_refresh WHERE MVIEW_ID = ? FOR UPDATE;

如果非手动 refresh 且 LAST_REFRESH_TIME + REFRESH_INTERVAL > now()

	COMMIT;
	RETURN;

TSO = GET_COMMIT_TSO();

找出该物化视图所依赖的所有 MVLOG

READ MVLOG data in range (LAST_REFRESH_READ_TSO, TSO])

COMPUTE new MV data;

UPDATE MV table with new data;

UPDATE mysql.tidb_mview_refresh SET

	LAST_REFRESH_TIME = now(),
	LAST_REFRESH_READ_TSO = TSO,
	LAST_REFRESH_ERR = NULL

WHERE MVIEW_ID = ?;

COMMIT;

返回所有相关 MVLOG 的 ID
*/
func (m *mv) refresh() (relatedMVLog []string, nextRefresh time.Time, err error) {
	return
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

/*

CREATE TABLE IF NOT EXISTS mysql.tidb_mlog_purge (
	MLOG_ID bigint NOT NULL,
	LAST_PURGE_TIME datetime DEFAULT NULL,
	LAST_PURGE_ROWS bigint DEFAULT NULL,
	LAST_PURGE_DURATION bigint DEFAULT NULL,
	PRIMARY KEY(MLOG_ID)
)

CREATE TABLE IF NOT EXISTS mysql.tidb_mview_refresh (
	MVIEW_ID bigint NOT NULL,
	LAST_REFRESH_RESULT varchar(16) DEFAULT NULL,
	LAST_REFRESH_TYPE varchar(16) DEFAULT NULL,
	LAST_REFRESH_TIME datetime DEFAULT NULL,
	LAST_SUCCESSFUL_REFRESH_READ_TSO bigint DEFAULT NULL,
	LAST_REFRESH_FAILED_REASON longtext DEFAULT NULL,
	PRIMARY KEY(MVIEW_ID)
)
*/

func (t *MVJobsManager) Start() {
	if t.running.Swap(true) {
		return
	}
	if t.executor == nil {
		t.executor = NewTaskExecutor(t.maxConcurrency, t.taskTimeout)
	}
	ticker := time.NewTicker(time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if time.Since(t.lastRefresh) >= time.Second*30 {
					t.mvDDLEventCh.Wake()
				}
			case <-t.mvDDLEventCh.C:
				t.mvDDLEventCh.clear()
				if !t.running.Load() {
					return
				}
				t.FetchAll()
				mvLogToPurge, mvToRefresh := t.fetchExecTasks()
				t.purge_mvLog(mvLogToPurge)
				t.refreshMV(mvToRefresh)
			}
		}
	}()
}

// Close stops the background refresh loop.
func (t *MVJobsManager) Close() {
	if t.running.Swap(false) {
		t.mvDDLEventCh.Wake()
	}
	if t.executor != nil {
		t.executor.Close()
		t.executor = nil
	}
}

func (t *MVJobsManager) fetchExecTasks() (mvLogToPurge []*mvLog, mvToRefresh []*mv) {
	now := time.Now()
	{
		t.mvLogPurgeMu.Lock() // guard mvlog purge queue
		for t.mvLogPurgeMu.prio.Len() > 0 {
			l := t.mvLogPurgeMu.prio.Front().Value
			if l.nextPurge.Before(now) {
				mvLogToPurge = append(mvLogToPurge, l)
				t.mvLogPurgeMu.prio.Pop()
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
			m := t.mvRefreshMu.prio.Front().Value
			if m.nextRefresh.Before(now) {
				mvToRefresh = append(mvToRefresh, m)
				t.mvRefreshMu.prio.Pop()
			} else {
				break
			}
		}
		t.metrics.pendingMVRefreshCount.Store(int64(t.mvRefreshMu.prio.Len()))
		t.mvRefreshMu.Unlock() // release mv refresh queue guard
	}
	return
}

func (t *MVJobsManager) refreshMV(mvToRefresh []*mv) {
	if len(mvToRefresh) == 0 {
		return
	}
	for _, m := range mvToRefresh {
		t.executor.Submit("mv-refresh/"+m.ID, func() error {
			t.metrics.runningMVRefreshCount.Add(1)
			defer t.metrics.runningMVRefreshCount.Add(-1)
			_, nextRefresh, err := m.refresh()
			if err != nil {
				return err
			}
			t.mvRefreshMu.Lock() // guard mv refresh queue
			if p, ok := t.mvRefreshMu.pending[m.ID]; ok && p.Value == m {
				m.nextRefresh = nextRefresh
				t.mvRefreshMu.pending[m.ID] = t.mvRefreshMu.prio.Push(m)
			}
			t.metrics.pendingMVRefreshCount.Store(int64(t.mvRefreshMu.prio.Len()))
			t.mvRefreshMu.Unlock() // release mv refresh queue guard
			return nil
		})
	}
}

func (t *MVJobsManager) purge_mvLog(mvLogToPurge []*mvLog) {
	if len(mvLogToPurge) == 0 {
		return
	}
	for _, l := range mvLogToPurge {
		t.executor.Submit("mvlog-purge/"+l.ID, func() error {
			t.metrics.runningMVLogPurgeCount.Add(1)
			defer t.metrics.runningMVLogPurgeCount.Add(-1)
			nextPurge, err := l.purge()
			if err != nil {
				return err
			}
			t.mvLogPurgeMu.Lock() // guard mvlog purge queue
			if p, ok := t.mvLogPurgeMu.pending[l.ID]; ok && p.Value == l {
				l.nextPurge = nextPurge
				t.mvLogPurgeMu.pending[l.ID] = t.mvLogPurgeMu.prio.Push(l)
			}
			t.metrics.pendingMVLogPurgeCount.Store(int64(t.mvLogPurgeMu.prio.Len()))
			t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard
			return nil
		})
	}
}

// ExecRCRestrictedSQL is used to execute a restricted SQL which related to resource control.
func ExecRCRestrictedSQL(sysSessionPool basic.SessionPool, sql string, params []any) ([]chunk.Row, error) {
	se, err := sysSessionPool.Get()
	defer func() {
		sysSessionPool.Put(se)
	}()
	if err != nil {
		return nil, err
	}
	sctx := se.(sessionctx.Context)
	exec := sctx.GetRestrictedSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	r, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		sql, params...,
	)
	return r, err
}

func (t *MVJobsManager) buildMLogPurgeTasks(newPending map[string]*mvLog) error {
	t.mvLogPurgeMu.Lock()         // guard mvlog purge queue
	defer t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard

	if t.mvLogPurgeMu.pending == nil {
		t.mvLogPurgeMu.pending = make(map[string]mvLogItem, len(newPending))
	}
	for id, nl := range newPending {
		if ol, ok := t.mvLogPurgeMu.pending[id]; ok {
			if ol.Value.nextPurge != nl.nextPurge {
				ol.Value.nextPurge = nl.nextPurge
				t.mvLogPurgeMu.prio.Update(ol, ol.Value)
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

func (t *MVJobsManager) buildMVRefreshTasks(newPending map[string]*mv) error {
	t.mvRefreshMu.Lock()         // guard mv refresh queue
	defer t.mvRefreshMu.Unlock() // release mv refresh queue guard

	if t.mvRefreshMu.pending == nil {
		t.mvRefreshMu.pending = make(map[string]mvItem, len(newPending))
	}
	for id, nm := range newPending {
		if om, ok := t.mvRefreshMu.pending[id]; ok {
			if om.Value.nextRefresh != nm.nextRefresh {
				om.Value.nextRefresh = nm.nextRefresh
				t.mvRefreshMu.prio.Update(om, om.Value)
			}
			continue
		}
		t.mvRefreshMu.pending[id] = t.mvRefreshMu.prio.Push(nm)
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

func (t *MVJobsManager) fetchAllTiDBMLogPurge() error {
	const SQL = `SELECT MLOG_ID, PURGE_INTERVAL, LAST_PURGE_TIME FROM mysql.tidb_mlog_purge t join mysql.tidb_mlogs l on t.MLOG_ID = l.MLOG_ID`
	rows, err := ExecRCRestrictedSQL(t.sysSessionPool, SQL, nil)
	if err != nil {
		return err
	}
	newPending := make(map[string]*mvLog, len(rows))
	for _, row := range rows {
		l := &mvLog{
			ID: row.GetString(0),
		}
		if l.ID == "" || !t.sch.Available(l.ID) {
			continue
		}
		l.purgeInterval = time.Second * time.Duration(row.GetInt64(1))
		if !row.IsNull(2) {
			gt, _ := row.GetTime(2).GoTime(time.Local)
			l.nextPurge = gt.Add(l.purgeInterval)
		}
		newPending[l.ID] = l
	}

	return t.buildMLogPurgeTasks(newPending)
}

func (t *MVJobsManager) fetchAllTiDBMViews() error {
	const SQL = `SELECT MVIEW_ID, REFRESH_INTERVAL, LAST_REFRESH_TIME FROM mysql.tidb_mview_refresh t JOIN mysql.tidb_mviews v ON t.MVIEW_ID = v.MVIEW_ID`
	rows, err := ExecRCRestrictedSQL(t.sysSessionPool, SQL, nil)

	if err != nil {
		return err
	}
	newPending := make(map[string]*mv, len(rows))
	for _, row := range rows {
		m := &mv{
			ID: row.GetString(0),
		}
		if m.ID == "" {
			continue
		}
		if !t.sch.Available(m.ID) {
			continue
		}
		m.refreshInterval = time.Duration(row.GetInt64(1)) * time.Second
		if !row.IsNull(2) {
			gt, _ := row.GetTime(2).GoTime(time.Local)
			m.nextRefresh = gt.Add(m.refreshInterval)
		}
		newPending[m.ID] = m
	}

	return t.buildMVRefreshTasks(newPending)
}

func (t *MVJobsManager) FetchAll() error {
	if err := t.fetchAllTiDBMLogPurge(); err != nil {
		return err
	}
	if err := t.fetchAllTiDBMViews(); err != nil {
		return err
	}

	t.lastRefresh = time.Now()
	return nil
}

const (
	ActionAddMV     = 146
	ActionDropMV    = 147
	ActionAlterMV   = 148
	ActionAddMVLog  = 149
	ActionDropMVLog = 150
)

var mvs *MVJobsManager

// RegisterMVS registers a DDL event handler for MV-related events.
func RegisterMVS(ddlNotifier *notifier.DDLNotifier, se basic.SessionPool) {
	if ddlNotifier == nil {
		return
	}

	mvs = NewMVJobsManager(se)

	ddlNotifier.RegisterHandler(notifier.MVJobsHandlerID, func(_ context.Context, _ sessionctx.Context, event *notifier.SchemaChangeEvent) error {
		switch event.GetType() {
		case ActionAddMV, ActionDropMVLog, ActionAlterMV, ActionAddMVLog, ActionDropMV:
			mvs.mvDDLEventCh.Wake()
		}
		return nil
	})

	mvs.Start()
}
