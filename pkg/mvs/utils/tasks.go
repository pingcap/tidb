package utils

import (
	"container/heap"
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

type mvLogItem *Item[*mvLog]
type mvItem *Item[*mv]

type MVJobsManager struct {
	sysSessionPool basic.SessionPool
	lastRefresh    time.Time
	running        atomic.Bool
	sch            *ServerConsistentHash

	mvMu struct {
		sync.Mutex
		pending map[string]mvItem
		prio    PriorityQueue[*mv]
	}
	mvLogMu struct {
		sync.Mutex
		pending map[string]mvLogItem
		prio    PriorityQueue[*mvLog]
	}
}

var mvDDLEventCh = NewNotifier()

// NewMVJobsManager creates a MVJobsManager with a SQL executor.
func NewMVJobsManager(se basic.SessionPool) *MVJobsManager {
	mgr := &MVJobsManager{
		sysSessionPool: se,
		sch:            NewServerConsistentHash(10),
	}
	return mgr
}

type mv struct {
	ID              string
	refreshInterval time.Duration

	singleMLog  bool
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
    PURGE_METHOD varchar(32) NOT NULL,
    PURGE_START datetime DEFAULT NULL,
    PURGE_INTERVAL bigint DEFAULT NULL,
    LAST_PURGE_TIME datetime DEFAULT NULL,
    LAST_PURGE_ROWS bigint DEFAULT NULL,
    LAST_PURGE_DURATION bigint DEFAULT NULL,
    PRIMARY KEY(MLOG_ID)
)

CREATE TABLE IF NOT EXISTS mysql.tidb_mview_refresh (
    MVIEW_ID bigint NOT NULL,
    REFRESH_METHOD varchar(32) DEFAULT NULL,
    START_WITH varchar(256) DEFAULT NULL,
    REFRESH_INTERVAL bigint DEFAULT NULL,
    LAST_REFRESH_RESULT varchar(16) DEFAULT NULL,
    LAST_REFRESH_TYPE varchar(16) DEFAULT NULL,
    LAST_REFRESH_TIME datetime DEFAULT NULL,
    LAST_REFRESH_READ_TSO bigint DEFAULT NULL,
    LAST_REFRESH_FAILED_REASON longtext DEFAULT NULL,
    PRIMARY KEY(MVIEW_ID)
)
*/

func (t *MVJobsManager) Start() {
	if t.running.Swap(true) {
		return
	}
	ticker := time.NewTicker(time.Second)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if time.Since(t.lastRefresh) >= time.Second*20 {
					t.RefreshAll()
				}
			case <-mvDDLEventCh.C:
				mvDDLEventCh.clear()
				if !t.running.Load() {
					return
				}
				t.RefreshAll()
			}
		}
	}()
}

// Close stops the background refresh loop.
func (t *MVJobsManager) Close() {
	if t.running.Swap(false) {
		mvDDLEventCh.Wake()
	}
}

func (t *MVJobsManager) exec() error {
	now := time.Now()
	mvLogToPurge := make([]mvLogItem, 0)
	mvToRefresh := make([]mvItem, 0)
	{
		t.mvLogMu.Lock()
		for t.mvLogMu.prio.Len() > 0 {
			x := heap.Pop(&t.mvLogMu.prio).(mvLogItem)
			l := x.Value
			if l.nextPurge.Before(now) {
				mvLogToPurge = append(mvLogToPurge, x)
			} else {
				heap.Push(&t.mvLogMu.prio, x)
				break
			}
		}
		t.mvLogMu.Unlock()
	}
	{
		t.mvMu.Lock()
		for t.mvMu.prio.Len() > 0 {
			x := heap.Pop(&t.mvMu.prio).(mvItem)
			m := x.Value
			if m.nextRefresh.Before(now) {
				mvToRefresh = append(mvToRefresh, x)
			} else {
				heap.Push(&t.mvMu.prio, x)
				break
			}
		}
		t.mvMu.Unlock()
	}

	if len(mvToRefresh) > 0 {
		for _, m := range mvToRefresh {
			go func() {
				_, nextRefresh, err := m.Value.refresh()
				if err != nil {
					return
				}
				{
					t.mvMu.Lock()

					if p := t.mvMu.pending[m.Value.ID]; p == m {
						m.Value.nextRefresh = nextRefresh
						heap.Push(&t.mvMu.prio, m)
					}

					t.mvMu.Unlock()
				}
			}()
		}
	}
	if len(mvLogToPurge) > 0 {
		for _, l := range mvLogToPurge {
			go func() {
				nextPurge, err := l.Value.purge()
				if err != nil {
					return
				}
				{
					t.mvLogMu.Lock()

					if p := t.mvLogMu.pending[l.Value.ID]; p == l {
						l.Value.nextPurge = nextPurge
						heap.Push(&t.mvLogMu.prio, l)
					}

					t.mvLogMu.Unlock()
				}
			}()
		}
	}
	return nil
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

// deprecated
// func (t *MVJobsManager) AnyUpdate(sch *ServerConsistentHash) (bool, error) {
// 	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
// 	exec := t.se.GetSQLExecutor()
// 	{
// 		rs, err := exec.ExecuteInternal(ctx, "SELECT count(1) FROM mysql.tidb_mlogs")
// 		if rs != nil {
// 			//nolint: errcheck
// 			defer rs.Close()
// 		}
// 		if err != nil {
// 			return false, err
// 		}
// 		count := int64(0)
// 		if rs != nil {
// 			rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
// 			if err != nil {
// 				return false, err
// 			}
// 			count = rows[0].GetInt64(0)
// 		}
// 		t.mvLogMu.Lock()
// 		pendingCount := int64(len(t.mvLogMu.pending))
// 		t.mvLogMu.Unlock()
// 		if count != pendingCount {
// 			return true, nil
// 		}
// 	}
// 	{
// 		rs, err := exec.ExecuteInternal(ctx, "SELECT count(1) FROM mysql.tidb_mviews")
// 		if rs != nil {
// 			//nolint: errcheck
// 			defer rs.Close()
// 		}
// 		if err != nil {
// 			return false, err
// 		}
// 		count := int64(0)
// 		if rs != nil {
// 			rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
// 			if err != nil {
// 				return false, err
// 			}
// 			count = rows[0].GetInt64(0)
// 		}
// 		t.mvMu.Lock()
// 		pendingCount := int64(len(t.mvMu.pending))
// 		t.mvMu.Unlock()
// 		if count != pendingCount {
// 			return true, nil
// 		}
// 	}
// 	return false, nil
// }

func (t *MVJobsManager) refreshAllTiDBMLogPurge() error {
	rows, err := ExecRCRestrictedSQL(t.sysSessionPool, "SELECT MLOG_ID, PURGE_INTERVAL, LAST_PURGE_TIME FROM mysql.tidb_mlog_purge", nil)
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

	{
		t.mvLogMu.Lock()
		if t.mvLogMu.pending == nil {
			t.mvLogMu.pending = make(map[string]mvLogItem, len(newPending))
		}
		for id, nl := range newPending {
			if ol, ok := t.mvLogMu.pending[id]; ok {
				if ol.Value.nextPurge != nl.nextPurge {
					ol.Value.nextPurge = nl.nextPurge
					heap.Fix(&t.mvLogMu.prio, ol.index)
				}
				continue
			}
			item := &Item[*mvLog]{Value: nl}
			heap.Push(&t.mvLogMu.prio, item)
			t.mvLogMu.pending[id] = item
		}
		for id, item := range t.mvLogMu.pending {
			if _, ok := newPending[id]; ok {
				continue
			}
			delete(t.mvLogMu.pending, id)
			heap.Remove(&t.mvLogMu.prio, item.index)
		}
		t.mvLogMu.Unlock()
	}
	return nil
}

func (t *MVJobsManager) refreshAllTiDBMViews() error {
	rows, err := ExecRCRestrictedSQL(t.sysSessionPool, "SELECT MVIEW_ID, REFRESH_INTERVAL, LAST_REFRESH_TIME FROM mysql.tidb_mview_refresh", nil)

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

	t.mvMu.Lock()
	// TODO
	t.mvMu.Unlock()

	return nil
}

func (t *MVJobsManager) RefreshAll() error {
	if err := t.refreshAllTiDBMLogPurge(); err != nil {
		return err
	}
	if err := t.refreshAllTiDBMViews(); err != nil {
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
			mvDDLEventCh.Wake()
		}
		return nil
	})

	mvs.Start()
}
