package utils

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

type MVJobsManager struct {
	se          sessionctx.Context
	lastRefresh time.Time

	mvMu struct {
		sync.Mutex
		pending     map[string]*mv
		orderByNext []*mv
	}
	mvlogMu struct {
		sync.Mutex
		pending     map[string]*mvlog
		orderByNext []*mvlog
	}
}

// NewMVJobsManager creates a MVJobsManager with a SQL executor.
func NewMVJobsManager(se sessionctx.Context) *MVJobsManager {
	return &MVJobsManager{se: se}
}

type mv struct {
	ID              string
	lastRefreshTime time.Time
	refreshInterval time.Duration

	singleMlog  bool
	nextRefresh time.Time
}

type mvlog struct {
	ID            string
	purgeInterval time.Duration
	lastPurgeTime time.Time
	purgeMethod   string

	nextPurge time.Time
}

func (m *mvlog) purge() error {
	// 0. start transaction
	// 1. find all mysql.tidb_mlogs that depend on this mlog
	// 2. for each mview, find the last read_tso
	// 3. delete all entries in mlog whose tso < last read_tso
	// 4. update the mlog metadata in mysql.tidb_mlogs
	// 5. commit transaction
	return nil
}

func (m *mv) refresh() error {
	// 0. start transaction
	// 1. read the mview definition from mysql.tidb_mviews
	// 2. execute the definition to get the new data
	// 3. replace the data in the mview table
	// 4. update the mview metadata in mysql.tidb_mviews
	// 5. commit transaction
	return nil
}

func (m *mv) purgeMlogsAfterRefresh() error {
	// only if the mv depends on one mlog
	return nil
}

/*
// CreateTiDBMViewsTable is a table to store materialized view metadata.
CreateTiDBMViewsTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_mviews (
	TABLE_CATALOG varchar(512) NOT NULL,
	TABLE_SCHEMA varchar(64) NOT NULL,
	MVIEW_ID varchar(64) NOT NULL,
	MVIEW_NAME varchar(64) NOT NULL,
	MVIEW_OWNER varchar(64) NOT NULL,
	MVIEW_DEFINITION longtext NOT NULL,
	MVIEW_COMMENT varchar(128) DEFAULT NULL,
	MVIEW_TIFLASH_REPLICAS int DEFAULT 0,
	MVIEW_MODIFY_TIME datetime NOT NULL,
	REFRESH_METHOD varchar(32) DEFAULT NULL,
	REFRESH_MODE varchar(256) DEFAULT NULL,
	LAST_REFRESH_METHOD varchar(32) DEFAULT NULL,
	LAST_REFRESH_TIME datetime DEFAULT NULL,
	REFRESH_INTERVAL bigint NOT NULL,
	LAST_REFRESH_ENDTIME datetime DEFAULT NULL,
	STALENESS varchar(32) DEFAULT NULL,
	PRIMARY KEY(MVIEW_ID),
	UNIQUE KEY uniq_mview_name(TABLE_SCHEMA, MVIEW_NAME))`

// CreateTiDBMLogsTable is a table to store materialized view log metadata.
CreateTiDBMLogsTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_mlogs (
	TABLE_CATALOG varchar(512) NOT NULL,
	TABLE_SCHEMA varchar(64) NOT NULL,
	MLOG_ID varchar(64) NOT NULL,
	MLOG_NAME varchar(64) NOT NULL,
	MLOG_OWNER varchar(64) NOT NULL,
	MLOG_COLUMNS longtext NOT NULL,
	BASE_TABLE_CATALOG varchar(512) NOT NULL,
	BASE_TABLE_SCHEMA varchar(64) NOT NULL,
	BASE_TABLE_ID varchar(64) NOT NULL,
	BASE_TABLE_NAME varchar(64) NOT NULL,
	PURGE_METHOD varchar(32) NOT NULL,
	PURGE_START datetime NOT NULL,
	PURGE_INTERVAL bigint NOT NULL,
	LAST_PURGE_TIME datetime NOT NULL,
	LAST_PURGE_ROWS bigint NOT NULL,
	LAST_PURGE_DURATION bigint NOT NULL,
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

// CreateTiDBMLogJobTable is a table for scheme2 worker coordination (one row per base table / mlog).
// It's an internal table and not exposed via information_schema.
CreateTiDBMLogJobTable = `CREATE TABLE IF NOT EXISTS mysql.tidb_mlog_job (
	MLOG_ID varchar(64) NOT NULL,
	BASE_TABLE_ID varchar(64) NOT NULL,
	NEXT_RUN_TIME datetime DEFAULT NULL,
	LEASE_OWNER varchar(64) DEFAULT NULL,
	LEASE_EXPIRE datetime DEFAULT NULL,
	STATE varchar(32) NOT NULL DEFAULT 'idle',
	LAST_RUN_TIME datetime DEFAULT NULL,
	LAST_ERR longtext DEFAULT NULL,
	HEARTBEAT datetime DEFAULT NULL,
	PRIMARY KEY(MLOG_ID),
	UNIQUE KEY uniq_job_base_table(BASE_TABLE_ID))`

Column Name
Description
mv_id
ID of Mv
last_refresh_result
Result of the last refresh, success or failure
last_refresh_type
The type of the last refresh includes at least two types: fast and complete, corresponding to incremental refresh and full refresh respectively
last_refresh_time
Time of the last refresh
last_refresh_read_tso
read_tso used during the last successful refresh
last_refresh_failed_reason
Reason for the last refresh failure (and time?)
*/

func (t *MVJobsManager) Start(sch *ServerConsistentHash) {
	// TODO: enhance refresh logic to avoid sync all frequently
	// 	listen to DDL events of add/remove/alter mview/mlog

	ticker := time.NewTicker(time.Second)
	go func() {
		for range ticker.C {
			if time.Since(t.lastRefresh) >= time.Second*20 {
				t.RefreshAll(sch)
			} else if time.Since(t.lastRefresh) >= time.Second {
				if update, err := t.AnyUpdate(sch); err != nil {
					continue
				} else if update {
					t.RefreshAll(sch)
				}
			}
		}
	}()
}

func (t *MVJobsManager) exec() error {
	now := time.Now()
	mvlogToPurge := make([]*mvlog, 0)
	mvToRefresh := make([]*mv, 0)
	{
		t.mvlogMu.Lock()
		for _, l := range t.mvlogMu.orderByNext {
			if l.nextPurge.After(now) {
				break
			}
			mvlogToPurge = append(mvlogToPurge, l)
		}
		t.mvlogMu.Unlock()
	}
	{
		t.mvMu.Lock()
		for _, m := range t.mvMu.orderByNext {
			if m.nextRefresh.After(now) {
				break
			}
			mvToRefresh = append(mvToRefresh, m)
		}
		t.mvMu.Unlock()
	}
	if len(mvToRefresh) > 0 {
		
	}
	if len(mvlogToPurge) > 0 {
	}
	return nil
}

func (t *MVJobsManager) AnyUpdate(sch *ServerConsistentHash) (bool, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	exec := t.se.GetSQLExecutor()
	{
		rs, err := exec.ExecuteInternal(ctx, "SELECT count(1) FROM mysql.tidb_mlogs")
		if rs != nil {
			//nolint: errcheck
			defer rs.Close()
		}
		if err != nil {
			return false, err
		}
		count := int64(0)
		if rs != nil {
			rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
			if err != nil {
				return false, err
			}
			count = rows[0].GetInt64(0)
		}
		t.mvlogMu.Lock()
		pendingCount := int64(len(t.mvlogMu.pending))
		t.mvlogMu.Unlock()

		if count != pendingCount {
			return true, nil
		}
	}
	{
		rs, err := exec.ExecuteInternal(ctx, "SELECT count(1) FROM mysql.tidb_mviews")
		if rs != nil {
			//nolint: errcheck
			defer rs.Close()
		}
		if err != nil {
			return false, err
		}
		count := int64(0)
		if rs != nil {
			rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
			if err != nil {
				return false, err
			}
			count = rows[0].GetInt64(0)
		}

		t.mvMu.Lock()
		pendingCount := int64(len(t.mvMu.pending))
		t.mvMu.Unlock()

		if count != pendingCount {
			return true, nil
		}
	}
	return false, nil
}

func (t *MVJobsManager) RefreshAll(sch *ServerConsistentHash) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	exec := t.se.GetSQLExecutor()
	{
		rs, err := exec.ExecuteInternal(ctx, "SELECT MLOG_ID, PURGE_INTERVAL, PURGE_METHOD, LAST_PURGE_TIME FROM mysql.tidb_mlogs")
		if rs != nil {
			//nolint: errcheck
			defer rs.Close()
		}
		if err != nil {
			return err
		}
		if rs != nil {
			rows, err := sqlexec.DrainRecordSet(ctx, rs, 8)
			if err != nil {
				return err
			}
			newPending := make(map[string]*mvlog, len(rows))
			for _, row := range rows {
				l := &mvlog{
					ID: row.GetString(0),
				}
				if l.ID == "" {
					continue
				}
				if !sch.Available(l.ID) {
					continue
				}
				l.purgeInterval = time.Second * time.Duration(row.GetInt64(1))
				l.purgeMethod = row.GetString(2)
				if !row.IsNull(3) {
					lastPurgeTime, _ := row.GetTime(3).GoTime(time.Local)
					l.lastPurgeTime = lastPurgeTime
				}
				if l.purgeInterval > 0 && !l.lastPurgeTime.IsZero() {
					l.nextPurge = l.lastPurgeTime.Add(l.purgeInterval)
				}
				newPending[l.ID] = l
			}
			newOrderByNext := make([]*mvlog, 0, len(newPending))
			for _, l := range newPending {
				newOrderByNext = append(newOrderByNext, l)
			}
			slices.SortFunc(newOrderByNext, func(a, b *mvlog) int {
				return a.nextPurge.Compare(b.nextPurge)
			})

			t.mvlogMu.Lock()
			t.mvlogMu.pending = newPending
			t.mvlogMu.orderByNext = newOrderByNext
			t.mvlogMu.Unlock()
		}
	}
	{
		rs, err := exec.ExecuteInternal(ctx, "SELECT MVIEW_ID, REFRESH_INTERVAL, LAST_REFRESH_TIME FROM mysql.tidb_mviews")
		if rs != nil {
			//nolint: errcheck
			defer rs.Close()
		}
		if err != nil {
			return err
		}
		if rs != nil {

			rows, err := sqlexec.DrainRecordSet(ctx, rs, 8)
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
				if !sch.Available(m.ID) {
					continue
				}
				m.refreshInterval = time.Duration(row.GetInt64(1)) * time.Second
				if !row.IsNull(2) {
					lastRefreshTime, _ := row.GetTime(2).GoTime(time.Local)
					m.lastRefreshTime = lastRefreshTime
				}
				if m.refreshInterval > 0 && !m.lastRefreshTime.IsZero() {
					m.nextRefresh = m.lastRefreshTime.Add(m.refreshInterval)
				}
				newPending[m.ID] = m
			}
			newOrderByNext := make([]*mv, 0, len(newPending))
			for _, m := range newPending {
				newOrderByNext = append(newOrderByNext, m)
			}
			slices.SortFunc(newOrderByNext, func(a, b *mv) int {
				return a.nextRefresh.Compare(b.nextRefresh)
			})

			t.mvMu.Lock()
			t.mvMu.pending = newPending
			t.mvMu.orderByNext = newOrderByNext
			t.mvMu.Unlock()
		}
	}

	t.lastRefresh = time.Now()
	return nil
}
