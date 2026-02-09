package mvs

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	meta "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

/* Define mysql.tidb_mviews
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
*/

/* Define mysql.tidb_mlogs
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
*/

type serverHelper struct {
}

func (m *serverHelper) serverFilter(s serverInfo) bool {
	return true
}

func (m *serverHelper) getServerInfo() (serverInfo, error) {
	localSrv, err := infosync.GetServerInfo()
	if err != nil {
		return serverInfo{}, err
	}
	return serverInfo{
		ID: localSrv.ID,
	}, nil
}

func (m *serverHelper) getAllServerInfo(ctx context.Context) (map[string]serverInfo, error) {
	servers := make(map[string]serverInfo)
	allServers, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		return nil, err
	}
	for _, srv := range allServers {
		servers[srv.ID] = serverInfo{
			ID: srv.ID,
		}
	}
	return servers, nil
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
func (*serverHelper) RefreshMV(_ context.Context, _ basic.SessionPool, _ string) (relatedMVLog []string, nextRefresh time.Time, err error) {
	return nil, time.Time{}, ErrMVRefreshHandlerNotRegistered
}

/*
TODO: 实现 purge 逻辑，伪代码如下：

开始 txn

令 find_mvs_by_mslog：找出所有 mvLogID 关联的 MV

	执行 sql：select RELATED_MV,PURGE_START,PURGE_INTERVAL,MLOG_NAME from mysql.tidb_mlogs where MLOG_ID = `mvLogID`

令 find_min_mv_tso：找出所有 mvLogID 关联的 MV 中最小的 LAST_REFRESH_READ_TSO

	执行 sql：SELECT MIN(LAST_SUCCESSFUL_REFRESH_READ_TSO) as COMMIT_TSO FROM mysql.tidb_mview_refresh WHERE MVIEW_ID IN (find_mvs_by_mslog())

执行 sql： SELECT 1 FROM mysql.tidb_mlog_purge WHERE MLOG_ID = `mvLogID` FOR UPDATE

执行 sql：DELETE FROM `MLOG_NAME` WHERE COMMIT_TSO IN (0, find_min_mv_tso()];

last_purge_time = now()

UPDATE mysql.tidb_mlog_purge SET LAST_PURGE_TIME = `last_purge_time`, LAST_PURGE_ERR = NULL WHERE MLOG_ID = `mvLogID`;

执行结果记录到 mysql.tidb_mlog_purge_hist（表结构定义在 pkg/session/bootstrap.go）

提交 txn

将 PURGE_START 精确到秒级，计算从 PURGE_START 开始，每隔 PURGE_INTERVAL 秒需要执行一次 purge 的下次执行时间点，找出比 last_purge_time 晚的第一个时间点，作为 nextPurge 返回
*/
func (*serverHelper) PurgeMVLog(ctx context.Context, sysSessionPool basic.SessionPool, mvLogID string) (nextPurge time.Time, err error) {
	purgeTime := time.Now()
	purgeEndTime := purgeTime
	mlogName := mvLogID
	purgeMethod := "UNKNOWN"
	var failedErr error

	err = withRCRestrictedTxn(ctx, sysSessionPool, func(txnCtx context.Context, sctx sessionctx.Context) error {
		recordFailedPurge := func(stepErr error, purgeRows uint64) error {
			failedErr = stepErr
			purgeEndTime = time.Now()
			return recordMLogPurgeHist(txnCtx, sctx, mvLogID, mlogName, purgeMethod, purgeTime, purgeEndTime, purgeRows, "FAILED")
		}

		const findMLogSQL = `SELECT TABLE_SCHEMA, MLOG_NAME, RELATED_MV, PURGE_START, PURGE_INTERVAL, PURGE_METHOD FROM mysql.tidb_mlogs WHERE MLOG_ID = %?`
		rows, runErr := execRCRestrictedSQLWithSession(txnCtx, sctx, findMLogSQL, []any{mvLogID})
		if runErr != nil {
			return recordFailedPurge(runErr, 0)
		}
		if len(rows) == 0 {
			return recordFailedPurge(errors.New("mvlog metadata not found"), 0)
		}
		row := rows[0]
		if row.IsNull(0) || row.IsNull(1) || row.IsNull(3) || row.IsNull(4) {
			return recordFailedPurge(errors.New("mvlog metadata is invalid"), 0)
		}

		schemaName := row.GetString(0)
		mlogName = row.GetString(1)
		purgeStart, gtErr := row.GetTime(3).GoTime(time.Local)
		if gtErr != nil {
			return recordFailedPurge(errors.New("mvlog metadata is invalid"), 0)
		}
		purgeIntervalSec := row.GetInt64(4)
		if purgeIntervalSec <= 0 {
			return recordFailedPurge(errors.New("mvlog metadata is invalid"), 0)
		}
		if !row.IsNull(5) && row.GetString(5) != "" {
			purgeMethod = row.GetString(5)
		}

		var relatedMVs []string
		if !row.IsNull(2) {
			relatedMVs = parseRelatedMVIDs(row.GetString(2))
		}

		minRefreshReadTSO, runErr := getMinRefreshReadTSO(txnCtx, sctx, relatedMVs)
		if runErr != nil {
			return recordFailedPurge(runErr, 0)
		}

		const lockPurgeSQL = `SELECT 1 FROM mysql.tidb_mlog_purge WHERE MLOG_ID = %? FOR UPDATE`
		rows, runErr = execRCRestrictedSQLWithSession(txnCtx, sctx, lockPurgeSQL, []any{mvLogID})
		if runErr != nil {
			return recordFailedPurge(runErr, 0)
		}
		if len(rows) == 0 {
			return recordFailedPurge(errors.New("mvlog purge state not found"), 0)
		}

		const deleteMLogSQL = `DELETE FROM %n.%n WHERE COMMIT_TSO = 0 OR (COMMIT_TSO > 0 AND COMMIT_TSO <= %?)`
		deleteStart := time.Now()
		if _, runErr = execRCRestrictedSQLWithSession(txnCtx, sctx, deleteMLogSQL, []any{schemaName, mlogName, minRefreshReadTSO}); runErr != nil {
			return recordFailedPurge(runErr, 0)
		}
		deleteDurationMS := time.Since(deleteStart).Milliseconds()
		affectedRows := sctx.GetSessionVars().StmtCtx.AffectedRows()

		purgeEndTime = time.Now()
		const updatePurgeSQL = `UPDATE mysql.tidb_mlog_purge SET LAST_PURGE_TIME = %?, LAST_PURGE_ROWS = %?, LAST_PURGE_DURATION = %? WHERE MLOG_ID = %?`
		if _, runErr = execRCRestrictedSQLWithSession(txnCtx, sctx, updatePurgeSQL, []any{purgeEndTime, affectedRows, deleteDurationMS, mvLogID}); runErr != nil {
			return recordFailedPurge(runErr, affectedRows)
		}
		if runErr = recordMLogPurgeHist(txnCtx, sctx, mvLogID, mlogName, purgeMethod, purgeTime, purgeEndTime, affectedRows, "SUCCESS"); runErr != nil {
			return runErr
		}
		nextPurge = calcNextPurgeTime(purgeStart, purgeIntervalSec, purgeEndTime)
		return nil
	})
	if err != nil {
		return time.Time{}, err
	}
	return nextPurge, failedErr
}

func calcNextPurgeTime(purgeStart time.Time, purgeIntervalSec int64, last time.Time) time.Time {
	return calcNextScheduleTime(purgeStart, purgeIntervalSec, last)
}

func calcNextRefreshTime(refreshStart time.Time, refreshIntervalSec int64, last time.Time) time.Time {
	return calcNextScheduleTime(refreshStart, refreshIntervalSec, last)
}

func calcNextScheduleTime(start time.Time, intervalSec int64, last time.Time) time.Time {
	if intervalSec <= 0 {
		return last
	}
	startSec := start.Unix()
	lastSec := last.Unix()
	if lastSec < startSec {
		return time.Unix(startSec, 0)
	}
	elapsed := lastSec - startSec
	intervals := elapsed / intervalSec
	nextSec := startSec + (intervals+1)*intervalSec
	return time.Unix(nextSec, 0)
}

func (*serverHelper) fetchAllTiDBMLogPurge(ctx context.Context, sysSessionPool basic.SessionPool) (map[string]*mvLog, error) {
	const sql = `SELECT t.MLOG_ID, l.PURGE_START, l.PURGE_INTERVAL, t.LAST_PURGE_TIME FROM mysql.tidb_mlog_purge t JOIN mysql.tidb_mlogs l ON t.MLOG_ID = l.MLOG_ID`
	rows, err := execRCRestrictedSQL(ctx, sysSessionPool, sql, nil)
	if err != nil {
		return nil, err
	}
	newPending := make(map[string]*mvLog, len(rows))
	now := time.Now()
	for _, row := range rows {
		l := &mvLog{
			ID: row.GetString(0),
		}
		if l.ID == "" {
			continue
		}
		if row.IsNull(1) || row.IsNull(2) {
			continue
		}
		purgeStart, gtErr := row.GetTime(1).GoTime(time.Local)
		if gtErr != nil {
			continue
		}
		intervalSec := row.GetInt64(2)
		l.purgeInterval = time.Second * time.Duration(intervalSec)
		calcBase := now
		if !row.IsNull(3) {
			if gt, gtErr := row.GetTime(3).GoTime(time.Local); gtErr == nil {
				calcBase = gt
			}
		}
		l.nextPurge = calcNextPurgeTime(purgeStart, intervalSec, calcBase)
		l.orderTs = l.nextPurge.UnixMilli()
		newPending[l.ID] = l
	}
	return newPending, nil
}

func (*serverHelper) fetchAllTiDBMViews(ctx context.Context, sysSessionPool basic.SessionPool) (map[string]*mv, error) {
	const sql = `SELECT t.MVIEW_ID, v.REFRESH_START, v.REFRESH_INTERVAL, t.LAST_REFRESH_TIME FROM mysql.tidb_mview_refresh t JOIN mysql.tidb_mviews v ON t.MVIEW_ID = v.MVIEW_ID`
	rows, err := execRCRestrictedSQL(ctx, sysSessionPool, sql, nil)
	if err != nil {
		return nil, err
	}
	newPending := make(map[string]*mv, len(rows))
	now := time.Now()
	for _, row := range rows {
		m := &mv{
			ID: row.GetString(0),
		}
		if m.ID == "" {
			continue
		}
		if row.IsNull(1) || row.IsNull(2) {
			continue
		}
		refreshStart, gtErr := row.GetTime(1).GoTime(time.Local)
		if gtErr != nil {
			continue
		}
		intervalSec := row.GetInt64(2)
		m.refreshInterval = time.Duration(intervalSec) * time.Second
		calcBase := now
		if !row.IsNull(3) {
			if gt, gtErr := row.GetTime(3).GoTime(time.Local); gtErr == nil {
				calcBase = gt
			}
		}
		m.nextRefresh = calcNextRefreshTime(refreshStart, intervalSec, calcBase)
		m.orderTs = m.nextRefresh.UnixMilli()
		newPending[m.ID] = m
	}
	return newPending, nil
}

func execRCRestrictedSQL(ctx context.Context, sysSessionPool basic.SessionPool, sql string, params []any) ([]chunk.Row, error) {
	se, err := sysSessionPool.Get()
	if err != nil {
		return nil, err
	}
	defer sysSessionPool.Put(se)
	sctx := se.(sessionctx.Context)
	return execRCRestrictedSQLWithSession(ctx, sctx, sql, params)
}

func execRCRestrictedSQLWithSession(ctx context.Context, sctx sessionctx.Context, sql string, params []any) ([]chunk.Row, error) {
	r, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		sql, params...,
	)
	return r, err
}

func withRCRestrictedTxn(ctx context.Context, sysSessionPool basic.SessionPool, fn func(txnCtx context.Context, sctx sessionctx.Context) error) (err error) {
	se, err := sysSessionPool.Get()
	if err != nil {
		return err
	}
	defer sysSessionPool.Put(se)

	sctx := se.(sessionctx.Context)
	sqlExec := sctx.GetSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)

	if _, err = sqlExec.ExecuteInternal(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			_, _ = sqlExec.ExecuteInternal(ctx, "ROLLBACK")
			panic(r)
		}
		if err == nil {
			_, err = sqlExec.ExecuteInternal(ctx, "COMMIT")
			return
		}
		_, _ = sqlExec.ExecuteInternal(ctx, "ROLLBACK")
	}()

	err = fn(ctx, sctx)
	return err
}

func parseRelatedMVIDs(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	mvIDs := make([]string, 0, len(parts))
	for _, p := range parts {
		id := strings.TrimSpace(p)
		if id == "" {
			continue
		}
		mvIDs = append(mvIDs, id)
	}
	return mvIDs
}

func getMinRefreshReadTSO(ctx context.Context, sctx sessionctx.Context, mvIDs []string) (int64, error) {
	if len(mvIDs) == 0 {
		return 0, nil
	}
	var b strings.Builder
	params := make([]any, 0, len(mvIDs))
	b.WriteString(`SELECT COALESCE(MIN(LAST_SUCCESSFUL_REFRESH_READ_TSO), 0) FROM mysql.tidb_mview_refresh WHERE MVIEW_ID IN (`)
	for i, id := range mvIDs {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("%?")
		params = append(params, id)
	}
	b.WriteString(")")
	rows, err := execRCRestrictedSQLWithSession(ctx, sctx, b.String(), params)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		return 0, nil
	}
	return rows[0].GetInt64(0), nil
}

func recordMLogPurgeHist(
	ctx context.Context,
	sctx sessionctx.Context,
	mvLogID string,
	mlogName string,
	purgeMethod string,
	purgeTime time.Time,
	purgeEndTime time.Time,
	purgeRows uint64,
	purgeStatus string,
) error {
	if mlogName == "" {
		mlogName = mvLogID
	}
	if purgeMethod == "" {
		purgeMethod = "UNKNOWN"
	}
	const clearNewestSQL = `UPDATE mysql.tidb_mlog_purge_hist SET IS_NEWEST_PURGE = 'NO' WHERE MLOG_ID = %? AND IS_NEWEST_PURGE = 'YES'`
	if _, err := execRCRestrictedSQLWithSession(ctx, sctx, clearNewestSQL, []any{mvLogID}); err != nil {
		return err
	}
	const insertHistSQL = `INSERT INTO mysql.tidb_mlog_purge_hist (MLOG_ID, MLOG_NAME, IS_NEWEST_PURGE, PURGE_METHOD, PURGE_TIME, PURGE_ENDTIME, PURGE_ROWS, PURGE_STATUS) VALUES (%?, %?, 'YES', %?, %?, %?, %?, %?)`
	_, err := execRCRestrictedSQLWithSession(ctx, sctx, insertHistSQL, []any{mvLogID, mlogName, purgeMethod, purgeTime, purgeEndTime, purgeRows, purgeStatus})
	return err
}

// RegisterMVS registers a DDL event handler for MV-related events.
// onDDLHandled is invoked after the local MV service is notified, and can be
// used by callers to fan out this event to other nodes.
func RegisterMVS(
	ctx context.Context,
	registerHandler func(notifier.HandlerID, notifier.SchemaChangeHandler),
	se basic.SessionPool,
	onDDLHandled func(),
) *MVService {
	if registerHandler == nil || se == nil || onDDLHandled == nil {
		return nil
	}

	mvs := NewMVJobsManager(ctx, se, &serverHelper{})
	mvs.NotifyDDLChange() // always trigger a refresh after startup to make sure the in-memory state is up-to-date

	// callback for DDL events only will be triggered on the DDL owner
	// other nodes will get notified through the NotifyDDLChange method from the domain service registry
	registerHandler(notifier.MVJobsHandlerID, func(_ context.Context, _ sessionctx.Context, event *notifier.SchemaChangeEvent) error {
		switch event.GetType() {
		//TODO: events related to materialized view metadata changes should also trigger refresh
		case meta.ActionCreateMaterializedViewLog:
			onDDLHandled()
		}
		return nil
	})

	return mvs
}
