package mvs

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	meta "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

/*
CREATE TABLE IF NOT EXISTS mysql.tidb_mview_refresh_info (
  `MVIEW_ID` bigint(21) NOT NULL,
  `MVIEW_NAME` varchar(64) NOT NULL,
  `REFRESH_JOB_ID` varchar(64) NOT NULL,
  `REFRESH_METHOD` varchar(32) NOT NULL,

  `REFRESH_TIME` datetime DEFAULT NULL,
  `REFRESH_ENDTIME` datetime DEFAULT NULL,
  `REFRESH_STATUS` varchar(16) DEFAULT NULL,

  `LAST_SUCCESS_READ_TSO` bigint(21) DEFAULT NULL,
  `LAST_SUCCESS_ENDTIME` datetime DEFAULT NULL,
  `LAST_FAILED_REASON` text DEFAULT NULL,

  `NEXT_TIME` datetime DEFAULT NULL,

  PRIMARY KEY (`MVIEW_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
*/

/*
CREATE TABLE IF NOT EXISTS mysql.tidb_mlog_purge_info (
  `MLOG_ID` bigint(21) NOT NULL,
  `MLOG_NAME` varchar(64) NOT NULL,
  `PURGE_JOB_ID` varchar(64) NOT NULL,
  `PURGE_METHOD` varchar(32) NOT NULL,

  `PURGE_TIME` datetime DEFAULT NULL,
  `PURGE_ENDTIME` datetime DEFAULT NULL,

  `PURGE_ROWS` bigint NOT NULL,
  `PURGE_STATUS` varchar(16) DEFAULT NULL,

  `NEXT_TIME` datetime DEFAULT NULL,

  PRIMARY KEY (`MLOG_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
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
实现 mv 增量刷新逻辑，伪代码如下：

执行 sql：SELECT TABLE_SCHEMA, MVIEW_NAME from mysql.tidb_mviews where MVIEW_ID = `mvID`

	无结果返回，说明 mv 已经被删除，返回 deleted = true

执行 sql：REFRESH MATERIALIZED VIEW TABLE_SCHEMA.MVIEW_NAME WITH SYNC MODE FAST

	该 sql 执行 select * from mysql.tidb_mview_refresh_info where MVIEW_ID = `mvID` for update
	该 sql 执行 update mysql.tidb_mview_refresh_hist

执行 sql：SELECT UNIX_TIMESTAMP(NEXT_TIME) as NEXT_TIME_SEC FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = `mvID` AND NEXT_TIME iS NOT NULL

	无结果返回，说明 mv 已经被删除，返回 deleted = true

返回 NEXT_TIME_SEC
*/
func (*serverHelper) RefreshMV(ctx context.Context, sysSessionPool basic.SessionPool, mvID string) (nextRefresh time.Time, deleted bool, err error) {
	const (
		findMVSQL       = `SELECT TABLE_SCHEMA, MVIEW_NAME FROM mysql.tidb_mviews WHERE MVIEW_ID = %?`
		refreshMVSQL    = `REFRESH MATERIALIZED VIEW %n.%n WITH SYNC MODE FAST`
		findNextTimeSQL = `SELECT UNIX_TIMESTAMP(NEXT_TIME) FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? AND NEXT_TIME IS NOT NULL`
	)

	rows, err := execRCRestrictedSQLWithSessionPool(ctx, sysSessionPool, findMVSQL, []any{mvID})
	if err != nil {
		return time.Time{}, false, err
	}
	if len(rows) == 0 {
		return time.Time{}, true, nil
	}

	row := rows[0]
	if row.IsNull(0) || row.IsNull(1) {
		return time.Time{}, false, errors.New("mview metadata is invalid")
	}
	schemaName := row.GetString(0)
	mviewName := row.GetString(1)
	if schemaName == "" || mviewName == "" {
		return time.Time{}, false, errors.New("mview metadata is invalid")
	}

	if _, err = execRCRestrictedSQLWithSessionPool(ctx, sysSessionPool, refreshMVSQL, []any{schemaName, mviewName}); err != nil {
		return time.Time{}, false, err
	}

	rows, err = execRCRestrictedSQLWithSessionPool(ctx, sysSessionPool, findNextTimeSQL, []any{mvID})
	if err != nil {
		return time.Time{}, false, err
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		return time.Time{}, true, nil
	}

	nextRefresh = mvsUnix(rows[0].GetInt64(0), 0)
	return nextRefresh, false, nil
}

func PurgeMVLog(ctx context.Context, sctx sessionctx.Context, TABLE_SCHEMA, TABLE_NAME string) error {
	is := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	tbl, err := is.TableByName(ctx, model.NewCIStr(TABLE_SCHEMA), model.NewCIStr(TABLE_NAME))
	if err != nil {
		return err
	}
	baseTableMeta := tbl.Meta()
	mvInfo := baseTableMeta.MaterializedViewBase
	if mvInfo == nil {
		return errors.New("table is not a materialized view log")
	}
	mvlo_tbl, ok := is.TableByID(ctx, mvInfo.MLogID)
	if !ok {
		return errors.New("materialized view log table not found")
	}
	mvlogMeta := mvlo_tbl.Meta()

	return purgeMVLogImpl(ctx, sctx, mvInfo, mvlogMeta.Name.L)
}

/*
执行 sql：SELECT MIN(LAST_SUCCESS_READ_TSO) as MIN_COMMIT_TSO FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN (`mvInfo.MViewIDs`)

开始 txn
执行 sql： SELECT 1 FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = `mvInfo.MLogID` FOR UPDATE
执行 sql： DELETE FROM `mvlogName` WHERE COMMIT_TSO IN (0, `MIN_COMMIT_TSO`];

last_purge_time = now()

UPDATE mysql.tidb_mlog_purge SET LAST_PURGE_TIME = `last_purge_time`, LAST_PURGE_ERR = NULL WHERE MLOG_ID = `MLogID`;

执行结果记录到 mysql.tidb_mlog_purge_hist（表结构定义在 pkg/session/bootstrap.go）

提交 txn
*/
func purgeMVLogImpl(ctx context.Context, sctx sessionctx.Context, mvInfo *meta.MaterializedViewBaseInfo, mvlogName string) error {
	if mvInfo == nil {
		return errors.New("materialized view base info is nil")
	}
	if mvInfo.MLogID <= 0 {
		return errors.New("materialized view log id is invalid")
	}
	if mvlogName == "" {
		return errors.New("materialized view log table name is empty")
	}

	minRefreshReadTSO, err := getMinRefreshReadTSOFromInfo(ctx, sctx, mvInfo.MViewIDs)
	if err != nil {
		return err
	}

	return withRCRestrictedTxn(ctx, sctx, func(txnCtx context.Context, sctx sessionctx.Context) error {
		const lockPurgeSQL = `SELECT 1 FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE`
		rows, err := execRCRestrictedSQLWithSession(txnCtx, sctx, lockPurgeSQL, []any{mvInfo.MLogID})
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return errors.New("mvlog purge state not found")
		}

		const deleteMLogSQL = `DELETE FROM %n WHERE COMMIT_TSO = 0 OR (COMMIT_TSO > 0 AND COMMIT_TSO <= %?)`
		purgeTime := mvsNow()
		deleteStart := purgeTime
		if _, err = execRCRestrictedSQLWithSession(txnCtx, sctx, deleteMLogSQL, []any{mvlogName, minRefreshReadTSO}); err != nil {
			return err
		}
		deleteDurationMS := mvsSince(deleteStart).Milliseconds()
		affectedRows := sctx.GetSessionVars().StmtCtx.AffectedRows()
		purgeEndTime := mvsNow()

		const updatePurgeSQL = `UPDATE mysql.tidb_mlog_purge SET LAST_PURGE_TIME = %?, LAST_PURGE_ROWS = %?, LAST_PURGE_DURATION = %? WHERE MLOG_ID = %?`
		if _, err = execRCRestrictedSQLWithSession(txnCtx, sctx, updatePurgeSQL, []any{purgeEndTime, affectedRows, deleteDurationMS, mvInfo.MLogID}); err != nil {
			return err
		}

		mvLogID := strconv.FormatInt(mvInfo.MLogID, 10)
		if err = recordMLogPurgeHist(txnCtx, sctx, mvLogID, mvlogName, "MANUAL", purgeTime, purgeEndTime, affectedRows, "SUCCESS"); err != nil {
			return err
		}

		return nil
	})

}

/*
实现 purge 逻辑，伪代码如下：

开始 txn

令 find_mvs_by_mslog：找出所有 mvLogID 关联的 MV

	执行 sql：select RELATED_MV, UNIX_TIMESTAMP(PURGE_START) as PURGE_START_SEC, PURGE_INTERVAL, MLOG_NAME from mysql.tidb_mlogs where MLOG_ID = `mvLogID`

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
func (*serverHelper) PurgeMVLog(ctx context.Context, sysSessionPool basic.SessionPool, mvLogID string) (nextPurge time.Time, deleted bool, err error) {
	purgeTime := mvsNow()
	purgeEndTime := purgeTime
	mlogName := mvLogID
	purgeMethod := "UNKNOWN"
	var failedErr error

	err = withRCRestrictedTxnWithSessionPool(ctx, sysSessionPool, func(txnCtx context.Context, sctx sessionctx.Context) error {
		recordFailedPurge := func(stepErr error, purgeRows uint64) error {
			failedErr = stepErr
			purgeEndTime = mvsNow()
			return recordMLogPurgeHist(txnCtx, sctx, mvLogID, mlogName, purgeMethod, purgeTime, purgeEndTime, purgeRows, "FAILED")
		}

		const findMLogSQL = `SELECT TABLE_SCHEMA, MLOG_NAME, RELATED_MV, UNIX_TIMESTAMP(PURGE_START), PURGE_INTERVAL, PURGE_METHOD FROM mysql.tidb_mlogs WHERE MLOG_ID = %?`
		rows, runErr := execRCRestrictedSQLWithSession(txnCtx, sctx, findMLogSQL, []any{mvLogID})
		if runErr != nil {
			return recordFailedPurge(runErr, 0)
		}
		if len(rows) == 0 {
			deleted = true
			return nil
		}
		row := rows[0]
		if row.IsNull(0) || row.IsNull(1) || row.IsNull(3) || row.IsNull(4) {
			return recordFailedPurge(errors.New("mvlog metadata is invalid"), 0)
		}

		schemaName := row.GetString(0)
		mlogName = row.GetString(1)
		purgeStartSec := row.GetInt64(3)
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
		deleteStart := mvsNow()
		if _, runErr = execRCRestrictedSQLWithSession(txnCtx, sctx, deleteMLogSQL, []any{schemaName, mlogName, minRefreshReadTSO}); runErr != nil {
			return recordFailedPurge(runErr, 0)
		}
		deleteDurationMS := mvsSince(deleteStart).Milliseconds()
		affectedRows := sctx.GetSessionVars().StmtCtx.AffectedRows()

		purgeEndTime = mvsNow()
		const updatePurgeSQL = `UPDATE mysql.tidb_mlog_purge SET LAST_PURGE_TIME = %?, LAST_PURGE_ROWS = %?, LAST_PURGE_DURATION = %? WHERE MLOG_ID = %?`
		if _, runErr = execRCRestrictedSQLWithSession(txnCtx, sctx, updatePurgeSQL, []any{purgeEndTime, affectedRows, deleteDurationMS, mvLogID}); runErr != nil {
			return recordFailedPurge(runErr, affectedRows)
		}
		if runErr = recordMLogPurgeHist(txnCtx, sctx, mvLogID, mlogName, purgeMethod, purgeTime, purgeEndTime, affectedRows, "SUCCESS"); runErr != nil {
			return runErr
		}
		nextPurge = calcNextExecTime(mvsUnix(purgeStartSec, 0), purgeIntervalSec, purgeEndTime)
		return nil
	})
	if err != nil {
		return time.Time{}, false, err
	}
	return nextPurge, deleted, failedErr
}

func calcNextExecTime(start time.Time, intervalSec int64, last time.Time) time.Time {
	if intervalSec <= 0 {
		return last
	}
	startSec := start.Unix()
	lastSec := last.Unix()
	if lastSec < startSec {
		return mvsUnix(startSec, 0)
	}
	elapsed := lastSec - startSec
	intervals := elapsed / intervalSec
	nextSec := startSec + (intervals+1)*intervalSec
	return mvsUnix(nextSec, 0)
}

/*
执行 sql： SELECT UNIX_TIMESTAMP(NEXT_TIME) as NEXT_TIME_SEC, MLOG_ID FROM mysql.tidb_mlog_purge_info WHERE NEXT_TIME iS NOT NULL

	如果 NEXT_TIME 为空，说明该 MV 已经被删除，忽略该行数据

对于每行数据：

	计算 nextRefresh 为 NEXT_TIME
*/
func (*serverHelper) fetchAllTiDBMLogPurge(ctx context.Context, sysSessionPool basic.SessionPool) (map[string]*mvLog, error) {
	const sql = `SELECT UNIX_TIMESTAMP(NEXT_TIME) as NEXT_TIME_SEC, MLOG_ID FROM mysql.tidb_mlog_purge_info WHERE NEXT_TIME IS NOT NULL`
	rows, err := execRCRestrictedSQLWithSessionPool(ctx, sysSessionPool, sql, nil)
	if err != nil {
		return nil, err
	}
	newPending := make(map[string]*mvLog, len(rows))
	for _, row := range rows {
		if row.IsNull(0) {
			continue
		}
		mvLogID := row.GetString(1)
		if mvLogID == "" {
			continue
		}
		nextPurge := mvsUnix(row.GetInt64(0), 0)
		l := &mvLog{
			ID:        mvLogID,
			nextPurge: nextPurge,
		}
		l.orderTs = l.nextPurge.UnixMilli()
		newPending[mvLogID] = l
	}
	return newPending, nil
}

/*
执行 sql： SELECT UNIX_TIMESTAMP(NEXT_TIME) as NEXT_TIME_SEC, MVIEW_ID FROM mysql.tidb_mview_refresh_info WHERE NEXT_TIME iS NOT NULL

	如果 NEXT_TIME 为空，说明该 MV 已经被删除，忽略该行数据

对于每行数据：

	计算 nextRefresh 为 NEXT_TIME
*/
func (*serverHelper) fetchAllTiDBMViews(ctx context.Context, sysSessionPool basic.SessionPool) (map[string]*mv, error) {
	const sql = `SELECT UNIX_TIMESTAMP(NEXT_TIME) as NEXT_TIME_SEC, MVIEW_ID FROM mysql.tidb_mview_refresh_info WHERE NEXT_TIME IS NOT NULL`
	rows, err := execRCRestrictedSQLWithSessionPool(ctx, sysSessionPool, sql, nil)
	if err != nil {
		return nil, err
	}
	newPending := make(map[string]*mv, len(rows))
	for _, row := range rows {
		if row.IsNull(0) {
			continue
		}
		mvID := row.GetString(1)
		if mvID == "" {
			continue
		}
		nextRefresh := mvsUnix(row.GetInt64(0), 0)
		m := &mv{
			ID:          mvID,
			nextRefresh: nextRefresh,
		}
		m.orderTs = m.nextRefresh.UnixMilli()
		newPending[mvID] = m
	}
	return newPending, nil
}

func execRCRestrictedSQLWithSessionPool(ctx context.Context, sysSessionPool basic.SessionPool, sql string, params []any) ([]chunk.Row, error) {
	se, err := sysSessionPool.Get()
	if err != nil {
		return nil, err
	}
	defer sysSessionPool.Put(se)
	return execRCRestrictedSQL(ctx, se.(sessionctx.Context), sql, params)
}

func execRCRestrictedSQL(ctx context.Context, sctx sessionctx.Context, sql string, params []any) ([]chunk.Row, error) {
	return execRCRestrictedSQLWithSession(ctx, sctx, sql, params)
}

func execRCRestrictedSQLWithSession(ctx context.Context, sctx sessionctx.Context, sql string, params []any) ([]chunk.Row, error) {
	r, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		sql, params...,
	)
	return r, err
}

func withRCRestrictedTxnWithSessionPool(ctx context.Context, sysSessionPool basic.SessionPool, fn func(txnCtx context.Context, sctx sessionctx.Context) error) error {
	se, err := sysSessionPool.Get()
	if err != nil {
		return err
	}
	defer sysSessionPool.Put(se)
	return withRCRestrictedTxn(ctx, se.(sessionctx.Context), fn)
}

func withRCRestrictedTxn(ctx context.Context, sctx sessionctx.Context, fn func(txnCtx context.Context, sctx sessionctx.Context) error) (err error) {
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

func getMinRefreshReadTSOFromInfo(ctx context.Context, sctx sessionctx.Context, mvIDs []int64) (int64, error) {
	if len(mvIDs) == 0 {
		return 0, nil
	}
	var b strings.Builder
	params := make([]any, 0, len(mvIDs))
	b.WriteString(`SELECT MIN(LAST_SUCCESS_READ_TSO) AS MIN_COMMIT_TSO FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN (`)
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
	mvs.SetTaskBackpressureController(NewCPUMemBackpressureController(defaultMVTaskBackpressureCPUThreshold, defaultMVTaskBackpressureMemThreshold, defaultTaskBackpressureDelay))

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
