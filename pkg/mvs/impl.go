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
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

/* mysql.tidb_mview_refresh_info 表结构定义如下：
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

/* mysql.tidb_mlog_purge_info 表结构定义如下：
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

/*
执行 mvlog purge 逻辑，函数返回 2 个值：error 仅表示 purge 过程中发生了错误；nextPurge 表示下一次 purge 的时间点，如果为零值，表示不需要再次 purge

伪代码如下：

如果 autoPurge 为 false，表示当前 purge 是由用户手动触发的；如果 autoPurge 为 true，表示当前 purge 是自动触发的
通过 baseTable 找到对应的 mvlog 信息（详见 MaterializedViewLogInfo struct ），包括 MLogID，关联的 MV 列表，PurgeStartWith，PurgeNext, PurgeMethod

PurgeMethod 目前有两种取值：
- MANUALLY
- AUTOMATICALLY

执行 sql：SELECT MIN(LAST_SUCCESS_READ_TSO) as MIN_COMMIT_TSO FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID IN (关联的 MV 列表)

开始 txn
执行 sql：SELECT NEXT_TIME FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = `MLogID` FOR UPDATE

如果 sql 无结果，说明该 MV LOG 已经被删除。如果手动触发 purge，直接返回错误；如果自动触发 purge，直接返回零值的 nextPurge，表示不需要再次 purge

如果 NEXT_TIME 为 null，说明该 MV LOG 仅支持手动 purge。如果手动触发 purge，继续执行；如果自动触发 purge，直接返回零值的 nextPurge，表示不需要再次 purge

如果 autoPurge 为 true，且 NEXT_TIME 为 null，或 NEXT_TIME > now()，则跳过本次 purge，直接返回 NEXT_TIME

执行 sql：DELETE FROM `mvlogSchemaName`.`mvlogName` WHERE COMMIT_TSO IN (0, `MIN_COMMIT_TSO`]

计算新的 NEXT_TIME：从 purgeStartWith 开始，每隔 purgeNext 秒执行一次，直到下一个 NEXT_TIME > now()

UPDATE mysql.tidb_mlog_purge_info 设置相关字段

执行结果记录到 mysql.tidb_mlog_purge_hist（表结构定义在 pkg/session/bootstrap.go）

提交 txn
*/
func PurgeMVLog(ctx context.Context, sctx sessionctx.Context, baseTable table.Table, autoPurge bool) (nextPurge time.Time, err error) {
	const (
		purgeMethodManually      = "MANUALLY"
		purgeMethodAutomatically = "AUTOMATICALLY"
	)
	if baseTable == nil {
		return time.Time{}, errors.New("base table is nil")
	}
	purgeMethod := purgeMethodManually
	if autoPurge {
		purgeMethod = purgeMethodAutomatically
	}
	infoSchema := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	baseTableMeta := baseTable.Meta()
	if baseTableMeta == nil {
		return time.Time{}, errors.New("base table metadata is nil")
	}
	mvBaseInfo := baseTableMeta.MaterializedViewBase
	if mvBaseInfo == nil {
		return time.Time{}, errors.New("table is not a materialized view log")
	}
	mvLogTable, ok := infoSchema.TableByID(ctx, mvBaseInfo.MLogID)
	if !ok {
		return time.Time{}, errors.New("materialized view log table not found")
	}
	mvLogMeta := mvLogTable.Meta()
	if mvBaseInfo.MLogID <= 0 {
		return time.Time{}, errors.New("materialized view log id is invalid")
	}
	mvLogName := mvLogMeta.Name.L
	if mvLogName == "" {
		return time.Time{}, errors.New("materialized view log table name is empty")
	}
	mvLogSchemaName := ""
	if dbInfo, ok := infoSchema.SchemaByID(baseTableMeta.DBID); ok && dbInfo != nil && dbInfo.Name.L != "" {
		mvLogSchemaName = dbInfo.Name.L
	}
	if dbInfo, ok := infoSchema.SchemaByID(mvLogMeta.DBID); ok && dbInfo != nil && dbInfo.Name.L != "" {
		mvLogSchemaName = dbInfo.Name.L
	}
	if mvLogSchemaName == "" {
		return time.Time{}, errors.New("materialized view log schema name is empty")
	}
	mvLogInfo := mvLogMeta.MaterializedViewLog
	if mvLogInfo == nil {
		return time.Time{}, errors.New("materialized view log metadata is invalid")
	}
	purgeStartWithText := strings.TrimSpace(mvLogInfo.PurgeStartWith)
	if purgeStartWithText == "" {
		return time.Time{}, errors.New("materialized view log purge start with is empty")
	}
	purgeStartWithText = strings.Trim(purgeStartWithText, "'")
	loc := time.Local
	if vars := sctx.GetSessionVars(); vars != nil && vars.Location() != nil {
		loc = vars.Location()
	}
	purgeStartWith, err := time.ParseInLocation("2006-01-02 15:04:05", purgeStartWithText, loc)
	if err != nil {
		return time.Time{}, err
	}
	purgeNextText := strings.TrimSpace(mvLogInfo.PurgeNext)
	if purgeNextText == "" {
		return time.Time{}, errors.New("materialized view log purge next is empty")
	}
	purgeNextSec, err := strconv.ParseInt(purgeNextText, 10, 64)
	if err != nil || purgeNextSec <= 0 {
		return time.Time{}, errors.New("materialized view log purge next is invalid")
	}
	minRefreshReadTSO, err := getMinRefreshReadTSOFromInfo(ctx, sctx, mvBaseInfo.MViewIDs)
	if err != nil {
		return time.Time{}, err
	}

	err = withRCRestrictedTxn(ctx, sctx, func(txnCtx context.Context, sctx sessionctx.Context) error {
		const lockPurgeSQL = `SELECT UNIX_TIMESTAMP(NEXT_TIME) FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE`
		rows, err := execRCRestrictedSQLWithSession(txnCtx, sctx, lockPurgeSQL, []any{mvBaseInfo.MLogID})
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			if autoPurge {
				nextPurge = time.Time{}
				return nil
			}
			return errors.New("mvlog purge state not found")
		}
		if rows[0].IsNull(0) {
			if autoPurge {
				nextPurge = time.Time{}
				return nil
			}
		} else {
			nextPurge = mvsUnix(rows[0].GetInt64(0), 0)
		}
		if autoPurge {
			if nextPurge.After(mvsNow()) {
				return nil
			}
		}

		const deleteMLogSQL = `DELETE FROM %n.%n WHERE COMMIT_TSO = 0 OR (COMMIT_TSO > 0 AND COMMIT_TSO <= %?)`
		purgeTime := mvsNow()
		if _, err = execRCRestrictedSQLWithSession(txnCtx, sctx, deleteMLogSQL, []any{mvLogSchemaName, mvLogName, minRefreshReadTSO}); err != nil {
			return err
		}
		affectedRows := sctx.GetSessionVars().StmtCtx.AffectedRows()
		purgeEndTime := mvsNow()
		nextPurge = calcNextExecTime(purgeStartWith, purgeNextSec, purgeEndTime)

		const updatePurgeSQL = `UPDATE mysql.tidb_mlog_purge_info SET PURGE_TIME = %?, PURGE_ENDTIME = %?, PURGE_ROWS = %?, PURGE_STATUS = 'SUCCESS', NEXT_TIME = %? WHERE MLOG_ID = %?`
		if _, err = execRCRestrictedSQLWithSession(txnCtx, sctx, updatePurgeSQL, []any{purgeTime, purgeEndTime, affectedRows, nextPurge, mvBaseInfo.MLogID}); err != nil {
			return err
		}

		mvLogID := strconv.FormatInt(mvBaseInfo.MLogID, 10)
		if err = recordMLogPurgeHist(txnCtx, sctx, mvLogID, mvLogName, purgeMethod, purgeTime, purgeEndTime, affectedRows, "SUCCESS"); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return time.Time{}, err
	}
	return nextPurge, nil
}

/*
令 MLOG_ID = int64(mvLogID)
获取 session 后通过 InfoSchema interface 的 TableByID 方法获取 mvlog 的表信息
调用 func PurgeMVLog 实现具体 purge 逻辑
*/
func (*serverHelper) PurgeMVLog(ctx context.Context, sysSessionPool basic.SessionPool, mvLogID string) (nextPurge time.Time, err error) {
	mvLogTableID, err := strconv.ParseInt(mvLogID, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	purgeTime := mvsNow()
	purgeEndTime := purgeTime
	mlogName := mvLogID
	purgeMethod := "AUTOMATICALLY"
	se, err := sysSessionPool.Get()
	if err != nil {
		return time.Time{}, err
	}
	defer sysSessionPool.Put(se)
	sctx := se.(sessionctx.Context)
	infoSchema := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)

	mvLogTable, ok := infoSchema.TableByID(ctx, mvLogTableID)
	if !ok {
		return time.Time{}, nil
	}
	mvLogMeta := mvLogTable.Meta()
	if mvLogMeta == nil {
		return time.Time{}, errors.New("mvlog metadata is invalid")
	}
	if mvLogMeta.Name.L != "" {
		mlogName = mvLogMeta.Name.L
	}
	mvLogInfo := mvLogMeta.MaterializedViewLog
	if mvLogInfo == nil {
		return time.Time{}, errors.New("mvlog metadata is invalid")
	}
	if mvLogInfo.BaseTableID <= 0 {
		return time.Time{}, errors.New("mvlog metadata is invalid")
	}
	baseTable, ok := infoSchema.TableByID(ctx, mvLogInfo.BaseTableID)
	if !ok {
		return time.Time{}, errors.New("materialized view base table not found")
	}

	nextPurge, err = PurgeMVLog(ctx, sctx, baseTable, true)
	if err != nil {
		purgeEndTime = mvsNow()
		histErr := withRCRestrictedTxn(ctx, sctx, func(txnCtx context.Context, sctx sessionctx.Context) error {
			return recordMLogPurgeHist(txnCtx, sctx, mvLogID, mlogName, purgeMethod, purgeTime, purgeEndTime, 0, "FAILED")
		})
		if histErr != nil {
			return time.Time{}, histErr
		}
		return time.Time{}, err
	}
	return nextPurge, nil
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
