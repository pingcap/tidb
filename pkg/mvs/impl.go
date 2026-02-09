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

	执行 sql：select RELATED_MV,PURGE_INTERVAL,MLOG_NAME from mysql.tidb_mlogs where MLOG_ID = `mvLogID`

令 find_min_mv_tso：找出所有 mvLogID 关联的 MV 中最小的 LAST_REFRESH_READ_TSO

	执行 sql：SELECT MIN(LAST_SUCCESSFUL_REFRESH_READ_TSO) as COMMIT_TSO FROM mysql.tidb_mview_refresh WHERE MVIEW_ID IN (find_mvs_by_mslog())

执行 sql： SELECT LAST_PURGE_TIME FROM mysql.tidb_mlog_purge WHERE MLOG_ID = `mvLogID` FOR UPDATE

如果 LAST_PURGE_TIME + PURGE_INTERVAL > now()：

	提交 txn
	RETURN LAST_PURGE_TIME

执行 sql：DELETE FROM `MLOG_NAME` WHERE COMMIT_TSO IN (0, find_min_mv_tso()];

TS = now()

UPDATE mysql.tidb_mlog_purge SET LAST_PURGE_TIME = TS, LAST_PURGE_ERR = NULL WHERE MLOG_ID = `mvLogID`;

提交 txn

return TS + PURGE_INTERVAL
*/
func (*serverHelper) PurgeMVLog(ctx context.Context, sysSessionPool basic.SessionPool, mvLogID string) (nextPurge time.Time, err error) {
	err = withRCRestrictedTxn(ctx, sysSessionPool, func(txnCtx context.Context, sctx sessionctx.Context) error {
		const findMLogSQL = `SELECT TABLE_SCHEMA, MLOG_NAME, RELATED_MV, PURGE_INTERVAL FROM mysql.tidb_mlogs WHERE MLOG_ID = %?`
		rows, err := execRCRestrictedSQLWithSession(txnCtx, sctx, findMLogSQL, []any{mvLogID})
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return errors.New("mvlog metadata not found")
		}
		row := rows[0]
		if row.IsNull(0) || row.IsNull(1) || row.IsNull(3) {
			return errors.New("mvlog metadata is invalid")
		}
		schemaName := row.GetString(0)
		mlogName := row.GetString(1)
		purgeInterval := time.Second * time.Duration(row.GetInt64(3))
		var relatedMVs []string
		if !row.IsNull(2) {
			relatedMVs = parseRelatedMVIDs(row.GetString(2))
		}

		const lockPurgeSQL = `SELECT LAST_PURGE_TIME FROM mysql.tidb_mlog_purge WHERE MLOG_ID = %? FOR UPDATE`
		rows, err = execRCRestrictedSQLWithSession(txnCtx, sctx, lockPurgeSQL, []any{mvLogID})
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return errors.New("mvlog purge state not found")
		}

		now := time.Now()
		if !rows[0].IsNull(0) {
			lastPurge, gtErr := rows[0].GetTime(0).GoTime(time.Local)
			if gtErr == nil {
				nextPurge = lastPurge.Add(purgeInterval)
				if nextPurge.After(now) {
					return nil
				}
			}
		}

		minRefreshReadTSO, err := getMinRefreshReadTSO(txnCtx, sctx, relatedMVs)
		if err != nil {
			return err
		}

		const deleteMLogSQL = `DELETE FROM %n.%n WHERE COMMIT_TSO > 0 AND COMMIT_TSO <= %?`
		deleteStart := time.Now()
		if _, err = execRCRestrictedSQLWithSession(txnCtx, sctx, deleteMLogSQL, []any{schemaName, mlogName, minRefreshReadTSO}); err != nil {
			return err
		}
		deleteDurationMS := time.Since(deleteStart).Milliseconds()
		affectedRows := sctx.GetSessionVars().StmtCtx.AffectedRows()

		purgeTS := time.Now()
		const updatePurgeSQL = `UPDATE mysql.tidb_mlog_purge SET LAST_PURGE_TIME = %?, LAST_PURGE_ROWS = %?, LAST_PURGE_DURATION = %? WHERE MLOG_ID = %?`
		if _, err = execRCRestrictedSQLWithSession(txnCtx, sctx, updatePurgeSQL, []any{purgeTS, affectedRows, deleteDurationMS, mvLogID}); err != nil {
			return err
		}
		nextPurge = purgeTS.Add(purgeInterval)
		return nil
	})
	return nextPurge, err
}

func (*serverHelper) fetchAllTiDBMLogPurge(ctx context.Context, sysSessionPool basic.SessionPool) (map[string]*mvLog, error) {
	const sql = `SELECT MLOG_ID, PURGE_INTERVAL, LAST_PURGE_TIME FROM mysql.tidb_mlog_purge t join mysql.tidb_mlogs l on t.MLOG_ID = l.MLOG_ID`
	rows, err := execRCRestrictedSQL(ctx, sysSessionPool, sql, nil)
	if err != nil {
		return nil, err
	}
	newPending := make(map[string]*mvLog, len(rows))
	for _, row := range rows {
		l := &mvLog{
			ID: row.GetString(0),
		}
		if l.ID == "" {
			continue
		}
		l.purgeInterval = time.Second * time.Duration(row.GetInt64(1))
		if !row.IsNull(2) {
			gt, gtErr := row.GetTime(2).GoTime(time.Local)
			if gtErr != nil {
				l.nextPurge = time.Now()
			} else {
				l.nextPurge = gt.Add(l.purgeInterval)
			}
			l.orderTs = l.nextPurge.UnixMilli()
		}
		newPending[l.ID] = l
	}
	return newPending, nil
}

func (*serverHelper) fetchAllTiDBMViews(ctx context.Context, sysSessionPool basic.SessionPool) (map[string]*mv, error) {
	const sql = `SELECT MVIEW_ID, REFRESH_INTERVAL, LAST_REFRESH_TIME FROM mysql.tidb_mview_refresh t JOIN mysql.tidb_mviews v ON t.MVIEW_ID = v.MVIEW_ID`
	rows, err := execRCRestrictedSQL(ctx, sysSessionPool, sql, nil)
	if err != nil {
		return nil, err
	}
	newPending := make(map[string]*mv, len(rows))
	for _, row := range rows {
		m := &mv{
			ID: row.GetString(0),
		}
		if m.ID == "" {
			continue
		}
		m.refreshInterval = time.Duration(row.GetInt64(1)) * time.Second
		if !row.IsNull(2) {
			gt, gtErr := row.GetTime(2).GoTime(time.Local)
			if gtErr != nil {
				m.nextRefresh = time.Now()
			} else {
				m.nextRefresh = gt.Add(m.refreshInterval)
			}
			m.orderTs = m.nextRefresh.UnixMilli()
		}
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
	exec := sctx.GetRestrictedSQLExecutor()
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	r, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
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

// RegisterMVS registers a DDL event handler for MV-related events.
// onDDLHandled is invoked after the local MV service is notified, and can be
// used by callers to fan out this event to other nodes.
func RegisterMVS(ctx context.Context, ddlNotifier *notifier.DDLNotifier, se basic.SessionPool, onDDLHandled func()) *MVService {
	if ddlNotifier == nil || se == nil || onDDLHandled == nil {
		return nil
	}

	mvs := NewMVJobsManager(ctx, se, &serverHelper{})

	ddlNotifier.RegisterHandler(notifier.MVJobsHandlerID, func(_ context.Context, _ sessionctx.Context, event *notifier.SchemaChangeEvent) error {
		switch event.GetType() {
		case meta.ActionCreateMaterializedViewLog:
			onDDLHandled()
		}
		return nil
	})

	return mvs
}
