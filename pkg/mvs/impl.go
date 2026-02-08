package utils

import (
	"context"
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
func (*serverHelper) PurgeMVLog(_ context.Context, _ basic.SessionPool, _ string) (nextPurge time.Time, err error) {
	return time.Time{}, ErrMVLogPurgeHandlerNotRegistered
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
	defer func() {
		sysSessionPool.Put(se)
	}()
	sctx := se.(sessionctx.Context)
	exec := sctx.GetRestrictedSQLExecutor()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	r, _, err := exec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		sql, params...,
	)
	return r, err
}

var mvs *MVService

// RegisterMVS registers a DDL event handler for MV-related events.
func RegisterMVS(ddlNotifier *notifier.DDLNotifier, se basic.SessionPool) {
	if ddlNotifier == nil {
		return
	}

	mvs = NewMVJobsManager(se, &serverHelper{})

	ddlNotifier.RegisterHandler(notifier.MVJobsHandlerID, func(_ context.Context, _ sessionctx.Context, event *notifier.SchemaChangeEvent) error {
		switch event.GetType() {
		case meta.ActionCreateMaterializedViewLog:
			mvs.ddlDirty.Store(true)
			mvs.notifier.Wake()
		}
		return nil
	})

	mvs.Start()
}
