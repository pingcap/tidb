// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mvservice

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	meta "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type serviceHelper struct {
	durationObserverCache durationObserverCache
	runEventCounterCache  runEventCounterCache

	reportCache struct {
		submittedCount int64
		finishedCount  int64
		failedCount    int64
		timeoutCount   int64
		rejectedCount  int64
	}
}

type mvMetricTypeResultKey struct {
	typ    string
	result string
}

type durationObserverCache struct {
	mu   sync.RWMutex
	data map[mvMetricTypeResultKey]prometheus.Observer
}

func newDurationObserverCache(capacity int) durationObserverCache {
	if capacity < 0 {
		capacity = 0
	}
	return durationObserverCache{
		data: make(map[mvMetricTypeResultKey]prometheus.Observer, capacity),
	}
}

type runEventCounterCache struct {
	mu   sync.RWMutex
	data map[string]prometheus.Counter
}

func newRunEventCounterCache(capacity int) runEventCounterCache {
	if capacity < 0 {
		capacity = 0
	}
	return runEventCounterCache{
		data: make(map[string]prometheus.Counter, capacity),
	}
}

// newServiceHelper builds a default helper used by MVService.
func newServiceHelper() *serviceHelper {
	return &serviceHelper{
		durationObserverCache: newDurationObserverCache(8),
		runEventCounterCache:  newRunEventCounterCache(16),
	}
}

func (*serviceHelper) serverFilter(s serverInfo) bool {
	return true
}

func (*serviceHelper) getServerInfo() (serverInfo, error) {
	localSrv, err := infosync.GetServerInfo()
	if err != nil {
		return serverInfo{}, err
	}
	return serverInfo{
		ID: localSrv.ID,
	}, nil
}

func (*serviceHelper) getAllServerInfo(ctx context.Context) (map[string]serverInfo, error) {
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

// RefreshMV executes one incremental refresh round for a materialized view.
//
// It:
// 1. Gets a system session from the pool.
// 2. Resolves schema/table names from MVIEW_ID.
// 3. Executes `REFRESH MATERIALIZED VIEW ... WITH SYNC MODE FAST`.
// 4. Reads NEXT_TIME from mysql.tidb_mview_refresh_info.
//
// The returned error only represents execution failures. A zero nextRefresh means
// no further scheduling is needed (for example, the MV metadata was removed).
func (*serviceHelper) RefreshMV(ctx context.Context, sysSessionPool basic.SessionPool, mvID int64) (nextRefresh time.Time, err error) {
	const (
		refreshMVSQL    = `REFRESH MATERIALIZED VIEW %n.%n WITH SYNC MODE FAST`
		findNextTimeSQL = `SELECT TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? AND NEXT_TIME IS NOT NULL`
	)
	startAt := mvsNow()
	var schemaName, mviewName string
	defer func() {
		if err == nil {
			return
		}
		logutil.BgLogger().Warn(
			"refresh materialized view failed",
			zap.Int64("mview_id", mvID),
			zap.String("schema", schemaName),
			zap.String("mview", mviewName),
			zap.Duration("elapsed", mvsSince(startAt)),
			zap.Error(err),
		)
	}()

	se, err := sysSessionPool.Get()
	if err != nil {
		return time.Time{}, err
	}
	defer sysSessionPool.Put(se)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMVMaintenance)

	sctx := se.(sessionctx.Context)
	infoSchema := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	mvTable, ok := infoSchema.TableByID(ctx, mvID)
	if !ok {
		return time.Time{}, nil
	}
	mvMeta := mvTable.Meta()
	if mvMeta == nil {
		return time.Time{}, errors.New("mview metadata is invalid")
	}
	mviewName = mvMeta.Name.L
	dbInfo, ok := infoSchema.SchemaByID(mvMeta.DBID)
	if !ok || dbInfo == nil {
		return time.Time{}, errors.New("mview metadata is invalid")
	}
	schemaName = dbInfo.Name.L
	if schemaName == "" || mviewName == "" {
		return time.Time{}, errors.New("mview metadata is invalid")
	}

	if _, err = execRCRestrictedSQL(ctx, sctx, refreshMVSQL, []any{schemaName, mviewName}); err != nil {
		return time.Time{}, err
	}

	rows, err := execRCRestrictedSQL(ctx, sctx, findNextTimeSQL, []any{mvID})
	if err != nil {
		return time.Time{}, err
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		return time.Time{}, nil
	}
	nextRefresh = mvsUnix(rows[0].GetInt64(0), 0)
	return nextRefresh, nil
}

// PurgeMVLog runs one auto-purge round for the specified MV log ID.
//
// Behavior overview:
// 1. Gets a system session from the pool.
// 2. Resolves schema/table names from mvLogID.
// 3. Executes `purge materialized view log on <schema>.<table>`.
// 4. Reads NEXT_TIME from mysql.tidb_mlog_purge_info.
func (*serviceHelper) PurgeMVLog(ctx context.Context, sysSessionPool basic.SessionPool, mvLogID int64) (nextPurge time.Time, err error) {
	const (
		purgeMVLogSQL   = `PURGE MATERIALIZED VIEW LOG ON %n.%n`
		findNextTimeSQL = `SELECT TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? AND NEXT_TIME IS NOT NULL`
	)
	var (
		baseSchema string
		baseTable  string
	)
	startAt := mvsNow()
	defer func() {
		if err == nil {
			return
		}
		logutil.BgLogger().Warn(
			"purge materialized view log failed",
			zap.Int64("mvlog_id", mvLogID),
			zap.String("base_table_schema", baseSchema),
			zap.String("base_table_name", baseTable),
			zap.Duration("elapsed", mvsSince(startAt)),
			zap.Error(err),
		)
	}()
	se, err := sysSessionPool.Get()
	if err != nil {
		logutil.BgLogger().Warn("get system session failed for mvlog purge", zap.Int64("mvlog_id", mvLogID), zap.Error(err))
		return time.Time{}, err
	}
	defer sysSessionPool.Put(se)
	sctx := se.(sessionctx.Context)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMVMaintenance)

	if mvLogID <= 0 {
		return time.Time{}, errors.New("materialized view log id is invalid")
	}
	infoSchema := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	mLogTbl, ok := infoSchema.TableByID(ctx, mvLogID)
	if !ok {
		return time.Time{}, nil
	}
	mLogMeta := mLogTbl.Meta()
	if mLogMeta == nil {
		return time.Time{}, errors.New("materialized view log metadata is invalid")
	}

	mLogInfo := mLogMeta.MaterializedViewLog
	if mLogInfo == nil || mLogInfo.BaseTableID <= 0 {
		return time.Time{}, errors.New("materialized view log metadata is invalid")
	}

	baseTbl, ok := infoSchema.TableByID(ctx, mLogInfo.BaseTableID)
	if !ok {
		return time.Time{}, errors.New("materialized view base table not found")
	}
	baseMeta := baseTbl.Meta()
	if baseMeta == nil {
		return time.Time{}, errors.New("materialized view base table metadata is invalid")
	}
	baseTable = baseMeta.Name.L
	if baseTable == "" {
		return time.Time{}, errors.New("materialized view base table name is empty")
	}
	if dbInfo, ok := infoSchema.SchemaByID(baseMeta.DBID); ok && dbInfo != nil && dbInfo.Name.L != "" {
		baseSchema = dbInfo.Name.L
	}
	if baseSchema == "" {
		return time.Time{}, errors.New("materialized view base table schema name is empty")
	}
	if _, err = execRCRestrictedSQL(ctx, sctx, purgeMVLogSQL, []any{baseSchema, baseTable}); err != nil {
		return time.Time{}, err
	}

	rows, err := execRCRestrictedSQL(ctx, sctx, findNextTimeSQL, []any{mvLogID})
	if err != nil {
		return time.Time{}, err
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		return time.Time{}, nil
	}
	nextPurge = mvsUnix(rows[0].GetInt64(0), 0)
	return nextPurge, nil
}

// GetCurrentTSO fetches current cluster TSO from TiDB.
func (*serviceHelper) GetCurrentTSO(_ context.Context, sysSessionPool basic.SessionPool) (uint64, error) {
	se, err := sysSessionPool.Get()
	if err != nil {
		return 0, err
	}
	defer sysSessionPool.Put(se)
	sctx := se.(sessionctx.Context)
	store := sctx.GetStore()
	if store == nil {
		return 0, errors.New("get current tso failed: store is nil")
	}
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return 0, err
	}
	if ver.Ver == 0 {
		return 0, errors.New("get current tso failed: invalid version")
	}
	return ver.Ver, nil
}

// PurgeMVHistoryBeforeTSO removes old records from MV history tables.
func (*serviceHelper) PurgeMVHistoryBeforeTSO(ctx context.Context, sysSessionPool basic.SessionPool, cutoffTSO uint64) error {
	const (
		deleteMVRefreshHistSQL  = `DELETE FROM mysql.tidb_mview_refresh_hist WHERE REFRESH_JOB_ID < %?`
		deleteMVLogPurgeHistSQL = `DELETE FROM mysql.tidb_mlog_purge_hist WHERE PURGE_JOB_ID < %?`
	)
	params := []any{cutoffTSO}
	if _, err := execRCRestrictedSQLWithSessionPool(ctx, sysSessionPool, deleteMVRefreshHistSQL, params); err != nil {
		return err
	}
	if _, err := execRCRestrictedSQLWithSessionPool(ctx, sysSessionPool, deleteMVLogPurgeHistSQL, params); err != nil {
		return err
	}
	return nil
}

// fetchAllTiDBMVLogPurge loads all scheduled MV log purge tasks from metadata.
func (*serviceHelper) fetchAllTiDBMVLogPurge(ctx context.Context, sysSessionPool basic.SessionPool) (map[int64]*mvLog, error) {
	const sql = `SELECT TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) as NEXT_TIME_SEC, MLOG_ID FROM mysql.tidb_mlog_purge_info WHERE NEXT_TIME IS NOT NULL`
	rows, err := execRCRestrictedSQLWithSessionPool(ctx, sysSessionPool, sql, nil)
	if err != nil {
		logutil.BgLogger().Warn("fetch mysql.tidb_mlog_purge_info failed", zap.Error(err))
		return nil, err
	}
	newPending := make(map[int64]*mvLog, len(rows))
	for _, row := range rows {
		if row.IsNull(0) || row.IsNull(1) {
			continue
		}
		mvLogID := row.GetInt64(1)
		if mvLogID <= 0 {
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

// fetchAllTiDBMVRefresh loads all scheduled MV refresh tasks from metadata.
func (*serviceHelper) fetchAllTiDBMVRefresh(ctx context.Context, sysSessionPool basic.SessionPool) (map[int64]*mv, error) {
	const sql = `SELECT TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) as NEXT_TIME_SEC, MVIEW_ID FROM mysql.tidb_mview_refresh_info WHERE NEXT_TIME IS NOT NULL`
	rows, err := execRCRestrictedSQLWithSessionPool(ctx, sysSessionPool, sql, nil)
	if err != nil {
		logutil.BgLogger().Warn("fetch mysql.tidb_mview_refresh_info failed", zap.Error(err))
		return nil, err
	}
	newPending := make(map[int64]*mv, len(rows))
	for _, row := range rows {
		if row.IsNull(0) || row.IsNull(1) {
			continue
		}
		mvID := row.GetInt64(1)
		if mvID <= 0 {
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

// execRCRestrictedSQLWithSessionPool executes restricted SQL with a borrowed session.
func execRCRestrictedSQLWithSessionPool(ctx context.Context, sysSessionPool basic.SessionPool, sql string, params []any) ([]chunk.Row, error) {
	se, err := sysSessionPool.Get()
	if err != nil {
		logutil.BgLogger().Warn(
			"get system session for restricted SQL failed",
			zap.String("sql", sql),
			zap.Int("param_count", len(params)),
			zap.Any("params", params),
			zap.Error(err),
		)
		return nil, err
	}
	defer sysSessionPool.Put(se)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMVMaintenance)
	return execRCRestrictedSQL(ctx, se.(sessionctx.Context), sql, params)
}

// execRCRestrictedSQL is a small wrapper over execRCRestrictedSQLWithSession.
func execRCRestrictedSQL(ctx context.Context, sctx sessionctx.Context, sql string, params []any) ([]chunk.Row, error) {
	return execRCRestrictedSQLWithSession(ctx, sctx, sql, params)
}

// execRCRestrictedSQLWithSession executes SQL through the restricted SQL executor.
func execRCRestrictedSQLWithSession(ctx context.Context, sctx sessionctx.Context, sql string, params []any) ([]chunk.Row, error) {
	r, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		sql, params...,
	)
	if err != nil {
		logutil.BgLogger().Warn(
			"execute restricted SQL failed",
			zap.String("sql", sql),
			zap.Int("param_count", len(params)),
			zap.Any("params", params),
			zap.NamedError("context_error", ctx.Err()),
			zap.Error(err),
		)
	}
	return r, err
}

// RegisterMVService registers a DDL event handler for MV-related events.
// onDDLHandled is invoked after the local MV service is notified, and can be
// used by callers to fan out this event to other nodes.
func RegisterMVService(
	ctx context.Context,
	registerHandler func(notifier.HandlerID, notifier.SchemaChangeHandler),
	se basic.SessionPool,
	onDDLHandled func(),
) *MVService {
	if registerHandler == nil || se == nil || onDDLHandled == nil {
		return nil
	}

	cfg := DefaultMVServiceConfig()
	mvService := NewMVService(ctx, se, newServiceHelper(), cfg)
	mvService.NotifyDDLChange() // always trigger a refresh after startup to make sure the in-memory state is up-to-date

	// callback for DDL events only will be triggered on the DDL owner
	// other nodes will get notified through the NotifyDDLChange method from the domain service registry
	registerHandler(notifier.MVServiceHandlerID, func(_ context.Context, _ sessionctx.Context, event *notifier.SchemaChangeEvent) error {
		if shouldHandleMVCreateEvent(event) {
			onDDLHandled()
		}
		return nil
	})

	return mvService
}

func shouldHandleMVCreateEvent(event *notifier.SchemaChangeEvent) bool {
	if event == nil {
		return false
	}

	switch event.GetType() {
	case meta.ActionCreateMaterializedViewLog, meta.ActionCreateMaterializedView:
		return true
	case meta.ActionCreateTable:
		tbl := event.GetCreateTableInfo()
		if tbl == nil {
			return false
		}
		return tbl.MaterializedView != nil || tbl.MaterializedViewLog != nil
	default:
		// For other DDL types, rely on periodic refresh.
		return false
	}
}
