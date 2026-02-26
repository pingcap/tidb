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

package mvs

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

type serverHelper struct {
	durationObserverCache durationObserverCache
	runEventCounterCache  runEventCounterCache

	reportCache struct {
		submittedCount int64
		completedCount int64
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

// newServerHelper builds a default helper used by MVService.
func newServerHelper() *serverHelper {
	return &serverHelper{
		durationObserverCache: newDurationObserverCache(8),
		runEventCounterCache:  newRunEventCounterCache(16),
	}
}

func (*serverHelper) serverFilter(s serverInfo) bool {
	return true
}

func (*serverHelper) getServerInfo() (serverInfo, error) {
	localSrv, err := infosync.GetServerInfo()
	if err != nil {
		return serverInfo{}, err
	}
	return serverInfo{
		ID: localSrv.ID,
	}, nil
}

func (*serverHelper) getAllServerInfo(ctx context.Context) (map[string]serverInfo, error) {
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
func (*serverHelper) RefreshMV(ctx context.Context, sysSessionPool basic.SessionPool, mvID int64) (nextRefresh time.Time, err error) {
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
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)

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
func (*serverHelper) PurgeMVLog(ctx context.Context, sysSessionPool basic.SessionPool, mvLogID int64) (nextPurge time.Time, err error) {
	const (
		purgeMVLogSQL   = `PURGE MATERIALIZED VIEW LOG ON %n.%n`
		findNextTimeSQL = `SELECT TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? AND NEXT_TIME IS NOT NULL`
	)
	var (
		baseSchemaName string
		baseTableName  string
	)
	startAt := mvsNow()
	defer func() {
		if err == nil {
			return
		}
		logutil.BgLogger().Warn(
			"purge materialized view log failed",
			zap.Int64("mvlog_id", mvLogID),
			zap.String("base_table_schema", baseSchemaName),
			zap.String("base_table_name", baseTableName),
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
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)

	if mvLogID <= 0 {
		return time.Time{}, errors.New("materialized view log id is invalid")
	}
	infoSchema := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	mvLogTable, ok := infoSchema.TableByID(ctx, mvLogID)
	if !ok {
		return time.Time{}, nil
	}
	mvLogMeta := mvLogTable.Meta()
	if mvLogMeta == nil {
		return time.Time{}, errors.New("materialized view log metadata is invalid")
	}

	mvLogInfo := mvLogMeta.MaterializedViewLog
	if mvLogInfo == nil || mvLogInfo.BaseTableID <= 0 {
		return time.Time{}, errors.New("materialized view log metadata is invalid")
	}

	baseTable, ok := infoSchema.TableByID(ctx, mvLogInfo.BaseTableID)
	if !ok {
		return time.Time{}, errors.New("materialized view base table not found")
	}
	baseTableMeta := baseTable.Meta()
	if baseTableMeta == nil {
		return time.Time{}, errors.New("materialized view base table metadata is invalid")
	}
	baseTableName = baseTableMeta.Name.L
	if baseTableName == "" {
		return time.Time{}, errors.New("materialized view base table name is empty")
	}
	if dbInfo, ok := infoSchema.SchemaByID(baseTableMeta.DBID); ok && dbInfo != nil && dbInfo.Name.L != "" {
		baseSchemaName = dbInfo.Name.L
	}
	if baseSchemaName == "" {
		return time.Time{}, errors.New("materialized view base table schema name is empty")
	}
	if _, err = execRCRestrictedSQL(ctx, sctx, purgeMVLogSQL, []any{baseSchemaName, baseTableName}); err != nil {
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

// fetchAllTiDBMVLogPurge loads all scheduled MV log purge tasks from metadata.
func (*serverHelper) fetchAllTiDBMVLogPurge(ctx context.Context, sysSessionPool basic.SessionPool) (map[int64]*mvLog, error) {
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
func (*serverHelper) fetchAllTiDBMVRefresh(ctx context.Context, sysSessionPool basic.SessionPool) (map[int64]*mv, error) {
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
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
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

	cfg := DefaultMVServiceConfig()
	cfg.TaskBackpressure = TaskBackpressureConfig{
		CPUThreshold: defaultMVTaskBackpressureCPUThreshold,
		MemThreshold: defaultMVTaskBackpressureMemThreshold,
		Delay:        defaultTaskBackpressureDelay,
	}
	mvs := NewMVService(ctx, se, newServerHelper(), cfg)
	mvs.NotifyDDLChange() // always trigger a refresh after startup to make sure the in-memory state is up-to-date

	// callback for DDL events only will be triggered on the DDL owner
	// other nodes will get notified through the NotifyDDLChange method from the domain service registry
	registerHandler(notifier.MVServiceHandlerID, func(_ context.Context, _ sessionctx.Context, event *notifier.SchemaChangeEvent) error {
		switch event.GetType() {
		case meta.ActionCreateMaterializedViewLog, meta.ActionCreateMaterializedView:
			onDDLHandled()
		default:
			_ = event
			// do nothing for other events
			// just let the regular refresh loop handle them
			// no need to fan out immediately
		}
		return nil
	})

	return mvs
}
