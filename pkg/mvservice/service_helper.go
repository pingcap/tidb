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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	meta "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	historyGCDeleteBatchSize        = 10000
	refreshAlertWriteBatchSize      = 128
	mvHistoryHeartbeatTimeout       = 24 * time.Hour
	mvHistoryOrphanedStatus         = "orphaned"
	mvRefreshHistoryOrphanedReason  = "task heartbeat expired before refresh history finalize"
	mvLogPurgeHistoryOrphanedReason = "task heartbeat expired before purge history finalize"
)

type serviceHelper struct {
	durationObserverCache durationObserverCache
	runEventCounterCache  runEventCounterCache

	reportCache struct {
		submittedCount    int64
		finishedCount     int64
		failedCount       int64
		timeoutCount      int64
		rejectedCount     int64
		backpressureCount int64
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

func buildDeleteMVRefreshAlertSQL(mviewIDs []int64) string {
	var sql strings.Builder
	sql.WriteString("DELETE FROM mysql.tidb_mview_refresh_alert WHERE MVIEW_ID IN (")
	for i, mviewID := range mviewIDs {
		if i > 0 {
			sql.WriteString(",")
		}
		sqlescape.MustFormatSQL(&sql, "%?", mviewID)
	}
	sql.WriteString(")")
	return sql.String()
}

func buildUpsertMVRefreshAlertSQL(updatedAt time.Time, states []refreshAlertTask) string {
	var sql strings.Builder
	sql.WriteString(`INSERT INTO mysql.tidb_mview_refresh_alert (
MVIEW_ID,
MV_SCHEMA,
MV_NAME,
ALERT_LEVEL,
LAST_SUCCESS_TIME,
UPDATED_AT
) VALUES `)
	for i, state := range states {
		if i > 0 {
			sql.WriteString(",")
		}
		sqlescape.MustFormatSQL(
			&sql,
			"(%?, %?, %?, %?, %?, %?)",
			state.mviewID,
			state.schemaName,
			state.mviewName,
			state.alertLevel,
			state.lastSuccessTime.Round(0),
			updatedAt.Round(0),
		)
	}
	sql.WriteString(` ON DUPLICATE KEY UPDATE
MV_SCHEMA = VALUES(MV_SCHEMA),
MV_NAME = VALUES(MV_NAME),
ALERT_LEVEL = VALUES(ALERT_LEVEL),
LAST_SUCCESS_TIME = VALUES(LAST_SUCCESS_TIME),
UPDATED_AT = VALUES(UPDATED_AT)`)
	return sql.String()
}

func buildCleanupStaleMVRefreshAlertSQL() string {
	return `DELETE a
FROM mysql.tidb_mview_refresh_alert AS a
LEFT JOIN mysql.tidb_mview_refresh_info AS i ON a.MVIEW_ID = i.MVIEW_ID
WHERE i.MVIEW_ID IS NULL OR i.NEXT_TIME IS NULL`
}

func (*serviceHelper) SyncMVRefreshAlertStates(
	ctx context.Context,
	sysSessionPool basic.SessionPool,
	updatedAt time.Time,
	states []refreshAlertTask,
) error {
	if len(states) == 0 {
		return nil
	}
	se, err := sysSessionPool.Get()
	if err != nil {
		return err
	}
	defer sysSessionPool.Put(se)

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMVMaintenance)
	sctx := se.(sessionctx.Context)

	deleteIDs := make([]int64, 0, len(states))
	upsertStates := make([]refreshAlertTask, 0, len(states))
	for _, state := range states {
		if state.mviewID <= 0 {
			continue
		}
		if state.metadataUnresolved {
			continue
		}
		if state.alertLevel == "" {
			deleteIDs = append(deleteIDs, state.mviewID)
			continue
		}
		upsertStates = append(upsertStates, state)
	}

	for start := 0; start < len(deleteIDs); start += refreshAlertWriteBatchSize {
		end := min(start+refreshAlertWriteBatchSize, len(deleteIDs))
		if _, err := execRCRestrictedSQLWithSession(ctx, sctx, buildDeleteMVRefreshAlertSQL(deleteIDs[start:end]), nil); err != nil {
			return err
		}
	}
	for start := 0; start < len(upsertStates); start += refreshAlertWriteBatchSize {
		end := min(start+refreshAlertWriteBatchSize, len(upsertStates))
		if _, err := execRCRestrictedSQLWithSession(ctx, sctx, buildUpsertMVRefreshAlertSQL(updatedAt, upsertStates[start:end]), nil); err != nil {
			return err
		}
	}
	return nil
}

func (*serviceHelper) CleanupStaleMVRefreshAlerts(
	ctx context.Context,
	sysSessionPool basic.SessionPool,
) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMVMaintenance)
	_, err := execRCRestrictedSQLWithSessionPool(ctx, sysSessionPool, buildCleanupStaleMVRefreshAlertSQL(), nil)
	return err
}

func (*serviceHelper) getServerInfo() (serverInfo, error) {
	localSrv, err := infosync.GetServerInfo()
	if err != nil {
		return serverInfo{}, err
	}
	return serverInfo{
		ID:             localSrv.ID,
		IP:             localSrv.IP,
		Port:           localSrv.Port,
		StartTimestamp: localSrv.StartTimestamp,
	}, nil
}

func (*serviceHelper) getAllServerInfo(ctx context.Context) (map[string]serverInfo, error) {
	allServers, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		return nil, err
	}
	return latestServerInfosByInstance(allServers), nil
}

func latestServerInfosByInstance(allServers map[string]*infosync.ServerInfo) map[string]serverInfo {
	servers := make(map[string]serverInfo, len(allServers))
	instanceToID := make(map[string]string, len(allServers))
	for _, srv := range allServers {
		if srv == nil {
			continue
		}
		instance := disttaskutil.GenerateExecID(srv)
		info := serverInfo{
			ID:             srv.ID,
			IP:             srv.IP,
			Port:           srv.Port,
			StartTimestamp: srv.StartTimestamp,
		}
		if existingID, ok := instanceToID[instance]; ok {
			existing := servers[existingID]
			// A fast restart can leave both the old and new ddl_id visible in infosync.
			// Keep the latest entry for the same IP:Port so ownership stays on the live node.
			if info.StartTimestamp <= existing.StartTimestamp {
				continue
			}
			delete(servers, existingID)
		}
		instanceToID[instance] = info.ID
		servers[info.ID] = info
	}
	return servers
}

func resolveMVIdentityByID(
	ctx context.Context,
	sctx sessionctx.Context,
	mvID int64,
) (schemaName, mviewName string, alertWarningSec, alertOverdueSec int64, found bool, err error) {
	if mvID <= 0 {
		return "", "", 0, 0, false, errors.New("mview id is invalid")
	}
	infoSchema := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	mvTable, ok := infoSchema.TableByID(ctx, mvID)
	if !ok {
		return "", "", 0, 0, false, nil
	}
	mvMeta := mvTable.Meta()
	if mvMeta == nil {
		return "", "", 0, 0, false, errors.New("mview metadata is invalid")
	}
	if mvMeta.MaterializedView != nil {
		alertWarningSec = max(0, mvMeta.MaterializedView.AlertWarningSec)
		alertOverdueSec = max(0, mvMeta.MaterializedView.AlertOverdueSec)
	}
	dbInfo, ok := infoSchema.SchemaByID(mvMeta.DBID)
	if !ok || dbInfo == nil {
		return "", "", 0, 0, false, errors.New("mview metadata is invalid")
	}
	schemaName = dbInfo.Name.L
	mviewName = mvMeta.Name.L
	if schemaName == "" || mviewName == "" {
		return "", "", 0, 0, false, errors.New("mview metadata is invalid")
	}
	return schemaName, mviewName, alertWarningSec, alertOverdueSec, true, nil
}

// RefreshMV executes one incremental refresh round for a materialized view.
//
// It:
// 1. Gets a system session from the pool.
// 2. Resolves schema/table names from MVIEW_ID.
// 3. Executes `REFRESH MATERIALIZED VIEW ... FAST`.
// 4. Reads NEXT_TIME from mysql.tidb_mview_refresh_info.
//
// The returned error only represents execution failures. A zero nextRefresh means
// no further scheduling is needed (for example, the MV metadata was removed).
func (*serviceHelper) RefreshMV(ctx context.Context, sysSessionPool basic.SessionPool, mvID int64) (nextRefresh time.Time, err error) {
	const (
		refreshMVSQL    = `REFRESH MATERIALIZED VIEW %n.%n FAST`
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
	schemaName, mviewName, _, _, found, err := resolveMVIdentityByID(ctx, sctx, mvID)
	if err != nil {
		return time.Time{}, err
	}
	if !found {
		return time.Time{}, nil
	}

	restoreRefreshSessionVars, err := applyMVRefreshSessionVarsFromGlobal(ctx, sctx.GetSessionVars())
	if err != nil {
		return time.Time{}, err
	}
	defer restoreRefreshSessionVars()

	if _, err = execRCRestrictedSQLWithSession(ctx, sctx, refreshMVSQL, []any{schemaName, mviewName}); err != nil {
		if isMVTaskCanceledManually(err) {
			return time.Time{}, errMVTaskCanceledManually
		}
		return time.Time{}, err
	}

	rows, err := execRCRestrictedSQLWithSession(ctx, sctx, findNextTimeSQL, []any{mvID})
	if err != nil {
		return time.Time{}, err
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		return time.Time{}, nil
	}
	nextRefresh = mvsUnix(rows[0].GetInt64(0), 0)
	return nextRefresh, nil
}

func getGlobalSystemVarBestEffort(ctx context.Context, sessVars *variable.SessionVars, varName string) (string, bool) {
	val, err := sessVars.GetGlobalSystemVar(ctx, varName)
	if err != nil {
		logutil.BgLogger().Warn(
			"mv service: failed to read global session var, fallback to current session value",
			zap.String("var", varName),
			zap.Error(err),
		)
		return "", false
	}
	return val, true
}

func applyMVRefreshSessionVarsFromGlobal(ctx context.Context, sessVars *variable.SessionVars) (func(), error) {
	if sessVars == nil {
		return nil, errors.New("mv service: session vars is nil")
	}

	target := variable.CaptureMViewExecutionSessionVars(sessVars)
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, variable.TiDBMVMaintainMemQuota); ok {
		target.MaintainMemQuota = variable.TidbOptInt64(val, target.MaintainMemQuota)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, variable.TiDBMaxTiFlashThreads); ok {
		target.TiFlashMaxThreads = variable.TidbOptInt64(val, target.TiFlashMaxThreads)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, variable.TiDBMaxBytesBeforeTiFlashExternalJoin); ok {
		target.TiFlashMaxBytesBeforeExtJoin = variable.TidbOptInt64(val, target.TiFlashMaxBytesBeforeExtJoin)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, variable.TiDBMaxBytesBeforeTiFlashExternalGroupBy); ok {
		target.TiFlashMaxBytesBeforeExtAgg = variable.TidbOptInt64(val, target.TiFlashMaxBytesBeforeExtAgg)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, variable.TiDBMaxBytesBeforeTiFlashExternalSort); ok {
		target.TiFlashMaxBytesBeforeExtSort = variable.TidbOptInt64(val, target.TiFlashMaxBytesBeforeExtSort)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, variable.TiFlashMemQuotaQueryPerNode); ok {
		target.TiFlashMemQuotaQueryPerNode = variable.TidbOptInt64(val, target.TiFlashMemQuotaQueryPerNode)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, variable.TiFlashQuerySpillRatio); ok {
		querySpillRatio, err := strconv.ParseFloat(val, 64)
		if err != nil {
			logutil.BgLogger().Warn(
				"mv service: failed to parse global session var, fallback to current session value",
				zap.String("var", variable.TiFlashQuerySpillRatio),
				zap.String("value", val),
				zap.Error(err),
			)
		} else {
			target.TiFlashQuerySpillRatio = querySpillRatio
		}
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, variable.TiFlashFineGrainedShuffleStreamCount); ok {
		target.FineGrainedStreamCount = variable.TidbOptInt64(val, target.FineGrainedStreamCount)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, variable.TiFlashFineGrainedShuffleBatchSize); ok {
		target.FineGrainedBatchSize = uint64(variable.TidbOptInt64(val, int64(target.FineGrainedBatchSize)))
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, variable.TiDBMViewMaintainImportThreads); ok {
		target.ImportThreads = variable.TidbOptInt(val, target.ImportThreads)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, variable.TiDBMViewMaintainImportDiskQuota); ok {
		target.ImportDiskQuota = val
	}
	return applyRefreshSessionVars(sessVars, target)
}

func applyMVMaintainMemQuotaFromGlobal(ctx context.Context, sessVars *variable.SessionVars) (func(), error) {
	if sessVars == nil {
		return nil, errors.New("mv service: session vars is nil")
	}

	originMaintainMemQuota := sessVars.MVMaintainMemQuota
	targetMaintainMemQuota := originMaintainMemQuota
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, variable.TiDBMVMaintainMemQuota); ok {
		targetMaintainMemQuota = variable.TidbOptInt64(val, originMaintainMemQuota)
	}
	if originMaintainMemQuota == targetMaintainMemQuota {
		return func() {}, nil
	}

	if err := sessVars.SetSystemVar(variable.TiDBMVMaintainMemQuota, strconv.FormatInt(targetMaintainMemQuota, 10)); err != nil {
		logutil.BgLogger().Warn(
			"mv service: failed to apply maintenance session var from global setting, fallback to current session value",
			zap.String("var", variable.TiDBMVMaintainMemQuota),
			zap.Int64("value", targetMaintainMemQuota),
			zap.Error(err),
		)
		return func() {}, nil
	}
	return func() {
		if err := sessVars.SetSystemVar(variable.TiDBMVMaintainMemQuota, strconv.FormatInt(originMaintainMemQuota, 10)); err != nil {
			logutil.BgLogger().Warn(
				"mv service: failed to restore tidb_mv_maintain_mem_quota after maintenance",
				zap.Int64("originMaintainMemQuota", originMaintainMemQuota),
				zap.Int64("currentMaintainMemQuota", targetMaintainMemQuota),
				zap.Error(err),
			)
		}
	}, nil
}

func applyRefreshSessionVars(sessVars *variable.SessionVars, target variable.MViewExecutionSessionVars) (func(), error) {
	return variable.ApplyMViewExecutionSessionVarsWithConfig(
		sessVars,
		target,
		variable.MViewExecutionSessionVarsApplyConfig{
			MaintainMemQuotaVarName: variable.TiDBMVMaintainMemQuota,
			CaptureAppliedVars:      variable.CaptureMViewExecutionSessionVars,
			BestEffort:              true,
			OnApplyError: func(name, value string, err error) {
				logutil.BgLogger().Warn(
					"mv service: failed to apply refresh session var from global setting, fallback to current session value",
					zap.String("var", name),
					zap.String("value", value),
					zap.Error(err),
				)
			},
			OnRestoreError: func(name, originValue, currentValue string, err error) {
				logutil.BgLogger().Warn(
					"mv service: failed to restore refresh session var after refresh",
					zap.String("var", name),
					zap.String("origin", originValue),
					zap.String("current", currentValue),
					zap.Error(err),
				)
			},
		},
	)
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

	restoreMaintainMemQuota, err := applyMVMaintainMemQuotaFromGlobal(ctx, sctx.GetSessionVars())
	if err != nil {
		return time.Time{}, err
	}
	defer restoreMaintainMemQuota()

	if _, err = execRCRestrictedSQLWithSession(ctx, sctx, purgeMVLogSQL, []any{baseSchema, baseTable}); err != nil {
		if isMVTaskCanceledManually(err) {
			return time.Time{}, errMVTaskCanceledManually
		}
		return time.Time{}, err
	}

	rows, err := execRCRestrictedSQLWithSession(ctx, sctx, findNextTimeSQL, []any{mvLogID})
	if err != nil {
		return time.Time{}, err
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		return time.Time{}, nil
	}
	nextPurge = mvsUnix(rows[0].GetInt64(0), 0)
	return nextPurge, nil
}

func (*serviceHelper) TryBackoffRefreshManualCancel(
	ctx context.Context,
	sysSessionPool basic.SessionPool,
	mvID int64,
	nextRefresh time.Time,
) (bool, time.Time, error) {
	return tryBackoffMVTaskManualCancel(
		ctx,
		sysSessionPool,
		`SELECT NEXT_TIME FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? FOR UPDATE NOWAIT`,
		`UPDATE mysql.tidb_mview_refresh_info SET NEXT_TIME = %? WHERE MVIEW_ID = %?`,
		mvID,
		nextRefresh,
		deriveMVRefreshManualCancelNextTime,
	)
}

func (*serviceHelper) TryBackoffPurgeManualCancel(
	ctx context.Context,
	sysSessionPool basic.SessionPool,
	mvLogID int64,
	nextPurge time.Time,
) (bool, time.Time, error) {
	return tryBackoffMVTaskManualCancel(
		ctx,
		sysSessionPool,
		`SELECT NEXT_TIME FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE NOWAIT`,
		`UPDATE mysql.tidb_mlog_purge_info SET NEXT_TIME = %? WHERE MLOG_ID = %?`,
		mvLogID,
		nextPurge,
		deriveMLogPurgeManualCancelNextTime,
	)
}

type mvTaskManualCancelNextResolver func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error)

func deriveMVRefreshManualCancelNextTime(
	ctx context.Context,
	sctx sessionctx.Context,
	mvID int64,
) (*time.Time, bool, error) {
	infoSchema := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	mvTbl, ok := infoSchema.TableByID(ctx, mvID)
	if !ok {
		return nil, false, nil
	}
	mvMeta := mvTbl.Meta()
	if mvMeta == nil || mvMeta.MaterializedView == nil {
		return nil, false, errors.New("materialized view metadata is invalid")
	}
	nextTime, shouldUpdate, err := deriveMaterializedScheduleNextTimeForManualCancel(
		ctx,
		sctx,
		mvMeta.MaterializedView.RefreshStartWith,
		mvMeta.MaterializedView.RefreshNext,
		mvMeta.MaterializedView.DefinitionSQLMode,
	)
	if err != nil {
		return nil, false, err
	}
	if shouldUpdate && nextTime == nil {
		if dbInfo, ok := infoschema.SchemaByTable(infoSchema, mvMeta); ok && dbInfo != nil {
			logManualCancelMaterializedViewRefreshNextTimeUpdateNull(dbInfo.Name.O, mvMeta.Name.O, mvMeta.MaterializedView.RefreshNext)
		} else {
			logManualCancelMaterializedViewRefreshNextTimeUpdateNull("", mvMeta.Name.O, mvMeta.MaterializedView.RefreshNext)
		}
	}
	return nextTime, shouldUpdate, nil
}

func deriveMLogPurgeManualCancelNextTime(
	ctx context.Context,
	sctx sessionctx.Context,
	mvLogID int64,
) (*time.Time, bool, error) {
	infoSchema := sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	mlogTbl, ok := infoSchema.TableByID(ctx, mvLogID)
	if !ok {
		return nil, false, nil
	}
	mlogMeta := mlogTbl.Meta()
	if mlogMeta == nil || mlogMeta.MaterializedViewLog == nil {
		return nil, false, errors.New("materialized view log metadata is invalid")
	}
	nextTime, shouldUpdate, err := deriveMaterializedScheduleNextTimeForManualCancel(
		ctx,
		sctx,
		mlogMeta.MaterializedViewLog.PurgeStartWith,
		mlogMeta.MaterializedViewLog.PurgeNext,
		mlogMeta.MaterializedViewLog.DefinitionSQLMode,
	)
	if err != nil {
		return nil, false, err
	}
	if shouldUpdate && nextTime == nil {
		if dbInfo, ok := infoschema.SchemaByTable(infoSchema, mlogMeta); ok && dbInfo != nil {
			logManualCancelMaterializedViewLogPurgeNextTimeUpdateNull(dbInfo.Name.O, mlogMeta.Name.O, mlogMeta.MaterializedViewLog.PurgeNext)
		} else {
			logManualCancelMaterializedViewLogPurgeNextTimeUpdateNull("", mlogMeta.Name.O, mlogMeta.MaterializedViewLog.PurgeNext)
		}
	}
	return nextTime, shouldUpdate, nil
}

func deriveMaterializedScheduleNextTimeForManualCancel(
	ctx context.Context,
	sctx sessionctx.Context,
	startExpr string,
	nextExpr string,
	scheduleSQLMode mysql.SQLMode,
) (*time.Time, bool, error) {
	nextAt, shouldUpdate, err := expression.DeriveMaterializedScheduleNextTimeUTC(
		ctx,
		sctx,
		sctx,
		startExpr,
		nextExpr,
		scheduleSQLMode,
	)
	if err != nil {
		return nil, false, err
	}
	if !shouldUpdate || nextAt == nil {
		return nil, shouldUpdate, nil
	}
	goTime, err := nextAt.GoTime(time.UTC)
	if err != nil {
		return nil, false, err
	}
	return &goTime, true, nil
}

func logManualCancelMaterializedViewRefreshNextTimeUpdateNull(
	schemaName string,
	mvName string,
	nextExpr string,
) {
	if strings.TrimSpace(nextExpr) == "" {
		return
	}
	logutil.BgLogger().Error(
		"refresh MV manual cancel backoff: automatic refresh schedule disabled because NEXT expression evaluated to NULL, updating NEXT_TIME to NULL",
		zap.String("schemaName", schemaName),
		zap.String("tableName", mvName),
		zap.String("refreshNext", nextExpr),
	)
}

func logManualCancelMaterializedViewLogPurgeNextTimeUpdateNull(
	schemaName string,
	mlogName string,
	nextExpr string,
) {
	if strings.TrimSpace(nextExpr) == "" {
		return
	}
	logutil.BgLogger().Error(
		"purge MV log manual cancel backoff: automatic purge schedule disabled because NEXT expression evaluated to NULL, updating NEXT_TIME to NULL",
		zap.String("schemaName", schemaName),
		zap.String("tableName", mlogName),
		zap.String("purgeNext", nextExpr),
	)
}

func tryBackoffMVTaskManualCancel(
	ctx context.Context,
	sysSessionPool basic.SessionPool,
	lockSQL string,
	updateSQL string,
	objectID int64,
	nextTime time.Time,
	resolveExpectedNext mvTaskManualCancelNextResolver,
) (bool, time.Time, error) {
	if objectID <= 0 {
		return false, time.Time{}, errors.New("mv service manual cancel backoff target id is invalid")
	}
	if nextTime.IsZero() {
		return false, time.Time{}, errors.New("mv service manual cancel backoff target time is invalid")
	}
	nextTimeLoc := nextTime.Location()
	nextTimeUTC := nextTime.UTC()

	se, err := sysSessionPool.Get()
	if err != nil {
		return false, time.Time{}, err
	}
	defer sysSessionPool.Put(se)

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMVMaintenance)
	sctx := se.(sessionctx.Context)
	sqlExec := sctx.GetSQLExecutor()
	if _, err := sqlExec.ExecuteInternal(ctx, "BEGIN PESSIMISTIC"); err != nil {
		return false, time.Time{}, err
	}

	committed := false
	defer func() {
		if !committed {
			_, _ = sqlExec.ExecuteInternal(ctx, "ROLLBACK")
		}
	}()

	rows, err := execRestrictedSQLWithSession(ctx, sctx, lockSQL, []any{objectID})
	if err != nil {
		if storeerr.ErrLockAcquireFailAndNoWaitSet.Equal(err) {
			return false, time.Time{}, nil
		}
		return false, time.Time{}, err
	}
	if len(rows) == 0 {
		return false, time.Time{}, nil
	}

	appliedNextUTC := nextTimeUTC
	var appliedNextArg any = nextTimeUTC
	clearAppliedNext := false
	resolvedNext, shouldUpdate, err := resolveExpectedNext(ctx, sctx, objectID)
	// If current metadata cannot provide a better schedule, keep the cooldown fallback
	// instead of dropping the persisted backoff on the floor.
	if err == nil {
		if shouldUpdate {
			if resolvedNext == nil {
				appliedNextArg = nil
				clearAppliedNext = true
			} else {
				appliedNextUTC = resolvedNext.UTC()
				if appliedNextUTC.Before(nextTimeUTC) {
					appliedNextUTC = nextTimeUTC
				}
				appliedNextArg = appliedNextUTC
			}
		}
	}
	if _, err := execRCRestrictedSQLWithSession(ctx, sctx, updateSQL, []any{appliedNextArg, objectID}); err != nil {
		return false, time.Time{}, err
	}
	if _, err := sqlExec.ExecuteInternal(ctx, "COMMIT"); err != nil {
		return false, time.Time{}, err
	}
	committed = true
	if clearAppliedNext {
		return true, time.Time{}, nil
	}
	return true, appliedNextUTC.In(nextTimeLoc), nil
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
func (*serviceHelper) PurgeMVHistoryBeforeTSO(
	ctx context.Context,
	sysSessionPool basic.SessionPool,
	currentTSO uint64,
	mviewRefreshRetention time.Duration,
	mlogPurgeRetention time.Duration,
) error {
	markStaleMVRefreshHistOrphanedSQL := fmt.Sprintf(
		`UPDATE mysql.tidb_mview_refresh_hist
SET REFRESH_STATUS = '%s',
	REFRESH_ENDTIME = IFNULL(REFRESH_ENDTIME, NOW(6)),
	REFRESH_DURATION_SEC = IFNULL(REFRESH_DURATION_SEC, CASE WHEN REFRESH_TIME IS NULL THEN NULL ELSE TIMESTAMPDIFF(MICROSECOND, REFRESH_TIME, NOW(6)) / 1000000.0 END),
	REFRESH_FAILED_REASON = IFNULL(REFRESH_FAILED_REASON, '%s')
WHERE REFRESH_STATUS = 'running'
  AND COALESCE(LAST_HEARTBEAT_AT, REFRESH_TIME) < %%?
ORDER BY REFRESH_JOB_ID
LIMIT %d`,
		mvHistoryOrphanedStatus,
		mvRefreshHistoryOrphanedReason,
		historyGCDeleteBatchSize,
	)
	markStaleMVLogPurgeHistOrphanedSQL := fmt.Sprintf(
		`UPDATE mysql.tidb_mlog_purge_hist
SET PURGE_STATUS = '%s',
	PURGE_ENDTIME = IFNULL(PURGE_ENDTIME, NOW(6)),
	PURGE_DURATION_SEC = IFNULL(PURGE_DURATION_SEC, CASE WHEN PURGE_TIME IS NULL THEN NULL ELSE TIMESTAMPDIFF(MICROSECOND, PURGE_TIME, NOW(6)) / 1000000.0 END),
	PURGE_FAILED_REASON = IFNULL(PURGE_FAILED_REASON, '%s')
WHERE PURGE_STATUS = 'running'
  AND COALESCE(LAST_HEARTBEAT_AT, PURGE_TIME) < %%?
ORDER BY PURGE_JOB_ID
LIMIT %d`,
		mvHistoryOrphanedStatus,
		mvLogPurgeHistoryOrphanedReason,
		historyGCDeleteBatchSize,
	)
	deleteMVRefreshHistSQL := fmt.Sprintf(
		`DELETE FROM mysql.tidb_mview_refresh_hist WHERE (REFRESH_STATUS IS NULL OR REFRESH_STATUS <> 'running') AND REFRESH_JOB_ID < %%? ORDER BY REFRESH_JOB_ID LIMIT %d`,
		historyGCDeleteBatchSize,
	)
	deleteMVLogPurgeHistSQL := fmt.Sprintf(
		`DELETE FROM mysql.tidb_mlog_purge_hist WHERE (PURGE_STATUS IS NULL OR PURGE_STATUS <> 'running') AND PURGE_JOB_ID < %%? ORDER BY PURGE_JOB_ID LIMIT %d`,
		historyGCDeleteBatchSize,
	)
	const countMVRefreshHistSQL = `SELECT COUNT(*), MIN(REFRESH_JOB_ID) FROM mysql.tidb_mview_refresh_hist`
	const countMVLogPurgeHistSQL = `SELECT COUNT(*), MIN(PURGE_JOB_ID) FROM mysql.tidb_mlog_purge_hist`
	deleteMVRefreshHistByCountSQL := func(limit uint64) string {
		return fmt.Sprintf(
			`DELETE FROM mysql.tidb_mview_refresh_hist WHERE (REFRESH_STATUS IS NULL OR REFRESH_STATUS <> 'running') AND REFRESH_JOB_ID >= %%? ORDER BY REFRESH_JOB_ID LIMIT %d`,
			limit,
		)
	}
	deleteMVLogPurgeHistByCountSQL := func(limit uint64) string {
		return fmt.Sprintf(
			`DELETE FROM mysql.tidb_mlog_purge_hist WHERE (PURGE_STATUS IS NULL OR PURGE_STATUS <> 'running') AND PURGE_JOB_ID >= %%? ORDER BY PURGE_JOB_ID LIMIT %d`,
			limit,
		)
	}

	calcCutoffTSO := func(retention time.Duration) uint64 {
		cutoffTSO := currentTSO
		if retention > 0 {
			cutoffPhysical := max(oracle.ExtractPhysical(currentTSO)-int64(retention/time.Millisecond), 0)
			cutoffTSO = oracle.ComposeTS(cutoffPhysical, 0)
		}
		return cutoffTSO
	}
	calcHeartbeatCutoffTime := func(timeout time.Duration) time.Time {
		cutoffPhysical := max(oracle.ExtractPhysical(currentTSO)-int64(timeout/time.Millisecond), 0)
		return oracle.GetTimeFromTS(oracle.ComposeTS(cutoffPhysical, 0))
	}

	se, err := sysSessionPool.Get()
	if err != nil {
		return err
	}
	defer sysSessionPool.Put(se)

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMVMaintenance)
	sctx := se.(sessionctx.Context)

	var purgeErrs []error
	if err := reconcileMVHistoryRunningRowsInBatches(ctx, sctx, markStaleMVRefreshHistOrphanedSQL, calcHeartbeatCutoffTime(mvHistoryHeartbeatTimeout)); err != nil {
		purgeErrs = append(purgeErrs, fmt.Errorf("reconcile stale mview refresh history running rows failed: %w", err))
	}
	if err := purgeMVHistoryInBatches(ctx, sctx, deleteMVRefreshHistSQL, calcCutoffTSO(mviewRefreshRetention)); err != nil {
		purgeErrs = append(purgeErrs, fmt.Errorf("purge mview refresh history by retention failed: %w", err))
	}
	if err := purgeMVHistoryByCountLimit(ctx, sctx, countMVRefreshHistSQL, deleteMVRefreshHistByCountSQL, defaultMVHistoryGCMaxRecords); err != nil {
		purgeErrs = append(purgeErrs, fmt.Errorf("purge mview refresh history by count limit failed: %w", err))
	}
	if err := reconcileMVHistoryRunningRowsInBatches(ctx, sctx, markStaleMVLogPurgeHistOrphanedSQL, calcHeartbeatCutoffTime(mvHistoryHeartbeatTimeout)); err != nil {
		purgeErrs = append(purgeErrs, fmt.Errorf("reconcile stale mlog purge history running rows failed: %w", err))
	}
	if err := purgeMVHistoryInBatches(ctx, sctx, deleteMVLogPurgeHistSQL, calcCutoffTSO(mlogPurgeRetention)); err != nil {
		purgeErrs = append(purgeErrs, fmt.Errorf("purge mlog purge history by retention failed: %w", err))
	}
	if err := purgeMVHistoryByCountLimit(ctx, sctx, countMVLogPurgeHistSQL, deleteMVLogPurgeHistByCountSQL, defaultMVHistoryGCMaxRecords); err != nil {
		purgeErrs = append(purgeErrs, fmt.Errorf("purge mlog purge history by count limit failed: %w", err))
	}
	if len(purgeErrs) > 0 {
		return errors.Join(purgeErrs...)
	}
	return nil
}

func reconcileMVHistoryRunningRowsInBatches(ctx context.Context, sctx sessionctx.Context, sql string, cutoffTime time.Time) error {
	for {
		if _, err := execRCRestrictedSQLWithSession(ctx, sctx, sql, []any{cutoffTime}); err != nil {
			return err
		}
		affectedRows := sctx.GetSessionVars().StmtCtx.AffectedRows()
		if affectedRows < uint64(historyGCDeleteBatchSize) {
			return nil
		}
	}
}

func purgeMVHistoryInBatches(ctx context.Context, sctx sessionctx.Context, sql string, cutoffTSO uint64) error {
	for {
		if _, err := execRCRestrictedSQLWithSession(ctx, sctx, sql, []any{cutoffTSO}); err != nil {
			return err
		}
		affectedRows := sctx.GetSessionVars().StmtCtx.AffectedRows()
		if affectedRows < uint64(historyGCDeleteBatchSize) {
			return nil
		}
	}
}

func purgeMVHistoryByCountLimit(
	ctx context.Context,
	sctx sessionctx.Context,
	countSQL string,
	deleteSQL func(limit uint64) string,
	maxRecords uint64,
) error {
	if maxRecords == 0 {
		return nil
	}
	rows, err := execRCRestrictedSQLWithSession(ctx, sctx, countSQL, nil)
	if err != nil {
		return err
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		return nil
	}
	totalCount := rows[0].GetInt64(0)
	if totalCount <= 0 || rows[0].IsNull(1) {
		return nil
	}
	totalCountUint := uint64(totalCount)
	if totalCountUint <= maxRecords {
		return nil
	}
	minJobID := rows[0].GetUint64(1)
	remainingDelete := totalCountUint - maxRecords
	for remainingDelete > 0 {
		batchSize := remainingDelete
		if batchSize > uint64(historyGCDeleteBatchSize) {
			batchSize = uint64(historyGCDeleteBatchSize)
		}
		if _, err := execRCRestrictedSQLWithSession(ctx, sctx, deleteSQL(batchSize), []any{minJobID}); err != nil {
			return err
		}
		affectedRows := sctx.GetSessionVars().StmtCtx.AffectedRows()
		if affectedRows == 0 || affectedRows < batchSize {
			return nil
		}
		remainingDelete -= affectedRows
	}
	return nil
}

// loadAllTiDBMVLogPurge loads all scheduled MV log purge tasks from metadata.
func (*serviceHelper) loadAllTiDBMVLogPurge(ctx context.Context, sysSessionPool basic.SessionPool) (map[int64]*mvLog, error) {
	const sql = `SELECT TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) as NEXT_TIME_SEC, MLOG_ID FROM mysql.tidb_mlog_purge_info WHERE NEXT_TIME IS NOT NULL`
	rows, err := execRCRestrictedSQLWithSessionPool(ctx, sysSessionPool, sql, nil)
	if err != nil {
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

// loadAllTiDBMVRefresh loads all scheduled MV refresh tasks from metadata.
func (*serviceHelper) loadAllTiDBMVRefresh(ctx context.Context, sysSessionPool basic.SessionPool) (map[int64]*mv, error) {
	const sql = `SELECT TIMESTAMPDIFF(SECOND, '1970-01-01 00:00:00', NEXT_TIME) as NEXT_TIME_SEC, MVIEW_ID, LAST_SUCCESS_READ_TSO FROM mysql.tidb_mview_refresh_info WHERE NEXT_TIME IS NOT NULL`
	se, err := sysSessionPool.Get()
	if err != nil {
		return nil, err
	}
	defer sysSessionPool.Put(se)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMVMaintenance)
	sctx := se.(sessionctx.Context)
	rows, err := execRCRestrictedSQLWithSession(ctx, sctx, sql, nil)
	if err != nil {
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
		var lastSuccessReadTSO uint64
		var lastSuccessTime time.Time
		if !row.IsNull(2) {
			tso := row.GetInt64(2)
			if tso > 0 {
				lastSuccessReadTSO = uint64(tso)
				lastSuccessTime = mvsUnixMilli(oracle.ExtractPhysical(lastSuccessReadTSO))
			}
		}
		m := &mv{
			ID:                 mvID,
			nextRefresh:        nextRefresh,
			lastSuccessReadTSO: lastSuccessReadTSO,
			lastSuccessTime:    lastSuccessTime,
		}
		schemaName, mviewName, alertWarningSec, alertOverdueSec, found, resolveErr := resolveMVIdentityByID(ctx, sctx, mvID)
		if resolveErr == nil && found {
			m.schemaName = schemaName
			m.mviewName = mviewName
			m.alertWarningSec = alertWarningSec
			m.alertOverdueSec = alertOverdueSec
		} else {
			m.metadataUnresolved = true
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
			zap.Error(err),
		)
		return nil, err
	}
	defer sysSessionPool.Put(se)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMVMaintenance)
	return execRCRestrictedSQLWithSession(ctx, se.(sessionctx.Context), sql, params)
}

// execRCRestrictedSQLWithSession executes SQL through the restricted SQL executor.
func execRCRestrictedSQLWithSession(ctx context.Context, sctx sessionctx.Context, sql string, params []any) ([]chunk.Row, error) {
	r, err := execRestrictedSQLWithSession(ctx, sctx, sql, params)
	if err != nil {
		logutil.BgLogger().Warn(
			"execute restricted SQL failed",
			zap.String("sql", sql),
			zap.Int("param_count", len(params)),
			zap.NamedError("context_error", ctx.Err()),
			zap.Error(err),
		)
	}
	return r, err
}

func execRestrictedSQLWithSession(ctx context.Context, sctx sessionctx.Context, sql string, params []any) ([]chunk.Row, error) {
	r, _, err := sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(
		ctx,
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession},
		sql,
		params...,
	)
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
	cfg.TaskBackpressure = TaskBackpressureConfig{
		CPUThreshold: defaultBackpressureCPUThreshold,
		MemThreshold: defaultBackpressureMemThreshold,
		Delay:        defaultTaskBackpressureDelay,
	}
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
	case meta.ActionCreateTable:
		tbl := event.GetCreateTableInfo()
		return hasMVRelatedTableInfo(tbl)
	case meta.ActionDropTable:
		dropped := event.GetDropTableInfo()
		return hasMVRelatedTableInfo(dropped)
	case meta.ActionAlterMaterializedViewRefresh, meta.ActionAlterMaterializedViewAttributes, meta.ActionAlterMaterializedViewLogPurge, meta.ActionMViewRefreshOutOfPlaceCutover:
		return true
	default:
		// For other DDL types, rely on periodic refresh.
		return false
	}
}

func hasMVRelatedTableInfo(tbl *meta.TableInfo) bool {
	if tbl == nil {
		return false
	}
	return tbl.MaterializedView != nil || tbl.MaterializedViewLog != nil
}
