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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	meta "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	storeerr "github.com/pingcap/tidb/pkg/store/driver/error"
	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type serviceHelper struct {
	durationObserverCache durationObserverCache
	runEventCounterCache  runEventCounterCache

	reportCache struct {
		refresh taskExecutorReportCache
		purge   taskExecutorReportCache
	}
}

type taskExecutorReportCache struct {
	submittedCount    int64
	finishedCount     int64
	failedCount       int64
	timeoutCount      int64
	rejectedCount     int64
	backpressureCount int64
}

type mviewMetricTypeResultKey struct {
	typ    string
	result string
}

type durationObserverCache struct {
	mu   sync.RWMutex
	data map[mviewMetricTypeResultKey]prometheus.Observer
}

func newDurationObserverCache(capacity int) durationObserverCache {
	if capacity < 0 {
		capacity = 0
	}
	return durationObserverCache{
		data: make(map[mviewMetricTypeResultKey]prometheus.Observer, capacity),
	}
}

type runEventCounterCache struct {
	mu   sync.RWMutex
	data map[mviewMetricComponentTypeKey]prometheus.Counter
}

func newRunEventCounterCache(capacity int) runEventCounterCache {
	if capacity < 0 {
		capacity = 0
	}
	return runEventCounterCache{
		data: make(map[mviewMetricComponentTypeKey]prometheus.Counter, capacity),
	}
}

type mviewMetricComponentTypeKey struct {
	component string
	typ       string
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

func latestServerInfosByInstance(allServers map[string]*serverinfo.ServerInfo) map[string]serverInfo {
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
	mviewID int64,
) (schemaName, mviewName string, found bool, err error) {
	if mviewID <= 0 {
		return "", "", false, errors.New("mview id is invalid")
	}
	infoSchema := sctx.GetLatestISWithoutSessExt().(infoschema.InfoSchema)
	mviewTable, ok := infoSchema.TableByID(ctx, mviewID)
	if !ok {
		return "", "", false, nil
	}
	mviewMeta := mviewTable.Meta()
	if mviewMeta == nil {
		return "", "", false, errors.New("mview metadata is invalid")
	}
	dbInfo, ok := infoSchema.SchemaByID(mviewMeta.DBID)
	if !ok || dbInfo == nil {
		return "", "", false, errors.New("mview metadata is invalid")
	}
	schemaName = dbInfo.Name.L
	mviewName = mviewMeta.Name.L
	if schemaName == "" || mviewName == "" {
		return "", "", false, errors.New("mview metadata is invalid")
	}
	return schemaName, mviewName, true, nil
}

// RefreshMV executes one incremental refresh round for a materialized view.
//
// It:
// 1. Gets a system session from the pool.
// 2. Resolves schema/table names from MVIEW_ID.
// 3. Executes `REFRESH MATERIALIZED VIEW ... FAST`.
// 4. Reads NEXT_REFRESH_UNIX_SECONDS from mysql.tidb_mview_refresh_info.
//
// The returned error only represents execution failures. A zero nextRefresh means
// no further scheduling is needed (for example, the MV metadata was removed).
func (*serviceHelper) RefreshMV(ctx context.Context, sysSessionPool basic.SessionPool, mviewID int64) (nextRefresh time.Time, err error) {
	const (
		refreshMVSQL    = `REFRESH MATERIALIZED VIEW %n.%n FAST`
		findNextTimeSQL = `SELECT NEXT_REFRESH_UNIX_SECONDS FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? AND NEXT_REFRESH_UNIX_SECONDS IS NOT NULL`
	)
	startAt := mvsNow()
	var schemaName, mviewName string
	defer func() {
		if err == nil {
			return
		}
		logutil.BgLogger().Warn(
			"refresh materialized view failed",
			zap.Int64("mview_id", mviewID),
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
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMViewMaintenance)

	sctx := se.(sessionctx.Context)
	schemaName, mviewName, found, err := resolveMVIdentityByID(ctx, sctx, mviewID)
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

	rows, err := execRCRestrictedSQLWithSession(ctx, sctx, findNextTimeSQL, []any{mviewID})
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
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiDBMViewMaintainMemQuota); ok {
		target.MaintainMemQuota = variable.TidbOptInt64(val, target.MaintainMemQuota)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiDBMaxTiFlashThreads); ok {
		target.TiFlashMaxThreads = variable.TidbOptInt64(val, target.TiFlashMaxThreads)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiDBMaxBytesBeforeTiFlashExternalJoin); ok {
		target.TiFlashMaxBytesBeforeExtJoin = variable.TidbOptInt64(val, target.TiFlashMaxBytesBeforeExtJoin)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiDBMaxBytesBeforeTiFlashExternalGroupBy); ok {
		target.TiFlashMaxBytesBeforeExtAgg = variable.TidbOptInt64(val, target.TiFlashMaxBytesBeforeExtAgg)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiDBMaxBytesBeforeTiFlashExternalSort); ok {
		target.TiFlashMaxBytesBeforeExtSort = variable.TidbOptInt64(val, target.TiFlashMaxBytesBeforeExtSort)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiFlashMemQuotaQueryPerNode); ok {
		target.TiFlashMemQuotaQueryPerNode = variable.TidbOptInt64(val, target.TiFlashMemQuotaQueryPerNode)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiFlashQuerySpillRatio); ok {
		querySpillRatio, err := strconv.ParseFloat(val, 64)
		if err != nil {
			logutil.BgLogger().Warn(
				"mv service: failed to parse global session var, fallback to current session value",
				zap.String("var", vardef.TiFlashQuerySpillRatio),
				zap.String("value", val),
				zap.Error(err),
			)
		} else {
			target.TiFlashQuerySpillRatio = querySpillRatio
		}
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiFlashFineGrainedShuffleStreamCount); ok {
		target.FineGrainedStreamCount = variable.TidbOptInt64(val, target.FineGrainedStreamCount)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiFlashFineGrainedShuffleBatchSize); ok {
		target.FineGrainedBatchSize = uint64(variable.TidbOptInt64(val, int64(target.FineGrainedBatchSize)))
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiDBMViewMaintainImportThreads); ok {
		target.ImportThreads = variable.TidbOptInt(val, target.ImportThreads)
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiDBMViewMaintainImportDiskQuota); ok {
		target.ImportDiskQuota = val
	}
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiDBMViewMaintainIsolationReadEngines); ok {
		target.IsolationReadEngines = val
	}
	return applyRefreshSessionVars(sessVars, target)
}

type mviewMaintenanceSessionVarApplySpec struct {
	varName        string
	originValue    string
	targetValue    string
	onApplyError   func(error)
	onRestoreError func(error)
}

func applyMVMaintenanceSessionVarBestEffort(
	sessVars *variable.SessionVars,
	spec mviewMaintenanceSessionVarApplySpec,
) (func(), error) {
	if sessVars == nil {
		return nil, errors.New("mv service: session vars is nil")
	}
	if spec.originValue == spec.targetValue {
		return func() {}, nil
	}

	if err := sessVars.SetSystemVar(spec.varName, spec.targetValue); err != nil {
		if spec.onApplyError != nil {
			spec.onApplyError(err)
		}
		return func() {}, nil
	}
	return func() {
		if err := sessVars.SetSystemVar(spec.varName, spec.originValue); err != nil && spec.onRestoreError != nil {
			spec.onRestoreError(err)
		}
	}, nil
}

func applyMVMaintenanceSessionVarsFromGlobal(ctx context.Context, sessVars *variable.SessionVars) (func(), error) {
	if sessVars == nil {
		return nil, errors.New("mv service: session vars is nil")
	}

	originMaintainMemQuota := sessVars.MViewMaintainMemQuota
	targetMaintainMemQuota := originMaintainMemQuota
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiDBMViewMaintainMemQuota); ok {
		targetMaintainMemQuota = variable.TidbOptInt64(val, originMaintainMemQuota)
	}
	restoreMaintainMemQuota, err := applyMVMaintenanceSessionVarBestEffort(
		sessVars,
		mviewMaintenanceSessionVarApplySpec{
			varName:     vardef.TiDBMViewMaintainMemQuota,
			originValue: strconv.FormatInt(originMaintainMemQuota, 10),
			targetValue: strconv.FormatInt(targetMaintainMemQuota, 10),
			onApplyError: func(err error) {
				logutil.BgLogger().Warn(
					"mv service: failed to apply maintenance session var from global setting, fallback to current session value",
					zap.String("var", vardef.TiDBMViewMaintainMemQuota),
					zap.Int64("value", targetMaintainMemQuota),
					zap.Error(err),
				)
			},
			onRestoreError: func(err error) {
				logutil.BgLogger().Warn(
					"mv service: failed to restore tidb_mv_maintain_mem_quota after maintenance",
					zap.Int64("originMaintainMemQuota", originMaintainMemQuota),
					zap.Int64("currentMaintainMemQuota", targetMaintainMemQuota),
					zap.Error(err),
				)
			},
		},
	)
	if err != nil {
		return nil, err
	}

	originIsolationReadEngines := sessVars.MViewMaintainIsolationReadEngines
	targetIsolationReadEngines := originIsolationReadEngines
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiDBMViewMaintainIsolationReadEngines); ok {
		targetIsolationReadEngines = val
	}
	restoreIsolationReadEngines, err := applyMVMaintenanceSessionVarBestEffort(
		sessVars,
		mviewMaintenanceSessionVarApplySpec{
			varName:     vardef.TiDBMViewMaintainIsolationReadEngines,
			originValue: originIsolationReadEngines,
			targetValue: targetIsolationReadEngines,
			onApplyError: func(err error) {
				logutil.BgLogger().Warn(
					"mv service: failed to apply maintenance isolation read engines from global setting, fallback to current session value",
					zap.String("var", vardef.TiDBMViewMaintainIsolationReadEngines),
					zap.String("origin", originIsolationReadEngines),
					zap.String("target", targetIsolationReadEngines),
					zap.Error(err),
				)
			},
			onRestoreError: func(err error) {
				logutil.BgLogger().Warn(
					"mv service: failed to restore tidb_mv_maintain_isolation_read_engines after maintenance",
					zap.String("originIsolationReadEngines", originIsolationReadEngines),
					zap.String("currentIsolationReadEngines", targetIsolationReadEngines),
					zap.Error(err),
				)
			},
		},
	)
	if err != nil {
		restoreMaintainMemQuota()
		return nil, err
	}
	originPurgeBatchSize := sessVars.MLogPurgeBatchSize
	targetPurgeBatchSize := originPurgeBatchSize
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiDBMLogPurgeBatchSize); ok {
		targetPurgeBatchSize = variable.TidbOptInt(val, originPurgeBatchSize)
	}
	restorePurgeBatchSize, err := applyMVMaintenanceSessionVarBestEffort(
		sessVars,
		mviewMaintenanceSessionVarApplySpec{
			varName:     vardef.TiDBMLogPurgeBatchSize,
			originValue: strconv.Itoa(originPurgeBatchSize),
			targetValue: strconv.Itoa(targetPurgeBatchSize),
			onApplyError: func(err error) {
				logutil.BgLogger().Warn(
					"mv service: failed to apply purge batch size from global setting, fallback to current session value",
					zap.String("var", vardef.TiDBMLogPurgeBatchSize),
					zap.Int("origin", originPurgeBatchSize),
					zap.Int("target", targetPurgeBatchSize),
					zap.Error(err),
				)
			},
			onRestoreError: func(err error) {
				logutil.BgLogger().Warn(
					"mv service: failed to restore tidb_mlog_purge_batch_size after maintenance",
					zap.Int("originPurgeBatchSize", originPurgeBatchSize),
					zap.Int("currentPurgeBatchSize", targetPurgeBatchSize),
					zap.Error(err),
				)
			},
		},
	)
	if err != nil {
		restoreIsolationReadEngines()
		restoreMaintainMemQuota()
		return nil, err
	}
	originPurgeMinRate := sessVars.MLogPurgeMinRate
	targetPurgeMinRate := originPurgeMinRate
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiDBMLogPurgeMinRate); ok {
		targetPurgeMinRate = variable.TidbOptInt(val, originPurgeMinRate)
	}
	restorePurgeMinRate, err := applyMVMaintenanceSessionVarBestEffort(
		sessVars,
		mviewMaintenanceSessionVarApplySpec{
			varName:     vardef.TiDBMLogPurgeMinRate,
			originValue: strconv.Itoa(originPurgeMinRate),
			targetValue: strconv.Itoa(targetPurgeMinRate),
			onApplyError: func(err error) {
				logutil.BgLogger().Warn(
					"mv service: failed to apply purge min rate from global setting, fallback to current session value",
					zap.String("var", vardef.TiDBMLogPurgeMinRate),
					zap.Int("origin", originPurgeMinRate),
					zap.Int("target", targetPurgeMinRate),
					zap.Error(err),
				)
			},
			onRestoreError: func(err error) {
				logutil.BgLogger().Warn(
					"mv service: failed to restore tidb_mlog_purge_min_rate after maintenance",
					zap.Int("originPurgeMinRate", originPurgeMinRate),
					zap.Int("currentPurgeMinRate", targetPurgeMinRate),
					zap.Error(err),
				)
			},
		},
	)
	if err != nil {
		restorePurgeBatchSize()
		restoreIsolationReadEngines()
		restoreMaintainMemQuota()
		return nil, err
	}
	originPurgeRateBudgetRatio := sessVars.MLogPurgeRateBudgetRatio
	targetPurgeRateBudgetRatio := originPurgeRateBudgetRatio
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiDBMLogPurgeRateBudgetRatio); ok {
		parsed, parseErr := strconv.ParseFloat(val, 64)
		if parseErr != nil {
			logutil.BgLogger().Warn(
				"mv service: failed to parse global session var, fallback to current session value",
				zap.String("var", vardef.TiDBMLogPurgeRateBudgetRatio),
				zap.String("value", val),
				zap.Error(parseErr),
			)
		} else {
			targetPurgeRateBudgetRatio = parsed
		}
	}
	restorePurgeRateBudgetRatio, err := applyMVMaintenanceSessionVarBestEffort(
		sessVars,
		mviewMaintenanceSessionVarApplySpec{
			varName:     vardef.TiDBMLogPurgeRateBudgetRatio,
			originValue: strconv.FormatFloat(originPurgeRateBudgetRatio, 'f', -1, 64),
			targetValue: strconv.FormatFloat(targetPurgeRateBudgetRatio, 'f', -1, 64),
			onApplyError: func(err error) {
				logutil.BgLogger().Warn(
					"mv service: failed to apply purge rate budget ratio from global setting, fallback to current session value",
					zap.String("var", vardef.TiDBMLogPurgeRateBudgetRatio),
					zap.Float64("origin", originPurgeRateBudgetRatio),
					zap.Float64("target", targetPurgeRateBudgetRatio),
					zap.Error(err),
				)
			},
			onRestoreError: func(err error) {
				logutil.BgLogger().Warn(
					"mv service: failed to restore tidb_mlog_purge_rate_budget_ratio after maintenance",
					zap.Float64("originPurgeRateBudgetRatio", originPurgeRateBudgetRatio),
					zap.Float64("currentPurgeRateBudgetRatio", targetPurgeRateBudgetRatio),
					zap.Error(err),
				)
			},
		},
	)
	if err != nil {
		restorePurgeMinRate()
		restorePurgeBatchSize()
		restoreIsolationReadEngines()
		restoreMaintainMemQuota()
		return nil, err
	}
	originPurgeDeleteTiFlashThreads := sessVars.MLogPurgeDeleteTiFlashThreads
	targetPurgeDeleteTiFlashThreads := originPurgeDeleteTiFlashThreads
	if val, ok := getGlobalSystemVarBestEffort(ctx, sessVars, vardef.TiDBMLogPurgeDeleteTiFlashThreads); ok {
		targetPurgeDeleteTiFlashThreads = variable.TidbOptInt64(val, originPurgeDeleteTiFlashThreads)
	}
	restorePurgeDeleteTiFlashThreads, err := applyMVMaintenanceSessionVarBestEffort(
		sessVars,
		mviewMaintenanceSessionVarApplySpec{
			varName:     vardef.TiDBMLogPurgeDeleteTiFlashThreads,
			originValue: strconv.FormatInt(originPurgeDeleteTiFlashThreads, 10),
			targetValue: strconv.FormatInt(targetPurgeDeleteTiFlashThreads, 10),
			onApplyError: func(err error) {
				logutil.BgLogger().Warn(
					"mv service: failed to apply purge delete TiFlash threads from global setting, fallback to current session value",
					zap.String("var", vardef.TiDBMLogPurgeDeleteTiFlashThreads),
					zap.Int64("origin", originPurgeDeleteTiFlashThreads),
					zap.Int64("target", targetPurgeDeleteTiFlashThreads),
					zap.Error(err),
				)
			},
			onRestoreError: func(err error) {
				logutil.BgLogger().Warn(
					"mv service: failed to restore tidb_mlog_purge_delete_tiflash_threads after maintenance",
					zap.Int64("originPurgeDeleteTiFlashThreads", originPurgeDeleteTiFlashThreads),
					zap.Int64("currentPurgeDeleteTiFlashThreads", targetPurgeDeleteTiFlashThreads),
					zap.Error(err),
				)
			},
		},
	)
	if err != nil {
		restorePurgeRateBudgetRatio()
		restorePurgeMinRate()
		restorePurgeBatchSize()
		restoreIsolationReadEngines()
		restoreMaintainMemQuota()
		return nil, err
	}
	return func() {
		restorePurgeDeleteTiFlashThreads()
		restorePurgeRateBudgetRatio()
		restorePurgeMinRate()
		restorePurgeBatchSize()
		restoreIsolationReadEngines()
		restoreMaintainMemQuota()
	}, nil
}

func applyRefreshSessionVars(sessVars *variable.SessionVars, target variable.MViewExecutionSessionVars) (func(), error) {
	return variable.ApplyMViewExecutionSessionVarsWithConfig(
		sessVars,
		target,
		variable.MViewExecutionSessionVarsApplyConfig{
			MaintainMemQuotaVarName:             vardef.TiDBMViewMaintainMemQuota,
			MaintainIsolationReadEnginesVarName: vardef.TiDBMViewMaintainIsolationReadEngines,
			CaptureAppliedVars:                  variable.CaptureMViewExecutionSessionVars,
			BestEffort:                          true,
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
// 2. Resolves schema/table names from mlogID.
// 3. Executes `purge materialized view log on <schema>.<table>`.
// 4. Reads NEXT_PURGE_UNIX_SECONDS from mysql.tidb_mlog_purge_info.
func (*serviceHelper) PurgeMVLog(ctx context.Context, sysSessionPool basic.SessionPool, mlogID int64) (nextPurge time.Time, err error) {
	const (
		purgeMVLogSQL   = `PURGE MATERIALIZED VIEW LOG ON %n.%n`
		findNextTimeSQL = `SELECT NEXT_PURGE_UNIX_SECONDS FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? AND NEXT_PURGE_UNIX_SECONDS IS NOT NULL`
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
			zap.Int64("mvlog_id", mlogID),
			zap.String("base_table_schema", baseSchema),
			zap.String("base_table_name", baseTable),
			zap.Duration("elapsed", mvsSince(startAt)),
			zap.Error(err),
		)
	}()
	se, err := sysSessionPool.Get()
	if err != nil {
		logutil.BgLogger().Warn("get system session failed for mvlog purge", zap.Int64("mvlog_id", mlogID), zap.Error(err))
		return time.Time{}, err
	}
	defer sysSessionPool.Put(se)
	sctx := se.(sessionctx.Context)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMViewMaintenance)

	if mlogID <= 0 {
		return time.Time{}, errors.New("materialized view log id is invalid")
	}
	infoSchema := sctx.GetLatestISWithoutSessExt().(infoschema.InfoSchema)
	mLogTbl, ok := infoSchema.TableByID(ctx, mlogID)
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

	restoreMaintainSessionVars, err := applyMVMaintenanceSessionVarsFromGlobal(ctx, sctx.GetSessionVars())
	if err != nil {
		return time.Time{}, err
	}
	defer restoreMaintainSessionVars()

	if _, err = execRCRestrictedSQLWithSession(ctx, sctx, purgeMVLogSQL, []any{baseSchema, baseTable}); err != nil {
		if isMVTaskCanceledManually(err) {
			return time.Time{}, errMVTaskCanceledManually
		}
		return time.Time{}, err
	}

	rows, err := execRCRestrictedSQLWithSession(ctx, sctx, findNextTimeSQL, []any{mlogID})
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
	mviewID int64,
	nextRefresh time.Time,
) (bool, time.Time, error) {
	return tryBackoffMVTaskManualCancel(
		ctx,
		sysSessionPool,
		`SELECT NEXT_REFRESH_UNIX_SECONDS FROM mysql.tidb_mview_refresh_info WHERE MVIEW_ID = %? FOR UPDATE NOWAIT`,
		`UPDATE mysql.tidb_mview_refresh_info SET NEXT_REFRESH_UNIX_SECONDS = %? WHERE MVIEW_ID = %?`,
		mviewID,
		nextRefresh,
		deriveMVRefreshManualCancelNextTime,
	)
}

func (*serviceHelper) TryBackoffPurgeManualCancel(
	ctx context.Context,
	sysSessionPool basic.SessionPool,
	mlogID int64,
	nextPurge time.Time,
) (bool, time.Time, error) {
	return tryBackoffMVTaskManualCancel(
		ctx,
		sysSessionPool,
		`SELECT NEXT_PURGE_UNIX_SECONDS FROM mysql.tidb_mlog_purge_info WHERE MLOG_ID = %? FOR UPDATE NOWAIT`,
		`UPDATE mysql.tidb_mlog_purge_info SET NEXT_PURGE_UNIX_SECONDS = %? WHERE MLOG_ID = %?`,
		mlogID,
		nextPurge,
		deriveMLogPurgeManualCancelNextTime,
	)
}

type mviewTaskManualCancelNextResolver func(context.Context, sessionctx.Context, int64) (*time.Time, bool, error)

func deriveMVRefreshManualCancelNextTime(
	ctx context.Context,
	sctx sessionctx.Context,
	mviewID int64,
) (*time.Time, bool, error) {
	infoSchema := sctx.GetLatestISWithoutSessExt().(infoschema.InfoSchema)
	mviewTable, ok := infoSchema.TableByID(ctx, mviewID)
	if !ok {
		return nil, false, nil
	}
	mviewMeta := mviewTable.Meta()
	if mviewMeta == nil || mviewMeta.MaterializedView == nil {
		return nil, false, errors.New("materialized view metadata is invalid")
	}
	nextTime, shouldUpdate, err := deriveMaterializedScheduleNextTimeForManualCancel(
		ctx,
		sctx,
		mviewMeta.MaterializedView.RefreshNext,
		mviewMeta.MaterializedView.RefreshScheduleSQLMode,
	)
	if err != nil {
		return nil, false, err
	}
	if shouldUpdate && nextTime == nil {
		if dbInfo, ok := infoschema.SchemaByTable(infoSchema, mviewMeta); ok && dbInfo != nil {
			logManualCancelMaterializedViewRefreshNextTimeUpdateNull(dbInfo.Name.O, mviewMeta.Name.O, mviewMeta.MaterializedView.RefreshNext)
		} else {
			logManualCancelMaterializedViewRefreshNextTimeUpdateNull("", mviewMeta.Name.O, mviewMeta.MaterializedView.RefreshNext)
		}
	}
	return nextTime, shouldUpdate, nil
}

func deriveMLogPurgeManualCancelNextTime(
	ctx context.Context,
	sctx sessionctx.Context,
	mlogID int64,
) (*time.Time, bool, error) {
	infoSchema := sctx.GetLatestISWithoutSessExt().(infoschema.InfoSchema)
	mlogTbl, ok := infoSchema.TableByID(ctx, mlogID)
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
		mlogMeta.MaterializedViewLog.PurgeNext,
		mlogMeta.MaterializedViewLog.PurgeScheduleSQLMode,
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
	nextExpr string,
	scheduleSQLMode mysql.SQLMode,
) (*time.Time, bool, error) {
	nextAt, shouldUpdate, err := expression.DeriveMaterializedScheduleNextTime(
		ctx,
		sctx,
		nextExpr,
		scheduleSQLMode,
	)
	if err != nil {
		return nil, false, err
	}
	if !shouldUpdate || nextAt == nil {
		return nil, shouldUpdate, nil
	}
	nextUnixSeconds, err := expression.MaterializedScheduleTimeToUnixSeconds(nextAt)
	if err != nil {
		return nil, false, err
	}
	nextTime := mvsUnix(*nextUnixSeconds, 0)
	return &nextTime, true, nil
}

func logManualCancelMaterializedViewRefreshNextTimeUpdateNull(
	schemaName string,
	mviewName string,
	nextExpr string,
) {
	if strings.TrimSpace(nextExpr) == "" {
		return
	}
	logutil.BgLogger().Error(
		"refresh MV manual cancel backoff: automatic refresh schedule disabled because NEXT expression evaluated to NULL, updating NEXT_REFRESH_UNIX_SECONDS to NULL",
		zap.String("schemaName", schemaName),
		zap.String("tableName", mviewName),
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
		"purge MV log manual cancel backoff: automatic purge schedule disabled because NEXT expression evaluated to NULL, updating NEXT_PURGE_UNIX_SECONDS to NULL",
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
	resolveExpectedNext mviewTaskManualCancelNextResolver,
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

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMViewMaintenance)
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
	if rows[0].IsNull(0) {
		if _, err := sqlExec.ExecuteInternal(ctx, "COMMIT"); err != nil {
			return false, time.Time{}, err
		}
		committed = true
		return true, time.Time{}, nil
	}

	appliedNextUTC := nextTimeUTC
	var appliedNextArg any = nextTimeUTC.Unix()
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
				appliedNextArg = appliedNextUTC.Unix()
			}
		}
	} else {
		logutil.BgLogger().Warn(
			"derive materialized view task next schedule after manual cancel failed, fallback to cooldown",
			zap.Int64("object_id", objectID),
			zap.Time("cooldown_next_time_utc", nextTimeUTC),
			zap.Error(err),
		)
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

// LoadAllTiDBMVLogPurge loads all scheduled MV log purge tasks from metadata.
func (*serviceHelper) LoadAllTiDBMVLogPurge(ctx context.Context, sysSessionPool basic.SessionPool) (map[int64]*mlogPurgeTask, error) {
	const sql = `SELECT NEXT_PURGE_UNIX_SECONDS, MLOG_ID FROM mysql.tidb_mlog_purge_info WHERE NEXT_PURGE_UNIX_SECONDS IS NOT NULL`
	rows, err := execRCRestrictedSQLWithSessionPool(ctx, sysSessionPool, sql, nil)
	if err != nil {
		return nil, err
	}
	newPending := make(map[int64]*mlogPurgeTask, len(rows))
	for _, row := range rows {
		if row.IsNull(0) || row.IsNull(1) {
			continue
		}
		mlogID := row.GetInt64(1)
		if mlogID <= 0 {
			continue
		}
		nextPurge := mvsUnix(row.GetInt64(0), 0)
		l := &mlogPurgeTask{
			ID:        mlogID,
			nextPurge: nextPurge,
		}
		l.orderTs = l.nextPurge.UnixMilli()
		newPending[mlogID] = l
	}
	return newPending, nil
}

// LoadAllTiDBMVRefresh loads all scheduled MV refresh tasks from metadata.
func (*serviceHelper) LoadAllTiDBMVRefresh(ctx context.Context, sysSessionPool basic.SessionPool) (map[int64]*mviewTask, error) {
	const sql = `SELECT NEXT_REFRESH_UNIX_SECONDS, MVIEW_ID FROM mysql.tidb_mview_refresh_info WHERE NEXT_REFRESH_UNIX_SECONDS IS NOT NULL`
	se, err := sysSessionPool.Get()
	if err != nil {
		return nil, err
	}
	defer sysSessionPool.Put(se)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMViewMaintenance)
	sctx := se.(sessionctx.Context)
	rows, err := execRCRestrictedSQLWithSession(ctx, sctx, sql, nil)
	if err != nil {
		return nil, err
	}
	newPending := make(map[int64]*mviewTask, len(rows))
	for _, row := range rows {
		if row.IsNull(0) || row.IsNull(1) {
			continue
		}
		mviewID := row.GetInt64(1)
		if mviewID <= 0 {
			continue
		}
		nextRefresh := mvsUnix(row.GetInt64(0), 0)
		m := &mviewTask{
			ID:          mviewID,
			nextRefresh: nextRefresh,
			orderTs:     nextRefresh.UnixMilli(),
		}
		newPending[mviewID] = m
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
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMViewMaintenance)
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

// RegisterMVService constructs MVService and registers a local DDL handler for
// MV-related create/drop events.
// onDDLHandled is invoked on the DDL owner when such an event is observed, and
// callers may use it to fan out the metadata change to other TiDB nodes.
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
	mvService.NotifyDDLChange() // Trigger one startup refresh so in-memory state catches up with metadata.

	// The notifier callback runs on the DDL owner only.
	// Other nodes should be notified through the domain-side registry path.
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
		// For other DDL types, rely on the periodic metadata refresh.
		return false
	}
}

func hasMVRelatedTableInfo(tbl *meta.TableInfo) bool {
	if tbl == nil {
		return false
	}
	return tbl.MaterializedView != nil || tbl.MaterializedViewLog != nil
}
