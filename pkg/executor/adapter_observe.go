// Copyright 2018 PingCAP, Inc.
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

package executor

import (
	"context"
	"fmt"
	"runtime/trace"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/plugin"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/tikv/client-go/v2/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (a *ExecStmt) resetPhaseDurations() {
	a.phaseBuildDurations[1] += a.phaseBuildDurations[0]
	a.phaseBuildDurations[0] = 0
	a.phaseOpenDurations[1] += a.phaseOpenDurations[0]
	a.phaseOpenDurations[0] = 0
	a.phaseNextDurations[1] += a.phaseNextDurations[0]
	a.phaseNextDurations[0] = 0
	a.phaseLockDurations[1] += a.phaseLockDurations[0]
	a.phaseLockDurations[0] = 0
}

// QueryReplacer replaces new line and tab for grep result including query string.
var QueryReplacer = strings.NewReplacer("\r", " ", "\n", " ", "\t", " ")

func (a *ExecStmt) logAudit() {
	sessVars := a.Ctx.GetSessionVars()
	if sessVars.InRestrictedSQL {
		return
	}

	err := plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		audit := plugin.DeclareAuditManifest(p.Manifest)
		if audit.OnGeneralEvent != nil {
			cmdBin := byte(atomic.LoadUint32(&a.Ctx.GetSessionVars().CommandValue))
			cmd := mysql.Command2Str[cmdBin]
			ctx := context.WithValue(context.Background(), plugin.ExecStartTimeCtxKey, a.Ctx.GetSessionVars().StartTime)
			if execStmt, ok := a.StmtNode.(*ast.ExecuteStmt); ok {
				ctx = context.WithValue(ctx, plugin.PrepareStmtIDCtxKey, execStmt.PrepStmtId)
			}
			ctx = context.WithValue(ctx, plugin.IsRetryingCtxKey, a.retryCount > 0 || sessVars.RetryInfo.Retrying)
			if intest.InTest && (cmdBin == mysql.ComStmtPrepare ||
				cmdBin == mysql.ComStmtExecute || cmdBin == mysql.ComStmtClose) {
				intest.Assert(ctx.Value(plugin.PrepareStmtIDCtxKey) != nil, "prepare statement id should not be nil")
			}
			audit.OnGeneralEvent(ctx, sessVars, plugin.Completed, cmd)
		}
		return nil
	})
	if err != nil {
		log.Error("log audit log failure", zap.Error(err))
	}
}

// FormatSQL is used to format the original SQL, e.g. truncating long SQL, appending prepared arguments.
func FormatSQL(sql string) stringutil.StringerFunc {
	return func() string { return formatSQL(sql) }
}

func formatSQL(sql string) string {
	length := len(sql)
	maxQueryLen := vardef.QueryLogMaxLen.Load()
	if maxQueryLen <= 0 {
		return QueryReplacer.Replace(sql) // no limit
	}
	if int32(length) > maxQueryLen {
		var result strings.Builder
		result.Grow(int(maxQueryLen))
		result.WriteString(sql[:maxQueryLen])
		fmt.Fprintf(&result, "(len:%d)", length)
		return QueryReplacer.Replace(result.String())
	}
	return QueryReplacer.Replace(sql)
}

func getPhaseDurationObserver(phase string, internal bool) prometheus.Observer {
	if internal {
		if ob, found := executor_metrics.PhaseDurationObserverMapInternal[phase]; found {
			return ob
		}
		return executor_metrics.ExecUnknownInternal
	}
	if ob, found := executor_metrics.PhaseDurationObserverMap[phase]; found {
		return ob
	}
	return executor_metrics.ExecUnknown
}

func (a *ExecStmt) observePhaseDurations(internal bool, commitDetails *util.CommitDetails) {
	for _, it := range []struct {
		duration time.Duration
		phase    string
	}{
		{a.phaseBuildDurations[0], executor_metrics.PhaseBuildFinal},
		{a.phaseBuildDurations[1], executor_metrics.PhaseBuildLocking},
		{a.phaseOpenDurations[0], executor_metrics.PhaseOpenFinal},
		{a.phaseOpenDurations[1], executor_metrics.PhaseOpenLocking},
		{a.phaseNextDurations[0], executor_metrics.PhaseNextFinal},
		{a.phaseNextDurations[1], executor_metrics.PhaseNextLocking},
		{a.phaseLockDurations[0], executor_metrics.PhaseLockFinal},
		{a.phaseLockDurations[1], executor_metrics.PhaseLockLocking},
	} {
		if it.duration > 0 {
			getPhaseDurationObserver(it.phase, internal).Observe(it.duration.Seconds())
		}
	}
	if commitDetails != nil {
		for _, it := range []struct {
			duration time.Duration
			phase    string
		}{
			{commitDetails.PrewriteTime, executor_metrics.PhaseCommitPrewrite},
			{commitDetails.CommitTime, executor_metrics.PhaseCommitCommit},
			{commitDetails.GetCommitTsTime, executor_metrics.PhaseCommitWaitCommitTS},
			{commitDetails.GetLatestTsTime, executor_metrics.PhaseCommitWaitLatestTS},
			{commitDetails.LocalLatchTime, executor_metrics.PhaseCommitWaitLatch},
			{commitDetails.WaitPrewriteBinlogTime, executor_metrics.PhaseCommitWaitBinlog},
		} {
			if it.duration > 0 {
				getPhaseDurationObserver(it.phase, internal).Observe(it.duration.Seconds())
			}
		}
	}
	if stmtDetailsRaw := a.GoCtx.Value(execdetails.StmtExecDetailKey); stmtDetailsRaw != nil {
		d := stmtDetailsRaw.(*execdetails.StmtExecDetails).WriteSQLRespDuration
		if d > 0 {
			getPhaseDurationObserver(executor_metrics.PhaseWriteResponse, internal).Observe(d.Seconds())
		}
	}
}

// FinishExecuteStmt is used to record some information after `ExecStmt` execution finished:
// 1. record slow log if needed.
// 2. record summary statement.
// 3. record execute duration metric.
// 4. update the `PrevStmt` in session variable.
// 5. reset `DurationParse` in session variable.
func (a *ExecStmt) FinishExecuteStmt(txnTS uint64, err error, hasMoreResults bool) {
	a.checkPlanReplayerCapture(txnTS)

	sessVars := a.Ctx.GetSessionVars()
	execDetail := sessVars.StmtCtx.GetExecDetails()
	// Attach commit/lockKeys runtime stats to executor runtime stats.
	if (execDetail.CommitDetail != nil || execDetail.LockKeysDetail != nil || execDetail.SharedLockKeysDetail != nil) && sessVars.StmtCtx.RuntimeStatsColl != nil {
		statsWithCommit := &execdetails.RuntimeStatsWithCommit{
			Commit:         execDetail.CommitDetail,
			LockKeys:       execDetail.LockKeysDetail,
			SharedLockKeys: execDetail.SharedLockKeysDetail,
		}
		sessVars.StmtCtx.RuntimeStatsColl.RegisterStats(a.Plan.ID(), statsWithCommit)
	}
	// Record related SLI metrics.
	if execDetail.CommitDetail != nil && execDetail.CommitDetail.WriteSize > 0 {
		a.Ctx.GetTxnWriteThroughputSLI().AddTxnWriteSize(execDetail.CommitDetail.WriteSize, execDetail.CommitDetail.WriteKeys)
	}
	if execDetail.ScanDetail != nil && sessVars.StmtCtx.AffectedRows() > 0 {
		processedKeys := atomic.LoadInt64(&execDetail.ScanDetail.ProcessedKeys)
		if processedKeys > 0 {
			// Only record the read keys in write statement which affect row more than 0.
			a.Ctx.GetTxnWriteThroughputSLI().AddReadKeys(processedKeys)
		}
	}
	succ := err == nil
	if a.Plan != nil {
		// If this statement has a Plan, the StmtCtx.plan should have been set when it comes here,
		// but we set it again in case we missed some code paths.
		sessVars.StmtCtx.SetPlan(a.Plan)
	}

	a.updateNetworkTrafficStatsAndMetrics()
	// `LowSlowQuery` and `SummaryStmt` must be called before recording `PrevStmt`.
	a.LogSlowQuery(txnTS, succ, hasMoreResults)
	a.SummaryStmt(succ)
	a.observeStmtFinishedForTopProfiling()
	a.UpdatePlanCacheRuntimeInfo()
	if sessVars.StmtCtx.IsTiFlash.Load() {
		if succ {
			executor_metrics.TotalTiFlashQuerySuccCounter.Inc()
		} else {
			metrics.TiFlashQueryTotalCounter.WithLabelValues(metrics.ExecuteErrorToLabel(err), metrics.LblError).Inc()
		}
	}
	a.updatePrevStmt()
	a.recordLastQueryInfo(err)
	a.recordAffectedRows2Metrics()
	a.observePhaseDurations(sessVars.InRestrictedSQL, execDetail.CommitDetail)
	executeDuration := sessVars.GetExecuteDuration()
	if sessVars.InRestrictedSQL {
		executor_metrics.SessionExecuteRunDurationInternal.Observe(executeDuration.Seconds())
	} else {
		executor_metrics.SessionExecuteRunDurationGeneral.Observe(executeDuration.Seconds())
	}
	// Reset DurationParse due to the next statement may not need to be parsed (not a text protocol query).
	sessVars.DurationParse = 0
	// Clean the stale read flag when statement execution finish
	sessVars.StmtCtx.IsStaleness = false
	// Clean the MPP query info
	sessVars.StmtCtx.MPPQueryInfo.QueryID.Store(0)
	sessVars.StmtCtx.MPPQueryInfo.QueryTS.Store(0)
	sessVars.StmtCtx.MPPQueryInfo.AllocatedMPPTaskID.Store(0)
	sessVars.StmtCtx.MPPQueryInfo.AllocatedMPPGatherID.Store(0)

	if sessVars.StmtCtx.ReadFromTableCache {
		metrics.ReadFromTableCacheCounter.Inc()
	}

	// Update fair locking related counters by stmt
	if execDetail.LockKeysDetail != nil {
		if execDetail.LockKeysDetail.AggressiveLockNewCount > 0 || execDetail.LockKeysDetail.AggressiveLockDerivedCount > 0 {
			executor_metrics.FairLockingStmtUsedCount.Inc()
			// If this statement is finished when some of the keys are locked with conflict in the last retry, or
			// some of the keys are derived from the previous retry, we consider the optimization of fair locking
			// takes effect on this statement.
			if execDetail.LockKeysDetail.LockedWithConflictCount > 0 || execDetail.LockKeysDetail.AggressiveLockDerivedCount > 0 {
				executor_metrics.FairLockingStmtEffectiveCount.Inc()
			}
		}
	}
	// If the transaction is committed, update fair locking related counters by txn
	if execDetail.CommitDetail != nil {
		if sessVars.TxnCtx.FairLockingUsed {
			executor_metrics.FairLockingTxnUsedCount.Inc()
		}
		if sessVars.TxnCtx.FairLockingEffective {
			executor_metrics.FairLockingTxnEffectiveCount.Inc()
		}
	}

	a.Ctx.ReportUsageStats()
}

func (a *ExecStmt) recordAffectedRows2Metrics() {
	sessVars := a.Ctx.GetSessionVars()
	if affectedRows := sessVars.StmtCtx.AffectedRows(); affectedRows > 0 {
		switch sessVars.StmtCtx.StmtType {
		case "Insert":
			metrics.AffectedRowsCounterInsert.Add(float64(affectedRows))
		case "Replace":
			metrics.AffectedRowsCounterReplace.Add(float64(affectedRows))
		case "Delete":
			metrics.AffectedRowsCounterDelete.Add(float64(affectedRows))
		case "Update":
			metrics.AffectedRowsCounterUpdate.Add(float64(affectedRows))
		case "NTDML-Delete":
			metrics.AffectedRowsCounterNTDMLDelete.Add(float64(affectedRows))
		case "NTDML-Update":
			metrics.AffectedRowsCounterNTDMLUpdate.Add(float64(affectedRows))
		case "NTDML-Insert":
			metrics.AffectedRowsCounterNTDMLInsert.Add(float64(affectedRows))
		case "NTDML-Replace":
			metrics.AffectedRowsCounterNTDMLReplace.Add(float64(affectedRows))
		}
	}
}

func (a *ExecStmt) recordLastQueryInfo(err error) {
	sessVars := a.Ctx.GetSessionVars()
	// Record diagnostic information for DML statements
	recordLastQuery := false
	switch typ := a.StmtNode.(type) {
	case *ast.ShowStmt:
		recordLastQuery = typ.Tp != ast.ShowSessionStates
	case *ast.ExecuteStmt, ast.DMLNode:
		recordLastQuery = true
	}
	if recordLastQuery {
		var lastRUConsumption float64
		if ruDetailRaw := a.GoCtx.Value(util.RUDetailsCtxKey); ruDetailRaw != nil {
			ruDetail := ruDetailRaw.(*util.RUDetails)
			lastRUConsumption = ruDetail.RRU() + ruDetail.WRU()
		}
		failpoint.Inject("mockRUConsumption", func(_ failpoint.Value) {
			lastRUConsumption = float64(len(sessVars.StmtCtx.OriginalSQL))
		})
		// Keep the previous queryInfo for `show session_states` because the statement needs to encode it.
		sessVars.LastQueryInfo = sessionstates.QueryInfo{
			TxnScope:      sessVars.CheckAndGetTxnScope(),
			StartTS:       sessVars.TxnCtx.StartTS,
			ForUpdateTS:   sessVars.TxnCtx.GetForUpdateTS(),
			RUConsumption: lastRUConsumption,
		}
		if err != nil {
			sessVars.LastQueryInfo.ErrMsg = err.Error()
		}
	}
}

func (a *ExecStmt) checkPlanReplayerCapture(txnTS uint64) {
	if kv.GetInternalSourceType(a.GoCtx) == kv.InternalTxnStats {
		return
	}
	se := a.Ctx
	if !se.GetSessionVars().InRestrictedSQL && se.GetSessionVars().IsPlanReplayerCaptureEnabled() {
		stmtNode := a.GetStmtNode()
		if se.GetSessionVars().EnablePlanReplayedContinuesCapture {
			if checkPlanReplayerContinuesCaptureValidStmt(stmtNode) {
				checkPlanReplayerContinuesCapture(se, stmtNode, txnTS)
			}
		} else {
			checkPlanReplayerCaptureTask(se, stmtNode, txnTS)
		}
	}
}

// CloseRecordSet will finish the execution of current statement and do some record work
func (a *ExecStmt) CloseRecordSet(txnStartTS uint64, lastErr error) {
	a.FinishExecuteStmt(txnStartTS, lastErr, false)
	a.logAudit()
	a.Ctx.GetSessionVars().StmtCtx.DetachMemDiskTracker()
}

// Clean CTE storage shared by different CTEFullScan executor within a SQL stmt.
// Will return err in two situations:
// 1. Got err when remove disk spill file.
// 2. Some logical error like ref count of CTEStorage is less than 0.
func resetCTEStorageMap(se sessionctx.Context) error {
	tmp := se.GetSessionVars().StmtCtx.CTEStorageMap
	if tmp == nil {
		// Close() is already called, so no need to reset. Such as TraceExec.
		return nil
	}
	storageMap, ok := tmp.(map[int]*CTEStorages)
	if !ok {
		return errors.New("type assertion for CTEStorageMap failed")
	}
	for _, v := range storageMap {
		v.ResTbl.Lock()
		err1 := v.ResTbl.DerefAndClose()
		// Make sure we do not hold the lock for longer than necessary.
		v.ResTbl.Unlock()
		// No need to lock IterInTbl.
		err2 := v.IterInTbl.DerefAndClose()
		if err1 != nil {
			return err1
		}
		if err2 != nil {
			return err2
		}
	}
	se.GetSessionVars().StmtCtx.CTEStorageMap = nil
	return nil
}

func slowQueryDumpTriggerCheck(config *traceevent.DumpTriggerConfig) bool {
	return config.Event.Type == "slow_query"
}

// LogSlowQuery is used to print the slow query in the log files.
func (a *ExecStmt) LogSlowQuery(txnTS uint64, succ bool, hasMoreResults bool) {
	sessVars := a.Ctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	cfg := config.GetGlobalConfig()
	var slowItems *variable.SlowQueryLogItems
	var matchRules bool
	if !stmtCtx.WriteSlowLog {
		// If the level is Debug, or trace is enabled, print slow logs anyway.
		force := log.GetLevel() <= zapcore.DebugLevel || trace.IsEnabled()
		if !cfg.Instance.EnableSlowLog.Load() && !force {
			return
		}

		sessVars.StmtCtx.ExecSuccess = succ
		sessVars.StmtCtx.ExecRetryCount = uint64(a.retryCount)
		globalRules := vardef.GlobalSlowLogRules.Load()
		slowItems = PrepareSlowLogItemsForRules(a.GoCtx, globalRules, sessVars)
		// EffectiveFields is not empty (unique fields for this session including global rules),
		// so we use these rules to decide whether to write the slow log.
		if len(sessVars.SlowLogRules.EffectiveFields) != 0 {
			matchRules = ShouldWriteSlowLog(globalRules, sessVars, slowItems)
			defer putSlowLogItems(slowItems)
		} else {
			threshold := time.Duration(atomic.LoadUint64(&cfg.Instance.SlowThreshold)) * time.Millisecond
			matchRules = sessVars.GetTotalCostDuration() >= threshold
		}
		if !matchRules && !force {
			return
		}
	}

	if !vardef.GlobalSlowLogRateLimiter.Allow() {
		sampleLoggerFactory().Info("slow log skipped due to rate limiting", zap.Int64("tidb_slow_log_max_per_sec", int64(vardef.GlobalSlowLogRateLimiter.Limit())))
		return
	}

	if slowItems == nil {
		slowItems = &variable.SlowQueryLogItems{}
	}
	SetSlowLogItems(a, txnTS, hasMoreResults, slowItems)
	failpoint.Inject("assertSyncStatsFailed", func(val failpoint.Value) {
		if val.(bool) {
			if !slowItems.IsSyncStatsFailed {
				panic("isSyncStatsFailed should be true")
			}
		}
	})
	slowLog := sessVars.SlowLogFormat(slowItems)
	logutil.SlowQueryLogger.Warn(slowLog)

	if trace.IsEnabled() {
		trace.Log(a.GoCtx, "details", slowLog)
	}
	traceevent.CheckFlightRecorderDumpTrigger(a.GoCtx, "dump_trigger.suspicious_event", slowQueryDumpTriggerCheck)

	if !matchRules {
		return
	}
	costTime := slowItems.TimeTotal
	execDetail := slowItems.ExecDetail
	if sessVars.InRestrictedSQL {
		executor_metrics.TotalQueryProcHistogramInternal.Observe(costTime.Seconds())
		executor_metrics.TotalCopProcHistogramInternal.Observe(execDetail.TimeDetail.ProcessTime.Seconds())
		executor_metrics.TotalCopWaitHistogramInternal.Observe(execDetail.TimeDetail.WaitTime.Seconds())
	} else {
		executor_metrics.TotalQueryProcHistogramGeneral.Observe(costTime.Seconds())
		executor_metrics.TotalCopProcHistogramGeneral.Observe(execDetail.TimeDetail.ProcessTime.Seconds())
		executor_metrics.TotalCopWaitHistogramGeneral.Observe(execDetail.TimeDetail.WaitTime.Seconds())
		if execDetail.ScanDetail != nil && execDetail.ScanDetail.ProcessedKeys != 0 {
			executor_metrics.CopMVCCRatioHistogramGeneral.Observe(float64(execDetail.ScanDetail.TotalKeys) / float64(execDetail.ScanDetail.ProcessedKeys))
		}
	}
	var userString string
	if sessVars.User != nil {
		userString = sessVars.User.String()
	}
	var tableIDs string
	if len(sessVars.StmtCtx.TableIDs) > 0 {
		tableIDs = strings.ReplaceAll(fmt.Sprintf("%v", sessVars.StmtCtx.TableIDs), " ", ",")
	}
	// TODO log slow query for cross keyspace query?
	dom := domain.GetDomain(a.Ctx)
	if dom != nil {
		dom.LogSlowQuery(&domain.SlowQueryInfo{
			SQL:        slowItems.SQL,
			Digest:     slowItems.Digest,
			Start:      sessVars.StartTime,
			Duration:   costTime,
			Detail:     *execDetail,
			Succ:       succ,
			ConnID:     sessVars.ConnectionID,
			SessAlias:  sessVars.SessionAlias,
			TxnTS:      txnTS,
			User:       userString,
			DB:         sessVars.CurrentDB,
			TableIDs:   tableIDs,
			IndexNames: slowItems.IndexNames,
			Internal:   sessVars.InRestrictedSQL,
		})
	}
}

func (a *ExecStmt) updateNetworkTrafficStatsAndMetrics() {
	hasMPPTraffic := a.updateMPPNetworkTraffic()
	tikvExecDetailRaw := a.GoCtx.Value(util.ExecDetailsKey)
	if tikvExecDetailRaw != nil {
		tikvExecDetail := tikvExecDetailRaw.(*util.ExecDetails)
		executor_metrics.ExecutorNetworkTransmissionSentTiKVTotal.Add(float64(tikvExecDetail.UnpackedBytesSentKVTotal))
		executor_metrics.ExecutorNetworkTransmissionSentTiKVCrossZone.Add(float64(tikvExecDetail.UnpackedBytesSentKVCrossZone))
		executor_metrics.ExecutorNetworkTransmissionReceivedTiKVTotal.Add(float64(tikvExecDetail.UnpackedBytesReceivedKVTotal))
		executor_metrics.ExecutorNetworkTransmissionReceivedTiKVCrossZone.Add(float64(tikvExecDetail.UnpackedBytesReceivedKVCrossZone))
		if hasMPPTraffic {
			executor_metrics.ExecutorNetworkTransmissionSentTiFlashTotal.Add(float64(tikvExecDetail.UnpackedBytesSentMPPTotal))
			executor_metrics.ExecutorNetworkTransmissionSentTiFlashCrossZone.Add(float64(tikvExecDetail.UnpackedBytesSentMPPCrossZone))
			executor_metrics.ExecutorNetworkTransmissionReceivedTiFlashTotal.Add(float64(tikvExecDetail.UnpackedBytesReceivedMPPTotal))
			executor_metrics.ExecutorNetworkTransmissionReceivedTiFlashCrossZone.Add(float64(tikvExecDetail.UnpackedBytesReceivedMPPCrossZone))
		}
	}
}

func (a *ExecStmt) updateMPPNetworkTraffic() bool {
	sessVars := a.Ctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	runtimeStatsColl := stmtCtx.RuntimeStatsColl
	if runtimeStatsColl == nil {
		return false
	}
	tiflashNetworkStats := runtimeStatsColl.GetStmtCopRuntimeStats().TiflashNetworkStats
	if tiflashNetworkStats == nil {
		return false
	}
	tikvExecDetailRaw := a.GoCtx.Value(util.ExecDetailsKey)
	if tikvExecDetailRaw == nil {
		tikvExecDetailRaw = &util.ExecDetails{}
		a.GoCtx = context.WithValue(a.GoCtx, util.ExecDetailsKey, tikvExecDetailRaw)
	}

	tikvExecDetail := tikvExecDetailRaw.(*util.ExecDetails)
	tiflashNetworkStats.UpdateTiKVExecDetails(tikvExecDetail)
	return true
}

// getFlatPlan generates a FlatPhysicalPlan from the plan stored in stmtCtx.plan,
// then stores it in stmtCtx.flatPlan.
func getFlatPlan(stmtCtx *stmtctx.StatementContext) *plannercore.FlatPhysicalPlan {
	pp := stmtCtx.GetPlan()
	if pp == nil {
		return nil
	}
	if flat := stmtCtx.GetFlatPlan(); flat != nil {
		f := flat.(*plannercore.FlatPhysicalPlan)
		return f
	}
	p := pp.(base.Plan)
	flat := plannercore.FlattenPhysicalPlan(p, false)
	if flat != nil {
		stmtCtx.SetFlatPlan(flat)
		return flat
	}
	return nil
}

func getBinaryPlan(sCtx sessionctx.Context) string {
	stmtCtx := sCtx.GetSessionVars().StmtCtx
	binaryPlan := stmtCtx.GetBinaryPlan()
	if len(binaryPlan) > 0 {
		return binaryPlan
	}
	flat := getFlatPlan(stmtCtx)
	binaryPlan = plannercore.BinaryPlanStrFromFlatPlan(sCtx.GetPlanCtx(), flat, false)
	stmtCtx.SetBinaryPlan(binaryPlan)
	return binaryPlan
}

// getPlanTree will try to get the select plan tree if the plan is select or the select plan of delete/update/insert statement.
func getPlanTree(stmtCtx *stmtctx.StatementContext) string {
	cfg := config.GetGlobalConfig()
	if atomic.LoadUint32(&cfg.Instance.RecordPlanInSlowLog) == 0 {
		return ""
	}
	planTree, _ := getEncodedPlan(stmtCtx, false)
	if len(planTree) == 0 {
		return planTree
	}
	return variable.SlowLogPlanPrefix + planTree + variable.SlowLogPlanSuffix
}

// GetPlanDigest will try to get the select plan tree if the plan is select or the select plan of delete/update/insert statement.
func GetPlanDigest(stmtCtx *stmtctx.StatementContext) (string, *parser.Digest) {
	normalized, planDigest := stmtCtx.GetPlanDigest()
	if len(normalized) > 0 && planDigest != nil {
		return normalized, planDigest
	}
	flat := getFlatPlan(stmtCtx)
	normalized, planDigest = plannercore.NormalizeFlatPlan(flat)
	stmtCtx.SetPlanDigest(normalized, planDigest)
	return normalized, planDigest
}

// getEncodedPlan gets the encoded plan, and generates the hint string if indicated.
func getEncodedPlan(stmtCtx *stmtctx.StatementContext, genHint bool) (encodedPlan, hintStr string) {
	var hintSet bool
	encodedPlan = stmtCtx.GetEncodedPlan()
	hintStr, hintSet = stmtCtx.GetPlanHint()
	if len(encodedPlan) > 0 && (!genHint || hintSet) {
		return
	}
	flat := getFlatPlan(stmtCtx)
	if len(encodedPlan) == 0 {
		encodedPlan = plannercore.EncodeFlatPlan(flat)
		stmtCtx.SetEncodedPlan(encodedPlan)
	}
	if genHint {
		hints := plannercore.GenHintsFromFlatPlan(flat)
		for _, tableHint := range stmtCtx.OriginalTableHints {
			// some hints like 'memory_quota' cannot be extracted from the PhysicalPlan directly,
			// so we have to iterate all hints from the customer and keep some other necessary hints.
			switch tableHint.HintName.L {
			case hint.HintMemoryQuota, hint.HintUseToja, hint.HintNoIndexMerge,
				hint.HintMaxExecutionTime, hint.HintIgnoreIndex, hint.HintReadFromStorage,
				hint.HintMerge, hint.HintSemiJoinRewrite, hint.HintNoDecorrelate:
				hints = append(hints, tableHint)
			}
		}

		hintStr = hint.RestoreOptimizerHints(hints)
		stmtCtx.SetPlanHint(hintStr)
	}
	return
}
