// Copyright 2024 PingCAP, Inc.
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
	"bytes"
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
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/plugin"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	stmtsummaryv2 "github.com/pingcap/tidb/pkg/util/stmtsummary/v2"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ExecStmtRuntime controls the execute and close of a statement.
type ExecStmtRuntime struct {
	// GoCtx stores parent go context.Context for a stmt.
	GoCtx context.Context
	// InfoSchema stores a reference to the schema information.
	InfoSchema infoschema.InfoSchema
	// Plan stores a reference to the final physical plan.
	Plan base.Plan
	// Text represents the origin query text.
	Text string

	StmtNode ast.StmtNode

	Ctx sessionctx.Context

	isPreparedStmt bool
	retryCount     uint
	retryStartTime time.Time

	// Phase durations are splited into two parts: 1. trying to lock keys (but
	// failed); 2. the final iteration of the retry loop. Here we use
	// [2]time.Duration to record such info for each phase. The first duration
	// is increased only within the current iteration. When we meet a
	// pessimistic lock error and decide to retry, we add the first duration to
	// the second and reset the first to 0 by calling `resetPhaseDurations`.
	phaseBuildDurations [2]time.Duration
	phaseOpenDurations  [2]time.Duration
	phaseNextDurations  [2]time.Duration
	phaseLockDurations  [2]time.Duration

	// OutputNames will be set if using cached plan
	OutputNames []*types.FieldName
}

// GetStmtNode returns the stmtNode inside Statement
func (a *ExecStmtRuntime) GetStmtNode() ast.StmtNode {
	return a.StmtNode
}

func (a *ExecStmtRuntime) next(ctx context.Context, e exec.Executor, req *chunk.Chunk) error {
	start := time.Now()
	err := exec.Next(ctx, e, req)
	a.phaseNextDurations[0] += time.Since(start)
	return err
}

func (a *ExecStmtRuntime) logAudit() {
	sessVars := a.Ctx.GetSessionVars()
	if sessVars.InRestrictedSQL {
		return
	}

	err := plugin.ForeachPlugin(plugin.Audit, func(p *plugin.Plugin) error {
		audit := plugin.DeclareAuditManifest(p.Manifest)
		if audit.OnGeneralEvent != nil {
			cmd := mysql.Command2Str[byte(atomic.LoadUint32(&a.Ctx.GetSessionVars().CommandValue))]
			ctx := context.WithValue(context.Background(), plugin.ExecStartTimeCtxKey, a.Ctx.GetSessionVars().StmtCtx.StartTime)
			audit.OnGeneralEvent(ctx, sessVars, plugin.Completed, cmd)
		}
		return nil
	})
	if err != nil {
		log.Error("log audit log failure", zap.Error(err))
	}
}

func (a *ExecStmtRuntime) observePhaseDurations(internal bool, commitDetails *util.CommitDetails) {
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

// FinishExecuteStmt is used to record some information after `ExecStmtRuntime` execution finished:
// 1. record slow log if needed.
// 2. record summary statement.
// 3. record execute duration metric.
// 4. update the `PrevStmt` in session variable.
// 5. reset `DurationParse` in session variable.
func (a *ExecStmtRuntime) FinishExecuteStmt(txnTS uint64, err error, hasMoreResults bool) {
	a.checkPlanReplayerCapture(txnTS)

	sessVars := a.Ctx.GetSessionVars()
	execDetail := sessVars.StmtCtx.GetExecDetails()
	// Attach commit/lockKeys runtime stats to executor runtime stats.
	if (execDetail.CommitDetail != nil || execDetail.LockKeysDetail != nil) && sessVars.StmtCtx.RuntimeStatsColl != nil {
		statsWithCommit := &execdetails.RuntimeStatsWithCommit{
			Commit:   execDetail.CommitDetail,
			LockKeys: execDetail.LockKeysDetail,
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
	// `LowSlowQuery` and `SummaryStmt` must be called before recording `PrevStmt`.
	a.LogSlowQuery(txnTS, succ, hasMoreResults)
	a.SummaryStmt(succ)
	a.observeStmtFinishedForTopSQL()
	if sessVars.StmtCtx.IsTiFlash.Load() {
		if succ {
			executor_metrics.TotalTiFlashQuerySuccCounter.Inc()
		} else {
			metrics.TiFlashQueryTotalCounter.WithLabelValues(metrics.ExecuteErrorToLabel(err), metrics.LblError).Inc()
		}
	}
	sessVars.PrevStmt = FormatSQL(a.GetTextToLog(false))
	a.recordLastQueryInfo(err)
	a.observePhaseDurations(sessVars.InRestrictedSQL, execDetail.CommitDetail)
	executeDuration := time.Since(sessVars.StmtCtx.StartTime) - sessVars.StmtCtx.DurationCompile
	if sessVars.InRestrictedSQL {
		executor_metrics.SessionExecuteRunDurationInternal.Observe(executeDuration.Seconds())
	} else {
		executor_metrics.SessionExecuteRunDurationGeneral.Observe(executeDuration.Seconds())
	}

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

func (a *ExecStmtRuntime) recordLastQueryInfo(err error) {
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

func (a *ExecStmtRuntime) checkPlanReplayerCapture(txnTS uint64) {
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
func (a *ExecStmtRuntime) CloseRecordSet(txnStartTS uint64, lastErr error) {
	a.FinishExecuteStmt(txnStartTS, lastErr, false)
	a.logAudit()
	a.Ctx.GetSessionVars().StmtCtx.DetachMemDiskTracker()
}

// LogSlowQuery is used to print the slow query in the log files.
func (a *ExecStmtRuntime) LogSlowQuery(txnTS uint64, succ bool, hasMoreResults bool) {
	sessVars := a.Ctx.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	level := log.GetLevel()
	cfg := config.GetGlobalConfig()
	costTime := time.Since(sessVars.StmtCtx.StartTime) + sessVars.StmtCtx.DurationParse
	threshold := time.Duration(atomic.LoadUint64(&cfg.Instance.SlowThreshold)) * time.Millisecond
	enable := cfg.Instance.EnableSlowLog.Load()
	// if the level is Debug, or trace is enabled, print slow logs anyway
	force := level <= zapcore.DebugLevel || trace.IsEnabled()
	if (!enable || costTime < threshold) && !force {
		return
	}
	sql := FormatSQL(a.GetTextToLog(true))
	_, digest := stmtCtx.SQLDigest()

	var indexNames string
	if len(stmtCtx.IndexNames) > 0 {
		// remove duplicate index.
		idxMap := make(map[string]struct{})
		buf := bytes.NewBuffer(make([]byte, 0, 4))
		buf.WriteByte('[')
		for _, idx := range stmtCtx.IndexNames {
			_, ok := idxMap[idx]
			if ok {
				continue
			}
			idxMap[idx] = struct{}{}
			if buf.Len() > 1 {
				buf.WriteByte(',')
			}
			buf.WriteString(idx)
		}
		buf.WriteByte(']')
		indexNames = buf.String()
	}
	var stmtDetail execdetails.StmtExecDetails
	stmtDetailRaw := a.GoCtx.Value(execdetails.StmtExecDetailKey)
	if stmtDetailRaw != nil {
		stmtDetail = *(stmtDetailRaw.(*execdetails.StmtExecDetails))
	}
	var tikvExecDetail util.ExecDetails
	tikvExecDetailRaw := a.GoCtx.Value(util.ExecDetailsKey)
	if tikvExecDetailRaw != nil {
		tikvExecDetail = *(tikvExecDetailRaw.(*util.ExecDetails))
	}
	ruDetails := util.NewRUDetails()
	if ruDetailsVal := a.GoCtx.Value(util.RUDetailsCtxKey); ruDetailsVal != nil {
		ruDetails = ruDetailsVal.(*util.RUDetails)
	}

	execDetail := stmtCtx.GetExecDetails()
	copTaskInfo := stmtCtx.CopTasksDetails()
	memMax := sessVars.MemTracker.MaxConsumed()
	diskMax := sessVars.DiskTracker.MaxConsumed()
	_, planDigest := GetPlanDigest(stmtCtx)

	binaryPlan := ""
	if variable.GenerateBinaryPlan.Load() {
		binaryPlan = getBinaryPlan(a.Ctx)
		if len(binaryPlan) > 0 {
			binaryPlan = variable.SlowLogBinaryPlanPrefix + binaryPlan + variable.SlowLogPlanSuffix
		}
	}

	resultRows := GetResultRowsCount(stmtCtx, a.Plan)

	var (
		keyspaceName string
		keyspaceID   uint32
	)
	keyspaceName = keyspace.GetKeyspaceNameBySettings()
	if !keyspace.IsKeyspaceNameEmpty(keyspaceName) {
		keyspaceID = uint32(a.Ctx.GetStore().GetCodec().GetKeyspaceID())
	}
	if txnTS == 0 {
		// TODO: txnTS maybe ambiguous, consider logging stale-read-ts with a new field in the slow log.
		txnTS = sessVars.TxnCtx.StaleReadTs
	}

	slowItems := &variable.SlowQueryLogItems{
		TxnTS:             txnTS,
		KeyspaceName:      keyspaceName,
		KeyspaceID:        keyspaceID,
		SQL:               sql.String(),
		Digest:            digest.String(),
		TimeTotal:         costTime,
		TimeParse:         sessVars.StmtCtx.DurationParse,
		TimeCompile:       sessVars.StmtCtx.DurationCompile,
		TimeOptimize:      sessVars.StmtCtx.DurationOptimization,
		TimeWaitTS:        sessVars.StmtCtx.DurationWaitTS,
		IndexNames:        indexNames,
		CopTasks:          copTaskInfo,
		ExecDetail:        execDetail,
		MemMax:            memMax,
		DiskMax:           diskMax,
		Succ:              succ,
		Plan:              getPlanTree(stmtCtx),
		PlanDigest:        planDigest.String(),
		BinaryPlan:        binaryPlan,
		Prepared:          a.isPreparedStmt,
		HasMoreResults:    hasMoreResults,
		PlanFromCache:     sessVars.FoundInPlanCache,
		PlanFromBinding:   sessVars.FoundInBinding,
		RewriteInfo:       sessVars.RewritePhaseInfo,
		KVTotal:           time.Duration(atomic.LoadInt64(&tikvExecDetail.WaitKVRespDuration)),
		PDTotal:           time.Duration(atomic.LoadInt64(&tikvExecDetail.WaitPDRespDuration)),
		BackoffTotal:      time.Duration(atomic.LoadInt64(&tikvExecDetail.BackoffDuration)),
		WriteSQLRespTotal: stmtDetail.WriteSQLRespDuration,
		ResultRows:        resultRows,
		ExecRetryCount:    a.retryCount,
		IsExplicitTxn:     sessVars.TxnCtx.IsExplicit,
		IsWriteCacheTable: stmtCtx.WaitLockLeaseTime > 0,
		UsedStats:         stmtCtx.GetUsedStatsInfo(false),
		IsSyncStatsFailed: stmtCtx.IsSyncStatsFailed,
		Warnings:          collectWarningsForSlowLog(stmtCtx),
		ResourceGroupName: sessVars.StmtCtx.ResourceGroupName,
		RRU:               ruDetails.RRU(),
		WRU:               ruDetails.WRU(),
		WaitRUDuration:    ruDetails.RUWaitDuration(),
	}
	failpoint.Inject("assertSyncStatsFailed", func(val failpoint.Value) {
		if val.(bool) {
			if !slowItems.IsSyncStatsFailed {
				panic("isSyncStatsFailed should be true")
			}
		}
	})
	if a.retryCount > 0 {
		slowItems.ExecRetryTime = costTime - sessVars.StmtCtx.DurationParse - sessVars.StmtCtx.DurationCompile - time.Since(a.retryStartTime)
	}
	if _, ok := a.StmtNode.(*ast.CommitStmt); ok && sessVars.PrevStmt != nil {
		slowItems.PrevStmt = sessVars.PrevStmt.String()
	}
	slowLog := sessVars.SlowLogFormat(slowItems)
	if trace.IsEnabled() {
		trace.Log(a.GoCtx, "details", slowLog)
	}
	logutil.SlowQueryLogger.Warn(slowLog)
	if costTime >= threshold {
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
		if len(stmtCtx.TableIDs) > 0 {
			tableIDs = strings.ReplaceAll(fmt.Sprintf("%v", stmtCtx.TableIDs), " ", ",")
		}
		domain.GetDomain(a.Ctx).LogSlowQuery(&domain.SlowQueryInfo{
			SQL:        sql.String(),
			Digest:     digest.String(),
			Start:      sessVars.StmtCtx.StartTime,
			Duration:   costTime,
			Detail:     stmtCtx.GetExecDetails(),
			Succ:       succ,
			ConnID:     sessVars.ConnectionID,
			SessAlias:  sessVars.SessionAlias,
			TxnTS:      txnTS,
			User:       userString,
			DB:         sessVars.CurrentDB,
			TableIDs:   tableIDs,
			IndexNames: indexNames,
			Internal:   sessVars.InRestrictedSQL,
		})
	}
}

// SummaryStmt collects statements for information_schema.statements_summary
func (a *ExecStmtRuntime) SummaryStmt(succ bool) {
	sessVars := a.Ctx.GetSessionVars()
	var userString string
	if sessVars.User != nil {
		userString = sessVars.User.Username
	}

	// Internal SQLs must also be recorded to keep the consistency of `PrevStmt` and `PrevStmtDigest`.
	if !stmtsummaryv2.Enabled() || ((sessVars.InRestrictedSQL || len(userString) == 0) && !stmtsummaryv2.EnabledInternal()) {
		sessVars.SetPrevStmtDigest("")
		return
	}
	// Ignore `PREPARE` statements, but record `EXECUTE` statements.
	if _, ok := a.StmtNode.(*ast.PrepareStmt); ok {
		return
	}
	stmtCtx := sessVars.StmtCtx
	// Make sure StmtType is filled even if succ is false.
	if stmtCtx.StmtType == "" {
		stmtCtx.StmtType = ast.GetStmtLabel(a.StmtNode)
	}
	normalizedSQL, digest := stmtCtx.SQLDigest()
	costTime := time.Since(sessVars.StmtCtx.StartTime) + sessVars.StmtCtx.DurationParse
	charset, collation := sessVars.GetCharsetInfo()

	var prevSQL, prevSQLDigest string
	if _, ok := a.StmtNode.(*ast.CommitStmt); ok {
		// If prevSQLDigest is not recorded, it means this `commit` is the first SQL once stmt summary is enabled,
		// so it's OK just to ignore it.
		if prevSQLDigest = sessVars.GetPrevStmtDigest(); len(prevSQLDigest) == 0 {
			return
		}
		prevSQL = sessVars.PrevStmt.String()
	}
	sessVars.SetPrevStmtDigest(digest.String())

	// No need to encode every time, so encode lazily.
	planGenerator := func() (p string, h string, e any) {
		defer func() {
			e = recover()
			if e != nil {
				logutil.BgLogger().Warn("fail to generate plan info",
					zap.Stack("backtrace"),
					zap.Any("error", e))
			}
		}()
		p, h = getEncodedPlan(stmtCtx, !sessVars.InRestrictedSQL)
		return
	}
	var binPlanGen func() string
	if variable.GenerateBinaryPlan.Load() {
		binPlanGen = func() string {
			binPlan := getBinaryPlan(a.Ctx)
			return binPlan
		}
	}
	// Generating plan digest is slow, only generate it once if it's 'Point_Get'.
	// If it's a point get, different SQLs leads to different plans, so SQL digest
	// is enough to distinguish different plans in this case.
	var planDigest string
	var planDigestGen func() string
	if a.Plan.TP() == plancodec.TypePointGet {
		planDigestGen = func() string {
			_, planDigest := GetPlanDigest(stmtCtx)
			return planDigest.String()
		}
	} else {
		_, tmp := GetPlanDigest(stmtCtx)
		planDigest = tmp.String()
	}

	execDetail := stmtCtx.GetExecDetails()
	copTaskInfo := stmtCtx.CopTasksDetails()
	memMax := sessVars.MemTracker.MaxConsumed()
	diskMax := sessVars.DiskTracker.MaxConsumed()
	sql := a.GetTextToLog(false)
	var stmtDetail execdetails.StmtExecDetails
	stmtDetailRaw := a.GoCtx.Value(execdetails.StmtExecDetailKey)
	if stmtDetailRaw != nil {
		stmtDetail = *(stmtDetailRaw.(*execdetails.StmtExecDetails))
	}
	var tikvExecDetail util.ExecDetails
	tikvExecDetailRaw := a.GoCtx.Value(util.ExecDetailsKey)
	if tikvExecDetailRaw != nil {
		tikvExecDetail = *(tikvExecDetailRaw.(*util.ExecDetails))
	}
	var ruDetail *util.RUDetails
	if ruDetailRaw := a.GoCtx.Value(util.RUDetailsCtxKey); ruDetailRaw != nil {
		ruDetail = ruDetailRaw.(*util.RUDetails)
	}

	if stmtCtx.WaitLockLeaseTime > 0 {
		if execDetail.BackoffSleep == nil {
			execDetail.BackoffSleep = make(map[string]time.Duration)
		}
		execDetail.BackoffSleep["waitLockLeaseForCacheTable"] = stmtCtx.WaitLockLeaseTime
		execDetail.BackoffTime += stmtCtx.WaitLockLeaseTime
		execDetail.TimeDetail.WaitTime += stmtCtx.WaitLockLeaseTime
	}

	resultRows := GetResultRowsCount(stmtCtx, a.Plan)

	var (
		keyspaceName string
		keyspaceID   uint32
	)
	keyspaceName = keyspace.GetKeyspaceNameBySettings()
	if !keyspace.IsKeyspaceNameEmpty(keyspaceName) {
		keyspaceID = uint32(a.Ctx.GetStore().GetCodec().GetKeyspaceID())
	}

	stmtExecInfo := &stmtsummary.StmtExecInfo{
		SchemaName:          strings.ToLower(sessVars.CurrentDB),
		OriginalSQL:         sql,
		Charset:             charset,
		Collation:           collation,
		NormalizedSQL:       normalizedSQL,
		Digest:              digest.String(),
		PrevSQL:             prevSQL,
		PrevSQLDigest:       prevSQLDigest,
		PlanGenerator:       planGenerator,
		BinaryPlanGenerator: binPlanGen,
		PlanDigest:          planDigest,
		PlanDigestGen:       planDigestGen,
		User:                userString,
		TotalLatency:        costTime,
		ParseLatency:        sessVars.StmtCtx.DurationParse,
		CompileLatency:      sessVars.StmtCtx.DurationCompile,
		StmtCtx:             stmtCtx,
		CopTasks:            copTaskInfo,
		ExecDetail:          &execDetail,
		MemMax:              memMax,
		DiskMax:             diskMax,
		StartTime:           sessVars.StmtCtx.StartTime,
		IsInternal:          sessVars.InRestrictedSQL,
		Succeed:             succ,
		PlanInCache:         sessVars.FoundInPlanCache,
		PlanInBinding:       sessVars.FoundInBinding,
		ExecRetryCount:      a.retryCount,
		StmtExecDetails:     stmtDetail,
		ResultRows:          resultRows,
		TiKVExecDetails:     tikvExecDetail,
		Prepared:            a.isPreparedStmt,
		KeyspaceName:        keyspaceName,
		KeyspaceID:          keyspaceID,
		RUDetail:            ruDetail,
		ResourceGroupName:   sessVars.StmtCtx.ResourceGroupName,

		PlanCacheUnqualified: sessVars.StmtCtx.PlanCacheUnqualified(),
	}
	if a.retryCount > 0 {
		stmtExecInfo.ExecRetryTime = costTime - sessVars.StmtCtx.DurationParse - sessVars.StmtCtx.DurationCompile - time.Since(a.retryStartTime)
	}
	stmtsummaryv2.Add(stmtExecInfo)
}

// GetTextToLog return the query text to log.
func (a *ExecStmtRuntime) GetTextToLog(keepHint bool) string {
	var sql string
	sessVars := a.Ctx.GetSessionVars()
	rmode := sessVars.EnableRedactLog
	if rmode == errors.RedactLogEnable {
		if keepHint {
			sql = parser.NormalizeKeepHint(sessVars.StmtCtx.OriginalSQL)
		} else {
			sql, _ = sessVars.StmtCtx.SQLDigest()
		}
	} else if sensitiveStmt, ok := a.StmtNode.(ast.SensitiveStmtNode); ok {
		sql = sensitiveStmt.SecureText()
	} else {
		sql = redact.String(rmode, sessVars.StmtCtx.OriginalSQL+sessVars.PlanCacheParams.String())
	}
	return sql
}

func (a *ExecStmtRuntime) observeStmtFinishedForTopSQL() {
	vars := a.Ctx.GetSessionVars()
	if vars == nil {
		return
	}
	if stats := a.Ctx.GetStmtStats(); stats != nil && topsqlstate.TopSQLEnabled() {
		sqlDigest, planDigest := a.getSQLPlanDigest()
		execDuration := time.Since(vars.StmtCtx.StartTime) + vars.StmtCtx.DurationParse
		stats.OnExecutionFinished(sqlDigest, planDigest, execDuration)
	}
}

func (a *ExecStmtRuntime) getSQLPlanDigest() ([]byte, []byte) {
	var sqlDigest, planDigest []byte
	vars := a.Ctx.GetSessionVars()
	if _, d := vars.StmtCtx.SQLDigest(); d != nil {
		sqlDigest = d.Bytes()
	}
	if _, d := vars.StmtCtx.GetPlanDigest(); d != nil {
		planDigest = d.Bytes()
	}
	return sqlDigest, planDigest
}
