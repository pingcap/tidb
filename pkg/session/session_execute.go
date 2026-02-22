// Copyright 2015 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package session

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"go.uber.org/zap"
)

func queryFailDumpTriggerCheck(config *traceevent.DumpTriggerConfig) bool {
	return config.Event.Type == "query_fail"
}

type isInternalAlias struct {
	bool
}

// isInternalAlias is not intuitive, but it is defined to avoid allocation.
// If the code is written as
//
//		traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.suspicious_event.is_internal", func(conf *traceevent.DumpTriggerConfig) {
//	     	conf.Event.IsInternal = conf.Event.IsInternal
//		})
//
// It's uncertain whether the Go compiler escape analysis is powerful enough to avoid allocation for the closure object.
// isInternalAlias is defined to help the compiler, this coding style will not cause closure object allocation.
//
//	traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.suspicious_event.is_internal", isInternalAlias{s.isInternal()}.isInternalDumpTriggerCheck)
func (i isInternalAlias) isInternalDumpTriggerCheck(config *traceevent.DumpTriggerConfig) bool {
	return config.Event.IsInternal == i.bool
}

func (s *session) ExecuteStmt(ctx context.Context, stmtNode ast.StmtNode) (sqlexec.RecordSet, error) {
	if fr := traceevent.GetFlightRecorder(); fr != nil {
		traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.sampling", fr.CheckSampling)
	}
	rs, err := s.executeStmtImpl(ctx, stmtNode)
	if err != nil {
		traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.suspicious_event", queryFailDumpTriggerCheck)
	}
	return rs, err
}

func (s *session) executeStmtImpl(ctx context.Context, stmtNode ast.StmtNode) (sqlexec.RecordSet, error) {
	r, ctx := tracing.StartRegionEx(ctx, "session.ExecuteStmt")
	defer r.End()

	if err := s.PrepareTxnCtx(ctx, stmtNode); err != nil {
		return nil, err
	}
	if err := s.loadCommonGlobalVariablesIfNeeded(); err != nil {
		return nil, err
	}

	sessVars := s.sessionVars
	sessVars.StartTime = time.Now()
	traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.suspicious_event.is_internal", isInternalAlias{s.isInternal()}.isInternalDumpTriggerCheck)

	// Some executions are done in compile stage, so we reset them before compile.
	if err := executor.ResetContextOfStmt(s, stmtNode); err != nil {
		return nil, err
	}

	if execStmt, ok := stmtNode.(*ast.ExecuteStmt); ok {
		if binParam, ok := execStmt.BinaryArgs.([]param.BinaryParam); ok {
			args, err := expression.ExecBinaryParam(s.GetSessionVars().StmtCtx.TypeCtx(), binParam)
			if err != nil {
				return nil, err
			}
			execStmt.BinaryArgs = args
		}
	}

	normalizedSQL, digest := s.sessionVars.StmtCtx.SQLDigest()
	cmdByte := byte(atomic.LoadUint32(&s.GetSessionVars().CommandValue))
	if topsqlstate.TopProfilingEnabled() {
		s.sessionVars.StmtCtx.IsSQLRegistered.Store(true)
		ctx = topsql.AttachAndRegisterSQLInfo(ctx, normalizedSQL, digest, s.sessionVars.InRestrictedSQL)
	}

	if err := s.validateStatementInTxn(stmtNode); err != nil {
		return nil, err
	}

	if err := s.validateStatementReadOnlyInStaleness(stmtNode); err != nil {
		return nil, err
	}

	// Uncorrelated subqueries will execute once when building plan, so we reset process info before building plan.
	s.currentPlan = nil // reset current plan
	s.SetProcessInfo(stmtNode.Text(), time.Now(), cmdByte, 0)
	s.txn.onStmtStart(digest.String())
	defer sessiontxn.GetTxnManager(s).OnStmtEnd()
	defer s.txn.onStmtEnd()

	if err := s.onTxnManagerStmtStartOrRetry(ctx, stmtNode); err != nil {
		return nil, err
	}

	failpoint.Inject("mockStmtSlow", func(val failpoint.Value) {
		if strings.Contains(stmtNode.Text(), "/* sleep */") {
			v, _ := val.(int)
			time.Sleep(time.Duration(v) * time.Millisecond)
		}
	})

	var stmtLabel string
	if execStmt, ok := stmtNode.(*ast.ExecuteStmt); ok {
		prepareStmt, err := plannercore.GetPreparedStmt(execStmt, s.sessionVars)
		if err == nil && prepareStmt.PreparedAst != nil {
			stmtLabel = stmtctx.GetStmtLabel(ctx, prepareStmt.PreparedAst.Stmt)
		}
	}
	if stmtLabel == "" {
		stmtLabel = stmtctx.GetStmtLabel(ctx, stmtNode)
	}
	s.setRequestSource(ctx, stmtLabel, stmtNode)

	globalMemArbitrator := memory.GlobalMemArbitrator()
	arbitratorMode := globalMemArbitrator.WorkMode()
	enableWaitAverse := sessVars.MemArbitrator.WaitAverse == variable.MemArbitratorWaitAverseEnable
	execUseArbitrator := globalMemArbitrator != nil && sessVars.ConnectionID != 0 &&
		sessVars.MemArbitrator.WaitAverse != variable.MemArbitratorNolimit &&
		sessVars.StmtCtx.MemSensitive
	compilePlanMemQuota := int64(0) // mem quota for compiler & optimizer
	if execUseArbitrator {
		compilePlanMemQuota = approxCompilePlanMemQuota(normalizedSQL, sessVars.StmtCtx.InSelectStmt)
		execUseArbitrator = compilePlanMemQuota > 0
	}

	releaseCommonQuota := func() { // release common quota
		if compilePlanMemQuota > 0 {
			_ = globalMemArbitrator.ConsumeQuotaFromAwaitFreePool(sessVars.ConnectionID, -compilePlanMemQuota)
			compilePlanMemQuota = 0
		}
	}

	if execUseArbitrator {
		intoErrBeforeExec := func() error {
			if enableWaitAverse {
				metrics.GlobalMemArbitratorSubTasks.CancelWaitAversePlan.Inc()
				return exeerrors.ErrQueryExecStopped.GenWithStackByArgs(memory.ArbitratorWaitAverseCancel.String()+defSuffixCompilePlan, sessVars.ConnectionID)
			}
			if arbitratorMode == memory.ArbitratorModeStandard {
				metrics.GlobalMemArbitratorSubTasks.CancelStandardModePlan.Inc()
				return exeerrors.ErrQueryExecStopped.GenWithStackByArgs(memory.ArbitratorStandardCancel.String()+defSuffixCompilePlan, sessVars.ConnectionID)
			}
			return nil
		}

		if globalMemArbitrator.AtMemRisk() {
			if s.sessionPlanCache != nil {
				s.sessionPlanCache.DeleteAll()
			}
			if err := intoErrBeforeExec(); err != nil {
				return nil, err
			}
			for globalMemArbitrator.AtMemRisk() {
				if globalMemArbitrator.AtOOMRisk() {
					metrics.GlobalMemArbitratorSubTasks.ForceKillPlan.Inc()
					return nil, exeerrors.ErrQueryExecStopped.GenWithStackByArgs(memory.ArbitratorOOMRiskKill.String()+defSuffixCompilePlan, sessVars.ConnectionID)
				}
				time.Sleep(defOOMRiskCheckDur)
			}
		}

		arbitratorOutOfQuota := !globalMemArbitrator.ConsumeQuotaFromAwaitFreePool(sessVars.ConnectionID, compilePlanMemQuota)
		defer releaseCommonQuota()

		if arbitratorOutOfQuota { // for SQL which needs to be controlled by mem-arbitrator
			if s.sessionPlanCache != nil && s.sessionPlanCache.Size() > 0 {
				s.sessionPlanCache.DeleteAll()
				// one more chance to get quota after clearing plan cache
			} else if err := intoErrBeforeExec(); err != nil {
				return nil, err
			}
		}
	}

	var stmt *executor.ExecStmt
	var err error

	{
		// Transform abstract syntax tree to a physical plan(stored in executor.ExecStmt).
		compiler := executor.Compiler{Ctx: s}
		stmt, err = compiler.Compile(ctx, stmtNode)
		// TODO: report precise tracked heap inuse to the global mem-arbitrator if necessary
	}

	// check if resource group hint is valid, can't do this in planner.Optimize because we can access
	// infoschema there.
	if sessVars.StmtCtx.ResourceGroupName != sessVars.ResourceGroupName {
		// if target resource group doesn't exist, fallback to the origin resource group.
		if _, ok := s.infoCache.GetLatest().ResourceGroupByName(ast.NewCIStr(sessVars.StmtCtx.ResourceGroupName)); !ok {
			logutil.Logger(ctx).Warn("Unknown resource group from hint", zap.String("name", sessVars.StmtCtx.ResourceGroupName))
			sessVars.StmtCtx.ResourceGroupName = sessVars.ResourceGroupName
			if txn, err := s.Txn(false); err == nil && txn != nil && txn.Valid() {
				kv.SetTxnResourceGroup(txn, sessVars.ResourceGroupName)
			}
		}
	}

	if err != nil {
		s.rollbackOnError(ctx)

		// Only print log message when this SQL is from the user.
		// Mute the warning for internal SQLs.
		if !s.sessionVars.InRestrictedSQL {
			if !variable.ErrUnknownSystemVar.Equal(err) {
				sql := stmtNode.Text()
				sql = parser.Normalize(sql, s.sessionVars.EnableRedactLog)
				logutil.Logger(ctx).Warn("compile SQL failed", zap.Error(err),
					zap.String("SQL", sql))
			}
		}
		return nil, err
	}

	durCompile := time.Since(s.sessionVars.StartTime)
	s.GetSessionVars().DurationCompile = durCompile
	if s.isInternal() {
		session_metrics.SessionExecuteCompileDurationInternal.Observe(durCompile.Seconds())
	} else {
		session_metrics.SessionExecuteCompileDurationGeneral.Observe(durCompile.Seconds())
	}
	s.currentPlan = stmt.Plan
	if execStmt, ok := stmtNode.(*ast.ExecuteStmt); ok {
		if execStmt.Name == "" {
			// for exec-stmt on bin-protocol, ignore the plan detail in `show process` to gain performance benefits.
			s.currentPlan = nil
		}
	}

	// Execute the physical plan.
	defer logStmt(stmt, s) // defer until txnStartTS is set

	if sessVars.MemArbitrator.WaitAverse == variable.MemArbitratorNolimit {
		metrics.GlobalMemArbitratorSubTasks.NoLimit.Inc()
	}
	if execUseArbitrator {
		releaseCommonQuota()

		reserveSize := min(sessVars.MemArbitrator.QueryReserved, memory.DefMaxLimit)

		memPriority := memory.ArbitrationPriorityMedium

		if sg, ok := domain.GetDomain(s).InfoSchema().ResourceGroupByName(ast.NewCIStr(sessVars.StmtCtx.ResourceGroupName)); ok {
			switch sg.Priority {
			case ast.LowPriorityValue:
				memPriority = memory.ArbitrationPriorityLow
			case ast.MediumPriorityValue:
				memPriority = memory.ArbitrationPriorityMedium
			case ast.HighPriorityValue:
				memPriority = memory.ArbitrationPriorityHigh
			}
		}

		digestKey := normalizedSQL
		// digestKey := sessVars.StmtCtx.OriginalSQL

		tracker := sessVars.StmtCtx.MemTracker
		if !tracker.InitMemArbitrator(
			globalMemArbitrator,
			sessVars.MemTracker.Killer,
			digestKey,
			memPriority,
			enableWaitAverse,
			reserveSize,
			s.isInternal(),
		) {
			return nil, errors.New("failed to init mem-arbitrator")
		}

		defer func() { // detach mem-arbitrator and rethrow panic if any
			if r := recover(); r != nil {
				tracker.DetachMemArbitrator(true)
				panic(r)
			}
		}()
	}

	var recordSet sqlexec.RecordSet
	if stmt.PsStmt != nil { // point plan short path
		ctx, prevTraceID := resetStmtTraceID(ctx, s)

		// Emit stmt.start trace event (simplified for point-get fast path)
		if traceevent.IsEnabled(traceevent.StmtLifecycle) {
			fields := []zap.Field{
				zap.Uint64("conn_id", s.sessionVars.ConnectionID),
			}
			// Include previous trace ID to create statement chain
			if len(prevTraceID) > 0 {
				fields = append(fields, zap.String("prev_trace_id", redact.Key(prevTraceID)))
			}
			traceevent.TraceEvent(ctx, traceevent.StmtLifecycle, "stmt.start", fields...)
		}

		// Defer stmt.finish trace event (simplified for point-get fast path)
		defer func() {
			if traceevent.IsEnabled(traceevent.StmtLifecycle) {
				fields := []zap.Field{
					zap.Uint64("conn_id", s.sessionVars.ConnectionID),
				}
				if err != nil {
					fields = append(fields, zap.Error(err))
				}
				traceevent.TraceEvent(ctx, traceevent.StmtLifecycle, "stmt.finish", fields...)
			}
		}()

		recordSet, err = stmt.PointGet(ctx)
		s.setLastTxnInfoBeforeTxnEnd()
		s.txn.changeToInvalid()
	} else {
		recordSet, err = runStmt(ctx, s, stmt)
	}

	// Observe the resource group query total counter if the resource control is enabled and the
	// current session is attached with a resource group.
	resourceGroupName := s.GetSessionVars().StmtCtx.ResourceGroupName
	if len(resourceGroupName) > 0 {
		metrics.ResourceGroupQueryTotalCounter.WithLabelValues(resourceGroupName, resourceGroupName).Inc()
	}

	if err != nil {
		if !errIsNoisy(err) {
			logutil.Logger(ctx).Warn("run statement failed",
				zap.Int64("schemaVersion", s.GetInfoSchema().SchemaMetaVersion()),
				zap.Error(err),
				zap.String("session", s.String()))
		}
		return recordSet, err
	}
	if !s.isInternal() && config.GetGlobalConfig().EnableTelemetry {
		telemetry.CurrentExecuteCount.Inc()
		tiFlashPushDown, tiFlashExchangePushDown := plannercore.IsTiFlashContained(stmt.Plan)
		if tiFlashPushDown {
			telemetry.CurrentTiFlashPushDownCount.Inc()
		}
		if tiFlashExchangePushDown {
			telemetry.CurrentTiFlashExchangePushDownCount.Inc()
		}
	}
	return recordSet, nil
}

func (s *session) GetSQLExecutor() sqlexec.SQLExecutor {
	return s
}

func (s *session) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return s
}

func (s *session) onTxnManagerStmtStartOrRetry(ctx context.Context, node ast.StmtNode) error {
	if s.sessionVars.RetryInfo.Retrying {
		return sessiontxn.GetTxnManager(s).OnStmtRetry(ctx)
	}
	return sessiontxn.GetTxnManager(s).OnStmtStart(ctx, node)
}

func (s *session) validateStatementInTxn(stmtNode ast.StmtNode) error {
	vars := s.GetSessionVars()
	if _, ok := stmtNode.(*ast.ImportIntoStmt); ok && vars.InTxn() {
		return errors.New("cannot run IMPORT INTO in explicit transaction")
	}
	return nil
}

func (s *session) validateStatementReadOnlyInStaleness(stmtNode ast.StmtNode) error {
	vars := s.GetSessionVars()
	if !vars.TxnCtx.IsStaleness && vars.TxnReadTS.PeakTxnReadTS() == 0 && !vars.EnableExternalTSRead || vars.InRestrictedSQL {
		return nil
	}
	errMsg := "only support read-only statement during read-only staleness transactions"
	node := stmtNode.(ast.Node)
	switch v := node.(type) {
	case *ast.SplitRegionStmt:
		return nil
	case *ast.SelectStmt:
		// select lock statement needs start a transaction which will be conflict to stale read,
		// we forbid select lock statement in stale read for now.
		if v.LockInfo != nil {
			return errors.New("select lock hasn't been supported in stale read yet")
		}
		if !plannercore.IsReadOnly(stmtNode, vars) {
			return errors.New(errMsg)
		}
		return nil
	case *ast.ExplainStmt, *ast.DoStmt, *ast.ShowStmt, *ast.SetOprStmt, *ast.ExecuteStmt, *ast.SetOprSelectList:
		if !plannercore.IsReadOnly(stmtNode, vars) {
			return errors.New(errMsg)
		}
		return nil
	default:
	}
	// covered DeleteStmt/InsertStmt/UpdateStmt/CallStmt/LoadDataStmt
	if _, ok := stmtNode.(ast.DMLNode); ok {
		return errors.New(errMsg)
	}
	return nil
}

