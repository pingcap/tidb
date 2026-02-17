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
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/staticrecordset"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/param"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session/cursor"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/telemetry"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/topsql"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/tikv/client-go/v2/trace"
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

// fileTransInConnKeys contains the keys of queries that will be handled by handleFileTransInConn.
var fileTransInConnKeys = []fmt.Stringer{
	executor.LoadDataVarKey,
	executor.LoadStatsVarKey,
	executor.PlanReplayerLoadVarKey,
}

func (s *session) hasFileTransInConn() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, k := range fileTransInConnKeys {
		v := s.mu.values[k]
		if v != nil {
			return true
		}
	}
	return false
}

type sqlDigestAlias struct {
	Digest string
}

func (digest sqlDigestAlias) sqlDigestDumpTriggerCheck(config *traceevent.DumpTriggerConfig) bool {
	return config.UserCommand.SQLDigest == digest.Digest
}

type userAlias struct {
	user string
}

func (u userAlias) byUserDumpTriggerCheck(config *traceevent.DumpTriggerConfig) bool {
	return config.UserCommand.ByUser == u.user
}

type stmtLabelAlias struct {
	label string
}

func (s stmtLabelAlias) stmtLabelDumpTriggerCheck(config *traceevent.DumpTriggerConfig) bool {
	return config.UserCommand.StmtLabel == s.label
}

// resetStmtTraceID generates a new trace ID for the current statement,
// injects it into the session context for cross-statement correlation, and returns the previous trace ID.
func resetStmtTraceID(ctx context.Context, se *session) (context.Context, []byte) {
	// Capture previous trace ID from session variables (for statement chaining)
	// We store it in session variables instead of context because the context
	// is recreated for each statement and doesn't persist across executions
	prevTraceID := se.sessionVars.PrevTraceID

	// Inject trace ID into context for correlation across TiDB -> client-go -> TiKV
	// This enables trace events to be correlated by trace_id field
	// The trace ID is generated from transaction start_ts and statement count
	startTS := se.sessionVars.TxnCtx.StartTS
	stmtCount := uint64(se.sessionVars.TxnCtx.StatementCount)
	traceID := traceevent.GenerateTraceID(ctx, startTS, stmtCount)
	ctx = trace.ContextWithTraceID(ctx, traceID)
	se.currentCtx = ctx
	// Store trace ID for next statement
	se.sessionVars.PrevTraceID = traceID

	return ctx, prevTraceID
}

// runStmt executes the sqlexec.Statement and commit or rollback the current transaction.
func runStmt(ctx context.Context, se *session, s sqlexec.Statement) (rs sqlexec.RecordSet, err error) {
	failpoint.Inject("assertTxnManagerInRunStmt", func() {
		sessiontxn.RecordAssert(se, "assertTxnManagerInRunStmt", true)
		if stmt, ok := s.(*executor.ExecStmt); ok {
			sessiontxn.AssertTxnManagerInfoSchema(se, stmt.InfoSchema)
		}
	})

	r, ctx := tracing.StartRegionEx(ctx, "session.runStmt")
	defer r.End()
	if r.Span != nil {
		r.Span.LogKV("sql", s.Text())
	}

	ctx, prevTraceID := resetStmtTraceID(ctx, se)
	stmtCtx := se.sessionVars.StmtCtx
	sqlDigest, _ := stmtCtx.SQLDigest()
	// Make sure StmtType is filled even if succ is false.
	if stmtCtx.StmtType == "" {
		stmtCtx.StmtType = stmtctx.GetStmtLabel(ctx, s.GetStmtNode())
	}

	// Emit stmt.start trace event
	if traceevent.IsEnabled(traceevent.StmtLifecycle) {
		fields := []zap.Field{
			zap.String("sql_digest", sqlDigest),
			zap.Bool("autocommit", se.sessionVars.IsAutocommit()),
			zap.Uint64("conn_id", se.sessionVars.ConnectionID),
		}
		// Include previous trace ID to create statement chain
		if len(prevTraceID) > 0 {
			fields = append(fields, zap.String("prev_trace_id", redact.Key(prevTraceID)))
		}
		traceevent.TraceEvent(ctx, traceevent.StmtLifecycle, "stmt.start", fields...)
	}
	// Not using closure to avoid unnecessary memory allocation.
	traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.user_command.sql_digest", sqlDigestAlias{sqlDigest}.sqlDigestDumpTriggerCheck)
	if se.sessionVars.User != nil && se.sessionVars.User.Username != "" {
		traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.user_command.by_user", userAlias{se.sessionVars.User.Username}.byUserDumpTriggerCheck)
	}
	traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.user_command.stmt_label", stmtLabelAlias{stmtCtx.StmtType}.stmtLabelDumpTriggerCheck)

	// Defer stmt.finish trace event to capture final state including errors
	defer func() {
		if traceevent.IsEnabled(traceevent.StmtLifecycle) {
			stmtCtx := se.sessionVars.StmtCtx
			sqlDigest, _ := stmtCtx.SQLDigest()
			_, planDigest := stmtCtx.GetPlanDigest()
			var planDigestHex string
			if planDigest != nil {
				planDigestHex = hex.EncodeToString(planDigest.Bytes())
			}
			fields := []zap.Field{
				zap.String("sql_digest", sqlDigest),
				zap.String("plan_digest", planDigestHex),
				zap.Bool("autocommit", se.sessionVars.IsAutocommit()),
				zap.Uint64("conn_id", se.sessionVars.ConnectionID),
				zap.Int("retry_count", se.sessionVars.TxnCtx.StatementCount),
			}
			if err != nil {
				fields = append(fields, zap.Error(err))
			}
			traceevent.TraceEvent(ctx, traceevent.StmtLifecycle, "stmt.finish", fields...)
		}
	}()

	se.SetValue(sessionctx.QueryString, s.Text())
	if _, ok := s.(*executor.ExecStmt).StmtNode.(ast.DDLNode); ok {
		se.SetValue(sessionctx.LastExecuteDDL, true)
	} else {
		se.ClearValue(sessionctx.LastExecuteDDL)
	}

	sessVars := se.sessionVars

	// Save origTxnCtx here to avoid it reset in the transaction retry.
	origTxnCtx := sessVars.TxnCtx
	err = se.checkTxnAborted(s)
	if err != nil {
		return nil, err
	}
	if sessVars.TxnCtx.CouldRetry && !s.IsReadOnly(sessVars) {
		// Only when the txn is could retry and the statement is not read only, need to do stmt-count-limit check,
		// otherwise, the stmt won't be add into stmt history, and also don't need check.
		// About `stmt-count-limit`, see more in https://docs.pingcap.com/tidb/stable/tidb-configuration-file#stmt-count-limit
		if err := checkStmtLimit(ctx, se, false); err != nil {
			return nil, err
		}
	}

	rs, err = s.Exec(ctx)

	if se.txn.Valid() && se.txn.IsPipelined() {
		// Pipelined-DMLs can return assertion errors and write conflicts here because they flush
		// during execution, handle these errors as we would handle errors after a commit.
		if err != nil {
			err = se.handleAssertionFailure(ctx, err)
		}
		newErr := se.tryReplaceWriteConflictError(ctx, err)
		if newErr != nil {
			err = newErr
		}
	}

	se.updateTelemetryMetric(s.(*executor.ExecStmt))
	sessVars.TxnCtx.StatementCount++
	if rs != nil {
		if se.GetSessionVars().StmtCtx.IsExplainAnalyzeDML {
			if !sessVars.InTxn() {
				se.StmtCommit(ctx)
				if err := se.CommitTxn(ctx); err != nil {
					return nil, err
				}
			}
		}
		return &execStmtResult{
			RecordSet: rs,
			sql:       s,
			se:        se,
		}, err
	}

	err = finishStmt(ctx, se, err, s)
	if se.hasFileTransInConn() {
		// The query will be handled later in handleFileTransInConn,
		// then should call the ExecStmt.FinishExecuteStmt to finish this statement.
		se.SetValue(ExecStmtVarKey, s.(*executor.ExecStmt))
	} else {
		// If it is not a select statement or special query, we record its slow log here,
		// then it could include the transaction commit time.
		s.(*executor.ExecStmt).FinishExecuteStmt(origTxnCtx.StartTS, err, false)
	}
	return nil, err
}

// ExecStmtVarKeyType is a dummy type to avoid naming collision in context.
type ExecStmtVarKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (ExecStmtVarKeyType) String() string {
	return "exec_stmt_var_key"
}

// ExecStmtVarKey is a variable key for ExecStmt.
const ExecStmtVarKey ExecStmtVarKeyType = 0

// execStmtResult is the return value of ExecuteStmt and it implements the sqlexec.RecordSet interface.
// Why we need a struct to wrap a RecordSet and provide another RecordSet?
// This is because there are so many session state related things that definitely not belongs to the original
// RecordSet, so this struct exists and RecordSet.Close() is overridden to handle that.
type execStmtResult struct {
	sqlexec.RecordSet
	se     *session
	sql    sqlexec.Statement
	once   sync.Once
	closed bool
}

func (rs *execStmtResult) Finish() error {
	var err error
	rs.once.Do(func() {
		var err1 error
		if f, ok := rs.RecordSet.(interface{ Finish() error }); ok {
			err1 = f.Finish()
		}
		err2 := finishStmt(context.Background(), rs.se, err, rs.sql)
		if err1 != nil {
			err = err1
		} else {
			err = err2
		}
	})
	return err
}

func (rs *execStmtResult) Close() error {
	if rs.closed {
		return nil
	}
	err1 := rs.Finish()
	err2 := rs.RecordSet.Close()
	rs.closed = true
	if err1 != nil {
		return err1
	}
	return err2
}

func (rs *execStmtResult) TryDetach() (sqlexec.RecordSet, bool, error) {
	// If `TryDetach` is called, the connection must have set `mysql.ServerStatusCursorExists`, or
	// the `StatementContext` will be re-used and cause data race.
	intest.Assert(rs.se.GetSessionVars().HasStatusFlag(mysql.ServerStatusCursorExists))

	if !rs.sql.IsReadOnly(rs.se.GetSessionVars()) {
		return nil, false, nil
	}
	if !plannercore.IsAutoCommitTxn(rs.se.GetSessionVars()) {
		return nil, false, nil
	}

	drs, ok := rs.RecordSet.(sqlexec.DetachableRecordSet)
	if !ok {
		return nil, false, nil
	}
	detachedRS, ok, err := drs.TryDetach()
	if !ok || err != nil {
		return nil, ok, err
	}
	cursorHandle := rs.se.GetCursorTracker().NewCursor(
		cursor.State{StartTS: rs.se.GetSessionVars().TxnCtx.StartTS},
	)
	crs := staticrecordset.WrapRecordSetWithCursor(cursorHandle, detachedRS)

	// Now, a transaction is not needed for the detached record set, so we commit the transaction and cleanup
	// the session state.
	err = finishStmt(context.Background(), rs.se, nil, rs.sql)
	if err != nil {
		err2 := detachedRS.Close()
		if err2 != nil {
			logutil.BgLogger().Error("close detached record set failed", zap.Error(err2))
		}
		return nil, true, err
	}

	return crs, true, nil
}

// GetExecutor4Test exports the internal executor for test purpose.
func (rs *execStmtResult) GetExecutor4Test() any {
	return rs.RecordSet.(interface{ GetExecutor4Test() any }).GetExecutor4Test()
}

// rollbackOnError makes sure the next statement starts a new transaction with the latest InfoSchema.
func (s *session) rollbackOnError(ctx context.Context) {
	if !s.sessionVars.InTxn() {
		s.RollbackTxn(ctx)
	}
}

// PrepareStmt is used for executing prepare statement in binary protocol
func (s *session) PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*resolve.ResultField, err error) {
	defer func() {
		if s.sessionVars.StmtCtx != nil {
			s.sessionVars.StmtCtx.DetachMemDiskTracker()
		}
	}()
	if s.sessionVars.TxnCtx.InfoSchema == nil {
		// We don't need to create a transaction for prepare statement, just get information schema will do.
		s.sessionVars.TxnCtx.InfoSchema = s.infoCache.GetLatest()
	}
	err = s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		return
	}

	ctx := context.Background()
	// NewPrepareExec may need startTS to build the executor, for example prepare statement has subquery in int.
	// So we have to call PrepareTxnCtx here.
	if err = s.PrepareTxnCtx(ctx, nil); err != nil {
		return
	}

	prepareStmt := &ast.PrepareStmt{SQLText: sql}
	if err = s.onTxnManagerStmtStartOrRetry(ctx, prepareStmt); err != nil {
		return
	}

	if err = sessiontxn.GetTxnManager(s).AdviseWarmup(); err != nil {
		return
	}
	prepareExec := executor.NewPrepareExec(s, sql)
	err = prepareExec.Next(ctx, nil)
	// Rollback even if err is nil.
	s.rollbackOnError(ctx)

	if err != nil {
		return
	}
	return prepareExec.ID, prepareExec.ParamCount, prepareExec.Fields, nil
}

// ExecutePreparedStmt executes a prepared statement.
func (s *session) ExecutePreparedStmt(ctx context.Context, stmtID uint32, params []expression.Expression) (sqlexec.RecordSet, error) {
	prepStmt, err := s.sessionVars.GetPreparedStmtByID(stmtID)
	if err != nil {
		err = plannererrors.ErrStmtNotFound
		logutil.Logger(ctx).Error("prepared statement not found", zap.Uint32("stmtID", stmtID))
		return nil, err
	}
	stmt, ok := prepStmt.(*plannercore.PlanCacheStmt)
	if !ok {
		return nil, errors.Errorf("invalid PlanCacheStmt type")
	}
	execStmt := &ast.ExecuteStmt{
		BinaryArgs: params,
		PrepStmt:   stmt,
		PrepStmtId: stmtID,
	}
	return s.ExecuteStmt(ctx, execStmt)
}

func (s *session) DropPreparedStmt(stmtID uint32) error {
	vars := s.sessionVars
	if _, ok := vars.PreparedStmts[stmtID]; !ok {
		return plannererrors.ErrStmtNotFound
	}
	vars.RetryInfo.DroppedPreparedStmtIDs = append(vars.RetryInfo.DroppedPreparedStmtIDs, stmtID)
	return nil
}
