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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/executor/staticrecordset"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session/cursor"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/tikv/client-go/v2/trace"
	"go.uber.org/zap"
)

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
