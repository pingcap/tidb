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

package session

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/tikv/client-go/v2/oracle"
	tikvutil "github.com/tikv/client-go/v2/util"
)

// PrepareTxnCtx begins a transaction, and creates a new transaction context.
// When stmt is provided, it determines transaction mode based on the statement.
// When stmt is nil, it uses the session's default transaction mode.
func (s *session) PrepareTxnCtx(ctx context.Context, stmt ast.StmtNode) error {
	s.currentCtx = ctx
	if s.txn.validOrPending() {
		return nil
	}

	txnMode := s.decideTxnMode(stmt)

	return sessiontxn.GetTxnManager(s).EnterNewTxn(ctx, &sessiontxn.EnterNewTxnRequest{
		Type:    sessiontxn.EnterNewTxnBeforeStmt,
		TxnMode: txnMode,
	})
}

// decideTxnMode determines whether to use pessimistic or optimistic transaction mode
// based on the current session state, configuration, and the statement being executed.
// When stmt is nil, it uses the session's default transaction mode.
func (s *session) decideTxnMode(stmt ast.StmtNode) string {
	if s.sessionVars.RetryInfo.Retrying {
		return ast.Pessimistic
	}

	if s.sessionVars.TxnMode != ast.Pessimistic {
		return ast.Optimistic
	}

	if !s.sessionVars.IsAutocommit() {
		return s.sessionVars.TxnMode
	}

	if stmt != nil && s.shouldUsePessimisticAutoCommit(stmt) {
		return ast.Pessimistic
	}

	return ast.Optimistic
}

// shouldUsePessimisticAutoCommit checks if pessimistic-auto-commit should be applied
// for the current statement.
func (s *session) shouldUsePessimisticAutoCommit(stmtNode ast.StmtNode) bool {
	// Check if pessimistic-auto-commit is enabled globally
	if !config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load() {
		return false
	}

	// Disabled for bulk DML operations
	if s.GetSessionVars().BulkDMLEnabled {
		return false
	}

	if s.isInternal() {
		return false
	}

	// Use direct AST inspection to determine if this is a DML statement
	return s.isDMLStatement(stmtNode)
}

// isDMLStatement checks if the given statement should use pessimistic-auto-commit.
// It handles EXECUTE unwrapping and properly handles EXPLAIN statements by checking their inner statement.
func (s *session) isDMLStatement(stmtNode ast.StmtNode) bool {
	if stmtNode == nil {
		return false
	}

	// Handle EXECUTE statements - unwrap to get the actual prepared statement
	actualStmt := stmtNode
	if execStmt, ok := stmtNode.(*ast.ExecuteStmt); ok {
		prepareStmt, err := plannercore.GetPreparedStmt(execStmt, s.GetSessionVars())
		if err != nil || prepareStmt == nil {
			return false
		}
		actualStmt = prepareStmt.PreparedAst.Stmt
	}

	// For EXPLAIN statements, check the underlying statement
	// This ensures EXPLAIN shows the correct plan that would be used if the statement were executed
	if explainStmt, ok := actualStmt.(*ast.ExplainStmt); ok {
		return s.isDMLStatement(explainStmt.Stmt)
	}

	// Only these DML statements should use pessimistic-auto-commit
	// Note: LOAD DATA and IMPORT are intentionally excluded
	switch actualStmt.(type) {
	case *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt:
		return true
	default:
		return false
	}
}

// PrepareTSFuture uses to try to get ts future.
func (s *session) PrepareTSFuture(ctx context.Context, future oracle.Future, scope string) error {
	if s.txn.Valid() {
		return errors.New("cannot prepare ts future when txn is valid")
	}

	failpoint.Inject("assertTSONotRequest", func() {
		if _, ok := future.(sessiontxn.ConstantFuture); !ok && !s.isInternal() {
			panic("tso shouldn't be requested")
		}
	})

	failpoint.InjectContext(ctx, "mockGetTSFail", func() {
		future = txnFailFuture{}
	})

	s.txn.changeToPending(&txnFuture{
		future:                          future,
		store:                           s.store,
		txnScope:                        scope,
		pipelined:                       s.usePipelinedDmlOrWarn(ctx),
		pipelinedFlushConcurrency:       s.GetSessionVars().PipelinedFlushConcurrency,
		pipelinedResolveLockConcurrency: s.GetSessionVars().PipelinedResolveLockConcurrency,
		pipelinedWriteThrottleRatio:     s.GetSessionVars().PipelinedWriteThrottleRatio,
	})
	return nil
}

// GetPreparedTxnFuture returns the TxnFuture if it is valid or pending.
// It returns nil otherwise.
func (s *session) GetPreparedTxnFuture() sessionctx.TxnFuture {
	if !s.txn.validOrPending() {
		return nil
	}
	return &s.txn
}

// RefreshTxnCtx implements context.RefreshTxnCtx interface.
func (s *session) RefreshTxnCtx(ctx context.Context) error {
	var commitDetail *tikvutil.CommitDetails
	ctx = context.WithValue(ctx, tikvutil.CommitDetailCtxKey, &commitDetail)
	err := s.doCommit(ctx)
	if commitDetail != nil {
		s.GetSessionVars().StmtCtx.MergeExecDetails(commitDetail)
	}
	if err != nil {
		return err
	}

	s.updateStatsDeltaToCollector()

	return sessiontxn.NewTxn(ctx, s)
}

// GetStore gets the store of session.
func (s *session) GetStore() kv.Storage {
	return s.store
}

// usePipelinedDmlOrWarn returns the current statement can be executed as a pipelined DML.
func (s *session) usePipelinedDmlOrWarn(ctx context.Context) bool {
	if !s.sessionVars.BulkDMLEnabled {
		return false
	}
	stmtCtx := s.sessionVars.StmtCtx
	if stmtCtx == nil {
		return false
	}
	if stmtCtx.IsReadOnly {
		return false
	}
	vars := s.GetSessionVars()
	if !vars.TxnCtx.EnableMDL {
		stmtCtx.AppendWarning(
			errors.New(
				"Pipelined DML can not be used without Metadata Lock. Fallback to standard mode",
			),
		)
		return false
	}
	if (vars.BatchCommit || vars.BatchInsert || vars.BatchDelete) && vars.DMLBatchSize > 0 && vardef.EnableBatchDML.Load() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used with the deprecated Batch DML. Fallback to standard mode"))
		return false
	}
	if !(stmtCtx.InInsertStmt || stmtCtx.InDeleteStmt || stmtCtx.InUpdateStmt) {
		if !stmtCtx.IsReadOnly {
			stmtCtx.AppendWarning(errors.New("Pipelined DML can only be used for auto-commit INSERT, REPLACE, UPDATE or DELETE. Fallback to standard mode"))
		}
		return false
	}
	if s.isInternal() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used for internal SQL. Fallback to standard mode"))
		return false
	}
	if vars.InTxn() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used in transaction. Fallback to standard mode"))
		return false
	}
	if !vars.IsAutocommit() {
		stmtCtx.AppendWarning(errors.New("Pipelined DML can only be used in autocommit mode. Fallback to standard mode"))
		return false
	}
	if s.GetSessionVars().ConstraintCheckInPlace {
		// we enforce that pipelined DML must lazily check key.
		stmtCtx.AppendWarning(
			errors.New(
				"Pipelined DML can not be used when tidb_constraint_check_in_place=ON. " +
					"Fallback to standard mode",
			),
		)
		return false
	}
	is, ok := s.GetLatestInfoSchema().(infoschema.InfoSchema)
	if !ok {
		stmtCtx.AppendWarning(errors.New("Pipelined DML failed to get latest InfoSchema. Fallback to standard mode"))
		return false
	}
	for _, t := range stmtCtx.Tables {
		// get table schema from current infoschema
		tbl, err := is.TableByName(ctx, ast.NewCIStr(t.DB), ast.NewCIStr(t.Table))
		if err != nil {
			stmtCtx.AppendWarning(errors.New("Pipelined DML failed to get table schema. Fallback to standard mode"))
			return false
		}
		if tbl.Meta().IsView() {
			stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used on view. Fallback to standard mode"))
			return false
		}
		if tbl.Meta().IsSequence() {
			stmtCtx.AppendWarning(errors.New("Pipelined DML can not be used on sequence. Fallback to standard mode"))
			return false
		}
		if vars.ForeignKeyChecks && (len(tbl.Meta().ForeignKeys) > 0 || len(is.GetTableReferredForeignKeys(t.DB, t.Table)) > 0) {
			stmtCtx.AppendWarning(
				errors.New(
					"Pipelined DML can not be used on table with foreign keys when foreign_key_checks = ON. Fallback to standard mode",
				),
			)
			return false
		}
		if tbl.Meta().TempTableType != model.TempTableNone {
			stmtCtx.AppendWarning(
				errors.New(
					"Pipelined DML can not be used on temporary tables. " +
						"Fallback to standard mode",
				),
			)
			return false
		}
		if tbl.Meta().TableCacheStatusType != model.TableCacheStatusDisable {
			stmtCtx.AppendWarning(
				errors.New(
					"Pipelined DML can not be used on cached tables. " +
						"Fallback to standard mode",
				),
			)
			return false
		}
	}

	// tidb_dml_type=bulk will invalidate the config pessimistic-auto-commit.
	// The behavior is as if the config is set to false. But we generate a warning for it.
	if config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load() {
		stmtCtx.AppendWarning(
			errors.New(
				"pessimistic-auto-commit config is ignored in favor of Pipelined DML",
			),
		)
	}
	return true
}
