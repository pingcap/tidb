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

package executor

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/util/breakpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
	tikvtrace "github.com/tikv/client-go/v2/trace"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// Exec builds an Executor from a plan. If the Executor doesn't return result,
// like the INSERT, UPDATE statements, it executes in this function. If the Executor returns
// result, execution is done after this function returns, in the returned sqlexec.RecordSet Next method.
func (a *ExecStmt) Exec(ctx context.Context) (_ sqlexec.RecordSet, err error) {
	defer func() {
		r := recover()
		if r == nil {
			if a.retryCount > 0 {
				metrics.StatementPessimisticRetryCount.Observe(float64(a.retryCount))
			}
			execDetails := a.Ctx.GetSessionVars().StmtCtx.GetExecDetails()
			if execDetails.LockKeysDetail != nil {
				if execDetails.LockKeysDetail.LockKeys > 0 {
					metrics.StatementLockKeysCount.Observe(float64(execDetails.LockKeysDetail.LockKeys))
				}
				if a.Ctx.GetSessionVars().StmtCtx.PessimisticLockStarted() && execDetails.LockKeysDetail.TotalTime > 0 {
					metrics.PessimisticLockKeysDuration.Observe(execDetails.LockKeysDetail.TotalTime.Seconds())
				}
			}
			if execDetails.SharedLockKeysDetail != nil {
				if execDetails.SharedLockKeysDetail.LockKeys > 0 {
					metrics.StatementSharedLockKeysCount.Observe(float64(execDetails.SharedLockKeysDetail.LockKeys))
				}
			}

			if err == nil && execDetails.LockKeysDetail != nil &&
				(execDetails.LockKeysDetail.AggressiveLockNewCount > 0 || execDetails.LockKeysDetail.AggressiveLockDerivedCount > 0) {
				a.Ctx.GetSessionVars().TxnCtx.FairLockingUsed = true
				// If this statement is finished when some of the keys are locked with conflict in the last retry, or
				// some of the keys are derived from the previous retry, we consider the optimization of fair locking
				// takes effect on this statement.
				if execDetails.LockKeysDetail.LockedWithConflictCount > 0 || execDetails.LockKeysDetail.AggressiveLockDerivedCount > 0 {
					a.Ctx.GetSessionVars().TxnCtx.FairLockingEffective = true
				}
			}
			return
		}
		recoverdErr, ok := r.(error)
		if !ok || !(exeerrors.ErrMemoryExceedForQuery.Equal(recoverdErr) ||
			exeerrors.ErrMemoryExceedForInstance.Equal(recoverdErr) ||
			exeerrors.ErrQueryInterrupted.Equal(recoverdErr) ||
			exeerrors.ErrMaxExecTimeExceeded.Equal(recoverdErr)) {
			panic(r)
		}
		err = recoverdErr
		logutil.Logger(ctx).Warn("execute sql panic", zap.String("sql", a.GetTextToLog(false)), zap.Stack("stack"))
	}()

	failpoint.Inject("assertStaleTSO", func(val failpoint.Value) {
		if n, ok := val.(int); ok && staleread.IsStmtStaleness(a.Ctx) {
			txnManager := sessiontxn.GetTxnManager(a.Ctx)
			ts, err := txnManager.GetStmtReadTS()
			if err != nil {
				panic(err)
			}
			startTS := oracle.ExtractPhysical(ts) / 1000
			if n != int(startTS) {
				panic(fmt.Sprintf("different tso %d != %d", n, startTS))
			}
		}
	})
	sctx := a.Ctx
	ctx = util.SetSessionID(ctx, sctx.GetSessionVars().ConnectionID)
	if _, ok := a.Plan.(*plannercore.Analyze); ok && sctx.GetSessionVars().InRestrictedSQL {
		oriStats, ok := sctx.GetSessionVars().GetSystemVar(vardef.TiDBBuildStatsConcurrency)
		if !ok {
			oriStats = strconv.Itoa(vardef.DefBuildStatsConcurrency)
		}
		oriScan := sctx.GetSessionVars().AnalyzeDistSQLScanConcurrency()
		oriIso, ok := sctx.GetSessionVars().GetSystemVar(vardef.TxnIsolation)
		if !ok {
			oriIso = "REPEATABLE-READ"
		}
		autoConcurrency, err1 := sctx.GetSessionVars().GetSessionOrGlobalSystemVar(ctx, vardef.TiDBAutoBuildStatsConcurrency)
		terror.Log(err1)
		if err1 == nil {
			terror.Log(sctx.GetSessionVars().SetSystemVar(vardef.TiDBBuildStatsConcurrency, autoConcurrency))
		}
		sVal, err2 := sctx.GetSessionVars().GetSessionOrGlobalSystemVar(ctx, vardef.TiDBSysProcScanConcurrency)
		terror.Log(err2)
		if err2 == nil {
			concurrency, err3 := strconv.ParseInt(sVal, 10, 64)
			terror.Log(err3)
			if err3 == nil {
				sctx.GetSessionVars().SetAnalyzeDistSQLScanConcurrency(int(concurrency))
			}
		}
		terror.Log(sctx.GetSessionVars().SetSystemVar(vardef.TxnIsolation, ast.ReadCommitted))
		defer func() {
			terror.Log(sctx.GetSessionVars().SetSystemVar(vardef.TiDBBuildStatsConcurrency, oriStats))
			sctx.GetSessionVars().SetAnalyzeDistSQLScanConcurrency(oriScan)
			terror.Log(sctx.GetSessionVars().SetSystemVar(vardef.TxnIsolation, oriIso))
		}()
	}

	if sctx.GetSessionVars().StmtCtx.HasMemQuotaHint {
		sctx.GetSessionVars().MemTracker.SetBytesLimit(sctx.GetSessionVars().StmtCtx.MemQuotaQuery)
	}

	// must set plan according to the `Execute` plan before getting planDigest
	a.inheritContextFromExecuteStmt()
	var rm *runaway.Manager
	dom := domain.GetDomain(sctx)
	if dom != nil {
		rm = dom.RunawayManager()
	}
	if vardef.EnableResourceControl.Load() && rm != nil {
		sessionVars := sctx.GetSessionVars()
		stmtCtx := sessionVars.StmtCtx
		_, planDigest := GetPlanDigest(stmtCtx)
		_, sqlDigest := stmtCtx.SQLDigest()
		stmtCtx.RunawayChecker = rm.DeriveChecker(stmtCtx.ResourceGroupName, stmtCtx.OriginalSQL, sqlDigest.String(), planDigest.String(), sessionVars.StartTime)
		switchGroupName, err := stmtCtx.RunawayChecker.BeforeExecutor()
		if err != nil {
			return nil, err
		}
		if len(switchGroupName) > 0 {
			stmtCtx.ResourceGroupName = switchGroupName
		}
	}
	ctx = a.observeStmtBeginForTopProfiling(ctx)

	// Record start time before buildExecutor() to include TSO waiting time in maxExecutionTime timeout.
	// buildExecutor() may block waiting for TSO, so we should start the timer earlier.
	cmd32 := atomic.LoadUint32(&sctx.GetSessionVars().CommandValue)
	cmd := byte(cmd32)
	var execStartTime time.Time
	var pi processinfoSetter
	if raw, ok := sctx.(processinfoSetter); ok {
		pi = raw
		execStartTime = time.Now()
	}

	e, err := a.buildExecutor()
	if err != nil {
		return nil, err
	}

	if pi != nil {
		sql := a.getSQLForProcessInfo()
		maxExecutionTime := sctx.GetSessionVars().GetMaxExecutionTime()
		// Update processinfo, ShowProcess() will use it.
		if a.Ctx.GetSessionVars().StmtCtx.StmtType == "" {
			a.Ctx.GetSessionVars().StmtCtx.StmtType = stmtctx.GetStmtLabel(ctx, a.StmtNode)
		}
		// Since maxExecutionTime is used only for SELECT statements, here we limit its scope.
		if !a.Ctx.GetSessionVars().StmtCtx.InSelectStmt {
			maxExecutionTime = 0
		}
		pi.SetProcessInfo(sql, execStartTime, cmd, maxExecutionTime)
	}

	breakpoint.Inject(a.Ctx, sessiontxn.BreakPointBeforeExecutorFirstRun)
	if err = a.openExecutor(ctx, e); err != nil {
		terror.Log(exec.Close(e))
		return nil, err
	}

	isPessimistic := sctx.GetSessionVars().TxnCtx.IsPessimistic

	if a.isSelectForUpdate {
		if sctx.GetSessionVars().UseLowResolutionTSO() {
			terror.Log(exec.Close(e))
			return nil, errors.New("can not execute select for update statement when 'tidb_low_resolution_tso' is set")
		}
		// Special handle for "select for update statement" in pessimistic transaction.
		if isPessimistic {
			return a.handlePessimisticSelectForUpdate(ctx, e)
		}
	}

	a.prepareFKCascadeContext(e)
	if handled, result, err := a.handleNoDelay(ctx, e, isPessimistic); handled || err != nil {
		return result, err
	}

	var txnStartTS uint64
	txn, err := sctx.Txn(false)
	if err != nil {
		terror.Log(exec.Close(e))
		return nil, err
	}
	if txn.Valid() {
		txnStartTS = txn.StartTS()
	}

	// Extract trace ID from context to store in recordSet for lazy execution
	traceID := tikvtrace.TraceIDFromContext(ctx)

	return &recordSet{
		executor:   e,
		schema:     e.Schema(),
		stmt:       a,
		txnStartTS: txnStartTS,
		traceID:    traceID,
	}, nil
}

func (a *ExecStmt) inheritContextFromExecuteStmt() {
	if executePlan, ok := a.Plan.(*plannercore.Execute); ok {
		a.Ctx.SetValue(sessionctx.QueryString, executePlan.Stmt.Text())
		a.OutputNames = executePlan.OutputNames()
		a.isPreparedStmt = true
		a.Plan = executePlan.Plan
		a.Ctx.GetSessionVars().StmtCtx.SetPlan(executePlan.Plan)
	}
}

func (a *ExecStmt) getSQLForProcessInfo() string {
	sql := a.Text()
	if simple, ok := a.Plan.(*plannercore.Simple); ok && simple.Statement != nil {
		if ss, ok := simple.Statement.(ast.SensitiveStmtNode); ok {
			// Use SecureText to avoid leak password information.
			sql = ss.SecureText()
		}
	} else if sn, ok2 := a.StmtNode.(ast.SensitiveStmtNode); ok2 {
		// such as import into statement
		sql = sn.SecureText()
	}
	return sql
}

func (a *ExecStmt) handleStmtForeignKeyTrigger(ctx context.Context, e exec.Executor) error {
	stmtCtx := a.Ctx.GetSessionVars().StmtCtx
	if stmtCtx.ForeignKeyTriggerCtx.HasFKCascades {
		// If the ExecStmt has foreign key cascade to be executed, we need call `StmtCommit` to commit the ExecStmt itself
		// change first.
		// Since `UnionScanExec` use `SnapshotIter` and `SnapshotGetter` to read txn mem-buffer, if we don't do `StmtCommit`,
		// then the fk cascade executor can't read the mem-buffer changed by the ExecStmt.
		a.Ctx.StmtCommit(ctx)
	}
	err := a.handleForeignKeyTrigger(ctx, e, 1)
	if err != nil {
		err1 := a.handleFKTriggerError(stmtCtx)
		if err1 != nil {
			return errors.Errorf("handle foreign key trigger error failed, err: %v, original_err: %v", err1, err)
		}
		return err
	}
	if stmtCtx.ForeignKeyTriggerCtx.SavepointName != "" {
		a.Ctx.GetSessionVars().TxnCtx.ReleaseSavepoint(stmtCtx.ForeignKeyTriggerCtx.SavepointName)
	}
	return nil
}

var maxForeignKeyCascadeDepth = 15

func (a *ExecStmt) handleForeignKeyTrigger(ctx context.Context, e exec.Executor, depth int) error {
	exec, ok := e.(WithForeignKeyTrigger)
	if !ok {
		return nil
	}
	fkChecks := exec.GetFKChecks()
	for _, fkCheck := range fkChecks {
		err := fkCheck.doCheck(ctx)
		if err != nil {
			return err
		}
	}
	fkCascades := exec.GetFKCascades()
	for _, fkCascade := range fkCascades {
		err := a.handleForeignKeyCascade(ctx, fkCascade, depth)
		if err != nil {
			return err
		}
	}
	return nil
}
