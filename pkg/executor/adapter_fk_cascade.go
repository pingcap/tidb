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
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
)

// handleForeignKeyCascade uses to execute foreign key cascade behaviour, the progress is:
//  1. Build delete/update executor for foreign key on delete/update behaviour.
//     a. Construct delete/update AST. We used to try generated SQL string first and then parse the SQL to get AST,
//     but we need convert Datum to string, there may be some risks here, since assert_eq(datum_a, parse(datum_a.toString())) may be broken.
//     so we chose to construct AST directly.
//     b. Build plan by the delete/update AST.
//     c. Build executor by the delete/update plan.
//  2. Execute the delete/update executor.
//  3. Close the executor.
//  4. `StmtCommit` to commit the kv change to transaction mem-buffer.
//  5. If the foreign key cascade behaviour has more fk value need to be cascaded, go to step 1.
func (a *ExecStmt) handleForeignKeyCascade(ctx context.Context, fkc *FKCascadeExec, depth int) error {
	if a.Ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil {
		fkc.stats = &FKCascadeRuntimeStats{}
		defer a.Ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.RegisterStats(fkc.plan.ID(), fkc.stats)
	}
	if len(fkc.fkValues) == 0 && len(fkc.fkUpdatedValuesMap) == 0 {
		return nil
	}
	if depth > maxForeignKeyCascadeDepth {
		return exeerrors.ErrForeignKeyCascadeDepthExceeded.GenWithStackByArgs(maxForeignKeyCascadeDepth)
	}
	a.Ctx.GetSessionVars().StmtCtx.InHandleForeignKeyTrigger = true
	defer func() {
		a.Ctx.GetSessionVars().StmtCtx.InHandleForeignKeyTrigger = false
	}()
	if fkc.stats != nil {
		start := time.Now()
		defer func() {
			fkc.stats.Total += time.Since(start)
		}()
	}
	for {
		e, err := fkc.buildExecutor(ctx)
		if err != nil || e == nil {
			return err
		}
		if err := exec.Open(ctx, e); err != nil {
			terror.Log(exec.Close(e))
			return err
		}
		err = exec.Next(ctx, e, exec.NewFirstChunk(e))
		failpoint.Inject("handleForeignKeyCascadeError", func(val failpoint.Value) {
			// Next can recover panic and convert it to error. So we inject error directly here.
			if val.(bool) && err == nil {
				err = errors.New("handleForeignKeyCascadeError")
			}
		})
		closeErr := exec.Close(e)
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return err
		}
		// Call `StmtCommit` uses to flush the fk cascade executor change into txn mem-buffer,
		// then the later fk cascade executors can see the mem-buffer changes.
		a.Ctx.StmtCommit(ctx)
		err = a.handleForeignKeyTrigger(ctx, e, depth+1)
		if err != nil {
			return err
		}
	}
}

// prepareFKCascadeContext records a transaction savepoint for foreign key cascade when this ExecStmt has foreign key
// cascade behaviour and this ExecStmt is in transaction.
func (a *ExecStmt) prepareFKCascadeContext(e exec.Executor) {
	var execWithFKTrigger WithForeignKeyTrigger
	if explain, ok := e.(*ExplainExec); ok {
		execWithFKTrigger = explain.getAnalyzeExecWithForeignKeyTrigger()
	} else {
		execWithFKTrigger, _ = e.(WithForeignKeyTrigger)
	}
	if execWithFKTrigger == nil || !execWithFKTrigger.HasFKCascades() {
		return
	}
	sessVar := a.Ctx.GetSessionVars()
	sessVar.StmtCtx.ForeignKeyTriggerCtx.HasFKCascades = true
	if !sessVar.InTxn() {
		return
	}
	txn, err := a.Ctx.Txn(false)
	if err != nil || !txn.Valid() {
		return
	}
	// Record a txn savepoint if ExecStmt in transaction, the savepoint is use to do rollback when handle foreign key
	// cascade failed.
	savepointName := "fk_sp_" + strconv.FormatUint(txn.StartTS(), 10)
	memDBCheckpoint := txn.GetMemDBCheckpoint()
	sessVar.TxnCtx.AddSavepoint(savepointName, memDBCheckpoint)
	sessVar.StmtCtx.ForeignKeyTriggerCtx.SavepointName = savepointName
}

func (a *ExecStmt) handleFKTriggerError(sc *stmtctx.StatementContext) error {
	if sc.ForeignKeyTriggerCtx.SavepointName == "" {
		return nil
	}
	txn, err := a.Ctx.Txn(false)
	if err != nil || !txn.Valid() {
		return err
	}
	savepointRecord := a.Ctx.GetSessionVars().TxnCtx.RollbackToSavepoint(sc.ForeignKeyTriggerCtx.SavepointName)
	if savepointRecord == nil {
		// Normally should never run into here, but just in case, rollback the transaction.
		err = txn.Rollback()
		if err != nil {
			return err
		}
		return errors.Errorf("foreign key cascade savepoint '%s' not found, transaction is rollback, should never happen", sc.ForeignKeyTriggerCtx.SavepointName)
	}
	txn.RollbackMemDBToCheckpoint(savepointRecord.MemDBCheckpoint)
	a.Ctx.GetSessionVars().TxnCtx.ReleaseSavepoint(sc.ForeignKeyTriggerCtx.SavepointName)
	return nil
}

// buildExecutor build an executor from plan, prepared statement may need additional procedure.
func (a *ExecStmt) buildExecutor() (exec.Executor, error) {
	defer func(start time.Time) { a.phaseBuildDurations[0] += time.Since(start) }(time.Now())
	ctx := a.Ctx
	stmtCtx := ctx.GetSessionVars().StmtCtx
	if _, ok := a.Plan.(*plannercore.Execute); !ok {
		if stmtCtx.Priority == mysql.NoPriority && a.LowerPriority {
			stmtCtx.Priority = kv.PriorityLow
		}
	}
	if _, ok := a.Plan.(*plannercore.Analyze); ok && ctx.GetSessionVars().InRestrictedSQL {
		ctx.GetSessionVars().StmtCtx.Priority = kv.PriorityLow
	}

	b := newExecutorBuilder(ctx, a.InfoSchema, a.Ti)
	e := b.build(a.Plan)
	if b.err != nil {
		return nil, errors.Trace(b.err)
	}

	failpoint.Inject("assertTxnManagerAfterBuildExecutor", func() {
		sessiontxn.RecordAssert(a.Ctx, "assertTxnManagerAfterBuildExecutor", true)
		sessiontxn.AssertTxnManagerInfoSchema(b.ctx, b.is)
	})

	// ExecuteExec is not a real Executor, we only use it to build another Executor from a prepared statement.
	if executorExec, ok := e.(*ExecuteExec); ok {
		err := executorExec.Build(b)
		if err != nil {
			return nil, err
		}
		if executorExec.lowerPriority {
			ctx.GetSessionVars().StmtCtx.Priority = kv.PriorityLow
		}
		e = executorExec.stmtExec
	}
	a.isSelectForUpdate = b.hasLock && (!stmtCtx.InDeleteStmt && !stmtCtx.InUpdateStmt && !stmtCtx.InInsertStmt)
	return e, nil
}

func (a *ExecStmt) openExecutor(ctx context.Context, e exec.Executor) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = util2.GetRecoverError(r)
		}
	}()
	start := time.Now()
	err = exec.Open(ctx, e)
	a.phaseOpenDurations[0] += time.Since(start)
	return err
}

func (a *ExecStmt) next(ctx context.Context, e exec.Executor, req *chunk.Chunk) error {
	start := time.Now()
	err := exec.Next(ctx, e, req)
	a.phaseNextDurations[0] += time.Since(start)
	return err
}


