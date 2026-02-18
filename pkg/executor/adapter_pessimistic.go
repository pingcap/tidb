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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	executor_metrics "github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/breakpoint"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/tracing"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

func (a *ExecStmt) handleNoDelay(ctx context.Context, e exec.Executor, isPessimistic bool) (handled bool, rs sqlexec.RecordSet, err error) {
	sc := a.Ctx.GetSessionVars().StmtCtx
	defer func() {
		// If the stmt have no rs like `insert`, The session tracker detachment will be directly
		// done in the `defer` function. If the rs is not nil, the detachment will be done in
		// `rs.Close` in `handleStmt`
		if handled && sc != nil && rs == nil {
			sc.DetachMemDiskTracker()
			cteErr := resetCTEStorageMap(a.Ctx)
			if err == nil {
				// Only overwrite err when it's nil.
				err = cteErr
			}
		}
	}()

	toCheck := e
	isExplainAnalyze := false
	if explain, ok := e.(*ExplainExec); ok {
		if analyze := explain.getAnalyzeExecToExecutedNoDelay(); analyze != nil {
			toCheck = analyze
			isExplainAnalyze = true
			a.Ctx.GetSessionVars().StmtCtx.IsExplainAnalyzeDML = isExplainAnalyze
		}
	}

	// If the executor doesn't return any result to the client, we execute it without delay.
	if toCheck.Schema().Len() == 0 {
		handled = !isExplainAnalyze
		if isPessimistic {
			err := a.handlePessimisticDML(ctx, toCheck)
			return handled, nil, err
		}
		r, err := a.handleNoDelayExecutor(ctx, toCheck)
		return handled, r, err
	} else if proj, ok := toCheck.(*ProjectionExec); ok && proj.calculateNoDelay {
		// Currently this is only for the "DO" statement. Take "DO 1, @a=2;" as an example:
		// the Projection has two expressions and two columns in the schema, but we should
		// not return the result of the two expressions.
		r, err := a.handleNoDelayExecutor(ctx, e)
		return true, r, err
	}

	return false, nil, nil
}

func isNoResultPlan(p base.Plan) bool {
	if p.Schema().Len() == 0 {
		return true
	}

	// Currently this is only for the "DO" statement. Take "DO 1, @a=2;" as an example:
	// the Projection has two expressions and two columns in the schema, but we should
	// not return the result of the two expressions.
	switch raw := p.(type) {
	case *logicalop.LogicalProjection:
		if raw.CalculateNoDelay {
			return true
		}
	case *physicalop.PhysicalProjection:
		if raw.CalculateNoDelay {
			return true
		}
	}
	return false
}

type chunkRowRecordSet struct {
	rows     []chunk.Row
	idx      int
	fields   []*resolve.ResultField
	e        exec.Executor
	execStmt *ExecStmt
}

func (c *chunkRowRecordSet) Fields() []*resolve.ResultField {
	if c.fields == nil {
		c.fields = colNames2ResultFields(c.e.Schema(), c.execStmt.OutputNames, c.execStmt.Ctx.GetSessionVars().CurrentDB)
	}
	return c.fields
}

func (c *chunkRowRecordSet) Next(_ context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if !chk.IsFull() && c.idx < len(c.rows) {
		numToAppend := min(len(c.rows)-c.idx, chk.RequiredRows()-chk.NumRows())
		chk.AppendRows(c.rows[c.idx : c.idx+numToAppend])
		c.idx += numToAppend
	}
	return nil
}

func (c *chunkRowRecordSet) NewChunk(alloc chunk.Allocator) *chunk.Chunk {
	if alloc == nil {
		return exec.NewFirstChunk(c.e)
	}

	return alloc.Alloc(c.e.RetFieldTypes(), c.e.InitCap(), c.e.MaxChunkSize())
}

func (c *chunkRowRecordSet) Close() error {
	c.execStmt.CloseRecordSet(c.execStmt.Ctx.GetSessionVars().TxnCtx.StartTS, nil)
	return nil
}

func (a *ExecStmt) handlePessimisticSelectForUpdate(ctx context.Context, e exec.Executor) (_ sqlexec.RecordSet, retErr error) {
	if snapshotTS := a.Ctx.GetSessionVars().SnapshotTS; snapshotTS != 0 {
		terror.Log(exec.Close(e))
		return nil, errors.New("can not execute write statement when 'tidb_snapshot' is set")
	}

	txnManager := sessiontxn.GetTxnManager(a.Ctx)
	err := txnManager.OnPessimisticStmtStart(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		isSuccessful := retErr == nil
		err1 := txnManager.OnPessimisticStmtEnd(ctx, isSuccessful)
		if retErr == nil && err1 != nil {
			retErr = err1
		}
	}()

	isFirstAttempt := true

	for {
		startTime := time.Now()
		rs, err := a.runPessimisticSelectForUpdate(ctx, e)

		if isFirstAttempt {
			executor_metrics.SelectForUpdateFirstAttemptDuration.Observe(time.Since(startTime).Seconds())
			isFirstAttempt = false
		} else {
			executor_metrics.SelectForUpdateRetryDuration.Observe(time.Since(startTime).Seconds())
		}

		e, err = a.handlePessimisticLockError(ctx, err)
		if err != nil {
			return nil, err
		}
		if e == nil {
			return rs, nil
		}

		failpoint.Inject("pessimisticSelectForUpdateRetry", nil)
	}
}

func (a *ExecStmt) runPessimisticSelectForUpdate(ctx context.Context, e exec.Executor) (sqlexec.RecordSet, error) {
	defer func() {
		terror.Log(exec.Close(e))
	}()
	var rows []chunk.Row
	var err error
	req := exec.TryNewCacheChunk(e)
	for {
		err = a.next(ctx, e, req)
		if err != nil {
			// Handle 'write conflict' error.
			break
		}
		if req.NumRows() == 0 {
			return &chunkRowRecordSet{rows: rows, e: e, execStmt: a}, nil
		}
		iter := chunk.NewIterator4Chunk(req)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req = chunk.Renew(req, a.Ctx.GetSessionVars().MaxChunkSize)
	}
	return nil, err
}

func (a *ExecStmt) handleNoDelayExecutor(ctx context.Context, e exec.Executor) (sqlexec.RecordSet, error) {
	sctx := a.Ctx
	r, ctx := tracing.StartRegionEx(ctx, "executor.handleNoDelayExecutor")
	defer r.End()

	var err error
	defer func() {
		terror.Log(exec.Close(e))
		a.logAudit()
	}()

	// Check if "tidb_snapshot" is set for the write executors.
	// In history read mode, we can not do write operations.
	// TODO: it's better to use a.ReadOnly to check if the statement is a write statement
	// instead of listing executor types here.
	switch e.(type) {
	case *DeleteExec, *InsertExec, *UpdateExec, *ReplaceExec, *LoadDataExec, *DDLExec, *ImportIntoExec:
		snapshotTS := sctx.GetSessionVars().SnapshotTS
		if snapshotTS != 0 {
			return nil, errors.New("can not execute write statement when 'tidb_snapshot' is set")
		}
		if sctx.GetSessionVars().UseLowResolutionTSO() {
			return nil, errors.New("can not execute write statement when 'tidb_low_resolution_tso' is set")
		}
	}

	err = a.next(ctx, e, exec.TryNewCacheChunk(e))
	if err != nil {
		return nil, err
	}
	err = a.handleStmtForeignKeyTrigger(ctx, e)
	return nil, err
}

func (a *ExecStmt) handlePessimisticDML(ctx context.Context, e exec.Executor) (err error) {
	sctx := a.Ctx
	// Do not activate the transaction here.
	// When autocommit = 0 and transaction in pessimistic mode,
	// statements like set xxx = xxx; should not active the transaction.
	txn, err := sctx.Txn(false)
	if err != nil {
		return err
	}
	txnCtx := sctx.GetSessionVars().TxnCtx
	defer func() {
		if err != nil && !sctx.GetSessionVars().ConstraintCheckInPlacePessimistic && sctx.GetSessionVars().InTxn() {
			// If it's not a retryable error, rollback current transaction instead of rolling back current statement like
			// in normal transactions, because we cannot locate and rollback the statement that leads to the lock error.
			// This is too strict, but since the feature is not for everyone, it's the easiest way to guarantee safety.
			stmtText := parser.Normalize(a.Text(), sctx.GetSessionVars().EnableRedactLog)
			logutil.Logger(ctx).Info("Transaction abort for the safety of lazy uniqueness check. "+
				"Note this may not be a uniqueness violation.",
				zap.Error(err),
				zap.String("statement", stmtText),
				zap.Uint64("conn", sctx.GetSessionVars().ConnectionID),
				zap.Uint64("txnStartTS", txnCtx.StartTS),
				zap.Uint64("forUpdateTS", txnCtx.GetForUpdateTS()),
			)
			sctx.GetSessionVars().SetInTxn(false)
			err = exeerrors.ErrLazyUniquenessCheckFailure.GenWithStackByArgs(err.Error())
		}
	}()

	txnManager := sessiontxn.GetTxnManager(a.Ctx)
	err = txnManager.OnPessimisticStmtStart(ctx)
	if err != nil {
		return err
	}
	defer func() {
		isSuccessful := err == nil
		err1 := txnManager.OnPessimisticStmtEnd(ctx, isSuccessful)
		if err == nil && err1 != nil {
			err = err1
		}
	}()

	tryLockKeys := func(e exec.Executor, keys []kv.Key, shared bool) (exec.Executor, error) {
		seVars := sctx.GetSessionVars()
		keys = filterTemporaryTableKeys(seVars, keys)
		keys = filterLockTableKeys(seVars.StmtCtx, keys)
		if len(keys) == 0 {
			return nil, nil
		}
		lockCtx, err := newLockCtx(sctx, seVars.LockWaitTimeout, len(keys), shared)
		if err != nil {
			return nil, err
		}
		var lockKeyStats *util.LockKeysDetails
		ctx = context.WithValue(ctx, util.LockKeysDetailCtxKey, &lockKeyStats)
		startLocking := time.Now()
		err = txn.LockKeys(ctx, lockCtx, keys...)
		a.phaseLockDurations[0] += time.Since(startLocking)
		if e.RuntimeStats() != nil {
			e.RuntimeStats().Record(time.Since(startLocking), 0)
		}
		if lockKeyStats != nil {
			if shared {
				seVars.StmtCtx.MergeSharedLockKeysExecDetails(lockKeyStats)
			} else {
				seVars.StmtCtx.MergeLockKeysExecDetails(lockKeyStats)
			}
		}
		if err == nil {
			return nil, nil
		}
		e, err = a.handlePessimisticLockError(ctx, err)
		if err != nil {
			if exeerrors.ErrDeadlock.Equal(err) {
				metrics.StatementDeadlockDetectDuration.Observe(time.Since(startLocking).Seconds())
			}
			return nil, err
		}
		return e, nil
	}
	isFirstAttempt := true

	for {
		if !isFirstAttempt {
			failpoint.Inject("pessimisticDMLRetry", nil)
		}

		txnCtx.ResetUnchangedKeysForLock()
		startTime := time.Now()
		_, err = a.handleNoDelayExecutor(ctx, e)
		if !txn.Valid() {
			return err
		}

		if isFirstAttempt {
			executor_metrics.DmlFirstAttemptDuration.Observe(time.Since(startTime).Seconds())
			isFirstAttempt = false
		} else {
			executor_metrics.DmlRetryDuration.Observe(time.Since(startTime).Seconds())
		}

		if err != nil {
			// It is possible the DML has point get plan that locks the key.
			e, err = a.handlePessimisticLockError(ctx, err)
			if err != nil {
				if exeerrors.ErrDeadlock.Equal(err) {
					metrics.StatementDeadlockDetectDuration.Observe(time.Since(startTime).Seconds())
				}
				return err
			}
			continue
		}

		keys, err1 := txn.(pessimisticTxn).KeysNeedToLock()
		if err1 != nil {
			return err1
		}

		if !a.Ctx.GetSessionVars().ForeignKeyCheckInSharedLock {
			// When `tidb_foreign_key_check_in_shared_lock` is off, lock all keys in exclusive mode
			keys = txnCtx.CollectUnchangedKeysForXLock(keys)
			keys = txnCtx.CollectUnchangedKeysForSLock(keys)
			startLock := time.Now()
			ex, err := tryLockKeys(e, keys, false)
			if err != nil {
				return err
			}
			if ex != nil {
				e = ex
				continue
			}
			updateFKCheckLockStats(e, time.Since(startLock))
			return nil
		}

		// When `tidb_foreign_key_check_in_shared_lock` is on, lock keys in two phases:
		//
		//   1. acquire exclusive locks for keys that need exclusive locks
		//   2. acquire shared locks for keys that need shared locks

		// acquire xlocks
		keys = txnCtx.CollectUnchangedKeysForXLock(keys)
		if ex, err := tryLockKeys(e, keys, false); err != nil {
			return err
		} else if ex != nil {
			e = ex
			continue
		}

		// acquire slocks
		keys = txnCtx.CollectUnchangedKeysForSLock(keys[:0])
		startLock := time.Now()
		if ex, err := tryLockKeys(e, keys, true); err != nil {
			return err
		} else if ex != nil {
			e = ex
			continue
		}
		updateFKCheckLockStats(e, time.Since(startLock))

		return nil
	}
}

// updateFKCheckLockStats updates the Lock stats of FK check executors after the deferred
// pessimistic lock phase completes. In pessimistic mode, FK check keys are not locked
// inline during doCheck but deferred to handlePessimisticDML. This function attributes
// the lock duration back to FK check runtime stats.
func updateFKCheckLockStats(e exec.Executor, lockDur time.Duration) {
	fkExec, ok := e.(WithForeignKeyTrigger)
	if !ok {
		return
	}
	for _, fkCheck := range fkExec.GetFKChecks() {
		if fkCheck.stats != nil {
			fkCheck.stats.Lock = lockDur
			fkCheck.stats.Total += lockDur
		}
	}
}

// handlePessimisticLockError updates TS and rebuild executor if the err is write conflict.
func (a *ExecStmt) handlePessimisticLockError(ctx context.Context, lockErr error) (_ exec.Executor, err error) {
	if lockErr == nil {
		return nil, nil
	}
	failpoint.Inject("assertPessimisticLockErr", func() {
		if terror.ErrorEqual(kv.ErrWriteConflict, lockErr) {
			sessiontxn.AddAssertEntranceForLockError(a.Ctx, "errWriteConflict")
		} else if terror.ErrorEqual(kv.ErrKeyExists, lockErr) {
			sessiontxn.AddAssertEntranceForLockError(a.Ctx, "errDuplicateKey")
		}
	})

	defer func() {
		if _, ok := errors.Cause(err).(*tikverr.ErrDeadlock); ok {
			err = exeerrors.ErrDeadlock
		}
	}()

	txnManager := sessiontxn.GetTxnManager(a.Ctx)
	action, err := txnManager.OnStmtErrorForNextAction(ctx, sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	if err != nil {
		return nil, err
	}

	if action != sessiontxn.StmtActionRetryReady {
		return nil, lockErr
	}

	if a.retryCount >= config.GetGlobalConfig().PessimisticTxn.MaxRetryCount {
		return nil, errors.New("pessimistic lock retry limit reached")
	}
	a.retryCount++
	a.retryStartTime = time.Now()

	err = txnManager.OnStmtRetry(ctx)
	if err != nil {
		return nil, err
	}

	// Without this line of code, the result will still be correct. But it can ensure that the update time of for update read
	// is determined which is beneficial for testing.
	if _, err = txnManager.GetStmtForUpdateTS(); err != nil {
		return nil, err
	}

	breakpoint.Inject(a.Ctx, sessiontxn.BreakPointOnStmtRetryAfterLockError)

	a.resetPhaseDurations()

	a.inheritContextFromExecuteStmt()
	e, err := a.buildExecutor()
	if err != nil {
		return nil, err
	}
	// Rollback the statement change before retry it.
	a.Ctx.StmtRollback(ctx, true)
	a.Ctx.GetSessionVars().StmtCtx.ResetForRetry()
	a.Ctx.GetSessionVars().RetryInfo.ResetOffset()

	failpoint.Inject("assertTxnManagerAfterPessimisticLockErrorRetry", func() {
		sessiontxn.RecordAssert(a.Ctx, "assertTxnManagerAfterPessimisticLockErrorRetry", true)
	})

	if err = a.openExecutor(ctx, e); err != nil {
		return nil, err
	}
	return e, nil
}

type pessimisticTxn interface {
	kv.Transaction
	// KeysNeedToLock returns the keys need to be locked.
	KeysNeedToLock() ([]kv.Key, error)
}

