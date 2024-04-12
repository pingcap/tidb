// Copyright 2022 PingCAP, Inc.
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

package isolation

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util/logutil"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.uber.org/zap"
)

// PessimisticRRTxnContextProvider provides txn context for isolation level repeatable-read
type PessimisticRRTxnContextProvider struct {
	basePessimisticTxnContextProvider

	// Used for ForUpdateRead statement
	forUpdateTS       uint64
	latestForUpdateTS uint64
	// It may decide whether to update forUpdateTs when calling provider's getForUpdateTs
	// See more details in the comments of optimizeWithPlan
	optimizeForNotFetchingLatestTS bool
}

// NewPessimisticRRTxnContextProvider returns a new PessimisticRRTxnContextProvider
func NewPessimisticRRTxnContextProvider(sctx sessionctx.Context, causalConsistencyOnly bool) *PessimisticRRTxnContextProvider {
	provider := &PessimisticRRTxnContextProvider{
		basePessimisticTxnContextProvider: basePessimisticTxnContextProvider{
			baseTxnContextProvider: baseTxnContextProvider{
				sctx:                  sctx,
				causalConsistencyOnly: causalConsistencyOnly,
				onInitializeTxnCtx: func(txnCtx *variable.TransactionContext) {
					txnCtx.IsPessimistic = true
					txnCtx.Isolation = ast.RepeatableRead
				},
				onTxnActiveFunc: func(txn kv.Transaction, _ sessiontxn.EnterNewTxnType) {
					txn.SetOption(kv.Pessimistic, true)
				},
			},
		},
	}

	provider.getStmtReadTSFunc = provider.getTxnStartTS
	provider.getStmtForUpdateTSFunc = provider.getForUpdateTs

	return provider
}

func (p *PessimisticRRTxnContextProvider) getForUpdateTs() (ts uint64, err error) {
	if p.forUpdateTS != 0 {
		return p.forUpdateTS, nil
	}

	var txn kv.Transaction
	if txn, err = p.ActivateTxn(); err != nil {
		return 0, err
	}

	if p.optimizeForNotFetchingLatestTS {
		p.forUpdateTS = p.sctx.GetSessionVars().TxnCtx.GetForUpdateTS()
		return p.forUpdateTS, nil
	}

	txnCtx := p.sctx.GetSessionVars().TxnCtx
	futureTS := newOracleFuture(p.ctx, p.sctx, txnCtx.TxnScope)

	start := time.Now()
	if ts, err = futureTS.Wait(); err != nil {
		return 0, err
	}
	p.sctx.GetSessionVars().DurationWaitTS += time.Since(start)

	txnCtx.SetForUpdateTS(ts)
	txn.SetOption(kv.SnapshotTS, ts)

	p.forUpdateTS = ts

	return
}

// updateForUpdateTS acquires the latest TSO and update the TransactionContext and kv.Transaction with it.
func (p *PessimisticRRTxnContextProvider) updateForUpdateTS() (err error) {
	sctx := p.sctx
	var txn kv.Transaction

	if txn, err = sctx.Txn(false); err != nil {
		return err
	}

	if !txn.Valid() {
		return errors.Trace(kv.ErrInvalidTxn)
	}

	failpoint.Inject("RequestTsoFromPD", func() {
		sessiontxn.TsoRequestCountInc(sctx)
	})

	// Because the ForUpdateTS is used for the snapshot for reading data in DML.
	// We can avoid allocating a global TSO here to speed it up by using the local TSO.
	version, err := sctx.GetStore().CurrentVersion(sctx.GetSessionVars().TxnCtx.TxnScope)
	if err != nil {
		return err
	}

	sctx.GetSessionVars().TxnCtx.SetForUpdateTS(version.Ver)
	p.latestForUpdateTS = version.Ver
	txn.SetOption(kv.SnapshotTS, version.Ver)

	return nil
}

// OnStmtStart is the hook that should be called when a new statement started
func (p *PessimisticRRTxnContextProvider) OnStmtStart(ctx context.Context, node ast.StmtNode) error {
	if err := p.basePessimisticTxnContextProvider.OnStmtStart(ctx, node); err != nil {
		return err
	}

	p.forUpdateTS = 0
	p.optimizeForNotFetchingLatestTS = false

	return nil
}

// OnStmtRetry is the hook that should be called when a statement is retried internally.
func (p *PessimisticRRTxnContextProvider) OnStmtRetry(ctx context.Context) (err error) {
	if err = p.basePessimisticTxnContextProvider.OnStmtRetry(ctx); err != nil {
		return err
	}

	// If TxnCtx.forUpdateTS is updated in OnStmtErrorForNextAction, we assign the value to the provider
	if p.latestForUpdateTS > p.forUpdateTS {
		p.forUpdateTS = p.latestForUpdateTS
	} else {
		p.forUpdateTS = 0
	}

	p.optimizeForNotFetchingLatestTS = false

	return nil
}

// OnStmtErrorForNextAction is the hook that should be called when a new statement get an error
func (p *PessimisticRRTxnContextProvider) OnStmtErrorForNextAction(ctx context.Context, point sessiontxn.StmtErrorHandlePoint, err error) (sessiontxn.StmtErrorAction, error) {
	switch point {
	case sessiontxn.StmtErrAfterPessimisticLock:
		return p.handleAfterPessimisticLockError(ctx, err)
	default:
		return sessiontxn.NoIdea()
	}
}

// AdviseOptimizeWithPlan optimizes for update point get related execution.
// Use case: In for update point get related operations, we do not fetch ts from PD but use the last ts we fetched.
//
//	We expect that the data that the point get acquires has not been changed.
//
// Benefit: Save the cost of acquiring ts from PD.
// Drawbacks: If the data has been changed since the ts we used, we need to retry.
// One exception is insert operation, when it has no select plan, we do not fetch the latest ts immediately. We only update ts
// if write conflict is incurred.
func (p *PessimisticRRTxnContextProvider) AdviseOptimizeWithPlan(val any) (err error) {
	if p.isTidbSnapshotEnabled() || p.isBeginStmtWithStaleRead() {
		return nil
	}

	plan, ok := val.(base.Plan)
	if !ok {
		return nil
	}

	if execute, ok := plan.(*plannercore.Execute); ok {
		plan = execute.Plan
	}

	p.optimizeForNotFetchingLatestTS = notNeedGetLatestTSFromPD(plan, false)

	return nil
}

// notNeedGetLatestTSFromPD searches for optimization condition recursively
// Note: For point get and batch point get (name it plan), if one of the ancestor node is update/delete/physicalLock,
// we should check whether the plan.Lock is true or false. See comments in needNotToBeOptimized.
// inLockOrWriteStmt = true means one of the ancestor node is update/delete/physicalLock.
func notNeedGetLatestTSFromPD(plan base.Plan, inLockOrWriteStmt bool) bool {
	switch v := plan.(type) {
	case *plannercore.PointGetPlan:
		// We do not optimize the point get/ batch point get if plan.lock = false and inLockOrWriteStmt = true.
		// Theoretically, the plan.lock should be true if the flag is true. But due to the bug describing in Issue35524,
		// the plan.lock can be false in the case of inLockOrWriteStmt being true. In this case, optimization here can lead to different results
		// which cannot be accepted as AdviseOptimizeWithPlan cannot change results.
		return !inLockOrWriteStmt || v.Lock
	case *plannercore.BatchPointGetPlan:
		return !inLockOrWriteStmt || v.Lock
	case base.PhysicalPlan:
		if len(v.Children()) == 0 {
			return false
		}
		_, isPhysicalLock := v.(*plannercore.PhysicalLock)
		for _, p := range v.Children() {
			if !notNeedGetLatestTSFromPD(p, isPhysicalLock || inLockOrWriteStmt) {
				return false
			}
		}
		return true
	case *plannercore.Update:
		return notNeedGetLatestTSFromPD(v.SelectPlan, true)
	case *plannercore.Delete:
		return notNeedGetLatestTSFromPD(v.SelectPlan, true)
	case *plannercore.Insert:
		return v.SelectPlan == nil
	}
	return false
}

func (p *PessimisticRRTxnContextProvider) handleAfterPessimisticLockError(ctx context.Context, lockErr error) (sessiontxn.StmtErrorAction, error) {
	sessVars := p.sctx.GetSessionVars()
	txnCtx := sessVars.TxnCtx

	if deadlock, ok := errors.Cause(lockErr).(*tikverr.ErrDeadlock); ok {
		if !deadlock.IsRetryable {
			return sessiontxn.ErrorAction(lockErr)
		}

		logutil.Logger(p.ctx).Info("single statement deadlock, retry statement",
			zap.Uint64("txn", txnCtx.StartTS),
			zap.Uint64("lockTS", deadlock.LockTs),
			zap.Stringer("lockKey", kv.Key(deadlock.LockKey)),
			zap.Uint64("deadlockKeyHash", deadlock.DeadlockKeyHash))

		// In fair locking mode, when statement retry happens, `retryFairLockingIfNeeded` should be
		// called to make its state ready for retrying. But single-statement deadlock is an exception. We need to exit
		// fair locking in single-statement-deadlock case, otherwise the lock this statement has acquired won't be
		// released after retrying, so it still blocks another transaction and the deadlock won't be resolved.
		if err := p.cancelFairLockingIfNeeded(ctx); err != nil {
			return sessiontxn.ErrorAction(err)
		}
	} else if terror.ErrorEqual(kv.ErrWriteConflict, lockErr) {
		// Always update forUpdateTS by getting a new timestamp from PD.
		// If we use the conflict commitTS as the new forUpdateTS and async commit
		// is used, the commitTS of this transaction may exceed the max timestamp
		// that PD allocates. Then, the change may be invisible to a new transaction,
		// which means linearizability is broken.
		errStr := lockErr.Error()
		forUpdateTS := txnCtx.GetForUpdateTS()

		logutil.Logger(p.ctx).Debug("pessimistic write conflict, retry statement",
			zap.Uint64("txn", txnCtx.StartTS),
			zap.Uint64("forUpdateTS", forUpdateTS),
			zap.String("err", errStr))
	} else {
		// This branch: if err is not nil, always update forUpdateTS to avoid problem described below.
		// For nowait, when ErrLock happened, ErrLockAcquireFailAndNoWaitSet will be returned, and in the same txn
		// the select for updateTs must be updated, otherwise there maybe rollback problem.
		// 		   begin
		// 		   select for update key1 (here encounters ErrLocked or other errors (or max_execution_time like util),
		//         					     key1 lock has not gotten and async rollback key1 is raised)
		//         select for update key1 again (this time lock is acquired successfully (maybe lock was released by others))
		//         the async rollback operation rollbacks the lock just acquired
		if err := p.updateForUpdateTS(); err != nil {
			logutil.Logger(p.ctx).Warn("UpdateForUpdateTS failed", zap.Error(err))
		}

		return sessiontxn.ErrorAction(lockErr)
	}

	if err := p.updateForUpdateTS(); err != nil {
		return sessiontxn.ErrorAction(lockErr)
	}

	if err := p.retryFairLockingIfNeeded(ctx); err != nil {
		return sessiontxn.ErrorAction(err)
	}
	return sessiontxn.RetryReady()
}
