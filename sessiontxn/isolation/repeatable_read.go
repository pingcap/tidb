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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/util/logutil"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.uber.org/zap"
)

// PessimisticRRTxnContextProvider provides txn context for isolation level repeatable-read
type PessimisticRRTxnContextProvider struct {
	baseTxnContextProvider

	// Used for ForUpdateRead statement
	forUpdateTS uint64
	// It may decide whether to update forUpdateTs when calling provider's getForUpdateTs
	// See more details in the comments of optimizeWithPlan
	followingOperatorIsPointGetForUpdate bool
}

// NewPessimisticRRTxnContextProvider returns a new PessimisticRRTxnContextProvider
func NewPessimisticRRTxnContextProvider(sctx sessionctx.Context, causalConsistencyOnly bool) *PessimisticRRTxnContextProvider {
	provider := &PessimisticRRTxnContextProvider{
		baseTxnContextProvider: baseTxnContextProvider{
			sctx:                  sctx,
			causalConsistencyOnly: causalConsistencyOnly,
			onInitializeTxnCtx: func(txnCtx *variable.TransactionContext) {
				txnCtx.IsPessimistic = true
				txnCtx.Isolation = ast.RepeatableRead
			},
			onTxnActive: func(txn kv.Transaction) {
				txn.SetOption(kv.Pessimistic, true)
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

	if p.followingOperatorIsPointGetForUpdate {
		p.forUpdateTS = p.sctx.GetSessionVars().TxnCtx.GetForUpdateTS()
		return p.forUpdateTS, nil
	}

	txnCtx := p.sctx.GetSessionVars().TxnCtx
	futureTS := sessiontxn.NewOracleFuture(p.ctx, p.sctx, txnCtx.TxnScope)

	if ts, err = futureTS.Wait(); err != nil {
		return 0, err
	}

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

	// Because the ForUpdateTS is used for the snapshot for reading data in DML.
	// We can avoid allocating a global TSO here to speed it up by using the local TSO.
	version, err := sctx.GetStore().CurrentVersion(sctx.GetSessionVars().TxnCtx.TxnScope)
	if err != nil {
		return err
	}

	sctx.GetSessionVars().TxnCtx.SetForUpdateTS(version.Ver)
	txn.SetOption(kv.SnapshotTS, sctx.GetSessionVars().TxnCtx.GetForUpdateTS())

	return nil
}

// OnStmtStart is the hook that should be called when a new statement started
func (p *PessimisticRRTxnContextProvider) OnStmtStart(ctx context.Context) error {
	if err := p.baseTxnContextProvider.OnStmtStart(ctx); err != nil {
		return err
	}

	p.forUpdateTS = 0
	p.followingOperatorIsPointGetForUpdate = false

	return nil
}

// OnStmtRetry is the hook that should be called when a statement is retried internally.
func (p *PessimisticRRTxnContextProvider) OnStmtRetry(ctx context.Context) (err error) {
	if err = p.baseTxnContextProvider.OnStmtRetry(ctx); err != nil {
		return err
	}

	txnCtxForUpdateTS := p.sctx.GetSessionVars().TxnCtx.GetForUpdateTS()
	// If TxnCtx.forUpdateTS is updated in OnStmtErrorForNextAction, we assign the value to the provider
	if txnCtxForUpdateTS > p.forUpdateTS {
		p.forUpdateTS = txnCtxForUpdateTS
	} else {
		p.forUpdateTS = 0
	}

	p.followingOperatorIsPointGetForUpdate = false

	return nil
}

// OnStmtErrorForNextAction is the hook that should be called when a new statement get an error
func (p *PessimisticRRTxnContextProvider) OnStmtErrorForNextAction(point sessiontxn.StmtErrorHandlePoint, err error) (sessiontxn.StmtErrorAction, error) {
	switch point {
	case sessiontxn.StmtErrAfterPessimisticLock:
		return p.handleAfterPessimisticLockError(err)
	default:
		return sessiontxn.NoIdea()
	}
}

// AdviseOptimizeWithPlan optimizes for update point get related execution.
// Use case: In for update point get related operations, we do not fetch ts from PD but use the last ts we fetched.
//     We expect that the data that the point get acquires has not been changed.
// Benefit: Save the cost of acquiring ts from PD.
// Drawbacks: If the data has been changed since the ts we used, we need to retry.
func (p *PessimisticRRTxnContextProvider) AdviseOptimizeWithPlan(val interface{}) (err error) {
	if p.isTidbSnapshotEnabled() || p.isBeginStmtWithStaleRead() {
		return nil
	}

	plan, ok := val.(plannercore.Plan)
	if !ok {
		return nil
	}

	if execute, ok := plan.(*plannercore.Execute); ok {
		plan = execute.Plan
	}

	mayOptimizeForPointGet := false
	if v, ok := plan.(*plannercore.PhysicalLock); ok {
		if _, ok := v.Children()[0].(*plannercore.PointGetPlan); ok {
			mayOptimizeForPointGet = true
		}
	} else if v, ok := plan.(*plannercore.Update); ok {
		if _, ok := v.SelectPlan.(*plannercore.PointGetPlan); ok {
			mayOptimizeForPointGet = true
		}
	} else if v, ok := plan.(*plannercore.Delete); ok {
		if _, ok := v.SelectPlan.(*plannercore.PointGetPlan); ok {
			mayOptimizeForPointGet = true
		}
	}

	p.followingOperatorIsPointGetForUpdate = mayOptimizeForPointGet

	return nil
}

func (p *PessimisticRRTxnContextProvider) handleAfterPessimisticLockError(lockErr error) (sessiontxn.StmtErrorAction, error) {
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

	} else if terror.ErrorEqual(kv.ErrWriteConflict, lockErr) {
		errStr := lockErr.Error()
		forUpdateTS := txnCtx.GetForUpdateTS()

		logutil.Logger(p.ctx).Debug("pessimistic write conflict, retry statement",
			zap.Uint64("txn", txnCtx.StartTS),
			zap.Uint64("forUpdateTS", forUpdateTS),
			zap.String("err", errStr))
		// Always update forUpdateTS by getting a new timestamp from PD.
		// If we use the conflict commitTS as the new forUpdateTS and async commit
		// is used, the commitTS of this transaction may exceed the max timestamp
		// that PD allocates. Then, the change may be invisible to a new transaction,
		// which means linearizability is broken.
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

	return sessiontxn.RetryReady()
}
