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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/util/logutil"
	tikverr "github.com/tikv/client-go/v2/error"
	"go.uber.org/zap"
)

type PessimisticRRTxnContextProvider struct {
	baseTxnContextProvider

	// Used for ForUpdateRead statement
	forUpdateTS uint64
}

func (p *PessimisticRRTxnContextProvider) getForUpdateTs() (ts uint64, err error) {
	if p.forUpdateTS != 0 {
		return p.forUpdateTS, nil
	}

	var txn kv.Transaction
	if txn, err = p.activeTxn(); err != nil {
		return 0, err
	}

	txnCxt := p.sctx.GetSessionVars().TxnCtx
	futureTS := sessiontxn.NewOracleFuture(p.ctx, p.sctx, txnCxt.TxnScope)

	if ts, err = futureTS.Wait(); err != nil {
		return 0, err
	}

	txnCxt.SetForUpdateTS(ts)
	txn.SetOption(kv.SnapshotTS, ts)

	p.forUpdateTS = ts

	return
}

func (p *PessimisticRRTxnContextProvider) updateForUpdateTS() (err error) {
	seCtx := p.sctx
	var txn kv.Transaction

	if txn, err = seCtx.Txn(false); err != nil {
		return err
	}

	if !txn.Valid() {
		return errors.Trace(kv.ErrInvalidTxn)
	}

	// Because the ForUpdateTS is used for the snapshot for reading data in DML.
	// We can avoid allocating a global TSO here to speed it up by using the local TSO.
	version, err := seCtx.GetStore().CurrentVersion(seCtx.GetSessionVars().TxnCtx.TxnScope)
	if err != nil {
		return err
	}

	seCtx.GetSessionVars().TxnCtx.SetForUpdateTS(version.Ver)
	txn.SetOption(kv.SnapshotTS, seCtx.GetSessionVars().TxnCtx.GetForUpdateTS())

	return nil
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

// OnStmtStart is the hook that should be called when a new statement started
func (p *PessimisticRRTxnContextProvider) OnStmtStart(ctx context.Context) error {
	if err := p.baseTxnContextProvider.OnStmtStart(ctx); err != nil {
		return err
	}

	p.forUpdateTS = 0
	p.infoSchema = sessiontxn.GetTxnManager(p.sctx).GetTxnInfoSchema()

	return nil
}

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

// Advise is used to give advice to provider
func (p *PessimisticRRTxnContextProvider) Advise(tp sessiontxn.AdviceType, val []any) error {
	switch tp {
	case sessiontxn.AdviceWarmUp:
		return p.warmUp()
	case sessiontxn.AdviceOptimizeWithPlan:
		return p.optimizeWithPlan(val)
	default:
		return p.baseTxnContextProvider.Advise(tp, val)
	}
}

// optimizeWithPlan todo: optimize the forUpdateTs acquisition of point/batch point get
func (p *PessimisticRRTxnContextProvider) optimizeWithPlan(val []any) (err error) {
	return nil
}

func (p *PessimisticRRTxnContextProvider) warmUp() error {
	if p.isTidbSnapshotEnabled() {
		return nil
	}

	if err := p.prepareTxn(); err != nil {
		return err
	}
	return nil
}

// handleAfterQueryError will be called when the handle point is `StmtErrAfterQuery`.
// At this point the query will be retried from the beginning.
func (p *PessimisticRRTxnContextProvider) handleAfterQueryError(queryErr error) (sessiontxn.StmtErrorAction, error) {
	sessVars := p.sctx.GetSessionVars()
	if errors.ErrorEqual(queryErr, kv.ErrWriteConflict) {
		logutil.Logger(p.ctx).Info("Pessimistic repeatable read failed, retry it",
			zap.String("sql", sessVars.StmtCtx.OriginalSQL))
		return sessiontxn.RetryReady()
	}

	return sessiontxn.NoIdea()
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
