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
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type updateStmtState struct {
	updateTS       uint64
	updateTSFuture oracle.Future

	maxUpdateTS uint64
}

type PessimisticRRTxnContextProvider struct {
	baseTxnContextProvider
	updateStmtState
}

func (p *PessimisticRRTxnContextProvider) getForUpdateTs() (ts uint64, err error) {
	if p.updateTS != 0 {
		return p.updateTS, nil
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

	p.updateTS = ts
	p.maxUpdateTS = ts

	return
}

func (p *PessimisticRRTxnContextProvider) updateMaxForUpdateTs() error {
	txnCxt := p.sctx.GetSessionVars().TxnCtx
	futureTS := sessiontxn.NewOracleFuture(p.ctx, p.sctx, txnCxt.TxnScope)

	var ts uint64
	var err error

	if ts, err = futureTS.Wait(); err != nil {
		return err
	}

	p.maxUpdateTS = ts

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

	p.updateTS = 0
	p.infoSchema = sessiontxn.GetTxnManager(p.sctx).GetTxnInfoSchema()

	return nil
}

func (p *PessimisticRRTxnContextProvider) OnStmtRetry(ctx context.Context) error {
	if err := p.baseTxnContextProvider.OnStmtRetry(ctx); err != nil {
		return err
	}

	// In OnStmtErrorForNextAction, for those error cases that need to retry, the maxUpdateTs will be updated
	// to the latest ts. It can be used now to retry.
	p.updateTS = p.maxUpdateTS

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

func (p *PessimisticRRTxnContextProvider) optimizeWithPlan(val []any) (err error) {
	if p.isTidbSnapshotEnabled() {
		return nil
	}

	if len(val) == 0 {
		return nil
	}

	plan, ok := val[0].(plannercore.Plan)
	if !ok {
		return nil
	}

	if execute, ok := plan.(*plannercore.Execute); ok {
		plan = execute.Plan
	}

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
	} else {
		if err := p.updateMaxForUpdateTs(); err != nil {
			logutil.Logger(p.ctx).Warn("UpdateForUpdateTS failed", zap.Error(err))
		}

		return sessiontxn.ErrorAction(lockErr)
	}

	if err := p.updateMaxForUpdateTs(); err != nil {
		return sessiontxn.ErrorAction(lockErr)
	}

	return sessiontxn.RetryReady()
}
