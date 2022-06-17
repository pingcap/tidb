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
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type stmtState struct {
	stmtTS            uint64
	stmtTSFuture      oracle.Future
	stmtUseStartTS    bool
	onNextRetryOrStmt func() error
}

func (s *stmtState) prepareStmt(useStartTS bool) error {
	onNextStmt := s.onNextRetryOrStmt
	*s = stmtState{
		stmtUseStartTS: useStartTS,
	}
	if onNextStmt != nil {
		return onNextStmt()
	}
	return nil
}

// PessimisticRCTxnContextProvider provides txn context for isolation level read-committed
type PessimisticRCTxnContextProvider struct {
	baseTxnContextProvider
	stmtState
	availableRCCheckTS uint64
}

// NewPessimisticRCTxnContextProvider returns a new PessimisticRCTxnContextProvider
func NewPessimisticRCTxnContextProvider(sctx sessionctx.Context, causalConsistencyOnly bool) *PessimisticRCTxnContextProvider {
	provider := &PessimisticRCTxnContextProvider{
		baseTxnContextProvider: baseTxnContextProvider{
			sctx:                  sctx,
			causalConsistencyOnly: causalConsistencyOnly,
			onInitializeTxnCtx: func(txnCtx *variable.TransactionContext) {
				txnCtx.IsPessimistic = true
				txnCtx.Isolation = ast.ReadCommitted
			},
			onTxnActive: func(txn kv.Transaction) {
				txn.SetOption(kv.Pessimistic, true)
			},
		},
	}

	provider.getStmtReadTSFunc = provider.getStmtTS
	provider.getStmtForUpdateTSFunc = provider.getStmtTS
	return provider
}

// OnStmtStart is the hook that should be called when a new statement started
func (p *PessimisticRCTxnContextProvider) OnStmtStart(ctx context.Context) error {
	if err := p.baseTxnContextProvider.OnStmtStart(ctx); err != nil {
		return err
	}
	return p.prepareStmt(!p.isTxnPrepared)
}

// OnStmtErrorForNextAction is the hook that should be called when a new statement get an error
func (p *PessimisticRCTxnContextProvider) OnStmtErrorForNextAction(point sessiontxn.StmtErrorHandlePoint, err error) (sessiontxn.StmtErrorAction, error) {
	// Invalid rc check for next statement or retry when error occurs
	p.availableRCCheckTS = 0

	switch point {
	case sessiontxn.StmtErrAfterQuery:
		return p.handleAfterQueryError(err)
	case sessiontxn.StmtErrAfterPessimisticLock:
		return p.handleAfterPessimisticLockError(err)
	default:
		return p.baseTxnContextProvider.OnStmtErrorForNextAction(point, err)
	}
}

// OnStmtRetry is the hook that should be called when a statement is retried internally.
func (p *PessimisticRCTxnContextProvider) OnStmtRetry(ctx context.Context) error {
	if err := p.baseTxnContextProvider.OnStmtRetry(ctx); err != nil {
		return err
	}
	return p.prepareStmt(false)
}

func (p *PessimisticRCTxnContextProvider) prepareStmtTS() {
	if p.stmtTSFuture != nil {
		return
	}

	sessVars := p.sctx.GetSessionVars()
	var stmtTSFuture oracle.Future
	switch {
	case p.stmtUseStartTS:
		stmtTSFuture = sessiontxn.FuncFuture(p.getTxnStartTS)
	case p.availableRCCheckTS != 0 && sessVars.StmtCtx.RCCheckTS:
		stmtTSFuture = sessiontxn.ConstantFuture(p.availableRCCheckTS)
	default:
		stmtTSFuture = sessiontxn.NewOracleFuture(p.ctx, p.sctx, sessVars.TxnCtx.TxnScope)
	}

	p.stmtTSFuture = stmtTSFuture
}

func (p *PessimisticRCTxnContextProvider) getStmtTS() (ts uint64, err error) {
	if p.stmtTS != 0 {
		return p.stmtTS, nil
	}

	var txn kv.Transaction
	if txn, err = p.activeTxn(); err != nil {
		return 0, err
	}

	p.prepareStmtTS()
	if ts, err = p.stmtTSFuture.Wait(); err != nil {
		return 0, err
	}

	// forUpdateTS should exactly equal to the read ts
	txnCtx := p.sctx.GetSessionVars().TxnCtx
	txnCtx.SetForUpdateTS(ts)
	txn.SetOption(kv.SnapshotTS, ts)

	p.stmtTS = ts
	p.availableRCCheckTS = ts
	return
}

// handleAfterQueryError will be called when the handle point is `StmtErrAfterQuery`.
// At this point the query will be retried from the beginning.
func (p *PessimisticRCTxnContextProvider) handleAfterQueryError(queryErr error) (sessiontxn.StmtErrorAction, error) {
	sessVars := p.sctx.GetSessionVars()
	if sessVars.StmtCtx.RCCheckTS && errors.ErrorEqual(queryErr, kv.ErrWriteConflict) {
		logutil.Logger(p.ctx).Info("RC read with ts checking has failed, retry RC read",
			zap.String("sql", sessVars.StmtCtx.OriginalSQL), zap.Error(queryErr))
		return sessiontxn.RetryReady()
	}

	return sessiontxn.NoIdea()
}

func (p *PessimisticRCTxnContextProvider) handleAfterPessimisticLockError(lockErr error) (sessiontxn.StmtErrorAction, error) {
	txnCtx := p.sctx.GetSessionVars().TxnCtx
	retryable := false
	if deadlock, ok := errors.Cause(lockErr).(*tikverr.ErrDeadlock); ok && deadlock.IsRetryable {
		logutil.Logger(p.ctx).Info("single statement deadlock, retry statement",
			zap.Uint64("txn", txnCtx.StartTS),
			zap.Uint64("lockTS", deadlock.LockTs),
			zap.Stringer("lockKey", kv.Key(deadlock.LockKey)),
			zap.Uint64("deadlockKeyHash", deadlock.DeadlockKeyHash))
		retryable = true
	} else if terror.ErrorEqual(kv.ErrWriteConflict, lockErr) {
		logutil.Logger(p.ctx).Debug("pessimistic write conflict, retry statement",
			zap.Uint64("txn", txnCtx.StartTS),
			zap.Uint64("forUpdateTS", txnCtx.GetForUpdateTS()),
			zap.String("err", lockErr.Error()))
		retryable = true
	}

	// force refresh ts in next retry or statement when lock error occurs
	p.onNextRetryOrStmt = func() error {
		_, err := p.getStmtTS()
		return err
	}

	if retryable {
		return sessiontxn.RetryReady()
	}

	return sessiontxn.ErrorAction(lockErr)
}

// AdviseWarmup provides warmup for inner state
func (p *PessimisticRCTxnContextProvider) AdviseWarmup() error {
	if p.isTidbSnapshotEnabled() {
		return nil
	}

	if err := p.prepareTxn(); err != nil {
		return err
	}
	p.prepareStmtTS()
	return nil
}
