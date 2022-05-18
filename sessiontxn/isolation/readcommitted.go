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
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/table/temptable"
	"github.com/pingcap/tidb/util/logutil"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type stmtState struct {
	stmtInfoSchema    infoschema.InfoSchema
	stmtTS            uint64
	stmtTSFuture      oracle.Future
	stmtUseStartTS    bool
	onNextRetryOrStmt func() error
}

func (s *stmtState) nextStmt(useStartTS bool) error {
	onNextStmt := s.onNextRetryOrStmt
	*s = stmtState{
		stmtUseStartTS: useStartTS,
	}
	if onNextStmt != nil {
		return onNextStmt()
	}
	return nil
}

func (s *stmtState) retry() error {
	onNextRetry := s.onNextRetryOrStmt
	*s = stmtState{
		stmtInfoSchema: s.stmtInfoSchema,
	}

	if onNextRetry != nil {
		return onNextRetry()
	}
	return nil
}

// PessimisticRCTxnContextProvider provides txn context for isolation level read-committed
type PessimisticRCTxnContextProvider struct {
	ctx                   context.Context
	sctx                  sessionctx.Context
	causalConsistencyOnly bool

	is            infoschema.InfoSchema
	isTxnPrepared bool
	isTxnActive   bool

	stmtState
	availableRCCheckTS uint64
}

// NewPessimisticRCTxnContextProvider returns a new PessimisticRCTxnContextProvider
func NewPessimisticRCTxnContextProvider(sctx sessionctx.Context, causalConsistencyOnly bool) *PessimisticRCTxnContextProvider {
	return &PessimisticRCTxnContextProvider{
		sctx:                  sctx,
		causalConsistencyOnly: causalConsistencyOnly,
	}
}

// GetTxnInfoSchema returns the information schema used by txn
func (p *PessimisticRCTxnContextProvider) GetTxnInfoSchema() infoschema.InfoSchema {
	if is := p.sctx.GetSessionVars().SnapshotInfoschema; is != nil {
		return is.(infoschema.InfoSchema)
	}

	if p.stmtInfoSchema != nil {
		return p.stmtInfoSchema
	}

	return p.is
}

// GetStmtReadTS returns the read timestamp used by select statement (not for select ... for update)
func (p *PessimisticRCTxnContextProvider) GetStmtReadTS() (ts uint64, err error) {
	if snapshotTS := p.sctx.GetSessionVars().SnapshotTS; snapshotTS != 0 {
		return snapshotTS, nil
	}
	return p.getStmtTS()
}

// GetStmtForUpdateTS returns the read timestamp used by update/insert/delete or select ... for update
func (p *PessimisticRCTxnContextProvider) GetStmtForUpdateTS() (uint64, error) {
	return p.GetStmtReadTS()
}

// OnInitialize is the hook that should be called when enter a new txn with this provider
func (p *PessimisticRCTxnContextProvider) OnInitialize(ctx context.Context, tp sessiontxn.EnterNewTxnType) (err error) {
	p.ctx = ctx
	p.is = temptable.AttachLocalTemporaryTableInfoSchema(p.sctx, domain.GetDomain(p.sctx).InfoSchema())
	activeNow := false
	switch tp {
	case sessiontxn.EnterNewTxnDefault, sessiontxn.EnterNewTxnWithBeginStmt:
		shouldReuseTxn := tp == sessiontxn.EnterNewTxnWithBeginStmt && sessiontxn.CanReuseTxnWhenExplictBegin(p.sctx)
		if !shouldReuseTxn {
			if err = p.sctx.NewTxn(ctx); err != nil {
				return err
			}
		}
		activeNow = true
	case sessiontxn.EnterNewTxnBeforeStmt:
		activeNow = false
	default:
		return errors.Errorf("Unsupported type: %v", tp)
	}

	p.sctx.GetSessionVars().TxnCtx = p.newRCTxnCtx(p.is)
	if activeNow {
		_, err = p.activeTxn()
	}

	return err
}

// OnStmtStart is the hook that should be called when a new statement started
func (p *PessimisticRCTxnContextProvider) OnStmtStart(ctx context.Context) error {
	p.ctx = ctx
	return p.nextStmt(!p.isTxnPrepared)
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
		return sessiontxn.NoIdea()
	}
}

// OnStmtRetry is the hook that should be called when a statement is retried internally.
func (p *PessimisticRCTxnContextProvider) OnStmtRetry(ctx context.Context) error {
	p.ctx = ctx
	return p.retry()
}

// Advise is used to give advice to provider
func (p *PessimisticRCTxnContextProvider) Advise(tp sessiontxn.AdviceType) error {
	switch tp {
	case sessiontxn.AdviceWarmUp:
		if snapshotTS := p.sctx.GetSessionVars().SnapshotTS; snapshotTS == 0 {
			p.prepareTxn()
			p.prepareStmtTS()
		}
	}
	return nil
}

// ReplaceStmtInfoSchema replaces the current info schema
func (p *PessimisticRCTxnContextProvider) ReplaceStmtInfoSchema(is infoschema.InfoSchema) {
	p.stmtInfoSchema = is
}

func (p *PessimisticRCTxnContextProvider) newRCTxnCtx(is infoschema.InfoSchema) *variable.TransactionContext {
	sessVars := p.sctx.GetSessionVars()
	return &variable.TransactionContext{
		CreateTime:    time.Now(),
		InfoSchema:    is,
		ShardStep:     int(sessVars.ShardAllocateStep),
		TxnScope:      sessVars.CheckAndGetTxnScope(),
		IsPessimistic: true,
		Isolation:     ast.ReadCommitted,
	}
}

func (p *PessimisticRCTxnContextProvider) activeTxn() (kv.Transaction, error) {
	p.prepareTxn()
	txn, err := p.sctx.Txn(true)
	if err != nil {
		return nil, err
	}

	if p.isTxnActive {
		return txn, nil
	}

	txn.SetOption(kv.Pessimistic, true)
	if p.causalConsistencyOnly {
		txn.SetOption(kv.GuaranteeLinearizability, false)
	}

	p.isTxnActive = true
	return txn, nil
}

func (p *PessimisticRCTxnContextProvider) prepareStmtTS() {
	if p.stmtTSFuture != nil {
		return
	}

	sessVars := p.sctx.GetSessionVars()
	var stmtTSFuture oracle.Future
	switch {
	case p.stmtUseStartTS:
		stmtTSFuture = p.getTxnStartTSFuture()
	case p.availableRCCheckTS != 0 && sessVars.StmtCtx.RCCheckTS:
		stmtTSFuture = sessiontxn.ConstantFuture(p.availableRCCheckTS)
	default:
		stmtTSFuture = sessiontxn.NewOracleFuture(p.ctx, p.sctx, sessVars.TxnCtx.TxnScope)
	}

	p.stmtTSFuture = stmtTSFuture
	return
}

func (p *PessimisticRCTxnContextProvider) prepareTxn() {
	if p.isTxnPrepared {
		return
	}

	p.sctx.PrepareTSFuture(p.ctx)
	p.isTxnPrepared = true
}

func (p *PessimisticRCTxnContextProvider) getTxnStartTSFuture() sessiontxn.FuncFuture {
	return func() (uint64, error) {
		txn, err := p.sctx.Txn(false)
		if err != nil {
			return 0, err
		}

		if !txn.Valid() {
			return 0, errors.New("invalid transaction")
		}

		return txn.StartTS(), nil
	}
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
			zap.String("sql", sessVars.StmtCtx.OriginalSQL))
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
