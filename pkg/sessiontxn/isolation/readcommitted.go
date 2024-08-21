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
	isolation_metrics "github.com/pingcap/tidb/pkg/sessiontxn/isolation/metrics"
	"github.com/pingcap/tidb/pkg/util/logutil"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

type stmtState struct {
	stmtTS         uint64
	stmtTSFuture   oracle.Future
	stmtUseStartTS bool
}

func (s *stmtState) prepareStmt(useStartTS bool) error {
	*s = stmtState{
		stmtUseStartTS: useStartTS,
	}
	return nil
}

// PessimisticRCTxnContextProvider provides txn context for isolation level read-committed
type PessimisticRCTxnContextProvider struct {
	basePessimisticTxnContextProvider
	stmtState
	latestOracleTS uint64
	// latestOracleTSValid shows whether we have already fetched a ts from pd and whether the ts we fetched is still valid.
	latestOracleTSValid bool
	// checkTSInWriteStmt is used to set RCCheckTS isolation for getting value when doing point-write
	checkTSInWriteStmt bool
}

// NewPessimisticRCTxnContextProvider returns a new PessimisticRCTxnContextProvider
func NewPessimisticRCTxnContextProvider(sctx sessionctx.Context, causalConsistencyOnly bool) *PessimisticRCTxnContextProvider {
	provider := &PessimisticRCTxnContextProvider{
		basePessimisticTxnContextProvider: basePessimisticTxnContextProvider{
			baseTxnContextProvider: baseTxnContextProvider{
				sctx:                  sctx,
				causalConsistencyOnly: causalConsistencyOnly,
				onInitializeTxnCtx: func(txnCtx *variable.TransactionContext) {
					txnCtx.IsPessimistic = true
					txnCtx.Isolation = ast.ReadCommitted
				},
				onTxnActiveFunc: func(txn kv.Transaction, _ sessiontxn.EnterNewTxnType) {
					txn.SetOption(kv.Pessimistic, true)
				},
			},
		},
	}

	provider.onTxnActiveFunc = func(txn kv.Transaction, _ sessiontxn.EnterNewTxnType) {
		txn.SetOption(kv.Pessimistic, true)
		provider.latestOracleTS = txn.StartTS()
		provider.latestOracleTSValid = true
	}
	provider.getStmtReadTSFunc = provider.getStmtTS
	provider.getStmtForUpdateTSFunc = provider.getStmtTS
	return provider
}

// OnStmtStart is the hook that should be called when a new statement started
func (p *PessimisticRCTxnContextProvider) OnStmtStart(ctx context.Context, node ast.StmtNode) error {
	if err := p.basePessimisticTxnContextProvider.OnStmtStart(ctx, node); err != nil {
		return err
	}

	// Try to mark the `RCCheckTS` flag for the first time execution of in-transaction read requests
	// using read-consistency isolation level.
	if node != nil && NeedSetRCCheckTSFlag(p.sctx, node) {
		p.sctx.GetSessionVars().StmtCtx.RCCheckTS = true
	}
	p.checkTSInWriteStmt = false

	return p.prepareStmt(!p.isTxnPrepared)
}

// NeedSetRCCheckTSFlag checks whether it's needed to set `RCCheckTS` flag in current stmtctx.
func NeedSetRCCheckTSFlag(ctx sessionctx.Context, node ast.Node) bool {
	sessionVars := ctx.GetSessionVars()
	if sessionVars.ConnectionID > 0 && variable.EnableRCReadCheckTS.Load() &&
		sessionVars.InTxn() && !sessionVars.RetryInfo.Retrying &&
		plannercore.IsReadOnly(node, sessionVars) {
		return true
	}
	return false
}

// OnStmtErrorForNextAction is the hook that should be called when a new statement get an error
func (p *PessimisticRCTxnContextProvider) OnStmtErrorForNextAction(ctx context.Context, point sessiontxn.StmtErrorHandlePoint, err error) (sessiontxn.StmtErrorAction, error) {
	switch point {
	case sessiontxn.StmtErrAfterQuery:
		return p.handleAfterQueryError(err)
	case sessiontxn.StmtErrAfterPessimisticLock:
		return p.handleAfterPessimisticLockError(ctx, err)
	default:
		return p.basePessimisticTxnContextProvider.OnStmtErrorForNextAction(ctx, point, err)
	}
}

// OnStmtRetry is the hook that should be called when a statement is retried internally.
func (p *PessimisticRCTxnContextProvider) OnStmtRetry(ctx context.Context) error {
	if err := p.basePessimisticTxnContextProvider.OnStmtRetry(ctx); err != nil {
		return err
	}
	failpoint.Inject("CallOnStmtRetry", func() {
		sessiontxn.OnStmtRetryCountInc(p.sctx)
	})
	p.latestOracleTSValid = false
	p.checkTSInWriteStmt = false
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
		stmtTSFuture = funcFuture(p.getTxnStartTS)
	case p.latestOracleTSValid && sessVars.StmtCtx.RCCheckTS:
		stmtTSFuture = sessiontxn.ConstantFuture(p.latestOracleTS)
	default:
		stmtTSFuture = p.getOracleFuture()
	}

	p.stmtTSFuture = stmtTSFuture
}

func (p *PessimisticRCTxnContextProvider) getOracleFuture() funcFuture {
	txnCtx := p.sctx.GetSessionVars().TxnCtx
	future := newOracleFuture(p.ctx, p.sctx, txnCtx.TxnScope)
	return func() (ts uint64, err error) {
		if ts, err = future.Wait(); err != nil {
			return
		}
		failpoint.Inject("waitTsoOfOracleFuture", func() {
			sessiontxn.TsoWaitCountInc(p.sctx)
		})
		txnCtx.SetForUpdateTS(ts)
		ts = txnCtx.GetForUpdateTS()
		p.latestOracleTS = ts
		p.latestOracleTSValid = true
		return
	}
}

func (p *PessimisticRCTxnContextProvider) getStmtTS() (ts uint64, err error) {
	if p.stmtTS != 0 {
		return p.stmtTS, nil
	}

	var txn kv.Transaction
	if txn, err = p.ActivateTxn(); err != nil {
		return 0, err
	}

	p.prepareStmtTS()
	start := time.Now()
	if ts, err = p.stmtTSFuture.Wait(); err != nil {
		return 0, err
	}
	p.sctx.GetSessionVars().DurationWaitTS += time.Since(start)

	txn.SetOption(kv.SnapshotTS, ts)
	p.stmtTS = ts
	return
}

// handleAfterQueryError will be called when the handle point is `StmtErrAfterQuery`.
// At this point the query will be retried from the beginning.
func (p *PessimisticRCTxnContextProvider) handleAfterQueryError(queryErr error) (sessiontxn.StmtErrorAction, error) {
	sessVars := p.sctx.GetSessionVars()
	if !errors.ErrorEqual(queryErr, kv.ErrWriteConflict) || !sessVars.StmtCtx.RCCheckTS {
		return sessiontxn.NoIdea()
	}

	isolation_metrics.RcReadCheckTSWriteConfilictCounter.Inc()

	logutil.Logger(p.ctx).Info("RC read with ts checking has failed, retry RC read",
		zap.String("sql", sessVars.StmtCtx.OriginalSQL), zap.Error(queryErr))
	return sessiontxn.RetryReady()
}

func (p *PessimisticRCTxnContextProvider) handleAfterPessimisticLockError(ctx context.Context, lockErr error) (sessiontxn.StmtErrorAction, error) {
	txnCtx := p.sctx.GetSessionVars().TxnCtx
	retryable := false
	if deadlock, ok := errors.Cause(lockErr).(*tikverr.ErrDeadlock); ok && deadlock.IsRetryable {
		logutil.Logger(p.ctx).Info("single statement deadlock, retry statement",
			zap.Uint64("txn", txnCtx.StartTS),
			zap.Uint64("lockTS", deadlock.LockTs),
			zap.Stringer("lockKey", kv.Key(deadlock.LockKey)),
			zap.Uint64("deadlockKeyHash", deadlock.DeadlockKeyHash))
		retryable = true

		// In fair locking mode, when statement retry happens, `retryFairLockingIfNeeded` should be
		// called to make its state ready for retrying. But single-statement deadlock is an exception. We need to exit
		// fair locking in single-statement-deadlock case, otherwise the lock this statement has acquired won't be
		// released after retrying, so it still blocks another transaction and the deadlock won't be resolved.
		if err := p.cancelFairLockingIfNeeded(ctx); err != nil {
			return sessiontxn.ErrorAction(err)
		}
	} else if terror.ErrorEqual(kv.ErrWriteConflict, lockErr) {
		logutil.Logger(p.ctx).Debug("pessimistic write conflict, retry statement",
			zap.Uint64("txn", txnCtx.StartTS),
			zap.Uint64("forUpdateTS", txnCtx.GetForUpdateTS()),
			zap.String("err", lockErr.Error()))
		retryable = true
		if p.checkTSInWriteStmt {
			isolation_metrics.RcWriteCheckTSWriteConfilictCounter.Inc()
		}
	}

	if retryable {
		if err := p.basePessimisticTxnContextProvider.retryFairLockingIfNeeded(ctx); err != nil {
			return sessiontxn.ErrorAction(err)
		}
		return sessiontxn.RetryReady()
	}
	return sessiontxn.ErrorAction(lockErr)
}

// AdviseWarmup provides warmup for inner state
func (p *PessimisticRCTxnContextProvider) AdviseWarmup() error {
	if err := p.prepareTxn(); err != nil {
		return err
	}

	if !p.isTidbSnapshotEnabled() {
		p.prepareStmtTS()
	}

	return nil
}

// planSkipGetTsoFromPD identifies the plans which don't need get newest ts from PD.
func planSkipGetTsoFromPD(sctx sessionctx.Context, plan base.Plan, inLockOrWriteStmt bool) bool {
	switch v := plan.(type) {
	case *plannercore.PointGetPlan:
		return sctx.GetSessionVars().RcWriteCheckTS && (v.Lock || inLockOrWriteStmt)
	case base.PhysicalPlan:
		if len(v.Children()) == 0 {
			return false
		}
		_, isPhysicalLock := v.(*plannercore.PhysicalLock)
		for _, p := range v.Children() {
			if !planSkipGetTsoFromPD(sctx, p, isPhysicalLock || inLockOrWriteStmt) {
				return false
			}
		}
		return true
	case *plannercore.Update:
		return planSkipGetTsoFromPD(sctx, v.SelectPlan, true)
	case *plannercore.Delete:
		return planSkipGetTsoFromPD(sctx, v.SelectPlan, true)
	case *plannercore.Insert:
		return v.SelectPlan == nil && len(v.OnDuplicate) == 0 && !v.IsReplace
	}
	return false
}

// AdviseOptimizeWithPlan in read-committed covers as many cases as repeatable-read.
// We do not fetch latest ts immediately for such scenes.
// 1. A query like the form of "SELECT ... FOR UPDATE" whose execution plan is "PointGet".
// 2. An INSERT statement without "SELECT" subquery.
// 3. A UPDATE statement whose sub execution plan is "PointGet".
// 4. A DELETE statement whose sub execution plan is "PointGet".
func (p *PessimisticRCTxnContextProvider) AdviseOptimizeWithPlan(val any) (err error) {
	if p.isTidbSnapshotEnabled() || p.isBeginStmtWithStaleRead() {
		return nil
	}
	if p.stmtUseStartTS || !p.latestOracleTSValid {
		return nil
	}

	plan, ok := val.(base.Plan)
	if !ok {
		return nil
	}

	if execute, ok := plan.(*plannercore.Execute); ok {
		plan = execute.Plan
	}

	useLastOracleTS := false
	if !p.sctx.GetSessionVars().RetryInfo.Retrying {
		useLastOracleTS = planSkipGetTsoFromPD(p.sctx, plan, false)
	}

	if useLastOracleTS {
		failpoint.Inject("tsoUseConstantFuture", func() {
			sessiontxn.TsoUseConstantCountInc(p.sctx)
		})
		p.checkTSInWriteStmt = true
		p.stmtTSFuture = sessiontxn.ConstantFuture(p.latestOracleTS)
	}

	return nil
}

// GetSnapshotWithStmtForUpdateTS gets snapshot with for update ts
func (p *PessimisticRCTxnContextProvider) GetSnapshotWithStmtForUpdateTS() (kv.Snapshot, error) {
	snapshot, err := p.basePessimisticTxnContextProvider.GetSnapshotWithStmtForUpdateTS()
	if err != nil {
		return nil, err
	}
	if p.checkTSInWriteStmt {
		snapshot.SetOption(kv.IsolationLevel, kv.RCCheckTS)
	}
	return snapshot, err
}

// GetSnapshotWithStmtReadTS gets snapshot with read ts
func (p *PessimisticRCTxnContextProvider) GetSnapshotWithStmtReadTS() (kv.Snapshot, error) {
	snapshot, err := p.basePessimisticTxnContextProvider.GetSnapshotWithStmtForUpdateTS()
	if err != nil {
		return nil, err
	}

	if p.sctx.GetSessionVars().StmtCtx.RCCheckTS {
		snapshot.SetOption(kv.IsolationLevel, kv.RCCheckTS)
	}

	return snapshot, nil
}

// IsCheckTSInWriteStmtMode is only used for test
func (p *PessimisticRCTxnContextProvider) IsCheckTSInWriteStmtMode() bool {
	return p.checkTSInWriteStmt
}
