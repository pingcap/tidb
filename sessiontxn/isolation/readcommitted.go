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
	"unsafe"

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
	baseTxnContextProvider
	stmtState
	latestOracleTS uint64
	// latestOracleTSValid shows whether we have already fetched a ts from pd and whether the ts we fetched is still valid.
	latestOracleTSValid bool
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
			onTxnActiveFunc: func(txn kv.Transaction, _ sessiontxn.EnterNewTxnType) {
				txn.SetOption(kv.Pessimistic, true)
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
	if err := p.baseTxnContextProvider.OnStmtStart(ctx, node); err != nil {
		return err
	}

	// Try to mark the `RCCheckTS` flag for the first time execution of in-transaction read requests
	// using read-consistency isolation level.
	if node != nil {
		if NeedSetRCCheckTSFlag(p.sctx, node) {
			p.sctx.GetSessionVars().StmtCtx.RCCheckTS = true
		} else if NeedDisableWarmupInOptimizer(p.sctx, node) {
			p.sctx.GetSessionVars().StmtCtx.DisableWarmupInOptimizer = true
		}
	}

	return p.prepareStmt(!p.isTxnPrepared)
}

// NeedSetRCCheckTSFlag checks whether it's needed to set `RCCheckTS` flag in current stmtctx.
func NeedSetRCCheckTSFlag(ctx sessionctx.Context, node ast.Node) bool {
	sessionVars := ctx.GetSessionVars()
	if sessionVars.ConnectionID > 0 && sessionVars.RcReadCheckTS && sessionVars.InTxn() &&
		!sessionVars.RetryInfo.Retrying && plannercore.IsReadOnly(node, sessionVars) {
		return true
	}
	return false
}

// NeedDisableWarmupInOptimizer checks whether optimizer calls `txnManger.AdviseWarmup()` to warmup in RC isolation.
// txnManger.AdviseWarmup makes all write statement get tso from PD. But for insert, it may not be necessary
// to do that and it makes higher performance. In fact, txnManger.AdviseWarmup makes read tatement get tso
// from PD too, except that it's a "RcReadCheckTS" statement, please reffer to tidb_rc_read_check_ts variable.
// The necessary condition not performing warmup is as followers:
// 1. RC isolation 2. not internal sqls 3. tidb_rc_insert_use_last_tso is true
// 4. In transaction 5. Insert without select
func NeedDisableWarmupInOptimizer(sctx sessionctx.Context, node ast.Node) bool {
	sessionVars := sctx.GetSessionVars()
	if sessionVars.ConnectionID > 0 &&
		variable.InsertUseLastTso.Load() &&
		sessionVars.InTxn() {
		if insert, ok := node.(*ast.InsertStmt); ok && insert.Select == nil {
			return true
		}
	}

	return false
}

// OnStmtErrorForNextAction is the hook that should be called when a new statement get an error
func (p *PessimisticRCTxnContextProvider) OnStmtErrorForNextAction(point sessiontxn.StmtErrorHandlePoint, err error) (sessiontxn.StmtErrorAction, error) {
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
		if !p.sctx.GetSessionVars().InRestrictedSQL {
			logutil.Logger(p.ctx).Info("prepareStmtTS stmtTSFuture is not nil ====", zap.Uint64("sctx", uint64(uintptr(unsafe.Pointer(&p.sctx)))))
		}

		return
	}
	if !p.sctx.GetSessionVars().InRestrictedSQL {
		logutil.Logger(p.ctx).Info("prepareStmtTS stmtTSFuture is nil ====", zap.Uint64("sctx", uint64(uintptr(unsafe.Pointer(&p.sctx)))))
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
		txnCtx.SetForUpdateTS(ts)
		ts = txnCtx.GetForUpdateTS()
		p.latestOracleTS = ts
		p.latestOracleTSValid = true
		return
	}
}

func (p *PessimisticRCTxnContextProvider) getStmtTS() (ts uint64, err error) {
	if !p.sctx.GetSessionVars().InRestrictedSQL {
		logutil.Logger(p.ctx).Info("(p *PessimisticRCTxnContextProvider) getStmtTS()")
	}
	if p.stmtTS != 0 {
		return p.stmtTS, nil
	}

	var txn kv.Transaction
	if txn, err = p.ActivateTxn(); err != nil {
		return 0, err
	}

	p.prepareStmtTS()
	if ts, err = p.stmtTSFuture.Wait(); err != nil {
		return 0, err
	}

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

	p.latestOracleTSValid = false
	logutil.Logger(p.ctx).Info("RC read with ts checking has failed, retry RC read",
		zap.String("sql", sessVars.StmtCtx.OriginalSQL), zap.Error(queryErr))
	return sessiontxn.RetryReady()
}

func (p *PessimisticRCTxnContextProvider) handleAfterPessimisticLockError(lockErr error) (sessiontxn.StmtErrorAction, error) {
	p.latestOracleTSValid = false
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

	if retryable {
		return sessiontxn.RetryReady()
	}
	return sessiontxn.ErrorAction(lockErr)
}

// AdviseWarmup provides warmup for inner state
func (p *PessimisticRCTxnContextProvider) AdviseWarmup() error {
	if !p.sctx.GetSessionVars().InRestrictedSQL {
		logutil.Logger(p.ctx).Info("(p *PessimisticRCTxnContextProvider) AdviseWarmup()")
	}

	if err := p.prepareTxn(); err != nil {
		return err
	}

	if !p.isTidbSnapshotEnabled() {
		p.prepareStmtTS()
	}

	return nil
}

func isPointLockReadUseLastTso() bool {
	return true
}

// AdviseOptimizeWithPlan in RC covers much fewer cases compared with pessimistic repeatable read.
// We do not fetch latest ts immediately for such scenes.
// 1. PointGet of SELECT ... FOR UPDATE  2. Insert without select 3. update by PointGet 4. delete by PointGet.
// We don't optimize for PointGet of SELECT ... FOR UPDATE defaultly, it may decrease the success rate
// We get for_update_ts if write conflict is incurred.
// The func `planner.Optimize` always calls the `txnManger.AdviseWarmup` to warmup in the past, it makes all sqls
// execept `SELECT` queries with RcCheckTs get tso from pd. If intend to use the tso of last sql in current trx,
// we should notify `planner.Optimize` to skip calling `txnManger.AdviseWarmup`.
// 1. For PointGet of SELECT ... FOR UPDATE, DELETE, UPDATE, the `fastplan` process makes `planner.Optimize`
//    skips calling `txnManger.AdviseWarmup`, and the sqls above skip getting tso from PD by `AdviseOptimizeWithPlan`.
// 2. For Insert statements which have no `select` subquery, we use the flag `DisableWarmupInOptimizer` to
//    make `planner.Optimize` skips calling `txnManger.AdviseWarmup`, see the details in func `NeedDisableWarmupInOptimizer`
func (p *PessimisticRCTxnContextProvider) AdviseOptimizeWithPlan(val interface{}) (err error) {
	if p.isTidbSnapshotEnabled() || p.isBeginStmtWithStaleRead() {
		return nil
	}
	if !p.sctx.GetSessionVars().InRestrictedSQL {
		logutil.Logger(p.ctx).Info("AdviseOptimizeWithPlan",
			zap.Bool("stmtUseStartTS", p.stmtUseStartTS), zap.Bool("latestOracleTSValid", p.latestOracleTSValid))
	}
	if p.stmtUseStartTS || !p.latestOracleTSValid {
		return nil
	}

	plan, ok := val.(plannercore.Plan)
	if !ok {
		return nil
	}

	if execute, ok := plan.(*plannercore.Execute); ok {
		plan = execute.Plan
	}

	useLastOracleTS := false
	switch v := plan.(type) {
	case *plannercore.PointGetPlan:
		if v.Lock && variable.PointLockReadUseLastTso.Load() {
			useLastOracleTS = true
		}
	case *plannercore.Insert:
		if v.SelectPlan == nil {
			useLastOracleTS = true
		}
	case *plannercore.Update:
		if _, Ok := v.SelectPlan.(*plannercore.PointGetPlan); Ok && variable.PointLockReadUseLastTso.Load() {
			useLastOracleTS = true
		}
	case *plannercore.Delete:
		if _, ok := v.SelectPlan.(*plannercore.PointGetPlan); ok && variable.PointLockReadUseLastTso.Load() {
			useLastOracleTS = true
		}
	}

	if useLastOracleTS {
		p.stmtTSFuture = sessiontxn.ConstantFuture(p.latestOracleTS)
	}

	return nil
}

// GetSnapshotWithStmtReadTS gets snapshot with read ts
func (p *PessimisticRCTxnContextProvider) GetSnapshotWithStmtReadTS() (kv.Snapshot, error) {
	snapshot, err := p.baseTxnContextProvider.GetSnapshotWithStmtForUpdateTS()
	if err != nil {
		return nil, err
	}

	if p.sctx.GetSessionVars().StmtCtx.RCCheckTS {
		snapshot.SetOption(kv.IsolationLevel, kv.RCCheckTS)
	}

	return snapshot, nil
}
