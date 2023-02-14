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
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/internal"
	"github.com/pingcap/tidb/sessiontxn/staleread"
	"github.com/pingcap/tidb/table/temptable"
	"github.com/pingcap/tidb/util/tracing"
	"github.com/tikv/client-go/v2/oracle"
)

// baseTxnContextProvider is a base class for the transaction context providers that implement `TxnContextProvider` in different isolation.
// It provides some common functions below:
//   - Provides a default `OnInitialize` method to initialize its inner state.
//   - Provides some methods like `activateTxn` and `prepareTxn` to manage the inner transaction.
//   - Provides default methods `GetTxnInfoSchema`, `GetStmtReadTS` and `GetStmtForUpdateTS` and return the snapshot information schema or ts when `tidb_snapshot` is set.
//   - Provides other default methods like `Advise`, `OnStmtStart`, `OnStmtRetry` and `OnStmtErrorForNextAction`
//
// The subclass can set some inner property of `baseTxnContextProvider` when it is constructed.
// For example, `getStmtReadTSFunc` and `getStmtForUpdateTSFunc` should be set, and they will be called when `GetStmtReadTS`
// or `GetStmtForUpdate` to get the timestamp that should be used by the corresponding isolation level.
type baseTxnContextProvider struct {
	// States that should be initialized when baseTxnContextProvider is created and should not be changed after that
	sctx                   sessionctx.Context
	causalConsistencyOnly  bool
	onInitializeTxnCtx     func(*variable.TransactionContext)
	onTxnActiveFunc        func(kv.Transaction, sessiontxn.EnterNewTxnType)
	getStmtReadTSFunc      func() (uint64, error)
	getStmtForUpdateTSFunc func() (uint64, error)

	// Runtime states
	ctx             context.Context
	infoSchema      infoschema.InfoSchema
	txn             kv.Transaction
	isTxnPrepared   bool
	enterNewTxnType sessiontxn.EnterNewTxnType
	// constStartTS is only used by point get max ts optimization currently.
	// When constStartTS != 0, we use constStartTS directly without fetching it from tso.
	// To save the cpu cycles `PrepareTSFuture` will also not be called when warmup (postpone to activate txn).
	constStartTS uint64
}

// OnInitialize is the hook that should be called when enter a new txn with this provider
func (p *baseTxnContextProvider) OnInitialize(ctx context.Context, tp sessiontxn.EnterNewTxnType) (err error) {
	if p.getStmtReadTSFunc == nil || p.getStmtForUpdateTSFunc == nil {
		return errors.New("ts functions should not be nil")
	}

	p.ctx = ctx
	sessVars := p.sctx.GetSessionVars()
	activeNow := true
	switch tp {
	case sessiontxn.EnterNewTxnDefault:
		// As we will enter a new txn, we need to commit the old txn if it's still valid.
		// There are two main steps here to enter a new txn:
		// 1. prepareTxnWithOracleTS
		// 2. ActivateTxn
		if err := internal.CommitBeforeEnterNewTxn(p.ctx, p.sctx); err != nil {
			return err
		}
		if err := p.prepareTxnWithOracleTS(); err != nil {
			return err
		}
	case sessiontxn.EnterNewTxnWithBeginStmt:
		if !canReuseTxnWhenExplicitBegin(p.sctx) {
			// As we will enter a new txn, we need to commit the old txn if it's still valid.
			// There are two main steps here to enter a new txn:
			// 1. prepareTxnWithOracleTS
			// 2. ActivateTxn
			if err := internal.CommitBeforeEnterNewTxn(p.ctx, p.sctx); err != nil {
				return err
			}
			if err := p.prepareTxnWithOracleTS(); err != nil {
				return err
			}
		}
		sessVars.SetInTxn(true)
	case sessiontxn.EnterNewTxnBeforeStmt:
		activeNow = false
	default:
		return errors.Errorf("Unsupported type: %v", tp)
	}

	p.enterNewTxnType = tp
	p.infoSchema = p.sctx.GetDomainInfoSchema().(infoschema.InfoSchema)
	txnCtx := &variable.TransactionContext{
		TxnCtxNoNeedToRestore: variable.TxnCtxNoNeedToRestore{
			CreateTime: time.Now(),
			InfoSchema: p.infoSchema,
			ShardStep:  int(sessVars.ShardAllocateStep),
			TxnScope:   sessVars.CheckAndGetTxnScope(),
		},
	}
	if p.onInitializeTxnCtx != nil {
		p.onInitializeTxnCtx(txnCtx)
	}
	sessVars.TxnCtxMu.Lock()
	sessVars.TxnCtx = txnCtx
	sessVars.TxnCtxMu.Unlock()
	if variable.EnableMDL.Load() {
		sessVars.TxnCtx.EnableMDL = true
	}

	txn, err := p.sctx.Txn(false)
	if err != nil {
		return err
	}
	p.isTxnPrepared = txn.Valid() || p.sctx.GetPreparedTxnFuture() != nil
	if activeNow {
		_, err = p.ActivateTxn()
	}

	return err
}

// GetTxnInfoSchema returns the information schema used by txn
func (p *baseTxnContextProvider) GetTxnInfoSchema() infoschema.InfoSchema {
	if is := p.sctx.GetSessionVars().SnapshotInfoschema; is != nil {
		return is.(infoschema.InfoSchema)
	}
	if _, ok := p.infoSchema.(*infoschema.SessionExtendedInfoSchema); !ok {
		p.infoSchema = &infoschema.SessionExtendedInfoSchema{
			InfoSchema: p.infoSchema,
		}
		p.sctx.GetSessionVars().TxnCtx.InfoSchema = p.infoSchema
	}
	return p.infoSchema
}

func (p *baseTxnContextProvider) SetTxnInfoSchema(is infoschema.InfoSchema) {
	p.infoSchema = is
}

// GetTxnScope returns the current txn scope
func (p *baseTxnContextProvider) GetTxnScope() string {
	return p.sctx.GetSessionVars().TxnCtx.TxnScope
}

// GetReadReplicaScope returns the read replica scope
func (p *baseTxnContextProvider) GetReadReplicaScope() string {
	if txnScope := p.GetTxnScope(); txnScope != kv.GlobalTxnScope && txnScope != "" {
		// In local txn, we should use txnScope as the readReplicaScope
		return txnScope
	}

	if p.sctx.GetSessionVars().GetReplicaRead().IsClosestRead() {
		// If closest read is set, we should use the scope where instance located.
		return config.GetTxnScopeFromConfig()
	}

	// When it is not local txn or closet read, we should use global scope
	return kv.GlobalReplicaScope
}

// GetStmtReadTS returns the read timestamp used by select statement (not for select ... for update)
func (p *baseTxnContextProvider) GetStmtReadTS() (uint64, error) {
	if _, err := p.ActivateTxn(); err != nil {
		return 0, err
	}

	if snapshotTS := p.sctx.GetSessionVars().SnapshotTS; snapshotTS != 0 {
		return snapshotTS, nil
	}
	return p.getStmtReadTSFunc()
}

// GetStmtForUpdateTS returns the read timestamp used by update/insert/delete or select ... for update
func (p *baseTxnContextProvider) GetStmtForUpdateTS() (uint64, error) {
	if _, err := p.ActivateTxn(); err != nil {
		return 0, err
	}

	if snapshotTS := p.sctx.GetSessionVars().SnapshotTS; snapshotTS != 0 {
		return snapshotTS, nil
	}
	return p.getStmtForUpdateTSFunc()
}

// OnStmtStart is the hook that should be called when a new statement started
func (p *baseTxnContextProvider) OnStmtStart(ctx context.Context, _ ast.StmtNode) error {
	p.ctx = ctx
	return nil
}

// OnHandlePessimisticStmtStart is the hook that should be called when starts handling a pessimistic DML or
// a pessimistic select-for-update statements.
func (p *baseTxnContextProvider) OnHandlePessimisticStmtStart(_ context.Context) error {
	return nil
}

// OnStmtRetry is the hook that should be called when a statement is retried internally.
func (p *baseTxnContextProvider) OnStmtRetry(ctx context.Context) error {
	p.ctx = ctx
	return nil
}

// OnStmtCommit is the hook that should be called when a statement is executed successfully.
func (p *baseTxnContextProvider) OnStmtCommit(_ context.Context) error {
	return nil
}

// OnStmtRollback is the hook that should be called when a statement fails to execute.
func (p *baseTxnContextProvider) OnStmtRollback(_ context.Context, _ bool) error {
	return nil
}

// OnLocalTemporaryTableCreated is the hook that should be called when a local temporary table created.
func (p *baseTxnContextProvider) OnLocalTemporaryTableCreated() {
	p.infoSchema = temptable.AttachLocalTemporaryTableInfoSchema(p.sctx, p.infoSchema)
	p.sctx.GetSessionVars().TxnCtx.InfoSchema = p.infoSchema
	if p.txn != nil && p.txn.Valid() {
		if interceptor := temptable.SessionSnapshotInterceptor(p.sctx, p.infoSchema); interceptor != nil {
			p.txn.SetOption(kv.SnapInterceptor, interceptor)
		}
	}
}

// OnStmtErrorForNextAction is the hook that should be called when a new statement get an error
func (p *baseTxnContextProvider) OnStmtErrorForNextAction(point sessiontxn.StmtErrorHandlePoint, err error) (sessiontxn.StmtErrorAction, error) {
	switch point {
	case sessiontxn.StmtErrAfterPessimisticLock:
		// for pessimistic lock error, return the error by default
		return sessiontxn.ErrorAction(err)
	default:
		return sessiontxn.NoIdea()
	}
}

func (p *baseTxnContextProvider) getTxnStartTS() (uint64, error) {
	txn, err := p.ActivateTxn()
	if err != nil {
		return 0, err
	}
	return txn.StartTS(), nil
}

// ActivateTxn activates the transaction and set the relevant context variables.
func (p *baseTxnContextProvider) ActivateTxn() (kv.Transaction, error) {
	if p.txn != nil {
		return p.txn, nil
	}

	if err := p.prepareTxn(); err != nil {
		return nil, err
	}

	if p.constStartTS != 0 {
		if err := p.replaceTxnTsFuture(sessiontxn.ConstantFuture(p.constStartTS)); err != nil {
			return nil, err
		}
	}

	txnFuture := p.sctx.GetPreparedTxnFuture()
	txn, err := txnFuture.Wait(p.ctx, p.sctx)
	if err != nil {
		return nil, err
	}

	sessVars := p.sctx.GetSessionVars()
	sessVars.TxnCtx.StartTS = txn.StartTS()
	if sessVars.MemDBFootprint != nil {
		sessVars.MemDBFootprint.Detach()
	}
	sessVars.MemDBFootprint = nil

	if p.enterNewTxnType == sessiontxn.EnterNewTxnBeforeStmt && !sessVars.IsAutocommit() && sessVars.SnapshotTS == 0 {
		sessVars.SetInTxn(true)
	}

	txn.SetVars(sessVars.KVVars)

	readReplicaType := sessVars.GetReplicaRead()
	if readReplicaType.IsFollowerRead() {
		txn.SetOption(kv.ReplicaRead, readReplicaType)
	}

	if interceptor := temptable.SessionSnapshotInterceptor(p.sctx, p.infoSchema); interceptor != nil {
		txn.SetOption(kv.SnapInterceptor, interceptor)
	}

	if sessVars.StmtCtx.WeakConsistency {
		txn.SetOption(kv.IsolationLevel, kv.RC)
	}

	internal.SetTxnAssertionLevel(txn, sessVars.AssertionLevel)

	if p.causalConsistencyOnly {
		txn.SetOption(kv.GuaranteeLinearizability, false)
	}

	if p.onTxnActiveFunc != nil {
		p.onTxnActiveFunc(txn, p.enterNewTxnType)
	}

	if p.sctx.GetSessionVars().InRestrictedSQL {
		txn.SetOption(kv.RequestSourceInternal, true)
	}

	if tp := p.sctx.GetSessionVars().RequestSourceType; tp != "" {
		txn.SetOption(kv.RequestSourceType, tp)
	}

	p.txn = txn
	return txn, nil
}

// prepareTxn prepares txn with an oracle ts future. If the snapshotTS is set,
// the txn is prepared with it.
func (p *baseTxnContextProvider) prepareTxn() error {
	if p.isTxnPrepared {
		return nil
	}

	if snapshotTS := p.sctx.GetSessionVars().SnapshotTS; snapshotTS != 0 {
		return p.replaceTxnTsFuture(sessiontxn.ConstantFuture(snapshotTS))
	}

	future := newOracleFuture(p.ctx, p.sctx, p.sctx.GetSessionVars().TxnCtx.TxnScope)
	return p.replaceTxnTsFuture(future)
}

// prepareTxnWithOracleTS
// The difference between prepareTxnWithOracleTS and prepareTxn is that prepareTxnWithOracleTS
// does not consider snapshotTS
func (p *baseTxnContextProvider) prepareTxnWithOracleTS() error {
	if p.isTxnPrepared {
		return nil
	}

	future := newOracleFuture(p.ctx, p.sctx, p.sctx.GetSessionVars().TxnCtx.TxnScope)
	return p.replaceTxnTsFuture(future)
}

func (p *baseTxnContextProvider) forcePrepareConstStartTS(ts uint64) error {
	if p.txn != nil {
		return errors.New("cannot force prepare const start ts because txn is active")
	}
	p.constStartTS = ts
	p.isTxnPrepared = true
	return nil
}

func (p *baseTxnContextProvider) replaceTxnTsFuture(future oracle.Future) error {
	txn, err := p.sctx.Txn(false)
	if err != nil {
		return err
	}

	if txn.Valid() {
		return nil
	}

	txnScope := p.sctx.GetSessionVars().TxnCtx.TxnScope
	if err = p.sctx.PrepareTSFuture(p.ctx, future, txnScope); err != nil {
		return err
	}

	p.isTxnPrepared = true
	return nil
}

func (p *baseTxnContextProvider) isTidbSnapshotEnabled() bool {
	return p.sctx.GetSessionVars().SnapshotTS != 0
}

// isBeginStmtWithStaleRead indicates whether the current statement is `BeginStmt` type with stale read
// Because stale read will use `staleread.StalenessTxnContextProvider` for query, so if `staleread.IsStmtStaleness()`
// returns true in other providers, it means the current statement is `BeginStmt` with stale read
func (p *baseTxnContextProvider) isBeginStmtWithStaleRead() bool {
	return staleread.IsStmtStaleness(p.sctx)
}

// AdviseWarmup provides warmup for inner state
func (p *baseTxnContextProvider) AdviseWarmup() error {
	if p.isTxnPrepared || p.isBeginStmtWithStaleRead() {
		// When executing `START TRANSACTION READ ONLY AS OF ...` no need to warmUp
		return nil
	}
	return p.prepareTxn()
}

// AdviseOptimizeWithPlan providers optimization according to the plan
func (p *baseTxnContextProvider) AdviseOptimizeWithPlan(_ interface{}) error {
	return nil
}

// GetSnapshotWithStmtReadTS gets snapshot with read ts
func (p *baseTxnContextProvider) GetSnapshotWithStmtReadTS() (kv.Snapshot, error) {
	ts, err := p.GetStmtReadTS()
	if err != nil {
		return nil, err
	}

	return p.getSnapshotByTS(ts)
}

// GetSnapshotWithStmtForUpdateTS gets snapshot with for update ts
func (p *baseTxnContextProvider) GetSnapshotWithStmtForUpdateTS() (kv.Snapshot, error) {
	ts, err := p.GetStmtForUpdateTS()
	if err != nil {
		return nil, err
	}

	return p.getSnapshotByTS(ts)
}

// getSnapshotByTS get snapshot from store according to the snapshotTS and set the transaction related
// options before return
func (p *baseTxnContextProvider) getSnapshotByTS(snapshotTS uint64) (kv.Snapshot, error) {
	txn, err := p.sctx.Txn(false)
	if err != nil {
		return nil, err
	}

	txnCtx := p.sctx.GetSessionVars().TxnCtx
	if txn.Valid() && txnCtx.StartTS == txnCtx.GetForUpdateTS() && txnCtx.StartTS == snapshotTS {
		return txn.GetSnapshot(), nil
	}

	sessVars := p.sctx.GetSessionVars()
	snapshot := internal.GetSnapshotWithTS(
		p.sctx,
		snapshotTS,
		temptable.SessionSnapshotInterceptor(p.sctx, p.infoSchema),
	)

	replicaReadType := sessVars.GetReplicaRead()
	if replicaReadType.IsFollowerRead() &&
		!sessVars.StmtCtx.RCCheckTS &&
		!sessVars.RcWriteCheckTS {
		snapshot.SetOption(kv.ReplicaRead, replicaReadType)
	}

	return snapshot, nil
}

// canReuseTxnWhenExplicitBegin returns whether we should reuse the txn when starting a transaction explicitly
func canReuseTxnWhenExplicitBegin(sctx sessionctx.Context) bool {
	sessVars := sctx.GetSessionVars()
	txnCtx := sessVars.TxnCtx
	// If BEGIN is the first statement in TxnCtx, we can reuse the existing transaction, without the
	// need to call NewTxn, which commits the existing transaction and begins a new one.
	// If the last un-committed/un-rollback transaction is a time-bounded read-only transaction, we should
	// always create a new transaction.
	// If the variable `tidb_snapshot` is set, we should always create a new transaction because the current txn may be
	// initialized with snapshot ts.
	return txnCtx.History == nil && !txnCtx.IsStaleness && sessVars.SnapshotTS == 0
}

// newOracleFuture creates new future according to the scope and the session context
func newOracleFuture(ctx context.Context, sctx sessionctx.Context, scope string) oracle.Future {
	r, ctx := tracing.StartRegionEx(ctx, "isolation.newOracleFuture")
	defer r.End()

	failpoint.Inject("requestTsoFromPD", func() {
		sessiontxn.TsoRequestCountInc(sctx)
	})

	oracleStore := sctx.GetStore().GetOracle()
	option := &oracle.Option{TxnScope: scope}

	if sctx.GetSessionVars().LowResolutionTSO {
		return oracleStore.GetLowResolutionTimestampAsync(ctx, option)
	}
	return oracleStore.GetTimestampAsync(ctx, option)
}

// funcFuture implements oracle.Future
type funcFuture func() (uint64, error)

// Wait returns a ts got from the func
func (f funcFuture) Wait() (uint64, error) {
	return f()
}

// basePessimisticTxnContextProvider extends baseTxnContextProvider with some functionalities that are commonly used in
// pessimistic transactions.
type basePessimisticTxnContextProvider struct {
	baseTxnContextProvider
}

// OnHandlePessimisticStmtStart is the hook that should be called when starts handling a pessimistic DML or
// a pessimistic select-for-update statements.
func (p *basePessimisticTxnContextProvider) OnHandlePessimisticStmtStart(ctx context.Context) error {
	if err := p.baseTxnContextProvider.OnHandlePessimisticStmtStart(ctx); err != nil {
		return err
	}
	if p.sctx.GetSessionVars().PessimisticTransactionAggressiveLocking &&
		p.txn != nil &&
		p.sctx.GetSessionVars().ConnectionID != 0 &&
		!p.sctx.GetSessionVars().InRestrictedSQL {
		if err := p.txn.StartAggressiveLocking(); err != nil {
			return err
		}
	}
	return nil
}

// OnStmtRetry is the hook that should be called when a statement is retried internally.
func (p *basePessimisticTxnContextProvider) OnStmtRetry(ctx context.Context) error {
	if err := p.baseTxnContextProvider.OnStmtRetry(ctx); err != nil {
		return err
	}
	if p.txn != nil && p.txn.IsInAggressiveLockingMode() {
		if err := p.txn.RetryAggressiveLocking(ctx); err != nil {
			return err
		}
	}
	return nil
}

// OnStmtCommit is the hook that should be called when a statement is executed successfully.
func (p *basePessimisticTxnContextProvider) OnStmtCommit(ctx context.Context) error {
	if err := p.baseTxnContextProvider.OnStmtCommit(ctx); err != nil {
		return err
	}
	if p.txn != nil && p.txn.IsInAggressiveLockingMode() {
		if err := p.txn.DoneAggressiveLocking(ctx); err != nil {
			return err
		}
	}
	return nil
}

// OnStmtRollback is the hook that should be called when a statement fails to execute.
func (p *basePessimisticTxnContextProvider) OnStmtRollback(ctx context.Context, isForPessimisticRetry bool) error {
	if err := p.baseTxnContextProvider.OnStmtRollback(ctx, isForPessimisticRetry); err != nil {
		return err
	}
	if !isForPessimisticRetry && p.txn != nil && p.txn.IsInAggressiveLockingMode() {
		if err := p.txn.CancelAggressiveLocking(ctx); err != nil {
			return err
		}
	}
	return nil
}
