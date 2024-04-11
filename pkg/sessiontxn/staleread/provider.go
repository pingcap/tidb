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

package staleread

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/internal"
	"github.com/pingcap/tidb/pkg/table/temptable"
)

// StalenessTxnContextProvider implements sessiontxn.TxnContextProvider
type StalenessTxnContextProvider struct {
	ctx  context.Context
	sctx sessionctx.Context
	is   infoschema.InfoSchema
	ts   uint64
	txn  kv.Transaction
}

// NewStalenessTxnContextProvider creates a new StalenessTxnContextProvider
func NewStalenessTxnContextProvider(sctx sessionctx.Context, ts uint64, is infoschema.InfoSchema) *StalenessTxnContextProvider {
	return &StalenessTxnContextProvider{
		sctx: sctx,
		is:   is,
		ts:   ts,
	}
}

// GetTxnInfoSchema returns the information schema used by txn
func (p *StalenessTxnContextProvider) GetTxnInfoSchema() infoschema.InfoSchema {
	return p.is
}

// GetTxnScope returns the current txn scope
func (p *StalenessTxnContextProvider) GetTxnScope() string {
	return p.sctx.GetSessionVars().TxnCtx.TxnScope
}

// GetReadReplicaScope returns the read replica scope
func (p *StalenessTxnContextProvider) GetReadReplicaScope() string {
	return config.GetTxnScopeFromConfig()
}

// GetStmtReadTS returns the read timestamp
func (p *StalenessTxnContextProvider) GetStmtReadTS() (uint64, error) {
	return p.ts, nil
}

// GetStmtForUpdateTS will return an error because stale read does not support it
func (p *StalenessTxnContextProvider) GetStmtForUpdateTS() (uint64, error) {
	return 0, errors.New("GetForUpdateTS not supported for stalenessTxnProvider")
}

// OnInitialize is the hook that should be called when enter a new txn with this provider
func (p *StalenessTxnContextProvider) OnInitialize(ctx context.Context, tp sessiontxn.EnterNewTxnType) error {
	p.ctx = ctx
	switch tp {
	case sessiontxn.EnterNewTxnDefault, sessiontxn.EnterNewTxnWithBeginStmt:
		return p.activateStaleTxn()
	case sessiontxn.EnterNewTxnWithReplaceProvider:
		return p.enterNewStaleTxnWithReplaceProvider()
	default:
		return errors.Errorf("Unsupported type: %v", tp)
	}
}

// activateStaleTxn first commit old transaction if needed, and then prepare and activate a transaction
// with the staleness snapshot ts. After that, it sets the relevant context variables.
func (p *StalenessTxnContextProvider) activateStaleTxn() error {
	var err error
	if err = internal.CommitBeforeEnterNewTxn(p.ctx, p.sctx); err != nil {
		return err
	}

	txnScope := kv.GlobalTxnScope
	if err = p.sctx.PrepareTSFuture(p.ctx, sessiontxn.ConstantFuture(p.ts), txnScope); err != nil {
		return err
	}

	txnFuture := p.sctx.GetPreparedTxnFuture()
	txn, err := txnFuture.Wait(p.ctx, p.sctx)
	if err != nil {
		return err
	}

	sessVars := p.sctx.GetSessionVars()
	txn.SetVars(sessVars.KVVars)
	txn.SetOption(kv.IsStalenessReadOnly, true)
	txn.SetOption(kv.TxnScope, txnScope)
	internal.SetTxnAssertionLevel(txn, sessVars.AssertionLevel)
	is, err := GetSessionSnapshotInfoSchema(p.sctx, p.ts)
	if err != nil {
		return errors.Trace(err)
	}
	sessVars.TxnCtxMu.Lock()
	sessVars.TxnCtx = &variable.TransactionContext{
		TxnCtxNoNeedToRestore: variable.TxnCtxNoNeedToRestore{
			InfoSchema:  is,
			CreateTime:  time.Now(),
			StartTS:     txn.StartTS(),
			ShardStep:   int(sessVars.ShardAllocateStep),
			IsStaleness: true,
			TxnScope:    txnScope,
		},
	}
	sessVars.TxnCtxMu.Unlock()

	if interceptor := temptable.SessionSnapshotInterceptor(p.sctx, is); interceptor != nil {
		txn.SetOption(kv.SnapInterceptor, interceptor)
	}

	p.is = is
	err = p.sctx.GetSessionVars().SetSystemVar(variable.TiDBSnapshot, "")

	return err
}

func (p *StalenessTxnContextProvider) enterNewStaleTxnWithReplaceProvider() error {
	if p.is == nil {
		is, err := GetSessionSnapshotInfoSchema(p.sctx, p.ts)
		if err != nil {
			return err
		}
		p.is = is
	}

	txnCtx := p.sctx.GetSessionVars().TxnCtx
	txnCtx.TxnScope = kv.GlobalTxnScope
	txnCtx.IsStaleness = true
	txnCtx.InfoSchema = p.is
	return nil
}

// OnStmtStart is the hook that should be called when a new statement starte
func (p *StalenessTxnContextProvider) OnStmtStart(ctx context.Context, _ ast.StmtNode) error {
	p.ctx = ctx
	return nil
}

// OnPessimisticStmtStart is the hook that should be called when starts handling a pessimistic DML or
// a pessimistic select-for-update statements.
func (p *StalenessTxnContextProvider) OnPessimisticStmtStart(_ context.Context) error {
	return nil
}

// OnPessimisticStmtEnd is the hook that should be called when finishes handling a pessimistic DML or
// select-for-update statement.
func (p *StalenessTxnContextProvider) OnPessimisticStmtEnd(_ context.Context, _ bool) error {
	return nil
}

// ActivateTxn activates the transaction.
func (p *StalenessTxnContextProvider) ActivateTxn() (kv.Transaction, error) {
	if p.txn != nil {
		return p.txn, nil
	}

	err := p.activateStaleTxn()
	if err != nil {
		return nil, err
	}

	txn, err := p.sctx.Txn(false)
	if err != nil {
		return nil, err
	}

	p.txn = txn

	return p.txn, nil
}

// OnStmtErrorForNextAction is the hook that should be called when a new statement get an error
func (p *StalenessTxnContextProvider) OnStmtErrorForNextAction(ctx context.Context, point sessiontxn.StmtErrorHandlePoint, err error) (sessiontxn.StmtErrorAction, error) {
	return sessiontxn.NoIdea()
}

// OnStmtRetry is the hook that should be called when a statement retry
func (p *StalenessTxnContextProvider) OnStmtRetry(ctx context.Context) error {
	p.ctx = ctx
	return nil
}

// OnStmtCommit is the hook that should be called when a statement is executed successfully.
func (p *StalenessTxnContextProvider) OnStmtCommit(_ context.Context) error {
	return nil
}

// OnStmtRollback is the hook that should be called when a statement fails to execute.
func (p *StalenessTxnContextProvider) OnStmtRollback(_ context.Context, _ bool) error {
	return nil
}

// AdviseWarmup provides warmup for inner state
func (p *StalenessTxnContextProvider) AdviseWarmup() error {
	return nil
}

// AdviseOptimizeWithPlan providers optimization according to the plan
func (p *StalenessTxnContextProvider) AdviseOptimizeWithPlan(_ any) error {
	return nil
}

// GetSnapshotWithStmtReadTS gets snapshot with read ts and set the transaction related options
// before return
func (p *StalenessTxnContextProvider) GetSnapshotWithStmtReadTS() (kv.Snapshot, error) {
	txn, err := p.sctx.Txn(false)
	if err != nil {
		return nil, err
	}

	if txn.Valid() {
		return txn.GetSnapshot(), nil
	}

	sessVars := p.sctx.GetSessionVars()
	snapshot := internal.GetSnapshotWithTS(
		p.sctx,
		p.ts,
		temptable.SessionSnapshotInterceptor(p.sctx, p.is),
	)

	replicaReadType := sessVars.GetReplicaRead()
	if replicaReadType.IsFollowerRead() {
		snapshot.SetOption(kv.ReplicaRead, replicaReadType)
	}
	snapshot.SetOption(kv.IsStalenessReadOnly, true)

	return snapshot, nil
}

// GetSnapshotWithStmtForUpdateTS gets snapshot with for update ts
func (p *StalenessTxnContextProvider) GetSnapshotWithStmtForUpdateTS() (kv.Snapshot, error) {
	return nil, errors.New("GetSnapshotWithStmtForUpdateTS not supported for stalenessTxnProvider")
}

// OnLocalTemporaryTableCreated will not be called for StalenessTxnContextProvider
func (p *StalenessTxnContextProvider) OnLocalTemporaryTableCreated() {}

// SetOptionsBeforeCommit sets the options before commit, because stale read txn is read only, no need to set options.
func (p *StalenessTxnContextProvider) SetOptionsBeforeCommit(txn kv.Transaction, commitTSChecker func(uint64) bool) error {
	return nil
}
