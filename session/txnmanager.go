// Copyright 2021 PingCAP, Inc.
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

package session

import (
	"context"
	"errors"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/readcommitted"
	"github.com/pingcap/tidb/sessiontxn/staleread"
)

func init() {
	sessiontxn.GetTxnManager = getTxnManager
}

func getTxnManager(sctx sessionctx.Context) sessiontxn.TxnManager {
	if manager, ok := sctx.GetSessionVars().TxnManager.(sessiontxn.TxnManager); ok {
		return manager
	}

	manager := newTxnManager(sctx)
	sctx.GetSessionVars().TxnManager = manager
	return manager
}

// txnManager implements sessiontxn.TxnManager
type txnManager struct {
	sctx sessionctx.Context

	ctxProvider sessiontxn.TxnContextProvider
}

func newTxnManager(sctx sessionctx.Context) *txnManager {
	return &txnManager{sctx: sctx}
}

func (m *txnManager) GetTxnInfoSchema() infoschema.InfoSchema {
	if m.ctxProvider == nil {
		return nil
	}
	return m.ctxProvider.GetTxnInfoSchema()
}

func (m *txnManager) GetReadTS() (ts uint64, err error) {
	if m.ctxProvider == nil {
		return 0, errors.New("context provider not set")
	}
	if ts, err = m.ctxProvider.GetReadTS(); err != nil {
		return 0, err
	}
	m.handleAutoCommit()
	return
}

func (m *txnManager) GetForUpdateTS() (ts uint64, err error) {
	if m.ctxProvider == nil {
		return 0, errors.New("context provider not set")
	}
	if ts, err = m.ctxProvider.GetForUpdateTS(); err != nil {
		return 0, err
	}
	m.handleAutoCommit()
	return
}

func (m *txnManager) GetContextProvider() sessiontxn.TxnContextProvider {
	return m.ctxProvider
}

func (m *txnManager) ReplaceContextProvider(provider sessiontxn.TxnContextProvider) error {
	m.ctxProvider = provider
	return nil
}

func (m *txnManager) EnterNewTxn(ctx context.Context, r *sessiontxn.NewTxnRequest) error {
	sessVars := m.sctx.GetSessionVars()
	txnCtx := sessVars.TxnCtx
	if r.ExplictStart && txnCtx.History == nil && !txnCtx.IsStaleness && r.StaleReadTS == 0 {
		// If BEGIN is the first statement in TxnCtx, we can reuse the existing transaction, without the
		// need to call NewTxn, which commits the existing transaction and begins a new one.
		// If the last un-committed/un-rollback transaction is a time-bounded read-only transaction, we should
		// always create a new transaction.
		return nil
	}

	txnMode := r.TxnMode
	if txnMode == "" {
		txnMode = sessVars.TxnMode
	}

	switch {
	case r.StaleReadTS > 0:
		// stale read should be the first place to check, `is` is nil because it will be fetched when `ActiveTxn`
		m.ctxProvider = staleread.NewStalenessTxnContextProvider(m.sctx, r.StaleReadTS, nil)
	case txnMode == ast.Pessimistic && sessVars.IsIsolation(ast.ReadCommitted):
		// when txn mode is pessimistic and isolation level is 'READ-COMMITTED', use RC
		m.ctxProvider = readcommitted.NewRCTxnContextProvider(m.sctx, r.CausalConsistencyOnly)
	default:
		// otherwise, use `SimpleTxnContextProvider` for compatibility
		m.ctxProvider = &sessiontxn.SimpleTxnContextProvider{Sctx: m.sctx, CausalConsistencyOnly: r.CausalConsistencyOnly}
	}

	if r.ExplictStart {
		if _, err := m.ctxProvider.ActiveTxn(ctx); err != nil {
			return err
		}
		// With START TRANSACTION, autocommit remains disabled until you end
		// the transaction with COMMIT or ROLLBACK. The autocommit mode then
		// reverts to its previous state.
		sessVars.SetInTxn(true)
	} else {
		sessVars.SetInTxn(false)
	}
	return nil
}

func (m *txnManager) ActiveTxn(ctx context.Context) (txn kv.Transaction, err error) {
	if m.ctxProvider == nil {
		return nil, errors.New("context provider not set")
	}
	if txn, err = m.ctxProvider.ActiveTxn(ctx); err != nil {
		return nil, err
	}
	m.handleAutoCommit()
	return txn, err
}

func (m *txnManager) IsTxnActive() bool {
	txn, _ := m.sctx.Txn(false)
	return txn.Valid()
}

func (m *txnManager) OnStmtStart(ctx context.Context) error {
	if m.ctxProvider == nil {
		return errors.New("context provider not set")
	}
	return m.ctxProvider.OnStmtStart(ctx)
}

func (m *txnManager) OnStmtError(err error) {
	if m.ctxProvider != nil {
		m.ctxProvider.OnStmtError(err)
	}
}

func (m *txnManager) OnStmtRetry(ctx context.Context, err error) error {
	if m.ctxProvider == nil {
		return errors.New("context provider not set")
	}
	return m.ctxProvider.OnStmtRetry(ctx, err)
}

func (m *txnManager) Advise(opt sessiontxn.AdviceOption, val ...interface{}) error {
	if m.ctxProvider == nil {
		return errors.New("context provider not set")
	}

	return m.ctxProvider.Advise(opt, val)
}

func (m *txnManager) handleAutoCommit() {
	sessVars := m.sctx.GetSessionVars()
	if sessVars.IsAutocommit() && m.IsTxnActive() {
		sessVars.SetInTxn(true)
	}
}
