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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/isolation"
	"github.com/pingcap/tidb/sessiontxn/legacy"
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
	if m.ctxProvider != nil {
		return m.ctxProvider.GetTxnInfoSchema()
	}

	if is := m.sctx.GetDomainInfoSchema(); is != nil {
		return is.(infoschema.InfoSchema)
	}

	return nil
}

func (m *txnManager) GetStmtReadTS() (uint64, error) {
	if m.ctxProvider == nil {
		return 0, errors.New("context provider not set")
	}
	return m.ctxProvider.GetStmtReadTS()
}

func (m *txnManager) GetStmtForUpdateTS() (uint64, error) {
	if m.ctxProvider == nil {
		return 0, errors.New("context provider not set")
	}
	return m.ctxProvider.GetStmtForUpdateTS()
}

func (m *txnManager) GetContextProvider() sessiontxn.TxnContextProvider {
	return m.ctxProvider
}

func (m *txnManager) EnterNewTxn(ctx context.Context, r *sessiontxn.EnterNewTxnRequest) error {
	ctxProvider, err := m.newProviderWithRequest(r)
	if err != nil {
		return err
	}

	if err = ctxProvider.OnInitialize(ctx, r.Type); err != nil {
		m.sctx.RollbackTxn(ctx)
		return err
	}

	m.ctxProvider = ctxProvider
	if r.Type == sessiontxn.EnterNewTxnWithBeginStmt {
		m.sctx.GetSessionVars().SetInTxn(true)
	}
	return nil
}

func (m *txnManager) OnTxnEnd() {
	m.ctxProvider = nil
}

// OnStmtStart is the hook that should be called when a new statement started
func (m *txnManager) OnStmtStart(ctx context.Context) error {
	if m.ctxProvider == nil {
		return errors.New("context provider not set")
	}
	return m.ctxProvider.OnStmtStart(ctx)
}

// OnStmtErrorForNextAction is the hook that should be called when a new statement get an error
func (m *txnManager) OnStmtErrorForNextAction(point sessiontxn.StmtErrorHandlePoint, err error) (sessiontxn.StmtErrorAction, error) {
	if m.ctxProvider == nil {
		return sessiontxn.NoIdea()
	}
	return m.ctxProvider.OnStmtErrorForNextAction(point, err)
}

// OnStmtRetry is the hook that should be called when a statement retry
func (m *txnManager) OnStmtRetry(ctx context.Context) error {
	if m.ctxProvider == nil {
		return errors.New("context provider not set")
	}
	return m.ctxProvider.OnStmtRetry(ctx)
}

func (m *txnManager) Advise(tp sessiontxn.AdviceType, val ...any) error {
	if m.ctxProvider != nil {
		return m.ctxProvider.Advise(tp, val)
	}
	return nil
}

func (m *txnManager) newProviderWithRequest(r *sessiontxn.EnterNewTxnRequest) (sessiontxn.TxnContextProvider, error) {
	if r.Provider != nil {
		return r.Provider, nil
	}

	if r.StaleReadTS > 0 {
		return staleread.NewStalenessTxnContextProvider(m.sctx, r.StaleReadTS, nil), nil
	}

	sessVars := m.sctx.GetSessionVars()

	txnMode := r.TxnMode
	if txnMode == "" {
		txnMode = sessVars.TxnMode
	}

	switch txnMode {
	case "", ast.Optimistic:
		// When txnMode is 'OPTIMISTIC' or '', the transaction should be optimistic
		return isolation.NewOptimisticTxnContextProvider(m.sctx, r.CausalConsistencyOnly), nil
	case ast.Pessimistic:
		// When txnMode is 'PESSIMISTIC', the provider should be determined by the isolation level
		switch sessVars.IsolationLevelForNewTxn() {
		case ast.ReadCommitted:
			return isolation.NewPessimisticRCTxnContextProvider(m.sctx, r.CausalConsistencyOnly), nil
		default:
			return &legacy.SimpleTxnContextProvider{
				Sctx:                  m.sctx,
				Pessimistic:           txnMode == ast.Pessimistic,
				CausalConsistencyOnly: r.CausalConsistencyOnly,
				UpdateForUpdateTS:     executor.UpdateForUpdateTS,
			}, nil
		}
	default:
		return nil, errors.Errorf("Invalid txn mode '%s'", txnMode)
	}
}
