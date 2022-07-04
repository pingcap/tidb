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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/isolation"
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

	stmtNode    ast.StmtNode
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

	ts, err := m.ctxProvider.GetStmtForUpdateTS()
	if err != nil {
		return 0, err
	}

	failpoint.Inject("assertTxnManagerForUpdateTSEqual", func() {
		sessVars := m.sctx.GetSessionVars()
		if txnCtxForUpdateTS := sessVars.TxnCtx.GetForUpdateTS(); sessVars.SnapshotTS == 0 && ts != txnCtxForUpdateTS {
			panic(fmt.Sprintf("forUpdateTS not equal %d != %d", ts, txnCtxForUpdateTS))
		}
	})

	return ts, nil
}

// GetSnapshotWithStmtReadTS get snapshot with read ts
func (m *txnManager) GetSnapshotWithStmtReadTS() (kv.Snapshot, error) {
	if m.ctxProvider == nil {
		return nil, errors.New("context provider not set")
	}
	return m.ctxProvider.GetSnapshotWithStmtReadTS()
}

// GetSnapshotWithStmtForUpdateTS get snapshot with for update ts
func (m *txnManager) GetSnapshotWithStmtForUpdateTS() (kv.Snapshot, error) {
	if m.ctxProvider == nil {
		return nil, errors.New("context provider not set")
	}
	return m.ctxProvider.GetSnapshotWithStmtForUpdateTS()
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
	m.stmtNode = nil
}

func (m *txnManager) GetCurrentStmt() ast.StmtNode {
	return m.stmtNode
}

// OnStmtStart is the hook that should be called when a new statement started
func (m *txnManager) OnStmtStart(ctx context.Context, node ast.StmtNode) error {
	m.stmtNode = node

	if m.ctxProvider == nil {
		return errors.New("context provider not set")
	}
	return m.ctxProvider.OnStmtStart(ctx, m.stmtNode)
}

// OnStmtErrorForNextAction is the hook that should be called when a new statement get an error
func (m *txnManager) OnStmtErrorForNextAction(point sessiontxn.StmtErrorHandlePoint, err error) (sessiontxn.StmtErrorAction, error) {
	if m.ctxProvider == nil {
		return sessiontxn.NoIdea()
	}
	return m.ctxProvider.OnStmtErrorForNextAction(point, err)
}

// ActivateTxn decides to activate txn according to the parameter `active`
func (m *txnManager) ActivateTxn() (kv.Transaction, error) {
	if m.ctxProvider == nil {
		return nil, errors.AddStack(kv.ErrInvalidTxn)
	}
	return m.ctxProvider.ActivateTxn()
}

// OnStmtRetry is the hook that should be called when a statement retry
func (m *txnManager) OnStmtRetry(ctx context.Context) error {
	if m.ctxProvider == nil {
		return errors.New("context provider not set")
	}
	return m.ctxProvider.OnStmtRetry(ctx)
}

func (m *txnManager) AdviseWarmup() error {
	if m.ctxProvider != nil {
		return m.ctxProvider.AdviseWarmup()
	}
	return nil
}

// AdviseOptimizeWithPlan providers optimization according to the plan
func (m *txnManager) AdviseOptimizeWithPlan(plan interface{}) error {
	if m.ctxProvider != nil {
		return m.ctxProvider.AdviseOptimizeWithPlan(plan)
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
		case ast.Serializable:
			// The Oracle serializable isolation is actually SI in pessimistic mode.
			// Do not update ForUpdateTS when the user is using the Serializable isolation level.
			// It can be used temporarily on the few occasions when an Oracle-like isolation level is needed.
			// Support for this does not mean that TiDB supports serializable isolation of MySQL.
			// tidb_skip_isolation_level_check should still be disabled by default.
			return isolation.NewPessimisticSerializableTxnContextProvider(m.sctx, r.CausalConsistencyOnly), nil
		default:
			// We use Repeatable read for all other cases.
			return isolation.NewPessimisticRRTxnContextProvider(m.sctx, r.CausalConsistencyOnly), nil
		}
	default:
		return nil, errors.Errorf("Invalid txn mode '%s'", txnMode)
	}
}
