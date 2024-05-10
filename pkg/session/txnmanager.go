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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/isolation"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

	// We always reuse the same OptimisticTxnContextProvider in one session to reduce memory allocation cost for every new txn.
	reservedOptimisticProviders [2]isolation.OptimisticTxnContextProvider

	// used for slow transaction logs
	events          []event
	lastInstant     time.Time
	enterTxnInstant time.Time
}

type event struct {
	event    string
	duration time.Duration
}

func (s event) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("event", s.event)
	enc.AddDuration("gap", s.duration)
	return nil
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

func (m *txnManager) GetTxnScope() string {
	if m.ctxProvider == nil {
		return kv.GlobalTxnScope
	}
	return m.ctxProvider.GetTxnScope()
}

func (m *txnManager) GetReadReplicaScope() string {
	if m.ctxProvider == nil {
		return kv.GlobalReplicaScope
	}
	return m.ctxProvider.GetReadReplicaScope()
}

// GetSnapshotWithStmtReadTS gets snapshot with read ts
func (m *txnManager) GetSnapshotWithStmtReadTS() (kv.Snapshot, error) {
	if m.ctxProvider == nil {
		return nil, errors.New("context provider not set")
	}
	return m.ctxProvider.GetSnapshotWithStmtReadTS()
}

// GetSnapshotWithStmtForUpdateTS gets snapshot with for update ts
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

	m.resetEvents()
	m.recordEvent("enter txn")
	return nil
}

func (m *txnManager) OnTxnEnd() {
	m.ctxProvider = nil
	m.stmtNode = nil

	m.events = append(m.events, event{event: "txn end", duration: time.Since(m.lastInstant)})

	duration := time.Since(m.enterTxnInstant)
	threshold := m.sctx.GetSessionVars().SlowTxnThreshold
	if threshold > 0 && uint64(duration.Milliseconds()) >= threshold {
		logutil.BgLogger().Info(
			"slow transaction", zap.Duration("duration", duration),
			zap.Uint64("conn", m.sctx.GetSessionVars().ConnectionID),
			zap.Uint64("txnStartTS", m.sctx.GetSessionVars().TxnCtx.StartTS),
			zap.Objects("events", m.events),
		)
	}

	m.lastInstant = time.Now()
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

	var sql string
	if node != nil {
		sql = node.OriginalText()
		sql = parser.Normalize(sql, m.sctx.GetSessionVars().EnableRedactLog)
	}
	m.recordEvent(sql)
	return m.ctxProvider.OnStmtStart(ctx, m.stmtNode)
}

// OnStmtEnd implements the TxnManager interface
func (m *txnManager) OnStmtEnd() {
	m.recordEvent("stmt end")
}

// OnPessimisticStmtStart is the hook that should be called when starts handling a pessimistic DML or
// a pessimistic select-for-update statements.
func (m *txnManager) OnPessimisticStmtStart(ctx context.Context) error {
	if m.ctxProvider == nil {
		return errors.New("context provider not set")
	}
	return m.ctxProvider.OnPessimisticStmtStart(ctx)
}

// OnPessimisticStmtEnd is the hook that should be called when finishes handling a pessimistic DML or
// select-for-update statement.
func (m *txnManager) OnPessimisticStmtEnd(ctx context.Context, isSuccessful bool) error {
	if m.ctxProvider == nil {
		return errors.New("context provider not set")
	}
	return m.ctxProvider.OnPessimisticStmtEnd(ctx, isSuccessful)
}

// OnStmtErrorForNextAction is the hook that should be called when a new statement get an error
func (m *txnManager) OnStmtErrorForNextAction(ctx context.Context, point sessiontxn.StmtErrorHandlePoint, err error) (sessiontxn.StmtErrorAction, error) {
	if m.ctxProvider == nil {
		return sessiontxn.NoIdea()
	}
	return m.ctxProvider.OnStmtErrorForNextAction(ctx, point, err)
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

// OnStmtCommit is the hook that should be called when a statement is executed successfully.
func (m *txnManager) OnStmtCommit(ctx context.Context) error {
	if m.ctxProvider == nil {
		return errors.New("context provider not set")
	}
	m.recordEvent("stmt commit")
	return m.ctxProvider.OnStmtCommit(ctx)
}

func (m *txnManager) recordEvent(eventName string) {
	if m.events == nil {
		m.resetEvents()
	}
	m.events = append(m.events, event{event: eventName, duration: time.Since(m.lastInstant)})
	m.lastInstant = time.Now()
}

func (m *txnManager) resetEvents() {
	if m.events == nil {
		m.events = make([]event, 0, 10)
	} else {
		m.events = m.events[:0]
	}
	m.enterTxnInstant = time.Now()
}

// OnStmtRollback is the hook that should be called when a statement fails to execute.
func (m *txnManager) OnStmtRollback(ctx context.Context, isForPessimisticRetry bool) error {
	if m.ctxProvider == nil {
		return errors.New("context provider not set")
	}
	m.recordEvent("stmt rollback")
	return m.ctxProvider.OnStmtRollback(ctx, isForPessimisticRetry)
}

// OnLocalTemporaryTableCreated is the hook that should be called when a temporary table created.
// The provider will update its state then
func (m *txnManager) OnLocalTemporaryTableCreated() {
	if m.ctxProvider != nil {
		m.ctxProvider.OnLocalTemporaryTableCreated()
	}
}

func (m *txnManager) AdviseWarmup() error {
	if m.sctx.GetSessionVars().BulkDMLEnabled {
		// We don't want to validate the feasibility of pipelined DML here.
		// We'd like to check it later after optimization so that optimizer info can be used.
		// And it does not make much sense to save such a little time for pipelined-dml as it's
		// for bulk processing.
		return nil
	}

	if m.ctxProvider != nil {
		return m.ctxProvider.AdviseWarmup()
	}
	return nil
}

// AdviseOptimizeWithPlan providers optimization according to the plan
func (m *txnManager) AdviseOptimizeWithPlan(plan any) error {
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
		m.sctx.GetSessionVars().TxnCtx.StaleReadTs = r.StaleReadTS
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
		provider := &m.reservedOptimisticProviders[0]
		if old, ok := m.ctxProvider.(*isolation.OptimisticTxnContextProvider); ok && old == provider {
			// We should make sure the new provider is not the same with the old one
			provider = &m.reservedOptimisticProviders[1]
		}
		provider.ResetForNewTxn(m.sctx, r.CausalConsistencyOnly)
		return provider, nil
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

// SetOptionsBeforeCommit sets options before commit.
func (m *txnManager) SetOptionsBeforeCommit(txn kv.Transaction, commitTSChecker func(uint64) bool) error {
	return m.ctxProvider.SetOptionsBeforeCommit(txn, commitTSChecker)
}
