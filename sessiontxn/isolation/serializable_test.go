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

package isolation_test

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/isolation"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
	"testing"
	"time"
)

func TestPessimisticSerializableTxnProviderTS(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	se := tk.Session()
	provider := initializePessimisticSerializableProvider(t, tk)

	stmts, _, err := parser.New().Parse("select * from t", "", "")
	require.NoError(t, err)
	readOnlyStmt := stmts[0]

	stmts, _, err = parser.New().Parse("select * from t for update", "", "")
	require.NoError(t, err)
	forUpdateStmt := stmts[0]

	compareTS := getOracleTS(t, se)
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	ts, err := provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Greater(t, compareTS, ts)
	prevTs := ts

	// In Oracle-like serializable isolation, readTS equals to the for update ts
	require.NoError(t, executor.ResetContextOfStmt(se, forUpdateStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, compareTS, ts)
	require.Equal(t, prevTs, ts)
}

func TestPessimisticSerializableTxnContextProviderLockError(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	se := tk.Session()
	provider := initializePessimisticSerializableProvider(t, tk)

	stmts, _, err := parser.New().Parse("select * from t for update", "", "")
	require.NoError(t, err)
	stmt := stmts[0]

	// retryable errors
	for _, lockErr := range []error{
		kv.ErrWriteConflict,
		&tikverr.ErrDeadlock{Deadlock: &kvrpcpb.Deadlock{}, IsRetryable: true},
	} {
		require.NoError(t, executor.ResetContextOfStmt(se, stmt))
		require.NoError(t, provider.OnStmtStart(context.TODO()))
		nextAction, err := provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
		require.Same(t, lockErr, err)
		require.Equal(t, sessiontxn.StmtActionError, nextAction)
	}

	// non-retryable errors
	for _, lockErr := range []error{
		&tikverr.ErrDeadlock{Deadlock: &kvrpcpb.Deadlock{}, IsRetryable: false},
		errors.New("err"),
	} {
		require.NoError(t, executor.ResetContextOfStmt(se, stmt))
		require.NoError(t, provider.OnStmtStart(context.TODO()))
		nextAction, err := provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
		require.Same(t, lockErr, err)
		require.Equal(t, sessiontxn.StmtActionError, nextAction)
	}
}

func TestSerializableInitialize(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	se := tk.Session()
	tk.MustExec("set tidb_skip_isolation_level_check = 1")
	tk.MustExec("set @@tx_isolation = 'SERIALIZABLE'")
	tk.MustExec("set @@tidb_txn_mode='pessimistic'")

	// begin outsize a txn
	assert := activeSerializableAssert(t, se, true)
	tk.MustExec("begin")
	assert.Check(t)

	// begin outsize a txn
	assert = activeSerializableAssert(t, se, true)
	tk.MustExec("begin")
	assert.Check(t)

	// START TRANSACTION WITH CAUSAL CONSISTENCY ONLY
	assert = activeSerializableAssert(t, se, true)
	assert.causalConsistencyOnly = true
	tk.MustExec("START TRANSACTION WITH CAUSAL CONSISTENCY ONLY")
	assert.Check(t)

	// EnterNewTxnDefault will create an active txn, but not explicit
	assert = activeSerializableAssert(t, se, false)
	require.NoError(t, sessiontxn.GetTxnManager(se).EnterNewTxn(context.TODO(), &sessiontxn.EnterNewTxnRequest{
		Type:    sessiontxn.EnterNewTxnDefault,
		TxnMode: ast.Pessimistic,
	}))
	assert.Check(t)

	// non-active txn and then active it
	tk.MustExec("rollback")
	tk.MustExec("set @@autocommit=0")
	assert = inActiveSerializableAssert(se)
	assertAfterActive := activeSerializableAssert(t, se, true)
	require.NoError(t, se.PrepareTxnCtx(context.TODO()))
	provider := assert.CheckAndGetProvider(t)
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	ts, err := provider.GetStmtReadTS()
	require.NoError(t, err)
	assertAfterActive.Check(t)
	require.Equal(t, ts, se.GetSessionVars().TxnCtx.StartTS)
	tk.MustExec("rollback")
}

func activeSerializableAssert(t *testing.T, sctx sessionctx.Context,
	inTxn bool) *txnAssert[*isolation.PessimisticSerializableTxnContextProvider] {
	return &txnAssert[*isolation.PessimisticSerializableTxnContextProvider]{
		sctx:         sctx,
		isolation:    "SERIALIZABLE",
		minStartTime: time.Now(),
		active:       true,
		inTxn:        inTxn,
		minStartTS:   getOracleTS(t, sctx),
	}
}

func inActiveSerializableAssert(sctx sessionctx.Context) *txnAssert[*isolation.PessimisticSerializableTxnContextProvider] {
	return &txnAssert[*isolation.PessimisticSerializableTxnContextProvider]{
		sctx:         sctx,
		isolation:    "SERIALIZABLE",
		minStartTime: time.Now(),
		active:       false,
	}
}

func initializePessimisticSerializableProvider(t *testing.T,
	tk *testkit.TestKit) *isolation.PessimisticSerializableTxnContextProvider {
	tk.MustExec("set tidb_skip_isolation_level_check = 1")
	tk.MustExec("set @@tx_isolation = 'SERIALIZABLE'")
	assert := activeSerializableAssert(t, tk.Session(), true)
	tk.MustExec("begin pessimistic")
	return assert.CheckAndGetProvider(t)
}
