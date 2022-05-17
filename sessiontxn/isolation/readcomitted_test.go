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
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/isolation"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/oracle"
)

func TestPessimisticRCTxnContextProviderRCCheck(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_rc_read_check_ts=1")
	se := tk.Session()
	provider := initializePessimisticRCProvider(t, tk)

	stmts, _, err := parser.New().Parse("select * from t", "", "")
	require.NoError(t, err)
	readOnlyStmt := stmts[0]

	stmts, _, err = parser.New().Parse("select * from t for update", "", "")
	require.NoError(t, err)
	forUpdateStmt := stmts[0]

	compareTS := getOracleTS(t, se)
	// first ts should request from tso
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	ts, err := provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTS)
	rcCheckTS := ts

	// second ts should reuse first ts
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	ts, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Equal(t, rcCheckTS, ts)

	// when one statement did not getStmtReadTS, the next one should still reuse the first ts
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	ts, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Equal(t, rcCheckTS, ts)

	// error will invalidate the rc check
	nextAction, err := provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterQuery, kv.ErrWriteConflict)
	require.NoError(t, err)
	require.Equal(t, sessiontxn.StmtActionRetryReady, nextAction)
	_, err = provider.GetStmtReadTS()
	require.Error(t, err)
	compareTS = getOracleTS(t, se)
	require.NoError(t, provider.OnStmtRetry(context.TODO()))
	ts, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTS)
	rcCheckTS = ts

	// if retry succeed next statement will still use rc check
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	ts, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Equal(t, rcCheckTS, ts)

	// other error also invalidate rc check but not retry
	nextAction, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterQuery, errors.New("err"))
	require.NoError(t, err)
	require.Equal(t, sessiontxn.StmtActionNoIdea, nextAction)
	compareTS = getOracleTS(t, se)
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	ts, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTS)
	rcCheckTS = ts

	// `StmtErrAfterPessimisticLock` will still disable rc check
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	ts, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Equal(t, rcCheckTS, ts)
	nextAction, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, kv.ErrWriteConflict)
	require.NoError(t, err)
	require.Equal(t, sessiontxn.StmtActionRetryReady, nextAction)
	compareTS = getOracleTS(t, se)
	require.NoError(t, provider.OnStmtRetry(context.TODO()))
	ts, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTS)
	rcCheckTS = ts
	compareTS = getOracleTS(t, se)

	// only read-only stmt can retry for rc check
	require.NoError(t, executor.ResetContextOfStmt(se, forUpdateStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	ts, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTS)
	nextAction, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterQuery, kv.ErrWriteConflict)
	require.NoError(t, err)
	require.Equal(t, sessiontxn.StmtActionNoIdea, nextAction)
}

func TestPessimisticRCTxnContextProviderLockError(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	se := tk.Session()
	provider := initializePessimisticRCProvider(t, tk)

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
		require.NoError(t, err)
		require.Equal(t, sessiontxn.StmtActionRetryReady, nextAction)
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

func TestPessimisticRCTxnContextProviderTS(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	se := tk.Session()
	provider := initializePessimisticRCProvider(t, tk)
	compareTS := getOracleTS(t, se)

	stmts, _, err := parser.New().Parse("select * from t for update", "", "")
	require.NoError(t, err)
	stmt := stmts[0]

	// first read
	require.NoError(t, executor.ResetContextOfStmt(se, stmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	readTS, err := provider.GetStmtReadTS()
	require.NoError(t, err)
	forUpdateTS, err := provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, readTS, forUpdateTS)
	require.Greater(t, readTS, compareTS)

	// second read should use the newest ts
	compareTS = getOracleTS(t, se)
	require.Greater(t, compareTS, readTS)
	require.NoError(t, executor.ResetContextOfStmt(se, stmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	readTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	forUpdateTS, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, readTS, forUpdateTS)
	require.Greater(t, readTS, compareTS)

	// if we should retry, the ts should be updated
	nextAction, err := provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, kv.ErrWriteConflict)
	require.NoError(t, err)
	require.Equal(t, sessiontxn.StmtActionRetryReady, nextAction)
	require.NoError(t, provider.OnStmtRetry(context.TODO()))
	compareTS = getOracleTS(t, se)
	require.Greater(t, compareTS, readTS)
	readTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	forUpdateTS, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, readTS, forUpdateTS)
	require.Greater(t, readTS, compareTS)
}

func initializePessimisticRCProvider(t *testing.T, tk *testkit.TestKit) *isolation.PessimisticRCTxnContextProvider {
	tk.MustExec("set @@tx_isolation = 'READ-COMMITTED'")
	tk.MustExec("begin pessimistic")
	provider := sessiontxn.GetTxnManager(tk.Session()).GetContextProvider()
	require.IsType(t, &isolation.PessimisticRCTxnContextProvider{}, provider)
	return provider.(*isolation.PessimisticRCTxnContextProvider)
}

func getOracleTS(t *testing.T, sctx sessionctx.Context) uint64 {
	ts, err := sctx.GetStore().GetOracle().GetTimestamp(context.TODO(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoError(t, err)
	return ts
}
