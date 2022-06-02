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
	"errors"
	"math"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/isolation"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
)

func TestOptimisticRCTxnContextProviderTS(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, v int)")

	se := tk.Session()
	compareTS := getOracleTS(t, se)
	provider := initializeOptimisticRCProvider(t, tk, true)
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	readTS, err := provider.GetStmtReadTS()
	require.NoError(t, err)
	updateTS, err := provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, readTS, updateTS)
	require.Greater(t, readTS, compareTS)
	compareTS = readTS

	// for optimistic mode ts, ts should be the same for all statements
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	readTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	updateTS, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, compareTS, readTS)
	require.Equal(t, compareTS, updateTS)

	// when the plan is point get, `math.MaxUint64` should be used
	stmts, _, err := parser.New().Parse("select * from t where id=1", "", "")
	require.NoError(t, err)
	stmt := stmts[0]
	provider = initializeOptimisticRCProvider(t, tk, false)
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	_, _, err = planner.Optimize(context.TODO(), tk.Session(), stmt, provider.GetTxnInfoSchema())
	require.NoError(t, err)
	readTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	updateTS, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64), readTS)
	require.Equal(t, uint64(math.MaxUint64), updateTS)

	// if the oracle future is prepared fist, `math.MaxUint64` should still be used after plan
	provider = initializeOptimisticRCProvider(t, tk, false)
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	require.NoError(t, provider.Advise(sessiontxn.AdviceWarmUp, nil))
	_, _, err = planner.Optimize(context.TODO(), tk.Session(), stmt, provider.GetTxnInfoSchema())
	require.NoError(t, err)
	readTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	updateTS, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64), readTS)
	require.Equal(t, uint64(math.MaxUint64), updateTS)

	// when it is in explicit txn, we should not use `math.MaxUint64`
	compareTS = getOracleTS(t, se)
	provider = initializeOptimisticRCProvider(t, tk, true)
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	_, _, err = planner.Optimize(context.TODO(), tk.Session(), stmt, provider.GetTxnInfoSchema())
	require.NoError(t, err)
	readTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	updateTS, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, readTS, updateTS)
	require.Greater(t, readTS, compareTS)

	// when it autocommit=0, we should not use `math.MaxUint64`
	tk.MustExec("set @@autocommit=0")
	compareTS = getOracleTS(t, se)
	provider = initializeOptimisticRCProvider(t, tk, false)
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	_, _, err = planner.Optimize(context.TODO(), tk.Session(), stmt, provider.GetTxnInfoSchema())
	require.NoError(t, err)
	readTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	updateTS, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, readTS, updateTS)
	require.Greater(t, readTS, compareTS)
}

func TestOptimisticRCHandleError(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	provider := initializeOptimisticRCProvider(t, tk, true)

	cases := []struct {
		point sessiontxn.StmtErrorHandlePoint
		err   error
	}{
		{
			point: sessiontxn.StmtErrAfterPessimisticLock,
			err:   kv.ErrWriteConflict,
		},
		{
			point: sessiontxn.StmtErrAfterPessimisticLock,
			err:   &tikverr.ErrDeadlock{Deadlock: &kvrpcpb.Deadlock{}, IsRetryable: true},
		},
		{
			point: sessiontxn.StmtErrAfterPessimisticLock,
			err:   &tikverr.ErrDeadlock{Deadlock: &kvrpcpb.Deadlock{}, IsRetryable: false},
		},
		{
			point: sessiontxn.StmtErrAfterPessimisticLock,
			err:   errors.New("test"),
		},
		{
			point: sessiontxn.StmtErrAfterQuery,
			err:   kv.ErrWriteConflict,
		},
		{
			point: sessiontxn.StmtErrAfterQuery,
			err:   errors.New("test"),
		},
	}

	for _, c := range cases {
		action, err := provider.OnStmtErrorForNextAction(c.point, c.err)
		if c.point == sessiontxn.StmtErrAfterPessimisticLock {
			require.Error(t, err)
			require.Same(t, c.err, err)
			require.Equal(t, sessiontxn.StmtActionError, action)
		} else {
			require.NoError(t, err)
			require.Equal(t, sessiontxn.StmtActionNoIdea, action)
		}
	}
}

func initializeOptimisticRCProvider(t *testing.T, tk *testkit.TestKit, withExplicitBegin bool) *isolation.OptimisticTxnContextProvider {
	tk.MustExec("commit")
	if withExplicitBegin {
		tk.MustExec("begin optimistic")
	} else {
		err := sessiontxn.GetTxnManager(tk.Session()).EnterNewTxn(context.TODO(), &sessiontxn.EnterNewTxnRequest{
			Type:    sessiontxn.EnterNewTxnBeforeStmt,
			TxnMode: ast.Optimistic,
		})
		require.NoError(t, err)
	}
	provider := sessiontxn.GetTxnManager(tk.Session()).GetContextProvider()
	require.IsType(t, &isolation.OptimisticTxnContextProvider{}, provider)
	return provider.(*isolation.OptimisticTxnContextProvider)
}
