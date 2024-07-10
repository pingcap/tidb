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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/isolation"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
)

func TestOptimisticTxnContextProviderTS(t *testing.T) {
	store := testkit.CreateMockStore(t)

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")

	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, v int)")

	se := tk.Session()
	compareTS := getOracleTS(t, se)
	provider := initializeOptimisticProvider(t, tk, true)
	require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
	readTS, err := provider.GetStmtReadTS()
	require.NoError(t, err)
	updateTS, err := provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, readTS, updateTS)
	require.Greater(t, readTS, compareTS)
	compareTS = readTS

	// for optimistic mode ts, ts should be the same for all statements
	require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
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
	provider = initializeOptimisticProvider(t, tk, false)
	require.NoError(t, provider.OnStmtStart(context.TODO(), stmt))
	plan, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, provider.GetTxnInfoSchema())
	require.NoError(t, err)
	require.NoError(t, provider.AdviseOptimizeWithPlan(plan))
	readTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	updateTS, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64), readTS)
	require.Equal(t, uint64(math.MaxUint64), updateTS)

	// if the oracle future is prepared fist, `math.MaxUint64` should still be used after plan
	provider = initializeOptimisticProvider(t, tk, false)
	require.NoError(t, provider.OnStmtStart(context.TODO(), stmt))
	require.NoError(t, provider.AdviseWarmup())
	plan, _, err = planner.Optimize(context.TODO(), tk.Session(), stmt, provider.GetTxnInfoSchema())
	require.NoError(t, err)
	require.NoError(t, provider.AdviseOptimizeWithPlan(plan))
	readTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	updateTS, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64), readTS)
	require.Equal(t, uint64(math.MaxUint64), updateTS)

	// when it is in explicit txn, we should not use `math.MaxUint64`
	compareTS = getOracleTS(t, se)
	provider = initializeOptimisticProvider(t, tk, true)
	require.NoError(t, provider.OnStmtStart(context.TODO(), stmt))
	plan, _, err = planner.Optimize(context.TODO(), tk.Session(), stmt, provider.GetTxnInfoSchema())
	require.NoError(t, err)
	require.NoError(t, provider.AdviseOptimizeWithPlan(plan))
	readTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	updateTS, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, readTS, updateTS)
	require.Greater(t, readTS, compareTS)

	// when it autocommit=0, we should not use `math.MaxUint64`
	tk.MustExec("set @@autocommit=0")
	compareTS = getOracleTS(t, se)
	provider = initializeOptimisticProvider(t, tk, false)
	require.NoError(t, provider.OnStmtStart(context.TODO(), stmt))
	plan, _, err = planner.Optimize(context.TODO(), tk.Session(), stmt, provider.GetTxnInfoSchema())
	require.NoError(t, err)
	require.NoError(t, provider.AdviseOptimizeWithPlan(plan))
	readTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	updateTS, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, readTS, updateTS)
	require.Greater(t, readTS, compareTS)
}

func TestOptimisticHandleError(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")

	provider := initializeOptimisticProvider(t, tk, true)
	startTS := tk.Session().GetSessionVars().TxnCtx.StartTS
	checkTS := func() {
		ts, err := provider.GetStmtReadTS()
		require.NoError(t, err)
		require.Equal(t, startTS, ts)

		ts, err = provider.GetStmtForUpdateTS()
		require.NoError(t, err)
		require.Equal(t, startTS, ts)
	}

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
		require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
		action, err := provider.OnStmtErrorForNextAction(context.Background(), c.point, c.err)
		if c.point == sessiontxn.StmtErrAfterPessimisticLock {
			require.Error(t, err)
			require.Same(t, c.err, err)
			require.Equal(t, sessiontxn.StmtActionError, action)

			// next statement should not update ts
			require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
			checkTS()
		} else {
			require.NoError(t, err)
			require.Equal(t, sessiontxn.StmtActionNoIdea, action)

			// retry should not update ts
			require.NoError(t, provider.OnStmtRetry(context.TODO()))
			checkTS()

			// OnStmtErrorForNextAction again
			require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
			action, err = provider.OnStmtErrorForNextAction(context.Background(), c.point, c.err)
			require.NoError(t, err)
			require.Equal(t, sessiontxn.StmtActionNoIdea, action)

			// next statement should not update ts
			require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
			checkTS()
		}
	}
}

func TestTidbSnapshotVarInOptimisticTxn(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")

	se := tk.Session()
	tk.MustExec("set @@tx_isolation = 'READ-COMMITTED'")
	safePoint := "20160102-15:04:05 -0700"
	tk.MustExec(fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%s', '') ON DUPLICATE KEY UPDATE variable_value = '%s', comment=''`, safePoint, safePoint))

	time.Sleep(time.Millisecond * 50)
	tk.MustExec("set @a=now(6)")
	snapshotISVersion := dom.InfoSchema().SchemaMetaVersion()
	time.Sleep(time.Millisecond * 50)
	tk.MustExec("use test")
	tk.MustExec("create table t1(id int)")
	tk.MustExec("create temporary table t2(id int)")
	tk.MustExec("set @@tidb_snapshot=@a")
	snapshotTS := tk.Session().GetSessionVars().SnapshotTS
	isVersion := dom.InfoSchema().SchemaMetaVersion()

	assert := activeOptimisticTxnAssert(t, se, true)
	tk.MustExec("begin")
	provider := assert.CheckAndGetProvider(t)
	txn, err := se.Txn(false)
	require.NoError(t, err)
	require.Greater(t, txn.StartTS(), snapshotTS)

	checkUseSnapshot := func() {
		is := provider.GetTxnInfoSchema()
		require.Equal(t, snapshotISVersion, is.SchemaMetaVersion())
		require.IsType(t, &infoschema.SessionExtendedInfoSchema{}, is)
		readTS, err := provider.GetStmtReadTS()
		require.NoError(t, err)
		require.Equal(t, snapshotTS, readTS)
		forUpdateTS, err := provider.GetStmtForUpdateTS()
		require.NoError(t, err)
		require.Equal(t, readTS, forUpdateTS)
	}

	checkUseTxn := func() {
		is := provider.GetTxnInfoSchema()
		require.Equal(t, isVersion, is.SchemaMetaVersion())
		require.IsType(t, &infoschema.SessionExtendedInfoSchema{}, is)
		readTS, err := provider.GetStmtReadTS()
		require.NoError(t, err)
		require.NotEqual(t, snapshotTS, readTS)
		require.Equal(t, se.GetSessionVars().TxnCtx.StartTS, readTS)
		forUpdateTS, err := provider.GetStmtForUpdateTS()
		require.NoError(t, err)
		require.Equal(t, readTS, forUpdateTS)
	}

	// information schema and ts should equal to snapshot when tidb_snapshot is set
	require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
	checkUseSnapshot()

	// information schema and ts will restore when set tidb_snapshot to empty
	tk.MustExec("set @@tidb_snapshot=''")
	require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
	checkUseTxn()

	// txn will not be active after `GetStmtReadTS` or `GetStmtForUpdateTS` when `tidb_snapshot` is set
	for _, autocommit := range []int{0, 1} {
		func() {
			tk.MustExec("rollback")
			tk.MustExec(fmt.Sprintf("set @@autocommit=%d", autocommit))
			tk.MustExec("set @@tidb_snapshot=@a")
			if autocommit == 1 {
				origPessimisticAutoCommit := config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load()
				config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Store(true)
				defer func() {
					config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Store(origPessimisticAutoCommit)
				}()
			}
			assert = inactiveOptimisticTxnAssert(se)
			assertAfterUseSnapshot := activeSnapshotTxnAssert(se, se.GetSessionVars().SnapshotTS, "")
			require.NoError(t, se.PrepareTxnCtx(context.TODO()))
			provider = assert.CheckAndGetProvider(t)
			require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
			checkUseSnapshot()
			assertAfterUseSnapshot.Check(t)
		}()
	}
}

func activeOptimisticTxnAssert(t testing.TB, sctx sessionctx.Context, inTxn bool) *txnAssert[*isolation.OptimisticTxnContextProvider] {
	return &txnAssert[*isolation.OptimisticTxnContextProvider]{
		sctx:         sctx,
		minStartTime: time.Now(),
		active:       true,
		inTxn:        inTxn,
		minStartTS:   getOracleTS(t, sctx),
	}
}

func inactiveOptimisticTxnAssert(sctx sessionctx.Context) *txnAssert[*isolation.OptimisticTxnContextProvider] {
	return &txnAssert[*isolation.OptimisticTxnContextProvider]{
		sctx:         sctx,
		minStartTime: time.Now(),
		active:       false,
	}
}

func initializeOptimisticProvider(t testing.TB, tk *testkit.TestKit, withExplicitBegin bool) *isolation.OptimisticTxnContextProvider {
	tk.MustExec("commit")
	if withExplicitBegin {
		assert := activeOptimisticTxnAssert(t, tk.Session(), true)
		tk.MustExec("begin optimistic")
		return assert.CheckAndGetProvider(t)
	}

	assert := inactiveOptimisticTxnAssert(tk.Session())
	err := sessiontxn.GetTxnManager(tk.Session()).EnterNewTxn(context.TODO(), &sessiontxn.EnterNewTxnRequest{
		Type:    sessiontxn.EnterNewTxnBeforeStmt,
		TxnMode: ast.Optimistic,
	})
	require.NoError(t, err)
	return assert.CheckAndGetProvider(t)
}
