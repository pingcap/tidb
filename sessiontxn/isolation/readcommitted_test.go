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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/isolation"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testfork"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
)

func TestPessimisticRCTxnContextProviderRCCheck(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")

	tk.MustExec("set @@tidb_rc_read_check_ts=1")
	se := tk.Session()
	provider := initializePessimisticRCProvider(t, tk)

	stmts, _, err := parser.New().Parse("select * from t", "", "")
	require.NoError(t, err)
	readOnlyStmt := stmts[0]

	stmts, _, err = parser.New().Parse("select * from t for update", "", "")
	require.NoError(t, err)
	forUpdateStmt := stmts[0]

	compareTS := se.GetSessionVars().TxnCtx.StartTS
	// first ts should use the txn startTS
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO(), readOnlyStmt))
	ts, err := provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Equal(t, ts, compareTS)
	rcCheckTS := ts

	// second ts should reuse the txn startTS
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO(), readOnlyStmt))
	ts, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Equal(t, rcCheckTS, ts)

	// when one statement did not getStmtReadTS, the next one should still reuse the first ts
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO(), readOnlyStmt))
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO(), readOnlyStmt))
	ts, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Equal(t, rcCheckTS, ts)

	// error will invalidate the rc check
	nextAction, err := provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterQuery, kv.ErrWriteConflict)
	require.NoError(t, err)
	require.Equal(t, sessiontxn.StmtActionRetryReady, nextAction)
	compareTS = getOracleTS(t, se)
	require.Greater(t, compareTS, rcCheckTS)
	require.NoError(t, provider.OnStmtRetry(context.TODO()))
	ts, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTS)
	rcCheckTS = ts

	// if retry succeed next statement will still use rc check
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO(), readOnlyStmt))
	ts, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Equal(t, rcCheckTS, ts)

	// other error also invalidate rc check but not retry
	nextAction, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterQuery, errors.New("err"))
	require.NoError(t, err)
	require.Equal(t, sessiontxn.StmtActionNoIdea, nextAction)
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO(), readOnlyStmt))
	ts, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Equal(t, rcCheckTS, ts)

	// `StmtErrAfterPessimisticLock` will still disable rc check
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO(), readOnlyStmt))
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
	require.Greater(t, compareTS, rcCheckTS)

	// only read-only stmt can retry for rc check
	require.NoError(t, executor.ResetContextOfStmt(se, forUpdateStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO(), forUpdateStmt))
	ts, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTS)
	nextAction, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterQuery, kv.ErrWriteConflict)
	require.NoError(t, err)
	require.Equal(t, sessiontxn.StmtActionNoIdea, nextAction)
}

func TestPessimisticRCTxnContextProviderRCCheckForPrepareExecute(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")

	tk2 := testkit.NewTestKit(t, store)
	defer tk2.MustExec("rollback")

	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("create table t (id int primary key, v int)")
	tk2.MustExec("insert into t values(1, 1)")

	tk.MustExec("set @@tidb_rc_read_check_ts=1")
	se := tk.Session()
	ctx := context.Background()
	provider := initializePessimisticRCProvider(t, tk)
	txnStartTS := se.GetSessionVars().TxnCtx.StartTS

	// first ts should use the txn startTS
	stmt, _, _, err := tk.Session().PrepareStmt("select * from t")
	require.NoError(t, err)
	rs, err := tk.Session().ExecutePreparedStmt(ctx, stmt, expression.Args2Expressions4Test())
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1"))
	require.NoError(t, err)
	ts, err := provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, txnStartTS, ts)

	// second ts should reuse the txn startTS
	rs, err = tk.Session().ExecutePreparedStmt(ctx, stmt, expression.Args2Expressions4Test())
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 1"))
	require.NoError(t, err)
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, txnStartTS, ts)

	tk2.MustExec("update t set v = v + 10 where id = 1")
	compareTS := getOracleTS(t, se)
	rs, err = tk.Session().ExecutePreparedStmt(ctx, stmt, expression.Args2Expressions4Test())
	require.NoError(t, err)
	_, err = session.ResultSetToStringSlice(ctx, tk.Session(), rs)
	require.Error(t, err)
	rs.Close()

	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, compareTS, ts)
	// retry
	tk.Session().GetSessionVars().RetryInfo.Retrying = true
	rs, err = tk.Session().ExecutePreparedStmt(ctx, stmt, expression.Args2Expressions4Test())
	require.NoError(t, err)
	tk.ResultSetToResult(rs, fmt.Sprintf("%v", rs)).Check(testkit.Rows("1 11"))
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTS)
}

func TestPessimisticRCTxnContextProviderLockError(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")

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
		require.NoError(t, provider.OnStmtStart(context.TODO(), stmt))
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
		require.NoError(t, provider.OnStmtStart(context.TODO(), stmt))
		nextAction, err := provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
		require.Same(t, lockErr, err)
		require.Equal(t, sessiontxn.StmtActionError, nextAction)
	}
}

func TestPessimisticRCTxnContextProviderTS(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")

	se := tk.Session()
	provider := initializePessimisticRCProvider(t, tk)
	compareTS := getOracleTS(t, se)

	stmts, _, err := parser.New().Parse("select * from t for update", "", "")
	require.NoError(t, err)
	stmt := stmts[0]

	// first read
	require.NoError(t, executor.ResetContextOfStmt(se, stmt))
	require.NoError(t, provider.OnStmtStart(context.TODO(), stmt))
	readTS, err := provider.GetStmtReadTS()
	require.NoError(t, err)
	forUpdateTS, err := provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, readTS, forUpdateTS)
	require.Equal(t, forUpdateTS, se.GetSessionVars().TxnCtx.GetForUpdateTS())
	require.Greater(t, readTS, compareTS)

	// second read should use the newest ts
	compareTS = getOracleTS(t, se)
	require.Greater(t, compareTS, readTS)
	require.NoError(t, executor.ResetContextOfStmt(se, stmt))
	require.NoError(t, provider.OnStmtStart(context.TODO(), stmt))
	readTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	forUpdateTS, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, readTS, forUpdateTS)
	require.Equal(t, forUpdateTS, se.GetSessionVars().TxnCtx.GetForUpdateTS())
	require.Greater(t, readTS, compareTS)

	// if we should retry, the ts should be updated
	nextAction, err := provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, kv.ErrWriteConflict)
	require.NoError(t, err)
	require.Equal(t, sessiontxn.StmtActionRetryReady, nextAction)
	compareTS = getOracleTS(t, se)
	require.Greater(t, compareTS, readTS)
	require.NoError(t, provider.OnStmtRetry(context.TODO()))
	readTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	forUpdateTS, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, readTS, forUpdateTS)
	require.Equal(t, forUpdateTS, se.GetSessionVars().TxnCtx.GetForUpdateTS())
	require.Greater(t, readTS, compareTS)
}

func TestRCProviderInitialize(t *testing.T) {
	store := testkit.CreateMockStore(t)

	testfork.RunTest(t, func(t *testfork.T) {
		clearScopeSettings := forkScopeSettings(t, store)
		defer clearScopeSettings()

		tk := testkit.NewTestKit(t, store)
		defer tk.MustExec("rollback")

		se := tk.Session()
		tk.MustExec("set @@tx_isolation = 'READ-COMMITTED'")
		tk.MustExec("set @@tidb_txn_mode='pessimistic'")

		// begin outside a txn
		assert := activeRCTxnAssert(t, se, true)
		tk.MustExec("begin")
		assert.Check(t)

		// begin in a txn
		assert = activeRCTxnAssert(t, se, true)
		tk.MustExec("begin")
		assert.Check(t)

		// START TRANSACTION WITH CAUSAL CONSISTENCY ONLY
		assert = activeRCTxnAssert(t, se, true)
		assert.causalConsistencyOnly = true
		tk.MustExec("START TRANSACTION WITH CAUSAL CONSISTENCY ONLY")
		assert.Check(t)

		// EnterNewTxnDefault will create an active txn, but not explicit
		assert = activeRCTxnAssert(t, se, false)
		require.NoError(t, sessiontxn.GetTxnManager(se).EnterNewTxn(context.TODO(), &sessiontxn.EnterNewTxnRequest{
			Type:    sessiontxn.EnterNewTxnDefault,
			TxnMode: ast.Pessimistic,
		}))
		assert.Check(t)

		// non-active txn and then active it
		tk.MustExec("rollback")
		tk.MustExec("set @@autocommit=0")
		assert = inactiveRCTxnAssert(se)
		assertAfterActive := activeRCTxnAssert(t, se, true)
		require.NoError(t, se.PrepareTxnCtx(context.TODO()))
		provider := assert.CheckAndGetProvider(t)
		require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
		ts, err := provider.GetStmtReadTS()
		require.NoError(t, err)
		assertAfterActive.Check(t)
		require.Equal(t, ts, se.GetSessionVars().TxnCtx.StartTS)
		tk.MustExec("rollback")

		// Case Pessimistic Autocommit
		config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Store(true)
		assert = inactiveRCTxnAssert(se)
		assertAfterActive = activeRCTxnAssert(t, se, true)
		require.NoError(t, se.PrepareTxnCtx(context.TODO()))
		provider = assert.CheckAndGetProvider(t)
		require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
		ts, err = provider.GetStmtReadTS()
		require.NoError(t, err)
		assertAfterActive.Check(t)
		require.Equal(t, ts, se.GetSessionVars().TxnCtx.StartTS)
		tk.MustExec("rollback")
	})
}

func TestTidbSnapshotVarInRC(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

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

	assert := activeRCTxnAssert(t, se, true)
	tk.MustExec("begin pessimistic")
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

	checkUseTxn := func(useTxnTs bool) {
		is := provider.GetTxnInfoSchema()
		require.Equal(t, isVersion, is.SchemaMetaVersion())
		require.IsType(t, &infoschema.SessionExtendedInfoSchema{}, is)
		readTS, err := provider.GetStmtReadTS()
		require.NoError(t, err)
		require.NotEqual(t, snapshotTS, readTS)
		if useTxnTs {
			require.Equal(t, se.GetSessionVars().TxnCtx.StartTS, readTS)
		} else {
			require.Greater(t, readTS, se.GetSessionVars().TxnCtx.StartTS)
		}
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
	checkUseTxn(false)

	// txn will not be active after `GetStmtReadTS` or `GetStmtForUpdateTS` when `tidb_snapshot` is set
	for _, autocommit := range []int{0, 1} {
		func() {
			tk.MustExec("rollback")
			tk.MustExec("set @@tidb_txn_mode='pessimistic'")
			tk.MustExec(fmt.Sprintf("set @@autocommit=%d", autocommit))
			tk.MustExec("set @@tidb_snapshot=@a")
			if autocommit == 1 {
				origPessimisticAutoCommit := config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Load()
				config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Store(true)
				defer func() {
					config.GetGlobalConfig().PessimisticTxn.PessimisticAutoCommit.Store(origPessimisticAutoCommit)
				}()
			}
			assert = inactiveRCTxnAssert(se)
			assertAfterUseSnapshot := activeSnapshotTxnAssert(se, se.GetSessionVars().SnapshotTS, "READ-COMMITTED")
			require.NoError(t, se.PrepareTxnCtx(context.TODO()))
			provider = assert.CheckAndGetProvider(t)
			require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
			checkUseSnapshot()
			assertAfterUseSnapshot.Check(t)
		}()
	}
}

func TestConflictErrorsInRC(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/assertPessimisticLockErr", "return"))
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")

	se := tk.Session()

	tk2 := testkit.NewTestKit(t, store)
	defer tk2.MustExec("rollback")

	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("create table t (id int primary key, v int)")

	tk.MustExec("set tx_isolation='READ-COMMITTED'")

	// Test for insert
	tk.MustExec("begin pessimistic")
	tk2.MustExec("insert into t values (1, 2)")
	se.SetValue(sessiontxn.AssertLockErr, nil)
	_, err := tk.Exec("insert into t values (1, 1), (2, 2)")
	require.Error(t, err)
	records, ok := se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.True(t, ok)
	for _, name := range errorsInInsert {
		require.Equal(t, records[name], 1)
	}

	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("rollback")

	// Test for delete
	tk.MustExec("truncate t")
	tk.MustExec("insert into t values (1, 1), (2, 2)")

	tk.MustExec("begin pessimistic")
	tk2.MustExec("insert into t values (3, 1)")

	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("delete from t where v = 1")
	_, ok = se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.False(t, ok)
	tk.MustQuery("select * from t").Check(testkit.Rows("2 2"))
	tk.MustExec("commit")

	// Unlike RR, in RC, we will always fetch the latest ts. So write conflict will not be happened
	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t set id = 1 where id = 2")
	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("delete from t where id = 1")
	_, ok = se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.False(t, ok)
	tk.MustQuery("select * from t for update").Check(testkit.Rows())

	tk.MustExec("rollback")

	// Test for update
	tk.MustExec("truncate t")
	tk.MustExec("insert into t values (1, 1), (2, 2)")

	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t set v = v + 10")

	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("update t set v = v + 10")
	_, ok = se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.False(t, ok)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 21", "2 22"))
	tk.MustExec("commit")

	// Unlike RR, in RC, we will always fetch the latest ts. So write conflict will not be happened
	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t set v = v + 10 where id = 1")
	tk.MustExec("update t set v = v + 10 where id = 1")
	_, ok = se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.False(t, ok)
	tk.MustQuery("select * from t for update").Check(testkit.Rows("1 41", "2 22"))

	tk.MustExec("rollback")

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertPessimisticLockErr"))
}

func activeRCTxnAssert(t testing.TB, sctx sessionctx.Context, inTxn bool) *txnAssert[*isolation.PessimisticRCTxnContextProvider] {
	return &txnAssert[*isolation.PessimisticRCTxnContextProvider]{
		sctx:         sctx,
		isolation:    "READ-COMMITTED",
		minStartTime: time.Now(),
		active:       true,
		inTxn:        inTxn,
		minStartTS:   getOracleTS(t, sctx),
	}
}

func inactiveRCTxnAssert(sctx sessionctx.Context) *txnAssert[*isolation.PessimisticRCTxnContextProvider] {
	return &txnAssert[*isolation.PessimisticRCTxnContextProvider]{
		sctx:         sctx,
		isolation:    "READ-COMMITTED",
		minStartTime: time.Now(),
		active:       false,
	}
}

func initializePessimisticRCProvider(t testing.TB, tk *testkit.TestKit) *isolation.PessimisticRCTxnContextProvider {
	tk.MustExec("set @@tx_isolation = 'READ-COMMITTED'")
	assert := activeRCTxnAssert(t, tk.Session(), true)
	tk.MustExec("begin pessimistic")
	return assert.CheckAndGetProvider(t)
}
