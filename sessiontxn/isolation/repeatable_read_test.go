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
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/isolation"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testfork"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
)

func newDeadLockError(isRetryable bool) error {
	return &tikverr.ErrDeadlock{
		Deadlock:    &kvrpcpb.Deadlock{},
		IsRetryable: isRetryable,
	}
}

func TestPessimisticRRErrorHandle(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")
	se := tk.Session()
	provider := initializeRepeatableReadProvider(t, tk, true)

	var lockErr error

	compareTS := getOracleTS(t, se)
	lockErr = kv.ErrWriteConflict
	nextAction, err := provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	require.NoError(t, err)
	require.Equal(t, sessiontxn.StmtActionRetryReady, nextAction)
	err = provider.OnStmtRetry(context.TODO())
	// In OnStmtErrorForNextAction, we set the txnCtx.forUpdateTS to be the latest ts, which is used to
	// update the provider's in OnStmtRetry. So, if we acquire new ts now, it will be less than the current ts.
	compareTS2 := getOracleTS(t, se)
	require.NoError(t, err)
	ts, err := provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTS)
	require.Greater(t, compareTS2, ts)

	// Update compareTS for the next comparison
	compareTS = getOracleTS(t, se)
	lockErr = kv.ErrWriteConflict
	nextAction, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	require.NoError(t, err)
	require.Equal(t, sessiontxn.StmtActionRetryReady, nextAction)
	err = provider.OnStmtStart(context.TODO(), nil)
	// Unlike StmtRetry which uses forUpdateTS got in OnStmtErrorForNextAction, OnStmtStart will reset provider's forUpdateTS,
	// which leads GetStmtForUpdateTS to acquire the latest ts.
	compareTS2 = getOracleTS(t, se)
	require.NoError(t, err)
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTS)
	require.Greater(t, ts, compareTS2)

	lockErr = newDeadLockError(false)
	nextAction, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	require.Equal(t, lockErr, err)
	require.Equal(t, sessiontxn.StmtActionError, nextAction)

	// Update compareTS for the next comparison
	compareTS = getOracleTS(t, se)
	lockErr = newDeadLockError(true)
	nextAction, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	require.NoError(t, err)
	require.Equal(t, sessiontxn.StmtActionRetryReady, nextAction)
	err = provider.OnStmtRetry(context.TODO())
	require.NoError(t, err)
	// In OnStmtErrorForNextAction, we set the txnCtx.forUpdateTS to be the latest ts, which is used to
	// update the provider's in OnStmtRetry. So, if we acquire new ts now, it will be less than the current ts.
	compareTS2 = getOracleTS(t, se)
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTS)
	require.Greater(t, compareTS2, ts)

	// Update compareTS for the next comparison
	compareTS = getOracleTS(t, se)
	lockErr = newDeadLockError(true)
	nextAction, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	require.NoError(t, err)
	require.Equal(t, sessiontxn.StmtActionRetryReady, nextAction)
	err = provider.OnStmtStart(context.TODO(), nil)
	require.NoError(t, err)
	// Unlike StmtRetry which uses forUpdateTS got in OnStmtErrorForNextAction, OnStmtStart will reset provider's forUpdateTS,
	// which leads GetStmtForUpdateTS to acquire the latest ts.
	compareTS2 = getOracleTS(t, se)
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTS)
	require.Greater(t, ts, compareTS2)

	// StmtErrAfterLock: other errors should only update forUpdateTS but not retry
	lockErr = errors.New("other error")
	nextAction, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	require.Equal(t, lockErr, err)
	require.Equal(t, sessiontxn.StmtActionError, nextAction)

	// StmtErrAfterQuery: always not retry and not update forUpdateTS
	lockErr = kv.ErrWriteConflict
	nextAction, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterQuery, lockErr)
	require.Equal(t, sessiontxn.StmtActionNoIdea, nextAction)
	require.Nil(t, err)
}

func TestRepeatableReadProviderTS(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")
	se := tk.Session()
	provider := initializeRepeatableReadProvider(t, tk, true)

	stmts, _, err := parser.New().Parse("select * from t", "", "")
	require.NoError(t, err)
	readOnlyStmt := stmts[0]

	stmts, _, err = parser.New().Parse("select * from t for update", "", "")
	require.NoError(t, err)
	forUpdateStmt := stmts[0]

	var prevTS, CurrentTS uint64
	compareTS := getOracleTS(t, se)
	// The read ts should be less than the compareTS
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
	CurrentTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Greater(t, compareTS, CurrentTS)
	prevTS = CurrentTS

	// The read ts should also be less than the compareTS in a new statement (after calling OnStmtStart)
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
	CurrentTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Equal(t, CurrentTS, prevTS)

	// The read ts should not be changed after calling OnStmtRetry
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtRetry(context.TODO()))
	CurrentTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Equal(t, CurrentTS, prevTS)

	// The for update read ts should be larger than the compareTS
	require.NoError(t, executor.ResetContextOfStmt(se, forUpdateStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
	forUpdateTS, err := provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, forUpdateTS, compareTS)

	// But the read ts is still less than the compareTS
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
	CurrentTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Equal(t, CurrentTS, prevTS)
}

func TestRepeatableReadProviderInitialize(t *testing.T) {
	store := testkit.CreateMockStore(t)

	testfork.RunTest(t, func(t *testfork.T) {
		clearScopeSettings := forkScopeSettings(t, store)
		defer clearScopeSettings()

		tk := testkit.NewTestKit(t, store)
		defer tk.MustExec("rollback")
		se := tk.Session()
		tk.MustExec("set @@tx_isolation = 'REPEATABLE-READ'")
		tk.MustExec("set @@tidb_txn_mode='pessimistic'")

		// begin outside a txn
		assert := activePessimisticRRAssert(t, se, true)
		tk.MustExec("begin")
		assert.Check(t)

		// begin in a txn
		assert = activePessimisticRRAssert(t, se, true)
		tk.MustExec("begin")
		assert.Check(t)

		// START TRANSACTION WITH CAUSAL CONSISTENCY ONLY
		assert = activePessimisticRRAssert(t, se, true)
		assert.causalConsistencyOnly = true
		tk.MustExec("START TRANSACTION WITH CAUSAL CONSISTENCY ONLY")
		assert.Check(t)

		// EnterNewTxnDefault will create an active txn, but not explicit
		assert = activePessimisticRRAssert(t, se, false)
		require.NoError(t, sessiontxn.GetTxnManager(se).EnterNewTxn(context.TODO(), &sessiontxn.EnterNewTxnRequest{
			Type:    sessiontxn.EnterNewTxnDefault,
			TxnMode: ast.Pessimistic,
		}))
		assert.Check(t)

		// non-active txn and then active it
		tk.MustExec("rollback")
		tk.MustExec("set @@autocommit=0")
		assert = inactivePessimisticRRAssert(se)
		assertAfterActive := activePessimisticRRAssert(t, se, true)
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
		assert = inactivePessimisticRRAssert(se)
		assertAfterActive = activePessimisticRRAssert(t, se, true)
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

func TestTidbSnapshotVarInPessimisticRepeatableRead(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")
	se := tk.Session()
	tk.MustExec("set @@tx_isolation = 'REPEATABLE-READ'")
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

	assert := activePessimisticRRAssert(t, se, true)
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
		require.Greater(t, forUpdateTS, readTS)
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
			assert = inactivePessimisticRRAssert(se)
			assertAfterUseSnapshot := activeSnapshotTxnAssert(se, se.GetSessionVars().SnapshotTS, "REPEATABLE-READ")
			require.NoError(t, se.PrepareTxnCtx(context.TODO()))
			provider = assert.CheckAndGetProvider(t)
			require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
			checkUseSnapshot()
			assertAfterUseSnapshot.Check(t)
		}()
	}
}

func TestOptimizeWithPlanInPessimisticRR(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, v int)")
	tk.MustExec("insert into t values (1,1), (2,2)")
	se := tk.Session()
	provider := initializeRepeatableReadProvider(t, tk, true)
	lastFetchedForUpdateTS := se.GetSessionVars().TxnCtx.GetForUpdateTS()
	txnManager := sessiontxn.GetTxnManager(se)

	type testStruct struct {
		sql            string
		shouldOptimize bool
	}

	cases := []testStruct{
		{
			"delete from t where id = 1",
			true,
		},
		{
			"update t set v = v + 10 where id = 1",
			true,
		},
		{
			"select * from (select * from t where id = 1 for update) as t1 for update",
			true,
		},
		{
			"select * from t where id = 1 for update",
			true,
		},
		{
			"select * from t where id = 1 or id = 2 for update",
			true,
		},
		{
			"select * from t for update",
			false,
		},
	}

	var stmt ast.StmtNode
	var err error
	var execStmt *executor.ExecStmt
	var compiler executor.Compiler
	var ts, compareTS uint64
	var action sessiontxn.StmtErrorAction

	for _, c := range cases {
		compareTS = getOracleTS(t, se)

		require.NoError(t, txnManager.OnStmtStart(context.TODO(), nil))
		stmt, err = parser.New().ParseOneStmt(c.sql, "", "")
		require.NoError(t, err)

		err = provider.OnStmtStart(context.TODO(), nil)
		require.NoError(t, err)

		compiler = executor.Compiler{Ctx: se}
		execStmt, err = compiler.Compile(context.TODO(), stmt)
		require.NoError(t, err)

		err = txnManager.AdviseOptimizeWithPlan(execStmt.Plan)
		require.NoError(t, err)

		ts, err = provider.GetStmtForUpdateTS()
		require.NoError(t, err)

		if c.shouldOptimize {
			require.Greater(t, compareTS, ts)
			require.Equal(t, ts, lastFetchedForUpdateTS)
		} else {
			require.Greater(t, ts, compareTS)
		}

		// retry
		if c.shouldOptimize {
			action, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, kv.ErrWriteConflict)
			require.NoError(t, err)
			require.Equal(t, sessiontxn.StmtActionRetryReady, action)
			err = provider.OnStmtRetry(context.TODO())
			require.NoError(t, err)
			ts, err = provider.GetStmtForUpdateTS()
			require.NoError(t, err)
			require.Greater(t, ts, compareTS)

			lastFetchedForUpdateTS = ts
		}
	}

	// Test use startTS after optimize when autocommit=0
	activeAssert := activePessimisticRRAssert(t, tk.Session(), true)
	provider = initializeRepeatableReadProvider(t, tk, false)
	stmt, err = parser.New().ParseOneStmt("update t set v = v + 10 where id = 1", "", "")
	require.NoError(t, err)
	require.NoError(t, txnManager.OnStmtStart(context.TODO(), stmt))
	execStmt, err = compiler.Compile(context.TODO(), stmt)
	require.NoError(t, err)
	err = txnManager.AdviseOptimizeWithPlan(execStmt.Plan)
	require.NoError(t, err)
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Same(t, provider, activeAssert.CheckAndGetProvider(t))
	require.Equal(t, tk.Session().GetSessionVars().TxnCtx.StartTS, ts)

	// Test still fetch for update ts after optimize when autocommit=0
	compareTS = getOracleTS(t, se)
	activeAssert = activePessimisticRRAssert(t, tk.Session(), true)
	provider = initializeRepeatableReadProvider(t, tk, false)
	stmt, err = parser.New().ParseOneStmt("select * from t", "", "")
	require.NoError(t, err)
	require.NoError(t, txnManager.OnStmtStart(context.TODO(), stmt))
	execStmt, err = compiler.Compile(context.TODO(), stmt)
	require.NoError(t, err)
	err = txnManager.AdviseOptimizeWithPlan(execStmt.Plan)
	require.NoError(t, err)
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTS)
}

var errorsInInsert = []string{
	"errWriteConflict",
	"errDuplicateKey",
}

func TestConflictErrorInInsertInRR(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/assertPessimisticLockErr", "return"))
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")
	se := tk.Session()
	tk2 := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk2.MustExec("use test")
	tk.MustExec("create table t (id int primary key, v int)")

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
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertPessimisticLockErr"))
}

func TestConflictErrorInPointGetForUpdateInRR(t *testing.T) {
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
	tk.MustExec("insert into t values (1, 1), (2, 2)")

	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t set v = v + 10 where id = 1")
	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustQuery("select * from t where id = 1 for update").Check(testkit.Rows("1 11"))
	records, ok := se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.True(t, ok)
	require.Equal(t, records["errWriteConflict"], 1)
	tk.MustExec("commit")

	// batch point get
	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t set v = v + 10 where id = 1")
	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustQuery("select * from t where id = 1 or id = 2 for update").Check(testkit.Rows("1 21", "2 2"))
	records, ok = se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.True(t, ok)
	require.Equal(t, records["errWriteConflict"], 1)
	tk.MustExec("commit")

	tk.MustExec("rollback")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertPessimisticLockErr"))
}

// Delete should get the latest ts and thus does not incur write conflict
func TestConflictErrorInDeleteInRR(t *testing.T) {
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
	tk.MustExec("insert into t values (1, 1), (2, 2)")

	tk.MustExec("begin pessimistic")
	tk2.MustExec("insert into t values (3, 1)")
	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("delete from t where v = 1")
	_, ok := se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.False(t, ok)
	tk.MustQuery("select * from t").Check(testkit.Rows("2 2"))
	tk.MustExec("commit")

	tk.MustExec("begin pessimistic")
	// However, if sub select in delete is point get, we will incur one write conflict
	tk2.MustExec("update t set id = 1 where id = 2")
	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("delete from t where id = 1")

	records, ok := se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.True(t, ok)
	require.Equal(t, records["errWriteConflict"], 1)
	tk.MustQuery("select * from t for update").Check(testkit.Rows())

	tk.MustExec("rollback")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertPessimisticLockErr"))
}

func TestConflictErrorInUpdateInRR(t *testing.T) {
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
	tk.MustExec("insert into t values (1, 1), (2, 2)")

	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t set v = v + 10")
	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustExec("update t set v = v + 10")
	_, ok := se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.False(t, ok)
	tk.MustQuery("select * from t").Check(testkit.Rows("1 21", "2 22"))
	tk.MustExec("commit")

	tk.MustExec("begin pessimistic")
	// However, if the sub select plan is point get, we should incur one write conflict
	tk2.MustExec("update t set v = v + 10 where id = 1")
	tk.MustExec("update t set v = v + 10 where id = 1")
	records, ok := se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.True(t, ok)
	require.Equal(t, records["errWriteConflict"], 1)
	tk.MustQuery("select * from t for update").Check(testkit.Rows("1 41", "2 22"))

	tk.MustExec("rollback")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertPessimisticLockErr"))
}

func TestConflictErrorInOtherQueryContainingPointGet(t *testing.T) {
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
	tk.MustExec("insert into t values (1, 1)")

	tk.MustExec("begin pessimistic")
	tk2.MustExec("update t set v = v + 10 where id = 1")
	se.SetValue(sessiontxn.AssertLockErr, nil)
	tk.MustQuery("select * from t where id=1 and v > 1 for update").Check(testkit.Rows("1 11"))
	records, ok := se.Value(sessiontxn.AssertLockErr).(map[string]int)
	require.True(t, ok)
	require.Equal(t, records["errWriteConflict"], 1)

	tk.MustExec("rollback")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/assertPessimisticLockErr"))
}

func activePessimisticRRAssert(t testing.TB, sctx sessionctx.Context,
	inTxn bool) *txnAssert[*isolation.PessimisticRRTxnContextProvider] {
	return &txnAssert[*isolation.PessimisticRRTxnContextProvider]{
		sctx:         sctx,
		isolation:    "REPEATABLE-READ",
		minStartTime: time.Now(),
		active:       true,
		inTxn:        inTxn,
		minStartTS:   getOracleTS(t, sctx),
	}
}

func inactivePessimisticRRAssert(sctx sessionctx.Context) *txnAssert[*isolation.PessimisticRRTxnContextProvider] {
	return &txnAssert[*isolation.PessimisticRRTxnContextProvider]{
		sctx:         sctx,
		isolation:    "REPEATABLE-READ",
		minStartTime: time.Now(),
		active:       false,
	}
}

func initializeRepeatableReadProvider(t *testing.T, tk *testkit.TestKit, active bool) *isolation.PessimisticRRTxnContextProvider {
	tk.MustExec("commit")
	tk.MustExec("set @@tx_isolation = 'REPEATABLE-READ'")
	tk.MustExec("set @@tidb_txn_mode= 'pessimistic'")

	if active {
		assert := activePessimisticRRAssert(t, tk.Session(), true)
		tk.MustExec("begin pessimistic")
		return assert.CheckAndGetProvider(t)
	}

	tk.MustExec("set @@autocommit=0")
	assert := inactivePessimisticRRAssert(tk.Session())
	require.NoError(t, tk.Session().PrepareTxnCtx(context.TODO()))
	return assert.CheckAndGetProvider(t)
}

func TestRRWaitTSTimeInSlowLog(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	se := tk.Session()

	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, v int)")
	tk.MustExec("insert into t values (1, 1)")

	tk.MustExec("begin pessimistic")
	waitTS1 := se.GetSessionVars().DurationWaitTS
	tk.MustExec("update t set v = v + 10 where id = 1")
	waitTS2 := se.GetSessionVars().DurationWaitTS
	tk.MustExec("delete from t")
	waitTS3 := se.GetSessionVars().DurationWaitTS
	tk.MustExec("commit")
	require.NotEqual(t, waitTS1, waitTS2)
	require.NotEqual(t, waitTS1, waitTS3)
	require.NotEqual(t, waitTS2, waitTS3)
}

func TestIssue41194(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/session/enableAggressiveLockingOnBootstrap", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/session/enableAggressiveLockingOnBootstrap"))
	}()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, v int)")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("analyze table t")
}
