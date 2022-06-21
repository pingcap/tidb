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
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
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
}

func TestTidbSnapshotVarInPessimisticRepeatableRead(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
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
		require.IsType(t, &infoschema.TemporaryTableAttachedInfoSchema{}, is)
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
		require.IsType(t, &infoschema.TemporaryTableAttachedInfoSchema{}, is)
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
			require.NoError(t, provider.OnStmtStart(context.TODO()))
			checkUseSnapshot()
			assertAfterUseSnapshot.Check(t)
		}()
	}
}

func TestOptimizeWithPlanInPessimisticRR(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, v int)")
	tk.MustExec("insert into t values (1,1), (2,2)")
	se := tk.Session()
	provider := initializeRepeatableReadProvider(t, tk, true)
	forUpdateTS := se.GetSessionVars().TxnCtx.GetForUpdateTS()
	txnManager := sessiontxn.GetTxnManager(se)

	require.NoError(t, txnManager.OnStmtStart(context.TODO()))
	stmt, err := parser.New().ParseOneStmt("delete from t where id = 1", "", "")
	require.NoError(t, err)
	compareTs := getOracleTS(t, se)
	compiler := executor.Compiler{Ctx: se}
	execStmt, err := compiler.Compile(context.TODO(), stmt)
	require.NoError(t, err)
	err = txnManager.AdviseOptimizeWithPlan(execStmt.Plan)
	require.NoError(t, err)
	ts, err := provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, compareTs, ts)
	require.Equal(t, ts, forUpdateTS)

	require.NoError(t, txnManager.OnStmtStart(context.TODO()))
	stmt, err = parser.New().ParseOneStmt("update t set v = v + 10 where id = 1", "", "")
	require.NoError(t, err)
	compiler = executor.Compiler{Ctx: se}
	execStmt, err = compiler.Compile(context.TODO(), stmt)
	require.NoError(t, err)
	err = txnManager.AdviseOptimizeWithPlan(execStmt.Plan)
	require.NoError(t, err)
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, ts, forUpdateTS)

	require.NoError(t, txnManager.OnStmtStart(context.TODO()))
	stmt, err = parser.New().ParseOneStmt("select * from (select * from t where id = 1 for update) as t1 for update", "", "")
	require.NoError(t, err)
	compiler = executor.Compiler{Ctx: se}
	execStmt, err = compiler.Compile(context.TODO(), stmt)
	require.NoError(t, err)
	err = txnManager.AdviseOptimizeWithPlan(execStmt.Plan)
	require.NoError(t, err)
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, ts, forUpdateTS)

	// Now, test for one that does not use the optimization
	require.NoError(t, txnManager.OnStmtStart(context.TODO()))
	stmt, err = parser.New().ParseOneStmt("select * from t for update", "", "")
	compareTs = getOracleTS(t, se)
	require.NoError(t, err)
	compiler = executor.Compiler{Ctx: se}
	execStmt, err = compiler.Compile(context.TODO(), stmt)
	require.NoError(t, err)
	err = txnManager.AdviseOptimizeWithPlan(execStmt.Plan)
	require.NoError(t, err)
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTs)

	// Test use startTS after optimize when autocommit=0
	activeAssert := activePessimisticRRAssert(t, tk.Session(), true)
	provider = initializeRepeatableReadProvider(t, tk, false)
	require.NoError(t, txnManager.OnStmtStart(context.TODO()))
	stmt, err = parser.New().ParseOneStmt("update t set v = v + 10 where id = 1", "", "")
	require.NoError(t, err)
	execStmt, err = compiler.Compile(context.TODO(), stmt)
	require.NoError(t, err)
	err = txnManager.AdviseOptimizeWithPlan(execStmt.Plan)
	require.NoError(t, err)
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Same(t, provider, activeAssert.CheckAndGetProvider(t))
	require.Equal(t, tk.Session().GetSessionVars().TxnCtx.StartTS, ts)

	// Test still fetch for update ts after optimize when autocommit=0
	compareTs = getOracleTS(t, se)
	activeAssert = activePessimisticRRAssert(t, tk.Session(), true)
	provider = initializeRepeatableReadProvider(t, tk, false)
	require.NoError(t, txnManager.OnStmtStart(context.TODO()))
	stmt, err = parser.New().ParseOneStmt("select * from t", "", "")
	require.NoError(t, err)
	execStmt, err = compiler.Compile(context.TODO(), stmt)
	require.NoError(t, err)
	err = txnManager.AdviseOptimizeWithPlan(execStmt.Plan)
	require.NoError(t, err)
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTs)
}

func activePessimisticRRAssert(t *testing.T, sctx sessionctx.Context,
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
