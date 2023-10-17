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
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/isolation"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfork"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
)

func TestPessimisticSerializableTxnProviderTS(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")
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
	require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
	ts, err := provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Greater(t, compareTS, ts)
	prevTs := ts

	// In Oracle-like serializable isolation, readTS equals to the for update ts
	require.NoError(t, executor.ResetContextOfStmt(se, forUpdateStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, compareTS, ts)
	require.Equal(t, prevTs, ts)
}

func TestPessimisticSerializableTxnContextProviderLockError(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")
	se := tk.Session()
	provider := initializePessimisticSerializableProvider(t, tk)
	ctx := context.Background()

	stmts, _, err := parser.New().Parse("select * from t for update", "", "")
	require.NoError(t, err)
	stmt := stmts[0]

	// retryable errors
	for _, lockErr := range []error{
		kv.ErrWriteConflict,
		&tikverr.ErrDeadlock{Deadlock: &kvrpcpb.Deadlock{}, IsRetryable: true},
	} {
		require.NoError(t, executor.ResetContextOfStmt(se, stmt))
		require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
		nextAction, err := provider.OnStmtErrorForNextAction(ctx, sessiontxn.StmtErrAfterPessimisticLock, lockErr)
		require.Same(t, lockErr, err)
		require.Equal(t, sessiontxn.StmtActionError, nextAction)
	}

	// non-retryable errors
	for _, lockErr := range []error{
		&tikverr.ErrDeadlock{Deadlock: &kvrpcpb.Deadlock{}, IsRetryable: false},
		errors.New("err"),
	} {
		require.NoError(t, executor.ResetContextOfStmt(se, stmt))
		require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
		nextAction, err := provider.OnStmtErrorForNextAction(ctx, sessiontxn.StmtErrAfterPessimisticLock, lockErr)
		require.Same(t, lockErr, err)
		require.Equal(t, sessiontxn.StmtActionError, nextAction)
	}
}

func TestSerializableInitialize(t *testing.T) {
	store := testkit.CreateMockStore(t)

	testfork.RunTest(t, func(t *testfork.T) {
		clearScopeSettings := forkScopeSettings(t, store)
		defer clearScopeSettings()

		tk := testkit.NewTestKit(t, store)
		defer tk.MustExec("rollback")
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
		assert = inactiveSerializableAssert(se)
		assertAfterActive := activeSerializableAssert(t, se, true)
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
		assert = inactiveSerializableAssert(se)
		assertAfterActive = activeSerializableAssert(t, se, true)
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

func TestTidbSnapshotVarInSerialize(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("rollback")
	se := tk.Session()
	tk.MustExec("set tidb_skip_isolation_level_check = 1")
	tk.MustExec("set @@tx_isolation = 'SERIALIZABLE'")
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

	assert := activeSerializableAssert(t, se, true)
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
			assert = inactiveSerializableAssert(se)
			assertAfterUseSnapshot := activeSnapshotTxnAssert(se, se.GetSessionVars().SnapshotTS, "SERIALIZABLE")
			require.NoError(t, se.PrepareTxnCtx(context.TODO()))
			provider = assert.CheckAndGetProvider(t)
			require.NoError(t, provider.OnStmtStart(context.TODO(), nil))
			checkUseSnapshot()
			assertAfterUseSnapshot.Check(t)
		}()
	}
}

func activeSerializableAssert(t testing.TB, sctx sessionctx.Context,
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

func inactiveSerializableAssert(sctx sessionctx.Context) *txnAssert[*isolation.PessimisticSerializableTxnContextProvider] {
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
