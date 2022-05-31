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

	"github.com/pingcap/tidb/infoschema"

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
	compareTS = getOracleTS(t, se)
	require.Greater(t, compareTS, rcCheckTS)
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
	require.Greater(t, compareTS, rcCheckTS)
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
	require.Greater(t, compareTS, rcCheckTS)

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
	compareTS = getOracleTS(t, se)
	require.Greater(t, compareTS, readTS)
	require.NoError(t, provider.OnStmtRetry(context.TODO()))
	readTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	forUpdateTS, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Equal(t, readTS, forUpdateTS)
	require.Greater(t, readTS, compareTS)
}

func TestRCProviderInitialize(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	se := tk.Session()
	tk.MustExec("set @@tx_isolation = 'READ-COMMITTED'")
	tk.MustExec("set @@tidb_txn_mode='pessimistic'")

	// begin outside a txn
	minStartTime := time.Now()
	tk.MustExec("begin")
	checkActiveRCTxn(t, se, minStartTime)

	// begin in a txn
	minStartTime = time.Now()
	tk.MustExec("begin")
	checkActiveRCTxn(t, se, minStartTime)

	// non-active txn and then active it
	tk.MustExec("rollback")
	tk.MustExec("set @@autocommit=0")
	minStartTime = time.Now()
	require.NoError(t, se.PrepareTxnCtx(context.TODO()))
	provider := checkNotActiveRCTxn(t, se, minStartTime)
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	ts, err := provider.GetStmtReadTS()
	require.NoError(t, err)
	checkActiveRCTxn(t, se, minStartTime)
	require.Equal(t, ts, se.GetSessionVars().TxnCtx.StartTS)
	tk.MustExec("rollback")
}

func TestTidbSnapshotVar(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
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
	//isVersion := do.InfoSchema().(infoschema.InfoSchema).SchemaMetaVersion()
	tk.MustExec("create temporary table t2(id int)")
	tk.MustExec("set @@tidb_snapshot=@a")
	snapshotTS := tk.Session().GetSessionVars().SnapshotTS
	isVersion := dom.InfoSchema().SchemaMetaVersion()

	minStartTime := time.Now()
	tk.MustExec("begin pessimistic")
	provider := checkActiveRCTxn(t, se, minStartTime)
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

	checkUseTxn := func(useTxnTs bool) {
		is := provider.GetTxnInfoSchema()
		require.Equal(t, isVersion, is.SchemaMetaVersion())
		require.IsType(t, &infoschema.TemporaryTableAttachedInfoSchema{}, is)
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
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	checkUseSnapshot()

	// information schema and ts will restore when set tidb_snapshot to empty
	tk.MustExec("set @@tidb_snapshot=''")
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	checkUseTxn(false)

	// txn will not be active after `GetStmtReadTS` or `GetStmtForUpdateTS` when `tidb_snapshot` is set
	tk.MustExec("rollback")
	tk.MustExec("set @@tidb_txn_mode='pessimistic'")
	tk.MustExec("set @@autocommit=0")
	minStartTime = time.Now()
	require.NoError(t, se.PrepareTxnCtx(context.TODO()))
	provider = checkNotActiveRCTxn(t, se, minStartTime)
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	tk.MustExec("set @@tidb_snapshot=@a")
	checkUseSnapshot()
	txn, err = se.Txn(false)
	require.NoError(t, err)
	require.False(t, txn.Valid())
	tk.MustExec("set @@tidb_snapshot=''")
	checkUseTxn(true)
	checkActiveRCTxn(t, se, minStartTime)
	tk.MustExec("rollback")
}

func checkActiveRCTxn(t *testing.T, sctx sessionctx.Context, minStartTime time.Time) *isolation.PessimisticRCTxnContextProvider {
	sessVars := sctx.GetSessionVars()
	txnCtx := sessVars.TxnCtx
	provider := checkBasicRCTxn(t, sctx, minStartTime)

	txn, err := sctx.Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	require.Equal(t, txn.StartTS(), txnCtx.StartTS)
	require.True(t, txnCtx.IsExplicit)
	require.True(t, sessVars.InTxn())
	require.Same(t, sessVars.KVVars, txn.GetVars())
	return provider
}

func checkNotActiveRCTxn(t *testing.T, sctx sessionctx.Context, minStartTime time.Time) *isolation.PessimisticRCTxnContextProvider {
	sessVars := sctx.GetSessionVars()
	txnCtx := sessVars.TxnCtx
	provider := checkBasicRCTxn(t, sctx, minStartTime)

	txn, err := sctx.Txn(false)
	require.NoError(t, err)
	require.False(t, txn.Valid())
	require.Equal(t, uint64(0), txnCtx.StartTS)
	require.False(t, txnCtx.IsExplicit)
	require.False(t, sessVars.InTxn())
	return provider
}

func checkBasicRCTxn(t *testing.T, sctx sessionctx.Context, minStartTime time.Time) *isolation.PessimisticRCTxnContextProvider {
	provider := sessiontxn.GetTxnManager(sctx).GetContextProvider()
	require.IsType(t, &isolation.PessimisticRCTxnContextProvider{}, provider)

	sessVars := sctx.GetSessionVars()
	txnCtx := sessVars.TxnCtx

	if sessVars.SnapshotInfoschema == nil {
		require.Same(t, provider.GetTxnInfoSchema(), txnCtx.InfoSchema)
	} else {
		require.Equal(t, sessVars.SnapshotInfoschema.(infoschema.InfoSchema).SchemaMetaVersion(), provider.GetTxnInfoSchema().SchemaMetaVersion())
	}
	require.Equal(t, "READ-COMMITTED", txnCtx.Isolation)
	require.True(t, txnCtx.IsPessimistic)
	require.Equal(t, sctx.GetSessionVars().CheckAndGetTxnScope(), txnCtx.TxnScope)
	require.Equal(t, sessVars.ShardAllocateStep, int64(txnCtx.ShardStep))
	require.False(t, txnCtx.IsStaleness)
	require.GreaterOrEqual(t, txnCtx.CreateTime.Nanosecond(), minStartTime.Nanosecond())
	return provider.(*isolation.PessimisticRCTxnContextProvider)
}

func initializePessimisticRCProvider(t *testing.T, tk *testkit.TestKit) *isolation.PessimisticRCTxnContextProvider {
	tk.MustExec("set @@tx_isolation = 'READ-COMMITTED'")
	minStartTime := time.Now()
	tk.MustExec("begin pessimistic")
	return checkActiveRCTxn(t, tk.Session(), minStartTime)
}

func getOracleTS(t *testing.T, sctx sessionctx.Context) uint64 {
	ts, err := sctx.GetStore().GetOracle().GetTimestamp(context.TODO(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoError(t, err)
	return ts
}
