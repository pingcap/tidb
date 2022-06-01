package isolation_test

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/isolation"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
	"testing"
)

func newDeadLockError(isRetryable bool) error {
	return &tikverr.ErrDeadlock{
		Deadlock:    &kvrpcpb.Deadlock{},
		IsRetryable: isRetryable,
	}
}

func TestPessimisticRRStmtStartAndRetry(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	provider := initializeRepeatableReadProvider(t, tk)
	se := tk.Session()

	var lockErr error
	var compareTS uint64
	for _, firstActionOnError := range []func(ctx context.Context) error{
		provider.OnStmtStart,
		provider.OnStmtRetry,
	} {
		compareTS = getOracleTS(t, se)
		lockErr = kv.ErrWriteConflict
		nextAction, err := provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
		require.NoError(t, err)
		require.Equal(t, sessiontxn.StmtActionRetryReady, nextAction)
		require.NoError(t, firstActionOnError(context.TODO()))
		ts, err := provider.GetStmtForUpdateTS()
		require.NoError(t, err)
		// StmtActionRetryReady means we will update the forUpdateTS, so it should be larger than the compareTS
		require.Greater(t, ts, compareTS)
	}

	for _, firstActionOnError := range []func(ctx context.Context) error{
		provider.OnStmtStart,
		provider.OnStmtRetry,
	} {
		compareTS = getOracleTS(t, se)
		lockErr = newDeadLockError(true)
		nextAction, err := provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
		require.NoError(t, err)
		require.Equal(t, sessiontxn.StmtActionRetryReady, nextAction)
		require.NoError(t, firstActionOnError(context.TODO()))
		ts, err := provider.GetStmtForUpdateTS()
		require.NoError(t, err)
		// StmtActionRetryReady means we will update the forUpdateTS, so it should be larger than the compareTS
		require.Greater(t, ts, compareTS)
	}
}

func TestPessimisticRRErrorHandle(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	se := tk.Session()
	provider := initializeRepeatableReadProvider(t, tk)

	var lockErr error

	compareTS := getOracleTS(t, se)
	lockErr = kv.ErrWriteConflict
	nextAction, err := provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	require.NoError(t, err)
	require.Equal(t, sessiontxn.StmtActionRetryReady, nextAction)
	err = provider.OnStmtRetry(context.TODO())
	require.NoError(t, err)
	ts, err := provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	// StmtActionRetryReady means we will update the forUpdateTS, so it should be larger than the compareTS
	require.Greater(t, ts, compareTS)

	// Update compareTS for the next comparison
	compareTS = getOracleTS(t, se)
	lockErr = newDeadLockError(false)
	nextAction, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	require.Equal(t, lockErr, err)
	require.Equal(t, sessiontxn.StmtActionError, nextAction)
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	// StmtActionError means we will not update the forUpdateTS, so it should be less than the compareTS
	require.Greater(t, compareTS, ts)

	lockErr = newDeadLockError(true)
	nextAction, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	require.NoError(t, err)
	require.Equal(t, sessiontxn.StmtActionRetryReady, nextAction)
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	// StmtActionRetryReady means we will update the forUpdateTS, so it should be larger than the compareTS
	require.Greater(t, ts, compareTS)

	compareTS = getOracleTS(t, se)
	// StmtErrAfterLock: other errors should only update forUpdateTS but not retry
	lockErr = errors.New("other error")
	nextAction, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	require.Equal(t, lockErr, err)
	require.Equal(t, sessiontxn.StmtActionError, nextAction)
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, ts, compareTS)

	compareTS = getOracleTS(t, se)
	// StmtErrAfterQuery: always not retry and not update forUpdateTS
	lockErr = kv.ErrWriteConflict
	nextAction, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterQuery, lockErr)
	require.Equal(t, sessiontxn.StmtActionNoIdea, nextAction)
	require.Nil(t, err)
	ts, err = provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, compareTS, ts)

}

func TestRepeatableReadProvider(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	se := tk.Session()
	provider := initializeRepeatableReadProvider(t, tk)

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
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	CurrentTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Greater(t, compareTS, CurrentTS)
	prevTS = CurrentTS

	// The read ts should also be less than the compareTS in a new statement
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	CurrentTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Equal(t, CurrentTS, prevTS)

	// The read ts should still be less than the compareTS in a retry statement
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtRetry(context.TODO()))
	CurrentTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Greater(t, CurrentTS, prevTS)

	// The for update read ts should be larger than the compareTS
	require.NoError(t, executor.ResetContextOfStmt(se, forUpdateStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	forUpdateTS, err := provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, forUpdateTS, compareTS)

	// But the read ts is still less than the compareTS
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	CurrentTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Greater(t, CurrentTS, prevTS)
}

func TestSomething(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)

	tk1.MustExec("use test")
	tk2.MustExec("use test")

	tk1.MustExec("create table t (id int primary key, v int)")
	tk1.MustExec("insert into t values(1,1),(2,2)")

	tk1.MustExec("set  @@tx_isolation='REPEATABLE-READ'")
	tk1.MustExec("begin pessimistic")

	tk2.MustExec("insert into t values(3,3)")

	tk1.MustQuery("select * from t where id = 1 for update")
	tk1.MustQuery("select * from t")

	tk1.MustQuery("select * from t where id = 3 for update").Check(testkit.Rows("3 3"))
}

func initializeRepeatableReadProvider(t *testing.T, tk *testkit.TestKit) *isolation.PessimisticRRTxnContextProvider {
	tk.MustExec("set @@tx_isolation = 'REPEATABLE-READ'")
	tk.MustExec("begin pessimistic")
	provider := sessiontxn.GetTxnManager(tk.Session()).GetContextProvider()
	require.IsType(t, &isolation.PessimisticRRTxnContextProvider{}, provider)
	return provider.(*isolation.PessimisticRRTxnContextProvider)
}
