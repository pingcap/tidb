package isolation_test

import (
	"context"
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
	"testing"
	"time"
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
	provider := initializeRepeatableReadProvider(t, tk)

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
	err = provider.OnStmtStart(context.TODO())
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
	err = provider.OnStmtStart(context.TODO())
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

	// The read ts should also be less than the compareTS in a new statement (after calling OnStmtStart)
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
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
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	forUpdateTS, err := provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	require.Greater(t, forUpdateTS, compareTS)

	// But the read ts is still less than the compareTS
	require.NoError(t, executor.ResetContextOfStmt(se, readOnlyStmt))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	CurrentTS, err = provider.GetStmtReadTS()
	require.NoError(t, err)
	require.Equal(t, CurrentTS, prevTS)
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

func inActivePessimisticRRAssert(sctx sessionctx.Context) *txnAssert[*isolation.PessimisticRRTxnContextProvider] {
	return &txnAssert[*isolation.PessimisticRRTxnContextProvider]{
		sctx:         sctx,
		isolation:    "REPEATABLE-READ",
		minStartTime: time.Now(),
		active:       false,
	}
}

func initializeRepeatableReadProvider(t *testing.T, tk *testkit.TestKit) *isolation.PessimisticRRTxnContextProvider {
	tk.MustExec("set @@tx_isolation = 'REPEATABLE-READ'")
	assert := activePessimisticRRAssert(t, tk.Session(), true)
	tk.MustExec("begin pessimistic")
	return assert.CheckAndGetProvider(t)
}
