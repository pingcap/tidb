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

package legacy_test

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/sessiontxn/legacy"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
)

func TestErrorHandle(t *testing.T) {
	store, do, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	provider := newSimpleProvider(tk, do)
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	expectedForUpdateTS := getForUpdateTS(t, provider)

	var lockErr error

	// StmtErrAfterLock: ErrWriteConflict should retry and update forUpdateTS
	lockErr = kv.ErrWriteConflict
	action, err := provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	require.Equal(t, sessiontxn.StmtActionRetryReady, action)
	require.Nil(t, err)
	expectedForUpdateTS += 1
	require.Equal(t, expectedForUpdateTS, getForUpdateTS(t, provider))

	// StmtErrAfterLock: DeadLock that is not retryable will just return an error
	lockErr = newDeadLockError(false)
	action, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	require.Equal(t, sessiontxn.StmtActionError, action)
	require.Equal(t, lockErr, err)
	require.Equal(t, expectedForUpdateTS, getForUpdateTS(t, provider))

	// StmtErrAfterLock: DeadLock that is retryable should retry and update forUpdateTS
	lockErr = newDeadLockError(true)
	action, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	require.Equal(t, sessiontxn.StmtActionRetryReady, action)
	require.Nil(t, err)
	expectedForUpdateTS += 1
	require.Equal(t, expectedForUpdateTS, getForUpdateTS(t, provider))

	// StmtErrAfterLock: other errors should only update forUpdateTS but not retry
	lockErr = errors.New("other error")
	action, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterPessimisticLock, lockErr)
	require.Equal(t, sessiontxn.StmtActionError, action)
	require.Equal(t, lockErr, err)
	expectedForUpdateTS += 1
	require.Equal(t, expectedForUpdateTS, getForUpdateTS(t, provider))

	// StmtErrAfterQuery: ErrWriteConflict should not retry when not RCCheckTS read
	lockErr = kv.ErrWriteConflict
	action, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterQuery, lockErr)
	require.Equal(t, sessiontxn.StmtActionNoIdea, action)
	require.Nil(t, err)

	// StmtErrAfterQuery: ErrWriteConflict should retry when RCCheckTS read
	tk.MustExec("set @@tidb_rc_read_check_ts=1")
	tk.MustExec("set @@tx_isolation = 'READ-COMMITTED'")
	provider = newSimpleProvider(tk, do)
	stmts, _, err := parser.New().ParseSQL("select * from t")
	require.NoError(t, err)
	require.NoError(t, executor.ResetContextOfStmt(tk.Session(), stmts[0]))
	require.NoError(t, provider.OnStmtStart(context.TODO()))
	action, err = provider.OnStmtErrorForNextAction(sessiontxn.StmtErrAfterQuery, lockErr)
	require.Equal(t, sessiontxn.StmtActionRetryReady, action)
	require.Nil(t, err)
}

func getForUpdateTS(t *testing.T, provider *legacy.SimpleTxnContextProvider) uint64 {
	forUpdateTS, err := provider.GetStmtForUpdateTS()
	require.NoError(t, err)
	return forUpdateTS
}

func newDeadLockError(isRetryable bool) error {
	return &tikverr.ErrDeadlock{
		Deadlock:    &kvrpcpb.Deadlock{},
		IsRetryable: isRetryable,
	}
}

func newSimpleProvider(tk *testkit.TestKit, do *domain.Domain) *legacy.SimpleTxnContextProvider {
	tk.MustExec("begin pessimistic")
	readTS := uint64(1)
	forUpdateTS := uint64(1)
	return &legacy.SimpleTxnContextProvider{
		Ctx:        context.TODO(),
		Sctx:       tk.Session(),
		InfoSchema: do.InfoSchema(),
		GetReadTSFunc: func() (uint64, error) {
			return readTS, nil
		},
		GetForUpdateTSFunc: func() (uint64, error) {
			return forUpdateTS, nil
		},
		UpdateForUpdateTS: func(seCtx sessionctx.Context, newForUpdateTS uint64) error {
			if newForUpdateTS == 0 {
				forUpdateTS += 1
			} else {
				forUpdateTS = newForUpdateTS
			}
			return nil
		},
		Pessimistic: true,
	}
}
