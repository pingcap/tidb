// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package staleread_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestStaleReadTxnScope(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	checkProviderTxnScope := func() {
		provider := createStaleReadProvider(t, tk, false)
		require.Equal(t, kv.GlobalTxnScope, provider.GetTxnScope())

		provider = createStaleReadProvider(t, tk, true)
		require.Equal(t, kv.GlobalTxnScope, provider.GetTxnScope())

		tk.MustExec("rollback")
	}

	checkProviderTxnScope()

	require.NoError(t, failpoint.Enable("tikvclient/injectTxnScope", fmt.Sprintf(`return("%v")`, "bj")))
	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/injectTxnScope"))
	}()

	checkProviderTxnScope()

	tk.MustExec("set @@global.tidb_enable_local_txn=1")
	tk.MustExec("rollback")
	tk = testkit.NewTestKit(t, store)
	checkProviderTxnScope()
}

func TestStaleReadReplicaReadScope(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	checkProviderReplicaReadScope := func(scope string) {
		provider := createStaleReadProvider(t, tk, false)
		require.Equal(t, scope, provider.GetReadReplicaScope())

		provider = createStaleReadProvider(t, tk, true)
		require.Equal(t, scope, provider.GetReadReplicaScope())

		tk.MustExec("rollback")
	}

	checkProviderReplicaReadScope(kv.GlobalReplicaScope)

	require.NoError(t, failpoint.Enable("tikvclient/injectTxnScope", fmt.Sprintf(`return("%v")`, "bj")))
	defer func() {
		require.NoError(t, failpoint.Disable("tikvclient/injectTxnScope"))
	}()

	checkProviderReplicaReadScope("bj")
}

func createStaleReadProvider(t *testing.T, tk *testkit.TestKit, explicitTxn bool) *staleread.StalenessTxnContextProvider {
	tk.MustExec("rollback")
	require.NoError(t, tk.Session().PrepareTxnCtx(context.TODO()))
	se := tk.Session()
	ts := getOracleTS(t, se)
	if explicitTxn {
		err := sessiontxn.GetTxnManager(se).EnterNewTxn(context.TODO(), &sessiontxn.EnterNewTxnRequest{
			Type:        sessiontxn.EnterNewTxnWithBeginStmt,
			StaleReadTS: ts,
		})
		require.NoError(t, err)
	} else {
		is, err := domain.GetDomain(se).GetSnapshotInfoSchema(ts)
		require.NoError(t, err)
		err = sessiontxn.GetTxnManager(se).EnterNewTxn(context.TODO(), &sessiontxn.EnterNewTxnRequest{
			Type:     sessiontxn.EnterNewTxnWithReplaceProvider,
			Provider: staleread.NewStalenessTxnContextProvider(se, ts, is),
		})
		require.NoError(t, err)
	}
	return sessiontxn.GetTxnManager(se).GetContextProvider().(*staleread.StalenessTxnContextProvider)
}

func getOracleTS(t testing.TB, sctx sessionctx.Context) uint64 {
	ts, err := sctx.GetStore().GetOracle().GetTimestamp(context.TODO(), &oracle.Option{TxnScope: oracle.GlobalTxnScope})
	require.NoError(t, err)
	return ts
}
