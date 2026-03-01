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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/sessiontxn/staleread"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	storekv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
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

func TestGetStmtReadTSActivateTxnWhenAutocommitOff(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table if not exists t (id int primary key)")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("commit")
	tk.MustExec("set @@autocommit=0")

	createStaleReadProvider(t, tk, false)

	se := tk.Session()
	mgr := sessiontxn.GetTxnManager(se)
	require.NoError(t, mgr.OnStmtStart(context.Background(), nil))

	txn, err := se.Txn(false)
	require.NoError(t, err)
	require.False(t, txn.Valid())
	require.False(t, se.GetSessionVars().InTxn())

	// GetStmtReadTS should activate the transaction when autocommit is off.
	snap, err := mgr.GetStmtReadTS()
	require.NoError(t, err)
	require.NotNil(t, snap)

	txn, err = se.Txn(false)
	require.NoError(t, err)
	require.True(t, txn.Valid())
	require.True(t, se.GetSessionVars().InTxn())
}

func TestGetSnapshotWithStmtReadTSActivateTxnWhenAutocommitOff(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, v int)")
	tk.MustExec("insert into t values (1, 1)")
	tk.MustExec("commit")
	tk.MustExec("set @@autocommit=0")
	tk.MustExec("set @@tidb_replica_read='follower'")

	createStaleReadProvider(t, tk, false)

	se := tk.Session()
	mgr := sessiontxn.GetTxnManager(se)
	require.NoError(t, mgr.OnStmtStart(context.Background(), nil))
	se.GetSessionVars().StmtCtx.IsReadOnly = true
	require.Equal(t, kv.ReplicaReadFollower, se.GetSessionVars().GetReplicaRead())

	snap, err := mgr.GetSnapshotWithStmtReadTS()
	require.NoError(t, err)
	require.NotNil(t, snap)

	// In client-go, stale point-get requests (CmdGet) always override ReplicaReadType to "mixed"
	// via EnableStaleWithMixedReplicaRead(), which makes it hard to observe the snapshot's replica
	// read option. Use a scan request (CmdScan) to validate ReplicaReadType directly.
	got := storekv.ReplicaReadType(255)
	snap.SetOption(kv.RPCInterceptor, interceptor.NewRPCInterceptor("capture-replica-read", func(next interceptor.RPCInterceptorFunc) interceptor.RPCInterceptorFunc {
		return func(target string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
			if req != nil && req.Type == tikvrpc.CmdScan {
				got = req.ReplicaReadType
			}
			return next(target, req)
		}
	}))

	is := domain.GetDomain(se).InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	rowKey := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))

	iter, err := snap.Iter(rowKey, rowKey.PrefixNext())
	require.NoError(t, err)
	require.True(t, iter.Valid())
	iter.Close()

	require.Equal(t, storekv.ReplicaReadFollower, got)
}

func createStaleReadProvider(t *testing.T, tk *testkit.TestKit, explicitTxn bool) *staleread.StalenessTxnContextProvider {
	tk.MustExec("rollback")
	require.NoError(t, tk.Session().PrepareTxnCtx(context.TODO(), nil))
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
