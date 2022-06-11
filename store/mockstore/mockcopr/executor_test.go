// Copyright 2019-present, PingCAP, Inc.
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

package mockcopr_test

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore/mockcopr"
	"github.com/pingcap/tidb/store/mockstore/mockstorage"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

// This test checks the resolve lock functionality. When a txn meets the lock of a large transaction,
// it should not block by the lock.
func TestResolvedLargeTxnLocks(t *testing.T) {
	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.NoError(t, err)

	testutils.BootstrapWithSingleStore(cluster)
	mvccStore := rpcClient.MvccStore
	tikvStore, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.NoError(t, err)

	store, err := mockstorage.NewMockStorage(tikvStore)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Close())
	}()

	session.SetSchemaLease(0)
	session.DisableStats4Test()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, val int)")
	dom = domain.GetDomain(tk.Session())
	schema := dom.InfoSchema()
	tbl, err := schema.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	tk.MustExec("insert into t values (1, 1)")

	o := store.GetOracle()
	tso, err := o.GetTimestamp(context.Background(), &oracle.Option{TxnScope: kv.GlobalTxnScope})
	require.NoError(t, err)

	key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))
	pairs := mvccStore.Scan(key, nil, 1, tso, kvrpcpb.IsolationLevel_SI, nil)
	require.Len(t, pairs, 1)
	require.Nil(t, pairs[0].Err)

	// Simulate a large txn (holding a pk lock with large TTL).
	// Secondary lock 200ms, primary lock 100s
	require.True(t, prewriteMVCCStore(mvccStore, putMutations("primary", "value"), "primary", tso, 100000))
	require.True(t, prewriteMVCCStore(mvccStore, putMutations(string(key), "value"), "primary", tso, 200))

	// Simulate the action of reading meet the lock of a large txn.
	// The lock of the large transaction should not block read.
	// The first time, this query should meet a lock on the secondary key, then resolve lock.
	// After that, the query should read the previous version data.
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))

	// Cover BatchGet.
	tk.MustQuery("select * from t where id in (1)").Check(testkit.Rows("1 1"))

	// Cover PointGet.
	tk.MustExec("begin")
	tk.MustQuery("select * from t where id = 1").Check(testkit.Rows("1 1"))
	tk.MustExec("rollback")

	// And check the large txn is still alive.
	pairs = mvccStore.Scan([]byte("primary"), nil, 1, tso, kvrpcpb.IsolationLevel_SI, nil)
	require.Len(t, pairs, 1)
	_, ok := errors.Cause(pairs[0].Err).(*testutils.ErrLocked)
	require.True(t, ok)
}

func TestIssue15662(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")

	tk.MustExec("create table V (id int primary key, col_int int)")
	tk.MustExec("insert into V values (1, 8)")

	tk.MustExec("create table F (id int primary key, col_int int)")
	tk.MustExec("insert into F values (1, 8)")

	tk.MustQuery("select table1.`col_int` as field1, table1.`col_int` as field2 from V as table1 left join F as table2 on table1.`col_int` = table2.`col_int` order by field1, field2 desc limit 2").
		Check(testkit.Rows("8 8"))
}

func putMutations(kvpairs ...string) []*kvrpcpb.Mutation {
	var mutations []*kvrpcpb.Mutation
	for i := 0; i < len(kvpairs); i += 2 {
		mutations = append(mutations, &kvrpcpb.Mutation{
			Op:    kvrpcpb.Op_Put,
			Key:   []byte(kvpairs[i]),
			Value: []byte(kvpairs[i+1]),
		})
	}
	return mutations
}

func prewriteMVCCStore(store testutils.MVCCStore, mutations []*kvrpcpb.Mutation, primary string, startTS uint64, ttl uint64) bool {
	req := &kvrpcpb.PrewriteRequest{
		Mutations:    mutations,
		PrimaryLock:  []byte(primary),
		StartVersion: startTS,
		LockTtl:      ttl,
		MinCommitTs:  startTS + 1,
	}
	errs := store.Prewrite(req)
	for _, err := range errs {
		if err != nil {
			return false
		}
	}
	return true
}
