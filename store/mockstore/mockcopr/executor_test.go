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
// See the License for the specific language governing permissions and
// limitations under the License.

package mockcopr_test

import (
	"context"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore/mockcopr"
	"github.com/pingcap/tidb/store/mockstore/mockstorage"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testExecutorSuite{})

type testExecutorSuite struct {
	cluster   *testutils.MockCluster
	store     kv.Storage
	mvccStore testutils.MVCCStore
	dom       *domain.Domain
}

func (s *testExecutorSuite) SetUpSuite(c *C) {
	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	c.Assert(err, IsNil)
	testutils.BootstrapWithSingleStore(cluster)
	s.cluster = cluster
	s.mvccStore = rpcClient.MvccStore
	store, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	c.Assert(err, IsNil)
	s.store, err = mockstorage.NewMockStorage(store)
	c.Assert(err, IsNil)
	session.SetSchemaLease(0)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testExecutorSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *testExecutorSuite) TestResolvedLargeTxnLocks(c *C) {
	// This test checks the resolve lock functionality.
	// When a txn meets the lock of a large transaction, it should not block by the
	// lock.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, val int)")
	dom := domain.GetDomain(tk.Se)
	schema := dom.InfoSchema()
	tbl, err := schema.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)

	tk.MustExec("insert into t values (1, 1)")

	o := s.store.GetOracle()
	tso, err := o.GetTimestamp(context.Background(), &oracle.Option{TxnScope: kv.GlobalTxnScope})
	c.Assert(err, IsNil)

	key := tablecodec.EncodeRowKeyWithHandle(tbl.Meta().ID, kv.IntHandle(1))
	pairs := s.mvccStore.Scan(key, nil, 1, tso, kvrpcpb.IsolationLevel_SI, nil)
	c.Assert(pairs, HasLen, 1)
	c.Assert(pairs[0].Err, IsNil)

	// Simulate a large txn (holding a pk lock with large TTL).
	// Secondary lock 200ms, primary lock 100s
	c.Assert(prewriteMVCCStore(s.mvccStore, putMutations("primary", "value"), "primary", tso, 100000), IsTrue)
	c.Assert(prewriteMVCCStore(s.mvccStore, putMutations(string(key), "value"), "primary", tso, 200), IsTrue)

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
	pairs = s.mvccStore.Scan([]byte("primary"), nil, 1, tso, kvrpcpb.IsolationLevel_SI, nil)
	c.Assert(pairs, HasLen, 1)
	_, ok := errors.Cause(pairs[0].Err).(*testutils.ErrLocked)
	c.Assert(ok, IsTrue)
}

func (s *testExecutorSuite) TestIssue15662(c *C) {
	tk := testkit.NewTestKit(c, s.store)

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
