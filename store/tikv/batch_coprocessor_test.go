// Copyright 2020 PingCAP, Inc.
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

package tikv_test

import (
	"context"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/testkit"
)

type testBatchCopSuite struct {
}

var _ = SerialSuites(&testBatchCopSuite{})

func newStoreWithBootstrap(tiflashNum int) (kv.Storage, *domain.Domain, error) {
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockCluster := c.(*mocktikv.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			tiflashIdx := 0
			for tiflashIdx < tiflashNum {
				store2 := c.AllocID()
				peer2 := c.AllocID()
				addr2 := fmt.Sprintf("tiflash%d", tiflashIdx)
				mockCluster.AddStore(store2, addr2)
				mockCluster.UpdateStoreAddr(store2, addr2, &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
				mockCluster.AddPeer(region1, store2, peer2)
				tiflashIdx++
			}
		}),
		mockstore.WithStoreType(mockstore.MockTiKV),
	)

	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	session.SetSchemaLease(0)
	session.DisableStats4Test()

	dom, err := session.BootstrapSession(store)
	if err != nil {
		return nil, nil, err
	}

	dom.SetStatsUpdating(true)
	return store, dom, errors.Trace(err)
}

func testGetTableByName(c *C, ctx sessionctx.Context, db, table string) table.Table {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	c.Assert(err, IsNil)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr(db), model.NewCIStr(table))
	c.Assert(err, IsNil)
	return tbl
}

func (s *testBatchCopSuite) TestStoreErr(c *C) {
	store, dom, err := newStoreWithBootstrap(1)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")

	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int not null, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "t")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/BatchCopCancelled", "1*return(true)"), IsNil)

	err = tk.QueryToErr("select count(*) from t")
	c.Assert(errors.Cause(err), Equals, context.Canceled)

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/BatchCopRpcErrtiflash0", "1*return(\"tiflash0\")"), IsNil)

	tk.MustQuery("select count(*) from t").Check(testkit.Rows("1"))

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/BatchCopRpcErrtiflash0", "return(\"tiflash0\")"), IsNil)
	err = tk.QueryToErr("select count(*) from t")
	c.Assert(err, NotNil)
}

func (s *testBatchCopSuite) TestStoreSwitchPeer(c *C) {
	store, dom, err := newStoreWithBootstrap(2)
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`), IsNil)
	defer failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount")

	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int not null, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "t")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/BatchCopRpcErrtiflash0", "return(\"tiflash0\")"), IsNil)

	tk.MustQuery("select count(*) from t").Check(testkit.Rows("1"))

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/store/mockstore/mocktikv/BatchCopRpcErrtiflash1", "return(\"tiflash1\")"), IsNil)
	err = tk.QueryToErr("select count(*) from t")
	c.Assert(err, NotNil)

}
