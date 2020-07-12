package tikv_test

import (
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

var _ = Suite(&testBatchCopSuite{})

func newStoreWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockCluster := c.(*mocktikv.Cluster)
			_, _, region1 := mockstore.BootstrapWithSingleStore(c)
			store2 := c.AllocID()
			peer2 := c.AllocID()
			mockCluster.AddStore(store2, "tiflash0")
			mockCluster.UpdateStoreAddr(store2, "tiflash", &metapb.StoreLabel{Key: "engine", Value: "tiflash"})
			mockCluster.AddPeer(region1, store2, peer2)
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
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk := testkit.NewTestKit(c, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int not null, b int not null)")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := testGetTableByName(c, tk.Se, "test", "t")
	err = domain.GetDomain(tk.Se).DDL().UpdateTableReplicaInfo(tk.Se, tb.Meta().ID, true)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t values(1,0)")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")

	failpoint.Enable("rpcServerBusy", "return(true)")

	tk.MustQuery("select count(*) from t").Check(testkit.Rows("1"))
}
