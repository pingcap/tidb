package infoschema_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb"
)

func (s *testSuite) TestDataForTableRowsCountField(c *C) {
	testleak.BeforeTest()
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	do, err := tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	defer do.Close()

	h := do.StatsHandle()
	is := do.InfoSchema()
	tk := testkit.NewTestKit(c, store)


	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	tk.MustQuery("select table_rows from information_schema.tables where table_name='t'").Check(
		testkit.Rows("0"))
	tk.MustExec("insert into t(c, d) values(1, 2), (2, 3), (3, 4)")
	h.DumpStatsDeltaToKV()
	h.Update(is)
	tk.MustQuery("select table_rows from information_schema.tables where table_name='t'").Check(
		testkit.Rows("3"))
	tk.MustExec("insert into t(c, d) values(4, 5)")
	h.DumpStatsDeltaToKV()
	h.Update(is)
	tk.MustQuery("select table_rows from information_schema.tables where table_name='t'").Check(
		testkit.Rows("4"))
	tk.MustExec("delete from t where c >= 3")
	h.DumpStatsDeltaToKV()
	h.Update(is)
	tk.MustQuery("select table_rows from information_schema.tables where table_name='t'").Check(
		testkit.Rows("2"))
	tk.MustExec("delete from t where c=3")
	h.DumpStatsDeltaToKV()
	h.Update(is)
	tk.MustQuery("select table_rows from information_schema.tables where table_name='t'").Check(
		testkit.Rows("2"))
}
