package executor_test

import (
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/testkit"
)

// TODO: reopen the index merge join in future.

//func (s *testSuite9) TestIndexLookupMergeJoinHang(c *C) {
//	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/IndexMergeJoinMockOOM", `return(true)`), IsNil)
//	defer func() {
//		c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/IndexMergeJoinMockOOM"), IsNil)
//	}()
//
//	tk := testkit.NewTestKitWithInit(c, s.store)
//	tk.MustExec("drop table if exists t1, t2")
//	tk.MustExec("create table t1 (a int,b int,index idx(a))")
//	tk.MustExec("create table t2 (a int,b int,index idx(a))")
//	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3),(2000,2000)")
//	tk.MustExec("insert into t2 values (1,1),(2,2),(3,3),(2000,2000)")
//	// Do not hang in index merge join when OOM occurs.
//	err := tk.QueryToErr("select /*+ INL_MERGE_JOIN(t1, t2) */ * from t1, t2 where t1.a = t2.a")
//	c.Assert(err, NotNil)
//	c.Assert(err.Error(), Equals, "OOM test index merge join doesn't hang here.")
//}

func (s *testSuite9) TestIssue18068(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/testIssue18068", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/testIssue18068"), IsNil)
	}()

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t, s")
	tk.MustExec("create table t (a int, index idx(a))")
	tk.MustExec("create table s (a int, index idx(a))")
	tk.MustExec("insert into t values(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1);")
	tk.MustExec("insert into s values(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1);")
	tk.MustExec("set @@tidb_index_join_batch_size=1")
	tk.MustExec("set @@tidb_max_chunk_size=32")
	tk.MustExec("set @@tidb_init_chunk_size=1")
	tk.MustExec("set @@tidb_index_lookup_join_concurrency=2")

	tk.MustExec("select  /*+ inl_merge_join(s)*/ 1 from t join s on t.a = s.a limit 1")
	// Do not hang in index merge join when the second and third execute.
	tk.MustExec("select  /*+ inl_merge_join(s)*/ 1 from t join s on t.a = s.a limit 1")
	tk.MustExec("select  /*+ inl_merge_join(s)*/ 1 from t join s on t.a = s.a limit 1")
}

// TODO: reopen index merge join in future.

//func (s *testSuite9) TestIssue18631(c *C) {
//	tk := testkit.NewTestKitWithInit(c, s.store)
//	tk.MustExec("drop table if exists t1, t2")
//	tk.MustExec("create table t1(a int, b int, c int, d int, primary key(a,b,c))")
//	tk.MustExec("create table t2(a int, b int, c int, d int, primary key(a,b,c))")
//	tk.MustExec("insert into t1 values(1,1,1,1),(2,2,2,2),(3,3,3,3)")
//	tk.MustExec("insert into t2 values(1,1,1,1),(2,2,2,2)")
//	firstOperator := tk.MustQuery("explain select /*+ inl_merge_join(t1,t2) */ * from t1 left join t2 on t1.a = t2.a and t1.c = t2.c and t1.b = t2.b order by t1.a desc").Rows()[0][0].(string)
//	c.Assert(strings.Index(firstOperator, plancodec.TypeIndexMergeJoin), Equals, 0)
//	tk.MustQuery("select /*+ inl_merge_join(t1,t2) */ * from t1 left join t2 on t1.a = t2.a and t1.c = t2.c and t1.b = t2.b order by t1.a desc").Check(testkit.Rows(
//		"3 3 3 3 <nil> <nil> <nil> <nil>",
//		"2 2 2 2 2 2 2 2",
//		"1 1 1 1 1 1 1 1"))
//}
//
//func (s *testSuite9) TestIssue20137(c *C) {
//	tk := testkit.NewTestKitWithInit(c, s.store)
//	tk.MustExec("drop table if exists t1, t2")
//	tk.MustExec("create table t1 (id bigint(20) unsigned, primary key(id))")
//	tk.MustExec("create table t2 (id bigint(20) unsigned)")
//	tk.MustExec("insert into t1 values (8738875760185212610)")
//	tk.MustExec("insert into t1 values (9814441339970117597)")
//	tk.MustExec("insert into t2 values (8738875760185212610)")
//	tk.MustExec("insert into t2 values (9814441339970117597)")
//	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1, t2) */ * from t2 left join t1 on t1.id = t2.id order by t1.id").Check(
//		testkit.Rows("8738875760185212610 8738875760185212610", "9814441339970117597 9814441339970117597"))
//}

func (s *testSuiteAgg) TestIndexJoinOnSinglePartitionTable(c *C) {
	// For issue 19145
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1  (c_int int, c_str varchar(40), primary key (c_int) ) partition by range (c_int) ( partition p0 values less than (10), partition p1 values less than maxvalue )")
	tk.MustExec("create table t2  (c_int int, c_str varchar(40), primary key (c_int) ) partition by range (c_int) ( partition p0 values less than (10), partition p1 values less than maxvalue )")
	tk.MustExec("insert into t1 values (1, 'Alice')")
	tk.MustExec("insert into t2 values (1, 'Bob')")

	// TODO: reopen index merge join in future.

	//sql := "select /*+ INL_MERGE_JOIN(t1,t2) */ * from t1 join t2 partition(p0) on t1.c_int = t2.c_int and t1.c_str < t2.c_str"
	//tk.MustQuery(sql).Check(testkit.Rows("1 Alice 1 Bob"))
	//rows := s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
	//c.Assert(strings.Index(rows[0], "IndexMergeJoin"), Equals, 0)

	sql := "select /*+ INL_HASH_JOIN(t1,t2) */ * from t1 join t2 partition(p0) on t1.c_int = t2.c_int and t1.c_str < t2.c_str"
	tk.MustQuery(sql).Check(testkit.Rows("1 Alice 1 Bob"))
	rows := s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
	c.Assert(strings.Index(rows[0], "IndexHashJoin"), Equals, 0)

	sql = "select /*+ INL_JOIN(t1,t2) */ * from t1 join t2 partition(p0) on t1.c_int = t2.c_int and t1.c_str < t2.c_str"
	tk.MustQuery(sql).Check(testkit.Rows("1 Alice 1 Bob"))
	rows = s.testData.ConvertRowsToStrings(tk.MustQuery("explain " + sql).Rows())
	c.Assert(strings.Index(rows[0], "IndexJoin"), Equals, 0)
}

func (s *testSuite9) TestIssue20400(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t, s")
	tk.MustExec("create table s(a int, index(a))")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustQuery("select /*+ hash_join(t,s)*/ * from t left join s on t.a=s.a and t.a>1").Check(
		testkit.Rows("1 <nil>"))
	tk.MustQuery("select /*+ inl_merge_join(t,s)*/ * from t left join s on t.a=s.a and t.a>1").Check(
		testkit.Rows("1 <nil>"))
}

func (s *testSuite9) TestIndexMergeJoinBinding(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, key(b))")
	tk.MustExec("create table t2(a int, b int, key(b))")
	tk.MustExec("create session binding for select * from t1 join t2 on t1.b = t2.b using select /*+ inl_merge_join(t1, t2) */ * from t1 join t2 on t1.b = t2.b")
	rows := tk.MustQuery("explain select * from t1 join t2 on t1.b = t2.b").Rows()
	// TODO: reopen index merge join in future
	c.Assert(strings.Index(rows[0][0].(string), "IndexJoin"), Equals, 0)
}
