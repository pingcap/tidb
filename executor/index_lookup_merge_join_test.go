package executor_test

import (
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite9) TestIndexLookupMergeJoinHang(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/IndexMergeJoinMockOOM", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/IndexMergeJoinMockOOM"), IsNil)
	}()

	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int,b int,index idx(a))")
	tk.MustExec("create table t2 (a int,b int,index idx(a))")
	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3),(2000,2000)")
	tk.MustExec("insert into t2 values (1,1),(2,2),(3,3),(2000,2000)")
	// Do not hang in index merge join when OOM occurs.
	err := tk.QueryToErr("select /*+ INL_MERGE_JOIN(t1, t2) */ * from t1, t2 where t1.a = t2.a")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "OOM test index merge join doesn't hang here.")
}

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

func (s *testSuite9) TestIssue18631(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int, d int, primary key(a,b,c))")
	tk.MustExec("create table t2(a int, b int, c int, d int, primary key(a,b,c))")
	tk.MustExec("insert into t1 values(1,1,1,1),(2,2,2,2),(3,3,3,3)")
	tk.MustExec("insert into t2 values(1,1,1,1),(2,2,2,2)")
	firstOperator := tk.MustQuery("explain select /*+ inl_merge_join(t1,t2) */ * from t1 left join t2 on t1.a = t2.a and t1.c = t2.c and t1.b = t2.b order by t1.a desc").Rows()[0][0].(string)
	c.Assert(strings.Index(firstOperator, plancodec.TypeIndexMergeJoin), Equals, 0)
	tk.MustQuery("select /*+ inl_merge_join(t1,t2) */ * from t1 left join t2 on t1.a = t2.a and t1.c = t2.c and t1.b = t2.b order by t1.a desc").Check(testkit.Rows(
		"3 3 3 3 <nil> <nil> <nil> <nil>",
		"2 2 2 2 2 2 2 2",
		"1 1 1 1 1 1 1 1"))
}
