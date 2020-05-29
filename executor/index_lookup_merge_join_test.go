package executor_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
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
