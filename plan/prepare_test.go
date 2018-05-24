package plan_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testPrepareSuite{})

type testPrepareSuite struct {
}

func (s *testPrepareSuite) TestPrepareCache(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	orgEnable := plan.PreparedPlanCacheEnabled
	orgCapacity := plan.PreparedPlanCacheCapacity
	defer func() {
		dom.Close()
		store.Close()
		plan.PreparedPlanCacheEnabled = orgEnable
		plan.PreparedPlanCacheCapacity = orgCapacity
	}()
	plan.PreparedPlanCacheEnabled = true
	plan.PreparedPlanCacheCapacity = 100
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int, index idx1(a, b), index idx2(b))")
	tk.MustExec("insert into t values(1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 5, 5)")
	tk.MustExec(`prepare stmt1 from "select * from t use index(idx1) where a = ? and b = ?"`)
	tk.MustExec(`prepare stmt2 from "select a, b from t use index(idx2) where b = ?"`)
	tk.MustExec(`prepare stmt3 from "select * from t where a = ?"`)
	tk.MustExec("set @a=1, @b=1")
	// When executing one statement at the first time, we don't use cache, so we need to execute it at least twice to test the cache.
	tk.MustQuery("execute stmt1 using @a, @b").Check(testkit.Rows("1 1 1"))
	tk.MustQuery("execute stmt1 using @a, @b").Check(testkit.Rows("1 1 1"))
	tk.MustQuery("execute stmt2 using @b").Check(testkit.Rows("1 1"))
	tk.MustQuery("execute stmt2 using @b").Check(testkit.Rows("1 1"))
	tk.MustQuery("execute stmt3 using @a").Check(testkit.Rows("1 1 1"))
	tk.MustQuery("execute stmt3 using @a").Check(testkit.Rows("1 1 1"))
	tk.MustExec(`prepare stmt4 from "select * from t where a > ?"`)
	tk.MustExec("set @a=3")
	tk.MustQuery("execute stmt4 using @a").Check(testkit.Rows("4 4 4", "5 5 5"))
	tk.MustQuery("execute stmt4 using @a").Check(testkit.Rows("4 4 4", "5 5 5"))
	tk.MustExec(`prepare stmt5 from "select c from t order by c"`)
	tk.MustQuery("execute stmt5").Check(testkit.Rows("1", "2", "3", "4", "5"))
}
