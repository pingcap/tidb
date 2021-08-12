// Copyright 2021 PingCAP, Inc.
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

package executor_test

import (
	"context"
	"fmt"
	"math/rand"
	"sort"

	"github.com/pingcap/check"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = check.Suite(&CTETestSuite{&baseCTETestSuite{}})
var _ = check.SerialSuites(&CTESerialTestSuite{&baseCTETestSuite{}})

type baseCTETestSuite struct {
	store      kv.Storage
	dom        *domain.Domain
	sessionCtx sessionctx.Context
	session    session.Session
	ctx        context.Context
}

type CTETestSuite struct {
	*baseCTETestSuite
}

type CTESerialTestSuite struct {
	*baseCTETestSuite
}

func (test *baseCTETestSuite) SetUpSuite(c *check.C) {
	var err error
	test.store, err = mockstore.NewMockStore()
	c.Assert(err, check.IsNil)

	test.dom, err = session.BootstrapSession(test.store)
	c.Assert(err, check.IsNil)

	test.sessionCtx = mock.NewContext()

	test.session, err = session.CreateSession4Test(test.store)
	c.Assert(err, check.IsNil)
	test.session.SetConnectionID(0)

	test.ctx = context.Background()
}

func (test *baseCTETestSuite) TearDownSuite(c *check.C) {
	test.dom.Close()
	test.store.Close()
}

func (test *CTETestSuite) TestBasicCTE(c *check.C) {
	tk := testkit.NewTestKit(c, test.store)
	tk.MustExec("use test")

	rows := tk.MustQuery("with recursive cte1 as (" +
		"select 1 c1 " +
		"union all " +
		"select c1 + 1 c1 from cte1 where c1 < 5) " +
		"select * from cte1")
	rows.Check(testkit.Rows("1", "2", "3", "4", "5"))

	// Two seed parts.
	rows = tk.MustQuery("with recursive cte1 as (" +
		"select 1 c1 " +
		"union all " +
		"select 2 c1 " +
		"union all " +
		"select c1 + 1 c1 from cte1 where c1 < 10) " +
		"select * from cte1 order by c1")
	rows.Check(testkit.Rows("1", "2", "2", "3", "3", "4", "4", "5", "5", "6", "6", "7", "7", "8", "8", "9", "9", "10", "10"))

	// Two recursive parts.
	rows = tk.MustQuery("with recursive cte1 as (" +
		"select 1 c1 " +
		"union all " +
		"select 2 c1 " +
		"union all " +
		"select c1 + 1 c1 from cte1 where c1 < 3 " +
		"union all " +
		"select c1 + 2 c1 from cte1 where c1 < 5) " +
		"select * from cte1 order by c1")
	rows.Check(testkit.Rows("1", "2", "2", "3", "3", "3", "4", "4", "5", "5", "5", "6", "6"))

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(a int);")
	tk.MustExec("insert into t1 values(1);")
	tk.MustExec("insert into t1 values(2);")
	rows = tk.MustQuery("SELECT * FROM t1 dt WHERE EXISTS(WITH RECURSIVE qn AS (SELECT a*0 AS b UNION ALL SELECT b+1 FROM qn WHERE b=0) SELECT * FROM qn WHERE b=a);")
	rows.Check(testkit.Rows("1"))
	rows = tk.MustQuery("SELECT * FROM t1 dt WHERE EXISTS( WITH RECURSIVE qn AS (SELECT a*0 AS b UNION ALL SELECT b+1 FROM qn WHERE b=0 or b = 1) SELECT * FROM qn WHERE b=a );")
	rows.Check(testkit.Rows("1", "2"))

	rows = tk.MustQuery("with recursive  c(p) as (select 1), cte(a, b) as (select 1, 1 union select a+1, 1 from cte, c where a < 5)  select * from cte order by 1, 2;")
	rows.Check(testkit.Rows("1 1", "2 1", "3 1", "4 1", "5 1"))
}

func (test *CTESerialTestSuite) TestSpillToDisk(c *check.C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMUseTmpStorage = true
	})

	tk := testkit.NewTestKit(c, test.store)
	tk.MustExec("use test;")

	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/testCTEStorageSpill", "return(true)"), check.IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/testCTEStorageSpill"), check.IsNil)
		tk.MustExec("set tidb_mem_quota_query = 1073741824;")
	}()
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill", "return(true)"), check.IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill"), check.IsNil)
	}()

	// Use duplicated rows to test UNION DISTINCT.
	tk.MustExec("set tidb_mem_quota_query = 1073741824;")
	insertStr := "insert into t1 values(0)"
	rowNum := 1000
	vals := make([]int, rowNum)
	vals[0] = 0
	for i := 1; i < rowNum; i++ {
		v := rand.Intn(100)
		vals[i] = v
		insertStr += fmt.Sprintf(", (%d)", v)
	}
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec(insertStr)
	tk.MustExec("set tidb_mem_quota_query = 40000;")
	tk.MustExec("set cte_max_recursion_depth = 500000;")
	sql := fmt.Sprintf("with recursive cte1 as ( "+
		"select c1 from t1 "+
		"union "+
		"select c1 + 1 c1 from cte1 where c1 < %d) "+
		"select c1 from cte1 order by c1;", rowNum)
	rows := tk.MustQuery(sql)

	memTracker := tk.Se.GetSessionVars().StmtCtx.MemTracker
	diskTracker := tk.Se.GetSessionVars().StmtCtx.DiskTracker
	c.Assert(memTracker.MaxConsumed(), check.Greater, int64(0))
	c.Assert(diskTracker.MaxConsumed(), check.Greater, int64(0))

	sort.Ints(vals)
	resRows := make([]string, 0, rowNum)
	for i := vals[0]; i <= rowNum; i++ {
		resRows = append(resRows, fmt.Sprintf("%d", i))
	}
	rows.Check(testkit.Rows(resRows...))
}

func (test *CTETestSuite) TestUnionDistinct(c *check.C) {
	tk := testkit.NewTestKit(c, test.store)
	tk.MustExec("use test;")

	// Basic test. UNION/UNION ALL intersects.
	rows := tk.MustQuery("with recursive cte1(c1) as (select 1 union select 1 union select 1 union all select c1 + 1 from cte1 where c1 < 3) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2", "3"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union all select 1 union select 1 union all select c1 + 1 from cte1 where c1 < 3) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2", "3"))

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int, c2 int);")
	tk.MustExec("insert into t1 values(1, 1), (1, 2), (2, 2);")
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 c1 from t1) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2", "3"))

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec("insert into t1 values(1), (1), (1), (2), (2), (2);")
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 c1 from cte1 where c1 < 4) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2", "3", "4"))
}

func (test *CTETestSuite) TestCTEMaxRecursionDepth(c *check.C) {
	tk := testkit.NewTestKit(c, test.store)
	tk.MustExec("use test;")

	tk.MustExec("set @@cte_max_recursion_depth = -1;")
	err := tk.QueryToErr("with recursive cte1(c1) as (select 1 union select c1 + 1 c1 from cte1 where c1 < 100) select * from cte1;")
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Equals, "[executor:3636]Recursive query aborted after 1 iterations. Try increasing @@cte_max_recursion_depth to a larger value")
	// If there is no recursive part, query runs ok.
	rows := tk.MustQuery("with recursive cte1(c1) as (select 1 union select 2) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2"))
	rows = tk.MustQuery("with cte1(c1) as (select 1 union select 2) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2"))

	tk.MustExec("set @@cte_max_recursion_depth = 0;")
	err = tk.QueryToErr("with recursive cte1(c1) as (select 1 union select c1 + 1 c1 from cte1 where c1 < 0) select * from cte1;")
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Equals, "[executor:3636]Recursive query aborted after 1 iterations. Try increasing @@cte_max_recursion_depth to a larger value")
	err = tk.QueryToErr("with recursive cte1(c1) as (select 1 union select c1 + 1 c1 from cte1 where c1 < 1) select * from cte1;")
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Equals, "[executor:3636]Recursive query aborted after 1 iterations. Try increasing @@cte_max_recursion_depth to a larger value")
	// If there is no recursive part, query runs ok.
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select 2) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2"))
	rows = tk.MustQuery("with cte1(c1) as (select 1 union select 2) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2"))

	tk.MustExec("set @@cte_max_recursion_depth = 1;")
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 c1 from cte1 where c1 < 0) select * from cte1;")
	rows.Check(testkit.Rows("1"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 c1 from cte1 where c1 < 1) select * from cte1;")
	rows.Check(testkit.Rows("1"))
	err = tk.QueryToErr("with recursive cte1(c1) as (select 1 union select c1 + 1 c1 from cte1 where c1 < 2) select * from cte1;")
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Equals, "[executor:3636]Recursive query aborted after 2 iterations. Try increasing @@cte_max_recursion_depth to a larger value")
	// If there is no recursive part, query runs ok.
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select 2) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2"))
	rows = tk.MustQuery("with cte1(c1) as (select 1 union select 2) select * from cte1 order by c1;")
	rows.Check(testkit.Rows("1", "2"))
}

func (test *CTETestSuite) TestCTEWithLimit(c *check.C) {
	tk := testkit.NewTestKit(c, test.store)
	tk.MustExec("use test;")

	// Basic recursive tests.
	rows := tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 5 offset 0) select * from cte1")
	rows.Check(testkit.Rows("1", "2", "3", "4", "5"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 5 offset 1) select * from cte1")
	rows.Check(testkit.Rows("2", "3", "4", "5", "6"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 5 offset 10) select * from cte1")
	rows.Check(testkit.Rows("11", "12", "13", "14", "15"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 5 offset 995) select * from cte1")
	rows.Check(testkit.Rows("996", "997", "998", "999", "1000"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 5 offset 6) select * from cte1;")
	rows.Check(testkit.Rows("7", "8", "9", "10", "11"))

	// Test with cte_max_recursion_depth
	tk.MustExec("set cte_max_recursion_depth=2;")
	rows = tk.MustQuery("with recursive cte1(c1) as (select 0 union select c1 + 1 from cte1 limit 1 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("2"))

	err := tk.QueryToErr("with recursive cte1(c1) as (select 0 union select c1 + 1 from cte1 limit 1 offset 3) select * from cte1;")
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Equals, "[executor:3636]Recursive query aborted after 3 iterations. Try increasing @@cte_max_recursion_depth to a larger value")

	tk.MustExec("set cte_max_recursion_depth=1000;")
	rows = tk.MustQuery("with recursive cte1(c1) as (select 0 union select c1 + 1 from cte1 limit 5 offset 996) select * from cte1;")
	rows.Check(testkit.Rows("996", "997", "998", "999", "1000"))

	err = tk.QueryToErr("with recursive cte1(c1) as (select 0 union select c1 + 1 from cte1 limit 5 offset 997) select * from cte1;")
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Equals, "[executor:3636]Recursive query aborted after 1001 iterations. Try increasing @@cte_max_recursion_depth to a larger value")

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 0 offset 1) select * from cte1")
	rows.Check(testkit.Rows())

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 0 offset 10) select * from cte1")
	rows.Check(testkit.Rows())

	// Test join.
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 2 offset 1) select * from cte1 dt1 join cte1 dt2 order by dt1.c1, dt2.c1;")
	rows.Check(testkit.Rows("2 2", "2 3", "3 2", "3 3"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 2 offset 1) select * from cte1 dt1 join cte1 dt2 on dt1.c1 = dt2.c1 order by dt1.c1, dt1.c1;")
	rows.Check(testkit.Rows("2 2", "3 3"))

	// Test subquery.
	// Different with mysql, maybe it's mysql bug?(https://bugs.mysql.com/bug.php?id=103890&thanks=4)
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 2 offset 1) select c1 from cte1 where c1 in (select 2);")
	rows.Check(testkit.Rows("2"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 2 offset 1) select c1 from cte1 dt where c1 in (select c1 from cte1 where 1 = dt.c1 - 1);")
	rows.Check(testkit.Rows("2"))

	// Test Apply.
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select c1 + 1 from cte1 limit 2 offset 1) select c1 from cte1 where cte1.c1 = (select dt1.c1 from cte1 dt1 where dt1.c1 = cte1.c1);")
	rows.Check(testkit.Rows("2", "3"))

	// Recursive tests with table.
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec("insert into t1 values(1), (2), (3);")

	// Error: ERROR 1221 (HY000): Incorrect usage of UNION and LIMIT.
	// Limit can only be at the end of SQL stmt.
	err = tk.ExecToErr("with recursive cte1(c1) as (select c1 from t1 limit 1 offset 1 union select c1 + 1 from cte1 limit 0 offset 1) select * from cte1")
	c.Assert(err.Error(), check.Equals, "[planner:1221]Incorrect usage of UNION and LIMIT")

	// Basic non-recusive tests.
	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select 2 order by 1 limit 1 offset 1) select * from cte1")
	rows.Check(testkit.Rows("2"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select 2 order by 1 limit 0 offset 1) select * from cte1")
	rows.Check(testkit.Rows())

	rows = tk.MustQuery("with recursive cte1(c1) as (select 1 union select 2 order by 1 limit 2 offset 0) select * from cte1")
	rows.Check(testkit.Rows("1", "2"))

	// Test with table.
	tk.MustExec("drop table if exists t1;")
	insertStr := "insert into t1 values(0)"
	for i := 1; i < 300; i++ {
		insertStr += fmt.Sprintf(", (%d)", i)
	}

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec(insertStr)

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 c1 from cte1 limit 1) select * from cte1")
	rows.Check(testkit.Rows("0"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 c1 from cte1 limit 1 offset 100) select * from cte1")
	rows.Check(testkit.Rows("100"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 c1 from cte1 limit 5 offset 100) select * from cte1")
	rows.Check(testkit.Rows("100", "101", "102", "103", "104"))

	// Basic non-recursive tests.
	rows = tk.MustQuery("with cte1 as (select c1 from t1 limit 2 offset 1) select * from cte1")
	rows.Check(testkit.Rows("1", "2"))

	rows = tk.MustQuery("with cte1 as (select c1 from t1 limit 2 offset 1) select * from cte1 dt1 join cte1 dt2 on dt1.c1 = dt2.c1")
	rows.Check(testkit.Rows("1 1", "2 2"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select 2 limit 0 offset 1) select * from cte1")
	rows.Check(testkit.Rows())

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select 2 limit 0 offset 1) select * from cte1 dt1 join cte1 dt2 on dt1.c1 = dt2.c1")
	rows.Check(testkit.Rows())

	// rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select 2 limit 5 offset 100) select * from cte1")
	// rows.Check(testkit.Rows("100", "101", "102", "103", "104"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 limit 3 offset 100) select * from cte1")
	rows.Check(testkit.Rows("100", "101", "102"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 limit 3 offset 100) select * from cte1 dt1 join cte1 dt2 on dt1.c1 = dt2.c1")
	rows.Check(testkit.Rows("100 100", "101 101", "102 102"))

	// Test limit 0.
	tk.MustExec("set cte_max_recursion_depth = 0;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec("insert into t1 values(0);")
	rows = tk.MustQuery("with recursive cte1 as (select 1/c1 c1 from t1 union select c1 + 1 c1 from cte1 where c1 < 2 limit 0) select * from cte1;")
	rows.Check(testkit.Rows())
	// MySQL err: ERROR 1365 (22012): Division by 0. Because it gives error when computing 1/c1.
	err = tk.QueryToErr("with recursive cte1 as (select 1/c1 c1 from t1 union select c1 + 1 c1 from cte1 where c1 < 2 limit 1) select * from cte1;")
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Equals, "[executor:3636]Recursive query aborted after 1 iterations. Try increasing @@cte_max_recursion_depth to a larger value")

	tk.MustExec("set cte_max_recursion_depth = 1000;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int);")
	tk.MustExec("insert into t1 values(1), (2), (3);")

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 0 offset 2) select * from cte1;")
	rows.Check(testkit.Rows())
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 1 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 2 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3", "4"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 3 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3", "4", "5"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 4 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3", "4", "5", "6"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 0 offset 3) select * from cte1;")
	rows.Check(testkit.Rows())
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 1 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("4"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 2 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("4", "5"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 3 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("4", "5", "6"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 4 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("4", "5", "6", "7"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 0 offset 4) select * from cte1;")
	rows.Check(testkit.Rows())
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 1 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("5"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 2 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("5", "6"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 3 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("5", "6", "7"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union select c1 + 1 from cte1 limit 4 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("5", "6", "7", "8"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 0 offset 2) select * from cte1;")
	rows.Check(testkit.Rows())
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 1 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 2 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3", "2"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 3 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3", "2", "3"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 4 offset 2) select * from cte1;")
	rows.Check(testkit.Rows("3", "2", "3", "4"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 0 offset 3) select * from cte1;")
	rows.Check(testkit.Rows())
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 1 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("2"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 2 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("2", "3"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 3 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("2", "3", "4"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 4 offset 3) select * from cte1;")
	rows.Check(testkit.Rows("2", "3", "4", "3"))

	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 0 offset 4) select * from cte1;")
	rows.Check(testkit.Rows())
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 1 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("3"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 2 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("3", "4"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 3 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("3", "4", "3"))
	rows = tk.MustQuery("with recursive cte1(c1) as (select c1 from t1 union all select c1 + 1 from cte1 limit 4 offset 4) select * from cte1;")
	rows.Check(testkit.Rows("3", "4", "3", "4"))
}
