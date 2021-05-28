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

	"github.com/pingcap/check"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"

	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = check.Suite(&CTETestSuite{})

type CTETestSuite struct {
	store      kv.Storage
	dom        *domain.Domain
	sessionCtx sessionctx.Context
	session    session.Session
	ctx        context.Context
}

func (test *CTETestSuite) SetUpSuite(c *check.C) {
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

func (test *CTETestSuite) TearDownSuite(c *check.C) {
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

	// two seed part
	rows = tk.MustQuery("with recursive cte1 as (" +
		"select 1 c1 " +
		"union all " +
		"select 2 c1 " +
		"union all " +
		"select c1 + 1 c1 from cte1 where c1 < 10) " +
		"select * from cte1 order by c1")
	rows.Check(testkit.Rows("1", "2", "2", "3", "3", "4", "4", "5", "5", "6", "6", "7", "7", "8", "8", "9", "9", "10", "10"))

	// two recursive part
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
}

func (test *CTETestSuite) TestSpillToDisk(c *check.C) {
	tk := testkit.NewTestKit(c, test.store)
	tk.MustExec("use test;")

	insertStr := "insert into t1 values(0, 0)"
	for i := 1; i < 5000; i++ {
		insertStr += fmt.Sprintf(", (%d, %d)", i, i)
	}

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int, c2 int);")
	tk.MustExec(insertStr)
	tk.MustExec("set tidb_mem_quota_query = 50000;")
	rows := tk.MustQuery("with recursive cte1 as ( " +
		"select c1 from t1 " +
		"union " +
		"select c1 + 1 c1 from cte1 where c1 < 5000) " +
		"select c1 from cte1;")

	var resRows []string
	for i := 0; i <= 5000; i++ {
		resRows = append(resRows, fmt.Sprintf("%d", i))
	}
	rows.Check(testkit.Rows(resRows...))
	memTracker := tk.Se.GetSessionVars().StmtCtx.MemTracker
	diskTracker := tk.Se.GetSessionVars().StmtCtx.DiskTracker
	c.Assert(memTracker.BytesConsumed(), check.Equals, int64(0))
	c.Assert(memTracker.MaxConsumed(), check.Greater, int64(0))
	c.Assert(diskTracker.BytesConsumed(), check.Equals, int64(0))
	c.Assert(diskTracker.MaxConsumed(), check.Greater, int64(0))
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
