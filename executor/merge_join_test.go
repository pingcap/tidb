// Copyright 2017 PingCAP, Inc.
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
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

const plan1 = `[[TableScan_12 {
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
} MergeJoin_17] [TableScan_15 {
    "db": "test",
    "table": "t2",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
} MergeJoin_17] [MergeJoin_17 {
    "eqCond": [
        "eq(test.t1.c1, test.t2.c1)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": [],
    "leftPlan": "TableScan_12",
    "rightPlan": "TableScan_15",
    "desc": "false"
} MergeJoin_8] [TableScan_22 {
    "db": "test",
    "table": "t3",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
} MergeJoin_8] [MergeJoin_8 {
    "eqCond": [
        "eq(test.t2.c1, test.t3.c1)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": [],
    "leftPlan": "MergeJoin_17",
    "rightPlan": "TableScan_22",
    "desc": "false"
} Sort_23] [Sort_23 {
    "exprs": [
        {
            "Expr": "test.t1.c1",
            "Desc": false
        }
    ],
    "limit": null,
    "child": "MergeJoin_8"
} ]]`

const plan2 = `[[TableScan_12 {
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
} MergeJoin_17] [TableScan_15 {
    "db": "test",
    "table": "t2",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
} MergeJoin_17] [MergeJoin_17 {
    "eqCond": [
        "eq(test.t1.c1, test.t2.c1)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": [],
    "leftPlan": "TableScan_12",
    "rightPlan": "TableScan_15",
    "desc": "false"
} MergeJoin_8] [TableScan_22 {
    "db": "test",
    "table": "t3",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
} MergeJoin_8] [MergeJoin_8 {
    "eqCond": [
        "eq(test.t2.c1, test.t3.c1)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": [],
    "leftPlan": "MergeJoin_17",
    "rightPlan": "TableScan_22",
    "desc": "false"
} Sort_23] [Sort_23 {
    "exprs": [
        {
            "Expr": "test.t1.c1",
            "Desc": false
        }
    ],
    "limit": null,
    "child": "MergeJoin_8"
} ]]`

const plan3 = `[[TableScan_12 {
    "db": "test",
    "table": "t1",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
} MergeJoin_9] [TableScan_15 {
    "db": "test",
    "table": "t2",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
} MergeJoin_9] [MergeJoin_9 {
    "eqCond": [
        "eq(test.t1.c1, test.t2.c1)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": [],
    "leftPlan": "TableScan_12",
    "rightPlan": "TableScan_15",
    "desc": "false"
} Sort_16] [Sort_16 {
    "exprs": [
        {
            "Expr": "test.t1.c1",
            "Desc": false
        }
    ],
    "limit": null,
    "child": "MergeJoin_9"
} MergeJoin_8] [TableScan_23 {
    "db": "test",
    "table": "t3",
    "desc": false,
    "keep order": true,
    "push down info": {
        "limit": 0,
        "access conditions": null,
        "index filter conditions": null,
        "table filter conditions": null
    }
} MergeJoin_8] [MergeJoin_8 {
    "eqCond": [
        "eq(test.t1.c1, test.t3.c1)"
    ],
    "leftCond": null,
    "rightCond": null,
    "otherCond": [],
    "leftPlan": "Sort_16",
    "rightPlan": "TableScan_23",
    "desc": "false"
} ]]`

func checkMergeAndRun(tk *testkit.TestKit, c *C, sql string) *testkit.Result {
	explainedSql := "explain " + sql
	result := tk.MustQuery(explainedSql)
	resultStr := fmt.Sprintf("%v", result.Rows())
	if !strings.ContainsAny(resultStr, "MergeJoin") {
		c.Error("Expected MergeJoin in plan.")
	}
	return tk.MustQuery(sql)
}

func checkPlanAndRun(tk *testkit.TestKit, c *C, plan string, sql string) *testkit.Result {
	explainedSql := "explain " + sql
	result := tk.MustQuery(explainedSql)
	resultStr := fmt.Sprintf("%v", result.Rows())
	if plan != resultStr {
		c.Errorf("Plan not match. Obtained:\n %s\nExpected:\n %s\n", resultStr, plan)
	}
	return tk.MustQuery(sql)
}

func (s *testSuite) TestMergeJoin(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int, c2 int)")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tk.MustExec("insert into t values(1,1),(2,2)")
	tk.MustExec("insert into t1 values(2,3),(4,4)")

	result := checkMergeAndRun(tk, c, "select /*+ TIDB_SMJ(t) */ * from t left outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows("1 1 <nil> <nil>"))
	result = checkMergeAndRun(tk, c, "select /*+ TIDB_SMJ(t) */ * from t1 right outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows("<nil> <nil> 1 1"))
	result = checkMergeAndRun(tk, c, "select /*+ TIDB_SMJ(t) */ * from t right outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows())
	result = checkMergeAndRun(tk, c, "select /*+ TIDB_SMJ(t) */ * from t left outer join t1 on t.c1 = t1.c1 where t1.c1 = 3 or false")
	result.Check(testkit.Rows())
	result = checkMergeAndRun(tk, c, "select /*+ TIDB_SMJ(t) */ * from t left outer join t1 on t.c1 = t1.c1 and t.c1 != 1 order by t1.c1")
	result.Check(testkit.Rows("1 1 <nil> <nil>", "2 2 2 3"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop table if exists t3")

	tk.MustExec("create table t1 (c1 int, c2 int)")
	tk.MustExec("create table t2 (c1 int, c2 int)")
	tk.MustExec("create table t3 (c1 int, c2 int)")

	tk.MustExec("insert into t1 values (1,1), (2,2), (3,3)")
	tk.MustExec("insert into t2 values (1,1), (3,3), (5,5)")
	tk.MustExec("insert into t3 values (1,1), (5,5), (9,9)")

	result = tk.MustQuery("select /*+ TIDB_SMJ(t1,t2,t3) */ * from t1 left join t2 on t1.c1 = t2.c1 right join t3 on t2.c1 = t3.c1 order by t1.c1, t1.c2, t2.c1, t2.c2, t3.c1, t3.c2;")
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> 5 5", "<nil> <nil> <nil> <nil> 9 9", "1 1 1 1 1 1"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int)")
	tk.MustExec("insert into t1 values (1), (1), (1)")
	result = tk.MustQuery("select/*+ TIDB_SMJ(t) */  * from t1 a join t1 b on a.c1 = b.c1;")
	result.Check(testkit.Rows("1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int, index k(c1))")
	tk.MustExec("create table t1(c1 int)")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5),(6),(7)")
	tk.MustExec("insert into t1 values (1),(2),(3),(4),(5),(6),(7)")
	result = tk.MustQuery("select /*+ TIDB_SMJ(a,b) */ a.c1 from t a , t1 b where a.c1 = b.c1 order by a.c1;")
	result.Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7"))
	result = tk.MustQuery("select /*+ TIDB_SMJ(a, b) */ a.c1 from t a , (select * from t1 limit 3) b where a.c1 = b.c1 order by b.c1;")
	result.Check(testkit.Rows("1", "2", "3"))

	plan.AllowCartesianProduct = false
	_, err := tk.Exec("select /*+ TIDB_SMJ(t,t1) */ * from t, t1")
	c.Check(plan.ErrCartesianProductUnsupported.Equal(err), IsTrue)
	_, err = tk.Exec("select /*+ TIDB_SMJ(t,t1) */ * from t left join t1 on 1")
	c.Check(plan.ErrCartesianProductUnsupported.Equal(err), IsTrue)
	_, err = tk.Exec("select /*+ TIDB_SMJ(t,t1) */ * from t right join t1 on 1")
	c.Check(plan.ErrCartesianProductUnsupported.Equal(err), IsTrue)
	plan.AllowCartesianProduct = true
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int)")
	tk.MustExec("create table t1(c1 int unsigned)")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t1 values (1)")
	result = tk.MustQuery("select /*+ TIDB_SMJ(t,t1) */ t.c1 from t , t1 where t.c1 = t1.c1")
	result.Check(testkit.Rows("1"))
}

func (s *testSuite) Test3WaysMergeJoin(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t1(c1 int, c2 int, PRIMARY KEY (c1))")
	tk.MustExec("create table t2(c1 int, c2 int, PRIMARY KEY (c1))")
	tk.MustExec("create table t3(c1 int, c2 int, PRIMARY KEY (c1))")
	tk.MustExec("insert into t1 values(1,1),(2,2),(3,3)")
	tk.MustExec("insert into t2 values(2,3),(3,4),(4,5)")
	tk.MustExec("insert into t3 values(1,2),(2,4),(3,10)")
	result := checkPlanAndRun(tk, c, plan1, "select /*+ TIDB_SMJ(t1,t2,t3) */ * from t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1 order by 1")
	result.Check(testkit.Rows("2 2 2 3 2 4", "3 3 3 4 3 10"))

	result = checkPlanAndRun(tk, c, plan2, "select /*+ TIDB_SMJ(t1,t2,t3) */ * from t1 right outer join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1 order by 1")
	result.Check(testkit.Rows("2 2 2 3 2 4", "3 3 3 4 3 10"))

	// In below case, t1 side filled with null when no matched join, so that order is not kept and sort appended
	// On the other hand, t1 order kept so no final sort appended
	result = checkPlanAndRun(tk, c, plan3, "select /*+ TIDB_SMJ(t1,t2,t3) */ * from t1 right outer join t2 on t1.c1 = t2.c1 join t3 on t1.c1 = t3.c1 order by 1")
	result.Check(testkit.Rows("2 2 2 3 2 4", "3 3 3 4 3 10"))
}
