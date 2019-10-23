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
	"github.com/pingcap/tidb/util/testkit"
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
	explainedSQL := "explain " + sql
	result := tk.MustQuery(explainedSQL)
	resultStr := fmt.Sprintf("%v", result.Rows())
	if !strings.ContainsAny(resultStr, "MergeJoin") {
		c.Error("Expected MergeJoin in plan.")
	}
	return tk.MustQuery(sql)
}

func checkPlanAndRun(tk *testkit.TestKit, c *C, plan string, sql string) *testkit.Result {
	explainedSQL := "explain " + sql
	tk.MustQuery(explainedSQL)

	// TODO: Reopen it after refactoring explain.
	// resultStr := fmt.Sprintf("%v", result.Rows())
	// if plan != resultStr {
	//     c.Errorf("Plan not match. Obtained:\n %s\nExpected:\n %s\n", resultStr, plan)
	// }
	return tk.MustQuery(sql)
}

func (s *testSuite1) TestMergeJoin(c *C) {
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
	// Test LogicalSelection under LogicalJoin.
	result = tk.MustQuery("select /*+ TIDB_SMJ(a, b) */ a.c1 from t a , (select * from t1 limit 3) b where a.c1 = b.c1 and b.c1 is not null order by b.c1;")
	result.Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("begin;")
	// Test LogicalLock under LogicalJoin.
	result = tk.MustQuery("select /*+ TIDB_SMJ(a, b) */ a.c1 from t a , (select * from t1 for update) b where a.c1 = b.c1 order by a.c1;")
	result.Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7"))
	// Test LogicalUnionScan under LogicalJoin.
	tk.MustExec("insert into t1 values(8);")
	result = tk.MustQuery("select /*+ TIDB_SMJ(a, b) */ a.c1 from t a , t1 b where a.c1 = b.c1;")
	result.Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7"))
	tk.MustExec("rollback;")

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int)")
	tk.MustExec("create table t1(c1 int unsigned)")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t1 values (1)")
	result = tk.MustQuery("select /*+ TIDB_SMJ(t,t1) */ t.c1 from t , t1 where t.c1 = t1.c1")
	result.Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index a(a), index b(b))")
	tk.MustExec("insert into t values(1, 2)")
	tk.MustQuery("select /*+ TIDB_SMJ(t, t1) */ t.a, t1.b from t right join t t1 on t.a = t1.b order by t.a").Check(testkit.Rows("<nil> 2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists s")
	tk.MustExec("create table t(a int, b int, primary key(a, b))")
	tk.MustExec("insert into t value(1,1),(1,2),(1,3),(1,4)")
	tk.MustExec("create table s(a int, primary key(a))")
	tk.MustExec("insert into s value(1)")
	tk.MustQuery("select /*+ TIDB_SMJ(t, s) */ count(*) from t join s on t.a = s.a").Check(testkit.Rows("4"))

	// Test TIDB_SMJ for cartesian product.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t value(1),(2)")
	tk.MustQuery("explain select /*+ TIDB_SMJ(t1, t2) */ * from t t1 join t t2 order by t1.a, t2.a").Check(testkit.Rows(
		"Sort_6 100000000.00 root Column#5:asc, Column#6:asc",
		"└─MergeJoin_9 100000000.00 root inner join",
		"  ├─TableReader_11 10000.00 root data:TableScan_10",
		"  │ └─TableScan_10 10000.00 cop[tikv] table:t1, range:[-inf,+inf], keep order:false, stats:pseudo",
		"  └─TableReader_13 10000.00 root data:TableScan_12",
		"    └─TableScan_12 10000.00 cop[tikv] table:t2, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ * from t t1 join t t2 order by t1.a, t2.a").Check(testkit.Rows(
		"1 1",
		"1 2",
		"2 1",
		"2 2",
	))

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists s")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,1),(1,2)")
	tk.MustExec("create table s(a int, b int)")
	tk.MustExec("insert into s values(1,1)")
	tk.MustQuery("explain select /*+ TIDB_SMJ(t, s) */ a in (select a from s where s.b >= t.b) from t").Check(testkit.Rows(
		"Projection_7 10000.00 root Column#8",
		"└─MergeJoin_8 10000.00 root left outer semi join, other cond:eq(Column#1, Column#4), ge(Column#5, Column#2)",
		"  ├─TableReader_10 10000.00 root data:TableScan_9",
		"  │ └─TableScan_9 10000.00 cop[tikv] table:t, range:[-inf,+inf], keep order:false, stats:pseudo",
		"  └─TableReader_12 10000.00 root data:TableScan_11",
		"    └─TableScan_11 10000.00 cop[tikv] table:s, range:[-inf,+inf], keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ TIDB_SMJ(t, s) */ a in (select a from s where s.b >= t.b) from t").Check(testkit.Rows(
		"1",
		"0",
	))
}

func (s *testSuite1) Test3WaysMergeJoin(c *C) {
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

func (s *testSuite1) TestMergeJoinDifferentTypes(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`drop table if exists t2;`)
	tk.MustExec(`create table t1(a bigint, b bit(1), index idx_a(a));`)
	tk.MustExec(`create table t2(a bit(1) not null, b bit(1), index idx_a(a));`)
	tk.MustExec(`insert into t1 values(1, 1);`)
	tk.MustExec(`insert into t2 values(1, 1);`)
	tk.MustQuery(`select hex(t1.a), hex(t2.a) from t1 inner join t2 on t1.a=t2.a;`).Check(testkit.Rows(`1 1`))

	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`drop table if exists t2;`)
	tk.MustExec(`create table t1(a float, b double, index idx_a(a));`)
	tk.MustExec(`create table t2(a double not null, b double, index idx_a(a));`)
	tk.MustExec(`insert into t1 values(1, 1);`)
	tk.MustExec(`insert into t2 values(1, 1);`)
	tk.MustQuery(`select t1.a, t2.a from t1 inner join t2 on t1.a=t2.a;`).Check(testkit.Rows(`1 1`))

	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`drop table if exists t2;`)
	tk.MustExec(`create table t1(a bigint signed, b bigint, index idx_a(a));`)
	tk.MustExec(`create table t2(a bigint unsigned, b bigint, index idx_a(a));`)
	tk.MustExec(`insert into t1 values(-1, 0), (-1, 0), (0, 0), (0, 0), (pow(2, 63), 0), (pow(2, 63), 0);`)
	tk.MustExec(`insert into t2 values(18446744073709551615, 0), (18446744073709551615, 0), (0, 0), (0, 0), (pow(2, 63), 0), (pow(2, 63), 0);`)
	tk.MustQuery(`select t1.a, t2.a from t1 join t2 on t1.a=t2.a order by t1.a;`).Check(testkit.Rows(
		`0 0`,
		`0 0`,
		`0 0`,
		`0 0`,
	))
}
