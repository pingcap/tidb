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
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"sync"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util"
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

func (s *testSuite2) TestMergeJoinInDisk(c *C) {
	originCfg := config.GetGlobalConfig()
	newConf := *originCfg
	newConf.OOMUseTmpStorage = true
	config.StoreGlobalConfig(&newConf)
	defer config.StoreGlobalConfig(originCfg)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	sm := &mockSessionManager1{
		PS: make([]*util.ProcessInfo, 0),
	}
	tk.Se.SetSessionManager(sm)
	s.domain.ExpensiveQueryHandle().SetSessionManager(sm)

	tk.MustExec("set @@tidb_mem_quota_query=1;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int, c2 int)")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tk.MustExec("insert into t values(1,1)")
	tk.MustExec("insert into t1 values(1,3),(4,4)")

	result := checkMergeAndRun(tk, c, "select /*+ TIDB_SMJ(t) */ * from t1 left outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows("1 3 1 1"))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MemTracker.BytesConsumed(), Equals, int64(0))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.MemTracker.MaxConsumed(), Greater, int64(0))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.DiskTracker.BytesConsumed(), Equals, int64(0))
	c.Assert(tk.Se.GetSessionVars().StmtCtx.DiskTracker.MaxConsumed(), Greater, int64(0))
	return
}

func (s *testSuite2) TestMergeJoin(c *C) {
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
		"Sort_6 100000000.00 root test.t.a:asc, test.t.a:asc",
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
		"Projection_7 10000.00 root Column#7",
		"└─MergeJoin_8 10000.00 root left outer semi join, other cond:eq(test.t.a, test.s.a), ge(test.s.b, test.t.b)",
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

func (s *testSuite2) Test3WaysMergeJoin(c *C) {
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

func (s *testSuite2) TestMergeJoinDifferentTypes(c *C) {
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

// TestVectorizedMergeJoin is used to test vectorized merge join with some corner cases.
func (s *testSuite2) TestVectorizedMergeJoin(c *C) {
	runTest := func(tk *testkit.TestKit, prefix string, t1, t2 []int) {
		tn1 := prefix + "t1"
		tn2 := prefix + "t2"
		tk.MustExec("create table " + tn1 + " (a int, b int)")
		defer tk.MustExec("drop table " + tn1)
		tk.MustExec("create table " + tn2 + " (a int, b int)")
		defer tk.MustExec("drop table " + tn2)

		insert := func(tName string, ts []int) {
			for i, n := range ts {
				if n == 0 {
					continue
				}
				var buf bytes.Buffer
				buf.WriteString(fmt.Sprintf("insert into %v values ", tName))
				for j := 0; j < n; j++ {
					if j > 0 {
						buf.WriteString(", ")
					}
					buf.WriteString(fmt.Sprintf("(%v, %v)", i, rand.Intn(10)))
				}
				tk.MustExec(buf.String())
			}
		}
		insert(tn1, t1)
		insert(tn2, t2)

		selSQL := func(hint string) string {
			return fmt.Sprintf("select /*+ %v(%v, %v) */ * from %v, %v where %v.a=%v.a and %v.b>5 and %v.b<5",
				hint, tn1, tn2, tn1, tn2, tn1, tn2, tn1, tn2)
		}

		tk.MustQuery("explain " + selSQL("TIDB_SMJ")).Check(testkit.Rows(
			fmt.Sprintf(`MergeJoin_7 4150.01 root inner join, left key:test.%v.a, right key:test.%v.a`, tn1, tn2),
			fmt.Sprintf(`├─Sort_11 3330.00 root test.%v.a:asc`, tn1),
			`│ └─TableReader_10 3330.00 root data:Selection_9`,
			fmt.Sprintf(`│   └─Selection_9 3330.00 cop[tikv] gt(test.%v.b, 5), not(isnull(test.%v.a))`, tn1, tn1),
			fmt.Sprintf(`│     └─TableScan_8 10000.00 cop[tikv] table:%v, range:[-inf,+inf], keep order:false, stats:pseudo`, tn1),
			fmt.Sprintf(`└─Sort_15 3320.01 root test.%v.a:asc`, tn2),
			`  └─TableReader_14 3320.01 root data:Selection_13`,
			fmt.Sprintf(`    └─Selection_13 3320.01 cop[tikv] lt(test.%v.b, 5), not(isnull(test.%v.a))`, tn2, tn2),
			fmt.Sprintf(`      └─TableScan_12 10000.00 cop[tikv] table:%v, range:[-inf,+inf], keep order:false, stats:pseudo`, tn2),
		))
		tk.MustQuery("explain " + selSQL("TIDB_HJ")).Check(testkit.Rows(
			fmt.Sprintf(`HashLeftJoin_7 4150.01 root inner join, inner:TableReader_14, equal:[eq(test.%v.a, test.%v.a)]`, tn1, tn2),
			`├─TableReader_11 3330.00 root data:Selection_10`,
			fmt.Sprintf(`│ └─Selection_10 3330.00 cop[tikv] gt(test.%v.b, 5), not(isnull(test.%v.a))`, tn1, tn1),
			fmt.Sprintf(`│   └─TableScan_9 10000.00 cop[tikv] table:%v, range:[-inf,+inf], keep order:false, stats:pseudo`, tn1),
			`└─TableReader_14 3320.01 root data:Selection_13`,
			fmt.Sprintf(`  └─Selection_13 3320.01 cop[tikv] lt(test.%v.b, 5), not(isnull(test.%v.a))`, tn2, tn2),
			fmt.Sprintf(`    └─TableScan_12 10000.00 cop[tikv] table:%v, range:[-inf,+inf], keep order:false, stats:pseudo`, tn2),
		))

		r1 := tk.MustQuery(selSQL("TIDB_SMJ")).Sort()
		r2 := tk.MustQuery(selSQL("TIDB_HJ")).Sort()
		c.Assert(len(r1.Rows()), Equals, len(r2.Rows()))

		i := 0
		n := len(r1.Rows())
		for i < n {
			c.Assert(len(r1.Rows()[i]), Equals, len(r2.Rows()[i]))
			for j := range r1.Rows()[i] {
				c.Assert(r1.Rows()[i][j], Equals, r2.Rows()[i][j])
			}
			i += rand.Intn((n-i)/5+1) + 1 // just compare parts of results to speed up
		}
	}

	chunkSize := 16
	cases := []struct {
		t1 []int
		t2 []int
	}{
		{[]int{0}, []int{chunkSize}},
		{[]int{0}, []int{chunkSize - 1}},
		{[]int{0}, []int{chunkSize + 1}},
		{[]int{1}, []int{chunkSize}},
		{[]int{1}, []int{chunkSize - 1}},
		{[]int{1}, []int{chunkSize + 1}},
		{[]int{chunkSize - 1}, []int{chunkSize}},
		{[]int{chunkSize - 1}, []int{chunkSize - 1}},
		{[]int{chunkSize - 1}, []int{chunkSize + 1}},
		{[]int{chunkSize}, []int{chunkSize}},
		{[]int{chunkSize}, []int{chunkSize - 1}},
		{[]int{chunkSize}, []int{chunkSize + 1}},
		{[]int{chunkSize + 1}, []int{chunkSize}},
		{[]int{chunkSize + 1}, []int{chunkSize - 1}},
		{[]int{chunkSize + 1}, []int{chunkSize + 1}},
		{[]int{1, 1, 1}, []int{chunkSize + 1, chunkSize*5 + 10, chunkSize - 10}},
		{[]int{0, 0, chunkSize}, []int{chunkSize + 1, chunkSize*5 + 10, chunkSize - 10}},
		{[]int{chunkSize + 1, 0, chunkSize}, []int{chunkSize + 1, chunkSize*5 + 10, chunkSize - 10}},
	}

	//// random complex cases
	//genCase := func() []int {
	//	n := rand.Intn(32) + 32
	//	ts := make([]int, n)
	//	for i := 0; i < n; i++ {
	//		ts[i] = rand.Intn(chunkSize * 2)
	//	}
	//	return ts
	//}
	//for i := 0; i < 10; i++ {
	//	t1 := genCase()
	//	t2 := genCase()
	//	cases = append(cases, struct {
	//		t1 []int
	//		t2 []int
	//	}{t1, t2})
	//}

	ch := make(chan [][]int)
	var wg sync.WaitGroup
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func(id int) {
			tk := testkit.NewTestKit(c, s.store)
			tk.MustExec("use test")
			tk.Se.GetSessionVars().MaxChunkSize = chunkSize
			defer wg.Done()
			for {
				select {
				case ts, ok := <-ch:
					if !ok {
						return
					}
					runTest(tk, fmt.Sprintf("pre%v", id), ts[0], ts[1])
					runTest(tk, fmt.Sprintf("pre%v", id), ts[1], ts[0])
				}
			}
		}(i + 1)
	}
	for _, ca := range cases {
		ch <- [][]int{ca.t1, ca.t2}
	}
	close(ch)
	wg.Wait()
}
