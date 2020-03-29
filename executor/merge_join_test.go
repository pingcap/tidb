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
	"github.com/pingcap/tidb/sessionctx/variable"
	"math/rand"
	"strings"

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
		"Sort_6 100000000.00 root  test.t.a:asc, test.t.a:asc",
		"└─MergeJoin_9 100000000.00 root  inner join",
		"  ├─TableReader_13(Build) 10000.00 root  data:TableFullScan_12",
		"  │ └─TableFullScan_12 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"  └─TableReader_11(Probe) 10000.00 root  data:TableFullScan_10",
		"    └─TableFullScan_10 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
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
		"MergeJoin_8 10000.00 root left outer semi join, other cond:eq(test.t.a, test.s.a), ge(test.s.b, test.t.b)",
		"├─TableReader_12(Build) 10000.00 root data:TableFullScan_11",
		"│ └─TableFullScan_11 10000.00 cop[tikv] table:s, keep order:false, stats:pseudo",
		"└─TableReader_10(Probe) 10000.00 root data:TableFullScan_9",
		"  └─TableFullScan_9 10000.00 cop[tikv] table:t, keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ TIDB_SMJ(t, s) */ a in (select a from s where s.b >= t.b) from t").Check(testkit.Rows(
		"1",
		"0",
	))

	// Test TIDB_SMJ for join with order by desc, see https://github.com/pingcap/tidb/issues/14483
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t (a int, key(a))")
	tk.MustExec("create table t1 (a int, key(a))")
	tk.MustExec("insert into t values (1), (2), (3)")
	tk.MustExec("insert into t1 values (1), (2), (3)")
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t.a from t, t1 where t.a = t1.a order by t1.a desc").Check(testkit.Rows(
		"3", "2", "1"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, key(a), key(b))")
	tk.MustExec("insert into t values (1,1),(1,2),(1,3),(2,1),(2,2),(3,1),(3,2),(3,3)")
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a from t t1, t t2 where t1.a = t2.b order by t1.a desc").Check(testkit.Rows(
		"3", "3", "3", "3", "3", "3",
		"2", "2", "2", "2", "2", "2",
		"1", "1", "1", "1", "1", "1", "1", "1", "1"))

	tk.MustExec("drop table if exists s")
	tk.MustExec("create table s (a int)")
	tk.MustExec("insert into s values (4), (1), (3), (2)")
	tk.MustQuery("explain select s1.a1 from (select a as a1 from s order by s.a desc) as s1 join (select a as a2 from s order by s.a desc) as s2 on s1.a1 = s2.a2 order by s1.a1 desc").Check(testkit.Rows(
		"MergeJoin_28 12487.50 root inner join, left key:test.s.a, right key:test.s.a",
		"├─Sort_31(Build) 9990.00 root test.s.a:desc",
		"│ └─TableReader_26 9990.00 root data:Selection_25",
		"│   └─Selection_25 9990.00 cop[tikv] not(isnull(test.s.a))",
		"│     └─TableFullScan_24 10000.00 cop[tikv] table:s, keep order:false, stats:pseudo",
		"└─Sort_29(Probe) 9990.00 root test.s.a:desc",
		"  └─TableReader_21 9990.00 root data:Selection_20",
		"    └─Selection_20 9990.00 cop[tikv] not(isnull(test.s.a))",
		"      └─TableFullScan_19 10000.00 cop[tikv] table:s, keep order:false, stats:pseudo",
	))
	tk.MustQuery("select s1.a1 from (select a as a1 from s order by s.a desc) as s1 join (select a as a2 from s order by s.a desc) as s2 on s1.a1 = s2.a2 order by s1.a1 desc").Check(testkit.Rows(
		"4", "3", "2", "1"))
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
func (s *testSuiteJoin3) TestVectorizedMergeJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("create table t2 (a int, b int)")
	runTest := func(t1, t2 []int) {
		tk.MustExec("truncate table t1")
		tk.MustExec("truncate table t2")
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
		insert("t1", t1)
		insert("t2", t2)

		tk.MustQuery("explain select /*+ TIDB_SMJ(t1, t2) */ * from t1, t2 where t1.a=t2.a and t1.b>5 and t2.b<5").Check(testkit.Rows(
			`MergeJoin_7 4150.01 root  inner join, left key:test.t1.a, right key:test.t2.a`,
			`├─Sort_15(Build) 3320.01 root  test.t2.a:asc`,
			`│ └─TableReader_14 3320.01 root  data:Selection_13`,
			`│   └─Selection_13 3320.01 cop[tikv]  lt(test.t2.b, 5), not(isnull(test.t2.a))`,
			`│     └─TableFullScan_12 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo`,
			`└─Sort_11(Probe) 3330.00 root  test.t1.a:asc`,
			`  └─TableReader_10 3330.00 root  data:Selection_9`,
			`    └─Selection_9 3330.00 cop[tikv]  gt(test.t1.b, 5), not(isnull(test.t1.a))`,
			`      └─TableFullScan_8 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo`,
		))
		tk.MustQuery("explain select /*+ TIDB_HJ(t1, t2) */ * from t1, t2 where t1.a=t2.a and t1.b>5 and t2.b<5").Check(testkit.Rows(
			`HashJoin_7 4150.01 root  inner join, equal:[eq(test.t1.a, test.t2.a)]`,
			`├─TableReader_14(Build) 3320.01 root  data:Selection_13`,
			`│ └─Selection_13 3320.01 cop[tikv]  lt(test.t2.b, 5), not(isnull(test.t2.a))`,
			`│   └─TableFullScan_12 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo`,
			`└─TableReader_11(Probe) 3330.00 root  data:Selection_10`,
			`  └─Selection_10 3330.00 cop[tikv]  gt(test.t1.b, 5), not(isnull(test.t1.a))`,
			`    └─TableFullScan_9 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo`,
		))

		r1 := tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ * from t1, t2 where t1.a=t2.a and t1.b>5 and t2.b<5").Sort()
		r2 := tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ * from t1, t2 where t1.a=t2.a and t1.b>5 and t2.b<5").Sort()
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

	tk.Se.GetSessionVars().MaxChunkSize = variable.DefInitChunkSize
	chunkSize := tk.Se.GetSessionVars().MaxChunkSize
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
		{[]int{1, 1, 1}, []int{chunkSize + 1, chunkSize*5 + 5, chunkSize - 5}},
		{[]int{0, 0, chunkSize}, []int{chunkSize + 1, chunkSize*5 + 5, chunkSize - 5}},
		{[]int{chunkSize + 1, 0, chunkSize}, []int{chunkSize + 1, chunkSize*5 + 5, chunkSize - 5}},
	}
	for _, ca := range cases {
		runTest(ca.t1, ca.t2)
		runTest(ca.t2, ca.t1)
	}
}

func (s *testSuite2) TestMergeJoinWithOtherConditions(c *C) {
	// more than one inner tuple should be filtered on other conditions
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists R;`)
	tk.MustExec(`drop table if exists Y;`)
	tk.MustExec(`create table Y (a int primary key, b int, index id_b(b));`)
	tk.MustExec(`insert into Y values (0,2),(2,2);`)
	tk.MustExec(`create table R (a int primary key, b int);`)
	tk.MustExec(`insert into R values (2,2);`)
	// the max() limits the required rows at most one
	// TODO(fangzhuhe): specify Y as the build side using hints
	tk.MustQuery(`select /*+tidb_smj(R)*/ max(Y.a) from R join Y  on R.a=Y.b where R.b <= Y.a;`).Check(testkit.Rows(
		`2`,
	))
}
