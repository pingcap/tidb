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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
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

func checkMergeAndRun(tk *testkit.TestKit, t *testing.T, sql string) *testkit.Result {
	explainedSQL := "explain format = 'brief' " + sql
	result := tk.MustQuery(explainedSQL)
	resultStr := fmt.Sprintf("%v", result.Rows())
	require.Contains(t, resultStr, "MergeJoin")
	return tk.MustQuery(sql)
}

func checkPlanAndRun(tk *testkit.TestKit, t *testing.T, plan string, sql string) *testkit.Result {
	explainedSQL := "explain format = 'brief' " + sql
	/* result := */ tk.MustQuery(explainedSQL)
	// TODO: Reopen it after refactoring explain.
	// resultStr := fmt.Sprintf("%v", result.Rows())
	// require.Equal(t, resultStr, plan)
	return tk.MustQuery(sql)
}

func TestShuffleMergeJoinInDisk(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMUseTmpStorage = true
	})

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testMergeJoinRowContainerSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testMergeJoinRowContainerSpill"))
	}()
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	sm := &testkit.MockSessionManager{
		PS: make([]*util.ProcessInfo, 0),
	}
	tk.Session().SetSessionManager(sm)
	dom.ExpensiveQueryHandle().SetSessionManager(sm)

	tk.MustExec("set @@tidb_mem_quota_query=1;")
	tk.MustExec("set @@tidb_merge_join_concurrency=4;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int, c2 int)")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tk.MustExec("insert into t values(1,1)")
	tk.MustExec("insert into t1 values(1,3),(4,4)")

	result := checkMergeAndRun(tk, t, "select /*+ TIDB_SMJ(t) */ * from t1 left outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows("1 3 1 1"))
	require.Equal(t, int64(0), tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed())
	require.Greater(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.MaxConsumed(), int64(0))
	require.Equal(t, int64(0), tk.Session().GetSessionVars().StmtCtx.DiskTracker.BytesConsumed())
	require.Greater(t, tk.Session().GetSessionVars().StmtCtx.DiskTracker.MaxConsumed(), int64(0))
}

func TestMergeJoinInDisk(t *testing.T) {
	t.Skip("unstable, skip it and fix it before 20210618")
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMUseTmpStorage = true
		conf.TempStoragePath = t.TempDir()
	})

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testMergeJoinRowContainerSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testMergeJoinRowContainerSpill"))
	}()
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("SET GLOBAL tidb_mem_oom_action = DEFAULT")
	tk.MustExec("SET GLOBAL tidb_mem_oom_action='LOG'")
	tk.MustExec("use test")

	sm := &testkit.MockSessionManager{
		PS: make([]*util.ProcessInfo, 0),
	}
	tk.Session().SetSessionManager(sm)
	dom.ExpensiveQueryHandle().SetSessionManager(sm)

	tk.MustExec("set @@tidb_mem_quota_query=1;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int, c2 int)")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tk.MustExec("insert into t values(1,1)")
	tk.MustExec("insert into t1 values(1,3),(4,4)")

	result := checkMergeAndRun(tk, t, "select /*+ TIDB_SMJ(t) */ * from t1 left outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows("1 3 1 1"))
	require.Equal(t, int64(0), tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed())
	require.Greater(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.MaxConsumed(), int64(0))
	require.Equal(t, int64(0), tk.Session().GetSessionVars().StmtCtx.DiskTracker.BytesConsumed())
	require.Greater(t, tk.Session().GetSessionVars().StmtCtx.DiskTracker.MaxConsumed(), int64(0))
}

func TestMergeJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int, c2 int)")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tk.MustExec("insert into t values(1,1),(2,2)")
	tk.MustExec("insert into t1 values(2,3),(4,4)")

	result := checkMergeAndRun(tk, t, "select /*+ TIDB_SMJ(t) */ * from t left outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows("1 1 <nil> <nil>"))
	result = checkMergeAndRun(tk, t, "select /*+ TIDB_SMJ(t) */ * from t1 right outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows("<nil> <nil> 1 1"))
	result = checkMergeAndRun(tk, t, "select /*+ TIDB_SMJ(t) */ * from t right outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows())
	result = checkMergeAndRun(tk, t, "select /*+ TIDB_SMJ(t) */ * from t left outer join t1 on t.c1 = t1.c1 where t1.c1 = 3 or false")
	result.Check(testkit.Rows())
	result = checkMergeAndRun(tk, t, "select /*+ TIDB_SMJ(t) */ * from t left outer join t1 on t.c1 = t1.c1 and t.c1 != 1 order by t1.c1")
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
	tk.MustQuery("explain format = 'brief' select /*+ TIDB_SMJ(t1, t2) */ * from t t1 join t t2 order by t1.a, t2.a").Check(testkit.Rows(
		"Sort 100000000.00 root  test.t.a, test.t.a",
		"└─MergeJoin 100000000.00 root  inner join",
		"  ├─TableReader(Build) 10000.00 root  data:TableFullScan",
		"  │ └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"  └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"    └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
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
	tk.MustQuery("explain format = 'brief' select /*+ TIDB_SMJ(t, s) */ a in (select a from s where s.b >= t.b) from t").Check(testkit.Rows(
		"MergeJoin 10000.00 root  left outer semi join, other cond:eq(test.t.a, test.s.a), ge(test.s.b, test.t.b)",
		"├─TableReader(Build) 10000.00 root  data:TableFullScan",
		"│ └─TableFullScan 10000.00 cop[tikv] table:s keep order:false, stats:pseudo",
		"└─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"  └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
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
	tk.MustQuery("explain format = 'brief' select s1.a1 from (select a as a1 from s order by s.a desc) as s1 join (select a as a2 from s order by s.a desc) as s2 on s1.a1 = s2.a2 order by s1.a1 desc").Check(testkit.Rows(
		"MergeJoin 12487.50 root  inner join, left key:test.s.a, right key:test.s.a",
		"├─Sort(Build) 9990.00 root  test.s.a:desc",
		"│ └─TableReader 9990.00 root  data:Selection",
		"│   └─Selection 9990.00 cop[tikv]  not(isnull(test.s.a))",
		"│     └─TableFullScan 10000.00 cop[tikv] table:s keep order:false, stats:pseudo",
		"└─Sort(Probe) 9990.00 root  test.s.a:desc",
		"  └─TableReader 9990.00 root  data:Selection",
		"    └─Selection 9990.00 cop[tikv]  not(isnull(test.s.a))",
		"      └─TableFullScan 10000.00 cop[tikv] table:s keep order:false, stats:pseudo",
	))
	tk.MustQuery("select s1.a1 from (select a as a1 from s order by s.a desc) as s1 join (select a as a2 from s order by s.a desc) as s2 on s1.a1 = s2.a2 order by s1.a1 desc").Check(testkit.Rows(
		"4", "3", "2", "1"))
}

func TestShuffleMergeJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_merge_join_concurrency = 4;")

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int, c2 int)")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tk.MustExec("insert into t values(1,1),(2,2)")
	tk.MustExec("insert into t1 values(2,3),(4,4)")

	result := checkMergeAndRun(tk, t, "select /*+ TIDB_SMJ(t) */ * from t left outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows("1 1 <nil> <nil>"))
	result = checkMergeAndRun(tk, t, "select /*+ TIDB_SMJ(t) */ * from t1 right outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows("<nil> <nil> 1 1"))
	result = checkMergeAndRun(tk, t, "select /*+ TIDB_SMJ(t) */ * from t right outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows())
	result = checkMergeAndRun(tk, t, "select /*+ TIDB_SMJ(t) */ * from t left outer join t1 on t.c1 = t1.c1 where t1.c1 = 3 or false")
	result.Check(testkit.Rows())
	result = checkMergeAndRun(tk, t, "select /*+ TIDB_SMJ(t) */ * from t left outer join t1 on t.c1 = t1.c1 and t.c1 != 1 order by t1.c1")
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
	tk.MustQuery("explain format = 'brief' select /*+ TIDB_SMJ(t1, t2) */ * from t t1 join t t2 order by t1.a, t2.a").Check(testkit.Rows(
		"Sort 100000000.00 root  test.t.a, test.t.a",
		"└─MergeJoin 100000000.00 root  inner join",
		"  ├─TableReader(Build) 10000.00 root  data:TableFullScan",
		"  │ └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"  └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"    └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
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
	tk.MustQuery("explain format = 'brief' select /*+ TIDB_SMJ(t, s) */ a in (select a from s where s.b >= t.b) from t").Check(testkit.Rows(
		"MergeJoin 10000.00 root  left outer semi join, other cond:eq(test.t.a, test.s.a), ge(test.s.b, test.t.b)",
		"├─TableReader(Build) 10000.00 root  data:TableFullScan",
		"│ └─TableFullScan 10000.00 cop[tikv] table:s keep order:false, stats:pseudo",
		"└─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"  └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
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
	tk.MustQuery("explain format = 'brief' select s1.a1 from (select a as a1 from s order by s.a desc) as s1 join (select a as a2 from s order by s.a desc) as s2 on s1.a1 = s2.a2 order by s1.a1 desc").Check(testkit.Rows(
		"MergeJoin 12487.50 root  inner join, left key:test.s.a, right key:test.s.a",
		"├─Sort(Build) 9990.00 root  test.s.a:desc",
		"│ └─TableReader 9990.00 root  data:Selection",
		"│   └─Selection 9990.00 cop[tikv]  not(isnull(test.s.a))",
		"│     └─TableFullScan 10000.00 cop[tikv] table:s keep order:false, stats:pseudo",
		"└─Sort(Probe) 9990.00 root  test.s.a:desc",
		"  └─TableReader 9990.00 root  data:Selection",
		"    └─Selection 9990.00 cop[tikv]  not(isnull(test.s.a))",
		"      └─TableFullScan 10000.00 cop[tikv] table:s keep order:false, stats:pseudo",
	))
	tk.MustQuery("select s1.a1 from (select a as a1 from s order by s.a desc) as s1 join (select a as a2 from s order by s.a desc) as s2 on s1.a1 = s2.a2 order by s1.a1 desc").Check(testkit.Rows(
		"4", "3", "2", "1"))
}

func Test3WaysMergeJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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
	result := checkPlanAndRun(tk, t, plan1, "select /*+ TIDB_SMJ(t1,t2,t3) */ * from t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1 order by 1")
	result.Check(testkit.Rows("2 2 2 3 2 4", "3 3 3 4 3 10"))

	result = checkPlanAndRun(tk, t, plan2, "select /*+ TIDB_SMJ(t1,t2,t3) */ * from t1 right outer join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1 order by 1")
	result.Check(testkit.Rows("2 2 2 3 2 4", "3 3 3 4 3 10"))

	// In below case, t1 side filled with null when no matched join, so that order is not kept and sort appended
	// On the other hand, t1 order kept so no final sort appended
	result = checkPlanAndRun(tk, t, plan3, "select /*+ TIDB_SMJ(t1,t2,t3) */ * from t1 right outer join t2 on t1.c1 = t2.c1 join t3 on t1.c1 = t3.c1 order by 1")
	result.Check(testkit.Rows("2 2 2 3 2 4", "3 3 3 4 3 10"))
}

func Test3WaysShuffleMergeJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_merge_join_concurrency = 4;")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t1(c1 int, c2 int, PRIMARY KEY (c1))")
	tk.MustExec("create table t2(c1 int, c2 int, PRIMARY KEY (c1))")
	tk.MustExec("create table t3(c1 int, c2 int, PRIMARY KEY (c1))")
	tk.MustExec("insert into t1 values(1,1),(2,2),(3,3)")
	tk.MustExec("insert into t2 values(2,3),(3,4),(4,5)")
	tk.MustExec("insert into t3 values(1,2),(2,4),(3,10)")
	result := checkPlanAndRun(tk, t, plan1, "select /*+ TIDB_SMJ(t1,t2,t3) */ * from t1 join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1 order by 1")
	result.Check(testkit.Rows("2 2 2 3 2 4", "3 3 3 4 3 10"))

	result = checkPlanAndRun(tk, t, plan2, "select /*+ TIDB_SMJ(t1,t2,t3) */ * from t1 right outer join t2 on t1.c1 = t2.c1 join t3 on t2.c1 = t3.c1 order by 1")
	result.Check(testkit.Rows("2 2 2 3 2 4", "3 3 3 4 3 10"))

	// In below case, t1 side filled with null when no matched join, so that order is not kept and sort appended
	// On the other hand, t1 order kept so no final sort appended
	result = checkPlanAndRun(tk, t, plan3, "select /*+ TIDB_SMJ(t1,t2,t3) */ * from t1 right outer join t2 on t1.c1 = t2.c1 join t3 on t1.c1 = t3.c1 order by 1")
	result.Check(testkit.Rows("2 2 2 3 2 4", "3 3 3 4 3 10"))
}

func TestMergeJoinDifferentTypes(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@session.tidb_executor_concurrency = 4;")
	tk.MustExec("set @@session.tidb_hash_join_concurrency = 5;")
	tk.MustExec("set @@session.tidb_distsql_scan_concurrency = 15;")

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
//nolint:gosimple // generates false positive fmt.Sprintf warnings which keep aligned
func TestVectorizedMergeJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	existTableMap := make(map[string]struct{})
	runTest := func(ts1, ts2 []int) {
		getTable := func(prefix string, ts []int) string {
			tableName := prefix
			for _, i := range ts {
				tableName = tableName + "_" + strconv.Itoa(i)
			}
			if _, ok := existTableMap[tableName]; ok {
				return tableName
			}
			tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
			tk.MustExec(fmt.Sprintf("create table %s (a int, b int)", tableName))
			existTableMap[tableName] = struct{}{}
			for i, n := range ts {
				if n == 0 {
					continue
				}
				var buf bytes.Buffer
				buf.WriteString(fmt.Sprintf("insert into %v values ", tableName))
				for j := 0; j < n; j++ {
					if j > 0 {
						buf.WriteString(", ")
					}
					buf.WriteString(fmt.Sprintf("(%v, %v)", i, rand.Intn(10)))
				}
				tk.MustExec(buf.String())
			}
			return tableName
		}
		t1 := getTable("t", ts1)
		t2 := getTable("t", ts2)
		if t1 == t2 {
			t2 = getTable("t2", ts2)
		}

		tk.MustQuery(fmt.Sprintf("explain format = 'brief' select /*+ TIDB_SMJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Check(testkit.Rows(
			fmt.Sprintf(`MergeJoin 4150.01 root  inner join, left key:test.%s.a, right key:test.%s.a`, t1, t2),
			fmt.Sprintf(`├─Sort(Build) 3320.01 root  test.%s.a`, t2),
			fmt.Sprintf(`│ └─TableReader 3320.01 root  data:Selection`),
			fmt.Sprintf(`│   └─Selection 3320.01 cop[tikv]  lt(test.%s.b, 5), not(isnull(test.%s.a))`, t2, t2),
			fmt.Sprintf(`│     └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t2),
			fmt.Sprintf(`└─Sort(Probe) 3330.00 root  test.%s.a`, t1),
			fmt.Sprintf(`  └─TableReader 3330.00 root  data:Selection`),
			fmt.Sprintf(`    └─Selection 3330.00 cop[tikv]  gt(test.%s.b, 5), not(isnull(test.%s.a))`, t1, t1),
			fmt.Sprintf(`      └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t1),
		))
		tk.MustQuery(fmt.Sprintf("explain format = 'brief' select /*+ TIDB_HJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Check(testkit.Rows(
			fmt.Sprintf(`HashJoin 4150.01 root  inner join, equal:[eq(test.%s.a, test.%s.a)]`, t1, t2),
			fmt.Sprintf(`├─TableReader(Build) 3320.01 root  data:Selection`),
			fmt.Sprintf(`│ └─Selection 3320.01 cop[tikv]  lt(test.%s.b, 5), not(isnull(test.%s.a))`, t2, t2),
			fmt.Sprintf(`│   └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t2),
			fmt.Sprintf(`└─TableReader(Probe) 3330.00 root  data:Selection`),
			fmt.Sprintf(`  └─Selection 3330.00 cop[tikv]  gt(test.%s.b, 5), not(isnull(test.%s.a))`, t1, t1),
			fmt.Sprintf(`    └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t1),
		))

		r1 := tk.MustQuery(fmt.Sprintf("select /*+ TIDB_SMJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Sort()
		r2 := tk.MustQuery(fmt.Sprintf("select /*+ TIDB_HJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Sort()
		require.Equal(t, len(r2.Rows()), len(r1.Rows()))

		i := 0
		n := len(r1.Rows())
		for i < n {
			require.Equal(t, len(r2.Rows()[i]), len(r1.Rows()[i]))
			for j := range r1.Rows()[i] {
				require.Equal(t, r2.Rows()[i][j], r1.Rows()[i][j])
			}
			i += rand.Intn((n-i)/5+1) + 1 // just compare parts of results to speed up
		}
	}

	tk.Session().GetSessionVars().MaxChunkSize = variable.DefInitChunkSize
	chunkSize := tk.Session().GetSessionVars().MaxChunkSize
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
		{[]int{chunkSize}, []int{chunkSize + 1}},
		{[]int{chunkSize + 1}, []int{chunkSize + 1}},
		{[]int{1, 1, 1}, []int{chunkSize + 1, chunkSize*5 + 5, chunkSize - 5}},
		{[]int{0, 0, chunkSize}, []int{chunkSize + 1, chunkSize*5 + 5, chunkSize - 5}},
		{[]int{chunkSize + 1, 0, chunkSize}, []int{chunkSize + 1, chunkSize*5 + 5, chunkSize - 5}},
	}
	for _, ca := range cases {
		runTest(ca.t1, ca.t2)
		runTest(ca.t2, ca.t1)
	}
	fmt.Println(existTableMap)
	for tableName := range existTableMap {
		tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	}
}

// TestVectorizedShuffleMergeJoin is used to test vectorized shuffle merge join with some corner cases.
//nolint:gosimple // generates false positive fmt.Sprintf warnings which keep aligned
func TestVectorizedShuffleMergeJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@session.tidb_merge_join_concurrency = 4;")
	tk.MustExec("use test")
	existTableMap := make(map[string]struct{})
	runTest := func(ts1, ts2 []int) {
		getTable := func(prefix string, ts []int) string {
			tableName := prefix
			for _, i := range ts {
				tableName = tableName + "_" + strconv.Itoa(i)
			}
			if _, ok := existTableMap[tableName]; ok {
				return tableName
			}
			tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
			tk.MustExec(fmt.Sprintf("create table %s (a int, b int)", tableName))
			existTableMap[tableName] = struct{}{}
			for i, n := range ts {
				if n == 0 {
					continue
				}
				var buf bytes.Buffer
				buf.WriteString(fmt.Sprintf("insert into %v values ", tableName))
				for j := 0; j < n; j++ {
					if j > 0 {
						buf.WriteString(", ")
					}
					buf.WriteString(fmt.Sprintf("(%v, %v)", i, rand.Intn(10)))
				}
				tk.MustExec(buf.String())
			}
			return tableName
		}
		t1 := getTable("t", ts1)
		t2 := getTable("t", ts2)
		if t1 == t2 {
			t2 = getTable("t2", ts2)
		}

		tk.MustQuery(fmt.Sprintf("explain format = 'brief' select /*+ TIDB_SMJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Check(testkit.Rows(
			fmt.Sprintf(`Shuffle 4150.01 root  execution info: concurrency:4, data sources:[TableReader TableReader]`),
			fmt.Sprintf(`└─MergeJoin 4150.01 root  inner join, left key:test.%s.a, right key:test.%s.a`, t1, t2),
			fmt.Sprintf(`  ├─Sort(Build) 3320.01 root  test.%s.a`, t2),
			fmt.Sprintf(`  │ └─TableReader 3320.01 root  data:Selection`),
			fmt.Sprintf(`  │   └─Selection 3320.01 cop[tikv]  lt(test.%s.b, 5), not(isnull(test.%s.a))`, t2, t2),
			fmt.Sprintf(`  │     └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t2),
			fmt.Sprintf(`  └─Sort(Probe) 3330.00 root  test.%s.a`, t1),
			fmt.Sprintf(`    └─TableReader 3330.00 root  data:Selection`),
			fmt.Sprintf(`      └─Selection 3330.00 cop[tikv]  gt(test.%s.b, 5), not(isnull(test.%s.a))`, t1, t1),
			fmt.Sprintf(`        └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t1),
		))
		tk.MustQuery(fmt.Sprintf("explain format = 'brief' select /*+ TIDB_HJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Check(testkit.Rows(
			fmt.Sprintf(`HashJoin 4150.01 root  inner join, equal:[eq(test.%s.a, test.%s.a)]`, t1, t2),
			fmt.Sprintf(`├─TableReader(Build) 3320.01 root  data:Selection`),
			fmt.Sprintf(`│ └─Selection 3320.01 cop[tikv]  lt(test.%s.b, 5), not(isnull(test.%s.a))`, t2, t2),
			fmt.Sprintf(`│   └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t2),
			fmt.Sprintf(`└─TableReader(Probe) 3330.00 root  data:Selection`),
			fmt.Sprintf(`  └─Selection 3330.00 cop[tikv]  gt(test.%s.b, 5), not(isnull(test.%s.a))`, t1, t1),
			fmt.Sprintf(`    └─TableFullScan 10000.00 cop[tikv] table:%s keep order:false, stats:pseudo`, t1),
		))

		r1 := tk.MustQuery(fmt.Sprintf("select /*+ TIDB_SMJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Sort()
		r2 := tk.MustQuery(fmt.Sprintf("select /*+ TIDB_HJ(%s, %s) */ * from %s, %s where %s.a=%s.a and %s.b>5 and %s.b<5",
			t1, t2, t1, t2, t1, t2, t1, t2,
		)).Sort()
		require.Equal(t, len(r2.Rows()), len(r1.Rows()))

		i := 0
		n := len(r1.Rows())
		for i < n {
			require.Equal(t, len(r2.Rows()[i]), len(r1.Rows()[i]))
			for j := range r1.Rows()[i] {
				require.Equal(t, r2.Rows()[i][j], r1.Rows()[i][j])
			}
			i += rand.Intn((n-i)/5+1) + 1 // just compare parts of results to speed up
		}
	}

	tk.Session().GetSessionVars().MaxChunkSize = variable.DefInitChunkSize
	chunkSize := tk.Session().GetSessionVars().MaxChunkSize
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
		{[]int{chunkSize}, []int{chunkSize + 1}},
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

func TestMergeJoinWithOtherConditions(t *testing.T) {
	// more than one inner tuple should be filtered on other conditions
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
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

func TestShuffleMergeJoinWithOtherConditions(t *testing.T) {
	// more than one inner tuple should be filtered on other conditions
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("set @@session.tidb_merge_join_concurrency = 4;")
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
