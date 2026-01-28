// Copyright 2024 PingCAP, Inc.
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

package plancache

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestPlanCachePartitionSuite(t *testing.T) {
	store := testkit.CreateMockStore(t)
	newTestKit := func(t *testing.T) *testkit.TestKit {
		t.Helper()
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("drop database if exists test")
		tk.MustExec("create database test")
		tk.MustExec("use test")
		return tk
	}

	// fix-control-partition-plan-cache
	{
		tk := newTestKit(t)
		tk.MustExec("create table t (a int) partition by hash(a) partitions 4")
		tk.MustExec(`analyze table t`)
		tk.MustExec(`prepare st from 'select * from t where a=?'`)
		tk.MustQuery(`show warnings`).Check(testkit.Rows())
		tk.MustExec(`set @a=1`)
		tk.MustExec(`execute st using @a`)
		tk.MustQuery(`show warnings`).Check(testkit.Rows())
		tk.MustExec(`execute st using @a`)
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

		tk.MustExec(`set @@tidb_opt_fix_control = "49736:ON"`)
		tk.MustExec(`prepare st from 'select * from t where a=?'`)
		tk.MustQuery(`show warnings`).Check(testkit.Rows())
		tk.MustExec(`set @a=1`)
		tk.MustExec(`execute st using @a`)
		tk.MustQuery(`show warnings`).Check(testkit.Rows())
		tk.MustExec(`execute st using @a`)
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	}

	// prepared-plan-cache-partitions
	{
		tk := newTestKit(t)
		tk.MustExec(`create table t (a int primary key, b varchar(255)) partition by hash(a) partitions 3`)
		tk.MustExec(`insert into t values (1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"e"),(6,"f")`)
		tk.MustExec(`analyze table t`)
		tk.MustExec(`prepare stmt from 'select a,b from t where a = ?;'`)
		tk.MustExec(`set @a=1`)
		tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("1 a"))
		// Same partition works, due to pruning is not affected
		tk.MustExec(`set @a=4`)
		tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("4 d"))
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
		// Different partition needs code changes
		tk.MustExec(`set @a=2`)
		tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("2 b"))
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
		tk.MustExec(`prepare stmt2 from 'select b,a from t where a = ?;'`)
		tk.MustExec(`set @a=1`)
		tk.MustQuery(`execute stmt2 using @a`).Check(testkit.Rows("a 1"))
		tk.MustExec(`set @a=3`)
		tk.MustQuery(`execute stmt2 using @a`).Check(testkit.Rows("c 3"))
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
		tk.MustExec(`drop table t`)

		tk.MustExec(`create table t (a int primary key, b varchar(255), c varchar(255), key (b)) partition by range (a) (partition pNeg values less than (0), partition p0 values less than (1000000), partition p1M values less than (2000000))`)
		tk.MustExec(`insert into t values (-10, -10, -10), (0, 0, 0), (-1, NULL, NULL), (1000, 1000, 1000), (1000000, 1000000, 1000000), (1500000, 1500000, 1500000), (1999999, 1999999, 1999999)`)
		tk.MustExec(`analyze table t`)
		tk.MustExec(`prepare stmt3 from 'select a,c,b from t where a = ?'`)
		tk.MustExec(`set @a=2000000`)
		// This should use TableDual
		tk.MustQuery(`execute stmt3 using @a`).Check(testkit.Rows())
		require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
		tkProcess := tk.Session().ShowProcess()
		ps := []*sessmgr.ProcessInfo{tkProcess}
		tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
		tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).MultiCheckContain([]string{"Point_Get", "partition:dual", "handle:2000000"})
		tk.MustExec(`set @a=1999999`)
		tk.MustQuery(`execute stmt3 using @a`).Check(testkit.Rows("1999999 1999999 1999999"))
		require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
		tkProcess = tk.Session().ShowProcess()
		ps = []*sessmgr.ProcessInfo{tkProcess}
		tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
		tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).MultiCheckContain([]string{"Point_Get", "partition:p1M", "handle:1999999"})
		tk.MustQuery(`execute stmt3 using @a`).Check(testkit.Rows("1999999 1999999 1999999"))
		require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)

		tk.MustExec(`prepare stmt4 from 'select a,c,b from t where a IN (?,?,?)'`)
		tk.MustExec(`set @a=1999999,@b=0,@c=-1`)
		tk.MustQuery(`execute stmt4 using @a,@b,@c`).Sort().Check(testkit.Rows("-1 <nil> <nil>", "0 0 0", "1999999 1999999 1999999"))
		require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
		tk.MustQuery(`execute stmt4 using @a,@b,@c`).Sort().Check(testkit.Rows("-1 <nil> <nil>", "0 0 0", "1999999 1999999 1999999"))
		require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	}

	// partition-batch-point-get-duplicates
	{
		tk := newTestKit(t)
		tk.MustExec(`create table t (a int unique key, b int) partition by range (a) (
			partition p0 values less than (10000),
			partition p1 values less than (20000),
			partition p2 values less than (30000),
			partition p3 values less than (40000))`)
		tk.MustExec(`insert into t values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7)`)
		tk.MustExec(`insert into t select a + 10000, b + 10000 from t`)
		tk.MustExec(`insert into t select a + 20000, b + 20000 from t`)
		tk.MustExec(`analyze table t`)
		tk.MustQuery(`select @@session.tidb_enable_prepared_plan_cache`).Check(testkit.Rows("1"))
		tk.MustExec(`prepare stmt from 'select * from t use index(a) where a in (?,?,?)'`)
		tk.MustExec(`set @a0 = 1, @a1 = 10001, @a2 = 2`)
		tk.MustQuery(`execute stmt using @a0, @a1, @a2`).Sort().Check(testkit.Rows("1 1", "10001 10001", "2 2"))
		require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
		tk.MustExec(`set @a0 = 3, @a1 = 20001, @a2 = 50000`)
		tk.MustQuery(`execute stmt using @a0, @a1, @a2`).Sort().Check(testkit.Rows("20001 20001", "3 3"))
		require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
		tkProcess := tk.Session().ShowProcess()
		ps := []*sessmgr.ProcessInfo{tkProcess}
		tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
		tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0}, [][]any{{"Batch_Point_Get_1"}})
		tk.MustExec(`set @a0 = 30003, @a1 = 20002, @a2 = 4`)
		tk.MustQuery(`execute stmt using @a0, @a1, @a2`).Sort().Check(testkit.Rows("20002 20002", "30003 30003", "4 4"))
		require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
		tkExplain := testkit.NewTestKit(t, store)
		tkExplain.MustExec(`use test`)
		tkExplain.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0}, [][]any{{"Batch_Point_Get_1"}})
		tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip plan-cache: plan rebuild failed, rebuild to get an unsafe range, IndexValue length diff"))
	}
}

func TestPlanCachePartitionIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	preparedCache := tk.MustQuery("select @@session.tidb_enable_prepared_plan_cache").Rows()[0][0]
	nonPreparedCache := tk.MustQuery("select @@session.tidb_enable_non_prepared_plan_cache").Rows()[0][0]
	defer func() {
		tk.MustExec(fmt.Sprintf("set @@session.tidb_enable_prepared_plan_cache=%v", preparedCache))
		tk.MustExec(fmt.Sprintf("set @@session.tidb_enable_non_prepared_plan_cache=%v", nonPreparedCache))
	}()

	runPreparedPlanCachePartitionIndex(t, tk, "t_partition_prepared")
	runNonPreparedPlanCachePartitionIndex(t, tk, "t_partition_non_prepared")
}

func runPreparedPlanCachePartitionIndex(t *testing.T, tk *testkit.TestKit, tableName string) {
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf(`create table %s (b varchar(255), a int primary key nonclustered, key (b)) partition by key(a) partitions 3`, tableName))
	tk.MustExec(fmt.Sprintf(`insert into %s values ('Ab', 1),('abc',2),('BC',3),('AC',4),('BA',5),('cda',6)`, tableName))
	tk.MustExec(fmt.Sprintf(`analyze table %s`, tableName))
	tk.MustExec(fmt.Sprintf(`prepare stmt from 'select * from %s where a IN (?,?,?)'`, tableName))
	tk.MustExec(`set @a=1,@b=3,@c=4`)
	tk.MustQuery(`execute stmt using @a,@b,@c`).Sort().Check(testkit.Rows("AC 4", "Ab 1", "BC 3"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(`execute stmt using @a,@b,@c`).Sort().Check(testkit.Rows("AC 4", "Ab 1", "BC 3"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tkProcess := tk.Session().ShowProcess()
	ps := []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain format='brief' for connection %d", tkProcess.ID)).CheckAt([]int{0}, [][]any{
		{"IndexLookUp"},
		{"├─IndexRangeScan(Build)"},
		{"└─TableRowIDScan(Probe)"}})
	tk.MustExec(`set @a=2,@b=5,@c=4`)
	tk.MustQuery(`execute stmt using @a,@b,@c`).Sort().Check(testkit.Rows("AC 4", "BA 5", "abc 2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustExec(`deallocate prepare stmt`)
}

func runNonPreparedPlanCachePartitionIndex(t *testing.T, tk *testkit.TestKit, tableName string) {
	tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec(fmt.Sprintf("drop table if exists %s", tableName))
	tk.MustExec(fmt.Sprintf(`create table %s (b varchar(255), a int primary key nonclustered, key (b)) partition by key(a) partitions 3`, tableName))
	// [Batch]PointGet does not use the plan cache,
	// since it is already using the fast path!
	tk.MustExec(fmt.Sprintf(`insert into %s values ('Ab', 1),('abc',2),('BC',3),('AC',4),('BA',5),('cda',6)`, tableName))
	tk.MustExec(fmt.Sprintf(`analyze table %s`, tableName))
	tk.MustQuery(fmt.Sprintf(`explain format='plan_cache' select * from %s where a IN (2,1,4,1,1,5,5)`, tableName)).Check(testkit.Rows(""+
		"IndexLookUp_8 4.00 root partition:p1,p2 ",
		"├─IndexRangeScan_6(Build) 4.00 cop[tikv] table:"+tableName+", index:PRIMARY(a) range:[1,1], [2,2], [4,4], [5,5], keep order:false",
		"└─TableRowIDScan_7(Probe) 4.00 cop[tikv] table:"+tableName+" keep order:false"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(fmt.Sprintf(`select * from %s where a IN (2,1,4,1,1,5,5)`, tableName)).Sort().Check(testkit.Rows("AC 4", "Ab 1", "BA 5", "abc 2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(fmt.Sprintf(`select * from %s where a IN (1,3,4)`, tableName)).Sort().Check(testkit.Rows("AC 4", "Ab 1", "BC 3"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(fmt.Sprintf(`select * from %s where a IN (1,3,4)`, tableName)).Sort().Check(testkit.Rows("AC 4", "Ab 1", "BC 3"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(fmt.Sprintf(`select * from %s where a IN (2,5,4,2,5,5,1)`, tableName)).Sort().Check(testkit.Rows("AC 4", "Ab 1", "BA 5", "abc 2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(fmt.Sprintf(`select * from %s where a IN (1,2,3,4,5,5,1)`, tableName)).Sort().Check(testkit.Rows("AC 4", "Ab 1", "BA 5", "BC 3", "abc 2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(fmt.Sprintf(`select count(*) from %s partition (p0)`, tableName)).Check(testkit.Rows("0"))
	tk.MustQuery(fmt.Sprintf(`select count(*) from %s partition (p1)`, tableName)).Check(testkit.Rows("5"))
	tk.MustQuery(fmt.Sprintf(`select * from %s partition (p2)`, tableName)).Check(testkit.Rows("Ab 1"))

	tk.MustQuery(fmt.Sprintf(`explain format='plan_cache' select * from %s where a = 2`, tableName)).Check(testkit.Rows("Point_Get_1 1.00 root table:" + tableName + ", partition:p1, index:PRIMARY(a) "))
	tk.MustQuery(fmt.Sprintf(`explain format='plan_cache' select * from %s where a = 2`, tableName)).Check(testkit.Rows("Point_Get_1 1.00 root table:" + tableName + ", partition:p1, index:PRIMARY(a) "))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(fmt.Sprintf(`select * from %s where a = 2`, tableName)).Check(testkit.Rows("abc 2"))
	tk.MustExec(`create table tk (a int primary key nonclustered, b varchar(255), key (b)) partition by key (a) partitions 3`)
	tk.MustExec(fmt.Sprintf(`insert into tk select a, b from %s`, tableName))
	tk.MustExec(`analyze table tk`)
	tk.MustQuery(`explain format='plan_cache' select * from tk where a = 2`).Check(testkit.Rows("Point_Get_1 1.00 root table:tk, partition:p1, index:PRIMARY(a) "))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	// PointGet will use Fast Plan, so no Plan Cache, even for Key Partitioned tables.
	tk.MustQuery(`select * from tk where a = 2`).Check(testkit.Rows("2 abc"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
}

func TestPlanCacheFixControlRebuild(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery(`select @@session.tidb_enable_prepared_plan_cache`).Check(testkit.Rows("1"))

	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`CREATE TABLE t (a int primary key, b varchar(255), key (b)) PARTITION BY HASH (a) partitions 5`)
	tk.MustExec(`insert into t values(0,0),(1,1),(2,2),(3,3),(4,4)`)
	tk.MustExec(`insert into t select a + 5, b + 5 from t`)
	tk.MustExec(`analyze table t`)

	tk.MustExec(`prepare stmt from 'select * from t where a = ?'`)
	tk.MustExec(`set @a = 2`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("2 2"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustExec(`set @a = 3`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("3 3"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustExec(`set @@tidb_opt_fix_control = "33031:ON"`)
	tk.MustExec(`set @a = 1`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("1 1"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip plan-cache: plan rebuild failed, Fix33031 fix-control set and partitioned table in cached Point Get plan"))
	tk.MustExec(`set @@tidb_opt_fix_control = "33031:OFF"`)
	tk.MustExec(`set @a = 2`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("2 2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustExec(`deallocate prepare stmt`)

	tk.MustExec(`prepare stmt from 'select * from t where a IN (?,?)'`)
	tk.MustExec(`set @a = 2, @b = 5`)
	tk.MustQuery(`execute stmt using @a, @b`).Sort().Check(testkit.Rows("2 2", "5 5"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustExec(`set @a = 3, @b = 0`)
	tk.MustQuery(`execute stmt using @a, @b`).Sort().Check(testkit.Rows("0 0", "3 3"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustExec(`set @@tidb_opt_fix_control = "33031:ON"`)
	tk.MustExec(`set @a = 1, @b = 2`)
	tk.MustQuery(`execute stmt using @a, @b`).Check(testkit.Rows("1 1", "2 2"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip plan-cache: plan rebuild failed, Fix33031 fix-control set and partitioned table in cached Batch Point Get plan"))
	tk.MustExec(`set @@tidb_opt_fix_control = "33031:OFF"`)
	tk.MustExec(`set @a = 2, @b = 3`)
	tk.MustQuery(`execute stmt using @a, @b`).Check(testkit.Rows("2 2", "3 3"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
}

func TestPreparedStmtPartitionUnion(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, unique key (a))
partition by hash (a) partitions 3`)

	for i := range 100 {
		tk.MustExec("insert into t values (?, ?)", i, i)
	}
	tk.MustExec("analyze table t")
	tk.MustExec(`set tidb_partition_prune_mode = 'static'`)
	tk.MustQuery(`select b from t where a = 1 or a = 10 or a = 10 or a = 999999`).Sort().Check(testkit.Rows("1", "10"))
	tk.MustQuery(`explain format='brief' select b from t where a = 1 or a = 10 or a = 10 or a = 999999`).Check(testkit.Rows(""+
		"PartitionUnion 6.00 root  ",
		"├─Projection 3.00 root  test.t.b",
		"│ └─Batch_Point_Get 3.00 root table:t, partition:p0, index:a(a) keep order:false, desc:false",
		"└─Projection 3.00 root  test.t.b",
		"  └─Batch_Point_Get 3.00 root table:t, partition:p1, index:a(a) keep order:false, desc:false"))
	tk.MustExec(`prepare stmt from 'select b from t where a = 1 or a = 10 or a = 10 or a = 999999'`)
	tk.MustQuery(`execute stmt`)
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tkProcess := tk.Session().ShowProcess()
	ps := []*sessmgr.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).MultiCheckContain([]string{"PartitionUnion", "Batch_Point_Get", "partition:p0", "partition:p1"})
	tk.MustQuery(`execute stmt`)
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip prepared plan-cache: query accesses partitioned tables is un-cacheable if tidb_partition_pruning_mode = 'static'"))
}
