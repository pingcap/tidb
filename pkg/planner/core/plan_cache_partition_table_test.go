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

package core_test

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestIssue49736Partition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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

func TestPreparedPlanCachePartitions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).MultiCheckContain([]string{"Point_Get", "partition:dual", "handle:2000000"})
	tk.MustExec(`set @a=1999999`)
	tk.MustQuery(`execute stmt3 using @a`).Check(testkit.Rows("1999999 1999999 1999999"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
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

func TestPreparedPlanCachePartitionIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (b varchar(255), a int primary key nonclustered, key (b)) partition by key(a) partitions 3`)
	tk.MustExec(`insert into t values ('Ab', 1),('abc',2),('BC',3),('AC',4),('BA',5),('cda',6)`)
	tk.MustExec(`analyze table t`)
	tk.MustExec(`prepare stmt from 'select * from t where a IN (?,?,?)'`)
	tk.MustExec(`set @a=1,@b=3,@c=4`)
	tk.MustQuery(`execute stmt using @a,@b,@c`).Sort().Check(testkit.Rows("AC 4", "Ab 1", "BC 3"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(`execute stmt using @a,@b,@c`).Sort().Check(testkit.Rows("AC 4", "Ab 1", "BC 3"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0}, [][]any{
		{"IndexLookUp_7"},
		{"├─IndexRangeScan_5(Build)"},
		{"└─TableRowIDScan_6(Probe)"}})
	tk.MustExec(`set @a=2,@b=5,@c=4`)
	tk.MustQuery(`execute stmt using @a,@b,@c`).Sort().Check(testkit.Rows("AC 4", "BA 5", "abc 2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
}

func TestNonPreparedPlanCachePartitionIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec(`create table t (b varchar(255), a int primary key nonclustered, key (b)) partition by key(a) partitions 3`)
	// [Batch]PointGet does not use the plan cache,
	// since it is already using the fast path!
	tk.MustExec(`insert into t values ('Ab', 1),('abc',2),('BC',3),('AC',4),('BA',5),('cda',6)`)
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`explain format='plan_cache' select * from t where a IN (2,1,4,1,1,5,5)`).Check(testkit.Rows(""+
		"IndexLookUp_7 4.00 root partition:p1,p2 ",
		"├─IndexRangeScan_5(Build) 4.00 cop[tikv] table:t, index:PRIMARY(a) range:[1,1], [2,2], [4,4], [5,5], keep order:false",
		"└─TableRowIDScan_6(Probe) 4.00 cop[tikv] table:t keep order:false"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(`select * from t where a IN (2,1,4,1,1,5,5)`).Sort().Check(testkit.Rows("AC 4", "Ab 1", "BA 5", "abc 2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(`select * from t where a IN (1,3,4)`).Sort().Check(testkit.Rows("AC 4", "Ab 1", "BC 3"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(`select * from t where a IN (1,3,4)`).Sort().Check(testkit.Rows("AC 4", "Ab 1", "BC 3"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(`select * from t where a IN (2,5,4,2,5,5,1)`).Sort().Check(testkit.Rows("AC 4", "Ab 1", "BA 5", "abc 2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(`select * from t where a IN (1,2,3,4,5,5,1)`).Sort().Check(testkit.Rows("AC 4", "Ab 1", "BA 5", "BC 3", "abc 2"))
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(`select count(*) from t partition (p0)`).Check(testkit.Rows("0"))
	tk.MustQuery(`select count(*) from t partition (p1)`).Check(testkit.Rows("5"))
	tk.MustQuery(`select * from t partition (p2)`).Check(testkit.Rows("Ab 1"))

	tk.MustQuery(`explain format='plan_cache' select * from t where a = 2`).Check(testkit.Rows("Point_Get_1 1.00 root table:t, partition:p1, index:PRIMARY(a) "))
	tk.MustQuery(`explain format='plan_cache' select * from t where a = 2`).Check(testkit.Rows("Point_Get_1 1.00 root table:t, partition:p1, index:PRIMARY(a) "))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(`select * from t where a = 2`).Check(testkit.Rows("abc 2"))
	tk.MustExec(`create table tk (a int primary key nonclustered, b varchar(255), key (b)) partition by key (a) partitions 3`)
	tk.MustExec(`insert into tk select a, b from t`)
	tk.MustExec(`analyze table tk`)
	tk.MustQuery(`explain format='plan_cache' select * from tk where a = 2`).Check(testkit.Rows("Point_Get_1 1.00 root table:tk, partition:p1, index:PRIMARY(a) "))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	// PointGet will use Fast Plan, so no Plan Cache, even for Key Partitioned tables.
	tk.MustQuery(`select * from tk where a = 2`).Check(testkit.Rows("2 abc"))
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
}

func TestFixControl33031(t *testing.T) {
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

func TestPlanCachePartitionDuplicates(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
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
	ps := []*util.ProcessInfo{tkProcess}
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

func TestPreparedStmtIndexLookup(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, unique key (a))
partition by hash (a) partitions 3`)

	for i := 0; i < 100; i++ {
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
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).MultiCheckContain([]string{"PartitionUnion", "Batch_Point_Get", "partition:p0", "partition:p1"})
	tk.MustQuery(`execute stmt`)
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip prepared plan-cache: query accesses partitioned tables is un-cacheable if tidb_partition_pruning_mode = 'static'"))
}

type partCoverStruct struct {
	columns         []string
	keys            []string
	pointGetExplain []string
}

type partSQL struct {
	partSQL             string
	canUseBatchPointGet bool
}

func testPartitionFullCover(t *testing.T, tableDefSQL []partCoverStruct, partitionSQL []partSQL, useStringPK bool) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)
	seededRand := rand.New(rand.NewSource(seed))

	rows := 1000
	rowData := make(map[any]string, rows)
	ids := make([]any, 0, rows)
	maxRange := 2000000
	maxID := maxRange + 500000
	for i := 0; i < rows; i++ {
		var id any
		for createNew := true; createNew; _, createNew = rowData[id] {
			if useStringPK {
				id = randString(seededRand, 1, 20)
			} else {
				id = seededRand.Intn(maxID)
			}
		}
		rowData[id] = randString(seededRand, 1, 20)
		ids = append(ids, id)
	}
	filler := strings.Repeat("Filler", 1024/6)

	for i, testTbl := range tableDefSQL {
		cols := testTbl.columns
		keys := testTbl.keys
		seededRand.Shuffle(len(cols), func(i, j int) { cols[i], cols[j] = cols[j], cols[i] })
		seededRand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		tblDef := "(" +
			strings.Join(cols, ",\n")
		if len(keys) > 0 {
			tblDef += ",\n" + strings.Join(keys, ",\n")
		}
		tblDef += ")"

		tk.MustExec("CREATE TABLE tNorm " + tblDef)
		batchSize := 10
		for i := 0; i < len(ids); i += batchSize {
			sql := "INSERT INTO tNorm (a, b, c) VALUES "
			for j := 0; i+j < len(rowData) && j < batchSize; j++ {
				if useStringPK {
					if ids[i+j].(string) >= "t" {
						continue
					}
					if sql[len(sql)-1] == ')' {
						sql += ","
					}
					sql += "('" + ids[i+j].(string) + "', '" + rowData[ids[i+j]] + "', '" + filler + "')"
				} else {
					if ids[i+j].(int) >= maxRange {
						continue
					}
					if sql[len(sql)-1] == ')' {
						sql += ","
					}
					sql += "(" + strconv.Itoa(ids[i+j].(int)) + ", '" + rowData[ids[i+j].(int)] + "', '" + filler + "')"
				}
			}
			if sql[len(sql)-1] == ')' {
				tk.MustExec(sql)
			}
		}
		for j, part := range partitionSQL {
			currTest := fmt.Sprintf("t: %d, p:%d", i, j)
			comment := "/* " + currTest + " */"
			tk.MustExec("CREATE TABLE t " + tblDef + " " + part.partSQL + " " + comment)
			tk.MustExec("insert into t select * from tNorm " + comment)
			// Don't require global stats for using dynamic prune mode
			tk.MustExec(`set tidb_opt_fix_control='44262:ON' ` + comment)

			// Possible variations:
			// - static/dynamic tidb_partition_prune_mode

			preparedStmtPointGet(t, ids, tk, testTbl, seededRand, rowData, filler, currTest)
			nonPreparedStmtPointGet(t, ids, tk, testTbl, seededRand, rowData, filler, currTest)
			preparedStmtBatchPointGet(t, ids, tk, testTbl.pointGetExplain, seededRand, rowData, filler, currTest, part.canUseBatchPointGet)
			nonpreparedStmtBatchPointGet(t, ids, tk, testTbl.pointGetExplain, seededRand, rowData, filler, currTest, part.canUseBatchPointGet && testTbl.pointGetExplain != nil)

			tk.MustExec("drop table t")
		}
		tk.MustExec("DROP TABLE tNorm")
	}
}

/* TODO:
- KEY partitioning on multiple columns NOT SUPPORTED!
(LIST needs its own tailored test, due to each value needs to be
defined).
- LIST partitioning on a single column
- LIST partitioning on an expression (one or more columns) NOT SUPPORTED?
- LIST COLUMNS partitioning on a single column, varchar
- LIST COLUMNS partitioning on multiple columns NOT SUPPORTED!
- RANGE partitioning on other expressions or multiple columns NOT SUPPORTED?
- RANGE COLUMNS partitioning on a single column, datetime
- RANGE COLUMNS partitioning on multiple columns NOT SUPPORTED!
*/

func TestPartitionVarcharFullCover(t *testing.T) {
	tableDefSQL := []partCoverStruct{
		// Note: some partitioning functions does not work with bigint!
		{
			[]string{"a varchar(255) primary key", "b varchar(255)", "c text"},
			[]string{"key(b)"},
			[]string{"clustered index:PRIMARY(a)"},
		},
		{
			[]string{"a varchar(255) primary key", "b varchar(255)", "c text"},
			[]string{"key (b)"},
			[]string{"clustered index:PRIMARY(a)"},
		},
		{
			[]string{"a varchar(255)", "b varchar(255)", "c text"},
			[]string{"key (b)", "unique index a(a)"},
			[]string{"index:a"},
		},
		{
			[]string{"a varchar(255)", "b varchar(255)", "c text"},
			[]string{"key (b)"},
			nil,
		},
		{
			[]string{"a varchar(255)", "b varchar(255)", "c text"},
			[]string{"key (b)", "key (a)"},
			nil,
		},
	}
	partitionSQL := []partSQL{
		{
			"partition by range columns (a) (partition p0 values less than ('k'), partition p1 values less than ('x'))",
			false,
		},
		{
			"partition by key (a) partitions 7",
			false,
		},
	}
	testPartitionFullCover(t, tableDefSQL, partitionSQL, true)
}

func TestPartitionIntFullCover(t *testing.T) {
	tableDefSQL := []partCoverStruct{
		// Note: some partitioning functions does not work with bigint!
		{
			[]string{"a int unsigned primary key auto_increment", "b varchar(255)", "c text"},
			[]string{"key(b)"},
			[]string{"handle:"},
		},
		{
			[]string{"a int primary key auto_increment", "b varchar(255)", "c text"},
			[]string{"key (b)"},
			[]string{"handle:"},
		},
		{
			[]string{"a int", "b varchar(255)", "c text"},
			[]string{"key (b)", "unique index a(a)"},
			[]string{"index:a"},
		},
		{
			[]string{"a int", "b varchar(255)", "c text"},
			[]string{"key (b)"},
			nil,
		},
		{
			[]string{"a int", "b varchar(255)", "c text"},
			[]string{"key (b)", "key (a)"},
			nil,
		},
	}
	maxRange := 2000000
	partitionSQL := []partSQL{
		{
			"partition by range (a) (partition p0 values less than (1000000), partition p1 values less than (" + strconv.Itoa(maxRange) + "))",
			true,
		},
		{
			"partition by range (floor(a*0.5)*2) (partition p0 values less than (1000000), partition p1 values less than (" + strconv.Itoa(maxRange) + "))",
			false,
		},
		{
			"partition by hash (a) partitions 7",
			true,
		},
		{
			"partition by hash (floor(a*0.5)) partitions 3",
			// This is not yet enabled, and blocked by canConvertPointGet
			false,
		},
		{
			"partition by key (a) partitions 7",
			false,
		},
	}
	testPartitionFullCover(t, tableDefSQL, partitionSQL, false)
}

func getIDStr(id any) string {
	switch x := id.(type) {
	case int:
		return strconv.Itoa(x)
	case string:
		return "'" + x + "'"
	default:
		panic("Unsupported type")
	}
}

func preparedStmtPointGet(t *testing.T, ids []any, tk *testkit.TestKit, testTbl partCoverStruct, seededRand *rand.Rand, rowData map[any]string, filler, currTest string) {
	allCols := []string{"a", "b", "c", "space(1)"}
	seededRand.Shuffle(len(allCols), func(i, j int) {
		allCols[i], allCols[j] = allCols[j], allCols[i]
	})
	cols := allCols[:seededRand.Intn(len(allCols)-1)+1]
	// Test prepared statements
	colStr := strings.Join(cols, ",")
	queries := []string{"" +
		"select " + colStr + " from t where a = ?",
		// This uses an 'AccessCondition' for testing more
		// code paths
		"select " + colStr + " from t where a = ? and b is not null",
	}
	for i, q := range queries {
		comment := fmt.Sprintf("/* %s, q:%d */", currTest, i)
		id := ids[seededRand.Intn(len(ids))]
		var idStr string
		switch x := id.(type) {
		case int:
			idStr = strconv.Itoa(x)
		case string:
			idStr = "'" + x + "'"
		default:
			require.False(t, true, "Unsupported type")
		}
		tk.MustExec(`prepare stmt from '` + q + `' ` + comment)
		tk.MustExec(`set @a := ` + idStr + " " + comment)
		expect := getRowData(rowData, filler, cols, id)
		tk.MustQuery(`execute stmt using @a ` + comment).Check(testkit.Rows(expect...))
		require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
		id = ids[seededRand.Intn(len(ids))]
		idStr = getIDStr(id)
		tk.MustExec(`set @a := ` + idStr)
		expect = getRowData(rowData, filler, cols, id)
		tk.MustQuery(`execute stmt using @a ` + comment).Check(testkit.Rows(expect...))
		require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
		tkProcess := tk.Session().ShowProcess()
		ps := []*util.ProcessInfo{tkProcess}
		tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
		res := tk.MustQuery(fmt.Sprintf("explain for connection %d "+comment, tkProcess.ID))
		if len(testTbl.pointGetExplain) > 0 {
			res.MultiCheckContain(
				append([]string{"Point_Get"}, testTbl.pointGetExplain...))
		} else {
			res.CheckNotContain("Point_Get")
		}
		tk.MustExec(`deallocate prepare stmt`)
	}
}

func getRowData(rowData map[any]string, filler string, cols []string, id ...any) []string {
	maxRange := 2000000
	ret := make([]string, 0, len(id))
	dup := make(map[any]struct{}, len(id))
	for i := range id {
		isStr := false
		switch x := id[i].(type) {
		case int:
			if x >= maxRange {
				continue
			}
		case string:
			if x >= "t" {
				continue
			}
			isStr = true
		default:
			panic("Unsupported type")
		}
		if _, ok := dup[id[i]]; ok {
			continue
		}
		var row string
		for j, col := range cols {
			if j > 0 {
				row += " "
			}
			switch col {
			case "a":
				if isStr {
					row += id[i].(string)
				} else {
					row += strconv.Itoa(id[i].(int))
				}
			case "b":
				row += rowData[id[i]]
			case "c":
				row += filler
			case "space(1)":
				row += " "
			}
		}
		ret = append(ret, row)
		dup[id[i]] = struct{}{}
	}
	sort.Strings(ret)
	return ret
}

func getRandCols(seededRand *rand.Rand) ([]string, bool) {
	allCols := []string{"a", "b", "c", "space(1)"}
	seededRand.Shuffle(len(allCols), func(i, j int) {
		allCols[i], allCols[j] = allCols[j], allCols[i]
	})
	cols := allCols[:seededRand.Intn(len(allCols)-1)+1]
	hasSpaceCol := false
	for _, col := range cols {
		if col == "space(1)" {
			hasSpaceCol = true
			break
		}
	}
	return cols, hasSpaceCol
}

func preparedStmtBatchPointGet(t *testing.T, ids []any, tk *testkit.TestKit, pointGetExplain []string, seededRand *rand.Rand, rowData map[any]string, filler, currTest string, canUseBatchPointGet bool) {
	// Test prepared statements
	cols, hasSpaceCol := getRandCols(seededRand)
	queries := []struct {
		sql               string
		usesBatchPointGet bool
	}{
		{
			"select " + strings.Join(cols, ",") + " from t where a IN (?,?,?)",
			true,
			// Cannot convert to [Batch]Point get, due to dynamic pruning
		},
		{
			"select " + strings.Join(cols, ",") + " from t where a = ? or a = ? or a = ?",
			// See canConvertPointGet, just needs to be enabled :)
			false,
		},
		{
			// This uses an 'AccessCondition' for testing more
			// code paths
			"select " + strings.Join(cols, ",") + " from t where a IN (?,?,?) and b is not null",
			// Currently not enabled, since not only an IN (in tryWhereIn2BatchPointGet)
			// or have multiple values which does not yet enabled through canConvertPointGet.
			false,
		},
	}
	for i, q := range queries {
		comment := fmt.Sprintf("/* %s, q:%d */", currTest, i)
		// TODO: Test corner cases, where:
		// - No values matching a partition
		// - some values does not match any partition
		// - duplicate values
		a, b, c := ids[seededRand.Intn(len(ids))], ids[seededRand.Intn(len(ids))], ids[seededRand.Intn(len(ids))]
		tk.MustExec(`prepare stmt from '` + q.sql + `' ` + comment)
		tk.MustExec(fmt.Sprintf(`set @a := %s, @b := %s, @c := %s %s`, getIDStr(a), getIDStr(b), getIDStr(c), comment))
		expect := getRowData(rowData, filler, cols, a, b, c)
		tk.MustQuery(`execute stmt using @a, @b, @c ` + comment).Sort().Check(testkit.Rows(expect...))
		require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
		tkProcess := tk.Session().ShowProcess()
		ps := []*util.ProcessInfo{tkProcess}
		tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
		res := tk.MustQuery(fmt.Sprintf("explain for connection %d "+comment, tkProcess.ID))
		if q.usesBatchPointGet &&
			len(pointGetExplain) > 0 &&
			canUseBatchPointGet &&
			!hasSpaceCol {
			res.MultiCheckContain(
				append([]string{"Batch_Point_Get"}, pointGetExplain...))
		} else {
			res.CheckNotContain("Batch_Point_Get")
		}
		a2, b2, c2 := ids[seededRand.Intn(len(ids))], ids[seededRand.Intn(len(ids))], ids[seededRand.Intn(len(ids))]
		tk.MustExec(fmt.Sprintf(`set @a := %s, @b := %s, @c := %s %s`, getIDStr(a2), getIDStr(b2), getIDStr(c2), comment))
		expect = getRowData(rowData, filler, cols, a2, b2, c2)
		tk.MustQuery(`execute stmt using @a, @b, @c ` + comment).Sort().Check(testkit.Rows(expect...))
		if !tk.Session().GetSessionVars().FoundInPlanCache {
			warn := tk.MustQuery("show warnings " + comment)
			// previous plan removed at least one of the duplicate
			// argument.
			require.Equal(t, "Warning", warn.Rows()[0][0])
			require.Equal(t, "1105", warn.Rows()[0][1])
			// skip plan-cache: plan rebuild failed, rebuild to get an unsafe range, Handles length diff
			// skip plan-cache: plan rebuild failed, rebuild to get an unsafe range, IndexValue length diff
			warn.MultiCheckContain([]string{"skip plan-cache: plan rebuild failed, rebuild to get an unsafe range, ", " length diff"})
		}
		tk.MustExec(`deallocate prepare stmt`)
	}
}

func nonPreparedStmtPointGet(t *testing.T, ids []any, tk *testkit.TestKit, testTbl partCoverStruct, seededRand *rand.Rand, rowData map[any]string, filler, comment string) {
	// Test non-prepared statements
	// FastPlan will be used instead of checking plan cache!
	usePlanCache := len(testTbl.pointGetExplain) == 0
	tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=1`)
	id := ids[seededRand.Intn(len(ids))]
	idStr := getIDStr(id)
	cols, hasSpaceCol := getRandCols(seededRand)
	sql := `select ` + strings.Join(cols, ",") + ` from t where a = `
	tk.MustQuery(sql + idStr).Check(testkit.Rows(getRowData(rowData, filler, cols, id)...))
	prevID := id
	require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	id = ids[seededRand.Intn(len(ids))]
	idStr = getIDStr(id)
	tk.MustQuery(sql + idStr).Check(testkit.Rows(getRowData(rowData, filler, cols, id)...))
	if usePlanCache != tk.Session().GetSessionVars().FoundInPlanCache {
		require.Equal(t, usePlanCache || hasSpaceCol, tk.Session().GetSessionVars().FoundInPlanCache, fmt.Sprintf("id: %d, prev id: %d", id, prevID))
	}
	id = ids[seededRand.Intn(len(ids))]
	idStr = getIDStr(id)
	tk.MustQuery(sql + idStr).Check(testkit.Rows(getRowData(rowData, filler, cols, id)...))
	if usePlanCache || hasSpaceCol != tk.Session().GetSessionVars().FoundInPlanCache {
		require.Equal(t, usePlanCache || hasSpaceCol, tk.Session().GetSessionVars().FoundInPlanCache)
	}
	id = ids[seededRand.Intn(len(ids))]
	idStr = getIDStr(id)
	tk.MustQuery(sql + idStr).Check(testkit.Rows(getRowData(rowData, filler, cols, id)...))
	require.Equal(t, usePlanCache || hasSpaceCol, tk.Session().GetSessionVars().FoundInPlanCache)
	if usePlanCache {
		tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=0`)
		id = ids[seededRand.Intn(len(ids))]
		idStr = getIDStr(id)
		tk.MustQuery(sql + idStr).Check(testkit.Rows(getRowData(rowData, filler, cols, id)...))
		require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
	}
}

func nonpreparedStmtBatchPointGet(t *testing.T, ids []any, tk *testkit.TestKit, pointGetExplain []string, seededRand *rand.Rand, rowData map[any]string, filler, currTest string, canUseBatchPointGet bool) {
	// Test prepared statements
	usePlanCache := len(pointGetExplain) == 0
	tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=1`)
	// TODO: Fix columns
	cols, hasSpaceCol := getRandCols(seededRand)
	sql := `select ` + strings.Join(cols, ",") + ` from t where `
	queries := []struct {
		sql               string
		usesBatchPointGet bool
		canUsePlanCache   bool
	}{
		{
			sql + " a IN (%s,%s,%s)",
			true,
			true,
		},

		{
			sql + " a = %s or a = %s or a = %s",
			// See canConvertPointGet, just needs to be enabled :)
			false,
			true,
		},
		{
			// This uses an 'AccessCondition' for testing more
			// code paths
			sql + " a IN (%s,%s,%s) and b is not null",
			// Currently not enabled, since not only an IN (in tryWhereIn2BatchPointGet)
			// or have multiple values which does not yet enabled through canConvertPointGet.
			false,
			//
			false,
		},
	}
	for i, q := range queries {
		comment := fmt.Sprintf("/* %s, q:%d */", currTest, i)
		// TODO: Test corner cases, where:
		// - No values matching a partition
		// - some values does not match any partition
		a, b, c := ids[seededRand.Intn(len(ids))], ids[seededRand.Intn(len(ids))], ids[seededRand.Intn(len(ids))]
		query := fmt.Sprintf(q.sql+" %s", getIDStr(a), getIDStr(b), getIDStr(c), comment)
		tk.MustQuery(query).Sort().Check(testkit.Rows(getRowData(rowData, filler, cols, a, b, c)...))
		require.False(t, tk.Session().GetSessionVars().FoundInPlanCache)
		a, b, c = ids[seededRand.Intn(len(ids))], ids[seededRand.Intn(len(ids))], ids[seededRand.Intn(len(ids))]
		query = fmt.Sprintf(q.sql+" %s", getIDStr(a), getIDStr(b), getIDStr(c), comment)
		tk.MustQuery(query).Sort().Check(testkit.Rows(getRowData(rowData, filler, cols, a, b, c)...))
		if q.canUsePlanCache && usePlanCache && !tk.Session().GetSessionVars().FoundInPlanCache {
			tk.MustQuery("show warnings " + comment).Check(testkit.Rows("Warning 1105 skip prepared plan-cache: Batch/PointGet plans may be over-optimized"))
		}
		res := tk.MustQuery(fmt.Sprintf("explain %s", query))
		if len(pointGetExplain) > 0 && canUseBatchPointGet && q.usesBatchPointGet && !hasSpaceCol {
			res.MultiCheckContain(
				append([]string{"Batch_Point_Get"}, pointGetExplain...))
		} else {
			res.CheckNotContain("Batch_Point_Get")
		}
	}
}

// randString generates a random string between min and max length
// with [0-9a-zA-Z]
// Copy from expression/bench_test.go - randString
func randString(r *rand.Rand, minv, maxv int) string {
	n := minv + r.Intn(maxv)
	buf := make([]byte, n)
	for i := range buf {
		x := r.Intn(62)
		if x < 10 {
			buf[i] = byte('0' + x)
		} else if x-10 < 26 {
			buf[i] = byte('a' + x - 10)
		} else {
			buf[i] = byte('A' + x - 10 - 26)
		}
	}
	return string(buf)
}
