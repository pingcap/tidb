// Copyright 2019 PingCAP, Inc.
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

package indexmergereadtest

import (
	"cmp"
	"context"
	"fmt"
	"math/rand"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestIndexMergePickAndExecTaskPanic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(id int primary key, a int, b int, c int, d int)")
	tk.MustExec("create index t1a on t1(a)")
	tk.MustExec("create index t1b on t1(b)")
	tk.MustExec("insert into t1 values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5)")
	tk.MustQuery("select /*+ use_index_merge(t1, primary, t1a) */ * from t1 where id < 2 or a > 4 order by id").Check(testkit.Rows("1 1 1 1 1",
		"5 5 5 5 5"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergePickAndExecTaskPanic", "panic(\"pickAndExecTaskPanic\")"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/testIndexMergePickAndExecTaskPanic"))
	}()
	err := tk.QueryToErr("select /*+ use_index_merge(t1, primary, t1a) */ * from t1 where id < 2 or a > 4 order by id")
	require.Contains(t, err.Error(), "pickAndExecTaskPanic")
}

func TestPartitionTableRandomIndexMerge(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_index_merge=1")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec(`create table t (a int, b int, key(a), key(b))
		partition by range (a) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than (30),
		partition p4 values less than (40))`)
	tk.MustExec(`create table tnormal (a int, b int, key(a), key(b))`)

	values := make([]string, 0, 32)
	for i := 0; i < 32; i++ {
		values = append(values, fmt.Sprintf("(%v, %v)", rand.Intn(10), rand.Intn(10)))
	}
	tk.MustExec(fmt.Sprintf("insert into t values %v", strings.Join(values, ", ")))
	tk.MustExec(fmt.Sprintf("insert into tnormal values %v", strings.Join(values, ", ")))

	randRange := func() (int, int) {
		a, b := rand.Intn(10), rand.Intn(10)
		if a > b {
			return b, a
		}
		return a, b
	}
	for i := 0; i < 32; i++ {
		la, ra := randRange()
		lb, rb := randRange()
		cond := fmt.Sprintf("(a between %v and %v) or (b between %v and %v)", la, ra, lb, rb)
		result := tk.MustQuery("select * from tnormal where " + cond).Sort().Rows()
		tk.MustQuery("select /*+ USE_INDEX_MERGE(t, a, b) */ * from t where " + cond).Sort().Check(result)
	}
}

func TestPartitionTableRandomIndexMerge2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_index_merge=1")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")

	// test a table with a primary key
	tk.MustExec(`create table tpk (a int primary key, b int, key(b))
		partition by range (a) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than (30),
		partition p4 values less than (40))`)
	tk.MustExec(`create table tnormal (a int, b int, key(a), key(b))`)

	randRange := func() (int, int) {
		a, b := rand.Intn(10), rand.Intn(10)
		if a > b {
			return b, a
		}
		return a, b
	}
	values := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		values = append(values, fmt.Sprintf("(%v, %v)", i, rand.Intn(10)))
	}
	tk.MustExec(fmt.Sprintf("insert into tpk values %v", strings.Join(values, ", ")))
	tk.MustExec(fmt.Sprintf("insert into tnormal values %v", strings.Join(values, ", ")))
	for i := 0; i < 32; i++ {
		la, ra := randRange()
		lb, rb := randRange()
		cond := fmt.Sprintf("(a between %v and %v) or (b between %v and %v)", la, ra, lb, rb)
		result := tk.MustQuery("select * from tnormal where " + cond).Sort().Rows()
		tk.MustQuery("select /*+ USE_INDEX_MERGE(tpk, a, b) */ * from tpk where " + cond).Sort().Check(result)
	}
}

func TestIndexMergeWithPreparedStmt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec(`set tidb_enable_prepared_plan_cache=0`)
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int, c2 int, c3 int, key(c1), key(c2));")
	insertStr := "insert into t1 values(0, 0, 0)"
	for i := 1; i < 100; i++ {
		insertStr += fmt.Sprintf(", (%d, %d, %d)", i, i, i)
	}
	tk.MustExec(insertStr)

	tk.MustExec("prepare stmt1 from 'select /*+ use_index_merge(t1) */ count(1) from t1 where c1 < ? or c2 < ?';")
	tk.MustExec("set @a = 10;")
	tk.MustQuery("execute stmt1 using @a, @a;").Check(testkit.Rows("10"))
	tk.Session().SetSessionManager(&testkit.MockSessionManager{
		PS: []*util.ProcessInfo{tk.Session().ShowProcess()},
	})
	explainStr := "explain for connection " + strconv.FormatUint(tk.Session().ShowProcess().ID, 10)
	res := tk.MustQuery(explainStr)
	indexMergeLine := res.Rows()[1][0].(string)
	re, err := regexp.Compile(".*IndexMerge.*")
	require.NoError(t, err)
	require.True(t, re.MatchString(indexMergeLine))

	tk.MustExec("prepare stmt1 from 'select /*+ use_index_merge(t1) */ count(1) from t1 where c1 < ? or c2 < ? and c3';")
	tk.MustExec("set @a = 10;")
	tk.MustQuery("execute stmt1 using @a, @a;").Check(testkit.Rows("10"))
	res = tk.MustQuery(explainStr)
	indexMergeLine = res.Rows()[1][0].(string)
	require.True(t, re.MatchString(indexMergeLine))
}

func TestIndexMergeReaderMemTracker(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1(c1 int, c2 int, c3 int, key(c1), key(c2), key(c3));")

	insertStr := "insert into t1 values(0, 0, 0)"
	rowNum := 1000
	for i := 0; i < rowNum; i++ {
		insertStr += fmt.Sprintf(" ,(%d, %d, %d)", i, i, i)
	}
	insertStr += ";"
	memTracker := tk.Session().GetSessionVars().MemTracker

	tk.MustExec(insertStr)

	// We select all rows in t1, so the mem usage is more clear.
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where c1 > 1 or c2 > 1")

	memUsage := memTracker.MaxConsumed()
	require.Greater(t, memUsage, int64(0))

	res := tk.MustQuery("explain analyze select /*+ use_index_merge(t1) */ * from t1 where c1 > 1 or c2 > 1")
	require.Len(t, res.Rows(), 4)
	// Parse "xxx KB" and check it's greater than 0.
	memStr := res.Rows()[0][7].(string)
	re, err := regexp.Compile("[0-9]+ KB")
	require.NoError(t, err)
	require.True(t, re.MatchString(memStr))
	bytes, err := strconv.ParseFloat(memStr[:len(memStr)-3], 32)
	require.NoError(t, err)
	require.Greater(t, bytes, 0.0)
}

func TestPessimisticLockOnPartitionForIndexMerge(t *testing.T) {
	// Same purpose with TestPessimisticLockOnPartition, but test IndexMergeReader.
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec(`create table t1 (c_datetime datetime, c1 int, c2 int, primary key (c_datetime) NONCLUSTERED, key(c1), key(c2))
			partition by range (to_days(c_datetime)) (
				partition p0 values less than (to_days('2020-02-01')),
				partition p1 values less than (to_days('2020-04-01')),
				partition p2 values less than (to_days('2020-06-01')),
				partition p3 values less than maxvalue)`)
	tk.MustExec("create table t2 (c_datetime datetime, unique key(c_datetime))")
	tk.MustExec("insert into t1 values ('2020-06-26 03:24:00', 1, 1), ('2020-02-21 07:15:33', 2, 2), ('2020-04-27 13:50:58', 3, 3)")
	tk.MustExec("insert into t2 values ('2020-01-10 09:36:00'), ('2020-02-04 06:00:00'), ('2020-06-12 03:45:18')")
	tk.MustExec("analyze table t1")
	tk.MustExec("analyze table t2")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("set @@tidb_partition_prune_mode = 'static'")

	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
	tk.MustExec("begin pessimistic")
	tk.MustQuery(`explain format='brief' select /*+ use_index_merge(t1) */ c1 from t1 join t2
			on t1.c_datetime >= t2.c_datetime
			where t1.c1 < 10 or t1.c2 < 10 for update`).Check(testkit.Rows(
		"Projection 16635.64 root  test.t1.c1",
		"└─SelectLock 16635.64 root  for update 0",
		"  └─Projection 16635.64 root  test.t1.c1, test.t1._tidb_rowid, test.t1._tidb_tid, test.t2._tidb_rowid",
		"    └─HashJoin 16635.64 root  CARTESIAN inner join, other cond:ge(test.t1.c_datetime, test.t2.c_datetime)",
		"      ├─IndexReader(Build) 3.00 root  index:IndexFullScan",
		"      │ └─IndexFullScan 3.00 cop[tikv] table:t2, index:c_datetime(c_datetime) keep order:false",
		"      └─PartitionUnion(Probe) 5545.21 root  ",
		"        ├─Projection 5542.21 root  test.t1.c_datetime, test.t1.c1, test.t1._tidb_rowid, test.t1._tidb_tid",
		"        │ └─IndexMerge 5542.21 root  type: union",
		"        │   ├─IndexRangeScan(Build) 3323.33 cop[tikv] table:t1, partition:p0, index:c1(c1) range:[-inf,10), keep order:false, stats:pseudo",
		"        │   ├─IndexRangeScan(Build) 3323.33 cop[tikv] table:t1, partition:p0, index:c2(c2) range:[-inf,10), keep order:false, stats:pseudo",
		"        │   └─TableRowIDScan(Probe) 5542.21 cop[tikv] table:t1, partition:p0 keep order:false, stats:pseudo",
		"        ├─Projection 1.00 root  test.t1.c_datetime, test.t1.c1, test.t1._tidb_rowid, test.t1._tidb_tid",
		"        │ └─IndexMerge 1.00 root  type: union",
		"        │   ├─IndexRangeScan(Build) 1.00 cop[tikv] table:t1, partition:p1, index:c1(c1) range:[-inf,10), keep order:false",
		"        │   ├─IndexRangeScan(Build) 1.00 cop[tikv] table:t1, partition:p1, index:c2(c2) range:[-inf,10), keep order:false",
		"        │   └─TableRowIDScan(Probe) 1.00 cop[tikv] table:t1, partition:p1 keep order:false",
		"        ├─Projection 1.00 root  test.t1.c_datetime, test.t1.c1, test.t1._tidb_rowid, test.t1._tidb_tid",
		"        │ └─IndexMerge 1.00 root  type: union",
		"        │   ├─IndexRangeScan(Build) 1.00 cop[tikv] table:t1, partition:p2, index:c1(c1) range:[-inf,10), keep order:false",
		"        │   ├─IndexRangeScan(Build) 1.00 cop[tikv] table:t1, partition:p2, index:c2(c2) range:[-inf,10), keep order:false",
		"        │   └─TableRowIDScan(Probe) 1.00 cop[tikv] table:t1, partition:p2 keep order:false",
		"        └─Projection 1.00 root  test.t1.c_datetime, test.t1.c1, test.t1._tidb_rowid, test.t1._tidb_tid",
		"          └─IndexMerge 1.00 root  type: union",
		"            ├─IndexRangeScan(Build) 1.00 cop[tikv] table:t1, partition:p3, index:c1(c1) range:[-inf,10), keep order:false",
		"            ├─IndexRangeScan(Build) 1.00 cop[tikv] table:t1, partition:p3, index:c2(c2) range:[-inf,10), keep order:false",
		"            └─TableRowIDScan(Probe) 1.00 cop[tikv] table:t1, partition:p3 keep order:false",
	))
	tk.MustQuery(`select /*+ use_index_merge(t1) */ c1 from t1 join t2
			on t1.c_datetime >= t2.c_datetime
			where t1.c1 < 10 or t1.c2 < 10 for update`).Sort().Check(testkit.Rows("1", "1", "1", "2", "2", "3", "3"))
	tk1.MustExec("begin pessimistic")

	ch := make(chan int32, 5)
	go func() {
		tk1.MustExec("update t1 set c_datetime = '2020-06-26 03:24:00' where c1 = 1")
		ch <- 0
		tk1.MustExec("rollback")
		ch <- 0
	}()

	// Leave 50ms for tk1 to run, tk1 should be blocked at the update operation.
	time.Sleep(50 * time.Millisecond)
	ch <- 1

	tk.MustExec("commit")
	// tk1 should be blocked until tk commit, check the order.
	require.Equal(t, <-ch, int32(1))
	require.Equal(t, <-ch, int32(0))
	<-ch // wait for goroutine to quit.
}

func TestIndexMergeIntersectionConcurrency(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int, c2 bigint, c3 bigint, primary key(c1), key(c2), key(c3)) partition by hash(c1) partitions 10;")
	tk.MustExec("insert into t1 values(1, 1, 3000), (2, 1, 1)")
	tk.MustExec("analyze table t1;")
	tk.MustExec("set tidb_partition_prune_mode = 'dynamic'")
	res := tk.MustQuery("explain select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Rows()
	require.Contains(t, res[1][0], "IndexMerge")

	// Default is tidb_executor_concurrency.
	res = tk.MustQuery("select @@tidb_executor_concurrency;").Sort().Rows()
	defExecCon := res[0][0].(string)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergeIntersectionConcurrency", fmt.Sprintf("return(%s)", defExecCon)))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/testIndexMergeIntersectionConcurrency"))
	}()
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))

	tk.MustExec("set tidb_executor_concurrency = 10")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergeIntersectionConcurrency", "return(10)"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))
	// workerCnt = min(part_num, concurrency)
	tk.MustExec("set tidb_executor_concurrency = 20")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergeIntersectionConcurrency", "return(10)"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))
	tk.MustExec("set tidb_executor_concurrency = 2")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergeIntersectionConcurrency", "return(2)"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))

	tk.MustExec("set tidb_index_merge_intersection_concurrency = 9")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergeIntersectionConcurrency", "return(9)"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))
	tk.MustExec("set tidb_index_merge_intersection_concurrency = 21")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergeIntersectionConcurrency", "return(10)"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))
	tk.MustExec("set tidb_index_merge_intersection_concurrency = 3")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergeIntersectionConcurrency", "return(3)"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))

	// Concurrency only works for dynamic pruning partition table, so real concurrency is 1.
	tk.MustExec("set tidb_partition_prune_mode = 'static'")
	tk.MustExec("set tidb_index_merge_intersection_concurrency = 9")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergeIntersectionConcurrency", "return(1)"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))

	// Concurrency only works for dynamic pruning partition table. so real concurrency is 1.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int, c2 bigint, c3 bigint, primary key(c1), key(c2), key(c3));")
	tk.MustExec("insert into t1 values(1, 1, 3000), (2, 1, 1)")
	tk.MustExec("set tidb_index_merge_intersection_concurrency = 9")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergeIntersectionConcurrency", "return(1)"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))
}

func TestIntersectionWithDifferentConcurrency(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	var execCon []int
	tblSchemas := []string{
		// partition table
		"create table t1(c1 int, c2 bigint, c3 bigint, primary key(c1), key(c2), key(c3)) partition by hash(c1) partitions 10;",
		// non-partition table
		"create table t1(c1 int, c2 bigint, c3 bigint, primary key(c1), key(c2), key(c3));",
	}

	for tblIdx, tblSchema := range tblSchemas {
		if tblIdx == 0 {
			// Test different intersectionProcessWorker with partition table(10 partitions).
			execCon = []int{1, 3, 10, 11, 20}
		} else {
			// Default concurrency.
			execCon = []int{5}
		}
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1;")
		tk.MustExec(tblSchema)

		const queryCnt int = 5
		const rowCnt int = 100
		curRowCnt := 0
		insertStr := "insert into t1 values"
		for i := 0; i < rowCnt; i++ {
			if i != 0 {
				insertStr += ", "
			}
			insertStr += fmt.Sprintf("(%d, %d, %d)", i, rand.Int(), rand.Int())
			curRowCnt++
		}
		tk.MustExec(insertStr)
		tk.MustExec("analyze table t1")

		for _, concurrency := range execCon {
			tk.MustExec(fmt.Sprintf("set tidb_executor_concurrency = %d", concurrency))
			for i := 0; i < 2; i++ {
				sql := "select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024"
				if i == 0 {
					// Dynamic mode.
					tk.MustExec("set tidb_partition_prune_mode = 'dynamic'")
					tk.MustHavePlan(sql, "IndexMerge")
					tk.MustNotHavePlan(sql, "PartitionUnion")
				} else {
					tk.MustExec("set tidb_partition_prune_mode = 'static'")
					if tblIdx == 0 {
						// partition table
						tk.MustHavePlan(sql, "IndexMerge")
						tk.MustHavePlan(sql, "PartitionUnion")
					} else {
						tk.MustHavePlan(sql, "IndexMerge")
						tk.MustNotHavePlan(sql, "PartitionUnion")
					}
				}
				for i := 0; i < queryCnt; i++ {
					c3 := rand.Intn(1024)
					res := tk.MustQuery(fmt.Sprintf("select /*+ no_index_merge() */ c1 from t1 where c2 < 1024 and c3 > %d", c3)).Sort().Rows()
					tk.MustQuery(fmt.Sprintf("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > %d", c3)).Sort().Check(res)
				}

				// In transaction
				for i := 0; i < queryCnt; i++ {
					tk.MustExec("begin;")
					r := rand.Intn(3)
					if r == 0 {
						tk.MustExec(fmt.Sprintf("update t1 set c3 = %d where c1 = %d", rand.Int(), rand.Intn(rowCnt)))
					} else if r == 1 {
						tk.MustExec(fmt.Sprintf("delete from t1 where c1 = %d", rand.Intn(rowCnt)))
					} else if r == 2 {
						tk.MustExec(fmt.Sprintf("insert into t1 values(%d, %d, %d)", curRowCnt, rand.Int(), rand.Int()))
						curRowCnt++
					}
					c3 := rand.Intn(1024)
					res := tk.MustQuery(fmt.Sprintf("select /*+ no_index_merge() */ c1 from t1 where c2 < 1024 and c3 > %d", c3)).Sort().Rows()
					tk.MustQuery(fmt.Sprintf("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > %d", c3)).Sort().Check(res)
					tk.MustExec("commit;")
				}
			}
		}
		tk.MustExec("drop table t1")
	}
}

func TestIntersectionWorkerPanic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int, c2 bigint, c3 bigint, primary key(c1), key(c2), key(c3)) partition by hash(c1) partitions 10;")
	tk.MustExec("insert into t1 values(1, 1, 3000), (2, 1, 1)")
	tk.MustExec("analyze table t1;")
	tk.MustExec("set tidb_partition_prune_mode = 'dynamic'")
	res := tk.MustQuery("explain select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Rows()
	require.Contains(t, res[1][0], "IndexMerge")

	// Test panic in intersection.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergeIntersectionWorkerPanic", `panic("testIndexMergeIntersectionWorkerPanic")`))
	err := tk.QueryToErr("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024")
	require.Contains(t, err.Error(), "testIndexMergeIntersectionWorkerPanic")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/testIndexMergeIntersectionWorkerPanic"))
}

func setupPartitionTableHelper(tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int, c2 bigint, c3 bigint, primary key(c1), key(c2), key(c3));")
	insertStr := "insert into t1 values(0, 0, 0)"
	for i := 1; i < 500; i++ {
		insertStr += fmt.Sprintf(", (%d, %d, %d)", i, i, i)
	}
	tk.MustExec(insertStr)
	tk.MustExec("analyze table t1;")
	tk.MustExec("set tidb_partition_prune_mode = 'dynamic'")
}

func TestIndexMergeProcessWorkerHang(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	setupPartitionTableHelper(tk)

	var err error
	sql := "select /*+ use_index_merge(t1) */ c1 from t1 where c1 < 900 or c2 < 1000;"
	res := tk.MustQuery("explain " + sql).Rows()
	require.Contains(t, res[1][0], "IndexMerge")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergeMainReturnEarly", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergeProcessWorkerUnionHang", "return(true)"))
	err = tk.QueryToErr(sql)
	require.Contains(t, err.Error(), "testIndexMergeMainReturnEarly")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/testIndexMergeMainReturnEarly"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/testIndexMergeProcessWorkerUnionHang"))

	sql = "select /*+ use_index_merge(t1, c2, c3) */ c1 from t1 where c2 < 900 and c3 < 1000;"
	res = tk.MustQuery("explain " + sql).Rows()
	require.Contains(t, res[1][0], "IndexMerge")
	require.Contains(t, res[1][4], "intersection")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergeMainReturnEarly", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergeProcessWorkerIntersectionHang", "return(true)"))
	err = tk.QueryToErr(sql)
	require.Contains(t, err.Error(), "testIndexMergeMainReturnEarly")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/testIndexMergeMainReturnEarly"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/testIndexMergeProcessWorkerIntersectionHang"))
}

var indexMergePanicRunSQL = func(t *testing.T, tk *testkit.TestKit, fp string) {
	minV := 200
	maxV := 1000
	var sql string
	v1 := rand.Intn(maxV-minV) + minV
	v2 := rand.Intn(maxV-minV) + minV
	if !strings.Contains(fp, "Intersection") {
		sql = fmt.Sprintf("select /*+ use_index_merge(t1) */ c1 from t1 where c1 < %d or c2 < %d;", v1, v2)
	} else {
		sql = fmt.Sprintf("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c3 < %d and c2 < %d", v1, v2)
	}
	res := tk.MustQuery("explain " + sql).Rows()
	require.Contains(t, res[1][0], "IndexMerge")
	err := tk.QueryToErr(sql)
	require.Contains(t, err.Error(), fp)
}

func TestIndexMergePanic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	setupPartitionTableHelper(tk)

	// TestIndexMergePanicPartialIndexWorker
	fp := "github.com/pingcap/tidb/pkg/executor/testIndexMergePanicPartialIndexWorker"
	require.NoError(t, failpoint.Enable(fp, fmt.Sprintf(`panic("%s")`, fp)))
	for i := 0; i < 100; i++ {
		indexMergePanicRunSQL(t, tk, fp)
	}
	require.NoError(t, failpoint.Disable(fp))

	// TestIndexMergePanicPartialTableWorker
	fp = "github.com/pingcap/tidb/pkg/executor/testIndexMergePanicPartialTableWorker"
	require.NoError(t, failpoint.Enable(fp, fmt.Sprintf(`panic("%s")`, fp)))
	for i := 0; i < 100; i++ {
		indexMergePanicRunSQL(t, tk, fp)
	}
	require.NoError(t, failpoint.Disable(fp))

	// TestIndexMergePanicProcessWorkerUnion
	fp = "github.com/pingcap/tidb/pkg/executor/testIndexMergePanicProcessWorkerUnion"
	require.NoError(t, failpoint.Enable(fp, fmt.Sprintf(`panic("%s")`, fp)))
	for i := 0; i < 100; i++ {
		indexMergePanicRunSQL(t, tk, fp)
	}
	require.NoError(t, failpoint.Disable(fp))

	// TestIndexMergePanicProcessWorkerIntersection
	fp = "github.com/pingcap/tidb/pkg/executor/testIndexMergePanicProcessWorkerIntersection"
	require.NoError(t, failpoint.Enable(fp, fmt.Sprintf(`panic("%s")`, fp)))
	for i := 0; i < 100; i++ {
		indexMergePanicRunSQL(t, tk, fp)
	}
	require.NoError(t, failpoint.Disable(fp))

	// TestIndexMergePanicPartitionTableIntersectionWorker
	fp = "github.com/pingcap/tidb/pkg/executor/testIndexMergePanicPartitionTableIntersectionWorker"
	require.NoError(t, failpoint.Enable(fp, fmt.Sprintf(`panic("%s")`, fp)))
	for i := 0; i < 100; i++ {
		indexMergePanicRunSQL(t, tk, fp)
	}
	require.NoError(t, failpoint.Disable(fp))

	// TestIndexMergePanicTableScanWorker
	fp = "github.com/pingcap/tidb/pkg/executor/testIndexMergePanicTableScanWorker"
	require.NoError(t, failpoint.Enable(fp, fmt.Sprintf(`panic("%s")`, fp)))
	for i := 0; i < 100; i++ {
		indexMergePanicRunSQL(t, tk, fp)
	}
	require.NoError(t, failpoint.Disable(fp))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int, c2 bigint, c3 bigint, primary key(c1), key(c2), key(c3));")
	tk.MustExec("insert into t1 values(1, 1, 1), (100, 100, 100)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergeResultChCloseEarly", "return(true)"))
	tk.MustExec("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c1 < 100 or c2 < 100")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/testIndexMergeResultChCloseEarly"))
}

func TestIndexMergeError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	setupPartitionTableHelper(tk)

	packagePath := "github.com/pingcap/tidb/pkg/executor/"
	errFPPaths := []string{
		packagePath + "testIndexMergeErrorPartialIndexWorker",
		packagePath + "testIndexMergeErrorPartialTableWorker",
	}
	for _, fp := range errFPPaths {
		fmt.Println("handling failpoint: ", fp)
		require.NoError(t, failpoint.Enable(fp, fmt.Sprintf(`return("%s")`, fp)))
		for i := 0; i < 100; i++ {
			indexMergePanicRunSQL(t, tk, fp)
		}
		require.NoError(t, failpoint.Disable(fp))
	}
}

func TestIndexMergeCoprGoroutinesLeak(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	setupPartitionTableHelper(tk)

	var err error
	sql := "select /*+ use_index_merge(t1) */ c1 from t1 where c1 < 900 or c2 < 1000;"
	res := tk.MustQuery("explain " + sql).Rows()
	require.Contains(t, res[1][0], "IndexMerge")

	// If got goroutines leak in coprocessor, ci will fail.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergePartialTableWorkerCoprLeak", `panic("testIndexMergePartialTableWorkerCoprLeak")`))
	err = tk.QueryToErr(sql)
	require.Contains(t, err.Error(), "testIndexMergePartialTableWorkerCoprLeak")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/testIndexMergePartialTableWorkerCoprLeak"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIndexMergePartialIndexWorkerCoprLeak", `panic("testIndexMergePartialIndexWorkerCoprLeak")`))
	err = tk.QueryToErr(sql)
	require.Contains(t, err.Error(), "testIndexMergePartialIndexWorkerCoprLeak")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/testIndexMergePartialIndexWorkerCoprLeak"))
}

type valueStruct struct {
	a int
	b int
	c int
}

func getResult(values []*valueStruct, a int, b int, limit int, desc bool) []*valueStruct {
	ret := make([]*valueStruct, 0)
	for _, value := range values {
		if value.a == a || value.b == b {
			ret = append(ret, value)
		}
	}
	slices.SortFunc(ret, func(a, b *valueStruct) int {
		if desc {
			return cmp.Compare(b.c, a.c)
		}
		return cmp.Compare(a.c, b.c)
	})
	if len(ret) > limit {
		return ret[:limit]
	}
	return ret
}

func TestOrderByWithLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists thandle, tpk, tcommon, thash, tcommonhash, tpkhash")
	tk.MustExec("create table thandle(a int, b int, c int, index idx_ac(a, c), index idx_bc(b, c))")
	tk.MustExec("create table tpk(a int, b int, c int, d int auto_increment, primary key(d), index idx_ac(a, c), index idx_bc(b, c))")
	tk.MustExec("create table tcommon(a int, b int, c int, d int auto_increment, primary key(a, c, d), index idx_ac(a, c), index idx_bc(b, c))")
	tk.MustExec("create table thash(a int, b int, c int, index idx_ac(a, c), index idx_bc(b, c)) PARTITION BY HASH (`a`) PARTITIONS 4")
	tk.MustExec("create table tcommonhash(a int, b int, c int, d int auto_increment, primary key(a, c, d), index idx_bc(b, c)) PARTITION BY HASH (`c`) PARTITIONS 4")
	tk.MustExec("create table tpkhash(a int, b int, c int, d int auto_increment, primary key(d), index idx_ac(a, c), index idx_bc(b, c)) PARTITION BY HASH (`d`) PARTITIONS 4")

	// analyze before insert rows to speed up UT and let query run in dynamic pruning mode.
	tk.MustExec("analyze table thandle")
	tk.MustExec("analyze table tpk")
	tk.MustExec("analyze table tcommon")
	tk.MustExec("analyze table thash")
	tk.MustExec("analyze table tcommonhash")
	tk.MustExec("analyze table tpkhash")

	valueSlice := make([]*valueStruct, 0, 500)
	vals := make([]string, 0, 500)
	for i := 0; i < 500; i++ {
		a := rand.Intn(32)
		b := rand.Intn(32)
		c := rand.Intn(32)
		vals = append(vals, fmt.Sprintf("(%v, %v, %v)", a, b, c))
		valueSlice = append(valueSlice, &valueStruct{a, b, c})
	}
	valInserted := strings.Join(vals, ",")

	tk.MustExec(fmt.Sprintf("insert into thandle values %s", valInserted))
	tk.MustExec(fmt.Sprintf("insert into tpk(a,b,c) values %s", valInserted))
	tk.MustExec(fmt.Sprintf("insert into tcommon(a,b,c) values %s", valInserted))
	tk.MustExec(fmt.Sprintf("insert into thash(a,b,c) values %s", valInserted))
	tk.MustExec(fmt.Sprintf("insert into tcommonhash(a,b,c) values %s", valInserted))
	tk.MustExec(fmt.Sprintf("insert into tpkhash(a,b,c) values %s", valInserted))

	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			tk.MustExec("set tidb_partition_prune_mode = `static-only`")
		} else {
			tk.MustExec("set tidb_partition_prune_mode = `dynamic-only`")
		}
		a := rand.Intn(32)
		b := rand.Intn(32)
		limit := rand.Intn(10) + 1
		queryHandle := fmt.Sprintf("select /*+ use_index_merge(thandle, idx_ac, idx_bc) */ * from thandle where a = %v or b = %v order by c limit %v", a, b, limit)
		resHandle := tk.MustQuery(queryHandle).Rows()
		tk.MustHavePlan(queryHandle, "IndexMerge")
		tk.MustNotHavePlan(queryHandle, "TopN")

		queryPK := fmt.Sprintf("select /*+ use_index_merge(tpk, idx_ac, idx_bc) */ * from tpk where a = %v or b = %v order by c limit %v", a, b, limit)
		resPK := tk.MustQuery(queryPK).Rows()
		tk.MustHavePlan(queryPK, "IndexMerge")
		tk.MustNotHavePlan(queryPK, "TopN")

		queryCommon := fmt.Sprintf("select /*+ use_index_merge(tcommon, idx_ac, idx_bc) */ * from tcommon where a = %v or b = %v order by c limit %v", a, b, limit)
		resCommon := tk.MustQuery(queryCommon).Rows()
		tk.MustHavePlan(queryCommon, "IndexMerge")
		tk.MustNotHavePlan(queryCommon, "TopN")

		queryTableScan := fmt.Sprintf("select /*+ use_index_merge(tcommon, primary, idx_bc) */ * from tcommon where a = %v or b = %v order by c limit %v", a, b, limit)
		resTableScan := tk.MustQuery(queryTableScan).Rows()
		tk.MustHavePlan(queryTableScan, "IndexMerge")
		tk.MustHavePlan(queryTableScan, "TableRangeScan")
		tk.MustNotHavePlan(queryTableScan, "TopN")

		queryHash := fmt.Sprintf("select /*+ use_index_merge(thash, idx_ac, idx_bc) */ * from thash where a = %v or b = %v order by c limit %v", a, b, limit)
		resHash := tk.MustQuery(queryHash).Rows()
		tk.MustHavePlan(queryHash, "IndexMerge")
		if i%2 == 1 {
			tk.MustNotHavePlan(queryHash, "TopN")
		}

		queryCommonHash := fmt.Sprintf("select /*+ use_index_merge(tcommonhash, primary, idx_bc) */ * from tcommonhash where a = %v or b = %v order by c limit %v", a, b, limit)
		resCommonHash := tk.MustQuery(queryCommonHash).Rows()
		tk.MustHavePlan(queryCommonHash, "IndexMerge")
		if i%2 == 1 {
			tk.MustNotHavePlan(queryCommonHash, "TopN")
		}

		queryPKHash := fmt.Sprintf("select /*+ use_index_merge(tpkhash, idx_ac, idx_bc) */ * from tpkhash where a = %v or b = %v order by c limit %v", a, b, limit)
		resPKHash := tk.MustQuery(queryPKHash).Rows()
		tk.MustHavePlan(queryPKHash, "IndexMerge")
		if i%2 == 1 {
			tk.MustNotHavePlan(queryPKHash, "TopN")
		}

		sliceRes := getResult(valueSlice, a, b, limit, false)

		require.Equal(t, len(sliceRes), len(resHandle))
		require.Equal(t, len(sliceRes), len(resPK))
		require.Equal(t, len(sliceRes), len(resCommon))
		require.Equal(t, len(sliceRes), len(resTableScan))
		require.Equal(t, len(sliceRes), len(resHash))
		require.Equal(t, len(sliceRes), len(resCommonHash))
		require.Equal(t, len(sliceRes), len(resPKHash))

		for i := range sliceRes {
			expectValue := fmt.Sprintf("%v", sliceRes[i].c)
			// Only check column `c`
			require.Equal(t, expectValue, resHandle[i][2])
			require.Equal(t, expectValue, resPK[i][2])
			require.Equal(t, expectValue, resCommon[i][2])
			require.Equal(t, expectValue, resTableScan[i][2])
			require.Equal(t, expectValue, resHash[i][2])
			require.Equal(t, expectValue, resCommonHash[i][2])
			require.Equal(t, expectValue, resPKHash[i][2])
		}
	}
}

func TestProcessInfoRaceWithIndexScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int, c2 int, c3 int, c4 int, c5 int, key(c1), key(c2), key(c3), key(c4),key(c5));")
	insertStr := "insert into t1 values(0, 0, 0, 0 , 0)"
	for i := 1; i < 100; i++ {
		insertStr += fmt.Sprintf(", (%d, %d, %d, %d, %d)", i, i, i, i, i)
	}
	tk.MustExec(insertStr)

	tk.Session().SetSessionManager(&testkit.MockSessionManager{
		PS: []*util.ProcessInfo{tk.Session().ShowProcess()},
	})

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i <= 100; i++ {
			ps := tk.Session().ShowProcess()
			util.GenLogFields(233, ps, true)
		}
	}()
	for i := 0; i <= 100; i++ {
		tk.MustQuery("select /*+ use_index(t1, c1) */ c1 from t1 where c1 = 0 union all select /*+ use_index(t1, c2) */ c2 from t1 where c2 = 0 union all select /*+ use_index(t1, c3) */ c3 from t1 where c3 = 0 ")
	}
	wg.Wait()
}

func TestIndexMergeReaderIssue45279(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists reproduce;")
	tk.MustExec("CREATE TABLE reproduce (c1 int primary key, c2 int, c3 int, key ci2(c2), key ci3(c3));")
	tk.MustExec("insert into reproduce values (1, 1, 1), (2, 2, 2), (3, 3, 3);")
	tk.MustQuery("explain select * from reproduce where c1 in (0, 1, 2, 3) or c2 in (0, 1, 2);").Check(testkit.Rows(
		"IndexMerge_11 33.99 root  type: union",
		"├─TableRangeScan_8(Build) 4.00 cop[tikv] table:reproduce range:[0,0], [1,1], [2,2], [3,3], keep order:false, stats:pseudo",
		"├─IndexRangeScan_9(Build) 30.00 cop[tikv] table:reproduce, index:ci2(c2) range:[0,0], [1,1], [2,2], keep order:false, stats:pseudo",
		"└─TableRowIDScan_10(Probe) 33.99 cop[tikv] table:reproduce keep order:false, stats:pseudo"))

	// This function should return successfully
	var ctx context.Context
	ctx, executor.IndexMergeCancelFuncForTest = context.WithCancel(context.Background())
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testCancelContext", "return()"))
	rs, _ := tk.ExecWithContext(ctx, "select * from reproduce where c1 in (0, 1, 2, 3) or c2 in (0, 1, 2);")
	session.ResultSetToStringSlice(ctx, tk.Session(), rs)
	failpoint.Disable("github.com/pingcap/tidb/br/pkg/checksum/testCancelContext")
}

func TestIndexMergeLimitPushedAsIntersectionEmbeddedLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, index idx(a, c), index idx2(b, c), index idx3(a, b, c))")
	valsInsert := make([]string, 0, 1000)
	for i := 0; i < 500; i++ {
		valsInsert = append(valsInsert, fmt.Sprintf("(%v, %v, %v)", rand.Intn(100), rand.Intn(100), rand.Intn(100)))
	}
	tk.MustExec("analyze table t")
	tk.MustExec("insert into t values " + strings.Join(valsInsert, ","))
	for i := 0; i < 10; i++ {
		valA, valB, valC, limit := rand.Intn(100), rand.Intn(100), rand.Intn(50), rand.Intn(100)+1
		queryTableScan := fmt.Sprintf("select * from t use index() where a > %d and b > %d and c >= %d limit %d", valA, valB, valC, limit)
		queryWithIndexMerge := fmt.Sprintf("select /*+ USE_INDEX_MERGE(t, idx, idx2) */ * from t where a > %d and b > %d and c >= %d limit %d", valA, valB, valC, limit)
		tk.MustHavePlan(queryWithIndexMerge, "IndexMerge")
		require.True(t, tk.HasKeywordInOperatorInfo(queryWithIndexMerge, "limit embedded"))
		tk.MustHavePlan(queryTableScan, "TableFullScan")
		// index merge with embedded limit couldn't compare the exactly results with normal plan, because limit admission control has some difference, while we can only check
		// the row count is exactly the same with tableFullScan plan, in case of index pushedLimit and table pushedLimit cut down the source table rows.
		require.Equal(t, len(tk.MustQuery(queryWithIndexMerge).Rows()), len(tk.MustQuery(queryTableScan).Rows()))
	}
}

func TestIndexMergeLimitNotPushedOnPartialSideButKeepOrder(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, index idx(a, c), index idx2(b, c), index idx3(a, b, c))")
	valsInsert := make([]string, 0, 500)
	for i := 0; i < 500; i++ {
		valsInsert = append(valsInsert, fmt.Sprintf("(%v, %v, %v)", rand.Intn(100), rand.Intn(100), rand.Intn(100)))
	}
	tk.MustExec("analyze table t")
	tk.MustExec("insert into t values " + strings.Join(valsInsert, ","))
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceIndexMergeKeepOrder", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceIndexMergeKeepOrder")
	for i := 0; i < 50; i++ {
		valA, valB, valC, limit := rand.Intn(100), rand.Intn(100), rand.Intn(50), rand.Intn(100)+1
		maxEle := tk.MustQuery(fmt.Sprintf("select ifnull(max(c), 100) from (select c from t use index(idx3) where (a = %d or b = %d) and c >= %d order by c limit %d) t", valA, valB, valC, limit)).Rows()[0][0]
		queryWithIndexMerge := fmt.Sprintf("select /*+ USE_INDEX_MERGE(t, idx, idx2) */ * from t where (a = %d or b = %d) and c >= %d and c < greatest(%d, %v) order by c limit %d", valA, valB, valC, valC+1, maxEle, limit)
		queryWithNormalIndex := fmt.Sprintf("select * from t use index(idx3) where (a = %d or b = %d) and c >= %d and c < greatest(%d, %v) order by c limit %d", valA, valB, valC, valC+1, maxEle, limit)
		tk.MustHavePlan(queryWithIndexMerge, "IndexMerge")
		tk.MustHavePlan(queryWithIndexMerge, "Limit")
		normalResult := tk.MustQuery(queryWithNormalIndex).Sort().Rows()
		tk.MustQuery(queryWithIndexMerge).Sort().Check(normalResult)
	}
	for i := 0; i < 50; i++ {
		valA, valB, valC, limit, offset := rand.Intn(100), rand.Intn(100), rand.Intn(50), rand.Intn(100)+1, rand.Intn(20)
		maxEle := tk.MustQuery(fmt.Sprintf("select ifnull(max(c), 100) from (select c from t use index(idx3) where (a = %d or b = %d) and c >= %d order by c limit %d offset %d) t", valA, valB, valC, limit, offset)).Rows()[0][0]
		queryWithIndexMerge := fmt.Sprintf("select /*+ USE_INDEX_MERGE(t, idx, idx2) */ c from t where (a = %d or b = %d) and c >= %d and c < greatest(%d, %v) order by c limit %d offset %d", valA, valB, valC, valC+1, maxEle, limit, offset)
		queryWithNormalIndex := fmt.Sprintf("select c from t use index(idx3) where (a = %d or b = %d) and c >= %d and c < greatest(%d, %v) order by c limit %d offset %d", valA, valB, valC, valC+1, maxEle, limit, offset)
		tk.MustHavePlan(queryWithIndexMerge, "IndexMerge")
		tk.MustHavePlan(queryWithIndexMerge, "Limit")
		normalResult := tk.MustQuery(queryWithNormalIndex).Sort().Rows()
		tk.MustQuery(queryWithIndexMerge).Sort().Check(normalResult)
	}
}

func TestIssues46005(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_index_lookup_size = 1024")
	tk.MustExec("create table t(a int, b int, c int, index idx1(a, c), index idx2(b, c))")
	for i := 0; i < 1500; i++ {
		tk.MustExec(fmt.Sprintf("insert into t(a,b,c) values (1, 1, %d)", i))
	}

	tk.MustQuery("select /*+ USE_INDEX_MERGE(t, idx1, idx2) */ * from t where a = 1 or b = 1 order by c limit 1025")
}
