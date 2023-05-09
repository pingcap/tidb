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
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/memory"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestSingleTableRead(t *testing.T) {
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
	tk.MustQuery("select /*+ use_index_merge(t1, primary, t1a) */ a from t1 where id < 2 or a > 4 order by a").Check(testkit.Rows("1",
		"5"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, t1a) */ sum(a) from t1 where id < 2 or a > 4").Check(testkit.Rows("6"))
	tk.MustQuery("select /*+ use_index_merge(t1, t1a, t1b) */ * from t1 where a < 2 or b > 4 order by a").Check(testkit.Rows("1 1 1 1 1",
		"5 5 5 5 5"))
	tk.MustQuery("select /*+ use_index_merge(t1, t1a, t1b) */ a from t1 where a < 2 or b > 4 order by a").Check(testkit.Rows("1",
		"5"))
	tk.MustQuery("select /*+ use_index_merge(t1, t1a, t1b) */ sum(a) from t1 where a < 2 or b > 4").Check(testkit.Rows("6"))
}

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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergePickAndExecTaskPanic", "panic(\"pickAndExecTaskPanic\")"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testIndexMergePickAndExecTaskPanic"))
	}()
	err := tk.QueryToErr("select /*+ use_index_merge(t1, primary, t1a) */ * from t1 where id < 2 or a > 4 order by id")
	require.Contains(t, err.Error(), "pickAndExecTaskPanic")
}

func TestJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(id int primary key, a int, b int, c int, d int)")
	tk.MustExec("create index t1a on t1(a)")
	tk.MustExec("create index t1b on t1(b)")
	tk.MustExec("create table t2(id int primary key, a int)")
	tk.MustExec("create index t2a on t2(a)")
	tk.MustExec("insert into t1 values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5)")
	tk.MustExec("insert into t2 values(1,1),(5,5)")
	tk.MustQuery("select /*+ use_index_merge(t1, t1a, t1b) */ sum(t1.a) from t1 join t2 on t1.id = t2.id where t1.a < 2 or t1.b > 4").Check(testkit.Rows("6"))
	tk.MustQuery("select /*+ use_index_merge(t1, t1a, t1b) */ sum(t1.a) from t1 join t2 on t1.id = t2.id where t1.a < 2 or t1.b > 5").Check(testkit.Rows("1"))
}

func TestIndexMergeReaderAndGeneratedColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT AS (1), c1 INT PRIMARY KEY)")
	tk.MustExec("INSERT INTO t0(c1) VALUES (0)")
	tk.MustExec("CREATE INDEX i0 ON t0(c0)")
	tk.MustQuery("SELECT /*+ USE_INDEX_MERGE(t0, i0, PRIMARY)*/ t0.c0 FROM t0 WHERE t0.c1 OR t0.c0").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT t0.c0 FROM t0 WHERE t0.c1 OR t0.c0").Check(testkit.Rows("1"))
}

// issue 25045
func TestIndexMergeReaderIssue25045(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int primary key, b int, c int, key(b), key(c));")
	tk.MustExec("INSERT INTO t1 VALUES (10, 10, 10), (11, 11, 11)")
	tk.MustQuery("explain format='brief' select /*+ use_index_merge(t1) */ * from t1 where c=10 or (b=10 and a=10);").Check(testkit.Rows(
		"IndexMerge 0.01 root  type: union",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t1, index:c(c) range:[10,10], keep order:false, stats:pseudo",
		"├─TableRangeScan(Build) 1.00 cop[tikv] table:t1 range:[10,10], keep order:false, stats:pseudo",
		"└─Selection(Probe) 0.01 cop[tikv]  or(eq(test.t1.c, 10), and(eq(test.t1.b, 10), eq(test.t1.a, 10)))",
		"  └─TableRowIDScan 11.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where c=10 or (b=10 and a=10);").Check(testkit.Rows("10 10 10"))
}

func TestIssue16910(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1, t2, t3;")
	tk.MustExec("create table t1 (a int not null, b tinyint not null, index (a), index (b)) partition by range (a) (" +
		"partition p0 values less than (10)," +
		"partition p1 values less than (20)," +
		"partition p2 values less than (30)," +
		"partition p3 values less than (40)," +
		"partition p4 values less than MAXVALUE);")
	tk.MustExec("insert into t1 values(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (20, 20), (21, 21), " +
		"(22, 22), (23, 23), (24, 24), (25, 25), (30, 30), (31, 31), (32, 32), (33, 33), (34, 34), (35, 35), (36, 36), (40, 40), (50, 50), (80, 80), (90, 90), (100, 100);")
	tk.MustExec("create table t2 (a int not null, b bigint not null, index (a), index (b)) partition by hash(a) partitions 10;")
	tk.MustExec("insert into t2 values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16), (17, 17), (18, 18), (19, 19), (20, 20), (21, 21), (22, 22), (23, 23);")
	tk.MustQuery("select /*+ USE_INDEX_MERGE(t1, a, b) */ * from t1 partition (p0) join t2 partition (p1) on t1.a = t2.a where t1.a < 40 or t1.b < 30;").Check(testkit.Rows("1 1 1 1"))
}

func TestIndexMergeCausePanic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_index_merge = 1;")
	tk.MustExec("create table t (a int, b int, c int, primary key(a), key(b))")
	tk.MustQuery("explain select /*+ inl_join(t2) */ * from t t1 join t t2 on t1.a = t2.a and t1.c = t2.c where t2.a = 1 or t2.b = 1")
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

	values := make([]string, 0, 128)
	for i := 0; i < 128; i++ {
		values = append(values, fmt.Sprintf("(%v, %v)", rand.Intn(40), rand.Intn(40)))
	}
	tk.MustExec(fmt.Sprintf("insert into t values %v", strings.Join(values, ", ")))
	tk.MustExec(fmt.Sprintf("insert into tnormal values %v", strings.Join(values, ", ")))

	randRange := func() (int, int) {
		a, b := rand.Intn(40), rand.Intn(40)
		if a > b {
			return b, a
		}
		return a, b
	}
	for i := 0; i < 256; i++ {
		la, ra := randRange()
		lb, rb := randRange()
		cond := fmt.Sprintf("(a between %v and %v) or (b between %v and %v)", la, ra, lb, rb)
		result := tk.MustQuery("select * from tnormal where " + cond).Sort().Rows()
		tk.MustQuery("select /*+ USE_INDEX_MERGE(t, a, b) */ * from t where " + cond).Sort().Check(result)
	}

	// test a table with a primary key
	tk.MustExec(`create table tpk (a int primary key, b int, key(b))
		partition by range (a) (
		partition p1 values less than (10),
		partition p2 values less than (20),
		partition p3 values less than (30),
		partition p4 values less than (40))`)
	tk.MustExec("truncate tnormal")

	values = values[:0]
	for i := 0; i < 40; i++ {
		values = append(values, fmt.Sprintf("(%v, %v)", i, rand.Intn(40)))
	}
	tk.MustExec(fmt.Sprintf("insert into tpk values %v", strings.Join(values, ", ")))
	tk.MustExec(fmt.Sprintf("insert into tnormal values %v", strings.Join(values, ", ")))
	for i := 0; i < 256; i++ {
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

func TestIndexMergeInTransaction(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	for i := 0; i < 2; i++ {
		tk.MustExec("drop table if exists t1;")
		tk.MustExec("create table t1(c1 int, c2 int, c3 int, pk int, key(c1), key(c2), key(c3), primary key(pk));")
		if i == 1 {
			tk.MustExec("set tx_isolation = 'READ-COMMITTED';")
		}
		tk.MustExec("begin;")
		// Expect two IndexScan(c1, c2).
		tk.MustQuery("explain select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c2 < 10) and c3 < 10;").Check(testkit.Rows(
			"IndexMerge_9 1841.86 root  type: union",
			"├─IndexRangeScan_5(Build) 3323.33 cop[tikv] table:t1, index:c1(c1) range:[-inf,10), keep order:false, stats:pseudo",
			"├─IndexRangeScan_6(Build) 3323.33 cop[tikv] table:t1, index:c2(c2) range:[-inf,10), keep order:false, stats:pseudo",
			"└─Selection_8(Probe) 1841.86 cop[tikv]  lt(test.t1.c3, 10)",
			"  └─TableRowIDScan_7 5542.21 cop[tikv] table:t1 keep order:false, stats:pseudo"))
		// Expect one IndexScan(c2) and one TableScan(pk).
		tk.MustQuery("explain select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < 10) and c3 < 10;").Check(testkit.Rows(
			"IndexMerge_9 1106.67 root  type: union",
			"├─TableRangeScan_5(Build) 3333.33 cop[tikv] table:t1 range:[-inf,10), keep order:false, stats:pseudo",
			"├─IndexRangeScan_6(Build) 3323.33 cop[tikv] table:t1, index:c2(c2) range:[-inf,10), keep order:false, stats:pseudo",
			"└─Selection_8(Probe) 1106.67 cop[tikv]  lt(test.t1.c3, 10)",
			"  └─TableRowIDScan_7 3330.01 cop[tikv] table:t1 keep order:false, stats:pseudo"))
		tk.MustQuery("explain select /*+ use_index_merge(t1, c1, c2, c3) */ * from t1 where c1 < 10 and c2 < 10 and c3 < 10;").Check(testkit.Rows(
			"IndexMerge_9 367.05 root  type: intersection",
			"├─IndexRangeScan_5(Build) 3323.33 cop[tikv] table:t1, index:c1(c1) range:[-inf,10), keep order:false, stats:pseudo",
			"├─IndexRangeScan_6(Build) 3323.33 cop[tikv] table:t1, index:c2(c2) range:[-inf,10), keep order:false, stats:pseudo",
			"├─IndexRangeScan_7(Build) 3323.33 cop[tikv] table:t1, index:c3(c3) range:[-inf,10), keep order:false, stats:pseudo",
			"└─TableRowIDScan_8(Probe) 367.05 cop[tikv] table:t1 keep order:false, stats:pseudo"))

		// Test with normal key.
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c2 < -1) and c3 < 10;").Check(testkit.Rows())
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < -1 or c2 < 10) and c3 < 10;").Check(testkit.Rows())
		tk.MustQuery("select /*+ use_index_merge(t1, c1, c2, c3) */ * from t1 where (c1 < 10 and c2 < -1) and c3 < 10;").Check(testkit.Rows())
		tk.MustQuery("select /*+ use_index_merge(t1, c1, c2, c3) */ * from t1 where (c1 < -1 and c2 < 10) and c3 < 10;").Check(testkit.Rows())

		tk.MustExec("insert into t1 values(1, 1, 1, 1);")
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c2 < -1) and c3 < 10;").Check(testkit.Rows("1 1 1 1"))
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < -1 or c2 < 10) and c3 < 10;").Check(testkit.Rows("1 1 1 1"))
		tk.MustQuery("select /*+ use_index_merge(t1, c1, c2, c3) */ * from t1 where (c1 < 10 and c2 < 10) and c3 < 10;").Check(testkit.Rows("1 1 1 1"))
		tk.MustQuery("select /*+ use_index_merge(t1, c1, c2, c3) */ * from t1 where (c1 < 10 and c2 < 10) and c3 > 10;").Check(testkit.Rows())

		tk.MustExec("update t1 set c3 = 100 where c3 = 1;")
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c2 < -1) and c3 < 10;").Check(testkit.Rows())
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < -1 or c2 < 10) and c3 < 10;").Check(testkit.Rows())
		tk.MustQuery("select /*+ use_index_merge(t1, c1, c2, c3) */ * from t1 where (c1 < 10 and c2 < 10) and c3 > 10;").Check(testkit.Rows("1 1 100 1"))

		tk.MustExec("delete from t1;")
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c2 < -1) and c3 < 10;").Check(testkit.Rows())
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < -1 or c2 < 10) and c3 < 10;").Check(testkit.Rows())
		tk.MustQuery("select /*+ use_index_merge(t1, c1, c2, c3) */ * from t1 where (c1 < 10 and c2 < 10) and c3 > 10;").Check(testkit.Rows())

		// Test with primary key, so the partialPlan is TableScan.
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < -1 or c2 < 10) and c3 < 10;").Check(testkit.Rows())
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < -1) and c3 < 10;").Check(testkit.Rows())
		tk.MustQuery("select /*+ use_index_merge(t1, c2, c3, primary) */ * from t1 where (pk < -1 and c2 < 10) and c3 < 10;").Check(testkit.Rows())
		tk.MustQuery("select /*+ use_index_merge(t1, c2, c3, primary) */ * from t1 where (pk < 10 and c2 < -1) and c3 < 10;").Check(testkit.Rows())
		tk.MustExec("insert into t1 values(1, 1, 1, 1);")
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < -1 or c2 < 10) and c3 < 10;").Check(testkit.Rows("1 1 1 1"))
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < -1) and c3 < 10;").Check(testkit.Rows("1 1 1 1"))
		tk.MustQuery("select /*+ use_index_merge(t1, c2, c3, primary) */ * from t1 where (pk < 10 and c2 < 10) and c3 < 10;").Check(testkit.Rows("1 1 1 1"))
		tk.MustExec("update t1 set c3 = 100 where c3 = 1;")
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < -1 or c2 < 10) and c3 < 10;").Check(testkit.Rows())
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < -1) and c3 < 10;").Check(testkit.Rows())
		tk.MustQuery("select /*+ use_index_merge(t1, c2, c3, primary) */ * from t1 where (pk < 10 and c2 < 10) and c3 > 10;").Check(testkit.Rows("1 1 100 1"))
		tk.MustExec("delete from t1;")
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < -1 or c2 < 10) and c3 < 10;").Check(testkit.Rows())
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < -1) and c3 < 10;").Check(testkit.Rows())
		tk.MustQuery("select /*+ use_index_merge(t1, c2, c3, primary) */ * from t1 where (pk < 10 and c2 < 10) and c3 > 10;").Check(testkit.Rows())

		tk.MustExec("commit;")
		if i == 1 {
			tk.MustExec("set tx_isolation = 'REPEATABLE-READ';")
		}
	}

	// Same with above, but select ... for update.
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int, c2 int, c3 int, pk int, key(c1), key(c2), key(c3), primary key(pk));")
	tk.MustExec("begin;")
	tk.MustQuery("explain select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c2 < 10) and c3 < 10 for update;").Check(testkit.Rows(
		"SelectLock_6 1841.86 root  for update 0",
		"└─IndexMerge_11 1841.86 root  type: union",
		"  ├─IndexRangeScan_7(Build) 3323.33 cop[tikv] table:t1, index:c1(c1) range:[-inf,10), keep order:false, stats:pseudo",
		"  ├─IndexRangeScan_8(Build) 3323.33 cop[tikv] table:t1, index:c2(c2) range:[-inf,10), keep order:false, stats:pseudo",
		"  └─Selection_10(Probe) 1841.86 cop[tikv]  lt(test.t1.c3, 10)",
		"    └─TableRowIDScan_9 5542.21 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery("explain select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < 10) and c3 < 10 for update;").Check(testkit.Rows(
		"SelectLock_6 1106.67 root  for update 0",
		"└─IndexMerge_11 1106.67 root  type: union",
		"  ├─TableRangeScan_7(Build) 3333.33 cop[tikv] table:t1 range:[-inf,10), keep order:false, stats:pseudo",
		"  ├─IndexRangeScan_8(Build) 3323.33 cop[tikv] table:t1, index:c2(c2) range:[-inf,10), keep order:false, stats:pseudo",
		"  └─Selection_10(Probe) 1106.67 cop[tikv]  lt(test.t1.c3, 10)",
		"    └─TableRowIDScan_9 3330.01 cop[tikv] table:t1 keep order:false, stats:pseudo"))

	// Test with normal key.
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c2 < 10) and c3 < 10 for update;").Check(testkit.Rows())
	tk.MustExec("insert into t1 values(1, 1, 1, 1);")
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c2 < 10) and c3 < 10 for update;").Check(testkit.Rows("1 1 1 1"))
	tk.MustExec("update t1 set c3 = 100 where c3 = 1;")
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c2 < 10) and c3 < 10 for update;").Check(testkit.Rows())
	tk.MustExec("delete from t1;")
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c2 < 10) and c3 < 10 for update;").Check(testkit.Rows())

	// Test with primary key, so the partialPlan is TableScan.
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < 10) and c3 < 10 for update;").Check(testkit.Rows())
	tk.MustExec("insert into t1 values(1, 1, 1, 1);")
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < 10) and c3 < 10 for update;").Check(testkit.Rows("1 1 1 1"))
	tk.MustExec("update t1 set c3 = 100 where c3 = 1;")
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < 10) and c3 < 10 for update;").Check(testkit.Rows())
	tk.MustExec("delete from t1;")
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < 10) and c3 < 10 for update;").Check(testkit.Rows())
	tk.MustExec("commit;")

	// Test partition table.
	tk.MustExec("drop table if exists t1;")
	tk.MustExec(`create table t1(c1 int, c2 int, c3 int, pk int, part int, key(c1), key(c2), key(c3), primary key(pk, part))
			partition by range(part) (
			partition p0 values less than (10),
			partition p1 values less than (20),
			partition p2 values less than (maxvalue))`)
	tk.MustExec("begin;")
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 20 or c2 < 20) and c3 < 20;").Check(testkit.Rows())

	tk.MustExec("insert into t1 values(1, 1, 1, 1, 1);")
	tk.MustExec("insert into t1 values(11, 11, 11, 11, 11);")
	tk.MustExec("insert into t1 values(21, 21, 21, 21, 21);")
	tk.MustExec("insert into t1 values(31, 31, 31, 31, 31);")

	res := tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < -1 or c2 < 20) and c3 < 20;").Sort()
	res.Check(testkit.Rows("1 1 1 1 1", "11 11 11 11 11"))
	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 20 or c2 < -1) and c3 < 20;").Sort()
	res.Check(testkit.Rows("1 1 1 1 1", "11 11 11 11 11"))

	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < -1 or c2 < 20) and c3 < 20;").Sort()
	res.Check(testkit.Rows("1 1 1 1 1", "11 11 11 11 11"))
	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 20 or c2 < -1) and c3 < 20;").Sort()
	res.Check(testkit.Rows("1 1 1 1 1", "11 11 11 11 11"))

	tk.MustExec("update t1 set c3 = 100 where c3 = 1;")
	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < -1 or c2 < 20) and c3 < 20;")
	res.Check(testkit.Rows("11 11 11 11 11"))
	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 20 or c2 < -1) and c3 < 20;")
	res.Check(testkit.Rows("11 11 11 11 11"))

	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < -1 or c2 < 20) and c3 < 20;")
	res.Check(testkit.Rows("11 11 11 11 11"))
	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 20 or c2 < -1) and c3 < 20;")
	res.Check(testkit.Rows("11 11 11 11 11"))

	tk.MustExec("delete from t1;")
	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < -1 or c2 < 20) and c3 < 20;")
	res.Check(testkit.Rows())
	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 20 or c2 < -1) and c3 < 20;")
	res.Check(testkit.Rows())

	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < -1 or c2 < 20) and c3 < 20;")
	res.Check(testkit.Rows())
	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 20 or c2 < -1) and c3 < 20;")
	res.Check(testkit.Rows())
	tk.MustExec("commit;")
}

func TestIndexMergeReaderInTransIssue30685(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// This is a case generated by sqlgen to test if clustered index is ok.
	// Detect the bugs in memIndexMergeReader.getMemRowsHandle().
	tk.MustExec("drop table if exists t1;")
	tk.MustExec(`create table t1 (col_30 decimal default 0 ,
				      col_31 char(99) collate utf8_bin default 'sVgzHblmYYtEjVg' not null ,
				      col_37 int unsigned default 377206828 ,
				      primary key idx_16 ( col_37 ) , key idx_19 ( col_31) ) collate utf8mb4_general_ci ;`)
	tk.MustExec("begin;")
	tk.MustExec("insert ignore into t1 values (388021, '', 416235653);")
	tk.MustQuery("select /*+ use_index_merge( t1 ) */ 1 from t1 where ( t1.col_31 in ( 'OiOXzpCs' , 'oaVv' ) or t1.col_37 <= 4059907010 ) and t1.col_30 ;").Check(testkit.Rows("1"))
	tk.MustExec("commit;")

	tk.MustExec("drop table if exists tbl_3;")
	tk.MustExec(`create table tbl_3 ( col_30 decimal , col_31 char(99) , col_32 smallint ,
				  col_33 tinyint unsigned not null , col_34 char(209) ,
				  col_35 char(110) , col_36 int unsigned , col_37 int unsigned ,
				  col_38 decimal(50,15) not null , col_39 char(104),
				  primary key ( col_37 ) , unique key ( col_33,col_30,col_36,col_39 ) ,
				  unique key ( col_32,col_35 ) , key ( col_31,col_38 ) ,
				  key ( col_31,col_33,col_32,col_35,col_36 ) ,
				  unique key ( col_38,col_34,col_33,col_31,col_30,col_36,col_35,col_37,col_39 ) ,
				  unique key ( col_39,col_32 ) , unique key ( col_30,col_35,col_31,col_38 ) ,
				  key ( col_38,col_32,col_33 ) )`)
	tk.MustExec("begin;")
	tk.MustExec("insert ignore into tbl_3 values ( 71,'Fipc',-6676,30,'','FgfK',2464927398,4084082400,5602.5868,'' );")
	tk.MustQuery("select /*+ use_index_merge( tbl_3 ) */ 1 from tbl_3 where ( tbl_3.col_37 not in ( 1626615245 , 2433569159 ) or tbl_3.col_38 = 0.06 ) ;").Check(testkit.Rows("1"))
	tk.MustExec("commit;")

	// int + int compound type as clustered index pk.
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int, c2 int, c3 int, c4 int, primary key(c1, c2) /*T![clustered_index] CLUSTERED */, key(c3));")

	tk.MustExec("begin;")
	tk.MustExec("insert into t1 values(1, 1, 1, 1);")
	tk.MustQuery("explain select /*+ use_index_merge(t1) */ * from t1 where (c1 < -1 or c3 < 10) and c4 < 10;").Check(testkit.Rows(
		"UnionScan_6 1841.86 root  lt(test.t1.c4, 10), or(lt(test.t1.c1, -1), lt(test.t1.c3, 10))",
		"└─IndexMerge_11 1841.86 root  type: union",
		"  ├─TableRangeScan_7(Build) 3323.33 cop[tikv] table:t1 range:[-inf,-1), keep order:false, stats:pseudo",
		"  ├─IndexRangeScan_8(Build) 3323.33 cop[tikv] table:t1, index:c3(c3) range:[-inf,10), keep order:false, stats:pseudo",
		"  └─Selection_10(Probe) 1841.86 cop[tikv]  lt(test.t1.c4, 10)",
		"    └─TableRowIDScan_9 5542.21 cop[tikv] table:t1 keep order:false, stats:pseudo"))

	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < -1 or c3 < 10) and c4 < 10;").Check(testkit.Rows("1 1 1 1"))
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c3 < -1) and c4 < 10;").Check(testkit.Rows("1 1 1 1"))
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < -1 or c3 < -1) and c4 < 10;").Check(testkit.Rows())
	tk.MustExec("commit;")

	// Single int type as clustered index pk.
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 varchar(100), c2 int, c3 int, c4 int, primary key(c1) /*T![clustered_index] CLUSTERED */, key(c3));")

	tk.MustExec("begin;")
	tk.MustExec("insert into t1 values('b', 1, 1, 1);")
	tk.MustQuery("explain select /*+ use_index_merge(t1) */ * from t1 where (c1 < 'a' or c3 < 10) and c4 < 10;").Check(testkit.Rows(
		"UnionScan_6 1841.86 root  lt(test.t1.c4, 10), or(lt(test.t1.c1, \"a\"), lt(test.t1.c3, 10))",
		"└─IndexMerge_11 1841.86 root  type: union",
		"  ├─TableRangeScan_7(Build) 3323.33 cop[tikv] table:t1 range:[-inf,\"a\"), keep order:false, stats:pseudo",
		"  ├─IndexRangeScan_8(Build) 3323.33 cop[tikv] table:t1, index:c3(c3) range:[-inf,10), keep order:false, stats:pseudo",
		"  └─Selection_10(Probe) 1841.86 cop[tikv]  lt(test.t1.c4, 10)",
		"    └─TableRowIDScan_9 5542.21 cop[tikv] table:t1 keep order:false, stats:pseudo"))

	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 'a' or c3 < 10) and c4 < 10;").Check(testkit.Rows("b 1 1 1"))
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 <= 'b' or c3 < -1) and c4 < 10;").Check(testkit.Rows("b 1 1 1"))
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 'a' or c3 < -1) and c4 < 10;").Check(testkit.Rows())
	tk.MustExec("commit;")
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

func TestIndexMergeSplitTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("DROP TABLE IF EXISTS tab2;")
	tk.MustExec("CREATE TABLE tab2(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT);")
	tk.MustExec("CREATE INDEX idx_tab2_0 ON tab2 (col0 DESC,col3 DESC);")
	tk.MustExec("CREATE UNIQUE INDEX idx_tab2_3 ON tab2 (col4,col0 DESC);")
	tk.MustExec("CREATE INDEX idx_tab2_4 ON tab2 (col3,col1 DESC);")
	tk.MustExec("INSERT INTO tab2 VALUES(0,146,632.63,'shwwd',703,412.47,'xsppr');")
	tk.MustExec("INSERT INTO tab2 VALUES(1,81,536.29,'trhdh',49,726.3,'chuxv');")
	tk.MustExec("INSERT INTO tab2 VALUES(2,311,541.72,'txrvb',493,581.92,'xtrra');")
	tk.MustExec("INSERT INTO tab2 VALUES(3,669,293.27,'vcyum',862,415.14,'nbutk');")
	tk.MustExec("INSERT INTO tab2 VALUES(4,681,49.46,'odzhp',106,324.65,'deudp');")
	tk.MustExec("INSERT INTO tab2 VALUES(5,319,769.65,'aeqln',855,197.9,'apipa');")
	tk.MustExec("INSERT INTO tab2 VALUES(6,610,302.62,'bixap',184,840.31,'vggit');")
	tk.MustExec("INSERT INTO tab2 VALUES(7,253,453.21,'gjccm',107,104.5,'lvunv');")
	tk.MustExec("SPLIT TABLE tab2 BY (5);")
	tk.MustQuery("SELECT /*+ use_index_merge(tab2) */ pk FROM tab2 WHERE (col4 > 565.89 OR col0 > 68 ) and col0 > 10 order by 1;").Check(testkit.Rows("0", "1", "2", "3", "4", "5", "6", "7"))
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
		"        ├─IndexMerge 5542.21 root  type: union",
		"        │ ├─IndexRangeScan(Build) 3323.33 cop[tikv] table:t1, partition:p0, index:c1(c1) range:[-inf,10), keep order:false, stats:pseudo",
		"        │ ├─IndexRangeScan(Build) 3323.33 cop[tikv] table:t1, partition:p0, index:c2(c2) range:[-inf,10), keep order:false, stats:pseudo",
		"        │ └─TableRowIDScan(Probe) 5542.21 cop[tikv] table:t1, partition:p0 keep order:false, stats:pseudo",
		"        ├─IndexMerge 1.00 root  type: union",
		"        │ ├─IndexRangeScan(Build) 1.00 cop[tikv] table:t1, partition:p1, index:c1(c1) range:[-inf,10), keep order:false",
		"        │ ├─IndexRangeScan(Build) 1.00 cop[tikv] table:t1, partition:p1, index:c2(c2) range:[-inf,10), keep order:false",
		"        │ └─TableRowIDScan(Probe) 1.00 cop[tikv] table:t1, partition:p1 keep order:false",
		"        ├─IndexMerge 1.00 root  type: union",
		"        │ ├─IndexRangeScan(Build) 1.00 cop[tikv] table:t1, partition:p2, index:c1(c1) range:[-inf,10), keep order:false",
		"        │ ├─IndexRangeScan(Build) 1.00 cop[tikv] table:t1, partition:p2, index:c2(c2) range:[-inf,10), keep order:false",
		"        │ └─TableRowIDScan(Probe) 1.00 cop[tikv] table:t1, partition:p2 keep order:false",
		"        └─IndexMerge 1.00 root  type: union",
		"          ├─IndexRangeScan(Build) 1.00 cop[tikv] table:t1, partition:p3, index:c1(c1) range:[-inf,10), keep order:false",
		"          ├─IndexRangeScan(Build) 1.00 cop[tikv] table:t1, partition:p3, index:c2(c2) range:[-inf,10), keep order:false",
		"          └─TableRowIDScan(Probe) 1.00 cop[tikv] table:t1, partition:p3 keep order:false",
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

	// TODO: add support for index merge reader in dynamic tidb_partition_prune_mode
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergeIntersectionConcurrency", fmt.Sprintf("return(%s)", defExecCon)))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testIndexMergeIntersectionConcurrency"))
	}()
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))

	tk.MustExec("set tidb_executor_concurrency = 10")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergeIntersectionConcurrency", "return(10)"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))
	// workerCnt = min(part_num, concurrency)
	tk.MustExec("set tidb_executor_concurrency = 20")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergeIntersectionConcurrency", "return(10)"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))
	tk.MustExec("set tidb_executor_concurrency = 2")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergeIntersectionConcurrency", "return(2)"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))

	tk.MustExec("set tidb_index_merge_intersection_concurrency = 9")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergeIntersectionConcurrency", "return(9)"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))
	tk.MustExec("set tidb_index_merge_intersection_concurrency = 21")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergeIntersectionConcurrency", "return(10)"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))
	tk.MustExec("set tidb_index_merge_intersection_concurrency = 3")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergeIntersectionConcurrency", "return(3)"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))

	// Concurrency only works for dynamic pruning partition table, so real concurrency is 1.
	tk.MustExec("set tidb_partition_prune_mode = 'static'")
	tk.MustExec("set tidb_index_merge_intersection_concurrency = 9")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergeIntersectionConcurrency", "return(1)"))
	tk.MustQuery("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024").Check(testkit.Rows("1"))

	// Concurrency only works for dynamic pruning partition table. so real concurrency is 1.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int, c2 bigint, c3 bigint, primary key(c1), key(c2), key(c3));")
	tk.MustExec("insert into t1 values(1, 1, 3000), (2, 1, 1)")
	tk.MustExec("set tidb_index_merge_intersection_concurrency = 9")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergeIntersectionConcurrency", "return(1)"))
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

		const queryCnt int = 10
		const rowCnt int = 1000
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
				if i == 0 {
					// Dynamic mode.
					tk.MustExec("set tidb_partition_prune_mode = 'dynamic'")
					res := tk.MustQuery("explain select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024")
					require.Contains(t, res.Rows()[1][0], "IndexMerge")
				} else {
					tk.MustExec("set tidb_partition_prune_mode = 'static'")
					res := tk.MustQuery("explain select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024")
					if tblIdx == 0 {
						// partition table
						require.Contains(t, res.Rows()[1][0], "PartitionUnion")
						require.Contains(t, res.Rows()[2][0], "IndexMerge")
					} else {
						require.Contains(t, res.Rows()[1][0], "IndexMerge")
					}
				}
				for i := 0; i < queryCnt; i++ {
					c3 := rand.Intn(1024)
					res := tk.MustQuery(fmt.Sprintf("select /*+ no_index_merge() */ c1 from t1 where c2 < 1024 and c3 > %d", c3)).Sort().Rows()
					tk.MustQuery(fmt.Sprintf("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > %d", c3)).Sort().Check(res)
				}

				// In tranaction
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergeIntersectionWorkerPanic", `panic("testIndexMergeIntersectionWorkerPanic")`))
	err := tk.QueryToErr("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c2 < 1024 and c3 > 1024")
	require.Contains(t, err.Error(), "testIndexMergeIntersectionWorkerPanic")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testIndexMergeIntersectionWorkerPanic"))
}

func TestIntersectionMemQuota(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(pk varchar(100) primary key, c1 int, c2 int, index idx1(c1), index idx2(c2))")

	insertStr := "insert into t1 values"
	for i := 0; i < 20; i++ {
		if i != 0 {
			insertStr += ", "
		}
		insertStr += fmt.Sprintf("('%s', %d, %d)", testutil.RandStringRunes(100), 1, 1)
	}
	tk.MustExec(insertStr)
	res := tk.MustQuery("explain select /*+ use_index_merge(t1, primary, idx1, idx2) */ c1 from t1 where c1 < 1024 and c2 < 1024").Rows()
	require.Contains(t, res[1][0], "IndexMerge")

	tk.MustExec("set global tidb_mem_oom_action='CANCEL'")
	defer tk.MustExec("set global tidb_mem_oom_action = DEFAULT")
	tk.MustExec("set @@tidb_mem_quota_query = 4000")
	err := tk.QueryToErr("select /*+ use_index_merge(t1, primary, idx1, idx2) */ c1 from t1 where c1 < 1024 and c2 < 1024")
	require.Contains(t, err.Error(), memory.PanicMemoryExceedWarnMsg+memory.WarnMsgSuffixForSingleQuery)
}

func setupPartitionTableHelper(tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int, c2 bigint, c3 bigint, primary key(c1), key(c2), key(c3));")
	insertStr := "insert into t1 values(0, 0, 0)"
	for i := 1; i < 1000; i++ {
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

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergeMainReturnEarly", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergeProcessWorkerUnionHang", "return(true)"))
	err = tk.QueryToErr(sql)
	require.Contains(t, err.Error(), "testIndexMergeMainReturnEarly")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testIndexMergeMainReturnEarly"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testIndexMergeProcessWorkerUnionHang"))

	sql = "select /*+ use_index_merge(t1, c2, c3) */ c1 from t1 where c2 < 900 and c3 < 1000;"
	res = tk.MustQuery("explain " + sql).Rows()
	require.Contains(t, res[1][0], "IndexMerge")
	require.Contains(t, res[1][4], "intersection")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergeMainReturnEarly", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergeProcessWorkerIntersectionHang", "return(true)"))
	err = tk.QueryToErr(sql)
	require.Contains(t, err.Error(), "testIndexMergeMainReturnEarly")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testIndexMergeMainReturnEarly"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testIndexMergeProcessWorkerIntersectionHang"))
}

func TestIndexMergePanic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int, c2 bigint, c3 bigint, primary key(c1), key(c2), key(c3));")
	tk.MustExec("insert into t1 values(1, 1, 1), (100, 100, 100)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergeResultChCloseEarly", "return(true)"))
	tk.MustExec("select /*+ use_index_merge(t1, primary, c2, c3) */ c1 from t1 where c1 < 100 or c2 < 100")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testIndexMergeResultChCloseEarly"))

	setupPartitionTableHelper(tk)

	minV := 200
	maxV := 1000
	runSQL := func(fp string) {
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

	packagePath := "github.com/pingcap/tidb/executor/"
	panicFPPaths := []string{
		packagePath + "testIndexMergePanicPartialIndexWorker",
		packagePath + "testIndexMergePanicPartialTableWorker",

		packagePath + "testIndexMergePanicProcessWorkerUnion",
		packagePath + "testIndexMergePanicProcessWorkerIntersection",
		packagePath + "testIndexMergePanicPartitionTableIntersectionWorker",

		packagePath + "testIndexMergePanicTableScanWorker",
	}
	for _, fp := range panicFPPaths {
		fmt.Println("handling failpoint: ", fp)
		if !strings.Contains(fp, "testIndexMergePanicTableScanWorker") {
			// When mockSleepBeforeStartTableReader is enabled, will not read real data. This is to avoid leaking goroutines in coprocessor.
			// But should disable mockSleepBeforeStartTableReader for testIndexMergePanicTableScanWorker.
			// Because finalTableScanWorker need task.doneCh to pass error, so need partialIndexWorker/partialTableWorker runs normally.
			require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/mockSleepBeforeStartTableReader", "return(1000)"))
		}
		for i := 0; i < 1000; i++ {
			require.NoError(t, failpoint.Enable(fp, fmt.Sprintf(`panic("%s")`, fp)))
			runSQL(fp)
			require.NoError(t, failpoint.Disable(fp))
		}
		if !strings.Contains(fp, "testIndexMergePanicTableScanWorker") {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/mockSleepBeforeStartTableReader"))
		}
	}

	errFPPaths := []string{
		packagePath + "testIndexMergeErrorPartialIndexWorker",
		packagePath + "testIndexMergeErrorPartialTableWorker",
	}
	for _, fp := range errFPPaths {
		fmt.Println("handling failpoint: ", fp)
		require.NoError(t, failpoint.Enable(fp, fmt.Sprintf(`return("%s")`, fp)))
		for i := 0; i < 100; i++ {
			runSQL(fp)
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergePartialTableWorkerCoprLeak", `panic("testIndexMergePartialTableWorkerCoprLeak")`))
	err = tk.QueryToErr(sql)
	require.Contains(t, err.Error(), "testIndexMergePartialTableWorkerCoprLeak")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testIndexMergePartialTableWorkerCoprLeak"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIndexMergePartialIndexWorkerCoprLeak", `panic("testIndexMergePartialIndexWorkerCoprLeak")`))
	err = tk.QueryToErr(sql)
	require.Contains(t, err.Error(), "testIndexMergePartialIndexWorkerCoprLeak")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testIndexMergePartialIndexWorkerCoprLeak"))
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
	slices.SortFunc(ret, func(a, b *valueStruct) bool {
		if desc {
			return a.c > b.c
		}
		return a.c < b.c
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

	valueSlice := make([]*valueStruct, 0, 2000)
	vals := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
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

	for i := 0; i < 100; i++ {
		a := rand.Intn(32)
		b := rand.Intn(32)
		limit := rand.Intn(10) + 1
		queryHandle := fmt.Sprintf("select /*+ use_index_merge(thandle, idx_ac, idx_bc) */ * from thandle where a = %v or b = %v order by c limit %v", a, b, limit)
		resHandle := tk.MustQuery(queryHandle).Rows()
		require.True(t, tk.HasPlan(queryHandle, "IndexMerge"))
		require.False(t, tk.HasPlan(queryHandle, "TopN"))

		queryPK := fmt.Sprintf("select /*+ use_index_merge(tpk, idx_ac, idx_bc) */ * from tpk where a = %v or b = %v order by c limit %v", a, b, limit)
		resPK := tk.MustQuery(queryPK).Rows()
		require.True(t, tk.HasPlan(queryPK, "IndexMerge"))
		require.False(t, tk.HasPlan(queryPK, "TopN"))

		queryCommon := fmt.Sprintf("select /*+ use_index_merge(tcommon, idx_ac, idx_bc) */ * from tcommon where a = %v or b = %v order by c limit %v", a, b, limit)
		resCommon := tk.MustQuery(queryCommon).Rows()
		require.True(t, tk.HasPlan(queryCommon, "IndexMerge"))
		require.False(t, tk.HasPlan(queryCommon, "TopN"))

		queryTableScan := fmt.Sprintf("select /*+ use_index_merge(tcommon, primary, idx_bc) */ * from tcommon where a = %v or b = %v order by c limit %v", a, b, limit)
		resTableScan := tk.MustQuery(queryTableScan).Rows()
		require.True(t, tk.HasPlan(queryTableScan, "IndexMerge"))
		require.True(t, tk.HasPlan(queryTableScan, "TableRangeScan"))
		require.False(t, tk.HasPlan(queryTableScan, "TopN"))

		queryHash := fmt.Sprintf("select /*+ use_index_merge(thash, idx_ac, idx_bc) */ * from thash where a = %v or b = %v order by c limit %v", a, b, limit)
		resHash := tk.MustQuery(queryHash).Rows()
		require.True(t, tk.HasPlan(queryHash, "IndexMerge"))
		require.False(t, tk.HasPlan(queryHash, "TopN"))

		queryCommonHash := fmt.Sprintf("select /*+ use_index_merge(tcommonhash, primary, idx_bc) */ * from tcommonhash where a = %v or b = %v order by c limit %v", a, b, limit)
		resCommonHash := tk.MustQuery(queryCommonHash).Rows()
		require.True(t, tk.HasPlan(queryCommonHash, "IndexMerge"))
		require.False(t, tk.HasPlan(queryCommonHash, "TopN"))

		queryPKHash := fmt.Sprintf("select /*+ use_index_merge(tpkhash, idx_ac, idx_bc) */ * from tpkhash where a = %v or b = %v order by c limit %v", a, b, limit)
		resPKHash := tk.MustQuery(queryPKHash).Rows()
		require.True(t, tk.HasPlan(queryPKHash, "IndexMerge"))
		require.False(t, tk.HasPlan(queryPKHash, "TopN"))

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
