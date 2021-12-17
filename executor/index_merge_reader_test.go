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

package executor_test

import (
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/israce"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestSingleTableRead(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
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

func (s *testSuite1) TestJoin(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
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

func (s *testSuite1) TestIndexMergeReaderAndGeneratedColumn(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT AS (1), c1 INT PRIMARY KEY)")
	tk.MustExec("INSERT INTO t0(c1) VALUES (0)")
	tk.MustExec("CREATE INDEX i0 ON t0(c0)")
	tk.MustQuery("SELECT /*+ USE_INDEX_MERGE(t0, i0, PRIMARY)*/ t0.c0 FROM t0 WHERE t0.c1 OR t0.c0").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT t0.c0 FROM t0 WHERE t0.c1 OR t0.c0").Check(testkit.Rows("1"))
}

// issue 25045
func (s *testSuite1) TestIndexMergeReaderIssue25045(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int primary key, b int, c int, key(b), key(c));")
	tk.MustExec("INSERT INTO t1 VALUES (10, 10, 10), (11, 11, 11)")
	tk.MustQuery("explain format='brief' select /*+ use_index_merge(t1) */ * from t1 where c=10 or (b=10 and a=10);").Check(testkit.Rows(
		"IndexMerge 0.01 root  ",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t1, index:c(c) range:[10,10], keep order:false, stats:pseudo",
		"├─TableRangeScan(Build) 1.00 cop[tikv] table:t1 range:[10,10], keep order:false, stats:pseudo",
		"└─Selection(Probe) 0.01 cop[tikv]  or(eq(test.t1.c, 10), and(eq(test.t1.b, 10), eq(test.t1.a, 10)))",
		"  └─TableRowIDScan 11.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where c=10 or (b=10 and a=10);").Check(testkit.Rows("10 10 10"))
}

func (s *testSuite1) TestIssue16910(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
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

func (s *testSuite1) TestIndexMergeCausePanic(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_index_merge = 1;")
	tk.MustExec("create table t (a int, b int, c int, primary key(a), key(b))")
	tk.MustQuery("explain select /*+ inl_join(t2) */ * from t t1 join t t2 on t1.a = t2.a and t1.c = t2.c where t2.a = 1 or t2.b = 1")
}

func (s *testSuite1) TestPartitionTableRandomIndexMerge(c *C) {
	if israce.RaceEnabled {
		c.Skip("exhaustive types test, skip race test")
	}
	tk := testkit.NewTestKit(c, s.store)
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

func (s *testSuite1) TestIndexMergeWithPreparedStmt(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test;")
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
	tk.Se.SetSessionManager(&mockSessionManager1{
		PS: []*util.ProcessInfo{tk.Se.ShowProcess()},
	})
	explainStr := "explain for connection " + strconv.FormatUint(tk.Se.ShowProcess().ID, 10)
	res := tk.MustQuery(explainStr)
	indexMergeLine := res.Rows()[1][0].(string)
	re, err := regexp.Compile(".*IndexMerge.*")
	c.Assert(err, IsNil)
	c.Assert(re.MatchString(indexMergeLine), IsTrue)

	tk.MustExec("prepare stmt1 from 'select /*+ use_index_merge(t1) */ count(1) from t1 where c1 < ? or c2 < ? and c3';")
	tk.MustExec("set @a = 10;")
	tk.MustQuery("execute stmt1 using @a, @a;").Check(testkit.Rows("10"))
	res = tk.MustQuery(explainStr)
	indexMergeLine = res.Rows()[1][0].(string)
	c.Assert(re.MatchString(indexMergeLine), IsTrue)
}

func (s *testSuite1) TestIndexMergeInTransaction(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	for i := 0; i < 2; i++ {
		tk.MustExec("drop table if exists t1;")
		tk.MustExec("create table t1(c1 int, c2 int, c3 int, pk int, key(c1), key(c2), key(c3), primary key(pk));")
		if i == 1 {
			tk.MustExec("set tx_isolation = 'READ-COMMITTED';")
		}
		tk.MustExec("begin;")
		// Expect two IndexScan(c1, c2).
		tk.MustQuery("explain select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c2 < 10) and c3 < 10;").Check(testkit.Rows(
			"IndexMerge_9 1841.86 root  ",
			"├─IndexRangeScan_5(Build) 3323.33 cop[tikv] table:t1, index:c1(c1) range:[-inf,10), keep order:false, stats:pseudo",
			"├─IndexRangeScan_6(Build) 3323.33 cop[tikv] table:t1, index:c2(c2) range:[-inf,10), keep order:false, stats:pseudo",
			"└─Selection_8(Probe) 1841.86 cop[tikv]  lt(test.t1.c3, 10)",
			"  └─TableRowIDScan_7 5542.21 cop[tikv] table:t1 keep order:false, stats:pseudo"))
		// Expect one IndexScan(c2) and one TableScan(pk).
		tk.MustQuery("explain select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < 10) and c3 < 10;").Check(testkit.Rows(
			"IndexMerge_9 1106.67 root  ",
			"├─TableRangeScan_5(Build) 3333.33 cop[tikv] table:t1 range:[-inf,10), keep order:false, stats:pseudo",
			"├─IndexRangeScan_6(Build) 3323.33 cop[tikv] table:t1, index:c2(c2) range:[-inf,10), keep order:false, stats:pseudo",
			"└─Selection_8(Probe) 1106.67 cop[tikv]  lt(test.t1.c3, 10)",
			"  └─TableRowIDScan_7 3330.01 cop[tikv] table:t1 keep order:false, stats:pseudo"))

		// Test with normal key.
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c2 < 10) and c3 < 10;").Check(testkit.Rows())
		tk.MustExec("insert into t1 values(1, 1, 1, 1);")
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c2 < 10) and c3 < 10;").Check(testkit.Rows("1 1 1 1"))
		tk.MustExec("update t1 set c3 = 100 where c3 = 1;")
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c2 < 10) and c3 < 10;").Check(testkit.Rows())
		tk.MustExec("delete from t1;")
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 10 or c2 < 10) and c3 < 10;").Check(testkit.Rows())

		// Test with primary key, so the partialPlan is TableScan.
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < 10) and c3 < 10;").Check(testkit.Rows())
		tk.MustExec("insert into t1 values(1, 1, 1, 1);")
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < 10) and c3 < 10;").Check(testkit.Rows("1 1 1 1"))
		tk.MustExec("update t1 set c3 = 100 where c3 = 1;")
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < 10) and c3 < 10;").Check(testkit.Rows())
		tk.MustExec("delete from t1;")
		tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < 10) and c3 < 10;").Check(testkit.Rows())
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
		"└─IndexMerge_11 1841.86 root  ",
		"  ├─IndexRangeScan_7(Build) 3323.33 cop[tikv] table:t1, index:c1(c1) range:[-inf,10), keep order:false, stats:pseudo",
		"  ├─IndexRangeScan_8(Build) 3323.33 cop[tikv] table:t1, index:c2(c2) range:[-inf,10), keep order:false, stats:pseudo",
		"  └─Selection_10(Probe) 1841.86 cop[tikv]  lt(test.t1.c3, 10)",
		"    └─TableRowIDScan_9 5542.21 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery("explain select /*+ use_index_merge(t1) */ * from t1 where (pk < 10 or c2 < 10) and c3 < 10 for update;").Check(testkit.Rows(
		"SelectLock_6 1106.67 root  for update 0",
		"└─IndexMerge_11 1106.67 root  ",
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
	res := tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 20 or c2 < 20) and c3 < 20;").Sort()
	res.Check(testkit.Rows("1 1 1 1 1", "11 11 11 11 11"))
	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 20 or c2 < 20) and c3 < 20;").Sort()
	res.Check(testkit.Rows("1 1 1 1 1", "11 11 11 11 11"))

	tk.MustExec("update t1 set c3 = 100 where c3 = 1;")
	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 20 or c2 < 20) and c3 < 20;")
	res.Check(testkit.Rows("11 11 11 11 11"))
	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 20 or c2 < 20) and c3 < 20;")
	res.Check(testkit.Rows("11 11 11 11 11"))

	tk.MustExec("delete from t1;")
	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (c1 < 20 or c2 < 20) and c3 < 20;")
	res.Check(testkit.Rows())
	res = tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where (pk < 20 or c2 < 20) and c3 < 20;")
	res.Check(testkit.Rows())
	tk.MustExec("commit;")
}

func (test *testSerialSuite2) TestIndexMergeReaderMemTracker(c *C) {
	tk := testkit.NewTestKit(c, test.store)
	tk.MustExec("use test;")
	tk.MustExec("create table t1(c1 int, c2 int, c3 int, key(c1), key(c2), key(c3));")

	insertStr := "insert into t1 values(0, 0, 0)"
	rowNum := 1000
	for i := 0; i < rowNum; i++ {
		insertStr += fmt.Sprintf(" ,(%d, %d, %d)", i, i, i)
	}
	insertStr += ";"
	memTracker := tk.Se.GetSessionVars().StmtCtx.MemTracker

	tk.MustExec(insertStr)

	oriMaxUsage := memTracker.MaxConsumed()

	// We select all rows in t1, so the mem usage is more clear.
	tk.MustQuery("select /*+ use_index_merge(t1) */ * from t1 where c1 > 1 or c2 > 1")

	newMaxUsage := memTracker.MaxConsumed()
	c.Assert(newMaxUsage, Greater, oriMaxUsage)

	res := tk.MustQuery("explain analyze select /*+ use_index_merge(t1) */ * from t1 where c1 > 1 or c2 > 1")
	c.Assert(len(res.Rows()), Equals, 4)
	// Parse "xxx KB" and check it's greater than 0.
	memStr := res.Rows()[0][7].(string)
	re, err := regexp.Compile("[0-9]+ KB")
	c.Assert(err, IsNil)
	c.Assert(re.MatchString(memStr), IsTrue)
	bytes, err := strconv.ParseFloat(memStr[:len(memStr)-3], 32)
	c.Assert(err, IsNil)
	c.Assert(bytes, Greater, 0.0)
}
