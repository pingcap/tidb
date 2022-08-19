// Copyright 2020 PingCAP, Inc.
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
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestFourReader(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists pt")
	tk.MustExec(`create table pt (id int, c int, key i_id(id), key i_c(c)) partition by range (c) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (10))`)
	tk.MustExec("insert into pt values (0, 0), (2, 2), (4, 4), (6, 6), (7, 7), (9, 9), (null, null)")

	// Table reader
	tk.MustQuery("select * from pt").Sort().Check(testkit.Rows("0 0", "2 2", "4 4", "6 6", "7 7", "9 9", "<nil> <nil>"))
	// Table reader: table dual
	tk.MustQuery("select * from pt where c > 10").Check(testkit.Rows())
	// Table reader: one partition
	tk.MustQuery("select * from pt where c > 8").Check(testkit.Rows("9 9"))
	// Table reader: more than one partition
	tk.MustQuery("select * from pt where c < 2 or c >= 9").Check(testkit.Rows("0 0", "9 9"))

	// Index reader
	tk.MustQuery("select c from pt").Sort().Check(testkit.Rows("0", "2", "4", "6", "7", "9", "<nil>"))
	tk.MustQuery("select c from pt where c > 10").Check(testkit.Rows())
	tk.MustQuery("select c from pt where c > 8").Check(testkit.Rows("9"))
	tk.MustQuery("select c from pt where c < 2 or c >= 9").Sort().Check(testkit.Rows("0", "9"))

	// Index lookup
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt").Sort().Check(testkit.Rows("0 0", "2 2", "4 4", "6 6", "7 7", "9 9", "<nil> <nil>"))
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt where id < 4 and c > 10").Check(testkit.Rows())
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt where id < 10 and c > 8").Check(testkit.Rows("9 9"))
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt where id < 10 and c < 2 or c >= 9").Check(testkit.Rows("0 0", "9 9"))

	// Index Merge
	tk.MustExec("set @@tidb_enable_index_merge = 1")
	tk.MustQuery("select /*+ use_index(i_c, i_id) */ * from pt where id = 4 or c < 7").Sort().Check(testkit.Rows("0 0", "2 2", "4 4", "6 6"))
}

func TestPartitionIndexJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_enable_table_partition = 1")
	tk.MustExec("set @@session.tidb_enable_list_partition = 1")
	for i := 0; i < 3; i++ {
		tk.MustExec("drop table if exists p, t")
		if i == 0 {
			// Test for range partition
			tk.MustExec(`create table p (id int, c int, key i_id(id), key i_c(c)) partition by range (c) (
				partition p0 values less than (4),
				partition p1 values less than (7),
				partition p2 values less than (10))`)
		} else if i == 1 {
			// Test for list partition
			tk.MustExec(`create table p (id int, c int, key i_id(id), key i_c(c)) partition by list (c) (
				partition p0 values in (1,2,3,4),
				partition p1 values in (5,6,7),
				partition p2 values in (8, 9,10))`)
		} else {
			// Test for hash partition
			tk.MustExec(`create table p (id int, c int, key i_id(id), key i_c(c)) partition by hash(c) partitions 5;`)
		}

		tk.MustExec("create table t (id int)")
		tk.MustExec("insert into p values (3,3), (4,4), (6,6), (9,9)")
		tk.MustExec("insert into t values (4), (9)")

		// Build indexLookUp in index join
		tk.MustQuery("select /*+ INL_JOIN(p) */ * from p, t where p.id = t.id").Sort().Check(testkit.Rows("4 4 4", "9 9 9"))
		// Build index reader in index join
		tk.MustQuery("select /*+ INL_JOIN(p) */ p.id from p, t where p.id = t.id").Check(testkit.Rows("4", "9"))
	}
}

func TestPartitionUnionScanIndexJoin(t *testing.T) {
	// For issue https://github.com/pingcap/tidb/issues/19152
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1  (c_int int, c_str varchar(40), primary key (c_int)) partition by range (c_int) ( partition p0 values less than (10), partition p1 values less than maxvalue)")
	tk.MustExec("create table t2  (c_int int, c_str varchar(40), primary key (c_int, c_str)) partition by hash (c_int) partitions 4")
	tk.MustExec("insert into t1 values (10, 'interesting neumann')")
	tk.MustExec("insert into t2 select * from t1")
	tk.MustExec("begin")
	tk.MustExec("insert into t2 values (11, 'hopeful hoover');")
	tk.MustQuery("select /*+ INL_JOIN(t1,t2) */  * from t1 join t2 on t1.c_int = t2.c_int and t1.c_str = t2.c_str where t1.c_int in (10, 11)").Check(testkit.Rows("10 interesting neumann 10 interesting neumann"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1,t2) */  * from t1 join t2 on t1.c_int = t2.c_int and t1.c_str = t2.c_str where t1.c_int in (10, 11)").Check(testkit.Rows("10 interesting neumann 10 interesting neumann"))
	tk.MustExec("commit")
}

func TestPointGetwithRangeAndListPartitionTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_pointget_list_hash")
	tk.MustExec("use test_pointget_list_hash")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")

	// list partition table
	tk.MustExec(`create table tlist(a int, b int, unique index idx_a(a), index idx_b(b)) partition by list(a)(
		partition p0 values in (NULL, 1, 2, 3, 4),
			partition p1 values in (5, 6, 7, 8),
			partition p2 values in (9, 10, 11, 12));`)

	// range partition table
	tk.MustExec(`create table trange1(a int, unique key(a)) partition by range(a) (
		partition p0 values less than (30),
		partition p1 values less than (60),
		partition p2 values less than (90),
		partition p3 values less than (120));`)

	// range partition table + unsigned int
	tk.MustExec(`create table trange2(a int unsigned, unique key(a)) partition by range(a) (
		partition p0 values less than (30),
		partition p1 values less than (60),
		partition p2 values less than (90),
		partition p3 values less than (120));`)

	// insert data into list partition table
	tk.MustExec("insert into tlist values(1,1), (2,2), (3, 3), (4, 4), (5,5), (6, 6), (7,7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (NULL, NULL);")

	vals := make([]string, 0, 100)
	// insert data into range partition table and hash partition table
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%v)", i+1))
	}
	tk.MustExec("insert into trange1 values " + strings.Join(vals, ","))
	tk.MustExec("insert into trange2 values " + strings.Join(vals, ","))

	// test PointGet
	for i := 0; i < 100; i++ {
		// explain select a from t where a = {x}; // x >= 1 and x <= 100 Check if PointGet is used
		// select a from t where a={x}; // the result is {x}
		x := rand.Intn(100) + 1
		queryRange1 := fmt.Sprintf("select a from trange1 where a=%v", x)
		require.True(t, tk.HasPlan(queryRange1, "Point_Get")) // check if PointGet is used
		tk.MustQuery(queryRange1).Check(testkit.Rows(fmt.Sprintf("%v", x)))

		queryRange2 := fmt.Sprintf("select a from trange1 where a=%v", x)
		require.True(t, tk.HasPlan(queryRange2, "Point_Get")) // check if PointGet is used
		tk.MustQuery(queryRange2).Check(testkit.Rows(fmt.Sprintf("%v", x)))

		y := rand.Intn(12) + 1
		queryList := fmt.Sprintf("select a from tlist where a=%v", y)
		require.True(t, tk.HasPlan(queryList, "Point_Get")) // check if PointGet is used
		tk.MustQuery(queryList).Check(testkit.Rows(fmt.Sprintf("%v", y)))
	}

	// test table dual
	queryRange1 := "select a from trange1 where a=200"
	require.True(t, tk.HasPlan(queryRange1, "TableDual")) // check if TableDual is used
	tk.MustQuery(queryRange1).Check(testkit.Rows())

	queryRange2 := "select a from trange2 where a=200"
	require.True(t, tk.HasPlan(queryRange2, "TableDual")) // check if TableDual is used
	tk.MustQuery(queryRange2).Check(testkit.Rows())

	queryList := "select a from tlist where a=200"
	require.True(t, tk.HasPlan(queryList, "TableDual")) // check if TableDual is used
	tk.MustQuery(queryList).Check(testkit.Rows())
}

func TestPartitionReaderUnderApply(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// For issue 19458.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c_int int)")
	tk.MustExec("insert into t values(1), (2), (3), (4), (5), (6), (7), (8), (9)")
	tk.MustExec("DROP TABLE IF EXISTS `t1`")
	tk.MustExec(`CREATE TABLE t1 (
		  c_int int NOT NULL,
		  c_str varchar(40) NOT NULL,
		  c_datetime datetime NOT NULL,
		  c_timestamp timestamp NULL DEFAULT NULL,
		  c_double double DEFAULT NULL,
		  c_decimal decimal(12,6) DEFAULT NULL,
		  PRIMARY KEY (c_int,c_str,c_datetime)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
		 PARTITION BY RANGE (c_int)
		(PARTITION p0 VALUES LESS THAN (2) ENGINE = InnoDB,
		 PARTITION p1 VALUES LESS THAN (4) ENGINE = InnoDB,
		 PARTITION p2 VALUES LESS THAN (6) ENGINE = InnoDB,
		 PARTITION p3 VALUES LESS THAN (8) ENGINE = InnoDB,
		 PARTITION p4 VALUES LESS THAN (10) ENGINE = InnoDB,
		 PARTITION p5 VALUES LESS THAN (20) ENGINE = InnoDB,
		 PARTITION p6 VALUES LESS THAN (50) ENGINE = InnoDB,
		 PARTITION p7 VALUES LESS THAN (1000000000) ENGINE = InnoDB)`)
	tk.MustExec("INSERT INTO `t1` VALUES (19,'nifty feistel','2020-02-28 04:01:28','2020-02-04 06:11:57',32.430079,1.284000),(20,'objective snyder','2020-04-15 17:55:04','2020-05-30 22:04:13',37.690874,9.372000)")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values (22, 'wizardly saha', '2020-05-03 16:35:22', '2020-05-03 02:18:42', 96.534810, 0.088)")
	tk.MustQuery("select c_int from t where (select min(t1.c_int) from t1 where t1.c_int > t.c_int) > (select count(*) from t1 where t1.c_int > t.c_int) order by c_int").Check(testkit.Rows(
		"1", "2", "3", "4", "5", "6", "7", "8", "9"))
	tk.MustExec("rollback")

	// For issue 19450.
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1  (c_int int, c_str varchar(40), c_decimal decimal(12, 6), primary key (c_int))")
	tk.MustExec("create table t2  (c_int int, c_str varchar(40), c_decimal decimal(12, 6), primary key (c_int)) partition by hash (c_int) partitions 4")
	tk.MustExec("insert into t1 values (1, 'romantic robinson', 4.436), (2, 'stoic chaplygin', 9.826), (3, 'vibrant shamir', 6.300), (4, 'hungry wilson', 4.900), (5, 'naughty swartz', 9.524)")
	tk.MustExec("insert into t2 select * from t1")
	tk.MustQuery("select * from t1 where c_decimal in (select c_decimal from t2 where t1.c_int = t2.c_int or t1.c_int = t2.c_int and t1.c_str > t2.c_str)").Check(testkit.Rows(
		"1 romantic robinson 4.436000",
		"2 stoic chaplygin 9.826000",
		"3 vibrant shamir 6.300000",
		"4 hungry wilson 4.900000",
		"5 naughty swartz 9.524000"))

	// For issue 19450 release-4.0
	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
	tk.MustQuery("select * from t1 where c_decimal in (select c_decimal from t2 where t1.c_int = t2.c_int or t1.c_int = t2.c_int and t1.c_str > t2.c_str)").Check(testkit.Rows(
		"1 romantic robinson 4.436000",
		"2 stoic chaplygin 9.826000",
		"3 vibrant shamir 6.300000",
		"4 hungry wilson 4.900000",
		"5 naughty swartz 9.524000"))
}

func TestImproveCoverage(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table coverage_rr (
pk1 varchar(35) NOT NULL,
pk2 int NOT NULL,
c int,
PRIMARY KEY (pk1,pk2)) partition by hash(pk2) partitions 4;`)
	tk.MustExec("create table coverage_dt (pk1 varchar(35), pk2 int)")
	tk.MustExec("insert into coverage_rr values ('ios', 3, 2),('android', 4, 7),('linux',5,1)")
	tk.MustExec("insert into coverage_dt values ('apple',3),('ios',3),('linux',5)")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustQuery("select /*+ INL_JOIN(dt, rr) */ * from coverage_dt dt join coverage_rr rr on (dt.pk1 = rr.pk1 and dt.pk2 = rr.pk2);").Sort().Check(testkit.Rows("ios 3 ios 3 2", "linux 5 linux 5 1"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(dt, rr) */ * from coverage_dt dt join coverage_rr rr on (dt.pk1 = rr.pk1 and dt.pk2 = rr.pk2);").Sort().Check(testkit.Rows("ios 3 ios 3 2", "linux 5 linux 5 1"))
}

func TestPartitionInfoDisable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_info_null")
	tk.MustExec(`CREATE TABLE t_info_null (
  id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  date date NOT NULL,
  media varchar(32) NOT NULL DEFAULT '0',
  app varchar(32) NOT NULL DEFAULT '',
  xxx bigint(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (id, date),
  UNIQUE KEY idx_media_id (media, date, app)
) PARTITION BY RANGE COLUMNS(date) (
  PARTITION p201912 VALUES LESS THAN ("2020-01-01"),
  PARTITION p202001 VALUES LESS THAN ("2020-02-01"),
  PARTITION p202002 VALUES LESS THAN ("2020-03-01"),
  PARTITION p202003 VALUES LESS THAN ("2020-04-01"),
  PARTITION p202004 VALUES LESS THAN ("2020-05-01"),
  PARTITION p202005 VALUES LESS THAN ("2020-06-01"),
  PARTITION p202006 VALUES LESS THAN ("2020-07-01"),
  PARTITION p202007 VALUES LESS THAN ("2020-08-01"),
  PARTITION p202008 VALUES LESS THAN ("2020-09-01"),
  PARTITION p202009 VALUES LESS THAN ("2020-10-01"),
  PARTITION p202010 VALUES LESS THAN ("2020-11-01"),
  PARTITION p202011 VALUES LESS THAN ("2020-12-01")
)`)
	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t_info_null"))
	require.NoError(t, err)

	tbInfo := tbl.Meta()
	// Mock for a case that the tableInfo.Partition is not nil, but tableInfo.Partition.Enable is false.
	// That may happen when upgrading from a old version TiDB.
	tbInfo.Partition.Enable = false
	tbInfo.Partition.Num = 0

	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
	tk.MustQuery("explain select * from t_info_null where (date = '2020-10-02' or date = '2020-10-06') and app = 'xxx' and media = '19003006'").Check(testkit.Rows("Batch_Point_Get_5 2.00 root table:t_info_null, index:idx_media_id(media, date, app) keep order:false, desc:false"))
	tk.MustQuery("explain select * from t_info_null").Check(testkit.Rows("TableReader_5 10000.00 root  data:TableFullScan_4",
		"└─TableFullScan_4 10000.00 cop[tikv] table:t_info_null keep order:false, stats:pseudo"))
	// No panic.
	tk.MustQuery("select * from t_info_null where (date = '2020-10-02' or date = '2020-10-06') and app = 'xxx' and media = '19003006'").Check(testkit.Rows())
}

func TestOrderByandLimit(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_orderby_limit")
	tk.MustExec("use test_orderby_limit")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// range partition table
	tk.MustExec(`create table trange(a int, b int, index idx_a(a)) partition by range(a) (
		partition p0 values less than(300),
		partition p1 values less than (500),
		partition p2 values less than(1100));`)

	// hash partition table
	tk.MustExec("create table thash(a int, b int, index idx_a(a), index idx_b(b)) partition by hash(a) partitions 4;")

	// regular table
	tk.MustExec("create table tregular(a int, b int, index idx_a(a))")

	// generate some random data to be inserted
	vals := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(1100), rand.Intn(2000)))
	}
	tk.MustExec("insert into trange values " + strings.Join(vals, ","))
	tk.MustExec("insert into thash values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular values " + strings.Join(vals, ","))

	// test indexLookUp
	for i := 0; i < 100; i++ {
		// explain select * from t where a > {y}  use index(idx_a) order by a limit {x}; // check if IndexLookUp is used
		// select * from t where a > {y} use index(idx_a) order by a limit {x}; // it can return the correct result
		x := rand.Intn(1099)
		y := rand.Intn(2000) + 1
		queryPartition := fmt.Sprintf("select * from trange use index(idx_a) where a > %v order by a, b limit %v;", x, y)
		queryRegular := fmt.Sprintf("select * from tregular use index(idx_a) where a > %v order by a, b limit %v;", x, y)
		require.True(t, tk.HasPlan(queryPartition, "IndexLookUp")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
	}

	// test tableReader
	for i := 0; i < 100; i++ {
		// explain select * from t where a > {y}  ignore index(idx_a) order by a limit {x}; // check if IndexLookUp is used
		// select * from t where a > {y} ignore index(idx_a) order by a limit {x}; // it can return the correct result
		x := rand.Intn(1099)
		y := rand.Intn(2000) + 1
		queryPartition := fmt.Sprintf("select * from trange ignore index(idx_a) where a > %v order by a, b limit %v;", x, y)
		queryRegular := fmt.Sprintf("select * from tregular ignore index(idx_a) where a > %v order by a, b limit %v;", x, y)
		require.True(t, tk.HasPlan(queryPartition, "TableReader")) // check if tableReader is used
		tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
	}

	// test indexReader
	for i := 0; i < 100; i++ {
		// explain select a from t where a > {y}  use index(idx_a) order by a limit {x}; // check if IndexLookUp is used
		// select a from t where a > {y} use index(idx_a) order by a limit {x}; // it can return the correct result
		x := rand.Intn(1099)
		y := rand.Intn(2000) + 1
		queryPartition := fmt.Sprintf("select a from trange use index(idx_a) where a > %v order by a limit %v;", x, y)
		queryRegular := fmt.Sprintf("select a from tregular use index(idx_a) where a > %v order by a limit %v;", x, y)
		require.True(t, tk.HasPlan(queryPartition, "IndexReader")) // check if indexReader is used
		tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
	}

	// test indexMerge
	for i := 0; i < 100; i++ {
		// explain select /*+ use_index_merge(t) */ * from t where a > 2 or b < 5 order by a limit {x}; // check if IndexMerge is used
		// select /*+ use_index_merge(t) */ * from t where a > 2 or b < 5 order by a limit {x};  // can return the correct value
		y := rand.Intn(2000) + 1
		queryPartition := fmt.Sprintf("select /*+ use_index_merge(thash) */ * from thash where a > 2 or b < 5 order by a, b limit %v;", y)
		queryRegular := fmt.Sprintf("select * from tregular where a > 2 or b < 5 order by a, b limit %v;", y)
		require.True(t, tk.HasPlan(queryPartition, "IndexMerge")) // check if indexMerge is used
		tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
	}
}

func TestBatchGetandPointGetwithHashPartition(t *testing.T) {

	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_batchget_pointget")
	tk.MustExec("use test_batchget_pointget")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// hash partition table
	tk.MustExec("create table thash(a int, unique key(a)) partition by hash(a) partitions 4;")

	// regular partition table
	tk.MustExec("create table tregular(a int, unique key(a));")

	vals := make([]string, 0, 100)
	// insert data into range partition table and hash partition table
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%v)", i+1))
	}
	tk.MustExec("insert into thash values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular values " + strings.Join(vals, ","))

	// test PointGet
	for i := 0; i < 100; i++ {
		// explain select a from t where a = {x}; // x >= 1 and x <= 100 Check if PointGet is used
		// select a from t where a={x}; // the result is {x}
		x := rand.Intn(100) + 1
		queryHash := fmt.Sprintf("select a from thash where a=%v", x)
		queryRegular := fmt.Sprintf("select a from tregular where a=%v", x)
		require.True(t, tk.HasPlan(queryHash, "Point_Get")) // check if PointGet is used
		tk.MustQuery(queryHash).Check(tk.MustQuery(queryRegular).Rows())
	}

	// test empty PointGet
	queryHash := "select a from thash where a=200"
	require.True(t, tk.HasPlan(queryHash, "Point_Get")) // check if PointGet is used
	tk.MustQuery(queryHash).Check(testkit.Rows())

	// test BatchGet
	for i := 0; i < 100; i++ {
		// explain select a from t where a in ({x1}, {x2}, ... {x10}); // BatchGet is used
		// select a from t where where a in ({x1}, {x2}, ... {x10});
		points := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			x := rand.Intn(100) + 1
			points = append(points, fmt.Sprintf("%v", x))
		}

		queryHash := fmt.Sprintf("select a from thash where a in (%v)", strings.Join(points, ","))
		queryRegular := fmt.Sprintf("select a from tregular where a in (%v)", strings.Join(points, ","))
		require.True(t, tk.HasPlan(queryHash, "Point_Get")) // check if PointGet is used
		tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
	}
}

func TestView(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_view")
	tk.MustExec("use test_view")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table thash (a int, b int, key(a)) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a varchar(10), b varchar(10), key(a)) partition by range columns(a) (
						partition p0 values less than ('300'),
						partition p1 values less than ('600'),
						partition p2 values less than ('900'),
						partition p3 values less than ('9999'))`)
	tk.MustExec(`create table t1 (a int, b int, key(a))`)
	tk.MustExec(`create table t2 (a varchar(10), b varchar(10), key(a))`)

	// insert the same data into thash and t1
	vals := make([]string, 0, 3000)
	for i := 0; i < 3000; i++ {
		vals = append(vals, fmt.Sprintf(`(%v, %v)`, rand.Intn(10000), rand.Intn(10000)))
	}
	tk.MustExec(fmt.Sprintf(`insert into thash values %v`, strings.Join(vals, ", ")))
	tk.MustExec(fmt.Sprintf(`insert into t1 values %v`, strings.Join(vals, ", ")))

	// insert the same data into trange and t2
	vals = vals[:0]
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf(`("%v", "%v")`, rand.Intn(1000), rand.Intn(1000)))
	}
	tk.MustExec(fmt.Sprintf(`insert into trange values %v`, strings.Join(vals, ", ")))
	tk.MustExec(fmt.Sprintf(`insert into t2 values %v`, strings.Join(vals, ", ")))

	// test views on a single table
	tk.MustExec(`create definer='root'@'localhost' view vhash as select a*2 as a, a+b as b from thash`)
	tk.MustExec(`create definer='root'@'localhost' view v1 as select a*2 as a, a+b as b from t1`)
	tk.MustExec(`create definer='root'@'localhost' view vrange as select concat(a, b) as a, a+b as b from trange`)
	tk.MustExec(`create definer='root'@'localhost' view v2 as select concat(a, b) as a, a+b as b from t2`)
	for i := 0; i < 100; i++ {
		xhash := rand.Intn(10000)
		tk.MustQuery(fmt.Sprintf(`select * from vhash where a>=%v`, xhash)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from v1 where a>=%v`, xhash)).Sort().Rows())
		tk.MustQuery(fmt.Sprintf(`select * from vhash where b>=%v`, xhash)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from v1 where b>=%v`, xhash)).Sort().Rows())
		tk.MustQuery(fmt.Sprintf(`select * from vhash where a>=%v and b>=%v`, xhash, xhash)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from v1 where a>=%v and b>=%v`, xhash, xhash)).Sort().Rows())

		xrange := fmt.Sprintf(`"%v"`, rand.Intn(1000))
		tk.MustQuery(fmt.Sprintf(`select * from vrange where a>=%v`, xrange)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from v2 where a>=%v`, xrange)).Sort().Rows())
		tk.MustQuery(fmt.Sprintf(`select * from vrange where b>=%v`, xrange)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from v2 where b>=%v`, xrange)).Sort().Rows())
		tk.MustQuery(fmt.Sprintf(`select * from vrange where a>=%v and b<=%v`, xrange, xrange)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from v2 where a>=%v and b<=%v`, xrange, xrange)).Sort().Rows())
	}

	// test views on both tables
	tk.MustExec(`create definer='root'@'localhost' view vboth as select thash.a+trange.a as a, thash.b+trange.b as b from thash, trange where thash.a=trange.a`)
	tk.MustExec(`create definer='root'@'localhost' view vt as select t1.a+t2.a as a, t1.b+t2.b as b from t1, t2 where t1.a=t2.a`)
	for i := 0; i < 100; i++ {
		x := rand.Intn(10000)
		tk.MustQuery(fmt.Sprintf(`select * from vboth where a>=%v`, x)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from vt where a>=%v`, x)).Sort().Rows())
		tk.MustQuery(fmt.Sprintf(`select * from vboth where b>=%v`, x)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from vt where b>=%v`, x)).Sort().Rows())
		tk.MustQuery(fmt.Sprintf(`select * from vboth where a>=%v and b>=%v`, x, x)).Sort().Check(
			tk.MustQuery(fmt.Sprintf(`select * from vt where a>=%v and b>=%v`, x, x)).Sort().Rows())
	}
}

func TestDirectReadingwithIndexJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_dr_join")
	tk.MustExec("use test_dr_join")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// hash and range partition
	tk.MustExec("create table thash (a int, b int, c int, primary key(a), index idx_b(b)) partition by hash(a) partitions 4;")
	tk.MustExec(`create table trange (a int, b int, c int, primary key(a), index idx_b(b)) partition by range(a) (
		 partition p0 values less than(1000),
		 partition p1 values less than(2000),
		 partition p2 values less than(3000),
		 partition p3 values less than(4000));`)

	// regualr table
	tk.MustExec(`create table tnormal (a int, b int, c int, primary key(a), index idx_b(b));`)
	tk.MustExec(`create table touter (a int, b int, c int);`)

	// generate some random data to be inserted
	vals := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v, %v)", rand.Intn(4000), rand.Intn(4000), rand.Intn(4000)))
	}
	tk.MustExec("insert ignore into trange values " + strings.Join(vals, ","))
	tk.MustExec("insert ignore into thash values " + strings.Join(vals, ","))
	tk.MustExec("insert ignore into tnormal values " + strings.Join(vals, ","))
	tk.MustExec("insert ignore into touter values " + strings.Join(vals, ","))

	// test indexLookUp + hash
	queryPartition := "select /*+ INL_JOIN(touter, thash) */ * from touter join thash use index(idx_b) on touter.b = thash.b"
	queryRegular := "select /*+ INL_JOIN(touter, tnormal) */ * from touter join tnormal use index(idx_b) on touter.b = tnormal.b"
	tk.MustQuery("explain format = 'brief' " + queryPartition).Check(testkit.Rows(
		"IndexJoin 12487.50 root  inner join, inner:IndexLookUp, outer key:test_dr_join.touter.b, inner key:test_dr_join.thash.b, equal cond:eq(test_dr_join.touter.b, test_dr_join.thash.b)",
		"├─TableReader(Build) 9990.00 root  data:Selection",
		"│ └─Selection 9990.00 cop[tikv]  not(isnull(test_dr_join.touter.b))",
		"│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo",
		"└─IndexLookUp(Probe) 1.25 root partition:all ",
		"  ├─Selection(Build) 1.25 cop[tikv]  not(isnull(test_dr_join.thash.b))",
		"  │ └─IndexRangeScan 1.25 cop[tikv] table:thash, index:idx_b(b) range: decided by [eq(test_dr_join.thash.b, test_dr_join.touter.b)], keep order:false, stats:pseudo",
		"  └─TableRowIDScan(Probe) 1.25 cop[tikv] table:thash keep order:false, stats:pseudo")) // check if IndexLookUp is used
	tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// test tableReader + hash
	queryPartition = "select /*+ INL_JOIN(touter, thash) */ * from touter join thash on touter.a = thash.a"
	queryRegular = "select /*+ INL_JOIN(touter, tnormal) */ * from touter join tnormal on touter.a = tnormal.a"
	tk.MustQuery("explain format = 'brief' " + queryPartition).Check(testkit.Rows(
		"IndexJoin 12487.50 root  inner join, inner:TableReader, outer key:test_dr_join.touter.a, inner key:test_dr_join.thash.a, equal cond:eq(test_dr_join.touter.a, test_dr_join.thash.a)",
		"├─TableReader(Build) 9990.00 root  data:Selection",
		"│ └─Selection 9990.00 cop[tikv]  not(isnull(test_dr_join.touter.a))",
		"│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo",
		"└─TableReader(Probe) 1.00 root partition:all data:TableRangeScan",
		"  └─TableRangeScan 1.00 cop[tikv] table:thash range: decided by [test_dr_join.touter.a], keep order:false, stats:pseudo")) // check if tableReader is used
	tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// test indexReader + hash
	queryPartition = "select /*+ INL_JOIN(touter, thash) */ thash.b from touter join thash use index(idx_b) on touter.b = thash.b;"
	queryRegular = "select /*+ INL_JOIN(touter, tnormal) */ tnormal.b from touter join tnormal use index(idx_b) on touter.b = tnormal.b;"
	tk.MustQuery("explain format = 'brief' " + queryPartition).Check(testkit.Rows(
		"IndexJoin 12487.50 root  inner join, inner:IndexReader, outer key:test_dr_join.touter.b, inner key:test_dr_join.thash.b, equal cond:eq(test_dr_join.touter.b, test_dr_join.thash.b)",
		"├─TableReader(Build) 9990.00 root  data:Selection",
		"│ └─Selection 9990.00 cop[tikv]  not(isnull(test_dr_join.touter.b))",
		"│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo",
		"└─IndexReader(Probe) 1.25 root partition:all index:Selection",
		"  └─Selection 1.25 cop[tikv]  not(isnull(test_dr_join.thash.b))",
		"    └─IndexRangeScan 1.25 cop[tikv] table:thash, index:idx_b(b) range: decided by [eq(test_dr_join.thash.b, test_dr_join.touter.b)], keep order:false, stats:pseudo")) // check if indexReader is used
	tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// test indexLookUp + range
	// explain select /*+ INL_JOIN(touter, tinner) */ * from touter join tinner use index(a) on touter.a = tinner.a;
	queryPartition = "select /*+ INL_JOIN(touter, trange) */ * from touter join trange use index(idx_b) on touter.b = trange.b;"
	queryRegular = "select /*+ INL_JOIN(touter, tnormal) */ * from touter join tnormal use index(idx_b) on touter.b = tnormal.b;"
	tk.MustQuery("explain format = 'brief' " + queryPartition).Check(testkit.Rows(
		"IndexJoin 12487.50 root  inner join, inner:IndexLookUp, outer key:test_dr_join.touter.b, inner key:test_dr_join.trange.b, equal cond:eq(test_dr_join.touter.b, test_dr_join.trange.b)",
		"├─TableReader(Build) 9990.00 root  data:Selection",
		"│ └─Selection 9990.00 cop[tikv]  not(isnull(test_dr_join.touter.b))",
		"│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo",
		"└─IndexLookUp(Probe) 1.25 root partition:all ",
		"  ├─Selection(Build) 1.25 cop[tikv]  not(isnull(test_dr_join.trange.b))",
		"  │ └─IndexRangeScan 1.25 cop[tikv] table:trange, index:idx_b(b) range: decided by [eq(test_dr_join.trange.b, test_dr_join.touter.b)], keep order:false, stats:pseudo",
		"  └─TableRowIDScan(Probe) 1.25 cop[tikv] table:trange keep order:false, stats:pseudo")) // check if IndexLookUp is used
	tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// test tableReader + range
	queryPartition = "select /*+ INL_JOIN(touter, trange) */ * from touter join trange on touter.a = trange.a;"
	queryRegular = "select /*+ INL_JOIN(touter, tnormal) */ * from touter join tnormal on touter.a = tnormal.a;"
	tk.MustQuery("explain format = 'brief' " + queryPartition).Check(testkit.Rows(
		"IndexJoin 12487.50 root  inner join, inner:TableReader, outer key:test_dr_join.touter.a, inner key:test_dr_join.trange.a, equal cond:eq(test_dr_join.touter.a, test_dr_join.trange.a)",
		"├─TableReader(Build) 9990.00 root  data:Selection",
		"│ └─Selection 9990.00 cop[tikv]  not(isnull(test_dr_join.touter.a))",
		"│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo",
		"└─TableReader(Probe) 1.00 root partition:all data:TableRangeScan",
		"  └─TableRangeScan 1.00 cop[tikv] table:trange range: decided by [test_dr_join.touter.a], keep order:false, stats:pseudo")) // check if tableReader is used
	tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// test indexReader + range
	// explain select /*+ INL_JOIN(touter, tinner) */ tinner.a from touter join tinner on touter.a = tinner.a;
	queryPartition = "select /*+ INL_JOIN(touter, trange) */ trange.b from touter join trange use index(idx_b) on touter.b = trange.b;"
	queryRegular = "select /*+ INL_JOIN(touter, tnormal) */ tnormal.b from touter join tnormal use index(idx_b) on touter.b = tnormal.b;"
	tk.MustQuery("explain format = 'brief' " + queryPartition).Check(testkit.Rows(
		"IndexJoin 12487.50 root  inner join, inner:IndexReader, outer key:test_dr_join.touter.b, inner key:test_dr_join.trange.b, equal cond:eq(test_dr_join.touter.b, test_dr_join.trange.b)",
		"├─TableReader(Build) 9990.00 root  data:Selection",
		"│ └─Selection 9990.00 cop[tikv]  not(isnull(test_dr_join.touter.b))",
		"│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo",
		"└─IndexReader(Probe) 1.25 root partition:all index:Selection",
		"  └─Selection 1.25 cop[tikv]  not(isnull(test_dr_join.trange.b))",
		"    └─IndexRangeScan 1.25 cop[tikv] table:trange, index:idx_b(b) range: decided by [eq(test_dr_join.trange.b, test_dr_join.touter.b)], keep order:false, stats:pseudo")) // check if indexReader is used
	tk.MustQuery(queryPartition).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
}

func TestDynamicPruningUnderIndexJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create database pruing_under_index_join")
	tk.MustExec("use pruing_under_index_join")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table tnormal (a int, b int, c int, primary key(a), index idx_b(b))`)
	tk.MustExec(`create table thash (a int, b int, c int, primary key(a), index idx_b(b)) partition by hash(a) partitions 4`)
	tk.MustExec(`create table touter (a int, b int, c int)`)

	vals := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v, %v)", i, rand.Intn(10000), rand.Intn(10000)))
	}
	tk.MustExec(`insert into tnormal values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into thash values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into touter values ` + strings.Join(vals, ", "))

	// case 1: IndexReader in the inner side
	tk.MustQuery(`explain format='brief' select /*+ INL_JOIN(touter, thash) */ thash.b from touter join thash use index(idx_b) on touter.b = thash.b`).Check(testkit.Rows(
		`IndexJoin 12487.50 root  inner join, inner:IndexReader, outer key:pruing_under_index_join.touter.b, inner key:pruing_under_index_join.thash.b, equal cond:eq(pruing_under_index_join.touter.b, pruing_under_index_join.thash.b)`,
		`├─TableReader(Build) 9990.00 root  data:Selection`,
		`│ └─Selection 9990.00 cop[tikv]  not(isnull(pruing_under_index_join.touter.b))`,
		`│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`└─IndexReader(Probe) 1.25 root partition:all index:Selection`,
		`  └─Selection 1.25 cop[tikv]  not(isnull(pruing_under_index_join.thash.b))`,
		`    └─IndexRangeScan 1.25 cop[tikv] table:thash, index:idx_b(b) range: decided by [eq(pruing_under_index_join.thash.b, pruing_under_index_join.touter.b)], keep order:false, stats:pseudo`))
	tk.MustQuery(`select /*+ INL_JOIN(touter, thash) */ thash.b from touter join thash use index(idx_b) on touter.b = thash.b`).Sort().Check(
		tk.MustQuery(`select /*+ INL_JOIN(touter, tnormal) */ tnormal.b from touter join tnormal use index(idx_b) on touter.b = tnormal.b`).Sort().Rows())

	// case 2: TableReader in the inner side
	tk.MustQuery(`explain format='brief' select /*+ INL_JOIN(touter, thash) */ thash.* from touter join thash use index(primary) on touter.b = thash.a`).Check(testkit.Rows(
		`IndexJoin 12487.50 root  inner join, inner:TableReader, outer key:pruing_under_index_join.touter.b, inner key:pruing_under_index_join.thash.a, equal cond:eq(pruing_under_index_join.touter.b, pruing_under_index_join.thash.a)`,
		`├─TableReader(Build) 9990.00 root  data:Selection`,
		`│ └─Selection 9990.00 cop[tikv]  not(isnull(pruing_under_index_join.touter.b))`,
		`│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`└─TableReader(Probe) 1.00 root partition:all data:TableRangeScan`,
		`  └─TableRangeScan 1.00 cop[tikv] table:thash range: decided by [pruing_under_index_join.touter.b], keep order:false, stats:pseudo`))
	tk.MustQuery(`select /*+ INL_JOIN(touter, thash) */ thash.* from touter join thash use index(primary) on touter.b = thash.a`).Sort().Check(
		tk.MustQuery(`select /*+ INL_JOIN(touter, tnormal) */ tnormal.* from touter join tnormal use index(primary) on touter.b = tnormal.a`).Sort().Rows())

	// case 3: IndexLookUp in the inner side + read all inner columns
	tk.MustQuery(`explain format='brief' select /*+ INL_JOIN(touter, thash) */ thash.* from touter join thash use index(idx_b) on touter.b = thash.b`).Check(testkit.Rows(
		`IndexJoin 12487.50 root  inner join, inner:IndexLookUp, outer key:pruing_under_index_join.touter.b, inner key:pruing_under_index_join.thash.b, equal cond:eq(pruing_under_index_join.touter.b, pruing_under_index_join.thash.b)`,
		`├─TableReader(Build) 9990.00 root  data:Selection`,
		`│ └─Selection 9990.00 cop[tikv]  not(isnull(pruing_under_index_join.touter.b))`,
		`│   └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`└─IndexLookUp(Probe) 1.25 root partition:all `,
		`  ├─Selection(Build) 1.25 cop[tikv]  not(isnull(pruing_under_index_join.thash.b))`,
		`  │ └─IndexRangeScan 1.25 cop[tikv] table:thash, index:idx_b(b) range: decided by [eq(pruing_under_index_join.thash.b, pruing_under_index_join.touter.b)], keep order:false, stats:pseudo`,
		`  └─TableRowIDScan(Probe) 1.25 cop[tikv] table:thash keep order:false, stats:pseudo`))
	tk.MustQuery(`select /*+ INL_JOIN(touter, thash) */ thash.* from touter join thash use index(idx_b) on touter.b = thash.b`).Sort().Check(
		tk.MustQuery(`select /*+ INL_JOIN(touter, tnormal) */ tnormal.* from touter join tnormal use index(idx_b) on touter.b = tnormal.b`).Sort().Rows())
}

func TestIssue25527(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_issue_25527")
	tk.MustExec("use test_issue_25527")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")

	// the original case
	tk.MustExec(`CREATE TABLE t (
		  col1 tinyint(4) primary key
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin PARTITION BY HASH( COL1 DIV 80 )
		PARTITIONS 6`)
	tk.MustExec(`insert into t values(-128), (107)`)
	tk.MustExec(`prepare stmt from 'select col1 from t where col1 in (?, ?, ?)'`)
	tk.MustExec(`set @a=-128, @b=107, @c=-128`)
	tk.MustQuery(`execute stmt using @a,@b,@c`).Sort().Check(testkit.Rows("-128", "107"))

	// the minimal reproducible case for hash partitioning
	tk.MustExec(`CREATE TABLE t0 (a int primary key) PARTITION BY HASH( a DIV 80 ) PARTITIONS 2`)
	tk.MustExec(`insert into t0 values (1)`)
	tk.MustQuery(`select a from t0 where a in (1)`).Check(testkit.Rows("1"))

	// the minimal reproducible case for range partitioning
	tk.MustExec(`create table t1 (a int primary key) partition by range (a+5) (
		partition p0 values less than(10), partition p1 values less than(20))`)
	tk.MustExec(`insert into t1 values (5)`)
	tk.MustQuery(`select a from t1 where a in (5)`).Check(testkit.Rows("5"))

	// the minimal reproducible case for list partitioning
	tk.MustExec(`create table  t2 (a int primary key) partition by list (a+5) (
		partition p0 values in (5, 6, 7, 8), partition p1 values in (9, 10, 11, 12))`)
	tk.MustExec(`insert into t2 values (5)`)
	tk.MustQuery(`select a from t2 where a in (5)`).Check(testkit.Rows("5"))
}

func TestIssue25598(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_issue_25598")
	tk.MustExec("use test_issue_25598")
	tk.MustExec(`CREATE TABLE UK_HP16726 (
	  COL1 bigint(16) DEFAULT NULL,
	  COL2 varchar(20) DEFAULT NULL,
	  COL4 datetime DEFAULT NULL,
	  COL3 bigint(20) DEFAULT NULL,
	  COL5 float DEFAULT NULL,
	  UNIQUE KEY UK_COL1 (COL1) /*!80000 INVISIBLE */
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
	PARTITION BY HASH( COL1 )
	PARTITIONS 25`)

	tk.MustQuery(`select t1. col1, t2. col1 from UK_HP16726 as t1 inner join UK_HP16726 as t2 on t1.col1 = t2.col1 where t1.col1 > -9223372036854775808 group by t1.col1, t2.col1 having t1.col1 != 9223372036854775807`).Check(testkit.Rows())
	tk.MustExec(`explain select t1. col1, t2. col1 from UK_HP16726 as t1 inner join UK_HP16726 as t2 on t1.col1 = t2.col1 where t1.col1 > -9223372036854775808 group by t1.col1, t2.col1 having t1.col1 != 9223372036854775807`)

	tk.MustExec(`set @@tidb_partition_prune_mode = 'dynamic'`)
	tk.MustQuery(`select t1. col1, t2. col1 from UK_HP16726 as t1 inner join UK_HP16726 as t2 on t1.col1 = t2.col1 where t1.col1 > -9223372036854775808 group by t1.col1, t2.col1 having t1.col1 != 9223372036854775807`).Check(testkit.Rows())
	tk.MustExec(`explain select t1. col1, t2. col1 from UK_HP16726 as t1 inner join UK_HP16726 as t2 on t1.col1 = t2.col1 where t1.col1 > -9223372036854775808 group by t1.col1, t2.col1 having t1.col1 != 9223372036854775807`)
}

func TestBatchGetforRangeandListPartitionTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_pointget")
	tk.MustExec("use test_pointget")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")

	// list partition table
	tk.MustExec(`create table tlist(a int, b int, unique index idx_a(a), index idx_b(b)) partition by list(a)(
		partition p0 values in (1, 2, 3, 4),
			partition p1 values in (5, 6, 7, 8),
			partition p2 values in (9, 10, 11, 12));`)

	// range partition table
	tk.MustExec(`create table trange(a int, unique key(a)) partition by range(a) (
		partition p0 values less than (30),
		partition p1 values less than (60),
		partition p2 values less than (90),
		partition p3 values less than (120));`)

	// hash partition table
	tk.MustExec("create table thash(a int unsigned, unique key(a)) partition by hash(a) partitions 4;")

	// insert data into list partition table
	tk.MustExec("insert into tlist values(1,1), (2,2), (3, 3), (4, 4), (5,5), (6, 6), (7,7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12);")
	// regular partition table
	tk.MustExec("create table tregular1(a int, unique key(a));")
	tk.MustExec("create table tregular2(a int, unique key(a));")

	vals := make([]string, 0, 100)
	// insert data into range partition table and hash partition table
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%v)", i+1))
	}
	tk.MustExec("insert into trange values " + strings.Join(vals, ","))
	tk.MustExec("insert into thash values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular1 values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular2 values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12)")

	// test BatchGet
	for i := 0; i < 100; i++ {
		// explain select a from t where a in ({x1}, {x2}, ... {x10}); // BatchGet is used
		// select a from t where where a in ({x1}, {x2}, ... {x10});
		points := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			x := rand.Intn(100) + 1
			points = append(points, fmt.Sprintf("%v", x))
		}
		queryRegular1 := fmt.Sprintf("select a from tregular1 where a in (%v)", strings.Join(points, ","))

		queryHash := fmt.Sprintf("select a from thash where a in (%v)", strings.Join(points, ","))
		require.True(t, tk.HasPlan(queryHash, "Batch_Point_Get")) // check if BatchGet is used
		tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryRange := fmt.Sprintf("select a from trange where a in (%v)", strings.Join(points, ","))
		require.True(t, tk.HasPlan(queryRange, "Batch_Point_Get")) // check if BatchGet is used
		tk.MustQuery(queryRange).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		points = make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			x := rand.Intn(12) + 1
			points = append(points, fmt.Sprintf("%v", x))
		}
		queryRegular2 := fmt.Sprintf("select a from tregular2 where a in (%v)", strings.Join(points, ","))
		queryList := fmt.Sprintf("select a from tlist where a in (%v)", strings.Join(points, ","))
		require.True(t, tk.HasPlan(queryList, "Batch_Point_Get")) // check if BatchGet is used
		tk.MustQuery(queryList).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())
	}

	// test different data type
	// unsigned flag
	// partition table and reguar table pair
	tk.MustExec(`create table trange3(a int unsigned, unique key(a)) partition by range(a) (
		partition p0 values less than (30),
		partition p1 values less than (60),
		partition p2 values less than (90),
		partition p3 values less than (120));`)
	tk.MustExec("create table tregular3(a int unsigned, unique key(a));")
	vals = make([]string, 0, 100)
	// insert data into range partition table and hash partition table
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%v)", i+1))
	}
	tk.MustExec("insert into trange3 values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular3 values " + strings.Join(vals, ","))
	// test BatchGet
	// explain select a from t where a in ({x1}, {x2}, ... {x10}); // BatchGet is used
	// select a from t where where a in ({x1}, {x2}, ... {x10});
	points := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		x := rand.Intn(100) + 1
		points = append(points, fmt.Sprintf("%v", x))
	}
	queryRegular := fmt.Sprintf("select a from tregular3 where a in (%v)", strings.Join(points, ","))
	queryRange := fmt.Sprintf("select a from trange3 where a in (%v)", strings.Join(points, ","))
	require.True(t, tk.HasPlan(queryRange, "Batch_Point_Get")) // check if BatchGet is used
	tk.MustQuery(queryRange).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
}

func TestGlobalStatsAndSQLBinding(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_global_stats")
	tk.MustExec("use test_global_stats")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// hash and range and list partition
	tk.MustExec("create table thash(a int, b int, key(a)) partition by hash(a) partitions 4")
	tk.MustExec(`create table trange(a int, b int, key(a)) partition by range(a) (
		partition p0 values less than (200),
		partition p1 values less than (400),
		partition p2 values less than (600),
		partition p3 values less than (800),
		partition p4 values less than (1001))`)
	tk.MustExec(`create table tlist (a int, b int, key(a)) partition by list (a) (
		partition p0 values in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
		partition p1 values in (10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
		partition p2 values in (20, 21, 22, 23, 24, 25, 26, 27, 28, 29),
		partition p3 values in (30, 31, 32, 33, 34, 35, 36, 37, 38, 39),
		partition p4 values in (40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50))`)

	// construct some special data distribution
	vals := make([]string, 0, 1000)
	listVals := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		if i < 10 {
			// for hash and range partition, 1% of records are in [0, 100)
			vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(100), rand.Intn(100)))
			// for list partition, 1% of records are equal to 0
			listVals = append(listVals, "(0, 0)")
		} else {
			vals = append(vals, fmt.Sprintf("(%v, %v)", 100+rand.Intn(900), 100+rand.Intn(900)))
			listVals = append(listVals, fmt.Sprintf("(%v, %v)", 1+rand.Intn(50), 1+rand.Intn(50)))
		}
	}
	tk.MustExec("insert into thash values " + strings.Join(vals, ","))
	tk.MustExec("insert into trange values " + strings.Join(vals, ","))
	tk.MustExec("insert into tlist values " + strings.Join(listVals, ","))

	// before analyzing, the planner will choose TableScan to access the 1% of records
	require.True(t, tk.HasPlan("select * from thash where a<100", "TableFullScan"))
	require.True(t, tk.HasPlan("select * from trange where a<100", "TableFullScan"))
	require.True(t, tk.HasPlan("select * from tlist where a<1", "TableFullScan"))

	tk.MustExec("analyze table thash")
	tk.MustExec("analyze table trange")
	tk.MustExec("analyze table tlist")

	// after analyzing, the planner will use the Index(a)
	tk.MustIndexLookup("select * from thash where a<100")
	tk.MustIndexLookup("select * from trange where a<100")
	tk.MustIndexLookup("select * from tlist where a<1")

	// create SQL bindings
	tk.MustExec("create session binding for select * from thash where a<100 using select * from thash ignore index(a) where a<100")
	tk.MustExec("create session binding for select * from trange where a<100 using select * from trange ignore index(a) where a<100")
	tk.MustExec("create session binding for select * from tlist where a<100 using select * from tlist ignore index(a) where a<100")

	// use TableScan again since the Index(a) is ignored
	require.True(t, tk.HasPlan("select * from thash where a<100", "TableFullScan"))
	require.True(t, tk.HasPlan("select * from trange where a<100", "TableFullScan"))
	require.True(t, tk.HasPlan("select * from tlist where a<1", "TableFullScan"))

	// drop SQL bindings
	tk.MustExec("drop session binding for select * from thash where a<100")
	tk.MustExec("drop session binding for select * from trange where a<100")
	tk.MustExec("drop session binding for select * from tlist where a<100")

	// use Index(a) again
	tk.MustIndexLookup("select * from thash where a<100")
	tk.MustIndexLookup("select * from trange where a<100")
	tk.MustIndexLookup("select * from tlist where a<1")
}

func TestPartitionTableWithDifferentJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_partition_joins")
	tk.MustExec("use test_partition_joins")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// hash and range partition
	tk.MustExec("create table thash(a int, b int, key(a)) partition by hash(a) partitions 4")
	tk.MustExec("create table tregular1(a int, b int, key(a))")

	tk.MustExec(`create table trange(a int, b int, key(a)) partition by range(a) (
		partition p0 values less than (200),
		partition p1 values less than (400),
		partition p2 values less than (600),
		partition p3 values less than (800),
		partition p4 values less than (1001))`)
	tk.MustExec("create table tregular2(a int, b int, key(a))")

	vals := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(1000), rand.Intn(1000)))
	}
	tk.MustExec("insert into thash values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular1 values " + strings.Join(vals, ","))

	vals = make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(1000), rand.Intn(1000)))
	}
	tk.MustExec("insert into trange values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular2 values " + strings.Join(vals, ","))

	// random params
	x1 := rand.Intn(1000)
	x2 := rand.Intn(1000)
	x3 := rand.Intn(1000)
	x4 := rand.Intn(1000)

	// group 1
	// hash_join range partition and hash partition
	queryHash := fmt.Sprintf("select /*+ hash_join(trange, thash) */ * from trange, thash where trange.b=thash.b and thash.a = %v and trange.a > %v;", x1, x2)
	queryRegular := fmt.Sprintf("select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.b=tregular1.b and tregular1.a = %v and tregular2.a > %v;", x1, x2)
	require.True(t, tk.HasPlan(queryHash, "HashJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ hash_join(trange, thash) */ * from trange, thash where trange.a=thash.a and thash.a > %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.a > %v;", x1)
	require.True(t, tk.HasPlan(queryHash, "HashJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ hash_join(trange, thash) */ * from trange, thash where trange.a=thash.a and trange.b = thash.b and thash.a > %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.b = tregular2.b and tregular1.a > %v;", x1)
	require.True(t, tk.HasPlan(queryHash, "HashJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ hash_join(trange, thash) */ * from trange, thash where trange.a=thash.a and thash.a = %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.a = %v;", x1)
	require.True(t, tk.HasPlan(queryHash, "HashJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// group 2
	// hash_join range partition and regular table
	queryHash = fmt.Sprintf("select /*+ hash_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and trange.a >= %v and tregular1.a > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular2.a >= %v and tregular1.a > %v;", x1, x2)
	require.True(t, tk.HasPlan(queryHash, "HashJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ hash_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and trange.a in (%v, %v, %v);", x1, x2, x3)
	queryRegular = fmt.Sprintf("select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular2.a in (%v, %v, %v);", x1, x2, x3)
	require.True(t, tk.HasPlan(queryHash, "HashJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ hash_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and tregular1.a >= %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular1.a >= %v;", x1)
	require.True(t, tk.HasPlan(queryHash, "HashJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// group 3
	// merge_join range partition and hash partition
	queryHash = fmt.Sprintf("select /*+ merge_join(trange, thash) */ * from trange, thash where trange.b=thash.b and thash.a = %v and trange.a > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.b=tregular1.b and tregular1.a = %v and tregular2.a > %v;", x1, x2)
	require.True(t, tk.HasPlan(queryHash, "MergeJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ merge_join(trange, thash) */ * from trange, thash where trange.a=thash.a and thash.a > %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.a > %v;", x1)
	require.True(t, tk.HasPlan(queryHash, "MergeJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ merge_join(trange, thash) */ * from trange, thash where trange.a=thash.a and trange.b = thash.b and thash.a > %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.b = tregular2.b and tregular1.a > %v;", x1)
	require.True(t, tk.HasPlan(queryHash, "MergeJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ merge_join(trange, thash) */ * from trange, thash where trange.a=thash.a and thash.a = %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.a = %v;", x1)
	require.True(t, tk.HasPlan(queryHash, "MergeJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// group 4
	// merge_join range partition and regular table
	queryHash = fmt.Sprintf("select /*+ merge_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and trange.a >= %v and tregular1.a > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular2.a >= %v and tregular1.a > %v;", x1, x2)
	require.True(t, tk.HasPlan(queryHash, "MergeJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ merge_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and trange.a in (%v, %v, %v);", x1, x2, x3)
	queryRegular = fmt.Sprintf("select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular2.a in (%v, %v, %v);", x1, x2, x3)
	require.True(t, tk.HasPlan(queryHash, "MergeJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ merge_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and tregular1.a >= %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular1.a >= %v;", x1)
	require.True(t, tk.HasPlan(queryHash, "MergeJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// new table instances
	tk.MustExec("create table thash2(a int, b int, index idx(a)) partition by hash(a) partitions 4")
	tk.MustExec("create table tregular3(a int, b int, index idx(a))")

	tk.MustExec(`create table trange2(a int, b int, index idx(a)) partition by range(a) (
		partition p0 values less than (200),
		partition p1 values less than (400),
		partition p2 values less than (600),
		partition p3 values less than (800),
		partition p4 values less than (1001))`)
	tk.MustExec("create table tregular4(a int, b int, index idx(a))")

	vals = make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(1000), rand.Intn(1000)))
	}
	tk.MustExec("insert into thash2 values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular3 values " + strings.Join(vals, ","))

	vals = make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(1000), rand.Intn(1000)))
	}
	tk.MustExec("insert into trange2 values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular4 values " + strings.Join(vals, ","))

	// group 5
	// index_merge_join range partition and range partition
	// Currently don't support index merge join on two partition tables. Only test warning.
	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, trange2) */ * from trange, trange2 where trange.a=trange2.a and trange.a > %v;", x1)
	// queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v;", x1)
	// require.True(t,tk.HasPlan(queryHash, "IndexMergeJoin"))
	// tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
	tk.MustQuery(queryHash)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1815|Optimizer Hint /*+ INL_MERGE_JOIN(trange, trange2) */ is inapplicable"))

	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, trange2) */ * from trange, trange2 where trange.a=trange2.a and trange.a > %v and trange2.a > %v;", x1, x2)
	// queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v and tregular4.a > %v;", x1, x2)
	// require.True(t,tk.HasPlan(queryHash, "IndexMergeJoin"))
	// tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
	tk.MustQuery(queryHash)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1815|Optimizer Hint /*+ INL_MERGE_JOIN(trange, trange2) */ is inapplicable"))

	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, trange2) */ * from trange, trange2 where trange.a=trange2.a and trange.a > %v and trange.b > %v;", x1, x2)
	// queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v and tregular2.b > %v;", x1, x2)
	// require.True(t,tk.HasPlan(queryHash, "IndexMergeJoin"))
	// tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
	tk.MustQuery(queryHash)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1815|Optimizer Hint /*+ INL_MERGE_JOIN(trange, trange2) */ is inapplicable"))

	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, trange2) */ * from trange, trange2 where trange.a=trange2.a and trange.a > %v and trange2.b > %v;", x1, x2)
	// queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v and tregular4.b > %v;", x1, x2)
	// require.True(t,tk.HasPlan(queryHash, "IndexMergeJoin"))
	// tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
	tk.MustQuery(queryHash)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1815|Optimizer Hint /*+ INL_MERGE_JOIN(trange, trange2) */ is inapplicable"))

	// group 6
	// index_merge_join range partition and regualr table
	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, tregular4) */ * from trange, tregular4 where trange.a=tregular4.a and trange.a > %v;", x1)
	queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v;", x1)
	require.True(t, tk.HasPlan(queryHash, "IndexMergeJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, tregular4) */ * from trange, tregular4 where trange.a=tregular4.a and trange.a > %v and tregular4.a > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v and tregular4.a > %v;", x1, x2)
	require.True(t, tk.HasPlan(queryHash, "IndexMergeJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, tregular4) */ * from trange, tregular4 where trange.a=tregular4.a and trange.a > %v and trange.b > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v and tregular2.b > %v;", x1, x2)
	require.True(t, tk.HasPlan(queryHash, "IndexMergeJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ inl_merge_join(trange, tregular4) */ * from trange, tregular4 where trange.a=tregular4.a and trange.a > %v and tregular4.b > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v and tregular4.b > %v;", x1, x2)
	require.True(t, tk.HasPlan(queryHash, "IndexMergeJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// group 7
	// index_hash_join hash partition and hash partition
	queryHash = fmt.Sprintf("select /*+ inl_hash_join(thash, thash2) */ * from thash, thash2 where thash.a = thash2.a and thash.a in (%v, %v);", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a in (%v, %v);", x1, x2)
	require.True(t, tk.HasPlan(queryHash, "IndexHashJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ inl_hash_join(thash, thash2) */ * from thash, thash2 where thash.a = thash2.a and thash.a in (%v, %v) and thash2.a in (%v, %v);", x1, x2, x3, x4)
	queryRegular = fmt.Sprintf("select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a in (%v, %v) and tregular3.a in (%v, %v);", x1, x2, x3, x4)
	require.True(t, tk.HasPlan(queryHash, "IndexHashJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ inl_hash_join(thash, thash2) */ * from thash, thash2 where thash.a = thash2.a and thash.a > %v and thash2.b > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a > %v and tregular3.b > %v;", x1, x2)
	require.True(t, tk.HasPlan(queryHash, "IndexHashJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	// group 8
	// index_hash_join hash partition and hash partition
	queryHash = fmt.Sprintf("select /*+ inl_hash_join(thash, tregular3) */ * from thash, tregular3 where thash.a = tregular3.a and thash.a in (%v, %v);", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a in (%v, %v);", x1, x2)
	require.True(t, tk.HasPlan(queryHash, "IndexHashJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ inl_hash_join(thash, tregular3) */ * from thash, tregular3 where thash.a = tregular3.a and thash.a in (%v, %v) and tregular3.a in (%v, %v);", x1, x2, x3, x4)
	queryRegular = fmt.Sprintf("select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a in (%v, %v) and tregular3.a in (%v, %v);", x1, x2, x3, x4)
	require.True(t, tk.HasPlan(queryHash, "IndexHashJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())

	queryHash = fmt.Sprintf("select /*+ inl_hash_join(thash, tregular3) */ * from thash, tregular3 where thash.a = tregular3.a and thash.a > %v and tregular3.b > %v;", x1, x2)
	queryRegular = fmt.Sprintf("select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a > %v and tregular3.b > %v;", x1, x2)
	require.True(t, tk.HasPlan(queryHash, "IndexHashJoin"))
	tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
}

func createTable4DynamicPruneModeTestWithExpression(tk *testkit.TestKit) {
	tk.MustExec("create table trange(a int, b int) partition by range(a) (partition p0 values less than(3), partition p1 values less than (5), partition p2 values less than(11));")
	tk.MustExec("create table thash(a int, b int) partition by hash(a) partitions 4;")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into trange values(1, NULL), (1, NULL), (1, 1), (2, 1), (3, 2), (4, 3), (5, 5), (6, 7), (7, 7), (7, 7), (10, NULL), (NULL, NULL), (NULL, 1);")
	tk.MustExec("insert into thash values(1, NULL), (1, NULL), (1, 1), (2, 1), (3, 2), (4, 3), (5, 5), (6, 7), (7, 7), (7, 7), (10, NULL), (NULL, NULL), (NULL, 1);")
	tk.MustExec("insert into t values(1, NULL), (1, NULL), (1, 1), (2, 1), (3, 2), (4, 3), (5, 5), (6, 7), (7, 7), (7, 7), (10, NULL), (NULL, NULL), (NULL, 1);")
	tk.MustExec("set session tidb_partition_prune_mode='dynamic'")
	tk.MustExec("analyze table trange")
	tk.MustExec("analyze table thash")
	tk.MustExec("analyze table t")
}

type testData4Expression struct {
	sql        string
	partitions []string
}

func TestDateColWithUnequalExpression(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists db_datetime_unequal_expression")
	tk.MustExec("create database db_datetime_unequal_expression")
	tk.MustExec("use db_datetime_unequal_expression")
	tk.MustExec("set tidb_partition_prune_mode='dynamic'")
	tk.MustExec(`create table tp(a datetime, b int) partition by range columns (a) (partition p0 values less than("2012-12-10 00:00:00"), partition p1 values less than("2022-12-30 00:00:00"), partition p2 values less than("2025-12-12 00:00:00"))`)
	tk.MustExec(`create table t(a datetime, b int) partition by range columns (a) (partition p0 values less than("2012-12-10 00:00:00"), partition p1 values less than("2022-12-30 00:00:00"), partition p2 values less than("2025-12-12 00:00:00"))`)
	tk.MustExec(`insert into tp values("2015-09-09 00:00:00", 1), ("2020-08-08 19:00:01", 2), ("2024-01-01 01:01:01", 3)`)
	tk.MustExec(`insert into t values("2015-09-09 00:00:00", 1), ("2020-08-08 19:00:01", 2), ("2024-01-01 01:01:01", 3)`)
	tk.MustExec("analyze table tp")
	tk.MustExec("analyze table t")

	tests := []testData4Expression{
		{
			sql:        "select * from %s where a != '2024-01-01 01:01:01'",
			partitions: []string{"all"},
		},
		{
			sql:        "select * from %s where a != '2024-01-01 01:01:01' and a > '2015-09-09 00:00:00'",
			partitions: []string{"p1,p2"},
		},
	}

	for _, t := range tests {
		tpSQL := fmt.Sprintf(t.sql, "tp")
		tSQL := fmt.Sprintf(t.sql, "t")
		tk.MustPartition(tpSQL, t.partitions[0]).Sort().Check(tk.MustQuery(tSQL).Sort().Rows())
	}
}

func TestToDaysColWithExpression(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists db_to_days_expression")
	tk.MustExec("create database db_to_days_expression")
	tk.MustExec("use db_to_days_expression")
	tk.MustExec("set tidb_partition_prune_mode='dynamic'")
	tk.MustExec("create table tp(a date, b int) partition by range(to_days(a)) (partition p0 values less than (737822), partition p1 values less than (738019), partition p2 values less than (738154))")
	tk.MustExec("create table t(a date, b int)")
	tk.MustExec("insert into tp values('2020-01-01', 1), ('2020-03-02', 2), ('2020-05-05', 3), ('2020-11-11', 4)")
	tk.MustExec("insert into t values('2020-01-01', 1), ('2020-03-02', 2), ('2020-05-05', 3), ('2020-11-11', 4)")
	tk.MustExec("analyze table tp")
	tk.MustExec("analyze table t")

	tests := []testData4Expression{
		{
			sql:        "select * from %s where a < '2020-08-16'",
			partitions: []string{"p0,p1"},
		},
		{
			sql:        "select * from %s where a between '2020-05-01' and '2020-10-01'",
			partitions: []string{"p1,p2"},
		},
	}

	for _, t := range tests {
		tpSQL := fmt.Sprintf(t.sql, "tp")
		tSQL := fmt.Sprintf(t.sql, "t")
		tk.MustPartition(tpSQL, t.partitions[0]).Sort().Check(tk.MustQuery(tSQL).Sort().Rows())
	}
}

func TestWeekdayWithExpression(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists db_weekday_expression")
	tk.MustExec("create database db_weekday_expression")
	tk.MustExec("use db_weekday_expression")
	tk.MustExec("set tidb_partition_prune_mode='dynamic'")
	tk.MustExec("create table tp(a datetime, b int) partition by range(weekday(a)) (partition p0 values less than(3), partition p1 values less than(5), partition p2 values less than(8))")
	tk.MustExec("create table t(a datetime, b int)")
	tk.MustExec(`insert into tp values("2020-08-17 00:00:00", 1), ("2020-08-18 00:00:00", 2), ("2020-08-19 00:00:00", 4), ("2020-08-20 00:00:00", 5), ("2020-08-21 00:00:00", 6), ("2020-08-22 00:00:00", 0)`)
	tk.MustExec(`insert into t values("2020-08-17 00:00:00", 1), ("2020-08-18 00:00:00", 2), ("2020-08-19 00:00:00", 4), ("2020-08-20 00:00:00", 5), ("2020-08-21 00:00:00", 6), ("2020-08-22 00:00:00", 0)`)
	tk.MustExec("analyze table tp")
	tk.MustExec("analyze table t")

	tests := []testData4Expression{
		{
			sql:        "select * from %s where a = '2020-08-17 00:00:00'",
			partitions: []string{"p0"},
		},
		{
			sql:        "select * from %s where a= '2020-08-20 00:00:00' and a < '2020-08-22 00:00:00'",
			partitions: []string{"p1"},
		},
		{
			sql:        " select * from %s where a < '2020-08-19 00:00:00'",
			partitions: []string{"all"},
		},
	}

	for _, t := range tests {
		tpSQL := fmt.Sprintf(t.sql, "tp")
		tSQL := fmt.Sprintf(t.sql, "t")
		tk.MustPartition(tpSQL, t.partitions[0]).Sort().Check(tk.MustQuery(tSQL).Sort().Rows())
	}
}

func TestFloorUnixTimestampAndIntColWithExpression(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists db_floor_unix_timestamp_int_expression")
	tk.MustExec("create database db_floor_unix_timestamp_int_expression")
	tk.MustExec("use db_floor_unix_timestamp_int_expression")
	tk.MustExec("set tidb_partition_prune_mode='dynamic'")
	tk.MustExec("create table tp(a timestamp, b int) partition by range(floor(unix_timestamp(a))) (partition p0 values less than(1580670000), partition p1 values less than(1597622400), partition p2 values less than(1629158400))")
	tk.MustExec("create table t(a timestamp, b int)")
	tk.MustExec("insert into tp values('2020-01-01 19:00:00', 1),('2020-08-15 00:00:00', -1), ('2020-08-18 05:00:01', 2), ('2020-10-01 14:13:15', 3)")
	tk.MustExec("insert into t values('2020-01-01 19:00:00', 1),('2020-08-15 00:00:00', -1), ('2020-08-18 05:00:01', 2), ('2020-10-01 14:13:15', 3)")
	tk.MustExec("analyze table tp")
	tk.MustExec("analyze table t")

	tests := []testData4Expression{
		{
			sql:        "select * from %s where a > '2020-09-11 00:00:00'",
			partitions: []string{"p2"},
		},
		{
			sql:        "select * from %s where a < '2020-07-07 01:00:00'",
			partitions: []string{"p0,p1"},
		},
	}

	for _, t := range tests {
		tpSQL := fmt.Sprintf(t.sql, "tp")
		tSQL := fmt.Sprintf(t.sql, "t")
		tk.MustPartition(tpSQL, t.partitions[0]).Sort().Check(tk.MustQuery(tSQL).Sort().Rows())
	}
}

func TestUnixTimestampAndIntColWithExpression(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists db_unix_timestamp_int_expression")
	tk.MustExec("create database db_unix_timestamp_int_expression")
	tk.MustExec("use db_unix_timestamp_int_expression")
	tk.MustExec("set tidb_partition_prune_mode='dynamic'")
	tk.MustExec("create table tp(a timestamp, b int) partition by range(unix_timestamp(a)) (partition p0 values less than(1580670000), partition p1 values less than(1597622400), partition p2 values less than(1629158400))")
	tk.MustExec("create table t(a timestamp, b int)")
	tk.MustExec("insert into tp values('2020-01-01 19:00:00', 1),('2020-08-15 00:00:00', -1), ('2020-08-18 05:00:01', 2), ('2020-10-01 14:13:15', 3)")
	tk.MustExec("insert into t values('2020-01-01 19:00:00', 1),('2020-08-15 00:00:00', -1), ('2020-08-18 05:00:01', 2), ('2020-10-01 14:13:15', 3)")
	tk.MustExec("analyze table tp")
	tk.MustExec("analyze table t")

	tests := []testData4Expression{
		{
			sql:        "select * from %s where a > '2020-09-11 00:00:00'",
			partitions: []string{"p2"},
		},
		{
			sql:        "select * from %s where a < '2020-07-07 01:00:00'",
			partitions: []string{"p0,p1"},
		},
	}

	for _, t := range tests {
		tpSQL := fmt.Sprintf(t.sql, "tp")
		tSQL := fmt.Sprintf(t.sql, "t")
		tk.MustPartition(tpSQL, t.partitions[0]).Sort().Check(tk.MustQuery(tSQL).Sort().Rows())
	}
}

func TestDatetimeColAndIntColWithExpression(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists db_datetime_int_expression")
	tk.MustExec("create database db_datetime_int_expression")
	tk.MustExec("use db_datetime_int_expression")
	tk.MustExec("set tidb_partition_prune_mode='dynamic'")
	tk.MustExec("create table tp(a datetime, b int) partition by range columns(a) (partition p0 values less than('2020-02-02 00:00:00'), partition p1 values less than('2020-09-01 00:00:00'), partition p2 values less than('2020-12-20 00:00:00'))")
	tk.MustExec("create table t(a datetime, b int)")
	tk.MustExec("insert into tp values('2020-01-01 12:00:00', 1), ('2020-08-22 10:00:00', 2), ('2020-09-09 11:00:00', 3), ('2020-10-01 00:00:00', 4)")
	tk.MustExec("insert into t values('2020-01-01 12:00:00', 1), ('2020-08-22 10:00:00', 2), ('2020-09-09 11:00:00', 3), ('2020-10-01 00:00:00', 4)")
	tk.MustExec("analyze table tp")
	tk.MustExec("analyze table t")

	tests := []testData4Expression{
		{
			sql:        "select * from %s where a < '2020-09-01 00:00:00'",
			partitions: []string{"p0,p1"},
		},
		{
			sql:        "select * from %s where a > '2020-07-07 01:00:00'",
			partitions: []string{"p1,p2"},
		},
	}

	for _, t := range tests {
		tpSQL := fmt.Sprintf(t.sql, "tp")
		tSQL := fmt.Sprintf(t.sql, "t")
		tk.MustPartition(tpSQL, t.partitions[0]).Sort().Check(tk.MustQuery(tSQL).Sort().Rows())
	}
}

func TestVarcharColAndIntColWithExpression(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists db_varchar_int_expression")
	tk.MustExec("create database db_varchar_int_expression")
	tk.MustExec("use db_varchar_int_expression")
	tk.MustExec("set tidb_partition_prune_mode='dynamic'")
	tk.MustExec("create table tp(a varchar(255), b int) partition by range columns(a) (partition p0 values less than('ddd'), partition p1 values less than('ggggg'), partition p2 values less than('mmmmmm'))")
	tk.MustExec("create table t(a varchar(255), b int)")
	tk.MustExec("insert into tp values('aaa', 1), ('bbbb', 2), ('ccc', 3), ('dfg', 4), ('kkkk', 5), ('10', 6)")
	tk.MustExec("insert into t values('aaa', 1), ('bbbb', 2), ('ccc', 3), ('dfg', 4), ('kkkk', 5), ('10', 6)")
	tk.MustExec("analyze table tp")
	tk.MustExec("analyze table t")

	tests := []testData4Expression{
		{
			sql:        "select * from %s where a < '10'",
			partitions: []string{"p0"},
		},
		{
			sql:        "select * from %s where a > 0",
			partitions: []string{"all"},
		},
		{
			sql:        "select * from %s where a < 0",
			partitions: []string{"all"},
		},
	}

	for _, t := range tests {
		tpSQL := fmt.Sprintf(t.sql, "tp")
		tSQL := fmt.Sprintf(t.sql, "t")
		tk.MustPartition(tpSQL, t.partitions[0]).Sort().Check(tk.MustQuery(tSQL).Sort().Rows())
	}
}

func TestDynamicPruneModeWithExpression(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists db_equal_expression")
	tk.MustExec("create database db_equal_expression")
	tk.MustExec("use db_equal_expression")
	createTable4DynamicPruneModeTestWithExpression(tk)

	tables := []string{"trange", "thash"}
	tests := []testData4Expression{
		{
			sql: "select * from %s where a = 2",
			partitions: []string{
				"p0",
				"p2",
			},
		},
		{
			sql: "select * from %s where a = 4 or a = 1",
			partitions: []string{
				"p0,p1",
				"p0,p1",
			},
		},
		{
			sql: "select * from %s where a = -1",
			partitions: []string{
				"p0",
				"p1",
			},
		},
		{
			sql: "select * from %s where a is NULL",
			partitions: []string{
				"p0",
				"p0",
			},
		},
		{
			sql: "select * from %s where b is NULL",
			partitions: []string{
				"all",
				"all",
			},
		},
		{
			sql: "select * from %s where a > -1",
			partitions: []string{
				"all",
				"all",
			},
		},
		{
			sql: "select * from %s where a >= 4 and a <= 5",
			partitions: []string{
				"p1,p2",
				"p0,p1",
			},
		},
		{
			sql: "select * from %s where a > 10",
			partitions: []string{
				"dual",
				"all",
			},
		},
		{
			sql: "select * from %s where a >=2 and a <= 3",
			partitions: []string{
				"p0,p1",
				"p2,p3",
			},
		},
		{
			sql: "select * from %s where a between 2 and 3",
			partitions: []string{
				"p0,p1",
				"p2,p3",
			},
		},
		{
			sql: "select * from %s where a < 2",
			partitions: []string{
				"p0",
				"all",
			},
		},
		{
			sql: "select * from %s where a <= 3",
			partitions: []string{
				"p0,p1",
				"all",
			},
		},
		{
			sql: "select * from %s where a in (2, 3)",
			partitions: []string{
				"p0,p1",
				"p2,p3",
			},
		},
		{
			sql: "select * from %s where a in (1, 5)",
			partitions: []string{
				"p0,p2",
				"p1",
			},
		},
		{
			sql: "select * from %s where a not in (1, 5)",
			partitions: []string{
				"all",
				"all",
			},
		},
		{
			sql: "select * from %s where a = 2 and a = 2",
			partitions: []string{
				"p0",
				"p2",
			},
		},
		{
			sql: "select * from %s where a = 2 and a = 3",
			partitions: []string{
				// This means that we have no partition-read plan
				"",
				"",
			},
		},
		{
			sql: "select * from %s where a < 2 and a > 0",
			partitions: []string{
				"p0",
				"p1",
			},
		},
		{
			sql: "select * from %s where a < 2 and a < 3",
			partitions: []string{
				"p0",
				"all",
			},
		},
		{
			sql: "select * from %s where a > 1 and a > 2",
			partitions: []string{
				"p1,p2",
				"all",
			},
		},
		{
			sql: "select * from %s where a = 2 or a = 3",
			partitions: []string{
				"p0,p1",
				"p2,p3",
			},
		},
		{
			sql: "select * from %s where a = 2 or a in (3)",
			partitions: []string{
				"p0,p1",
				"p2,p3",
			},
		},
		{
			sql: "select * from %s where a = 2 or a > 3",
			partitions: []string{
				"all",
				"all",
			},
		},
		{
			sql: "select * from %s where a = 2 or a <= 1",
			partitions: []string{
				"p0",
				"all",
			},
		},
		{
			sql: "select * from %s where a = 2 or a between 2 and 2",
			partitions: []string{
				"p0",
				"p2",
			},
		},
		{
			sql: "select * from %s where a != 2",
			partitions: []string{
				"all",
				"all",
			},
		},
		{
			sql: "select * from %s where a != 2 and a > 4",
			partitions: []string{
				"p2",
				"all",
			},
		},
		{
			sql: "select * from %s where a != 2 and a != 3",
			partitions: []string{
				"all",
				"all",
			},
		},
		{
			sql: "select * from %s where a != 2 and a = 3",
			partitions: []string{
				"p1",
				"p3",
			},
		},
		{
			sql: "select * from %s where not (a = 2)",
			partitions: []string{
				"all",
				"all",
			},
		},
		{
			sql: "select * from %s where not (a > 2)",
			partitions: []string{
				"p0",
				"all",
			},
		},
		{
			sql: "select * from %s where not (a < 2)",
			partitions: []string{
				"all",
				"all",
			},
		},
		// cases that partition pruning can not work
		{
			sql: "select * from %s where a + 1 > 4",
			partitions: []string{
				"all",
				"all",
			},
		},
		{
			sql: "select * from %s where a - 1 > 0",
			partitions: []string{
				"all",
				"all",
			},
		},
		{
			sql: "select * from %s where a * 2 < 0",
			partitions: []string{
				"all",
				"all",
			},
		},
		{
			sql: "select * from %s where a << 1 < 0",
			partitions: []string{
				"all",
				"all",
			},
		},
		// comparison between int column and string column
		{
			sql: "select * from %s where a > '10'",
			partitions: []string{
				"dual",
				"all",
			},
		},
		{
			sql: "select * from %s where a > '10ab'",
			partitions: []string{
				"dual",
				"all",
			},
		},
	}

	for _, t := range tests {
		for i := range t.partitions {
			sql := fmt.Sprintf(t.sql, tables[i])
			tk.MustPartition(sql, t.partitions[i]).Sort().Check(tk.MustQuery(fmt.Sprintf(t.sql, "t")).Sort().Rows())
		}
	}
}

func TestAddDropPartitions(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_add_drop_partition")
	tk.MustExec("use test_add_drop_partition")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table t(a int) partition by range(a) (
		  partition p0 values less than (5),
		  partition p1 values less than (10),
		  partition p2 values less than (15))`)
	tk.MustExec(`insert into t values (2), (7), (12)`)
	tk.MustPartition(`select * from t where a < 3`, "p0").Sort().Check(testkit.Rows("2"))
	tk.MustPartition(`select * from t where a < 8`, "p0,p1").Sort().Check(testkit.Rows("2", "7"))
	tk.MustPartition(`select * from t where a < 20`, "all").Sort().Check(testkit.Rows("12", "2", "7"))

	// remove p0
	tk.MustExec(`alter table t drop partition p0`)
	tk.MustPartition(`select * from t where a < 3`, "p1").Sort().Check(testkit.Rows())
	tk.MustPartition(`select * from t where a < 8`, "p1").Sort().Check(testkit.Rows("7"))
	tk.MustPartition(`select * from t where a < 20`, "all").Sort().Check(testkit.Rows("12", "7"))

	// add 2 more partitions
	tk.MustExec(`alter table t add partition (partition p3 values less than (20))`)
	tk.MustExec(`alter table t add partition (partition p4 values less than (40))`)
	tk.MustExec(`insert into t values (15), (25)`)
	tk.MustPartition(`select * from t where a < 3`, "p1").Sort().Check(testkit.Rows())
	tk.MustPartition(`select * from t where a < 8`, "p1").Sort().Check(testkit.Rows("7"))
	tk.MustPartition(`select * from t where a < 20`, "p1,p2,p3").Sort().Check(testkit.Rows("12", "15", "7"))
}

func TestMPPQueryExplainInfo(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database tiflash_partition_test")
	tk.MustExec("use tiflash_partition_test")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table t(a int) partition by range(a) (
		  partition p0 values less than (5),
		  partition p1 values less than (10),
		  partition p2 values less than (15))`)
	tb := external.GetTableByName(t, tk, "tiflash_partition_test", "t")
	for _, partition := range tb.Meta().GetPartitionInfo().Definitions {
		err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), partition.ID, true)
		require.NoError(t, err)
	}
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec(`insert into t values (2), (7), (12)`)
	tk.MustExec("set tidb_enforce_mpp=1")
	tk.MustPartition(`select * from t where a < 3`, "p0").Sort().Check(testkit.Rows("2"))
	tk.MustPartition(`select * from t where a < 8`, "p0,p1").Sort().Check(testkit.Rows("2", "7"))
	tk.MustPartition(`select * from t where a < 20`, "all").Sort().Check(testkit.Rows("12", "2", "7"))
	tk.MustPartition(`select * from t where a < 5 union all select * from t where a > 10`, "p0").Sort().Check(testkit.Rows("12", "2"))
	tk.MustPartition(`select * from t where a < 5 union all select * from t where a > 10`, "p2").Sort().Check(testkit.Rows("12", "2"))
}

func TestPartitionPruningInTransaction(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_pruning_transaction")
	defer tk.MustExec(`drop database test_pruning_transaction`)
	tk.MustExec("use test_pruning_transaction")
	tk.MustExec(`create table t(a int, b int) partition by range(a) (partition p0 values less than(3), partition p1 values less than (5), partition p2 values less than(11))`)
	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
	tk.MustExec(`begin`)
	tk.MustPartitionByList(`select * from t`, []string{"p0", "p1", "p2"})
	tk.MustPartitionByList(`select * from t where a > 3`, []string{"p1", "p2"}) // partition pruning can work in transactions
	tk.MustPartitionByList(`select * from t where a > 7`, []string{"p2"})
	tk.MustExec(`rollback`)
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec(`begin`)
	tk.MustPartition(`select * from t`, "all")
	tk.MustPartition(`select * from t where a > 3`, "p1,p2") // partition pruning can work in transactions
	tk.MustPartition(`select * from t where a > 7`, "p2")
	tk.MustExec(`rollback`)
	tk.MustExec("set @@tidb_partition_prune_mode = default")
}

func TestIssue25253(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database issue25253")
	defer tk.MustExec("drop database issue25253")
	tk.MustExec("use issue25253")

	tk.MustExec(`CREATE TABLE IDT_HP23902 (
	  COL1 smallint DEFAULT NULL,
	  COL2 varchar(20) DEFAULT NULL,
	  COL4 datetime DEFAULT NULL,
	  COL3 bigint DEFAULT NULL,
	  COL5 float DEFAULT NULL,
	  KEY UK_COL1 (COL1)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
	PARTITION BY HASH( COL1+30 )
	PARTITIONS 6`)
	tk.MustExec(`insert ignore into IDT_HP23902 partition(p0, p1)(col1, col3) values(-10355, 1930590137900568573), (13810, -1332233145730692137)`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1748 Found a row not matching the given partition set",
		"Warning 1748 Found a row not matching the given partition set"))
	tk.MustQuery(`select * from IDT_HP23902`).Check(testkit.Rows())

	tk.MustExec(`create table t (
	  a int
	) partition by range(a) (
	  partition p0 values less than (10),
	  partition p1 values less than (20))`)
	tk.MustExec(`insert ignore into t partition(p0)(a) values(12)`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1748 Found a row not matching the given partition set"))
	tk.MustQuery(`select * from t`).Check(testkit.Rows())
}

func TestDML(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_DML")
	defer tk.MustExec(`drop database test_DML`)
	tk.MustExec("use test_DML")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table tinner (a int, b int)`)
	tk.MustExec(`create table thash (a int, b int) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a int, b int) partition by range(a) (
		partition p0 values less than(10000),
		partition p1 values less than(20000),
		partition p2 values less than(30000),
		partition p3 values less than(40000),
		partition p4 values less than MAXVALUE)`)

	vals := make([]string, 0, 50)
	for i := 0; i < 50; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(40000), rand.Intn(40000)))
	}
	tk.MustExec(`insert into tinner values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into thash values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into trange values ` + strings.Join(vals, ", "))

	// delete, insert, replace, update
	for i := 0; i < 200; i++ {
		var pattern string
		switch rand.Intn(4) {
		case 0: // delete
			col := []string{"a", "b"}[rand.Intn(2)]
			l := rand.Intn(40000)
			r := l + rand.Intn(5000)
			pattern = fmt.Sprintf(`delete from %%v where %v>%v and %v<%v`, col, l, col, r)
		case 1: // insert
			a, b := rand.Intn(40000), rand.Intn(40000)
			pattern = fmt.Sprintf(`insert into %%v values (%v, %v)`, a, b)
		case 2: // replace
			a, b := rand.Intn(40000), rand.Intn(40000)
			pattern = fmt.Sprintf(`replace into %%v(a, b) values (%v, %v)`, a, b)
		case 3: // update
			col := []string{"a", "b"}[rand.Intn(2)]
			l := rand.Intn(40000)
			r := l + rand.Intn(5000)
			x := rand.Intn(1000) - 500
			pattern = fmt.Sprintf(`update %%v set %v=%v+%v where %v>%v and %v<%v`, col, col, x, col, l, col, r)
		}
		for _, tbl := range []string{"tinner", "thash", "trange"} {
			tk.MustExec(fmt.Sprintf(pattern, tbl))
		}

		// check
		r := tk.MustQuery(`select * from tinner`).Sort().Rows()
		tk.MustQuery(`select * from thash`).Sort().Check(r)
		tk.MustQuery(`select * from trange`).Sort().Check(r)
	}
}

func TestUnion(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_union")
	defer tk.MustExec(`drop database test_union`)
	tk.MustExec("use test_union")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table t(a int, b int, key(a))`)
	tk.MustExec(`create table thash (a int, b int, key(a)) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a int, b int, key(a)) partition by range(a) (
		partition p0 values less than (10000),
		partition p1 values less than (20000),
		partition p2 values less than (30000),
		partition p3 values less than (40000))`)

	vals := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(40000), rand.Intn(40000)))
	}
	tk.MustExec(`insert into t values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into thash values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into trange values ` + strings.Join(vals, ", "))

	randRange := func() (int, int) {
		l, r := rand.Intn(40000), rand.Intn(40000)
		if l > r {
			l, r = r, l
		}
		return l, r
	}

	for i := 0; i < 100; i++ {
		a1l, a1r := randRange()
		a2l, a2r := randRange()
		b1l, b1r := randRange()
		b2l, b2r := randRange()
		for _, utype := range []string{"union all", "union distinct"} {
			pattern := fmt.Sprintf(`select * from %%v where a>=%v and a<=%v and b>=%v and b<=%v
			%v select * from %%v where a>=%v and a<=%v and b>=%v and b<=%v`, a1l, a1r, b1l, b1r, utype, a2l, a2r, b2l, b2r)
			r := tk.MustQuery(fmt.Sprintf(pattern, "t", "t")).Sort().Rows()
			tk.MustQuery(fmt.Sprintf(pattern, "thash", "thash")).Sort().Check(r)   // hash + hash
			tk.MustQuery(fmt.Sprintf(pattern, "trange", "trange")).Sort().Check(r) // range + range
			tk.MustQuery(fmt.Sprintf(pattern, "trange", "thash")).Sort().Check(r)  // range + hash
		}
	}
}

func TestSubqueries(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_subquery")
	defer tk.MustExec(`drop database test_subquery`)
	tk.MustExec("use test_subquery")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table touter (a int, b int, index(a))`)
	tk.MustExec(`create table tinner (a int, b int, c int, index(a))`)
	tk.MustExec(`create table thash (a int, b int, c int, index(a)) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a int, b int, c int, index(a)) partition by range(a) (
		partition p0 values less than(10000),
		partition p1 values less than(20000),
		partition p2 values less than(30000),
		partition p3 values less than(40000))`)

	outerVals := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		outerVals = append(outerVals, fmt.Sprintf(`(%v, %v)`, rand.Intn(40000), rand.Intn(40000)))
	}
	tk.MustExec(`insert into touter values ` + strings.Join(outerVals, ", "))
	vals := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf(`(%v, %v, %v)`, rand.Intn(40000), rand.Intn(40000), rand.Intn(40000)))
	}
	tk.MustExec(`insert into tinner values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into thash values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into trange values ` + strings.Join(vals, ", "))

	// in
	for i := 0; i < 50; i++ {
		for _, op := range []string{"in", "not in"} {
			x := rand.Intn(40000)
			var r [][]interface{}
			for _, t := range []string{"tinner", "thash", "trange"} {
				q := fmt.Sprintf(`select * from touter where touter.a %v (select %v.b from %v where %v.a > touter.b and %v.c > %v)`, op, t, t, t, t, x)
				if r == nil {
					r = tk.MustQuery(q).Sort().Rows()
				} else {
					tk.MustQuery(q).Sort().Check(r)
				}
			}
		}
	}

	// exist
	for i := 0; i < 50; i++ {
		for _, op := range []string{"exists", "not exists"} {
			x := rand.Intn(40000)
			var r [][]interface{}
			for _, t := range []string{"tinner", "thash", "trange"} {
				q := fmt.Sprintf(`select * from touter where %v (select %v.b from %v where %v.a > touter.b and %v.c > %v)`, op, t, t, t, t, x)
				if r == nil {
					r = tk.MustQuery(q).Sort().Rows()
				} else {
					tk.MustQuery(q).Sort().Check(r)
				}
			}
		}
	}
}

func TestSplitRegion(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_split_region")
	tk.MustExec("use test_split_region")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table tnormal (a int, b int)`)
	tk.MustExec(`create table thash (a int, b int, index(a)) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a int, b int, index(a)) partition by range(a) (
		partition p0 values less than (10000),
		partition p1 values less than (20000),
		partition p2 values less than (30000),
		partition p3 values less than (40000))`)
	vals := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(40000), rand.Intn(40000)))
	}
	tk.MustExec(`insert into tnormal values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into thash values ` + strings.Join(vals, ", "))
	tk.MustExec(`insert into trange values ` + strings.Join(vals, ", "))

	tk.MustExec(`SPLIT TABLE thash INDEX a BETWEEN (1) AND (25000) REGIONS 10`)
	tk.MustExec(`SPLIT TABLE trange INDEX a BETWEEN (1) AND (25000) REGIONS 10`)

	result := tk.MustQuery(`select * from tnormal where a>=1 and a<=15000`).Sort().Rows()
	tk.MustPartition(`select * from trange where a>=1 and a<=15000`, "p0,p1").Sort().Check(result)
	tk.MustPartition(`select * from thash where a>=1 and a<=15000`, "all").Sort().Check(result)

	result = tk.MustQuery(`select * from tnormal where a in (1, 10001, 20001)`).Sort().Rows()
	tk.MustPartition(`select * from trange where a in (1, 10001, 20001)`, "p0,p1,p2").Sort().Check(result)
	tk.MustPartition(`select * from thash where a in (1, 10001, 20001)`, "p1").Sort().Check(result)
}

func TestParallelApply(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_parallel_apply")
	tk.MustExec("use test_parallel_apply")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("set tidb_enable_parallel_apply=true")

	tk.MustExec(`create table touter (a int, b int)`)
	tk.MustExec(`create table tinner (a int, b int, key(a))`)
	tk.MustExec(`create table thash (a int, b int, key(a)) partition by hash(a) partitions 4`)
	tk.MustExec(`create table trange (a int, b int, key(a)) partition by range(a) (
			  partition p0 values less than(10000),
			  partition p1 values less than(20000),
			  partition p2 values less than(30000),
			  partition p3 values less than(40000))`)

	vouter := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		vouter = append(vouter, fmt.Sprintf("(%v, %v)", rand.Intn(40000), rand.Intn(40000)))
	}
	tk.MustExec("insert into touter values " + strings.Join(vouter, ", "))

	vals := make([]string, 0, 2000)
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(40000), rand.Intn(40000)))
	}
	tk.MustExec("insert into tinner values " + strings.Join(vals, ", "))
	tk.MustExec("insert into thash values " + strings.Join(vals, ", "))
	tk.MustExec("insert into trange values " + strings.Join(vals, ", "))

	// parallel apply + hash partition + IndexReader as its inner child
	tk.MustQuery(`explain format='brief' select * from touter where touter.a > (select sum(thash.a) from thash use index(a) where thash.a>touter.b)`).Check(testkit.Rows(
		`Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b`,
		`└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(20,0) BINARY), Column#7)`,
		`  ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
		`  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`  └─StreamAgg(Probe) 1.00 root  funcs:sum(Column#9)->Column#7`,
		`    └─IndexReader 1.00 root partition:all index:StreamAgg`, // IndexReader is a inner child of Apply
		`      └─StreamAgg 1.00 cop[tikv]  funcs:sum(test_parallel_apply.thash.a)->Column#9`,
		`        └─Selection 8000.00 cop[tikv]  gt(test_parallel_apply.thash.a, test_parallel_apply.touter.b)`,
		`          └─IndexFullScan 10000.00 cop[tikv] table:thash, index:a(a) keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from touter where touter.a > (select sum(thash.a) from thash use index(a) where thash.a>touter.b)`).Sort().Check(
		tk.MustQuery(`select * from touter where touter.a > (select sum(tinner.a) from tinner use index(a) where tinner.a>touter.b)`).Sort().Rows())

	// parallel apply + hash partition + TableReader as its inner child
	tk.MustQuery(`explain format='brief' select * from touter where touter.a > (select sum(thash.b) from thash ignore index(a) where thash.a>touter.b)`).Check(testkit.Rows(
		`Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b`,
		`└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(20,0) BINARY), Column#7)`,
		`  ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
		`  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`  └─StreamAgg(Probe) 1.00 root  funcs:sum(Column#9)->Column#7`,
		`    └─TableReader 1.00 root partition:all data:StreamAgg`, // TableReader is a inner child of Apply
		`      └─StreamAgg 1.00 cop[tikv]  funcs:sum(test_parallel_apply.thash.b)->Column#9`,
		`        └─Selection 8000.00 cop[tikv]  gt(test_parallel_apply.thash.a, test_parallel_apply.touter.b)`,
		`          └─TableFullScan 10000.00 cop[tikv] table:thash keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from touter where touter.a > (select sum(thash.b) from thash ignore index(a) where thash.a>touter.b)`).Sort().Check(
		tk.MustQuery(`select * from touter where touter.a > (select sum(tinner.b) from tinner ignore index(a) where tinner.a>touter.b)`).Sort().Rows())

	// parallel apply + hash partition + IndexLookUp as its inner child
	tk.MustQuery(`explain format='brief' select * from touter where touter.a > (select sum(tinner.b) from tinner use index(a) where tinner.a>touter.b)`).Check(testkit.Rows(
		`Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b`,
		`└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(20,0) BINARY), Column#7)`,
		`  ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
		`  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`  └─HashAgg(Probe) 1.00 root  funcs:sum(Column#9)->Column#7`,
		`    └─IndexLookUp 1.00 root  `, // IndexLookUp is a inner child of Apply
		`      ├─Selection(Build) 8000.00 cop[tikv]  gt(test_parallel_apply.tinner.a, test_parallel_apply.touter.b)`,
		`      │ └─IndexFullScan 10000.00 cop[tikv] table:tinner, index:a(a) keep order:false, stats:pseudo`,
		`      └─HashAgg(Probe) 1.00 cop[tikv]  funcs:sum(test_parallel_apply.tinner.b)->Column#9`,
		`        └─TableRowIDScan 8000.00 cop[tikv] table:tinner keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from touter where touter.a > (select sum(thash.b) from thash use index(a) where thash.a>touter.b)`).Sort().Check(
		tk.MustQuery(`select * from touter where touter.a > (select sum(tinner.b) from tinner use index(a) where tinner.a>touter.b)`).Sort().Rows())

	// parallel apply + range partition + IndexReader as its inner child
	tk.MustQuery(`explain format='brief' select * from touter where touter.a > (select sum(trange.a) from trange use index(a) where trange.a>touter.b)`).Check(testkit.Rows(
		`Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b`,
		`└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(20,0) BINARY), Column#7)`,
		`  ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
		`  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`  └─StreamAgg(Probe) 1.00 root  funcs:sum(Column#9)->Column#7`,
		`    └─IndexReader 1.00 root partition:all index:StreamAgg`, // IndexReader is a inner child of Apply
		`      └─StreamAgg 1.00 cop[tikv]  funcs:sum(test_parallel_apply.trange.a)->Column#9`,
		`        └─Selection 8000.00 cop[tikv]  gt(test_parallel_apply.trange.a, test_parallel_apply.touter.b)`,
		`          └─IndexFullScan 10000.00 cop[tikv] table:trange, index:a(a) keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from touter where touter.a > (select sum(trange.a) from trange use index(a) where trange.a>touter.b)`).Sort().Check(
		tk.MustQuery(`select * from touter where touter.a > (select sum(tinner.a) from tinner use index(a) where tinner.a>touter.b)`).Sort().Rows())

	// parallel apply + range partition + TableReader as its inner child
	tk.MustQuery(`explain format='brief' select * from touter where touter.a > (select sum(trange.b) from trange ignore index(a) where trange.a>touter.b)`).Check(testkit.Rows(
		`Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b`,
		`└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(20,0) BINARY), Column#7)`,
		`  ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
		`  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`  └─StreamAgg(Probe) 1.00 root  funcs:sum(Column#9)->Column#7`,
		`    └─TableReader 1.00 root partition:all data:StreamAgg`, // TableReader is a inner child of Apply
		`      └─StreamAgg 1.00 cop[tikv]  funcs:sum(test_parallel_apply.trange.b)->Column#9`,
		`        └─Selection 8000.00 cop[tikv]  gt(test_parallel_apply.trange.a, test_parallel_apply.touter.b)`,
		`          └─TableFullScan 10000.00 cop[tikv] table:trange keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from touter where touter.a > (select sum(trange.b) from trange ignore index(a) where trange.a>touter.b)`).Sort().Check(
		tk.MustQuery(`select * from touter where touter.a > (select sum(tinner.b) from tinner ignore index(a) where tinner.a>touter.b)`).Sort().Rows())

	// parallel apply + range partition + IndexLookUp as its inner child
	tk.MustQuery(`explain format='brief' select * from touter where touter.a > (select sum(tinner.b) from tinner use index(a) where tinner.a>touter.b)`).Check(testkit.Rows(
		`Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b`,
		`└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(20,0) BINARY), Column#7)`,
		`  ├─TableReader(Build) 10000.00 root  data:TableFullScan`,
		`  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo`,
		`  └─HashAgg(Probe) 1.00 root  funcs:sum(Column#9)->Column#7`,
		`    └─IndexLookUp 1.00 root  `, // IndexLookUp is a inner child of Apply
		`      ├─Selection(Build) 8000.00 cop[tikv]  gt(test_parallel_apply.tinner.a, test_parallel_apply.touter.b)`,
		`      │ └─IndexFullScan 10000.00 cop[tikv] table:tinner, index:a(a) keep order:false, stats:pseudo`,
		`      └─HashAgg(Probe) 1.00 cop[tikv]  funcs:sum(test_parallel_apply.tinner.b)->Column#9`,
		`        └─TableRowIDScan 8000.00 cop[tikv] table:tinner keep order:false, stats:pseudo`))
	tk.MustQuery(`select * from touter where touter.a > (select sum(trange.b) from trange use index(a) where trange.a>touter.b)`).Sort().Check(
		tk.MustQuery(`select * from touter where touter.a > (select sum(tinner.b) from tinner use index(a) where tinner.a>touter.b)`).Sort().Rows())

	// random queries
	ops := []string{"!=", ">", "<", ">=", "<="}
	aggFuncs := []string{"sum", "count", "max", "min"}
	tbls := []string{"tinner", "thash", "trange"}
	for i := 0; i < 50; i++ {
		var r [][]interface{}
		op := ops[rand.Intn(len(ops))]
		agg := aggFuncs[rand.Intn(len(aggFuncs))]
		x := rand.Intn(10000)
		for _, tbl := range tbls {
			q := fmt.Sprintf(`select * from touter where touter.a > (select %v(%v.b) from %v where %v.a%vtouter.b-%v)`, agg, tbl, tbl, tbl, op, x)
			if r == nil {
				r = tk.MustQuery(q).Sort().Rows()
			} else {
				tk.MustQuery(q).Sort().Check(r)
			}
		}
	}
}

func TestDirectReadingWithUnionScan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_unionscan")
	defer tk.MustExec(`drop database test_unionscan`)
	tk.MustExec("use test_unionscan")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table trange(a int, b int, index idx_a(a)) partition by range(a) (
		partition p0 values less than (10),
		partition p1 values less than (30),
		partition p2 values less than (50))`)
	tk.MustExec(`create table thash(a int, b int, index idx_a(a)) partition by hash(a) partitions 4`)
	tk.MustExec(`create table tnormal(a int, b int, index idx_a(a))`)

	vals := make([]string, 0, 1000)
	for i := 0; i < 1000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(50), rand.Intn(50)))
	}
	for _, tb := range []string{`trange`, `tnormal`, `thash`} {
		sql := fmt.Sprintf(`insert into %v values `+strings.Join(vals, ", "), tb)
		tk.MustExec(sql)
	}

	randCond := func(col string) string {
		la, ra := rand.Intn(50), rand.Intn(50)
		if la > ra {
			la, ra = ra, la
		}
		return fmt.Sprintf(`%v>=%v and %v<=%v`, col, la, col, ra)
	}

	tk.MustExec(`begin`)
	for i := 0; i < 1000; i++ {
		if i == 0 || rand.Intn(2) == 0 { // insert some inflight rows
			val := fmt.Sprintf("(%v, %v)", rand.Intn(50), rand.Intn(50))
			for _, tb := range []string{`trange`, `tnormal`, `thash`} {
				sql := fmt.Sprintf(`insert into %v values `+val, tb)
				tk.MustExec(sql)
			}
		} else {
			var sql string
			switch rand.Intn(3) {
			case 0: // table scan
				sql = `select * from %v ignore index(idx_a) where ` + randCond(`b`)
			case 1: // index reader
				sql = `select a from %v use index(idx_a) where ` + randCond(`a`)
			case 2: // index lookup
				sql = `select * from %v use index(idx_a) where ` + randCond(`a`) + ` and ` + randCond(`b`)
			}
			switch rand.Intn(2) {
			case 0: // order by a
				sql += ` order by a`
			case 1: // order by b
				sql += ` order by b`
			}

			var result [][]interface{}
			for _, tb := range []string{`trange`, `tnormal`, `thash`} {
				q := fmt.Sprintf(sql, tb)
				tk.HasPlan(q, `UnionScan`)
				if result == nil {
					result = tk.MustQuery(q).Sort().Rows()
				} else {
					tk.MustQuery(q).Sort().Check(result)
				}
			}
		}
	}
	tk.MustExec(`rollback`)
}

func TestIssue25030(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_issue_25030")
	tk.MustExec("use test_issue_25030")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`CREATE TABLE tbl_936 (
	col_5410 smallint NOT NULL,
	col_5411 double,
	col_5412 boolean NOT NULL DEFAULT 1,
	col_5413 set('Alice', 'Bob', 'Charlie', 'David') NOT NULL DEFAULT 'Charlie',
	col_5414 varbinary(147) COLLATE 'binary' DEFAULT 'bvpKgYWLfyuTiOYSkj',
	col_5415 timestamp NOT NULL DEFAULT '2021-07-06',
	col_5416 decimal(6, 6) DEFAULT 0.49,
	col_5417 text COLLATE utf8_bin,
	col_5418 float DEFAULT 2048.0762299371554,
	col_5419 int UNSIGNED NOT NULL DEFAULT 3152326370,
	PRIMARY KEY (col_5419) )
	PARTITION BY HASH (col_5419) PARTITIONS 3`)
	tk.MustQuery(`SELECT last_value(col_5414) OVER w FROM tbl_936
	WINDOW w AS (ORDER BY col_5410, col_5411, col_5412, col_5413, col_5414, col_5415, col_5416, col_5417, col_5418, col_5419)
	ORDER BY col_5410, col_5411, col_5412, col_5413, col_5414, col_5415, col_5416, col_5417, col_5418, col_5419, nth_value(col_5412, 5) OVER w`).
		Check(testkit.Rows()) // can work properly without any error or panic
}

func TestUnsignedPartitionColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_unsigned_partition")
	tk.MustExec("use test_unsigned_partition")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`create table thash_pk (a int unsigned, b int, primary key(a)) partition by hash (a) partitions 3`)
	tk.MustExec(`create table trange_pk (a int unsigned, b int, primary key(a)) partition by range (a) (
		partition p1 values less than (100000),
		partition p2 values less than (200000),
		partition p3 values less than (300000),
		partition p4 values less than (400000))`)
	tk.MustExec(`create table tnormal_pk (a int unsigned, b int, primary key(a))`)
	tk.MustExec(`create table thash_uniq (a int unsigned, b int, unique key(a)) partition by hash (a) partitions 3`)
	tk.MustExec(`create table trange_uniq (a int unsigned, b int, unique key(a)) partition by range (a) (
		partition p1 values less than (100000),
		partition p2 values less than (200000),
		partition p3 values less than (300000),
		partition p4 values less than (400000))`)
	tk.MustExec(`create table tnormal_uniq (a int unsigned, b int, unique key(a))`)

	valColA := make(map[int]struct{}, 1000)
	vals := make([]string, 0, 1000)
	for len(vals) < 1000 {
		a := rand.Intn(400000)
		if _, ok := valColA[a]; ok {
			continue
		}
		valColA[a] = struct{}{}
		vals = append(vals, fmt.Sprintf("(%v, %v)", a, rand.Intn(400000)))
	}
	valStr := strings.Join(vals, ", ")
	for _, tbl := range []string{"thash_pk", "trange_pk", "tnormal_pk", "thash_uniq", "trange_uniq", "tnormal_uniq"} {
		tk.MustExec(fmt.Sprintf("insert into %v values %v", tbl, valStr))
	}

	for i := 0; i < 100; i++ {
		scanCond := fmt.Sprintf("a %v %v", []string{">", "<"}[rand.Intn(2)], rand.Intn(400000))
		pointCond := fmt.Sprintf("a = %v", rand.Intn(400000))
		batchCond := fmt.Sprintf("a in (%v, %v, %v)", rand.Intn(400000), rand.Intn(400000), rand.Intn(400000))

		var rScan, rPoint, rBatch [][]interface{}
		for tid, tbl := range []string{"tnormal_pk", "trange_pk", "thash_pk"} {
			// unsigned + TableReader
			scanSQL := fmt.Sprintf("select * from %v use index(primary) where %v", tbl, scanCond)
			require.True(t, tk.HasPlan(scanSQL, "TableReader"))
			r := tk.MustQuery(scanSQL).Sort()
			if tid == 0 {
				rScan = r.Rows()
			} else {
				r.Check(rScan)
			}

			// unsigned + PointGet on PK
			pointSQL := fmt.Sprintf("select * from %v use index(primary) where %v", tbl, pointCond)
			tk.MustPointGet(pointSQL)
			r = tk.MustQuery(pointSQL).Sort()
			if tid == 0 {
				rPoint = r.Rows()
			} else {
				r.Check(rPoint)
			}

			// unsigned + BatchGet on PK
			batchSQL := fmt.Sprintf("select * from %v where %v", tbl, batchCond)
			require.True(t, tk.HasPlan(batchSQL, "Batch_Point_Get"))
			r = tk.MustQuery(batchSQL).Sort()
			if tid == 0 {
				rBatch = r.Rows()
			} else {
				r.Check(rBatch)
			}
		}

		lookupCond := fmt.Sprintf("a %v %v", []string{">", "<"}[rand.Intn(2)], rand.Intn(400000))
		var rLookup [][]interface{}
		for tid, tbl := range []string{"tnormal_uniq", "trange_uniq", "thash_uniq"} {
			// unsigned + IndexReader
			scanSQL := fmt.Sprintf("select a from %v use index(a) where %v", tbl, scanCond)
			require.True(t, tk.HasPlan(scanSQL, "IndexReader"))
			r := tk.MustQuery(scanSQL).Sort()
			if tid == 0 {
				rScan = r.Rows()
			} else {
				r.Check(rScan)
			}

			// unsigned + IndexLookUp
			lookupSQL := fmt.Sprintf("select * from %v use index(a) where %v", tbl, lookupCond)
			tk.MustIndexLookup(lookupSQL)
			r = tk.MustQuery(lookupSQL).Sort()
			if tid == 0 {
				rLookup = r.Rows()
			} else {
				r.Check(rLookup)
			}

			// unsigned + PointGet on UniqueIndex
			pointSQL := fmt.Sprintf("select * from %v use index(a) where %v", tbl, pointCond)
			tk.MustPointGet(pointSQL)
			r = tk.MustQuery(pointSQL).Sort()
			if tid == 0 {
				rPoint = r.Rows()
			} else {
				r.Check(rPoint)
			}

			// unsigned + BatchGet on UniqueIndex
			batchSQL := fmt.Sprintf("select * from %v where %v", tbl, batchCond)
			require.True(t, tk.HasPlan(batchSQL, "Batch_Point_Get"))
			r = tk.MustQuery(batchSQL).Sort()
			if tid == 0 {
				rBatch = r.Rows()
			} else {
				r.Check(rBatch)
			}
		}
	}
}

func TestDirectReadingWithAgg(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_dr_agg")
	tk.MustExec("use test_dr_agg")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// list partition table
	tk.MustExec(`create table tlist(a int, b int, index idx_a(a), index idx_b(b)) partition by list(a)(
		partition p0 values in (1, 2, 3, 4),
		partition p1 values in (5, 6, 7, 8),
		partition p2 values in (9, 10, 11, 12));`)

	// range partition table
	tk.MustExec(`create table trange(a int, b int, index idx_a(a), index idx_b(b)) partition by range(a) (
		partition p0 values less than(300),
		partition p1 values less than (500),
		partition p2 values less than(1100));`)

	// hash partition table
	tk.MustExec(`create table thash(a int, b int) partition by hash(a) partitions 4;`)

	// regular table
	tk.MustExec("create table tregular1(a int, b int, index idx_a(a))")
	tk.MustExec("create table tregular2(a int, b int, index idx_a(a))")

	// generate some random data to be inserted
	vals := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(1100), rand.Intn(2000)))
	}

	tk.MustExec("insert into trange values " + strings.Join(vals, ","))
	tk.MustExec("insert into thash values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular1 values " + strings.Join(vals, ","))

	vals = make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(12)+1, rand.Intn(20)))
	}

	tk.MustExec("insert into tlist values " + strings.Join(vals, ","))
	tk.MustExec("insert into tregular2 values " + strings.Join(vals, ","))

	// test range partition
	for i := 0; i < 200; i++ {
		// select /*+ stream_agg() */ a from t where a > ? group by a;
		// select /*+ hash_agg() */ a from t where a > ? group by a;
		// select /*+ stream_agg() */ a from t where a in(?, ?, ?) group by a;
		// select /*+ hash_agg() */ a from t where a in (?, ?, ?) group by a;
		x := rand.Intn(1099)

		queryPartition1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from trange where a > %v group by a;", x)
		queryRegular1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a > %v group by a;", x)
		require.True(t, tk.HasPlan(queryPartition1, "StreamAgg")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition1).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryPartition2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from trange where a > %v group by a;", x)
		queryRegular2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a > %v group by a;", x)
		require.True(t, tk.HasPlan(queryPartition2, "HashAgg")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition2).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())

		y := rand.Intn(1099)
		z := rand.Intn(1099)

		queryPartition3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from trange where a in(%v, %v, %v) group by a;", x, y, z)
		queryRegular3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a in(%v, %v, %v) group by a;", x, y, z)
		require.True(t, tk.HasPlan(queryPartition3, "StreamAgg")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition3).Sort().Check(tk.MustQuery(queryRegular3).Sort().Rows())

		queryPartition4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from trange where a in (%v, %v, %v) group by a;", x, y, z)
		queryRegular4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a in (%v, %v, %v) group by a;", x, y, z)
		require.True(t, tk.HasPlan(queryPartition4, "HashAgg")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition4).Sort().Check(tk.MustQuery(queryRegular4).Sort().Rows())
	}

	// test hash partition
	for i := 0; i < 200; i++ {
		// select /*+ stream_agg() */ a from t where a > ? group by a;
		// select /*+ hash_agg() */ a from t where a > ? group by a;
		// select /*+ stream_agg() */ a from t where a in(?, ?, ?) group by a;
		// select /*+ hash_agg() */ a from t where a in (?, ?, ?) group by a;
		x := rand.Intn(1099)

		queryPartition1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from thash where a > %v group by a;", x)
		queryRegular1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a > %v group by a;", x)
		require.True(t, tk.HasPlan(queryPartition1, "StreamAgg")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition1).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryPartition2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from thash where a > %v group by a;", x)
		queryRegular2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a > %v group by a;", x)
		require.True(t, tk.HasPlan(queryPartition2, "HashAgg")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition2).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())

		y := rand.Intn(1099)
		z := rand.Intn(1099)

		queryPartition3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from thash where a in(%v, %v, %v) group by a;", x, y, z)
		queryRegular3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a in(%v, %v, %v) group by a;", x, y, z)
		require.True(t, tk.HasPlan(queryPartition3, "StreamAgg")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition3).Sort().Check(tk.MustQuery(queryRegular3).Sort().Rows())

		queryPartition4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from thash where a in (%v, %v, %v) group by a;", x, y, z)
		queryRegular4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a in (%v, %v, %v) group by a;", x, y, z)
		require.True(t, tk.HasPlan(queryPartition4, "HashAgg")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition4).Sort().Check(tk.MustQuery(queryRegular4).Sort().Rows())
	}

	// test list partition
	for i := 0; i < 200; i++ {
		// select /*+ stream_agg() */ a from t where a > ? group by a;
		// select /*+ hash_agg() */ a from t where a > ? group by a;
		// select /*+ stream_agg() */ a from t where a in(?, ?, ?) group by a;
		// select /*+ hash_agg() */ a from t where a in (?, ?, ?) group by a;
		x := rand.Intn(12) + 1

		queryPartition1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tlist where a > %v group by a;", x)
		queryRegular1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular2 where a > %v group by a;", x)
		require.True(t, tk.HasPlan(queryPartition1, "StreamAgg")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition1).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryPartition2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tlist where a > %v group by a;", x)
		queryRegular2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular2 where a > %v group by a;", x)
		require.True(t, tk.HasPlan(queryPartition2, "HashAgg")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition2).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())

		y := rand.Intn(12) + 1
		z := rand.Intn(12) + 1

		queryPartition3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tlist where a in(%v, %v, %v) group by a;", x, y, z)
		queryRegular3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular2 where a in(%v, %v, %v) group by a;", x, y, z)
		require.True(t, tk.HasPlan(queryPartition3, "StreamAgg")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition3).Sort().Check(tk.MustQuery(queryRegular3).Sort().Rows())

		queryPartition4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tlist where a in (%v, %v, %v) group by a;", x, y, z)
		queryRegular4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular2 where a in (%v, %v, %v) group by a;", x, y, z)
		require.True(t, tk.HasPlan(queryPartition4, "HashAgg")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition4).Sort().Check(tk.MustQuery(queryRegular4).Sort().Rows())
	}
}

func TestDynamicModeByDefault(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_dynamic_by_default")

	tk.MustExec(`create table trange(a int, b int, primary key(a) clustered, index idx_b(b)) partition by range(a) (
		partition p0 values less than(300),
		partition p1 values less than(500),
		partition p2 values less than(1100));`)
	tk.MustExec(`create table thash(a int, b int, primary key(a) clustered, index idx_b(b)) partition by hash(a) partitions 4;`)

	for _, q := range []string{
		"explain select * from trange where a>400",
		"explain select * from thash where a>=100",
	} {
		for _, r := range tk.MustQuery(q).Rows() {
			require.NotContains(t, strings.ToLower(r[0].(string)), "partitionunion")
		}
	}
}

func TestIssue24636(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_issue_24636")
	tk.MustExec("use test_issue_24636")

	tk.MustExec(`CREATE TABLE t (a int, b date, c int, PRIMARY KEY (a,b))
		PARTITION BY RANGE ( TO_DAYS(b) ) (
		  PARTITION p0 VALUES LESS THAN (737821),
		  PARTITION p1 VALUES LESS THAN (738289)
		)`)
	tk.MustExec(`INSERT INTO t (a, b, c) VALUES(0, '2021-05-05', 0)`)
	tk.MustQuery(`select c from t use index(primary) where a=0 limit 1`).Check(testkit.Rows("0"))

	tk.MustExec(`
		CREATE TABLE test_partition (
		  a varchar(100) NOT NULL,
		  b date NOT NULL,
		  c varchar(100) NOT NULL,
		  d datetime DEFAULT NULL,
		  e datetime DEFAULT NULL,
		  f bigint(20) DEFAULT NULL,
		  g bigint(20) DEFAULT NULL,
		  h bigint(20) DEFAULT NULL,
		  i bigint(20) DEFAULT NULL,
		  j bigint(20) DEFAULT NULL,
		  k bigint(20) DEFAULT NULL,
		  l bigint(20) DEFAULT NULL,
		  PRIMARY KEY (a,b,c) /*T![clustered_index] NONCLUSTERED */
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
		PARTITION BY RANGE ( TO_DAYS(b) ) (
		  PARTITION pmin VALUES LESS THAN (737821),
		  PARTITION p20200601 VALUES LESS THAN (738289))`)
	tk.MustExec(`INSERT INTO test_partition (a, b, c, d, e, f, g, h, i, j, k, l) VALUES('aaa', '2021-05-05', '428ff6a1-bb37-42ac-9883-33d7a29961e6', '2021-05-06 08:13:38', '2021-05-06 13:28:08', 0, 8, 3, 0, 9, 1, 0)`)
	tk.MustQuery(`select c,j,l from test_partition where c='428ff6a1-bb37-42ac-9883-33d7a29961e6' and a='aaa' limit 0, 200`).Check(testkit.Rows("428ff6a1-bb37-42ac-9883-33d7a29961e6 9 0"))
}

func TestIdexMerge(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_idx_merge")
	tk.MustExec("use test_idx_merge")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// list partition table
	tk.MustExec(`create table tlist(a int, b int, primary key(a) clustered, index idx_b(b)) partition by list(a)(
		partition p0 values in (1, 2, 3, 4),
		partition p1 values in (5, 6, 7, 8),
		partition p2 values in (9, 10, 11, 12));`)

	// range partition table
	tk.MustExec(`create table trange(a int, b int, primary key(a) clustered, index idx_b(b)) partition by range(a) (
		partition p0 values less than(300),
		partition p1 values less than (500),
		partition p2 values less than(1100));`)

	// hash partition table
	tk.MustExec(`create table thash(a int, b int, primary key(a) clustered, index idx_b(b)) partition by hash(a) partitions 4;`)

	// regular table
	tk.MustExec("create table tregular1(a int, b int, primary key(a) clustered)")
	tk.MustExec("create table tregular2(a int, b int, primary key(a) clustered)")

	// generate some random data to be inserted
	vals := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(1100), rand.Intn(2000)))
	}

	tk.MustExec("insert ignore into trange values " + strings.Join(vals, ","))
	tk.MustExec("insert ignore into thash values " + strings.Join(vals, ","))
	tk.MustExec("insert ignore into tregular1 values " + strings.Join(vals, ","))

	vals = make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		vals = append(vals, fmt.Sprintf("(%v, %v)", rand.Intn(12)+1, rand.Intn(20)))
	}

	tk.MustExec("insert ignore into tlist values " + strings.Join(vals, ","))
	tk.MustExec("insert ignore into tregular2 values " + strings.Join(vals, ","))

	// test range partition
	for i := 0; i < 100; i++ {
		x1 := rand.Intn(1099)
		x2 := rand.Intn(1099)

		queryPartition1 := fmt.Sprintf("select /*+ use_index_merge(trange) */ * from trange where a > %v or b < %v;", x1, x2)
		queryRegular1 := fmt.Sprintf("select /*+ use_index_merge(tregular1) */ * from tregular1 where a > %v or b < %v;", x1, x2)
		require.True(t, tk.HasPlan(queryPartition1, "IndexMerge")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition1).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryPartition2 := fmt.Sprintf("select /*+ use_index_merge(trange) */ * from trange where a > %v or b > %v;", x1, x2)
		queryRegular2 := fmt.Sprintf("select /*+ use_index_merge(tregular1) */ * from tregular1 where a > %v or b > %v;", x1, x2)
		require.True(t, tk.HasPlan(queryPartition2, "IndexMerge")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition2).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())
	}

	// test hash partition
	for i := 0; i < 100; i++ {
		x1 := rand.Intn(1099)
		x2 := rand.Intn(1099)

		queryPartition1 := fmt.Sprintf("select /*+ use_index_merge(thash) */ * from thash where a > %v or b < %v;", x1, x2)
		queryRegular1 := fmt.Sprintf("select /*+ use_index_merge(tregualr1) */ * from tregular1 where a > %v or b < %v;", x1, x2)
		require.True(t, tk.HasPlan(queryPartition1, "IndexMerge")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition1).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryPartition2 := fmt.Sprintf("select /*+ use_index_merge(thash) */ * from thash where a > %v or b > %v;", x1, x2)
		queryRegular2 := fmt.Sprintf("select /*+ use_index_merge(tregular1) */ * from tregular1 where a > %v or b > %v;", x1, x2)
		require.True(t, tk.HasPlan(queryPartition2, "IndexMerge")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition2).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())
	}

	// test list partition
	for i := 0; i < 100; i++ {
		x1 := rand.Intn(12) + 1
		x2 := rand.Intn(12) + 1
		queryPartition1 := fmt.Sprintf("select /*+ use_index_merge(tlist) */ * from tlist where a > %v or b < %v;", x1, x2)
		queryRegular1 := fmt.Sprintf("select /*+ use_index_merge(tregular2) */ * from tregular2 where a > %v or b < %v;", x1, x2)
		require.True(t, tk.HasPlan(queryPartition1, "IndexMerge")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition1).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryPartition2 := fmt.Sprintf("select /*+ use_index_merge(tlist) */ * from tlist where a > %v or b > %v;", x1, x2)
		queryRegular2 := fmt.Sprintf("select /*+ use_index_merge(tregular2) */ * from tregular2 where a > %v or b > %v;", x1, x2)
		require.True(t, tk.HasPlan(queryPartition2, "IndexMerge")) // check if IndexLookUp is used
		tk.MustQuery(queryPartition2).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())
	}
}

func TestIssue25309(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test_issue_25309")
	tk.MustExec("use test_issue_25309")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	tk.MustExec(`CREATE TABLE tbl_500 (
      col_20 tinyint(4) NOT NULL,
      col_21 varchar(399) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
      col_22 json DEFAULT NULL,
      col_23 blob DEFAULT NULL,
      col_24 mediumint(9) NOT NULL,
      col_25 float NOT NULL DEFAULT '7306.384497585912',
      col_26 binary(196) NOT NULL,
      col_27 timestamp DEFAULT '1976-12-08 00:00:00',
      col_28 bigint(20) NOT NULL,
      col_29 tinyint(1) NOT NULL DEFAULT '1',
      PRIMARY KEY (col_29,col_20) /*T![clustered_index] NONCLUSTERED */,
      KEY idx_7 (col_28,col_20,col_26,col_27,col_21,col_24),
      KEY idx_8 (col_25,col_29,col_24)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)

	tk.MustExec(`CREATE TABLE tbl_600 (
      col_60 int(11) NOT NULL DEFAULT '-776833487',
      col_61 tinyint(1) NOT NULL DEFAULT '1',
      col_62 tinyint(4) NOT NULL DEFAULT '-125',
      PRIMARY KEY (col_62,col_60,col_61) /*T![clustered_index] NONCLUSTERED */,
      KEY idx_19 (col_60)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
    PARTITION BY HASH( col_60 )
    PARTITIONS 1`)

	tk.MustExec(`insert into tbl_500 select -34, 'lrfGPPPUuZjtT', '{"obj1": {"sub_obj0": 100}}', 0x6C47636D, 1325624, 7306.3843, 'abc', '1976-12-08', 4757891479624162031, 0`)
	tk.MustQuery(`select tbl_5.* from tbl_500 tbl_5 where col_24 in ( select col_62 from tbl_600 where tbl_5.col_26 < 'hSvHLdQeGBNIyOFXStV' )`).Check(testkit.Rows())
}

func TestGlobalIndexScan(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	restoreConfig := config.RestoreFunc()
	defer restoreConfig()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableGlobalIndex = true
	})
	tk.MustExec("use test")
	tk.MustExec("drop table if exists p")
	tk.MustExec(`create table p (id int, c int) partition by range (c) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (10))`)
	tk.MustExec("alter table p add unique idx(id)")
	tk.MustExec("insert into p values (1,3), (3,4), (5,6), (7,9)")
	tk.MustQuery("select id from p use index (idx)").Check(testkit.Rows("1", "3", "5", "7"))
}

func TestGlobalIndexDoubleRead(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	restoreConfig := config.RestoreFunc()
	defer restoreConfig()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableGlobalIndex = true
	})
	tk.MustExec("use test")
	tk.MustExec("drop table if exists p")
	tk.MustExec(`create table p (id int, c int) partition by range (c) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (10))`)
	tk.MustExec("alter table p add unique idx(id)")
	tk.MustExec("insert into p values (1,3), (3,4), (5,6), (7,9)")
	tk.MustQuery("select * from p use index (idx)").Sort().Check(testkit.Rows("1 3", "3 4", "5 6", "7 9"))
}

func TestIssue20028(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("set @@tidb_partition_prune_mode='static-only'")
	tk.MustExec(`create table t1 (c_datetime datetime, primary key (c_datetime))
partition by range (to_days(c_datetime)) ( partition p0 values less than (to_days('2020-02-01')),
partition p1 values less than (to_days('2020-04-01')),
partition p2 values less than (to_days('2020-06-01')),
partition p3 values less than maxvalue)`)
	tk.MustExec("create table t2 (c_datetime datetime, unique key(c_datetime))")
	tk.MustExec("insert into t1 values ('2020-06-26 03:24:00'), ('2020-02-21 07:15:33'), ('2020-04-27 13:50:58')")
	tk.MustExec("insert into t2 values ('2020-01-10 09:36:00'), ('2020-02-04 06:00:00'), ('2020-06-12 03:45:18')")
	tk.MustExec("begin")
	tk.MustQuery("select * from t1 join t2 on t1.c_datetime >= t2.c_datetime for update").
		Sort().
		Check(testkit.Rows(
			"2020-02-21 07:15:33 2020-01-10 09:36:00",
			"2020-02-21 07:15:33 2020-02-04 06:00:00",
			"2020-04-27 13:50:58 2020-01-10 09:36:00",
			"2020-04-27 13:50:58 2020-02-04 06:00:00",
			"2020-06-26 03:24:00 2020-01-10 09:36:00",
			"2020-06-26 03:24:00 2020-02-04 06:00:00",
			"2020-06-26 03:24:00 2020-06-12 03:45:18"))
	tk.MustExec("rollback")
}

func TestSelectLockOnPartitionTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists pt")
	tk.MustExec(`create table pt (id int primary key, k int, c int, index(k))
partition by range (id) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (11))`)

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	optimisticTableReader := func() {
		tk.MustExec("set @@tidb_txn_mode = 'optimistic'")
		tk2.MustExec("set @@tidb_txn_mode = 'optimistic'")
		tk.MustExec("begin")
		tk.MustQuery("select id, k from pt ignore index (k) where k = 5 for update").Check(testkit.Rows("5 5"))
		tk2.MustExec("update pt set c = c + 1 where k = 5")
		_, err := tk.Exec("commit")
		require.Error(t, err) // Write conflict
	}

	optimisticIndexReader := func() {
		tk.MustExec("set @@tidb_txn_mode = 'optimistic'")
		tk2.MustExec("set @@tidb_txn_mode = 'optimistic'")
		tk.MustExec("begin")
		// This is not index reader actually.
		tk.MustQuery("select k from pt where k = 5 for update").Check(testkit.Rows("5"))
		tk2.MustExec("update pt set c = c + 1 where k = 5")
		_, err := tk.Exec("commit")
		require.Error(t, err)
	}

	optimisticIndexLookUp := func() {
		tk.MustExec("set @@tidb_txn_mode = 'optimistic'")
		tk2.MustExec("set @@tidb_txn_mode = 'optimistic'")
		tk.MustExec("begin")
		tk.MustQuery("select c, k from pt use index (k) where k = 5 for update").Check(testkit.Rows("5 5"))
		tk2.MustExec("update pt set c = c + 1 where k = 5")
		_, err := tk.Exec("commit")
		require.Error(t, err)
	}

	pessimisticTableReader := func() {
		tk.MustExec("set @@tidb_txn_mode = 'pessimistic'")
		tk2.MustExec("set @@tidb_txn_mode = 'pessimistic'")
		tk.MustExec("begin")
		tk.MustQuery("select id, k from pt ignore index (k) where k = 5 for update").Check(testkit.Rows("5 5"))
		ch := make(chan int, 2)
		go func() {
			tk2.MustExec("update pt set c = c + 1 where k = 5")
			ch <- 1
		}()
		time.Sleep(100 * time.Millisecond)
		ch <- 2

		// Check the operation in the goroutine is blocked, if not the first result in
		// the channel should be 1.
		require.Equal(t, 2, <-ch)

		tk.MustExec("commit")
		<-ch
		tk.MustQuery("select c from pt where k = 5").Check(testkit.Rows("6"))
	}

	pessimisticIndexReader := func() {
		tk.MustExec("set @@tidb_txn_mode = 'pessimistic'")
		tk2.MustExec("set @@tidb_txn_mode = 'pessimistic'")
		tk.MustExec("begin")
		// This is not index reader actually.
		tk.MustQuery("select k from pt where k = 5 for update").Check(testkit.Rows("5"))
		ch := make(chan int, 2)
		go func() {
			tk2.MustExec("update pt set c = c + 1 where k = 5")
			ch <- 1
		}()
		time.Sleep(100 * time.Millisecond)
		ch <- 2

		// Check the operation in the goroutine is blocked,
		require.Equal(t, 2, <-ch)

		tk.MustExec("commit")
		<-ch
		tk.MustQuery("select c from pt where k = 5").Check(testkit.Rows("6"))
	}

	pessimisticIndexLookUp := func() {
		tk.MustExec("set @@tidb_txn_mode = 'pessimistic'")
		tk2.MustExec("set @@tidb_txn_mode = 'pessimistic'")
		tk.MustExec("begin")
		tk.MustQuery("select c, k from pt use index (k) where k = 5 for update").Check(testkit.Rows("5 5"))
		ch := make(chan int, 2)
		go func() {
			tk2.MustExec("update pt set c = c + 1 where k = 5")
			ch <- 1
		}()
		time.Sleep(100 * time.Millisecond)
		ch <- 2

		// Check the operation in the goroutine is blocked,
		require.Equal(t, 2, <-ch)

		tk.MustExec("commit")
		<-ch
		tk.MustQuery("select c from pt where k = 5").Check(testkit.Rows("6"))
	}

	partitionModes := []string{
		"'dynamic'",
		"'static'",
	}
	testCases := []func(){
		optimisticTableReader,
		optimisticIndexLookUp,
		optimisticIndexReader,
		pessimisticTableReader,
		pessimisticIndexReader,
		pessimisticIndexLookUp,
	}

	for _, mode := range partitionModes {
		tk.MustExec("set @@tidb_partition_prune_mode=" + mode)
		for _, c := range testCases {
			tk.MustExec("replace into pt values (5, 5, 5)")
			c()
		}
	}
}

func TestIssue21731(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists p, t")
	tk.MustExec("set @@tidb_enable_list_partition = OFF")
	// Notice that this does not really test the issue #21731
	tk.MustExec("create table t (a int, b int, unique index idx(a)) partition by list columns(b) (partition p0 values in (1), partition p1 values in (2));")
}

type testOutput struct {
	SQL  string
	Plan []string
	Res  []string
}

func verifyPartitionResult(tk *testkit.TestKit, input []string, output []testOutput) {
	for i, tt := range input {
		var isSelect = false
		if strings.HasPrefix(strings.ToLower(tt), "select ") {
			isSelect = true
		}
		testdata.OnRecord(func() {
			output[i].SQL = tt
			if isSelect {
				output[i].Plan = testdata.ConvertRowsToStrings(tk.UsedPartitions(tt).Rows())
				output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
			} else {
				// Just verify SELECT (also avoid double INSERTs during record)
				output[i].Res = nil
				output[i].Plan = nil
			}
		})
		if isSelect {
			tk.UsedPartitions(tt).Check(testkit.Rows(output[i].Plan...))
			tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
		} else {
			tk.MustExec(tt)
		}
	}
}

func TestRangePartitionBoundariesEq(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("SET @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("CREATE DATABASE TestRangePartitionBoundaries")
	defer tk.MustExec("DROP DATABASE TestRangePartitionBoundaries")
	tk.MustExec("USE TestRangePartitionBoundaries")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec(`CREATE TABLE t
(a INT, b varchar(255))
PARTITION BY RANGE (a) (
 PARTITION p0 VALUES LESS THAN (1000000),
 PARTITION p1 VALUES LESS THAN (2000000),
 PARTITION p2 VALUES LESS THAN (3000000));
`)

	var input []string
	var output []testOutput
	executorSuiteData.GetTestCases(t, &input, &output)
	verifyPartitionResult(tk, input, output)
}

func TestRangePartitionBoundariesNe(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("SET @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("CREATE DATABASE TestRangePartitionBoundariesNe")
	defer tk.MustExec("DROP DATABASE TestRangePartitionBoundariesNe")
	tk.MustExec("USE TestRangePartitionBoundariesNe")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec(`CREATE TABLE t
(a INT, b varchar(255))
PARTITION BY RANGE (a) (
 PARTITION p0 VALUES LESS THAN (1),
 PARTITION p1 VALUES LESS THAN (2),
 PARTITION p2 VALUES LESS THAN (3),
 PARTITION p3 VALUES LESS THAN (4),
 PARTITION p4 VALUES LESS THAN (5),
 PARTITION p5 VALUES LESS THAN (6),
 PARTITION p6 VALUES LESS THAN (7))`)

	var input []string
	var output []testOutput
	executorSuiteData.GetTestCases(t, &input, &output)
	verifyPartitionResult(tk, input, output)
}

func TestRangePartitionBoundariesBetweenM(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("CREATE DATABASE IF NOT EXISTS TestRangePartitionBoundariesBetweenM")
	defer tk.MustExec("DROP DATABASE TestRangePartitionBoundariesBetweenM")
	tk.MustExec("USE TestRangePartitionBoundariesBetweenM")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec(`CREATE TABLE t
(a INT, b varchar(255))
PARTITION BY RANGE (a) (
 PARTITION p0 VALUES LESS THAN (1000000),
 PARTITION p1 VALUES LESS THAN (2000000),
 PARTITION p2 VALUES LESS THAN (3000000))`)

	var input []string
	var output []testOutput
	executorSuiteData.GetTestCases(t, &input, &output)
	verifyPartitionResult(tk, input, output)
}

func TestRangePartitionBoundariesBetweenS(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("CREATE DATABASE IF NOT EXISTS TestRangePartitionBoundariesBetweenS")
	defer tk.MustExec("DROP DATABASE TestRangePartitionBoundariesBetweenS")
	tk.MustExec("USE TestRangePartitionBoundariesBetweenS")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec(`CREATE TABLE t
(a INT, b varchar(255))
PARTITION BY RANGE (a) (
 PARTITION p0 VALUES LESS THAN (1),
 PARTITION p1 VALUES LESS THAN (2),
 PARTITION p2 VALUES LESS THAN (3),
 PARTITION p3 VALUES LESS THAN (4),
 PARTITION p4 VALUES LESS THAN (5),
 PARTITION p5 VALUES LESS THAN (6),
 PARTITION p6 VALUES LESS THAN (7))`)

	var input []string
	var output []testOutput
	executorSuiteData.GetTestCases(t, &input, &output)
	verifyPartitionResult(tk, input, output)
}

func TestRangePartitionBoundariesLtM(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("create database TestRangePartitionBoundariesLtM")
	defer tk.MustExec("drop database TestRangePartitionBoundariesLtM")
	tk.MustExec("use TestRangePartitionBoundariesLtM")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`CREATE TABLE t
(a INT, b varchar(255))
PARTITION BY RANGE (a) (
 PARTITION p0 VALUES LESS THAN (1000000),
 PARTITION p1 VALUES LESS THAN (2000000),
 PARTITION p2 VALUES LESS THAN (3000000))`)

	var input []string
	var output []testOutput
	executorSuiteData.GetTestCases(t, &input, &output)
	verifyPartitionResult(tk, input, output)
}

func TestRangePartitionBoundariesLtS(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("create database TestRangePartitionBoundariesLtS")
	defer tk.MustExec("drop database TestRangePartitionBoundariesLtS")
	tk.MustExec("use TestRangePartitionBoundariesLtS")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`CREATE TABLE t
(a INT, b varchar(255))
PARTITION BY RANGE (a) (
 PARTITION p0 VALUES LESS THAN (1),
 PARTITION p1 VALUES LESS THAN (2),
 PARTITION p2 VALUES LESS THAN (3),
 PARTITION p3 VALUES LESS THAN (4),
 PARTITION p4 VALUES LESS THAN (5),
 PARTITION p5 VALUES LESS THAN (6),
 PARTITION p6 VALUES LESS THAN (7))`)

	var input []string
	var output []testOutput
	executorSuiteData.GetTestCases(t, &input, &output)
	verifyPartitionResult(tk, input, output)
}

func TestIssue25528(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
	tk.MustExec("use test")
	tk.MustExec("create table issue25528 (id int primary key, balance DECIMAL(10, 2), balance2 DECIMAL(10, 2) GENERATED ALWAYS AS (-balance) VIRTUAL, created_at TIMESTAMP) PARTITION BY HASH(id) PARTITIONS 8")
	tk.MustExec("insert into issue25528 (id, balance, created_at) values(1, 100, '2021-06-17 22:35:20')")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from issue25528 where id = 1 for update").Check(testkit.Rows("1 100.00 -100.00 2021-06-17 22:35:20"))

	tk.MustExec("drop table if exists issue25528")
	tk.MustExec("CREATE TABLE `issue25528` ( `c1` int(11) NOT NULL, `c2` int(11) DEFAULT NULL, `c3` int(11) DEFAULT NULL, `c4` int(11) DEFAULT NULL, PRIMARY KEY (`c1`) /*T![clustered_index] CLUSTERED */, KEY `k2` (`c2`), KEY `k3` (`c3`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin PARTITION BY HASH( `c1` ) PARTITIONS 10;")
	tk.MustExec("INSERT INTO issue25528 (`c1`, `c2`, `c3`, `c4`) VALUES (1, 1, 1, 1) , (3, 3, 3, 3) , (2, 2, 2, 2) , (4, 4, 4, 4);")
	tk.MustQuery("select * from issue25528 where c1 in (3, 4) order by c2 for update;").Check(testkit.Rows("3 3 3 3", "4 4 4 4"))
}

func TestIssue26251(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	restoreConfig := config.RestoreFunc()
	defer restoreConfig()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.EnableGlobalIndex = true
	})
	tk1.MustExec("use test")
	tk1.MustExec("create table tp (id int primary key) partition by range (id) (partition p0 values less than (100));")
	tk1.MustExec("create table tn (id int primary key);")
	tk1.MustExec("insert into tp values(1),(2);")
	tk1.MustExec("insert into tn values(1),(2);")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")

	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("select * from tp,tn where tp.id=tn.id and tn.id<=1 for update;").Check(testkit.Rows("1 1"))

	ch := make(chan struct{}, 1)
	tk2.MustExec("begin pessimistic")
	go func() {
		// This query should block.
		tk2.MustQuery("select * from tn where id=1 for update;").Check(testkit.Rows("1"))
		ch <- struct{}{}
	}()

	select {
	case <-time.After(100 * time.Millisecond):
		// Expected, query blocked, not finish within 100ms.
		tk1.MustExec("rollback")
	case <-ch:
		// Unexpected, test fail.
		t.Fail()
	}

	// Clean up
	<-ch
	tk2.MustExec("rollback")
}

func TestLeftJoinForUpdate(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("create database TestLeftJoinForUpdate")
	defer tk1.MustExec("drop database TestLeftJoinForUpdate")
	tk1.MustExec("use TestLeftJoinForUpdate")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use TestLeftJoinForUpdate")
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use TestLeftJoinForUpdate")

	tk1.MustExec("drop table if exists nt, pt")
	tk1.MustExec("create table nt (id int, col varchar(32), primary key (id))")
	tk1.MustExec("create table pt (id int, col varchar(32), primary key (id)) partition by hash(id) partitions 4")

	resetData := func() {
		tk1.MustExec("truncate table nt")
		tk1.MustExec("truncate table pt")
		tk1.MustExec("insert into nt values (1, 'hello')")
		tk1.MustExec("insert into pt values (2, 'test')")
	}

	// ========================== First round of test ==================
	// partition table left join normal table.
	// =================================================================
	resetData()
	ch := make(chan int, 10)
	tk1.MustExec("begin pessimistic")
	// No union scan
	tk1.MustQuery("select * from pt left join nt on pt.id = nt.id for update").Check(testkit.Rows("2 test <nil> <nil>"))
	go func() {
		// Check the key is locked.
		tk2.MustExec("update pt set col = 'xxx' where id = 2")
		ch <- 2
	}()

	// Union scan
	tk1.MustExec("insert into pt values (1, 'world')")
	tk1.MustQuery("select * from pt left join nt on pt.id = nt.id for update").Sort().Check(testkit.Rows("1 world 1 hello", "2 test <nil> <nil>"))
	go func() {
		// Check the key is locked.
		tk3.MustExec("update nt set col = 'yyy' where id = 1")
		ch <- 3
	}()

	// Give chance for the goroutines to run first.
	time.Sleep(80 * time.Millisecond)
	ch <- 1
	tk1.MustExec("rollback")

	checkOrder := func() {
		require.Equal(t, <-ch, 1)
		v1 := <-ch
		v2 := <-ch
		require.True(t, (v1 == 2 && v2 == 3) || (v1 == 3 && v2 == 2))
	}
	checkOrder()

	// ========================== Another round of test ==================
	// normal table left join partition table.
	// ===================================================================
	resetData()
	tk1.MustExec("begin pessimistic")
	// No union scan
	tk1.MustQuery("select * from nt left join pt on pt.id = nt.id for update").Check(testkit.Rows("1 hello <nil> <nil>"))

	// Union scan
	tk1.MustExec("insert into pt values (1, 'world')")
	tk1.MustQuery("select * from nt left join pt on pt.id = nt.id for update").Check(testkit.Rows("1 hello 1 world"))
	go func() {
		tk2.MustExec("replace into pt values (1, 'aaa')")
		ch <- 2
	}()
	go func() {
		tk3.MustExec("update nt set col = 'bbb' where id = 1")
		ch <- 3
	}()
	time.Sleep(80 * time.Millisecond)
	ch <- 1
	tk1.MustExec("rollback")
	checkOrder()
}

func TestIssue31024(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("create database TestIssue31024")
	defer tk1.MustExec("drop database TestIssue31024")
	tk1.MustExec("use TestIssue31024")
	tk1.MustExec("create table t1 (c_datetime datetime, c1 int, c2 int, primary key (c_datetime), key(c1), key(c2))" +
		" partition by range (to_days(c_datetime)) " +
		"( partition p0 values less than (to_days('2020-02-01'))," +
		" partition p1 values less than (to_days('2020-04-01'))," +
		" partition p2 values less than (to_days('2020-06-01'))," +
		" partition p3 values less than maxvalue)")
	tk1.MustExec("create table t2 (c_datetime datetime, unique key(c_datetime))")
	tk1.MustExec("insert into t1 values ('2020-06-26 03:24:00', 1, 1), ('2020-02-21 07:15:33', 2, 2), ('2020-04-27 13:50:58', 3, 3)")
	tk1.MustExec("insert into t2 values ('2020-01-10 09:36:00'), ('2020-02-04 06:00:00'), ('2020-06-12 03:45:18')")
	tk1.MustExec("SET GLOBAL tidb_txn_mode = 'pessimistic'")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use TestIssue31024")

	ch := make(chan int, 10)
	tk1.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk1.MustExec("begin pessimistic")
	tk1.MustQuery("select /*+ use_index_merge(t1) */ * from t1 join t2 on t1.c_datetime >= t2.c_datetime where t1.c1 < 10 or t1.c2 < 10 for update")

	go func() {
		// Check the key is locked.
		tk2.MustExec("set @@tidb_partition_prune_mode='dynamic'")
		tk2.MustExec("begin pessimistic")
		tk2.MustExec("update t1 set c_datetime = '2020-06-26 03:24:00' where c1 = 1")
		ch <- 2
	}()

	// Give chance for the goroutines to run first.
	time.Sleep(80 * time.Millisecond)
	ch <- 1
	tk1.MustExec("rollback")

	require.Equal(t, <-ch, 1)
	require.Equal(t, <-ch, 2)

	tk2.MustExec("rollback")
}

func TestIssue27346(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("create database TestIssue27346")
	defer tk1.MustExec("drop database TestIssue27346")
	tk1.MustExec("use TestIssue27346")

	tk1.MustExec("set @@tidb_enable_index_merge=1,@@tidb_partition_prune_mode='dynamic'")

	tk1.MustExec("DROP TABLE IF EXISTS `tbl_18`")
	tk1.MustExec("CREATE TABLE `tbl_18` (`col_119` binary(16) NOT NULL DEFAULT 'skPoKiwYUi',`col_120` int(10) unsigned NOT NULL,`col_121` timestamp NOT NULL,`col_122` double NOT NULL DEFAULT '3937.1887880628115',`col_123` bigint(20) NOT NULL DEFAULT '3550098074891542725',PRIMARY KEY (`col_123`,`col_121`,`col_122`,`col_120`) CLUSTERED,UNIQUE KEY `idx_103` (`col_123`,`col_119`,`col_120`),UNIQUE KEY `idx_104` (`col_122`,`col_120`),UNIQUE KEY `idx_105` (`col_119`,`col_120`),KEY `idx_106` (`col_121`,`col_120`,`col_122`,`col_119`),KEY `idx_107` (`col_121`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci PARTITION BY HASH( `col_120` ) PARTITIONS 3")
	tk1.MustExec("INSERT INTO tbl_18 (`col_119`, `col_120`, `col_121`, `col_122`, `col_123`) VALUES (X'736b506f4b6977595569000000000000', 672436701, '1974-02-24 00:00:00', 3937.1887880628115e0, -7373106839136381229), (X'736b506f4b6977595569000000000000', 2637316689, '1993-10-29 00:00:00', 3937.1887880628115e0, -4522626077860026631), (X'736b506f4b6977595569000000000000', 831809724, '1995-11-20 00:00:00', 3937.1887880628115e0, -4426441253940231780), (X'736b506f4b6977595569000000000000', 1588592628, '2001-03-28 00:00:00', 3937.1887880628115e0, 1329207475772244999), (X'736b506f4b6977595569000000000000', 3908038471, '2031-06-06 00:00:00', 3937.1887880628115e0, -6562815696723135786), (X'736b506f4b6977595569000000000000', 1674237178, '2001-10-24 00:00:00', 3937.1887880628115e0, -6459065549188938772), (X'736b506f4b6977595569000000000000', 3507075493, '2010-03-25 00:00:00', 3937.1887880628115e0, -4329597025765326929), (X'736b506f4b6977595569000000000000', 1276461709, '2019-07-20 00:00:00', 3937.1887880628115e0, 3550098074891542725)")

	tk1.MustQuery("select col_120,col_122,col_123 from tbl_18 where tbl_18.col_122 = 4763.320888074281 and not( tbl_18.col_121 in ( '2032-11-01' , '1975-05-21' , '1994-05-16' , '1984-01-15' ) ) or not( tbl_18.col_121 >= '2008-10-24' ) order by tbl_18.col_119,tbl_18.col_120,tbl_18.col_121,tbl_18.col_122,tbl_18.col_123 limit 919 for update").Sort().Check(testkit.Rows(
		"1588592628 3937.1887880628115 1329207475772244999",
		"1674237178 3937.1887880628115 -6459065549188938772",
		"2637316689 3937.1887880628115 -4522626077860026631",
		"672436701 3937.1887880628115 -7373106839136381229",
		"831809724 3937.1887880628115 -4426441253940231780"))
	tk1.MustQuery("select /*+ use_index_merge( tbl_18 ) */ col_120,col_122,col_123 from tbl_18 where tbl_18.col_122 = 4763.320888074281 and not( tbl_18.col_121 in ( '2032-11-01' , '1975-05-21' , '1994-05-16' , '1984-01-15' ) ) or not( tbl_18.col_121 >= '2008-10-24' ) order by tbl_18.col_119,tbl_18.col_120,tbl_18.col_121,tbl_18.col_122,tbl_18.col_123 limit 919 for update").Sort().Check(testkit.Rows(
		"1588592628 3937.1887880628115 1329207475772244999",
		"1674237178 3937.1887880628115 -6459065549188938772",
		"2637316689 3937.1887880628115 -4522626077860026631",
		"672436701 3937.1887880628115 -7373106839136381229",
		"831809724 3937.1887880628115 -4426441253940231780"))
}

func TestPartitionTableExplain(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database TestPartitionTableExplain")
	tk.MustExec("use TestPartitionTableExplain")
	tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
	tk.MustExec(`create table t (a int primary key, b int, key (b)) partition by hash(a) (partition P0, partition p1, partition P2)`)
	tk.MustExec(`create table t2 (a int, b int)`)
	tk.MustExec(`insert into t values (1,1),(2,2),(3,3)`)
	tk.MustExec(`insert into t2 values (1,1),(2,2),(3,3)`)
	tk.MustExec(`analyze table t`)
	tk.MustExec(`analyze table t2`)
	tk.MustQuery(`explain format = 'brief' select * from t`).Check(testkit.Rows(
		"PartitionUnion 3.00 root  ",
		"├─TableReader 1.00 root  data:TableFullScan",
		"│ └─TableFullScan 1.00 cop[tikv] table:t, partition:P0 keep order:false",
		"├─TableReader 1.00 root  data:TableFullScan",
		"│ └─TableFullScan 1.00 cop[tikv] table:t, partition:p1 keep order:false",
		"└─TableReader 1.00 root  data:TableFullScan",
		"  └─TableFullScan 1.00 cop[tikv] table:t, partition:P2 keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t partition(P0,p1)`).Check(testkit.Rows(
		"PartitionUnion 2.00 root  ",
		"├─TableReader 1.00 root  data:TableFullScan",
		"│ └─TableFullScan 1.00 cop[tikv] table:t, partition:P0 keep order:false",
		"└─TableReader 1.00 root  data:TableFullScan",
		"  └─TableFullScan 1.00 cop[tikv] table:t, partition:p1 keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where a = 1`).Check(testkit.Rows("Point_Get 1.00 root table:t, partition:p1 handle:1"))
	tk.MustQuery(`explain format = 'brief' select * from t where a = 2`).Check(testkit.Rows("Point_Get 1.00 root table:t, partition:P2 handle:2"))
	// above ^^ is enough for Issue32719, the below vv for completeness
	tk.MustQuery(`explain format = 'brief' select * from t where a = 1 OR a = 2`).Check(testkit.Rows(
		"PartitionUnion 2.00 root  ",
		"├─Batch_Point_Get 1.00 root table:t handle:[1 2], keep order:false, desc:false",
		"└─Batch_Point_Get 1.00 root table:t handle:[1 2], keep order:false, desc:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where a IN (2,3,4)`).Check(testkit.Rows("Batch_Point_Get 3.00 root table:t handle:[2 3 4], keep order:false, desc:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where a IN (2,3)`).Check(testkit.Rows("Batch_Point_Get 2.00 root table:t handle:[2 3], keep order:false, desc:false"))
	// above ^^ is for completeness, the below vv is enough for Issue32719
	tk.MustQuery(`explain format = 'brief' select * from t where b = 1`).Check(testkit.Rows(
		"PartitionUnion 1.00 root  ",
		"├─IndexReader 1.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, partition:P0, index:b(b) range:[1,1], keep order:false",
		"├─IndexReader 1.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, partition:p1, index:b(b) range:[1,1], keep order:false",
		"└─IndexReader 1.00 root  index:IndexRangeScan",
		"  └─IndexRangeScan 1.00 cop[tikv] table:t, partition:P2, index:b(b) range:[1,1], keep order:false"))
	// The below vvv is for completeness
	tk.MustQuery(`explain format = 'brief' select * from t where b = 2`).Check(testkit.Rows(
		"PartitionUnion 1.00 root  ",
		"├─IndexReader 1.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, partition:P0, index:b(b) range:[2,2], keep order:false",
		"├─IndexReader 1.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, partition:p1, index:b(b) range:[2,2], keep order:false",
		"└─IndexReader 1.00 root  index:IndexRangeScan",
		"  └─IndexRangeScan 1.00 cop[tikv] table:t, partition:P2, index:b(b) range:[2,2], keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where b = 1 OR b = 2`).Check(testkit.Rows(
		"PartitionUnion 2.00 root  ",
		"├─IndexReader 1.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, partition:P0, index:b(b) range:[1,2], keep order:false",
		"├─IndexReader 1.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, partition:p1, index:b(b) range:[1,2], keep order:false",
		"└─IndexReader 1.00 root  index:IndexRangeScan",
		"  └─IndexRangeScan 1.00 cop[tikv] table:t, partition:P2, index:b(b) range:[1,2], keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where b IN (2,3,4)`).Check(testkit.Rows(
		"PartitionUnion 2.00 root  ",
		"├─IndexReader 1.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, partition:P0, index:b(b) range:[2,2], [3,3], [4,4], keep order:false",
		"├─IndexReader 1.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, partition:p1, index:b(b) range:[2,2], [3,3], [4,4], keep order:false",
		"└─IndexReader 1.00 root  index:IndexRangeScan",
		"  └─IndexRangeScan 1.00 cop[tikv] table:t, partition:P2, index:b(b) range:[2,2], [3,3], [4,4], keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where b IN (2,3)`).Check(testkit.Rows(
		"PartitionUnion 2.00 root  ",
		"├─IndexReader 1.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, partition:P0, index:b(b) range:[2,2], [3,3], keep order:false",
		"├─IndexReader 1.00 root  index:IndexRangeScan",
		"│ └─IndexRangeScan 1.00 cop[tikv] table:t, partition:p1, index:b(b) range:[2,2], [3,3], keep order:false",
		"└─IndexReader 1.00 root  index:IndexRangeScan",
		"  └─IndexRangeScan 1.00 cop[tikv] table:t, partition:P2, index:b(b) range:[2,2], [3,3], keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t,t2 where t2.a = 1 and t2.b = t.b`).Check(testkit.Rows(
		"Projection 1.00 root  testpartitiontableexplain.t.a, testpartitiontableexplain.t.b, testpartitiontableexplain.t2.a, testpartitiontableexplain.t2.b",
		"└─HashJoin 1.00 root  inner join, equal:[eq(testpartitiontableexplain.t2.b, testpartitiontableexplain.t.b)]",
		"  ├─TableReader(Build) 1.00 root  data:Selection",
		"  │ └─Selection 1.00 cop[tikv]  eq(testpartitiontableexplain.t2.a, 1), not(isnull(testpartitiontableexplain.t2.b))",
		"  │   └─TableFullScan 3.00 cop[tikv] table:t2 keep order:false",
		"  └─PartitionUnion(Probe) 3.00 root  ",
		"    ├─IndexReader 1.00 root  index:IndexFullScan",
		"    │ └─IndexFullScan 1.00 cop[tikv] table:t, partition:P0, index:b(b) keep order:false",
		"    ├─IndexReader 1.00 root  index:IndexFullScan",
		"    │ └─IndexFullScan 1.00 cop[tikv] table:t, partition:p1, index:b(b) keep order:false",
		"    └─IndexReader 1.00 root  index:IndexFullScan",
		"      └─IndexFullScan 1.00 cop[tikv] table:t, partition:P2, index:b(b) keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t partition (p1),t2 where t2.a = 1 and t2.b = t.b`).Check(testkit.Rows(
		"HashJoin 1.00 root  inner join, equal:[eq(testpartitiontableexplain.t.b, testpartitiontableexplain.t2.b)]",
		"├─TableReader(Build) 1.00 root  data:Selection",
		"│ └─Selection 1.00 cop[tikv]  eq(testpartitiontableexplain.t2.a, 1), not(isnull(testpartitiontableexplain.t2.b))",
		"│   └─TableFullScan 3.00 cop[tikv] table:t2 keep order:false",
		"└─IndexReader(Probe) 1.00 root  index:IndexFullScan",
		"  └─IndexFullScan 1.00 cop[tikv] table:t, partition:p1, index:b(b) keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t,t2 where t2.a = 1 and t2.b = t.b and t.a = 1`).Check(testkit.Rows(
		"HashJoin 1.00 root  inner join, equal:[eq(testpartitiontableexplain.t.b, testpartitiontableexplain.t2.b)]",
		"├─TableReader(Build) 1.00 root  data:Selection",
		"│ └─Selection 1.00 cop[tikv]  eq(testpartitiontableexplain.t2.a, 1), not(isnull(testpartitiontableexplain.t2.b))",
		"│   └─TableFullScan 3.00 cop[tikv] table:t2 keep order:false",
		"└─Selection(Probe) 1.00 root  not(isnull(testpartitiontableexplain.t.b))",
		"  └─Point_Get 1.00 root table:t, partition:p1 handle:1"))

	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`explain format = 'brief' select * from t`).Check(testkit.Rows(
		"TableReader 3.00 root partition:all data:TableFullScan",
		"└─TableFullScan 3.00 cop[tikv] table:t keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t partition(P0,p1)`).Check(testkit.Rows(
		"TableReader 3.00 root partition:P0,p1 data:TableFullScan",
		"└─TableFullScan 3.00 cop[tikv] table:t keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where a = 1`).Check(testkit.Rows("Point_Get 1.00 root table:t, partition:p1 handle:1"))
	tk.MustQuery(`explain format = 'brief' select * from t where a = 2`).Check(testkit.Rows("Point_Get 1.00 root table:t, partition:P2 handle:2"))
	tk.MustQuery(`explain format = 'brief' select * from t where a = 1 OR a = 2`).Check(testkit.Rows(
		"TableReader 2.00 root partition:p1,P2 data:TableRangeScan",
		"└─TableRangeScan 2.00 cop[tikv] table:t range:[1,1], [2,2], keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where a IN (2,3,4)`).Check(testkit.Rows("Batch_Point_Get 3.00 root table:t handle:[2 3 4], keep order:false, desc:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where a IN (2,3)`).Check(testkit.Rows("Batch_Point_Get 2.00 root table:t handle:[2 3], keep order:false, desc:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where b = 1`).Check(testkit.Rows(
		"IndexReader 1.00 root partition:all index:IndexRangeScan",
		"└─IndexRangeScan 1.00 cop[tikv] table:t, index:b(b) range:[1,1], keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t partition (P0,p1) where b = 1`).Check(testkit.Rows(
		"IndexReader 1.00 root partition:P0,p1 index:IndexRangeScan",
		"└─IndexRangeScan 1.00 cop[tikv] table:t, index:b(b) range:[1,1], keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where b = 1 OR b = 2`).Check(testkit.Rows(
		"IndexReader 2.00 root partition:all index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t, index:b(b) range:[1,2], keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t partition (p1,P2) where b = 1 OR b = 2`).Check(testkit.Rows(
		"IndexReader 2.00 root partition:p1,P2 index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t, index:b(b) range:[1,2], keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where b IN (2,3,4)`).Check(testkit.Rows(
		"IndexReader 2.00 root partition:all index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t, index:b(b) range:[2,2], [3,3], [4,4], keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t where b IN (2,3)`).Check(testkit.Rows(
		"IndexReader 2.00 root partition:all index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t, index:b(b) range:[2,2], [3,3], keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t,t2 where t2.a = 1 and t2.b = t.b`).Check(testkit.Rows(
		"Projection 1.00 root  testpartitiontableexplain.t.a, testpartitiontableexplain.t.b, testpartitiontableexplain.t2.a, testpartitiontableexplain.t2.b",
		"└─IndexJoin 1.00 root  inner join, inner:IndexReader, outer key:testpartitiontableexplain.t2.b, inner key:testpartitiontableexplain.t.b, equal cond:eq(testpartitiontableexplain.t2.b, testpartitiontableexplain.t.b)",
		"  ├─TableReader(Build) 1.00 root  data:Selection",
		"  │ └─Selection 1.00 cop[tikv]  eq(testpartitiontableexplain.t2.a, 1), not(isnull(testpartitiontableexplain.t2.b))",
		"  │   └─TableFullScan 3.00 cop[tikv] table:t2 keep order:false",
		"  └─IndexReader(Probe) 1.00 root partition:all index:Selection",
		"    └─Selection 1.00 cop[tikv]  not(isnull(testpartitiontableexplain.t.b))",
		"      └─IndexRangeScan 1.00 cop[tikv] table:t, index:b(b) range: decided by [eq(testpartitiontableexplain.t.b, testpartitiontableexplain.t2.b)], keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t partition (p1),t2 where t2.a = 1 and t2.b = t.b`).Check(testkit.Rows(
		"Projection 1.00 root  testpartitiontableexplain.t.a, testpartitiontableexplain.t.b, testpartitiontableexplain.t2.a, testpartitiontableexplain.t2.b",
		"└─IndexJoin 1.00 root  inner join, inner:IndexReader, outer key:testpartitiontableexplain.t2.b, inner key:testpartitiontableexplain.t.b, equal cond:eq(testpartitiontableexplain.t2.b, testpartitiontableexplain.t.b)",
		"  ├─TableReader(Build) 1.00 root  data:Selection",
		"  │ └─Selection 1.00 cop[tikv]  eq(testpartitiontableexplain.t2.a, 1), not(isnull(testpartitiontableexplain.t2.b))",
		"  │   └─TableFullScan 3.00 cop[tikv] table:t2 keep order:false",
		"  └─IndexReader(Probe) 1.00 root partition:p1 index:Selection",
		"    └─Selection 1.00 cop[tikv]  not(isnull(testpartitiontableexplain.t.b))",
		"      └─IndexRangeScan 1.00 cop[tikv] table:t, index:b(b) range: decided by [eq(testpartitiontableexplain.t.b, testpartitiontableexplain.t2.b)], keep order:false"))
	tk.MustQuery(`explain format = 'brief' select * from t,t2 where t2.a = 1 and t2.b = t.b and t.a = 1`).Check(testkit.Rows(
		"HashJoin 1.00 root  inner join, equal:[eq(testpartitiontableexplain.t.b, testpartitiontableexplain.t2.b)]",
		"├─TableReader(Build) 1.00 root  data:Selection",
		"│ └─Selection 1.00 cop[tikv]  eq(testpartitiontableexplain.t2.a, 1), not(isnull(testpartitiontableexplain.t2.b))",
		"│   └─TableFullScan 3.00 cop[tikv] table:t2 keep order:false",
		"└─TableReader(Probe) 1.00 root partition:p1 data:Selection",
		"  └─Selection 1.00 cop[tikv]  not(isnull(testpartitiontableexplain.t.b))",
		"    └─TableRangeScan 1.00 cop[tikv] table:t range:[1,1], keep order:false"))
}
