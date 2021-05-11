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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"fmt"
	"math/rand"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/israce"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *partitionTableSuite) TestFourReader(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
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
	tk.MustQuery("select c from pt where c < 2 or c >= 9").Check(testkit.Rows("0", "9"))

	// Index lookup
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt").Sort().Check(testkit.Rows("0 0", "2 2", "4 4", "6 6", "7 7", "9 9", "<nil> <nil>"))
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt where id < 4 and c > 10").Check(testkit.Rows())
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt where id < 10 and c > 8").Check(testkit.Rows("9 9"))
	tk.MustQuery("select /*+ use_index(pt, i_id) */ * from pt where id < 10 and c < 2 or c >= 9").Check(testkit.Rows("0 0", "9 9"))

	// Index Merge
	tk.MustExec("set @@tidb_enable_index_merge = 1")
	tk.MustQuery("select /*+ use_index(i_c, i_id) */ * from pt where id = 4 or c < 7").Sort().Check(testkit.Rows("0 0", "2 2", "4 4", "6 6"))
}

func (s *partitionTableSuite) TestPartitionIndexJoin(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
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

func (s *partitionTableSuite) TestPartitionUnionScanIndexJoin(c *C) {
	// For issue https://github.com/pingcap/tidb/issues/19152
	tk := testkit.NewTestKitWithInit(c, s.store)
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

func (s *partitionTableSuite) TestPartitionReaderUnderApply(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
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
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
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

func (s *partitionTableSuite) TestImproveCoverage(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
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

func (s *partitionTableSuite) TestPartitionInfoDisable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
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
	is := tk.Se.GetSessionVars().GetInfoSchema().(infoschema.InfoSchema)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t_info_null"))
	c.Assert(err, IsNil)

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

func (s *partitionTableSuite) TestBatchGetandPointGetwithHashPartition(c *C) {
	if israce.RaceEnabled {
		c.Skip("exhaustive types test, skip race test")
	}

	tk := testkit.NewTestKitWithInit(c, s.store)
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
		queryRegular := fmt.Sprintf("select a from thash where a=%v", x)
		c.Assert(tk.HasPlan(queryHash, "Point_Get"), IsTrue) // check if PointGet is used
		tk.MustQuery(queryHash).Check(tk.MustQuery(queryRegular).Rows())
	}

	// test empty PointGet
	queryHash := fmt.Sprintf("select a from thash where a=200")
	c.Assert(tk.HasPlan(queryHash, "Point_Get"), IsTrue) // check if PointGet is used
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
		c.Assert(tk.HasPlan(queryHash, "Point_Get"), IsTrue) // check if PointGet is used
		tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
	}
}

func (s *partitionTableSuite) TestView(c *C) {
	if israce.RaceEnabled {
		c.Skip("exhaustive types test, skip race test")
	}

	tk := testkit.NewTestKitWithInit(c, s.store)
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

func (s *partitionTableSuite) TestGlobalStatsAndSQLBinding(c *C) {
	if israce.RaceEnabled {
		c.Skip("exhaustive types test, skip race test")
	}

	tk := testkit.NewTestKitWithInit(c, s.store)
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
	tk.MustExec(`create table tlist(a int, b int, key(a)) partition by list (a) (
		partition p0 values in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
		partition p0 values in (10, 11, 12, 13, 14, 15, 16, 17, 18, 19),
		partition p0 values in (20, 21, 22, 23, 24, 25, 26, 27, 28, 29),
		partition p0 values in (30, 31, 32, 33, 34, 35, 36, 37, 38, 39),
		partition p0 values in (40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50))`)

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
	c.Assert(tk.HasPlan("select * from thash where a<100", "TableFullScan"), IsTrue)
	c.Assert(tk.HasPlan("select * from trange where a<100", "TableFullScan"), IsTrue)
	c.Assert(tk.HasPlan("select * from tlist where a<1", "TableFullScan"), IsTrue)

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
	c.Assert(tk.HasPlan("select * from thash where a<100", "TableFullScan"), IsTrue)
	c.Assert(tk.HasPlan("select * from trange where a<100", "TableFullScan"), IsTrue)
	c.Assert(tk.HasPlan("select * from tlist where a<1", "TableFullScan"), IsTrue)

	// drop SQL bindings
	tk.MustExec("drop session binding for select * from thash where a<100")
	tk.MustExec("drop session binding for select * from trange where a<100")
	tk.MustExec("drop session binding for select * from tlist where a<100")

	// use Index(a) again
	tk.MustIndexLookup("select * from thash where a<100")
	tk.MustIndexLookup("select * from trange where a<100")
	tk.MustIndexLookup("select * from tlist where a<1")
}

func createTable4DynamicPruneModeTestWithExpression(tk *testkit.TestKit) {
	tk.MustExec("create table trange(a int) partition by range(a) (partition p0 values less than(3), partition p1 values less than (5), partition p2 values less than(11));")
	tk.MustExec("create table thash(a int) partition by hash(a) partitions 4;")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into trange values(1), (1), (1), (2), (3), (4), (5), (6), (7), (7), (10), (NULL), (NULL);")
	tk.MustExec("insert into thash values(1), (1), (1), (2), (3), (4), (5), (6), (7), (7), (10), (NULL), (NULL);")
	tk.MustExec("insert into t values(1), (1), (1), (2), (3), (4), (5), (6), (7), (7), (10), (NULL), (NULL);")
	tk.MustExec("set session tidb_partition_prune_mode='dynamic'")
	tk.MustExec("analyze table trange")
	tk.MustExec("analyze table thash")
	tk.MustExec("analyze table t")
}

type testData4Expression struct {
	sql        string
	partitions []string
}

func (s *partitionTableSuite) TestDynamicPruneModeWithEqualExpression(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
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
	}

	for _, t := range tests {
		for i := range t.partitions {
			sql := fmt.Sprintf(t.sql, tables[i])
			c.Assert(tk.MustPartition(sql, t.partitions[i]), IsTrue)
			tk.MustQuery(sql).Sort().Check(tk.MustQuery(fmt.Sprintf(t.sql, "t")).Sort().Rows())
		}
	}
}

func (s *partitionTableSuite) TestDirectReadingWithAgg(c *C) {
	if israce.RaceEnabled {
		c.Skip("exhaustive types test, skip race test")
	}

	tk := testkit.NewTestKitWithInit(c, s.store)
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
	for i := 0; i < 2000; i++ {
		// select /*+ stream_agg() */ a from t where a > ? group by a;
		// select /*+ hash_agg() */ a from t where a > ? group by a;
		// select /*+ stream_agg() */ a from t where a in(?, ?, ?) group by a;
		// select /*+ hash_agg() */ a from t where a in (?, ?, ?) group by a;
		x := rand.Intn(1099)

		queryPartition1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from trange where a > %v group by a;", x)
		queryRegular1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a > %v group by a;", x)
		c.Assert(tk.HasPlan(queryPartition1, "StreamAgg"), IsTrue) // check if IndexLookUp is used
		tk.MustQuery(queryPartition1).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryPartition2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from trange where a > %v group by a;", x)
		queryRegular2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a > %v group by a;", x)
		c.Assert(tk.HasPlan(queryPartition2, "HashAgg"), IsTrue) // check if IndexLookUp is used
		tk.MustQuery(queryPartition2).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())

		y := rand.Intn(1099)
		z := rand.Intn(1099)

		queryPartition3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from trange where a in(%v, %v, %v) group by a;", x, y, z)
		queryRegular3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a in(%v, %v, %v) group by a;", x, y, z)
		c.Assert(tk.HasPlan(queryPartition3, "StreamAgg"), IsTrue) // check if IndexLookUp is used
		tk.MustQuery(queryPartition3).Sort().Check(tk.MustQuery(queryRegular3).Sort().Rows())

		queryPartition4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from trange where a in (%v, %v, %v) group by a;", x, y, z)
		queryRegular4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a in (%v, %v, %v) group by a;", x, y, z)
		c.Assert(tk.HasPlan(queryPartition4, "HashAgg"), IsTrue) // check if IndexLookUp is used
		tk.MustQuery(queryPartition4).Sort().Check(tk.MustQuery(queryRegular4).Sort().Rows())
	}

	// test hash partition
	for i := 0; i < 2000; i++ {
		// select /*+ stream_agg() */ a from t where a > ? group by a;
		// select /*+ hash_agg() */ a from t where a > ? group by a;
		// select /*+ stream_agg() */ a from t where a in(?, ?, ?) group by a;
		// select /*+ hash_agg() */ a from t where a in (?, ?, ?) group by a;
		x := rand.Intn(1099)

		queryPartition1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from thash where a > %v group by a;", x)
		queryRegular1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a > %v group by a;", x)
		c.Assert(tk.HasPlan(queryPartition1, "StreamAgg"), IsTrue) // check if IndexLookUp is used
		tk.MustQuery(queryPartition1).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryPartition2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from thash where a > %v group by a;", x)
		queryRegular2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a > %v group by a;", x)
		c.Assert(tk.HasPlan(queryPartition2, "HashAgg"), IsTrue) // check if IndexLookUp is used
		tk.MustQuery(queryPartition2).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())

		y := rand.Intn(1099)
		z := rand.Intn(1099)

		queryPartition3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from thash where a in(%v, %v, %v) group by a;", x, y, z)
		queryRegular3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a in(%v, %v, %v) group by a;", x, y, z)
		c.Assert(tk.HasPlan(queryPartition3, "StreamAgg"), IsTrue) // check if IndexLookUp is used
		tk.MustQuery(queryPartition3).Sort().Check(tk.MustQuery(queryRegular3).Sort().Rows())

		queryPartition4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from thash where a in (%v, %v, %v) group by a;", x, y, z)
		queryRegular4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a in (%v, %v, %v) group by a;", x, y, z)
		c.Assert(tk.HasPlan(queryPartition4, "HashAgg"), IsTrue) // check if IndexLookUp is used
		tk.MustQuery(queryPartition4).Sort().Check(tk.MustQuery(queryRegular4).Sort().Rows())
	}

	// test list partition
	for i := 0; i < 2000; i++ {
		// select /*+ stream_agg() */ a from t where a > ? group by a;
		// select /*+ hash_agg() */ a from t where a > ? group by a;
		// select /*+ stream_agg() */ a from t where a in(?, ?, ?) group by a;
		// select /*+ hash_agg() */ a from t where a in (?, ?, ?) group by a;
		x := rand.Intn(12) + 1

		queryPartition1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tlist where a > %v group by a;", x)
		queryRegular1 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular2 where a > %v group by a;", x)
		c.Assert(tk.HasPlan(queryPartition1, "StreamAgg"), IsTrue) // check if IndexLookUp is used
		tk.MustQuery(queryPartition1).Sort().Check(tk.MustQuery(queryRegular1).Sort().Rows())

		queryPartition2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tlist where a > %v group by a;", x)
		queryRegular2 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular2 where a > %v group by a;", x)
		c.Assert(tk.HasPlan(queryPartition2, "HashAgg"), IsTrue) // check if IndexLookUp is used
		tk.MustQuery(queryPartition2).Sort().Check(tk.MustQuery(queryRegular2).Sort().Rows())

		y := rand.Intn(12) + 1
		z := rand.Intn(12) + 1

		queryPartition3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tlist where a in(%v, %v, %v) group by a;", x, y, z)
		queryRegular3 := fmt.Sprintf("select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular2 where a in(%v, %v, %v) group by a;", x, y, z)
		c.Assert(tk.HasPlan(queryPartition3, "StreamAgg"), IsTrue) // check if IndexLookUp is used
		tk.MustQuery(queryPartition3).Sort().Check(tk.MustQuery(queryRegular3).Sort().Rows())

		queryPartition4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tlist where a in (%v, %v, %v) group by a;", x, y, z)
		queryRegular4 := fmt.Sprintf("select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular2 where a in (%v, %v, %v) group by a;", x, y, z)
		c.Assert(tk.HasPlan(queryPartition4, "HashAgg"), IsTrue) // check if IndexLookUp is used
		tk.MustQuery(queryPartition4).Sort().Check(tk.MustQuery(queryRegular4).Sort().Rows())
	}
}

func (s *globalIndexSuite) TestGlobalIndexScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists p")
	tk.MustExec(`create table p (id int, c int) partition by range (c) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (10))`)
	tk.MustExec("alter table p add unique idx(id)")
	tk.MustExec("insert into p values (1,3), (3,4), (5,6), (7,9)")
	tk.MustQuery("select id from p use index (idx)").Check(testkit.Rows("1", "3", "5", "7"))
}

func (s *globalIndexSuite) TestGlobalIndexDoubleRead(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists p")
	tk.MustExec(`create table p (id int, c int) partition by range (c) (
partition p0 values less than (4),
partition p1 values less than (7),
partition p2 values less than (10))`)
	tk.MustExec("alter table p add unique idx(id)")
	tk.MustExec("insert into p values (1,3), (3,4), (5,6), (7,9)")
	tk.MustQuery("select * from p use index (idx)").Check(testkit.Rows("1 3", "3 4", "5 6", "7 9"))
}

func (s *globalIndexSuite) TestIssue21731(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists p, t")
	tk.MustExec("create table t (a int, b int, unique index idx(a)) partition by list columns(b) (partition p0 values in (1), partition p1 values in (2));")
}
