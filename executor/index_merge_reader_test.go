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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
	"strings"
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

func (s *testSuite1) TestIssue23569(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists tt;")
	tk.MustExec(`create table tt(id bigint(20) NOT NULL,create_time bigint(20) NOT NULL DEFAULT '0' ,driver varchar(64), PRIMARY KEY (id,create_time))
	PARTITION BY RANGE ( create_time ) (
		PARTITION p201901 VALUES LESS THAN (1577808000),
		PARTITION p202001 VALUES LESS THAN (1585670400),
		PARTITION p202002 VALUES LESS THAN (1593532800),
		PARTITION p202003 VALUES LESS THAN (1601481600),
		PARTITION p202004 VALUES LESS THAN (1609430400),
		PARTITION p202101 VALUES LESS THAN (1617206400),
		PARTITION p202102 VALUES LESS THAN (1625068800),
		PARTITION p202103 VALUES LESS THAN (1633017600),
		PARTITION p202104 VALUES LESS THAN (1640966400),
		PARTITION p202201 VALUES LESS THAN (1648742400),
		PARTITION p202202 VALUES LESS THAN (1656604800),
		PARTITION p202203 VALUES LESS THAN (1664553600),
		PARTITION p202204 VALUES LESS THAN (1672502400),
		PARTITION p202301 VALUES LESS THAN (1680278400)
	);`)
	tk.MustExec("insert tt value(1, 1577807000, 'jack'), (2, 1577809000, 'mike'), (3, 1585670500, 'right'), (4, 1601481500, 'hello');")
	tk.MustExec("set @@tidb_enable_index_merge=true;")
	rows := tk.MustQuery("explain select count(*) from tt partition(p202003) where _tidb_rowid is null or (_tidb_rowid>=1 and _tidb_rowid<100);").Rows()
	containsIndexMerge := false
	for _, r := range rows {
		if strings.Contains(fmt.Sprintf("%s", r[0]), "IndexMerge") {
			containsIndexMerge = true
			break
		}
	}
	c.Assert(containsIndexMerge, IsTrue)
	tk.MustQuery("select count(*) from tt partition(p202003) where _tidb_rowid is null or (_tidb_rowid>=1 and _tidb_rowid<100);").Check(testkit.Rows("1"))
}
