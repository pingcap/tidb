// Copyright 2016 PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite7) TestDirtyTransaction(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int, index idx_b (b));")
	tk.MustExec("insert t value (2, 3), (4, 8), (6, 8)")
	tk.MustExec("begin")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 3", "4 8", "6 8"))
	tk.MustExec("insert t values (1, 5), (3, 4), (7, 6)")
	tk.MustQuery("select * from information_schema.columns")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 5", "2 3", "3 4", "4 8", "6 8", "7 6"))
	tk.MustQuery("select * from t where a = 1").Check(testkit.Rows("1 5"))
	tk.MustQuery("select * from t order by a desc").Check(testkit.Rows("7 6", "6 8", "4 8", "3 4", "2 3", "1 5"))
	tk.MustQuery("select * from t order by b, a").Check(testkit.Rows("2 3", "3 4", "1 5", "7 6", "4 8", "6 8"))
	tk.MustQuery("select * from t order by b desc, a desc").Check(testkit.Rows("6 8", "4 8", "7 6", "1 5", "3 4", "2 3"))
	tk.MustQuery("select b from t where b = 8 order by b desc").Check(testkit.Rows("8", "8"))
	// Delete a snapshot row and a dirty row.
	tk.MustExec("delete from t where a = 2 or a = 3")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 5", "4 8", "6 8", "7 6"))
	tk.MustQuery("select * from t order by a desc").Check(testkit.Rows("7 6", "6 8", "4 8", "1 5"))
	tk.MustQuery("select * from t order by b, a").Check(testkit.Rows("1 5", "7 6", "4 8", "6 8"))
	tk.MustQuery("select * from t order by b desc, a desc").Check(testkit.Rows("6 8", "4 8", "7 6", "1 5"))
	// Add deleted row back.
	tk.MustExec("insert t values (2, 3), (3, 4)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 5", "2 3", "3 4", "4 8", "6 8", "7 6"))
	tk.MustQuery("select * from t order by a desc").Check(testkit.Rows("7 6", "6 8", "4 8", "3 4", "2 3", "1 5"))
	tk.MustQuery("select * from t order by b, a").Check(testkit.Rows("2 3", "3 4", "1 5", "7 6", "4 8", "6 8"))
	tk.MustQuery("select * from t order by b desc, a desc").Check(testkit.Rows("6 8", "4 8", "7 6", "1 5", "3 4", "2 3"))
	// Truncate Table
	tk.MustExec("truncate table t")
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert t values (1, 2)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 2"))
	tk.MustExec("truncate table t")
	tk.MustExec("insert t values (3, 4)")
	tk.MustQuery("select * from t").Check(testkit.Rows("3 4"))
	tk.MustExec("commit")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert t values (2, 3), (4, 5), (6, 7)")
	tk.MustExec("begin")
	tk.MustExec("insert t values (0, 1)")
	tk.MustQuery("select * from t where b = 3").Check(testkit.Rows("2 3"))
	tk.MustExec("commit")

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a json, b bigint);`)
	tk.MustExec(`begin;`)
	tk.MustExec(`insert into t values("\"1\"", 1);`)
	tk.MustQuery(`select * from t`).Check(testkit.Rows(`"1" 1`))
	tk.MustExec(`commit;`)

	tk.MustExec(`drop table if exists t`)
	tk.MustExec("create table t(a int, b int, c int, d int, index idx(c, d))")
	tk.MustExec("begin")
	tk.MustExec("insert into t values(1, 2, 3, 4)")
	tk.MustQuery("select * from t use index(idx) where c > 1 and d = 4").Check(testkit.Rows("1 2 3 4"))
	tk.MustExec("commit")

	// Test partitioned table use wrong table ID.
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`CREATE TABLE t (c1 smallint(6) NOT NULL, c2 char(5) DEFAULT NULL) PARTITION BY RANGE ( c1 ) (
			PARTITION p0 VALUES LESS THAN (10),
			PARTITION p1 VALUES LESS THAN (20),
			PARTITION p2 VALUES LESS THAN (30),
			PARTITION p3 VALUES LESS THAN (MAXVALUE)
	)`)
	tk.MustExec("begin")
	tk.MustExec("insert into t values (1, 1)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t where c1 < 5").Check(testkit.Rows("1 1"))
	tk.MustQuery("select c2 from t").Check(testkit.Rows("1"))
	tk.MustExec("commit")

	// Test general virtual column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int as (a+1), c int as (b+1), index(c));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (1, default, default), (2, default, default), (3, default, default);")
	// TableReader
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3", "2 3 4", "3 4 5"))
	tk.MustQuery("select b from t;").Check(testkit.Rows("2", "3", "4"))
	tk.MustQuery("select c from t;").Check(testkit.Rows("3", "4", "5"))
	tk.MustQuery("select a from t;").Check(testkit.Rows("1", "2", "3"))
	// IndexReader
	tk.MustQuery("select c from t where c > 3;").Check(testkit.Rows("4", "5"))
	tk.MustQuery("select c from t order by c;").Check(testkit.Rows("3", "4", "5"))
	// IndexLookup
	tk.MustQuery("select * from t where c > 3;").Check(testkit.Rows("2 3 4", "3 4 5"))
	tk.MustQuery("select a, b from t use index(c) where c > 3;").Check(testkit.Rows("2 3", "3 4"))
	tk.MustQuery("select a, c from t use index(c) where c > 3;").Check(testkit.Rows("2 4", "3 5"))
	tk.MustQuery("select b, c from t use index(c) where c > 3;").Check(testkit.Rows("3 4", "4 5"))
	// Delete and update some data
	tk.MustExec("delete from t where c > 4;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3", "2 3 4"))
	tk.MustExec("update t set a = 3 where b > 1;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 4 5", "3 4 5"))
	tk.MustExec("commit;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 4 5", "3 4 5"))
	// Again with non-empty table
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (1, default, default), (2, default, default), (3, default, default);")
	// TableReader
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 4 5", "3 4 5", "1 2 3", "2 3 4", "3 4 5"))
	tk.MustQuery("select b from t;").Check(testkit.Rows("4", "4", "2", "3", "4"))
	tk.MustQuery("select c from t;").Check(testkit.Rows("3", "4", "5", "5", "5"))
	tk.MustQuery("select a from t;").Check(testkit.Rows("3", "3", "1", "2", "3"))
	// IndexReader
	tk.MustQuery("select c from t where c > 3;").Check(testkit.Rows("4", "5", "5", "5"))
	tk.MustQuery("select c from t order by c;").Check(testkit.Rows("3", "4", "5", "5", "5"))
	// IndexLookup
	tk.MustQuery("select * from t where c > 3;").Check(testkit.Rows("3 4 5", "3 4 5", "2 3 4", "3 4 5"))
	tk.MustQuery("select a, b from t use index(c) where c > 3;").Check(testkit.Rows("2 3", "3 4", "3 4", "3 4"))
	tk.MustQuery("select a, c from t use index(c) where c > 3;").Check(testkit.Rows("2 4", "3 5", "3 5", "3 5"))
	tk.MustQuery("select b, c from t use index(c) where c > 3;").Check(testkit.Rows("3 4", "4 5", "4 5", "4 5"))
	// Delete and update some data
	tk.MustExec("delete from t where c > 4;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3", "2 3 4"))
	tk.MustExec("update t set a = 3 where b > 2;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3", "3 4 5"))
	tk.MustExec("commit;")
}

func (s *testSuite7) TestUnionScanWithCastCondition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table ta (a varchar(20))")
	tk.MustExec("insert ta values ('1'), ('2')")
	tk.MustExec("create table tb (a varchar(20))")
	tk.MustExec("begin")
	tk.MustQuery("select * from ta where a = 1").Check(testkit.Rows("1"))
	tk.MustExec("insert tb values ('0')")
	tk.MustQuery("select * from ta where a = 1").Check(testkit.Rows("1"))
	tk.MustExec("rollback")
}

func (s *testSuite7) TestUnionScanForMemBufferReader(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int,b int, index idx(b))")
	tk.MustExec("insert t values (1,1),(2,2)")

	// Test for delete in union scan
	tk.MustExec("begin")
	tk.MustExec("delete from t")
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("insert t values (1,1)")
	tk.MustQuery("select a,b from t").Check(testkit.Rows("1 1"))
	tk.MustQuery("select a,b from t use index(idx)").Check(testkit.Rows("1 1"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t")

	// Test update with untouched index columns.
	tk.MustExec("delete from t")
	tk.MustExec("insert t values (1,1),(2,2)")
	tk.MustExec("begin")
	tk.MustExec("update t set a=a+1")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1", "3 2"))
	tk.MustQuery("select * from t use index (idx)").Check(testkit.Rows("2 1", "3 2"))
	tk.MustQuery("select * from t use index (idx) order by b desc").Check(testkit.Rows("3 2", "2 1"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t")

	// Test update with index column.
	tk.MustQuery("select * from t").Check(testkit.Rows("2 1", "3 2"))
	tk.MustExec("begin")
	tk.MustExec("update t set b=b+1 where a=2")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 2", "3 2"))
	tk.MustQuery("select * from t use index(idx)").Check(testkit.Rows("2 2", "3 2"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t")

	// Test index reader order.
	tk.MustQuery("select * from t").Check(testkit.Rows("2 2", "3 2"))
	tk.MustExec("begin")
	tk.MustExec("insert t values (3,3),(1,1),(4,4),(-1,-1);")
	tk.MustQuery("select * from t use index (idx)").Check(testkit.Rows("-1 -1", "1 1", "2 2", "3 2", "3 3", "4 4"))
	tk.MustQuery("select b from t use index (idx) order by b desc").Check(testkit.Rows("4", "3", "2", "2", "1", "-1"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t")

	// test for update unique index.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int,b int, unique index idx(b))")
	tk.MustExec("insert t values (1,1),(2,2)")
	tk.MustExec("begin")
	_, err := tk.Exec("update t set b=b+1")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '2' for key 'idx'")
	// update with unchange index column.
	tk.MustExec("update t set a=a+1")
	tk.MustQuery("select * from t use index (idx)").Check(testkit.Rows("2 1", "3 2"))
	tk.MustQuery("select b from t use index (idx)").Check(testkit.Rows("1", "2"))
	tk.MustExec("update t set b=b+2 where a=2")
	tk.MustQuery("select * from t").Check(testkit.Rows("2 3", "3 2"))
	tk.MustQuery("select * from t use index (idx) order by b desc").Check(testkit.Rows("2 3", "3 2"))
	tk.MustQuery("select * from t use index (idx)").Check(testkit.Rows("3 2", "2 3"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t")

	// Test for getMissIndexRowsByHandle return nil.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int,b int, index idx(a))")
	tk.MustExec("insert into t values (1,1),(2,2),(3,3)")
	tk.MustExec("begin")
	tk.MustExec("update t set b=0 where a=2")
	tk.MustQuery("select * from t ignore index (idx) where a>0 and b>0;").Check(testkit.Rows("1 1", "3 3"))
	tk.MustQuery("select * from t use index (idx) where a>0 and b>0;").Check(testkit.Rows("1 1", "3 3"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t")

	// Test index lookup reader corner case.
	tk.MustExec("drop table if exists tt")
	tk.MustExec("create table tt (a bigint, b int,c int,primary key (a,b));")
	tk.MustExec("insert into tt set a=1,b=1;")
	tk.MustExec("begin;")
	tk.MustExec("update tt set c=1;")
	tk.MustQuery("select * from tt use index (PRIMARY) where c is not null;").Check(testkit.Rows("1 1 1"))
	tk.MustExec("commit")
	tk.MustExec("admin check table tt")

	// Test index reader corner case.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int,b int,primary key(a,b));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t1 values(1, 1);")
	tk.MustQuery("select * from t1 use index(primary) where a=1;").Check(testkit.Rows("1 1"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t1;")

	// Test index reader with pk handle.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int unsigned key,b int,c varchar(10), index idx(b,a,c));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t1 (a,b) values (0, 0), (1, 1);")
	tk.MustQuery("select a,b from t1 use index(idx) where b>0;").Check(testkit.Rows("1 1"))
	tk.MustQuery("select a,b,c from t1 ignore index(idx) where a>=1 order by a desc").Check(testkit.Rows("1 1 <nil>"))
	tk.MustExec("insert into t1 values (2, 2, null), (3, 3, 'a');")
	tk.MustQuery("select a,b from t1 use index(idx) where b>1 and c is not null;").Check(testkit.Rows("3 3"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t1;")

	// Test insert and update with untouched index.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int,b int,c int,index idx(b));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t1 values (1, 1, 1), (2, 2, 2);")
	tk.MustExec("update t1 set c=c+1 where a=1;")
	tk.MustQuery("select * from t1 use index(idx);").Check(testkit.Rows("1 1 2", "2 2 2"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t1;")

	// Test insert and update with untouched unique index.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int,b int,c int,unique index idx(b));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t1 values (1, 1, 1), (2, 2, 2);")
	tk.MustExec("update t1 set c=c+1 where a=1;")
	tk.MustQuery("select * from t1 use index(idx);").Check(testkit.Rows("1 1 2", "2 2 2"))
	tk.MustExec("commit")
	tk.MustExec("admin check table t1;")

	// Test update with 2 index, one untouched, the other index is touched.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int,b int,c int,unique index idx1(a), index idx2(b));")
	tk.MustExec("insert into t1 values (1, 1, 1);")
	tk.MustExec("update t1 set b=b+1 where a=1;")
	tk.MustQuery("select * from t1 use index(idx2);").Check(testkit.Rows("1 2 1"))
	tk.MustExec("admin check table t1;")
}
