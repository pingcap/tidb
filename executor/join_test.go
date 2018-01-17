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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	goctx "golang.org/x/net/context"
)

func (s *testSuite) TestJoinPanic(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode = 'ONLY_FULL_GROUP_BY'")
	tk.MustExec("drop table if exists events")
	tk.MustExec("create table events (clock int, source int)")
	tk.MustQuery("SELECT * FROM events e JOIN (SELECT MAX(clock) AS clock FROM events e2 GROUP BY e2.source) e3 ON e3.clock=e.clock")
	_, err := tk.Exec("SELECT * FROM events e JOIN (SELECT clock FROM events e2 GROUP BY e2.source) e3 ON e3.clock=e.clock")
	c.Check(err, NotNil)
}

func (s *testSuite) TestJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@tidb_index_lookup_size = 2")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int)")
	tk.MustExec("insert t values (1)")
	tests := []struct {
		sql    string
		result [][]interface{}
	}{
		{
			"select 1 from t as a left join t as b on 0",
			testkit.Rows("1"),
		},
		{
			"select 1 from t as a join t as b on 1",
			testkit.Rows("1"),
		},
	}
	for _, tt := range tests {
		result := tk.MustQuery(tt.sql)
		result.Check(tt.result)
	}

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int, c2 int)")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tk.MustExec("insert into t values(1,1),(2,2)")
	tk.MustExec("insert into t1 values(2,3),(4,4)")
	result := tk.MustQuery("select * from t left outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows("1 1 <nil> <nil>"))
	result = tk.MustQuery("select * from t1 right outer join t on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows("<nil> <nil> 1 1"))
	result = tk.MustQuery("select * from t right outer join t1 on t.c1 = t1.c1 where t.c1 = 1 or t1.c2 > 20")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select * from t left outer join t1 on t.c1 = t1.c1 where t1.c1 = 3 or false")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select * from t left outer join t1 on t.c1 = t1.c1 and t.c1 != 1 order by t1.c1")
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

	result = tk.MustQuery("select * from t1 left join t2 on t1.c1 = t2.c1 right join t3 on t2.c1 = t3.c1 order by t1.c1, t1.c2, t2.c1, t2.c2, t3.c1, t3.c2;")
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> 5 5", "<nil> <nil> <nil> <nil> 9 9", "1 1 1 1 1 1"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c1 int)")
	tk.MustExec("insert into t1 values (1), (1), (1)")
	result = tk.MustQuery("select * from t1 a join t1 b on a.c1 = b.c1;")
	result.Check(testkit.Rows("1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1", "1 1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int, index k(c1))")
	tk.MustExec("create table t1(c1 int)")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5),(6),(7)")
	tk.MustExec("insert into t1 values (1),(2),(3),(4),(5),(6),(7)")
	result = tk.MustQuery("select a.c1 from t a , t1 b where a.c1 = b.c1 order by a.c1;")
	result.Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7"))
	// Test race.
	result = tk.MustQuery("select a.c1 from t a , t1 b where a.c1 = b.c1 and a.c1 + b.c1 > 5 order by b.c1")
	result.Check(testkit.Rows("3", "4", "5", "6", "7"))
	result = tk.MustQuery("select a.c1 from t a , (select * from t1 limit 3) b where a.c1 = b.c1 order by b.c1;")
	result.Check(testkit.Rows("1", "2", "3"))

	plan.AllowCartesianProduct = false
	_, err := tk.Exec("select * from t, t1")
	c.Check(plan.ErrCartesianProductUnsupported.Equal(err), IsTrue)
	_, err = tk.Exec("select * from t left join t1 on 1")
	c.Check(plan.ErrCartesianProductUnsupported.Equal(err), IsTrue)
	_, err = tk.Exec("select * from t right join t1 on 1")
	c.Check(plan.ErrCartesianProductUnsupported.Equal(err), IsTrue)
	plan.AllowCartesianProduct = true
	tk.MustExec("drop table if exists t,t2,t1")
	tk.MustExec("create table t(c1 int)")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tk.MustExec("create table t2(c1 int, c2 int)")
	tk.MustExec("insert into t1 values(1,2),(2,3),(3,4)")
	tk.MustExec("insert into t2 values(1,0),(2,0),(3,0)")
	tk.MustExec("insert into t values(1),(2),(3)")
	result = tk.MustQuery("select * from t1 , t2 where t2.c1 = t1.c1 and t2.c2 = 0 and t1.c2 in (select * from t)")
	result.Sort().Check(testkit.Rows("1 2 1 0", "2 3 2 0"))
	result = tk.MustQuery("select * from t1 , t2 where t2.c1 = t1.c1 and t2.c2 = 0 and t1.c1 = 1 order by t1.c2 limit 1")
	result.Sort().Check(testkit.Rows("1 2 1 0"))
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustExec("create table t1(a int, b int, key s(b))")
	tk.MustExec("insert into t values(1, 1), (2, 2), (3, 3)")
	tk.MustExec("insert into t1 values(1, 2), (1, 3), (1, 4), (3, 4), (4, 5)")

	// The physical plans of the two sql are tested at physical_plan_test.go
	tk.MustQuery("select /*+ TIDB_INLJ(t, t1) */ * from t join t1 on t.a=t1.a").Check(testkit.Rows("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4"))
	tk.MustQuery("select /*+ TIDB_INLJ(t1) */ * from t1 join t on t.a=t1.a and t.a < t1.b").Check(testkit.Rows("1 2 1 1", "1 3 1 1", "1 4 1 1", "3 4 3 3"))
	// Test single index reader.
	tk.MustQuery("select /*+ TIDB_INLJ(t, t1) */ t1.b from t1 join t on t.b=t1.b").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select /*+ TIDB_INLJ(t, t1) */ * from t right outer join t1 on t.a=t1.a").Check(testkit.Rows("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4", "<nil> <nil> 4 5"))
	tk.MustQuery("select /*+ TIDB_INLJ(t, t1) */ avg(t.b) from t right outer join t1 on t.a=t1.a").Check(testkit.Rows("1.5000"))

	// Test that two conflict hints will return error.
	_, err = tk.Exec("select /*+ TIDB_INLJ(t) TIDB_SMJ(t) */ * from t join t1 on t.a=t1.a")
	c.Assert(err, NotNil)
	_, err = tk.Exec("select /*+ TIDB_INLJ(t) TIDB_HJ(t) */ from t join t1 on t.a=t1.a")
	c.Assert(err, NotNil)
	_, err = tk.Exec("select /*+ TIDB_SMJ(t) TIDB_HJ(t) */ from t join t1 on t.a=t1.a")
	c.Assert(err, NotNil)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1),(2), (3)")
	tk.MustQuery("select @a := @a + 1 from t, (select @a := 0) b;").Check(testkit.Rows("1", "2", "3"))

	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int primary key, b int, key s(b))")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("insert into t values(1, 3), (2, 2), (3, 1)")
	tk.MustExec("insert into t1 values(0, 0), (1, 2), (1, 3), (3, 4)")
	tk.MustQuery("select /*+ TIDB_INLJ(t) */ * from t join t1 on t.a=t1.a order by t.b").Check(testkit.Rows("3 1 3 4", "1 3 1 2", "1 3 1 3"))
	tk.MustQuery("select /*+ TIDB_INLJ(t1) */ t.a, t.b from t join t1 on t.a=t1.a where t1.b = 4 limit 1").Check(testkit.Rows("3 1"))
	tk.MustQuery("select /*+ TIDB_INLJ(t, t1) */ * from t right join t1 on t.a=t1.a order by t.b").Check(testkit.Rows("<nil> <nil> 0 0", "3 1 3 4", "1 3 1 2", "1 3 1 3"))

	// join reorder will disorganize the resulting schema
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("insert into t values(1,2)")
	tk.MustExec("insert into t1 values(3,4)")
	tk.MustQuery("select (select t1.a from t1 , t where t.a = s.a limit 2) from t as s").Check(testkit.Rows("3"))

	// test index join bug
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int, b int, key s1(a,b), key s2(b))")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("insert into t values(1,2), (5,3), (6,4)")
	tk.MustExec("insert into t1 values(1), (2), (3)")
	tk.MustQuery("select /*+ TIDB_INLJ(t1) */ t1.a from t1, t where t.a = 5 and t.b = t1.a").Check(testkit.Rows("3"))

	// test issue#4997
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec(`
	CREATE TABLE t1 (
  		pk int(11) NOT NULL AUTO_INCREMENT primary key,
  		a int(11) DEFAULT NULL,
  		b date DEFAULT NULL,
  		c varchar(1) DEFAULT NULL,
  		KEY a (a),
  		KEY b (b),
  		KEY c (c,a)
	)`)
	tk.MustExec(`
	CREATE TABLE t2 (
  		pk int(11) NOT NULL AUTO_INCREMENT primary key,
  		a int(11) DEFAULT NULL,
  		b date DEFAULT NULL,
  		c varchar(1) DEFAULT NULL,
  		KEY a (a),
  		KEY b (b),
  		KEY c (c,a)
	)`)
	tk.MustExec(`insert into t1 value(1,1,"2000-11-11", null);`)
	result = tk.MustQuery(`
	SELECT table2.b AS field2 FROM
	(
	  t1 AS table1  LEFT OUTER JOIN
		(SELECT tmp_t2.* FROM ( t2 AS tmp_t1 RIGHT JOIN t1 AS tmp_t2 ON (tmp_t2.a = tmp_t1.a))) AS table2
	  ON (table2.c = table1.c)
	) `)
	result.Check(testkit.Rows("<nil>"))

	// This case is for testing:
	// when the main thread calls Executor.Close() while the out data fetch worker and join workers are still working,
	// we need to stop the goroutines as soon as possible to avoid unexpected error.
	savedConcurrency := plan.JoinConcurrency
	plan.JoinConcurrency = 5
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int)")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t value(1)")
	}
	result = tk.MustQuery("select /*+ TIDB_HJ(s, r) */ * from t as s join t as r on s.a = r.a limit 1;")
	result.Check(testkit.Rows("1 1"))
	plan.JoinConcurrency = savedConcurrency
}

func (s *testSuite) TestJoinCast(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int)")
	tk.MustExec("create table t1(c1 int unsigned)")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t1 values (1)")
	result := tk.MustQuery("select t.c1 from t , t1 where t.c1 = t1.c1")
	result.Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int,c2 double)")
	tk.MustExec("create table t1(c1 double,c2 int)")
	tk.MustExec("insert into t values (1, 2), (1, NULL)")
	tk.MustExec("insert into t1 values (1, 2), (1, NULL)")
	result = tk.MustQuery("select * from t a , t1 b where (a.c1, a.c2) = (b.c1, b.c2);")
	result.Check(testkit.Rows("1 2 1 2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int)")
	tk.MustExec("create table t1(c1 decimal(4,2))")
	tk.MustExec("insert into t values(0), (2)")
	tk.MustExec("insert into t1 values(0), (9)")
	result = tk.MustQuery("select * from t left join t1 on t1.c1 = t.c1")
	result.Sort().Check(testkit.Rows("0 0.00", "2 <nil>"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 decimal(4,1))")
	tk.MustExec("create table t1(c1 decimal(4,2))")
	tk.MustExec("insert into t values(0), (2)")
	tk.MustExec("insert into t1 values(0), (9)")
	result = tk.MustQuery("select * from t left join t1 on t1.c1 = t.c1")
	result.Sort().Check(testkit.Rows("0.0 0.00", "2.0 <nil>"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 decimal(4,1))")
	tk.MustExec("create table t1(c1 decimal(4,2))")
	tk.MustExec("create index k1 on t1(c1)")
	tk.MustExec("insert into t values(0), (2)")
	tk.MustExec("insert into t1 values(0), (9)")
	result = tk.MustQuery("select /*+ TIDB_INLJ(t) */ * from t left join t1 on t1.c1 = t.c1")
	result.Sort().Check(testkit.Rows("0.0 0.00", "2.0 <nil>"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 char(10))")
	tk.MustExec("create table t1(c1 char(10))")
	tk.MustExec("create table t2(c1 char(10))")
	tk.MustExec("insert into t values('abd')")
	tk.MustExec("insert into t1 values('abc')")
	tk.MustExec("insert into t2 values('abc')")
	result = tk.MustQuery("select * from (select * from t union all select * from t1) t1 join t2 on t1.c1 = t2.c1")
	result.Sort().Check(testkit.Rows("abc abc"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(10), index idx(a))")
	tk.MustExec("insert into t values('1'), ('2'), ('3')")
	tk.MustExec("set @@tidb_max_chunk_size=1")
	result = tk.MustQuery("select a from (select /*+ TIDB_INLJ(t1, t2) */ t1.a from t t1 join t t2 on t1.a=t2.a) t group by a")
	result.Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("set @@tidb_max_chunk_size=1024")
}

func (s *testSuite) TestUsing(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3, t4")
	tk.MustExec("create table t1 (a int, c int)")
	tk.MustExec("create table t2 (a int, d int)")
	tk.MustExec("create table t3 (a int)")
	tk.MustExec("create table t4 (a int)")
	tk.MustExec("insert t1 values (2, 4), (1, 3)")
	tk.MustExec("insert t2 values (2, 5), (3, 6)")
	tk.MustExec("insert t3 values (1)")

	tk.MustQuery("select * from t1 join t2 using (a)").Check(testkit.Rows("2 4 5"))
	tk.MustQuery("select t1.a, t2.a from t1 join t2 using (a)").Check(testkit.Rows("2 2"))

	tk.MustQuery("select * from t1 right join t2 using (a) order by a").Check(testkit.Rows("2 5 4", "3 6 <nil>"))
	tk.MustQuery("select t1.a, t2.a from t1 right join t2 using (a) order by t2.a").Check(testkit.Rows("2 2", "<nil> 3"))

	tk.MustQuery("select * from t1 left join t2 using (a) order by a").Check(testkit.Rows("1 3 <nil>", "2 4 5"))
	tk.MustQuery("select t1.a, t2.a from t1 left join t2 using (a) order by t1.a").Check(testkit.Rows("1 <nil>", "2 2"))

	tk.MustQuery("select * from t1 join t2 using (a) right join t3 using (a)").Check(testkit.Rows("1 <nil> <nil>"))
	tk.MustQuery("select * from t1 join t2 using (a) right join t3 on (t2.a = t3.a)").Check(testkit.Rows("<nil> <nil> <nil> 1"))
	tk.MustQuery("select t2.a from t1 join t2 using (a) right join t3 on (t1.a = t3.a)").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select t1.a, t2.a, t3.a from t1 join t2 using (a) right join t3 using (a)").Check(testkit.Rows("<nil> <nil> 1"))
	tk.MustQuery("select t1.c, t2.d from t1 join t2 using (a) right join t3 using (a)").Check(testkit.Rows("<nil> <nil>"))

	tk.MustExec("alter table t1 add column b int default 1 after a")
	tk.MustExec("alter table t2 add column b int default 1 after a")
	tk.MustQuery("select * from t1 join t2 using (b, a)").Check(testkit.Rows("2 1 4 5"))

	tk.MustExec("select * from (t1 join t2 using (a)) join (t3 join t4 using (a)) on (t2.a = t4.a and t1.a = t3.a)")

	tk.MustExec("drop table if exists t, tt")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create table tt(b int, a int)")
	tk.MustExec("insert into t (a, b) values(1, 1)")
	tk.MustExec("insert into tt (a, b) values(1, 2)")
	tk.MustQuery("select * from t join tt using(a)").Check(testkit.Rows("1 1 2"))

	tk.MustExec("drop table if exists t, tt")
	tk.MustExec("create table t(a float, b int)")
	tk.MustExec("create table tt(b bigint, a int)")
	// Check whether this sql can execute successfully.
	tk.MustExec("select * from t join tt using(a)")
}

func (s *testSuite) TestNaturalJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("create table t2 (a int, c int)")
	tk.MustExec("insert t1 values (1, 2), (10, 20)")
	tk.MustExec("insert t2 values (1, 3), (100, 200)")

	tk.MustQuery("select * from t1 natural join t2").Check(testkit.Rows("1 2 3"))
	tk.MustQuery("select * from t1 natural left join t2 order by a").Check(testkit.Rows("1 2 3", "10 20 <nil>"))
	tk.MustQuery("select * from t1 natural right join t2 order by a").Check(testkit.Rows("1 3 2", "100 200 <nil>"))
}

func (s *testSuite) TestMultiJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t35(a35 int primary key, b35 int, x35 int)")
	tk.MustExec("create table t40(a40 int primary key, b40 int, x40 int)")
	tk.MustExec("create table t14(a14 int primary key, b14 int, x14 int)")
	tk.MustExec("create table t42(a42 int primary key, b42 int, x42 int)")
	tk.MustExec("create table t15(a15 int primary key, b15 int, x15 int)")
	tk.MustExec("create table t7(a7 int primary key, b7 int, x7 int)")
	tk.MustExec("create table t64(a64 int primary key, b64 int, x64 int)")
	tk.MustExec("create table t19(a19 int primary key, b19 int, x19 int)")
	tk.MustExec("create table t9(a9 int primary key, b9 int, x9 int)")
	tk.MustExec("create table t8(a8 int primary key, b8 int, x8 int)")
	tk.MustExec("create table t57(a57 int primary key, b57 int, x57 int)")
	tk.MustExec("create table t37(a37 int primary key, b37 int, x37 int)")
	tk.MustExec("create table t44(a44 int primary key, b44 int, x44 int)")
	tk.MustExec("create table t38(a38 int primary key, b38 int, x38 int)")
	tk.MustExec("create table t18(a18 int primary key, b18 int, x18 int)")
	tk.MustExec("create table t62(a62 int primary key, b62 int, x62 int)")
	tk.MustExec("create table t4(a4 int primary key, b4 int, x4 int)")
	tk.MustExec("create table t48(a48 int primary key, b48 int, x48 int)")
	tk.MustExec("create table t31(a31 int primary key, b31 int, x31 int)")
	tk.MustExec("create table t16(a16 int primary key, b16 int, x16 int)")
	tk.MustExec("create table t12(a12 int primary key, b12 int, x12 int)")
	tk.MustExec("insert into t35 values(1,1,1)")
	tk.MustExec("insert into t40 values(1,1,1)")
	tk.MustExec("insert into t14 values(1,1,1)")
	tk.MustExec("insert into t42 values(1,1,1)")
	tk.MustExec("insert into t15 values(1,1,1)")
	tk.MustExec("insert into t7 values(1,1,1)")
	tk.MustExec("insert into t64 values(1,1,1)")
	tk.MustExec("insert into t19 values(1,1,1)")
	tk.MustExec("insert into t9 values(1,1,1)")
	tk.MustExec("insert into t8 values(1,1,1)")
	tk.MustExec("insert into t57 values(1,1,1)")
	tk.MustExec("insert into t37 values(1,1,1)")
	tk.MustExec("insert into t44 values(1,1,1)")
	tk.MustExec("insert into t38 values(1,1,1)")
	tk.MustExec("insert into t18 values(1,1,1)")
	tk.MustExec("insert into t62 values(1,1,1)")
	tk.MustExec("insert into t4 values(1,1,1)")
	tk.MustExec("insert into t48 values(1,1,1)")
	tk.MustExec("insert into t31 values(1,1,1)")
	tk.MustExec("insert into t16 values(1,1,1)")
	tk.MustExec("insert into t12 values(1,1,1)")
	tk.MustExec("insert into t35 values(7,7,7)")
	tk.MustExec("insert into t40 values(7,7,7)")
	tk.MustExec("insert into t14 values(7,7,7)")
	tk.MustExec("insert into t42 values(7,7,7)")
	tk.MustExec("insert into t15 values(7,7,7)")
	tk.MustExec("insert into t7 values(7,7,7)")
	tk.MustExec("insert into t64 values(7,7,7)")
	tk.MustExec("insert into t19 values(7,7,7)")
	tk.MustExec("insert into t9 values(7,7,7)")
	tk.MustExec("insert into t8 values(7,7,7)")
	tk.MustExec("insert into t57 values(7,7,7)")
	tk.MustExec("insert into t37 values(7,7,7)")
	tk.MustExec("insert into t44 values(7,7,7)")
	tk.MustExec("insert into t38 values(7,7,7)")
	tk.MustExec("insert into t18 values(7,7,7)")
	tk.MustExec("insert into t62 values(7,7,7)")
	tk.MustExec("insert into t4 values(7,7,7)")
	tk.MustExec("insert into t48 values(7,7,7)")
	tk.MustExec("insert into t31 values(7,7,7)")
	tk.MustExec("insert into t16 values(7,7,7)")
	tk.MustExec("insert into t12 values(7,7,7)")
	result := tk.MustQuery(`SELECT x4,x8,x38,x44,x31,x9,x57,x48,x19,x40,x14,x12,x7,x64,x37,x18,x62,x35,x42,x15,x16 FROM
t35,t40,t14,t42,t15,t7,t64,t19,t9,t8,t57,t37,t44,t38,t18,t62,t4,t48,t31,t16,t12
WHERE b48=a57
AND a4=b19
AND a14=b16
AND b37=a48
AND a40=b42
AND a31=7
AND a15=b40
AND a38=b8
AND b15=a31
AND b64=a18
AND b12=a44
AND b7=a8
AND b35=a16
AND a12=b14
AND a64=b57
AND b62=a7
AND a35=b38
AND b9=a19
AND a62=b18
AND b4=a37
AND b44=a42`)
	result.Check(testkit.Rows("7 7 7 7 7 7 7 7 7 7 7 7 7 7 7 7 7 7 7 7 7"))
}

func (s *testSuite) TestSubquerySameTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert t values (1), (2)")
	result := tk.MustQuery("select a from t where exists(select 1 from t as x where x.a < t.a)")
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery("select a from t where not exists(select 1 from t as x where x.a < t.a)")
	result.Check(testkit.Rows("1"))
}

func (s *testSuite) TestSubquery(c *C) {
	plan.JoinConcurrency = 1
	defer func() {
		plan.JoinConcurrency = 5
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (2, 2)")
	tk.MustExec("insert t values (3, 4)")
	tk.MustExec("commit")
	result := tk.MustQuery("select * from t where exists(select * from t k where t.c = k.c having sum(c) = 1)")
	result.Check(testkit.Rows("1 1"))
	result = tk.MustQuery("select * from t where exists(select k.c, k.d from t k, t p where t.c = k.d)")
	result.Check(testkit.Rows("1 1", "2 2"))
	result = tk.MustQuery("select 1 = (select count(*) from t where t.c = k.d) from t k")
	result.Check(testkit.Rows("1", "1", "0"))
	result = tk.MustQuery("select 1 = (select count(*) from t where exists( select * from t m where t.c = k.d)) from t k")
	result.Check(testkit.Rows("1", "1", "0"))
	result = tk.MustQuery("select t.c = any (select count(*) from t) from t")
	result.Check(testkit.Rows("0", "0", "1"))
	result = tk.MustQuery("select * from t where (t.c, 6) = any (select count(*), sum(t.c) from t)")
	result.Check(testkit.Rows("3 4"))
	result = tk.MustQuery("select t.c from t where (t.c) < all (select count(*) from t)")
	result.Check(testkit.Rows("1", "2"))
	result = tk.MustQuery("select t.c from t where (t.c, t.d) = any (select * from t)")
	result.Check(testkit.Rows("1", "2", "3"))
	result = tk.MustQuery("select t.c from t where (t.c, t.d) != all (select * from t)")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select (select count(*) from t where t.c = k.d) from t k")
	result.Check(testkit.Rows("1", "1", "0"))
	result = tk.MustQuery("select t.c from t where (t.c, t.d) in (select * from t)")
	result.Check(testkit.Rows("1", "2", "3"))
	result = tk.MustQuery("select t.c from t where (t.c, t.d) not in (select * from t)")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select * from t A inner join t B on A.c = B.c and A.c > 100")
	result.Check(testkit.Rows())
	// = all empty set is true
	result = tk.MustQuery("select t.c from t where (t.c, t.d) != all (select * from t where d > 1000)")
	result.Check(testkit.Rows("1", "2", "3"))
	result = tk.MustQuery("select t.c from t where (t.c) < any (select c from t where d > 1000)")
	result.Check(testkit.Rows())
	tk.MustExec("insert t values (NULL, NULL)")
	result = tk.MustQuery("select (t.c) < any (select c from t) from t")
	result.Check(testkit.Rows("1", "1", "<nil>", "<nil>"))
	result = tk.MustQuery("select (10) > all (select c from t) from t")
	result.Check(testkit.Rows("<nil>", "<nil>", "<nil>", "<nil>"))
	result = tk.MustQuery("select (c) > all (select c from t) from t")
	result.Check(testkit.Rows("0", "0", "0", "<nil>"))

	tk.MustExec("drop table if exists a")
	tk.MustExec("create table a (c int, d int)")
	tk.MustExec("insert a values (1, 2)")
	tk.MustExec("drop table if exists b")
	tk.MustExec("create table b (c int, d int)")
	tk.MustExec("insert b values (2, 1)")

	result = tk.MustQuery("select * from a b where c = (select d from b a where a.c = 2 and b.c = 1)")
	result.Check(testkit.Rows("1 2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c int)")
	tk.MustExec("insert t values(10), (8), (7), (9), (11)")
	result = tk.MustQuery("select * from t where 9 in (select c from t s where s.c < t.c limit 3)")
	result.Check(testkit.Rows("10"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int, v int)")
	tk.MustExec("insert into t values(1, 1), (2, 2), (3, 3)")
	result = tk.MustQuery("select * from t where v=(select min(t1.v) from t t1, t t2, t t3 where t1.id=t2.id and t2.id=t3.id and t1.id=t.id)")
	result.Check(testkit.Rows("1 1", "2 2", "3 3"))

	result = tk.MustQuery("select exists (select t.id from t where s.id < 2 and t.id = s.id) from t s")
	result.Check(testkit.Rows("1", "0", "0"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c int)")
	result = tk.MustQuery("select exists(select count(*) from t)")
	result.Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, v int)")
	tk.MustExec("insert into t values(1, 1), (2, 2), (3, 3)")
	result = tk.MustQuery("select (select t.id from t where s.id < 2 and t.id = s.id) from t s")
	result.Sort().Check(testkit.Rows("1", "<nil>", "<nil>"))
	rs, err := tk.Exec("select (select t.id from t where t.id = t.v and t.v != s.id) from t s")
	c.Check(err, IsNil)
	_, err = tidb.GetRows4Test(goctx.Background(), rs)
	c.Check(err, NotNil)
	c.Check(rs.Close(), IsNil)

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists s")
	tk.MustExec("create table t(id int)")
	tk.MustExec("create table s(id int)")
	tk.MustExec("insert into t values(1), (2)")
	tk.MustExec("insert into s values(2), (2)")
	result = tk.MustQuery("select id from t where(select count(*) from s where s.id = t.id) > 0")
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery("select *, (select count(*) from s where id = t.id limit 1, 1) from t")
	result.Check(testkit.Rows("1 <nil>", "2 <nil>"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists s")
	tk.MustExec("create table t(id int primary key)")
	tk.MustExec("create table s(id int)")
	tk.MustExec("insert into t values(1), (2)")
	tk.MustExec("insert into s values(2), (2)")
	result = tk.MustQuery("select *, (select count(id) from s where id = t.id) from t")
	result.Check(testkit.Rows("1 0", "2 2"))
	result = tk.MustQuery("select *, 0 < any (select count(id) from s where id = t.id) from t")
	result.Check(testkit.Rows("1 0", "2 1"))
	result = tk.MustQuery("select (select count(*) from t k where t.id = id) from s, t where t.id = s.id limit 1")
	result.Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t, s")
	tk.MustExec("create table t(id int primary key)")
	tk.MustExec("create table s(id int, index k(id))")
	tk.MustExec("insert into t values(1), (2)")
	tk.MustExec("insert into s values(2), (2)")
	result = tk.MustQuery("select (select id from s where s.id = t.id order by s.id limit 1) from t")
	result.Check(testkit.Rows("<nil>", "2"))

	tk.MustExec("drop table if exists t, s")
	tk.MustExec("create table t(id int)")
	tk.MustExec("create table s(id int)")
	tk.MustExec("insert into t values(2), (2)")
	tk.MustExec("insert into s values(2)")
	result = tk.MustQuery("select (select id from s where s.id = t.id order by s.id) from t")
	result.Check(testkit.Rows("2", "2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(dt datetime)")
	result = tk.MustQuery("select (select 1 from t where DATE_FORMAT(o.dt,'%Y-%m')) from t o")
	result.Check(testkit.Rows())

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(f1 int, f2 int)")
	tk.MustExec("create table t2(fa int, fb int)")
	tk.MustExec("insert into t1 values (1,1),(1,1),(1,2),(1,2),(1,2),(1,3)")
	tk.MustExec("insert into t2 values (1,1),(1,2),(1,3)")
	result = tk.MustQuery("select f1,f2 from t1 group by f1,f2 having count(1) >= all (select fb from t2 where fa = f1)")
	result.Check(testkit.Rows("1 2"))

	tk.MustExec("DROP TABLE IF EXISTS t1, t2")
	tk.MustExec("CREATE TABLE t1(a INT)")
	tk.MustExec("CREATE TABLE t2 (d BINARY(2), PRIMARY KEY (d(1)), UNIQUE KEY (d))")
	tk.MustExec("INSERT INTO t1 values(1)")
	result = tk.MustQuery("SELECT 1 FROM test.t1, test.t2 WHERE 1 = (SELECT test.t2.d FROM test.t2 WHERE test.t1.a >= 1) and test.t2.d = 1;")
	result.Check(testkit.Rows())

	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec("CREATE TABLE t1(a int, b int default 0)")
	tk.MustExec("create index k1 on t1(a)")
	tk.MustExec("INSERT INTO t1 (a) values(1), (2), (3), (4), (5)")
	result = tk.MustQuery("select (select /*+ TIDB_INLJ(x1) */ x2.a from t1 x1, t1 x2 where x1.a = t1.a and x1.a = x2.a) from t1")
	result.Check(testkit.Rows("1", "2", "3", "4", "5"))
}

func (s *testSuite) TestInSubquery(c *C) {
	defer func() {
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert t values (1, 1), (2, 1)")
	result := tk.MustQuery("select m1.a from t as m1 where m1.a in (select m2.b from t as m2)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select m1.a from t as m1 where (3, m1.b) not in (select * from t as m2)")
	result.Sort().Check(testkit.Rows("1", "2"))
	result = tk.MustQuery("select m1.a from t as m1 where m1.a in (select m2.b+? from t as m2)", 1)
	result.Check(testkit.Rows("2"))
	tk.MustExec(`prepare stmt1 from 'select m1.a from t as m1 where m1.a in (select m2.b+? from t as m2)'`)
	tk.MustExec("set @a = 1")
	result = tk.MustQuery(`execute stmt1 using @a;`)
	result.Check(testkit.Rows("2"))
	tk.MustExec("set @a = 0")
	result = tk.MustQuery(`execute stmt1 using @a;`)
	result.Check(testkit.Rows("1"))

	result = tk.MustQuery("select m1.a from t as m1 where m1.a in (1, 3, 5)")
	result.Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a float)")
	tk.MustExec("insert t1 values (281.37)")
	tk.MustQuery("select a from t1 where (a in (select a from t1))").Check(testkit.Rows("281.37"))

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("insert into t1 values (0,0),(1,1),(2,2),(3,3),(4,4)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("insert into t2 values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")
	result = tk.MustQuery("select a from t1 where (1,1) in (select * from t2 s , t2 t where t1.a = s.a and s.a = t.a limit 1)")
	result.Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("insert into t1 values (1),(2)")
	tk.MustExec("insert into t2 values (1),(2)")
	tk.MustExec("set @@session.tidb_opt_insubquery_unfold = 1")
	result = tk.MustQuery("select * from t1 where a in (select * from t2)")
	result.Sort().Check(testkit.Rows("1", "2"))
	result = tk.MustQuery("select * from t1 where a in (select * from t2 where false)")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select * from t1 where a not in (select * from t2 where false)")
	result.Sort().Check(testkit.Rows("1", "2"))
	tk.MustExec("set @@session.tidb_opt_insubquery_unfold = 0")
	result = tk.MustQuery("select * from t1 where a in (select * from t2)")
	result.Sort().Check(testkit.Rows("1", "2"))
	result = tk.MustQuery("select * from t1 where a in (select * from t2 where false)")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select * from t1 where a not in (select * from t2 where false)")
	result.Sort().Check(testkit.Rows("1", "2"))

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, key b (a))")
	tk.MustExec("create table t2 (a int, key b (a))")
	tk.MustExec("insert into t1 values (1),(2),(2)")
	tk.MustExec("insert into t2 values (1),(2),(2)")
	result = tk.MustQuery("select * from t1 where a in (select * from t2) order by a desc")
	result.Check(testkit.Rows("2", "2", "1"))
	result = tk.MustQuery("select * from t1 where a in (select count(*) from t2 where t1.a = t2.a) order by a desc")
	result.Check(testkit.Rows("2", "2", "1"))
}

func (s *testSuite) TestJoinLeak(c *C) {
	savedConcurrency := plan.JoinConcurrency
	plan.JoinConcurrency = 1
	defer func() {
		plan.JoinConcurrency = savedConcurrency
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (d int)")
	tk.MustExec("begin")
	for i := 0; i < 1002; i++ {
		tk.MustExec("insert t values (1)")
	}
	tk.MustExec("commit")
	result, err := tk.Exec("select * from t t1 left join (select 1) t2 on 1")
	c.Assert(err, IsNil)
	result.Next(goctx.Background())
	time.Sleep(100 * time.Millisecond)
	result.Close()
}

func (s *testSuite) TestHashJoinExecEncodeDecodeRow(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (id int)")
	tk.MustExec("create table t2 (id int, name varchar(255), ts timestamp)")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1, 'xxx', '2003-06-09 10:51:26')")
	result := tk.MustQuery("select ts from t1 inner join t2 where t2.name = 'xxx'")
	result.Check(testkit.Rows("2003-06-09 10:51:26"))
}

func (s *testSuite) TestSubqueryInJoinOn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (id int)")
	tk.MustExec("create table t2 (id int)")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")

	_, err := tk.Exec("SELECT * FROM t1 JOIN t2 on (t2.id < all (SELECT 1))")
	c.Check(err, NotNil)
}

func (s *testSuite) TestIssue5255(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b date, c float, primary key(a, b))")
	tk.MustExec("create table t2(a int primary key)")
	tk.MustExec("insert into t1 values(1, '2017-11-29', 2.2)")
	tk.MustExec("insert into t2 values(1)")
	tk.MustQuery("select /*+ TIDB_INLJ(t2) */ * from t1 join t2 on t1.a=t2.a").Check(testkit.Rows("1 2017-11-29 2.2 1"))
}

func (s *testSuite) TestIssue5278(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, tt")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create table tt(a varchar(10), b int)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustQuery("select * from t left join tt on t.a=tt.a left join t ttt on t.a=ttt.a").Check(testkit.Rows("1 1 <nil> <nil> 1 1"))
}
