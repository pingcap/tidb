// Copyright 2015 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

type testSuiteAgg struct {
	*baseTestSuite
	testData testutil.TestData
}

func (s *testSuiteAgg) SetUpSuite(c *C) {
	s.baseTestSuite.SetUpSuite(c)
	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "agg_suite")
	c.Assert(err, IsNil)
}

func (s *testSuiteAgg) TearDownSuite(c *C) {
	s.baseTestSuite.TearDownSuite(c)
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testSuiteAgg) TestAggregation(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@tidb_hash_join_concurrency=1")
	tk.MustExec("use test")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'") // disable only-full-group-by
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	tk.MustExec("insert t values (NULL, 1)")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (1, 2)")
	tk.MustExec("insert t values (1, 3)")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (3, 2)")
	tk.MustExec("insert t values (4, 3)")
	tk.MustQuery("select bit_and(c) from t where NULL").Check(testkit.Rows("18446744073709551615"))
	tk.MustQuery("select bit_or(c) from t where NULL").Check(testkit.Rows("0"))
	tk.MustQuery("select bit_xor(c) from t where NULL").Check(testkit.Rows("0"))
	result := tk.MustQuery("select count(*) from t")
	result.Check(testkit.Rows("7"))
	result = tk.MustQuery("select count(*) from t group by d order by c")
	result.Check(testkit.Rows("3", "2", "2"))
	result = tk.MustQuery("select distinct 99 from t group by d having d > 0")
	result.Check(testkit.Rows("99"))
	result = tk.MustQuery("select count(*) from t having 1 = 0")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select c,d from t group by d order by d")
	result.Check(testkit.Rows("<nil> 1", "1 2", "1 3"))
	result = tk.MustQuery("select - c, c as d from t group by c having null not between c and avg(distinct d) - d")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select - c as c from t group by c having t.c > 5")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select t1.c from t t1, t t2 group by c having c > 5")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select count(*) from (select d, c from t) k where d != 0 group by d order by c")
	result.Check(testkit.Rows("3", "2", "2"))
	result = tk.MustQuery("select c as a from t group by d having a < 0")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select c as a from t group by d having sum(a) = 2")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select count(distinct c) from t group by d order by c")
	result.Check(testkit.Rows("1", "2", "2"))
	result = tk.MustQuery("select sum(c) as a from t group by d order by a")
	result.Check(testkit.Rows("2", "4", "5"))
	result = tk.MustQuery("select sum(c) as a, sum(c+1), sum(c), sum(c+1) from t group by d order by a")
	result.Check(testkit.Rows("2 4 2 4", "4 6 4 6", "5 7 5 7"))
	result = tk.MustQuery("select count(distinct c,d) from t")
	result.Check(testkit.Rows("5"))
	err := tk.ExecToErr("select count(c,d) from t")
	c.Assert(err, NotNil)
	result = tk.MustQuery("select d*2 as ee, sum(c) from t group by ee order by ee")
	result.Check(testkit.Rows("2 2", "4 4", "6 5"))
	result = tk.MustQuery("select sum(distinct c) as a from t group by d order by a")
	result.Check(testkit.Rows("1", "4", "5"))
	result = tk.MustQuery("select min(c) as a from t group by d order by a")
	result.Check(testkit.Rows("1", "1", "1"))
	result = tk.MustQuery("select max(c) as a from t group by d order by a")
	result.Check(testkit.Rows("1", "3", "4"))
	result = tk.MustQuery("select avg(c) as a from t group by d order by a")
	result.Check(testkit.Rows("1.0000", "2.0000", "2.5000"))
	result = tk.MustQuery("select d, d + 1 from t group by d order by d")
	result.Check(testkit.Rows("1 2", "2 3", "3 4"))
	result = tk.MustQuery("select count(*) from t")
	result.Check(testkit.Rows("7"))
	result = tk.MustQuery("select count(distinct d) from t")
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery("select count(*) as a from t group by d having sum(c) > 3 order by a")
	result.Check(testkit.Rows("2", "2"))
	result = tk.MustQuery("select max(c) from t group by d having sum(c) > 3 order by avg(c) desc")
	result.Check(testkit.Rows("4", "3"))
	result = tk.MustQuery("select sum(-1) from t a left outer join t b on not null is null")
	result.Check(testkit.Rows("-7"))
	result = tk.MustQuery("select count(*), b.d from t a left join t b on a.c = b.d group by b.d order by b.d")
	result.Check(testkit.Rows("2 <nil>", "12 1", "2 3"))
	result = tk.MustQuery("select count(b.d), b.d from t a left join t b on a.c = b.d group by b.d order by b.d")
	result.Check(testkit.Rows("0 <nil>", "12 1", "2 3"))
	result = tk.MustQuery("select count(b.d), b.d from t b right join t a on a.c = b.d group by b.d order by b.d")
	result.Check(testkit.Rows("0 <nil>", "12 1", "2 3"))
	result = tk.MustQuery("select count(*), b.d from t b right join t a on a.c = b.d group by b.d order by b.d")
	result.Check(testkit.Rows("2 <nil>", "12 1", "2 3"))
	result = tk.MustQuery("select max(case when b.d is null then 10 else b.c end), b.d from t b right join t a on a.c = b.d group by b.d order by b.d")
	result.Check(testkit.Rows("10 <nil>", "1 1", "4 3"))
	result = tk.MustQuery("select count(*) from t a , t b")
	result.Check(testkit.Rows("49"))
	result = tk.MustQuery("select count(*) from t a , t b, t c")
	result.Check(testkit.Rows("343"))
	result = tk.MustQuery("select count(*) from t a , t b where a.c = b.d")
	result.Check(testkit.Rows("14"))
	result = tk.MustQuery("select count(a.d), sum(b.c) from t a , t b where a.c = b.d order by a.d")
	result.Check(testkit.Rows("14 13"))
	result = tk.MustQuery("select count(*) from t a , t b, t c where a.c = b.d and b.d = c.d")
	result.Check(testkit.Rows("40"))
	result = tk.MustQuery("select count(*), a.c from t a , t b, t c where a.c = b.d and b.d = c.d group by c.d order by a.c")
	result.Check(testkit.Rows("36 1", "4 3"))
	result = tk.MustQuery("select count(a.c), c.d from t a , t b, t c where a.c = b.d and b.d = c.d group by c.d order by c.d")
	result.Check(testkit.Rows("36 1", "4 3"))
	result = tk.MustQuery("select count(*) from t a , t b where a.c = b.d and a.c + b.d = 2")
	result.Check(testkit.Rows("12"))
	result = tk.MustQuery("select count(*) from t a join t b having sum(a.c) < 0")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select count(*) from t a join t b where a.c < 0")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select sum(b.c), count(b.d), a.c from t a left join t b on a.c = b.d group by b.d order by b.d")
	result.Check(testkit.Rows("<nil> 0 <nil>", "8 12 1", "5 2 3"))
	// This two cases prove that having always resolve name from field list firstly.
	result = tk.MustQuery("select 1-d as d from t having d < 0 order by d desc")
	result.Check(testkit.Rows("-1", "-1", "-2", "-2"))
	result = tk.MustQuery("select 1-d as d from t having d + 1 < 0 order by d + 1")
	result.Check(testkit.Rows("-2", "-2"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (keywords varchar(20), type int)")
	tk.MustExec("insert into t values('测试', 1), ('test', 2)")
	result = tk.MustQuery("select group_concat(keywords) from t group by type order by type")
	result.Check(testkit.Rows("测试", "test"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	tk.MustExec("insert t values (1, -1)")
	tk.MustExec("insert t values (1, 0)")
	tk.MustExec("insert t values (1, 1)")
	result = tk.MustQuery("select d, d*d as d from t having d = -1")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select d*d as d from t group by d having d = -1")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select d, 1-d as d, c as d from t order by d")
	result.Check(testkit.Rows("1 0 1", "0 1 1", "-1 2 1"))
	result = tk.MustQuery("select d, 1-d as d, c as d from t order by d+1")
	result.Check(testkit.Rows("-1 2 1", "0 1 1", "1 0 1"))
	result = tk.MustQuery("select d, 1-d as d, c as d from t group by d order by d")
	result.Check(testkit.Rows("1 0 1", "0 1 1", "-1 2 1"))
	result = tk.MustQuery("select d as d1, t.d as d1, 1-d as d1, c as d1 from t having d1 < 10 order by d")
	result.Check(testkit.Rows("-1 -1 2 1", "0 0 1 1", "1 1 0 1"))
	result = tk.MustQuery("select d*d as d1, c as d1 from t group by d1 order by d1")
	result.Check(testkit.Rows("0 1", "1 1"))
	result = tk.MustQuery("select d*d as d1, c as d1 from t group by 2")
	result.Check(testkit.Rows("1 1"))
	result = tk.MustQuery("select * from t group by 2 order by d")
	result.Check(testkit.Rows("1 -1", "1 0", "1 1"))
	result = tk.MustQuery("select * , sum(d) from t group by 1 order by d")
	result.Check(testkit.Rows("1 -1 0"))
	result = tk.MustQuery("select sum(d), t.* from t group by 2 order by d")
	result.Check(testkit.Rows("0 1 -1"))
	result = tk.MustQuery("select d as d, c as d from t group by d + 1 order by t.d")
	result.Check(testkit.Rows("-1 1", "0 1", "1 1"))
	result = tk.MustQuery("select c as d, c as d from t group by d order by d")
	result.Check(testkit.Rows("1 1", "1 1", "1 1"))
	err = tk.ExecToErr("select d as d, c as d from t group by d")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("select t.d, c as d from t group by d")
	c.Assert(err, NotNil)
	result = tk.MustQuery("select *, c+1 as d from t group by 3 order by d")
	result.Check(testkit.Rows("1 -1 2"))
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a float, b int default 3)")
	tk.MustExec("insert into t1 (a) values (2), (11), (8)")
	result = tk.MustQuery("select min(a), min(case when 1=1 then a else NULL end), min(case when 1!=1 then NULL else a end) from t1 where b=3 group by b")
	result.Check(testkit.Rows("2 2 2"))
	// The following cases use streamed aggregation.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, index(a))")
	tk.MustExec("insert into t1 (a) values (1),(2),(3),(4),(5)")
	result = tk.MustQuery("select count(a) from t1 where a < 3")
	result.Check(testkit.Rows("2"))
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b int, index(a))")
	result = tk.MustQuery("select sum(b) from (select * from t1) t group by a")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select sum(b) from (select * from t1) t")
	result.Check(testkit.Rows("<nil>"))
	tk.MustExec("insert into t1 (a, b) values (1, 1),(2, 2),(3, 3),(1, 4),(3, 5)")
	result = tk.MustQuery("select avg(b) from (select * from t1) t group by a order by a")
	result.Check(testkit.Rows("2.5000", "2.0000", "4.0000"))
	result = tk.MustQuery("select sum(b) from (select * from t1) t group by a order by a")
	result.Check(testkit.Rows("5", "2", "8"))
	result = tk.MustQuery("select count(b) from (select * from t1) t group by a order by a")
	result.Check(testkit.Rows("2", "1", "2"))
	result = tk.MustQuery("select max(b) from (select * from t1) t group by a order by a")
	result.Check(testkit.Rows("4", "2", "5"))
	result = tk.MustQuery("select min(b) from (select * from t1) t group by a order by a")
	result.Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b int, index(a,b))")
	tk.MustExec("insert into t1 (a, b) values (1, 1),(2, 2),(3, 3),(1, 4), (1,1),(3, 5), (2,2), (3,5), (3,3)")
	result = tk.MustQuery("select avg(distinct b) from (select * from t1) t group by a order by a")
	result.Check(testkit.Rows("2.5000", "2.0000", "4.0000"))
	result = tk.MustQuery("select sum(distinct b) from (select * from t1) t group by a order by a")
	result.Check(testkit.Rows("5", "2", "8"))
	result = tk.MustQuery("select count(distinct b) from (select * from t1) t group by a order by a")
	result.Check(testkit.Rows("2", "1", "2"))
	result = tk.MustQuery("select max(distinct b) from (select * from t1) t group by a order by a")
	result.Check(testkit.Rows("4", "2", "5"))
	result = tk.MustQuery("select min(distinct b) from (select * from t1) t group by a order by a")
	result.Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b int, index(b, a))")
	tk.MustExec("insert into t1 (a, b) values (1, 1),(2, 2),(3, 3),(1, 4), (1,1),(3, 5), (2,2), (3,5), (3,3)")
	result = tk.MustQuery("select avg(distinct b) from (select * from t1) t group by a order by a")
	result.Check(testkit.Rows("2.5000", "2.0000", "4.0000"))
	result = tk.MustQuery("select sum(distinct b) from (select * from t1) t group by a order by a")
	result.Check(testkit.Rows("5", "2", "8"))
	result = tk.MustQuery("select count(distinct b) from (select * from t1) t group by a order by a")
	result.Check(testkit.Rows("2", "1", "2"))
	result = tk.MustQuery("select max(distinct b) from (select * from t1) t group by a order by a")
	result.Check(testkit.Rows("4", "2", "5"))
	result = tk.MustQuery("select min(distinct b) from (select * from t1) t group by a order by a")
	result.Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, ds date)")
	tk.MustExec("insert into t (id, ds) values (1, \"1991-09-05\"),(2,\"1991-09-05\"), (3, \"1991-09-06\"),(0,\"1991-09-06\")")
	result = tk.MustQuery("select sum(id), ds from t group by ds order by id")
	result.Check(testkit.Rows("3 1991-09-06", "3 1991-09-05"))
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (col0 int, col1 int)")
	tk.MustExec("create table t2 (col0 int, col1 int)")
	tk.MustExec("insert into t1 values(83, 0), (26, 0), (43, 81)")
	tk.MustExec("insert into t2 values(22, 2), (3, 12), (38, 98)")
	result = tk.MustQuery("SELECT COALESCE ( + 1, cor0.col0 ) + - CAST( NULL AS DECIMAL ) FROM t2, t1 AS cor0, t2 AS cor1 GROUP BY cor0.col1")
	result.Check(testkit.Rows("<nil>", "<nil>"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (c1 int)")
	tk.MustExec("create table t2 (c1 int)")
	tk.MustExec("insert into t1 values(3), (2)")
	tk.MustExec("insert into t2 values(1), (2)")
	tk.MustExec("set @@session.tidb_opt_insubq_to_join_and_agg = 0")
	result = tk.MustQuery("select sum(c1 in (select * from t2)) from t1")
	result.Check(testkit.Rows("1"))
	tk.MustExec("set @@session.tidb_opt_insubq_to_join_and_agg = 1")
	result = tk.MustQuery("select sum(c1 in (select * from t2)) from t1")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select sum(c1) k from (select * from t1 union all select * from t2)t group by c1 * 2 order by k")
	result.Check(testkit.Rows("1", "3", "4"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert into t values(1, 2, 3), (1, 2, 4)")
	result = tk.MustQuery("select count(distinct c), count(distinct a,b) from t")
	result.Check(testkit.Rows("2 1"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a float)")
	tk.MustExec("insert into t values(966.36), (363.97), (569.99), (453.33), (376.45), (321.93), (12.12), (45.77), (9.66), (612.17)")
	result = tk.MustQuery("select distinct count(distinct a) from t")
	result.Check(testkit.Rows("10"))

	tk.MustExec("create table idx_agg (a int, b int, index (b))")
	tk.MustExec("insert idx_agg values (1, 1), (1, 2), (2, 2)")
	tk.MustQuery("select sum(a), sum(b) from idx_agg where b > 0 and b < 10").Check(testkit.Rows("4 5"))

	// test without any aggregate function
	tk.MustQuery("select 10 from idx_agg group by b").Check(testkit.Rows("10", "10"))
	tk.MustQuery("select 11 from idx_agg group by a").Check(testkit.Rows("11", "11"))

	tk.MustExec("set @@tidb_init_chunk_size=1;")
	tk.MustQuery("select group_concat(b) from idx_agg group by b;").Sort().Check(testkit.Rows("1", "2,2"))
	tk.MustExec("set @@tidb_init_chunk_size=2;")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int(11), b decimal(15,2))")
	tk.MustExec("insert into t values(1,771.64),(2,378.49),(3,920.92),(4,113.97)")
	tk.MustQuery("select a, max(b) from t group by a order by a limit 2").Check(testkit.Rows("1 771.64", "2 378.49"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int(11), b char(15))")
	tk.MustExec("insert into t values(1,771.64),(2,378.49),(3,920.92),(4,113.97)")
	tk.MustQuery("select a, max(b) from t group by a order by a limit 2").Check(testkit.Rows("1 771.64", "2 378.49"))

	// for issue #6014
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int(11) NOT NULL, tags json DEFAULT NULL)")
	tk.MustExec(`insert into t values (1, '{"i": 1, "n": "n1"}')`)
	tk.MustExec(`insert into t values (2, '{"i": 2, "n": "n2"}')`)
	tk.MustExec(`insert into t values (3, '{"i": 3, "n": "n3"}')`)
	tk.MustExec(`insert into t values (4, '{"i": 4, "n": "n4"}')`)
	tk.MustExec(`insert into t values (5, '{"i": 5, "n": "n5"}')`)
	tk.MustExec(`insert into t values (6, '{"i": 0, "n": "n6"}')`)
	tk.MustExec(`insert into t values (7, '{"i": -1, "n": "n7"}')`)
	tk.MustQuery("select sum(tags->'$.i') from t").Check(testkit.Rows("14"))

	// test agg with empty input
	result = tk.MustQuery("select id, count(95), sum(95), avg(95), bit_or(95), bit_and(95), bit_or(95), max(95), min(95), group_concat(95) from t where null")
	result.Check(testkit.Rows("<nil> 0 <nil> <nil> 0 18446744073709551615 0 <nil> <nil> <nil>"))
	tk.MustExec("truncate table t")
	tk.MustExec("create table s(id int)")
	result = tk.MustQuery("select t.id, count(95), sum(95), avg(95), bit_or(95), bit_and(95), bit_or(95), max(95), min(95), group_concat(95) from t left join s on t.id = s.id")
	result.Check(testkit.Rows("<nil> 0 <nil> <nil> 0 18446744073709551615 0 <nil> <nil> <nil>"))
	tk.MustExec(`insert into t values (1, '{"i": 1, "n": "n1"}')`)
	result = tk.MustQuery("select t.id, count(95), sum(95), avg(95), bit_or(95), bit_and(95), bit_or(95), max(95), min(95), group_concat(95) from t left join s on t.id = s.id")
	result.Check(testkit.Rows("1 1 95 95.0000 95 95 95 95 95 95"))
	tk.MustExec("set @@tidb_hash_join_concurrency=5")

	// test agg bit col
	tk.MustExec("drop table t")
	tk.MustExec("CREATE TABLE `t` (`a` bit(1) NOT NULL, PRIMARY KEY (`a`))")
	tk.MustExec("insert into t value(1), (0)")
	tk.MustQuery("select a from t group by 1")
	// This result is compatible with MySQL, the readable result is shown in the next case.
	result = tk.MustQuery("select max(a) from t group by a order by a")
	result.Check(testkit.Rows(string([]byte{0x0}), string([]byte{0x1})))
	result = tk.MustQuery("select cast(a as signed) as idx, cast(max(a) as signed),  cast(min(a) as signed) from t group by 1 order by idx")
	result.Check(testkit.Rows("0 0 0", "1 1 1"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t value(null, null)")
	tk.MustQuery("select group_concat(a), group_concat(distinct a) from t").Check(testkit.Rows("<nil> <nil>"))
	tk.MustExec("insert into t value(1, null), (null, 1), (1, 2), (3, 4)")
	tk.MustQuery("select group_concat(a, b), group_concat(distinct a,b) from t").Check(testkit.Rows("12,34 12,34"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a decimal(10, 4))")
	tk.MustQuery("select 10 from t group by a").Check(testkit.Rows())
	tk.MustExec("insert into t value(0), (-0.9871), (-0.9871)")
	tk.MustQuery("select 10 from t group by a").Check(testkit.Rows("10", "10"))
	tk.MustQuery("select sum(a) from (select a from t union all select a from t) tmp").Check(testkit.Rows("-3.9484"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a tinyint, b smallint, c mediumint, d int, e bigint, f float, g double, h decimal)")
	tk.MustExec("insert into t values(1, 2, 3, 4, 5, 6.1, 7.2, 8.3), (1, 3, 4, 5, 6, 7.1, 8.2, 9.3)")
	result = tk.MustQuery("select var_pop(b), var_pop(c), var_pop(d), var_pop(e), var_pop(f), var_pop(g), var_pop(h) from t group by a")
	result.Check(testkit.Rows("0.25 0.25 0.25 0.25 0.25 0.25 0.25"))

	tk.MustExec("insert into t values(2, 3, 4, 5, 6, 7.2, 8.3, 9)")
	result = tk.MustQuery("select a, var_pop(b) over w, var_pop(c) over w from t window w as (partition by a)").Sort()
	result.Check(testkit.Rows("1 0.25 0.25", "1 0.25 0.25", "2 0 0"))

	tk.MustExec("delete from t where t.a = 2")
	tk.MustExec("insert into t values(1, 2, 4, 5, 6, 6.1, 7.2, 9)")
	result = tk.MustQuery("select a, var_pop(distinct b), var_pop(distinct c), var_pop(distinct d), var_pop(distinct e), var_pop(distinct f), var_pop(distinct g), var_pop(distinct h) from t group by a")
	result.Check(testkit.Rows("1 0.25 0.25 0.25 0.25 0.25 0.25 0.25"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int, b bigint, c float, d double, e decimal)")
	tk.MustExec("insert into t values(1, 1000, 6.8, 3.45, 8.3), (1, 3998, -3.4, 5.12, 9.3),(1, 288, 9.2, 6.08, 1)")
	result = tk.MustQuery("select variance(b), variance(c), variance(d), variance(e) from t group by a")
	result.Check(testkit.Rows("2584338.6666666665 29.840000178019228 1.1808222222222229 12.666666666666666"))

	tk.MustExec("insert into t values(1, 255, 6.8, 6.08, 1)")
	result = tk.MustQuery("select variance(distinct b), variance(distinct c), variance(distinct d), variance(distinct e) from t group by a")
	result.Check(testkit.Rows("2364075.6875 29.840000178019228 1.1808222222222229 12.666666666666666"))

	tk.MustExec("insert into t values(2, 322, 0.8, 2.22, 6)")
	result = tk.MustQuery("select a, variance(b) over w from t window w as (partition by a)").Sort()
	result.Check(testkit.Rows("1 2364075.6875", "1 2364075.6875", "1 2364075.6875", "1 2364075.6875", "2 0"))

	_, err = tk.Exec("select std(a) from t")
	c.Assert(errors.Cause(err).Error(), Equals, "unsupported agg function: std")
	_, err = tk.Exec("select stddev(a) from t")
	c.Assert(errors.Cause(err).Error(), Equals, "unsupported agg function: stddev")
	_, err = tk.Exec("select stddev_pop(a) from t")
	c.Assert(errors.Cause(err).Error(), Equals, "unsupported agg function: stddev_pop")
	_, err = tk.Exec("select std_samp(a) from t")
	// TODO: Fix this error message.
	c.Assert(errors.Cause(err).Error(), Equals, "[expression:1305]FUNCTION test.std_samp does not exist")
	_, err = tk.Exec("select var_samp(a) from t")
	c.Assert(errors.Cause(err).Error(), Equals, "unsupported agg function: var_samp")

	// For issue #14072: wrong result when using generated column with aggregate statement
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int, b int generated always as (-a) virtual, c int generated always as (-a) stored);")
	tk.MustExec("insert into t1 (a) values (2), (1), (1), (3), (NULL);")
	tk.MustQuery("select sum(a) from t1 group by b order by b;").Check(testkit.Rows("<nil>", "3", "2", "2"))
	tk.MustQuery("select sum(a) from t1 group by c order by c;").Check(testkit.Rows("<nil>", "3", "2", "2"))
	tk.MustQuery("select sum(b) from t1 group by a order by a;").Check(testkit.Rows("<nil>", "-2", "-2", "-3"))
	tk.MustQuery("select sum(b) from t1 group by c order by c;").Check(testkit.Rows("<nil>", "-3", "-2", "-2"))
	tk.MustQuery("select sum(c) from t1 group by a order by a;").Check(testkit.Rows("<nil>", "-2", "-2", "-3"))
	tk.MustQuery("select sum(c) from t1 group by b order by b;").Check(testkit.Rows("<nil>", "-3", "-2", "-2"))
}

func (s *testSuiteAgg) TestAggPrune(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, b varchar(50), c int)")
	tk.MustExec("insert into t values(1, '1ff', NULL), (2, '234.02', 1)")
	tk.MustQuery("select id, sum(b) from t group by id").Check(testkit.Rows("1 1", "2 234.02"))
	tk.MustQuery("select sum(b) from t").Check(testkit.Rows("235.02"))
	tk.MustQuery("select id, count(c) from t group by id").Check(testkit.Rows("1 0", "2 1"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, b float, c float)")
	tk.MustExec("insert into t values(1, 1, 3), (2, 1, 6)")
	tk.MustQuery("select sum(b/c) from t group by id").Check(testkit.Rows("0.3333333333333333", "0.16666666666666666"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, b float, c float, d float)")
	tk.MustExec("insert into t values(1, 1, 3, NULL), (2, 1, NULL, 6), (3, NULL, 1, 2), (4, NULL, NULL, 1), (5, NULL, 2, NULL), (6, 3, NULL, NULL), (7, NULL, NULL, NULL), (8, 1, 2 ,3)")
	tk.MustQuery("select count(distinct b, c, d) from t group by id").Check(testkit.Rows("0", "0", "0", "0", "0", "0", "0", "1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b varchar(10))")
	tk.MustExec("insert into t value(1, 11),(3, NULL)")
	tk.MustQuery("SELECT a, MIN(b), MAX(b) FROM t GROUP BY a").Check(testkit.Rows("1 11 11", "3 <nil> <nil>"))
}

func (s *testSuiteAgg) TestGroupConcatAggr(c *C) {
	// issue #5411
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table test(id int, name int)")
	tk.MustExec("insert into test values(1, 10);")
	tk.MustExec("insert into test values(1, 20);")
	tk.MustExec("insert into test values(1, 30);")
	tk.MustExec("insert into test values(2, 20);")
	tk.MustExec("insert into test values(3, 200);")
	tk.MustExec("insert into test values(3, 500);")
	result := tk.MustQuery("select id, group_concat(name) from test group by id order by id")
	result.Check(testkit.Rows("1 10,20,30", "2 20", "3 200,500"))

	result = tk.MustQuery("select id, group_concat(name SEPARATOR ';') from test group by id order by id")
	result.Check(testkit.Rows("1 10;20;30", "2 20", "3 200;500"))

	result = tk.MustQuery("select id, group_concat(name SEPARATOR ',') from test group by id order by id")
	result.Check(testkit.Rows("1 10,20,30", "2 20", "3 200,500"))

	result = tk.MustQuery(`select id, group_concat(name SEPARATOR '%') from test group by id order by id`)
	result.Check(testkit.Rows("1 10%20%30", "2 20", `3 200%500`))

	result = tk.MustQuery("select id, group_concat(name SEPARATOR '') from test group by id order by id")
	result.Check(testkit.Rows("1 102030", "2 20", "3 200500"))

	result = tk.MustQuery("select id, group_concat(name SEPARATOR '123') from test group by id order by id")
	result.Check(testkit.Rows("1 101232012330", "2 20", "3 200123500"))

	// issue #9920
	tk.MustQuery("select group_concat(123, null)").Check(testkit.Rows("<nil>"))
}

func (s *testSuiteAgg) TestSelectDistinct(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillData(tk, "select_distinct_test")

	tk.MustExec("begin")
	r := tk.MustQuery("select distinct name from select_distinct_test;")
	r.Check(testkit.Rows("hello"))
	tk.MustExec("commit")

}

func (s *testSuiteAgg) TestAggPushDown(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("alter table t add index idx(a, b, c)")
	// test for empty table
	tk.MustQuery("select count(a) from t group by a;").Check(testkit.Rows())
	tk.MustQuery("select count(a) from t;").Check(testkit.Rows("0"))
	// test for one row
	tk.MustExec("insert t values(0,0,0)")
	tk.MustQuery("select distinct b from t").Check(testkit.Rows("0"))
	tk.MustQuery("select count(b) from t group by a;").Check(testkit.Rows("1"))
	// test for rows
	tk.MustExec("insert t values(1,1,1),(3,3,6),(3,2,5),(2,1,4),(1,1,3),(1,1,2);")
	tk.MustQuery("select count(a) from t where b>0 group by a, b;").Sort().Check(testkit.Rows("1", "1", "1", "3"))
	tk.MustQuery("select count(a) from t where b>0 group by a, b order by a;").Check(testkit.Rows("3", "1", "1", "1"))
	tk.MustQuery("select count(a) from t where b>0 group by a, b order by a limit 1;").Check(testkit.Rows("3"))

	tk.MustExec("drop table if exists t, tt")
	tk.MustExec("create table t(a int primary key, b int, c int)")
	tk.MustExec("create table tt(a int primary key, b int, c int)")
	tk.MustExec("insert into t values(1, 1, 1), (2, 1, 1)")
	tk.MustExec("insert into tt values(1, 2, 1)")
	tk.MustQuery("select max(a.b), max(b.b) from t a join tt b on a.a = b.a group by a.c").Check(testkit.Rows("1 2"))
	tk.MustQuery("select a, count(b) from (select * from t union all select * from tt) k group by a order by a").Check(testkit.Rows("1 2", "2 1"))
}

func (s *testSuiteAgg) TestOnlyFullGroupBy(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode = 'ONLY_FULL_GROUP_BY'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null primary key, b int not null, c int default null, d int not null, unique key I_b_c (b,c), unique key I_b_d (b,d))")
	tk.MustExec("create table x(a int not null primary key, b int not null, c int default null, d int not null, unique key I_b_c (b,c), unique key I_b_d (b,d))")

	// test AggregateFunc
	tk.MustQuery("select max(a) from t group by d")
	// for issue #8161: enable `any_value` in aggregation if `ONLY_FULL_GROUP_BY` is set
	tk.MustQuery("select max(a), any_value(c) from t group by d;")
	// test incompatible with sql_mode = ONLY_FULL_GROUP_BY
	err := tk.ExecToErr("select * from t group by d")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select b-c from t group by b+c")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select (b-c)*(b+c), min(a) from t group by b+c, b-c")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select b between c and d from t group by b,c")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select case b when 1 then c when 2 then d else d end from t group by b,c")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select c > (select b from t) from t group by b")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select c is null from t group by b")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select c is true from t group by b")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select (c+b)*d from t group by c,d")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select b in (c,d) from t group by b,c")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select b like '%a' from t group by c")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select c REGEXP '1.*' from t group by b")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select -b from t group by c")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select a, max(b) from t")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrMixOfGroupFuncAndFields), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select sum(a)+b from t")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrMixOfGroupFuncAndFields), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select count(b), c from t")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrMixOfGroupFuncAndFields), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select distinct a, b, count(a) from t")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrMixOfGroupFuncAndFields), IsTrue, Commentf("err %v", err))
	// test compatible with sql_mode = ONLY_FULL_GROUP_BY
	tk.MustQuery("select a from t group by a,b,c")
	tk.MustQuery("select b from t group by b")
	tk.MustQuery("select b as e from t group by b")
	tk.MustQuery("select b+c from t group by b+c")
	tk.MustQuery("select b+c, min(a) from t group by b+c, b-c")
	tk.MustQuery("select b+c, min(a) from t group by b, c")
	tk.MustQuery("select b+c from t group by b,c")
	tk.MustQuery("select b between c and d from t group by b,c,d")
	tk.MustQuery("select case b when 1 then c when 2 then d else d end from t group by b,c,d")
	tk.MustQuery("select c > (select b from t) from t group by c")
	tk.MustQuery("select exists (select * from t) from t group by d;")
	tk.MustQuery("select c is null from t group by c")
	tk.MustQuery("select c is true from t group by c")
	tk.MustQuery("select (c+b)*d from t group by c,b,d")
	tk.MustQuery("select b in (c,d) from t group by b,c,d")
	tk.MustQuery("select b like '%a' from t group by b")
	tk.MustQuery("select c REGEXP '1.*' from t group by c")
	tk.MustQuery("select -b from t group by b")
	tk.MustQuery("select max(a+b) from t")
	tk.MustQuery("select avg(a)+1 from t")
	tk.MustQuery("select count(c), 5 from t")
	// test functinal depend on primary key
	tk.MustQuery("select * from t group by a")
	// test functional depend on unique not null columns
	tk.MustQuery("select * from t group by b,d")
	// test functional depend on a unique null column
	err = tk.ExecToErr("select * from t group by b,c")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	// test functional dependency derived from keys in where condition
	tk.MustQuery("select * from t where c = d group by b, c")
	tk.MustQuery("select t.*, x.* from t, x where t.a = x.a group by t.a")
	tk.MustQuery("select t.*, x.* from t, x where t.b = x.b and t.d = x.d group by t.b, t.d")
	tk.MustQuery("select t.*, x.* from t, x where t.b = x.a group by t.b, t.d")
	tk.MustQuery("select t.b, x.* from t, x where t.b = x.a group by t.b")
	err = tk.ExecToErr("select t.*, x.* from t, x where t.c = x.a group by t.b, t.c")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	// test functional dependency derived from keys in join
	tk.MustQuery("select t.*, x.* from t inner join x on t.a = x.a group by t.a")
	tk.MustQuery("select t.*, x.* from t inner join x  on (t.b = x.b and t.d = x.d) group by t.b, x.d")
	tk.MustQuery("select t.b, x.* from t inner join x on t.b = x.b group by t.b, x.d")
	tk.MustQuery("select t.b, x.* from t left join x on t.b = x.b group by t.b, x.d")
	tk.MustQuery("select t.b, x.* from t left join x on x.b = t.b group by t.b, x.d")
	tk.MustQuery("select x.b, t.* from t right join x on x.b = t.b group by x.b, t.d")
	tk.MustQuery("select x.b, t.* from t right join x on t.b = x.b group by x.b, t.d")
	err = tk.ExecToErr("select t.b, x.* from t right join x on t.b = x.b group by t.b, x.d")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	err = tk.ExecToErr("select t.b, x.* from t right join x on t.b = x.b group by t.b, x.d")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))

	// FixMe: test functional dependency of derived table
	//tk.MustQuery("select * from (select * from t) as e group by a")
	//tk.MustQuery("select * from (select * from t) as e group by b,d")
	//err = tk.ExecToErr("select * from (select * from t) as e group by b,c")
	//c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue)

	// test order by
	tk.MustQuery("select c from t group by c,d order by d")
	err = tk.ExecToErr("select c from t group by c order by d")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrFieldNotInGroupBy), IsTrue, Commentf("err %v", err))
	// test ambiguous column
	err = tk.ExecToErr("select c from t,x group by t.c")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrAmbiguous), IsTrue, Commentf("err %v", err))
}

func (s *testSuiteAgg) TestIssue13652(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode = 'ONLY_FULL_GROUP_BY'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a real)")
	tk.MustQuery("select a from t group by (a)")
	tk.MustQuery("select a from t group by ((a))")
	tk.MustQuery("select a from t group by +a")
	tk.MustQuery("select a from t group by ((+a))")
	_, err := tk.Exec("select a from t group by (-a)")
	c.Assert(err.Error(), Equals, "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.a' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
}

func (s *testSuiteAgg) TestIssue14947(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode = 'ONLY_FULL_GROUP_BY'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustQuery("select ((+a+1)) as tmp from t group by tmp")
}

func (s *testSuiteAgg) TestHaving(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int, c2 int, c3 int)")
	tk.MustExec("insert into t values (1,2,3), (2, 3, 1), (3, 1, 2)")

	tk.MustQuery("select c1 as c2, c3 from t having c2 = 2").Check(testkit.Rows("2 1"))
	tk.MustQuery("select c1 as c2, c3 from t group by c2 having c2 = 2;").Check(testkit.Rows("1 3"))
	tk.MustQuery("select c1 as c2, c3 from t group by c2 having sum(c2) = 2;").Check(testkit.Rows("1 3"))
	tk.MustQuery("select c1 as c2, c3 from t group by c3 having sum(c2) = 2;").Check(testkit.Rows("1 3"))
	tk.MustQuery("select c1 as c2, c3 from t group by c3 having sum(0) + c2 = 2;").Check(testkit.Rows("2 1"))
	tk.MustQuery("select c1 as a from t having c1 = 1;").Check(testkit.Rows("1"))
	tk.MustQuery("select t.c1 from t having c1 = 1;").Check(testkit.Rows("1"))
	tk.MustQuery("select a.c1 from t as a having c1 = 1;").Check(testkit.Rows("1"))
	tk.MustQuery("select c1 as a from t group by c3 having sum(a) = 1;").Check(testkit.Rows("1"))
	tk.MustQuery("select c1 as a from t group by c3 having sum(a) + a = 2;").Check(testkit.Rows("1"))
	tk.MustQuery("select a.c1 as c, a.c1 as d from t as a, t as b having c1 = 1 limit 1;").Check(testkit.Rows("1 1"))

	tk.MustQuery("select sum(c1) as s from t group by c1 having sum(c1) order by s").Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select sum(c1) - 1 as s from t group by c1 having sum(c1) - 1 order by s").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select 1 from t group by c1 having sum(abs(c2 + c3)) = c1").Check(testkit.Rows("1"))
}

func (s *testSuiteAgg) TestAggEliminator(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("create table t(a int primary key, b int)")
	tk.MustQuery("select min(a), min(a) from t").Check(testkit.Rows("<nil> <nil>"))
	tk.MustExec("insert into t values(1, -1), (2, -2), (3, 1), (4, NULL)")
	tk.MustQuery("select max(a) from t").Check(testkit.Rows("4"))
	tk.MustQuery("select min(b) from t").Check(testkit.Rows("-2"))
	tk.MustQuery("select max(b*b) from t").Check(testkit.Rows("4"))
	tk.MustQuery("select min(b*b) from t").Check(testkit.Rows("1"))
	tk.MustQuery("select group_concat(b, b) from t group by a").Sort().Check(testkit.Rows("-1-1", "-2-2", "11", "<nil>"))
}

func (s *testSuiteAgg) TestMaxMinFloatScalaFunc(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec(`DROP TABLE IF EXISTS T;`)
	tk.MustExec(`CREATE TABLE T(A VARCHAR(10), B VARCHAR(10), C FLOAT);`)
	tk.MustExec(`INSERT INTO T VALUES('0', "val_b", 12.191);`)
	tk.MustQuery(`SELECT MAX(CASE B WHEN 'val_b'  THEN C ELSE 0 END) val_b FROM T WHERE cast(A as signed) = 0 GROUP BY a;`).Check(testkit.Rows("12.190999984741211"))
	tk.MustQuery(`SELECT MIN(CASE B WHEN 'val_b'  THEN C ELSE 0 END) val_b FROM T WHERE cast(A as signed) = 0 GROUP BY a;`).Check(testkit.Rows("12.190999984741211"))
}

func (s *testSuiteAgg) TestBuildProjBelowAgg(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (i int);")
	tk.MustExec("insert into t values (1), (1), (1),(2),(3),(2),(3),(2),(3);")
	rs := tk.MustQuery("select i+1 as a, count(i+2), sum(i+3), group_concat(i+4), bit_or(i+5) from t group by i, hex(i+6) order by a")
	rs.Check(testkit.Rows(
		"2 3 12 5,5,5 6",
		"3 3 15 6,6,6 7",
		"4 3 18 7,7,7 8"))
}

func (s *testSuiteAgg) TestInjectProjBelowTopN(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (i int);")
	tk.MustExec("insert into t values (1), (1), (1),(2),(3),(2),(3),(2),(3);")
	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i]...))
	}
}

func (s *testSuiteAgg) TestFirstRowEnum(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a enum('a', 'b'));`)
	tk.MustExec(`insert into t values('a');`)
	tk.MustQuery(`select a from t group by a;`).Check(testkit.Rows(
		`a`,
	))
}

func (s *testSuiteAgg) TestAggJSON(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a datetime, b json, index idx(a));`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:00', '["a", "b", 1]');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:01', '["a", "b", 1]');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:02', '["a", "b", 1]');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:03', '{"k1": "value", "k2": [10, 20]}');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:04', '{"k1": "value", "k2": [10, 20]}');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:05', '{"k1": "value", "k2": [10, 20]}');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:06', '"hello"');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:07', '"hello"');`)
	tk.MustExec(`insert into t values('2019-03-20 21:50:08', '"hello"');`)
	tk.MustExec(`set @@sql_mode='';`)
	tk.MustQuery(`select b from t group by a order by a;`).Check(testkit.Rows(
		`["a", "b", 1]`,
		`["a", "b", 1]`,
		`["a", "b", 1]`,
		`{"k1": "value", "k2": [10, 20]}`,
		`{"k1": "value", "k2": [10, 20]}`,
		`{"k1": "value", "k2": [10, 20]}`,
		`"hello"`,
		`"hello"`,
		`"hello"`,
	))
	tk.MustQuery(`select min(b) from t group by a order by a;`).Check(testkit.Rows(
		`["a", "b", 1]`,
		`["a", "b", 1]`,
		`["a", "b", 1]`,
		`{"k1": "value", "k2": [10, 20]}`,
		`{"k1": "value", "k2": [10, 20]}`,
		`{"k1": "value", "k2": [10, 20]}`,
		`"hello"`,
		`"hello"`,
		`"hello"`,
	))
	tk.MustQuery(`select max(b) from t group by a order by a;`).Check(testkit.Rows(
		`["a", "b", 1]`,
		`["a", "b", 1]`,
		`["a", "b", 1]`,
		`{"k1": "value", "k2": [10, 20]}`,
		`{"k1": "value", "k2": [10, 20]}`,
		`{"k1": "value", "k2": [10, 20]}`,
		`"hello"`,
		`"hello"`,
		`"hello"`,
	))
}

func (s *testSuiteAgg) TestIssue10099(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b char(10))")
	tk.MustExec("insert into t values('1', '222'), ('12', '22')")
	tk.MustQuery("select count(distinct a, b) from t").Check(testkit.Rows("2"))
}

func (s *testSuiteAgg) TestIssue10098(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec("create table t(a char(10), b char(10))")
	tk.MustExec("insert into t values('1', '222'), ('12', '22')")
	tk.MustQuery("select group_concat(distinct a, b) from t").Check(testkit.Rows("1222,1222"))
}

func (s *testSuiteAgg) TestIssue10608(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`drop table if exists t, s;`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("create table s(a int, b int)")
	tk.MustExec("insert into s values(100292, 508931), (120002, 508932)")
	tk.MustExec("insert into t values(508931), (508932)")
	tk.MustQuery("select (select  /*+ stream_agg() */ group_concat(concat(123,'-')) from t where t.a = s.b group by t.a) as t from s;").Check(testkit.Rows("123-", "123-"))
	tk.MustQuery("select (select  /*+ hash_agg() */ group_concat(concat(123,'-')) from t where t.a = s.b group by t.a) as t from s;").Check(testkit.Rows("123-", "123-"))

}

func (s *testSuiteAgg) TestIssue12759HashAggCalledByApply(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.Se.GetSessionVars().HashAggFinalConcurrency = 4
	tk.MustExec(`insert into mysql.opt_rule_blacklist value("decorrelate");`)
	defer func() {
		tk.MustExec(`delete from mysql.opt_rule_blacklist where name = "decorrelate";`)
		tk.MustExec(`admin reload opt_rule_blacklist;`)
	}()
	tk.MustExec(`drop table if exists test;`)
	tk.MustExec("create table test (a int);")
	tk.MustExec("insert into test value(1);")
	tk.MustQuery("select /*+ hash_agg() */ sum(a), (select NULL from test where tt.a = test.a limit 1),(select NULL from test where tt.a = test.a limit 1),(select NULL from test where tt.a = test.a limit 1) from test tt;").Check(testkit.Rows("1 <nil> <nil> <nil>"))

	var (
		input  []string
		output [][]string
	)
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i] = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i]...))
	}
}

func (s *testSuiteAgg) TestPR15242ShallowCopy(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a json);`)
	tk.MustExec(`insert into t values ('{"id": 1,"score":23}');`)
	tk.MustExec(`insert into t values ('{"id": 2,"score":23}');`)
	tk.MustExec(`insert into t values ('{"id": 1,"score":233}');`)
	tk.MustExec(`insert into t values ('{"id": 2,"score":233}');`)
	tk.MustExec(`insert into t values ('{"id": 3,"score":233}');`)
	tk.Se.GetSessionVars().MaxChunkSize = 2
	tk.MustQuery(`select max(JSON_EXTRACT(a, '$.score')) as max_score,JSON_EXTRACT(a,'$.id') as id from t group by id order by id;`).Check(testkit.Rows("233 1", "233 2", "233 3"))

}
