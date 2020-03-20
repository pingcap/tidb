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
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/testkit"
)

type testSuiteJoin1 struct {
	*baseTestSuite
}

type testSuiteJoin2 struct {
	*baseTestSuite
}

type testSuiteJoin3 struct {
	*baseTestSuite
}

type testSuiteJoinSerial struct {
	*baseTestSuite
}

func (s *testSuiteJoin1) TestJoinPanic(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode = 'ONLY_FULL_GROUP_BY'")
	tk.MustExec("drop table if exists events")
	tk.MustExec("create table events (clock int, source int)")
	tk.MustQuery("SELECT * FROM events e JOIN (SELECT MAX(clock) AS clock FROM events e2 GROUP BY e2.source) e3 ON e3.clock=e.clock")
	err := tk.ExecToErr("SELECT * FROM events e JOIN (SELECT clock FROM events e2 GROUP BY e2.source) e3 ON e3.clock=e.clock")
	c.Check(err, NotNil)
}

func (s *testSuite) TestJoinInDisk(c *C) {
	originCfg := config.GetGlobalConfig()
	newConf := *originCfg
	newConf.OOMUseTmpStorage = true
	config.StoreGlobalConfig(&newConf)
	defer config.StoreGlobalConfig(originCfg)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	sm := &mockSessionManager1{
		PS: make([]*util.ProcessInfo, 0),
	}
	tk.Se.SetSessionManager(sm)
	s.domain.ExpensiveQueryHandle().SetSessionManager(sm)

	// TODO(fengliyuan): how to ensure that it is using disk really?
	tk.MustExec("set @@tidb_mem_quota_query=1;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int, c2 int)")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tk.MustExec("insert into t values(1,1),(2,2)")
	tk.MustExec("insert into t1 values(2,3),(4,4)")
	result := tk.MustQuery("select /*+ TIDB_HJ(t, t2) */ * from t, t1 where t.c1 = t1.c1")
	result.Check(testkit.Rows("2 2 2 3"))
}

func (s *testSuiteJoin2) TestJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("set @@tidb_index_lookup_join_concurrency = 200")
	c.Assert(tk.Se.GetSessionVars().IndexLookupJoinConcurrency, Equals, 200)

	tk.MustExec("set @@tidb_index_lookup_join_concurrency = 4")
	c.Assert(tk.Se.GetSessionVars().IndexLookupJoinConcurrency, Equals, 4)

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
	result = tk.MustQuery("select t.c1, t1.c1 from t left outer join t1 on t.c1 = t1.c1 and t.c2 + t1.c2 <= 5")
	result.Check(testkit.Rows("1 <nil>", "2 2"))

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
	tk.MustQuery("select /*+ INL_JOIN(t, t1) */ * from t join t1 on t.a=t1.a").Check(testkit.Rows("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, t1) */ * from t join t1 on t.a=t1.a").Check(testkit.Rows("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, t1) */ * from t join t1 on t.a=t1.a").Check(testkit.Rows("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4"))
	tk.MustQuery("select /*+ INL_JOIN(t) */ * from t1 join t on t.a=t1.a and t.a < t1.b").Check(testkit.Rows("1 2 1 1", "1 3 1 1", "1 4 1 1", "3 4 3 3"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t) */ * from t1 join t on t.a=t1.a and t.a < t1.b").Check(testkit.Rows("1 2 1 1", "1 3 1 1", "1 4 1 1", "3 4 3 3"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t) */ * from t1 join t on t.a=t1.a and t.a < t1.b").Check(testkit.Rows("1 2 1 1", "1 3 1 1", "1 4 1 1", "3 4 3 3"))
	// Test single index reader.
	tk.MustQuery("select /*+ INL_JOIN(t, t1) */ t1.b from t1 join t on t.b=t1.b").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, t1) */ t1.b from t1 join t on t.b=t1.b").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, t1) */ t1.b from t1 join t on t.b=t1.b").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select /*+ INL_JOIN(t1) */ * from t right outer join t1 on t.a=t1.a").Check(testkit.Rows("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4", "<nil> <nil> 4 5"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1) */ * from t right outer join t1 on t.a=t1.a").Check(testkit.Rows("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4", "<nil> <nil> 4 5"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1) */ * from t right outer join t1 on t.a=t1.a").Check(testkit.Rows("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4", "<nil> <nil> 4 5"))
	tk.MustQuery("select /*+ INL_JOIN(t) */ avg(t.b) from t right outer join t1 on t.a=t1.a").Check(testkit.Rows("1.5000"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t) */ avg(t.b) from t right outer join t1 on t.a=t1.a").Check(testkit.Rows("1.5000"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t) */ avg(t.b) from t right outer join t1 on t.a=t1.a").Check(testkit.Rows("1.5000"))

	// Test that two conflict hints will return warning.
	tk.MustExec("select /*+ TIDB_INLJ(t) TIDB_SMJ(t) */ * from t join t1 on t.a=t1.a")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)
	tk.MustExec("select /*+ TIDB_INLJ(t) TIDB_HJ(t) */ * from t join t1 on t.a=t1.a")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)
	tk.MustExec("select /*+ TIDB_SMJ(t) TIDB_HJ(t) */ * from t join t1 on t.a=t1.a")
	c.Assert(tk.Se.GetSessionVars().StmtCtx.GetWarnings(), HasLen, 1)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1),(2), (3)")
	tk.MustQuery("select @a := @a + 1 from t, (select @a := 0) b;").Check(testkit.Rows("1", "2", "3"))

	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t(a int primary key, b int, key s(b))")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("insert into t values(1, 3), (2, 2), (3, 1)")
	tk.MustExec("insert into t1 values(0, 0), (1, 2), (1, 3), (3, 4)")
	tk.MustQuery("select /*+ INL_JOIN(t1) */ * from t join t1 on t.a=t1.a order by t.b").Sort().Check(testkit.Rows("1 3 1 2", "1 3 1 3", "3 1 3 4"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1) */ * from t join t1 on t.a=t1.a order by t.b").Sort().Check(testkit.Rows("1 3 1 2", "1 3 1 3", "3 1 3 4"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1) */ * from t join t1 on t.a=t1.a order by t.b").Sort().Check(testkit.Rows("1 3 1 2", "1 3 1 3", "3 1 3 4"))
	tk.MustQuery("select /*+ INL_JOIN(t) */ t.a, t.b from t join t1 on t.a=t1.a where t1.b = 4 limit 1").Check(testkit.Rows("3 1"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t) */ t.a, t.b from t join t1 on t.a=t1.a where t1.b = 4 limit 1").Check(testkit.Rows("3 1"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t) */ t.a, t.b from t join t1 on t.a=t1.a where t1.b = 4 limit 1").Check(testkit.Rows("3 1"))
	tk.MustQuery("select /*+ INL_JOIN(t, t1) */ * from t right join t1 on t.a=t1.a order by t.b").Sort().Check(testkit.Rows("1 3 1 2", "1 3 1 3", "3 1 3 4", "<nil> <nil> 0 0"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, t1) */ * from t right join t1 on t.a=t1.a order by t.b").Sort().Check(testkit.Rows("1 3 1 2", "1 3 1 3", "3 1 3 4", "<nil> <nil> 0 0"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, t1) */ * from t right join t1 on t.a=t1.a order by t.b").Sort().Check(testkit.Rows("1 3 1 2", "1 3 1 3", "3 1 3 4", "<nil> <nil> 0 0"))

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
	tk.MustQuery("select /*+ INL_JOIN(t) */ t1.a from t1, t where t.a = 5 and t.b = t1.a").Check(testkit.Rows("3"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t) */ t1.a from t1, t where t.a = 5 and t.b = t1.a").Check(testkit.Rows("3"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t) */ t1.a from t1, t where t.a = 5 and t.b = t1.a").Check(testkit.Rows("3"))

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

	// test virtual rows are included (issue#5771)
	result = tk.MustQuery(`SELECT 1 FROM (SELECT 1) t1, (SELECT 1) t2`)
	result.Check(testkit.Rows("1"))

	result = tk.MustQuery(`
		SELECT @NUM := @NUM + 1 as NUM FROM
		( SELECT 1 UNION ALL
			SELECT 2 UNION ALL
			SELECT 3
		) a
		INNER JOIN
		( SELECT 1 UNION ALL
			SELECT 2 UNION ALL
			SELECT 3
		) b,
		(SELECT @NUM := 0) d;
	`)
	result.Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7", "8", "9"))

	// This case is for testing:
	// when the main thread calls Executor.Close() while the out data fetch worker and join workers are still working,
	// we need to stop the goroutines as soon as possible to avoid unexpected error.
	tk.MustExec("set @@tidb_hash_join_concurrency=5")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int)")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t value(1)")
	}
	result = tk.MustQuery("select /*+ TIDB_HJ(s, r) */ * from t as s join t as r on s.a = r.a limit 1;")
	result.Check(testkit.Rows("1 1"))

	tk.MustExec("drop table if exists user, aa, bb")
	tk.MustExec("create table aa(id int)")
	tk.MustExec("insert into aa values(1)")
	tk.MustExec("create table bb(id int)")
	tk.MustExec("insert into bb values(1)")
	tk.MustExec("create table user(id int, name varchar(20))")
	tk.MustExec("insert into user values(1, 'a'), (2, 'b')")
	tk.MustQuery("select user.id,user.name from user left join aa on aa.id = user.id left join bb on aa.id = bb.id where bb.id < 10;").Check(testkit.Rows("1 a"))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (a bigint);`)
	tk.MustExec(`insert into t values (1);`)
	tk.MustQuery(`select t2.a, t1.a from t t1 inner join (select "1" as a) t2 on t2.a = t1.a;`).Check(testkit.Rows("1 1"))
	tk.MustQuery(`select t2.a, t1.a from t t1 inner join (select "2" as b, "1" as a) t2 on t2.a = t1.a;`).Check(testkit.Rows("1 1"))

	tk.MustExec("drop table if exists t1, t2, t3, t4")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("create table t3(a int, b int)")
	tk.MustExec("create table t4(a int, b int)")
	tk.MustExec("insert into t1 values(1, 1)")
	tk.MustExec("insert into t2 values(1, 1)")
	tk.MustExec("insert into t3 values(1, 1)")
	tk.MustExec("insert into t4 values(1, 1)")
	tk.MustQuery("select min(t2.b) from t1 right join t2 on t2.a=t1.a right join t3 on t2.a=t3.a left join t4 on t3.a=t4.a").Check(testkit.Rows("1"))
}

func (s *testSuiteJoin2) TestJoinCast(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	var result *testkit.Result

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int)")
	tk.MustExec("create table t1(c1 int unsigned)")
	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t1 values (1)")
	result = tk.MustQuery("select t.c1 from t , t1 where t.c1 = t1.c1")
	result.Check(testkit.Rows("1"))

	// int64(-1) != uint64(18446744073709551615)
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 bigint)")
	tk.MustExec("create table t1(c1 bigint unsigned)")
	tk.MustExec("insert into t values (-1)")
	tk.MustExec("insert into t1 values (18446744073709551615)")
	result = tk.MustQuery("select * from t , t1 where t.c1 = t1.c1")
	result.Check(testkit.Rows())

	// float(1) == double(1)
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 float)")
	tk.MustExec("create table t1(c1 double)")
	tk.MustExec("insert into t values (1.0)")
	tk.MustExec("insert into t1 values (1.00)")
	result = tk.MustQuery("select t.c1 from t , t1 where t.c1 = t1.c1")
	result.Check(testkit.Rows("1"))

	// varchar("x") == char("x")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 varchar(1))")
	tk.MustExec("create table t1(c1 char(1))")
	tk.MustExec(`insert into t values ("x")`)
	tk.MustExec(`insert into t1 values ("x")`)
	result = tk.MustQuery("select t.c1 from t , t1 where t.c1 = t1.c1")
	result.Check(testkit.Rows("x"))

	// varchar("x") != char("y")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 varchar(1))")
	tk.MustExec("create table t1(c1 char(1))")
	tk.MustExec(`insert into t values ("x")`)
	tk.MustExec(`insert into t1 values ("y")`)
	result = tk.MustQuery("select t.c1 from t , t1 where t.c1 = t1.c1")
	result.Check(testkit.Rows())

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(c1 int,c2 double)")
	tk.MustExec("create table t1(c1 double,c2 int)")
	tk.MustExec("insert into t values (1, 2), (1, NULL)")
	tk.MustExec("insert into t1 values (1, 2), (1, NULL)")
	result = tk.MustQuery("select * from t a , t1 b where (a.c1, a.c2) = (b.c1, b.c2);")
	result.Check(testkit.Rows("1 2 1 2"))

	/* Enable & fix this test after https://github.com/pingcap/tidb/issues/11895 is fixed.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t(c1 bigint unsigned);")
	tk.MustExec("create table t1(c1 bit(64));")
	tk.MustExec("insert into t value(18446744073709551615);")
	tk.MustExec("insert into t1 value(-1);")
	result = tk.MustQuery("select * from t, t1 where t.c1 = t1.c1;")
	c.Check(len(result.Rows()), Equals, 1)
	*/

	/* https://github.com/pingcap/tidb/issues/11896
	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t(c1 bigint);")
	tk.MustExec("create table t1(c1 bit(64));")
	tk.MustExec("insert into t value(1);")
	tk.MustExec("insert into t1 value(1);")
	result = tk.MustQuery("select * from t, t1 where t.c1 = t1.c1;")
	c.Check(len(result.Rows()), Equals, 1)
	*/

	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t(c1 bigint);")
	tk.MustExec("create table t1(c1 bit(64));")
	tk.MustExec("insert into t value(-1);")
	tk.MustExec("insert into t1 value(18446744073709551615);")
	result = tk.MustQuery("select * from t, t1 where t.c1 = t1.c1;")
	// TODO: MySQL will return one row, because c1 in t1 is 0xffffffff, which equals to -1.
	c.Check(len(result.Rows()), Equals, 0)

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t(c1 bigint)")
	tk.MustExec("create table t1(c1 bigint unsigned)")
	tk.MustExec("create table t2(c1 Date)")
	tk.MustExec("insert into t value(20191111)")
	tk.MustExec("insert into t1 value(20191111)")
	tk.MustExec("insert into t2 value('2019-11-11')")
	result = tk.MustQuery("select * from t, t1, t2 where t.c1 = t2.c1 and t1.c1 = t2.c1")
	result.Check(testkit.Rows("20191111 20191111 2019-11-11"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t(c1 bigint);")
	tk.MustExec("create table t1(c1 bigint unsigned);")
	tk.MustExec("create table t2(c1 enum('a', 'b', 'c', 'd'));")
	tk.MustExec("insert into t value(3);")
	tk.MustExec("insert into t1 value(3);")
	tk.MustExec("insert into t2 value('c');")
	result = tk.MustQuery("select * from t, t1, t2 where t.c1 = t2.c1 and t1.c1 = t2.c1;")
	result.Check(testkit.Rows("3 3 c"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t(c1 bigint);")
	tk.MustExec("create table t1(c1 bigint unsigned);")
	tk.MustExec("create table t2 (c1 SET('a', 'b', 'c', 'd'));")
	tk.MustExec("insert into t value(9);")
	tk.MustExec("insert into t1 value(9);")
	tk.MustExec("insert into t2 value('a,d');")
	result = tk.MustQuery("select * from t, t1, t2 where t.c1 = t2.c1 and t1.c1 = t2.c1;")
	result.Check(testkit.Rows("9 9 a,d"))

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
	result = tk.MustQuery("select /*+ INL_JOIN(t1) */ * from t left join t1 on t1.c1 = t.c1")
	result.Sort().Check(testkit.Rows("0.0 0.00", "2.0 <nil>"))
	result = tk.MustQuery("select /*+ INL_HASH_JOIN(t1) */ * from t left join t1 on t1.c1 = t.c1")
	result.Sort().Check(testkit.Rows("0.0 0.00", "2.0 <nil>"))
	result = tk.MustQuery("select /*+ INL_MERGE_JOIN(t1) */ * from t left join t1 on t1.c1 = t.c1")
	result.Sort().Check(testkit.Rows("0.0 0.00", "2.0 <nil>"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
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
	tk.MustExec("set @@tidb_init_chunk_size=1")
	result = tk.MustQuery("select a from (select /*+ INL_JOIN(t1, t2) */ t1.a from t t1 join t t2 on t1.a=t2.a) t group by a")
	result.Sort().Check(testkit.Rows("1", "2", "3"))
	result = tk.MustQuery("select a from (select /*+ INL_HASH_JOIN(t1, t2) */ t1.a from t t1 join t t2 on t1.a=t2.a) t group by a")
	result.Sort().Check(testkit.Rows("1", "2", "3"))
	result = tk.MustQuery("select a from (select /*+ INL_MERGE_JOIN(t1, t2) */ t1.a from t t1 join t t2 on t1.a=t2.a) t group by a")
	result.Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("set @@tidb_init_chunk_size=32")
}

func (s *testSuiteJoin1) TestUsing(c *C) {
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

func (s *testSuiteJoin1) TestNaturalJoin(c *C) {
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

func (s *testSuiteJoin3) TestMultiJoin(c *C) {
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

func (s *testSuiteJoin3) TestSubquerySameTable(c *C) {
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

func (s *testSuiteJoin3) TestSubquery(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@tidb_hash_join_concurrency=1")
	tk.MustExec("set @@tidb_hashagg_partial_concurrency=1")
	tk.MustExec("set @@tidb_hashagg_final_concurrency=1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (2, 2)")
	tk.MustExec("insert t values (3, 4)")
	tk.MustExec("commit")

	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")

	result := tk.MustQuery("select * from t where exists(select * from t k where t.c = k.c having sum(c) = 1)")
	result.Check(testkit.Rows("1 1"))
	result = tk.MustQuery("select * from t where exists(select k.c, k.d from t k, t p where t.c = k.d)")
	result.Check(testkit.Rows("1 1", "2 2"))
	result = tk.MustQuery("select 1 = (select count(*) from t where t.c = k.d) from t k")
	result.Check(testkit.Rows("1", "1", "0"))
	result = tk.MustQuery("select 1 = (select count(*) from t where exists( select * from t m where t.c = k.d)) from t k")
	result.Sort().Check(testkit.Rows("0", "1", "1"))
	result = tk.MustQuery("select t.c = any (select count(*) from t) from t")
	result.Sort().Check(testkit.Rows("0", "0", "1"))
	result = tk.MustQuery("select * from t where (t.c, 6) = any (select count(*), sum(t.c) from t)")
	result.Check(testkit.Rows("3 4"))
	result = tk.MustQuery("select t.c from t where (t.c) < all (select count(*) from t)")
	result.Check(testkit.Rows("1", "2"))
	result = tk.MustQuery("select t.c from t where (t.c, t.d) = any (select * from t)")
	result.Sort().Check(testkit.Rows("1", "2", "3"))
	result = tk.MustQuery("select t.c from t where (t.c, t.d) != all (select * from t)")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select (select count(*) from t where t.c = k.d) from t k")
	result.Sort().Check(testkit.Rows("0", "1", "1"))
	result = tk.MustQuery("select t.c from t where (t.c, t.d) in (select * from t)")
	result.Sort().Check(testkit.Rows("1", "2", "3"))
	result = tk.MustQuery("select t.c from t where (t.c, t.d) not in (select * from t)")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select * from t A inner join t B on A.c = B.c and A.c > 100")
	result.Check(testkit.Rows())
	// = all empty set is true
	result = tk.MustQuery("select t.c from t where (t.c, t.d) != all (select * from t where d > 1000)")
	result.Sort().Check(testkit.Rows("1", "2", "3"))
	result = tk.MustQuery("select t.c from t where (t.c) < any (select c from t where d > 1000)")
	result.Check(testkit.Rows())
	tk.MustExec("insert t values (NULL, NULL)")
	result = tk.MustQuery("select (t.c) < any (select c from t) from t")
	result.Sort().Check(testkit.Rows("1", "1", "<nil>", "<nil>"))
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
	result.Sort().Check(testkit.Rows("0", "0", "1"))

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
	_, err = session.GetRows4Test(context.Background(), tk.Se, rs)
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
	result = tk.MustQuery("select (select /*+ INL_JOIN(x2) */ x2.a from t1 x1, t1 x2 where x1.a = t1.a and x1.a = x2.a) from t1")
	result.Check(testkit.Rows("1", "2", "3", "4", "5"))
	result = tk.MustQuery("select (select /*+ INL_HASH_JOIN(x2) */ x2.a from t1 x1, t1 x2 where x1.a = t1.a and x1.a = x2.a) from t1")
	result.Check(testkit.Rows("1", "2", "3", "4", "5"))
	result = tk.MustQuery("select (select /*+ INL_MERGE_JOIN(x2) */ x2.a from t1 x1, t1 x2 where x1.a = t1.a and x1.a = x2.a) from t1")
	result.Check(testkit.Rows("1", "2", "3", "4", "5"))

	// test left outer semi join & anti left outer semi join
	tk.MustQuery("select 1 from (select t1.a in (select t1.a from t1) from t1) x;").Check(testkit.Rows("1", "1", "1", "1", "1"))
	tk.MustQuery("select 1 from (select t1.a not in (select t1.a from t1) from t1) x;").Check(testkit.Rows("1", "1", "1", "1", "1"))

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(b int)")
	tk.MustExec("insert into t1 values(1)")
	tk.MustExec("insert into t2 values(1)")
	tk.MustQuery("select * from t1 where a in (select a from t2)").Check(testkit.Rows("1"))

	tk.MustExec("set @@tidb_hash_join_concurrency=5")
}

func (s *testSuiteJoin1) TestInSubquery(c *C) {
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
	tk.MustExec("set @@session.tidb_opt_insubq_to_join_and_agg = 0")
	result = tk.MustQuery("select * from t1 where a in (select * from t2)")
	result.Sort().Check(testkit.Rows("1", "2"))
	result = tk.MustQuery("select * from t1 where a in (select * from t2 where false)")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select * from t1 where a not in (select * from t2 where false)")
	result.Sort().Check(testkit.Rows("1", "2"))
	tk.MustExec("set @@session.tidb_opt_insubq_to_join_and_agg = 1")
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

func (s *testSuiteJoin1) TestJoinLeak(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@tidb_hash_join_concurrency=1")
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
	req := result.NewChunk()
	err = result.Next(context.Background(), req)
	c.Assert(err, IsNil)
	time.Sleep(time.Millisecond)
	result.Close()

	tk.MustExec("set @@tidb_hash_join_concurrency=5")
}

func (s *testSuiteJoin1) TestHashJoinExecEncodeDecodeRow(c *C) {
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

func (s *testSuiteJoin1) TestSubqueryInJoinOn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (id int)")
	tk.MustExec("create table t2 (id int)")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")

	err := tk.ExecToErr("SELECT * FROM t1 JOIN t2 on (t2.id < all (SELECT 1))")
	c.Check(err, NotNil)
}

func (s *testSuiteJoin1) TestIssue5255(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b date, c float, primary key(a, b))")
	tk.MustExec("create table t2(a int primary key)")
	tk.MustExec("insert into t1 values(1, '2017-11-29', 2.2)")
	tk.MustExec("insert into t2 values(1)")
	tk.MustQuery("select /*+ INL_JOIN(t1) */ * from t1 join t2 on t1.a=t2.a").Check(testkit.Rows("1 2017-11-29 2.2 1"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1) */ * from t1 join t2 on t1.a=t2.a").Check(testkit.Rows("1 2017-11-29 2.2 1"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1) */ * from t1 join t2 on t1.a=t2.a").Check(testkit.Rows("1 2017-11-29 2.2 1"))
}

func (s *testSuiteJoin1) TestIssue5278(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, tt")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create table tt(a varchar(10), b int)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustQuery("select * from t left join tt on t.a=tt.a left join t ttt on t.a=ttt.a").Check(testkit.Rows("1 1 <nil> <nil> 1 1"))
}

func (s *testSuiteJoin1) TestIndexLookupJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_init_chunk_size=2")
	tk.MustExec("DROP TABLE IF EXISTS t")
	tk.MustExec("CREATE TABLE `t` (`a` int, pk integer auto_increment,`b` char (20),primary key (pk))")
	tk.MustExec("CREATE INDEX idx_t_a ON t(`a`)")
	tk.MustExec("CREATE INDEX idx_t_b ON t(`b`)")
	tk.MustExec("INSERT INTO t VALUES (148307968, DEFAULT, 'nndsjofmpdxvhqv') ,  (-1327693824, DEFAULT, 'pnndsjofmpdxvhqvfny') ,  (-277544960, DEFAULT, 'fpnndsjo')")

	tk.MustExec("DROP TABLE IF EXISTS s")
	tk.MustExec("CREATE TABLE `s` (`a` int, `b` char (20))")
	tk.MustExec("CREATE INDEX idx_s_a ON s(`a`)")
	tk.MustExec("INSERT INTO s VALUES (-277544960, 'fpnndsjo') ,  (2, 'kfpnndsjof') ,  (2, 'vtdiockfpn'), (-277544960, 'fpnndsjo') ,  (2, 'kfpnndsjof') ,  (6, 'ckfp')")
	tk.MustQuery("select /*+ INL_JOIN(t, s) */ t.a from t join s on t.a = s.a").Sort().Check(testkit.Rows("-277544960", "-277544960"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, s) */ t.a from t join s on t.a = s.a").Sort().Check(testkit.Rows("-277544960", "-277544960"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, s) */ t.a from t join s on t.a = s.a").Sort().Check(testkit.Rows("-277544960", "-277544960"))

	tk.MustQuery("select /*+ INL_JOIN(t, s) */ t.a from t left join s on t.a = s.a").Sort().Check(testkit.Rows("-1327693824", "-277544960", "-277544960", "148307968"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, s) */ t.a from t left join s on t.a = s.a").Sort().Check(testkit.Rows("-1327693824", "-277544960", "-277544960", "148307968"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, s) */ t.a from t left join s on t.a = s.a").Sort().Check(testkit.Rows("-1327693824", "-277544960", "-277544960", "148307968"))

	tk.MustQuery("select /*+ INL_JOIN(t, s) */ t.a from t left join s on t.a = s.a where t.a = -277544960").Sort().Check(testkit.Rows("-277544960", "-277544960"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, s) */ t.a from t left join s on t.a = s.a where t.a = -277544960").Sort().Check(testkit.Rows("-277544960", "-277544960"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, s) */ t.a from t left join s on t.a = s.a where t.a = -277544960").Sort().Check(testkit.Rows("-277544960", "-277544960"))

	tk.MustQuery("select /*+ INL_JOIN(t, s) */ t.a from t right join s on t.a = s.a").Sort().Check(testkit.Rows("-277544960", "-277544960", "<nil>", "<nil>", "<nil>", "<nil>"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, s) */ t.a from t right join s on t.a = s.a").Sort().Check(testkit.Rows("-277544960", "-277544960", "<nil>", "<nil>", "<nil>", "<nil>"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, s) */ t.a from t right join s on t.a = s.a").Sort().Check(testkit.Rows("-277544960", "-277544960", "<nil>", "<nil>", "<nil>", "<nil>"))

	tk.MustQuery("select /*+ INL_JOIN(t, s) */ t.a from t left join s on t.a = s.a order by t.a desc").Check(testkit.Rows("148307968", "-277544960", "-277544960", "-1327693824"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, s) */ t.a from t left join s on t.a = s.a order by t.a desc").Check(testkit.Rows("148307968", "-277544960", "-277544960", "-1327693824"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, s) */ t.a from t left join s on t.a = s.a order by t.a desc").Check(testkit.Rows("148307968", "-277544960", "-277544960", "-1327693824"))

	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a BIGINT PRIMARY KEY, b BIGINT);")
	tk.MustExec("INSERT INTO t VALUES(1, 2);")
	tk.MustQuery("SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a=t2.a UNION ALL SELECT /*+ INL_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a=t2.a;").Check(testkit.Rows("1 2 1 2", "1 2 1 2"))
	tk.MustQuery("SELECT /*+ INL_HASH_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a=t2.a UNION ALL SELECT /*+ INL_HASH_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a=t2.a;").Check(testkit.Rows("1 2 1 2", "1 2 1 2"))
	tk.MustQuery("SELECT /*+ INL_MERGE_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a=t2.a UNION ALL SELECT /*+ INL_MERGE_JOIN(t1, t2) */ * FROM t t1 JOIN t t2 ON t1.a=t2.a;").Check(testkit.Rows("1 2 1 2", "1 2 1 2"))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a decimal(6,2), index idx(a));`)
	tk.MustExec(`insert into t values(1.01), (2.02), (NULL);`)
	tk.MustQuery(`select /*+ INL_JOIN(t2) */ t1.a from t t1 join t t2 on t1.a=t2.a order by t1.a;`).Check(testkit.Rows(
		`1.01`,
		`2.02`,
	))
	tk.MustQuery(`select /*+ INL_HASH_JOIN(t2) */ t1.a from t t1 join t t2 on t1.a=t2.a order by t1.a;`).Check(testkit.Rows(
		`1.01`,
		`2.02`,
	))
	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t2) */ t1.a from t t1 join t t2 on t1.a=t2.a order by t1.a;`).Check(testkit.Rows(
		`1.01`,
		`2.02`,
	))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint, b bigint, unique key idx1(a, b));`)
	tk.MustExec(`insert into t values(1, 1), (1, 2), (1, 3), (1, 4), (1, 5), (1, 6);`)
	tk.MustExec(`set @@tidb_init_chunk_size = 2;`)
	tk.MustQuery(`select /*+ INL_JOIN(t2) */ * from t t1 left join t t2 on t1.a = t2.a and t1.b = t2.b + 4;`).Check(testkit.Rows(
		`1 1 <nil> <nil>`,
		`1 2 <nil> <nil>`,
		`1 3 <nil> <nil>`,
		`1 4 <nil> <nil>`,
		`1 5 1 1`,
		`1 6 1 2`,
	))
	tk.MustQuery(`select /*+ INL_HASH_JOIN(t2) */ * from t t1 left join t t2 on t1.a = t2.a and t1.b = t2.b + 4;`).Check(testkit.Rows(
		`1 1 <nil> <nil>`,
		`1 2 <nil> <nil>`,
		`1 3 <nil> <nil>`,
		`1 4 <nil> <nil>`,
		`1 5 1 1`,
		`1 6 1 2`,
	))
	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t2) */ * from t t1 left join t t2 on t1.a = t2.a and t1.b = t2.b + 4;`).Check(testkit.Rows(
		`1 1 <nil> <nil>`,
		`1 2 <nil> <nil>`,
		`1 3 <nil> <nil>`,
		`1 4 <nil> <nil>`,
		`1 5 1 1`,
		`1 6 1 2`,
	))

	tk.MustExec(`drop table if exists t1, t2, t3;`)
	tk.MustExec("create table t1(a int primary key, b int)")
	tk.MustExec("insert into t1 values(1, 0), (2, null)")
	tk.MustExec("create table t2(a int primary key)")
	tk.MustExec("insert into t2 values(0)")
	tk.MustQuery("select /*+ INL_JOIN(t2)*/ * from t1 left join t2 on t1.b = t2.a;").Sort().Check(testkit.Rows(
		`1 0 0`,
		`2 <nil> <nil>`,
	))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t2)*/ * from t1 left join t2 on t1.b = t2.a;").Sort().Check(testkit.Rows(
		`1 0 0`,
		`2 <nil> <nil>`,
	))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t2)*/ * from t1 left join t2 on t1.b = t2.a;").Sort().Check(testkit.Rows(
		`1 0 0`,
		`2 <nil> <nil>`,
	))

	tk.MustExec("create table t3(a int, key(a))")
	tk.MustExec("insert into t3 values(0)")
	tk.MustQuery("select /*+ INL_JOIN(t3)*/ * from t1 left join t3 on t1.b = t3.a;").Check(testkit.Rows(
		`1 0 0`,
		`2 <nil> <nil>`,
	))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t3)*/ * from t1 left join t3 on t1.b = t3.a;").Check(testkit.Rows(
		`1 0 0`,
		`2 <nil> <nil>`,
	))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t3)*/ * from t1 left join t3 on t1.b = t3.a;").Check(testkit.Rows(
		`2 <nil> <nil>`,
		`1 0 0`,
	))

	tk.MustExec("drop table if exists t,s")
	tk.MustExec("create table t(a int primary key auto_increment, b time)")
	tk.MustExec("create table s(a int, b time)")
	tk.MustExec("alter table s add index idx(a,b)")
	tk.MustExec("set @@tidb_index_join_batch_size=4;set @@tidb_init_chunk_size=1;set @@tidb_max_chunk_size=32; set @@tidb_index_lookup_join_concurrency=15;")
	// insert 64 rows into `t`
	tk.MustExec("insert into t values(0, '01:01:01')")
	for i := 0; i < 6; i++ {
		tk.MustExec("insert into t select 0, b + 1 from t")
	}
	tk.MustExec("insert into s select a, b - 1 from t")
	tk.MustExec("analyze table t;")
	tk.MustExec("analyze table s;")

	tk.MustQuery("desc select /*+ TIDB_INLJ(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Rows(
		"HashAgg_9 1.00 root funcs:count(1)->Column#6",
		"└─IndexJoin_16 64.00 root inner join, inner:IndexReader_15, outer key:test.t.a, inner key:test.s.a, other cond:lt(test.s.b, test.t.b)",
		"  ├─TableReader_26(Build) 64.00 root data:Selection_25",
		"  │ └─Selection_25 64.00 cop[tikv] not(isnull(test.t.b))",
		"  │   └─TableFullScan_24 64.00 cop[tikv] table:t, keep order:false",
		"  └─IndexReader_15(Probe) 1.00 root index:Selection_14",
		"    └─Selection_14 1.00 cop[tikv] not(isnull(test.s.a)), not(isnull(test.s.b))",
		"      └─IndexRangeScan_13 1.00 cop[tikv] table:s, index:a, b, range: decided by [eq(test.s.a, test.t.a) lt(test.s.b, test.t.b)], keep order:false"))
	tk.MustQuery("select /*+ TIDB_INLJ(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Rows("64"))
	tk.MustExec("set @@tidb_index_lookup_join_concurrency=1;")
	tk.MustQuery("select /*+ TIDB_INLJ(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Rows("64"))

	tk.MustQuery("desc select /*+ INL_MERGE_JOIN(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Rows(
		"HashAgg_9 1.00 root funcs:count(1)->Column#6",
		"└─IndexMergeJoin_21 64.00 root inner join, inner:IndexReader_19, outer key:test.t.a, inner key:test.s.a, other cond:lt(test.s.b, test.t.b)",
		"  ├─TableReader_26(Build) 64.00 root data:Selection_25",
		"  │ └─Selection_25 64.00 cop[tikv] not(isnull(test.t.b))",
		"  │   └─TableFullScan_24 64.00 cop[tikv] table:t, keep order:false",
		"  └─IndexReader_19(Probe) 1.00 root index:Selection_18",
		"    └─Selection_18 1.00 cop[tikv] not(isnull(test.s.a)), not(isnull(test.s.b))",
		"      └─IndexRangeScan_17 1.00 cop[tikv] table:s, index:a, b, range: decided by [eq(test.s.a, test.t.a) lt(test.s.b, test.t.b)], keep order:true",
	))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Rows("64"))
	tk.MustExec("set @@tidb_index_lookup_join_concurrency=1;")
	tk.MustQuery("select /*+ INL_MERGE_JOIN(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Rows("64"))

	tk.MustQuery("desc select /*+ INL_HASH_JOIN(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Rows(
		"HashAgg_9 1.00 root funcs:count(1)->Column#6",
		"└─IndexHashJoin_23 64.00 root inner join, inner:IndexReader_15, outer key:test.t.a, inner key:test.s.a, other cond:lt(test.s.b, test.t.b)",
		"  ├─TableReader_26(Build) 64.00 root data:Selection_25",
		"  │ └─Selection_25 64.00 cop[tikv] not(isnull(test.t.b))",
		"  │   └─TableFullScan_24 64.00 cop[tikv] table:t, keep order:false",
		"  └─IndexReader_15(Probe) 1.00 root index:Selection_14",
		"    └─Selection_14 1.00 cop[tikv] not(isnull(test.s.a)), not(isnull(test.s.b))",
		"      └─IndexRangeScan_13 1.00 cop[tikv] table:s, index:a, b, range: decided by [eq(test.s.a, test.t.a) lt(test.s.b, test.t.b)], keep order:false",
	))
	tk.MustQuery("select /*+ INL_HASH_JOIN(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Rows("64"))
	tk.MustExec("set @@tidb_index_lookup_join_concurrency=1;")
	tk.MustQuery("select /*+ INL_HASH_JOIN(s) */ count(*) from t join s use index(idx) on s.a = t.a and s.b < t.b").Check(testkit.Rows("64"))
}

func (s *testSuiteJoin1) TestIndexNestedLoopHashJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_init_chunk_size=2")
	tk.MustExec("set @@tidb_index_join_batch_size=10")
	tk.MustExec("DROP TABLE IF EXISTS t, s")
	tk.MustExec("create table t(pk int primary key, a int)")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%d, %d)", i, i))
	}
	tk.MustExec("create table s(a int primary key)")
	for i := 0; i < 100; i++ {
		if rand.Float32() < 0.3 {
			tk.MustExec(fmt.Sprintf("insert into s values(%d)", i))
		} else {
			tk.MustExec(fmt.Sprintf("insert into s values(%d)", i*100))
		}
	}
	tk.MustExec("analyze table t")
	tk.MustExec("analyze table s")
	// Test IndexNestedLoopHashJoin keepOrder.
	tk.MustQuery("explain select /*+ INL_HASH_JOIN(s) */ * from t left join s on t.a=s.a order by t.pk").Check(testkit.Rows(
		"IndexHashJoin_28 100.00 root left outer join, inner:TableReader_22, outer key:test.t.a, inner key:test.s.a",
		"├─TableReader_30(Build) 100.00 root data:TableFullScan_29",
		"│ └─TableFullScan_29 100.00 cop[tikv] table:t, keep order:true",
		"└─TableReader_22(Probe) 1.00 root data:TableRangeScan_21",
		"  └─TableRangeScan_21 1.00 cop[tikv] table:s, range: decided by [test.t.a], keep order:false",
	))
	rs := tk.MustQuery("select /*+ INL_HASH_JOIN(s) */ * from t left join s on t.a=s.a order by t.pk")
	for i, row := range rs.Rows() {
		c.Assert(row[0].(string), Equals, fmt.Sprintf("%d", i))
	}
}

func (s *testSuiteJoin3) TestIssue13449(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, s;")
	tk.MustExec("create table t(a int, index(a));")
	tk.MustExec("create table s(a int, index(a));")
	for i := 1; i <= 128; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%d)", i))
	}
	tk.MustExec("insert into s values(1), (128)")
	tk.MustExec("set @@tidb_max_chunk_size=32;")
	tk.MustExec("set @@tidb_index_lookup_join_concurrency=1;")
	tk.MustExec("set @@tidb_index_join_batch_size=32;")

	tk.MustQuery("desc select /*+ INL_HASH_JOIN(s) */ * from t join s on t.a=s.a order by t.a;").Check(testkit.Rows(
		"IndexHashJoin_35 12487.50 root inner join, inner:IndexReader_27, outer key:test.t.a, inner key:test.s.a",
		"├─IndexReader_37(Build) 9990.00 root index:IndexFullScan_36",
		"│ └─IndexFullScan_36 9990.00 cop[tikv] table:t, index:a, keep order:true, stats:pseudo",
		"└─IndexReader_27(Probe) 1.25 root index:Selection_26",
		"  └─Selection_26 1.25 cop[tikv] not(isnull(test.s.a))",
		"    └─IndexRangeScan_25 1.25 cop[tikv] table:s, index:a, range: decided by [eq(test.s.a, test.t.a)], keep order:false, stats:pseudo"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(s) */ * from t join s on t.a=s.a order by t.a;").Check(testkit.Rows("1 1", "128 128"))
}

func (s *testSuiteJoin3) TestMergejoinOrder(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1(a bigint primary key, b bigint);")
	tk.MustExec("create table t2(a bigint primary key, b bigint);")
	tk.MustExec("insert into t1 values(1, 100), (2, 100), (3, 100), (4, 100), (5, 100);")
	tk.MustExec("insert into t2 select a*100, b*100 from t1;")

	tk.MustQuery("explain select /*+ TIDB_SMJ(t2) */ * from t1 left outer join t2 on t1.a=t2.a and t1.a!=3 order by t1.a;").Check(testkit.Rows(
		"MergeJoin_20 10000.00 root left outer join, left key:test.t1.a, right key:test.t2.a, left cond:[ne(test.t1.a, 3)]",
		"├─TableReader_14(Build) 6666.67 root data:TableRangeScan_13",
		"│ └─TableRangeScan_13 6666.67 cop[tikv] table:t2, range:[-inf,3), (3,+inf], keep order:true, stats:pseudo",
		"└─TableReader_12(Probe) 10000.00 root data:TableFullScan_11",
		"  └─TableFullScan_11 10000.00 cop[tikv] table:t1, keep order:true, stats:pseudo",
	))

	tk.MustExec("set @@tidb_init_chunk_size=1")
	tk.MustQuery("select /*+ TIDB_SMJ(t2) */ * from t1 left outer join t2 on t1.a=t2.a and t1.a!=3 order by t1.a;").Check(testkit.Rows(
		"1 100 <nil> <nil>",
		"2 100 <nil> <nil>",
		"3 100 <nil> <nil>",
		"4 100 <nil> <nil>",
		"5 100 <nil> <nil>",
	))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint, b bigint, index idx_1(a,b));`)
	tk.MustExec(`insert into t values(1, 1), (1, 2), (2, 1), (2, 2);`)
	tk.MustQuery(`select /*+ TIDB_SMJ(t1, t2) */ * from t t1 join t t2 on t1.b = t2.b and t1.a=t2.a;`).Check(testkit.Rows(
		`1 1 1 1`,
		`1 2 1 2`,
		`2 1 2 1`,
		`2 2 2 2`,
	))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a decimal(6,2), index idx(a));`)
	tk.MustExec(`insert into t values(1.01), (2.02), (NULL);`)
	tk.MustQuery(`select /*+ TIDB_SMJ(t1) */ t1.a from t t1 join t t2 on t1.a=t2.a order by t1.a;`).Check(testkit.Rows(
		`1.01`,
		`2.02`,
	))
}

func (s *testSuiteJoin1) TestEmbeddedOuterJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values(1, 1)")
	tk.MustQuery("select * from (t1 left join t2 on t1.a = t2.a) left join (t2 t3 left join t2 t4 on t3.a = t4.a) on t2.b = 1").
		Check(testkit.Rows("1 1 <nil> <nil> <nil> <nil> <nil> <nil>"))
}

func (s *testSuiteJoin1) TestHashJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int);")
	tk.MustExec("create table t2(a int, b int);")
	tk.MustExec("insert into t1 values(1,1),(2,2),(3,3),(4,4),(5,5);")
	tk.MustQuery("select count(*) from t1").Check(testkit.Rows("5"))
	tk.MustQuery("select count(*) from t2").Check(testkit.Rows("0"))
	tk.MustExec("set @@tidb_init_chunk_size=1;")
	result := tk.MustQuery("explain analyze select /*+ TIDB_HJ(t1, t2) */ * from t1 where exists (select a from t2 where t1.a = t2.a);")
	// HashLeftJoin_9 7992.00 root semi join, inner:TableReader_15, equal:[eq(test.t1.a, test.t2.a)] time:219.863µs, loops:1, rows:0
	// ├─TableReader_15(Build) 9990.00 root data:Selection_14 time:12.983µs, loops:1, rows:0
	// │ └─Selection_14 9990.00 cop[tikv] not(isnull(test.t2.a))
	// │     └─TableFullScan_13 10000.00 cop[tikv] table:t2, keep order:false, stats:pseudo time:0s, loops:0, rows:0
	// └─TableReader_12(Probe) 9990.00 root data:Selection_11 time:9.129µs, loops:1, rows:1
	//   └─Selection_11 9990.00 cop[tikv] not(isnull(test.t1.a))
	//     └─TableFullScan_10 10000.00 cop[tikv] table:t1, keep order:false, stats:pseudo time:0s, loops:0, rows:5
	row := result.Rows()
	c.Assert(len(row), Equals, 7)
	outerExecInfo := row[4][5].(string)
	// FIXME: revert this result to 1 after TableReaderExecutor can handle initChunkSize.
	c.Assert(outerExecInfo[strings.Index(outerExecInfo, "rows")+5:strings.Index(outerExecInfo, "rows")+6], Equals, "5")
	innerExecInfo := row[1][5].(string)
	c.Assert(innerExecInfo[strings.Index(innerExecInfo, "rows")+5:strings.Index(innerExecInfo, "rows")+6], Equals, "0")
}

func (s *testSuiteJoin1) TestJoinDifferentDecimals(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("Use test")
	tk.MustExec("Drop table if exists t1")
	tk.MustExec("Create table t1 (v int)")
	tk.MustExec("Insert into t1 value (1)")
	tk.MustExec("Insert into t1 value (2)")
	tk.MustExec("Insert into t1 value (3)")
	tk.MustExec("Drop table if exists t2")
	tk.MustExec("Create table t2 (v decimal(12, 3))")
	tk.MustExec("Insert into t2 value (1)")
	tk.MustExec("Insert into t2 value (2.0)")
	tk.MustExec("Insert into t2 value (000003.000000)")
	rst := tk.MustQuery("Select * from t1, t2 where t1.v = t2.v order by t1.v")
	row := rst.Rows()
	c.Assert(len(row), Equals, 3)
	rst.Check(testkit.Rows("1 1.000", "2 2.000", "3 3.000"))
}

func (s *testSuiteJoin2) TestNullEmptyAwareSemiJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index idx_a(a), index idb_b(b), index idx_c(c))")
	tk.MustExec("insert into t values(null, 1, 0), (1, 2, 0)")
	tests := []struct {
		sql string
	}{
		{
			"a, b from t t1 where a not in (select b from t t2)",
		},
		{
			"a, b from t t1 where a not in (select b from t t2 where t1.b = t2.a)",
		},
		{
			"a, b from t t1 where a not in (select a from t t2)",
		},
		{
			"a, b from t t1 where a not in (select a from t t2 where t1.b = t2.b)",
		},
		{
			"a, b from t t1 where a != all (select b from t t2)",
		},
		{
			"a, b from t t1 where a != all (select b from t t2 where t1.b = t2.a)",
		},
		{
			"a, b from t t1 where a != all (select a from t t2)",
		},
		{
			"a, b from t t1 where a != all (select a from t t2 where t1.b = t2.b)",
		},
		{
			"a, b from t t1 where not exists (select * from t t2 where t1.a = t2.b)",
		},
		{
			"a, b from t t1 where not exists (select * from t t2 where t1.a = t2.a)",
		},
	}
	results := []struct {
		result [][]interface{}
	}{
		{
			testkit.Rows(),
		},
		{
			testkit.Rows("1 2"),
		},
		{
			testkit.Rows(),
		},
		{
			testkit.Rows(),
		},
		{
			testkit.Rows(),
		},
		{
			testkit.Rows("1 2"),
		},
		{
			testkit.Rows(),
		},
		{
			testkit.Rows(),
		},
		{
			testkit.Rows("<nil> 1"),
		},
		{
			testkit.Rows("<nil> 1"),
		},
	}
	hints := [5]string{
		"/*+ HASH_JOIN(t1, t2) */",
		"/*+ MERGE_JOIN(t1, t2) */",
		"/*+ INL_JOIN(t1, t2) */",
		"/*+ INL_HASH_JOIN(t1, t2) */",
		"/*+ INL_MERGE_JOIN(t1, t2) */",
	}
	for i, tt := range tests {
		for _, hint := range hints {
			sql := fmt.Sprintf("select %s %s", hint, tt.sql)
			result := tk.MustQuery(sql)
			result.Check(results[i].result)
		}
	}

	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(1, null, 0), (2, 1, 0)")
	results = []struct {
		result [][]interface{}
	}{
		{
			testkit.Rows(),
		},
		{
			testkit.Rows("1 <nil>"),
		},
		{
			testkit.Rows(),
		},
		{
			testkit.Rows("1 <nil>"),
		},
		{
			testkit.Rows(),
		},
		{
			testkit.Rows("1 <nil>"),
		},
		{
			testkit.Rows(),
		},
		{
			testkit.Rows("1 <nil>"),
		},
		{
			testkit.Rows("2 1"),
		},
		{
			testkit.Rows(),
		},
	}
	for i, tt := range tests {
		for _, hint := range hints {
			sql := fmt.Sprintf("select %s %s", hint, tt.sql)
			result := tk.MustQuery(sql)
			result.Check(results[i].result)
		}
	}

	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(1, null, 0), (2, 1, 0), (null, 2, 0)")
	results = []struct {
		result [][]interface{}
	}{
		{
			testkit.Rows(),
		},
		{
			testkit.Rows("1 <nil>"),
		},
		{
			testkit.Rows(),
		},
		{
			testkit.Rows("1 <nil>"),
		},
		{
			testkit.Rows(),
		},
		{
			testkit.Rows("1 <nil>"),
		},
		{
			testkit.Rows(),
		},
		{
			testkit.Rows("1 <nil>"),
		},
		{
			testkit.Rows("<nil> 2"),
		},
		{
			testkit.Rows("<nil> 2"),
		},
	}
	for i, tt := range tests {
		for _, hint := range hints {
			sql := fmt.Sprintf("select %s %s", hint, tt.sql)
			result := tk.MustQuery(sql)
			result.Check(results[i].result)
		}
	}

	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(1, null, 0), (2, null, 0)")
	tests = []struct {
		sql string
	}{
		{
			"a, b from t t1 where b not in (select a from t t2)",
		},
	}
	results = []struct {
		result [][]interface{}
	}{
		{
			testkit.Rows(),
		},
	}
	for i, tt := range tests {
		for _, hint := range hints {
			sql := fmt.Sprintf("select %s %s", hint, tt.sql)
			result := tk.MustQuery(sql)
			result.Check(results[i].result)
		}
	}

	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(null, 1, 1), (2, 2, 2), (3, null, 3), (4, 4, 3)")
	tests = []struct {
		sql string
	}{
		{
			"a, b, a not in (select b from t t2) from t t1 order by a",
		},
		{
			"a, c, a not in (select c from t t2) from t t1 order by a",
		},
		{
			"a, b, a in (select b from t t2) from t t1 order by a",
		},
		{
			"a, c, a in (select c from t t2) from t t1 order by a",
		},
	}
	results = []struct {
		result [][]interface{}
	}{
		{
			testkit.Rows(
				"<nil> 1 <nil>",
				"2 2 0",
				"3 <nil> <nil>",
				"4 4 0",
			),
		},
		{
			testkit.Rows(
				"<nil> 1 <nil>",
				"2 2 0",
				"3 3 0",
				"4 3 1",
			),
		},
		{
			testkit.Rows(
				"<nil> 1 <nil>",
				"2 2 1",
				"3 <nil> <nil>",
				"4 4 1",
			),
		},
		{
			testkit.Rows(
				"<nil> 1 <nil>",
				"2 2 1",
				"3 3 1",
				"4 3 0",
			),
		},
	}
	for i, tt := range tests {
		for _, hint := range hints {
			sql := fmt.Sprintf("select %s %s", hint, tt.sql)
			result := tk.MustQuery(sql)
			result.Check(results[i].result)
		}
	}

	tk.MustExec("drop table if exists s")
	tk.MustExec("create table s(a int, b int)")
	tk.MustExec("insert into s values(1, 2)")
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(null, null, 0)")
	tests = []struct {
		sql string
	}{
		{
			"a in (select b from t t2 where t2.a = t1.b) from s t1",
		},
		{
			"a in (select b from s t2 where t2.a = t1.b) from t t1",
		},
	}
	results = []struct {
		result [][]interface{}
	}{
		{
			testkit.Rows("0"),
		},
		{
			testkit.Rows("0"),
		},
	}
	for i, tt := range tests {
		for _, hint := range hints {
			sql := fmt.Sprintf("select %s %s", hint, tt.sql)
			result := tk.MustQuery(sql)
			result.Check(results[i].result)
		}
	}

	tk.MustExec("truncate table s")
	tk.MustExec("insert into s values(2, 2)")
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(null, 1, 0)")
	tests = []struct {
		sql string
	}{
		{
			"a in (select a from s t2 where t2.b = t1.b) from t t1",
		},
		{
			"a in (select a from s t2 where t2.b < t1.b) from t t1",
		},
	}
	results = []struct {
		result [][]interface{}
	}{
		{
			testkit.Rows("0"),
		},
		{
			testkit.Rows("0"),
		},
	}
	for i, tt := range tests {
		for _, hint := range hints {
			sql := fmt.Sprintf("select %s %s", hint, tt.sql)
			result := tk.MustQuery(sql)
			result.Check(results[i].result)
		}
	}

	tk.MustExec("truncate table s")
	tk.MustExec("insert into s values(null, 2)")
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(1, 1, 0)")
	tests = []struct {
		sql string
	}{
		{
			"a in (select a from s t2 where t2.b = t1.b) from t t1",
		},
		{
			"b in (select a from s t2) from t t1",
		},
		{
			"* from t t1 where a not in (select a from s t2 where t2.b = t1.b)",
		},
		{
			"* from t t1 where a not in (select a from s t2)",
		},
		{
			"* from s t1 where a not in (select a from t t2)",
		},
	}
	results = []struct {
		result [][]interface{}
	}{
		{
			testkit.Rows("0"),
		},
		{
			testkit.Rows("<nil>"),
		},
		{
			testkit.Rows("1 1 0"),
		},
		{
			testkit.Rows(),
		},
		{
			testkit.Rows(),
		},
	}
	for i, tt := range tests {
		for _, hint := range hints {
			sql := fmt.Sprintf("select %s %s", hint, tt.sql)
			result := tk.MustQuery(sql)
			result.Check(results[i].result)
		}
	}
}

func (s *testSuiteJoin1) TestScalarFuncNullSemiJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(null, 1), (1, 2)")
	tk.MustExec("drop table if exists s")
	tk.MustExec("create table s(a varchar(20), b varchar(20))")
	tk.MustExec("insert into s values(null, '1')")
	tk.MustQuery("select a in (select a from s) from t").Check(testkit.Rows("<nil>", "<nil>"))
	tk.MustExec("drop table s")
	tk.MustExec("create table s(a int, b int)")
	tk.MustExec("insert into s values(null, 1)")
	tk.MustQuery("select a in (select a+b from s) from t").Check(testkit.Rows("<nil>", "<nil>"))
}

func (s *testSuiteJoin1) TestInjectProjOnTopN(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(a bigint, b bigint)")
	tk.MustExec("create table t2(a bigint, b bigint)")
	tk.MustExec("insert into t1 values(1, 1)")
	tk.MustQuery("select t1.a+t1.b as result from t1 left join t2 on 1 = 0 order by result limit 20;").Check(testkit.Rows(
		"2",
	))
}

func (s *testSuiteJoin1) TestIssue11544(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table 11544t(a int)")
	tk.MustExec("create table 11544tt(a int, b varchar(10), index idx(a, b(3)))")
	tk.MustExec("insert into 11544t values(1)")
	tk.MustExec("insert into 11544tt values(1, 'aaaaaaa'), (1, 'aaaabbb'), (1, 'aaaacccc')")
	tk.MustQuery("select /*+ INL_JOIN(tt) */ * from 11544t t, 11544tt tt where t.a=tt.a and (tt.b = 'aaaaaaa' or tt.b = 'aaaabbb')").Check(testkit.Rows("1 1 aaaaaaa", "1 1 aaaabbb"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(tt) */ * from 11544t t, 11544tt tt where t.a=tt.a and (tt.b = 'aaaaaaa' or tt.b = 'aaaabbb')").Check(testkit.Rows("1 1 aaaaaaa", "1 1 aaaabbb"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(tt) */ * from 11544t t, 11544tt tt where t.a=tt.a and (tt.b = 'aaaaaaa' or tt.b = 'aaaabbb')").Check(testkit.Rows("1 1 aaaaaaa", "1 1 aaaabbb"))

	tk.MustQuery("select /*+ INL_JOIN(tt) */ * from 11544t t, 11544tt tt where t.a=tt.a and tt.b in ('aaaaaaa', 'aaaabbb', 'aaaacccc')").Check(testkit.Rows("1 1 aaaaaaa", "1 1 aaaabbb", "1 1 aaaacccc"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(tt) */ * from 11544t t, 11544tt tt where t.a=tt.a and tt.b in ('aaaaaaa', 'aaaabbb', 'aaaacccc')").Check(testkit.Rows("1 1 aaaaaaa", "1 1 aaaabbb", "1 1 aaaacccc"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(tt) */ * from 11544t t, 11544tt tt where t.a=tt.a and tt.b in ('aaaaaaa', 'aaaabbb', 'aaaacccc')").Check(testkit.Rows("1 1 aaaaaaa", "1 1 aaaabbb", "1 1 aaaacccc"))
}

func (s *testSuiteJoin1) TestIssue11390(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table 11390t (k1 int unsigned, k2 int unsigned, key(k1, k2))")
	tk.MustExec("insert into 11390t values(1, 1)")
	tk.MustQuery("select /*+ INL_JOIN(t1, t2) */ * from 11390t t1, 11390t t2 where t1.k2 > 0 and t1.k2 = t2.k2 and t2.k1=1;").Check(testkit.Rows("1 1 1 1"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1, t2) */ * from 11390t t1, 11390t t2 where t1.k2 > 0 and t1.k2 = t2.k2 and t2.k1=1;").Check(testkit.Rows("1 1 1 1"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1, t2) */ * from 11390t t1, 11390t t2 where t1.k2 > 0 and t1.k2 = t2.k2 and t2.k1=1;").Check(testkit.Rows("1 1 1 1"))
}

func (s *testSuiteJoinSerial) TestOuterTableBuildHashTableIsuse13933(c *C) {
	plannercore.ForceUseOuterBuild4Test = true
	defer func() { plannercore.ForceUseOuterBuild4Test = false }()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, s")
	tk.MustExec("create table t (a int,b int)")
	tk.MustExec("create table s (a int,b int)")
	tk.MustExec("insert into t values (11,11),(1,2)")
	tk.MustExec("insert into s values (1,2),(2,1),(11,11)")
	tk.MustQuery("select * from t left join s on s.a > t.a").Sort().Check(testkit.Rows("1 2 11 11", "1 2 2 1", "11 11 <nil> <nil>"))
	tk.MustQuery("explain select * from t left join s on s.a > t.a").Check(testkit.Rows(
		"HashLeftJoin_6 99900000.00 root CARTESIAN left outer join, other cond:gt(test.s.a, test.t.a)",
		"├─TableReader_8(Build) 10000.00 root data:TableFullScan_7",
		"│ └─TableFullScan_7 10000.00 cop[tikv] table:t, keep order:false, stats:pseudo",
		"└─TableReader_11(Probe) 9990.00 root data:Selection_10",
		"  └─Selection_10 9990.00 cop[tikv] not(isnull(test.s.a))",
		"    └─TableFullScan_9 10000.00 cop[tikv] table:s, keep order:false, stats:pseudo"))
	tk.MustExec("drop table if exists t, s")
	tk.MustExec("Create table s (a int, b int, key(b))")
	tk.MustExec("Create table t (a int, b int, key(b))")
	tk.MustExec("Insert into s values (1,2),(2,1),(11,11)")
	tk.MustExec("Insert into t values (11,2),(1,2),(5,2)")
	tk.MustQuery("select /*+ INL_HASH_JOIN(s)*/ * from t left join s on s.b=t.b and s.a < t.a;").Sort().Check(testkit.Rows("1 2 <nil> <nil>", "11 2 1 2", "5 2 1 2"))
	tk.MustQuery("explain select /*+ INL_HASH_JOIN(s)*/ * from t left join s on s.b=t.b and s.a < t.a;").Check(testkit.Rows(
		"IndexHashJoin_22 12475.01 root left outer join, inner:IndexLookUp_11, outer key:test.t.b, inner key:test.s.b, other cond:lt(test.s.a, test.t.a)",
		"├─TableReader_24(Build) 10000.00 root data:TableFullScan_23",
		"│ └─TableFullScan_23 10000.00 cop[tikv] table:t, keep order:false, stats:pseudo",
		"└─IndexLookUp_11(Probe) 1.25 root ",
		"  ├─Selection_9(Build) 1.25 cop[tikv] not(isnull(test.s.b))",
		"  │ └─IndexRangeScan_7 1.25 cop[tikv] table:s, index:b, range: decided by [eq(test.s.b, test.t.b)], keep order:false, stats:pseudo",
		"  └─Selection_10(Probe) 1.25 cop[tikv] not(isnull(test.s.a))",
		"    └─TableRowIDScan_8 1.25 cop[tikv] table:s, keep order:false, stats:pseudo"))
}

func (s *testSuiteJoin1) TestIssue13177(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a varchar(20), b int, c int)")
	tk.MustExec("create table t2(a varchar(20), b int, c int, primary key(a, b))")
	tk.MustExec("insert into t1 values(\"abcd\", 1, 1), (\"bacd\", 2, 2), (\"cbad\", 3, 3)")
	tk.MustExec("insert into t2 values(\"bcd\", 1, 1), (\"acd\", 2, 2), (\"bad\", 3, 3)")
	tk.MustQuery("select /*+ inl_join(t1, t2) */ * from t1 join t2 on substr(t1.a, 2, 4) = t2.a and t1.b = t2.b where t1.c between 1 and 5").Check(testkit.Rows(
		"abcd 1 1 bcd 1 1",
		"bacd 2 2 acd 2 2",
		"cbad 3 3 bad 3 3",
	))
	tk.MustQuery("select /*+ inl_hash_join(t1, t2) */ * from t1 join t2 on substr(t1.a, 2, 4) = t2.a and t1.b = t2.b where t1.c between 1 and 5").Check(testkit.Rows(
		"abcd 1 1 bcd 1 1",
		"bacd 2 2 acd 2 2",
		"cbad 3 3 bad 3 3",
	))
	tk.MustQuery("select /*+ inl_merge_join(t1, t2) */ * from t1 join t2 on substr(t1.a, 2, 4) = t2.a and t1.b = t2.b where t1.c between 1 and 5").Check(testkit.Rows(
		"bacd 2 2 acd 2 2",
		"cbad 3 3 bad 3 3",
		"abcd 1 1 bcd 1 1",
	))
	tk.MustQuery("select /*+ inl_join(t1, t2) */ t1.* from t1 join t2 on substr(t1.a, 2, 4) = t2.a and t1.b = t2.b where t1.c between 1 and 5").Check(testkit.Rows(
		"abcd 1 1",
		"bacd 2 2",
		"cbad 3 3",
	))
	tk.MustQuery("select /*+ inl_hash_join(t1, t2) */ t1.* from t1 join t2 on substr(t1.a, 2, 4) = t2.a and t1.b = t2.b where t1.c between 1 and 5").Check(testkit.Rows(
		"bacd 2 2",
		"cbad 3 3",
		"abcd 1 1",
	))
	tk.MustQuery("select /*+ inl_merge_join(t1, t2) */ t1.* from t1 join t2 on substr(t1.a, 2, 4) = t2.a and t1.b = t2.b where t1.c between 1 and 5").Check(testkit.Rows(
		"bacd 2 2",
		"cbad 3 3",
		"abcd 1 1",
	))
}

func (s *testSuiteJoin1) TestIssue14514(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk varchar(14) primary key, a varchar(12));")
	tk.MustQuery("select * from (select t1.pk or '/' as c from t as t1 left join t as t2 on t1.a = t2.pk) as t where t.c = 1;").Check(testkit.Rows())
}

func (s *testSuiteJoinSerial) TestOuterMatchStatusIssue14742(c *C) {
	plannercore.ForceUseOuterBuild4Test = true
	defer func() { plannercore.ForceUseOuterBuild4Test = false }()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists testjoin;")
	tk.MustExec("create table testjoin(a int);")
	tk.Se.GetSessionVars().MaxChunkSize = 2

	tk.MustExec("insert into testjoin values (NULL);")
	tk.MustExec("insert into testjoin values (1);")
	tk.MustExec("insert into testjoin values (2), (2), (2);")
	tk.MustQuery("SELECT * FROM testjoin t1 RIGHT JOIN testjoin t2 ON t1.a > t2.a order by t1.a, t2.a;").Check(testkit.Rows(
		"<nil> <nil>",
		"<nil> 2",
		"<nil> 2",
		"<nil> 2",
		"2 1",
		"2 1",
		"2 1",
	))
}

func (s *testSuiteJoinSerial) TestInlineProjection4HashJoinIssue15316(c *C) {
	// Two necessary factors to reproduce this issue:
	// (1) taking HashLeftJoin, i.e., letting the probing tuple lay at the left side of joined tuples
	// (2) the projection only contains a part of columns from the build side, i.e., pruning the same probe side
	plannercore.ForcedHashLeftJoin4Test = true
	defer func() { plannercore.ForcedHashLeftJoin4Test = false }()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table S (a int not null, b int, c int);")
	tk.MustExec("create table T (a int not null, b int, c int);")
	tk.MustExec("insert into S values (0,1,2),(0,1,null),(0,1,2);")
	tk.MustExec("insert into T values (0,10,2),(0,10,null),(1,10,2);")
	tk.MustQuery("select T.a,T.a,T.c from S join T on T.a = S.a where S.b<T.b order by T.a,T.c;").Check(testkit.Rows(
		"0 0 <nil>",
		"0 0 <nil>",
		"0 0 <nil>",
		"0 0 2",
		"0 0 2",
		"0 0 2",
	))
	// NOTE: the HashLeftJoin should be kept
	tk.MustQuery("explain select T.a,T.a,T.c from S join T on T.a = S.a where S.b<T.b order by T.a,T.c;").Check(testkit.Rows(
		"Sort_8 12487.50 root test.t.a:asc, test.t.c:asc",
		"└─Projection_10 12487.50 root test.t.a, test.t.a, test.t.c",
		"  └─HashLeftJoin_11 12487.50 root inner join, equal:[eq(test.s.a, test.t.a)], other cond:lt(test.s.b, test.t.b)",
		"    ├─TableReader_17(Build) 9990.00 root data:Selection_16",
		"    │ └─Selection_16 9990.00 cop[tikv] not(isnull(test.t.b))",
		"    │   └─TableFullScan_15 10000.00 cop[tikv] table:T, keep order:false, stats:pseudo",
		"    └─TableReader_14(Probe) 9990.00 root data:Selection_13",
		"      └─Selection_13 9990.00 cop[tikv] not(isnull(test.s.b))",
		"        └─TableFullScan_12 10000.00 cop[tikv] table:S, keep order:false, stats:pseudo"))
}
