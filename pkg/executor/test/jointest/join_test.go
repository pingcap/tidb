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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jointest

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

func TestJoin2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set @@tidb_index_lookup_join_concurrency = 200")
	require.Equal(t, 200, tk.Session().GetSessionVars().IndexLookupJoinConcurrency())

	tk.MustExec("set @@tidb_index_lookup_join_concurrency = 4")
	require.Equal(t, 4, tk.Session().GetSessionVars().IndexLookupJoinConcurrency())

	tk.MustExec("set @@tidb_index_lookup_size = 2")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int)")
	tk.MustExec("insert t values (1)")
	tests := []struct {
		sql    string
		result [][]any
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
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, t1) */ * from t join t1 on t.a=t1.a").Sort().Check(testkit.Rows("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, t1) */ * from t join t1 on t.a=t1.a").Check(testkit.Rows("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4"))
	tk.MustQuery("select /*+ INL_JOIN(t) */ * from t1 join t on t.a=t1.a and t.a < t1.b").Check(testkit.Rows("1 2 1 1", "1 3 1 1", "1 4 1 1", "3 4 3 3"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t) */ * from t1 join t on t.a=t1.a and t.a < t1.b").Sort().Check(testkit.Rows("1 2 1 1", "1 3 1 1", "1 4 1 1", "3 4 3 3"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t) */ * from t1 join t on t.a=t1.a and t.a < t1.b").Check(testkit.Rows("1 2 1 1", "1 3 1 1", "1 4 1 1", "3 4 3 3"))
	// Test single index reader.
	tk.MustQuery("select /*+ INL_JOIN(t, t1) */ t1.b from t1 join t on t.b=t1.b").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t, t1) */ t1.b from t1 join t on t.b=t1.b").Sort().Check(testkit.Rows("2", "3"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t, t1) */ t1.b from t1 join t on t.b=t1.b").Check(testkit.Rows("2", "3"))
	tk.MustQuery("select /*+ INL_JOIN(t1) */ * from t right outer join t1 on t.a=t1.a").Sort().Check(testkit.Rows("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4", "<nil> <nil> 4 5"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1) */ * from t right outer join t1 on t.a=t1.a").Sort().Check(testkit.Rows("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4", "<nil> <nil> 4 5"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1) */ * from t right outer join t1 on t.a=t1.a").Sort().Check(testkit.Rows("1 1 1 2", "1 1 1 3", "1 1 1 4", "3 3 3 4", "<nil> <nil> 4 5"))
	tk.MustQuery("select /*+ INL_JOIN(t) */ avg(t.b) from t right outer join t1 on t.a=t1.a").Check(testkit.Rows("1.5000"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t) */ avg(t.b) from t right outer join t1 on t.a=t1.a").Check(testkit.Rows("1.5000"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t) */ avg(t.b) from t right outer join t1 on t.a=t1.a").Check(testkit.Rows("1.5000"))

	// Test that two conflict hints will return warning.
	tk.MustExec("select /*+ TIDB_INLJ(t) TIDB_SMJ(t) */ * from t join t1 on t.a=t1.a")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	tk.MustExec("select /*+ TIDB_INLJ(t) TIDB_HJ(t) */ * from t join t1 on t.a=t1.a")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	tk.MustExec("select /*+ TIDB_SMJ(t) TIDB_HJ(t) */ * from t join t1 on t.a=t1.a")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)

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

func TestJoinLeak(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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
	require.NoError(t, err)
	req := result.NewChunk(nil)
	err = result.Next(context.Background(), req)
	require.NoError(t, err)
	time.Sleep(time.Millisecond)
	require.NoError(t, result.Close())

	tk.MustExec("set @@tidb_hash_join_concurrency=5")
}

func TestNullEmptyAwareSemiJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
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
		result [][]any
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
		result [][]any
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
		result [][]any
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
		result [][]any
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
		result [][]any
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
		result [][]any
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
		result [][]any
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
		result [][]any
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

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int)")
	tk.MustExec("insert into t1 values(1),(2)")
	tk.MustExec("insert into t2 values(1),(null)")
	tk.MustQuery("select * from t1 where a not in (select a from t2 where t1.a = t2.a)").Check(testkit.Rows(
		"2",
	))
	tk.MustQuery("select * from t1 where a != all (select a from t2 where t1.a = t2.a)").Check(testkit.Rows(
		"2",
	))
	tk.MustQuery("select * from t1 where a <> all (select a from t2 where t1.a = t2.a)").Check(testkit.Rows(
		"2",
	))
}

func TestIssue18070(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("SET GLOBAL tidb_mem_oom_action = DEFAULT")
	tk.MustExec("SET GLOBAL tidb_mem_oom_action='CANCEL'")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, index(a))")
	tk.MustExec("create table t2(a int, index(a))")
	tk.MustExec("insert into t1 values(1),(2)")
	tk.MustExec("insert into t2 values(1),(1),(2),(2)")
	tk.MustExec("set @@tidb_mem_quota_query=1000")
	err := tk.ExecToErr("select /*+ inl_hash_join(t1)*/ * from t1 join t2 on t1.a = t2.a;")
	require.True(t, exeerrors.ErrMemoryExceedForQuery.Equal(err))

	fpName := "github.com/pingcap/tidb/pkg/executor/join/mockIndexMergeJoinOOMPanic"
	require.NoError(t, failpoint.Enable(fpName, `panic("ERROR 1105 (HY000): Out Of Memory Quota![conn=1]")`))
	defer func() {
		require.NoError(t, failpoint.Disable(fpName))
	}()
	err = tk.ExecToErr("select /*+ inl_merge_join(t1)*/ * from t1 join t2 on t1.a = t2.a;")
	require.True(t, exeerrors.ErrMemoryExceedForQuery.Equal(err))
}

func TestIssue20779(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b int, index idx(b));")
	tk.MustExec("insert into t1 values(1, 1);")
	tk.MustExec("insert into t1 select * from t1;")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/testIssue20779", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/testIssue20779"))
	}()

	rs, err := tk.Exec("select /*+ inl_hash_join(t2) */ t1.b from t1 left join t1 t2 on t1.b=t2.b order by t1.b;")
	require.NoError(t, err)
	_, err = session.GetRows4Test(context.Background(), nil, rs)
	require.EqualError(t, err, "testIssue20779")
	require.NoError(t, rs.Close())
}

func TestIssue30211(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1(a int, index(a));")
	tk.MustExec("create table t2(a int, index(a));")
	func() {
		fpName := "github.com/pingcap/tidb/pkg/executor/join/TestIssue30211"
		require.NoError(t, failpoint.Enable(fpName, `panic("TestIssue30211 IndexJoinPanic")`))
		defer func() {
			require.NoError(t, failpoint.Disable(fpName))
		}()
		err := tk.QueryToErr("select /*+ inl_join(t1) */ * from t1 join t2 on t1.a = t2.a;")
		require.EqualError(t, err, "failpoint panic: TestIssue30211 IndexJoinPanic")

		err = tk.QueryToErr("select /*+ inl_hash_join(t1) */ * from t1 join t2 on t1.a = t2.a;")
		require.EqualError(t, err, "failpoint panic: TestIssue30211 IndexJoinPanic")
	}()
	tk.MustExec("insert into t1 values(1),(2);")
	tk.MustExec("insert into t2 values(1),(1),(2),(2);")
	tk.MustExec("set @@tidb_mem_quota_query=8000;")
	tk.MustExec("set tidb_index_join_batch_size = 1;")
	tk.MustExec("SET GLOBAL tidb_mem_oom_action = 'CANCEL'")
	defer tk.MustExec("SET GLOBAL tidb_mem_oom_action='LOG'")
	err := tk.QueryToErr("select /*+ inl_join(t1) */ * from t1 join t2 on t1.a = t2.a;")
	require.True(t, exeerrors.ErrMemoryExceedForQuery.Equal(err))
	err = tk.QueryToErr("select /*+ inl_hash_join(t1) */ * from t1 join t2 on t1.a = t2.a;")
	require.True(t, exeerrors.ErrMemoryExceedForQuery.Equal(err))
}

func TestIssue37932(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk2.MustExec("use test")
	tk1.MustExec("create table tbl_1 ( col_1 set ( 'Alice','Bob','Charlie','David' )   not null default 'Alice' ,col_2 tinyint  unsigned ,col_3 decimal ( 34 , 3 )   not null default 79 ,col_4 bigint  unsigned not null ,col_5 bit ( 12 )   not null , unique key idx_1 ( col_2 ) ,unique key idx_2 ( col_2 ) ) charset utf8mb4 collate utf8mb4_bin ;")
	tk1.MustExec("create table tbl_2 ( col_6 text ( 52 ) collate utf8_unicode_ci  not null ,col_7 int  unsigned not null ,col_8 blob ( 369 ) ,col_9 bit ( 51 ) ,col_10 decimal ( 38 , 16 ) , unique key idx_3 ( col_7 ) ,unique key idx_4 ( col_7 ) ) charset utf8 collate utf8_unicode_ci ;")
	tk1.MustExec("create table tbl_3 ( col_11 set ( 'Alice','Bob','Charlie','David' )   not null ,col_12 bigint  unsigned not null default 1678891638492596595 ,col_13 text ( 18 ) ,col_14 set ( 'Alice','Bob','Charlie','David' )   not null default 'Alice' ,col_15 mediumint , key idx_5 ( col_12 ) ,unique key idx_6 ( col_12 ) ) charset utf8mb4 collate utf8mb4_general_ci ;")
	tk1.MustExec("create table tbl_4 ( col_16 set ( 'Alice','Bob','Charlie','David' )   not null ,col_17 tinyint  unsigned ,col_18 int  unsigned not null default 4279145838 ,col_19 varbinary ( 210 )   not null ,col_20 timestamp , primary key  ( col_18 ) /*T![clustered_index] nonclustered */ ,key idx_8 ( col_19 ) ) charset utf8mb4 collate utf8mb4_unicode_ci ;")
	tk1.MustExec("create table tbl_5 ( col_21 bigint ,col_22 set ( 'Alice','Bob','Charlie','David' ) ,col_23 blob ( 311 ) ,col_24 bigint  unsigned not null default 3415443099312152509 ,col_25 time , unique key idx_9 ( col_21 ) ,unique key idx_10 ( col_21 ) ) charset gbk collate gbk_bin ;")
	tk1.MustExec("insert into tbl_1 values ( 'Bob',null,0.04,2650749963804575036,4044 );")
	tk1.MustExec("insert into tbl_1 values ( 'Alice',171,1838.2,6452757231340518222,1190 );")
	tk1.MustExec("insert into tbl_1 values ( 'Bob',202,2.962,4304284252076747481,2112 );")
	tk1.MustExec("insert into tbl_1 values ( 'David',155,32610.05,5899651588546531414,104 );")
	tk1.MustExec("insert into tbl_1 values ( 'Charlie',52,4219.7,6151233689319516187,1246 );")
	tk1.MustExec("insert into tbl_1 values ( 'Bob',55,3963.11,3614977408465893392,1188 );")
	tk1.MustExec("insert into tbl_1 values ( 'Alice',203,72.01,1553550133494908281,1658 );")
	tk1.MustExec("insert into tbl_1 values ( 'Bob',40,871.569,8114062926218465773,1397 );")
	tk1.MustExec("insert into tbl_1 values ( 'Alice',165,7765,4481202107781982005,2089 );")
	tk1.MustExec("insert into tbl_1 values ( 'David',79,7.02,993594504887208796,514 );")
	tk1.MustExec("insert into tbl_2 values ( 'iB_%7c&q!6-gY4bkvg',2064909882,'dLN52t1YZSdJ',2251679806445488,32 );")
	tk1.MustExec("insert into tbl_2 values ( 'h_',1478443689,'EqP+iN=',180492371752598,0.1 );")
	tk1.MustExec("insert into tbl_2 values ( 'U@U&*WKfPzil=6YaDxp',4271201457,'QWuo24qkSSo',823931105457505,88514 );")
	tk1.MustExec("insert into tbl_2 values ( 'FR4GA=',505128825,'RpEmV6ph5Z7',568030123046798,609381 );")
	tk1.MustExec("insert into tbl_2 values ( '3GsU',166660047,'',1061132816887762,6.4605 );")
	tk1.MustExec("insert into tbl_2 values ( 'BA4hPRD0lm*pbg#NE',3440634757,'7gUPe2',288001159469205,6664.9 );")
	tk1.MustExec("insert into tbl_2 values ( '+z',2117152318,'WTkD(N',215697667226264,7.88 );")
	tk1.MustExec("insert into tbl_2 values ( 'x@SPhy9lOomPa4LF',2881759652,'ETUXQQ0b4HnBSKgTWIU',153379720424625,null );")
	tk1.MustExec("insert into tbl_2 values ( '',2075177391,'MPae!9%ufd',115899580476733,341.23 );")
	tk1.MustExec("insert into tbl_2 values ( '~udi',1839363347,'iQj$$YsZc5ULTxG)yH',111454353417190,6.6 );")
	tk1.MustExec("insert into tbl_3 values ( 'Alice',7032411265967085555,'P7*KBZ159','Alice',7516989 );")
	tk1.MustExec("insert into tbl_3 values ( 'David',486417871670147038,'','Charlie',-2135446 );")
	tk1.MustExec("insert into tbl_3 values ( 'Charlie',5784081664185069254,'7V_&YzKM~Q','Charlie',5583839 );")
	tk1.MustExec("insert into tbl_3 values ( 'David',6346366522897598558,')Lp&$2)SC@','Bob',2522913 );")
	tk1.MustExec("insert into tbl_3 values ( 'Charlie',224922711063053272,'gY','David',6624398 );")
	tk1.MustExec("insert into tbl_3 values ( 'Alice',4678579167560495958,'fPIXY%R8WyY(=u&O','David',-3267160 );")
	tk1.MustExec("insert into tbl_3 values ( 'David',8817108026311573677,'Cs0dZW*SPnKhV1','Alice',2359718 );")
	tk1.MustExec("insert into tbl_3 values ( 'Bob',3177426155683033662,'o2=@zv2qQDhKUs)4y','Bob',-8091802 );")
	tk1.MustExec("insert into tbl_3 values ( 'Bob',2543586640437235142,'hDa*CsOUzxmjf2m','Charlie',-8091935 );")
	tk1.MustExec("insert into tbl_3 values ( 'Charlie',6204182067887668945,'DX-!=)dbGPQO','David',-1954600 );")
	tk1.MustExec("insert into tbl_4 values ( 'David',167,576262750,'lX&x04W','2035-09-28' );")
	tk1.MustExec("insert into tbl_4 values ( 'Charlie',236,2637776757,'92OhsL!w%7','2036-02-08' );")
	tk1.MustExec("insert into tbl_4 values ( 'Bob',68,1077999933,'M0l','1997-09-16' );")
	tk1.MustExec("insert into tbl_4 values ( 'Charlie',184,1280264753,'FhjkfeXsK1Q(','2030-03-16' );")
	tk1.MustExec("insert into tbl_4 values ( 'Alice',10,2150711295,'Eqip)^tr*MoL','2032-07-02' );")
	tk1.MustExec("insert into tbl_4 values ( 'Bob',108,2421602476,'Eul~~Df_Q8s&I3Y-7','2019-06-10' );")
	tk1.MustExec("insert into tbl_4 values ( 'Alice',36,2811198561,'%XgRou0#iKtn*','2022-06-13' );")
	tk1.MustExec("insert into tbl_4 values ( 'Charlie',115,330972286,'hKeJS','2000-11-15' );")
	tk1.MustExec("insert into tbl_4 values ( 'Alice',6,2958326555,'c6+=1','2001-02-11' );")
	tk1.MustExec("insert into tbl_4 values ( 'Alice',99,387404826,'figc(@9R*k3!QM_Vve','2036-02-17' );")
	tk1.MustExec("insert into tbl_5 values ( -401358236474313609,'Charlie','4J$',701059766304691317,'08:19:10.00' );")
	tk1.MustExec("insert into tbl_5 values ( 2759837898825557143,'Bob','E',5158554038674310466,'11:04:03.00' );")
	tk1.MustExec("insert into tbl_5 values ( 273910054423832204,'Alice',null,8944547065167499612,'08:02:30.00' );")
	tk1.MustExec("insert into tbl_5 values ( 2875669873527090798,'Alice','4^SpR84',4072881341903432150,'18:24:55.00' );")
	tk1.MustExec("insert into tbl_5 values ( -8446590100588981557,'David','yBj8',8760380566452862549,'09:01:10.00' );")
	tk1.MustExec("insert into tbl_5 values ( -1075861460175889441,'Charlie','ti11Pl0lJ',9139997565676405627,'08:30:14.00' );")
	tk1.MustExec("insert into tbl_5 values ( 95663565223131772,'Alice','6$',8467839300407531400,'23:31:42.00' );")
	tk1.MustExec("insert into tbl_5 values ( -5661709703968335255,'Charlie','',8122758569495329946,'19:36:24.00' );")
	tk1.MustExec("insert into tbl_5 values ( 3338588216091909518,'Bob','',6558557574025196860,'15:22:56.00' );")
	tk1.MustExec("insert into tbl_5 values ( 8918630521194612922,'David','I$w',5981981639362947650,'22:03:24.00' );")
	tk1.MustExec("begin pessimistic;")
	tk1.MustExec("insert ignore into tbl_1 set col_1 = 'David', col_2 = 110, col_3 = 37065, col_4 = 8164500960513474805, col_5 = 1264 on duplicate key update col_3 = 22151.5, col_4 = 6266058887081523571, col_5 = 3254, col_2 = 59, col_1 = 'Bob';")
	tk1.MustExec("insert  into tbl_4 (col_16,col_17,col_18,col_19,col_20) values ( 'Charlie',34,2499970462,'Z','1978-10-27' ) ,( 'David',217,1732485689,'*)~@@Q8ryi','2004-12-01' ) ,( 'Charlie',40,1360558255,'H(Y','1998-06-25' ) ,( 'Alice',108,2973455447,'%CcP4$','1979-03-28' ) ,( 'David',9,3835209932,'tdKXUzLmAzwFf$','2009-03-03' ) ,( 'David',68,163270003,'uimsclz@FQJN','1988-09-11' ) ,( 'Alice',76,297067264,'BzFF','1989-01-05' ) on duplicate key update col_16 = 'Charlie', col_17 = 14, col_18 = 4062155275, col_20 = '2002-03-07', col_19 = 'tmvchLzp*o8';")
	tk2.MustExec("delete from tbl_3 where tbl_3.col_13 in ( null ,'' ,'g8EEzUU7LQ' ,'~fC3&B*cnOOx_' ,'%RF~AFto&x' ,'NlWkMWG^00' ,'e^4o2Ji^q_*Fa52Z' ) ;")
	tk2.MustExec("delete from tbl_5 where not( tbl_5.col_21 between -1075861460175889441 and 3338588216091909518 ) ;")
	tk1.MustExec("replace into tbl_1 (col_1,col_2,col_3,col_4,col_5) values ( 'Alice',83,8.33,4070808626051569664,455 ) ,( 'Alice',53,2.8,2763362085715461014,1912 ) ,( 'David',178,4242.8,962727993466011464,1844 ) ,( 'Alice',16,650054,5638988670318229867,565 ) ,( 'Alice',76,89783.1,3968605744540056024,2563 ) ,( 'Bob',120,0.89,1003144931151245839,2670 );")
	tk1.MustExec("delete from tbl_5 where col_24 is null ;")
	tk1.MustExec("delete from tbl_3 where tbl_3.col_11 in ( 'Alice' ,'Bob' ,'Alice' ) ;")
	tk2.MustExec("insert  into tbl_3 set col_11 = 'Bob', col_12 = 5701982550256146475, col_13 = 'Hhl)yCsQ2K3cfc^', col_14 = 'Alice', col_15 = -3718868 on duplicate key update col_15 = 7210750, col_12 = 6133680876296985245, col_14 = 'Alice', col_11 = 'David', col_13 = 'F+RMGE!_2^Cfr3Fw';")
	tk2.MustExec("insert ignore into tbl_5 set col_21 = 2439343116426563397, col_22 = 'Charlie', col_23 = '~Spa2YzRFFom16XD', col_24 = 5571575017340582365, col_25 = '13:24:38.00' ;")
	err := tk1.ExecToErr("update tbl_4 set tbl_4.col_20 = '2006-01-24' where tbl_4.col_18 in ( select col_11 from tbl_3 where IsNull( tbl_4.col_16 ) or not( tbl_4.col_19 in ( select col_3 from tbl_1 where tbl_4.col_16 between 'Alice' and 'David' and tbl_4.col_19 <= '%XgRou0#iKtn*' ) ) ) ;")
	if err != nil {
		print(err.Error())
		if strings.Contains(err.Error(), "Truncated incorrect DOUBLE value") {
			t.Log("Truncated incorrect DOUBLE value is within expectations, skipping")
			return
		}
	}
	require.NoError(t, err)
}

func TestIssue49033(t *testing.T) {
	val := runtime.GOMAXPROCS(1)
	defer func() {
		runtime.GOMAXPROCS(val)
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t, s;")
	tk.MustExec("create table t(a int, index(a));")
	tk.MustExec("create table s(a int, index(a));")
	tk.MustExec("insert into t values(1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15), (16), (17), (18), (19), (20), (21), (22), (23), (24), (25), (26), (27), (28), (29), (30), (31), (32), (33), (34), (35), (36), (37), (38), (39), (40), (41), (42), (43), (44), (45), (46), (47), (48), (49), (50), (51), (52), (53), (54), (55), (56), (57), (58), (59), (60), (61), (62), (63), (64), (65), (66), (67), (68), (69), (70), (71), (72), (73), (74), (75), (76), (77), (78), (79), (80), (81), (82), (83), (84), (85), (86), (87), (88), (89), (90), (91), (92), (93), (94), (95), (96), (97), (98), (99), (100), (101), (102), (103), (104), (105), (106), (107), (108), (109), (110), (111), (112), (113), (114), (115), (116), (117), (118), (119), (120), (121), (122), (123), (124), (125), (126), (127), (128);")
	tk.MustExec("insert into s values(1), (128);")
	tk.MustExec("set @@tidb_max_chunk_size=32;")
	tk.MustExec("set @@tidb_index_lookup_join_concurrency=1;")
	tk.MustExec("set @@tidb_index_join_batch_size=32;")
	tk.MustQuery("select /*+ INL_HASH_JOIN(s) */ * from t join s on t.a=s.a;")
	tk.MustQuery("select /*+ INL_HASH_JOIN(s) */ * from t join s on t.a=s.a order by t.a;")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/testIssue49033", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/testIssue49033"))
	}()

	rs, err := tk.Exec("select /*+ INL_HASH_JOIN(s) */ * from t join s on t.a=s.a order by t.a;")
	require.NoError(t, err)
	_, err = session.GetRows4Test(context.Background(), nil, rs)
	require.EqualError(t, err, "testIssue49033")
	require.NoError(t, rs.Close())

	rs, err = tk.Exec("select /*+ INL_HASH_JOIN(s) */ * from t join s on t.a=s.a")
	require.NoError(t, err)
	_, err = session.GetRows4Test(context.Background(), nil, rs)
	require.EqualError(t, err, "testIssue49033")
	require.NoError(t, rs.Close())
}
