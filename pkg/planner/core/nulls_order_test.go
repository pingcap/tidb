// Copyright 2026 PingCAP, Inc.
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

package core_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestNullsOrderResults(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, a int, b int not null, key ia(a), key iab(a, b))")
	tk.MustExec("insert into t values (1, 2, 10), (2, null, 20), (3, 1, 30), (4, null, 40), (5, 3, 50)")

	tk.MustQuery("select id from t order by a").Check(testkit.Rows("2", "4", "3", "1", "5"))
	tk.MustQuery("select id from t order by a nulls first, id").Check(testkit.Rows("2", "4", "3", "1", "5"))
	tk.MustQuery("select id from t order by a nulls last, id").Check(testkit.Rows("3", "1", "5", "2", "4"))
	tk.MustQuery("select id from t order by a asc nulls last, id").Check(testkit.Rows("3", "1", "5", "2", "4"))
	tk.MustQuery("select id from t order by a desc nulls first, id").Check(testkit.Rows("2", "4", "5", "1", "3"))
	tk.MustQuery("select id from t order by a desc nulls last, id").Check(testkit.Rows("5", "1", "3", "2", "4"))
	tk.MustQuery("select id from t order by a desc, id").Check(testkit.Rows("5", "1", "3", "2", "4"))

	// TopN, including when pushed down to the coprocessor.
	tk.MustQuery("select id from t order by a nulls last, id limit 4").Check(testkit.Rows("3", "1", "5", "2"))
	tk.MustQuery("select id from t order by a desc nulls first, id limit 3").Check(testkit.Rows("2", "4", "5"))
	tk.MustQuery("select id from t order by a desc nulls first, id limit 2, 2").Check(testkit.Rows("5", "1"))

	// Expressions, positions and aliases.
	tk.MustQuery("select id, a + 1 as x from t order by x nulls last, id").Check(testkit.Rows("3 2", "1 3", "5 4", "2 <nil>", "4 <nil>"))
	tk.MustQuery("select id, a from t order by 2 nulls last, 1").Check(testkit.Rows("3 1", "1 2", "5 3", "2 <nil>", "4 <nil>"))

	// The clause is ignored silently for expressions that cannot be NULL.
	tk.MustQuery("select id from t order by b desc nulls first").Check(testkit.Rows("5", "4", "3", "2", "1"))
	tk.MustQuery("explain format = 'brief' select id from t order by b nulls last").CheckNotContain("isnull")
	tk.MustQuery("explain format = 'brief' select id from t order by id nulls last").CheckNotContain("isnull")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	// ... but not for NOT NULL columns on the inner side of an outer join.
	tk.MustQuery("select t1.id, t2.b from t t1 left join t t2 on t1.id = t2.id + 3 order by t2.b nulls last, t1.id").
		Check(testkit.Rows("4 10", "5 20", "1 <nil>", "2 <nil>", "3 <nil>"))

	// Set operations, DELETE and UPDATE.
	tk.MustQuery("(select a from t where id < 3) union all (select a from t where id >= 3) order by a nulls last").
		Check(testkit.Rows("1", "2", "3", "<nil>", "<nil>"))
	tk.MustQuery("select group_concat(id order by a nulls last, id) from t").Check(testkit.Rows("3,1,5,2,4"))
	tk.MustQuery("select group_concat(id order by a desc nulls first, id) from t").Check(testkit.Rows("2,4,5,1,3"))
	tk.MustExec("update t set b = b + 1 order by a nulls last limit 3")
	tk.MustQuery("select id from t where b % 10 = 1 order by id").Check(testkit.Rows("1", "3", "5"))
	tk.MustExec("delete from t order by a desc nulls first limit 2")
	tk.MustQuery("select id from t order by id").Check(testkit.Rows("1", "3", "5"))

	// Window functions.
	tk.MustExec("insert into t values (2, null, 20), (4, null, 40)")
	tk.MustQuery("select id, row_number() over (order by a nulls last, id) from t order by id").
		Check(testkit.Rows("1 2", "2 4", "3 1", "4 5", "5 3"))
	tk.MustQuery("select id, rank() over (order by a desc nulls first) from t order by id").
		Check(testkit.Rows("1 4", "2 1", "3 5", "4 1", "5 3"))
	tk.MustQuery("select id, sum(id) over (order by a nulls last range between unbounded preceding and current row) from t order by id").
		Check(testkit.Rows("1 4", "2 15", "3 3", "4 15", "5 9"))
	tk.MustGetErrMsg("select sum(id) over (order by a nulls last range between 1 preceding and current row) from t",
		"[planner:1235]This version of TiDB doesn't yet support 'NULLS FIRST/LAST in the ORDER BY of a RANGE frame with an offset'")
	tk.MustQuery("select sum(id) over (order by b nulls last range between 1 preceding and current row) from t order by id").
		Check(testkit.Rows("1", "2", "3", "4", "5"))

	// NULLS FIRST/LAST only applies to ORDER BY.
	tk.MustGetErrMsg("select a from t group by a nulls last",
		"[planner:1235]This version of TiDB doesn't yet support 'NULLS FIRST/LAST in GROUP BY'")
	tk.MustGetErrMsg("select row_number() over (partition by a nulls last) from t",
		"[planner:1235]This version of TiDB doesn't yet support 'NULLS FIRST/LAST in PARTITION BY'")
}

func TestNullsOrderAvoidSort(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, a int, b int, c int, key ia(a), key icab(c, a, b))")
	tk.MustExec("insert into t values (1, 2, null, 1), (2, null, 1, 1), (3, 1, 2, 2), (4, null, null, 2), (5, 3, 1, 1)")
	tk.MustExec("create table tp (id int, a int, key ia(a)) partition by hash(id) partitions 3")
	tk.MustExec("insert into tp select id, a from t")
	tk.MustExec("create table tc (k varchar(10) primary key clustered, a int, key ia(a))")
	tk.MustExec("insert into tc values ('x', 2), ('y', null), ('z', 1), ('w', null)")

	// The index provides the order: its ranges are split into a non-NULL and a NULL
	// group, which are merge sorted with NULLs high.
	for _, tc := range []struct {
		sql  string
		rows []string
	}{
		{"select a from t use index(ia) order by a nulls last", []string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select a from t use index(ia) order by a desc nulls first", []string{"<nil>", "<nil>", "3", "2", "1"}},
		{"select a from t use index(ia) order by a nulls last limit 2", []string{"1", "2"}},
		{"select a from t use index(ia) order by a desc nulls first limit 3", []string{"<nil>", "<nil>", "3"}},
		{"select id from t use index(ia) order by a nulls last, id limit 4", []string{"3", "1", "5", "2"}},
		{"select id from t use index(ia) order by a desc nulls first, id desc limit 3", []string{"4", "2", "5"}},
		{"select a from t use index(ia) where a is null or a < 2 order by a nulls last", []string{"1", "<nil>", "<nil>"}},
		{"select a from t use index(ia) where a > 0 or a is null order by a nulls last", []string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select a from t use index(ia) where a > 1 order by a nulls last", []string{"2", "3"}},
		{"select a, b from t use index(icab) where c = 1 order by a nulls last, b", []string{"2 <nil>", "3 1", "<nil> 1"}},
		{"select c, a, b from t use index(icab) where c in (1, 2) order by a nulls last, b",
			[]string{"2 1 2", "1 2 <nil>", "1 3 1", "2 <nil> <nil>", "1 <nil> 1"}},
		{"select a from tp use index(ia) order by a nulls last limit 4", []string{"1", "2", "3", "<nil>"}},
		{"select k from tc use index(ia) order by a desc nulls first, k desc", []string{"y", "w", "x", "z"}},
		// ISNULL(a), a is recognized too, in both directions.
		{"select a from t use index(ia) order by a is null, a", []string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select a from t use index(ia) order by isnull(a) desc, a", []string{"<nil>", "<nil>", "1", "2", "3"}},
		// Reversing a nullable column keeps NULLs low, so -a DESC is a ASC NULLS LAST.
		{"select a from t use index(ia) order by -a desc", []string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select a from t use index(ia) order by -a", []string{"<nil>", "<nil>", "3", "2", "1"}},
		{"select a from t use index(ia) order by 0-a desc", []string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select id from t use index(ia) order by -a desc, id", []string{"3", "1", "5", "2", "4"}},
		{"select id from t use index(ia) order by -(a+1), id desc", []string{"4", "2", "5", "1", "3"}},
		{"select a, b from t use index(icab) where c = 1 order by -a desc, b", []string{"2 <nil>", "3 1", "<nil> 1"}},
	} {
		tk.MustQuery("explain format = 'brief' " + tc.sql).CheckNotContain("Sort")
		tk.MustQuery("explain format = 'brief' " + tc.sql).CheckNotContain("TopN")
		tk.MustQuery(tc.sql).Check(testkit.Rows(tc.rows...))
	}

	// Cases that still need a sort.
	for _, tc := range []struct {
		sql  string
		rows []string
	}{
		// Only the first sort item can split its NULLs.
		{"select a, b from t use index(icab) where c = 1 order by a nulls last, b nulls last", []string{"2 <nil>", "3 1", "<nil> 1"}},
		// Operators that order rows themselves know only the default NULL order.
		{"select a, count(*) from t use index(ia) group by a order by a nulls last", []string{"1 1", "2 1", "3 1", "<nil> 2"}},
		{"select /*+ merge_join(t1, t2) */ t1.a from t t1 left join t t2 on t1.a = t2.a order by t1.a nulls last",
			[]string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select x.a from (select a from t use index(ia) order by a limit 10) x order by x.a nulls last",
			[]string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select /*+ use_index_merge(t, ia, icab) */ a from t where a = 1 or c = 2 order by a nulls last", []string{"1", "<nil>"}},
	} {
		tk.MustQuery("explain format = 'brief' " + tc.sql).CheckContain("Sort")
		tk.MustQuery(tc.sql).Check(testkit.Rows(tc.rows...))
	}

	// Union scan merges the transaction's rows with the default NULL order.
	tk.MustExec("begin")
	tk.MustExec("insert into t values (6, null, 60, 3), (7, 0, 70, 3)")
	tk.MustQuery("explain format = 'brief' select a from t use index(ia) order by a nulls last").CheckContain("Sort")
	tk.MustQuery("select a from t use index(ia) order by a nulls last").Check(testkit.Rows("0", "1", "2", "3", "<nil>", "<nil>", "<nil>"))
	tk.MustExec("rollback")

	// Correlated ranges are rebuilt, and split again, for every outer row.
	tk.MustQuery("select id, (select t2.a from t t2 use index(icab) where t2.c = t1.c order by t2.a nulls last limit 1) from t t1 order by id").
		Check(testkit.Rows("1 2", "2 2", "3 1", "4 1", "5 2"))
	tk.MustQuery("select id, (select t2.a from t t2 use index(icab) where t2.c = t1.c order by t2.a desc nulls first limit 1) from t t1 order by id").
		Check(testkit.Rows("1 <nil>", "2 <nil>", "3 <nil>", "4 <nil>", "5 <nil>"))
}

func TestNullsOrderPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, a int, c int, key ia(a), key ica(c, a))")
	tk.MustExec("insert into t values (1, 2, 1), (2, null, 1), (3, 1, 2), (4, null, 2), (5, 3, 1)")
	tk.MustExec("set @@tidb_enable_prepared_plan_cache = 1")

	// The cached plan was built for ranges without NULLs; the rebuilt ranges contain them.
	tk.MustExec("prepare st from 'select a from t use index(ia) where a <=> ? or a <=> ? order by a nulls last'")
	tk.MustExec("set @x = 1, @y = 3")
	tk.MustQuery("execute st using @x, @y").Check(testkit.Rows("1", "3"))
	tk.MustQuery("execute st using @x, @y").Check(testkit.Rows("1", "3"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @x = null")
	tk.MustQuery("execute st using @x, @y").Check(testkit.Rows("3", "<nil>", "<nil>"))

	tk.MustExec("prepare st from 'select a from t use index(ica) where c = ? order by a desc nulls first limit 2'")
	tk.MustExec("set @x = 1")
	tk.MustQuery("execute st using @x").Check(testkit.Rows("<nil>", "3"))
	tk.MustExec("set @x = 2")
	tk.MustQuery("execute st using @x").Check(testkit.Rows("<nil>", "1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}
