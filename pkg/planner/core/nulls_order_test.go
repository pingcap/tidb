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

func TestNullsHighOrderAvoidSort(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, a int, b int, c int, key ia(a), key icab(c, a, b))")
	tk.MustExec("insert into t values (1, 2, null, 1), (2, null, 1, 1), (3, 1, 2, 2), (4, null, null, 2), (5, 3, 1, 1)")
	tk.MustExec("create table tp (id int, a int, key ia(a)) partition by hash(id) partitions 3")
	tk.MustExec("insert into tp select id, a from t")
	tk.MustExec("create table tc (k varchar(10) primary key clustered, a int, key ia(a))")
	tk.MustExec("insert into tc values ('x', 2), ('y', null), ('z', 1), ('w', null)")

	// `ISNULL(a), a` (MySQL's idiom for NULLS LAST) orders like a with NULLs high.
	// The index provides the order: its ranges are split into a non-NULL and a NULL
	// group, which are merge sorted with NULLs high.
	for _, tc := range []struct {
		sql  string
		rows []string
	}{
		{"select a from t use index(ia) order by a is null, a", []string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select a from t use index(ia) order by a is null desc, a desc", []string{"<nil>", "<nil>", "3", "2", "1"}},
		{"select a from t use index(ia) order by a is null, a limit 2", []string{"1", "2"}},
		{"select a from t use index(ia) order by a is null desc, a desc limit 3", []string{"<nil>", "<nil>", "3"}},
		{"select id from t use index(ia) order by a is null, a, id limit 4", []string{"3", "1", "5", "2"}},
		{"select id from t use index(ia) order by a is null desc, a desc, id desc limit 3", []string{"4", "2", "5"}},
		{"select a from t use index(ia) where a is null or a < 2 order by a is null, a", []string{"1", "<nil>", "<nil>"}},
		{"select a from t use index(ia) where a > 0 or a is null order by a is null, a", []string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select a from t use index(ia) where a > 1 order by a is null, a", []string{"2", "3"}},
		{"select a, b from t use index(icab) where c = 1 order by a is null, a, b", []string{"2 <nil>", "3 1", "<nil> 1"}},
		{"select c, a, b from t use index(icab) where c in (1, 2) order by a is null, a, b",
			[]string{"2 1 2", "1 2 <nil>", "1 3 1", "2 <nil> <nil>", "1 <nil> 1"}},
		{"select a from tp use index(ia) order by a is null, a limit 4", []string{"1", "2", "3", "<nil>"}},
		{"select k from tc use index(ia) order by a is null desc, a desc, k desc", []string{"y", "w", "x", "z"}},
		// ISNULL(a) in the opposite direction of a puts NULLs at the other end.
		{"select a from t use index(ia) order by isnull(a) desc, a", []string{"<nil>", "<nil>", "1", "2", "3"}},
		// So does a CASE or IF that maps the NULL test to two distinct literals.
		{"select a from t use index(ia) order by case when a is null then 1 else 0 end, a", []string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select a from t use index(ia) order by case when a is null then 1 else 0 end, a limit 2", []string{"1", "2"}},
		{"select a from t use index(ia) order by case when a is null then 1 else 0 end desc, a desc", []string{"<nil>", "<nil>", "3", "2", "1"}},
		{"select a from t use index(ia) order by case when a is null then 0 else 1 end, a desc", []string{"<nil>", "<nil>", "3", "2", "1"}},
		{"select a from t use index(ia) order by case when a is null then 0 else 1 end desc, a", []string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select a from t use index(ia) order by case when a is not null then 0 else 1 end, a", []string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select a from t use index(ia) order by case when a is null then 1 end, a", []string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select a from t use index(ia) order by case when a is null then 'z' else 'a' end, a", []string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select a from t use index(ia) order by if(a is null, 1, 0), a", []string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select a from t use index(ia) order by if(a is null, 0, 1), a", []string{"<nil>", "<nil>", "1", "2", "3"}},
		{"select a from t use index(ia) order by a is not null, a", []string{"<nil>", "<nil>", "1", "2", "3"}},
		{"select id from t use index(ia) order by case when a is null then 1 else 0 end, a, id limit 4", []string{"3", "1", "5", "2"}},
		{"select a, b from t use index(icab) where c = 1 order by case when a is null then 1 else 0 end, a, b", []string{"2 <nil>", "3 1", "<nil> 1"}},
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
		{"select a, b from t use index(icab) where c = 1 order by a is null, a, b is null, b", []string{"2 <nil>", "3 1", "<nil> 1"}},
		// Operators that order rows themselves know only the default NULL order.
		{"select a, count(*) from t use index(ia) group by a order by a is null, a", []string{"1 1", "2 1", "3 1", "<nil> 2"}},
		{"select /*+ merge_join(t1, t2) */ t1.a from t t1 left join t t2 on t1.a = t2.a order by t1.a is null, t1.a",
			[]string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select x.a from (select a from t use index(ia) order by a limit 10) x order by x.a is null, x.a",
			[]string{"1", "2", "3", "<nil>", "<nil>"}},
		{"select /*+ use_index_merge(t, ia, icab) */ a from t where a = 1 or c = 2 order by a is null, a", []string{"1", "<nil>"}},
		// The CASE must take two distinct literal values, decided by the NULL test of the next column alone.
		{"select a from t use index(ia) order by case when a is null then 1 else 1 end, a", []string{"<nil>", "<nil>", "1", "2", "3"}},
		{"select a from t use index(ia) order by case when a is null then b else 0 end, a", []string{"<nil>", "1", "2", "3", "<nil>"}},
		{"select a from t use index(ia) order by case when a is null then 1 when a > 2 then 2 else 0 end, a", []string{"1", "2", "<nil>", "<nil>", "3"}},
		{"select a from t use index(ia) order by case when b is null then 1 else 0 end, a", []string{"<nil>", "1", "3", "<nil>", "2"}},
		{"select a from t use index(ia) order by case when a is null then 1 else 0 end, b, a", []string{"2", "3", "1", "<nil>", "<nil>"}},
	} {
		tk.MustQuery("explain format = 'brief' " + tc.sql).CheckContain("Sort")
		tk.MustQuery(tc.sql).Check(testkit.Rows(tc.rows...))
	}

	// Union scan merges the transaction's rows with the default NULL order.
	tk.MustExec("begin")
	tk.MustExec("insert into t values (6, null, 60, 3), (7, 0, 70, 3)")
	tk.MustQuery("explain format = 'brief' select a from t use index(ia) order by a is null, a").CheckContain("Sort")
	tk.MustQuery("select a from t use index(ia) order by a is null, a").Check(testkit.Rows("0", "1", "2", "3", "<nil>", "<nil>", "<nil>"))
	tk.MustExec("rollback")

	// Correlated ranges are rebuilt, and split again, for every outer row.
	tk.MustQuery("select id, (select t2.a from t t2 use index(icab) where t2.c = t1.c order by t2.a is null, t2.a limit 1) from t t1 order by id").
		Check(testkit.Rows("1 2", "2 2", "3 1", "4 1", "5 2"))
	tk.MustQuery("select id, (select t2.a from t t2 use index(icab) where t2.c = t1.c order by t2.a is null desc, t2.a desc limit 1) from t t1 order by id").
		Check(testkit.Rows("1 <nil>", "2 <nil>", "3 <nil>", "4 <nil>", "5 <nil>"))
}

func TestNullsHighOrderPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, a int, c int, key ia(a), key ica(c, a))")
	tk.MustExec("insert into t values (1, 2, 1), (2, null, 1), (3, 1, 2), (4, null, 2), (5, 3, 1)")
	tk.MustExec("set @@tidb_enable_prepared_plan_cache = 1")

	// The cached plan was built for ranges without NULLs; the rebuilt ranges contain them.
	tk.MustExec("prepare st from 'select a from t use index(ia) where a <=> ? or a <=> ? order by a is null, a'")
	tk.MustExec("set @x = 1, @y = 3")
	tk.MustQuery("execute st using @x, @y").Check(testkit.Rows("1", "3"))
	tk.MustQuery("execute st using @x, @y").Check(testkit.Rows("1", "3"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @x = null")
	tk.MustQuery("execute st using @x, @y").Check(testkit.Rows("3", "<nil>", "<nil>"))

	tk.MustExec("prepare st from 'select a from t use index(ica) where c = ? order by a is null desc, a desc limit 2'")
	tk.MustExec("set @x = 1")
	tk.MustQuery("execute st using @x").Check(testkit.Rows("<nil>", "3"))
	tk.MustExec("set @x = 2")
	tk.MustQuery("execute st using @x").Check(testkit.Rows("<nil>", "1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// The CASE values are parameters, so the cached plan must not rely on their order.
	tk.MustExec("prepare st from 'select a from t use index(ia) order by case when a is null then ? else ? end, a'")
	tk.MustExec("set @x = 1, @y = 0")
	tk.MustQuery("execute st using @x, @y").Check(testkit.Rows("1", "2", "3", "<nil>", "<nil>"))
	tk.MustExec("set @x = 0, @y = 1")
	tk.MustQuery("execute st using @x, @y").Check(testkit.Rows("<nil>", "<nil>", "1", "2", "3"))
}
