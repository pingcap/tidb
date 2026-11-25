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

package schema

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

// TestUsingJoinNameScopeAfterSubqueryRewrite checks that a qualified column of
// the right side of a JOIN ... USING keeps resolving to that join after an IN or
// EXISTS predicate in the same query block is planned as a semi join or as an
// inner join. The outer derived table also names the column "b", so a lost
// lookup silently reads the outer value instead of failing.
func TestUsingJoinNameScopeAfterSubqueryRewrite(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table a (x int, s varchar(8) collate utf8mb4_general_ci)")
	tk.MustExec("create table b (x int)")
	tk.MustExec("create table c (s varchar(8) collate utf8mb4_general_ci)")
	tk.MustExec("create table c2 (s varchar(8) collate utf8mb4_general_ci)")
	tk.MustExec("insert into a values (1, 'a')")
	tk.MustExec("insert into b values (1)")
	tk.MustExec("insert into c values ('A')")
	tk.MustExec("insert into c2 values ('zz')")
	tk.MustExec("create table d (x int, s varchar(8) collate utf8mb4_general_ci)")
	tk.MustExec("insert into d values (1, 'A'), (2, 'zz')")

	predicates := []string{
		"a.s in (select s from c)",
		"a.s not in (select s from c2)",
		"exists (select 1 from c)",
		"not exists (select 1 from c2 where c2.s = 'none')",
		"a.s in (select z.s from a as z where z.x = a.x)",
	}
	for _, joinToAgg := range []string{"0", "1"} {
		tk.MustExec("set session tidb_opt_insubq_to_join_and_agg = " + joinToAgg)
		for _, pred := range predicates {
			// The scalar subquery is the case from the issue: b.x is read one query
			// block below the join that defines it.
			sql := fmt.Sprintf("select (select (select b.x) from a join b using (x) where %s) from (select 99 as x) as b", pred)
			tk.MustQuery(sql).Check(testkit.Rows("1"))
			// The reference in the join's own query block.
			sql = fmt.Sprintf("select (select b.x from a join b using (x) where %s) from (select 99 as x) as b", pred)
			tk.MustQuery(sql).Check(testkit.Rows("1"))
		}
		// A left join USING, a GROUP BY over the semi join, and an IN used as a
		// value, which adds a mark column to the semi join.
		tk.MustQuery("select (select (select b.x) from a left join b using (x) where a.s in (select s from c)) from (select 99 as x) as b").Check(testkit.Rows("1"))
		tk.MustQuery("select (select count(*) from a join b using (x) where a.s in (select s from c) group by b.x) from (select 99 as x) as b").Check(testkit.Rows("1"))
		tk.MustQuery("select (select concat(a.x in (select 1), '-', b.x) from a join b using (x)) from (select 99 as x) as b").Check(testkit.Rows("1-1"))
		// Two adjacent predicates: the second one is planned over the apply the
		// first one produced.
		tk.MustQuery(`select (
  select b.x from a join b using (x)
  where a.s in (select z.s from a as z where z.x = a.x) and a.x in (select 1)
) from (select 42 as x) as b`).Check(testkit.Rows("1"))
		// The IN subquery itself reads the right-hand USING column of the outer
		// join. That column is not in the join's output schema, so it must still
		// count as a correlated column; otherwise the IN becomes an inner join and
		// aggregation that leaves the reference unbound and the rows vanish.
		tk.MustQuery("select a.x from a join b using (x) where a.s in (select d.s from d where d.x = b.x)").Check(testkit.Rows("1"))
		tk.MustQuery("select (select a.x from a join b using (x) where a.s in (select d.s from d where d.x = b.x)) from (select 99 as x) as b").Check(testkit.Rows("1"))
		tk.MustQuery("select a.x from a join b using (x) where a.s in (select d.s from d where d.x = b.x + 1)").Check(testkit.Rows())
		tk.MustQuery("select a.x from a join b on a.x = b.x where a.s in (select d.s from d where d.x = b.x)").Check(testkit.Rows("1"))
		// The same reference when the subquery stays an apply instead of becoming a join.
		tk.MustQuery("select /*+ NO_DECORRELATE() */ a.x from a join b using (x) where a.s in (select d.s from d where d.x = b.x)").Check(testkit.Rows("1"))
		tk.MustQuery("select a.x from a join b using (x) where a.s in (select d.s from d where d.x = b.x limit 1)").Check(testkit.Rows("1"))
	}
}
