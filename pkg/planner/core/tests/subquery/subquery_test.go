// Copyright 2025 PingCAP, Inc.
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

package subquery

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCollateSubQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, col varchar(100), key ix(col)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("create table t1(id varchar(100)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	samePlan := testkit.Rows(
		"IndexHashJoin root  inner join, inner:IndexLookUp, outer key:Column, inner key:test.t.col, equal cond:eq(Column, test.t.col)",
		"├─HashAgg(Build) root  group by:Column, funcs:firstrow(Column)->Column",
		"│ └─TableReader root  data:HashAgg",
		"│   └─HashAgg cop[tikv]  group by:cast(test.t1.id, var_string(100)), ",
		"│     └─Selection cop[tikv]  not(isnull(cast(test.t1.id, var_string(100))))",
		"│       └─TableFullScan cop[tikv] table:t1 keep order:false, stats:pseudo",
		"└─IndexLookUp(Probe) root  ",
		"  ├─Selection(Build) cop[tikv]  not(isnull(test.t.col))",
		"  │ └─IndexRangeScan cop[tikv] table:t, index:ix(col) range: decided by [eq(test.t.col, Column)], keep order:false, stats:pseudo",
		"  └─TableRowIDScan(Probe) cop[tikv] table:t keep order:false, stats:pseudo")
	tk.MustQuery(`explain format="plan_tree" select * from t use index(ix) where col in (select cast(id as char) from t1);`).
		Check(samePlan)
	tk.MustExec(`set collation_connection='utf8_bin';`)
	tk.MustQuery(`explain format="plan_tree" select * from t use index(ix) where col in (select cast(id as char) from t1);`).
		Check(samePlan)
	tk.MustExec(`set collation_connection='latin1_bin';`)
	tk.MustQuery(`explain format="plan_tree" select * from t use index(ix) where col in (select cast(id as char) from t1);`).
		Check(samePlan)
}

func TestInSubqueryRewrite(t *testing.T) {
	store := testkit.CreateMockStore(t)
	newTestKit := func(t *testing.T) *testkit.TestKit {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		return tk
	}
	setup := newTestKit(t)
	setup.MustExec("create table t_outer (id int)")
	setup.MustExec("insert into t_outer values (1), (2), (3), (4)")

	t.Run("converted keys", func(t *testing.T) {
		tk := newTestKit(t)
		tk.MustExec("create table t_inner (a blob, b int)")
		tk.MustExec("insert into t_inner values ('x', 1), ('y', 1), ('z', 1)")

		tk.MustQuery("select count(*) from t_outer where 0 in (select a from t_inner)").Check(testkit.Rows("4"))
		tk.MustQuery("select count(*) from t_outer where (0, 1) in (select a, b from t_inner)").Check(testkit.Rows("4"))
		tk.MustQuery("select id from t_outer where (id, 0) in (select b, a from t_inner)").Check(testkit.Rows("1"))

		tk.MustExec("delete from t_inner")
		tk.MustExec("insert into t_inner values ('2', 2), ('02', 2)")
		query := "select id from t_outer where (id, id) in (select a, b from t_inner)"
		tk.MustQuery(query).Check(testkit.Rows("2"))
		// The join sees the projected RHS key. Its cast is below DISTINCT.
		tk.MustQuery("show warnings").Check(testkit.Rows())
		tk.MustExec("set tidb_opt_insubq_to_join_and_agg = 0")
		tk.MustQuery(query).Check(testkit.Rows("2"))
		tk.MustQuery("show warnings").Check(testkit.Rows(
			"Warning 1105 Implicit type or collation conversion on join keys (test.t_outer.id = test.t_inner.a) may make indexes unusable"))
		tk.MustExec("set tidb_opt_insubq_to_join_and_agg = default")

		tk.MustExec("delete from t_inner")
		tk.MustExec("insert into t_inner values ('x', 1)")
		for _, predicate := range []string{"id in (select a from t_inner)", "(id, 1) in (select a, b from t_inner)"} {
			tk.MustQuery("select id from t_outer where " + predicate).Check(testkit.Rows())
			tk.MustQuery("show warnings").CheckAt([]int{0, 1}, testkit.Rows("Warning 1292"))
		}
	})

	t.Run("USING column binding", func(t *testing.T) {
		tk := newTestKit(t)
		tk.MustExec("create table a (x int)")
		tk.MustExec("create table b (x int)")
		tk.MustExec("create table c (k int)")
		tk.MustExec("insert into a values (1)")
		tk.MustExec("insert into b values (1)")
		tk.MustExec("insert into c values (0)")
		// The qualified b.x belongs to the USING join in the inner query block.
		tk.MustQuery(`select (select b.x from a join b using (x)
			where floor(rand()) in (select k from c) limit 1) from (select 99 as x) b`).Check(testkit.Rows("1"))
	})

	t.Run("volatile tuple operand", func(t *testing.T) {
		tk := newTestKit(t)
		tk.MustExec("create table lhs (x int)")
		tk.MustExec("create table rhs (j int)")
		tk.MustExec("insert into lhs values (42)")
		tk.MustExec("insert into rhs values (2)")
		// Both tuple elements refer to the same k and FLOOR(RAND()) is always zero.
		tk.MustQuery(`select x from (select x, ceil(rand(1)*2) as k from lhs) d
			where (k+floor(rand(10)), k+floor(rand(1))) in (select j-1, j from rhs)`).Check(testkit.Rows())
		tk.MustQuery(`explain format='brief' select x from (select x, ceil(rand(1)*2) as k from lhs) d
			where (k,k) in (select j-1, j from rhs)`).CheckContain("semi join")
	})

	t.Run("query block hints", func(t *testing.T) {
		tk := newTestKit(t)
		tk.MustExec("create table l (id int primary key, k int, x int)")
		tk.MustExec("create table r (k int, s blob, s2 blob, key(k))")
		// Comparing x with s adds a projection inside the subquery.
		query := "explain format='brief' select /*+ INL_JOIN(r@%s) */ id from l where (k,x) in (select k,s from r)"
		tk.MustQuery(fmt.Sprintf(query, "sel_2")).CheckContain("IndexRangeScan")
		tk.MustQuery("show warnings").Check(testkit.Rows())
		tk.MustQuery(fmt.Sprintf(query, "sel_1"))
		tk.MustQuery("show warnings").CheckAt([]int{1}, testkit.Rows("1815"))

		// Even if both keys are cast, the RHS still belongs to r.
		tk.MustQuery(`explain format='brief' select /*+ MERGE_JOIN(r@sel_2) */ id from l
			where (k,x) in (select s,s2 from r)`).CheckContain("MergeJoin")
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// Casting hint_src.s must not make a multi-table RHS match a hint on r alone.
		tk.MustExec("create table hint_src (k int, s blob)")
		tk.MustQuery(`explain format='brief' select /*+ MERGE_JOIN(r@sel_2) */ id from l
			where (l.k,l.x) in (select r.k,hint_src.s from r join hint_src on r.k=hint_src.k)`)
		tk.MustQuery("show warnings").CheckAt([]int{1}, testkit.Rows("1815"))
	})

	t.Run("UTC timestamp lookup", func(t *testing.T) {
		tk := newTestKit(t)
		tk.MustExec("set time_zone='+00:00'")
		tk.MustExec("create table ts_lookup (id int, ts timestamp not null, key(ts))")
		tk.MustExec("create table ts_probe (ts timestamp primary key)")
		query := "select /*+ INL_JOIN(ts_lookup) */ id from ts_lookup where ts in (select ts from ts_probe)"
		tk.MustQuery("explain format='brief' " + query).CheckContain("IndexRangeScan")
	})

	t.Run("volatile projections", func(t *testing.T) {
		tk := newTestKit(t)
		tk.MustExec("create table seq_input (a blob, b int)")
		tk.MustExec("insert into seq_input values ('1', 1), ('1', 1)")
		query := "select id from t_outer where id in (select b from (select b, rand() as unused from seq_input) d)"
		tk.MustQuery("explain format='brief' " + query).CheckContain("inner join")
		tk.MustQuery(query).Check(testkit.Rows("1"))
		for _, predicate := range []string{
			"id in (select if(mod(nextval(seq), 3) = 0, cast('2' as binary), a) from seq_input)",
			"(id, 1) in (select if(mod(nextval(seq), 3) = 0, cast('2' as binary), a), b from seq_input)",
		} {
			tk.MustExec("create sequence seq")
			tk.MustQuery("select id from t_outer where " + predicate).Check(testkit.Rows("1"))
			tk.MustQuery("select lastval(seq)").Check(testkit.Rows("2"))
			tk.MustExec("drop sequence seq")
		}

		// A constant IN operand can push the comparison below DISTINCT.
		tk.MustExec("create table seq_rhs (a varchar(5) collate utf8mb4_bin)")
		tk.MustExec("insert into seq_rhs values ('1'), ('1'), ('1'), ('1')")
		for _, predicate := range []string{
			"1 in (select if(mod(nextval(seq), 3) = 0, '2', a) from seq_rhs)",
			"(1, id) in (select k, 1 from (select if(mod(nextval(seq), 3) = 0, '2', a) as k from seq_rhs) d)",
		} {
			tk.MustExec("create sequence seq")
			tk.MustQuery("select id from t_outer where id = 1 and " + predicate).Check(testkit.Rows("1"))
			tk.MustQuery("select lastval(seq)").Check(testkit.Rows("4"))
			tk.MustExec("drop sequence seq")
		}
	})

	t.Run("JSON and vector keys", func(t *testing.T) {
		tk := newTestKit(t)
		tk.MustExec("create table vectors (s varchar(20))")
		tk.MustExec("insert into vectors values ('[0]'), ('[-1e-60]')")
		tk.MustQuery("select count(*) from (select 1) o where vec_from_text('[0]') in (select s from vectors)").Check(testkit.Rows("1"))

		// DISTINCT cannot reproduce approximate JSON equality or vector signed-zero equality.
		tk.MustExec("create table special_keys (j json, v vector(1))")
		for _, col := range []string{"j", "v"} {
			query := "select " + col + " from special_keys where " + col + " in (select " + col + " from special_keys)"
			tk.MustQuery("explain format='brief' " + query).CheckContain("semi join")
		}
	})

	t.Run("DECIMAL scale", func(t *testing.T) {
		tk := newTestKit(t)
		// ROUND with a per-row scale produces equal decimal values represented as 1 and 1.0.
		tk.MustExec("create table dec_inner (n decimal(10,3), k int)")
		tk.MustExec("insert into dec_inner values (1.000, 0), (1.000, 1)")
		// Binary literals pass the collation check so these cases exercise DECIMAL grouping.
		for _, predicate := range []string{
			"_binary'1' in (select round(n,k) from dec_inner)",
			"(1, _binary'1') in (select 1, v from (select round(n,k) as v from dec_inner) d)",
		} {
			tk.MustQuery("select id from t_outer where id = 1 and " + predicate).Check(testkit.Rows("1"))
		}
		tk.MustQuery("explain format='brief' select id from t_outer where id in (select n from dec_inner)").CheckContain("inner join")
		// The CTE inherits n's column ID, but its recursive rows have different scales.
		tk.MustQuery(`with recursive c(v,k) as (
			select n,k from dec_inner where k=0
			union all select round(v,k), k+1 from c where k<2
		) select id from t_outer where id in (select v from c)`).Check(testkit.Rows("1"))
	})

	t.Run("ENUM SET BIT and YEAR", func(t *testing.T) {
		tk := newTestKit(t)
		// Stored ordinals and narrow integer types need their original comparison.
		tk.MustExec("create table labels (e enum('a','b'), s set('a','b'), b bit(8), y year)")
		for _, col := range []string{"e", "s", "b", "y"} {
			query := "select id from t_outer where id in (select " + col + " from labels)"
			tk.MustQuery("explain format='brief' " + query).CheckContain("semi join")
		}
	})

	t.Run("explicit collation", func(t *testing.T) {
		tk := newTestKit(t)
		tk.MustExec("create table coll_outer (s varchar(10) collate utf8mb4_bin)")
		tk.MustExec("insert into coll_outer values ('a')")
		tk.MustExec("create table coll_inner (a blob, b varchar(10) collate utf8mb4_bin)")
		tk.MustExec("insert into coll_inner values ('x', 'A')")

		tk.MustQuery("select s from coll_outer where (0, s) in (select a, b collate utf8mb4_general_ci from coll_inner)").Check(testkit.Rows("a"))
		tk.MustQuery("select s from coll_outer where (0, s) in (select a, b from coll_inner)").Check(testkit.Rows())
	})

	t.Run("incompatible collations", func(t *testing.T) {
		tk := newTestKit(t)
		tk.MustExec("create table o (s varchar(10) collate utf8mb4_bin)")
		tk.MustExec("create table t (b varchar(10) collate utf8mb4_bin)")
		tk.MustExec("create table t2 (b varchar(10) collate utf8mb4_general_ci)")
		tk.MustExec("insert into o values ('a')")
		tk.MustExec("insert into t values ('A'), ('a')")
		tk.MustExec("insert into t2 values ('A'), ('a'), ('b')")

		tk.MustQuery("select count(*) from o where (1, s collate utf8mb4_general_ci) in (select 1, b from t)").Check(testkit.Rows("1"))
		tk.MustQuery("select count(*) from o where (1, s) in (select 1, b from t2)").Check(testkit.Rows("1"))
		tk.MustQuery("explain format='brief' select * from o where (1, s) in (select 1, b from t2)").CheckContain("semi join")

		tk.MustExec("create table pad_o (s varchar(10)) charset utf8mb4 collate utf8mb4_0900_ai_ci")
		tk.MustExec("create table pad_i (s varchar(10)) charset utf8 collate utf8_unicode_ci")
		tk.MustExec("insert into pad_o values ('a'), ('a ')")
		tk.MustExec("insert into pad_i values ('a'), ('a ')")
		tk.MustQuery("select concat('[', s, ']') from pad_o where s in (select s from pad_i) order by 1").Check(testkit.Rows("[a ]", "[a]"))
		tk.MustQuery("explain format='brief' select * from pad_o where s in (select s from pad_i)").CheckContain("semi join")

		// A malformed probe can match different sort keys. The single outer row
		// must appear exactly once if any comparison matches.
		const probe = "(_utf8mb4 0xff collate utf8mb4_general_ci)"
		expected := tk.MustQuery("select exists(select 1 from t2 where " + probe + " = b)").Rows()
		tk.MustQuery("select count(*) from o where " + probe + " in (select b from t2)").Check(expected)

		// With the same case-insensitive collation on both sides, retain the
		// original keyed semi join when DISTINCT cannot prove matching sort keys.
		plan := tk.MustQuery("explain format='brief' select b from t2 where b in (select b from t2)")
		plan.CheckContain("semi join")
		plan.CheckContain("equal:[eq(")

		// Correlated index lookups can stop at the first match even when DISTINCT is unsafe.
		tk.MustExec("create table probe (s varchar(10) collate utf8mb4_general_ci not null, f int, key(s))")
		tk.MustExec("insert into probe values ('a', 1), ('A', 2), ('b', 2)")
		tk.MustExec("set tidb_opt_enable_alternative_logical_plans = on")
		query := "select s from probe where f = 1 and s in (select s from probe)"
		plan = tk.MustQuery("explain format='brief' " + query)
		plan.CheckContain("Apply")
		plan.CheckContain("Limit")
		tk.MustQuery(query).Check(testkit.Rows("a"))

		// Re-evaluating volatile operands per outer row could change their results.
		for _, predicate := range []string{
			"(s, rand()) in (select b, 1 from t2)",
			"s in (select if(rand() < 0.5, b, 'x') from t2)",
		} {
			tk.MustQuery("explain format='brief' select s from probe where " + predicate)
			require.False(t, tk.Session().GetSessionVars().StmtCtx.AlternativeLogicalPlanPreferCorrelate, predicate)
		}
	})
}
