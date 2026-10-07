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

package join

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func prepareLateMaterializationTables(tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec("set @@cte_max_recursion_depth = 100000")
	gen := "with recursive s(i) as (select 1 union all select i+1 from s where i < 3000) "
	tk.MustExec("create table o (id int primary key, c int, d int, pad varchar(64), key idx_c(c), key idx_d_c(d, c))")
	tk.MustExec("create table u (id int primary key, k int, v int, pad varchar(64), unique key uk(k))")
	tk.MustExec("create table h (id int primary key, v int, pad varchar(64))")
	tk.MustExec("create table m (id int primary key auto_increment, k int, v int, pad varchar(64), key ik(k))")
	// No int primary key: the handle is _tidb_rowid.
	tk.MustExec("create table r (k int, v int, pad varchar(64), key ik(k))")
	// c has duplicates and NULLs; u misses every 5th key, so inner joins drop rows.
	tk.MustExec("insert into o " + gen + "select i, if(i % 97 = 0, null, i div 2), i % 10, concat('o', i) from s")
	tk.MustExec("insert into u " + gen + "select i, i, i * 3, concat('u', i) from s where i % 5 <> 0")
	tk.MustExec("insert into h " + gen + "select i, i * 7, concat('h', i) from s where i % 3 <> 0")
	tk.MustExec("insert into m (k, v, pad) " + gen + "select i % 1000, i, concat('m', i) from s")
	tk.MustExec("insert into r " + gen + "select i, i * 11, concat('r', i) from s")
	tk.MustExec("analyze table o, u, h, m, r")
}

func setLateMaterialization(tk *testkit.TestKit, enabled bool) {
	if enabled {
		tk.MustExec("delete from mysql.opt_rule_blacklist where name = 'late_materialization'")
	} else {
		tk.MustExec("insert into mysql.opt_rule_blacklist values ('late_materialization')")
	}
	tk.MustExec("admin reload opt_rule_blacklist")
}

func lateMaterializationPlan(tk *testkit.TestKit, sql string) []string {
	rows := tk.MustQuery("explain format = 'plan_tree' " + sql).Rows()
	plan := make([]string, 0, len(rows))
	for _, r := range rows {
		plan = append(plan, strings.TrimRight(fmt.Sprintf("%v %v %v", r[0], r[1], r[2]), " "))
	}
	return plan
}

// TestLateMaterializationResults checks that queries whose table fetches are
// deferred past joins, filters and Limits return the same rows as without the
// rewrite.
func TestLateMaterializationResults(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	prepareLateMaterializationTables(tk)

	paged := []string{
		// Inner join, unique inner key.
		"select o.id, o.pad, u.pad from o join u on o.id = u.k order by o.c, o.id limit 10 offset %d",
		"select o.pad, u.pad, u.v from o join u on o.id = u.k order by o.c desc, o.id desc limit 7 offset %d",
		// Left and right outer joins.
		"select o.pad, u.pad from o left join u on o.id = u.k and u.v > 10 order by o.c, o.id limit 10 offset %d",
		"select o.pad, u.pad from u right join o on o.id = u.k order by o.c, o.id limit 10 offset %d",
		// Outer joins to a non-unique key: the offset is not pushed below the join,
		// and the null-supplying side is joined back with an outer join.
		"select o.pad, m.pad, m.v from o left join m on o.id = m.k order by o.c, o.id, m.id limit 10 offset %d",
		"select o.pad, m.pad from m right join o on o.id = m.k order by o.c, o.id, m.id limit 10 offset %d",
		// A filter on a non-indexed inner column keeps u's row; o can still be deferred.
		"select o.pad, u.pad from o join u on o.id = u.k where mod(u.v, 2) = 0 order by o.c, o.id limit 10 offset %d",
		// A filter on an indexed column of o.
		"select o.pad, u.pad from o join u on o.id = u.k where o.d = 3 order by o.c, o.id limit 10 offset %d",
		// Join on the inner table's int primary key.
		"select o.pad, h.pad from o join h on o.id = h.id order by o.c, o.id limit 10 offset %d",
		// Non-unique inner key.
		"select o.pad, m.pad from o join m on o.id = m.k order by o.c, o.id, m.id limit 10 offset %d",
		// _tidb_rowid handle.
		"select o.pad, r.pad, r.v from o join r on o.id = r.k order by o.c, o.id limit 10 offset %d",
		// Three tables, with an outer join to the third.
		"select o.pad, u.pad, h.pad from o join u on o.id = u.k join h on u.id = h.id order by o.c, o.id limit 10 offset %d",
		"select o.pad, u.pad, h.pad from o join u on o.id = u.k left join h on u.id = h.id order by o.c, o.id limit 10 offset %d",
		// Expressions over deferred columns.
		"select concat(o.pad, '-', u.pad), u.v + 1 from o join u on o.id = u.k order by o.c, o.id limit 10 offset %d",
		// The paged join as a derived table under an aggregation.
		"select count(*), max(x.p) from (select o.pad p from o join u on o.id = u.k order by o.c, o.id limit 25 offset %d) x",
		// Without OFFSET: o is read through idx_d_c and u's filter discards most of its rows.
		"select o.pad, u.pad from o join u on o.c = u.k where o.d = 3 and u.v < 300 order by o.c, o.id limit %d",
	}
	unpaged := []string{
		"select o.pad, u.pad from o join u on o.c = u.k where o.d = 3 and u.v < 300 order by o.c, o.id",
		"select o.pad, u.pad from o join u on o.c = u.k where o.d = 3",
		"select o.pad, u.pad from o join u on o.id = u.k where u.v < 300 order by o.id",
		"select o.pad, u.pad, h.pad from o join u on o.id = u.k join h on u.id = h.id where h.v < 700 order by o.id",
		"select count(*), max(o.pad), max(u.pad) from o join u on o.id = u.k where u.v < 300",
	}
	queries := make([]string, 0, len(paged)*6+len(unpaged))
	for _, q := range paged {
		for _, n := range []int{1, 9, 500, 2990, 5000} {
			queries = append(queries, fmt.Sprintf(q, n))
		}
	}
	queries = append(queries, unpaged...)

	changed := 0
	for _, sql := range queries {
		setLateMaterialization(tk, true)
		got := tk.MustQuery(sql).Sort().Rows()
		plan := lateMaterializationPlan(tk, sql)
		setLateMaterialization(tk, false)
		want := tk.MustQuery(sql).Sort().Rows()
		basePlan := lateMaterializationPlan(tk, sql)
		require.Equal(t, want, got, "sql: %s\nplan:\n%s", sql, strings.Join(plan, "\n"))
		if !slices.Equal(plan, basePlan) {
			changed++
		}
	}
	setLateMaterialization(tk, true)
	// Most of the paged queries are rewritten; a broken eligibility check would
	// turn this test into a comparison of identical plans.
	require.Greater(t, changed, len(queries)/2)

	// LIMIT without ORDER BY: any page is a valid answer, so compare row counts.
	for _, n := range []int{0, 9, 1990, 5000} {
		sql := fmt.Sprintf("select o.pad, h.pad from o join h on o.id = h.id limit 10 offset %d", n)
		require.Len(t, tk.MustQuery(sql).Rows(), max(0, min(10, 2000-n)), sql)
	}

	// Prepared statements share a cached plan across offsets.
	tk.MustExec("prepare st from 'select o.pad, u.pad from o join u on o.id = u.k order by o.c, o.id limit ?, ?'")
	for _, n := range []int{0, 500, 3, 2990, 0, 7} {
		tk.MustExec(fmt.Sprintf("set @off = %d, @cnt = 10", n))
		got := tk.MustQuery("execute st using @off, @cnt").Rows()
		want := tk.MustQuery(fmt.Sprintf("select /*+ ignore_plan_cache() */ o.pad, u.pad from o join u on o.id = u.k order by o.c, o.id limit %d, 10", n)).Rows()
		require.Equal(t, want, got, "offset %d", n)
	}
}

// TestLateMaterializationPlans checks which tables are deferred.
func TestLateMaterializationPlans(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	prepareLateMaterializationTables(tk)

	// Both tables are read from their indexes below the Limit and fetched for
	// the 10 surviving rows.
	require.Equal(t, []string{
		"Projection root",
		"└─IndexJoin root",
		"  ├─IndexHashJoin(Build) root",
		"  │ ├─Limit(Build) root",
		"  │ │ └─Projection root",
		"  │ │   └─IndexJoin root",
		"  │ │     ├─IndexReader(Build) root",
		"  │ │     │ └─IndexFullScan cop[tikv] table:o, index:idx_c(c)",
		"  │ │     └─IndexReader(Probe) root",
		"  │ │       └─Selection cop[tikv]",
		"  │ │         └─IndexRangeScan cop[tikv] table:u, index:uk(k)",
		"  │ └─TableReader(Probe) root",
		"  │   └─TableRangeScan cop[tikv] table:u",
		"  └─TableReader(Probe) root",
		"    └─TableRangeScan cop[tikv] table:o",
	}, lateMaterializationPlan(tk, "select o.pad, u.pad from o join u on o.id = u.k order by o.c, o.id limit 10 offset 2000"))

	// Without OFFSET: u's filter on a non-indexed column discards most o rows,
	// so o is fetched after the join. u keeps its row for the filter.
	require.Equal(t, []string{
		"Projection root",
		"└─IndexJoin root",
		"  ├─Limit(Build) root",
		"  │ └─Projection root",
		"  │   └─IndexJoin root",
		"  │     ├─IndexReader(Build) root",
		"  │     │ └─IndexRangeScan cop[tikv] table:o, index:idx_d_c(d, c)",
		"  │     └─IndexLookUp(Probe) root",
		"  │       ├─Selection(Build) cop[tikv]",
		"  │       │ └─IndexRangeScan cop[tikv] table:u, index:uk(k)",
		"  │       └─Selection(Probe) cop[tikv]",
		"  │         └─TableRowIDScan cop[tikv] table:u",
		"  └─TableReader(Probe) root",
		"    └─TableRangeScan cop[tikv] table:o",
	}, lateMaterializationPlan(tk, "select o.pad, u.pad from o join u on o.c = u.k where o.d = 3 and u.v < 300 order by o.c, o.id limit 5"))

	tk.MustExec("create table zo like o")
	tk.MustExec("create table zu like u")
	tk.MustExec("insert into zo select * from o")
	tk.MustExec("insert into zu select * from u")
	for _, sql := range []string{
		// o is on the inner side of the join and nothing above discards its rows.
		"select o.pad, u.pad from o join u on o.c = u.k where o.d = 3 and u.v < 300 order by o.c, o.id",
		// A join method hint keeps the plan as written.
		"select /*+ INL_JOIN(u) */ o.pad, u.pad from o join u on o.id = u.k order by o.c, o.id limit 10 offset 2000",
		// Tables that were never analyzed give no reliable row reduction.
		"select zo.pad, zu.pad from zo join zu on zo.id = zu.k order by zo.c, zo.id limit 10 offset 2000",
	} {
		plan := lateMaterializationPlan(tk, sql)
		setLateMaterialization(tk, false)
		require.Equal(t, lateMaterializationPlan(tk, sql), plan, sql)
		setLateMaterialization(tk, true)
	}
}

// TestLateMaterializationKeepsMergeJoin checks that a merge join the original
// plan chose survives the rewrite: the same merge join runs over index-only
// readers and the table rows are joined back after the Limit. A merge join the
// original plan did not choose is never introduced.
func TestLateMaterializationKeepsMergeJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@cte_max_recursion_depth = 100000")
	gen := "with recursive s(i) as (select 1 union all select i+1 from s where i < 20000) "
	tk.MustExec("create table a (id int primary key, k int, f int, pad varchar(64), key ik(k, f))")
	tk.MustExec("create table b (id int primary key, k int, g int, pad varchar(64), key ik(k, g))")
	tk.MustExec("insert into a " + gen + "select i, i div 2, i % 100, repeat('a', 60) from s")
	tk.MustExec("insert into b " + gen + "select i, i div 2, i % 50, repeat('b', 60) from s")
	tk.MustExec("analyze table a, b")

	// k is not unique, so tied rows may come in either order; the k values of
	// the page are the same for every valid answer.
	sql := "select a.k, a.pad, b.pad from a join b on a.k = b.k where a.f < 50 and b.g < 5 order by a.k limit 10"
	plan := strings.Join(lateMaterializationPlan(tk, sql), "\n")
	got := tk.MustQuery(sql).Rows()
	setLateMaterialization(tk, false)
	basePlan := strings.Join(lateMaterializationPlan(tk, sql), "\n")
	want := tk.MustQuery(sql).Rows()
	setLateMaterialization(tk, true)

	require.Len(t, got, 10)
	require.Len(t, want, 10)
	for i := range got {
		require.Equal(t, want[i][0], got[i][0])
	}
	require.Contains(t, basePlan, "MergeJoin")
	require.Contains(t, basePlan, "IndexLookUp")
	require.Contains(t, plan, "MergeJoin", plan)
	require.NotContains(t, plan, "IndexLookUp", plan)
	require.Equal(t, 2, strings.Count(plan, "TableRangeScan"), plan)
}
