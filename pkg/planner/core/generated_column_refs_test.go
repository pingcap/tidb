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
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

// builtVirtualExprs builds the logical plan of sql and returns, for every
// DataSource, "table:col" for each virtual generated column whose VirtualExpr
// was built, sorted.
func builtVirtualExprs(t *testing.T, tk *testkit.TestKit, sql string) []string {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err, sql)
	is := domain.GetDomain(tk.Session()).InfoSchema()
	nodeW := resolve.NewNodeW(stmt)
	require.NoError(t, core.Preprocess(context.Background(), tk.Session(), nodeW, core.WithPreprocessorReturn(&core.PreprocessorReturn{InfoSchema: is})), sql)
	p, err := core.BuildLogicalPlanForTest(context.Background(), tk.Session(), nodeW, is)
	require.NoError(t, err, sql)
	var built []string
	var walk func(lp base.LogicalPlan)
	walk = func(lp base.LogicalPlan) {
		if ds, ok := lp.(*logicalop.DataSource); ok {
			for i, col := range ds.Schema().Columns {
				if i >= len(ds.Columns) {
					break
				}
				info := ds.Columns[i]
				if info.IsGenerated() && !info.GeneratedStored && col.VirtualExpr != nil {
					built = append(built, ds.TableInfo.Name.L+":"+info.Name.L)
				}
			}
		}
		for _, child := range lp.Children() {
			walk(child)
		}
	}
	walk(p.(base.LogicalPlan))
	slices.Sort(built)
	return built
}

// TestSkipUnreferencedVirtualGeneratedColumns checks that buildDataSource only
// builds the VirtualExpr of virtual generated columns that the statement can
// use, and that queries relying on generated columns still plan and return the
// same results.
func TestSkipUnreferencedVirtualGeneratedColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table g (
		id int primary key, a varchar(20) default 'dflt', b varchar(20), j json, n int,
		a_l varchar(20) as (lower(a)),
		b_l varchar(20) as (lower(b)),
		b_l2 varchar(30) as (concat(b_l, 'x')),
		c int as (n + 1),
		k int as (1),
		key ka (a_l), key kb (b_l), key kexpr ((upper(b))),
		key kmv ((cast(j->'$.t' as signed array))))`)
	tk.MustExec(`insert into g (id, a, b, j, n) values
		(1, 'Aa', 'Bb', '{"t": [1, 2]}', 1), (2, 'Cc', 'Dd', '{"t": [3]}', 2), (3, 'Aa', 'Ee', '{"t": []}', 3)`)
	tk.MustExec("create table p (id int, a varchar(20), a_l varchar(20) as (lower(a)), x int as (id * 2)) partition by hash(id) partitions 2")
	tk.MustExec("create view v_named as select id, b_l2 from g")
	tk.MustExec("create view v_star as select * from g")
	tk.MustExec("analyze table g")

	const (
		exprIdx = "g:_v$_kexpr_0"
		mvIdx   = "g:_v$_kmv_0"
	)
	all := []string{exprIdx, mvIdx, "g:a_l", "g:b_l", "g:b_l2", "g:c", "g:k"}
	for _, tc := range []struct {
		sql   string
		built []string
	}{
		// Only generated columns over a mentioned column (plus the constant
		// one, whose dependencies are empty) are built.
		{"select id from g where a = 'aa'", []string{"g:a_l", "g:k"}},
		// An expression over b can be substituted with b_l or the expression
		// index; b_l2 is built from b_l, so it is built too.
		{"select id from g where lower(b) = 'bb'", []string{exprIdx, "g:b_l", "g:b_l2", "g:k"}},
		// Selecting b_l2 needs b_l to evaluate it; generated columns that
		// share b_l's dependency b are built too.
		{"select b_l2 from g where id = 1", []string{exprIdx, "g:b_l", "g:b_l2", "g:k"}},
		{"select id from g where 3 member of (j->'$.t')", []string{mvIdx, "g:k"}},
		// DEFAULT(a) mentions a even though DefaultExpr does not visit it.
		{"select default(a) from g", []string{"g:a_l", "g:k"}},
		// Names in subqueries, CTEs, set operations and USING lists count.
		{"select id from g where id in (select id from g where a_l = 'aa')", []string{"g:a_l", "g:a_l", "g:k", "g:k"}},
		{"with cte as (select b from g) select count(*) from cte", []string{exprIdx, "g:b_l", "g:b_l2", "g:k"}},
		{"select id from g union all select c from g", []string{"g:c", "g:c", "g:k", "g:k"}},
		{"select g1.id from g g1 join g g2 using (a)", []string{"g:a_l", "g:a_l", "g:k", "g:k"}},
		// Columns that can be referenced without being named: build everything.
		{"select * from g", all},
		{"select g.* from g", all},
		{"table g", all},
		{"select count(*) from (select * from g) t", all},
		{"select g1.id from g g1 natural join g g2", append(slices.Clone(all), all...)},
		// View definitions are not in the statement's AST.
		{"select id from v_named", []string{exprIdx, "g:b_l", "g:b_l2", "g:k"}},
		{"select id from v_star", all},
		// Partitioned tables build everything.
		{"select id from p", []string{"p:a_l", "p:x"}},
	} {
		got := builtVirtualExprs(t, tk, tc.sql)
		want := slices.Clone(tc.built)
		slices.Sort(want)
		require.Equal(t, want, got, tc.sql)
	}

	// Queries that rely on generated columns still use their indexes and
	// return correct results.
	planOf := func(sql string) string {
		return strings.Join(testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' "+sql).Rows()), "\n")
	}
	require.Contains(t, planOf("select id from g where lower(b) = 'bb'"), "index:kb(b_l)")
	tk.MustQuery("select id from g where lower(b) = 'bb'").Check(testkit.Rows("1"))
	require.Contains(t, planOf("select id from g where upper(b) = 'DD'"), "index:kexpr")
	tk.MustQuery("select id from g where upper(b) = 'DD'").Check(testkit.Rows("2"))
	require.Contains(t, planOf("select id from g where 3 member of (j->'$.t')"), "kmv")
	tk.MustQuery("select id from g where 3 member of (j->'$.t')").Check(testkit.Rows("2"))
	tk.MustQuery("select b_l2, c from g where id = 3").Check(testkit.Rows("eex 4"))
	tk.MustQuery("select count(*) from g use index (ka)").Check(testkit.Rows("3"))
	tk.MustQuery("select id from v_named where b_l2 = 'ddx'").Check(testkit.Rows("2"))
	tk.MustQuery("select count(*) from g g1 join g g2 using (a)").Check(testkit.Rows("5"))

	// Uncommitted rows are read through UnionScan.
	tk.MustExec("begin")
	tk.MustExec("insert into g (id, a, b, j) values (4, 'Zz', 'Bb', '[]')")
	tk.MustQuery("select id from g where lower(b) = 'bb' order by id").Check(testkit.Rows("1", "4"))
	tk.MustQuery("select count(*) from g where a = 'Zz'").Check(testkit.Rows("1"))
	tk.MustExec("rollback")

	// DML still maintains generated columns.
	tk.MustExec("update g set b = 'QQ' where lower(a) = 'cc'")
	tk.MustQuery("select b_l, b_l2 from g where id = 2").Check(testkit.Rows("qq qqx"))
	tk.MustExec("delete from g where lower(b) = 'ee'")
	tk.MustQuery("select count(*) from g").Check(testkit.Rows("2"))

	// Prepared statements, including plan cache hits.
	tk.MustExec("prepare st from 'select id from g where lower(b) = ?'")
	tk.MustExec("set @x = 'bb'")
	tk.MustQuery("execute st using @x").Check(testkit.Rows("1"))
	tk.MustQuery("execute st using @x").Check(testkit.Rows("1"))

	// An index on an expression-equivalent generated column still covers a
	// named generated column, even though neither the indexed column nor the
	// base column is named.
	tk.MustExec("create table e (id int primary key, a varchar(20), c1 varchar(20) as (lower(a)), c2 varchar(20) as (lower(a)), key k2 (c2))")
	tk.MustExec("insert into e (id, a) values (1, 'Xy'), (2, 'Zw')")
	require.Equal(t, []string{"e:c1", "e:c2"}, builtVirtualExprs(t, tk, "select c1 from e use index (k2)"))
	plan := planOf("select c1 from e use index (k2)")
	require.Contains(t, plan, "index:k2(c2)")
	require.NotContains(t, plan, "TableRowIDScan")
	tk.MustQuery("select c1 from e use index (k2) order by c1").Check(testkit.Rows("xy", "zw"))

	// A unique tidb_shard index is still detected for point lookups on its base column.
	tk.MustExec("create table s (id int primary key, a int, sh int as (tidb_shard(a)), unique key ush (sh, a))")
	tk.MustExec("insert into s (id, a) values (1, 10), (2, 20)")
	require.Contains(t, planOf("select id from s where a = 20"), "ush")
	tk.MustQuery("select id from s where a = 20").Check(testkit.Rows("2"))
}
