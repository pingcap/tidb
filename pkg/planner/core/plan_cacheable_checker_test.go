// Copyright 2017 PingCAP, Inc.
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
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/testkit"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestFixControl44823(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int)`)
	var va []string
	for i := 0; i < 201; i++ {
		tk.MustExec(fmt.Sprintf(`set @a%v = %v`, i, i))
		va = append(va, fmt.Sprintf("@a%v", i))
	}

	// prepared plan cache
	tk.MustExec(fmt.Sprintf(`prepare st from 'select * from t where a in (%v?)'`, strings.Repeat("?,", 200)))
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1105 skip prepared plan-cache: too many values in in-list`))
	tk.MustExec(fmt.Sprintf(`execute st using %v`, strings.Join(va, ",")))
	tk.MustExec(fmt.Sprintf(`execute st using %v`, strings.Join(va, ",")))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

	tk.MustExec(`set @@tidb_opt_fix_control = "44823:250"`)
	tk.MustExec(fmt.Sprintf(`prepare st from 'select * from t where a in (%v?)'`, strings.Repeat("?,", 200)))
	tk.MustQuery(`show warnings`).Check(testkit.Rows()) // no warning
	tk.MustExec(fmt.Sprintf(`execute st using %v`, strings.Join(va, ",")))
	tk.MustExec(fmt.Sprintf(`execute st using %v`, strings.Join(va, ",")))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1")) // can hit

	tk.MustExec(`set @@tidb_opt_fix_control = "44823:0"`)
	tk.MustExec(fmt.Sprintf(`prepare st from 'select * from t where a in (%v?)'`, strings.Repeat("?,", 200)))
	tk.MustQuery(`show warnings`).Check(testkit.Rows()) // no warning
	tk.MustExec(fmt.Sprintf(`execute st using %v`, strings.Join(va, ",")))
	tk.MustExec(fmt.Sprintf(`execute st using %v`, strings.Join(va, ",")))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	// non prepared plan cache
	values := make([]string, 0, 201)
	for i := 0; i < 201; i++ {
		values = append(values, fmt.Sprintf("%v", i))
	}
	query := fmt.Sprintf("select * from t where a in (%v)", strings.Join(values, ","))
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)

	tk.MustExec(`set @@tidb_opt_fix_control = ""`)
	tk.MustQuery(query).Check(testkit.Rows())
	tk.MustQuery(query).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

	tk.MustExec(`set @@tidb_opt_fix_control = "44823:250"`)
	tk.MustQuery(query).Check(testkit.Rows())
	tk.MustQuery(query).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`set @@tidb_opt_fix_control = "44823:0"`)
	tk.MustQuery(query).Check(testkit.Rows())
	tk.MustQuery(query).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestIssue46760(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int)`)
	tk.MustExec(`prepare st from 'select * from t where a<?'`)
	tk.MustExec(`set @a=1`)
	tk.MustQuery(`execute st using @a`).Check(testkit.Rows())
	tk.MustQuery(`execute st using @a`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	ctx := context.WithValue(context.Background(), core.PlanCacheKeyTestIssue46760{}, struct{}{})
	tk.MustExecWithContext(ctx, `prepare st from 'select * from t where a<?'`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip prepared plan-cache: find table test.t failed: mock error"))
	tk.MustExec(`set @a=1`)
	tk.MustQuery(`execute st using @a`).Check(testkit.Rows())
	tk.MustQuery(`execute st using @a`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
}

func TestCacheable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	mockCtx := mock.NewContext()
	mockCtx.GetSessionVars().EnablePlanCacheForParamLimit = true
	mockCtx.GetSessionVars().EnablePlanCacheForSubquery = true

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t1(a int, b int) partition by range(a) ( partition p0 values less than (6), partition p1 values less than (11) )")
	tk.MustExec("create table t2(a int, b int) partition by hash(a) partitions 11")
	tk.MustExec("create table t3(a int, b int)")
	tbl := &ast.TableName{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t3")}
	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	// test non-SelectStmt/-InsertStmt/-DeleteStmt/-UpdateStmt/-SetOprStmt
	var stmt ast.Node = &ast.ShowStmt{}
	require.False(t, core.Cacheable(stmt, is))

	stmt = &ast.LoadDataStmt{}
	require.False(t, core.Cacheable(stmt, is))

	stmt = &ast.ImportIntoStmt{}
	require.False(t, core.Cacheable(stmt, is))

	// test SetOprStmt
	stmt = &ast.SetOprStmt{}
	require.True(t, core.Cacheable(stmt, is))

	tableRefsClause := &ast.TableRefsClause{TableRefs: &ast.Join{Left: &ast.TableSource{Source: tbl}}}
	// test InsertStmt
	stmt = &ast.InsertStmt{Table: tableRefsClause} // insert-values-stmt
	require.True(t, core.Cacheable(stmt, is))
	stmt = &ast.InsertStmt{Table: tableRefsClause, Select: &ast.SelectStmt{}} // insert-select-stmt
	require.True(t, core.Cacheable(stmt, is))

	// test DeleteStmt
	whereExpr := &ast.FuncCallExpr{}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Where:     whereExpr,
	}
	require.True(t, core.Cacheable(stmt, is))

	for funcName := range expression.UnCacheableFunctions {
		whereExpr.FnName = model.NewCIStr(funcName)
		require.False(t, core.Cacheable(stmt, is))
	}

	whereExpr.FnName = model.NewCIStr(ast.Rand)
	require.True(t, core.Cacheable(stmt, is))

	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Where:     &ast.ExistsSubqueryExpr{Sel: &ast.SubqueryExpr{Query: &ast.SelectStmt{}}},
	}
	c, _ := core.CacheableWithCtx(mockCtx, stmt, is)
	require.True(t, c)

	limitStmt := &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c, _ = core.CacheableWithCtx(mockCtx, stmt, is)
	require.True(t, c)

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c, _ = core.CacheableWithCtx(mockCtx, stmt, is)
	require.True(t, c)

	limitStmt = &ast.Limit{}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c, _ = core.CacheableWithCtx(mockCtx, stmt, is)
	require.True(t, c)

	stmt.(*ast.DeleteStmt).TableHints = append(stmt.(*ast.DeleteStmt).TableHints, &ast.TableOptimizerHint{
		HintName: model.NewCIStr(hint.HintIgnorePlanCache),
	})
	require.False(t, core.Cacheable(stmt, is))

	// test UpdateStmt
	whereExpr = &ast.FuncCallExpr{}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Where:     whereExpr,
	}
	require.True(t, core.Cacheable(stmt, is))

	for funcName := range expression.UnCacheableFunctions {
		whereExpr.FnName = model.NewCIStr(funcName)
		require.False(t, core.Cacheable(stmt, is))
	}

	whereExpr.FnName = model.NewCIStr(ast.Rand)
	require.True(t, core.Cacheable(stmt, is))

	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Where:     &ast.ExistsSubqueryExpr{Sel: &ast.SubqueryExpr{Query: &ast.SelectStmt{}}},
	}
	c, _ = core.CacheableWithCtx(mockCtx, stmt, is)
	require.True(t, c)

	limitStmt = &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c, _ = core.CacheableWithCtx(mockCtx, stmt, is)
	require.True(t, c)

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c, _ = core.CacheableWithCtx(mockCtx, stmt, is)
	require.True(t, c)

	limitStmt = &ast.Limit{}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c, _ = core.CacheableWithCtx(mockCtx, stmt, is)
	require.True(t, c)

	stmt.(*ast.UpdateStmt).TableHints = append(stmt.(*ast.UpdateStmt).TableHints, &ast.TableOptimizerHint{
		HintName: model.NewCIStr(hint.HintIgnorePlanCache),
	})
	require.False(t, core.Cacheable(stmt, is))

	// test SelectStmt
	whereExpr = &ast.FuncCallExpr{}
	stmt = &ast.SelectStmt{
		Where: whereExpr,
	}
	require.True(t, core.Cacheable(stmt, is))

	for funcName := range expression.UnCacheableFunctions {
		whereExpr.FnName = model.NewCIStr(funcName)
		require.False(t, core.Cacheable(stmt, is))
	}

	whereExpr.FnName = model.NewCIStr(ast.Rand)
	require.True(t, core.Cacheable(stmt, is))

	stmt = &ast.SelectStmt{
		Where: &ast.ExistsSubqueryExpr{Sel: &ast.SubqueryExpr{Query: &ast.SelectStmt{}}},
	}
	c, _ = core.CacheableWithCtx(mockCtx, stmt, is)
	require.True(t, c)

	limitStmt = &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c, _ = core.CacheableWithCtx(mockCtx, stmt, is)
	require.True(t, c)

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c, _ = core.CacheableWithCtx(mockCtx, stmt, is)
	require.True(t, c)

	limitStmt = &ast.Limit{}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c, _ = core.CacheableWithCtx(mockCtx, stmt, is)
	require.True(t, c)

	paramExpr := &driver.ParamMarkerExpr{}
	orderByClause := &ast.OrderByClause{Items: []*ast.ByItem{{Expr: paramExpr}}}
	stmt = &ast.SelectStmt{
		OrderBy: orderByClause,
	}
	require.False(t, core.Cacheable(stmt, is))

	valExpr := &driver.ValueExpr{}
	orderByClause = &ast.OrderByClause{Items: []*ast.ByItem{{Expr: valExpr}}}
	stmt = &ast.SelectStmt{
		OrderBy: orderByClause,
	}
	require.True(t, core.Cacheable(stmt, is))

	stmt.(*ast.SelectStmt).TableHints = append(stmt.(*ast.SelectStmt).TableHints, &ast.TableOptimizerHint{
		HintName: model.NewCIStr(hint.HintIgnorePlanCache),
	})
	require.False(t, core.Cacheable(stmt, is))

	boundExpr := &ast.FrameBound{Expr: &driver.ParamMarkerExpr{}}
	require.False(t, core.Cacheable(boundExpr, is))

	// Partition table can not be cached.
	join := &ast.Join{
		Left:  &ast.TableName{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t1")},
		Right: &ast.TableName{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t2")},
	}
	stmt = &ast.SelectStmt{
		From: &ast.TableRefsClause{
			TableRefs: join,
		},
	}
	require.False(t, core.Cacheable(stmt, is))

	join = &ast.Join{
		Left: &ast.TableName{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t3")},
	}
	stmt = &ast.SelectStmt{
		From: &ast.TableRefsClause{
			TableRefs: join,
		},
	}
	require.True(t, core.Cacheable(stmt, is))

	// A CTE reference is not a physical table, but physical tables referenced by
	// the CTE definition must still be checked. A non-recursive CTE is not visible
	// inside its own definition.
	cteStmt, err := parser.New().ParseOneStmt("with cte as (select * from test.t3) select * from cte", "", "")
	require.NoError(t, err)
	cacheable, reason := core.CacheableWithCtx(tk.Session().GetPlanCtx(), cteStmt, is)
	require.True(t, cacheable, reason)

	cteStmt, err = parser.New().ParseOneStmt("with missing as (select * from missing) select * from missing", "", "")
	require.NoError(t, err)
	cacheable, _ = core.CacheableWithCtx(tk.Session().GetPlanCtx(), cteStmt, is)
	require.False(t, cacheable)

	tk.MustExec("create temporary table nested_shadow(a int)")
	cteCases := []struct {
		name string
		sql  string
	}{
		{
			name: "recursive self reference",
			sql:  "with recursive cte(n) as (select 1 union all select n + 1 from cte where n < 3) select n from cte",
		},
		{
			name: "sibling reference",
			sql:  "with first_cte as (select * from test.t3), second_cte as (select * from first_cte) select * from second_cte",
		},
		{
			name: "nested scope",
			sql:  "with outer_cte as (select * from test.t3) select * from (with inner_cte as (select 1 as a) select * from inner_cte) as nested_result",
		},
		{
			name: "nested same-name shadowing",
			sql:  "with shadow_cte as (select * from test.t3) select * from (with shadow_cte as (select 1 as a) select * from shadow_cte) as nested_result",
		},
		{
			name: "nested cte shadows temporary table",
			sql:  "with outer_cte as (select * from test.t3) select * from (with nested_shadow as (select 1 as a) select * from nested_shadow) as nested_result",
		},
	}
	for _, testCase := range cteCases {
		t.Run(testCase.name, func(t *testing.T) {
			stmt, err := parser.New().ParseOneStmt(testCase.sql, "", "")
			require.NoError(t, err)
			cacheable, reason := core.IsASTCacheable(context.Background(), tk.Session().GetPlanCtx(), stmt, is)
			require.True(t, cacheable, "query: %s, reason: %s", testCase.sql, reason)
		})
	}
}

func TestCacheableDMLCTE(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int)")
	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)

	queries := []string{
		"with cte(a) as (select 36) update t set a = 1 where a in (select a from cte)",
		"with cte(a) as (select 36) delete from t where a in (select a from cte)",
	}
	for _, query := range queries {
		stmt, err := parser.New().ParseOneStmt(query, "", "")
		require.NoError(t, err)
		cacheable, reason := core.IsASTCacheable(context.Background(), tk.Session().GetPlanCtx(), stmt, is)
		require.True(t, cacheable, "query: %s, reason: %s", query, reason)
	}
}

func TestIssue49166(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (c int)`)
	tk.MustContainErrMsg(`prepare stmt from "select c from t limit 1 into outfile 'text'"`, "This command is not supported in the prepared statement protocol yet")
}

func TestNonPreparedPlanCacheable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, c int, d int, key(a), key idx_b(b))`)
	tk.MustExec("create table t1(a int, b int, index idx_b(b)) partition by range(a) ( partition p0 values less than (6), partition p1 values less than (11) )")
	tk.MustExec("create table t2(a int, b int) partition by hash(a) partitions 11")
	tk.MustExec("create table t3(a int, b int)")
	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)

	p := parser.New()
	charset := mysql.DefaultCharset
	collation := mysql.DefaultCollationName

	supported := []string{
		"select * from test.t where a<10",
		"select * from test.t where a<13 and b<15",
		"select * from test.t where b=13",
		"select * from test.t where c<8",
		"select * from test.t where d>8",
		"select * from test.t where c=8 and d>10",
		"select * from test.t where a<12 and b<13 and c<12 and d>2",
		"select * from test.t where a in (1, 2, 3)",
		"select * from test.t where a<13 or b<15",
		"select * from test.t where a<13 or b<15 and c=13",
		"select * from test.t where a in (1, 2)",
		"select * from test.t where a in (1, 2) and b in (1, 2, 3)",
		"select * from test.t where a in (1, 2) and b < 15",
		"select * from test.t where a between 1 and 10",
		"select * from test.t where a between 1 and 10 and b < 15",
		"select * from test.t where a+b=13",      // '+'
		"select * from test.t where mod(a, 3)=1", // mod
		"select * from test.t where d>now()",     // now
		"select coalesce(a, 0) from test.t where a<13",
		"select * from test.t where a=coalesce(1, b)",
		"select a+1 from test.t where a<13",
		"select mod(a, 10) from test.t where a<13",
		"select * from test.t limit 1",                                  // limit
		"select distinct a from test.t where a > 1 and b < 2",           // distinct
		"select distinct a from test.t1 where a > 1 and b < 2",          // distinct & partitioned
		"select count(*) from test.t where a > 1 and b < 2 group by a",  // group by
		"select count(*) from test.t1 where a > 1 and b < 2 group by a", // group by & partitioned
		"select * from test.t order by a",                               // order by
		"select * from test.t1 order by a",                              // order by & partitioned
		"select /*+ set_var(max_execution_time=2000) */ * from test.t where a > 1",
		"select /*+ resource_group(rg1) */ * from test.t where a > 1",

		// 2-way joins
		"select * from test.t inner join test.t3 on test.t.a=test.t3.a",
		"select * from test.t inner join test.t3 on test.t.a=test.t3.a where test.t.a<10",
		"select * from test.t, test.t3",
		"select * from test.t, test.t3 where test.t.a=test.t3.a",
		"select * from test.t, test.t3 where test.t.a=test.t3.a and test.t.b=t3.b",
		"select * from test.t, test.t3 where test.t.a=test.t3.a and test.t.a<10",
	}

	unsupported := []string{
		// having
		"select a, sum(b) as c from test.t where a > 1 and b < 2 group by a having sum(b) > 1",
		// having & partitioned
		"select a, sum(b) as c from test.t1 where a > 1 and b < 2 group by a having sum(b) > 1",
		"select /*+ use_index(t1, idx_b) */ * from t where a > 1 and b < 2", // hint
		"select /*+ use_index(t, idx_b) */ * from t1 where a > 1 and b < 2", // hint & partitioned

		"select * from (select * from test.t) t",        // sub-query
		"select * from (select * from test.t1) t",       // sub-query & partitioned
		"insert into test.t values(1, 1, 1, 1)",         // insert
		"insert into test.t1 values(1, 1)",              // insert & partitioned
		"insert into t(a, b) select a, b from test.t",   // insert into select
		"insert into t1(a, b) select a, b from test.t1", // insert into select & partitioned
		"update test.t set a = 1 where b = 2",           // update
		"update test.t1 set a = 1 where b = 2",          // update & partitioned
		"delete from test.t where b = 1",                // delete
		"delete from test.t1 where b = 1",               // delete & partitioned
		"select * from test.t for update",               // lock
		"select * from test.t1 for update",              // lock & partitioned

		// uncorrelated sub-query
		"select * from test.t where a in (select a from t)",
		// uncorrelated sub-query & partitioned
		"select * from test.t1 where a in (select a from t)",
		// correlated sub-query
		"select * from test.t where a in (select a from test.t where a > t1.a)",
		// correlated sub-query & partitioned
		"select * from test.t1 where a in (select a from test.t where a > t1.a)",
	}

	sctx := tk.Session()
	for i, q := range unsupported {
		stmt, err := p.ParseOneStmt(q, charset, collation)
		require.NoError(t, err)
		ok, _ := core.NonPreparedPlanCacheableWithCtx(sctx.GetPlanCtx(), stmt, is)
		require.False(t, ok, "unsupported index: %d: %s", i, q)
	}

	for _, q := range supported {
		stmt, err := p.ParseOneStmt(q, charset, collation)
		require.NoError(t, err)
		ok, _ := core.NonPreparedPlanCacheableWithCtx(sctx.GetPlanCtx(), stmt, is)
		require.True(t, ok)
	}
}

func BenchmarkNonPreparedPlanCacheableChecker(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")

	p := parser.New()
	sql := "select * from test.t where a<10"
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		b.Fatal(err)
	}
	sctx := tk.Session()
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()

	core.NonPreparedPlanCacheableWithCtx(sctx.GetPlanCtx(), stmt, is)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, _ := core.NonPreparedPlanCacheableWithCtx(sctx.GetPlanCtx(), stmt, is)
		if !ok {
			b.Fatal()
		}
	}
}

type nonPreparedPlanCacheBenchmarkCase struct {
	name             string
	sql              string
	legacyCacheable  bool
	unifiedCacheable bool
}

func nonPreparedPlanCacheBenchmarkCases() []nonPreparedPlanCacheBenchmarkCase {
	return []nonPreparedPlanCacheBenchmarkCase{
		{
			name:             "short",
			sql:              "select a from test.t where a = 1",
			legacyCacheable:  true,
			unifiedCacheable: true,
		},
		{
			name: "many_expressions",
			sql: "select a from test.t where a = 1" +
				strings.Repeat(" and (a > 0 or b < 100)", 16),
			legacyCacheable:  true,
			unifiedCacheable: true,
		},
		{
			name:             "join",
			sql:              "select t.a from test.t join test.t2 on t.a = t2.a and t2.b = 1 where t.b = 2",
			legacyCacheable:  true,
			unifiedCacheable: true,
		},
		{
			name:             "cte",
			sql:              "with cte as (select a, b from test.t where b > 1) select a from cte where a = 2",
			legacyCacheable:  false,
			unifiedCacheable: true,
		},
		{
			name:             "uncacheable_function",
			sql:              "select a from test.t where a = 1 and database() = 'test'",
			legacyCacheable:  false,
			unifiedCacheable: false,
		},
	}
}

// BenchmarkNonPreparedPlanCacheColdFrontend compares all AST work performed by
// the legacy and unified frontends when there is no reusable statement carrier.
// Parsing the original SQL and building/optimizing the plan are deliberately
// excluded so the result exposes checker and parameterization overhead.
func BenchmarkNonPreparedPlanCacheColdFrontend(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, key(a))")
	tk.MustExec("create table t2 (a int, b int, key(a))")
	sctx := tk.Session()
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()

	for _, testCase := range nonPreparedPlanCacheBenchmarkCases() {
		b.Run(testCase.name, func(b *testing.B) {
			b.Run("legacy", func(b *testing.B) {
				stmt, err := parser.New().ParseOneStmt(testCase.sql, "", "")
				require.NoError(b, err)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					cacheable, reason := core.NonPreparedPlanCacheableWithCtx(sctx.GetPlanCtx(), stmt, is)
					if cacheable != testCase.legacyCacheable {
						b.Fatalf("unexpected cacheability: cacheable=%v reason=%s", cacheable, reason)
					}
					if !cacheable {
						continue
					}
					paramSQL, _, err := core.GetParamSQLFromAST(stmt)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := core.ParseParameterizedSQL(sctx, paramSQL); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("unified", func(b *testing.B) {
				stmt, err := parser.New().ParseOneStmt(testCase.sql, "", "")
				require.NoError(b, err)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					result, supported, reason := core.ParameterizeForNonPreparedPlanCache(sctx.GetPlanCtx(), stmt)
					if !supported {
						if testCase.unifiedCacheable {
							b.Fatalf("unexpected parameterization rejection: %s", reason)
						}
						continue
					}
					paramStmt, err := core.ParseParameterizedSQL(sctx, result.ParamSQL)
					if err != nil {
						b.Fatal(err)
					}
					if !core.ValidateNonPreparedParamMarkerCount(paramStmt, len(result.ParamValues)) {
						b.Fatal("parameter marker count mismatch")
					}
					cacheable, cacheableReason := core.IsASTCacheable(context.Background(), sctx.GetPlanCtx(), paramStmt, is)
					if cacheable != testCase.unifiedCacheable {
						b.Fatalf("unexpected cacheability: cacheable=%v reason=%s", cacheable, cacheableReason)
					}
				}
			})
		})
	}
}

type nonPreparedPlanCacheExecutionBenchmarkCase struct {
	name               string
	sqls               []string
	statementCacheSize int
	legacyExpectedHit  bool
	unifiedExpectedHit bool
}

func nonPreparedPlanCacheExecutionBenchmarkCases() []nonPreparedPlanCacheExecutionBenchmarkCase {
	return []nonPreparedPlanCacheExecutionBenchmarkCase{
		{
			name:               "short_warm_hit",
			sqls:               []string{"select count(*) from t where b > 0", "select count(*) from t where b > 1"},
			legacyExpectedHit:  true,
			unifiedExpectedHit: true,
		},
		{
			name: "many_expressions_warm_hit",
			sqls: []string{
				"select a from t where a = 1" + strings.Repeat(" and (a > 0 or b < 100)", 16),
				"select a from t where a = 2" + strings.Repeat(" and (a > 1 or b < 101)", 16),
			},
			legacyExpectedHit:  true,
			unifiedExpectedHit: true,
		},
		{
			name: "cte",
			sqls: []string{
				"with cte as (select a, b from t where b > 0) select a from cte where a = 1",
				"with cte as (select a, b from t where b > 1) select a from cte where a = 2",
			},
			legacyExpectedHit:  false,
			unifiedExpectedHit: true,
		},
		{
			name: "statement_lru_miss",
			sqls: []string{
				"select 1001 as identity, a from t where a = 1",
				"select 1002 as identity, a from t where a = 2",
			},
			statementCacheSize: 1,
			legacyExpectedHit:  false,
			unifiedExpectedHit: false,
		},
		{
			name: "uncacheable_function",
			sqls: []string{
				"select a from t where a = 1 and database() = 'test'",
				"select a from t where a = 2 and database() = 'test'",
			},
			legacyExpectedHit:  false,
			unifiedExpectedHit: false,
		},
	}
}

// BenchmarkNonPreparedPlanCacheExecutionPatterns measures the complete request
// path. The hit assertion makes each sub-benchmark's cache behavior explicit so
// a faster early rejection cannot be mistaken for a faster cache hit.
func BenchmarkNonPreparedPlanCacheExecutionPatterns(b *testing.B) {
	store := testkit.CreateMockStore(b)
	setupTK := testkit.NewTestKit(b, store)
	setupTK.MustExec("use test")
	setupTK.MustExec("create table t (a int primary key, b int, key(b))")
	setupTK.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")

	for _, testCase := range nonPreparedPlanCacheExecutionBenchmarkCases() {
		b.Run(testCase.name, func(b *testing.B) {
			for _, unified := range []bool{false, true} {
				name := "legacy"
				expectedHit := testCase.legacyExpectedHit
				if unified {
					name = "unified"
					expectedHit = testCase.unifiedExpectedHit
				}
				b.Run(name, func(b *testing.B) {
					tk := testkit.NewTestKit(b, store)
					tk.MustExec("use test")
					tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
					tk.MustExec(fmt.Sprintf("set tidb_enable_non_prepared_plan_cache_unified=%v", unified))
					if testCase.statementCacheSize > 0 {
						tk.MustExec(fmt.Sprintf("set tidb_session_plan_cache_size=%d", testCase.statementCacheSize))
					}

					for _, sql := range testCase.sqls {
						tk.MustQuery(sql)
					}
					if hit := tk.Session().GetSessionVars().FoundInPlanCache; hit != expectedHit {
						b.Fatalf("unexpected warm-up cache state: hit=%v, expected=%v", hit, expectedHit)
					}

					cacheHits := 0
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						tk.MustQuery(testCase.sqls[i%len(testCase.sqls)])
						hit := tk.Session().GetSessionVars().FoundInPlanCache
						if hit != expectedHit {
							b.Fatalf("unexpected cache state: hit=%v, expected=%v", hit, expectedHit)
						}
						if hit {
							cacheHits++
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(cacheHits)/float64(b.N), "plan_cache_hits/op")
				})
			}
		})
	}
}

func BenchmarkUnifiedNonPreparedPlanCacheabilityCheck(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, key(a))")

	stmt, err := parser.New().ParseOneStmt("select a, count(*) from t where a>1 and b<2 group by a having count(*)>3", "", "")
	require.NoError(b, err)
	sctx := tk.Session()
	result, supported, reason := core.ParameterizeForNonPreparedPlanCache(sctx.GetPlanCtx(), stmt)
	require.True(b, supported, reason)
	paramStmt, err := core.ParseParameterizedSQL(sctx, result.ParamSQL)
	require.NoError(b, err)
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, supported, reason = core.ParameterizeForNonPreparedPlanCache(sctx.GetPlanCtx(), stmt)
		if !supported {
			b.Fatal(reason)
		}
		cacheable, _ := core.IsASTCacheable(context.Background(), sctx.GetPlanCtx(), paramStmt, is)
		if !cacheable {
			b.Fatal("parameterized statement is not cacheable")
		}
	}
}

func BenchmarkUnifiedNonPreparedPlanCacheabilityCheckStatementLRUHit(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, key(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified=on")

	// Warm the statement carrier and the plan cache. The second execution must
	// be a real statement-LRU hit with reusable AST validation provenance.
	tk.MustQuery("select count(*) from t where a > 0")
	if tk.Session().GetSessionVars().FoundInPlanCache {
		b.Fatal("first execution unexpectedly hit the plan cache")
	}
	tk.MustQuery("select count(*) from t where a > 1")
	if !tk.Session().GetSessionVars().FoundInPlanCache {
		b.Fatal("second execution did not hit the plan cache")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		tk.MustQuery("select count(*) from t where a > 2")
	}
}

func BenchmarkUnifiedNonPreparedPlanCacheabilityCheckStatementLRUHitReject(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec(`create table tp (a int, b int, key(a)) partition by range (a) (
		partition p0 values less than (10), partition p1 values less than maxvalue)`)
	tk.MustExec("insert into tp values (1, 1), (11, 11)")
	tk.MustExec("analyze table tp")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified=on")
	tk.MustExec("set tidb_partition_prune_mode=dynamic")

	tk.MustQuery("select b from tp where a > 0")
	if tk.Session().GetSessionVars().FoundInPlanCache {
		b.Fatal("first execution unexpectedly hit the plan cache")
	}
	tk.MustQuery("select b from tp where a > 1")
	if !tk.Session().GetSessionVars().FoundInPlanCache {
		b.Fatal("second execution did not hit the plan cache")
	}

	// Keep the statement carrier, but make the current InfoSchema/session state
	// reject it in the public checker on every statement-LRU lookup.
	tk.MustExec("set tidb_partition_prune_mode=static")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		tk.MustQuery("select b from tp where a > 2")
		if tk.Session().GetSessionVars().FoundInPlanCache {
			b.Fatal("static partition pruning unexpectedly hit the plan cache")
		}
	}
}

func BenchmarkUnifiedNonPreparedPlanCacheabilityCheckUnknownLiteralContext(b *testing.B) {
	bench := func(b *testing.B, unknownContext bool) {
		store := testkit.CreateMockStore(b)
		tk := testkit.NewTestKit(b, store)
		tk.MustExec("use test")
		tk.MustExec("create table t (a int, key(a))")
		tk.MustExec("insert into t values (1), (2), (3)")
		tk.MustExec("set tidb_enable_non_prepared_plan_cache=on")
		tk.MustExec("set tidb_enable_non_prepared_plan_cache_unified=on")

		identities := make(map[string]struct{})
		statementLRUHits := 0
		planCacheHits := 0
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var sql string
			if unknownContext {
				// Literals in the projection are not selected by the context-aware
				// parameterizer and therefore remain part of ParamSQL identity.
				sql = fmt.Sprintf("select %d as preserved_literal, a from t where a > 0 order by a", i)
			} else {
				sql = fmt.Sprintf("select a from t where a > %d order by a", i)
			}

			stmt, err := parser.New().ParseOneStmt(sql, "", "")
			if err != nil {
				b.Fatal(err)
			}
			result, supported, reason := core.ParameterizeForNonPreparedPlanCache(tk.Session().GetPlanCtx(), stmt)
			if !supported {
				b.Fatalf("parameterize failed: %s", reason)
			}
			identities[result.ParamSQL] = struct{}{}
			lookupCtx := context.WithValue(context.Background(), core.PlanCacheKeyTestNonPreparedStmtLookup{}, func(found bool) {
				if found {
					statementLRUHits++
				}
			})
			tk.MustQueryWithContext(lookupCtx, sql)
			if tk.Session().GetSessionVars().FoundInPlanCache {
				planCacheHits++
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(len(identities)), "paramsql_identities")
		b.ReportMetric(float64(statementLRUHits)/float64(b.N), "statement_lru_hits/op")
		b.ReportMetric(float64(planCacheHits)/float64(b.N), "plan_cache_hits/op")
	}

	b.Run("unknown_context_preserve", func(b *testing.B) {
		bench(b, true)
	})
	b.Run("filter_parameterize", func(b *testing.B) {
		bench(b, false)
	})
}
