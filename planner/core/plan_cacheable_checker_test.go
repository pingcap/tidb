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
	"testing"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/testkit"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

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
		HintName: model.NewCIStr(core.HintIgnorePlanCache),
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
		HintName: model.NewCIStr(core.HintIgnorePlanCache),
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
		HintName: model.NewCIStr(core.HintIgnorePlanCache),
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
}

func TestNonPreparedPlanCacheable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, c int, d int, key(a), key(b))`)
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
		"select a+1 from test.t where a<13",
		"select mod(a, 10) from test.t where a<13",
		"select * from test.t limit 1", // limit

		// 2-way joins
		"select * from test.t inner join test.t3 on test.t.a=test.t3.a",
		"select * from test.t inner join test.t3 on test.t.a=test.t3.a where test.t.a<10",
		"select * from test.t, test.t3",
		"select * from test.t, test.t3 where test.t.a=test.t3.a",
		"select * from test.t, test.t3 where test.t.a=test.t3.a and test.t.b=t3.b",
		"select * from test.t, test.t3 where test.t.a=test.t3.a and test.t.a<10",
	}

	unsupported := []string{
		"select /*+ use_index(t1, idx_b) */ * from t1 where a > 1 and b < 2",                    // hint
		"select distinct a from test.t1 where a > 1 and b < 2",                                  // distinct
		"select count(*) from test.t1 where a > 1 and b < 2 group by a",                         // group by
		"select a, sum(b) as c from test.t1 where a > 1 and b < 2 group by a having sum(b) > 1", // having
		"select * from test.t1 order by a",                                                      // order by
		"select * from (select * from test.t1) t",                                               // sub-query
		"insert into test.t1 values(1, 1)",                                                      // insert
		"insert into t1(a, b) select a, b from test.t1",                                         // insert into select
		"update test.t1 set a = 1 where b = 2",                                                  // update
		"delete from test.t1 where b = 1",                                                       // delete
		"select * from test.t1 for update",                                                      // lock
		"select * from test.t1 where a in (select a from t)",                                    // uncorrelated sub-query
		"select * from test.t1 where a in (select a from test.t where a > t1.a)",                // correlated sub-query
	}

	sctx := tk.Session()
	for _, q := range unsupported {
		stmt, err := p.ParseOneStmt(q, charset, collation)
		require.NoError(t, err)
		ok, _ := core.NonPreparedPlanCacheableWithCtx(sctx, stmt, is)
		require.False(t, ok)
	}

	for _, q := range supported {
		stmt, err := p.ParseOneStmt(q, charset, collation)
		require.NoError(t, err)
		ok, _ := core.NonPreparedPlanCacheableWithCtx(sctx, stmt, is)
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

	core.NonPreparedPlanCacheableWithCtx(sctx, stmt, is)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ok, _ := core.NonPreparedPlanCacheableWithCtx(sctx, stmt, is)
		if !ok {
			b.Fatal()
		}
	}
}
