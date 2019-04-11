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
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/expression"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestPreparedNameResolver(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, KEY id (id))")
	tk.MustExec("prepare stmt from 'select * from t limit ? offset ?'")
	_, err := tk.Exec("prepare stmt from 'select b from t'")
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'b' in 'field list'")

	_, err = tk.Exec("prepare stmt from '(select * FROM t) union all (select * FROM t) order by a limit ?'")
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'a' in 'order clause'")
}

func (s *testSuite1) TestCacheable(c *C) {
	// test non-SelectStmt/-InsertStmt/-DeleteStmt/-UpdateStmt/-SelectStmt
	var stmt ast.Node = &ast.UnionStmt{}
	c.Assert(cacheable(stmt), IsFalse)

	stmt = &ast.ShowStmt{}
	c.Assert(cacheable(stmt), IsFalse)

	stmt = &ast.LoadDataStmt{}
	c.Assert(cacheable(stmt), IsFalse)

	tableRefsClause := &ast.TableRefsClause{TableRefs: &ast.Join{Left: &ast.TableSource{Source: &ast.TableName{}}}}
	// test InsertStmt
	stmt = &ast.InsertStmt{Table: tableRefsClause}
	c.Assert(cacheable(stmt), IsTrue)

	// test DeleteStmt
	whereExpr := &ast.FuncCallExpr{}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Where:     whereExpr,
	}
	c.Assert(cacheable(stmt), IsTrue)

	for funcName := range expression.UnCacheableFunctions {
		whereExpr.FnName = model.NewCIStr(funcName)
		c.Assert(cacheable(stmt), IsFalse)
	}

	whereExpr.FnName = model.NewCIStr(ast.Rand)
	c.Assert(cacheable(stmt), IsTrue)

	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Where:     &ast.ExistsSubqueryExpr{Sel: &driver.ValueExpr{}},
	}
	c.Assert(cacheable(stmt), IsFalse)

	limitStmt := &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(cacheable(stmt), IsTrue)

	// test UpdateStmt
	whereExpr = &ast.FuncCallExpr{}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Where:     whereExpr,
	}
	c.Assert(cacheable(stmt), IsTrue)

	for funcName := range expression.UnCacheableFunctions {
		whereExpr.FnName = model.NewCIStr(funcName)
		c.Assert(cacheable(stmt), IsFalse)
	}

	whereExpr.FnName = model.NewCIStr(ast.Rand)
	c.Assert(cacheable(stmt), IsTrue)

	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Where:     &ast.ExistsSubqueryExpr{Sel: &driver.ValueExpr{}},
	}
	c.Assert(cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(cacheable(stmt), IsTrue)

	// test SelectStmt
	whereExpr = &ast.FuncCallExpr{}
	stmt = &ast.SelectStmt{
		Where: whereExpr,
	}
	c.Assert(cacheable(stmt), IsTrue)

	for funcName := range expression.UnCacheableFunctions {
		whereExpr.FnName = model.NewCIStr(funcName)
		c.Assert(cacheable(stmt), IsFalse)
	}

	whereExpr.FnName = model.NewCIStr(ast.Rand)
	c.Assert(cacheable(stmt), IsTrue)

	stmt = &ast.SelectStmt{
		Where: &ast.ExistsSubqueryExpr{Sel: &driver.ValueExpr{}},
	}
	c.Assert(cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c.Assert(cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c.Assert(cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c.Assert(cacheable(stmt), IsTrue)

	paramExpr := &driver.ParamMarkerExpr{}
	orderByClause := &ast.OrderByClause{Items: []*ast.ByItem{{Expr: paramExpr}}}
	stmt = &ast.SelectStmt{
		OrderBy: orderByClause,
	}
	c.Assert(cacheable(stmt), IsFalse)

	valExpr := &driver.ValueExpr{}
	orderByClause = &ast.OrderByClause{Items: []*ast.ByItem{{Expr: valExpr}}}
	stmt = &ast.SelectStmt{
		OrderBy: orderByClause,
	}
	c.Assert(cacheable(stmt), IsTrue)
}

func cacheable(stmt ast.Node) bool {
	v := &executor.PrepareVisitor{CheckCacheable: true}
	stmt.Accept(v)
	return v.Cacheable(stmt)
}
