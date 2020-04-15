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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/v4/expression"
	"github.com/pingcap/tidb/v4/types/parser_driver"
)

var _ = Suite(&testCacheableSuite{})

type testCacheableSuite struct {
}

func (s *testCacheableSuite) TestCacheable(c *C) {
	// test non-SelectStmt/-InsertStmt/-DeleteStmt/-UpdateStmt/-SelectStmt
	var stmt ast.Node = &ast.UnionStmt{}
	c.Assert(Cacheable(stmt), IsFalse)

	stmt = &ast.ShowStmt{}
	c.Assert(Cacheable(stmt), IsFalse)

	stmt = &ast.LoadDataStmt{}
	c.Assert(Cacheable(stmt), IsFalse)

	tableRefsClause := &ast.TableRefsClause{TableRefs: &ast.Join{Left: &ast.TableSource{Source: &ast.TableName{}}}}
	// test InsertStmt
	stmt = &ast.InsertStmt{Table: tableRefsClause}
	c.Assert(Cacheable(stmt), IsTrue)

	// test DeleteStmt
	whereExpr := &ast.FuncCallExpr{}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Where:     whereExpr,
	}
	c.Assert(Cacheable(stmt), IsTrue)

	for funcName := range expression.UnCacheableFunctions {
		whereExpr.FnName = model.NewCIStr(funcName)
		c.Assert(Cacheable(stmt), IsFalse)
	}

	whereExpr.FnName = model.NewCIStr(ast.Rand)
	c.Assert(Cacheable(stmt), IsTrue)

	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Where:     &ast.ExistsSubqueryExpr{},
	}
	c.Assert(Cacheable(stmt), IsFalse)

	limitStmt := &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(Cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(Cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(Cacheable(stmt), IsTrue)

	stmt.(*ast.DeleteStmt).TableHints = append(stmt.(*ast.DeleteStmt).TableHints, &ast.TableOptimizerHint{
		HintName: model.NewCIStr(HintIgnorePlanCache),
	})
	c.Assert(Cacheable(stmt), IsFalse)

	// test UpdateStmt
	whereExpr = &ast.FuncCallExpr{}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Where:     whereExpr,
	}
	c.Assert(Cacheable(stmt), IsTrue)

	for funcName := range expression.UnCacheableFunctions {
		whereExpr.FnName = model.NewCIStr(funcName)
		c.Assert(Cacheable(stmt), IsFalse)
	}

	whereExpr.FnName = model.NewCIStr(ast.Rand)
	c.Assert(Cacheable(stmt), IsTrue)

	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Where:     &ast.ExistsSubqueryExpr{},
	}
	c.Assert(Cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(Cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(Cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(Cacheable(stmt), IsTrue)

	stmt.(*ast.UpdateStmt).TableHints = append(stmt.(*ast.UpdateStmt).TableHints, &ast.TableOptimizerHint{
		HintName: model.NewCIStr(HintIgnorePlanCache),
	})
	c.Assert(Cacheable(stmt), IsFalse)

	// test SelectStmt
	whereExpr = &ast.FuncCallExpr{}
	stmt = &ast.SelectStmt{
		Where: whereExpr,
	}
	c.Assert(Cacheable(stmt), IsTrue)

	for funcName := range expression.UnCacheableFunctions {
		whereExpr.FnName = model.NewCIStr(funcName)
		c.Assert(Cacheable(stmt), IsFalse)
	}

	whereExpr.FnName = model.NewCIStr(ast.Rand)
	c.Assert(Cacheable(stmt), IsTrue)

	stmt = &ast.SelectStmt{
		Where: &ast.ExistsSubqueryExpr{},
	}
	c.Assert(Cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c.Assert(Cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c.Assert(Cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c.Assert(Cacheable(stmt), IsTrue)

	paramExpr := &driver.ParamMarkerExpr{}
	orderByClause := &ast.OrderByClause{Items: []*ast.ByItem{{Expr: paramExpr}}}
	stmt = &ast.SelectStmt{
		OrderBy: orderByClause,
	}
	c.Assert(Cacheable(stmt), IsFalse)

	valExpr := &driver.ValueExpr{}
	orderByClause = &ast.OrderByClause{Items: []*ast.ByItem{{Expr: valExpr}}}
	stmt = &ast.SelectStmt{
		OrderBy: orderByClause,
	}
	c.Assert(Cacheable(stmt), IsTrue)

	stmt.(*ast.SelectStmt).TableHints = append(stmt.(*ast.SelectStmt).TableHints, &ast.TableOptimizerHint{
		HintName: model.NewCIStr(HintIgnorePlanCache),
	})
	c.Assert(Cacheable(stmt), IsFalse)

	boundExpr := &ast.FrameBound{Expr: &driver.ParamMarkerExpr{}}
	c.Assert(Cacheable(boundExpr), IsFalse)
}
