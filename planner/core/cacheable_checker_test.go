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

package core_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/testkit"
)

var _ = Suite(&testCacheableSuite{})

type testCacheableSuite struct {
}

func (s *testCacheableSuite) TestCacheable(c *C) {
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int, b int) partition by range(a) ( partition p0 values less than (6), partition p1 values less than (11) )")
	tk.MustExec("create table t2(a int, b int) partition by hash(a) partitions 11")
	tk.MustExec("create table t3(a int, b int)")
	tbl := &ast.TableName{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t3")}
	is := infoschema.GetInfoSchema(tk.Se)
	// test non-SelectStmt/-InsertStmt/-DeleteStmt/-UpdateStmt/-SelectStmt
	var stmt ast.Node = &ast.UnionStmt{}
	c.Assert(core.Cacheable(stmt, is), IsFalse)

	stmt = &ast.ShowStmt{}
	c.Assert(core.Cacheable(stmt, is), IsFalse)

	stmt = &ast.LoadDataStmt{}
	c.Assert(core.Cacheable(stmt, is), IsFalse)

	tableRefsClause := &ast.TableRefsClause{TableRefs: &ast.Join{Left: &ast.TableSource{Source: tbl}}}
	// test InsertStmt
	stmt = &ast.InsertStmt{Table: tableRefsClause}
	c.Assert(core.Cacheable(stmt, is), IsTrue)

	// test DeleteStmt
	whereExpr := &ast.FuncCallExpr{}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Where:     whereExpr,
	}
	c.Assert(core.Cacheable(stmt, is), IsTrue)

	for funcName := range expression.UnCacheableFunctions {
		whereExpr.FnName = model.NewCIStr(funcName)
		c.Assert(core.Cacheable(stmt, is), IsFalse)
	}

	whereExpr.FnName = model.NewCIStr(ast.Rand)
	c.Assert(core.Cacheable(stmt, is), IsTrue)

	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Where:     &ast.ExistsSubqueryExpr{},
	}
	c.Assert(core.Cacheable(stmt, is), IsFalse)

	limitStmt := &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(core.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(core.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{}
	stmt = &ast.DeleteStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(core.Cacheable(stmt, is), IsTrue)

<<<<<<< HEAD
=======
	stmt.(*ast.DeleteStmt).TableHints = append(stmt.(*ast.DeleteStmt).TableHints, &ast.TableOptimizerHint{
		HintName: model.NewCIStr(core.HintIgnorePlanCache),
	})
	c.Assert(core.Cacheable(stmt, is), IsFalse)

>>>>>>> 79211fe... plan: make query on partition table not cacheable (#16375)
	// test UpdateStmt
	whereExpr = &ast.FuncCallExpr{}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Where:     whereExpr,
	}
	c.Assert(core.Cacheable(stmt, is), IsTrue)

	for funcName := range expression.UnCacheableFunctions {
		whereExpr.FnName = model.NewCIStr(funcName)
		c.Assert(core.Cacheable(stmt, is), IsFalse)
	}

	whereExpr.FnName = model.NewCIStr(ast.Rand)
	c.Assert(core.Cacheable(stmt, is), IsTrue)

	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Where:     &ast.ExistsSubqueryExpr{},
	}
	c.Assert(core.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(core.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(core.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{}
	stmt = &ast.UpdateStmt{
		TableRefs: tableRefsClause,
		Limit:     limitStmt,
	}
	c.Assert(core.Cacheable(stmt, is), IsTrue)

<<<<<<< HEAD
=======
	stmt.(*ast.UpdateStmt).TableHints = append(stmt.(*ast.UpdateStmt).TableHints, &ast.TableOptimizerHint{
		HintName: model.NewCIStr(core.HintIgnorePlanCache),
	})
	c.Assert(core.Cacheable(stmt, is), IsFalse)

>>>>>>> 79211fe... plan: make query on partition table not cacheable (#16375)
	// test SelectStmt
	whereExpr = &ast.FuncCallExpr{}
	stmt = &ast.SelectStmt{
		Where: whereExpr,
	}
	c.Assert(core.Cacheable(stmt, is), IsTrue)

	for funcName := range expression.UnCacheableFunctions {
		whereExpr.FnName = model.NewCIStr(funcName)
		c.Assert(core.Cacheable(stmt, is), IsFalse)
	}

	whereExpr.FnName = model.NewCIStr(ast.Rand)
	c.Assert(core.Cacheable(stmt, is), IsTrue)

	stmt = &ast.SelectStmt{
		Where: &ast.ExistsSubqueryExpr{},
	}
	c.Assert(core.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{
		Count: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c.Assert(core.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{
		Offset: &driver.ParamMarkerExpr{},
	}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c.Assert(core.Cacheable(stmt, is), IsFalse)

	limitStmt = &ast.Limit{}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c.Assert(core.Cacheable(stmt, is), IsTrue)

	paramExpr := &driver.ParamMarkerExpr{}
	orderByClause := &ast.OrderByClause{Items: []*ast.ByItem{{Expr: paramExpr}}}
	stmt = &ast.SelectStmt{
		OrderBy: orderByClause,
	}
	c.Assert(core.Cacheable(stmt, is), IsFalse)

	valExpr := &driver.ValueExpr{}
	orderByClause = &ast.OrderByClause{Items: []*ast.ByItem{{Expr: valExpr}}}
	stmt = &ast.SelectStmt{
		OrderBy: orderByClause,
	}
	c.Assert(core.Cacheable(stmt, is), IsTrue)

<<<<<<< HEAD
=======
	stmt.(*ast.SelectStmt).TableHints = append(stmt.(*ast.SelectStmt).TableHints, &ast.TableOptimizerHint{
		HintName: model.NewCIStr(core.HintIgnorePlanCache),
	})
	c.Assert(core.Cacheable(stmt, is), IsFalse)

>>>>>>> 79211fe... plan: make query on partition table not cacheable (#16375)
	boundExpr := &ast.FrameBound{Expr: &driver.ParamMarkerExpr{}}
	c.Assert(core.Cacheable(boundExpr, is), IsFalse)

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
	c.Assert(core.Cacheable(stmt, is), IsFalse)

	join = &ast.Join{
		Left: &ast.TableName{Schema: model.NewCIStr("test"), Name: model.NewCIStr("t3")},
	}
	stmt = &ast.SelectStmt{
		From: &ast.TableRefsClause{
			TableRefs: join,
		},
	}
	c.Assert(core.Cacheable(stmt, is), IsTrue)
}
