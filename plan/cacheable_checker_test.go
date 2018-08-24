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

package plan

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
)

var _ = Suite(&testCacheableSuite{})

type testCacheableSuite struct {
}

func (s *testCacheableSuite) TestCacheable(c *C) {
	// test non-SelectStmt
	var stmt ast.Node = &ast.DeleteStmt{}
	c.Assert(Cacheable(stmt), IsFalse)

	stmt = &ast.InsertStmt{}
	c.Assert(Cacheable(stmt), IsFalse)

	stmt = &ast.UnionStmt{}
	c.Assert(Cacheable(stmt), IsFalse)

	stmt = &ast.UpdateStmt{}
	c.Assert(Cacheable(stmt), IsFalse)

	stmt = &ast.ShowStmt{}
	c.Assert(Cacheable(stmt), IsFalse)

	stmt = &ast.LoadDataStmt{}
	c.Assert(Cacheable(stmt), IsFalse)

	// test SelectStmt
	whereExpr := &ast.FuncCallExpr{}
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

	limitStmt := &ast.Limit{
		Count: &ast.ParamMarkerExpr{},
	}
	stmt = &ast.SelectStmt{
		Limit: limitStmt,
	}
	c.Assert(Cacheable(stmt), IsFalse)

	limitStmt = &ast.Limit{
		Offset: &ast.ParamMarkerExpr{},
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
}
