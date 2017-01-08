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

package ast

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
)

var _ = Suite(&testFunctionsSuite{})

type testFunctionsSuite struct {
}

func (ts *testFunctionsSuite) TestAggregateFuncExtractor(c *C) {
	var expr Node

	extractor := &AggregateFuncExtractor{}
	expr = &AggregateFuncExpr{}
	expr.Accept(extractor)
	c.Assert(extractor.AggFuncs, HasLen, 1)
	c.Assert(extractor.AggFuncs[0], Equals, expr)

	extractor = &AggregateFuncExtractor{}
	expr = &FuncCallExpr{
		FnName: model.NewCIStr("FAKE_FUNC"),
	}
	expr.Accept(extractor)
	c.Assert(extractor.AggFuncs, HasLen, 0)

	extractor = &AggregateFuncExtractor{}
	r := &AggregateFuncExpr{}
	expr = &BinaryOperationExpr{
		L: &FuncCallExpr{
			FnName: model.NewCIStr("FAKE_FUNC"),
		},
		R: r,
	}
	expr.Accept(extractor)
	c.Assert(extractor.AggFuncs, HasLen, 1)
	c.Assert(extractor.AggFuncs[0], Equals, r)

	// convert ColumnNameExpr to AggregateFuncExpr
	extractor = &AggregateFuncExtractor{}
	expr = &ColumnNameExpr{}
	f := &SelectField{Expr: expr.(ExprNode)}
	f.Accept(extractor)
	c.Assert(extractor.AggFuncs, HasLen, 1)
	e := extractor.AggFuncs[0]
	c.Assert(e, NotNil)
	c.Assert(e.F, Equals, AggFuncFirstRow)

	// select exists(select count(c) from t) from t
	// subquery contains aggregate function
	expr1 := &AggregateFuncExpr{}
	field1 := &SelectField{Expr: expr1}
	fields1 := &FieldList{Fields: []*SelectField{field1}}
	subSel := &SelectStmt{Fields: fields1}

	subExpr := &ExistsSubqueryExpr{
		Sel: &SubqueryExpr{Query: subSel},
	}
	field := &SelectField{Expr: subExpr}
	fields := &FieldList{Fields: []*SelectField{field}}
	sel := &SelectStmt{Fields: fields}

	extractor = &AggregateFuncExtractor{}
	sel.Accept(extractor)
	c.Assert(extractor.AggFuncs, HasLen, 0)
	extractor = &AggregateFuncExtractor{}
	subSel.Accept(extractor)
	c.Assert(extractor.AggFuncs, HasLen, 1)
}
