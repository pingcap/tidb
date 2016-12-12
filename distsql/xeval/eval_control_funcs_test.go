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

package xeval

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

func (s *testEvalSuite) TestEvalCaseWhen(c *C) {
	colID := int64(1)
	xevaluator := NewEvaluator(new(variable.StatementContext))
	xevaluator.Row[colID] = types.NewIntDatum(100)
	trueCond := types.NewIntDatum(1)
	falseCond := types.NewIntDatum(0)
	nullCond := types.Datum{}
	nullCond.SetNull()
	cases := []struct {
		expr   *tipb.Expr
		result types.Datum
	}{
		{
			expr: buildExpr(tipb.ExprType_Case,
				falseCond, types.NewStringDatum("case1"),
				trueCond, types.NewStringDatum("case2"),
				trueCond, types.NewStringDatum("case3")),
			result: types.NewStringDatum("case2"),
		},
		{
			expr: buildExpr(tipb.ExprType_Case,
				falseCond, types.NewStringDatum("case1"),
				falseCond, types.NewStringDatum("case2"),
				falseCond, types.NewStringDatum("case3"),
				types.NewStringDatum("Else")),
			result: types.NewStringDatum("Else"),
		},
		{
			expr: buildExpr(tipb.ExprType_Case,
				falseCond, types.NewStringDatum("case1"),
				falseCond, types.NewStringDatum("case2"),
				falseCond, types.NewStringDatum("case3")),
			result: types.Datum{},
		},
		{
			expr: buildExpr(tipb.ExprType_Case,
				buildExpr(tipb.ExprType_Case,
					falseCond, types.NewIntDatum(0),
					trueCond, types.NewIntDatum(1),
				), types.NewStringDatum("nested case when"),
				falseCond, types.NewStringDatum("case1"),
				trueCond, types.NewStringDatum("case2"),
				trueCond, types.NewStringDatum("case3")),
			result: types.NewStringDatum("nested case when"),
		},
		{
			expr: buildExpr(tipb.ExprType_Case,
				nullCond, types.NewStringDatum("case1"),
				falseCond, types.NewStringDatum("case2"),
				trueCond, types.NewStringDatum("case3")),
			result: types.NewStringDatum("case3"),
		},
	}
	for _, ca := range cases {
		result, err := xevaluator.Eval(ca.expr)
		c.Assert(err, IsNil)
		c.Assert(result.Kind(), Equals, ca.result.Kind())
		cmp, err := result.CompareDatum(xevaluator.sc, ca.result)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func (s *testEvalSuite) TestEvalIf(c *C) {
	colID := int64(1)
	xevaluator := NewEvaluator(new(variable.StatementContext))
	xevaluator.Row[colID] = types.NewIntDatum(100)
	trueCond, falseCond, null := types.NewIntDatum(1), types.NewIntDatum(0), types.Datum{}
	expr1, expr2 := types.NewStringDatum("expr1"), types.NewStringDatum("expr2")
	cases := []struct {
		expr   *tipb.Expr
		result types.Datum
	}{
		{
			expr: buildExpr(tipb.ExprType_If,
				trueCond, types.NewStringDatum("expr1"), types.NewStringDatum("expr2")),
			result: expr1,
		},
		{
			expr: buildExpr(tipb.ExprType_If,
				falseCond, types.NewStringDatum("expr1"), types.NewStringDatum("expr2")),
			result: expr2,
		},
		{
			expr: buildExpr(tipb.ExprType_If,
				null, types.NewStringDatum("expr1"), types.NewStringDatum("expr2")),
			result: expr2,
		},
		{
			expr: buildExpr(tipb.ExprType_If,
				trueCond, null, types.NewStringDatum("expr2")),
			result: null,
		},
		{
			expr: buildExpr(tipb.ExprType_If,
				falseCond, types.NewStringDatum("expr1"), null),
			result: null,
		},
		{
			expr: buildExpr(tipb.ExprType_If,
				trueCond, types.NewStringDatum("expr1"), types.NewStringDatum("expr2")),
			result: expr1,
		},
		{
			expr: buildExpr(tipb.ExprType_If, buildExpr(tipb.ExprType_If, trueCond, null, trueCond),
				buildExpr(tipb.ExprType_If, trueCond, expr1, expr2),
				buildExpr(tipb.ExprType_If, falseCond, expr1, expr2)),
			result: expr2,
		},
	}
	for _, ca := range cases {
		result, err := xevaluator.Eval(ca.expr)
		c.Assert(err, IsNil)
		c.Assert(result.Kind(), Equals, ca.result.Kind())
		cmp, err := result.CompareDatum(xevaluator.sc, ca.result)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func (s *testEvalSuite) TestEvalNullIf(c *C) {
	colID := int64(1)
	xevaluator := NewEvaluator(new(variable.StatementContext))
	xevaluator.Row[colID] = types.NewDatum(100)
	null := types.Datum{}
	cases := []struct {
		expr   *tipb.Expr
		result types.Datum
	}{
		{
			expr: buildExpr(tipb.ExprType_NullIf,
				types.NewStringDatum("abc"), types.NewStringDatum("abc")),
			result: null,
		},
		{
			expr: buildExpr(tipb.ExprType_NullIf,
				null, null),
			result: null,
		},
		{
			expr: buildExpr(tipb.ExprType_NullIf,
				types.NewIntDatum(123), types.NewIntDatum(111)),
			result: types.NewIntDatum(123),
		},
		{
			expr: buildExpr(tipb.ExprType_NullIf,
				types.NewIntDatum(123), null),
			result: types.NewIntDatum(123),
		},
	}
	for _, ca := range cases {
		result, err := xevaluator.Eval(ca.expr)
		c.Assert(err, IsNil)
		c.Assert(result.Kind(), Equals, ca.result.Kind())
		cmp, err := result.CompareDatum(xevaluator.sc, ca.result)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func (s *testEvalSuite) TestEvalIfNull(c *C) {
	colID := int64(1)
	xevaluator := NewEvaluator(new(variable.StatementContext))
	xevaluator.Row[colID] = types.NewDatum(100)
	null, notNull, expr := types.Datum{}, types.NewStringDatum("left"), types.NewStringDatum("right")
	cases := []struct {
		expr   *tipb.Expr
		result types.Datum
	}{
		{
			expr: buildExpr(tipb.ExprType_IfNull,
				null, expr),
			result: expr,
		},
		{
			expr: buildExpr(tipb.ExprType_IfNull,
				notNull, expr),
			result: notNull,
		},
		{
			expr: buildExpr(tipb.ExprType_IfNull,
				notNull, null),
			result: notNull,
		},
	}
	for _, ca := range cases {
		result, err := xevaluator.Eval(ca.expr)
		c.Assert(err, IsNil)
		c.Assert(result.Kind(), Equals, ca.result.Kind())
		cmp, err := result.CompareDatum(xevaluator.sc, ca.result)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}
