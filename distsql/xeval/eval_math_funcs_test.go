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
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

func (s *testEvalSuite) TestEvalAbs(c *C) {
	colID := int64(1)
	row := make(map[int64]types.Datum)
	row[colID] = types.NewIntDatum(100)
	xevaluator := &Evaluator{Row: row}
	cases := []struct {
		expr   *tipb.Expr
		result types.Datum
	}{
		{
			expr:   buildExpr(tipb.ExprType_Abs, types.Datum{}),
			result: types.Datum{},
		},
		{
			expr:   buildExpr(tipb.ExprType_Abs, types.NewIntDatum(-1)),
			result: types.NewIntDatum(1),
		},
		{
			expr:   buildExpr(tipb.ExprType_Abs, types.NewUintDatum(1)),
			result: types.NewUintDatum(1),
		},
		{
			expr:   buildExpr(tipb.ExprType_Abs, types.NewFloat64Datum(-1.5)),
			result: types.NewFloat64Datum(1.5),
		},
		{
			expr:   buildExpr(tipb.ExprType_Abs, types.NewStringDatum("-1.456")),
			result: types.NewFloat64Datum((1.456)),
		},
	}
	for _, ca := range cases {
		result, err := xevaluator.Eval(ca.expr)
		c.Assert(err, IsNil)
		c.Assert(result.Kind(), Equals, ca.result.Kind())
		cmp, err := result.CompareDatum(ca.result)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func (s *testEvalSuite) TestEvalPow(c *C) {
	colID := int64(1)
	row := make(map[int64]types.Datum)
	row[colID] = types.NewIntDatum(100)
	xevaluator := &Evaluator{Row: row}
	cases := []struct {
		expr   *tipb.Expr
		result types.Datum
	}{
		{
			expr:   buildExpr(tipb.ExprType_Pow, types.Datum{}, types.NewIntDatum(1)),
			result: types.Datum{},
		},
		{
			expr:   buildExpr(tipb.ExprType_Pow, types.NewIntDatum(1), types.Datum{}),
			result: types.Datum{},
		},
		{
			expr:   buildExpr(tipb.ExprType_Pow, types.NewIntDatum(2), types.NewIntDatum(10)),
			result: types.NewFloat64Datum(1024),
		},
		{
			expr:   buildExpr(tipb.ExprType_Pow, types.NewFloat64Datum(0.5), types.NewIntDatum(-1)),
			result: types.NewFloat64Datum(2),
		},
	}
	for _, ca := range cases {
		result, err := xevaluator.Eval(ca.expr)
		c.Assert(err, IsNil)
		c.Assert(result.Kind(), Equals, ca.result.Kind())
		cmp, err := result.CompareDatum(ca.result)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}

func (s *testEvalSuite) TestEvalRound(c *C) {
	colID := int64(1)
	row := make(map[int64]types.Datum)
	row[colID] = types.NewIntDatum(100)
	xevaluator := &Evaluator{Row: row}
	ans := new(mysql.MyDecimal)
	cases := []struct {
		expr   *tipb.Expr
		result string
	}{
		{
			expr:   buildExpr(tipb.ExprType_Round, types.NewFloat64Datum(123.4), types.NewIntDatum(0)),
			result: "123",
		},
		{
			expr:   buildExpr(tipb.ExprType_Round, types.NewFloat64Datum(123.4), types.NewIntDatum(0)),
			result: "123",
		},
		{
			expr:   buildExpr(tipb.ExprType_Round, types.NewFloat64Datum(123.5), types.NewIntDatum(0)),
			result: "124",
		},
		{
			expr:   buildExpr(tipb.ExprType_Round, types.NewFloat64Datum(123.434), types.NewIntDatum(-1)),
			result: "120",
		},
		{
			expr:   buildExpr(tipb.ExprType_Round, types.NewFloat64Datum(123.45), types.NewIntDatum(1)),
			result: "123.5",
		},
	}
	for _, ca := range cases {
		result, err := xevaluator.Eval(ca.expr)
		c.Assert(err, IsNil)
		ans.FromString([]byte(ca.result))
		x := types.NewDecimalDatum(ans)
		c.Assert(result.Kind(), Equals, x.Kind())
		cmp, err := result.CompareDatum(x)
		c.Assert(err, IsNil)
		c.Assert(cmp, Equals, 0)
	}
}
