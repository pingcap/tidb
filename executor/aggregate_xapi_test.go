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

package executor

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
)

func (s *testAggFuncSuite) TestXAPICount(c *C) {
	defer testleak.AfterTest(c)()
	// Compose aggregate exec for "select count(c2) from t groupby c1";
	// c1  c2
	// 1	1	// region1
	// 2	1	// region1
	// 1    1	// region1
	// 1    nil	// region2
	// 1    1	// region2
	// 2    1	// region2
	//
	// Expected result:
	// count(c2)
	// 3
	// 2
	c1 := ast.NewValueExpr([]byte{0})
	rf1 := &ast.ResultField{Expr: c1}
	c2 := ast.NewValueExpr(0)
	rf2 := &ast.ResultField{Expr: c2}
	c3 := ast.NewValueExpr(nil)
	rf3 := &ast.ResultField{Expr: c3}
	col2 := &ast.ColumnNameExpr{Refer: rf2}
	fc := &ast.AggregateFuncExpr{
		F:    ast.AggFuncCount,
		Args: []ast.ExprNode{col2},
	}
	// Return row:
	// GroupKey, Count
	// Partial result from region1
	row1 := types.MakeDatums([]byte{1}, 2)
	row2 := types.MakeDatums([]byte{2}, 1)
	// Partial result from region2
	row3 := types.MakeDatums([]byte{1}, 1)
	row4 := types.MakeDatums([]byte{2}, 1)
	data := []([]types.Datum){row1, row2, row3, row4}

	rows := make([]*Row, 0, 3)
	for _, d := range data {
		rows = append(rows, &Row{Data: d})
	}
	src := &mockExec{
		rows:   rows,
		fields: []*ast.ResultField{rf1, rf2, rf3},
	}
	agg := &XAggregateExec{
		AggFuncs: []*ast.AggregateFuncExpr{fc},
		Src:      src,
	}
	ast.SetFlag(fc)
	var (
		row *Row
		err error
	)
	// First Row: 3
	row, err = agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	ctx := mock.NewContext()
	val, err := evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(int64(3)))
	// Second Row: 2
	row, err = agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	val, err = evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(int64(2)))
	agg.Close()
	// After clear up, fc's value should be default.
	val, err = evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(int64(0)))
}

func (s *testAggFuncSuite) TestXAPIFirstRow(c *C) {
	defer testleak.AfterTest(c)()
	// Compose aggregate exec for "select c2 from t groupby c1";
	// c1  c2
	// 1	11	// region1
	// 2	21	// region1
	// 1    1	// region1
	// 1    nil	// region2
	// 1    3	// region2
	// 3    31	// region2
	//
	// Expected result:
	// c2
	// 11
	// 21
	// 31
	c1 := ast.NewValueExpr([]byte{0})
	rf1 := &ast.ResultField{Expr: c1}
	c2 := ast.NewValueExpr(0)
	rf2 := &ast.ResultField{Expr: c2}
	col2 := &ast.ColumnNameExpr{Refer: rf2}
	fc := &ast.AggregateFuncExpr{
		F:    ast.AggFuncFirstRow,
		Args: []ast.ExprNode{col2},
	}
	// Return row:
	// GroupKey, Count
	// Partial result from region1
	row1 := types.MakeDatums([]byte{1}, 11)
	row2 := types.MakeDatums([]byte{2}, 21)
	// Partial result from region2
	row3 := types.MakeDatums([]byte{1}, nil)
	row4 := types.MakeDatums([]byte{3}, 31)
	data := []([]types.Datum){row1, row2, row3, row4}

	rows := make([]*Row, 0, 3)
	for _, d := range data {
		rows = append(rows, &Row{Data: d})
	}
	src := &mockExec{
		rows:   rows,
		fields: []*ast.ResultField{rf1, rf2},
	}
	agg := &XAggregateExec{
		AggFuncs: []*ast.AggregateFuncExpr{fc},
		Src:      src,
	}
	ast.SetFlag(fc)
	var (
		row *Row
		err error
	)
	// First Row: 11
	row, err = agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	ctx := mock.NewContext()
	val, err := evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(int64(11)))
	// Second Row: 21
	row, err = agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	val, err = evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(int64(21)))
	// Third Row: 31
	row, err = agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	val, err = evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(int64(31)))
	agg.Close()
	// After clear up, fc's value should be default.
	val, err = evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(nil))
}
