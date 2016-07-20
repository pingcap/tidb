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
	"github.com/pingcap/tidb/mysql"
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
	// First Row: 3
	row, err := agg.Next()
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
	// First Row: 11
	row, err := agg.Next()
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

func (s *testAggFuncSuite) TestXAPISum(c *C) {
	defer testleak.AfterTest(c)()
	// Compose aggregate exec for "select sum(c2) from t groupby c1";
	//
	// Data in region1:
	// c1  c2
	// 1	11
	// 2	21
	// 1    1
	//
	// Partial aggregate result for region1:
	// groupkey	sum
	// 1		12
	// 2		21
	//
	// Data in region2:
	// 1    nil
	// 1    3
	// 3    31
	//
	// Partial aggregate result for region2:
	// groupkey	sum
	// 1		3
	// 3		31
	//
	// Expected final aggregate result:
	// sum(c2)
	// 15
	// 21
	// 31
	c1 := ast.NewValueExpr([]byte{0})
	rf1 := &ast.ResultField{Expr: c1}
	c2 := ast.NewValueExpr(0)
	rf2 := &ast.ResultField{Expr: c2}
	col2 := &ast.ColumnNameExpr{Refer: rf2}
	fc := &ast.AggregateFuncExpr{
		F:    ast.AggFuncSum,
		Args: []ast.ExprNode{col2},
	}
	// Return row:
	// GroupKey, Sum
	// Partial result from region1
	row1 := types.MakeDatums([]byte{1}, 12)
	row2 := types.MakeDatums([]byte{2}, 21)
	// Partial result from region2
	row3 := types.MakeDatums([]byte{1}, 3)
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
	// First row: 15
	row, err := agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	ctx := mock.NewContext()
	val, err := evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDecimalDatum(mysql.NewDecimalFromInt(int64(15), 0)))
	// Second row: 21
	row, err = agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	val, err = evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDecimalDatum(mysql.NewDecimalFromInt(int64(21), 0)))
	// Third row: 31
	row, err = agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	val, err = evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDecimalDatum(mysql.NewDecimalFromInt(int64(31), 0)))
	agg.Close()
	// After clear up, fc's value should be default.
	val, err = evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(nil))
}

func (s *testAggFuncSuite) TestXAPIAvg(c *C) {
	defer testleak.AfterTest(c)()
	// Compose aggregate exec for "select avg(c2) from t groupby c1";
	//
	// Data in region1:
	// c1  c2
	// 1	11
	// 2	21
	// 1    1
	// 3    2
	//
	// Partial aggregate result for region1:
	// groupkey	cnt	sum
	// 1		2	12
	// 2		1	21
	// 3		1	2
	//
	// Data in region2:
	// 1    nil
	// 1    3
	// 3    31
	//
	// Partial aggregate result for region2:
	// groupkey	cnt	sum
	// 1		1	3
	// 3		1	31
	//
	// Expected final aggregate result:
	// avg(c2)
	// 5
	// 21
	// 16.500000
	c1 := ast.NewValueExpr([]byte{0})
	rf1 := &ast.ResultField{Expr: c1}
	c2 := ast.NewValueExpr(0)
	rf2 := &ast.ResultField{Expr: c2}
	c3 := ast.NewValueExpr(0)
	rf3 := &ast.ResultField{Expr: c3}
	col2 := &ast.ColumnNameExpr{Refer: rf2}
	fc := &ast.AggregateFuncExpr{
		F:    ast.AggFuncAvg,
		Args: []ast.ExprNode{col2},
	}
	// Return row:
	// GroupKey, Sum
	// Partial result from region1
	row1 := types.MakeDatums([]byte{1}, 2, 12)
	row2 := types.MakeDatums([]byte{2}, 1, 21)
	row3 := types.MakeDatums([]byte{3}, 1, 2)
	// Partial result from region2
	row4 := types.MakeDatums([]byte{1}, 1, 3)
	row5 := types.MakeDatums([]byte{3}, 1, 31)
	data := []([]types.Datum){row1, row2, row3, row4, row5}

	rows := make([]*Row, 0, 5)
	for _, d := range data {
		rows = append(rows, &Row{Data: d})
	}
	src := &mockExec{
		rows:   rows,
		fields: []*ast.ResultField{rf1, rf2, rf3}, // groupby, cnt, sum
	}
	agg := &XAggregateExec{
		AggFuncs: []*ast.AggregateFuncExpr{fc},
		Src:      src,
	}
	ast.SetFlag(fc)
	// First row: 5
	row, err := agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	ctx := mock.NewContext()
	val, err := evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDecimalDatum(mysql.NewDecimalFromInt(int64(5), 0)))
	// Second row: 21
	row, err = agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	val, err = evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDecimalDatum(mysql.NewDecimalFromInt(int64(21), 0)))
	// Third row: 16.5000
	row, err = agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	val, err = evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	d := mysql.NewDecimalFromFloat(float64(16.5))
	d.SetFracDigits(4) // For div operator, default frac is 4.
	c.Assert(val, testutil.DatumEquals, types.NewDecimalDatum(d))
	// Forth row: nil
	row, err = agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, IsNil)
	// Close executor
	err = agg.Close()
	c.Assert(err, IsNil)
}

func (s *testAggFuncSuite) TestXAPIMaxMin(c *C) {
	defer testleak.AfterTest(c)()
	// Compose aggregate exec for "select max(c2), min(c2) from t groupby c1";
	//
	// Data in region1:
	// c1  c2
	// 1	11
	// 2	21
	// 1    1
	// 3    2
	//
	// Partial aggregate result for region1:
	// groupkey	max(c2)	min(c2)
	// 1		11	1
	// 2		21	21
	// 3		2	2
	//
	// Data in region2:
	// 1    nil
	// 1    3
	// 3    31
	// 4	nil
	//
	// Partial aggregate result for region2:
	// groupkey	max(c2)	min(c2)
	// 1		3	3
	// 3		31	31
	// 4		nil	nil
	//
	// Expected final aggregate result:
	// max(c2)	min(c2)
	// 11		1
	// 21		21
	// 31		2
	// nil		nil
	c1 := ast.NewValueExpr([]byte{0})
	rf1 := &ast.ResultField{Expr: c1}
	c2 := ast.NewValueExpr(0)
	rf2 := &ast.ResultField{Expr: c2}
	c3 := ast.NewValueExpr(0)
	rf3 := &ast.ResultField{Expr: c3}
	col2 := &ast.ColumnNameExpr{Refer: rf2}
	fc := &ast.AggregateFuncExpr{
		F:    ast.AggFuncMax,
		Args: []ast.ExprNode{col2},
	}
	fc1 := &ast.AggregateFuncExpr{
		F:    ast.AggFuncMin,
		Args: []ast.ExprNode{col2},
	}
	ast.SetFlag(fc)
	ast.SetFlag(fc1)
	// Return row:
	// GroupKey, max(c2), min(c2)
	// Partial result from region1
	row1 := types.MakeDatums([]byte{1}, int64(11), int64(1))
	row2 := types.MakeDatums([]byte{2}, int64(21), int64(21))
	row3 := types.MakeDatums([]byte{3}, int64(2), int64(2))
	// Partial result from region2
	row4 := types.MakeDatums([]byte{1}, int64(3), int64(3))
	row5 := types.MakeDatums([]byte{3}, int64(31), int64(31))
	row6 := types.MakeDatums([]byte{4}, nil, nil)
	data := []([]types.Datum){row1, row2, row3, row4, row5, row6}

	rows := make([]*Row, 0, 6)
	for _, d := range data {
		rows = append(rows, &Row{Data: d})
	}
	src := &mockExec{
		rows:   rows,
		fields: []*ast.ResultField{rf1, rf2, rf3}, // group, max(c2), min(c2)
	}
	agg := &XAggregateExec{
		AggFuncs: []*ast.AggregateFuncExpr{fc, fc1},
		Src:      src,
	}
	ast.SetFlag(fc)
	// First row: 11, 1
	row, err := agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	ctx := mock.NewContext()
	val, err := evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(int64(11)))
	val, err = evaluator.Eval(ctx, fc1)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(int64(1)))

	// Second row: 21, 21
	row, err = agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	val, err = evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(int64(21)))
	val, err = evaluator.Eval(ctx, fc1)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(int64(21)))

	// Third row: 31, 2
	row, err = agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	val, err = evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(int64(31)))
	val, err = evaluator.Eval(ctx, fc1)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(int64(2)))

	// Forth row: nil, nil
	row, err = agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, NotNil)
	val, err = evaluator.Eval(ctx, fc)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(nil))
	val, err = evaluator.Eval(ctx, fc1)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(nil))

	// Fifth row: nil
	row, err = agg.Next()
	c.Assert(err, IsNil)
	c.Assert(row, IsNil)
	// Close executor
	err = agg.Close()
	c.Assert(err, IsNil)
}
