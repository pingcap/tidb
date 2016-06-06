// Copyright 2015 PingCAP, Inc.
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

var _ = Suite(&testAggFuncSuite{})

type testAggFuncSuite struct {
}

func (s *testAggFuncSuite) SetUpSuite(c *C) {
}

func (s *testAggFuncSuite) TearDownSuite(c *C) {
}

type mockExec struct {
	fields    []*ast.ResultField
	rows      []*Row
	curRowIdx int
}

func (m *mockExec) Fields() []*ast.ResultField {
	return m.fields
}

func (m *mockExec) Next() (*Row, error) {
	if m.curRowIdx >= len(m.rows) {
		return nil, nil
	}
	r := m.rows[m.curRowIdx]
	m.curRowIdx++
	for i, d := range r.Data {
		m.fields[i].Expr.SetValue(d.GetValue())
	}
	return r, nil
}

func (m *mockExec) Close() error {
	return nil
}

func (s *testAggFuncSuite) TestCount(c *C) {
	defer testleak.AfterTest(c)()
	// Compose aggregate exec for "select c1, count(c2) from t";
	// c1  c2
	// 1	1
	// 2	1
	// 3    nil
	c1 := ast.NewValueExpr(0)
	rf1 := &ast.ResultField{Expr: c1}
	col1 := &ast.ColumnNameExpr{Refer: rf1}
	fc1 := &ast.AggregateFuncExpr{
		F:    ast.AggFuncFirstRow,
		Args: []ast.ExprNode{col1},
	}
	c2 := ast.NewValueExpr(0)
	rf2 := &ast.ResultField{Expr: c2}
	col2 := &ast.ColumnNameExpr{Refer: rf2}
	fc2 := &ast.AggregateFuncExpr{
		F:    ast.AggFuncCount,
		Args: []ast.ExprNode{col2},
	}
	row1 := types.MakeDatums(1, 1)
	row2 := types.MakeDatums(2, 1)
	row3 := types.MakeDatums(3, nil)
	data := []([]types.Datum){row1, row2, row3}

	rows := make([]*Row, 0, 3)
	for _, d := range data {
		rows = append(rows, &Row{Data: d})
	}
	src := &mockExec{
		rows:   rows,
		fields: []*ast.ResultField{rf1, rf2},
	}
	agg := &AggregateExec{
		AggFuncs: []*ast.AggregateFuncExpr{fc1, fc2},
		Src:      src,
	}
	ast.SetFlag(fc1)
	ast.SetFlag(fc2)
	var (
		row *Row
		cnt int
	)
	for {
		r, err := agg.Next()
		c.Assert(err, IsNil)
		if r == nil {
			break
		}
		row = r
		cnt++
	}
	c.Assert(cnt, Equals, 1)
	c.Assert(row, NotNil)
	ctx := mock.NewContext()
	val, err := evaluator.Eval(ctx, fc1)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(int64(1)))
	val, err = evaluator.Eval(ctx, fc2)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(int64(2)))

	agg.Close()
	val, err = evaluator.Eval(ctx, fc1)
	c.Assert(err, IsNil)
	c.Assert(val.Kind(), Equals, types.KindNull)
	val, err = evaluator.Eval(ctx, fc2)
	c.Assert(err, IsNil)
	c.Assert(val, testutil.DatumEquals, types.NewDatum(int64(0)))
}
