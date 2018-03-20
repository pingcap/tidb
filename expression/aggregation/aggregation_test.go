// Copyright 2018 PingCAP, Inc.
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

package aggregation

import (
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testAggFuncSuit{})

type testAggFuncSuit struct {
	ctx     sessionctx.Context
	rows    []types.DatumRow
	nullRow types.DatumRow
}

func generateRowData() []types.DatumRow {
	rows := make([]types.DatumRow, 0, 5050)
	for i := 1; i <= 100; i++ {
		for j := 0; j < i; j++ {
			rows = append(rows, types.MakeDatums(i))
		}
	}
	return rows
}

func (s *testAggFuncSuit) SetUpSuite(c *C) {
	s.ctx = mock.NewContext()
	s.rows = generateRowData()
	s.nullRow = []types.Datum{{}}
}

func (s *testAggFuncSuit) TestAvg(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	avgFunc := NewAggFuncDesc(s.ctx, ast.AggFuncAvg, []expression.Expression{col}, false).GetAggFunc()
	evalCtx := avgFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := avgFunc.GetResult(evalCtx)
	c.Assert(result.IsNull(), IsTrue)

	for _, row := range s.rows {
		err := avgFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
		c.Assert(err, IsNil)
	}
	result = avgFunc.GetResult(evalCtx)
	needed := types.NewDecFromStringForTest("67.000000000000000000000000000000")
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)
	err := avgFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, s.nullRow)
	c.Assert(err, IsNil)
	result = avgFunc.GetResult(evalCtx)
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)

	distinctAvgFunc := NewAggFuncDesc(s.ctx, ast.AggFuncAvg, []expression.Expression{col}, true).GetAggFunc()
	evalCtx = distinctAvgFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)
	for _, row := range s.rows {
		err := distinctAvgFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
		c.Assert(err, IsNil)
	}
	result = distinctAvgFunc.GetResult(evalCtx)
	needed = types.NewDecFromStringForTest("50.500000000000000000000000000000")
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)
	partialResult := distinctAvgFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetInt64(), Equals, int64(100))
	needed = types.NewDecFromStringForTest("5050")
	c.Assert(partialResult[1].GetMysqlDecimal().Compare(needed) == 0, IsTrue, Commentf("%v, %v ", result.GetMysqlDecimal(), needed))
}

func (s *testAggFuncSuit) TestAvgFinalMode(c *C) {
	rows := make([]types.DatumRow, 0, 100)
	for i := 1; i <= 100; i++ {
		rows = append(rows, types.MakeDatums(i, types.NewDecFromInt(int64(i*i))))
	}
	cntCol := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	sumCol := &expression.Column{
		Index:   1,
		RetType: types.NewFieldType(mysql.TypeDecimal),
	}
	aggFunc := NewAggFuncDesc(s.ctx, ast.AggFuncAvg, []expression.Expression{cntCol, sumCol}, false)
	aggFunc.Mode = FinalMode
	avgFunc := aggFunc.GetAggFunc()
	evalCtx := avgFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	for _, row := range rows {
		err := avgFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
		c.Assert(err, IsNil)
	}
	result := avgFunc.GetResult(evalCtx)
	needed := types.NewDecFromStringForTest("67.000000000000000000000000000000")
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)
}

func (s *testAggFuncSuit) TestSum(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	sumFunc := NewAggFuncDesc(s.ctx, ast.AggFuncSum, []expression.Expression{col}, false).GetAggFunc()
	evalCtx := sumFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := sumFunc.GetResult(evalCtx)
	c.Assert(result.IsNull(), IsTrue)

	for _, row := range s.rows {
		err := sumFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
		c.Assert(err, IsNil)
	}
	result = sumFunc.GetResult(evalCtx)
	needed := types.NewDecFromStringForTest("338350")
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)
	err := sumFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, s.nullRow)
	c.Assert(err, IsNil)
	result = sumFunc.GetResult(evalCtx)
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)
	partialResult := sumFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetMysqlDecimal().Compare(needed) == 0, IsTrue)

	distinctSumFunc := NewAggFuncDesc(s.ctx, ast.AggFuncSum, []expression.Expression{col}, true).GetAggFunc()
	evalCtx = distinctSumFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)
	for _, row := range s.rows {
		err := distinctSumFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
		c.Assert(err, IsNil)
	}
	result = distinctSumFunc.GetResult(evalCtx)
	needed = types.NewDecFromStringForTest("5050")
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)
}

func (s *testAggFuncSuit) TestBitAnd(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	bitAndFunc := NewAggFuncDesc(s.ctx, ast.AggFuncBitAnd, []expression.Expression{col}, false).GetAggFunc()
	evalCtx := bitAndFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(math.MaxUint64))

	row := types.MakeDatums(1)
	err := bitAndFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	err = bitAndFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, s.nullRow)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	row = types.MakeDatums(1)
	err = bitAndFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	row = types.MakeDatums(3)
	err = bitAndFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	row = types.MakeDatums(2)
	err = bitAndFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))
	partialResult := bitAndFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetUint64(), Equals, uint64(0))
}

func (s *testAggFuncSuit) TestBitOr(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	bitOrFunc := NewAggFuncDesc(s.ctx, ast.AggFuncBitOr, []expression.Expression{col}, false).GetAggFunc()
	evalCtx := bitOrFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))

	row := types.MakeDatums(1)
	err := bitOrFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	err = bitOrFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, s.nullRow)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	row = types.MakeDatums(1)
	err = bitOrFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	row = types.MakeDatums(3)
	err = bitOrFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(3))

	row = types.MakeDatums(2)
	err = bitOrFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(3))
	partialResult := bitOrFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetUint64(), Equals, uint64(3))
}

func (s *testAggFuncSuit) TestBitXor(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	bitXorFunc := NewAggFuncDesc(s.ctx, ast.AggFuncBitXor, []expression.Expression{col}, false).GetAggFunc()
	evalCtx := bitXorFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))

	row := types.MakeDatums(1)
	err := bitXorFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	err = bitXorFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, s.nullRow)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	row = types.MakeDatums(1)
	err = bitXorFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))

	row = types.MakeDatums(3)
	err = bitXorFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(3))

	row = types.MakeDatums(2)
	err = bitXorFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))
	partialResult := bitXorFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetUint64(), Equals, uint64(1))
}

func (s *testAggFuncSuit) TestCount(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	countFunc := NewAggFuncDesc(s.ctx, ast.AggFuncCount, []expression.Expression{col}, false).GetAggFunc()
	evalCtx := countFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := countFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(0))

	for _, row := range s.rows {
		err := countFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
		c.Assert(err, IsNil)
	}
	result = countFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(5050))
	err := countFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, s.nullRow)
	c.Assert(err, IsNil)
	result = countFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(5050))
	partialResult := countFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetInt64(), Equals, int64(5050))

	distinctCountFunc := NewAggFuncDesc(s.ctx, ast.AggFuncCount, []expression.Expression{col}, true).GetAggFunc()
	evalCtx = distinctCountFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	for _, row := range s.rows {
		err := distinctCountFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
		c.Assert(err, IsNil)
	}
	result = distinctCountFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(100))
}

func (s *testAggFuncSuit) TestConcat(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	sep := &expression.Column{
		Index:   1,
		RetType: types.NewFieldType(mysql.TypeVarchar),
	}
	concatFunc := NewAggFuncDesc(s.ctx, ast.AggFuncGroupConcat, []expression.Expression{col, sep}, false).GetAggFunc()
	evalCtx := concatFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := concatFunc.GetResult(evalCtx)
	c.Assert(result.IsNull(), IsTrue)

	row := types.MakeDatums(1, "x")
	err := concatFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = concatFunc.GetResult(evalCtx)
	c.Assert(result.GetString(), Equals, "1")

	row[0].SetInt64(2)
	err = concatFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = concatFunc.GetResult(evalCtx)
	c.Assert(result.GetString(), Equals, "1x2")

	row[0].SetNull()
	err = concatFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = concatFunc.GetResult(evalCtx)
	c.Assert(result.GetString(), Equals, "1x2")
	partialResult := concatFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetString(), Equals, "1x2")

	distinctConcatFunc := NewAggFuncDesc(s.ctx, ast.AggFuncGroupConcat, []expression.Expression{col, sep}, true).GetAggFunc()
	evalCtx = distinctConcatFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	row[0].SetInt64(1)
	err = distinctConcatFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = distinctConcatFunc.GetResult(evalCtx)
	c.Assert(result.GetString(), Equals, "1")

	row[0].SetInt64(1)
	err = distinctConcatFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = distinctConcatFunc.GetResult(evalCtx)
	c.Assert(result.GetString(), Equals, "1")
}

func (s *testAggFuncSuit) TestFirstRow(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}

	firstRowFunc := NewAggFuncDesc(s.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false).GetAggFunc()
	evalCtx := firstRowFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	row := types.MakeDatums(1)
	err := firstRowFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result := firstRowFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	row = types.MakeDatums(2)
	err = firstRowFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = firstRowFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))
	partialResult := firstRowFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetUint64(), Equals, uint64(1))
}

func (s *testAggFuncSuit) TestMaxMin(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}

	maxFunc := NewAggFuncDesc(s.ctx, ast.AggFuncMax, []expression.Expression{col}, false).GetAggFunc()
	minFunc := NewAggFuncDesc(s.ctx, ast.AggFuncMin, []expression.Expression{col}, false).GetAggFunc()
	maxEvalCtx := maxFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)
	minEvalCtx := minFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.IsNull(), IsTrue)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.IsNull(), IsTrue)

	row := types.MakeDatums(2)
	err := maxFunc.Update(maxEvalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(2))
	err = minFunc.Update(minEvalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(2))

	row[0].SetInt64(3)
	err = maxFunc.Update(maxEvalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(3))
	err = minFunc.Update(minEvalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(2))

	row[0].SetInt64(1)
	err = maxFunc.Update(maxEvalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(3))
	err = minFunc.Update(minEvalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(1))

	row[0].SetNull()
	err = maxFunc.Update(maxEvalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(3))
	err = minFunc.Update(minEvalCtx, s.ctx.GetSessionVars().StmtCtx, types.DatumRow(row))
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(1))
	partialResult := minFunc.GetPartialResult(minEvalCtx)
	c.Assert(partialResult[0].GetInt64(), Equals, int64(1))
}
