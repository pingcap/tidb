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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/expression"
	"github.com/pingcap/tidb/v4/sessionctx"
	"github.com/pingcap/tidb/v4/sessionctx/variable"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/chunk"
	"github.com/pingcap/tidb/v4/util/mock"
)

var _ = Suite(&testAggFuncSuit{})

type testAggFuncSuit struct {
	ctx     sessionctx.Context
	rows    []chunk.Row
	nullRow chunk.Row
}

func generateRowData() []chunk.Row {
	rows := make([]chunk.Row, 0, 5050)
	for i := 1; i <= 100; i++ {
		for j := 0; j < i; j++ {
			rows = append(rows, chunk.MutRowFromDatums(types.MakeDatums(i)).ToRow())
		}
	}
	return rows
}

func (s *testAggFuncSuit) SetUpSuite(c *C) {
	s.ctx = mock.NewContext()
	s.ctx.GetSessionVars().GlobalVarsAccessor = variable.NewMockGlobalAccessor()
	s.rows = generateRowData()
	s.nullRow = chunk.MutRowFromDatums([]types.Datum{{}}).ToRow()
}

func (s *testAggFuncSuit) TestAvg(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncAvg, []expression.Expression{col}, false)
	c.Assert(err, IsNil)
	avgFunc := desc.GetAggFunc(ctx)
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
	err = avgFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, s.nullRow)
	c.Assert(err, IsNil)
	result = avgFunc.GetResult(evalCtx)
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)

	desc, err = NewAggFuncDesc(s.ctx, ast.AggFuncAvg, []expression.Expression{col}, true)
	c.Assert(err, IsNil)
	distinctAvgFunc := desc.GetAggFunc(ctx)
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
	rows := make([][]types.Datum, 0, 100)
	for i := 1; i <= 100; i++ {
		rows = append(rows, types.MakeDatums(i, types.NewDecFromInt(int64(i*i))))
	}
	ctx := mock.NewContext()
	cntCol := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	sumCol := &expression.Column{
		Index:   1,
		RetType: types.NewFieldType(mysql.TypeNewDecimal),
	}
	aggFunc, err := NewAggFuncDesc(s.ctx, ast.AggFuncAvg, []expression.Expression{cntCol, sumCol}, false)
	c.Assert(err, IsNil)
	aggFunc.Mode = FinalMode
	avgFunc := aggFunc.GetAggFunc(ctx)
	evalCtx := avgFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	for _, row := range rows {
		err := avgFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, chunk.MutRowFromDatums(row).ToRow())
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
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncSum, []expression.Expression{col}, false)
	c.Assert(err, IsNil)
	sumFunc := desc.GetAggFunc(ctx)
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
	err = sumFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, s.nullRow)
	c.Assert(err, IsNil)
	result = sumFunc.GetResult(evalCtx)
	c.Assert(result.GetMysqlDecimal().Compare(needed) == 0, IsTrue)
	partialResult := sumFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetMysqlDecimal().Compare(needed) == 0, IsTrue)

	desc, err = NewAggFuncDesc(s.ctx, ast.AggFuncSum, []expression.Expression{col}, true)
	c.Assert(err, IsNil)
	distinctSumFunc := desc.GetAggFunc(ctx)
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
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncBitAnd, []expression.Expression{col}, false)
	c.Assert(err, IsNil)
	bitAndFunc := desc.GetAggFunc(ctx)
	evalCtx := bitAndFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(math.MaxUint64))

	row := chunk.MutRowFromDatums(types.MakeDatums(1)).ToRow()
	err = bitAndFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	err = bitAndFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, s.nullRow)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	row = chunk.MutRowFromDatums(types.MakeDatums(1)).ToRow()
	err = bitAndFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	row = chunk.MutRowFromDatums(types.MakeDatums(3)).ToRow()
	err = bitAndFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	row = chunk.MutRowFromDatums(types.MakeDatums(2)).ToRow()
	err = bitAndFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))
	partialResult := bitAndFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetUint64(), Equals, uint64(0))

	// test bit_and( decimal )
	col.RetType = types.NewFieldType(mysql.TypeNewDecimal)
	bitAndFunc.ResetContext(s.ctx.GetSessionVars().StmtCtx, evalCtx)

	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(math.MaxUint64))

	var dec types.MyDecimal
	err = dec.FromString([]byte("1.234"))
	c.Assert(err, IsNil)
	row = chunk.MutRowFromDatums(types.MakeDatums(&dec)).ToRow()
	err = bitAndFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	err = dec.FromString([]byte("3.012"))
	c.Assert(err, IsNil)
	row = chunk.MutRowFromDatums(types.MakeDatums(&dec)).ToRow()
	err = bitAndFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	err = dec.FromString([]byte("2.12345678"))
	c.Assert(err, IsNil)
	row = chunk.MutRowFromDatums(types.MakeDatums(&dec)).ToRow()
	err = bitAndFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitAndFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))
}

func (s *testAggFuncSuit) TestBitOr(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncBitOr, []expression.Expression{col}, false)
	c.Assert(err, IsNil)
	bitOrFunc := desc.GetAggFunc(ctx)
	evalCtx := bitOrFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))

	row := chunk.MutRowFromDatums(types.MakeDatums(1)).ToRow()
	err = bitOrFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	err = bitOrFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, s.nullRow)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	row = chunk.MutRowFromDatums(types.MakeDatums(1)).ToRow()
	err = bitOrFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	row = chunk.MutRowFromDatums(types.MakeDatums(3)).ToRow()
	err = bitOrFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(3))

	row = chunk.MutRowFromDatums(types.MakeDatums(2)).ToRow()
	err = bitOrFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(3))
	partialResult := bitOrFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetUint64(), Equals, uint64(3))

	// test bit_or( decimal )
	col.RetType = types.NewFieldType(mysql.TypeNewDecimal)
	bitOrFunc.ResetContext(s.ctx.GetSessionVars().StmtCtx, evalCtx)

	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))

	var dec types.MyDecimal
	err = dec.FromString([]byte("12.234"))
	c.Assert(err, IsNil)
	row = chunk.MutRowFromDatums(types.MakeDatums(&dec)).ToRow()
	err = bitOrFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(12))

	err = dec.FromString([]byte("1.012"))
	c.Assert(err, IsNil)
	row = chunk.MutRowFromDatums(types.MakeDatums(&dec)).ToRow()
	err = bitOrFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(13))
	err = dec.FromString([]byte("15.12345678"))
	c.Assert(err, IsNil)

	row = chunk.MutRowFromDatums(types.MakeDatums(&dec)).ToRow()
	err = bitOrFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(15))

	err = dec.FromString([]byte("16.00"))
	c.Assert(err, IsNil)
	row = chunk.MutRowFromDatums(types.MakeDatums(&dec)).ToRow()
	err = bitOrFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitOrFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(31))
}

func (s *testAggFuncSuit) TestBitXor(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncBitXor, []expression.Expression{col}, false)
	c.Assert(err, IsNil)
	bitXorFunc := desc.GetAggFunc(ctx)
	evalCtx := bitXorFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))

	row := chunk.MutRowFromDatums(types.MakeDatums(1)).ToRow()
	err = bitXorFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	err = bitXorFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, s.nullRow)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	row = chunk.MutRowFromDatums(types.MakeDatums(1)).ToRow()
	err = bitXorFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))

	row = chunk.MutRowFromDatums(types.MakeDatums(3)).ToRow()
	err = bitXorFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(3))

	row = chunk.MutRowFromDatums(types.MakeDatums(2)).ToRow()
	err = bitXorFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))
	partialResult := bitXorFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetUint64(), Equals, uint64(1))

	// test bit_xor( decimal )
	col.RetType = types.NewFieldType(mysql.TypeNewDecimal)
	bitXorFunc.ResetContext(s.ctx.GetSessionVars().StmtCtx, evalCtx)

	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))

	var dec types.MyDecimal
	err = dec.FromString([]byte("1.234"))
	c.Assert(err, IsNil)
	row = chunk.MutRowFromDatums(types.MakeDatums(&dec)).ToRow()
	err = bitXorFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	err = dec.FromString([]byte("1.012"))
	c.Assert(err, IsNil)
	row = chunk.MutRowFromDatums(types.MakeDatums(&dec)).ToRow()
	err = bitXorFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(0))

	err = dec.FromString([]byte("2.12345678"))
	c.Assert(err, IsNil)
	row = chunk.MutRowFromDatums(types.MakeDatums(&dec)).ToRow()
	err = bitXorFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result = bitXorFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(2))
}

func (s *testAggFuncSuit) TestCount(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncCount, []expression.Expression{col}, false)
	c.Assert(err, IsNil)
	countFunc := desc.GetAggFunc(ctx)
	evalCtx := countFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := countFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(0))

	for _, row := range s.rows {
		err := countFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
		c.Assert(err, IsNil)
	}
	result = countFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(5050))
	err = countFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, s.nullRow)
	c.Assert(err, IsNil)
	result = countFunc.GetResult(evalCtx)
	c.Assert(result.GetInt64(), Equals, int64(5050))
	partialResult := countFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetInt64(), Equals, int64(5050))

	desc, err = NewAggFuncDesc(s.ctx, ast.AggFuncCount, []expression.Expression{col}, true)
	c.Assert(err, IsNil)
	distinctCountFunc := desc.GetAggFunc(ctx)
	evalCtx = distinctCountFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	for _, row := range s.rows {
		err := distinctCountFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
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
	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncGroupConcat, []expression.Expression{col, sep}, false)
	c.Assert(err, IsNil)
	concatFunc := desc.GetAggFunc(ctx)
	evalCtx := concatFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := concatFunc.GetResult(evalCtx)
	c.Assert(result.IsNull(), IsTrue)

	row := chunk.MutRowFromDatums(types.MakeDatums(1, "x"))
	err = concatFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = concatFunc.GetResult(evalCtx)
	c.Assert(result.GetString(), Equals, "1")

	row.SetDatum(0, types.NewIntDatum(2))
	err = concatFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = concatFunc.GetResult(evalCtx)
	c.Assert(result.GetString(), Equals, "1x2")

	row.SetDatum(0, types.NewDatum(nil))
	err = concatFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = concatFunc.GetResult(evalCtx)
	c.Assert(result.GetString(), Equals, "1x2")
	partialResult := concatFunc.GetPartialResult(evalCtx)
	c.Assert(partialResult[0].GetString(), Equals, "1x2")

	desc, err = NewAggFuncDesc(s.ctx, ast.AggFuncGroupConcat, []expression.Expression{col, sep}, true)
	c.Assert(err, IsNil)
	distinctConcatFunc := desc.GetAggFunc(ctx)
	evalCtx = distinctConcatFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	row.SetDatum(0, types.NewIntDatum(1))
	err = distinctConcatFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = distinctConcatFunc.GetResult(evalCtx)
	c.Assert(result.GetString(), Equals, "1")

	row.SetDatum(0, types.NewIntDatum(1))
	err = distinctConcatFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = distinctConcatFunc.GetResult(evalCtx)
	c.Assert(result.GetString(), Equals, "1")
}

func (s *testAggFuncSuit) TestFirstRow(c *C) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}

	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncFirstRow, []expression.Expression{col}, false)
	c.Assert(err, IsNil)
	firstRowFunc := desc.GetAggFunc(ctx)
	evalCtx := firstRowFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	row := chunk.MutRowFromDatums(types.MakeDatums(1)).ToRow()
	err = firstRowFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
	c.Assert(err, IsNil)
	result := firstRowFunc.GetResult(evalCtx)
	c.Assert(result.GetUint64(), Equals, uint64(1))

	row = chunk.MutRowFromDatums(types.MakeDatums(2)).ToRow()
	err = firstRowFunc.Update(evalCtx, s.ctx.GetSessionVars().StmtCtx, row)
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

	ctx := mock.NewContext()
	desc, err := NewAggFuncDesc(s.ctx, ast.AggFuncMax, []expression.Expression{col}, false)
	c.Assert(err, IsNil)
	maxFunc := desc.GetAggFunc(ctx)
	desc, err = NewAggFuncDesc(s.ctx, ast.AggFuncMin, []expression.Expression{col}, false)
	c.Assert(err, IsNil)
	minFunc := desc.GetAggFunc(ctx)
	maxEvalCtx := maxFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)
	minEvalCtx := minFunc.CreateContext(s.ctx.GetSessionVars().StmtCtx)

	result := maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.IsNull(), IsTrue)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.IsNull(), IsTrue)

	row := chunk.MutRowFromDatums(types.MakeDatums(2))
	err = maxFunc.Update(maxEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(2))
	err = minFunc.Update(minEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(2))

	row.SetDatum(0, types.NewIntDatum(3))
	err = maxFunc.Update(maxEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(3))
	err = minFunc.Update(minEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(2))

	row.SetDatum(0, types.NewIntDatum(1))
	err = maxFunc.Update(maxEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(3))
	err = minFunc.Update(minEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(1))

	row.SetDatum(0, types.NewDatum(nil))
	err = maxFunc.Update(maxEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = maxFunc.GetResult(maxEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(3))
	err = minFunc.Update(minEvalCtx, s.ctx.GetSessionVars().StmtCtx, row.ToRow())
	c.Assert(err, IsNil)
	result = minFunc.GetResult(minEvalCtx)
	c.Assert(result.GetInt64(), Equals, int64(1))
	partialResult := minFunc.GetPartialResult(minEvalCtx)
	c.Assert(partialResult[0].GetInt64(), Equals, int64(1))
}
