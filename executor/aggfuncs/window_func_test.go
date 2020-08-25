// Copyright 2019 PingCAP, Inc.
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

package aggfuncs_test

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

type windowTest struct {
	dataType    *types.FieldType
	numRows     int
	funcName    string
	args        []expression.Expression
	orderByCols []*expression.Column
	results     []types.Datum
}

func (s *testSuite) testWindowFunc(c *C, p windowTest) {
	srcChk := chunk.NewChunkWithCapacity([]*types.FieldType{p.dataType}, p.numRows)
	dataGen := getDataGenFunc(p.dataType)
	for i := 0; i < p.numRows; i++ {
		dt := dataGen(i)
		srcChk.AppendDatum(0, &dt)
	}

	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, p.args, false)
	c.Assert(err, IsNil)
	finalFunc := aggfuncs.BuildWindowFunctions(s.ctx, desc, 0, p.orderByCols)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)

	iter := chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		_, err = finalFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, finalPr)
		c.Assert(err, IsNil)
	}

	c.Assert(p.numRows, Equals, len(p.results))
	for i := 0; i < p.numRows; i++ {
		err = finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
		c.Assert(err, IsNil)
		dt := resultChk.GetRow(0).GetDatum(0, desc.RetTp)
		result, err := dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[i])
		c.Assert(err, IsNil)
		c.Assert(result, Equals, 0)
		resultChk.Reset()
	}
	finalFunc.ResetPartialResult(finalPr)
}

func buildWindowTesterWithArgs(funcName string, tp byte, args []expression.Expression, orderByCols int, numRows int, results ...interface{}) windowTest {
	pt := windowTest{
		dataType: types.NewFieldType(tp),
		numRows:  numRows,
		funcName: funcName,
	}
	if funcName != ast.WindowFuncNtile {
		pt.args = append(pt.args, &expression.Column{RetType: pt.dataType, Index: 0})
	}
	pt.args = append(pt.args, args...)
	if orderByCols > 0 {
		pt.orderByCols = append(pt.orderByCols, &expression.Column{RetType: pt.dataType, Index: 0})
	}

	for _, result := range results {
		pt.results = append(pt.results, types.NewDatum(result))
	}
	return pt
}

func buildWindowTester(funcName string, tp byte, constantArg uint64, orderByCols int, numRows int, results ...interface{}) windowTest {
	pt := windowTest{
		dataType: types.NewFieldType(tp),
		numRows:  numRows,
		funcName: funcName,
	}
	if funcName != ast.WindowFuncNtile {
		pt.args = append(pt.args, &expression.Column{RetType: pt.dataType, Index: 0})
	}
	if constantArg > 0 {
		pt.args = append(pt.args, &expression.Constant{Value: types.NewUintDatum(constantArg)})
	}
	if orderByCols > 0 {
		pt.orderByCols = append(pt.orderByCols, &expression.Column{RetType: pt.dataType, Index: 0})
	}

	for _, result := range results {
		pt.results = append(pt.results, types.NewDatum(result))
	}
	return pt
}

func (s *testSuite) TestWindowFunctions(c *C) {
	tests := []windowTest{
		buildWindowTester(ast.WindowFuncCumeDist, mysql.TypeLonglong, 0, 1, 1, 1),
		buildWindowTester(ast.WindowFuncCumeDist, mysql.TypeLonglong, 0, 0, 2, 1, 1),
		buildWindowTester(ast.WindowFuncCumeDist, mysql.TypeLonglong, 0, 1, 4, 0.25, 0.5, 0.75, 1),

		buildWindowTester(ast.WindowFuncDenseRank, mysql.TypeLonglong, 0, 0, 2, 1, 1),
		buildWindowTester(ast.WindowFuncDenseRank, mysql.TypeLonglong, 0, 1, 4, 1, 2, 3, 4),

		buildWindowTester(ast.WindowFuncFirstValue, mysql.TypeLonglong, 0, 1, 2, 0, 0),
		buildWindowTester(ast.WindowFuncFirstValue, mysql.TypeFloat, 0, 1, 2, 0, 0),
		buildWindowTester(ast.WindowFuncFirstValue, mysql.TypeDouble, 0, 1, 2, 0, 0),
		buildWindowTester(ast.WindowFuncFirstValue, mysql.TypeNewDecimal, 0, 1, 2, types.NewDecFromInt(0), types.NewDecFromInt(0)),
		buildWindowTester(ast.WindowFuncFirstValue, mysql.TypeString, 0, 1, 2, "0", "0"),
		buildWindowTester(ast.WindowFuncFirstValue, mysql.TypeDate, 0, 1, 2, types.TimeFromDays(365), types.TimeFromDays(365)),
		buildWindowTester(ast.WindowFuncFirstValue, mysql.TypeDuration, 0, 1, 2, types.Duration{Duration: time.Duration(0)}, types.Duration{Duration: time.Duration(0)}),
		buildWindowTester(ast.WindowFuncFirstValue, mysql.TypeJSON, 0, 1, 2, json.CreateBinary(int64(0)), json.CreateBinary(int64(0))),

		buildWindowTester(ast.WindowFuncLastValue, mysql.TypeLonglong, 1, 0, 2, 1, 1),

		buildWindowTester(ast.WindowFuncNthValue, mysql.TypeLonglong, 2, 0, 3, 1, 1, 1),
		buildWindowTester(ast.WindowFuncNthValue, mysql.TypeLonglong, 5, 0, 3, nil, nil, nil),

		buildWindowTester(ast.WindowFuncNtile, mysql.TypeLonglong, 3, 0, 4, 1, 1, 2, 3),
		buildWindowTester(ast.WindowFuncNtile, mysql.TypeLonglong, 5, 0, 3, 1, 2, 3),

		buildWindowTester(ast.WindowFuncPercentRank, mysql.TypeLonglong, 0, 1, 1, 0),
		buildWindowTester(ast.WindowFuncPercentRank, mysql.TypeLonglong, 0, 0, 3, 0, 0, 0),
		buildWindowTester(ast.WindowFuncPercentRank, mysql.TypeLonglong, 0, 1, 4, 0, 0.3333333333333333, 0.6666666666666666, 1),

		buildWindowTester(ast.WindowFuncRank, mysql.TypeLonglong, 0, 1, 1, 1),
		buildWindowTester(ast.WindowFuncRank, mysql.TypeLonglong, 0, 0, 3, 1, 1, 1),
		buildWindowTester(ast.WindowFuncRank, mysql.TypeLonglong, 0, 1, 4, 1, 2, 3, 4),

		buildWindowTester(ast.WindowFuncRowNumber, mysql.TypeLonglong, 0, 0, 4, 1, 2, 3, 4),
	}
	for _, test := range tests {
		s.testWindowFunc(c, test)
	}
}
