// Copyright 2021 PingCAP, Inc.
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
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

func (p *multiArgsAggTest) genSrcChk2() *chunk.Chunk {
	srcChk := chunk.NewChunkWithCapacity(p.dataTypes, p.numRows)
	for i := 0; i < p.numRows; i++ {
		for j := 0; j < len(p.dataGens); j++ {
			fdt := p.dataGens[j](i)
			srcChk.AppendDatum(j, &fdt)
		}
	}
	return srcChk
}

func (s *testSuite) testSingleArgMergePartialResult(c *C, p multiArgsAggTest) {
	srcChk := p.genSrcChk2()
	iter := chunk.NewIterator4Chunk(srcChk)

	args := make([]expression.Expression, len(p.dataTypes))
	for k := 0; k < len(p.dataTypes); k++ {
		args[k] = &expression.Column{RetType: p.dataTypes[k], Index: k}
	}

	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, false)
	c.Assert(err, IsNil)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	partialDesc, finalDesc := desc.Split([]int{0, 1})

	// build partial func for partial phase.
	partialFunc := aggfuncs.Build(s.ctx, partialDesc, 0)
	partialResult, _ := partialFunc.AllocPartialResult()

	// build final func for final phase.
	finalFunc := aggfuncs.Build(s.ctx, finalDesc, 0)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{p.retType}, 1)

	// update partial result.
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		// FIXME: cannot assert error since there are cases of error, e.g. JSON documents may not contain NULL member
		_, _ = partialFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialResult)
	}
	p.messUpChunk(srcChk)
	err = partialFunc.AppendFinalResult2Chunk(s.ctx, partialResult, resultChk)
	c.Assert(err, IsNil)
	dt := resultChk.GetRow(0).GetDatum(0, p.retType)
	result, err := dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[0])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("iter len=%v", iter.Len()))

	_, err = finalFunc.MergePartialResult(s.ctx, partialResult, finalPr)
	c.Assert(err, IsNil)
	partialFunc.ResetPartialResult(partialResult)

	srcChk = p.genSrcChk2()
	iter = chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		// FIXME: cannot check error
		_, _ = partialFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialResult)
	}
	p.messUpChunk(srcChk)
	resultChk.Reset()
	err = partialFunc.AppendFinalResult2Chunk(s.ctx, partialResult, resultChk)
	c.Assert(err, IsNil)
	dt = resultChk.GetRow(0).GetDatum(0, p.retType)
	result, err = dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[1])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("dt %v <=> result %v", dt, p.results[1]))
	_, err = finalFunc.MergePartialResult(s.ctx, partialResult, finalPr)
	c.Assert(err, IsNil)

	resultChk.Reset()
	err = finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)

	dt = resultChk.GetRow(0).GetDatum(0, p.retType)
	result, err = dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[2])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0)
}

func (s *testSuite) testSingleArgsAggFunc(c *C, p multiArgsAggTest) {
	srcChk := p.genSrcChk2()

	args := make([]expression.Expression, len(p.dataTypes))
	for k := 0; k < len(p.dataTypes); k++ {
		args[k] = &expression.Column{RetType: p.dataTypes[k], Index: k}
	}
	if p.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(separator), RetType: types.NewFieldType(mysql.TypeString)})
	}

	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, false)
	c.Assert(err, IsNil)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(s.ctx, desc, 0)
	finalPr, _ := finalFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{desc.RetTp}, 1)

	iter := chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		// FIXME: cannot assert error since there are cases of error, e.g. rows were cut by GROUPCONCAT
		_, _ = finalFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, finalPr)
	}
	p.messUpChunk(srcChk)
	err = finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)
	dt := resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err := dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[1])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[1]))

	// test the empty input
	resultChk.Reset()
	finalFunc.ResetPartialResult(finalPr)
	err = finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)
	dt = resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err = dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[0])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[0]))

	// test the agg func with distinct
	desc, err = aggregation.NewAggFuncDesc(s.ctx, p.funcName, args, true)
	c.Assert(err, IsNil)
	if p.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc = aggfuncs.Build(s.ctx, desc, 0)
	finalPr, _ = finalFunc.AllocPartialResult()

	resultChk.Reset()
	srcChk = p.genSrcChk2()
	iter = chunk.NewIterator4Chunk(srcChk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		// FIXME: cannot check error
		_, _ = finalFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, finalPr)
	}
	p.messUpChunk(srcChk)
	err = finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)
	dt = resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err = dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[1])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0, Commentf("%v != %v", dt.String(), p.results[1]))

	// test the empty input
	resultChk.Reset()
	finalFunc.ResetPartialResult(finalPr)
	err = finalFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)
	dt = resultChk.GetRow(0).GetDatum(0, desc.RetTp)
	result, err = dt.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &p.results[0])
	c.Assert(err, IsNil)
	c.Assert(result, Equals, 0)
}

func (s *testSuite) TestMergePartialResult4JsonArrayagg(c *C) {
	typeList := []byte{mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeString, mysql.TypeJSON}
	var argCombines [][]byte
	for i := 0; i < len(typeList); i++ {
		argTypes := []byte{typeList[i]}
		argCombines = append(argCombines, argTypes)
	}

	var tests []multiArgsAggTest
	numRows := 5

	for k := 0; k < len(argCombines); k++ {
		entries1 := make([]interface{}, 0)
		entries2 := make([]interface{}, 0)
		ret := make([]interface{}, 0)

		argTypes := argCombines[k]
		vGenFunc := getDataGenFunc(types.NewFieldType(argTypes[0]))

		for m := 0; m < numRows; m++ {
			firstArg := vGenFunc(m)
			entries1 = append(entries1, firstArg.GetValue())
			ret = append(ret, firstArg.GetValue())
		}

		for m := 0; m < numRows; m++ {
			firstArg := vGenFunc(m)
			entries2 = append(entries2, firstArg.GetValue())
			ret = append(ret, firstArg.GetValue())
		}

		aggTest := buildMultiArgsAggTester(ast.AggFuncJsonArrayagg, argTypes, mysql.TypeJSON, numRows, json.CreateBinary(entries1), json.CreateBinary(entries2), json.CreateBinary(ret))

		tests = append(tests, aggTest)
	}

	for _, test := range tests {
		s.testSingleArgMergePartialResult(c, test)
	}
}

func (s *testSuite) TestJsonArrayagg(c *C) {
	typeList := []byte{mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeString, mysql.TypeJSON}
	var argCombines [][]byte
	for i := 0; i < len(typeList); i++ {
		argTypes := []byte{typeList[i]}
		argCombines = append(argCombines, argTypes)
	}

	var tests []multiArgsAggTest
	numRows := 5

	for k := 0; k < len(argCombines); k++ {
		entries := make([]interface{}, 0)

		argTypes := argCombines[k]
		vGenFunc := getDataGenFunc(types.NewFieldType(argTypes[0]))

		for m := 0; m < numRows; m++ {
			firstArg := vGenFunc(m)
			entries = append(entries, firstArg.GetValue())
		}

		aggTest := buildMultiArgsAggTester(ast.AggFuncJsonArrayagg, argTypes, mysql.TypeJSON, numRows, nil, json.CreateBinary(entries))

		tests = append(tests, aggTest)
	}

	for _, test := range tests {
		s.testSingleArgsAggFunc(c, test)
	}
}

func defaultSingleArgsMemDeltaGens(srcChk *chunk.Chunk, dataTypes []*types.FieldType, byItems []*util.ByItems) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)
	for i := 0; i < srcChk.NumRows(); i++ {
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		memDelta := int64(0)
		memDelta += aggfuncs.DefInterfaceSize
		switch dataTypes[0].Tp {
		case mysql.TypeLonglong:
			memDelta += aggfuncs.DefUint64Size
		case mysql.TypeDouble:
			memDelta += aggfuncs.DefFloat64Size
		case mysql.TypeString:
			val := row.GetString(0)
			memDelta += int64(len(val))
		case mysql.TypeJSON:
			val := row.GetJSON(0)
			// +1 for the memory usage of the TypeCode of json
			memDelta += int64(len(val.Value) + 1)
		case mysql.TypeDuration:
			memDelta += aggfuncs.DefDurationSize
		case mysql.TypeDate:
			memDelta += aggfuncs.DefTimeSize
		case mysql.TypeNewDecimal:
			memDelta += aggfuncs.DefMyDecimalSize
		default:
			return memDeltas, errors.Errorf("unsupported type - %v", dataTypes[0].Tp)
		}
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}

func (s *testSuite) testSingleArgsAggMemFunc(c *C, p multiArgsAggMemTest) {
	srcChk := p.multiArgsAggTest.genSrcChk2()

	args := make([]expression.Expression, len(p.multiArgsAggTest.dataTypes))
	for k := 0; k < len(p.multiArgsAggTest.dataTypes); k++ {
		args[k] = &expression.Column{RetType: p.multiArgsAggTest.dataTypes[k], Index: k}
	}
	if p.multiArgsAggTest.funcName == ast.AggFuncGroupConcat {
		args = append(args, &expression.Constant{Value: types.NewStringDatum(separator), RetType: types.NewFieldType(mysql.TypeString)})
	}

	desc, err := aggregation.NewAggFuncDesc(s.ctx, p.multiArgsAggTest.funcName, args, p.isDistinct)
	c.Assert(err, IsNil)
	if p.multiArgsAggTest.orderBy {
		desc.OrderByItems = []*util.ByItems{
			{Expr: args[0], Desc: true},
		}
	}
	finalFunc := aggfuncs.Build(s.ctx, desc, 0)
	finalPr, memDelta := finalFunc.AllocPartialResult()
	c.Assert(memDelta, Equals, p.allocMemDelta)

	updateMemDeltas, err := p.multiArgsUpdateMemDeltaGens(srcChk, p.multiArgsAggTest.dataTypes, desc.OrderByItems)
	c.Assert(err, IsNil)
	iter := chunk.NewIterator4Chunk(srcChk)
	i := 0
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		memDelta, _ := finalFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, finalPr)
		c.Assert(memDelta, Equals, updateMemDeltas[i], Commentf("i = %d", i))
		i++
	}
}

func (s *testSuite) TestMemJsonArrayagg(c *C) {
	typeList := []byte{mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeString, mysql.TypeJSON, mysql.TypeDuration, mysql.TypeNewDecimal, mysql.TypeDate}
	var argCombines [][]byte
	for i := 0; i < len(typeList); i++ {
		argTypes := []byte{typeList[i]}
		argCombines = append(argCombines, argTypes)
	}
	numRows := 5
	for k := 0; k < len(argCombines); k++ {
		entries := make([]interface{}, 0)

		argTypes := argCombines[k]
		vGenFunc := getDataGenFunc(types.NewFieldType(argTypes[0]))

		for m := 0; m < numRows; m++ {
			firstArg := vGenFunc(m)
			entries = append(entries, firstArg.GetValue())
		}

		// appendBinary does not support some type such as uint8、types.time，so convert is needed here
		for i, val := range entries {
			switch x := val.(type) {
			case *types.MyDecimal:
				float64Val, _ := x.ToFloat64()
				entries[i] = float64Val
			case []uint8, types.Time, types.Duration:
				strVal, _ := types.ToString(x)
				entries[i] = strVal
			}
		}

		tests := []multiArgsAggMemTest{
			buildMultiArgsAggMemTester(ast.AggFuncJsonArrayagg, argTypes, mysql.TypeJSON, numRows, aggfuncs.DefPartialResult4JsonArrayAgg, defaultSingleArgsMemDeltaGens, true),
			buildMultiArgsAggMemTester(ast.AggFuncJsonArrayagg, argTypes, mysql.TypeJSON, numRows, aggfuncs.DefPartialResult4JsonArrayAgg, defaultSingleArgsMemDeltaGens, false),
		}
		for _, test := range tests {
			s.testSingleArgsAggMemFunc(c, test)
		}
	}
}
