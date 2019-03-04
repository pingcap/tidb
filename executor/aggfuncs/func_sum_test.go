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

package aggfuncs_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (s *testSuite) TestMergePartialResult4SumDecimal(c *C) {
	srcChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeNewDecimal)}, 5)
	for i := int64(0); i < 5; i++ {
		srcChk.AppendMyDecimal(0, types.NewDecFromInt(i))
	}
	iter := chunk.NewIterator4Chunk(srcChk)

	args := []expression.Expression{&expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0}}
	desc := aggregation.NewAggFuncDesc(s.ctx, ast.AggFuncSum, args, false)
	partialDesc, finalDesc := desc.Split([]int{0})

	// build sum func for partial phase.
	partialSumFunc := aggfuncs.Build(s.ctx, partialDesc, 0)
	partialPr1 := partialSumFunc.AllocPartialResult()
	partialPr2 := partialSumFunc.AllocPartialResult()

	// build final func for final phase.
	finalAvgFunc := aggfuncs.Build(s.ctx, finalDesc, 0)
	finalPr := finalAvgFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeNewDecimal)}, 1)

	// update partial result.
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		partialSumFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialPr1)
	}
	// 0+1+2+3+4
	partialSumFunc.AppendFinalResult2Chunk(s.ctx, partialPr1, resultChk)
	c.Assert(resultChk.GetRow(0).GetMyDecimal(0).Compare(types.NewDecFromInt(10)) == 0, IsTrue)

	iter.Begin()
	iter.Next()
	for row := iter.Next(); row != iter.End(); row = iter.Next() {
		partialSumFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialPr2)
	}
	resultChk.Reset()
	// 2+3+4
	partialSumFunc.AppendFinalResult2Chunk(s.ctx, partialPr2, resultChk)
	c.Assert(resultChk.GetRow(0).GetMyDecimal(0).Compare(types.NewDecFromInt(9)) == 0, IsTrue)

	// merge two partial results.
	err := finalAvgFunc.MergePartialResult(s.ctx, partialPr1, finalPr)
	c.Assert(err, IsNil)
	err = finalAvgFunc.MergePartialResult(s.ctx, partialPr2, finalPr)
	c.Assert(err, IsNil)

	resultChk.Reset()
	err = finalAvgFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)
	// 10+9
	c.Assert(resultChk.GetRow(0).GetMyDecimal(0).Compare(types.NewDecFromInt(19)) == 0, IsTrue)
}

func (s *testSuite) TestMergePartialResult4SumFloat(c *C) {
	srcChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDouble)}, 5)
	for i := int64(0); i < 5; i++ {
		srcChk.AppendFloat64(0, float64(i))
	}
	iter := chunk.NewIterator4Chunk(srcChk)

	args := []expression.Expression{&expression.Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0}}
	desc := aggregation.NewAggFuncDesc(s.ctx, ast.AggFuncSum, args, false)
	partialDesc, finalDesc := desc.Split([]int{0})

	// build sum func for partial phase.
	partialSumFunc := aggfuncs.Build(s.ctx, partialDesc, 0)
	partialPr1 := partialSumFunc.AllocPartialResult()
	partialPr2 := partialSumFunc.AllocPartialResult()

	// build final func for final phase.
	finalAvgFunc := aggfuncs.Build(s.ctx, finalDesc, 0)
	finalPr := finalAvgFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDouble)}, 1)

	// update partial result.
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		partialSumFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialPr1)
	}
	partialSumFunc.AppendFinalResult2Chunk(s.ctx, partialPr1, resultChk)
	// (0+1+2+3+4)
	c.Assert(resultChk.GetRow(0).GetFloat64(0) == float64(10), IsTrue)

	iter.Begin()
	iter.Next()
	for row := iter.Next(); row != iter.End(); row = iter.Next() {
		partialSumFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialPr2)
	}
	resultChk.Reset()
	partialSumFunc.AppendFinalResult2Chunk(s.ctx, partialPr2, resultChk)
	// (2+3+4)
	c.Assert(resultChk.GetRow(0).GetFloat64(0) == float64(9), IsTrue)

	// merge two partial results.
	err := finalAvgFunc.MergePartialResult(s.ctx, partialPr1, finalPr)
	c.Assert(err, IsNil)
	err = finalAvgFunc.MergePartialResult(s.ctx, partialPr2, finalPr)
	c.Assert(err, IsNil)

	resultChk.Reset()
	err = finalAvgFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)
	// (10 + 9)
	c.Assert(resultChk.GetRow(0).GetFloat64(0) == float64(19), IsTrue)
}
