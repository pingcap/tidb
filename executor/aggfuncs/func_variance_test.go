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

func (s *testSuite) TestMergePartialResult4VarianceFloat(c *C) {
	srcChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDouble)}, 5)
	for i := int64(0); i < 5; i++ {
		srcChk.AppendFloat64(0, float64(i))
	}
	iter := chunk.NewIterator4Chunk(srcChk)

	desc := &aggregation.AggFuncDesc{
		Name:  ast.AggFuncVarPop,
		Mode:  aggregation.CompleteMode,
		Args:  []expression.Expression{&expression.Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0}},
		RetTp: types.NewFieldType(mysql.TypeDouble),
	}
	partialDesc, finalDesc := desc.Split([]int{0, 1, 2})

	// build variance func for partial phase.
	partialVarianceFunc := aggfuncs.Build(s.ctx, partialDesc, 0)
	partialPr1 := partialVarianceFunc.AllocPartialResult()
	partialPr2 := partialVarianceFunc.AllocPartialResult()

	// build final func for final phase.
	finalVarianceFunc := aggfuncs.Build(s.ctx, finalDesc, 0)
	finalPr := finalVarianceFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDouble)}, 1)

	// update partial result.
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		partialVarianceFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialPr1)
	}
	partialVarianceFunc.AppendFinalResult2Chunk(s.ctx, partialPr1, resultChk)
	// avg: (0+1+2+3+4)/5 => 2
	// variance: (2^2 + 1^2 + 0^2 + 1^2 + 2^2)/5 => 2
	c.Assert(resultChk.GetRow(0).GetFloat64(0) == float64(2), IsTrue)

	row := iter.Begin()
	row = iter.Next()
	for row = iter.Next(); row != iter.End(); row = iter.Next() {
		partialVarianceFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialPr2)
	}
	resultChk.Reset()
	partialVarianceFunc.AppendFinalResult2Chunk(s.ctx, partialPr2, resultChk)
	// avg: (2+3+4)/3 => 3
	// variance: (1^2 + 0^2 + 1^2)/3 => 2/3
	c.Assert(resultChk.GetRow(0).GetFloat64(0) == float64(2/3.0), IsTrue)

	// merge two partial results.
	err := finalVarianceFunc.MergePartialResult(s.ctx, partialPr1, finalPr)
	c.Assert(err, IsNil)
	err = finalVarianceFunc.MergePartialResult(s.ctx, partialPr2, finalPr)
	c.Assert(err, IsNil)

	resultChk.Reset()
	err = finalVarianceFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)
	c.Assert(resultChk.GetRow(0).GetFloat64(0) == float64(1.734375), IsTrue)
}
