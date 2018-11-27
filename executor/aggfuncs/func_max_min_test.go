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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (s *testSuite) TestMergePartialResult4MaxDecimal(c *C) {
	srcChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeNewDecimal)}, 5)
	for i := int64(0); i < 5; i++ {
		srcChk.AppendMyDecimal(0, types.NewDecFromInt(i))
	}
	iter := chunk.NewIterator4Chunk(srcChk)

	desc := &aggregation.AggFuncDesc{
		Name:  ast.AggFuncMax,
		Mode:  aggregation.CompleteMode,
		Args:  []expression.Expression{&expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0}},
		RetTp: types.NewFieldType(mysql.TypeNewDecimal),
	}
	partialDesc, finalDesc := desc.Split([]int{0})

	// build max func for partial phase.
	partialMaxFunc := aggfuncs.Build(s.ctx, partialDesc, 0)
	partialPr1 := partialMaxFunc.AllocPartialResult()
	partialPr2 := partialMaxFunc.AllocPartialResult()

	// build final func for final phase.
	finalMaxFunc := aggfuncs.Build(s.ctx, finalDesc, 0)
	finalPr := finalMaxFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeNewDecimal)}, 1)

	// update partial result.
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		partialMaxFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialPr1)
	}
	partialMaxFunc.AppendFinalResult2Chunk(s.ctx, partialPr1, resultChk)
	c.Assert(resultChk.GetRow(0).GetMyDecimal(0).Compare(types.NewDecFromInt(4)) == 0, IsTrue)

	row := iter.Begin()
	row = iter.Next()
	for row = iter.Next(); row != iter.End(); row = iter.Next() {
		partialMaxFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialPr2)
	}
	resultChk.Reset()
	partialMaxFunc.AppendFinalResult2Chunk(s.ctx, partialPr2, resultChk)
	c.Assert(resultChk.GetRow(0).GetMyDecimal(0).Compare(types.NewDecFromInt(4)) == 0, IsTrue)

	// merge two partial results.
	err := finalMaxFunc.MergePartialResult(s.ctx, partialPr1, finalPr)
	c.Assert(err, IsNil)
	err = finalMaxFunc.MergePartialResult(s.ctx, partialPr2, finalPr)
	c.Assert(err, IsNil)

	resultChk.Reset()
	err = finalMaxFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)
	c.Assert(resultChk.GetRow(0).GetMyDecimal(0).Compare(types.NewDecFromInt(4)) == 0, IsTrue)
}

func (s *testSuite) TestMergePartialResult4MaxFloat(c *C) {
	srcChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDouble)}, 5)
	for i := int64(0); i < 5; i++ {
		srcChk.AppendFloat64(0, float64(i))
	}
	iter := chunk.NewIterator4Chunk(srcChk)

	desc := &aggregation.AggFuncDesc{
		Name:  ast.AggFuncMax,
		Mode:  aggregation.CompleteMode,
		Args:  []expression.Expression{&expression.Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0}},
		RetTp: types.NewFieldType(mysql.TypeDouble),
	}
	partialDesc, finalDesc := desc.Split([]int{0})

	// build max func for partial phase.
	partialMaxFunc := aggfuncs.Build(s.ctx, partialDesc, 0)
	partialPr1 := partialMaxFunc.AllocPartialResult()
	partialPr2 := partialMaxFunc.AllocPartialResult()

	// build final func for final phase.
	finalMaxFunc := aggfuncs.Build(s.ctx, finalDesc, 0)
	finalPr := finalMaxFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDouble)}, 1)

	// update partial result.
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		partialMaxFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialPr1)
	}
	partialMaxFunc.AppendFinalResult2Chunk(s.ctx, partialPr1, resultChk)
	c.Assert(resultChk.GetRow(0).GetFloat64(0) == float64(4), IsTrue)

	row := iter.Begin()
	row = iter.Next()
	for row = iter.Next(); row != iter.End(); row = iter.Next() {
		partialMaxFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialPr2)
	}
	resultChk.Reset()
	partialMaxFunc.AppendFinalResult2Chunk(s.ctx, partialPr2, resultChk)
	c.Assert(resultChk.GetRow(0).GetFloat64(0) == float64(4), IsTrue)

	// merge two partial results.
	err := finalMaxFunc.MergePartialResult(s.ctx, partialPr1, finalPr)
	c.Assert(err, IsNil)
	err = finalMaxFunc.MergePartialResult(s.ctx, partialPr2, finalPr)
	c.Assert(err, IsNil)

	resultChk.Reset()
	err = finalMaxFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)
	c.Assert(resultChk.GetRow(0).GetFloat64(0) == float64(4), IsTrue)
}

func (s *testSuite) TestMergePartialResult4MinDecimal(c *C) {
	srcChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeNewDecimal)}, 5)
	for i := int64(0); i < 5; i++ {
		srcChk.AppendMyDecimal(0, types.NewDecFromInt(i))
	}
	iter := chunk.NewIterator4Chunk(srcChk)

	desc := &aggregation.AggFuncDesc{
		Name:  ast.AggFuncMin,
		Mode:  aggregation.CompleteMode,
		Args:  []expression.Expression{&expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0}},
		RetTp: types.NewFieldType(mysql.TypeNewDecimal),
	}
	partialDesc, finalDesc := desc.Split([]int{0})

	// build min func for partial phase.
	partialMinFunc := aggfuncs.Build(s.ctx, partialDesc, 0)
	partialPr1 := partialMinFunc.AllocPartialResult()
	partialPr2 := partialMinFunc.AllocPartialResult()

	// build final func for final phase.
	finalMinFunc := aggfuncs.Build(s.ctx, finalDesc, 0)
	finalPr := finalMinFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeNewDecimal)}, 1)

	// update partial result.
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		partialMinFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialPr1)
	}
	partialMinFunc.AppendFinalResult2Chunk(s.ctx, partialPr1, resultChk)
	c.Assert(resultChk.GetRow(0).GetMyDecimal(0).Compare(types.NewDecFromInt(0)) == 0, IsTrue)

	row := iter.Begin()
	row = iter.Next()
	for row = iter.Next(); row != iter.End(); row = iter.Next() {
		partialMinFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialPr2)
	}
	resultChk.Reset()
	partialMinFunc.AppendFinalResult2Chunk(s.ctx, partialPr2, resultChk)
	// Min in [2,3,4] -> 2
	c.Assert(resultChk.GetRow(0).GetMyDecimal(0).Compare(types.NewDecFromInt(2)) == 0, IsTrue)

	// merge two partial results.
	err := finalMinFunc.MergePartialResult(s.ctx, partialPr1, finalPr)
	c.Assert(err, IsNil)
	err = finalMinFunc.MergePartialResult(s.ctx, partialPr2, finalPr)
	c.Assert(err, IsNil)

	resultChk.Reset()
	err = finalMinFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)
	// Min in [0,1,2,3,4] -> 0
	c.Assert(resultChk.GetRow(0).GetMyDecimal(0).Compare(types.NewDecFromInt(0)) == 0, IsTrue)
}

func (s *testSuite) TestMergePartialResult4MinFloat(c *C) {
	srcChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDouble)}, 5)
	for i := int64(0); i < 5; i++ {
		srcChk.AppendFloat64(0, float64(i))
	}
	iter := chunk.NewIterator4Chunk(srcChk)

	desc := &aggregation.AggFuncDesc{
		Name:  ast.AggFuncMin,
		Mode:  aggregation.CompleteMode,
		Args:  []expression.Expression{&expression.Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0}},
		RetTp: types.NewFieldType(mysql.TypeDouble),
	}
	partialDesc, finalDesc := desc.Split([]int{0})

	// build min func for partial phase.
	partialMinFunc := aggfuncs.Build(s.ctx, partialDesc, 0)
	partialPr1 := partialMinFunc.AllocPartialResult()
	partialPr2 := partialMinFunc.AllocPartialResult()

	// build final func for final phase.
	finalMinFunc := aggfuncs.Build(s.ctx, finalDesc, 0)
	finalPr := finalMinFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeDouble)}, 1)

	// update partial result.
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		partialMinFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialPr1)
	}
	partialMinFunc.AppendFinalResult2Chunk(s.ctx, partialPr1, resultChk)
	c.Assert(resultChk.GetRow(0).GetFloat64(0) == float64(0), IsTrue)

	row := iter.Begin()
	row = iter.Next()
	for row = iter.Next(); row != iter.End(); row = iter.Next() {
		partialMinFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialPr2)
	}
	resultChk.Reset()
	partialMinFunc.AppendFinalResult2Chunk(s.ctx, partialPr2, resultChk)
	// Min in [2.0,3.0,4.0] -> 0
	c.Assert(resultChk.GetRow(0).GetFloat64(0) == float64(2), IsTrue)

	// merge two partial results.
	err := finalMinFunc.MergePartialResult(s.ctx, partialPr1, finalPr)
	c.Assert(err, IsNil)
	err = finalMinFunc.MergePartialResult(s.ctx, partialPr2, finalPr)
	c.Assert(err, IsNil)

	resultChk.Reset()
	err = finalMinFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)
	// Min in [0.0,1.0,2.0,3.0,4.0] -> 0
	c.Assert(resultChk.GetRow(0).GetFloat64(0) == float64(0), IsTrue)
}
