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

func (s *testSuite) TestMergePartialResult4Count(c *C) {
	srcChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 5)
	for i := int64(0); i < 5; i++ {
		srcChk.AppendInt64(0, i)
	}
	iter := chunk.NewIterator4Chunk(srcChk)

	desc := &aggregation.AggFuncDesc{
		Name:  ast.AggFuncCount,
		Mode:  aggregation.CompleteMode,
		Args:  []expression.Expression{&expression.Column{RetType: types.NewFieldType(mysql.TypeLong), Index: 0}},
		RetTp: types.NewFieldType(mysql.TypeLonglong),
	}
	partialDesc, finalDesc := desc.Split([]int{0})

	// build count func for partial phase.
	partialCountFunc := aggfuncs.Build(s.ctx, partialDesc, 0)
	partialPr1 := partialCountFunc.AllocPartialResult()

	// build final func for final phase.
	finalCountFunc := aggfuncs.Build(s.ctx, finalDesc, 0)
	finalPr := finalCountFunc.AllocPartialResult()
	resultChk := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, 1)

	// update partial result.
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		partialCountFunc.UpdatePartialResult(s.ctx, []chunk.Row{row}, partialPr1)
	}
	partialCountFunc.AppendFinalResult2Chunk(s.ctx, partialPr1, resultChk)
	c.Assert(resultChk.GetRow(0).GetInt64(0), Equals, int64(5))

	// suppose there are two partial workers.
	partialPr2 := partialPr1

	// merge two partial results.
	err := finalCountFunc.MergePartialResult(s.ctx, partialPr1, finalPr)
	c.Assert(err, IsNil)
	err = finalCountFunc.MergePartialResult(s.ctx, partialPr2, finalPr)
	c.Assert(err, IsNil)

	resultChk.Reset()
	err = finalCountFunc.AppendFinalResult2Chunk(s.ctx, finalPr, resultChk)
	c.Assert(err, IsNil)
	c.Assert(resultChk.GetRow(0).GetInt64(0), Equals, int64(10))
}
