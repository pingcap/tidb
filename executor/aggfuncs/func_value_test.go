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
	"unsafe"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func getEvaluatedMemDelta(row *chunk.Row, dataType *types.FieldType) (memDelta int64) {
	memDelta = 0
	switch dataType.Tp {
	case mysql.TypeString:
		memDelta = int64(len(row.GetString(0)))
	case mysql.TypeJSON:
		memDelta = int64(unsafe.Sizeof(row.GetJSON(0)))
	}
	return
}

func firstValueEvaluateRowUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)
	if srcChk.NumRows() > 0 {
		row := srcChk.GetRow(0)
		memDeltas = append(memDeltas, getEvaluatedMemDelta(&row, dataType))
	}
	for i := 1; i < srcChk.NumRows(); i++ {
		memDeltas = append(memDeltas, int64(0))
	}
	return memDeltas, nil
}

func lastValueEvaluateRowUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)
	for i := 0; i < srcChk.NumRows(); i++ {
		row := srcChk.GetRow(0)
		memDeltas = append(memDeltas, getEvaluatedMemDelta(&row, dataType))
	}
	return memDeltas, nil
}

func nthValueEvaluateRowUpdateMemDeltaGens(nth int) updateMemDeltaGens {
	return func(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
		memDeltas = make([]int64, 0)
		for i := 0; i < srcChk.NumRows(); i++ {
			memDeltas = append(memDeltas, int64(0))
		}
		if nth < srcChk.NumRows() {
			row := srcChk.GetRow(nth - 1)
			memDeltas[nth-1] = getEvaluatedMemDelta(&row, dataType)
		}
		return memDeltas, nil
	}
}

func (s *testSuite) TestMemValue(c *C) {
	tests := []windowMemTest{
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeLonglong, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4IntSize, firstValueEvaluateRowUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeFloat, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4Float32Size, firstValueEvaluateRowUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeDouble, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4Float64Size, firstValueEvaluateRowUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeNewDecimal, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4DecimalSize, firstValueEvaluateRowUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeString, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4StringSize, firstValueEvaluateRowUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeDate, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4TimeSize, firstValueEvaluateRowUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeDuration, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4DurationSize, firstValueEvaluateRowUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeJSON, 0, 1, 2,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4JSONSize, firstValueEvaluateRowUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncLastValue, mysql.TypeLonglong, 1, 0, 2,
			aggfuncs.DefPartialResult4LastValueSize+aggfuncs.DefValue4IntSize, lastValueEvaluateRowUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncLastValue, mysql.TypeString, 1, 0, 2,
			aggfuncs.DefPartialResult4LastValueSize+aggfuncs.DefValue4StringSize, lastValueEvaluateRowUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncLastValue, mysql.TypeJSON, 1, 0, 2,
			aggfuncs.DefPartialResult4LastValueSize+aggfuncs.DefValue4JSONSize, lastValueEvaluateRowUpdateMemDeltaGens, false),
		buildWindowMemTester(ast.WindowFuncNthValue, mysql.TypeLonglong, 2, 0, 3,
			aggfuncs.DefPartialResult4NthValueSize+aggfuncs.DefValue4IntSize, nthValueEvaluateRowUpdateMemDeltaGens(2), false),
		buildWindowMemTester(ast.WindowFuncNthValue, mysql.TypeLonglong, 5, 0, 3,
			aggfuncs.DefPartialResult4NthValueSize+aggfuncs.DefValue4IntSize, nthValueEvaluateRowUpdateMemDeltaGens(5), false),
		buildWindowMemTester(ast.WindowFuncNthValue, mysql.TypeJSON, 2, 0, 3,
			aggfuncs.DefPartialResult4NthValueSize+aggfuncs.DefValue4JSONSize, nthValueEvaluateRowUpdateMemDeltaGens(2), false),
		buildWindowMemTester(ast.WindowFuncNthValue, mysql.TypeString, 5, 0, 3,
			aggfuncs.DefPartialResult4NthValueSize+aggfuncs.DefValue4StringSize, nthValueEvaluateRowUpdateMemDeltaGens(5), false),
	}
	for _, test := range tests {
		s.testWindowMemFunc(c, test)
	}
}
