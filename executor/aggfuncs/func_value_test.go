// Copyright 2020 PingCAP, Inc.
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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func getEvaluatedMemDelta(row *chunk.Row, dataType *types.FieldType) (memDelta int64) {
	memDelta = 0
	switch dataType.Tp {
	case mysql.TypeString:
		memDelta = int64(len(row.GetString(0)))
	case mysql.TypeJSON:
		memDelta = int64(len(row.GetJSON(0).Value))
	}
	return
}

func lastValueEvaluateRowUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)
	lastMemDelta := int64(0)
	for i := 0; i < srcChk.NumRows(); i++ {
		row := srcChk.GetRow(0)
		curMemDelta := getEvaluatedMemDelta(&row, dataType)
		memDeltas = append(memDeltas, curMemDelta-lastMemDelta)
		lastMemDelta = curMemDelta
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
	firstMemDeltaGens := nthValueEvaluateRowUpdateMemDeltaGens(1)
	secondMemDeltaGens := nthValueEvaluateRowUpdateMemDeltaGens(2)
	fifthMemDeltaGens := nthValueEvaluateRowUpdateMemDeltaGens(5)
	tests := []windowMemTest{
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeLonglong, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4IntSize, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeFloat, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4Float32Size, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeDouble, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4Float64Size, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeNewDecimal, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4DecimalSize, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeString, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4StringSize, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeDate, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4TimeSize, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeDuration, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4DurationSize, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncFirstValue, mysql.TypeJSON, 0, 2, 1,
			aggfuncs.DefPartialResult4FirstValueSize+aggfuncs.DefValue4JSONSize, firstMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncLastValue, mysql.TypeLonglong, 1, 2, 0,
			aggfuncs.DefPartialResult4LastValueSize+aggfuncs.DefValue4IntSize, lastValueEvaluateRowUpdateMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncLastValue, mysql.TypeString, 1, 2, 0,
			aggfuncs.DefPartialResult4LastValueSize+aggfuncs.DefValue4StringSize, lastValueEvaluateRowUpdateMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncLastValue, mysql.TypeJSON, 1, 2, 0,
			aggfuncs.DefPartialResult4LastValueSize+aggfuncs.DefValue4JSONSize, lastValueEvaluateRowUpdateMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncNthValue, mysql.TypeLonglong, 2, 3, 0,
			aggfuncs.DefPartialResult4NthValueSize+aggfuncs.DefValue4IntSize, secondMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncNthValue, mysql.TypeLonglong, 5, 3, 0,
			aggfuncs.DefPartialResult4NthValueSize+aggfuncs.DefValue4IntSize, fifthMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncNthValue, mysql.TypeJSON, 2, 3, 0,
			aggfuncs.DefPartialResult4NthValueSize+aggfuncs.DefValue4JSONSize, secondMemDeltaGens),
		buildWindowMemTester(ast.WindowFuncNthValue, mysql.TypeString, 5, 3, 0,
			aggfuncs.DefPartialResult4NthValueSize+aggfuncs.DefValue4StringSize, fifthMemDeltaGens),
	}
	for _, test := range tests {
		s.testWindowAggMemFunc(c, test)
	}
}
