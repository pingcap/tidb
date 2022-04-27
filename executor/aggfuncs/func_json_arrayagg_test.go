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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs_test

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

func TestMergePartialResult4JsonArrayagg(t *testing.T) {
	typeList := []byte{mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeString, mysql.TypeJSON}

	tests := make([]aggTest, 0, len(typeList))
	numRows := 5
	for _, argType := range typeList {
		entries1 := make([]interface{}, 0)
		entries2 := make([]interface{}, 0)
		entries3 := make([]interface{}, 0)

		genFunc := getDataGenFunc(types.NewFieldType(argType))

		for m := 0; m < numRows; m++ {
			arg := genFunc(m)
			entries1 = append(entries1, arg.GetValue())
		}
		// to adapt the `genSrcChk` Chunk format
		entries1 = append(entries1, nil)

		for m := 2; m < numRows; m++ {
			arg := genFunc(m)
			entries2 = append(entries2, arg.GetValue())
		}
		// to adapt the `genSrcChk` Chunk format
		entries2 = append(entries2, nil)

		entries3 = append(entries3, entries1...)
		entries3 = append(entries3, entries2...)

		tests = append(tests, buildAggTester(ast.AggFuncJsonArrayagg, argType, numRows, json.CreateBinary(entries1), json.CreateBinary(entries2), json.CreateBinary(entries3)))
	}

	for _, test := range tests {
		testMergePartialResult(t, test)
	}
}

func TestJsonArrayagg(t *testing.T) {
	typeList := []byte{mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeString, mysql.TypeJSON}

	tests := make([]aggTest, 0, len(typeList))
	numRows := 5

	for _, argType := range typeList {
		entries := make([]interface{}, 0)

		genFunc := getDataGenFunc(types.NewFieldType(argType))

		for m := 0; m < numRows; m++ {
			arg := genFunc(m)
			entries = append(entries, arg.GetValue())
		}
		// to adapt the `genSrcChk` Chunk format
		entries = append(entries, nil)

		tests = append(tests, buildAggTester(ast.AggFuncJsonArrayagg, argType, numRows, nil, json.CreateBinary(entries)))
	}

	for _, test := range tests {
		testAggFuncWithoutDistinct(t, test)
	}
}

func jsonArrayaggMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)
	for i := 0; i < srcChk.NumRows(); i++ {
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			memDeltas = append(memDeltas, aggfuncs.DefInterfaceSize)
			continue
		}

		memDelta := int64(0)
		memDelta += aggfuncs.DefInterfaceSize
		switch dataType.GetType() {
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
			return memDeltas, errors.Errorf("unsupported type - %v", dataType.GetType())
		}
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}

func TestMemJsonArrayagg(t *testing.T) {
	typeList := []byte{mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeString, mysql.TypeJSON}

	tests := make([]aggMemTest, 0, len(typeList))
	numRows := 5
	for _, argType := range typeList {
		tests = append(tests, buildAggMemTester(ast.AggFuncJsonArrayagg, argType, numRows, aggfuncs.DefPartialResult4JsonArrayagg+aggfuncs.DefSliceSize, jsonArrayaggMemDeltaGens, false))
	}

	for _, test := range tests {
		testAggMemFunc(t, test)
	}
}
