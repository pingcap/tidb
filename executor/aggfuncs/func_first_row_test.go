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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs_test

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

func TestMergePartialResult4FirstRow(t *testing.T) {
	elems := []string{"e", "d", "c", "b", "a"}
	enumC, _ := types.ParseEnumName(elems, "c", mysql.DefaultCollationName)
	enumE, _ := types.ParseEnumName(elems, "e", mysql.DefaultCollationName)

	setED, _ := types.ParseSetName(elems, "e,d", mysql.DefaultCollationName)
	setE, _ := types.ParseSetName(elems, "e", mysql.DefaultCollationName)

	tests := []aggTest{
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeLonglong, 5, 0, 2, 0),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeFloat, 5, 0.0, 2.0, 0.0),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeDouble, 5, 0.0, 2.0, 0.0),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeNewDecimal, 5, types.NewDecFromInt(0), types.NewDecFromInt(2), types.NewDecFromInt(0)),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeString, 5, "0", "2", "0"),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeDate, 5, types.TimeFromDays(365), types.TimeFromDays(367), types.TimeFromDays(365)),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeDuration, 5, types.Duration{Duration: time.Duration(0)}, types.Duration{Duration: time.Duration(2)}, types.Duration{Duration: time.Duration(0)}),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeJSON, 5, json.CreateBinary(int64(0)), json.CreateBinary(int64(2)), json.CreateBinary(int64(0))),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeEnum, 5, enumE, enumC, enumE),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeSet, 5, setE, setED, setE),
	}
	for _, test := range tests {
		testMergePartialResult(t, test)
	}
}

func TestMemFirstRow(t *testing.T) {
	tests := []aggMemTest{
		buildAggMemTester(ast.AggFuncFirstRow, mysql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4FirstRowIntSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncFirstRow, mysql.TypeFloat, 5,
			aggfuncs.DefPartialResult4FirstRowFloat32Size, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncFirstRow, mysql.TypeDouble, 5,
			aggfuncs.DefPartialResult4FirstRowFloat64Size, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncFirstRow, mysql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4FirstRowDecimalSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncFirstRow, mysql.TypeString, 5,
			aggfuncs.DefPartialResult4FirstRowStringSize, firstRowUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncFirstRow, mysql.TypeDate, 5,
			aggfuncs.DefPartialResult4FirstRowTimeSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncFirstRow, mysql.TypeDuration, 5,
			aggfuncs.DefPartialResult4FirstRowDurationSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncFirstRow, mysql.TypeJSON, 5,
			aggfuncs.DefPartialResult4FirstRowJSONSize, firstRowUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncFirstRow, mysql.TypeEnum, 5,
			aggfuncs.DefPartialResult4FirstRowEnumSize, firstRowUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncFirstRow, mysql.TypeSet, 5,
			aggfuncs.DefPartialResult4FirstRowSetSize, firstRowUpdateMemDeltaGens, false),
	}
	for _, test := range tests {
		testAggMemFunc(t, test)
	}
}

func firstRowUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	for i := 0; i < srcChk.NumRows(); i++ {
		row := srcChk.GetRow(i)
		if i > 0 {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		switch dataType.GetType() {
		case mysql.TypeString:
			val := row.GetString(0)
			memDeltas = append(memDeltas, int64(len(val)))
		case mysql.TypeJSON:
			jsonVal := row.GetJSON(0)
			memDeltas = append(memDeltas, int64(len(string(jsonVal.Value))))
		case mysql.TypeEnum:
			enum := row.GetEnum(0)
			memDeltas = append(memDeltas, int64(len(enum.Name)))
		case mysql.TypeSet:
			typeSet := row.GetSet(0)
			memDeltas = append(memDeltas, int64(len(typeSet.Name)))
		}
	}
	return memDeltas, nil
}
