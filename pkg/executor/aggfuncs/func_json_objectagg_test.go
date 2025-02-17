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
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/mock"
)

func getJSONValue(secondArg types.Datum, valueType *types.FieldType) any {
	if valueType.GetType() == mysql.TypeString && valueType.GetCharset() == charset.CharsetBin {
		buf := make([]byte, valueType.GetFlen())
		copy(buf, secondArg.GetBytes())
		return types.Opaque{
			TypeCode: mysql.TypeString,
			Buf:      buf,
		}
	}
	if valueType.GetType() == mysql.TypeFloat {
		return float64(secondArg.GetFloat32())
	}
	return secondArg.GetValue()
}

func TestMergePartialResult4JsonObjectagg(t *testing.T) {
	typeList := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeFloat),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeJSON),
		types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlen(10).SetCharset(charset.CharsetBin).BuildP(),
		types.NewFieldType(mysql.TypeDate),
		types.NewFieldType(mysql.TypeDuration),
	}
	var argCombines [][]*types.FieldType
	for i := 0; i < len(typeList); i++ {
		if typeList[i].GetCharset() == charset.CharsetBin {
			// skip because binary charset cannot be used as key.
			continue
		}
		for j := 0; j < len(typeList); j++ {
			argTypes := []*types.FieldType{typeList[i], typeList[j]}
			argCombines = append(argCombines, argTypes)
		}
	}

	var tests []multiArgsAggTest
	numRows := 5

	for k := 0; k < len(argCombines); k++ {
		entries1 := make(map[string]any)
		entries2 := make(map[string]any)

		fGenFunc := getDataGenFunc(argCombines[k][0])
		sGenFunc := getDataGenFunc(argCombines[k][1])

		for m := 0; m < numRows; m++ {
			firstArg := fGenFunc(m)
			secondArg := sGenFunc(m)
			keyString, _ := firstArg.ToString()

			valueType := argCombines[k][1]
			entries1[keyString] = getJSONValue(secondArg, valueType)
		}

		for m := 2; m < numRows; m++ {
			firstArg := fGenFunc(m)
			secondArg := sGenFunc(m)
			keyString, _ := firstArg.ToString()

			valueType := argCombines[k][1]
			entries2[keyString] = getJSONValue(secondArg, valueType)
		}

		aggTest := buildMultiArgsAggTesterWithFieldType(ast.AggFuncJsonObjectAgg, argCombines[k], types.NewFieldType(mysql.TypeJSON), numRows, types.CreateBinaryJSON(entries1), types.CreateBinaryJSON(entries2), types.CreateBinaryJSON(entries1))

		tests = append(tests, aggTest)
	}

	ctx := mock.NewContext()
	for _, test := range tests {
		testMultiArgsMergePartialResult(t, ctx, test)
	}
}

func TestJsonObjectagg(t *testing.T) {
	typeList := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeFloat),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeJSON),
		types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlen(10).SetCharset(charset.CharsetBin).BuildP(),
		types.NewFieldType(mysql.TypeDate),
		types.NewFieldType(mysql.TypeDuration),
	}
	var argCombines [][]*types.FieldType
	for i := 0; i < len(typeList); i++ {
		if typeList[i].GetCharset() == charset.CharsetBin {
			// skip because binary charset cannot be used as key.
			continue
		}
		for j := 0; j < len(typeList); j++ {
			argTypes := []*types.FieldType{typeList[i], typeList[j]}
			argCombines = append(argCombines, argTypes)
		}
	}

	var tests []multiArgsAggTest
	numRows := 5

	for k := 0; k < len(argCombines); k++ {
		entries := make(map[string]any)

		argTypes := argCombines[k]
		fGenFunc := getDataGenFunc(argTypes[0])
		sGenFunc := getDataGenFunc(argTypes[1])

		for m := 0; m < numRows; m++ {
			firstArg := fGenFunc(m)
			secondArg := sGenFunc(m)
			keyString, _ := firstArg.ToString()

			valueType := argCombines[k][1]
			entries[keyString] = getJSONValue(secondArg, valueType)
		}

		aggTest := buildMultiArgsAggTesterWithFieldType(ast.AggFuncJsonObjectAgg, argTypes, types.NewFieldType(mysql.TypeJSON), numRows, nil, types.CreateBinaryJSON(entries))

		tests = append(tests, aggTest)
	}

	ctx := mock.NewContext()
	for _, test := range tests {
		testMultiArgsAggFunc(t, ctx, test)
	}
}

func TestMemJsonObjectagg(t *testing.T) {
	typeList := []byte{mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeFloat, mysql.TypeString, mysql.TypeJSON, mysql.TypeDuration, mysql.TypeNewDecimal, mysql.TypeDate}
	var argCombines [][]byte
	for i := 0; i < len(typeList); i++ {
		for j := 0; j < len(typeList); j++ {
			argTypes := []byte{typeList[i], typeList[j]}
			argCombines = append(argCombines, argTypes)
		}
	}
	numRows := 5
	for k := 0; k < len(argCombines); k++ {
		entries := make(map[string]any)

		argTypes := argCombines[k]
		fGenFunc := getDataGenFunc(types.NewFieldType(argTypes[0]))
		sGenFunc := getDataGenFunc(types.NewFieldType(argTypes[1]))

		for m := 0; m < numRows; m++ {
			firstArg := fGenFunc(m)
			secondArg := sGenFunc(m)
			keyString, _ := firstArg.ToString()
			entries[keyString] = secondArg.GetValue()
		}

		// appendBinary does not support some type such as uint8、types.time，so convert is needed here
		for key, val := range entries {
			switch x := val.(type) {
			case *types.MyDecimal:
				float64Val, _ := x.ToFloat64()
				entries[key] = float64Val
			case []uint8, types.Time, types.Duration:
				strVal, _ := types.ToString(x)
				entries[key] = strVal
			}
		}

		tests := []multiArgsAggMemTest{
			buildMultiArgsAggMemTester(ast.AggFuncJsonObjectAgg, argTypes, mysql.TypeJSON, numRows, aggfuncs.DefPartialResult4JsonObjectAgg+hack.DefBucketMemoryUsageForMapStringToAny, jsonMultiArgsMemDeltaGens, true),
			buildMultiArgsAggMemTester(ast.AggFuncJsonObjectAgg, argTypes, mysql.TypeJSON, numRows, aggfuncs.DefPartialResult4JsonObjectAgg+hack.DefBucketMemoryUsageForMapStringToAny, jsonMultiArgsMemDeltaGens, false),
		}
		for _, test := range tests {
			testMultiArgsAggMemFunc(t, test)
		}
	}
}

func jsonMultiArgsMemDeltaGens(_ sessionctx.Context, srcChk *chunk.Chunk, dataTypes []*types.FieldType, byItems []*util.ByItems) (memDeltas []int64, err error) {
	memDeltas = make([]int64, 0)
	m := make(map[string]bool)
	for i := 0; i < srcChk.NumRows(); i++ {
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		datum := row.GetDatum(0, dataTypes[0])
		if datum.IsNull() {
			memDeltas = append(memDeltas, int64(0))
			continue
		}

		memDelta := int64(0)
		key, err := datum.ToString()
		if err != nil {
			return memDeltas, errors.Errorf("fail to get key - %s", key)
		}
		if _, ok := m[key]; ok {
			memDeltas = append(memDeltas, int64(0))
			continue
		}
		m[key] = true
		memDelta += int64(len(key))

		memDelta += aggfuncs.DefInterfaceSize
		switch dataTypes[1].GetType() {
		case mysql.TypeLonglong:
			memDelta += aggfuncs.DefUint64Size
		case mysql.TypeFloat:
			memDelta += aggfuncs.DefFloat64Size
		case mysql.TypeDouble:
			memDelta += aggfuncs.DefFloat64Size
		case mysql.TypeString:
			val := row.GetString(1)
			memDelta += int64(len(val))
		case mysql.TypeJSON:
			val := row.GetJSON(1)
			// +1 for the memory usage of the JSONTypeCode of json
			memDelta += int64(len(val.Value) + 1)
		case mysql.TypeDuration:
			memDelta += aggfuncs.DefDurationSize
		case mysql.TypeDate:
			memDelta += aggfuncs.DefTimeSize
		case mysql.TypeNewDecimal:
			memDelta += aggfuncs.DefFloat64Size
		default:
			return memDeltas, errors.Errorf("unsupported type - %v", dataTypes[1].GetType())
		}
		memDeltas = append(memDeltas, memDelta)
	}
	return memDeltas, nil
}
