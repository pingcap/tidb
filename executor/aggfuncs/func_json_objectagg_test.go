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

	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/mock"
)

func TestMergePartialResult4JsonObjectagg(t *testing.T) {
	typeList := []byte{mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeString, mysql.TypeJSON}
	var argCombines [][]byte
	for i := 0; i < len(typeList); i++ {
		for j := 0; j < len(typeList); j++ {
			argTypes := []byte{typeList[i], typeList[j]}
			argCombines = append(argCombines, argTypes)
		}
	}

	var tests []multiArgsAggTest
	numRows := 5

	for k := 0; k < len(argCombines); k++ {
		entries1 := make(map[string]interface{})
		entries2 := make(map[string]interface{})

		argTypes := argCombines[k]
		fGenFunc := getDataGenFunc(types.NewFieldType(argTypes[0]))
		sGenFunc := getDataGenFunc(types.NewFieldType(argTypes[1]))

		for m := 0; m < numRows; m++ {
			firstArg := fGenFunc(m)
			secondArg := sGenFunc(m)
			keyString, _ := firstArg.ToString()
			entries1[keyString] = secondArg.GetValue()
		}

		for m := 2; m < numRows; m++ {
			firstArg := fGenFunc(m)
			secondArg := sGenFunc(m)
			keyString, _ := firstArg.ToString()
			entries2[keyString] = secondArg.GetValue()
		}

		aggTest := buildMultiArgsAggTester(ast.AggFuncJsonObjectAgg, argTypes, mysql.TypeJSON, numRows, json.CreateBinary(entries1), json.CreateBinary(entries2), json.CreateBinary(entries1))

		tests = append(tests, aggTest)
	}

	ctx := mock.NewContext()
	for _, test := range tests {
		testMultiArgsMergePartialResult(t, ctx, test)
	}
}

func TestJsonObjectagg(t *testing.T) {
	typeList := []byte{mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeString, mysql.TypeJSON}
	var argCombines [][]byte
	for i := 0; i < len(typeList); i++ {
		for j := 0; j < len(typeList); j++ {
			argTypes := []byte{typeList[i], typeList[j]}
			argCombines = append(argCombines, argTypes)
		}
	}

	var tests []multiArgsAggTest
	numRows := 5

	for k := 0; k < len(argCombines); k++ {
		entries := make(map[string]interface{})

		argTypes := argCombines[k]
		fGenFunc := getDataGenFunc(types.NewFieldType(argTypes[0]))
		sGenFunc := getDataGenFunc(types.NewFieldType(argTypes[1]))

		for m := 0; m < numRows; m++ {
			firstArg := fGenFunc(m)
			secondArg := sGenFunc(m)
			keyString, _ := firstArg.ToString()
			entries[keyString] = secondArg.GetValue()
		}

		aggTest := buildMultiArgsAggTester(ast.AggFuncJsonObjectAgg, argTypes, mysql.TypeJSON, numRows, nil, json.CreateBinary(entries))

		tests = append(tests, aggTest)
	}

	ctx := mock.NewContext()
	for _, test := range tests {
		testMultiArgsAggFunc(t, ctx, test)
	}
}

func TestMemJsonObjectagg(t *testing.T) {
	typeList := []byte{mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeString, mysql.TypeJSON, mysql.TypeDuration, mysql.TypeNewDecimal, mysql.TypeDate}
	var argCombines [][]byte
	for i := 0; i < len(typeList); i++ {
		for j := 0; j < len(typeList); j++ {
			argTypes := []byte{typeList[i], typeList[j]}
			argCombines = append(argCombines, argTypes)
		}
	}
	numRows := 5
	for k := 0; k < len(argCombines); k++ {
		entries := make(map[string]interface{})

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
			buildMultiArgsAggMemTester(ast.AggFuncJsonObjectAgg, argTypes, mysql.TypeJSON, numRows, aggfuncs.DefPartialResult4JsonObjectAgg+aggfuncs.DefMapStringInterfaceBucketSize, defaultMultiArgsMemDeltaGens, true),
			buildMultiArgsAggMemTester(ast.AggFuncJsonObjectAgg, argTypes, mysql.TypeJSON, numRows, aggfuncs.DefPartialResult4JsonObjectAgg+aggfuncs.DefMapStringInterfaceBucketSize, defaultMultiArgsMemDeltaGens, false),
		}
		for _, test := range tests {
			testMultiArgsAggMemFunc(t, test)
		}
	}
}
