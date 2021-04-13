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
	"github.com/pingcap/tidb/types/json"
)

func (s *testSuite) TestMergePartialResult4JsonArrayagg(c *C) {
	typeList := []byte{mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeString, mysql.TypeJSON}
	var argCombines [][]byte
	for i := 0; i < len(typeList); i++ {
		argTypes := []byte{typeList[i]}
		argCombines = append(argCombines, argTypes)
	}

	var tests []multiArgsAggTest
	numRows := 5

	for k := 0; k < len(argCombines); k++ {
		entries1 := make([]interface{}, 0)
		entries2 := make([]interface{}, 0)

		argTypes := argCombines[k]
		vGenFunc := getDataGenFunc(types.NewFieldType(argTypes[0]))

		for m := 0; m < numRows; m++ {
			firstArg := vGenFunc(m)
			entries1 = append(entries1, firstArg.GetValue())
		}

		for m := 2; m < numRows; m++ {
			firstArg := vGenFunc(m)
			entries2 = append(entries2, firstArg.GetValue())
		}

		aggTest := buildMultiArgsAggTester(ast.AggFuncJsonArrayagg, argTypes, mysql.TypeJSON, numRows, json.CreateBinary(entries1), json.CreateBinary(entries2), json.CreateBinary(entries1))

		tests = append(tests, aggTest)
	}

	for _, test := range tests {
		s.testMultiArgsMergePartialResult(c, test)
	}
}

func (s *testSuite) TestJsonArrayagg(c *C) {
	typeList := []byte{mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeString, mysql.TypeJSON}
	var argCombines [][]byte
	for i := 0; i < len(typeList); i++ {
		argTypes := []byte{typeList[i]}
		argCombines = append(argCombines, argTypes)
	}

	var tests []multiArgsAggTest
	numRows := 5

	for k := 0; k < len(argCombines); k++ {
		entries := make([]interface{}, 0)

		argTypes := argCombines[k]
		vGenFunc := getDataGenFunc(types.NewFieldType(argTypes[0]))

		for m := 0; m < numRows; m++ {
			firstArg := vGenFunc(m)
			entries = append(entries, firstArg.GetValue())
		}

		aggTest := buildMultiArgsAggTester(ast.AggFuncJsonArrayagg, argTypes, mysql.TypeJSON, numRows, nil, json.CreateBinary(entries))

		tests = append(tests, aggTest)
	}

	for _, test := range tests {
		s.testMultiArgsAggFunc(c, test)
	}
}

func (s *testSuite) TestMemJsonArrayagg(c *C) {
	typeList := []byte{mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeString, mysql.TypeJSON, mysql.TypeDuration, mysql.TypeNewDecimal, mysql.TypeDate}
	var argCombines [][]byte
	for i := 0; i < len(typeList); i++ {
		argTypes := []byte{typeList[i]}
		argCombines = append(argCombines, argTypes)
	}
	numRows := 5
	for k := 0; k < len(argCombines); k++ {
		entries := make([]interface{}, 0)

		argTypes := argCombines[k]
		vGenFunc := getDataGenFunc(types.NewFieldType(argTypes[0]))

		for m := 0; m < numRows; m++ {
			firstArg := vGenFunc(m)
			entries = append(entries, firstArg.GetValue())
		}

		// appendBinary does not support some type such as uint8、types.time，so convert is needed here
		for i, val := range entries {
			switch x := val.(type) {
			case *types.MyDecimal:
				float64Val, _ := x.ToFloat64()
				entries[i] = float64Val
			case []uint8, types.Time, types.Duration:
				strVal, _ := types.ToString(x)
				entries[i] = strVal
			}
		}

		tests := []multiArgsAggMemTest{
			buildMultiArgsAggMemTester(ast.AggFuncJsonArrayagg, argTypes, mysql.TypeJSON, numRows, aggfuncs.DefPartialResult4JsonArrayAgg, defaultMultiArgsMemDeltaGens, true),
			buildMultiArgsAggMemTester(ast.AggFuncJsonArrayagg, argTypes, mysql.TypeJSON, numRows, aggfuncs.DefPartialResult4JsonArrayAgg, defaultMultiArgsMemDeltaGens, false),
		}
		for _, test := range tests {
			s.testMultiArgsAggMemFunc(c, test)
		}
	}
}
