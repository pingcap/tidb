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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

func (s *testSuite) TestMergePartialResult4JsonObjectagg(c *C) {
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

	for _, test := range tests {
		s.testMultiArgsMergePartialResult(c, test)
	}
}

func (s *testSuite) TestJsonObjectagg(c *C) {
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

	for _, test := range tests {
		s.testMultiArgsAggFunc(c, test)
	}
}
