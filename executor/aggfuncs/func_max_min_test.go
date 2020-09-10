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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

func maxMinUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType, isMax bool) (memDeltas []int64, err error) {
	memDeltas = make([]int64, srcChk.NumRows())
	var (
		preStringVal string
		preJSONVal   string
		preEnumVal   types.Enum
		preSetVal    types.Set
	)

	for i := 0; i < srcChk.NumRows(); i++ {
		row := srcChk.GetRow(i)
		if row.IsNull(0) {
			continue
		}
		switch dataType.Tp {
		case mysql.TypeString:
			curVal := row.GetString(0)
			if i == 0 {
				memDeltas[i] = int64(len(curVal))
				preStringVal = curVal
			} else if isMax && curVal > preStringVal || !isMax && curVal < preStringVal {
				memDeltas[i] = int64(len(curVal)) - int64(len(preStringVal))
				preStringVal = curVal
			}
		case mysql.TypeJSON:
			curVal := row.GetJSON(0)
			curStringVal := string(curVal.Value)
			if i == 0 {
				memDeltas[i] = int64(len(curStringVal))
				preJSONVal = curStringVal
			} else if isMax && curStringVal > preJSONVal || !isMax && curStringVal < preJSONVal {
				memDeltas[i] = int64(len(curStringVal)) - int64(len(preJSONVal))
				preJSONVal = curStringVal
			}
		case mysql.TypeEnum:
			curVal := row.GetEnum(0)
			if i == 0 {
				memDeltas[i] = int64(len(curVal.Name))
				preEnumVal = curVal
			} else if isMax && curVal.Value > preEnumVal.Value || !isMax && curVal.Value < preEnumVal.Value {
				memDeltas[i] = int64(len(curVal.Name)) - int64(len(preEnumVal.Name))
				preEnumVal = curVal
			}
		case mysql.TypeSet:
			curVal := row.GetSet(0)
			if i == 0 {
				memDeltas[i] = int64(len(curVal.Name))
				preSetVal = curVal
			} else if isMax && curVal.Value > preSetVal.Value || !isMax && curVal.Value < preSetVal.Value {
				memDeltas[i] = int64(len(curVal.Name)) - int64(len(preSetVal.Name))
				preSetVal = curVal
			}
		}
	}
	return memDeltas, nil
}

func maxUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	return maxMinUpdateMemDeltaGens(srcChk, dataType, true)
}

func minUpdateMemDeltaGens(srcChk *chunk.Chunk, dataType *types.FieldType) (memDeltas []int64, err error) {
	return maxMinUpdateMemDeltaGens(srcChk, dataType, false)
}

func (s *testSuite) TestMergePartialResult4MaxMin(c *C) {
	elems := []string{"a", "b", "c", "d", "e"}
	enumA, _ := types.ParseEnumName(elems, "a", mysql.DefaultCollationName)
	enumC, _ := types.ParseEnumName(elems, "c", mysql.DefaultCollationName)
	enumE, _ := types.ParseEnumName(elems, "e", mysql.DefaultCollationName)

	setA, _ := types.ParseSetName(elems, "a", mysql.DefaultCollationName)    // setA.Value == 1
	setAB, _ := types.ParseSetName(elems, "a,b", mysql.DefaultCollationName) // setAB.Value == 3
	setAC, _ := types.ParseSetName(elems, "a,c", mysql.DefaultCollationName) // setAC.Value == 5

	unsignedType := types.NewFieldType(mysql.TypeLonglong)
	unsignedType.Flag |= mysql.UnsignedFlag
	tests := []aggTest{
		buildAggTester(ast.AggFuncMax, mysql.TypeLonglong, 5, 4, 4, 4),
		buildAggTesterWithFieldType(ast.AggFuncMax, unsignedType, 5, 4, 4, 4),
		buildAggTester(ast.AggFuncMax, mysql.TypeFloat, 5, 4.0, 4.0, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeDouble, 5, 4.0, 4.0, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeNewDecimal, 5, types.NewDecFromInt(4), types.NewDecFromInt(4), types.NewDecFromInt(4)),
		buildAggTester(ast.AggFuncMax, mysql.TypeString, 5, "4", "4", "4"),
		buildAggTester(ast.AggFuncMax, mysql.TypeDate, 5, types.TimeFromDays(369), types.TimeFromDays(369), types.TimeFromDays(369)),
		buildAggTester(ast.AggFuncMax, mysql.TypeDuration, 5, types.Duration{Duration: time.Duration(4)}, types.Duration{Duration: time.Duration(4)}, types.Duration{Duration: time.Duration(4)}),
		buildAggTester(ast.AggFuncMax, mysql.TypeJSON, 5, json.CreateBinary(int64(4)), json.CreateBinary(int64(4)), json.CreateBinary(int64(4))),
		buildAggTester(ast.AggFuncMax, mysql.TypeEnum, 5, enumE, enumE, enumE),
		buildAggTester(ast.AggFuncMax, mysql.TypeSet, 5, setAC, setAC, setAC),

		buildAggTester(ast.AggFuncMin, mysql.TypeLonglong, 5, 0, 2, 0),
		buildAggTesterWithFieldType(ast.AggFuncMin, unsignedType, 5, 0, 2, 0),
		buildAggTester(ast.AggFuncMin, mysql.TypeFloat, 5, 0.0, 2.0, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeDouble, 5, 0.0, 2.0, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeNewDecimal, 5, types.NewDecFromInt(0), types.NewDecFromInt(2), types.NewDecFromInt(0)),
		buildAggTester(ast.AggFuncMin, mysql.TypeString, 5, "0", "2", "0"),
		buildAggTester(ast.AggFuncMin, mysql.TypeDate, 5, types.TimeFromDays(365), types.TimeFromDays(367), types.TimeFromDays(365)),
		buildAggTester(ast.AggFuncMin, mysql.TypeDuration, 5, types.Duration{Duration: time.Duration(0)}, types.Duration{Duration: time.Duration(2)}, types.Duration{Duration: time.Duration(0)}),
		buildAggTester(ast.AggFuncMin, mysql.TypeJSON, 5, json.CreateBinary(int64(0)), json.CreateBinary(int64(2)), json.CreateBinary(int64(0))),
		buildAggTester(ast.AggFuncMin, mysql.TypeEnum, 5, enumA, enumC, enumA),
		buildAggTester(ast.AggFuncMin, mysql.TypeSet, 5, setA, setAB, setA),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestMaxMin(c *C) {
	unsignedType := types.NewFieldType(mysql.TypeLonglong)
	unsignedType.Flag |= mysql.UnsignedFlag
	tests := []aggTest{
		buildAggTester(ast.AggFuncMax, mysql.TypeLonglong, 5, nil, 4),
		buildAggTesterWithFieldType(ast.AggFuncMax, unsignedType, 5, nil, 4),
		buildAggTester(ast.AggFuncMax, mysql.TypeFloat, 5, nil, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeDouble, 5, nil, 4.0),
		buildAggTester(ast.AggFuncMax, mysql.TypeNewDecimal, 5, nil, types.NewDecFromInt(4)),
		buildAggTester(ast.AggFuncMax, mysql.TypeString, 5, nil, "4", "4"),
		buildAggTester(ast.AggFuncMax, mysql.TypeDate, 5, nil, types.TimeFromDays(369)),
		buildAggTester(ast.AggFuncMax, mysql.TypeDuration, 5, nil, types.Duration{Duration: time.Duration(4)}),
		buildAggTester(ast.AggFuncMax, mysql.TypeJSON, 5, nil, json.CreateBinary(int64(4))),

		buildAggTester(ast.AggFuncMin, mysql.TypeLonglong, 5, nil, 0),
		buildAggTesterWithFieldType(ast.AggFuncMin, unsignedType, 5, nil, 0),
		buildAggTester(ast.AggFuncMin, mysql.TypeFloat, 5, nil, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeDouble, 5, nil, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeNewDecimal, 5, nil, types.NewDecFromInt(0)),
		buildAggTester(ast.AggFuncMin, mysql.TypeString, 5, nil, "0"),
		buildAggTester(ast.AggFuncMin, mysql.TypeDate, 5, nil, types.TimeFromDays(365)),
		buildAggTester(ast.AggFuncMin, mysql.TypeDuration, 5, nil, types.Duration{Duration: time.Duration(0)}),
		buildAggTester(ast.AggFuncMin, mysql.TypeJSON, 5, nil, json.CreateBinary(int64(0))),
	}
	for _, test := range tests {
		s.testAggFunc(c, test)
	}
}
