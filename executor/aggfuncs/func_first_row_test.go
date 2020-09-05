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
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs_test

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
)

func (s *testSuite) TestMergePartialResult4FirstRow(c *C) {
	elems := []string{"a", "b", "c", "d", "e"}
	enumA, _ := types.ParseEnumName(elems, "a", mysql.DefaultCollationName)
	enumC, _ := types.ParseEnumName(elems, "c", mysql.DefaultCollationName)

	setA, _ := types.ParseSetName(elems, "a", mysql.DefaultCollationName)
	setAB, _ := types.ParseSetName(elems, "a,b", mysql.DefaultCollationName)

	tests := []aggTest{
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeLonglong, 5, 0, 2, 0),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeFloat, 5, 0.0, 2.0, 0.0),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeDouble, 5, 0.0, 2.0, 0.0),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeNewDecimal, 5, types.NewDecFromInt(0), types.NewDecFromInt(2), types.NewDecFromInt(0)),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeString, 5, "0", "2", "0"),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeDate, 5, types.TimeFromDays(365), types.TimeFromDays(367), types.TimeFromDays(365)),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeDuration, 5, types.Duration{Duration: time.Duration(0)}, types.Duration{Duration: time.Duration(2)}, types.Duration{Duration: time.Duration(0)}),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeJSON, 5, json.CreateBinary(int64(0)), json.CreateBinary(int64(2)), json.CreateBinary(int64(0))),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeEnum, 5, enumA, enumC, enumA),
		buildAggTester(ast.AggFuncFirstRow, mysql.TypeSet, 5, setA, setAB, setA),
	}
	for _, test := range tests {
		s.testMergePartialResult(c, test)
	}
}

func (s *testSuite) TestMemFirstRow(c *C) {
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
			aggfuncs.DefPartialResult4FirstRowStringSize, FirstRowUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncFirstRow, mysql.TypeDate, 5,
			aggfuncs.DefPartialResult4FirstRowTimeSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncFirstRow, mysql.TypeDuration, 5,
			aggfuncs.DefPartialResult4FirstRowDurationSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncFirstRow, mysql.TypeJSON, 5,
			aggfuncs.DefPartialResult4FirstRowJSONSize, FirstRowUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncFirstRow, mysql.TypeEnum, 5,
			aggfuncs.DefPartialResult4FirstRowEnumSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncFirstRow, mysql.TypeSet, 5,
			aggfuncs.DefPartialResult4FirstRowSetSize, defaultUpdateMemDeltaGens, false),
	}
	for _, test := range tests {
		s.testAggMemFunc(c, test)
	}
}
