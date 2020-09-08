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
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/testkit"
)

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

type maxSlidingWindowTestCase struct {
	rowType       string
	insertValue   string
	expect        []string
	orderByExpect []string
	orderBy       bool
	frameType     ast.FrameType
}

func testMaxSlidingWindow(tk *testkit.TestKit, tc maxSlidingWindowTestCase) {
	tk.MustExec(fmt.Sprintf("CREATE TABLE t (a %s);", tc.rowType))
	tk.MustExec(fmt.Sprintf("insert into t values %s;", tc.insertValue))
	var orderBy string
	if tc.orderBy {
		orderBy = "ORDER BY a"
	}
	var result *testkit.Result
	switch tc.frameType {
	case ast.Rows:
		result = tk.MustQuery(fmt.Sprintf("SELECT max(a) OVER (%s ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;", orderBy))
	case ast.Ranges:
		result = tk.MustQuery(fmt.Sprintf("SELECT max(a) OVER (%s RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t;", orderBy))
	default:
		result = tk.MustQuery(fmt.Sprintf("SELECT max(a) OVER (%s) FROM t;", orderBy))
		if tc.orderBy {
			result.Check(testkit.Rows(tc.orderByExpect...))
			return
		}
	}
	result.Check(testkit.Rows(tc.expect...))
}

func (s *testSuite) TestMaxSlidingWindow(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	testCases := []maxSlidingWindowTestCase{
		{
			rowType:       "bigint",
			insertValue:   "(1), (3), (2)",
			expect:        []string{"3", "3", "3"},
			orderByExpect: []string{"1", "2", "3"},
		},
		{
			rowType:       "float",
			insertValue:   "(1.1), (3.3), (2.2)",
			expect:        []string{"3.3", "3.3", "3.3"},
			orderByExpect: []string{"1.1", "2.2", "3.3"},
		},
		{
			rowType:       "double",
			insertValue:   "(1.1), (3.3), (2.2)",
			expect:        []string{"3.3", "3.3", "3.3"},
			orderByExpect: []string{"1.1", "2.2", "3.3"},
		},
		{
			rowType:       "decimal(5, 2)",
			insertValue:   "(1.1), (3.3), (2.2)",
			expect:        []string{"3.30", "3.30", "3.30"},
			orderByExpect: []string{"1.10", "2.20", "3.30"},
		},
		{
			rowType:       "text",
			insertValue:   "('1.1'), ('3.3'), ('2.2')",
			expect:        []string{"3.3", "3.3", "3.3"},
			orderByExpect: []string{"1.1", "2.2", "3.3"},
		},
		{
			rowType:       "time",
			insertValue:   "('00:00:00'), ('03:00:00'), ('02:00:00')",
			expect:        []string{"03:00:00", "03:00:00", "03:00:00"},
			orderByExpect: []string{"00:00:00", "02:00:00", "03:00:00"},
		},
		{
			rowType:       "date",
			insertValue:   "('2020-09-08'), ('2022-09-10'), ('2020-09-10')",
			expect:        []string{"2022-09-10", "2022-09-10", "2022-09-10"},
			orderByExpect: []string{"2020-09-08", "2020-09-10", "2022-09-10"},
		},
		{
			rowType:       "datetime",
			insertValue:   "('2020-09-08 02:00:00'), ('2022-09-10 00:00:00'), ('2020-09-10 00:00:00')",
			expect:        []string{"2022-09-10 00:00:00", "2022-09-10 00:00:00", "2022-09-10 00:00:00"},
			orderByExpect: []string{"2020-09-08 02:00:00", "2020-09-10 00:00:00", "2022-09-10 00:00:00"},
		},
	}

	orderBy := []bool{false, true}
	frameType := []ast.FrameType{ast.Rows, ast.Ranges, -1}
	for _, o := range orderBy {
		for _, f := range frameType {
			for _, tc := range testCases {
				tc.frameType = f
				tc.orderBy = o
				tk.MustExec("drop table if exists t;")
				testMaxSlidingWindow(tk, tc)
			}
		}
	}
}
