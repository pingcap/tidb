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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggfuncs_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
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
		switch dataType.GetType() {
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
			} else if isMax && curVal.Name > preEnumVal.Name || !isMax && curVal.Name < preEnumVal.Name {
				memDeltas[i] = int64(len(curVal.Name)) - int64(len(preEnumVal.Name))
				preEnumVal = curVal
			}
		case mysql.TypeSet:
			curVal := row.GetSet(0)
			if i == 0 {
				memDeltas[i] = int64(len(curVal.Name))
				preSetVal = curVal
			} else if isMax && curVal.Name > preSetVal.Name || !isMax && curVal.Name < preSetVal.Name {
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

func TestMergePartialResult4MaxMin(t *testing.T) {
	elems := []string{"e", "d", "c", "b", "a"}
	enumA, _ := types.ParseEnum(elems, "a", mysql.DefaultCollationName)
	enumC, _ := types.ParseEnum(elems, "c", mysql.DefaultCollationName)
	enumE, _ := types.ParseEnum(elems, "e", mysql.DefaultCollationName)

	setC, _ := types.ParseSet(elems, "c", mysql.DefaultCollationName)    // setC.Value == 4
	setED, _ := types.ParseSet(elems, "e,d", mysql.DefaultCollationName) // setED.Value == 3

	unsignedType := types.NewFieldType(mysql.TypeLonglong)
	unsignedType.AddFlag(mysql.UnsignedFlag)
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
		buildAggTester(ast.AggFuncMax, mysql.TypeEnum, 5, enumE, enumC, enumE),
		buildAggTester(ast.AggFuncMax, mysql.TypeSet, 5, setED, setED, setED),

		buildAggTester(ast.AggFuncMin, mysql.TypeLonglong, 5, 0, 2, 0),
		buildAggTesterWithFieldType(ast.AggFuncMin, unsignedType, 5, 0, 2, 0),
		buildAggTester(ast.AggFuncMin, mysql.TypeFloat, 5, 0.0, 2.0, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeDouble, 5, 0.0, 2.0, 0.0),
		buildAggTester(ast.AggFuncMin, mysql.TypeNewDecimal, 5, types.NewDecFromInt(0), types.NewDecFromInt(2), types.NewDecFromInt(0)),
		buildAggTester(ast.AggFuncMin, mysql.TypeString, 5, "0", "2", "0"),
		buildAggTester(ast.AggFuncMin, mysql.TypeDate, 5, types.TimeFromDays(365), types.TimeFromDays(367), types.TimeFromDays(365)),
		buildAggTester(ast.AggFuncMin, mysql.TypeDuration, 5, types.Duration{Duration: time.Duration(0)}, types.Duration{Duration: time.Duration(2)}, types.Duration{Duration: time.Duration(0)}),
		buildAggTester(ast.AggFuncMin, mysql.TypeJSON, 5, json.CreateBinary(int64(0)), json.CreateBinary(int64(2)), json.CreateBinary(int64(0))),
		buildAggTester(ast.AggFuncMin, mysql.TypeEnum, 5, enumA, enumA, enumA),
		buildAggTester(ast.AggFuncMin, mysql.TypeSet, 5, setC, setC, setC),
	}
	for _, test := range tests {
		test := test
		t.Run(test.funcName, func(t *testing.T) {
			testMergePartialResult(t, test)
		})
	}
}

func TestMaxMin(t *testing.T) {
	unsignedType := types.NewFieldType(mysql.TypeLonglong)
	unsignedType.AddFlag(mysql.UnsignedFlag)
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
		test := test
		t.Run(test.funcName, func(t *testing.T) {
			testAggFunc(t, test)
		})
	}
}

func TestMemMaxMin(t *testing.T) {
	tests := []aggMemTest{
		buildAggMemTester(ast.AggFuncMax, mysql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4MaxMinIntSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMax, mysql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4MaxMinUintSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMax, mysql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4MaxMinDecimalSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMax, mysql.TypeFloat, 5,
			aggfuncs.DefPartialResult4MaxMinFloat32Size, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMax, mysql.TypeDouble, 5,
			aggfuncs.DefPartialResult4MaxMinFloat64Size, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMax, mysql.TypeDate, 5,
			aggfuncs.DefPartialResult4MaxMinTimeSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMax, mysql.TypeDuration, 5,
			aggfuncs.DefPartialResult4MaxMinDurationSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMax, mysql.TypeString, 99,
			aggfuncs.DefPartialResult4MaxMinStringSize, maxUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMax, mysql.TypeJSON, 99,
			aggfuncs.DefPartialResult4MaxMinJSONSize, maxUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMax, mysql.TypeEnum, 99,
			aggfuncs.DefPartialResult4MaxMinEnumSize, maxUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMax, mysql.TypeSet, 99,
			aggfuncs.DefPartialResult4MaxMinSetSize, maxUpdateMemDeltaGens, false),

		buildAggMemTester(ast.AggFuncMin, mysql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4MaxMinIntSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMin, mysql.TypeLonglong, 5,
			aggfuncs.DefPartialResult4MaxMinUintSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMin, mysql.TypeNewDecimal, 5,
			aggfuncs.DefPartialResult4MaxMinDecimalSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMin, mysql.TypeFloat, 5,
			aggfuncs.DefPartialResult4MaxMinFloat32Size, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMin, mysql.TypeDouble, 5,
			aggfuncs.DefPartialResult4MaxMinFloat64Size, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMin, mysql.TypeDate, 5,
			aggfuncs.DefPartialResult4MaxMinTimeSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMin, mysql.TypeDuration, 5,
			aggfuncs.DefPartialResult4MaxMinDurationSize, defaultUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMin, mysql.TypeString, 99,
			aggfuncs.DefPartialResult4MaxMinStringSize, minUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMin, mysql.TypeJSON, 99,
			aggfuncs.DefPartialResult4MaxMinJSONSize, minUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMin, mysql.TypeEnum, 99,
			aggfuncs.DefPartialResult4MaxMinEnumSize, minUpdateMemDeltaGens, false),
		buildAggMemTester(ast.AggFuncMin, mysql.TypeSet, 99,
			aggfuncs.DefPartialResult4MaxMinSetSize, minUpdateMemDeltaGens, false),
	}
	for _, test := range tests {
		test := test
		t.Run(test.aggTest.funcName, func(t *testing.T) {
			testAggMemFunc(t, test)
		})
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

func TestMaxSlidingWindow(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	testCases := []maxSlidingWindowTestCase{
		{
			rowType:       "bigint",
			insertValue:   "(1), (3), (2)",
			expect:        []string{"3", "3", "3"},
			orderByExpect: []string{"1", "2", "3"},
		},
		{
			rowType:       "int unsigned",
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
				t.Run(fmt.Sprintf("%s_%v_%d", tc.rowType, o, f), func(t *testing.T) {
					tc.frameType = f
					tc.orderBy = o
					tk.MustExec("drop table if exists t;")
					testMaxSlidingWindow(tk, tc)
				})
			}
		}
	}
}

func TestDequeReset(t *testing.T) {
	deque := aggfuncs.NewDeque(true, func(i, j interface{}) int {
		return types.CompareInt64(i.(int64), j.(int64))
	})
	deque.PushBack(0, 12)
	deque.Reset()
	require.Len(t, deque.Items, 0)
	require.True(t, deque.IsMax)
}

func TestDequePushPop(t *testing.T) {
	deque := aggfuncs.NewDeque(true, func(i, j interface{}) int {
		return types.CompareInt64(i.(int64), j.(int64))
	})
	times := 15
	// pushes element from back of deque
	for i := 0; i < times; i++ {
		if i != 0 {
			front, isEnd := deque.Front()
			require.False(t, isEnd)
			require.Zero(t, front.Item)
			require.Zero(t, front.Idx)
		}
		deque.PushBack(uint64(i), i)
		back, isEnd := deque.Back()
		require.False(t, isEnd)
		require.Equal(t, back.Item, i)
		require.Equal(t, back.Idx, uint64(i))
	}

	// pops element from back of deque
	for i := 0; i < times; i++ {
		pair, isEnd := deque.Back()
		require.False(t, isEnd)
		require.Equal(t, pair.Item, times-i-1)
		require.Equal(t, pair.Idx, uint64(times-i-1))
		front, isEnd := deque.Front()
		require.False(t, isEnd)
		require.Zero(t, front.Item)
		require.Zero(t, front.Idx)
		err := deque.PopBack()
		require.NoError(t, err)
	}
}
