// Copyright 2017 PingCAP, Inc.
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

package expression

import (
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestCompareFunctionWithRefine(t *testing.T) {
	ctx := createContext(t)

	tblInfo := newTestTableBuilder("").add("a", mysql.TypeLong, mysql.NotNullFlag).build()
	tests := []struct {
		exprStr string
		result  string
	}{
		{"a < '1.0'", "lt(a, 1)"},
		{"a <= '1.0'", "le(a, 1)"},
		{"a > '1'", "gt(a, 1)"},
		{"a >= '1'", "ge(a, 1)"},
		{"a = '1'", "eq(a, 1)"},
		{"a <=> '1'", "nulleq(a, 1)"},
		{"a != '1'", "ne(a, 1)"},
		{"a < '1.1'", "lt(a, 2)"},
		{"a <= '1.1'", "le(a, 1)"},
		{"a > 1.1", "gt(a, 1)"},
		{"a >= '1.1'", "ge(a, 2)"},
		{"a = '1.1'", "0"},
		{"a <=> '1.1'", "0"},
		{"a != '1.1'", "ne(cast(a, double BINARY), 1.1)"},
		{"'1' < a", "lt(1, a)"},
		{"'1' <= a", "le(1, a)"},
		{"'1' > a", "gt(1, a)"},
		{"'1' >= a", "ge(1, a)"},
		{"'1' = a", "eq(1, a)"},
		{"'1' <=> a", "nulleq(1, a)"},
		{"'1' != a", "ne(1, a)"},
		{"'1.1' < a", "lt(1, a)"},
		{"'1.1' <= a", "le(2, a)"},
		{"'1.1' > a", "gt(2, a)"},
		{"'1.1' >= a", "ge(1, a)"},
		{"'1.1' = a", "0"},
		{"'1.1' <=> a", "0"},
		{"'1.1' != a", "ne(1.1, cast(a, double BINARY))"},
		{"'123456789123456711111189' = a", "0"},
		{"123456789123456789.12345 = a", "0"},
		{"123456789123456789123456789.12345 > a", "1"},
		{"-123456789123456789123456789.12345 > a", "0"},
		{"123456789123456789123456789.12345 < a", "0"},
		{"-123456789123456789123456789.12345 < a", "1"},
		{"'aaaa'=a", "eq(0, a)"},
	}
	cols, names, err := ColumnInfos2ColumnsAndNames(ctx, model.NewCIStr(""), tblInfo.Name, tblInfo.Cols(), tblInfo)
	require.NoError(t, err)
	schema := NewSchema(cols...)
	for _, test := range tests {
		f, err := ParseSimpleExprsWithNames(ctx, test.exprStr, schema, names)
		require.NoError(t, err)
		require.Equal(t, test.result, f[0].String())
	}
}

func TestCompare(t *testing.T) {
	ctx := createContext(t)

	intVal, uintVal, realVal, stringVal, decimalVal := 1, uint64(1), 1.1, "123", types.NewDecFromFloatForTest(123.123)
	timeVal := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeDatetime, 6)
	durationVal := types.Duration{Duration: 12*time.Hour + 1*time.Minute + 1*time.Second}
	jsonVal := json.CreateBinary("123")
	// test cases for generating function signatures.
	tests := []struct {
		arg0     interface{}
		arg1     interface{}
		funcName string
		tp       byte
		expected int64
	}{
		{intVal, intVal, ast.LT, mysql.TypeLonglong, 0},
		{stringVal, stringVal, ast.LT, mysql.TypeVarString, 0},
		{intVal, decimalVal, ast.LT, mysql.TypeNewDecimal, 1},
		{realVal, decimalVal, ast.LT, mysql.TypeDouble, 1},
		{durationVal, durationVal, ast.LT, mysql.TypeDuration, 0},
		{realVal, realVal, ast.LT, mysql.TypeDouble, 0},
		{intVal, intVal, ast.NullEQ, mysql.TypeLonglong, 1},
		{decimalVal, decimalVal, ast.LE, mysql.TypeNewDecimal, 1},
		{decimalVal, decimalVal, ast.GT, mysql.TypeNewDecimal, 0},
		{decimalVal, decimalVal, ast.GE, mysql.TypeNewDecimal, 1},
		{decimalVal, decimalVal, ast.NE, mysql.TypeNewDecimal, 0},
		{decimalVal, decimalVal, ast.EQ, mysql.TypeNewDecimal, 1},
		{decimalVal, decimalVal, ast.NullEQ, mysql.TypeNewDecimal, 1},
		{durationVal, durationVal, ast.LE, mysql.TypeDuration, 1},
		{durationVal, durationVal, ast.GT, mysql.TypeDuration, 0},
		{durationVal, durationVal, ast.GE, mysql.TypeDuration, 1},
		{durationVal, durationVal, ast.EQ, mysql.TypeDuration, 1},
		{durationVal, durationVal, ast.NE, mysql.TypeDuration, 0},
		{durationVal, durationVal, ast.NullEQ, mysql.TypeDuration, 1},
		{nil, nil, ast.NullEQ, mysql.TypeNull, 1},
		{nil, intVal, ast.NullEQ, mysql.TypeDouble, 0},
		{uintVal, intVal, ast.NullEQ, mysql.TypeLonglong, 1},
		{uintVal, intVal, ast.EQ, mysql.TypeLonglong, 1},
		{intVal, uintVal, ast.NullEQ, mysql.TypeLonglong, 1},
		{intVal, uintVal, ast.EQ, mysql.TypeLonglong, 1},
		{timeVal, timeVal, ast.LT, mysql.TypeDatetime, 0},
		{timeVal, timeVal, ast.LE, mysql.TypeDatetime, 1},
		{timeVal, timeVal, ast.GT, mysql.TypeDatetime, 0},
		{timeVal, timeVal, ast.GE, mysql.TypeDatetime, 1},
		{timeVal, timeVal, ast.EQ, mysql.TypeDatetime, 1},
		{timeVal, timeVal, ast.NE, mysql.TypeDatetime, 0},
		{timeVal, timeVal, ast.NullEQ, mysql.TypeDatetime, 1},
		{jsonVal, jsonVal, ast.LT, mysql.TypeJSON, 0},
		{jsonVal, jsonVal, ast.LE, mysql.TypeJSON, 1},
		{jsonVal, jsonVal, ast.GT, mysql.TypeJSON, 0},
		{jsonVal, jsonVal, ast.GE, mysql.TypeJSON, 1},
		{jsonVal, jsonVal, ast.NE, mysql.TypeJSON, 0},
		{jsonVal, jsonVal, ast.EQ, mysql.TypeJSON, 1},
		{jsonVal, jsonVal, ast.NullEQ, mysql.TypeJSON, 1},
	}

	for _, test := range tests {
		bf, err := funcs[test.funcName].getFunction(ctx, primitiveValsToConstants(ctx, []interface{}{test.arg0, test.arg1}))
		require.NoError(t, err)
		args := bf.getArgs()
		require.Equal(t, test.tp, args[0].GetType().GetType())
		require.Equal(t, test.tp, args[1].GetType().GetType())
		res, isNil, err := bf.evalInt(chunk.Row{})
		require.NoError(t, err)
		require.False(t, isNil)
		require.Equal(t, test.expected, res)
	}

	// test <non-const decimal expression> <cmp> <const string expression>
	decimalCol, stringCon := &Column{RetType: types.NewFieldType(mysql.TypeNewDecimal)}, &Constant{RetType: types.NewFieldType(mysql.TypeVarchar)}
	bf, err := funcs[ast.LT].getFunction(ctx, []Expression{decimalCol, stringCon})
	require.NoError(t, err)
	args := bf.getArgs()
	require.Equal(t, mysql.TypeNewDecimal, args[0].GetType().GetType())
	require.Equal(t, mysql.TypeNewDecimal, args[1].GetType().GetType())

	// test <time column> <cmp> <non-time const>
	timeCol := &Column{RetType: types.NewFieldType(mysql.TypeDatetime)}
	bf, err = funcs[ast.LT].getFunction(ctx, []Expression{timeCol, stringCon})
	require.NoError(t, err)
	args = bf.getArgs()
	require.Equal(t, mysql.TypeDatetime, args[0].GetType().GetType())
	require.Equal(t, mysql.TypeDatetime, args[1].GetType().GetType())
}

func TestCoalesce(t *testing.T) {
	ctx := createContext(t)

	cases := []struct {
		args     []interface{}
		expected interface{}
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{nil}, nil, true, false},
		{[]interface{}{nil, nil}, nil, true, false},
		{[]interface{}{nil, nil, nil}, nil, true, false},
		{[]interface{}{nil, 1}, int64(1), false, false},
		{[]interface{}{nil, 1.1}, 1.1, false, false},
		{[]interface{}{1, 1.1}, float64(1), false, false},
		{[]interface{}{nil, types.NewDecFromFloatForTest(123.456)}, types.NewDecFromFloatForTest(123.456), false, false},
		{[]interface{}{1, types.NewDecFromFloatForTest(123.456)}, types.NewDecFromInt(1), false, false},
		{[]interface{}{nil, duration}, duration, false, false},
		{[]interface{}{nil, tm, nil}, tm, false, false},
		{[]interface{}{nil, dt, nil}, dt, false, false},
		{[]interface{}{tm, dt}, tm, false, false},
	}

	for _, test := range cases {
		f, err := newFunctionForTest(ctx, ast.Coalesce, primitiveValsToConstants(ctx, test.args)...)
		require.NoError(t, err)

		d, err := f.Eval(chunk.Row{})

		if test.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if test.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, test.expected, d.GetValue())
			}
		}
	}

	_, err := funcs[ast.Length].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestIntervalFunc(t *testing.T) {
	ctx := createContext(t)

	sc := ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	sc.IgnoreTruncate = true
	defer func() {
		sc.IgnoreTruncate = origin
	}()

	for _, test := range []struct {
		args   []types.Datum
		ret    int64
		getErr bool
	}{
		{types.MakeDatums(nil, 1, 2), -1, false},
		{types.MakeDatums(1, 2, 3), 0, false},
		{types.MakeDatums(2, 1, 3), 1, false},
		{types.MakeDatums(3, 1, 2), 2, false},
		{types.MakeDatums(0, "b", "1", "2"), 1, false},
		{types.MakeDatums("a", "b", "1", "2"), 1, false},
		{types.MakeDatums(23, 1, 23, 23, 23, 30, 44, 200), 4, false},
		{types.MakeDatums(23, 1.7, 15.3, 23.1, 30, 44, 200), 2, false},
		{types.MakeDatums(9007199254740992, 9007199254740993), 0, false},
		{types.MakeDatums(uint64(9223372036854775808), uint64(9223372036854775809)), 0, false},
		{types.MakeDatums(9223372036854775807, uint64(9223372036854775808)), 0, false},
		{types.MakeDatums(-9223372036854775807, uint64(9223372036854775808)), 0, false},
		{types.MakeDatums(uint64(9223372036854775806), 9223372036854775807), 0, false},
		{types.MakeDatums(uint64(9223372036854775806), -9223372036854775807), 1, false},
		{types.MakeDatums("9007199254740991", "9007199254740992"), 0, false},
		{types.MakeDatums(1, uint32(1), uint32(1)), 0, true},
		{types.MakeDatums(-1, 2333, nil), 0, false},
		{types.MakeDatums(1, nil, nil, nil), 3, false},
		{types.MakeDatums(1, nil, nil, nil, 2), 3, false},
		{types.MakeDatums(uint64(9223372036854775808), nil, nil, nil, 4), 4, false},

		// tests for appropriate precision loss
		{types.MakeDatums(9007199254740992, "9007199254740993"), 1, false},
		{types.MakeDatums("9007199254740992", 9007199254740993), 1, false},
		{types.MakeDatums("9007199254740992", "9007199254740993"), 1, false},
	} {
		fc := funcs[ast.Interval]
		f, err := fc.getFunction(ctx, datumsToConstants(test.args))
		require.NoError(t, err)
		if test.getErr {
			v, err := evalBuiltinFunc(f, chunk.Row{})
			require.Error(t, err)
			require.Equal(t, test.ret, v.GetInt64())
			continue
		}
		v, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, test.ret, v.GetInt64())
	}
}

// greatest/least function is compatible with MySQL 8.0
func TestGreatestLeastFunc(t *testing.T) {
	ctx := createContext(t)
	sc := ctx.GetSessionVars().StmtCtx
	originIgnoreTruncate := sc.IgnoreTruncate
	sc.IgnoreTruncate = true
	decG := &types.MyDecimal{}
	decL := &types.MyDecimal{}
	defer func() {
		sc.IgnoreTruncate = originIgnoreTruncate
	}()

	for _, test := range []struct {
		args             []interface{}
		expectedGreatest interface{}
		expectedLeast    interface{}
		isNil            bool
		getErr           bool
	}{
		{
			[]interface{}{int64(-9223372036854775808), uint64(9223372036854775809)},
			decG.FromUint(9223372036854775809), decL.FromInt(-9223372036854775808), false, false,
		},
		{
			[]interface{}{uint64(9223372036854775808), uint64(9223372036854775809)},
			uint64(9223372036854775809), uint64(9223372036854775808), false, false,
		},
		{
			[]interface{}{1, 2, 3, 4},
			int64(4), int64(1), false, false,
		},
		{
			[]interface{}{"a", "b", "c"},
			"c", "a", false, false,
		},
		{
			[]interface{}{"123a", "b", "c", 12},
			"c", "12", false, false,
		},
		{
			[]interface{}{tm, "123"},
			curTimeString, "123", false, false,
		},
		{
			[]interface{}{tm, 123},
			curTimeString, "123", false, false,
		},
		{
			[]interface{}{tm, "invalid_time_1", "invalid_time_2", tmWithFsp},
			"invalid_time_2", curTimeString, false, false,
		},
		{
			[]interface{}{tm, "invalid_time_2", "invalid_time_1", tmWithFsp},
			"invalid_time_2", curTimeString, false, false,
		},
		{
			[]interface{}{tm, "invalid_time", nil, tmWithFsp},
			nil, nil, true, false,
		},
		{
			[]interface{}{duration, "123"},
			"12:59:59", "123", false, false,
		},
		{
			[]interface{}{duration, duration},
			duration, duration, false, false,
		},
		{
			[]interface{}{"123", nil, "123"},
			nil, nil, true, false,
		},
		{
			[]interface{}{errors.New("must error"), 123},
			nil, nil, false, true,
		},
		{
			[]interface{}{794755072.0, 4556, "2000-01-09"},
			"794755072", "2000-01-09", false, false,
		},
		{
			[]interface{}{905969664.0, 4556, "1990-06-16 17:22:56.005534"},
			"905969664", "1990-06-16 17:22:56.005534", false, false,
		},
		{
			[]interface{}{105969664.0, 120000, types.Duration{Duration: 20*time.Hour + 0*time.Minute + 0*time.Second}},
			"20:00:00", "105969664", false, false,
		},
	} {
		f0, err := newFunctionForTest(ctx, ast.Greatest, primitiveValsToConstants(ctx, test.args)...)
		require.NoError(t, err)
		d, err := f0.Eval(chunk.Row{})
		if test.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if test.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, test.expectedGreatest, d.GetValue())
			}
		}

		f1, err := newFunctionForTest(ctx, ast.Least, primitiveValsToConstants(ctx, test.args)...)
		require.NoError(t, err)
		d, err = f1.Eval(chunk.Row{})
		if test.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if test.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, test.expectedLeast, d.GetValue())
			}
		}
	}
	_, err := funcs[ast.Greatest].getFunction(ctx, []Expression{NewZero(), NewOne()})
	require.NoError(t, err)
	_, err = funcs[ast.Least].getFunction(ctx, []Expression{NewZero(), NewOne()})
	require.NoError(t, err)
}
