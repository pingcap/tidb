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
	"math"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/testkit/trequire"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestUnary(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args     interface{}
		expected interface{}
		overflow bool
		getErr   bool
	}{
		{uint64(9223372036854775809), "-9223372036854775809", true, false},
		{uint64(9223372036854775810), "-9223372036854775810", true, false},
		{uint64(9223372036854775808), int64(-9223372036854775808), false, false},
		{int64(math.MinInt64), "9223372036854775808", true, false}, // --9223372036854775808
	}
	sc := ctx.GetSessionVars().StmtCtx
	origin := sc.InSelectStmt
	sc.InSelectStmt = true
	defer func() {
		sc.InSelectStmt = origin
	}()

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.UnaryMinus, primitiveValsToConstants(ctx, []interface{}{c.args})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		if !c.getErr {
			require.NoError(t, err)
			if !c.overflow {
				require.Equal(t, c.expected, d.GetValue())
			} else {
				require.Equal(t, c.expected, d.GetMysqlDecimal().String())
			}
		} else {
			require.Error(t, err)
		}
	}

	_, err := funcs[ast.UnaryMinus].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestLogicAnd(t *testing.T) {
	ctx := createContext(t)
	sc := ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []interface{}
		expected int64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{1, 1}, 1, false, false},
		{[]interface{}{1, 0}, 0, false, false},
		{[]interface{}{0, 1}, 0, false, false},
		{[]interface{}{0, 0}, 0, false, false},
		{[]interface{}{2, -1}, 1, false, false},
		{[]interface{}{"a", "0"}, 0, false, false},
		{[]interface{}{"a", "1"}, 0, false, false},
		{[]interface{}{"1a", "0"}, 0, false, false},
		{[]interface{}{"1a", "1"}, 1, false, false},
		{[]interface{}{0, nil}, 0, false, false},
		{[]interface{}{nil, 0}, 0, false, false},
		{[]interface{}{nil, 1}, 0, true, false},
		{[]interface{}{0.001, 0}, 0, false, false},
		{[]interface{}{0.001, 1}, 1, false, false},
		{[]interface{}{nil, 0.000}, 0, false, false},
		{[]interface{}{nil, 0.001}, 0, true, false},
		{[]interface{}{types.NewDecFromStringForTest("0.000001"), 0}, 0, false, false},
		{[]interface{}{types.NewDecFromStringForTest("0.000001"), 1}, 1, false, false},
		{[]interface{}{types.NewDecFromStringForTest("0.000000"), nil}, 0, false, false},
		{[]interface{}{types.NewDecFromStringForTest("0.000001"), nil}, 0, true, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.LogicAnd, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetInt64())
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(ctx, ast.LogicAnd, NewZero())
	require.Error(t, err)

	_, err = funcs[ast.LogicAnd].getFunction(ctx, []Expression{NewZero(), NewZero()})
	require.NoError(t, err)
}

func TestLeftShift(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args     []interface{}
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{123, 2}, uint64(492), false, false},
		{[]interface{}{-123, 2}, uint64(18446744073709551124), false, false},
		{[]interface{}{nil, 1}, 0, true, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.LeftShift, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetUint64())
			}
		}
	}
}

func TestRightShift(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args     []interface{}
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{123, 2}, uint64(30), false, false},
		{[]interface{}{-123, 2}, uint64(4611686018427387873), false, false},
		{[]interface{}{nil, 1}, 0, true, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.RightShift, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetUint64())
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(ctx, ast.RightShift, NewZero())
	require.Error(t, err)

	_, err = funcs[ast.RightShift].getFunction(ctx, []Expression{NewZero(), NewZero()})
	require.NoError(t, err)
}

func TestBitXor(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args     []interface{}
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{123, 321}, uint64(314), false, false},
		{[]interface{}{-123, 321}, uint64(18446744073709551300), false, false},
		{[]interface{}{nil, 1}, 0, true, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Xor, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetUint64())
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(ctx, ast.Xor, NewZero())
	require.Error(t, err)

	_, err = funcs[ast.Xor].getFunction(ctx, []Expression{NewZero(), NewZero()})
	require.NoError(t, err)
}

func TestBitOr(t *testing.T) {
	ctx := createContext(t)
	sc := ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []interface{}
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{123, 321}, uint64(379), false, false},
		{[]interface{}{-123, 321}, uint64(18446744073709551557), false, false},
		{[]interface{}{nil, 1}, 0, true, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Or, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetUint64())
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(ctx, ast.Or, NewZero())
	require.Error(t, err)

	_, err = funcs[ast.Or].getFunction(ctx, []Expression{NewZero(), NewZero()})
	require.NoError(t, err)
}

func TestLogicOr(t *testing.T) {
	ctx := createContext(t)
	sc := ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []interface{}
		expected int64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{1, 1}, 1, false, false},
		{[]interface{}{1, 0}, 1, false, false},
		{[]interface{}{0, 1}, 1, false, false},
		{[]interface{}{0, 0}, 0, false, false},
		{[]interface{}{2, -1}, 1, false, false},
		{[]interface{}{"a", "0"}, 0, false, false},
		{[]interface{}{"a", "1"}, 1, false, false},
		{[]interface{}{"1a", "0"}, 1, false, false},
		{[]interface{}{"1a", "1"}, 1, false, false},
		{[]interface{}{"0.0a", 0}, 0, false, false},
		{[]interface{}{"0.0001a", 0}, 1, false, false},
		{[]interface{}{1, nil}, 1, false, false},
		{[]interface{}{nil, 1}, 1, false, false},
		{[]interface{}{nil, 0}, 0, true, false},
		{[]interface{}{0.000, 0}, 0, false, false},
		{[]interface{}{0.001, 0}, 1, false, false},
		{[]interface{}{nil, 0.000}, 0, true, false},
		{[]interface{}{nil, 0.001}, 1, false, false},
		{[]interface{}{types.NewDecFromStringForTest("0.000000"), 0}, 0, false, false},
		{[]interface{}{types.NewDecFromStringForTest("0.000000"), 1}, 1, false, false},
		{[]interface{}{types.NewDecFromStringForTest("0.000000"), nil}, 0, true, false},
		{[]interface{}{types.NewDecFromStringForTest("0.000001"), 0}, 1, false, false},
		{[]interface{}{types.NewDecFromStringForTest("0.000001"), 1}, 1, false, false},
		{[]interface{}{types.NewDecFromStringForTest("0.000001"), nil}, 1, false, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.LogicOr, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetInt64())
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(ctx, ast.LogicOr, NewZero())
	require.Error(t, err)

	_, err = funcs[ast.LogicOr].getFunction(ctx, []Expression{NewZero(), NewZero()})
	require.NoError(t, err)
}

func TestBitAnd(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args     []interface{}
		expected int64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{123, 321}, 65, false, false},
		{[]interface{}{-123, 321}, 257, false, false},
		{[]interface{}{nil, 1}, 0, true, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.And, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetInt64())
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(ctx, ast.And, NewZero())
	require.Error(t, err)

	_, err = funcs[ast.And].getFunction(ctx, []Expression{NewZero(), NewZero()})
	require.NoError(t, err)
}

func TestBitNeg(t *testing.T) {
	ctx := createContext(t)
	sc := ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []interface{}
		expected uint64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{123}, uint64(18446744073709551492), false, false},
		{[]interface{}{-123}, uint64(122), false, false},
		{[]interface{}{nil}, 0, true, false},

		{[]interface{}{errors.New("must error")}, 0, false, true},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.BitNeg, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetUint64())
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(ctx, ast.BitNeg, NewZero(), NewZero())
	require.Error(t, err)

	_, err = funcs[ast.BitNeg].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestUnaryNot(t *testing.T) {
	ctx := createContext(t)
	sc := ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []interface{}
		expected int64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{1}, 0, false, false},
		{[]interface{}{0}, 1, false, false},
		{[]interface{}{123}, 0, false, false},
		{[]interface{}{-123}, 0, false, false},
		{[]interface{}{"123"}, 0, false, false},
		{[]interface{}{float64(0.3)}, 0, false, false},
		{[]interface{}{"0.3"}, 0, false, false},
		{[]interface{}{types.NewDecFromFloatForTest(0.3)}, 0, false, false},
		{[]interface{}{nil}, 0, true, false},

		{[]interface{}{errors.New("must error")}, 0, false, true},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.UnaryNot, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetInt64())
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(ctx, ast.UnaryNot, NewZero(), NewZero())
	require.Error(t, err)

	_, err = funcs[ast.UnaryNot].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestIsTrueOrFalse(t *testing.T) {
	ctx := createContext(t)
	sc := ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	testCases := []struct {
		args    []interface{}
		isTrue  interface{}
		isFalse interface{}
	}{
		{
			args:    []interface{}{-12},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []interface{}{12},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []interface{}{0},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []interface{}{float64(0)},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []interface{}{"aaa"},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []interface{}{""},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []interface{}{"0.3"},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []interface{}{float64(0.3)},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []interface{}{types.NewDecFromFloatForTest(0.3)},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []interface{}{nil},
			isTrue:  0,
			isFalse: 0,
		},
		{
			args:    []interface{}{types.NewDuration(0, 0, 0, 1000, 3)},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []interface{}{types.NewDuration(0, 0, 0, 0, 3)},
			isTrue:  0,
			isFalse: 1,
		},
		{
			args:    []interface{}{types.NewTime(types.FromDate(0, 0, 0, 0, 0, 0, 1000), mysql.TypeDatetime, 3)},
			isTrue:  1,
			isFalse: 0,
		},
		{
			args:    []interface{}{types.NewTime(types.CoreTime(0), mysql.TypeTimestamp, 3)},
			isTrue:  0,
			isFalse: 1,
		},
	}

	for _, tc := range testCases {
		isTrueSig, err := funcs[ast.IsTruthWithoutNull].getFunction(ctx, datumsToConstants(types.MakeDatums(tc.args...)))
		require.NoError(t, err)
		require.NotNil(t, isTrueSig)

		isTrue, err := evalBuiltinFunc(isTrueSig, chunk.Row{})
		require.NoError(t, err)
		trequire.DatumEqual(t, types.NewDatum(tc.isTrue), isTrue)
	}

	for _, tc := range testCases {
		isFalseSig, err := funcs[ast.IsFalsity].getFunction(ctx, datumsToConstants(types.MakeDatums(tc.args...)))
		require.NoError(t, err)
		require.NotNil(t, isFalseSig)

		isFalse, err := evalBuiltinFunc(isFalseSig, chunk.Row{})
		require.NoError(t, err)
		trequire.DatumEqual(t, types.NewDatum(tc.isFalse), isFalse)
	}
}

func TestLogicXor(t *testing.T) {
	ctx := createContext(t)
	sc := ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []interface{}
		expected int64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{1, 1}, 0, false, false},
		{[]interface{}{1, 0}, 1, false, false},
		{[]interface{}{0, 1}, 1, false, false},
		{[]interface{}{0, 0}, 0, false, false},
		{[]interface{}{2, -1}, 0, false, false},
		{[]interface{}{"a", "0"}, 0, false, false},
		{[]interface{}{"a", "1"}, 1, false, false},
		{[]interface{}{"1a", "0"}, 1, false, false},
		{[]interface{}{"1a", "1"}, 0, false, false},
		{[]interface{}{0, nil}, 0, true, false},
		{[]interface{}{nil, 0}, 0, true, false},
		{[]interface{}{nil, 1}, 0, true, false},
		{[]interface{}{0.5000, 0.4999}, 0, false, false},
		{[]interface{}{0.5000, 1.0}, 0, false, false},
		{[]interface{}{0.4999, 1.0}, 0, false, false},
		{[]interface{}{nil, 0.000}, 0, true, false},
		{[]interface{}{nil, 0.001}, 0, true, false},
		{[]interface{}{types.NewDecFromStringForTest("0.000001"), 0.00001}, 0, false, false},
		{[]interface{}{types.NewDecFromStringForTest("0.000001"), 1}, 0, false, false},
		{[]interface{}{types.NewDecFromStringForTest("0.000000"), nil}, 0, true, false},
		{[]interface{}{types.NewDecFromStringForTest("0.000001"), nil}, 0, true, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.LogicXor, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetInt64())
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(ctx, ast.LogicXor, NewZero())
	require.Error(t, err)

	_, err = funcs[ast.LogicXor].getFunction(ctx, []Expression{NewZero(), NewZero()})
	require.NoError(t, err)
}
