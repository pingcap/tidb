// Copyright 2015 PingCAP, Inc.
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
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestAbs(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		Arg any
		Ret any
	}{
		{nil, nil},
		{int64(1), int64(1)},
		{uint64(1), uint64(1)},
		{int64(-1), int64(1)},
		{float64(3.14), float64(3.14)},
		{float64(-3.14), float64(3.14)},
	}

	Dtbl := tblToDtbl(tbl)

	for _, tt := range Dtbl {
		fc := funcs[ast.Abs]
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Arg"]))
		require.NoError(t, err)
		v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, tt["Ret"][0], v)
	}
}

func TestCeil(t *testing.T) {
	ctx := createContext(t)
	sc := ctx.GetSessionVars().StmtCtx
	oldTypeFlags := sc.TypeFlags()
	defer func() {
		sc.SetTypeFlags(oldTypeFlags)
	}()
	sc.SetTypeFlags(oldTypeFlags.WithIgnoreTruncateErr(true))

	type testCase struct {
		arg    any
		expect any
		isNil  bool
		getErr bool
	}

	cases := []testCase{
		{nil, nil, true, false},
		{int64(1), int64(1), false, false},
		{float64(1.23), float64(2), false, false},
		{float64(-1.23), float64(-1), false, false},
		{"1.23", float64(2), false, false},
		{"-1.23", float64(-1), false, false},
		{"tidb", float64(0), false, false},
		{"1tidb", float64(1), false, false}}

	expressions := []Expression{
		&Constant{
			Value:   types.NewDatum(0),
			RetType: types.NewFieldType(mysql.TypeTiny),
		},
		&Constant{
			Value:   types.NewFloat64Datum(float64(12.34)),
			RetType: types.NewFieldType(mysql.TypeFloat),
		},
	}

	runCasesOn := func(funcName string, cases []testCase, exps []Expression) {
		for _, test := range cases {
			f, err := newFunctionForTest(ctx, funcName, primitiveValsToConstants(ctx, []any{test.arg})...)
			require.NoError(t, err)

			result, err := f.Eval(ctx, chunk.Row{})
			if test.getErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if test.isNil {
					require.Equal(t, types.KindNull, result.Kind())
				} else {
					testutil.DatumEqual(t, types.NewDatum(test.expect), result)
				}
			}
		}

		for _, exp := range exps {
			_, err := funcs[funcName].getFunction(ctx, []Expression{exp})
			require.NoError(t, err)
		}
	}

	runCasesOn(ast.Ceil, cases, expressions)
	runCasesOn(ast.Ceiling, cases, expressions)
}

func TestExp(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args       any
		expect     float64
		isNil      bool
		getWarning bool
		errMsg     string
	}{
		{nil, 0, true, false, ""},
		{int64(1), 2.718281828459045, false, false, ""},
		{float64(1.23), 3.4212295362896734, false, false, ""},
		{float64(-1.23), 0.2922925776808594, false, false, ""},
		{float64(0), 1, false, false, ""},
		{"0", 1, false, false, ""},
		{"tidb", 0, false, true, ""},
		{float64(100000), 0, false, true, "[types:1690]DOUBLE value is out of range in 'exp(100000)'"},
	}

	if runtime.GOARCH == "ppc64le" {
		tests[1].expect = 2.7182818284590455
	}

	for _, test := range tests {
		preWarningCnt := ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(ctx, ast.Exp, primitiveValsToConstants(ctx, []any{test.args})...)
		require.NoError(t, err)

		result, err := f.Eval(ctx, chunk.Row{})
		if test.getWarning {
			if test.errMsg != "" {
				require.Error(t, err)
				require.Equal(t, test.errMsg, err.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, preWarningCnt+1, ctx.GetSessionVars().StmtCtx.WarningCount())
			}
		} else {
			require.NoError(t, err)
			if test.isNil {
				require.Equal(t, types.KindNull, result.Kind())
			} else {
				require.Equal(t, test.expect, result.GetFloat64())
			}
		}
	}

	_, err := funcs[ast.Exp].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestFloor(t *testing.T) {
	ctx := createContext(t)
	sc := ctx.GetSessionVars().StmtCtx
	oldTypeFlags := sc.TypeFlags()
	defer func() {
		sc.SetTypeFlags(oldTypeFlags)
	}()
	sc.SetTypeFlags(oldTypeFlags.WithIgnoreTruncateErr(true))

	genDuration := func(h, m, s int64) types.Duration {
		duration := time.Duration(h)*time.Hour +
			time.Duration(m)*time.Minute +
			time.Duration(s)*time.Second

		return types.Duration{Duration: duration, Fsp: types.DefaultFsp}
	}

	genTime := func(y, m, d int) types.Time {
		return types.NewTime(types.FromDate(y, m, d, 0, 0, 0, 0), mysql.TypeDatetime, types.DefaultFsp)
	}

	for _, test := range []struct {
		arg    any
		expect any
		isNil  bool
		getErr bool
	}{
		{nil, nil, true, false},
		{int64(1), int64(1), false, false},
		{float64(1.23), float64(1), false, false},
		{float64(-1.23), float64(-2), false, false},
		{"1.23", float64(1), false, false},
		{"-1.23", float64(-2), false, false},
		{"-1.b23", float64(-1), false, false},
		{"abce", float64(0), false, false},
		{genDuration(12, 59, 59), float64(125959), false, false},
		{genDuration(0, 12, 34), float64(1234), false, false},
		{genTime(2017, 7, 19), float64(20170719000000), false, false},
	} {
		f, err := newFunctionForTest(ctx, ast.Floor, primitiveValsToConstants(ctx, []any{test.arg})...)
		require.NoError(t, err)

		result, err := f.Eval(ctx, chunk.Row{})
		if test.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if test.isNil {
				require.Equal(t, types.KindNull, result.Kind())
			} else {
				testutil.DatumEqual(t, types.NewDatum(test.expect), result)
			}
		}
	}

	for _, exp := range []Expression{
		&Constant{
			Value:   types.NewDatum(0),
			RetType: types.NewFieldType(mysql.TypeTiny),
		},
		&Constant{
			Value:   types.NewFloat64Datum(float64(12.34)),
			RetType: types.NewFieldType(mysql.TypeFloat),
		},
	} {
		_, err := funcs[ast.Floor].getFunction(ctx, []Expression{exp})
		require.NoError(t, err)
	}
}

func TestLog(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args         []any
		expect       float64
		isNil        bool
		warningCount uint16
	}{
		{[]any{nil}, 0, true, 0},
		{[]any{nil, nil}, 0, true, 0},
		{[]any{int64(100)}, 4.605170185988092, false, 0},
		{[]any{float64(100)}, 4.605170185988092, false, 0},
		{[]any{int64(10), int64(100)}, 2, false, 0},
		{[]any{float64(10), float64(100)}, 2, false, 0},
		{[]any{float64(-1)}, 0, true, 1},
		{[]any{float64(2), float64(-1)}, 0, true, 1},
		{[]any{float64(-1), float64(2)}, 0, true, 1},
		{[]any{float64(1), float64(2)}, 0, true, 1},
		{[]any{float64(0.5), float64(0.25)}, 2, false, 0},
		{[]any{"abc"}, 0, true, 2},
	}

	for _, test := range tests {
		preWarningCnt := ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(ctx, ast.Log, primitiveValsToConstants(ctx, test.args)...)
		require.NoError(t, err)

		result, err := f.Eval(ctx, chunk.Row{})
		require.NoError(t, err)
		if test.warningCount > 0 {
			require.Equal(t, preWarningCnt+test.warningCount, ctx.GetSessionVars().StmtCtx.WarningCount())
		}
		if test.isNil {
			require.Equal(t, types.KindNull, result.Kind())
		} else {
			require.Equal(t, test.expect, result.GetFloat64())
		}
	}

	_, err := funcs[ast.Log].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestLog2(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args         any
		expect       float64
		isNil        bool
		warningCount uint16
	}{
		{nil, 0, true, 0},
		{int64(16), 4, false, 0},
		{float64(16), 4, false, 0},
		{int64(5), 2.321928094887362, false, 0},
		{int64(-1), 0, true, 1},
		{"4abc", 2, false, 1},
		{"abc", 0, true, 2},
	}

	for _, test := range tests {
		preWarningCnt := ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(ctx, ast.Log2, primitiveValsToConstants(ctx, []any{test.args})...)
		require.NoError(t, err)

		result, err := f.Eval(ctx, chunk.Row{})
		require.NoError(t, err)
		if test.warningCount > 0 {
			require.Equal(t, preWarningCnt+test.warningCount, ctx.GetSessionVars().StmtCtx.WarningCount())
		}
		if test.isNil {
			require.Equal(t, types.KindNull, result.Kind())
		} else {
			require.Equal(t, test.expect, result.GetFloat64())
		}
	}

	_, err := funcs[ast.Log2].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestLog10(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args         any
		expect       float64
		isNil        bool
		warningCount uint16
	}{
		{nil, 0, true, 0},
		{int64(100), 2, false, 0},
		{float64(100), 2, false, 0},
		{int64(101), 2.0043213737826426, false, 0},
		{int64(-1), 0, true, 1},
		{"100abc", 2, false, 1},
		{"abc", 0, true, 2},
	}

	for _, test := range tests {
		preWarningCnt := ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(ctx, ast.Log10, primitiveValsToConstants(ctx, []any{test.args})...)
		require.NoError(t, err)

		result, err := f.Eval(ctx, chunk.Row{})
		require.NoError(t, err)
		if test.warningCount > 0 {
			require.Equal(t, preWarningCnt+test.warningCount, ctx.GetSessionVars().StmtCtx.WarningCount())
		}
		if test.isNil {
			require.Equal(t, types.KindNull, result.Kind())
		} else {
			require.Equal(t, test.expect, result.GetFloat64())
		}
	}

	_, err := funcs[ast.Log10].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestRand(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.Rand]
	f, err := fc.getFunction(ctx, nil)
	require.NoError(t, err)
	v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
	require.NoError(t, err)
	require.Less(t, v.GetFloat64(), float64(1))
	require.GreaterOrEqual(t, v.GetFloat64(), float64(0))

	// issue 3211
	f2, err := fc.getFunction(ctx, []Expression{&Constant{Value: types.NewIntDatum(20160101), RetType: types.NewFieldType(mysql.TypeLonglong)}})
	require.NoError(t, err)
	randGen := mathutil.NewWithSeed(20160101)
	for i := 0; i < 3; i++ {
		v, err = evalBuiltinFunc(f2, ctx, chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, randGen.Gen(), v.GetFloat64())
	}
}

func TestPow(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		Arg []any
		Ret float64
	}{
		{[]any{1, 3}, 1},
		{[]any{2, 2}, 4},
		{[]any{4, 0.5}, 2},
		{[]any{4, -2}, 0.0625},
	}

	Dtbl := tblToDtbl(tbl)

	for _, tt := range Dtbl {
		fc := funcs[ast.Pow]
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Arg"]))
		require.NoError(t, err)
		v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, tt["Ret"][0], v)
	}

	errTbl := []struct {
		Arg []any
	}{
		{[]any{"test", "test"}},
		{[]any{1, "test"}},
		{[]any{10, 700}}, // added overflow test
	}

	errDtbl := tblToDtbl(errTbl)
	for i, tt := range errDtbl {
		fc := funcs[ast.Pow]
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Arg"]))
		require.NoError(t, err)
		_, err = evalBuiltinFunc(f, ctx, chunk.Row{})
		if i == 2 {
			require.Error(t, err)
			require.Equal(t, "[types:1690]DOUBLE value is out of range in 'pow(10, 700)'", err.Error())
		} else {
			require.NoError(t, err)
		}
	}
	require.Equal(t, 3, int(ctx.GetSessionVars().StmtCtx.WarningCount()))
}

func TestRound(t *testing.T) {
	ctx := createContext(t)
	newDec := types.NewDecFromStringForTest
	tbl := []struct {
		Arg []any
		Ret any
	}{
		{[]any{-1.23}, -1},
		{[]any{-1.23, 0}, -1},
		{[]any{-1.58}, -2},
		{[]any{1.58}, 2},
		{[]any{1.298, 1}, 1.3},
		{[]any{1.298}, 1},
		{[]any{1.298, 0}, 1},
		{[]any{-1.5, 0}, -2},
		{[]any{1.5, 0}, 2},
		{[]any{23.298, -1}, 20},
		{[]any{newDec("-1.23")}, newDec("-1")},
		{[]any{newDec("-1.23"), 1}, newDec("-1.2")},
		{[]any{newDec("-1.58")}, newDec("-2")},
		{[]any{newDec("1.58")}, newDec("2")},
		{[]any{newDec("1.58"), 1}, newDec("1.6")},
		{[]any{newDec("23.298"), -1}, newDec("20")},
		{[]any{nil, 2}, nil},
		{[]any{1, -2012}, 0},
		{[]any{1, -201299999999999}, 0},
	}

	Dtbl := tblToDtbl(tbl)

	for _, tt := range Dtbl {
		fc := funcs[ast.Round]
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Arg"]))
		require.NoError(t, err)
		switch f.(type) {
		case *builtinRoundWithFracIntSig:
			require.Equal(t, tipb.ScalarFuncSig_RoundWithFracInt, f.PbCode())
		case *builtinRoundWithFracDecSig:
			require.Equal(t, tipb.ScalarFuncSig_RoundWithFracDec, f.PbCode())
		case *builtinRoundWithFracRealSig:
			require.Equal(t, tipb.ScalarFuncSig_RoundWithFracReal, f.PbCode())
		case *builtinRoundIntSig:
			require.Equal(t, tipb.ScalarFuncSig_RoundInt, f.PbCode())
		case *builtinRoundDecSig:
			require.Equal(t, tipb.ScalarFuncSig_RoundDec, f.PbCode())
		case *builtinRoundRealSig:
			require.Equal(t, tipb.ScalarFuncSig_RoundReal, f.PbCode())
		}
		v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, tt["Ret"][0], v)
	}
}

func TestTruncate(t *testing.T) {
	ctx := createContext(t)
	newDec := types.NewDecFromStringForTest
	tbl := []struct {
		Arg []any
		Ret any
	}{
		{[]any{-1.23, 0}, -1},
		{[]any{1.58, 0}, 1},
		{[]any{1.298, 1}, 1.2},
		{[]any{123.2, -1}, 120},
		{[]any{123.2, 100}, 123.2},
		{[]any{123.2, -100}, 0},
		{[]any{123.2, -100}, 0},
		{[]any{1.797693134862315708145274237317043567981e+308, 2},
			1.797693134862315708145274237317043567981e+308},
		{[]any{newDec("-1.23"), 0}, newDec("-1")},
		{[]any{newDec("-1.23"), 1}, newDec("-1.2")},
		{[]any{newDec("-11.23"), -1}, newDec("-10")},
		{[]any{newDec("1.58"), 0}, newDec("1")},
		{[]any{newDec("1.58"), 1}, newDec("1.5")},
		{[]any{newDec("11.58"), -1}, newDec("10")},
		{[]any{newDec("23.298"), -1}, newDec("20")},
		{[]any{newDec("23.298"), -100}, newDec("0")},
		{[]any{newDec("23.298"), 100}, newDec("23.298")},
		{[]any{nil, 2}, nil},
		{[]any{uint64(9223372036854775808), -10}, 9223372030000000000},
		{[]any{9223372036854775807, -7}, 9223372036850000000},
		{[]any{uint64(18446744073709551615), -10}, uint64(18446744070000000000)},
	}

	Dtbl := tblToDtbl(tbl)

	for _, tt := range Dtbl {
		fc := funcs[ast.Truncate]
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Arg"]))
		require.NoError(t, err)
		require.NotNil(t, f)
		v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, tt["Ret"][0], v)
	}
}

func TestCRC32(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		input  []any
		chs    string
		result int64
		isNull bool
	}{
		{[]any{nil}, "utf8", 0, true},
		{[]any{""}, "utf8", 0, false},
		{[]any{-1}, "utf8", 808273962, false},
		{[]any{"-1"}, "utf8", 808273962, false},
		{[]any{"mysql"}, "utf8", 2501908538, false},
		{[]any{"MySQL"}, "utf8", 3259397556, false},
		{[]any{"hello"}, "utf8", 907060870, false},
		{[]any{"一二三"}, "utf8", 1785250883, false},
		{[]any{"一"}, "utf8", 2416838398, false},
		{[]any{"一二三"}, "gbk", 3461331449, false},
		{[]any{"一"}, "gbk", 2925846374, false},
	}
	for _, c := range tbl {
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, c.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.CRC32, primitiveValsToConstants(ctx, c.input)...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		require.NoError(t, err)
		if c.isNull {
			require.True(t, d.IsNull())
		} else {
			require.Equal(t, c.result, d.GetInt64())
		}
	}
}

func TestConv(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args     []any
		expected any
		isNil    bool
		getErr   bool
	}{
		{[]any{"a", 16, 2}, "1010", false, false},
		{[]any{"6E", 18, 8}, "172", false, false},
		{[]any{"-17", 10, -18}, "-H", false, false},
		{[]any{"-17", 10, 18}, "2D3FGB0B9CG4BD1H", false, false},
		{[]any{nil, 10, 10}, "0", true, false},
		{[]any{"+18aZ", 7, 36}, "1", false, false},
		{[]any{"18446744073709551615", -10, 16}, "7FFFFFFFFFFFFFFF", false, false},
		{[]any{"12F", -10, 16}, "C", false, false},
		{[]any{"  FF ", 16, 10}, "255", false, false},
		{[]any{"TIDB", 10, 8}, "0", false, false},
		{[]any{"aa", 10, 2}, "0", false, false},
		{[]any{" A", -10, 16}, "0", false, false},
		{[]any{"a6a", 10, 8}, "0", false, false},
		{[]any{"a6a", 1, 8}, "0", true, false},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Conv, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		tp := f.GetType(ctx)
		require.Equal(t, mysql.TypeVarString, tp.GetType())
		require.Equal(t, charset.CharsetUTF8MB4, tp.GetCharset())
		require.Equal(t, charset.CollationUTF8MB4, tp.GetCollate())
		require.Equal(t, uint(0), tp.GetFlag())

		d, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetString())
			}
		}
	}

	v := []struct {
		s    string
		base int64
		ret  string
	}{
		{"-123456D1f", 5, "-1234"},
		{"+12azD", 16, "12a"},
		{"+", 12, ""},
	}
	for _, tt := range v {
		r := getValidPrefix(tt.s, tt.base)
		require.Equal(t, tt.ret, r)
	}

	_, err := funcs[ast.Conv].getFunction(ctx, []Expression{NewZero(), NewZero(), NewZero()})
	require.NoError(t, err)
}

func TestSign(t *testing.T) {
	ctx := createContext(t)
	sc := ctx.GetSessionVars().StmtCtx
	oldTypeFlags := sc.TypeFlags()
	defer func() {
		sc.SetTypeFlags(oldTypeFlags)
	}()
	sc.SetTypeFlags(oldTypeFlags.WithIgnoreTruncateErr(true))

	for _, tt := range []struct {
		num []any
		ret any
	}{
		{[]any{nil}, nil},
		{[]any{1}, int64(1)},
		{[]any{0}, int64(0)},
		{[]any{-1}, int64(-1)},
		{[]any{0.4}, int64(1)},
		{[]any{-0.4}, int64(-1)},
		{[]any{"1"}, int64(1)},
		{[]any{"-1"}, int64(-1)},
		{[]any{"1a"}, int64(1)},
		{[]any{"-1a"}, int64(-1)},
		{[]any{"a"}, int64(0)},
		{[]any{uint64(9223372036854775808)}, int64(1)},
	} {
		fc := funcs[ast.Sign]
		f, err := fc.getFunction(ctx, primitiveValsToConstants(ctx, tt.num))
		require.NoError(t, err)
		v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(tt.ret), v)
	}
}

func TestDegrees(t *testing.T) {
	ctx := createContext(t)
	sc := ctx.GetSessionVars().StmtCtx
	oldTypeFlags := sc.TypeFlags()
	defer func() {
		sc.SetTypeFlags(oldTypeFlags)
	}()
	sc.SetTypeFlags(oldTypeFlags.WithIgnoreTruncateErr(false))

	cases := []struct {
		args       any
		expected   float64
		isNil      bool
		getWarning bool
	}{
		{nil, 0, true, false},
		{int64(0), float64(0), false, false},
		{int64(1), float64(57.29577951308232), false, false},
		{float64(1), float64(57.29577951308232), false, false},
		{float64(math.Pi), float64(180), false, false},
		{float64(-math.Pi / 2), float64(-90), false, false},
		{"", float64(0), false, false},
		{"-2", float64(-114.59155902616465), false, false},
		{"abc", float64(0), false, true},
		{"+1abc", 57.29577951308232, false, true},
	}

	for _, c := range cases {
		preWarningCnt := ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(ctx, ast.Degrees, primitiveValsToConstants(ctx, []any{c.args})...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		if c.getWarning {
			require.NoError(t, err)
			require.Equal(t, preWarningCnt+1, ctx.GetSessionVars().StmtCtx.WarningCount())
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetFloat64())
			}
		}
	}
	_, err := funcs[ast.Degrees].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestSqrt(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		Arg []any
		Ret any
	}{
		{[]any{nil}, nil},
		{[]any{int64(1)}, float64(1)},
		{[]any{float64(4)}, float64(2)},
		{[]any{"4"}, float64(2)},
		{[]any{"9"}, float64(3)},
		{[]any{"-16"}, nil},
	}

	for _, tt := range tbl {
		fc := funcs[ast.Sqrt]
		f, err := fc.getFunction(ctx, primitiveValsToConstants(ctx, tt.Arg))
		require.NoError(t, err)
		v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(tt.Ret), v)
	}
}

func TestPi(t *testing.T) {
	ctx := createContext(t)
	f, err := funcs[ast.PI].getFunction(ctx, nil)
	require.NoError(t, err)

	pi, err := evalBuiltinFunc(f, ctx, chunk.Row{})
	require.NoError(t, err)
	testutil.DatumEqual(t, types.NewDatum(math.Pi), pi)
}

func TestRadians(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		Arg any
		Ret any
	}{
		{nil, nil},
		{0, float64(0)},
		{float64(180), float64(math.Pi)},
		{-360, -2 * float64(math.Pi)},
		{"180", float64(math.Pi)},
	}

	Dtbl := tblToDtbl(tbl)
	for _, tt := range Dtbl {
		fc := funcs[ast.Radians]
		f, err := fc.getFunction(ctx, datumsToConstants(tt["Arg"]))
		require.NoError(t, err)
		require.NotNil(t, f)
		v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, tt["Ret"][0], v)
	}

	invalidArg := "notNum"
	fc := funcs[ast.Radians]
	f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{types.NewDatum(invalidArg)}))
	require.NoError(t, err)
	_, err = evalBuiltinFunc(f, ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, 1, int(ctx.GetSessionVars().StmtCtx.WarningCount()))
}

func TestSin(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args       any
		expected   float64
		isNil      bool
		getWarning bool
	}{
		{nil, 0, true, false},
		{int64(0), float64(0), false, false},
		{math.Pi, math.Sin(math.Pi), false, false}, // Pie ==> 0
		{-math.Pi, math.Sin(-math.Pi), false, false},
		{math.Pi / 2, math.Sin(math.Pi / 2), false, false}, // Pie/2 ==> 1
		{-math.Pi / 2, math.Sin(-math.Pi / 2), false, false},
		{math.Pi / 6, math.Sin(math.Pi / 6), false, false}, // Pie/6(30 degrees) ==> 0.5
		{-math.Pi / 6, math.Sin(-math.Pi / 6), false, false},
		{math.Pi * 2, math.Sin(math.Pi * 2), false, false},
		{"adfsdfgs", 0, false, true},
		{"0.000", 0, false, false},
	}

	for _, c := range cases {
		preWarningCnt := ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(ctx, ast.Sin, primitiveValsToConstants(ctx, []any{c.args})...)
		require.NoError(t, err)

		d, err := f.Eval(ctx, chunk.Row{})
		if c.getWarning {
			require.NoError(t, err)
			require.Equal(t, preWarningCnt+1, ctx.GetSessionVars().StmtCtx.WarningCount())
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetFloat64())
			}
		}
	}

	_, err := funcs[ast.Sin].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestCos(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args       any
		expected   float64
		isNil      bool
		getWarning bool
	}{
		{nil, 0, true, false},
		{int64(0), float64(1), false, false},
		{math.Pi, float64(-1), false, false}, // cos pi equals -1
		{-math.Pi, float64(-1), false, false},
		{math.Pi / 2, math.Cos(math.Pi / 2), false, false}, // Pi/2 is some near 0 (6.123233995736766e-17) but not 0. Even in math it is 0.
		{-math.Pi / 2, math.Cos(-math.Pi / 2), false, false},
		{"0.000", float64(1), false, false}, // string value case
		{"sdfgsfsdf", float64(0), false, true},
	}

	for _, c := range cases {
		preWarningCnt := ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(ctx, ast.Cos, primitiveValsToConstants(ctx, []any{c.args})...)
		require.NoError(t, err)

		d, err := f.Eval(ctx, chunk.Row{})
		if c.getWarning {
			require.NoError(t, err)
			require.Equal(t, preWarningCnt+1, ctx.GetSessionVars().StmtCtx.WarningCount())
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetFloat64())
			}
		}
	}

	_, err := funcs[ast.Cos].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestAcos(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args       any
		expect     float64
		isNil      bool
		getWarning bool
	}{
		{nil, 0, true, false},
		{float64(1), 0, false, false},
		{float64(2), 0, true, false},
		{float64(-1), 3.141592653589793, false, false},
		{float64(-2), 0, true, false},
		{"tidb", 0, false, true},
	}

	for _, test := range tests {
		preWarningCnt := ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(ctx, ast.Acos, primitiveValsToConstants(ctx, []any{test.args})...)
		require.NoError(t, err)

		result, err := f.Eval(ctx, chunk.Row{})
		if test.getWarning {
			require.NoError(t, err)
			require.Equal(t, preWarningCnt+1, ctx.GetSessionVars().StmtCtx.WarningCount())
		} else {
			require.NoError(t, err)
			if test.isNil {
				require.Equal(t, types.KindNull, result.Kind())
			} else {
				require.Equal(t, test.expect, result.GetFloat64())
			}
		}
	}

	_, err := funcs[ast.Acos].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestAsin(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args       any
		expect     float64
		isNil      bool
		getWarning bool
	}{
		{nil, 0, true, false},
		{float64(1), 1.5707963267948966, false, false},
		{float64(2), 0, true, false},
		{float64(-1), -1.5707963267948966, false, false},
		{float64(-2), 0, true, false},
		{"tidb", 0, false, true},
	}

	for _, test := range tests {
		preWarningCnt := ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(ctx, ast.Asin, primitiveValsToConstants(ctx, []any{test.args})...)
		require.NoError(t, err)

		result, err := f.Eval(ctx, chunk.Row{})
		if test.getWarning {
			require.NoError(t, err)
			require.Equal(t, preWarningCnt+1, ctx.GetSessionVars().StmtCtx.WarningCount())
		} else {
			require.NoError(t, err)
			if test.isNil {
				require.Equal(t, types.KindNull, result.Kind())
			} else {
				require.Equal(t, test.expect, result.GetFloat64())
			}
		}
	}

	_, err := funcs[ast.Asin].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestAtan(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args       []any
		expect     float64
		isNil      bool
		getWarning bool
	}{
		{[]any{nil}, 0, true, false},
		{[]any{nil, nil}, 0, true, false},
		{[]any{float64(1)}, 0.7853981633974483, false, false},
		{[]any{float64(-1)}, -0.7853981633974483, false, false},
		{[]any{float64(0), float64(-2)}, float64(math.Pi), false, false},
		{[]any{"tidb"}, 0, false, true},
	}

	for _, test := range tests {
		preWarningCnt := ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(ctx, ast.Atan, primitiveValsToConstants(ctx, test.args)...)
		require.NoError(t, err)

		result, err := f.Eval(ctx, chunk.Row{})
		if test.getWarning {
			require.NoError(t, err)
			require.Equal(t, preWarningCnt+1, ctx.GetSessionVars().StmtCtx.WarningCount())
		} else {
			require.NoError(t, err)
			if test.isNil {
				require.Equal(t, types.KindNull, result.Kind())
			} else {
				require.Equal(t, test.expect, result.GetFloat64())
			}
		}
	}

	_, err := funcs[ast.Atan].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestTan(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args       any
		expected   float64
		isNil      bool
		getWarning bool
	}{
		{nil, 0, true, false},
		{int64(0), float64(0), false, false},
		{math.Pi / 4, math.Tan(math.Pi / 4), false, false},
		{-math.Pi / 4, math.Tan(-math.Pi / 4), false, false},
		{math.Pi * 3 / 4, math.Tan(math.Pi * 3 / 4), false, false}, // in mysql and golang, it equals -1.0000000000000002, not -1
		{"0.000", float64(0), false, false},
		{"sdfgsdfg", 0, false, true},
	}

	for _, c := range cases {
		preWarningCnt := ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(ctx, ast.Tan, primitiveValsToConstants(ctx, []any{c.args})...)
		require.NoError(t, err)

		d, err := f.Eval(ctx, chunk.Row{})
		if c.getWarning {
			require.NoError(t, err)
			require.Equal(t, preWarningCnt+1, ctx.GetSessionVars().StmtCtx.WarningCount())
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.expected, d.GetFloat64())
			}
		}
	}

	_, err := funcs[ast.Tan].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestCot(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args   any
		expect float64
		isNil  bool
		getErr bool
		errMsg string
	}{
		{nil, 0, true, false, ""},
		{float64(0), 0, false, true, "[types:1690]DOUBLE value is out of range in 'cot(0)'"},
		{float64(-1), -0.6420926159343308, false, false, ""},
		{float64(1), 0.6420926159343308, false, false, ""},
		{math.Pi / 4, 1 / math.Tan(math.Pi/4), false, false, ""},
		{math.Pi / 2, 1 / math.Tan(math.Pi/2), false, false, ""},
		{math.Pi, 1 / math.Tan(math.Pi), false, false, ""},
		{"tidb", 0, false, true, ""},
	}

	for _, test := range tests {
		f, err := newFunctionForTest(ctx, ast.Cot, primitiveValsToConstants(ctx, []any{test.args})...)
		require.NoError(t, err)

		result, err := f.Eval(ctx, chunk.Row{})
		if test.getErr {
			require.Error(t, err)
			if test.errMsg != "" {
				require.Equal(t, test.errMsg, err.Error())
			}
		} else {
			require.NoError(t, err)
			if test.isNil {
				require.Equal(t, types.KindNull, result.Kind())
			} else {
				require.Equal(t, test.expect, result.GetFloat64())
			}
		}
	}

	_, err := funcs[ast.Cot].getFunction(ctx, []Expression{NewOne()})
	require.NoError(t, err)
}
