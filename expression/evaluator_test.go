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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
)

func kindToFieldType(kind byte) types.FieldType {
	ft := types.FieldType{}
	switch kind {
	case types.KindNull:
		ft.SetType(mysql.TypeNull)
	case types.KindInt64:
		ft.SetType(mysql.TypeLonglong)
	case types.KindUint64:
		ft.SetType(mysql.TypeLonglong)
		ft.SetFlag(ft.GetFlag() | mysql.UnsignedFlag)
	case types.KindMinNotNull:
		ft.SetType(mysql.TypeLonglong)
	case types.KindMaxValue:
		ft.SetType(mysql.TypeLonglong)
	case types.KindFloat32:
		ft.SetType(mysql.TypeDouble)
	case types.KindFloat64:
		ft.SetType(mysql.TypeDouble)
	case types.KindString:
		ft.SetType(mysql.TypeVarString)
	case types.KindBytes:
		ft.SetType(mysql.TypeVarString)
	case types.KindMysqlEnum:
		ft.SetType(mysql.TypeEnum)
	case types.KindMysqlSet:
		ft.SetType(mysql.TypeSet)
	case types.KindInterface:
		ft.SetType(mysql.TypeVarString)
	case types.KindMysqlDecimal:
		ft.SetType(mysql.TypeNewDecimal)
	case types.KindMysqlDuration:
		ft.SetType(mysql.TypeDuration)
	case types.KindMysqlTime:
		ft.SetType(mysql.TypeDatetime)
	case types.KindBinaryLiteral:
		ft.SetType(mysql.TypeVarString)
		ft.SetCharset(charset.CharsetBin)
		ft.SetCollate(charset.CollationBin)
	case types.KindMysqlBit:
		ft.SetType(mysql.TypeBit)
	case types.KindMysqlJSON:
		ft.SetType(mysql.TypeJSON)
	}
	return ft
}

func datumsToConstants(datums []types.Datum) []Expression {
	constants := make([]Expression, 0, len(datums))
	for _, d := range datums {
		ft := kindToFieldType(d.Kind())
		if types.IsNonBinaryStr(&ft) {
			ft.SetCollate(d.Collation())
		}
		ft.SetFlen(types.UnspecifiedLength)
		ft.SetDecimal(types.UnspecifiedLength)
		constants = append(constants, &Constant{Value: d, RetType: &ft})
	}
	return constants
}

func primitiveValsToConstants(ctx sessionctx.Context, args []interface{}) []Expression {
	cons := datumsToConstants(types.MakeDatums(args...))
	char, col := ctx.GetSessionVars().GetCharsetInfo()
	for i, arg := range args {
		types.DefaultTypeForValue(arg, cons[i].GetType(), char, col)
	}
	return cons
}

func TestSleep(t *testing.T) {
	ctx := createContext(t)
	sessVars := ctx.GetSessionVars()

	fc := funcs[ast.Sleep]
	// non-strict model
	sessVars.StmtCtx.BadNullAsWarning = true
	d := make([]types.Datum, 1)
	f, err := fc.getFunction(ctx, datumsToConstants(d))
	require.NoError(t, err)
	ret, isNull, err := f.evalInt(chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, int64(0), ret)
	d[0].SetInt64(-1)
	f, err = fc.getFunction(ctx, datumsToConstants(d))
	require.NoError(t, err)
	ret, isNull, err = f.evalInt(chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, int64(0), ret)

	// for error case under the strict model
	sessVars.StmtCtx.BadNullAsWarning = false
	d[0].SetNull()
	_, err = fc.getFunction(ctx, datumsToConstants(d))
	require.NoError(t, err)
	_, isNull, err = f.evalInt(chunk.Row{})
	require.Error(t, err)
	require.False(t, isNull)
	d[0].SetFloat64(-2.5)
	_, err = fc.getFunction(ctx, datumsToConstants(d))
	require.NoError(t, err)
	_, isNull, err = f.evalInt(chunk.Row{})
	require.Error(t, err)
	require.False(t, isNull)

	// strict model
	d[0].SetFloat64(0.5)
	start := time.Now()
	f, err = fc.getFunction(ctx, datumsToConstants(d))
	require.NoError(t, err)
	ret, isNull, err = f.evalInt(chunk.Row{})
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, int64(0), ret)
	sub := time.Since(start)
	require.GreaterOrEqual(t, sub.Nanoseconds(), int64(0.5*1e9))
	d[0].SetFloat64(3)
	f, err = fc.getFunction(ctx, datumsToConstants(d))
	require.NoError(t, err)
	start = time.Now()
	go func() {
		time.Sleep(1 * time.Second)
		atomic.CompareAndSwapUint32(&ctx.GetSessionVars().Killed, 0, 1)
	}()
	ret, isNull, err = f.evalInt(chunk.Row{})
	sub = time.Since(start)
	require.NoError(t, err)
	require.False(t, isNull)
	require.Equal(t, int64(1), ret)
	require.LessOrEqual(t, sub.Nanoseconds(), int64(2*1e9))
	require.GreaterOrEqual(t, sub.Nanoseconds(), int64(1*1e9))
}

func TestBinopComparison(t *testing.T) {
	tbl := []struct {
		lhs    interface{}
		op     string
		rhs    interface{}
		result int64 // 0 for false, 1 for true
	}{
		// test EQ
		{1, ast.EQ, 2, 0},
		{false, ast.EQ, false, 1},
		{false, ast.EQ, true, 0},
		{true, ast.EQ, true, 1},
		{true, ast.EQ, false, 0},
		{"1", ast.EQ, true, 1},
		{"1", ast.EQ, false, 0},

		// test NEQ
		{1, ast.NE, 2, 1},
		{false, ast.NE, false, 0},
		{false, ast.NE, true, 1},
		{true, ast.NE, true, 0},
		{"1", ast.NE, true, 0},
		{"1", ast.NE, false, 1},

		// test GT, GE
		{1, ast.GT, 0, 1},
		{1, ast.GT, 1, 0},
		{1, ast.GE, 1, 1},
		{3.14, ast.GT, 3, 1},
		{3.14, ast.GE, 3.14, 1},

		// test LT, LE
		{1, ast.LT, 2, 1},
		{1, ast.LT, 1, 0},
		{1, ast.LE, 1, 1},
	}
	ctx := createContext(t)
	for _, tt := range tbl {
		fc := funcs[tt.op]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.lhs, tt.rhs)))
		require.NoError(t, err)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		val, err := v.ToBool(ctx.GetSessionVars().StmtCtx)
		require.NoError(t, err)
		require.Equal(t, tt.result, val)
	}

	// test nil
	nilTbl := []struct {
		lhs interface{}
		op  string
		rhs interface{}
	}{
		{nil, ast.EQ, nil},
		{nil, ast.EQ, 1},
		{nil, ast.NE, nil},
		{nil, ast.NE, 1},
		{nil, ast.LT, nil},
		{nil, ast.LT, 1},
		{nil, ast.LE, nil},
		{nil, ast.LE, 1},
		{nil, ast.GT, nil},
		{nil, ast.GT, 1},
		{nil, ast.GE, nil},
		{nil, ast.GE, 1},
	}

	for _, tt := range nilTbl {
		fc := funcs[tt.op]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.lhs, tt.rhs)))
		require.NoError(t, err)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, types.KindNull, v.Kind())
	}
}

func TestBinopLogic(t *testing.T) {
	tbl := []struct {
		lhs interface{}
		op  string
		rhs interface{}
		ret interface{}
	}{
		{nil, ast.LogicAnd, 1, nil},
		{nil, ast.LogicAnd, 0, 0},
		{nil, ast.LogicOr, 1, 1},
		{nil, ast.LogicOr, 0, nil},
		{nil, ast.LogicXor, 1, nil},
		{nil, ast.LogicXor, 0, nil},
		{1, ast.LogicAnd, 0, 0},
		{1, ast.LogicAnd, 1, 1},
		{1, ast.LogicOr, 0, 1},
		{1, ast.LogicOr, 1, 1},
		{0, ast.LogicOr, 0, 0},
		{1, ast.LogicXor, 0, 1},
		{1, ast.LogicXor, 1, 0},
		{0, ast.LogicXor, 0, 0},
		{0, ast.LogicXor, 1, 1},
	}
	ctx := createContext(t)
	for _, tt := range tbl {
		fc := funcs[tt.op]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.lhs, tt.rhs)))
		require.NoError(t, err)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		switch x := tt.ret.(type) {
		case nil:
			require.Equal(t, types.KindNull, v.Kind())
		case int:
			require.Equal(t, v, types.NewDatum(int64(x)))
		}
	}
}

func TestBinopBitop(t *testing.T) {
	tbl := []struct {
		lhs interface{}
		op  string
		rhs interface{}
		ret interface{}
	}{
		{1, ast.And, 1, 1},
		{1, ast.Or, 1, 1},
		{1, ast.Xor, 1, 0},
		{1, ast.LeftShift, 1, 2},
		{2, ast.RightShift, 1, 1},
		{nil, ast.And, 1, nil},
		{1, ast.And, nil, nil},
		{nil, ast.Or, 1, nil},
		{nil, ast.Xor, 1, nil},
		{nil, ast.LeftShift, 1, nil},
		{nil, ast.RightShift, 1, nil},
	}

	ctx := createContext(t)
	for _, tt := range tbl {
		fc := funcs[tt.op]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.lhs, tt.rhs)))
		require.NoError(t, err)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)

		switch x := tt.ret.(type) {
		case nil:
			require.Equal(t, types.KindNull, v.Kind())
		case int:
			require.Equal(t, v, types.NewDatum(uint64(x)))
		}
	}
}

func TestBinopNumeric(t *testing.T) {
	tbl := []struct {
		lhs interface{}
		op  string
		rhs interface{}
		ret interface{}
	}{
		// plus
		{1, ast.Plus, 1, 2},
		{1, ast.Plus, uint64(1), 2},
		{1, ast.Plus, "1", 2},
		{1, ast.Plus, types.NewDecFromInt(1), 2},
		{uint64(1), ast.Plus, 1, 2},
		{uint64(1), ast.Plus, uint64(1), 2},
		{uint64(1), ast.Plus, -1, 0},
		{1, ast.Plus, []byte("1"), 2},
		{1, ast.Plus, types.NewBinaryLiteralFromUint(1, -1), 2},
		{1, ast.Plus, types.Enum{Name: "a", Value: 1}, 2},
		{1, ast.Plus, types.Set{Name: "a", Value: 1}, 2},

		// minus
		{1, ast.Minus, 1, 0},
		{1, ast.Minus, uint64(1), 0},
		{1, ast.Minus, float64(1), 0},
		{1, ast.Minus, types.NewDecFromInt(1), 0},
		{uint64(1), ast.Minus, 1, 0},
		{uint64(1), ast.Minus, uint64(1), 0},
		{types.NewDecFromInt(1), ast.Minus, 1, 0},
		{"1", ast.Minus, []byte("1"), 0},

		// mul
		{1, ast.Mul, 1, 1},
		{1, ast.Mul, uint64(1), 1},
		{1, ast.Mul, float64(1), 1},
		{1, ast.Mul, types.NewDecFromInt(1), 1},
		{uint64(1), ast.Mul, 1, 1},
		{uint64(1), ast.Mul, uint64(1), 1},
		{types.NewTime(types.FromDate(0, 0, 0, 0, 0, 0, 0), 0, 0), ast.Mul, 0, 0},
		{types.ZeroDuration, ast.Mul, 0, 0},
		{types.NewTime(types.FromGoTime(time.Now()), mysql.TypeDatetime, 0), ast.Mul, 0, 0},
		{types.NewTime(types.FromGoTime(time.Now()), mysql.TypeDatetime, 6), ast.Mul, 0, 0},
		{types.Duration{Duration: 100000000, Fsp: 6}, ast.Mul, 0, 0},

		// div
		{1, ast.Div, float64(1), 1},
		{1, ast.Div, float64(0), nil},
		{1, ast.Div, 2, 0.5},
		{1, ast.Div, 0, nil},

		// int div
		{1, ast.IntDiv, 2, 0},
		{1, ast.IntDiv, uint64(2), 0},
		{1, ast.IntDiv, 0, nil},
		{1, ast.IntDiv, uint64(0), nil},
		{uint64(1), ast.IntDiv, 2, 0},
		{uint64(1), ast.IntDiv, uint64(2), 0},
		{uint64(1), ast.IntDiv, 0, nil},
		{uint64(1), ast.IntDiv, uint64(0), nil},
		{1.0, ast.IntDiv, 2.0, 0},

		// mod
		{10, ast.Mod, 2, 0},
		{10, ast.Mod, uint64(2), 0},
		{10, ast.Mod, 0, nil},
		{10, ast.Mod, uint64(0), nil},
		{-10, ast.Mod, uint64(2), 0},
		{uint64(10), ast.Mod, 2, 0},
		{uint64(10), ast.Mod, uint64(2), 0},
		{uint64(10), ast.Mod, 0, nil},
		{uint64(10), ast.Mod, uint64(0), nil},
		{uint64(10), ast.Mod, -2, 0},
		{float64(10), ast.Mod, 2, 0},
		{float64(10), ast.Mod, 0, nil},
		{types.NewDecFromInt(10), ast.Mod, 2, 0},
		{types.NewDecFromInt(10), ast.Mod, 0, nil},
	}

	ctx := createContext(t)
	for _, tt := range tbl {
		fc := funcs[tt.op]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.lhs, tt.rhs)))
		require.NoError(t, err)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		switch v.Kind() {
		case types.KindNull:
			require.Nil(t, tt.ret)
		default:
			// we use float64 as the result type check for all.
			sc := ctx.GetSessionVars().StmtCtx
			f, err := v.ToFloat64(sc)
			require.NoError(t, err)
			d := types.NewDatum(tt.ret)
			r, err := d.ToFloat64(sc)
			require.NoError(t, err)
			require.Equal(t, r, f)
		}
	}

	testcases := []struct {
		lhs interface{}
		op  string
		rhs interface{}
	}{
		// div
		{1, ast.Div, float64(0)},
		{1, ast.Div, 0},
		// int div
		{1, ast.IntDiv, 0},
		{1, ast.IntDiv, uint64(0)},
		{uint64(1), ast.IntDiv, 0},
		{uint64(1), ast.IntDiv, uint64(0)},
		// mod
		{10, ast.Mod, 0},
		{10, ast.Mod, uint64(0)},
		{uint64(10), ast.Mod, 0},
		{uint64(10), ast.Mod, uint64(0)},
		{float64(10), ast.Mod, 0},
		{types.NewDecFromInt(10), ast.Mod, 0},
	}

	ctx.GetSessionVars().StmtCtx.InSelectStmt = false
	ctx.GetSessionVars().SQLMode |= mysql.ModeErrorForDivisionByZero
	ctx.GetSessionVars().StmtCtx.InInsertStmt = true
	for _, tt := range testcases {
		fc := funcs[tt.op]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.lhs, tt.rhs)))
		require.NoError(t, err)
		_, err = evalBuiltinFunc(f, chunk.Row{})
		require.Error(t, err)
	}

	ctx.GetSessionVars().StmtCtx.DividedByZeroAsWarning = true
	for _, tt := range testcases {
		fc := funcs[tt.op]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.lhs, tt.rhs)))
		require.NoError(t, err)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, types.KindNull, v.Kind())
	}
}

func TestExtract(t *testing.T) {
	ctx := createContext(t)
	str := "2011-11-11 10:10:10.123456"
	tbl := []struct {
		Unit   string
		Expect int64
	}{
		{"MICROSECOND", 123456},
		{"SECOND", 10},
		{"MINUTE", 10},
		{"HOUR", 10},
		{"DAY", 11},
		{"WEEK", 45},
		{"MONTH", 11},
		{"QUARTER", 4},
		{"YEAR", 2011},
		{"SECOND_MICROSECOND", 10123456},
		{"MINUTE_MICROSECOND", 1010123456},
		{"MINUTE_SECOND", 1010},
		{"HOUR_MICROSECOND", 101010123456},
		{"HOUR_SECOND", 101010},
		{"HOUR_MINUTE", 1010},
		{"DAY_MICROSECOND", 11101010123456},
		{"DAY_SECOND", 11101010},
		{"DAY_MINUTE", 111010},
		{"DAY_HOUR", 1110},
		{"YEAR_MONTH", 201111},
	}
	for _, tt := range tbl {
		fc := funcs[ast.Extract]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.Unit, str)))
		require.NoError(t, err)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, types.NewDatum(tt.Expect), v)
	}

	// Test nil
	fc := funcs[ast.Extract]
	f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums("SECOND", nil)))
	require.NoError(t, err)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.KindNull, v.Kind())
}

func TestUnaryOp(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		arg    interface{}
		op     string
		result interface{}
	}{
		// test NOT.
		{1, ast.UnaryNot, int64(0)},
		{0, ast.UnaryNot, int64(1)},
		{nil, ast.UnaryNot, nil},
		{types.NewBinaryLiteralFromUint(0, -1), ast.UnaryNot, int64(1)},
		{types.NewBinaryLiteralFromUint(1, -1), ast.UnaryNot, int64(0)},
		{types.Enum{Name: "a", Value: 1}, ast.UnaryNot, int64(0)},
		{types.Set{Name: "a", Value: 1}, ast.UnaryNot, int64(0)},

		// test BitNeg.
		{nil, ast.BitNeg, nil},
		{-1, ast.BitNeg, uint64(0)},

		// test Minus.
		{nil, ast.UnaryMinus, nil},
		{1.0, ast.UnaryMinus, -1.0},
		{int64(1), ast.UnaryMinus, int64(-1)},
		{int64(1), ast.UnaryMinus, int64(-1)},
		{uint64(1), ast.UnaryMinus, -int64(1)},
		{"1.0", ast.UnaryMinus, -1.0},
		{[]byte("1.0"), ast.UnaryMinus, -1.0},
		{types.NewBinaryLiteralFromUint(1, -1), ast.UnaryMinus, -1.0},
		{true, ast.UnaryMinus, int64(-1)},
		{false, ast.UnaryMinus, int64(0)},
		{types.Enum{Name: "a", Value: 1}, ast.UnaryMinus, -1.0},
		{types.Set{Name: "a", Value: 1}, ast.UnaryMinus, -1.0},
	}
	for i, tt := range tbl {
		fc := funcs[tt.op]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.arg)))
		require.NoError(t, err)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		require.Equalf(t, types.NewDatum(tt.result), result, "%d", i)
	}

	tbl = []struct {
		arg    interface{}
		op     string
		result interface{}
	}{
		{types.NewDecFromInt(1), ast.UnaryMinus, types.NewDecFromInt(-1)},
		{types.ZeroDuration, ast.UnaryMinus, new(types.MyDecimal)},
		{types.NewTime(types.FromGoTime(time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)), mysql.TypeDatetime, 0), ast.UnaryMinus, types.NewDecFromInt(-20091110230000)},
	}

	for _, tt := range tbl {
		fc := funcs[tt.op]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.arg)))
		require.NoError(t, err)
		require.NotNil(t, f)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)

		expect := types.NewDatum(tt.result)
		ret, err := result.Compare(ctx.GetSessionVars().StmtCtx, &expect, collate.GetBinaryCollator())
		require.NoError(t, err)
		require.Equalf(t, 0, ret, "%v %s", tt.arg, tt.op)
	}
}

func TestMod(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.Mod]
	f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(234, 10)))
	require.NoError(t, err)
	r, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.NewIntDatum(4), r)
	f, err = fc.getFunction(ctx, datumsToConstants(types.MakeDatums(29, 9)))
	require.NoError(t, err)
	r, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.NewIntDatum(2), r)
	f, err = fc.getFunction(ctx, datumsToConstants(types.MakeDatums(34.5, 3)))
	require.NoError(t, err)
	r, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.NewDatum(1.5), r)
}
