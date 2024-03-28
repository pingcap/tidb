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
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/stretchr/testify/require"
)

func TestBitCount(t *testing.T) {
	ctx := createContext(t)
	stmtCtx := ctx.GetSessionVars().StmtCtx
	oldTypeFlags := stmtCtx.TypeFlags()
	defer func() {
		stmtCtx.SetTypeFlags(oldTypeFlags)
	}()
	stmtCtx.SetTypeFlags(oldTypeFlags.WithIgnoreTruncateErr(true))
	fc := funcs[ast.BitCount]
	var bitCountCases = []struct {
		origin any
		count  any
	}{
		{int64(8), int64(1)},
		{int64(29), int64(4)},
		{int64(0), int64(0)},
		{int64(-1), int64(64)},
		{int64(-11), int64(62)},
		{int64(-1000), int64(56)},
		{float64(1.1), int64(1)},
		{float64(3.1), int64(2)},
		{float64(-1.1), int64(64)},
		{float64(-3.1), int64(63)},
		{uint64(math.MaxUint64), int64(64)},
		{"xxx", int64(0)},
		{nil, nil},
	}
	for _, test := range bitCountCases {
		in := types.NewDatum(test.origin)
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{in}))
		require.NoError(t, err)
		require.NotNil(t, f)
		count, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		if count.IsNull() {
			require.Nil(t, test.count)
			continue
		}
		ctx := types.DefaultStmtNoWarningContext.WithFlags(types.DefaultStmtFlags.WithIgnoreTruncateErr(true))
		res, err := count.ToInt64(ctx)
		require.NoError(t, err)
		require.Equal(t, test.count, res)
	}
}

func TestRowFunc(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.RowFunc]
	_, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums([]any{"1", 1.2, true, 120}...)))
	require.NoError(t, err)
}

func TestSetVar(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.SetVar]
	dec := types.NewDecFromInt(5)
	timeDec := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 0)
	testCases := []struct {
		args []any
		res  any
	}{
		{[]any{"a", "12"}, "12"},
		{[]any{"b", "34"}, "34"},
		{[]any{"c", nil}, nil},
		{[]any{"c", "ABC"}, "ABC"},
		{[]any{"c", "dEf"}, "dEf"},
		{[]any{"d", int64(3)}, int64(3)},
		{[]any{"e", float64(2.5)}, float64(2.5)},
		{[]any{"f", dec}, dec},
		{[]any{"g", timeDec}, timeDec},
	}
	for _, tc := range testCases {
		fn, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tc.args...)))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(fn, ctx, chunk.MutRowFromDatums(types.MakeDatums(tc.args...)).ToRow())
		require.NoError(t, err)
		require.Equal(t, tc.res, d.GetValue())
		if tc.args[1] != nil {
			key, ok := tc.args[0].(string)
			require.Equal(t, true, ok)
			sessionVar, ok := ctx.GetSessionVars().GetUserVarVal(key)
			require.Equal(t, true, ok)
			require.Equal(t, tc.res, sessionVar.GetValue())
		}
	}
}

func TestGetVar(t *testing.T) {
	ctx := createContext(t)
	dec := types.NewDecFromInt(5)
	timeDec := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 0)
	sessionVars := []struct {
		key string
		val any
	}{
		{"a", "中"},
		{"b", "文字符chuan"},
		{"c", ""},
		{"e", int64(3)},
		{"f", float64(2.5)},
		{"g", dec},
		{"h", timeDec},
	}
	for _, kv := range sessionVars {
		ctx.GetSessionVars().SetUserVarVal(kv.key, types.NewDatum(kv.val))
		var tp *types.FieldType
		if _, ok := kv.val.(types.Time); ok {
			tp = types.NewFieldType(mysql.TypeDatetime)
		} else {
			tp = types.NewFieldType(mysql.TypeVarString)
		}
		types.InferParamTypeFromUnderlyingValue(kv.val, tp)
		ctx.GetSessionVars().SetUserVarType(kv.key, tp)
	}

	testCases := []struct {
		args []any
		res  any
	}{
		{[]any{"a"}, "中"},
		{[]any{"b"}, "文字符chuan"},
		{[]any{"c"}, ""},
		{[]any{"d"}, nil},
		{[]any{"e"}, int64(3)},
		{[]any{"f"}, float64(2.5)},
		{[]any{"g"}, dec},
		{[]any{"h"}, timeDec.String()},
	}
	for _, tc := range testCases {
		tp, ok := ctx.GetSessionVars().GetUserVarType(tc.args[0].(string))
		if !ok {
			tp = types.NewFieldType(mysql.TypeVarString)
		}
		fn, err := BuildGetVarFunction(ctx, datumsToConstants(types.MakeDatums(tc.args...))[0], tp)
		require.NoError(t, err)
		d, err := fn.Eval(ctx, chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, tc.res, d.GetValue())
	}
}

func TestTypeConversion(t *testing.T) {
	ctx := createContext(t)
	// Set value as int64
	key := "a"
	val := int64(3)
	ctx.GetSessionVars().SetUserVarVal(key, types.NewDatum(val))
	tp := types.NewFieldType(mysql.TypeLonglong)
	ctx.GetSessionVars().SetUserVarType(key, tp)

	args := []any{"a"}
	// To Decimal.
	tp = types.NewFieldType(mysql.TypeNewDecimal)
	fn, err := BuildGetVarFunction(ctx, datumsToConstants(types.MakeDatums(args...))[0], tp)
	require.NoError(t, err)
	d, err := fn.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	des := types.NewDecFromInt(3)
	require.Equal(t, des, d.GetValue())
	// To Float.
	tp = types.NewFieldType(mysql.TypeDouble)
	fn, err = BuildGetVarFunction(ctx, datumsToConstants(types.MakeDatums(args...))[0], tp)
	require.NoError(t, err)
	d, err = fn.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, float64(3), d.GetValue())
}

func TestValues(t *testing.T) {
	ctx := createContext(t)
	fc := &valuesFunctionClass{baseFunctionClass{ast.Values, 0, 0}, 1, types.NewFieldType(mysql.TypeVarchar)}
	_, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums("")))
	require.Error(t, err)
	require.Regexp(t, "Incorrect parameter count in the call to native function 'values'$", err.Error())

	sig, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums()))
	require.NoError(t, err)

	ret, err := evalBuiltinFunc(sig, ctx, chunk.Row{})
	require.NoError(t, err)
	require.True(t, ret.IsNull())

	ctx.GetSessionVars().CurrInsertValues = chunk.MutRowFromDatums(types.MakeDatums("1")).ToRow()
	ret, err = evalBuiltinFunc(sig, ctx, chunk.Row{})
	require.Error(t, err)
	require.Regexp(t, "^Session current insert values len", err.Error())

	currInsertValues := types.MakeDatums("1", "2")
	ctx.GetSessionVars().CurrInsertValues = chunk.MutRowFromDatums(currInsertValues).ToRow()
	ret, err = evalBuiltinFunc(sig, ctx, chunk.Row{})
	require.NoError(t, err)

	cmp, err := ret.Compare(types.DefaultStmtNoWarningContext, &currInsertValues[1], collate.GetBinaryCollator())
	require.NoError(t, err)
	require.Equal(t, 0, cmp)
}

func TestSetVarFromColumn(t *testing.T) {
	ctx := createContext(t)
	ft1 := types.FieldType{}
	ft1.SetType(mysql.TypeVarString)
	ft1.SetFlen(20)

	ft2 := ft1.Clone()
	ft3 := ft1.Clone()
	// Construct arguments.
	argVarName := &Constant{
		Value:   types.NewStringDatum("a"),
		RetType: &ft1,
	}
	argCol := &Column{
		RetType: ft2,
		Index:   0,
	}

	// Construct SetVar function.
	funcSetVar, err := NewFunction(
		ctx,
		ast.SetVar,
		ft3,
		[]Expression{argVarName, argCol}...,
	)
	require.NoError(t, err)

	// Construct input and output Chunks.
	inputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{argCol.RetType}, 1)
	inputChunk.AppendString(0, "a")
	outputChunk := chunk.NewChunkWithCapacity([]*types.FieldType{argCol.RetType}, 1)

	// Evaluate the SetVar function.
	err = evalOneCell(ctx, funcSetVar, inputChunk.GetRow(0), outputChunk, 0)
	require.NoError(t, err)
	require.Equal(t, "a", outputChunk.GetRow(0).GetString(0))

	// Change the content of the underlying Chunk.
	inputChunk.Reset()
	inputChunk.AppendString(0, "b")

	// Check whether the user variable changed.
	sessionVars := ctx.GetSessionVars()
	sessionVar, ok := sessionVars.GetUserVarVal("a")
	require.Equal(t, true, ok)
	require.Equal(t, "a", sessionVar.GetString())
}

func TestInFunc(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.In]
	decimal1 := types.NewDecFromFloatForTest(123.121)
	decimal2 := types.NewDecFromFloatForTest(123.122)
	decimal3 := types.NewDecFromFloatForTest(123.123)
	decimal4 := types.NewDecFromFloatForTest(123.124)
	time1 := types.NewTime(types.FromGoTime(time.Date(2017, 1, 1, 1, 1, 1, 1, time.UTC)), mysql.TypeDatetime, 6)
	time2 := types.NewTime(types.FromGoTime(time.Date(2017, 1, 2, 1, 1, 1, 1, time.UTC)), mysql.TypeDatetime, 6)
	time3 := types.NewTime(types.FromGoTime(time.Date(2017, 1, 3, 1, 1, 1, 1, time.UTC)), mysql.TypeDatetime, 6)
	time4 := types.NewTime(types.FromGoTime(time.Date(2017, 1, 4, 1, 1, 1, 1, time.UTC)), mysql.TypeDatetime, 6)
	duration1 := types.Duration{Duration: 12*time.Hour + 1*time.Minute + 1*time.Second}
	duration2 := types.Duration{Duration: 12*time.Hour + 1*time.Minute}
	duration3 := types.Duration{Duration: 12*time.Hour + 1*time.Second}
	duration4 := types.Duration{Duration: 12 * time.Hour}
	json1 := types.CreateBinaryJSON("123")
	json2 := types.CreateBinaryJSON("123.1")
	json3 := types.CreateBinaryJSON("123.2")
	json4 := types.CreateBinaryJSON("123.3")
	testCases := []struct {
		args []any
		res  any
	}{
		{[]any{1, 1, 2, 3}, int64(1)},
		{[]any{1, 0, 2, 3}, int64(0)},
		{[]any{1, nil, 2, 3}, nil},
		{[]any{nil, nil, 2, 3}, nil},
		{[]any{uint64(0), 0, 2, 3}, int64(1)},
		{[]any{uint64(math.MaxUint64), uint64(math.MaxUint64), 2, 3}, int64(1)},
		{[]any{-1, uint64(math.MaxUint64), 2, 3}, int64(0)},
		{[]any{uint64(math.MaxUint64), -1, 2, 3}, int64(0)},
		{[]any{1, 0, 2, 3}, int64(0)},
		{[]any{1.1, 1.2, 1.3}, int64(0)},
		{[]any{1.1, 1.1, 1.2, 1.3}, int64(1)},
		{[]any{decimal1, decimal2, decimal3, decimal4}, int64(0)},
		{[]any{decimal1, decimal2, decimal3, decimal1}, int64(1)},
		{[]any{"1.1", "1.1", "1.2", "1.3"}, int64(1)},
		{[]any{"1.1", hack.Slice("1.1"), "1.2", "1.3"}, int64(1)},
		{[]any{hack.Slice("1.1"), "1.1", "1.2", "1.3"}, int64(1)},
		{[]any{time1, time2, time3, time1}, int64(1)},
		{[]any{time1, time2, time3, time4}, int64(0)},
		{[]any{duration1, duration2, duration3, duration4}, int64(0)},
		{[]any{duration1, duration2, duration1, duration4}, int64(1)},
		{[]any{json1, json2, json3, json4}, int64(0)},
		{[]any{json1, json1, json3, json4}, int64(1)},
	}
	for _, tc := range testCases {
		fn, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tc.args...)))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(fn, ctx, chunk.MutRowFromDatums(types.MakeDatums(tc.args...)).ToRow())
		require.NoError(t, err)
		require.Equalf(t, tc.res, d.GetValue(), "%v", types.MakeDatums(tc.args))
	}
	strD1 := types.NewCollationStringDatum("a", "utf8_general_ci")
	strD2 := types.NewCollationStringDatum("Á", "utf8_general_ci")
	fn, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{strD1, strD2}))
	require.NoError(t, err)
	d, err := evalBuiltinFunc(fn, ctx, chunk.Row{})
	require.NoError(t, err)
	require.False(t, d.IsNull())
	require.Equal(t, types.KindInt64, d.Kind())
	require.Equalf(t, int64(1), d.GetInt64(), "%v, %v", strD1, strD2)
	chk1 := chunk.NewChunkWithCapacity(nil, 1)
	chk1.SetNumVirtualRows(1)
	chk2 := chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeTiny)}, 1)
	err = vecEvalType(ctx, fn, types.ETInt, chk1, chk2.Column(0))
	require.NoError(t, err)
	require.Equal(t, int64(1), chk2.Column(0).GetInt64(0))
}
