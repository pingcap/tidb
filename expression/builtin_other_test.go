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

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
)

func TestBitCount(t *testing.T) {
	ctx := createContext(t)
	stmtCtx := ctx.GetSessionVars().StmtCtx
	origin := stmtCtx.IgnoreTruncate
	stmtCtx.IgnoreTruncate = true
	defer func() {
		stmtCtx.IgnoreTruncate = origin
	}()
	fc := funcs[ast.BitCount]
	var bitCountCases = []struct {
		origin interface{}
		count  interface{}
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
		count, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		if count.IsNull() {
			require.Nil(t, test.count)
			continue
		}
		sc := new(stmtctx.StatementContext)
		sc.IgnoreTruncate = true
		res, err := count.ToInt64(sc)
		require.NoError(t, err)
		require.Equal(t, test.count, res)
	}
}

func TestRowFunc(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.RowFunc]
	_, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums([]interface{}{"1", 1.2, true, 120}...)))
	require.NoError(t, err)
}

func TestSetVar(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.SetVar]
	dec := types.NewDecFromInt(5)
	timeDec := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 0)
	testCases := []struct {
		args []interface{}
		res  interface{}
	}{
		{[]interface{}{"a", "12"}, "12"},
		{[]interface{}{"b", "34"}, "34"},
		{[]interface{}{"c", nil}, nil},
		{[]interface{}{"c", "ABC"}, "ABC"},
		{[]interface{}{"c", "dEf"}, "dEf"},
		{[]interface{}{"d", int64(3)}, int64(3)},
		{[]interface{}{"e", float64(2.5)}, float64(2.5)},
		{[]interface{}{"f", dec}, dec},
		{[]interface{}{"g", timeDec}, timeDec},
	}
	for _, tc := range testCases {
		fn, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tc.args...)))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(fn, chunk.MutRowFromDatums(types.MakeDatums(tc.args...)).ToRow())
		require.NoError(t, err)
		require.Equal(t, tc.res, d.GetValue())
		if tc.args[1] != nil {
			key, ok := tc.args[0].(string)
			require.Equal(t, true, ok)
			sessionVar, ok := ctx.GetSessionVars().Users[key]
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
		val interface{}
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
		ctx.GetSessionVars().Users[kv.key] = types.NewDatum(kv.val)
		var tp *types.FieldType
		if _, ok := kv.val.(types.Time); ok {
			tp = types.NewFieldType(mysql.TypeDatetime)
		} else {
			tp = types.NewFieldType(mysql.TypeVarString)
		}
		types.DefaultParamTypeForValue(kv.val, tp)
		ctx.GetSessionVars().UserVarTypes[kv.key] = tp
	}

	testCases := []struct {
		args []interface{}
		res  interface{}
	}{
		{[]interface{}{"a"}, "中"},
		{[]interface{}{"b"}, "文字符chuan"},
		{[]interface{}{"c"}, ""},
		{[]interface{}{"d"}, nil},
		{[]interface{}{"e"}, int64(3)},
		{[]interface{}{"f"}, float64(2.5)},
		{[]interface{}{"g"}, dec},
		{[]interface{}{"h"}, timeDec.String()},
	}
	for _, tc := range testCases {
		tp, ok := ctx.GetSessionVars().UserVarTypes[tc.args[0].(string)]
		if !ok {
			tp = types.NewFieldType(mysql.TypeVarString)
		}
		fn, err := BuildGetVarFunction(ctx, datumsToConstants(types.MakeDatums(tc.args...))[0], tp)
		require.NoError(t, err)
		d, err := fn.Eval(chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, tc.res, d.GetValue())
	}
}

func TestValues(t *testing.T) {
	ctx := createContext(t)
	fc := &valuesFunctionClass{baseFunctionClass{ast.Values, 0, 0}, 1, types.NewFieldType(mysql.TypeVarchar)}
	_, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums("")))
	require.Error(t, err)
	require.Regexp(t, "Incorrect parameter count in the call to native function 'values'$", err.Error())

	sig, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums()))
	require.NoError(t, err)

	ret, err := evalBuiltinFunc(sig, chunk.Row{})
	require.NoError(t, err)
	require.True(t, ret.IsNull())

	ctx.GetSessionVars().CurrInsertValues = chunk.MutRowFromDatums(types.MakeDatums("1")).ToRow()
	ret, err = evalBuiltinFunc(sig, chunk.Row{})
	require.Error(t, err)
	require.Regexp(t, "^Session current insert values len", err.Error())

	currInsertValues := types.MakeDatums("1", "2")
	ctx.GetSessionVars().CurrInsertValues = chunk.MutRowFromDatums(currInsertValues).ToRow()
	ret, err = evalBuiltinFunc(sig, chunk.Row{})
	require.NoError(t, err)

	cmp, err := ret.Compare(nil, &currInsertValues[1], collate.GetBinaryCollator())
	require.NoError(t, err)
	require.Equal(t, 0, cmp)
}

func TestSetVarFromColumn(t *testing.T) {
	ctx := createContext(t)
	// Construct arguments.
	argVarName := &Constant{
		Value:   types.NewStringDatum("a"),
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Flen: 20},
	}
	argCol := &Column{
		RetType: &types.FieldType{Tp: mysql.TypeVarString, Flen: 20},
		Index:   0,
	}

	// Construct SetVar function.
	funcSetVar, err := NewFunction(
		ctx,
		ast.SetVar,
		&types.FieldType{Tp: mysql.TypeVarString, Flen: 20},
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
	sessionVars.UsersLock.RLock()
	defer sessionVars.UsersLock.RUnlock()
	sessionVar, ok := sessionVars.Users["a"]
	require.Equal(t, true, ok)
	require.Equal(t, "a", sessionVar.GetString())
}
