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
	"errors"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestCaseWhen(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		Arg []any
		Ret any
	}{
		{[]any{true, 1, true, 2, 3}, 1},
		{[]any{false, 1, true, 2, 3}, 2},
		{[]any{nil, 1, true, 2, 3}, 2},
		{[]any{false, 1, false, 2, 3}, 3},
		{[]any{nil, 1, nil, 2, 3}, 3},
		{[]any{false, 1, nil, 2, 3}, 3},
		{[]any{nil, 1, false, 2, 3}, 3},
		{[]any{1, jsonInt.GetMysqlJSON(), nil}, 3},
		{[]any{0, jsonInt.GetMysqlJSON(), nil}, nil},
		{[]any{0.1, 1, 2}, 1},
		{[]any{0.0, 1, 0.1, 2}, 2},
	}
	fc := funcs[ast.Case]
	for _, tt := range tbl {
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.Arg...)))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(tt.Ret), d)
	}
	f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(errors.New("can't convert string to bool"), 1, true)))
	require.NoError(t, err)
	_, err = evalBuiltinFunc(f, ctx, chunk.Row{})
	require.Error(t, err)
}

func TestIf(t *testing.T) {
	ctx := createContext(t)
	stmtCtx := ctx.GetSessionVars().StmtCtx
	oldTypeFlags := stmtCtx.TypeFlags()
	defer func() {
		stmtCtx.SetTypeFlags(oldTypeFlags)
	}()
	stmtCtx.SetTypeFlags(oldTypeFlags.WithIgnoreTruncateErr(true))
	tbl := []struct {
		Arg1 any
		Arg2 any
		Arg3 any
		Ret  any
	}{
		{1, 1, 2, 1},
		{nil, 1, 2, 2},
		{0, 1, 2, 2},
		{"abc", 1, 2, 2},
		{"1abc", 1, 2, 1},
		{tm, 1, 2, 1},
		{duration, 1, 2, 1},
		{types.Duration{Duration: time.Duration(0)}, 1, 2, 2},
		{types.NewDecFromStringForTest("1.2"), 1, 2, 1},
		{jsonInt.GetMysqlJSON(), 1, 2, 1},
		{0.1, 1, 2, 1},
		{0.0, 1, 2, 2},
		{types.NewDecFromStringForTest("0.1"), 1, 2, 1},
		{types.NewDecFromStringForTest("0.0"), 1, 2, 2},
		{"0.1", 1, 2, 1},
		{"0.0", 1, 2, 2},
	}

	fc := funcs[ast.If]
	for _, tt := range tbl {
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.Arg1, tt.Arg2, tt.Arg3)))
		require.NoError(t, err)
		d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(tt.Ret), d)
	}
	f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(errors.New("must error"), 1, 2)))
	require.NoError(t, err)
	_, err = evalBuiltinFunc(f, ctx, chunk.Row{})
	require.Error(t, err)
	_, err = fc.getFunction(ctx, datumsToConstants(types.MakeDatums(1, 2)))
	require.Error(t, err)
}

func TestIfNull(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		arg1     any
		arg2     any
		expected any
		isNil    bool
		getErr   bool
	}{
		{1, 2, int64(1), false, false},
		{nil, 2, int64(2), false, false},
		{nil, nil, nil, true, false},
		{tm, nil, tm, false, false},
		{nil, duration, duration, false, false},
		{nil, types.NewDecFromFloatForTest(123.123), types.NewDecFromFloatForTest(123.123), false, false},
		{nil, types.NewBinaryLiteralFromUint(0x01, -1), "\x01", false, false},
		{nil, types.Set{Value: 1, Name: "abc"}, "abc", false, false},
		{nil, jsonInt.GetMysqlJSON(), jsonInt.GetMysqlJSON(), false, false},
		{"abc", nil, "abc", false, false},
		{errors.New(""), nil, "", true, true},
	}

	for _, tt := range tbl {
		f, err := newFunctionForTest(ctx, ast.Ifnull, primitiveValsToConstants(ctx, []any{tt.arg1, tt.arg2})...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		if tt.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if tt.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, tt.expected, d.GetValue())
			}
		}
	}

	_, err := funcs[ast.Ifnull].getFunction(ctx, []Expression{NewZero(), NewZero()})
	require.NoError(t, err)

	_, err = funcs[ast.Ifnull].getFunction(ctx, []Expression{NewZero()})
	require.Error(t, err)
}
