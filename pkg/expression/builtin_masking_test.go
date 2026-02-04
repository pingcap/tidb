// Copyright 2026 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestMaskFull(t *testing.T) {
	ctx := createContext(t)

	f, err := newFunctionForTest(ctx, ast.MaskFull, primitiveValsToConstants(ctx, []any{"abc"})...)
	require.NoError(t, err)
	d, err := f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "XXXX", d.GetString())

	dateInput := types.NewTime(types.FromDate(2020, 1, 2, 0, 0, 0, 0), mysql.TypeDate, 0)
	f, err = newFunctionForTest(ctx, ast.MaskFull, primitiveValsToConstants(ctx, []any{dateInput})...)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "1970-01-01", d.GetMysqlTime().String())

	dtInput := types.NewTime(types.FromDate(2020, 1, 2, 3, 4, 5, 0), mysql.TypeDatetime, 0)
	f, err = newFunctionForTest(ctx, ast.MaskFull, primitiveValsToConstants(ctx, []any{dtInput})...)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "1970-01-01 00:00:00", d.GetMysqlTime().String())

	durationInput := types.Duration{Duration: time.Hour + time.Minute, Fsp: 0}
	f, err = newFunctionForTest(ctx, ast.MaskFull, primitiveValsToConstants(ctx, []any{durationInput})...)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "00:00:00", d.GetMysqlDuration().String())

	yearType := types.NewFieldType(mysql.TypeYear)
	yearArg := &Constant{Value: types.NewIntDatum(2020), RetType: yearType}
	f, err = newFunctionForTest(ctx, ast.MaskFull, yearArg)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, int64(1970), d.GetInt64())
}

func TestMaskNull(t *testing.T) {
	ctx := createContext(t)
	f, err := newFunctionForTest(ctx, ast.MaskNull, primitiveValsToConstants(ctx, []any{"abc"})...)
	require.NoError(t, err)
	d, err := f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.KindNull, d.Kind())
}

func TestMaskPartial(t *testing.T) {
	ctx := createContext(t)
	f, err := newFunctionForTest(ctx, ast.MaskPartial, primitiveValsToConstants(ctx, []any{"abcdef", "*", 1, 3})...)
	require.NoError(t, err)
	d, err := f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "a***ef", d.GetString())

	f, err = newFunctionForTest(ctx, ast.MaskPartial, primitiveValsToConstants(ctx, []any{"abcdef", "*", 6, 3})...)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "abcdef", d.GetString())

	f, err = newFunctionForTest(ctx, ast.MaskPartial, primitiveValsToConstants(ctx, []any{"abcdef", "*", 2, 0})...)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "abcdef", d.GetString())

	f, err = newFunctionForTest(ctx, ast.MaskPartial, primitiveValsToConstants(ctx, []any{"abcdef", "**", 1, 2})...)
	require.NoError(t, err)
	_, err = f.Eval(ctx, chunk.Row{})
	require.Error(t, err)
}

func TestMaskDate(t *testing.T) {
	ctx := createContext(t)
	dateInput := types.NewTime(types.FromDate(2019, 12, 30, 0, 0, 0, 0), mysql.TypeDate, 0)
	f, err := newFunctionForTest(ctx, ast.MaskDate, primitiveValsToConstants(ctx, []any{dateInput, "2020-01-02"})...)
	require.NoError(t, err)
	d, err := f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "2020-01-02", d.GetMysqlTime().String())

	dtInput := types.NewTime(types.FromDate(2019, 12, 30, 11, 22, 33, 0), mysql.TypeDatetime, 0)
	f, err = newFunctionForTest(ctx, ast.MaskDate, primitiveValsToConstants(ctx, []any{dtInput, "2020-01-02"})...)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "2020-01-02 00:00:00", d.GetMysqlTime().String())

	f, err = newFunctionForTest(ctx, ast.MaskDate, primitiveValsToConstants(ctx, []any{dtInput, "2020-1-2"})...)
	require.NoError(t, err)
	_, err = f.Eval(ctx, chunk.Row{})
	require.Error(t, err)
}
