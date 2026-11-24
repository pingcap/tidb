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
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
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
	require.Equal(t, "XXX", d.GetString())

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
	require.Equal(t, int64(0), d.GetInt64())

	// Test REAL (DOUBLE) type - mask_full returns 0.0
	realType := types.NewFieldType(mysql.TypeDouble)
	realArg := &Constant{Value: types.NewFloat64Datum(123.456), RetType: realType}
	f, err = newFunctionForTest(ctx, ast.MaskFull, realArg)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, float64(0), d.GetFloat64())

	// Test DECIMAL type - mask_full returns zero decimal
	decType := types.NewFieldType(mysql.TypeNewDecimal)
	decArg := &Constant{Value: types.NewDecimalDatum(types.NewDecFromStringForTest("123.456")), RetType: decType}
	f, err = newFunctionForTest(ctx, ast.MaskFull, decArg)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "0", d.GetMysqlDecimal().String())
}

func TestMaskNull(t *testing.T) {
	ctx := createContext(t)
	f, err := newFunctionForTest(ctx, ast.MaskNull, primitiveValsToConstants(ctx, []any{"abc"})...)
	require.NoError(t, err)
	d, err := f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.KindNull, d.Kind())

	f, err = newFunctionForTest(ctx, ast.MaskNull, primitiveValsToConstants(ctx, []any{123})...)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.KindNull, d.Kind())

	decType := types.NewFieldType(mysql.TypeNewDecimal)
	decArg := &Constant{Value: types.NewDecimalDatum(types.NewDecFromStringForTest("85000.00")), RetType: decType}
	f, err = newFunctionForTest(ctx, ast.MaskNull, decArg)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.KindNull, d.Kind())
}

func TestMaskPartial(t *testing.T) {
	ctx := createContext(t)
	f, err := newFunctionForTest(ctx, ast.MaskPartial, primitiveValsToConstants(ctx, []any{"abcdef", 1, 2, "*"})...)
	require.NoError(t, err)
	d, err := f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "a***ef", d.GetString())

	f, err = newFunctionForTest(ctx, ast.MaskPartial, primitiveValsToConstants(ctx, []any{"abcdef", 6, 0, "*"})...)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "abcdef", d.GetString())

	f, err = newFunctionForTest(ctx, ast.MaskPartial, primitiveValsToConstants(ctx, []any{"abcdef", 0, 0, "*"})...)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "******", d.GetString())

	f, err = newFunctionForTest(ctx, ast.MaskPartial, primitiveValsToConstants(ctx, []any{"abcdef", 1, 2, "**"})...)
	require.NoError(t, err)
	_, err = f.Eval(ctx, chunk.Row{})
	require.Error(t, err)

	f, err = newFunctionForTest(ctx, ast.MaskPartial, primitiveValsToConstants(ctx, []any{"abcdef", -1, 0, "*"})...)
	require.NoError(t, err)
	_, err = f.Eval(ctx, chunk.Row{})
	require.Error(t, err)

	f, err = newFunctionForTest(ctx, ast.MaskPartial, primitiveValsToConstants(ctx, []any{"abcdef", math.MaxInt64, math.MaxInt64 - 1, "*"})...)
	require.NoError(t, err)
	require.NotPanics(t, func() {
		d, evalErr := f.Eval(ctx, chunk.Row{})
		require.NoError(t, evalErr)
		require.Equal(t, "abcdef", d.GetString())
	})

	// Non-string args[0] is rejected at build time.
	_, err = newFunctionForTest(ctx, ast.MaskPartial, primitiveValsToConstants(ctx, []any{12345, 1, 2, "*"})...)
	require.Error(t, err)

	// A binary pad must not switch a text input to byte masking: the result
	// keeps the text type, so cutting inside a character would corrupt it.
	binaryPad := &Constant{Value: types.NewBytesDatum([]byte("*")), RetType: types.NewFieldType(mysql.TypeVarString)}
	binaryPad.RetType.SetCharset(charset.CharsetBin)
	binaryPad.RetType.SetCollate(charset.CollationBin)
	binaryPad.RetType.AddFlag(mysql.BinaryFlag)
	textArgs := append(primitiveValsToConstants(ctx, []any{"中文字", 1, 1}), binaryPad)
	f, err = newFunctionForTest(ctx, ast.MaskPartial, textArgs...)
	require.NoError(t, err)
	require.Equal(t, charset.CharsetUTF8MB4, f.GetType(ctx).GetCharset())
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "中*字", d.GetString())
	// A binary input is still masked by bytes.
	binaryInput := &Constant{Value: types.NewBytesDatum([]byte("中文字")), RetType: binaryPad.RetType.Clone()}
	f, err = newFunctionForTest(ctx, ast.MaskPartial, append([]Expression{binaryInput}, textArgs[1:]...)...)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "\xe4*******\x97", d.GetString())
}

func TestMaskPartialBinaryPadPreservesTextCharacters(t *testing.T) {
	ctx := createContext(t)
	args := primitiveValsToConstants(ctx, []any{"中文甲乙", 1, 1, []byte("*")})
	f, err := newFunctionForTest(ctx, ast.MaskPartial, args...)
	require.NoError(t, err)
	d, err := f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "中**乙", d.GetString())
	require.Equal(t, args[0].GetType(ctx).GetCharset(), f.GetType(ctx).GetCharset())
	args = primitiveValsToConstants(ctx, []any{[]byte("abcdef"), 1, 1, []byte("*")})
	f, err = newFunctionForTest(ctx, ast.MaskPartial, args...)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "a****f", d.GetString())
}

func TestMaskDate(t *testing.T) {
	ctx := createContext(t)
	dateInput := types.NewTime(types.FromDate(2019, 12, 30, 0, 0, 0, 0), mysql.TypeDate, 0)
	// Fixed-literal template (all components redacted): backward compatible.
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

	// Template placeholders preserve the corresponding input component.
	f, err = newFunctionForTest(ctx, ast.MaskDate, primitiveValsToConstants(ctx, []any{dateInput, "Y-01-01"})...)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "2019-01-01", d.GetMysqlTime().String())

	f, err = newFunctionForTest(ctx, ast.MaskDate, primitiveValsToConstants(ctx, []any{dateInput, "Y-M-D"})...)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "2019-12-30", d.GetMysqlTime().String())

	// DATETIME keeps the date masking result but drops the time component.
	f, err = newFunctionForTest(ctx, ast.MaskDate, primitiveValsToConstants(ctx, []any{dtInput, "Y-06-15"})...)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "2019-06-15 00:00:00", d.GetMysqlTime().String())

	// Non-zero-padded fixed values are accepted by the template.
	f, err = newFunctionForTest(ctx, ast.MaskDate, primitiveValsToConstants(ctx, []any{dtInput, "2020-1-2"})...)
	require.NoError(t, err)
	d, err = f.Eval(ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "2020-01-02 00:00:00", d.GetMysqlTime().String())

	// Out-of-range month is rejected after reassembly.
	f, err = newFunctionForTest(ctx, ast.MaskDate, primitiveValsToConstants(ctx, []any{dtInput, "Y-13-01"})...)
	require.NoError(t, err)
	_, err = f.Eval(ctx, chunk.Row{})
	require.Error(t, err)

	// A non-placeholder, non-digit component is rejected.
	f, err = newFunctionForTest(ctx, ast.MaskDate, primitiveValsToConstants(ctx, []any{dtInput, "Y-1x-01"})...)
	require.NoError(t, err)
	_, err = f.Eval(ctx, chunk.Row{})
	require.Error(t, err)
}
