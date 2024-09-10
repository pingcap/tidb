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
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestLengthAndOctetLength(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args     any
		expected int64
		isNil    bool
		getErr   bool
	}{
		{"abc", 3, false, false},
		{"你好", 6, false, false},
		{1, 1, false, false},
		{3.14, 4, false, false},
		{types.NewDecFromFloatForTest(123.123), 7, false, false},
		{types.NewTime(types.FromGoTime(time.Now()), mysql.TypeDatetime, 6), 26, false, false},
		{types.NewBinaryLiteralFromUint(0x01, -1), 1, false, false},
		{types.Set{Value: 1, Name: "abc"}, 3, false, false},
		{types.Duration{Duration: 12*time.Hour + 1*time.Minute + 1*time.Second, Fsp: types.DefaultFsp}, 8, false, false},
		{nil, 0, true, false},
		{errors.New("must error"), 0, false, true},
	}

	lengthMethods := []string{ast.Length, ast.OctetLength}
	for _, lengthMethod := range lengthMethods {
		for _, c := range cases {
			f, err := newFunctionForTest(ctx, lengthMethod, primitiveValsToConstants(ctx, []any{c.args})...)
			require.NoError(t, err)
			d, err := f.Eval(ctx, chunk.Row{})
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
	}

	_, err := funcs[ast.Length].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)

	// Test GBK String
	tbl := []struct {
		input  string
		chs    string
		result int64
	}{
		{"abc", "gbk", 3},
		{"一二三", "gbk", 6},
		{"一二三", "", 9},
		{"一二三!", "gbk", 7},
		{"一二三!", "", 10},
	}
	for _, lengthMethod := range lengthMethods {
		for _, c := range tbl {
			err := ctx.GetSessionVars().SetSystemVarWithoutValidation(variable.CharacterSetConnection, c.chs)
			require.NoError(t, err)
			f, err := newFunctionForTest(ctx, lengthMethod, primitiveValsToConstants(ctx, []any{c.input})...)
			require.NoError(t, err)
			d, err := f.Eval(ctx, chunk.Row{})
			require.NoError(t, err)
			require.Equal(t, c.result, d.GetInt64())
		}
	}
}

func TestASCII(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args     any
		expected int64
		isNil    bool
		getErr   bool
	}{
		{"2", 50, false, false},
		{2, 50, false, false},
		{"23", 50, false, false},
		{23, 50, false, false},
		{2.3, 50, false, false},
		{nil, 0, true, false},
		{"", 0, false, false},
		{"你好", 228, false, false},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.ASCII, primitiveValsToConstants(ctx, []any{c.args})...)
		require.NoError(t, err)

		d, err := f.Eval(ctx, chunk.Row{})
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
	_, err := funcs[ast.Length].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)

	// Test GBK String
	tbl := []struct {
		input  string
		chs    string
		result int64
	}{
		{"abc", "gbk", 97},
		{"你好", "gbk", 196},
		{"你好", "", 228},
		{"世界", "gbk", 202},
		{"世界", "", 228},
	}

	for _, c := range tbl {
		err := ctx.GetSessionVars().SetSystemVarWithoutValidation(variable.CharacterSetConnection, c.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.ASCII, primitiveValsToConstants(ctx, []any{c.input})...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, c.result, d.GetInt64())
	}
}

func TestConcat(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args    []any
		isNil   bool
		getErr  bool
		res     string
		retType *types.FieldType
	}{
		{
			[]any{nil},
			true, false, "",
			types.NewFieldTypeBuilder().SetType(mysql.TypeVarString).SetFlag(mysql.BinaryFlag).SetDecimal(types.UnspecifiedLength).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin).BuildP(),
		},
		{
			[]any{"a", "b",
				1, 2,
				1.1, 1.2,
				types.NewDecFromFloatForTest(1.1),
				types.NewTime(types.FromDate(2000, 1, 1, 12, 01, 01, 0), mysql.TypeDatetime, types.DefaultFsp),
				types.Duration{
					Duration: 12*time.Hour + 1*time.Minute + 1*time.Second,
					Fsp:      types.DefaultFsp},
			},
			false, false, "ab121.11.21.12000-01-01 12:01:0112:01:01",
			types.NewFieldTypeBuilder().SetType(mysql.TypeVarString).SetFlag(mysql.BinaryFlag).SetFlen(40).SetDecimal(types.UnspecifiedLength).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin).BuildP(),
		},
		{
			[]any{"a", "b", nil, "c"},
			true, false, "",
			types.NewFieldTypeBuilder().SetType(mysql.TypeVarString).SetFlag(mysql.BinaryFlag).SetFlen(3).SetDecimal(types.UnspecifiedLength).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin).BuildP(),
		},
		{
			[]any{errors.New("must error")},
			false, true, "",
			types.NewFieldTypeBuilder().SetType(mysql.TypeVarString).SetFlag(mysql.BinaryFlag).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin).BuildP(),
		},
	}
	fcName := ast.Concat
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, fcName, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		v, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, v.Kind())
			} else {
				require.Equal(t, c.res, v.GetString())
			}
		}
	}
}

func TestConcatSig(t *testing.T) {
	ctx := createContext(t)
	colTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeVarchar),
	}

	resultType := &types.FieldType{}
	resultType.SetType(mysql.TypeVarchar)
	resultType.SetFlen(1000)
	args := []Expression{
		&Column{Index: 0, RetType: colTypes[0]},
		&Column{Index: 1, RetType: colTypes[1]},
	}
	base := baseBuiltinFunc{args: args, tp: resultType}
	concat := &builtinConcatSig{base, 5}

	cases := []struct {
		args     []any
		warnings int
		res      string
	}{
		{[]any{"a", "b"}, 0, "ab"},
		{[]any{"aaa", "bbb"}, 1, ""},
		{[]any{"中", "a"}, 0, "中a"},
		{[]any{"中文", "a"}, 2, ""},
	}

	for _, c := range cases {
		input := chunk.NewChunkWithCapacity(colTypes, 10)
		input.AppendString(0, c.args[0].(string))
		input.AppendString(1, c.args[1].(string))

		res, isNull, err := concat.evalString(ctx, input.GetRow(0))
		require.Equal(t, c.res, res)
		require.NoError(t, err)
		if c.warnings == 0 {
			require.False(t, isNull)
		} else {
			require.True(t, isNull)
			warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
			require.Len(t, warnings, c.warnings)
			lastWarn := warnings[len(warnings)-1]
			require.True(t, terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err))
		}
	}
}

func TestConcatWS(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args     []any
		isNil    bool
		getErr   bool
		expected string
	}{
		{
			[]any{nil, nil},
			true, false, "",
		},
		{
			[]any{nil, "a", "b"},
			true, false, "",
		},
		{
			[]any{",", "a", "b", "hello", `$^%`},
			false, false,
			`a,b,hello,$^%`,
		},
		{
			[]any{"|", "a", nil, "b", "c"},
			false, false,
			"a|b|c",
		},
		{
			[]any{",", "a", ",", "b", "c"},
			false, false,
			"a,,,b,c",
		},
		{
			[]any{errors.New("must error"), "a", "b"},
			false, true, "",
		},
		{
			[]any{",", "a", "b", 1, 2, 1.1, 0.11,
				types.NewDecFromFloatForTest(1.1),
				types.NewTime(types.FromDate(2000, 1, 1, 12, 01, 01, 0), mysql.TypeDatetime, types.DefaultFsp),
				types.Duration{
					Duration: 12*time.Hour + 1*time.Minute + 1*time.Second,
					Fsp:      types.DefaultFsp},
			},
			false, false, "a,b,1,2,1.1,0.11,1.1,2000-01-01 12:01:01,12:01:01",
		},
	}

	fcName := ast.ConcatWS
	// ERROR 1582 (42000): Incorrect parameter count in the call to native function 'concat_ws'
	_, err := newFunctionForTest(ctx, fcName, primitiveValsToConstants(ctx, []any{nil})...)
	require.Error(t, err)

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, fcName, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		val, err1 := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.NotNil(t, err1)
		} else {
			require.Nil(t, err1)
			if c.isNil {
				require.Equal(t, types.KindNull, val.Kind())
			} else {
				require.Equal(t, c.expected, val.GetString())
			}
		}
	}

	_, err = funcs[ast.ConcatWS].getFunction(ctx, primitiveValsToConstants(ctx, []any{nil, nil}))
	require.NoError(t, err)
}

func TestConcatWSSig(t *testing.T) {
	ctx := createContext(t)
	colTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeVarchar),
	}
	resultType := &types.FieldType{}
	resultType.SetType(mysql.TypeVarchar)
	resultType.SetFlen(1000)
	args := []Expression{
		&Column{Index: 0, RetType: colTypes[0]},
		&Column{Index: 1, RetType: colTypes[1]},
		&Column{Index: 2, RetType: colTypes[2]},
	}
	base := baseBuiltinFunc{args: args, tp: resultType}
	concat := &builtinConcatWSSig{base, 6}

	cases := []struct {
		args     []any
		warnings int
		res      string
	}{
		{[]any{",", "a", "b"}, 0, "a,b"},
		{[]any{",", "aaa", "bbb"}, 1, ""},
		{[]any{",", "中", "a"}, 0, "中,a"},
		{[]any{",", "中文", "a"}, 2, ""},
	}

	for _, c := range cases {
		input := chunk.NewChunkWithCapacity(colTypes, 10)
		input.AppendString(0, c.args[0].(string))
		input.AppendString(1, c.args[1].(string))
		input.AppendString(2, c.args[2].(string))

		res, isNull, err := concat.evalString(ctx, input.GetRow(0))
		require.Equal(t, c.res, res)
		require.NoError(t, err)
		if c.warnings == 0 {
			require.False(t, isNull)
		} else {
			require.True(t, isNull)
			warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
			require.Len(t, warnings, c.warnings)
			lastWarn := warnings[len(warnings)-1]
			require.True(t, terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err))
		}
	}
}

func TestLeft(t *testing.T) {
	ctx := createContext(t)
	stmtCtx := ctx.GetSessionVars().StmtCtx
	oldTypeFlags := stmtCtx.TypeFlags()
	defer func() {
		stmtCtx.SetTypeFlags(oldTypeFlags)
	}()
	stmtCtx.SetTypeFlags(oldTypeFlags.WithIgnoreTruncateErr(true))

	cases := []struct {
		args   []any
		isNil  bool
		getErr bool
		res    string
	}{
		{[]any{"abcde", 3}, false, false, "abc"},
		{[]any{"abcde", 0}, false, false, ""},
		{[]any{"abcde", 1.2}, false, false, "a"},
		{[]any{"abcde", 1.9}, false, false, "ab"},
		{[]any{"abcde", -1}, false, false, ""},
		{[]any{"abcde", 100}, false, false, "abcde"},
		{[]any{"abcde", nil}, true, false, ""},
		{[]any{nil, 3}, true, false, ""},
		{[]any{"abcde", "3"}, false, false, "abc"},
		{[]any{"abcde", "a"}, false, false, ""},
		{[]any{1234, 3}, false, false, "123"},
		{[]any{12.34, 3}, false, false, "12."},
		{[]any{types.NewBinaryLiteralFromUint(0x0102, -1), 1}, false, false, string([]byte{0x01})},
		{[]any{errors.New("must err"), 0}, false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Left, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		v, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, v.Kind())
			} else {
				require.Equal(t, c.res, v.GetString())
			}
		}
	}

	_, err := funcs[ast.Left].getFunction(ctx, []Expression{getVarcharCon(), getInt8Con()})
	require.NoError(t, err)
}

func TestRight(t *testing.T) {
	ctx := createContext(t)
	stmtCtx := ctx.GetSessionVars().StmtCtx
	oldTypeFlags := stmtCtx.TypeFlags()
	defer func() {
		stmtCtx.SetTypeFlags(oldTypeFlags)
	}()
	stmtCtx.SetTypeFlags(oldTypeFlags.WithIgnoreTruncateErr(true))

	cases := []struct {
		args   []any
		isNil  bool
		getErr bool
		res    string
	}{
		{[]any{"abcde", 3}, false, false, "cde"},
		{[]any{"abcde", 0}, false, false, ""},
		{[]any{"abcde", 1.2}, false, false, "e"},
		{[]any{"abcde", 1.9}, false, false, "de"},
		{[]any{"abcde", -1}, false, false, ""},
		{[]any{"abcde", 100}, false, false, "abcde"},
		{[]any{"abcde", nil}, true, false, ""},
		{[]any{nil, 1}, true, false, ""},
		{[]any{"abcde", "3"}, false, false, "cde"},
		{[]any{"abcde", "a"}, false, false, ""},
		{[]any{1234, 3}, false, false, "234"},
		{[]any{12.34, 3}, false, false, ".34"},
		{[]any{types.NewBinaryLiteralFromUint(0x0102, -1), 1}, false, false, string([]byte{0x02})},
		{[]any{errors.New("must err"), 0}, false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Right, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		v, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, v.Kind())
			} else {
				require.Equal(t, c.res, v.GetString())
			}
		}
	}

	_, err := funcs[ast.Right].getFunction(ctx, []Expression{getVarcharCon(), getInt8Con()})
	require.NoError(t, err)
}

func TestRepeat(t *testing.T) {
	cases := []struct {
		args   []any
		isNull bool
		res    string
	}{
		{[]any{"a", int64(2)}, false, "aa"},
		{[]any{"a", uint64(16777217)}, false, strings.Repeat("a", 16777217)},
		{[]any{"a", int64(16777216)}, false, strings.Repeat("a", 16777216)},
		{[]any{"a", int64(-1)}, false, ""},
		{[]any{"a", int64(0)}, false, ""},
		{[]any{"a", uint64(0)}, false, ""},
	}

	ctx := createContext(t)
	fc := funcs[ast.Repeat]
	for _, c := range cases {
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(c.args...)))
		require.NoError(t, err)
		v, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		if c.isNull {
			require.True(t, v.IsNull())
		} else {
			require.Equal(t, v.GetString(), c.res)
		}
	}
}

func TestRepeatSig(t *testing.T) {
	ctx := createContext(t)
	colTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeLonglong),
	}
	resultType := &types.FieldType{}
	resultType.SetType(mysql.TypeVarchar)
	resultType.SetFlen(1000)
	args := []Expression{
		&Column{Index: 0, RetType: colTypes[0]},
		&Column{Index: 1, RetType: colTypes[1]},
	}
	base := baseBuiltinFunc{args: args, tp: resultType}
	repeat := &builtinRepeatSig{base, 1000}

	cases := []struct {
		args    []any
		warning int
		res     string
	}{
		{[]any{"a", int64(6)}, 0, "aaaaaa"},
		{[]any{"a", int64(10001)}, 1, ""},
		{[]any{"毅", int64(6)}, 0, "毅毅毅毅毅毅"},
		{[]any{"毅", int64(334)}, 2, ""},
	}

	for _, c := range cases {
		input := chunk.NewChunkWithCapacity(colTypes, 10)
		input.AppendString(0, c.args[0].(string))
		input.AppendInt64(1, c.args[1].(int64))

		res, isNull, err := repeat.evalString(ctx, input.GetRow(0))
		require.Equal(t, c.res, res)
		require.NoError(t, err)
		if c.warning == 0 {
			require.False(t, isNull)
		} else {
			require.True(t, isNull)
			require.NoError(t, err)
			warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
			require.Len(t, warnings, c.warning)
			lastWarn := warnings[len(warnings)-1]
			require.True(t, terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err))
		}
	}
}

func TestLower(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args   []any
		isNil  bool
		getErr bool
		res    string
	}{
		{[]any{nil}, true, false, ""},
		{[]any{"ab"}, false, false, "ab"},
		{[]any{1}, false, false, "1"},
		{[]any{"one week’s time TEST"}, false, false, "one week’s time test"},
		{[]any{"one week's time TEST"}, false, false, "one week's time test"},
		{[]any{"ABC测试DEF"}, false, false, "abc测试def"},
		{[]any{"ABCテストDEF"}, false, false, "abcテストdef"},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Lower, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		v, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, v.Kind())
			} else {
				require.Equal(t, c.res, v.GetString())
			}
		}
	}

	_, err := funcs[ast.Lower].getFunction(ctx, []Expression{getVarcharCon()})
	require.NoError(t, err)

	// Test GBK String
	tbl := []struct {
		input  string
		chs    string
		result string
	}{
		{"ABC", "gbk", "abc"},
		{"一二三", "gbk", "一二三"},
		{"àáèéêìíòóùúüāēěīńňōūǎǐǒǔǖǘǚǜⅪⅫ", "gbk", "àáèéêìíòóùúüāēěīńňōūǎǐǒǔǖǘǚǜⅪⅫ"},
		{"àáèéêìíòóùúüāēěīńňōūǎǐǒǔǖǘǚǜⅪⅫ", "", "àáèéêìíòóùúüāēěīńňōūǎǐǒǔǖǘǚǜⅺⅻ"},
	}
	for _, c := range tbl {
		err := ctx.GetSessionVars().SetSystemVarWithoutValidation(variable.CharacterSetConnection, c.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.Lower, primitiveValsToConstants(ctx, []any{c.input})...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, c.result, d.GetString())
	}
}

func TestUpper(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args   []any
		isNil  bool
		getErr bool
		res    string
	}{
		{[]any{nil}, true, false, ""},
		{[]any{"ab"}, false, false, "ab"},
		{[]any{1}, false, false, "1"},
		{[]any{"one week’s time TEST"}, false, false, "ONE WEEK’S TIME TEST"},
		{[]any{"one week's time TEST"}, false, false, "ONE WEEK'S TIME TEST"},
		{[]any{"abc测试def"}, false, false, "ABC测试DEF"},
		{[]any{"abcテストdef"}, false, false, "ABCテストDEF"},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Upper, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		v, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, v.Kind())
			} else {
				require.Equal(t, strings.ToUpper(c.res), v.GetString())
			}
		}
	}

	_, err := funcs[ast.Upper].getFunction(ctx, []Expression{getVarcharCon()})
	require.NoError(t, err)

	// Test GBK String
	tbl := []struct {
		input  string
		chs    string
		result string
	}{
		{"abc", "gbk", "ABC"},
		{"一二三", "gbk", "一二三"},
		{"àbc", "gbk", "àBC"},
		{"àáèéêìíòóùúüāēěīńňōūǎǐǒǔǖǘǚǜⅪⅫ", "gbk", "àáèéêìíòóùúüāēěīńňōūǎǐǒǔǖǘǚǜⅪⅫ"},
		{"àáèéêìíòóùúüāēěīńňōūǎǐǒǔǖǘǚǜⅪⅫ", "", "ÀÁÈÉÊÌÍÒÓÙÚÜĀĒĚĪŃŇŌŪǍǏǑǓǕǗǙǛⅪⅫ"},
	}
	for _, c := range tbl {
		err := ctx.GetSessionVars().SetSystemVarWithoutValidation(variable.CharacterSetConnection, c.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.Upper, primitiveValsToConstants(ctx, []any{c.input})...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, c.result, d.GetString())
	}
}

func TestReverse(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.Reverse]
	f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(nil)))
	require.NoError(t, err)
	d, err := evalBuiltinFunc(f, ctx, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.KindNull, d.Kind())

	tbl := []struct {
		Input  any
		Expect string
	}{
		{"abc", "cba"},
		{"LIKE", "EKIL"},
		{123, "321"},
		{"", ""},
	}

	dtbl := tblToDtbl(tbl)

	for _, c := range dtbl {
		f, err = fc.getFunction(ctx, datumsToConstants(c["Input"]))
		require.NoError(t, err)
		require.NotNil(t, f)
		d, err = evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, c["Expect"][0], d)
	}
}

func TestStrcmp(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args   []any
		isNil  bool
		getErr bool
		res    int64
	}{
		{[]any{"123", "123"}, false, false, 0},
		{[]any{"123", "1"}, false, false, 1},
		{[]any{"1", "123"}, false, false, -1},
		{[]any{"123", "45"}, false, false, -1},
		{[]any{123, "123"}, false, false, 0},
		{[]any{"12.34", 12.34}, false, false, 0},
		{[]any{nil, "123"}, true, false, 0},
		{[]any{"123", nil}, true, false, 0},
		{[]any{"", "123"}, false, false, -1},
		{[]any{"123", ""}, false, false, 1},
		{[]any{"", ""}, false, false, 0},
		{[]any{"", nil}, true, false, 0},
		{[]any{nil, ""}, true, false, 0},
		{[]any{nil, nil}, true, false, 0},
		{[]any{"123", errors.New("must err")}, false, true, 0},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Strcmp, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.res, d.GetInt64())
			}
		}
	}
}

func TestReplace(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args   []any
		isNil  bool
		getErr bool
		res    string
		flen   int
	}{
		{[]any{"www.mysql.com", "mysql", "pingcap"}, false, false, "www.pingcap.com", 17},
		{[]any{"www.mysql.com", "w", 1}, false, false, "111.mysql.com", 260},
		{[]any{1234, 2, 55}, false, false, "15534", 20},
		{[]any{"", "a", "b"}, false, false, "", 0},
		{[]any{"abc", "", "d"}, false, false, "abc", 3},
		{[]any{"aaa", "a", ""}, false, false, "", 3},
		{[]any{nil, "a", "b"}, true, false, "", 0},
		{[]any{"a", nil, "b"}, true, false, "", 1},
		{[]any{"a", "b", nil}, true, false, "", 1},
		{[]any{errors.New("must err"), "a", "b"}, false, true, "", -1},
	}
	for i, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Replace, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		require.Equalf(t, c.flen, f.GetType(ctx).GetFlen(), "test %v", i)
		d, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equalf(t, types.KindNull, d.Kind(), "test %v", i)
			} else {
				require.Equalf(t, c.res, d.GetString(), "test %v", i)
			}
		}
	}

	_, err := funcs[ast.Replace].getFunction(ctx, []Expression{NewZero(), NewZero(), NewZero()})
	require.NoError(t, err)
}

func TestSubstring(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args   []any
		isNil  bool
		getErr bool
		res    string
	}{
		{[]any{"Quadratically", 5}, false, false, "ratically"},
		{[]any{"Sakila", 1}, false, false, "Sakila"},
		{[]any{"Sakila", 2}, false, false, "akila"},
		{[]any{"Sakila", -3}, false, false, "ila"},
		{[]any{"Sakila", 0}, false, false, ""},
		{[]any{"Sakila", 100}, false, false, ""},
		{[]any{"Sakila", -100}, false, false, ""},
		{[]any{"Quadratically", 5, 6}, false, false, "ratica"},
		{[]any{"Sakila", -5, 3}, false, false, "aki"},
		{[]any{"Sakila", 2, 0}, false, false, ""},
		{[]any{"Sakila", 2, -1}, false, false, ""},
		{[]any{"Sakila", 2, 100}, false, false, "akila"},
		{[]any{nil, 2, 3}, true, false, ""},
		{[]any{"Sakila", nil, 3}, true, false, ""},
		{[]any{"Sakila", 2, nil}, true, false, ""},
		{[]any{errors.New("must error"), 2, 3}, false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Substring, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.res, d.GetString())
			}
		}
	}

	_, err := funcs[ast.Substring].getFunction(ctx, []Expression{NewZero(), NewZero(), NewZero()})
	require.NoError(t, err)

	_, err = funcs[ast.Substring].getFunction(ctx, []Expression{NewZero(), NewZero()})
	require.NoError(t, err)
}

func TestConvert(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		str           any
		cs            string
		result        string
		hasBinaryFlag bool
	}{
		{"haha", "utf8", "haha", false},
		{"haha", "ascii", "haha", false},
		{"haha", "binary", "haha", true},
		{"haha", "bInAry", "haha", true},
		{types.NewBinaryLiteralFromUint(0x7e, -1), "BiNarY", "~", true},
		{types.NewBinaryLiteralFromUint(0xe4b8ade696870a, -1), "uTf8", "中文\n", false},
	}
	for _, v := range tbl {
		fc := funcs[ast.Convert]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(v.str, v.cs)))
		require.NoError(t, err)
		require.NotNil(t, f)
		retType := f.getRetTp()
		require.Equal(t, strings.ToLower(v.cs), retType.GetCharset())
		collate, err := charset.GetDefaultCollation(strings.ToLower(v.cs))
		require.NoError(t, err)
		require.Equal(t, collate, retType.GetCollate())
		require.Equal(t, v.hasBinaryFlag, mysql.HasBinaryFlag(retType.GetFlag()))

		r, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, types.KindString, r.Kind())
		require.Equal(t, v.result, r.GetString())
	}

	// Test case for getFunction() error
	errTbl := []struct {
		str any
		cs  string
		err string
	}{
		{"haha", "wrongcharset", "[expression:1115]Unknown character set: 'wrongcharset'"},
		{"haha", "cp866", "[expression:1115]Unknown character set: 'cp866'"},
	}
	for _, v := range errTbl {
		fc := funcs[ast.Convert]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(v.str, v.cs)))
		require.Equal(t, v.err, err.Error())
		require.Nil(t, f)
	}

	// Test wrong charset while evaluating.
	fc := funcs[ast.Convert]
	f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums("haha", "utf8")))
	require.NoError(t, err)
	require.NotNil(t, f)
	wrongFunction := f.(*builtinConvertSig)
	wrongFunction.tp.SetCharset("wrongcharset")
	_, err = evalBuiltinFunc(wrongFunction, ctx, chunk.Row{})
	require.Error(t, err)
	require.Equal(t, "[expression:1115]Unknown character set: 'wrongcharset'", err.Error())
}

func TestSubstringIndex(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args   []any
		isNil  bool
		getErr bool
		res    string
	}{
		{[]any{"www.pingcap.com", ".", 2}, false, false, "www.pingcap"},
		{[]any{"www.pingcap.com", ".", -2}, false, false, "pingcap.com"},
		{[]any{"www.pingcap.com", ".", 0}, false, false, ""},
		{[]any{"www.pingcap.com", ".", 100}, false, false, "www.pingcap.com"},
		{[]any{"www.pingcap.com", ".", -100}, false, false, "www.pingcap.com"},
		{[]any{"www.pingcap.com", "d", 0}, false, false, ""},
		{[]any{"www.pingcap.com", "d", 1}, false, false, "www.pingcap.com"},
		{[]any{"www.pingcap.com", "d", -1}, false, false, "www.pingcap.com"},
		{[]any{"www.pingcap.com", "", 0}, false, false, ""},
		{[]any{"www.pingcap.com", "", 1}, false, false, ""},
		{[]any{"www.pingcap.com", "", -1}, false, false, ""},
		{[]any{"", ".", 0}, false, false, ""},
		{[]any{"", ".", 1}, false, false, ""},
		{[]any{"", ".", -1}, false, false, ""},
		{[]any{nil, ".", 1}, true, false, ""},
		{[]any{"www.pingcap.com", nil, 1}, true, false, ""},
		{[]any{"www.pingcap.com", ".", nil}, true, false, ""},
		{[]any{errors.New("must error"), ".", 1}, false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.SubstringIndex, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.res, d.GetString())
			}
		}
	}

	_, err := funcs[ast.SubstringIndex].getFunction(ctx, []Expression{NewZero(), NewZero(), NewZero()})
	require.NoError(t, err)
}

func TestSpace(t *testing.T) {
	ctx := createContext(t)
	stmtCtx := ctx.GetSessionVars().StmtCtx
	oldTypeFlags := stmtCtx.TypeFlags()
	defer func() {
		stmtCtx.SetTypeFlags(oldTypeFlags)
	}()
	stmtCtx.SetTypeFlags(oldTypeFlags.WithIgnoreTruncateErr(true))

	cases := []struct {
		arg    any
		isNil  bool
		getErr bool
		res    string
	}{
		{0, false, false, ""},
		{3, false, false, "   "},
		{mysql.MaxBlobWidth + 1, true, false, ""},
		{-1, false, false, ""},
		{"abc", false, false, ""},
		{"3", false, false, "   "},
		{1.2, false, false, " "},
		{1.9, false, false, "  "},
		{nil, true, false, ""},
		{errors.New("must error"), false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Space, primitiveValsToConstants(ctx, []any{c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.res, d.GetString())
			}
		}
	}

	_, err := funcs[ast.Space].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestSpaceSig(t *testing.T) {
	ctx := createContext(t)
	colTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
	}
	resultType := &types.FieldType{}
	resultType.SetType(mysql.TypeVarchar)
	resultType.SetFlen(1000)
	args := []Expression{
		&Column{Index: 0, RetType: colTypes[0]},
	}
	base := baseBuiltinFunc{args: args, tp: resultType}
	space := &builtinSpaceSig{base, 1000}
	input := chunk.NewChunkWithCapacity(colTypes, 10)
	input.AppendInt64(0, 6)
	input.AppendInt64(0, 1001)
	res, isNull, err := space.evalString(ctx, input.GetRow(0))
	require.Equal(t, "      ", res)
	require.False(t, isNull)
	require.NoError(t, err)
	res, isNull, err = space.evalString(ctx, input.GetRow(1))
	require.Equal(t, "", res)
	require.True(t, isNull)
	require.NoError(t, err)
	warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
	require.Equal(t, 1, len(warnings))
	lastWarn := warnings[len(warnings)-1]
	require.True(t, terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err))
}

func TestLocate(t *testing.T) {
	ctx := createContext(t)
	// 1. Test LOCATE without binary input.
	tbl := []struct {
		Args []any
		Want any
	}{
		{[]any{"bar", "foobarbar"}, 4},
		{[]any{"xbar", "foobar"}, 0},
		{[]any{"", "foobar"}, 1},
		{[]any{"foobar", ""}, 0},
		{[]any{"", ""}, 1},
		{[]any{"好世", "你好世界"}, 2},
		{[]any{"界面", "你好世界"}, 0},
		{[]any{"b", "中a英b文"}, 4},
		{[]any{"bAr", "foobArbar"}, 4},
		{[]any{nil, "foobar"}, nil},
		{[]any{"bar", nil}, nil},
		{[]any{"bar", "foobarbar", 5}, 7},
		{[]any{"xbar", "foobar", 1}, 0},
		{[]any{"", "foobar", 2}, 2},
		{[]any{"foobar", "", 1}, 0},
		{[]any{"", "", 2}, 0},
		{[]any{"A", "大A写的A", 0}, 0},
		{[]any{"A", "大A写的A", 1}, 2},
		{[]any{"A", "大A写的A", 2}, 2},
		{[]any{"A", "大A写的A", 3}, 5},
		{[]any{"BaR", "foobarBaR", 5}, 7},
		{[]any{nil, nil}, nil},
		{[]any{"", nil}, nil},
		{[]any{nil, ""}, nil},
		{[]any{nil, nil, 1}, nil},
		{[]any{"", nil, 1}, nil},
		{[]any{nil, "", 1}, nil},
		{[]any{"foo", nil, -1}, nil},
		{[]any{nil, "bar", 0}, nil},
	}
	Dtbl := tblToDtbl(tbl)
	instr := funcs[ast.Locate]
	for i, c := range Dtbl {
		f, err := instr.getFunction(ctx, datumsToConstants(c["Args"]))
		require.NoError(t, err)
		got, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		require.NotNil(t, f)
		require.Equalf(t, c["Want"][0], got, "[%d]: args: %v", i, c["Args"])
	}
	// 2. Test LOCATE with binary input
	tbl2 := []struct {
		Args []any
		Want any
	}{
		{[]any{[]byte("BaR"), "foobArbar"}, 0},
		{[]any{"BaR", []byte("foobArbar")}, 0},
		{[]any{[]byte("bAr"), "foobarBaR", 5}, 0},
		{[]any{"bAr", []byte("foobarBaR"), 5}, 0},
		{[]any{"bAr", []byte("foobarbAr"), 5}, 7},
	}
	Dtbl2 := tblToDtbl(tbl2)
	for i, c := range Dtbl2 {
		exprs := datumsToConstants(c["Args"])
		types.SetBinChsClnFlag(exprs[0].GetType(ctx))
		types.SetBinChsClnFlag(exprs[1].GetType(ctx))
		f, err := instr.getFunction(ctx, exprs)
		require.NoError(t, err)
		got, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		require.NotNil(t, f)
		require.Equalf(t, c["Want"][0], got, "[%d]: args: %v", i, c["Args"])
	}
}

func TestTrim(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args   []any
		isNil  bool
		getErr bool
		res    string
	}{
		{[]any{"   bar   "}, false, false, "bar"},
		{[]any{"\t   bar   \n"}, false, false, "\t   bar   \n"},
		{[]any{"\r   bar   \t"}, false, false, "\r   bar   \t"},
		{[]any{"   \tbar\n     "}, false, false, "\tbar\n"},
		{[]any{""}, false, false, ""},
		{[]any{nil}, true, false, ""},
		{[]any{"xxxbarxxx", "x"}, false, false, "bar"},
		{[]any{"bar", "x"}, false, false, "bar"},
		{[]any{"   bar   ", ""}, false, false, "   bar   "},
		{[]any{"", "x"}, false, false, ""},
		{[]any{"bar", nil}, true, false, ""},
		{[]any{nil, "x"}, true, false, ""},
		{[]any{"xxxbarxxx", "x", int(ast.TrimLeading)}, false, false, "barxxx"},
		{[]any{"barxxyz", "xyz", int(ast.TrimTrailing)}, false, false, "barx"},
		{[]any{"xxxbarxxx", "x", int(ast.TrimBoth)}, false, false, "bar"},
		{[]any{"bar", nil, int(ast.TrimLeading)}, true, false, ""},
		{[]any{errors.New("must error")}, false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Trim, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.res, d.GetString())
			}
		}
	}

	_, err := funcs[ast.Trim].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)

	_, err = funcs[ast.Trim].getFunction(ctx, []Expression{NewZero(), NewZero()})
	require.NoError(t, err)

	_, err = funcs[ast.Trim].getFunction(ctx, []Expression{NewZero(), NewZero(), NewZero()})
	require.NoError(t, err)
}

func TestLTrim(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		arg    any
		isNil  bool
		getErr bool
		res    string
	}{
		{"   bar   ", false, false, "bar   "},
		{"\t   bar   ", false, false, "\t   bar   "},
		{"   \tbar   ", false, false, "\tbar   "},
		{"\t   bar   ", false, false, "\t   bar   "},
		{"   \tbar   ", false, false, "\tbar   "},
		{"\r   bar   ", false, false, "\r   bar   "},
		{"   \rbar   ", false, false, "\rbar   "},
		{"\n   bar   ", false, false, "\n   bar   "},
		{"   \nbar   ", false, false, "\nbar   "},
		{"bar", false, false, "bar"},
		{"", false, false, ""},
		{nil, true, false, ""},
		{errors.New("must error"), false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.LTrim, primitiveValsToConstants(ctx, []any{c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.res, d.GetString())
			}
		}
	}

	_, err := funcs[ast.LTrim].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestRTrim(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		arg    any
		isNil  bool
		getErr bool
		res    string
	}{
		{"   bar   ", false, false, "   bar"},
		{"bar", false, false, "bar"},
		{"bar     \n", false, false, "bar     \n"},
		{"bar\n     ", false, false, "bar\n"},
		{"bar     \r", false, false, "bar     \r"},
		{"bar\r     ", false, false, "bar\r"},
		{"bar     \t", false, false, "bar     \t"},
		{"bar\t     ", false, false, "bar\t"},
		{"", false, false, ""},
		{nil, true, false, ""},
		{errors.New("must error"), false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.RTrim, primitiveValsToConstants(ctx, []any{c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.res, d.GetString())
			}
		}
	}

	_, err := funcs[ast.RTrim].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestHexFunc(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		arg    any
		isNil  bool
		getErr bool
		res    string
	}{
		{"abc", false, false, "616263"},
		{"你好", false, false, "E4BDA0E5A5BD"},
		{12, false, false, "C"},
		{12.3, false, false, "C"},
		{12.8, false, false, "D"},
		{-1, false, false, "FFFFFFFFFFFFFFFF"},
		{-12.3, false, false, "FFFFFFFFFFFFFFF4"},
		{-12.8, false, false, "FFFFFFFFFFFFFFF3"},
		{types.NewBinaryLiteralFromUint(0xC, -1), false, false, "0C"},
		{0x12, false, false, "12"},
		{nil, true, false, ""},
		{errors.New("must err"), false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Hex, primitiveValsToConstants(ctx, []any{c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.res, d.GetString())
			}
		}
	}

	strCases := []struct {
		arg     string
		chs     string
		res     string
		errCode int
	}{
		{"你好", "", "E4BDA0E5A5BD", 0},
		{"你好", "gbk", "C4E3BAC3", 0},
		{"一忒(๑•ㅂ•)و✧", "", "E4B880E5BF9228E0B991E280A2E38582E280A229D988E29CA7", 0},
		{"一忒(๑•ㅂ•)و✧", "gbk", "", errno.ErrInvalidCharacterString},
	}
	for _, c := range strCases {
		err := ctx.GetSessionVars().SetSystemVarWithoutValidation(variable.CharacterSetConnection, c.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.Hex, primitiveValsToConstants(ctx, []any{c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		if c.errCode != 0 {
			require.Error(t, err)
			require.True(t, strings.Contains(err.Error(), strconv.Itoa(c.errCode)))
		} else {
			require.NoError(t, err)
			require.Equal(t, c.res, d.GetString())
		}
	}

	_, err := funcs[ast.Hex].getFunction(ctx, []Expression{getInt8Con()})
	require.NoError(t, err)

	_, err = funcs[ast.Hex].getFunction(ctx, []Expression{getVarcharCon()})
	require.NoError(t, err)
}

func TestUnhexFunc(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		arg    any
		isNil  bool
		getErr bool
		res    string
	}{
		{"4D7953514C", false, false, "MySQL"},
		{"1267", false, false, string([]byte{0x12, 0x67})},
		{"126", false, false, string([]byte{0x01, 0x26})},
		{"", false, false, ""},
		{1267, false, false, string([]byte{0x12, 0x67})},
		{126, false, false, string([]byte{0x01, 0x26})},
		{1267.3, true, false, ""},
		{"string", true, false, ""},
		{"你好", true, false, ""},
		{nil, true, false, ""},
		{errors.New("must error"), false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Unhex, primitiveValsToConstants(ctx, []any{c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.res, d.GetString())
			}
		}
	}

	_, err := funcs[ast.Unhex].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestBitLength(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args     any
		chs      string
		expected int64
		isNil    bool
		getErr   bool
	}{
		{"hi", "", 16, false, false},
		{"你好", "", 48, false, false},
		{"", "", 0, false, false},
		{"abc", "gbk", 24, false, false},
		{"一二三", "gbk", 48, false, false},
		{"一二三", "", 72, false, false},
		{"一二三!", "gbk", 56, false, false},
		{"一二三!", "", 80, false, false},
	}

	for _, c := range cases {
		err := ctx.GetSessionVars().SetSystemVarWithoutValidation(variable.CharacterSetConnection, c.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.BitLength, primitiveValsToConstants(ctx, []any{c.args})...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
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

	_, err := funcs[ast.BitLength].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestChar(t *testing.T) {
	ctx := createContext(t)
	typeFlags := ctx.GetSessionVars().StmtCtx.TypeFlags()
	ctx.GetSessionVars().StmtCtx.SetTypeFlags(typeFlags.WithIgnoreTruncateErr(true))
	tbl := []struct {
		str      string
		iNum     int64
		fNum     float64
		charset  any
		result   any
		warnings int
	}{
		{"65", 66, 67.5, "utf8", "ABD", 0},                               // float
		{"65", 16740, 67.5, "utf8", "AAdD", 0},                           // large num
		{"65", -1, 67.5, nil, "A\xff\xff\xff\xffD", 0},                   // negative int
		{"a", -1, 67.5, nil, "\x00\xff\xff\xff\xffD", 0},                 // invalid 'a'
		{"65", -1, 67.5, "utf8", nil, 1},                                 // with utf8, return nil
		{"a", -1, 67.5, "utf8", nil, 1},                                  // with utf8, return nil
		{"1234567", 1234567, 1234567, "gbk", "\u0012謬\u0012謬\u0012謬", 0}, // test char for gbk
		{"123456789", 123456789, 123456789, "gbk", nil, 1},               // invalid 123456789 in gbk
	}
	run := func(i int, result any, warnCnt int, dts ...any) {
		fc := funcs[ast.CharFunc]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(dts...)))
		require.NoError(t, err, i)
		require.NotNil(t, f, i)
		r, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err, i)
		testutil.DatumEqual(t, types.NewDatum(result), r, i)
		if warnCnt != 0 {
			warnings := ctx.GetSessionVars().StmtCtx.TruncateWarnings(0)
			require.Equal(t, warnCnt, len(warnings), fmt.Sprintf("%d: %v", i, warnings))
		}
	}
	for i, v := range tbl {
		run(i, v.result, v.warnings, v.str, v.iNum, v.fNum, v.charset)
	}
	// char() returns null only when the sql_mode is strict.
	require.True(t, ctx.GetSessionVars().SQLMode.HasStrictMode())
	run(-1, nil, 1, 123456, "utf8")

	ctx.GetSessionVars().SQLMode = ctx.GetSessionVars().SQLMode &^ (mysql.ModeStrictTransTables | mysql.ModeStrictAllTables)
	require.False(t, ctx.GetSessionVars().SQLMode.HasStrictMode())
	run(-2, string([]byte{1}), 1, 123456, "utf8")
}

func TestCharLength(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		input  any
		result any
	}{
		{"33", 2},  // string
		{"你好", 2},  // mb string
		{33, 2},    // int
		{3.14, 4},  // float
		{nil, nil}, // nil
	}
	for _, v := range tbl {
		fc := funcs[ast.CharLength]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(v.input)))
		require.NoError(t, err)
		r, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(v.result), r)
	}

	// Test binary string
	tbl = []struct {
		input  any
		result any
	}{
		{"33", 2},   // string
		{"你好", 6},   // mb string
		{"CAFÉ", 5}, // mb string
		{"", 0},     // mb string
		{nil, nil},  // nil
	}
	for _, v := range tbl {
		fc := funcs[ast.CharLength]
		arg := datumsToConstants(types.MakeDatums(v.input))
		tp := arg[0].GetType(ctx)
		tp.SetType(mysql.TypeVarString)
		tp.SetCharset(charset.CharsetBin)
		tp.SetCollate(charset.CollationBin)
		tp.SetFlen(types.UnspecifiedLength)
		tp.SetFlag(mysql.BinaryFlag)
		f, err := fc.getFunction(ctx, arg)
		require.NoError(t, err)
		r, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(v.result), r)
	}
}

func TestFindInSet(t *testing.T) {
	ctx := createContext(t)
	for _, c := range []struct {
		str    any
		strlst any
		ret    any
	}{
		{"foo", "foo,bar", 1},
		{"foo", "foobar,bar", 0},
		{" foo ", "foo, foo ", 2},
		{"", "foo,bar,", 3},
		{"", "", 0},
		{1, 1, 1},
		{1, "1", 1},
		{"1", 1, 1},
		{"a,b", "a,b,c", 0},
		{"foo", nil, nil},
		{nil, "bar", nil},
	} {
		fc := funcs[ast.FindInSet]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(c.str, c.strlst)))
		require.NoError(t, err)
		r, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(c.ret), r, fmt.Sprintf("FindInSet(%s, %s)", c.str, c.strlst))
	}
}

func TestField(t *testing.T) {
	ctx := createContext(t)
	stmtCtx := ctx.GetSessionVars().StmtCtx
	oldTypeFlags := stmtCtx.TypeFlags()
	defer func() {
		stmtCtx.SetTypeFlags(oldTypeFlags)
	}()
	stmtCtx.SetTypeFlags(oldTypeFlags.WithIgnoreTruncateErr(true))

	tbl := []struct {
		argLst []any
		ret    any
	}{
		{[]any{"ej", "Hej", "ej", "Heja", "hej", "foo"}, int64(2)},
		{[]any{"fo", "Hej", "ej", "Heja", "hej", "foo"}, int64(0)},
		{[]any{"ej", "Hej", "ej", "Heja", "ej", "hej", "foo"}, int64(2)},
		{[]any{1, 2, 3, 11, 1}, int64(4)},
		{[]any{nil, 2, 3, 11, 1}, int64(0)},
		{[]any{1.1, 2.1, 3.1, 11.1, 1.1}, int64(4)},
		{[]any{1.1, "2.1", "3.1", "11.1", "1.1"}, int64(4)},
		{[]any{"1.1a", 2.1, 3.1, 11.1, 1.1}, int64(4)},
		{[]any{1.10, 0, 11e-1}, int64(2)},
		{[]any{"abc", 0, 1, 11.1, 1.1}, int64(1)},
	}
	for _, c := range tbl {
		fc := funcs[ast.Field]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(c.argLst...)))
		require.NoError(t, err)
		require.NotNil(t, f)
		r, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(c.ret), r)
	}
}

func TestLpad(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		str    string
		len    int64
		padStr string
		expect any
	}{
		{"hi", 5, "?", "???hi"},
		{"hi", 1, "?", "h"},
		{"hi", 0, "?", ""},
		{"hi", -1, "?", nil},
		{"hi", 1, "", "h"},
		{"hi", 5, "", nil},
		{"hi", 5, "ab", "abahi"},
		{"hi", 6, "ab", "ababhi"},
		{"中文", 5, "字符", "字符字中文"},
		{"中文", 1, "a", "中"},
		{"中文", -5, "字符", nil},
		{"中文", 10, "", nil},
	}
	fc := funcs[ast.Lpad]
	for _, test := range tests {
		str := types.NewStringDatum(test.str)
		length := types.NewIntDatum(test.len)
		padStr := types.NewStringDatum(test.padStr)
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{str, length, padStr}))
		require.NoError(t, err)
		require.NotNil(t, f)
		result, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		if test.expect == nil {
			require.Equal(t, types.KindNull, result.Kind())
		} else {
			expect, _ := test.expect.(string)
			require.Equal(t, expect, result.GetString())
		}
	}
}

func TestRpad(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		str    string
		len    int64
		padStr string
		expect any
	}{
		{"hi", 5, "?", "hi???"},
		{"hi", 1, "?", "h"},
		{"hi", 0, "?", ""},
		{"hi", -1, "?", nil},
		{"hi", 1, "", "h"},
		{"hi", 5, "", nil},
		{"hi", 5, "ab", "hiaba"},
		{"hi", 6, "ab", "hiabab"},
		{"中文", 5, "字符", "中文字符字"},
		{"中文", 1, "a", "中"},
		{"中文", -5, "字符", nil},
		{"中文", 10, "", nil},
	}
	fc := funcs[ast.Rpad]
	for _, test := range tests {
		str := types.NewStringDatum(test.str)
		length := types.NewIntDatum(test.len)
		padStr := types.NewStringDatum(test.padStr)
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{str, length, padStr}))
		require.NoError(t, err)
		require.NotNil(t, f)
		result, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		if test.expect == nil {
			require.Equal(t, types.KindNull, result.Kind())
		} else {
			expect, _ := test.expect.(string)
			require.Equal(t, expect, result.GetString())
		}
	}
}

func TestRpadSig(t *testing.T) {
	ctx := createContext(t)
	colTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarchar),
	}
	resultType := &types.FieldType{}
	resultType.SetType(mysql.TypeVarchar)
	resultType.SetFlen(1000)

	args := []Expression{
		&Column{Index: 0, RetType: colTypes[0]},
		&Column{Index: 1, RetType: colTypes[1]},
		&Column{Index: 2, RetType: colTypes[2]},
	}

	base := baseBuiltinFunc{args: args, tp: resultType}
	rpad := &builtinRpadUTF8Sig{base, 1000}

	input := chunk.NewChunkWithCapacity(colTypes, 10)
	input.AppendString(0, "abc")
	input.AppendString(0, "abc")
	input.AppendInt64(1, 6)
	input.AppendInt64(1, 10000)
	input.AppendString(2, "123")
	input.AppendString(2, "123")

	res, isNull, err := rpad.evalString(ctx, input.GetRow(0))
	require.Equal(t, "abc123", res)
	require.False(t, isNull)
	require.NoError(t, err)

	res, isNull, err = rpad.evalString(ctx, input.GetRow(1))
	require.Equal(t, "", res)
	require.True(t, isNull)
	require.NoError(t, err)

	warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
	require.Equal(t, 1, len(warnings))
	lastWarn := warnings[len(warnings)-1]
	require.Truef(t, terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err), "err %v", lastWarn.Err)
}

func TestInsertBinarySig(t *testing.T) {
	ctx := createContext(t)
	colTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarchar),
	}
	resultType := &types.FieldType{}
	resultType.SetType(mysql.TypeVarchar)
	resultType.SetFlen(3)

	args := []Expression{
		&Column{Index: 0, RetType: colTypes[0]},
		&Column{Index: 1, RetType: colTypes[1]},
		&Column{Index: 2, RetType: colTypes[2]},
		&Column{Index: 3, RetType: colTypes[3]},
	}

	base := baseBuiltinFunc{args: args, tp: resultType}
	insert := &builtinInsertSig{base, 3}

	input := chunk.NewChunkWithCapacity(colTypes, 2)
	input.AppendString(0, "abc")
	input.AppendString(0, "abc")
	input.AppendString(0, "abc")
	input.AppendNull(0)
	input.AppendString(0, "abc")
	input.AppendString(0, "abc")
	input.AppendString(0, "abc")
	input.AppendInt64(1, 3)
	input.AppendInt64(1, 3)
	input.AppendInt64(1, 0)
	input.AppendInt64(1, 3)
	input.AppendNull(1)
	input.AppendInt64(1, 3)
	input.AppendInt64(1, 3)
	input.AppendInt64(2, -1)
	input.AppendInt64(2, -1)
	input.AppendInt64(2, -1)
	input.AppendInt64(2, -1)
	input.AppendInt64(2, -1)
	input.AppendNull(2)
	input.AppendInt64(2, -1)
	input.AppendString(3, "d")
	input.AppendString(3, "de")
	input.AppendString(3, "d")
	input.AppendString(3, "d")
	input.AppendString(3, "d")
	input.AppendString(3, "d")
	input.AppendNull(3)

	res, isNull, err := insert.evalString(ctx, input.GetRow(0))
	require.Equal(t, "abd", res)
	require.False(t, isNull)
	require.NoError(t, err)

	res, isNull, err = insert.evalString(ctx, input.GetRow(1))
	require.Equal(t, "", res)
	require.True(t, isNull)
	require.NoError(t, err)

	res, isNull, err = insert.evalString(ctx, input.GetRow(2))
	require.Equal(t, "abc", res)
	require.False(t, isNull)
	require.NoError(t, err)

	res, isNull, err = insert.evalString(ctx, input.GetRow(3))
	require.Equal(t, "", res)
	require.True(t, isNull)
	require.NoError(t, err)

	res, isNull, err = insert.evalString(ctx, input.GetRow(4))
	require.Equal(t, "", res)
	require.True(t, isNull)
	require.NoError(t, err)

	res, isNull, err = insert.evalString(ctx, input.GetRow(5))
	require.Equal(t, "", res)
	require.True(t, isNull)
	require.NoError(t, err)

	res, isNull, err = insert.evalString(ctx, input.GetRow(6))
	require.Equal(t, "", res)
	require.True(t, isNull)
	require.NoError(t, err)

	warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
	require.Equal(t, 1, len(warnings))
	lastWarn := warnings[len(warnings)-1]
	require.Truef(t, terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err), "err %v", lastWarn.Err)
}

func TestInstr(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		Args []any
		Want any
	}{
		{[]any{"foobarbar", "bar"}, 4},
		{[]any{"xbar", "foobar"}, 0},

		{[]any{123456234, 234}, 2},
		{[]any{123456, 567}, 0},
		{[]any{1e10, 1e2}, 1},
		{[]any{1.234, ".234"}, 2},
		{[]any{1.234, ""}, 1},
		{[]any{"", 123}, 0},
		{[]any{"", ""}, 1},

		{[]any{"中文美好", "美好"}, 3},
		{[]any{"中文美好", "世界"}, 0},
		{[]any{"中文abc", "a"}, 3},

		{[]any{"live long and prosper", "long"}, 6},

		{[]any{"not binary string", "binary"}, 5},
		{[]any{"upper case", "upper"}, 1},
		{[]any{"UPPER CASE", "CASE"}, 7},
		{[]any{"中文abc", "abc"}, 3},

		{[]any{"foobar", nil}, nil},
		{[]any{nil, "foobar"}, nil},
		{[]any{nil, nil}, nil},
	}

	Dtbl := tblToDtbl(tbl)
	instr := funcs[ast.Instr]
	for i, c := range Dtbl {
		f, err := instr.getFunction(ctx, datumsToConstants(c["Args"]))
		require.NoError(t, err)
		require.NotNil(t, f)
		got, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		require.Equalf(t, c["Want"][0], got, "[%d]: args: %v", i, c["Args"])
	}
}

func TestLoadFile(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		arg    any
		isNil  bool
		getErr bool
		res    string
	}{
		{"", true, false, ""},
		{"/tmp/tikv/tikv.frm", true, false, ""},
		{"tidb.sql", true, false, ""},
		{nil, true, false, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.LoadFile, primitiveValsToConstants(ctx, []any{c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		if c.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.res, d.GetString())
			}
		}
	}
	_, err := funcs[ast.LoadFile].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestMakeSet(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		argList []any
		ret     any
	}{
		{[]any{1, "a", "b", "c"}, "a"},
		{[]any{1 | 4, "hello", "nice", "world"}, "hello,world"},
		{[]any{1 | 4, "hello", "nice", nil, "world"}, "hello"},
		{[]any{0, "a", "b", "c"}, ""},
		{[]any{nil, "a", "b", "c"}, nil},
		{[]any{-100 | 4, "hello", "nice", "abc", "world"}, "abc,world"},
		{[]any{-1, "hello", "nice", "abc", "world"}, "hello,nice,abc,world"},
	}

	for _, c := range tbl {
		fc := funcs[ast.MakeSet]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(c.argList...)))
		require.NoError(t, err)
		require.NotNil(t, f)
		r, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(c.ret), r)
	}
}

func TestOct(t *testing.T) {
	ctx := createContext(t)
	octTests := []struct {
		origin any
		ret    string
	}{
		{"-2.7", "1777777777777777777776"},
		{-1.5, "1777777777777777777777"},
		{-1, "1777777777777777777777"},
		{"0", "0"},
		{"1", "1"},
		{"8", "10"},
		{"12", "14"},
		{"20", "24"},
		{"100", "144"},
		{"1024", "2000"},
		{"2048", "4000"},
		{1.0, "1"},
		{9.5, "11"},
		{13, "15"},
		{1025, "2001"},
		{"8a8", "10"},
		{"abc", "0"},
		// overflow uint64
		{"9999999999999999999999999", "1777777777777777777777"},
		{"-9999999999999999999999999", "1777777777777777777777"},
		{types.NewBinaryLiteralFromUint(255, -1), "377"}, // b'11111111'
		{types.NewBinaryLiteralFromUint(10, -1), "12"},   // b'1010'
		{types.NewBinaryLiteralFromUint(5, -1), "5"},     // b'0101'
	}
	fc := funcs[ast.Oct]
	for _, tt := range octTests {
		in := types.NewDatum(tt.origin)
		f, _ := fc.getFunction(ctx, datumsToConstants([]types.Datum{in}))
		require.NotNil(t, f)
		r, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		res, err := r.ToString()
		require.NoError(t, err)
		require.Equalf(t, tt.ret, res, "select oct(%v);", tt.origin)
	}
	// tt NULL input for sha
	var argNull types.Datum
	f, _ := fc.getFunction(ctx, datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, ctx, chunk.Row{})
	require.NoError(t, err)
	require.True(t, r.IsNull())
}

func TestFormat(t *testing.T) {
	ctx := createContext(t)
	formatTests := []struct {
		number    any
		precision any
		locale    string
		ret       any
	}{
		{12332.12341111111111111111111111111111111111111, 4, "en_US", "12,332.1234"},
		{nil, 22, "en_US", nil},
	}
	formatTests1 := []struct {
		number    any
		precision any
		ret       any
		warnings  int
	}{
		// issue #8796
		{1.12345, 4, "1.1235", 0},
		{9.99999, 4, "10.0000", 0},
		{1.99999, 4, "2.0000", 0},
		{1.09999, 4, "1.1000", 0},
		{-2.5000, 0, "-3", 0},

		{12332.123444, 4, "12,332.1234", 0},
		{12332.123444, 0, "12,332", 0},
		{12332.123444, -4, "12,332", 0},
		{-12332.123444, 4, "-12,332.1234", 0},
		{-12332.123444, 0, "-12,332", 0},
		{-12332.123444, -4, "-12,332", 0},
		{"12332.123444", "4", "12,332.1234", 0},
		{"12332.123444A", "4", "12,332.1234", 1},
		{"-12332.123444", "4", "-12,332.1234", 0},
		{"-12332.123444A", "4", "-12,332.1234", 1},
		{"A123345", "4", "0.0000", 1},
		{"-A123345", "4", "0.0000", 1},
		{"-12332.123444", "A", "-12,332", 1},
		{"12332.123444", "A", "12,332", 1},
		{"-12332.123444", "4A", "-12,332.1234", 1},
		{"12332.123444", "4A", "12,332.1234", 1},
		{"-A12332.123444", "A", "0", 2},
		{"A12332.123444", "A", "0", 2},
		{"-A12332.123444", "4A", "0.0000", 2},
		{"A12332.123444", "4A", "0.0000", 2},
		{"-.12332.123444", "4A", "-0.1233", 2},
		{".12332.123444", "4A", "0.1233", 2},
		{"12332.1234567890123456789012345678901", 22, "12,332.1234567890110000000000", 0},
		{nil, 22, nil, 0},
		{1, 1024, "1.000000000000000000000000000000", 0},
		{"", 1, "0.0", 0},
		{1, "", "1", 1},
	}
	formatTests2 := struct {
		number    any
		precision any
		locale    string
		ret       any
	}{-12332.123456, -4, "zh_CN", "-12,332"}
	formatTests3 := struct {
		number    any
		precision any
		locale    string
		ret       any
	}{"-12332.123456", "4", "de_GE", "-12,332.1235"}
	formatTests4 := struct {
		number    any
		precision any
		locale    any
		ret       any
	}{1, 4, nil, "1.0000"}

	fc := funcs[ast.Format]
	for _, tt := range formatTests {
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.number, tt.precision, tt.locale)))
		require.NoError(t, err)
		require.NotNil(t, f)
		r, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(tt.ret), r)
	}

	origTypeFlags := ctx.GetSessionVars().StmtCtx.TypeFlags()
	ctx.GetSessionVars().StmtCtx.SetTypeFlags(origTypeFlags.WithTruncateAsWarning(true))
	for _, tt := range formatTests1 {
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.number, tt.precision)))
		require.NoError(t, err)
		require.NotNil(t, f)
		r, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(tt.ret), r, fmt.Sprintf("test %v", tt))
		if tt.warnings > 0 {
			warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
			require.Lenf(t, warnings, tt.warnings, "test %v", tt)
			for i := 0; i < tt.warnings; i++ {
				require.Truef(t, terror.ErrorEqual(types.ErrTruncatedWrongVal, warnings[i].Err), "test %v", tt)
			}
			ctx.GetSessionVars().StmtCtx.SetWarnings([]contextutil.SQLWarn{})
		}
	}
	ctx.GetSessionVars().StmtCtx.SetTypeFlags(origTypeFlags)

	f2, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(formatTests2.number, formatTests2.precision, formatTests2.locale)))
	require.NoError(t, err)
	require.NotNil(t, f2)
	r2, err := evalBuiltinFunc(f2, ctx, chunk.Row{})
	testutil.DatumEqual(t, types.NewDatum(errors.New("not implemented")), types.NewDatum(err))
	testutil.DatumEqual(t, types.NewDatum(formatTests2.ret), r2)

	f3, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(formatTests3.number, formatTests3.precision, formatTests3.locale)))
	require.NoError(t, err)
	require.NotNil(t, f3)
	r3, err := evalBuiltinFunc(f3, ctx, chunk.Row{})
	testutil.DatumEqual(t, types.NewDatum(errors.New("not support for the specific locale")), types.NewDatum(err))
	testutil.DatumEqual(t, types.NewDatum(formatTests3.ret), r3)

	f4, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(formatTests4.number, formatTests4.precision, formatTests4.locale)))
	require.NoError(t, err)
	require.NotNil(t, f4)
	r4, err := evalBuiltinFunc(f4, ctx, chunk.Row{})
	require.NoError(t, err)
	testutil.DatumEqual(t, types.NewDatum(formatTests4.ret), r4)
	warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
	require.Equal(t, 3, len(warnings))
	for i := 0; i < 3; i++ {
		require.True(t, terror.ErrorEqual(errUnknownLocale, warnings[i].Err))
	}
	ctx.GetSessionVars().StmtCtx.SetWarnings([]contextutil.SQLWarn{})
}

func TestFromBase64(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args   any
		expect any
	}{
		{"", ""},
		{"YWJj", "abc"},
		{"YWIgYw==", "ab c"},
		{"YWIKYw==", "ab\nc"},
		{"YWIJYw==", "ab\tc"},
		{"cXdlcnR5MTIzNDU2", "qwerty123456"},
		{
			"QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrL0FCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4\neXowMTIzNDU2Nzg5Ky9BQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWmFiY2RlZmdoaWprbG1ub3Bx\ncnN0dXZ3eHl6MDEyMzQ1Njc4OSsv",
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
		},
		{
			"QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0NTY3ODkrLw==",
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
		},
		{
			"QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0NTY3ODkrLw==",
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
		},
		{
			"QUJDREVGR0hJSkt\tMTU5PUFFSU1RVVld\nYWVphYmNkZ\rWZnaGlqa2xt   bm9wcXJzdHV2d3h5ejAxMjM0NTY3ODkrLw==",
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
		},
	}
	fc := funcs[ast.FromBase64]
	for _, test := range tests {
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(test.args)))
		require.NoError(t, err)
		require.NotNil(t, f)
		result, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		if test.expect == nil {
			require.Equal(t, types.KindNull, result.Kind())
		} else {
			expect, _ := test.expect.(string)
			require.Equal(t, expect, result.GetString())
		}
	}
}

func TestFromBase64Sig(t *testing.T) {
	ctx := createContext(t)
	colTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
	}

	tests := []struct {
		args           string
		expect         string
		isNil          bool
		maxAllowPacket uint64
	}{
		{"YWJj", "abc", false, 3},
		{"YWJj", "", true, 2},
		{
			"QUJDREVGR0hJSkt\tMTU5PUFFSU1RVVld\nYWVphYmNkZ\rWZnaGlqa2xt   bm9wcXJzdHV2d3h5ejAxMjM0NTY3ODkrLw==",
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
			false,
			70,
		},
		{
			"QUJDREVGR0hJSkt\tMTU5PUFFSU1RVVld\nYWVphYmNkZ\rWZnaGlqa2xt   bm9wcXJzdHV2d3h5ejAxMjM0NTY3ODkrLw==",
			"",
			true,
			69,
		},
	}

	args := []Expression{
		&Column{Index: 0, RetType: colTypes[0]},
	}

	for _, test := range tests {
		resultType := &types.FieldType{}
		resultType.SetType(mysql.TypeVarchar)
		resultType.SetFlen(mysql.MaxBlobWidth)
		base := baseBuiltinFunc{args: args, tp: resultType}
		fromBase64 := &builtinFromBase64Sig{base, test.maxAllowPacket}

		input := chunk.NewChunkWithCapacity(colTypes, 1)
		input.AppendString(0, test.args)
		res, isNull, err := fromBase64.evalString(ctx, input.GetRow(0))
		require.NoError(t, err)
		require.Equal(t, test.isNil, isNull)
		if isNull {
			warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
			require.Equal(t, 1, len(warnings))
			lastWarn := warnings[len(warnings)-1]
			require.True(t, terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err))
			ctx.GetSessionVars().StmtCtx.SetWarnings([]contextutil.SQLWarn{})
		}
		require.Equal(t, test.expect, res)
	}
}

func TestInsert(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args   []any
		expect any
	}{
		{[]any{"Quadratic", 3, 4, "What"}, "QuWhattic"},
		{[]any{"Quadratic", -1, 4, "What"}, "Quadratic"},
		{[]any{"Quadratic", 3, 100, "What"}, "QuWhat"},
		{[]any{nil, 3, 100, "What"}, nil},
		{[]any{"Quadratic", nil, 4, "What"}, nil},
		{[]any{"Quadratic", 3, nil, "What"}, nil},
		{[]any{"Quadratic", 3, 4, nil}, nil},
		{[]any{"Quadratic", 3, -1, "What"}, "QuWhat"},
		{[]any{"Quadratic", 3, 1, "What"}, "QuWhatdratic"},
		{[]any{"Quadratic", -1, nil, "What"}, nil},
		{[]any{"Quadratic", -1, 4, nil}, nil},

		{[]any{"我叫小雨呀", 3, 2, "王雨叶"}, "我叫王雨叶呀"},
		{[]any{"我叫小雨呀", -1, 2, "王雨叶"}, "我叫小雨呀"},
		{[]any{"我叫小雨呀", 3, 100, "王雨叶"}, "我叫王雨叶"},
		{[]any{nil, 3, 100, "王雨叶"}, nil},
		{[]any{"我叫小雨呀", nil, 4, "王雨叶"}, nil},
		{[]any{"我叫小雨呀", 3, nil, "王雨叶"}, nil},
		{[]any{"我叫小雨呀", 3, 4, nil}, nil},
		{[]any{"我叫小雨呀", 3, -1, "王雨叶"}, "我叫王雨叶"},
		{[]any{"我叫小雨呀", 3, 1, "王雨叶"}, "我叫王雨叶雨呀"},
		{[]any{"我叫小雨呀", -1, nil, "王雨叶"}, nil},
		{[]any{"我叫小雨呀", -1, 2, nil}, nil},
	}
	fc := funcs[ast.InsertFunc]
	for _, test := range tests {
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(test.args...)))
		require.NoError(t, err)
		require.NotNil(t, f)
		result, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		if test.expect == nil {
			require.Equal(t, types.KindNull, result.Kind())
		} else {
			expect, _ := test.expect.(string)
			require.Equal(t, expect, result.GetString())
		}
	}
}

func TestOrd(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args     any
		expected int64
		chs      string
		isNil    bool
		getErr   bool
	}{
		{"2", 50, "", false, false},
		{2, 50, "", false, false},
		{"23", 50, "", false, false},
		{23, 50, "", false, false},
		{2.3, 50, "", false, false},
		{nil, 0, "", true, false},
		{"", 0, "", false, false},
		{"你好", 14990752, "utf8mb4", false, false},
		{"にほん", 14909867, "utf8mb4", false, false},
		{"한국", 15570332, "utf8mb4", false, false},
		{"👍", 4036989325, "utf8mb4", false, false},
		{"א", 55184, "utf8mb4", false, false},
		{"abc", 97, "gbk", false, false},
		{"一二三", 53947, "gbk", false, false},
		{"àáèé", 43172, "gbk", false, false},
		{"数据库", 51965, "gbk", false, false},
	}
	for _, c := range cases {
		err := ctx.GetSessionVars().SetSystemVarWithoutValidation(variable.CharacterSetConnection, c.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.Ord, primitiveValsToConstants(ctx, []any{c.args})...)
		require.NoError(t, err)

		d, err := f.Eval(ctx, chunk.Row{})
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
	_, err := funcs[ast.Ord].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestElt(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		argLst []any
		ret    any
	}{
		{[]any{1, "Hej", "ej", "Heja", "hej", "foo"}, "Hej"},
		{[]any{9, "Hej", "ej", "Heja", "hej", "foo"}, nil},
		{[]any{-1, "Hej", "ej", "Heja", "ej", "hej", "foo"}, nil},
		{[]any{0, 2, 3, 11, 1}, nil},
		{[]any{3, 2, 3, 11, 1}, "11"},
		{[]any{1.1, "2.1", "3.1", "11.1", "1.1"}, "2.1"},
	}
	for _, c := range tbl {
		fc := funcs[ast.Elt]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(c.argLst...)))
		require.NoError(t, err)
		r, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(c.ret), r)
	}
}

func TestExportSet(t *testing.T) {
	ctx := createContext(t)
	estd := []struct {
		argLst []any
		res    string
	}{
		{[]any{-9223372036854775807, "Y", "N", ",", 5}, "Y,N,N,N,N"},
		{[]any{-6, "Y", "N", ",", 5}, "N,Y,N,Y,Y"},
		{[]any{5, "Y", "N", ",", 4}, "Y,N,Y,N"},
		{[]any{5, "Y", "N", ",", 0}, ""},
		{[]any{5, "Y", "N", ",", 1}, "Y"},
		{[]any{6, "1", "0", ",", 10}, "0,1,1,0,0,0,0,0,0,0"},
		{[]any{333333, "Ysss", "sN", "---", 9}, "Ysss---sN---Ysss---sN---Ysss---sN---sN---sN---sN"},
		{[]any{7, "Y", "N"}, "Y,Y,Y,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N"},
		{[]any{7, "Y", "N", 6}, "Y6Y6Y6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N"},
		{[]any{7, "Y", "N", 6, 133}, "Y6Y6Y6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N"},
	}
	fc := funcs[ast.ExportSet]
	for _, c := range estd {
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(c.argLst...)))
		require.NoError(t, err)
		require.NotNil(t, f)
		exportSetRes, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		res, err := exportSetRes.ToString()
		require.NoError(t, err)
		require.Equal(t, c.res, res)
	}
}

func TestBin(t *testing.T) {
	tbl := []struct {
		Input    any
		Expected any
	}{
		{"10", "1010"},
		{"10.2", "1010"},
		{"10aa", "1010"},
		{"10.2aa", "1010"},
		{"aaa", "0"},
		{"", nil},
		{10, "1010"},
		{10.0, "1010"},
		{-1, "1111111111111111111111111111111111111111111111111111111111111111"},
		{"-1", "1111111111111111111111111111111111111111111111111111111111111111"},
		{nil, nil},
	}
	fc := funcs[ast.Bin]
	dtbl := tblToDtbl(tbl)
	ctx := mock.NewContext()
	typeFlags := ctx.GetSessionVars().StmtCtx.TypeFlags()
	ctx.GetSessionVars().StmtCtx.SetTypeFlags(typeFlags.WithIgnoreTruncateErr(true))
	for _, c := range dtbl {
		f, err := fc.getFunction(ctx, datumsToConstants(c["Input"]))
		require.NoError(t, err)
		require.NotNil(t, f)
		r, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(c["Expected"][0]), r)
	}
}

func TestQuote(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		arg any
		ret any
	}{
		{`Don\'t!`, `'Don\\\'t!'`},
		{`Don't`, `'Don\'t'`},
		{`Don"`, `'Don"'`},
		{`Don\"`, `'Don\\"'`},
		{`\'`, `'\\\''`},
		{`\"`, `'\\"'`},
		{`萌萌哒(๑•ᴗ•๑)😊`, `'萌萌哒(๑•ᴗ•๑)😊'`},
		{`㍿㌍㍑㌫`, `'㍿㌍㍑㌫'`},
		{string([]byte{0, 26}), `'\0\Z'`},
		{nil, "NULL"},
	}

	for _, c := range tbl {
		fc := funcs[ast.Quote]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(c.arg)))
		require.NoError(t, err)
		require.NotNil(t, f)
		r, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(c.ret), r)
	}
}

func TestToBase64(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args   any
		expect string
		isNil  bool
		getErr bool
	}{
		{"", "", false, false},
		{"abc", "YWJj", false, false},
		{"ab c", "YWIgYw==", false, false},
		{1, "MQ==", false, false},
		{1.1, "MS4x", false, false},
		{"ab\nc", "YWIKYw==", false, false},
		{"ab\tc", "YWIJYw==", false, false},
		{"qwerty123456", "cXdlcnR5MTIzNDU2", false, false},
		{
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
			"QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrLw==",
			false,
			false,
		},
		{
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
			"QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrL0FCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4\neXowMTIzNDU2Nzg5Ky9BQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWmFiY2RlZmdoaWprbG1ub3Bx\ncnN0dXZ3eHl6MDEyMzQ1Njc4OSsv",
			false,
			false,
		},
		{
			"ABCD  EFGHI\nJKLMNOPQRSTUVWXY\tZabcdefghijklmnopqrstuv  wxyz012\r3456789+/",
			"QUJDRCAgRUZHSEkKSktMTU5PUFFSU1RVVldYWQlaYWJjZGVmZ2hpamtsbW5vcHFyc3R1diAgd3h5\nejAxMg0zNDU2Nzg5Ky8=",
			false,
			false,
		},
		{nil, "", true, false},
	}
	if strconv.IntSize == 32 {
		tests = append(tests, struct {
			args   any
			expect string
			isNil  bool
			getErr bool
		}{
			strings.Repeat("a", 1589695687),
			"",
			true,
			false,
		})
	}

	for _, test := range tests {
		f, err := newFunctionForTest(ctx, ast.ToBase64, primitiveValsToConstants(ctx, []any{test.args})...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		if test.getErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if test.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, test.expect, d.GetString())
			}
		}
	}

	_, err := funcs[ast.ToBase64].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)

	// Test GBK String
	tbl := []struct {
		input  string
		chs    string
		result string
	}{
		{"abc", "gbk", "YWJj"},
		{"一二三", "gbk", "0ru2/sj9"},
		{"一二三", "", "5LiA5LqM5LiJ"},
		{"一二三!", "gbk", "0ru2/sj9IQ=="},
		{"一二三!", "", "5LiA5LqM5LiJIQ=="},
	}
	for _, c := range tbl {
		err := ctx.GetSessionVars().SetSystemVarWithoutValidation(variable.CharacterSetConnection, c.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.ToBase64, primitiveValsToConstants(ctx, []any{c.input})...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, c.result, d.GetString())
	}
}

func TestToBase64Sig(t *testing.T) {
	ctx := createContext(t)
	colTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeVarchar),
	}

	tests := []struct {
		args           string
		expect         string
		isNil          bool
		maxAllowPacket uint64
	}{
		{"abc", "YWJj", false, 4},
		{"abc", "", true, 3},
		{
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
			"QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrLw==",
			false,
			89,
		},
		{
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
			"",
			true,
			88,
		},
		{
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
			"QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrL0FCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4\neXowMTIzNDU2Nzg5Ky9BQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWmFiY2RlZmdoaWprbG1ub3Bx\ncnN0dXZ3eHl6MDEyMzQ1Njc4OSsv",
			false,
			259,
		},
		{
			"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/",
			"",
			true,
			258,
		},
	}

	args := []Expression{
		&Column{Index: 0, RetType: colTypes[0]},
	}

	for _, test := range tests {
		resultType := &types.FieldType{}
		resultType.SetType(mysql.TypeVarchar)
		resultType.SetFlen(base64NeededEncodedLength(len(test.args)))
		base := baseBuiltinFunc{args: args, tp: resultType}
		toBase64 := &builtinToBase64Sig{base, test.maxAllowPacket}

		input := chunk.NewChunkWithCapacity(colTypes, 1)
		input.AppendString(0, test.args)
		res, isNull, err := toBase64.evalString(ctx, input.GetRow(0))
		require.NoError(t, err)
		if test.isNil {
			require.True(t, isNull)

			warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
			require.Equal(t, 1, len(warnings))
			lastWarn := warnings[len(warnings)-1]
			require.True(t, terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err))
			ctx.GetSessionVars().StmtCtx.SetWarnings([]contextutil.SQLWarn{})
		} else {
			require.False(t, isNull)
		}
		require.Equal(t, test.expect, res)
	}
}

func TestStringRight(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.Right]
	tests := []struct {
		str    any
		length any
		expect any
	}{
		{"helloworld", 5, "world"},
		{"helloworld", 10, "helloworld"},
		{"helloworld", 11, "helloworld"},
		{"helloworld", -1, ""},
		{"", 2, ""},
		{nil, 2, nil},
	}

	for _, test := range tests {
		str := types.NewDatum(test.str)
		length := types.NewDatum(test.length)
		f, _ := fc.getFunction(ctx, datumsToConstants([]types.Datum{str, length}))
		result, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		if result.IsNull() {
			require.Nil(t, test.expect)
			continue
		}
		res, err := result.ToString()
		require.NoError(t, err)
		require.Equal(t, test.expect, res)
	}
}

func TestWeightString(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.WeightString]
	tests := []struct {
		expr    any
		padding string
		length  int
		expect  any
	}{
		{nil, "NONE", 0, nil},
		{7, "NONE", 0, nil},
		{7.0, "NONE", 0, nil},
		{"a", "NONE", 0, "a"},
		{"a ", "NONE", 0, "a"},
		{"中", "NONE", 0, "中"},
		{"中 ", "NONE", 0, "中"},
		{nil, "CHAR", 5, nil},
		{7, "CHAR", 5, nil},
		{7.0, "NONE", 0, nil},
		{"a", "CHAR", 5, "a"},
		{"a ", "CHAR", 5, "a"},
		{"中", "CHAR", 5, "中"},
		{"中 ", "CHAR", 5, "中"},
		{nil, "BINARY", 5, nil},
		{7, "BINARY", 2, "7\x00"},
		{7.0, "NONE", 0, nil},
		{"a", "BINARY", 1, "a"},
		{"ab", "BINARY", 1, "a"},
		{"a", "BINARY", 5, "a\x00\x00\x00\x00"},
		{"a ", "BINARY", 5, "a \x00\x00\x00"},
		{"中", "BINARY", 1, "\xe4"},
		{"中", "BINARY", 2, "\xe4\xb8"},
		{"中", "BINARY", 3, "中"},
		{"中", "BINARY", 5, "中\x00\x00"},
	}

	for _, test := range tests {
		str := types.NewDatum(test.expr)
		var f builtinFunc
		var err error
		if test.padding == "NONE" {
			f, err = fc.getFunction(ctx, datumsToConstants([]types.Datum{str}))
		} else {
			padding := types.NewDatum(test.padding)
			length := types.NewDatum(test.length)
			f, err = fc.getFunction(ctx, datumsToConstants([]types.Datum{str, padding, length}))
		}
		require.NoError(t, err)

		retType := f.getRetTp()
		require.Equal(t, charset.CollationBin, retType.GetCollate())

		// Reset warnings.
		ctx.GetSessionVars().StmtCtx.ResetForRetry()
		result, err := evalBuiltinFunc(f, ctx, chunk.Row{})
		require.NoError(t, err)
		if result.IsNull() {
			require.Nil(t, test.expect)
			continue
		}
		res, err := result.ToString()
		require.NoError(t, err)
		require.Equal(t, test.expect, res)
		if test.expr == nil {
			continue
		}
		strExpr := fmt.Sprintf("%v", test.expr)
		if test.padding == "BINARY" && test.length < len(strExpr) {
			expectWarn := fmt.Sprintf("[expression:1292]Truncated incorrect BINARY(%d) value: '%s'", test.length, strExpr)
			obtainedWarns := ctx.GetSessionVars().StmtCtx.GetWarnings()
			require.Equal(t, 1, len(obtainedWarns))
			require.Equal(t, "Warning", obtainedWarns[0].Level)
			require.Equal(t, expectWarn, obtainedWarns[0].Err.Error())
		}
	}
}

func TestTranslate(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args  []any
		isNil bool
		isErr bool
		res   string
	}{
		{[]any{"ABC", "A", "B"}, false, false, "BBC"},
		{[]any{"ABC", "Z", "ABC"}, false, false, "ABC"},
		{[]any{"A.B.C", ".A", "|"}, false, false, "|B|C"},
		{[]any{"中文", "文", "国"}, false, false, "中国"},
		{[]any{"UPPERCASE", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"}, false, false, "uppercase"},
		{[]any{"lowercase", "abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"}, false, false, "LOWERCASE"},
		{[]any{"aaaaabbbbb", "aaabbb", "xyzXYZ"}, false, false, "xxxxxXXXXX"},
		{[]any{"Ti*DB User's Guide", " */'", "___"}, false, false, "Ti_DB_Users_Guide"},
		{[]any{"abc", "ab", ""}, false, false, "c"},
		{[]any{"aaa", "a", ""}, false, false, ""},
		{[]any{"", "null", "null"}, false, false, ""},
		{[]any{"null", "", "null"}, false, false, "null"},
		{[]any{"null", "null", ""}, false, false, ""},
		{[]any{nil, "error", "error"}, true, false, ""},
		{[]any{"error", nil, "error"}, true, false, ""},
		{[]any{"error", "error", nil}, true, false, ""},
		{[]any{nil, nil, nil}, true, false, ""},
		{[]any{[]byte{255}, []byte{255}, []byte{255}}, false, false, string([]byte{255})},
		{[]any{[]byte{255, 255}, []byte{255}, []byte{254}}, false, false, string([]byte{254, 254})},
		{[]any{[]byte{255, 255}, []byte{255, 255}, []byte{254, 253}}, false, false, string([]byte{254, 254})},
		{[]any{[]byte{255, 254, 253, 252, 251}, []byte{253, 252, 251}, []byte{254, 253}}, false, false, string([]byte{255, 254, 254, 253})},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Translate, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(ctx, chunk.Row{})
		if c.isErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			if c.isNil {
				require.Equal(t, types.KindNull, d.Kind())
			} else {
				require.Equal(t, c.res, d.GetString())
			}
		}
	}
}

func TestCIWeightString(t *testing.T) {
	ctx := createContext(t)

	type weightStringTest struct {
		str     string
		padding string
		length  int
		expect  any
	}

	checkResult := func(collation string, tests []weightStringTest) {
		fc := funcs[ast.WeightString]
		for _, test := range tests {
			str := types.NewCollationStringDatum(test.str, collation)
			var f builtinFunc
			var err error
			if test.padding == "NONE" {
				f, err = fc.getFunction(ctx, datumsToConstants([]types.Datum{str}))
			} else {
				padding := types.NewDatum(test.padding)
				length := types.NewDatum(test.length)
				f, err = fc.getFunction(ctx, datumsToConstants([]types.Datum{str, padding, length}))
			}
			require.NoError(t, err)
			result, err := evalBuiltinFunc(f, ctx, chunk.Row{})
			require.NoError(t, err)
			if result.IsNull() {
				require.Nil(t, test.expect)
				continue
			}
			res, err := result.ToString()
			require.NoError(t, err)
			require.Equal(t, test.expect, res, "test case: '%s' '%s' %d", test.str, test.padding, test.length)
		}
	}

	generalTests := []weightStringTest{
		{"aAÁàãăâ", "NONE", 0, "\x00A\x00A\x00A\x00A\x00A\x00A\x00A"},
		{"中", "NONE", 0, "\x4E\x2D"},
		{"a", "CHAR", 5, "\x00A"},
		{"a ", "CHAR", 5, "\x00A"},
		{"中", "CHAR", 5, "\x4E\x2D"},
		{"中 ", "CHAR", 5, "\x4E\x2D"},
		{"a", "BINARY", 1, "a"},
		{"ab", "BINARY", 1, "a"},
		{"a", "BINARY", 5, "a\x00\x00\x00\x00"},
		{"a ", "BINARY", 5, "a \x00\x00\x00"},
		{"中", "BINARY", 1, "\xe4"},
		{"中", "BINARY", 2, "\xe4\xb8"},
		{"中", "BINARY", 3, "中"},
		{"中", "BINARY", 5, "中\x00\x00"},
	}

	unicodeTests := []weightStringTest{
		{"aAÁàãăâ", "NONE", 0, "\x0e3\x0e3\x0e3\x0e3\x0e3\x0e3\x0e3"},
		{"中", "NONE", 0, "\xfb\x40\xce\x2d"},
		{"a", "CHAR", 5, "\x0e3"},
		{"a ", "CHAR", 5, "\x0e3"},
		{"中", "CHAR", 5, "\xfb\x40\xce\x2d"},
		{"中 ", "CHAR", 5, "\xfb\x40\xce\x2d"},
		{"a", "BINARY", 1, "a"},
		{"ab", "BINARY", 1, "a"},
		{"a", "BINARY", 5, "a\x00\x00\x00\x00"},
		{"a ", "BINARY", 5, "a \x00\x00\x00"},
		{"中", "BINARY", 1, "\xe4"},
		{"中", "BINARY", 2, "\xe4\xb8"},
		{"中", "BINARY", 3, "中"},
		{"中", "BINARY", 5, "中\x00\x00"},
	}

	unicode0900Tests := []weightStringTest{
		{"aAÁàãăâ", "NONE", 0, "\x1cG\x1cG\x1cG\x1cG\x1cG\x1cG\x1cG"},
		{"中", "NONE", 0, "\xfb\x40\xce\x2d"},
		{"a", "CHAR", 5, "\x1c\x47\x02\x09\x02\x09\x02\x09\x02\x09"},
		{"a ", "CHAR", 5, "\x1c\x47\x02\x09\x02\x09\x02\x09\x02\x09"},
		{"中", "CHAR", 5, "\xfb\x40\xce\x2d\x02\x09\x02\x09\x02\x09\x02\x09"},
		{"中 ", "CHAR", 5, "\xfb\x40\xce\x2d\x02\x09\x02\x09\x02\x09\x02\x09"},
		{"a", "BINARY", 1, "a"},
		{"ab", "BINARY", 1, "a"},
		{"a", "BINARY", 5, "a\x00\x00\x00\x00"},
		{"a ", "BINARY", 5, "a \x00\x00\x00"},
		{"中", "BINARY", 1, "\xe4"},
		{"中", "BINARY", 2, "\xe4\xb8"},
		{"中", "BINARY", 3, "中"},
		{"中", "BINARY", 5, "中\x00\x00"},
	}

	checkResult("utf8mb4_general_ci", generalTests)
	checkResult("utf8mb4_unicode_ci", unicodeTests)
	checkResult("utf8mb4_0900_ai_ci", unicode0900Tests)
}
