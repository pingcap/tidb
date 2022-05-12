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
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit/testutil"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestLengthAndOctetLength(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args     interface{}
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
			f, err := newFunctionForTest(ctx, lengthMethod, primitiveValsToConstants(ctx, []interface{}{c.args})...)
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
			err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, c.chs)
			require.NoError(t, err)
			f, err := newFunctionForTest(ctx, lengthMethod, primitiveValsToConstants(ctx, []interface{}{c.input})...)
			require.NoError(t, err)
			d, err := f.Eval(chunk.Row{})
			require.NoError(t, err)
			require.Equal(t, c.result, d.GetInt64())
		}
	}
}

func TestASCII(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args     interface{}
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
		f, err := newFunctionForTest(ctx, ast.ASCII, primitiveValsToConstants(ctx, []interface{}{c.args})...)
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
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, c.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.ASCII, primitiveValsToConstants(ctx, []interface{}{c.input})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, c.result, d.GetInt64())
	}
}

func TestConcat(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args    []interface{}
		isNil   bool
		getErr  bool
		res     string
		retType *types.FieldType
	}{
		{
			[]interface{}{nil},
			true, false, "",
			types.NewFieldTypeBuilder().SetType(mysql.TypeVarString).SetFlag(mysql.BinaryFlag).SetDecimal(types.UnspecifiedLength).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin).BuildP(),
		},
		{
			[]interface{}{"a", "b",
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
			[]interface{}{"a", "b", nil, "c"},
			true, false, "",
			types.NewFieldTypeBuilder().SetType(mysql.TypeVarString).SetFlag(mysql.BinaryFlag).SetFlen(3).SetDecimal(types.UnspecifiedLength).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin).BuildP(),
		},
		{
			[]interface{}{errors.New("must error")},
			false, true, "",
			types.NewFieldTypeBuilder().SetType(mysql.TypeVarString).SetFlag(mysql.BinaryFlag).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin).BuildP(),
		},
	}
	fcName := ast.Concat
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, fcName, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		v, err := f.Eval(chunk.Row{})
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
	base := baseBuiltinFunc{args: args, ctx: ctx, tp: resultType}
	concat := &builtinConcatSig{base, 5}

	cases := []struct {
		args     []interface{}
		warnings int
		res      string
	}{
		{[]interface{}{"a", "b"}, 0, "ab"},
		{[]interface{}{"aaa", "bbb"}, 1, ""},
		{[]interface{}{"中", "a"}, 0, "中a"},
		{[]interface{}{"中文", "a"}, 2, ""},
	}

	for _, c := range cases {
		input := chunk.NewChunkWithCapacity(colTypes, 10)
		input.AppendString(0, c.args[0].(string))
		input.AppendString(1, c.args[1].(string))

		res, isNull, err := concat.evalString(input.GetRow(0))
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
		args     []interface{}
		isNil    bool
		getErr   bool
		expected string
	}{
		{
			[]interface{}{nil, nil},
			true, false, "",
		},
		{
			[]interface{}{nil, "a", "b"},
			true, false, "",
		},
		{
			[]interface{}{",", "a", "b", "hello", `$^%`},
			false, false,
			`a,b,hello,$^%`,
		},
		{
			[]interface{}{"|", "a", nil, "b", "c"},
			false, false,
			"a|b|c",
		},
		{
			[]interface{}{",", "a", ",", "b", "c"},
			false, false,
			"a,,,b,c",
		},
		{
			[]interface{}{errors.New("must error"), "a", "b"},
			false, true, "",
		},
		{
			[]interface{}{",", "a", "b", 1, 2, 1.1, 0.11,
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
	_, err := newFunctionForTest(ctx, fcName, primitiveValsToConstants(ctx, []interface{}{nil})...)
	require.Error(t, err)

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, fcName, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		val, err1 := f.Eval(chunk.Row{})
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

	_, err = funcs[ast.ConcatWS].getFunction(ctx, primitiveValsToConstants(ctx, []interface{}{nil, nil}))
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
	base := baseBuiltinFunc{args: args, ctx: ctx, tp: resultType}
	concat := &builtinConcatWSSig{base, 6}

	cases := []struct {
		args     []interface{}
		warnings int
		res      string
	}{
		{[]interface{}{",", "a", "b"}, 0, "a,b"},
		{[]interface{}{",", "aaa", "bbb"}, 1, ""},
		{[]interface{}{",", "中", "a"}, 0, "中,a"},
		{[]interface{}{",", "中文", "a"}, 2, ""},
	}

	for _, c := range cases {
		input := chunk.NewChunkWithCapacity(colTypes, 10)
		input.AppendString(0, c.args[0].(string))
		input.AppendString(1, c.args[1].(string))
		input.AppendString(2, c.args[2].(string))

		res, isNull, err := concat.evalString(input.GetRow(0))
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
	origin := stmtCtx.IgnoreTruncate
	stmtCtx.IgnoreTruncate = true
	defer func() {
		stmtCtx.IgnoreTruncate = origin
	}()

	cases := []struct {
		args   []interface{}
		isNil  bool
		getErr bool
		res    string
	}{
		{[]interface{}{"abcde", 3}, false, false, "abc"},
		{[]interface{}{"abcde", 0}, false, false, ""},
		{[]interface{}{"abcde", 1.2}, false, false, "a"},
		{[]interface{}{"abcde", 1.9}, false, false, "ab"},
		{[]interface{}{"abcde", -1}, false, false, ""},
		{[]interface{}{"abcde", 100}, false, false, "abcde"},
		{[]interface{}{"abcde", nil}, true, false, ""},
		{[]interface{}{nil, 3}, true, false, ""},
		{[]interface{}{"abcde", "3"}, false, false, "abc"},
		{[]interface{}{"abcde", "a"}, false, false, ""},
		{[]interface{}{1234, 3}, false, false, "123"},
		{[]interface{}{12.34, 3}, false, false, "12."},
		{[]interface{}{types.NewBinaryLiteralFromUint(0x0102, -1), 1}, false, false, string([]byte{0x01})},
		{[]interface{}{errors.New("must err"), 0}, false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Left, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		v, err := f.Eval(chunk.Row{})
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
	origin := stmtCtx.IgnoreTruncate
	stmtCtx.IgnoreTruncate = true
	defer func() {
		stmtCtx.IgnoreTruncate = origin
	}()

	cases := []struct {
		args   []interface{}
		isNil  bool
		getErr bool
		res    string
	}{
		{[]interface{}{"abcde", 3}, false, false, "cde"},
		{[]interface{}{"abcde", 0}, false, false, ""},
		{[]interface{}{"abcde", 1.2}, false, false, "e"},
		{[]interface{}{"abcde", 1.9}, false, false, "de"},
		{[]interface{}{"abcde", -1}, false, false, ""},
		{[]interface{}{"abcde", 100}, false, false, "abcde"},
		{[]interface{}{"abcde", nil}, true, false, ""},
		{[]interface{}{nil, 1}, true, false, ""},
		{[]interface{}{"abcde", "3"}, false, false, "cde"},
		{[]interface{}{"abcde", "a"}, false, false, ""},
		{[]interface{}{1234, 3}, false, false, "234"},
		{[]interface{}{12.34, 3}, false, false, ".34"},
		{[]interface{}{types.NewBinaryLiteralFromUint(0x0102, -1), 1}, false, false, string([]byte{0x02})},
		{[]interface{}{errors.New("must err"), 0}, false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Right, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		v, err := f.Eval(chunk.Row{})
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
	ctx := createContext(t)
	args := []interface{}{"a", int64(2)}
	fc := funcs[ast.Repeat]
	f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "aa", v.GetString())

	args = []interface{}{"a", uint64(2)}
	f, err = fc.getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "aa", v.GetString())

	args = []interface{}{"a", uint64(16777217)}
	f, err = fc.getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.False(t, v.IsNull())

	args = []interface{}{"a", uint64(16777216)}
	f, err = fc.getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.False(t, v.IsNull())

	args = []interface{}{"a", int64(-1)}
	f, err = fc.getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "", v.GetString())

	args = []interface{}{"a", int64(0)}
	f, err = fc.getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "", v.GetString())

	args = []interface{}{"a", uint64(0)}
	f, err = fc.getFunction(ctx, datumsToConstants(types.MakeDatums(args...)))
	require.NoError(t, err)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "", v.GetString())
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
	base := baseBuiltinFunc{args: args, ctx: ctx, tp: resultType}
	repeat := &builtinRepeatSig{base, 1000}

	cases := []struct {
		args    []interface{}
		warning int
		res     string
	}{
		{[]interface{}{"a", int64(6)}, 0, "aaaaaa"},
		{[]interface{}{"a", int64(10001)}, 1, ""},
		{[]interface{}{"毅", int64(6)}, 0, "毅毅毅毅毅毅"},
		{[]interface{}{"毅", int64(334)}, 2, ""},
	}

	for _, c := range cases {
		input := chunk.NewChunkWithCapacity(colTypes, 10)
		input.AppendString(0, c.args[0].(string))
		input.AppendInt64(1, c.args[1].(int64))

		res, isNull, err := repeat.evalString(input.GetRow(0))
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
		args   []interface{}
		isNil  bool
		getErr bool
		res    string
	}{
		{[]interface{}{nil}, true, false, ""},
		{[]interface{}{"ab"}, false, false, "ab"},
		{[]interface{}{1}, false, false, "1"},
		{[]interface{}{"one week’s time TEST"}, false, false, "one week’s time test"},
		{[]interface{}{"one week's time TEST"}, false, false, "one week's time test"},
		{[]interface{}{"ABC测试DEF"}, false, false, "abc测试def"},
		{[]interface{}{"ABCテストDEF"}, false, false, "abcテストdef"},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Lower, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		v, err := f.Eval(chunk.Row{})
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
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, c.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.Lower, primitiveValsToConstants(ctx, []interface{}{c.input})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, c.result, d.GetString())
	}
}

func TestUpper(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args   []interface{}
		isNil  bool
		getErr bool
		res    string
	}{
		{[]interface{}{nil}, true, false, ""},
		{[]interface{}{"ab"}, false, false, "ab"},
		{[]interface{}{1}, false, false, "1"},
		{[]interface{}{"one week’s time TEST"}, false, false, "ONE WEEK’S TIME TEST"},
		{[]interface{}{"one week's time TEST"}, false, false, "ONE WEEK'S TIME TEST"},
		{[]interface{}{"abc测试def"}, false, false, "ABC测试DEF"},
		{[]interface{}{"abcテストdef"}, false, false, "ABCテストDEF"},
	}

	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Upper, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		v, err := f.Eval(chunk.Row{})
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
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, c.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.Upper, primitiveValsToConstants(ctx, []interface{}{c.input})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, c.result, d.GetString())
	}
}

func TestReverse(t *testing.T) {
	ctx := createContext(t)
	fc := funcs[ast.Reverse]
	f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(nil)))
	require.NoError(t, err)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, types.KindNull, d.Kind())

	tbl := []struct {
		Input  interface{}
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
		d, err = evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, c["Expect"][0], d)
	}
}

func TestStrcmp(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args   []interface{}
		isNil  bool
		getErr bool
		res    int64
	}{
		{[]interface{}{"123", "123"}, false, false, 0},
		{[]interface{}{"123", "1"}, false, false, 1},
		{[]interface{}{"1", "123"}, false, false, -1},
		{[]interface{}{"123", "45"}, false, false, -1},
		{[]interface{}{123, "123"}, false, false, 0},
		{[]interface{}{"12.34", 12.34}, false, false, 0},
		{[]interface{}{nil, "123"}, true, false, 0},
		{[]interface{}{"123", nil}, true, false, 0},
		{[]interface{}{"", "123"}, false, false, -1},
		{[]interface{}{"123", ""}, false, false, 1},
		{[]interface{}{"", ""}, false, false, 0},
		{[]interface{}{"", nil}, true, false, 0},
		{[]interface{}{nil, ""}, true, false, 0},
		{[]interface{}{nil, nil}, true, false, 0},
		{[]interface{}{"123", errors.New("must err")}, false, true, 0},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Strcmp, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
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
		args   []interface{}
		isNil  bool
		getErr bool
		res    string
		flen   int
	}{
		{[]interface{}{"www.mysql.com", "mysql", "pingcap"}, false, false, "www.pingcap.com", 17},
		{[]interface{}{"www.mysql.com", "w", 1}, false, false, "111.mysql.com", 260},
		{[]interface{}{1234, 2, 55}, false, false, "15534", 20},
		{[]interface{}{"", "a", "b"}, false, false, "", 0},
		{[]interface{}{"abc", "", "d"}, false, false, "abc", 3},
		{[]interface{}{"aaa", "a", ""}, false, false, "", 3},
		{[]interface{}{nil, "a", "b"}, true, false, "", 0},
		{[]interface{}{"a", nil, "b"}, true, false, "", 1},
		{[]interface{}{"a", "b", nil}, true, false, "", 1},
		{[]interface{}{errors.New("must err"), "a", "b"}, false, true, "", -1},
	}
	for i, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Replace, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		require.Equalf(t, c.flen, f.GetType().GetFlen(), "test %v", i)
		d, err := f.Eval(chunk.Row{})
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
		args   []interface{}
		isNil  bool
		getErr bool
		res    string
	}{
		{[]interface{}{"Quadratically", 5}, false, false, "ratically"},
		{[]interface{}{"Sakila", 1}, false, false, "Sakila"},
		{[]interface{}{"Sakila", 2}, false, false, "akila"},
		{[]interface{}{"Sakila", -3}, false, false, "ila"},
		{[]interface{}{"Sakila", 0}, false, false, ""},
		{[]interface{}{"Sakila", 100}, false, false, ""},
		{[]interface{}{"Sakila", -100}, false, false, ""},
		{[]interface{}{"Quadratically", 5, 6}, false, false, "ratica"},
		{[]interface{}{"Sakila", -5, 3}, false, false, "aki"},
		{[]interface{}{"Sakila", 2, 0}, false, false, ""},
		{[]interface{}{"Sakila", 2, -1}, false, false, ""},
		{[]interface{}{"Sakila", 2, 100}, false, false, "akila"},
		{[]interface{}{nil, 2, 3}, true, false, ""},
		{[]interface{}{"Sakila", nil, 3}, true, false, ""},
		{[]interface{}{"Sakila", 2, nil}, true, false, ""},
		{[]interface{}{errors.New("must error"), 2, 3}, false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Substring, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
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
		str           interface{}
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

		r, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, types.KindString, r.Kind())
		require.Equal(t, v.result, r.GetString())
	}

	// Test case for getFunction() error
	errTbl := []struct {
		str interface{}
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
	_, err = evalBuiltinFunc(wrongFunction, chunk.Row{})
	require.Error(t, err)
	require.Equal(t, "[expression:1115]Unknown character set: 'wrongcharset'", err.Error())
}

func TestSubstringIndex(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args   []interface{}
		isNil  bool
		getErr bool
		res    string
	}{
		{[]interface{}{"www.pingcap.com", ".", 2}, false, false, "www.pingcap"},
		{[]interface{}{"www.pingcap.com", ".", -2}, false, false, "pingcap.com"},
		{[]interface{}{"www.pingcap.com", ".", 0}, false, false, ""},
		{[]interface{}{"www.pingcap.com", ".", 100}, false, false, "www.pingcap.com"},
		{[]interface{}{"www.pingcap.com", ".", -100}, false, false, "www.pingcap.com"},
		{[]interface{}{"www.pingcap.com", "d", 0}, false, false, ""},
		{[]interface{}{"www.pingcap.com", "d", 1}, false, false, "www.pingcap.com"},
		{[]interface{}{"www.pingcap.com", "d", -1}, false, false, "www.pingcap.com"},
		{[]interface{}{"www.pingcap.com", "", 0}, false, false, ""},
		{[]interface{}{"www.pingcap.com", "", 1}, false, false, ""},
		{[]interface{}{"www.pingcap.com", "", -1}, false, false, ""},
		{[]interface{}{"", ".", 0}, false, false, ""},
		{[]interface{}{"", ".", 1}, false, false, ""},
		{[]interface{}{"", ".", -1}, false, false, ""},
		{[]interface{}{nil, ".", 1}, true, false, ""},
		{[]interface{}{"www.pingcap.com", nil, 1}, true, false, ""},
		{[]interface{}{"www.pingcap.com", ".", nil}, true, false, ""},
		{[]interface{}{errors.New("must error"), ".", 1}, false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.SubstringIndex, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
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
	origin := stmtCtx.IgnoreTruncate
	stmtCtx.IgnoreTruncate = true
	defer func() {
		stmtCtx.IgnoreTruncate = origin
	}()

	cases := []struct {
		arg    interface{}
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
		f, err := newFunctionForTest(ctx, ast.Space, primitiveValsToConstants(ctx, []interface{}{c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
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
	base := baseBuiltinFunc{args: args, ctx: ctx, tp: resultType}
	space := &builtinSpaceSig{base, 1000}
	input := chunk.NewChunkWithCapacity(colTypes, 10)
	input.AppendInt64(0, 6)
	input.AppendInt64(0, 1001)
	res, isNull, err := space.evalString(input.GetRow(0))
	require.Equal(t, "      ", res)
	require.False(t, isNull)
	require.NoError(t, err)
	res, isNull, err = space.evalString(input.GetRow(1))
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
		Args []interface{}
		Want interface{}
	}{
		{[]interface{}{"bar", "foobarbar"}, 4},
		{[]interface{}{"xbar", "foobar"}, 0},
		{[]interface{}{"", "foobar"}, 1},
		{[]interface{}{"foobar", ""}, 0},
		{[]interface{}{"", ""}, 1},
		{[]interface{}{"好世", "你好世界"}, 2},
		{[]interface{}{"界面", "你好世界"}, 0},
		{[]interface{}{"b", "中a英b文"}, 4},
		{[]interface{}{"bAr", "foobArbar"}, 4},
		{[]interface{}{nil, "foobar"}, nil},
		{[]interface{}{"bar", nil}, nil},
		{[]interface{}{"bar", "foobarbar", 5}, 7},
		{[]interface{}{"xbar", "foobar", 1}, 0},
		{[]interface{}{"", "foobar", 2}, 2},
		{[]interface{}{"foobar", "", 1}, 0},
		{[]interface{}{"", "", 2}, 0},
		{[]interface{}{"A", "大A写的A", 0}, 0},
		{[]interface{}{"A", "大A写的A", 1}, 2},
		{[]interface{}{"A", "大A写的A", 2}, 2},
		{[]interface{}{"A", "大A写的A", 3}, 5},
		{[]interface{}{"BaR", "foobarBaR", 5}, 7},
		{[]interface{}{nil, nil}, nil},
		{[]interface{}{"", nil}, nil},
		{[]interface{}{nil, ""}, nil},
		{[]interface{}{nil, nil, 1}, nil},
		{[]interface{}{"", nil, 1}, nil},
		{[]interface{}{nil, "", 1}, nil},
		{[]interface{}{"foo", nil, -1}, nil},
		{[]interface{}{nil, "bar", 0}, nil},
	}
	Dtbl := tblToDtbl(tbl)
	instr := funcs[ast.Locate]
	for i, c := range Dtbl {
		f, err := instr.getFunction(ctx, datumsToConstants(c["Args"]))
		require.NoError(t, err)
		got, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		require.NotNil(t, f)
		require.Equalf(t, c["Want"][0], got, "[%d]: args: %v", i, c["Args"])
	}
	// 2. Test LOCATE with binary input
	tbl2 := []struct {
		Args []interface{}
		Want interface{}
	}{
		{[]interface{}{[]byte("BaR"), "foobArbar"}, 0},
		{[]interface{}{"BaR", []byte("foobArbar")}, 0},
		{[]interface{}{[]byte("bAr"), "foobarBaR", 5}, 0},
		{[]interface{}{"bAr", []byte("foobarBaR"), 5}, 0},
		{[]interface{}{"bAr", []byte("foobarbAr"), 5}, 7},
	}
	Dtbl2 := tblToDtbl(tbl2)
	for i, c := range Dtbl2 {
		exprs := datumsToConstants(c["Args"])
		types.SetBinChsClnFlag(exprs[0].GetType())
		types.SetBinChsClnFlag(exprs[1].GetType())
		f, err := instr.getFunction(ctx, exprs)
		require.NoError(t, err)
		got, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		require.NotNil(t, f)
		require.Equalf(t, c["Want"][0], got, "[%d]: args: %v", i, c["Args"])
	}
}

func TestTrim(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		args   []interface{}
		isNil  bool
		getErr bool
		res    string
	}{
		{[]interface{}{"   bar   "}, false, false, "bar"},
		{[]interface{}{"\t   bar   \n"}, false, false, "\t   bar   \n"},
		{[]interface{}{"\r   bar   \t"}, false, false, "\r   bar   \t"},
		{[]interface{}{"   \tbar\n     "}, false, false, "\tbar\n"},
		{[]interface{}{""}, false, false, ""},
		{[]interface{}{nil}, true, false, ""},
		{[]interface{}{"xxxbarxxx", "x"}, false, false, "bar"},
		{[]interface{}{"bar", "x"}, false, false, "bar"},
		{[]interface{}{"   bar   ", ""}, false, false, "   bar   "},
		{[]interface{}{"", "x"}, false, false, ""},
		{[]interface{}{"bar", nil}, true, false, ""},
		{[]interface{}{nil, "x"}, true, false, ""},
		{[]interface{}{"xxxbarxxx", "x", int(ast.TrimLeading)}, false, false, "barxxx"},
		{[]interface{}{"barxxyz", "xyz", int(ast.TrimTrailing)}, false, false, "barx"},
		{[]interface{}{"xxxbarxxx", "x", int(ast.TrimBoth)}, false, false, "bar"},
		{[]interface{}{"bar", nil, int(ast.TrimLeading)}, true, false, ""},
		{[]interface{}{errors.New("must error")}, false, true, ""},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Trim, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
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
		arg    interface{}
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
		f, err := newFunctionForTest(ctx, ast.LTrim, primitiveValsToConstants(ctx, []interface{}{c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
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
		arg    interface{}
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
		f, err := newFunctionForTest(ctx, ast.RTrim, primitiveValsToConstants(ctx, []interface{}{c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
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
		arg    interface{}
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
		f, err := newFunctionForTest(ctx, ast.Hex, primitiveValsToConstants(ctx, []interface{}{c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
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
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, c.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.Hex, primitiveValsToConstants(ctx, []interface{}{c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
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
		arg    interface{}
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
		f, err := newFunctionForTest(ctx, ast.Unhex, primitiveValsToConstants(ctx, []interface{}{c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
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
		args     interface{}
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
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, c.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.BitLength, primitiveValsToConstants(ctx, []interface{}{c.args})...)
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

	_, err := funcs[ast.BitLength].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestChar(t *testing.T) {
	ctx := createContext(t)
	ctx.GetSessionVars().StmtCtx.IgnoreTruncate = true
	tbl := []struct {
		str      string
		iNum     int64
		fNum     float64
		charset  interface{}
		result   interface{}
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
	run := func(i int, result interface{}, warnCnt int, dts ...interface{}) {
		fc := funcs[ast.CharFunc]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(dts...)))
		require.NoError(t, err, i)
		require.NotNil(t, f, i)
		r, err := evalBuiltinFunc(f, chunk.Row{})
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
	ctx.GetSessionVars().StrictSQLMode = true
	run(-1, nil, 1, 123456, "utf8")
	ctx.GetSessionVars().StrictSQLMode = false
	run(-2, string([]byte{1}), 1, 123456, "utf8")
}

func TestCharLength(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		input  interface{}
		result interface{}
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
		r, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(v.result), r)
	}

	// Test binary string
	tbl = []struct {
		input  interface{}
		result interface{}
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
		tp := arg[0].GetType()
		tp.SetType(mysql.TypeVarString)
		tp.SetCharset(charset.CharsetBin)
		tp.SetCollate(charset.CollationBin)
		tp.SetFlen(types.UnspecifiedLength)
		tp.SetFlag(mysql.BinaryFlag)
		f, err := fc.getFunction(ctx, arg)
		require.NoError(t, err)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(v.result), r)
	}
}

func TestFindInSet(t *testing.T) {
	ctx := createContext(t)
	for _, c := range []struct {
		str    interface{}
		strlst interface{}
		ret    interface{}
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
		r, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(c.ret), r, fmt.Sprintf("FindInSet(%s, %s)", c.str, c.strlst))
	}
}

func TestField(t *testing.T) {
	ctx := createContext(t)
	stmtCtx := ctx.GetSessionVars().StmtCtx
	origin := stmtCtx.IgnoreTruncate
	stmtCtx.IgnoreTruncate = true
	defer func() {
		stmtCtx.IgnoreTruncate = origin
	}()

	tbl := []struct {
		argLst []interface{}
		ret    interface{}
	}{
		{[]interface{}{"ej", "Hej", "ej", "Heja", "hej", "foo"}, int64(2)},
		{[]interface{}{"fo", "Hej", "ej", "Heja", "hej", "foo"}, int64(0)},
		{[]interface{}{"ej", "Hej", "ej", "Heja", "ej", "hej", "foo"}, int64(2)},
		{[]interface{}{1, 2, 3, 11, 1}, int64(4)},
		{[]interface{}{nil, 2, 3, 11, 1}, int64(0)},
		{[]interface{}{1.1, 2.1, 3.1, 11.1, 1.1}, int64(4)},
		{[]interface{}{1.1, "2.1", "3.1", "11.1", "1.1"}, int64(4)},
		{[]interface{}{"1.1a", 2.1, 3.1, 11.1, 1.1}, int64(4)},
		{[]interface{}{1.10, 0, 11e-1}, int64(2)},
		{[]interface{}{"abc", 0, 1, 11.1, 1.1}, int64(1)},
	}
	for _, c := range tbl {
		fc := funcs[ast.Field]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(c.argLst...)))
		require.NoError(t, err)
		require.NotNil(t, f)
		r, err := evalBuiltinFunc(f, chunk.Row{})
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
		expect interface{}
	}{
		{"hi", 5, "?", "???hi"},
		{"hi", 1, "?", "h"},
		{"hi", 0, "?", ""},
		{"hi", -1, "?", nil},
		{"hi", 1, "", "h"},
		{"hi", 5, "", nil},
		{"hi", 5, "ab", "abahi"},
		{"hi", 6, "ab", "ababhi"},
	}
	fc := funcs[ast.Lpad]
	for _, test := range tests {
		str := types.NewStringDatum(test.str)
		length := types.NewIntDatum(test.len)
		padStr := types.NewStringDatum(test.padStr)
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{str, length, padStr}))
		require.NoError(t, err)
		require.NotNil(t, f)
		result, err := evalBuiltinFunc(f, chunk.Row{})
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
		expect interface{}
	}{
		{"hi", 5, "?", "hi???"},
		{"hi", 1, "?", "h"},
		{"hi", 0, "?", ""},
		{"hi", -1, "?", nil},
		{"hi", 1, "", "h"},
		{"hi", 5, "", nil},
		{"hi", 5, "ab", "hiaba"},
		{"hi", 6, "ab", "hiabab"},
	}
	fc := funcs[ast.Rpad]
	for _, test := range tests {
		str := types.NewStringDatum(test.str)
		length := types.NewIntDatum(test.len)
		padStr := types.NewStringDatum(test.padStr)
		f, err := fc.getFunction(ctx, datumsToConstants([]types.Datum{str, length, padStr}))
		require.NoError(t, err)
		require.NotNil(t, f)
		result, err := evalBuiltinFunc(f, chunk.Row{})
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

	base := baseBuiltinFunc{args: args, ctx: ctx, tp: resultType}
	rpad := &builtinRpadUTF8Sig{base, 1000}

	input := chunk.NewChunkWithCapacity(colTypes, 10)
	input.AppendString(0, "abc")
	input.AppendString(0, "abc")
	input.AppendInt64(1, 6)
	input.AppendInt64(1, 10000)
	input.AppendString(2, "123")
	input.AppendString(2, "123")

	res, isNull, err := rpad.evalString(input.GetRow(0))
	require.Equal(t, "abc123", res)
	require.False(t, isNull)
	require.NoError(t, err)

	res, isNull, err = rpad.evalString(input.GetRow(1))
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

	base := baseBuiltinFunc{args: args, ctx: ctx, tp: resultType}
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

	res, isNull, err := insert.evalString(input.GetRow(0))
	require.Equal(t, "abd", res)
	require.False(t, isNull)
	require.NoError(t, err)

	res, isNull, err = insert.evalString(input.GetRow(1))
	require.Equal(t, "", res)
	require.True(t, isNull)
	require.NoError(t, err)

	res, isNull, err = insert.evalString(input.GetRow(2))
	require.Equal(t, "abc", res)
	require.False(t, isNull)
	require.NoError(t, err)

	res, isNull, err = insert.evalString(input.GetRow(3))
	require.Equal(t, "", res)
	require.True(t, isNull)
	require.NoError(t, err)

	res, isNull, err = insert.evalString(input.GetRow(4))
	require.Equal(t, "", res)
	require.True(t, isNull)
	require.NoError(t, err)

	res, isNull, err = insert.evalString(input.GetRow(5))
	require.Equal(t, "", res)
	require.True(t, isNull)
	require.NoError(t, err)

	res, isNull, err = insert.evalString(input.GetRow(6))
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
		Args []interface{}
		Want interface{}
	}{
		{[]interface{}{"foobarbar", "bar"}, 4},
		{[]interface{}{"xbar", "foobar"}, 0},

		{[]interface{}{123456234, 234}, 2},
		{[]interface{}{123456, 567}, 0},
		{[]interface{}{1e10, 1e2}, 1},
		{[]interface{}{1.234, ".234"}, 2},
		{[]interface{}{1.234, ""}, 1},
		{[]interface{}{"", 123}, 0},
		{[]interface{}{"", ""}, 1},

		{[]interface{}{"中文美好", "美好"}, 3},
		{[]interface{}{"中文美好", "世界"}, 0},
		{[]interface{}{"中文abc", "a"}, 3},

		{[]interface{}{"live long and prosper", "long"}, 6},

		{[]interface{}{"not binary string", "binary"}, 5},
		{[]interface{}{"upper case", "upper"}, 1},
		{[]interface{}{"UPPER CASE", "CASE"}, 7},
		{[]interface{}{"中文abc", "abc"}, 3},

		{[]interface{}{"foobar", nil}, nil},
		{[]interface{}{nil, "foobar"}, nil},
		{[]interface{}{nil, nil}, nil},
	}

	Dtbl := tblToDtbl(tbl)
	instr := funcs[ast.Instr]
	for i, c := range Dtbl {
		f, err := instr.getFunction(ctx, datumsToConstants(c["Args"]))
		require.NoError(t, err)
		require.NotNil(t, f)
		got, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		require.Equalf(t, c["Want"][0], got, "[%d]: args: %v", i, c["Args"])
	}
}

func TestLoadFile(t *testing.T) {
	ctx := createContext(t)
	cases := []struct {
		arg    interface{}
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
		f, err := newFunctionForTest(ctx, ast.LoadFile, primitiveValsToConstants(ctx, []interface{}{c.arg})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
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
		argList []interface{}
		ret     interface{}
	}{
		{[]interface{}{1, "a", "b", "c"}, "a"},
		{[]interface{}{1 | 4, "hello", "nice", "world"}, "hello,world"},
		{[]interface{}{1 | 4, "hello", "nice", nil, "world"}, "hello"},
		{[]interface{}{0, "a", "b", "c"}, ""},
		{[]interface{}{nil, "a", "b", "c"}, nil},
		{[]interface{}{-100 | 4, "hello", "nice", "abc", "world"}, "abc,world"},
		{[]interface{}{-1, "hello", "nice", "abc", "world"}, "hello,nice,abc,world"},
	}

	for _, c := range tbl {
		fc := funcs[ast.MakeSet]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(c.argList...)))
		require.NoError(t, err)
		require.NotNil(t, f)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(c.ret), r)
	}
}

func TestOct(t *testing.T) {
	ctx := createContext(t)
	octTests := []struct {
		origin interface{}
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
		r, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		res, err := r.ToString()
		require.NoError(t, err)
		require.Equalf(t, tt.ret, res, "select oct(%v);", tt.origin)
	}
	// tt NULL input for sha
	var argNull types.Datum
	f, _ := fc.getFunction(ctx, datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	require.NoError(t, err)
	require.True(t, r.IsNull())
}

func TestFormat(t *testing.T) {
	ctx := createContext(t)
	formatTests := []struct {
		number    interface{}
		precision interface{}
		locale    string
		ret       interface{}
	}{
		{12332.12341111111111111111111111111111111111111, 4, "en_US", "12,332.1234"},
		{nil, 22, "en_US", nil},
	}
	formatTests1 := []struct {
		number    interface{}
		precision interface{}
		ret       interface{}
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
		number    interface{}
		precision interface{}
		locale    string
		ret       interface{}
	}{-12332.123456, -4, "zh_CN", "-12,332"}
	formatTests3 := struct {
		number    interface{}
		precision interface{}
		locale    string
		ret       interface{}
	}{"-12332.123456", "4", "de_GE", "-12,332.1235"}
	formatTests4 := struct {
		number    interface{}
		precision interface{}
		locale    interface{}
		ret       interface{}
	}{1, 4, nil, "1.0000"}

	fc := funcs[ast.Format]
	for _, tt := range formatTests {
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.number, tt.precision, tt.locale)))
		require.NoError(t, err)
		require.NotNil(t, f)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(tt.ret), r)
	}

	origConfig := ctx.GetSessionVars().StmtCtx.TruncateAsWarning
	ctx.GetSessionVars().StmtCtx.TruncateAsWarning = true
	for _, tt := range formatTests1 {
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(tt.number, tt.precision)))
		require.NoError(t, err)
		require.NotNil(t, f)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(tt.ret), r, fmt.Sprintf("test %v", tt))
		if tt.warnings > 0 {
			warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
			require.Lenf(t, warnings, tt.warnings, "test %v", tt)
			for i := 0; i < tt.warnings; i++ {
				require.Truef(t, terror.ErrorEqual(types.ErrTruncatedWrongVal, warnings[i].Err), "test %v", tt)
			}
			ctx.GetSessionVars().StmtCtx.SetWarnings([]stmtctx.SQLWarn{})
		}
	}
	ctx.GetSessionVars().StmtCtx.TruncateAsWarning = origConfig

	f2, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(formatTests2.number, formatTests2.precision, formatTests2.locale)))
	require.NoError(t, err)
	require.NotNil(t, f2)
	r2, err := evalBuiltinFunc(f2, chunk.Row{})
	testutil.DatumEqual(t, types.NewDatum(errors.New("not implemented")), types.NewDatum(err))
	testutil.DatumEqual(t, types.NewDatum(formatTests2.ret), r2)

	f3, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(formatTests3.number, formatTests3.precision, formatTests3.locale)))
	require.NoError(t, err)
	require.NotNil(t, f3)
	r3, err := evalBuiltinFunc(f3, chunk.Row{})
	testutil.DatumEqual(t, types.NewDatum(errors.New("not support for the specific locale")), types.NewDatum(err))
	testutil.DatumEqual(t, types.NewDatum(formatTests3.ret), r3)

	f4, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(formatTests4.number, formatTests4.precision, formatTests4.locale)))
	require.NoError(t, err)
	require.NotNil(t, f4)
	r4, err := evalBuiltinFunc(f4, chunk.Row{})
	require.NoError(t, err)
	testutil.DatumEqual(t, types.NewDatum(formatTests4.ret), r4)
	warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
	require.Equal(t, 3, len(warnings))
	for i := 0; i < 3; i++ {
		require.True(t, terror.ErrorEqual(errUnknownLocale, warnings[i].Err))
	}
	ctx.GetSessionVars().StmtCtx.SetWarnings([]stmtctx.SQLWarn{})
}

func TestFromBase64(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args   interface{}
		expect interface{}
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
		result, err := evalBuiltinFunc(f, chunk.Row{})
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
		base := baseBuiltinFunc{args: args, ctx: ctx, tp: resultType}
		fromBase64 := &builtinFromBase64Sig{base, test.maxAllowPacket}

		input := chunk.NewChunkWithCapacity(colTypes, 1)
		input.AppendString(0, test.args)
		res, isNull, err := fromBase64.evalString(input.GetRow(0))
		require.NoError(t, err)
		require.Equal(t, test.isNil, isNull)
		if isNull {
			warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
			require.Equal(t, 1, len(warnings))
			lastWarn := warnings[len(warnings)-1]
			require.True(t, terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err))
			ctx.GetSessionVars().StmtCtx.SetWarnings([]stmtctx.SQLWarn{})
		}
		require.Equal(t, test.expect, res)
	}
}

func TestInsert(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args   []interface{}
		expect interface{}
	}{
		{[]interface{}{"Quadratic", 3, 4, "What"}, "QuWhattic"},
		{[]interface{}{"Quadratic", -1, 4, "What"}, "Quadratic"},
		{[]interface{}{"Quadratic", 3, 100, "What"}, "QuWhat"},
		{[]interface{}{nil, 3, 100, "What"}, nil},
		{[]interface{}{"Quadratic", nil, 4, "What"}, nil},
		{[]interface{}{"Quadratic", 3, nil, "What"}, nil},
		{[]interface{}{"Quadratic", 3, 4, nil}, nil},
		{[]interface{}{"Quadratic", 3, -1, "What"}, "QuWhat"},
		{[]interface{}{"Quadratic", 3, 1, "What"}, "QuWhatdratic"},
		{[]interface{}{"Quadratic", -1, nil, "What"}, nil},
		{[]interface{}{"Quadratic", -1, 4, nil}, nil},

		{[]interface{}{"我叫小雨呀", 3, 2, "王雨叶"}, "我叫王雨叶呀"},
		{[]interface{}{"我叫小雨呀", -1, 2, "王雨叶"}, "我叫小雨呀"},
		{[]interface{}{"我叫小雨呀", 3, 100, "王雨叶"}, "我叫王雨叶"},
		{[]interface{}{nil, 3, 100, "王雨叶"}, nil},
		{[]interface{}{"我叫小雨呀", nil, 4, "王雨叶"}, nil},
		{[]interface{}{"我叫小雨呀", 3, nil, "王雨叶"}, nil},
		{[]interface{}{"我叫小雨呀", 3, 4, nil}, nil},
		{[]interface{}{"我叫小雨呀", 3, -1, "王雨叶"}, "我叫王雨叶"},
		{[]interface{}{"我叫小雨呀", 3, 1, "王雨叶"}, "我叫王雨叶雨呀"},
		{[]interface{}{"我叫小雨呀", -1, nil, "王雨叶"}, nil},
		{[]interface{}{"我叫小雨呀", -1, 2, nil}, nil},
	}
	fc := funcs[ast.InsertFunc]
	for _, test := range tests {
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(test.args...)))
		require.NoError(t, err)
		require.NotNil(t, f)
		result, err := evalBuiltinFunc(f, chunk.Row{})
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
		args     interface{}
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
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, c.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.Ord, primitiveValsToConstants(ctx, []interface{}{c.args})...)
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
	_, err := funcs[ast.Ord].getFunction(ctx, []Expression{NewZero()})
	require.NoError(t, err)
}

func TestElt(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		argLst []interface{}
		ret    interface{}
	}{
		{[]interface{}{1, "Hej", "ej", "Heja", "hej", "foo"}, "Hej"},
		{[]interface{}{9, "Hej", "ej", "Heja", "hej", "foo"}, nil},
		{[]interface{}{-1, "Hej", "ej", "Heja", "ej", "hej", "foo"}, nil},
		{[]interface{}{0, 2, 3, 11, 1}, nil},
		{[]interface{}{3, 2, 3, 11, 1}, "11"},
		{[]interface{}{1.1, "2.1", "3.1", "11.1", "1.1"}, "2.1"},
	}
	for _, c := range tbl {
		fc := funcs[ast.Elt]
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(c.argLst...)))
		require.NoError(t, err)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(c.ret), r)
	}
}

func TestExportSet(t *testing.T) {
	ctx := createContext(t)
	estd := []struct {
		argLst []interface{}
		res    string
	}{
		{[]interface{}{-9223372036854775807, "Y", "N", ",", 5}, "Y,N,N,N,N"},
		{[]interface{}{-6, "Y", "N", ",", 5}, "N,Y,N,Y,Y"},
		{[]interface{}{5, "Y", "N", ",", 4}, "Y,N,Y,N"},
		{[]interface{}{5, "Y", "N", ",", 0}, ""},
		{[]interface{}{5, "Y", "N", ",", 1}, "Y"},
		{[]interface{}{6, "1", "0", ",", 10}, "0,1,1,0,0,0,0,0,0,0"},
		{[]interface{}{333333, "Ysss", "sN", "---", 9}, "Ysss---sN---Ysss---sN---Ysss---sN---sN---sN---sN"},
		{[]interface{}{7, "Y", "N"}, "Y,Y,Y,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N,N"},
		{[]interface{}{7, "Y", "N", 6}, "Y6Y6Y6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N"},
		{[]interface{}{7, "Y", "N", 6, 133}, "Y6Y6Y6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N6N"},
	}
	fc := funcs[ast.ExportSet]
	for _, c := range estd {
		f, err := fc.getFunction(ctx, datumsToConstants(types.MakeDatums(c.argLst...)))
		require.NoError(t, err)
		require.NotNil(t, f)
		exportSetRes, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		res, err := exportSetRes.ToString()
		require.NoError(t, err)
		require.Equal(t, c.res, res)
	}
}

func TestBin(t *testing.T) {
	tbl := []struct {
		Input    interface{}
		Expected interface{}
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
	ctx.GetSessionVars().StmtCtx.IgnoreTruncate = true
	for _, c := range dtbl {
		f, err := fc.getFunction(ctx, datumsToConstants(c["Input"]))
		require.NoError(t, err)
		require.NotNil(t, f)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(c["Expected"][0]), r)
	}
}

func TestQuote(t *testing.T) {
	ctx := createContext(t)
	tbl := []struct {
		arg interface{}
		ret interface{}
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
		r, err := evalBuiltinFunc(f, chunk.Row{})
		require.NoError(t, err)
		testutil.DatumEqual(t, types.NewDatum(c.ret), r)
	}
}

func TestToBase64(t *testing.T) {
	ctx := createContext(t)
	tests := []struct {
		args   interface{}
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
			args   interface{}
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
		f, err := newFunctionForTest(ctx, ast.ToBase64, primitiveValsToConstants(ctx, []interface{}{test.args})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
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
		err := ctx.GetSessionVars().SetSystemVar(variable.CharacterSetConnection, c.chs)
		require.NoError(t, err)
		f, err := newFunctionForTest(ctx, ast.ToBase64, primitiveValsToConstants(ctx, []interface{}{c.input})...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
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
		base := baseBuiltinFunc{args: args, ctx: ctx, tp: resultType}
		toBase64 := &builtinToBase64Sig{base, test.maxAllowPacket}

		input := chunk.NewChunkWithCapacity(colTypes, 1)
		input.AppendString(0, test.args)
		res, isNull, err := toBase64.evalString(input.GetRow(0))
		require.NoError(t, err)
		if test.isNil {
			require.True(t, isNull)

			warnings := ctx.GetSessionVars().StmtCtx.GetWarnings()
			require.Equal(t, 1, len(warnings))
			lastWarn := warnings[len(warnings)-1]
			require.True(t, terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err))
			ctx.GetSessionVars().StmtCtx.SetWarnings([]stmtctx.SQLWarn{})

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
		str    interface{}
		length interface{}
		expect interface{}
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
		result, err := evalBuiltinFunc(f, chunk.Row{})
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
		expr    interface{}
		padding string
		length  int
		expect  interface{}
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
		// Reset warnings.
		ctx.GetSessionVars().StmtCtx.ResetForRetry()
		result, err := evalBuiltinFunc(f, chunk.Row{})
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
		args  []interface{}
		isNil bool
		isErr bool
		res   string
	}{
		{[]interface{}{"ABC", "A", "B"}, false, false, "BBC"},
		{[]interface{}{"ABC", "Z", "ABC"}, false, false, "ABC"},
		{[]interface{}{"A.B.C", ".A", "|"}, false, false, "|B|C"},
		{[]interface{}{"中文", "文", "国"}, false, false, "中国"},
		{[]interface{}{"UPPERCASE", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"}, false, false, "uppercase"},
		{[]interface{}{"lowercase", "abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"}, false, false, "LOWERCASE"},
		{[]interface{}{"aaaaabbbbb", "aaabbb", "xyzXYZ"}, false, false, "xxxxxXXXXX"},
		{[]interface{}{"Ti*DB User's Guide", " */'", "___"}, false, false, "Ti_DB_Users_Guide"},
		{[]interface{}{"abc", "ab", ""}, false, false, "c"},
		{[]interface{}{"aaa", "a", ""}, false, false, ""},
		{[]interface{}{"", "null", "null"}, false, false, ""},
		{[]interface{}{"null", "", "null"}, false, false, "null"},
		{[]interface{}{"null", "null", ""}, false, false, ""},
		{[]interface{}{nil, "error", "error"}, true, false, ""},
		{[]interface{}{"error", nil, "error"}, true, false, ""},
		{[]interface{}{"error", "error", nil}, true, false, ""},
		{[]interface{}{nil, nil, nil}, true, false, ""},
		{[]interface{}{[]byte{255}, []byte{255}, []byte{255}}, false, false, string([]byte{255})},
		{[]interface{}{[]byte{255, 255}, []byte{255}, []byte{254}}, false, false, string([]byte{254, 254})},
		{[]interface{}{[]byte{255, 255}, []byte{255, 255}, []byte{254, 253}}, false, false, string([]byte{254, 254})},
		{[]interface{}{[]byte{255, 254, 253, 252, 251}, []byte{253, 252, 251}, []byte{254, 253}}, false, false, string([]byte{255, 254, 254, 253})},
	}
	for _, c := range cases {
		f, err := newFunctionForTest(ctx, ast.Translate, primitiveValsToConstants(ctx, c.args)...)
		require.NoError(t, err)
		d, err := f.Eval(chunk.Row{})
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
		expect  interface{}
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
			result, err := evalBuiltinFunc(f, chunk.Row{})
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

	checkResult("utf8mb4_general_ci", generalTests)
	checkResult("utf8mb4_unicode_ci", unicodeTests)
}
