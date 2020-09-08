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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testutil"
)

func (s *testEvaluatorSuite) TestLengthAndOctetLength(c *C) {
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
		for _, t := range cases {
			f, err := newFunctionForTest(s.ctx, lengthMethod, s.primitiveValsToConstants([]interface{}{t.args})...)
			c.Assert(err, IsNil)
			d, err := f.Eval(chunk.Row{})
			if t.getErr {
				c.Assert(err, NotNil)
			} else {
				c.Assert(err, IsNil)
				if t.isNil {
					c.Assert(d.Kind(), Equals, types.KindNull)
				} else {
					c.Assert(d.GetInt64(), Equals, t.expected)
				}
			}
		}
	}

	_, err := funcs[ast.Length].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestASCII(c *C) {
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
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.ASCII, s.primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)

		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}
	_, err := funcs[ast.Length].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestConcat(c *C) {
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
			&types.FieldType{Tp: mysql.TypeVarString, Flen: 0, Decimal: types.UnspecifiedLength, Charset: charset.CharsetBin, Collate: charset.CollationBin, Flag: mysql.BinaryFlag},
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
			&types.FieldType{Tp: mysql.TypeVarString, Flen: 40, Decimal: types.UnspecifiedLength, Charset: charset.CharsetBin, Collate: charset.CollationBin, Flag: mysql.BinaryFlag},
		},
		{
			[]interface{}{"a", "b", nil, "c"},
			true, false, "",
			&types.FieldType{Tp: mysql.TypeVarString, Flen: 3, Decimal: types.UnspecifiedLength, Charset: charset.CharsetBin, Collate: charset.CollationBin, Flag: mysql.BinaryFlag},
		},
		{
			[]interface{}{errors.New("must error")},
			false, true, "",
			&types.FieldType{Tp: mysql.TypeVarString, Flen: types.UnspecifiedLength, Decimal: types.UnspecifiedLength, Charset: charset.CharsetBin, Collate: charset.CollationBin, Flag: mysql.BinaryFlag},
		},
	}
	fcName := ast.Concat
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, fcName, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		v, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(v.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(v.GetString(), Equals, t.res)
			}
		}
	}
}

func (s *testEvaluatorSuite) TestConcatSig(c *C) {
	colTypes := []*types.FieldType{
		{Tp: mysql.TypeVarchar},
		{Tp: mysql.TypeVarchar},
	}
	resultType := &types.FieldType{Tp: mysql.TypeVarchar, Flen: 1000}
	args := []Expression{
		&Column{Index: 0, RetType: colTypes[0]},
		&Column{Index: 1, RetType: colTypes[1]},
	}
	base := baseBuiltinFunc{args: args, ctx: s.ctx, tp: resultType}
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

	for _, t := range cases {
		input := chunk.NewChunkWithCapacity(colTypes, 10)
		input.AppendString(0, t.args[0].(string))
		input.AppendString(1, t.args[1].(string))

		res, isNull, err := concat.evalString(input.GetRow(0))
		c.Assert(res, Equals, t.res)
		c.Assert(err, IsNil)
		if t.warnings == 0 {
			c.Assert(isNull, IsFalse)
		} else {
			c.Assert(isNull, IsTrue)
			warnings := s.ctx.GetSessionVars().StmtCtx.GetWarnings()
			c.Assert(warnings, HasLen, t.warnings)
			lastWarn := warnings[len(warnings)-1]
			c.Assert(terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err), IsTrue)
		}
	}
}

func (s *testEvaluatorSuite) TestConcatWS(c *C) {
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
	_, err := newFunctionForTest(s.ctx, fcName, s.primitiveValsToConstants([]interface{}{nil})...)
	c.Assert(err, NotNil)

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, fcName, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		val, err1 := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err1, NotNil)
		} else {
			c.Assert(err1, IsNil)
			if t.isNil {
				c.Assert(val.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(val.GetString(), Equals, t.expected)
			}
		}
	}

	_, err = funcs[ast.ConcatWS].getFunction(s.ctx, s.primitiveValsToConstants([]interface{}{nil, nil}))
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestConcatWSSig(c *C) {
	colTypes := []*types.FieldType{
		{Tp: mysql.TypeVarchar},
		{Tp: mysql.TypeVarchar},
		{Tp: mysql.TypeVarchar},
	}
	resultType := &types.FieldType{Tp: mysql.TypeVarchar, Flen: 1000}
	args := []Expression{
		&Column{Index: 0, RetType: colTypes[0]},
		&Column{Index: 1, RetType: colTypes[1]},
		&Column{Index: 2, RetType: colTypes[2]},
	}
	base := baseBuiltinFunc{args: args, ctx: s.ctx, tp: resultType}
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

	for _, t := range cases {
		input := chunk.NewChunkWithCapacity(colTypes, 10)
		input.AppendString(0, t.args[0].(string))
		input.AppendString(1, t.args[1].(string))
		input.AppendString(2, t.args[2].(string))

		res, isNull, err := concat.evalString(input.GetRow(0))
		c.Assert(res, Equals, t.res)
		c.Assert(err, IsNil)
		if t.warnings == 0 {
			c.Assert(isNull, IsFalse)
		} else {
			c.Assert(isNull, IsTrue)
			warnings := s.ctx.GetSessionVars().StmtCtx.GetWarnings()
			c.Assert(warnings, HasLen, t.warnings)
			lastWarn := warnings[len(warnings)-1]
			c.Assert(terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err), IsTrue)
		}
	}
}

func (s *testEvaluatorSuite) TestLeft(c *C) {
	stmtCtx := s.ctx.GetSessionVars().StmtCtx
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
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Left, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		v, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(v.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(v.GetString(), Equals, t.res)
			}
		}
	}

	_, err := funcs[ast.Left].getFunction(s.ctx, []Expression{varcharCon, int8Con})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestRight(c *C) {
	stmtCtx := s.ctx.GetSessionVars().StmtCtx
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
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Right, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		v, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(v.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(v.GetString(), Equals, t.res)
			}
		}
	}

	_, err := funcs[ast.Right].getFunction(s.ctx, []Expression{varcharCon, int8Con})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestRepeat(c *C) {
	args := []interface{}{"a", int64(2)}
	fc := funcs[ast.Repeat]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "aa")

	args = []interface{}{"a", uint64(2)}
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "aa")

	args = []interface{}{"a", uint64(16777217)}
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.IsNull(), IsTrue)

	args = []interface{}{"a", uint64(16777216)}
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.IsNull(), IsFalse)

	args = []interface{}{"a", int64(-1)}
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "")

	args = []interface{}{"a", int64(0)}
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "")

	args = []interface{}{"a", uint64(0)}
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "")
}

func (s *testEvaluatorSuite) TestRepeatSig(c *C) {
	colTypes := []*types.FieldType{
		{Tp: mysql.TypeVarchar},
		{Tp: mysql.TypeLonglong},
	}
	resultType := &types.FieldType{Tp: mysql.TypeVarchar, Flen: 1000}
	args := []Expression{
		&Column{Index: 0, RetType: colTypes[0]},
		&Column{Index: 1, RetType: colTypes[1]},
	}
	base := baseBuiltinFunc{args: args, ctx: s.ctx, tp: resultType}
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

	for _, t := range cases {
		input := chunk.NewChunkWithCapacity(colTypes, 10)
		input.AppendString(0, t.args[0].(string))
		input.AppendInt64(1, t.args[1].(int64))

		res, isNull, err := repeat.evalString(input.GetRow(0))
		c.Assert(res, Equals, t.res)
		c.Assert(err, IsNil)
		if t.warning == 0 {
			c.Assert(isNull, IsFalse)
		} else {
			c.Assert(isNull, IsTrue)
			c.Assert(err, IsNil)
			warnings := s.ctx.GetSessionVars().StmtCtx.GetWarnings()
			c.Assert(len(warnings), Equals, t.warning)
			lastWarn := warnings[len(warnings)-1]
			c.Assert(terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err), IsTrue)
		}
	}
}

func (s *testEvaluatorSuite) TestLower(c *C) {
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

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Lower, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		v, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(v.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(v.GetString(), Equals, t.res)
			}
		}
	}

	_, err := funcs[ast.Lower].getFunction(s.ctx, []Expression{varcharCon})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestUpper(c *C) {
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

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Upper, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		v, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(v.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(v.GetString(), Equals, strings.ToUpper(t.res))
			}
		}
	}

	_, err := funcs[ast.Upper].getFunction(s.ctx, []Expression{varcharCon})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestReverse(c *C) {
	fc := funcs[ast.Reverse]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)

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

	for _, t := range dtbl {
		f, err = fc.getFunction(s.ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		d, err = evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expect"][0])
	}
}

func (s *testEvaluatorSuite) TestStrcmp(c *C) {
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
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Strcmp, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.res)
			}
		}
	}
}

func (s *testEvaluatorSuite) TestReplace(c *C) {
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
	for i, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Replace, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil, Commentf("test %v", i))
		c.Assert(f.GetType().Flen, Equals, t.flen, Commentf("test %v", i))
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil, Commentf("test %v", i))
		} else {
			c.Assert(err, IsNil, Commentf("test %v", i))
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull, Commentf("test %v", i))
			} else {
				c.Assert(d.GetString(), Equals, t.res, Commentf("test %v", i))
			}
		}
	}

	_, err := funcs[ast.Replace].getFunction(s.ctx, []Expression{NewZero(), NewZero(), NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestSubstring(c *C) {
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
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Substring, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetString(), Equals, t.res)
			}
		}
	}

	_, err := funcs[ast.Substring].getFunction(s.ctx, []Expression{NewZero(), NewZero(), NewZero()})
	c.Assert(err, IsNil)

	_, err = funcs[ast.Substring].getFunction(s.ctx, []Expression{NewZero(), NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestConvert(c *C) {
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
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(v.str, v.cs)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		retType := f.getRetTp()
		c.Assert(retType.Charset, Equals, strings.ToLower(v.cs))
		collate, err := charset.GetDefaultCollation(strings.ToLower(v.cs))
		c.Assert(err, IsNil)
		c.Assert(retType.Collate, Equals, collate)
		c.Assert(mysql.HasBinaryFlag(retType.Flag), Equals, v.hasBinaryFlag)

		r, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(r.Kind(), Equals, types.KindString)
		c.Assert(r.GetString(), Equals, v.result)
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
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(v.str, v.cs)))
		c.Assert(err.Error(), Equals, v.err)
		c.Assert(f, IsNil)
	}

	// Test wrong charset while evaluating.
	fc := funcs[ast.Convert]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums("haha", "utf8")))
	c.Assert(err, IsNil)
	c.Assert(f, NotNil)
	wrongFunction := f.(*builtinConvertSig)
	wrongFunction.tp.Charset = "wrongcharset"
	_, err = evalBuiltinFunc(wrongFunction, chunk.Row{})
	c.Assert(err.Error(), Equals, "[expression:1115]Unknown character set: 'wrongcharset'")
}

func (s *testEvaluatorSuite) TestSubstringIndex(c *C) {
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
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.SubstringIndex, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetString(), Equals, t.res)
			}
		}
	}

	_, err := funcs[ast.SubstringIndex].getFunction(s.ctx, []Expression{NewZero(), NewZero(), NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestSpace(c *C) {
	stmtCtx := s.ctx.GetSessionVars().StmtCtx
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
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Space, s.primitiveValsToConstants([]interface{}{t.arg})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetString(), Equals, t.res)
			}
		}
	}

	_, err := funcs[ast.Space].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestSpaceSig(c *C) {
	colTypes := []*types.FieldType{
		{Tp: mysql.TypeLonglong},
	}
	resultType := &types.FieldType{Tp: mysql.TypeVarchar, Flen: 1000}
	args := []Expression{
		&Column{Index: 0, RetType: colTypes[0]},
	}
	base := baseBuiltinFunc{args: args, ctx: s.ctx, tp: resultType}
	space := &builtinSpaceSig{base, 1000}
	input := chunk.NewChunkWithCapacity(colTypes, 10)
	input.AppendInt64(0, 6)
	input.AppendInt64(0, 1001)
	res, isNull, err := space.evalString(input.GetRow(0))
	c.Assert(res, Equals, "      ")
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)
	res, isNull, err = space.evalString(input.GetRow(1))
	c.Assert(res, Equals, "")
	c.Assert(isNull, IsTrue)
	c.Assert(err, IsNil)
	warnings := s.ctx.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 1)
	lastWarn := warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))
}

func (s *testEvaluatorSuite) TestLocate(c *C) {
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
	for i, t := range Dtbl {
		f, err := instr.getFunction(s.ctx, s.datumsToConstants(t["Args"]))
		c.Assert(err, IsNil)
		got, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		c.Assert(got, DeepEquals, t["Want"][0], Commentf("[%d]: args: %v", i, t["Args"]))
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
	for i, t := range Dtbl2 {
		exprs := s.datumsToConstants(t["Args"])
		types.SetBinChsClnFlag(exprs[0].GetType())
		types.SetBinChsClnFlag(exprs[1].GetType())
		f, err := instr.getFunction(s.ctx, exprs)
		c.Assert(err, IsNil)
		got, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		c.Assert(got, DeepEquals, t["Want"][0], Commentf("[%d]: args: %v", i, t["Args"]))
	}
}

func (s *testEvaluatorSuite) TestTrim(c *C) {
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
		// FIXME: the result for this test shuold be nil, current is "bar"
		{[]interface{}{"bar", nil, int(ast.TrimLeading)}, false, false, "bar"},
		{[]interface{}{errors.New("must error")}, false, true, ""},
	}
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Trim, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetString(), Equals, t.res)
			}
		}
	}

	_, err := funcs[ast.Trim].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)

	_, err = funcs[ast.Trim].getFunction(s.ctx, []Expression{NewZero(), NewZero()})
	c.Assert(err, IsNil)

	_, err = funcs[ast.Trim].getFunction(s.ctx, []Expression{NewZero(), NewZero(), NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestLTrim(c *C) {
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
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.LTrim, s.primitiveValsToConstants([]interface{}{t.arg})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetString(), Equals, t.res)
			}
		}
	}

	_, err := funcs[ast.LTrim].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestRTrim(c *C) {
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
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.RTrim, s.primitiveValsToConstants([]interface{}{t.arg})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetString(), Equals, t.res)
			}
		}
	}

	_, err := funcs[ast.RTrim].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestHexFunc(c *C) {
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
		{types.NewBinaryLiteralFromUint(0xC, -1), false, false, "C"},
		{0x12, false, false, "12"},
		{nil, true, false, ""},
		{errors.New("must err"), false, true, ""},
	}
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Hex, s.primitiveValsToConstants([]interface{}{t.arg})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetString(), Equals, t.res)
			}
		}
	}

	_, err := funcs[ast.Hex].getFunction(s.ctx, []Expression{int8Con})
	c.Assert(err, IsNil)

	_, err = funcs[ast.Hex].getFunction(s.ctx, []Expression{varcharCon})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestUnhexFunc(c *C) {
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
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Unhex, s.primitiveValsToConstants([]interface{}{t.arg})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetString(), Equals, t.res)
			}
		}
	}

	_, err := funcs[ast.Unhex].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestBitLength(c *C) {
	cases := []struct {
		args     interface{}
		expected int64
		isNil    bool
		getErr   bool
	}{
		{"hi", 16, false, false},
		{"你好", 48, false, false},
		{"", 0, false, false},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.BitLength, s.primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}

	_, err := funcs[ast.BitLength].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestChar(c *C) {
	stmtCtx := s.ctx.GetSessionVars().StmtCtx
	origin := stmtCtx.IgnoreTruncate
	stmtCtx.IgnoreTruncate = true
	defer func() {
		stmtCtx.IgnoreTruncate = origin
	}()

	tbl := []struct {
		str    string
		iNum   int64
		fNum   float64
		result string
	}{
		{"65", 66, 67.5, "ABD"},                  // float
		{"65", 16740, 67.5, "AAdD"},              // large num
		{"65", -1, 67.5, "A\xff\xff\xff\xffD"},   // nagtive int
		{"a", -1, 67.5, "\x00\xff\xff\xff\xffD"}, // invalid 'a'
	}
	for _, v := range tbl {
		for _, char := range []interface{}{"utf8", nil} {
			fc := funcs[ast.CharFunc]
			f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(v.str, v.iNum, v.fNum, char)))
			c.Assert(err, IsNil)
			c.Assert(f, NotNil)
			r, err := evalBuiltinFunc(f, chunk.Row{})
			c.Assert(err, IsNil, Commentf("err: %v", err))
			c.Assert(r, testutil.DatumEquals, types.NewDatum(v.result))
		}
	}

	fc := funcs[ast.CharFunc]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums("65", 66, nil)))
	c.Assert(err, IsNil)
	r, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(r, testutil.DatumEquals, types.NewDatum("AB"))
}

func (s *testEvaluatorSuite) TestCharLength(c *C) {
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
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(v.input)))
		c.Assert(err, IsNil)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(v.result))
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
		arg := s.datumsToConstants(types.MakeDatums(v.input))
		tp := arg[0].GetType()
		tp.Tp = mysql.TypeVarString
		tp.Charset = charset.CharsetBin
		tp.Collate = charset.CollationBin
		tp.Flen = types.UnspecifiedLength
		tp.Flag = mysql.BinaryFlag
		f, err := fc.getFunction(s.ctx, arg)
		c.Assert(err, IsNil)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(v.result))
	}
}

func (s *testEvaluatorSuite) TestFindInSet(c *C) {
	for _, t := range []struct {
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
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.str, t.strlst)))
		c.Assert(err, IsNil)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t.ret), Commentf("FindInSet(%s, %s)", t.str, t.strlst))
	}
}

func (s *testEvaluatorSuite) TestField(c *C) {
	stmtCtx := s.ctx.GetSessionVars().StmtCtx
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
	for _, t := range tbl {
		fc := funcs[ast.Field]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.argLst...)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}

func (s *testEvaluatorSuite) TestLpad(c *C) {
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
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{str, length, padStr}))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		if test.expect == nil {
			c.Assert(result.Kind(), Equals, types.KindNull)
		} else {
			expect, _ := test.expect.(string)
			c.Assert(result.GetString(), Equals, expect)
		}
	}
}

func (s *testEvaluatorSuite) TestRpad(c *C) {
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
		f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{str, length, padStr}))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		if test.expect == nil {
			c.Assert(result.Kind(), Equals, types.KindNull)
		} else {
			expect, _ := test.expect.(string)
			c.Assert(result.GetString(), Equals, expect)
		}
	}
}

func (s *testEvaluatorSuite) TestRpadSig(c *C) {
	colTypes := []*types.FieldType{
		{Tp: mysql.TypeVarchar},
		{Tp: mysql.TypeLonglong},
		{Tp: mysql.TypeVarchar},
	}
	resultType := &types.FieldType{Tp: mysql.TypeVarchar, Flen: 1000}

	args := []Expression{
		&Column{Index: 0, RetType: colTypes[0]},
		&Column{Index: 1, RetType: colTypes[1]},
		&Column{Index: 2, RetType: colTypes[2]},
	}

	base := baseBuiltinFunc{args: args, ctx: s.ctx, tp: resultType}
	rpad := &builtinRpadUTF8Sig{base, 1000}

	input := chunk.NewChunkWithCapacity(colTypes, 10)
	input.AppendString(0, "abc")
	input.AppendString(0, "abc")
	input.AppendInt64(1, 6)
	input.AppendInt64(1, 10000)
	input.AppendString(2, "123")
	input.AppendString(2, "123")

	res, isNull, err := rpad.evalString(input.GetRow(0))
	c.Assert(res, Equals, "abc123")
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	res, isNull, err = rpad.evalString(input.GetRow(1))
	c.Assert(res, Equals, "")
	c.Assert(isNull, IsTrue)
	c.Assert(err, IsNil)

	warnings := s.ctx.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 1)
	lastWarn := warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))
}

func (s *testEvaluatorSuite) TestInsertBinarySig(c *C) {
	colTypes := []*types.FieldType{
		{Tp: mysql.TypeVarchar},
		{Tp: mysql.TypeLonglong},
		{Tp: mysql.TypeLonglong},
		{Tp: mysql.TypeVarchar},
	}
	resultType := &types.FieldType{Tp: mysql.TypeVarchar, Flen: 3}

	args := []Expression{
		&Column{Index: 0, RetType: colTypes[0]},
		&Column{Index: 1, RetType: colTypes[1]},
		&Column{Index: 2, RetType: colTypes[2]},
		&Column{Index: 3, RetType: colTypes[3]},
	}

	base := baseBuiltinFunc{args: args, ctx: s.ctx, tp: resultType}
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
	c.Assert(res, Equals, "abd")
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	res, isNull, err = insert.evalString(input.GetRow(1))
	c.Assert(res, Equals, "")
	c.Assert(isNull, IsTrue)
	c.Assert(err, IsNil)

	res, isNull, err = insert.evalString(input.GetRow(2))
	c.Assert(res, Equals, "abc")
	c.Assert(isNull, IsFalse)
	c.Assert(err, IsNil)

	res, isNull, err = insert.evalString(input.GetRow(3))
	c.Assert(res, Equals, "")
	c.Assert(isNull, IsTrue)
	c.Assert(err, IsNil)

	res, isNull, err = insert.evalString(input.GetRow(4))
	c.Assert(res, Equals, "")
	c.Assert(isNull, IsTrue)
	c.Assert(err, IsNil)

	res, isNull, err = insert.evalString(input.GetRow(5))
	c.Assert(res, Equals, "")
	c.Assert(isNull, IsTrue)
	c.Assert(err, IsNil)

	res, isNull, err = insert.evalString(input.GetRow(6))
	c.Assert(res, Equals, "")
	c.Assert(isNull, IsTrue)
	c.Assert(err, IsNil)

	warnings := s.ctx.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 1)
	lastWarn := warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))
}

func (s *testEvaluatorSuite) TestInstr(c *C) {
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
	for i, t := range Dtbl {
		f, err := instr.getFunction(s.ctx, s.datumsToConstants(t["Args"]))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		got, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(got, DeepEquals, t["Want"][0], Commentf("[%d]: args: %v", i, t["Args"]))
	}
}

func (s *testEvaluatorSuite) TestMakeSet(c *C) {
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

	for _, t := range tbl {
		fc := funcs[ast.MakeSet]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.argList...)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}

func (s *testEvaluatorSuite) TestOct(c *C) {
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
		//overflow uint64
		{"9999999999999999999999999", "1777777777777777777777"},
		{"-9999999999999999999999999", "1777777777777777777777"},
		{types.NewBinaryLiteralFromUint(255, -1), "377"}, // b'11111111'
		{types.NewBinaryLiteralFromUint(10, -1), "12"},   // b'1010'
		{types.NewBinaryLiteralFromUint(5, -1), "5"},     // b'0101'
	}
	fc := funcs[ast.Oct]
	for _, tt := range octTests {
		in := types.NewDatum(tt.origin)
		f, _ := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{in}))
		c.Assert(f, NotNil)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		res, err := r.ToString()
		c.Assert(err, IsNil)
		c.Assert(res, Equals, tt.ret, Commentf("select oct(%v);", tt.origin))
	}
	// tt NULL input for sha
	var argNull types.Datum
	f, _ := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(r.IsNull(), IsTrue)
}

func (s *testEvaluatorSuite) TestFormat(c *C) {
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
	}{-12332.123456, -4, "zh_CN", nil}
	formatTests3 := struct {
		number    interface{}
		precision interface{}
		locale    string
		ret       interface{}
	}{12332.2, 2, "de_DE", 12332.20}
	formatTests4 := struct {
		number    interface{}
		precision interface{}
		locale    interface{}
		ret       interface{}
	}{1, 4, nil, "1.0000"}

	fc := funcs[ast.Format]
	for _, tt := range formatTests {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tt.number, tt.precision, tt.locale)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(tt.ret))
	}

	origConfig := s.ctx.GetSessionVars().StmtCtx.TruncateAsWarning
	s.ctx.GetSessionVars().StmtCtx.TruncateAsWarning = true
	for _, tt := range formatTests1 {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tt.number, tt.precision)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(tt.ret), Commentf("test %v", tt))
		if tt.warnings > 0 {
			warnings := s.ctx.GetSessionVars().StmtCtx.GetWarnings()
			c.Assert(len(warnings), Equals, tt.warnings, Commentf("test %v", tt))
			for i := 0; i < tt.warnings; i++ {
				c.Assert(terror.ErrorEqual(types.ErrTruncatedWrongVal, warnings[i].Err), IsTrue, Commentf("test %v", tt))
			}
			s.ctx.GetSessionVars().StmtCtx.SetWarnings([]stmtctx.SQLWarn{})
		}
	}
	s.ctx.GetSessionVars().StmtCtx.TruncateAsWarning = origConfig

	f2, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(formatTests2.number, formatTests2.precision, formatTests2.locale)))
	c.Assert(err, IsNil)
	c.Assert(f2, NotNil)
	r2, err := evalBuiltinFunc(f2, chunk.Row{})
	c.Assert(types.NewDatum(err), testutil.DatumEquals, types.NewDatum(errors.New("not implemented")))
	c.Assert(r2, testutil.DatumEquals, types.NewDatum(formatTests2.ret))

	f3, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(formatTests3.number, formatTests3.precision, formatTests3.locale)))
	c.Assert(err, IsNil)
	c.Assert(f3, NotNil)
	r3, err := evalBuiltinFunc(f3, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(types.NewDatum(r3), testutil.DatumEquals, types.NewDatum(formatTests3.ret))
	showwarnings := s.ctx.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(terror.ErrorEqual(errUnknownLocale, showwarnings[0].Err), IsTrue)
	s.ctx.GetSessionVars().StmtCtx.SetWarnings([]stmtctx.SQLWarn{})

	f4, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(formatTests4.number, formatTests4.precision, formatTests4.locale)))
	c.Assert(err, IsNil)
	c.Assert(f4, NotNil)
	r4, err := evalBuiltinFunc(f4, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(r4, testutil.DatumEquals, types.NewDatum(formatTests4.ret))
	warnings := s.ctx.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), Equals, 1)
	c.Assert(terror.ErrorEqual(errUnknownLocale, warnings[0].Err), IsTrue)
	s.ctx.GetSessionVars().StmtCtx.SetWarnings([]stmtctx.SQLWarn{})
}

func (s *testEvaluatorSuite) TestFromBase64(c *C) {
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
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(test.args)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		if test.expect == nil {
			c.Assert(result.Kind(), Equals, types.KindNull)
		} else {
			expect, _ := test.expect.(string)
			c.Assert(result.GetString(), Equals, expect)
		}
	}
}

func (s *testEvaluatorSuite) TestFromBase64Sig(c *C) {
	colTypes := []*types.FieldType{
		{Tp: mysql.TypeVarchar},
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
		resultType := &types.FieldType{Tp: mysql.TypeVarchar, Flen: mysql.MaxBlobWidth}
		base := baseBuiltinFunc{args: args, ctx: s.ctx, tp: resultType}
		fromBase64 := &builtinFromBase64Sig{base, test.maxAllowPacket}

		input := chunk.NewChunkWithCapacity(colTypes, 1)
		input.AppendString(0, test.args)
		res, isNull, err := fromBase64.evalString(input.GetRow(0))
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, test.isNil)
		if isNull {
			warnings := s.ctx.GetSessionVars().StmtCtx.GetWarnings()
			c.Assert(len(warnings), Equals, 1)
			lastWarn := warnings[len(warnings)-1]
			c.Assert(terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err), IsTrue)
			s.ctx.GetSessionVars().StmtCtx.SetWarnings([]stmtctx.SQLWarn{})
		}
		c.Assert(res, Equals, test.expect)
	}
}

func (s *testEvaluatorSuite) TestInsert(c *C) {
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
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(test.args...)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		if test.expect == nil {
			c.Assert(result.Kind(), Equals, types.KindNull)
		} else {
			expect, _ := test.expect.(string)
			c.Assert(result.GetString(), Equals, expect)
		}
	}
}

func (s *testEvaluatorSuite) TestOrd(c *C) {
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
		{"你好", 14990752, false, false},
		{"にほん", 14909867, false, false},
		{"한국", 15570332, false, false},
		{"👍", 4036989325, false, false},
		{"א", 55184, false, false},
	}
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Ord, s.primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)

		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}
	_, err := funcs[ast.Ord].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestElt(c *C) {
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
	for _, t := range tbl {
		fc := funcs[ast.Elt]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.argLst...)))
		c.Assert(err, IsNil)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}

func (s *testEvaluatorSuite) TestExportSet(c *C) {
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
	for _, t := range estd {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.argLst...)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		exportSetRes, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		res, err := exportSetRes.ToString()
		c.Assert(err, IsNil)
		c.Assert(res, Equals, t.res)
	}
}

func (s *testEvaluatorSuite) TestBin(c *C) {
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
	for _, t := range dtbl {
		f, err := fc.getFunction(ctx, s.datumsToConstants(t["Input"]))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t["Expected"][0]))
	}
}

func (s *testEvaluatorSuite) TestQuote(c *C) {
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

	for _, t := range tbl {
		fc := funcs[ast.Quote]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.arg)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		r, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}

func (s *testEvaluatorSuite) TestToBase64(c *C) {
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
		f, err := newFunctionForTest(s.ctx, ast.ToBase64, s.primitiveValsToConstants([]interface{}{test.args})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if test.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if test.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetString(), Equals, test.expect)
			}
		}
	}

	_, err := funcs[ast.ToBase64].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestToBase64Sig(c *C) {
	colTypes := []*types.FieldType{
		{Tp: mysql.TypeVarchar},
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
		resultType := &types.FieldType{Tp: mysql.TypeVarchar, Flen: base64NeededEncodedLength(len(test.args))}
		base := baseBuiltinFunc{args: args, ctx: s.ctx, tp: resultType}
		toBase64 := &builtinToBase64Sig{base, test.maxAllowPacket}

		input := chunk.NewChunkWithCapacity(colTypes, 1)
		input.AppendString(0, test.args)
		res, isNull, err := toBase64.evalString(input.GetRow(0))
		c.Assert(err, IsNil)
		if test.isNil {
			c.Assert(isNull, IsTrue)

			warnings := s.ctx.GetSessionVars().StmtCtx.GetWarnings()
			c.Assert(len(warnings), Equals, 1)
			lastWarn := warnings[len(warnings)-1]
			c.Assert(terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err), IsTrue)
			s.ctx.GetSessionVars().StmtCtx.SetWarnings([]stmtctx.SQLWarn{})

		} else {
			c.Assert(isNull, IsFalse)
		}
		c.Assert(res, Equals, test.expect)
	}
}

func (s *testEvaluatorSuite) TestStringRight(c *C) {
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
		f, _ := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{str, length}))
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		if result.IsNull() {
			c.Assert(test.expect, IsNil)
			continue
		}
		res, err := result.ToString()
		c.Assert(err, IsNil)
		c.Assert(res, Equals, test.expect)
	}
}

func (s *testEvaluatorSuite) TestWeightString(c *C) {
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
		{"a ", "NONE", 0, "a "},
		{"中", "NONE", 0, "中"},
		{"中 ", "NONE", 0, "中 "},
		{nil, "CHAR", 5, nil},
		{7, "CHAR", 5, nil},
		{7.0, "NONE", 0, nil},
		{"a", "CHAR", 5, "a    "},
		{"a ", "CHAR", 5, "a    "},
		{"中", "CHAR", 5, "中    "},
		{"中 ", "CHAR", 5, "中    "},
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
			f, err = fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{str}))
		} else {
			padding := types.NewDatum(test.padding)
			length := types.NewDatum(test.length)
			f, err = fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{str, padding, length}))
		}
		c.Assert(err, IsNil)
		// Reset warnings.
		s.ctx.GetSessionVars().StmtCtx.ResetForRetry()
		result, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		if result.IsNull() {
			c.Assert(test.expect, IsNil)
			continue
		}
		res, err := result.ToString()
		c.Assert(err, IsNil)
		c.Assert(res, Equals, test.expect)
		if test.expr == nil {
			continue
		}
		strExpr := fmt.Sprintf("%v", test.expr)
		if test.padding == "BINARY" && test.length < len(strExpr) {
			expectWarn := fmt.Sprintf("[expression:1292]Truncated incorrect BINARY(%d) value: '%s'", test.length, strExpr)
			obtainedWarns := s.ctx.GetSessionVars().StmtCtx.GetWarnings()
			c.Assert(len(obtainedWarns), Equals, 1)
			c.Assert(obtainedWarns[0].Level, Equals, "Warning")
			c.Assert(obtainedWarns[0].Err.Error(), Equals, expectWarn)
		}
	}
}

func (s *testEvaluatorSerialSuites) TestCIWeightString(c *C) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	type weightStringTest struct {
		str     string
		padding string
		length  int
		expect  interface{}
	}

	checkResult := func(collation string, tests []weightStringTest) {
		fc := funcs[ast.WeightString]
		for _, test := range tests {
			str := types.NewCollationStringDatum(test.str, collation, utf8.RuneCountInString(test.str))
			var f builtinFunc
			var err error
			if test.padding == "NONE" {
				f, err = fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{str}))
			} else {
				padding := types.NewDatum(test.padding)
				length := types.NewDatum(test.length)
				f, err = fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{str, padding, length}))
			}
			c.Assert(err, IsNil)
			result, err := evalBuiltinFunc(f, chunk.Row{})
			c.Assert(err, IsNil)
			if result.IsNull() {
				c.Assert(test.expect, IsNil)
				continue
			}
			res, err := result.ToString()
			c.Assert(err, IsNil)
			c.Assert(res, Equals, test.expect)
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
