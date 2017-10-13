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

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestLength(c *C) {
	defer testleak.AfterTest(c)()
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
		{types.Time{Time: types.FromGoTime(time.Now()), Fsp: 6, Type: mysql.TypeDatetime}, 26, false, false},
		{types.NewBinaryLiteralFromUint(0x01, -1), 1, false, false},
		{types.Set{Value: 1, Name: "abc"}, 3, false, false},
		{types.Duration{Duration: time.Duration(12*time.Hour + 1*time.Minute + 1*time.Second), Fsp: types.DefaultFsp}, 8, false, false},
		{nil, 0, true, false},
		{errors.New("must error"), 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Length, s.primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
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

	_, err := funcs[ast.Length].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestASCII(c *C) {
	defer testleak.AfterTest(c)()

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

		d, err := f.Eval(nil)
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
	_, err := funcs[ast.Length].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestConcat(c *C) {
	defer testleak.AfterTest(c)()
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
				types.Time{
					Time: types.FromDate(2000, 1, 1, 12, 01, 01, 0),
					Type: mysql.TypeDatetime,
					Fsp:  types.DefaultFsp},
				types.Duration{
					Duration: time.Duration(12*time.Hour + 1*time.Minute + 1*time.Second),
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
		v, err := f.Eval(nil)
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

func (s *testEvaluatorSuite) TestConcatWS(c *C) {
	defer testleak.AfterTest(c)()
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
			[]interface{}{",", "a", "b", "hello", "$^%"},
			false, false,
			"a,b,hello,$^%",
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
				types.Time{
					Time: types.FromDate(2000, 1, 1, 12, 01, 01, 0),
					Type: mysql.TypeDatetime,
					Fsp:  types.DefaultFsp},
				types.Duration{
					Duration: time.Duration(12*time.Hour + 1*time.Minute + 1*time.Second),
					Fsp:      types.DefaultFsp},
			},
			false, false, "a,b,1,2,1.1,0.11,1.1,2000-01-01 12:01:01,12:01:01",
		},
	}

	fcName := ast.ConcatWS
	// ERROR 1582 (42000): Incorrect parameter count in the call to native function 'concat_ws'
	f, err := newFunctionForTest(s.ctx, fcName, s.primitiveValsToConstants([]interface{}{nil})...)
	c.Assert(err, NotNil)

	for _, t := range cases {
		f, err = newFunctionForTest(s.ctx, fcName, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		val, err1 := f.Eval(nil)
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

func (s *testEvaluatorSuite) TestLeft(c *C) {
	defer testleak.AfterTest(c)()
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
		v, err := f.Eval(nil)
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
	defer testleak.AfterTest(c)()
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
		v, err := f.Eval(nil)
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
	defer testleak.AfterTest(c)()
	args := []interface{}{"a", int64(2)}
	fc := funcs[ast.Repeat]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "aa")

	args = []interface{}{"a", uint64(2)}
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "aa")

	args = []interface{}{"a", uint64(16777217)}
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(v.IsNull(), IsTrue)

	args = []interface{}{"a", uint64(16777216)}
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(v.IsNull(), IsFalse)

	args = []interface{}{"a", int64(-1)}
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "")

	args = []interface{}{"a", int64(0)}
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "")

	args = []interface{}{"a", uint64(0)}
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	v, err = evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "")
}

func (s *testEvaluatorSuite) TestLower(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		args   []interface{}
		isNil  bool
		getErr bool
		res    string
	}{
		{[]interface{}{nil}, true, false, ""},
		{[]interface{}{"ab"}, false, false, "ab"},
		{[]interface{}{1}, false, false, "1"},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Lower, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		v, err := f.Eval(nil)
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
	defer testleak.AfterTest(c)()
	cases := []struct {
		args   []interface{}
		isNil  bool
		getErr bool
		res    string
	}{
		{[]interface{}{nil}, true, false, ""},
		{[]interface{}{"ab"}, false, false, "ab"},
		{[]interface{}{1}, false, false, "1"},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Upper, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		v, err := f.Eval(nil)
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
	defer testleak.AfterTest(c)()
	fc := funcs[ast.Reverse]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(nil)))
	c.Assert(err, IsNil)
	d, err := evalBuiltinFunc(f, nil)
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
		d, err = evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expect"][0])
	}
}

func (s *testEvaluatorSuite) TestStrcmp(c *C) {
	defer testleak.AfterTest(c)()
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
		d, err := f.Eval(nil)
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
	defer testleak.AfterTest(c)()

	cases := []struct {
		args   []interface{}
		isNil  bool
		getErr bool
		res    string
		flen   int
	}{
		{[]interface{}{"www.mysql.com", "mysql", "pingcap"}, false, false, "www.pingcap.com", 17},
		{[]interface{}{"www.mysql.com", "w", 1}, false, false, "111.mysql.com", 13},
		{[]interface{}{1234, 2, 55}, false, false, "15534", 8},
		{[]interface{}{"", "a", "b"}, false, false, "", 0},
		{[]interface{}{"abc", "", "d"}, false, false, "abc", 3},
		{[]interface{}{"aaa", "a", ""}, false, false, "", 3},
		{[]interface{}{nil, "a", "b"}, true, false, "", 0},
		{[]interface{}{"a", nil, "b"}, true, false, "", 1},
		{[]interface{}{"a", "b", nil}, true, false, "", 1},
		{[]interface{}{errors.New("must err"), "a", "b"}, false, true, "", -1},
	}
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Replace, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		c.Assert(f.GetType().Flen, Equals, t.flen)
		d, err := f.Eval(nil)
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

	_, err := funcs[ast.Replace].getFunction(s.ctx, []Expression{Zero, Zero, Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestSubstring(c *C) {
	defer testleak.AfterTest(c)()

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
		d, err := f.Eval(nil)
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

	_, err := funcs[ast.Substring].getFunction(s.ctx, []Expression{Zero, Zero, Zero})
	c.Assert(err, IsNil)

	_, err = funcs[ast.Substring].getFunction(s.ctx, []Expression{Zero, Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestConvert(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		str    string
		cs     string
		result string
	}{
		{"haha", "utf8", "haha"},
		{"haha", "ascii", "haha"},
	}
	for _, v := range tbl {
		fc := funcs[ast.Convert]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(v.str, v.cs)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		r, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(r.Kind(), Equals, types.KindString)
		c.Assert(r.GetString(), Equals, v.result)
	}

	// Test case for error
	errTbl := []struct {
		str    interface{}
		cs     string
		result string
	}{
		{"haha", "wrongcharset", "haha"},
	}
	for _, v := range errTbl {
		fc := funcs[ast.Convert]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(v.str, v.cs)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		_, err = evalBuiltinFunc(f, nil)
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestSubstringIndex(c *C) {
	defer testleak.AfterTest(c)()

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
		d, err := f.Eval(nil)
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

	_, err := funcs[ast.SubstringIndex].getFunction(s.ctx, []Expression{Zero, Zero, Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestSpace(c *C) {
	defer testleak.AfterTest(c)()
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
		d, err := f.Eval(nil)
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

	_, err := funcs[ast.Space].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestLocate(c *C) {
	// 1. Test LOCATE without binary input.
	defer testleak.AfterTest(c)()
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
		{[]interface{}{"BaR", "foobArbar"}, 4},
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
		{[]interface{}{"bAr", "foobarBaR", 5}, 7},
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
		got, err := evalBuiltinFunc(f, nil)
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
		got, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		c.Assert(got, DeepEquals, t["Want"][0], Commentf("[%d]: args: %v", i, t["Args"]))
	}
}

func (s *testEvaluatorSuite) TestTrim(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		args   []interface{}
		isNil  bool
		getErr bool
		res    string
	}{
		{[]interface{}{"   bar   "}, false, false, "bar"},
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
		d, err := f.Eval(nil)
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

	_, err := funcs[ast.Trim].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)

	_, err = funcs[ast.Trim].getFunction(s.ctx, []Expression{Zero, Zero})
	c.Assert(err, IsNil)

	_, err = funcs[ast.Trim].getFunction(s.ctx, []Expression{Zero, Zero, Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestLTrim(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		arg    interface{}
		isNil  bool
		getErr bool
		res    string
	}{
		{"   bar   ", false, false, "bar   "},
		{"bar", false, false, "bar"},
		{"", false, false, ""},
		{nil, true, false, ""},
		{errors.New("must error"), false, true, ""},
	}
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.LTrim, s.primitiveValsToConstants([]interface{}{t.arg})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
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

	_, err := funcs[ast.LTrim].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestRTrim(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		arg    interface{}
		isNil  bool
		getErr bool
		res    string
	}{
		{"   bar   ", false, false, "   bar"},
		{"bar", false, false, "bar"},
		{"", false, false, ""},
		{nil, true, false, ""},
		{errors.New("must error"), false, true, ""},
	}
	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.RTrim, s.primitiveValsToConstants([]interface{}{t.arg})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
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

	_, err := funcs[ast.RTrim].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestHexFunc(c *C) {
	defer testleak.AfterTest(c)()
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
		d, err := f.Eval(nil)
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
	defer testleak.AfterTest(c)()
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
		d, err := f.Eval(nil)
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

	_, err := funcs[ast.Unhex].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestBitLength(c *C) {
	defer testleak.AfterTest(c)()
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
		d, err := f.Eval(nil)
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

	_, err := funcs[ast.BitLength].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestChar(c *C) {
	defer testleak.AfterTest(c)()
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
			r, err := evalBuiltinFunc(f, nil)
			if err != nil {
				fmt.Printf("%s\n", err.Error())
			}
			c.Assert(err, IsNil)
			c.Assert(r, testutil.DatumEquals, types.NewDatum(v.result))
		}
	}

	v := struct {
		str    string
		iNum   int64
		fNum   interface{}
		result string
	}{"65", 66, nil, "AB"}

	fc := funcs[ast.CharFunc]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(v.str, v.iNum, nil)))
	c.Assert(err, IsNil)
	r, err := evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(r, testutil.DatumEquals, types.NewDatum(v.result))
}

func (s *testEvaluatorSuite) TestCharLength(c *C) {
	defer testleak.AfterTest(c)()
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
		r, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(v.result))
	}
}

func (s *testEvaluatorSuite) TestFindInSet(c *C) {
	defer testleak.AfterTest(c)()

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
		r, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t.ret), Commentf("FindInSet(%s, %s)", t.str, t.strlst))
	}
}

func (s *testEvaluatorSuite) TestField(c *C) {
	defer testleak.AfterTest(c)()
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
		r, err := evalBuiltinFunc(f, nil)
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
		result, err := evalBuiltinFunc(f, nil)
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
		result, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		if test.expect == nil {
			c.Assert(result.Kind(), Equals, types.KindNull)
		} else {
			expect, _ := test.expect.(string)
			c.Assert(result.GetString(), Equals, expect)
		}
	}
}

func (s *testEvaluatorSuite) TestInstr(c *C) {
	defer testleak.AfterTest(c)()
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

		{[]interface{}{"live LONG and prosper", "long"}, 6},

		{[]interface{}{"not BINARY string", "binary"}, 5},
		{[]interface{}{"UPPER case", "upper"}, 1},
		{[]interface{}{"UPPER case", "CASE"}, 7},
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
		got, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(got, DeepEquals, t["Want"][0], Commentf("[%d]: args: %v", i, t["Args"]))
	}
}

func (s *testEvaluatorSuite) TestMakeSet(c *C) {
	defer testleak.AfterTest(c)()

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
		r, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}

func (s *testEvaluatorSuite) TestOct(c *C) {
	defer testleak.AfterTest(c)()
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
		r, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		res, err := r.ToString()
		c.Assert(err, IsNil)
		c.Assert(res, Equals, tt.ret, Commentf("select oct(%v);", tt.origin))
	}
	// tt NULL input for sha
	var argNull types.Datum
	f, _ := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{argNull}))
	r, err := evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(r.IsNull(), IsTrue)
}

func (s *testEvaluatorSuite) TestFormat(c *C) {
	defer testleak.AfterTest(c)()
	formatTests := []struct {
		number    interface{}
		precision interface{}
		locale    string
		ret       interface{}
	}{
		{12332.1234561111111111111111111111111111111111111, 4, "en_US", "12,332.1234"},
		{nil, 22, "en_US", nil},
	}
	formatTests1 := []struct {
		number    interface{}
		precision interface{}
		ret       interface{}
	}{
		{12332.123456, 4, "12,332.1234"},
		{12332.123456, 0, "12,332"},
		{12332.123456, -4, "12,332"},
		{-12332.123456, 4, "-12,332.1234"},
		{-12332.123456, 0, "-12,332"},
		{-12332.123456, -4, "-12,332"},
		{"12332.123456", "4", "12,332.1234"},
		{"12332.123456A", "4", "12,332.1234"},
		{"-12332.123456", "4", "-12,332.1234"},
		{"-12332.123456A", "4", "-12,332.1234"},
		{"A123345", "4", "0.0000"},
		{"-A123345", "4", "0.0000"},
		{"-12332.123456", "A", "-12,332"},
		{"12332.123456", "A", "12,332"},
		{"-12332.123456", "4A", "-12,332.1234"},
		{"12332.123456", "4A", "12,332.1234"},
		{"-A12332.123456", "A", "0"},
		{"A12332.123456", "A", "0"},
		{"-A12332.123456", "4A", "0.0000"},
		{"A12332.123456", "4A", "0.0000"},
		{"-.12332.123456", "4A", "-0.1233"},
		{".12332.123456", "4A", "0.1233"},
		{"12332.1234567890123456789012345678901", 22, "12,332.1234567890123456789012"},
		{nil, 22, nil},
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
	}{"-12332.123456", "4", "de_GE", nil}

	for _, tt := range formatTests {
		fc := funcs[ast.Format]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tt.number, tt.precision, tt.locale)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		r, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(tt.ret))
	}

	for _, tt := range formatTests1 {
		fc := funcs[ast.Format]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tt.number, tt.precision)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		r, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(tt.ret))
	}

	fc2 := funcs[ast.Format]
	f2, err := fc2.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(formatTests2.number, formatTests2.precision, formatTests2.locale)))
	c.Assert(err, IsNil)
	c.Assert(f2, NotNil)
	r2, err := evalBuiltinFunc(f2, nil)
	c.Assert(types.NewDatum(err), testutil.DatumEquals, types.NewDatum(errors.New("not implemented")))
	c.Assert(r2, testutil.DatumEquals, types.NewDatum(formatTests2.ret))

	fc3 := funcs[ast.Format]
	f3, err := fc3.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(formatTests3.number, formatTests3.precision, formatTests3.locale)))
	c.Assert(err, IsNil)
	c.Assert(f3, NotNil)
	r3, err := evalBuiltinFunc(f3, nil)
	c.Assert(types.NewDatum(err), testutil.DatumEquals, types.NewDatum(errors.New("not support for the specific locale")))
	c.Assert(r3, testutil.DatumEquals, types.NewDatum(formatTests3.ret))
}

func (s *testEvaluatorSuite) TestFromBase64(c *C) {
	tests := []struct {
		args   interface{}
		expect interface{}
	}{
		{string(""), string("")},
		{string("YWJj"), string("abc")},
		{string("YWIgYw=="), string("ab c")},
		{string("YWIKYw=="), string("ab\nc")},
		{string("YWIJYw=="), string("ab\tc")},
		{string("cXdlcnR5MTIzNDU2"), string("qwerty123456")},
		{
			string("QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrL0FCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4\neXowMTIzNDU2Nzg5Ky9BQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWmFiY2RlZmdoaWprbG1ub3Bx\ncnN0dXZ3eHl6MDEyMzQ1Njc4OSsv"),
			string("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"),
		},
		{
			string("QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0NTY3ODkrLw=="),
			string("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"),
		},
		{
			string("QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0NTY3ODkrLw=="),
			string("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"),
		},
		{
			string("QUJDREVGR0hJSkt\tMTU5PUFFSU1RVVld\nYWVphYmNkZ\rWZnaGlqa2xt   bm9wcXJzdHV2d3h5ejAxMjM0NTY3ODkrLw=="),
			string("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"),
		},
	}
	fc := funcs[ast.FromBase64]
	for _, test := range tests {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(test.args)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		result, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		if test.expect == nil {
			c.Assert(result.Kind(), Equals, types.KindNull)
		} else {
			expect, _ := test.expect.(string)
			c.Assert(result.GetString(), Equals, expect)
		}
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

		{[]interface{}{"我叫小雨呀", 3, 2, "王雨叶"}, "我叫王雨叶呀"},
		{[]interface{}{"我叫小雨呀", -1, 2, "王雨叶"}, "我叫小雨呀"},
		{[]interface{}{"我叫小雨呀", 3, 100, "王雨叶"}, "我叫王雨叶"},
		{[]interface{}{nil, 3, 100, "王雨叶"}, nil},
		{[]interface{}{"我叫小雨呀", nil, 4, "王雨叶"}, nil},
		{[]interface{}{"我叫小雨呀", 3, nil, "王雨叶"}, nil},
		{[]interface{}{"我叫小雨呀", 3, 4, nil}, nil},
		{[]interface{}{"我叫小雨呀", 3, -1, "王雨叶"}, "我叫王雨叶"},
		{[]interface{}{"我叫小雨呀", 3, 1, "王雨叶"}, "我叫王雨叶雨呀"},
	}
	fc := funcs[ast.InsertFunc]
	for _, test := range tests {
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(test.args...)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		result, err := evalBuiltinFunc(f, nil)
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
	defer testleak.AfterTest(c)()

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

		d, err := f.Eval(nil)
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
	_, err := funcs[ast.Ord].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestElt(c *C) {
	defer testleak.AfterTest(c)()

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
		r, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}

func (s *testEvaluatorSuite) TestExportSet(c *C) {
	defer testleak.AfterTest(c)()

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
		exportSetRes, err := evalBuiltinFunc(f, nil)
		res, err := exportSetRes.ToString()
		c.Assert(err, IsNil)
		c.Assert(res, Equals, t.res)
	}
}

func (s *testEvaluatorSuite) TestBin(c *C) {
	defer testleak.AfterTest(c)()

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
		r, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t["Expected"][0]))
	}
}

func (s *testEvaluatorSuite) TestQuote(c *C) {
	defer testleak.AfterTest(c)()

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
		{nil, nil},
	}

	for _, t := range tbl {
		fc := funcs[ast.Quote]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.arg)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		r, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}

func (s *testEvaluatorSuite) TestToBase64(c *C) {
	defer testleak.AfterTest(c)()

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
		d, err := f.Eval(nil)
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

	_, err := funcs[ast.ToBase64].getFunction(s.ctx, []Expression{Zero})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestStringRight(c *C) {
	defer testleak.AfterTest(c)()
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
		result, err := evalBuiltinFunc(f, nil)
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
