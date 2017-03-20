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
	"errors"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestLength(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.Length]
	f, err := fc.getFunction(datumsToConstants(types.MakeDatums(nil)), s.ctx)
	c.Assert(err, IsNil)
	d, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)

	tbl := []struct {
		Input    interface{}
		Expected int64
	}{
		{"abc", 3},
		{1, 1},
		{3.14, 4},
		{types.Time{Time: types.FromGoTime(time.Now()), Fsp: 6, Type: mysql.TypeDatetime}, 26},
		{types.Bit{Value: 1, Width: 8}, 1},
		{types.Hex{Value: 1}, 1},
		{types.Set{Value: 1, Name: "abc"}, 3},
	}

	dtbl := tblToDtbl(tbl)

	for _, t := range dtbl {
		f, err := fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		d, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expected"][0])
	}
}

func (s *testEvaluatorSuite) TestASCII(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.ASCII]
	f, err := fc.getFunction(datumsToConstants(types.MakeDatums(nil)), s.ctx)
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	for _, t := range []struct {
		Input    interface{}
		Expected int64
	}{
		{"", 0},
		{"A", 65},
		{"你好", 228},
		{1, 49},
		{1.2, 49},
		{true, 49},
		{false, 48},
	} {
		f, err = fc.getFunction(datumsToConstants(types.MakeDatums(t.Input)), s.ctx)
		c.Assert(err, IsNil)
		v, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v.GetInt64(), Equals, t.Expected)
	}

	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(errors.New("must error"))), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestConcat(c *C) {
	defer testleak.AfterTest(c)()
	args := []interface{}{nil}

	fc := funcs[ast.Concat]
	f, err := fc.getFunction(datumsToConstants(types.MakeDatums(args...)), s.ctx)
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	args = []interface{}{"a", "b", "c"}
	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(args...)), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "abc")

	args = []interface{}{"a", "b", nil, "c"}
	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(args...)), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	args = []interface{}{errors.New("must error")}
	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(args...)), s.ctx)
	c.Assert(err, IsNil)
	_, err = f.eval(nil)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestConcatWS(c *C) {
	defer testleak.AfterTest(c)()
	args := types.MakeDatums([]interface{}{nil, nil}...)

	fc := funcs[ast.ConcatWS]
	f, err := fc.getFunction(datumsToConstants(args), s.ctx)
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	args = types.MakeDatums([]interface{}{"|", "a", nil, "b", "c"}...)
	f, err = fc.getFunction(datumsToConstants(args), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "a|b|c")

	args = types.MakeDatums([]interface{}{errors.New("must error"), nil}...)
	f, err = fc.getFunction(datumsToConstants(args), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestLeft(c *C) {
	defer testleak.AfterTest(c)()
	args := types.MakeDatums([]interface{}{"abcdefg", int64(2)}...)

	fc := funcs[ast.Left]
	f, err := fc.getFunction(datumsToConstants(args), s.ctx)
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "ab")

	args = types.MakeDatums([]interface{}{"abcdefg", int64(-1)}...)
	f, err = fc.getFunction(datumsToConstants(args), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "")

	args = types.MakeDatums([]interface{}{"abcdefg", int64(100)}...)
	f, err = fc.getFunction(datumsToConstants(args), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "abcdefg")

	args = types.MakeDatums([]interface{}{1, int64(1)}...)
	f, err = fc.getFunction(datumsToConstants(args), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)

	args = types.MakeDatums([]interface{}{"abcdefg", "xxx"}...)
	f, err = fc.getFunction(datumsToConstants(args), s.ctx)
	c.Assert(err, IsNil)
	_, err = f.eval(nil)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestRepeat(c *C) {
	defer testleak.AfterTest(c)()
	args := []interface{}{"a", int64(2)}
	fc := funcs[ast.Repeat]
	f, err := fc.getFunction(datumsToConstants(types.MakeDatums(args...)), s.ctx)
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "aa")

	args = []interface{}{"a", uint64(2)}
	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(args...)), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "aa")

	args = []interface{}{"a", int64(-1)}
	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(args...)), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "")

	args = []interface{}{"a", int64(0)}
	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(args...)), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "")

	args = []interface{}{"a", uint64(0)}
	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(args...)), s.ctx)
	c.Assert(err, IsNil)
	v, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "")
}

func (s *testEvaluatorSuite) TestLowerAndUpper(c *C) {
	defer testleak.AfterTest(c)()
	lower := funcs[ast.Lower]
	f, err := lower.getFunction(datumsToConstants(types.MakeDatums(nil)), s.ctx)
	c.Assert(err, IsNil)
	d, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)

	upper := funcs[ast.Upper]
	f, err = upper.getFunction(datumsToConstants(types.MakeDatums(nil)), s.ctx)
	c.Assert(err, IsNil)
	d, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)

	tbl := []struct {
		Input  interface{}
		Expect string
	}{
		{"abc", "abc"},
		{1, "1"},
	}

	dtbl := tblToDtbl(tbl)

	for _, t := range dtbl {
		f, err = lower.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		d, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expect"][0])

		f, err = upper.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		d, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(d.GetString(), Equals, strings.ToUpper(t["Expect"][0].GetString()))
	}
}

func (s *testEvaluatorSuite) TestReverse(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.Reverse]
	f, err := fc.getFunction(datumsToConstants(types.MakeDatums(nil)), s.ctx)
	c.Assert(err, IsNil)
	d, err := f.eval(nil)
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
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		d, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expect"][0])
	}
}

func (s *testEvaluatorSuite) TestStrcmp(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input  []interface{}
		Expect interface{}
	}{
		{[]interface{}{"1", "2"}, -1},
		{[]interface{}{"2", "1"}, 1},
		{[]interface{}{"123", "2"}, -1},
		{[]interface{}{"1", "213"}, -1},
		{[]interface{}{"123", "123"}, 0},
		{[]interface{}{"", "123"}, -1},
		{[]interface{}{"123", ""}, 1},
		{[]interface{}{"", ""}, 0},
		{[]interface{}{nil, "123"}, nil},
		{[]interface{}{"123", nil}, nil},
		{[]interface{}{nil, nil}, nil},
		{[]interface{}{"", nil}, nil},
		{[]interface{}{nil, ""}, nil},
	}

	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		fc := funcs[ast.Strcmp]
		f, err := fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		d, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expect"][0])
	}
}

func (s *testEvaluatorSuite) TestReplace(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input  []interface{}
		Expect interface{}
	}{
		{[]interface{}{nil, nil, nil}, nil},
		{[]interface{}{1, nil, 2}, nil},
		{[]interface{}{1, 1, nil}, nil},
		{[]interface{}{"12345", 2, 222}, "1222345"},
		{[]interface{}{"12325", 2, "a"}, "1a3a5"},
		{[]interface{}{12345, 2, "aa"}, "1aa345"},
	}

	dtbl := tblToDtbl(tbl)

	for _, t := range dtbl {
		fc := funcs[ast.Replace]
		f, err := fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		d, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expect"][0])
	}
}

func (s *testEvaluatorSuite) TestSubstring(c *C) {
	defer testleak.AfterTest(c)()

	fc := funcs[ast.Substring]
	f, err := fc.getFunction(datumsToConstants(types.MakeDatums("hello", 2, -1)), s.ctx)
	c.Assert(err, IsNil)
	d, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.GetString(), Equals, "")

	tbl := []struct {
		str    string
		pos    int64
		slen   int64
		result string
	}{
		{"Quadratically", 5, -1, "ratically"},
		{"foobarbar", 4, -1, "barbar"},
		{"Sakila", 1, -1, "Sakila"},
		{"Sakila", 2, -1, "akila"},
		{"Sakila", -3, -1, "ila"},
		{"Sakila", -5, 3, "aki"},
		{"Sakila", -4, 2, "ki"},
		{"Quadratically", 5, 6, "ratica"},
		{"Sakila", 1, 4, "Saki"},
		{"Sakila", -6, 4, "Saki"},
		{"Sakila", 2, 1000, "akila"},
		{"Sakila", -5, 1000, "akila"},
		{"Sakila", 2, -2, ""},
		{"Sakila", -5, -2, ""},
		{"Sakila", 2, 0, ""},
		{"Sakila", -5, -3, ""},
		{"Sakila", -1000, 3, ""},
		{"Sakila", 1000, 2, ""},
		{"", 2, 3, ""},
	}
	for _, v := range tbl {
		datums := types.MakeDatums(v.str, v.pos)
		if v.slen != -1 {
			datums = append(datums, types.NewDatum(v.slen))
		}
		args := datumsToConstants(datums)
		f, err := fc.getFunction(args, s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(r.Kind(), Equals, types.KindString)
		c.Assert(r.GetString(), Equals, v.result)

		r1, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(r1.Kind(), Equals, types.KindString)
		c.Assert(r.GetString(), Equals, r1.GetString())
	}
	errTbl := []struct {
		str    interface{}
		pos    interface{}
		len    interface{}
		result string
	}{
		{"foobarbar", "4", -1, "barbar"},
		{"Quadratically", 5, "6", "ratica"},
	}
	for _, v := range errTbl {
		fc := funcs[ast.Substring]
		datums := types.MakeDatums(v.str, v.pos)
		if v.len != -1 {
			datums = append(datums, types.NewDatum(v.len))
		}
		args := datumsToConstants(datums)
		f, err := fc.getFunction(args, s.ctx)
		c.Assert(err, IsNil)
		_, err = f.eval(nil)
		c.Assert(err, NotNil)
	}
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
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(v.str, v.cs)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
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
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(v.str, v.cs)), s.ctx)
		c.Assert(err, IsNil)
		_, err = f.eval(nil)
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestSubstringIndex(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		str    string
		delim  string
		count  int64
		result string
	}{
		{"www.mysql.com", ".", 2, "www.mysql"},
		{"www.mysql.com", ".", -2, "mysql.com"},
		{"www.mysql.com", ".", 0, ""},
		{"www.mysql.com", ".", 3, "www.mysql.com"},
		{"www.mysql.com", ".", 4, "www.mysql.com"},
		{"www.mysql.com", ".", -3, "www.mysql.com"},
		{"www.mysql.com", ".", -4, "www.mysql.com"},

		{"www.mysql.com", "d", 1, "www.mysql.com"},
		{"www.mysql.com", "d", 0, ""},
		{"www.mysql.com", "d", -1, "www.mysql.com"},

		{"", ".", 2, ""},
		{"", ".", -2, ""},
		{"", ".", 0, ""},

		{"www.mysql.com", "", 1, ""},
		{"www.mysql.com", "", -1, ""},
		{"www.mysql.com", "", 0, ""},
	}
	for _, v := range tbl {
		fc := funcs[ast.SubstringIndex]
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(v.str, v.delim, v.count)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(r.Kind(), Equals, types.KindString)
		c.Assert(r.GetString(), Equals, v.result)
	}
	errTbl := []struct {
		str   interface{}
		delim interface{}
		count interface{}
	}{
		{nil, ".", 2},
		{nil, ".", -2},
		{nil, ".", 0},
		{"asdf", nil, 2},
		{"asdf", nil, -2},
		{"asdf", nil, 0},
		{"www.mysql.com", ".", nil},
	}
	for _, v := range errTbl {
		fc := funcs[ast.SubstringIndex]
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(v.str, v.delim, v.count)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(err, NotNil)
		c.Assert(r.Kind(), Equals, types.KindNull)
	}
}

func (s *testEvaluatorSuite) TestSpace(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.Space]
	f, err := fc.getFunction(datumsToConstants(types.MakeDatums(nil)), s.ctx)
	c.Assert(err, IsNil)
	d, err := f.eval(nil)
	c.Assert(d.Kind(), Equals, types.KindNull)

	f, err = fc.getFunction(datumsToConstants(types.MakeDatums(8888888888)), s.ctx)
	c.Assert(err, IsNil)
	d, err = f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)

	tbl := []struct {
		Input  interface{}
		Expect string
	}{
		{5, "     "},
		{0, ""},
		{-1, ""},
		{"5", "     "},
	}

	dtbl := tblToDtbl(tbl)
	for _, t := range dtbl {
		f, err = fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		d, err = f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expect"][0])
	}

	// TODO: the error depends on statement context, add those back when statemen context is supported.
	//wrong := []struct {
	//	Input string
	//}{
	//	{"abc"},
	//	{"3.3"},
	//	{""},
	//}

	//
	//dwrong := tblToDtbl(wrong)
	//for _, t := range dwrong {
	//	_, err = builtinSpace(t["Input"], s.ctx)
	//	c.Assert(err, NotNil)
	//}
}

func (s *testEvaluatorSuite) TestLocate(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		subStr string
		Str    string
		result int64
	}{
		{"bar", "foobarbar", 4},
		{"xbar", "foobar", 0},
		{"", "foobar", 1},
		{"foobar", "", 0},
		{"", "", 1},
	}
	for _, v := range tbl {
		fc := funcs[ast.Locate]
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(v.subStr, v.Str)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(r.Kind(), Equals, types.KindInt64)
		c.Assert(r.GetInt64(), Equals, v.result)
	}

	tbl2 := []struct {
		subStr string
		Str    string
		pos    int64
		result int64
	}{
		{"bar", "foobarbar", 5, 7},
		{"xbar", "foobar", 1, 0},
		{"", "foobar", 2, 2},
		{"foobar", "", 1, 0},
		{"", "", 2, 0},
	}
	for _, v := range tbl2 {
		fc := funcs[ast.Locate]
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(v.subStr, v.Str, v.pos)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(r.Kind(), Equals, types.KindInt64)
		c.Assert(r.GetInt64(), Equals, v.result)
	}

	errTbl := []struct {
		subStr interface{}
		Str    interface{}
	}{
		{nil, nil},
		{"", nil},
		{nil, ""},
		{"foo", nil},
		{nil, "bar"},
	}
	for _, v := range errTbl {
		fc := funcs[ast.Locate]
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(v.subStr, v.Str)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(r.Kind(), Equals, types.KindNull)
	}

	errTbl2 := []struct {
		subStr interface{}
		Str    interface{}
		pos    interface{}
	}{
		{nil, nil, 1},
		{"", nil, 1},
		{nil, "", 1},
		{"foo", nil, -1},
		{nil, "bar", 0},
	}
	for _, v := range errTbl2 {
		fc := funcs[ast.Locate]
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(v.subStr, v.Str)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(r.Kind(), Equals, types.KindNull)
	}
}

func (s *testEvaluatorSuite) TestTrim(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		str    interface{}
		remstr interface{}
		dir    ast.TrimDirectionType
		result interface{}
	}{
		{"  bar   ", nil, ast.TrimBothDefault, "bar"},
		{"xxxbarxxx", "x", ast.TrimLeading, "barxxx"},
		{"xxxbarxxx", "x", ast.TrimBoth, "bar"},
		{"barxxyz", "xyz", ast.TrimTrailing, "barx"},
		{nil, "xyz", ast.TrimBoth, nil},
		{1, 2, ast.TrimBoth, "1"},
		{"  \t\rbar\n   ", nil, ast.TrimBothDefault, "bar"},
	}
	for _, v := range tbl {
		fc := funcs[ast.Trim]
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(v.str, v.remstr, v.dir)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(v.result))
	}

	for _, v := range []struct {
		str, result interface{}
		fn          string
	}{
		{"  ", "", ast.LTrim},
		{"  ", "", ast.RTrim},
		{"foo0", "foo0", ast.LTrim},
		{"bar0", "bar0", ast.RTrim},
		{"  foo1", "foo1", ast.LTrim},
		{"bar1  ", "bar1", ast.RTrim},
		{spaceChars + "foo2  ", "foo2  ", ast.LTrim},
		{"  bar2" + spaceChars, "  bar2", ast.RTrim},
		{nil, nil, ast.LTrim},
		{nil, nil, ast.RTrim},
	} {
		fc := funcs[v.fn]
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(v.str)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(v.result))
	}
}
func (s *testEvaluatorSuite) TestHexFunc(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input  interface{}
		Expect string
	}{
		{12, "C"},
		{12.3, "C"},
		{12.5, "D"},
		{-12.3, "FFFFFFFFFFFFFFF4"},
		{-12.5, "FFFFFFFFFFFFFFF3"},
		{"12", "3132"},
		{0x12, "12"},
		{"", ""},
	}

	dtbl := tblToDtbl(tbl)
	fc := funcs[ast.Hex]
	for _, t := range dtbl {
		f, err := fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		d, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expect"][0])

	}
}
func (s *testEvaluatorSuite) TestUnhexFunc(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Input  interface{}
		Expect string
	}{
		{"4D7953514C", "MySQL"},
		{"31323334", "1234"},
		{"", ""},
	}

	dtbl := tblToDtbl(tbl)
	fc := funcs[ast.Unhex]
	for _, t := range dtbl {
		f, err := fc.getFunction(datumsToConstants(t["Input"]), s.ctx)
		c.Assert(err, IsNil)
		d, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(d, testutil.DatumEquals, t["Expect"][0])

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
		f, err := fc.getFunction(datumsToConstants([]types.Datum{str, length, padStr}), s.ctx)
		c.Assert(err, IsNil)
		result, err := f.eval(nil)
		c.Assert(err, IsNil)
		if test.expect == nil {
			c.Assert(result.Kind(), Equals, types.KindNull)
		} else {
			expect, _ := test.expect.(string)
			c.Assert(result.GetString(), Equals, expect)
		}
	}
}

func (s *testEvaluatorSuite) TestBitLength(c *C) {
	tests := []struct {
		str    string
		expect int64
	}{
		{"hi", 16},
		{"你好", 48},
		{"", 0},
	}
	for _, test := range tests {
		fc := funcs[ast.BitLength]
		str := types.NewStringDatum(test.str)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{str}), s.ctx)
		c.Assert(err, IsNil)
		result, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(result.GetInt64(), Equals, test.expect)
	}

	errTbl := []struct {
		str    interface{}
		expect interface{}
	}{
		{nil, nil},
	}
	for _, test := range errTbl {
		fc := funcs[ast.BitLength]
		str := types.NewDatum(test.str)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{str}), s.ctx)
		c.Assert(err, IsNil)
		result, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(result.Kind(), Equals, types.KindNull)
	}

}

func (s *testEvaluatorSuite) TestChar(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		str    string
		iNum   int64
		fNum   float64
		result string
	}{
		{"65", 66, 67.5, "ABD"},                // float
		{"65", 16740, 67.5, "AAdD"},            // large num
		{"65", -1, 67.5, "A\xff\xff\xff\xffD"}, // nagtive int
		{"a", -1, 67.5, ""},                    // invalid 'a'
	}
	for _, v := range tbl {
		for _, char := range []interface{}{"utf8", nil} {
			fc := funcs[ast.CharFunc]
			f, err := fc.getFunction(datumsToConstants(types.MakeDatums(v.str, v.iNum, v.fNum, char)), s.ctx)
			c.Assert(err, IsNil)
			r, err := f.eval(nil)
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
	f, err := fc.getFunction(datumsToConstants(types.MakeDatums(v.str, v.iNum, nil)), s.ctx)
	c.Assert(err, IsNil)
	r, err := f.eval(nil)
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
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(v.input)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
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
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(t.str, t.strlst)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}

func (s *testEvaluatorSuite) TestField(c *C) {
	defer testleak.AfterTest(c)()

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
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(t.argLst...)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
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
		f, err := fc.getFunction(datumsToConstants([]types.Datum{str, length, padStr}), s.ctx)
		c.Assert(err, IsNil)
		result, err := f.eval(nil)
		c.Assert(err, IsNil)
		if test.expect == nil {
			c.Assert(result.Kind(), Equals, types.KindNull)
		} else {
			expect, _ := test.expect.(string)
			c.Assert(result.GetString(), Equals, expect)
		}
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
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(t.argList...)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}

func (s *testEvaluatorSuite) TestOct(c *C) {
	defer testleak.AfterTest(c)()
	octCases := []struct {
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
	}
	fc := funcs[ast.Oct]
	for _, test := range octCases {
		in := types.NewDatum(test.origin)
		f, _ := fc.getFunction(datumsToConstants([]types.Datum{in}), s.ctx)
		r, err := f.eval(nil)
		c.Assert(err, IsNil)
		res, err := r.ToString()
		c.Assert(err, IsNil)
		c.Assert(res, Equals, test.ret)
	}
	// test NULL input for sha
	var argNull types.Datum
	f, _ := fc.getFunction(datumsToConstants([]types.Datum{argNull}), s.ctx)
	r, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(r.IsNull(), IsTrue)
}

func (s *testEvaluatorSuite) TestOrd(c *C) {
	defer testleak.AfterTest(c)()
	ordCases := []struct {
		origin interface{}
		ret    int64
	}{
		// ASCII test cases
		{"", 0},
		{"A", 65},
		{"你好", 14990752},
		{1, 49},
		{1.2, 49},
		{true, 49},
		{false, 48},

		{2, 50},
		{-1, 45},
		{"-1", 45},
		{"2", 50},
		{"PingCap", 80},
		{"中国", 14989485},
		{"にほん", 14909867},
		{"한국", 15570332},
	}

	fc := funcs[ast.Ord]
	for _, testcase := range ordCases {
		in := types.NewDatum(testcase.origin)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{in}), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v.GetInt64(), Equals, testcase.ret)
	}
	// test NULL input for sha
	var argNull types.Datum
	f, err := fc.getFunction(datumsToConstants([]types.Datum{argNull}), s.ctx)
	c.Assert(err, IsNil)
	r, err := f.eval(nil)
	c.Assert(r.IsNull(), IsTrue)
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
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(t.argLst...)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}
