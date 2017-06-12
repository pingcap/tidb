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
	"strings"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/mock"
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
		{[]interface{}{"www.mysql.com", "", "tidb"}, "www.mysql.com"},
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

	// TODO: the error depends on statement context, add those back when statement context is supported.
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
		{[]interface{}{[]byte("BaR"), "foobArbar"}, 0},
		{[]interface{}{"BaR", []byte("foobArbar")}, 0},
		{[]interface{}{nil, "foobar"}, nil},
		{[]interface{}{"bar", nil}, nil},
	}

	Dtbl := tblToDtbl(tbl)
	instr := funcs[ast.Locate]
	for i, t := range Dtbl {
		f, err := instr.getFunction(datumsToConstants(t["Args"]), s.ctx)
		c.Assert(err, IsNil)
		got, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(got, DeepEquals, t["Want"][0], Commentf("[%d]: args: %v", i, t["Args"]))
	}

	tbl2 := []struct {
		Args []interface{}
		Want interface{}
	}{
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
		{[]interface{}{[]byte("bAr"), "foobarBaR", 5}, 0},
		{[]interface{}{"bAr", []byte("foobarBaR"), 5}, 0},
		{[]interface{}{"bAr", []byte("foobarbAr"), 5}, 7},
	}
	Dtbl2 := tblToDtbl(tbl2)
	for i, t := range Dtbl2 {
		f, err := instr.getFunction(datumsToConstants(t["Args"]), s.ctx)
		c.Assert(err, IsNil)
		got, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(got, DeepEquals, t["Want"][0], Commentf("[%d]: args: %v", i, t["Args"]))
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
		c.Assert(err, IsNil)
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
		{[]interface{}{[]byte("BINARY string"), []byte("binary")}, 0},
		{[]interface{}{[]byte("BINARY string"), []byte("BINARY")}, 1},
		{[]interface{}{[]byte("中文abc"), []byte("abc")}, 7},

		{[]interface{}{"foobar", nil}, nil},
		{[]interface{}{nil, "foobar"}, nil},
		{[]interface{}{nil, nil}, nil},
	}

	Dtbl := tblToDtbl(tbl)
	instr := funcs[ast.Instr]
	for i, t := range Dtbl {
		f, err := instr.getFunction(datumsToConstants(t["Args"]), s.ctx)
		c.Assert(err, IsNil)
		got, err := f.eval(nil)
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
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(t.argList...)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
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
	}
	fc := funcs[ast.Oct]
	for _, tt := range octTests {
		in := types.NewDatum(tt.origin)
		f, _ := fc.getFunction(datumsToConstants([]types.Datum{in}), s.ctx)
		r, err := f.eval(nil)
		c.Assert(err, IsNil)
		res, err := r.ToString()
		c.Assert(err, IsNil)
		c.Assert(res, Equals, tt.ret)
	}
	// tt NULL input for sha
	var argNull types.Datum
	f, _ := fc.getFunction(datumsToConstants([]types.Datum{argNull}), s.ctx)
	r, err := f.eval(nil)
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
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(tt.number, tt.precision, tt.locale)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(tt.ret))
	}

	for _, tt := range formatTests1 {
		fc := funcs[ast.Format]
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(tt.number, tt.precision)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(tt.ret))
	}

	fc2 := funcs[ast.Format]
	f2, err := fc2.getFunction(datumsToConstants(types.MakeDatums(formatTests2.number, formatTests2.precision, formatTests2.locale)), s.ctx)
	c.Assert(err, IsNil)
	r2, err := f2.eval(nil)
	c.Assert(types.NewDatum(err), testutil.DatumEquals, types.NewDatum(errors.New("not implemented")))
	c.Assert(r2, testutil.DatumEquals, types.NewDatum(formatTests2.ret))

	fc3 := funcs[ast.Format]
	f3, err := fc3.getFunction(datumsToConstants(types.MakeDatums(formatTests3.number, formatTests3.precision, formatTests3.locale)), s.ctx)
	c.Assert(err, IsNil)
	r3, err := f3.eval(nil)
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
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(test.args)), s.ctx)
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
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(test.args...)), s.ctx)
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

func (s *testEvaluatorSuite) TestOrd(c *C) {
	defer testleak.AfterTest(c)()
	ordTests := []struct {
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
	for _, tt := range ordTests {
		in := types.NewDatum(tt.origin)
		f, err := fc.getFunction(datumsToConstants([]types.Datum{in}), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v.GetInt64(), Equals, tt.ret)
	}
	// test NULL input for sha
	var argNull types.Datum
	f, err := fc.getFunction(datumsToConstants([]types.Datum{argNull}), s.ctx)
	c.Assert(err, IsNil)
	r, err := f.eval(nil)
	c.Assert(err, IsNil)
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
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(t.argLst...)), s.ctx)
		c.Assert(err, IsNil)
		exportSetRes, err := f.eval(nil)
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
		f, err := fc.getFunction(datumsToConstants(t["Input"]), ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
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
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(t.arg)), s.ctx)
		c.Assert(err, IsNil)
		r, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}

func (s *testEvaluatorSuite) TestToBase64(c *C) {
	tests := []struct {
		args   interface{}
		expect interface{}
	}{
		{string(""), string("")},
		{string("abc"), string("YWJj")},
		{string("ab c"), string("YWIgYw==")},
		{string("ab\nc"), string("YWIKYw==")},
		{string("ab\tc"), string("YWIJYw==")},
		{string("qwerty123456"), string("cXdlcnR5MTIzNDU2")},
		{
			string("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"),
			string("QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrLw=="),
		},
		{
			string("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"),
			string("QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ejAxMjM0\nNTY3ODkrL0FCQ0RFRkdISUpLTE1OT1BRUlNUVVZXWFlaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4\neXowMTIzNDU2Nzg5Ky9BQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWmFiY2RlZmdoaWprbG1ub3Bx\ncnN0dXZ3eHl6MDEyMzQ1Njc4OSsv"),
		},
		{
			string("ABCD  EFGHI\nJKLMNOPQRSTUVWXY\tZabcdefghijklmnopqrstuv  wxyz012\r3456789+/"),
			string("QUJDRCAgRUZHSEkKSktMTU5PUFFSU1RVVldYWQlaYWJjZGVmZ2hpamtsbW5vcHFyc3R1diAgd3h5\nejAxMg0zNDU2Nzg5Ky8="),
		},
	}
	fc := funcs[ast.ToBase64]
	for _, test := range tests {
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(test.args)), s.ctx)
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
		f, _ := fc.getFunction(datumsToConstants([]types.Datum{str, length}), s.ctx)
		result, err := f.eval(nil)
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
