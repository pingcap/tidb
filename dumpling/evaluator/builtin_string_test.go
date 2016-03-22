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

package evaluator

import (
	"errors"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestLength(c *C) {
	d, err := builtinLength(types.MakeDatums([]interface{}{nil}...), nil)
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)

	tbl := []struct {
		Input    interface{}
		Expected int64
	}{
		{"abc", 3},
		{1, 1},
		{3.14, 4},
		{mysql.Time{Time: time.Now(), Fsp: 6, Type: mysql.TypeDatetime}, 26},
		{mysql.Bit{Value: 1, Width: 8}, 1},
		{mysql.Hex{Value: 1}, 1},
		{mysql.Set{Value: 1, Name: "abc"}, 3},
	}

	dtbl := tblToDtbl(tbl)

	for _, t := range dtbl {
		d, err = builtinLength(t["Input"], nil)
		c.Assert(err, IsNil)
		c.Assert(d, DatumEquals, t["Expected"][0])
	}
}

func (s *testEvaluatorSuite) TestConcat(c *C) {
	args := []interface{}{nil}

	v, err := builtinConcat(types.MakeDatums(args...), nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	args = []interface{}{"a", "b", "c"}
	v, err = builtinConcat(types.MakeDatums(args...), nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "abc")

	args = []interface{}{"a", "b", nil, "c"}
	v, err = builtinConcat(types.MakeDatums(args...), nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	args = []interface{}{errors.New("must error")}
	_, err = builtinConcat(types.MakeDatums(args...), nil)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestConcatWS(c *C) {
	args := types.MakeDatums([]interface{}{nil}...)

	v, err := builtinConcatWS(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)

	args = types.MakeDatums([]interface{}{"|", "a", nil, "b", "c"}...)

	v, err = builtinConcatWS(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "a|b|c")

	args = types.MakeDatums([]interface{}{errors.New("must error")}...)
	_, err = builtinConcatWS(args, nil)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestLeft(c *C) {
	args := types.MakeDatums([]interface{}{"abcdefg", int64(2)}...)
	v, err := builtinLeft(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "ab")

	args = types.MakeDatums([]interface{}{"abcdefg", int64(-1)}...)
	v, err = builtinLeft(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "")

	args = types.MakeDatums([]interface{}{"abcdefg", int64(100)}...)
	v, err = builtinLeft(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "abcdefg")

	args = types.MakeDatums([]interface{}{1, int64(1)}...)
	_, err = builtinLeft(args, nil)
	c.Assert(err, IsNil)

	args = types.MakeDatums([]interface{}{"abcdefg", "xxx"}...)
	_, err = builtinLeft(args, nil)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestRepeat(c *C) {
	args := []interface{}{"a", int64(2)}
	v, err := builtinRepeat(types.MakeDatums(args...), nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "aa")

	args = []interface{}{"a", uint64(2)}
	v, err = builtinRepeat(types.MakeDatums(args...), nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "aa")

	args = []interface{}{"a", int64(-1)}
	v, err = builtinRepeat(types.MakeDatums(args...), nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "")

	args = []interface{}{"a", int64(0)}
	v, err = builtinRepeat(types.MakeDatums(args...), nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "")

	args = []interface{}{"a", uint64(0)}
	v, err = builtinRepeat(types.MakeDatums(args...), nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetString(), Equals, "")
}

func (s *testEvaluatorSuite) TestLowerAndUpper(c *C) {
	d, err := builtinLower(types.MakeDatums([]interface{}{nil}...), nil)
	c.Assert(err, IsNil)
	c.Assert(d.Kind(), Equals, types.KindNull)

	d, err = builtinUpper(types.MakeDatums([]interface{}{nil}...), nil)
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
		d, err = builtinLower(t["Input"], nil)
		c.Assert(err, IsNil)
		c.Assert(d, DatumEquals, t["Expect"][0])

		d, err = builtinUpper(t["Input"], nil)
		c.Assert(err, IsNil)
		c.Assert(d.GetString(), Equals, strings.ToUpper(t["Expect"][0].GetString()))
	}
}

func (s *testEvaluatorSuite) TestStrcmp(c *C) {
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
		d, err := builtinStrcmp(t["Input"], nil)
		c.Assert(err, IsNil)
		c.Assert(d, DatumEquals, t["Expect"][0])
	}
}

func (s *testEvaluatorSuite) TestReplace(c *C) {
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
		d, err := builtinReplace(t["Input"], nil)
		c.Assert(err, IsNil)
		c.Assert(d, DatumEquals, t["Expect"][0])
	}
}

func (s *testEvaluatorSuite) TestSubstring(c *C) {
	tbl := []struct {
		str    string
		pos    int64
		slen   int64
		result string
	}{
		{"Quadratically", 5, -1, "ratically"},
		{"foobarbar", 4, -1, "barbar"},
		{"Quadratically", 5, 6, "ratica"},
		{"Sakila", -3, -1, "ila"},
		{"Sakila", -5, 3, "aki"},
		{"Sakila", -4, 2, "ki"},
		{"Sakila", 1000, 2, ""},
		{"", 2, 3, ""},
	}
	ctx := mock.NewContext()
	for _, v := range tbl {
		f := &ast.FuncCallExpr{
			FnName: model.NewCIStr("SUBSTRING"),
			Args:   []ast.ExprNode{ast.NewValueExpr(v.str), ast.NewValueExpr(v.pos)},
		}
		if v.slen != -1 {
			f.Args = append(f.Args, ast.NewValueExpr(v.slen))
		}
		r, err := Eval(ctx, f)
		c.Assert(err, IsNil)
		s, ok := r.(string)
		c.Assert(ok, IsTrue)
		c.Assert(s, Equals, v.result)

		r1, err := Eval(ctx, f)
		c.Assert(err, IsNil)
		s1, ok := r1.(string)
		c.Assert(ok, IsTrue)
		c.Assert(s, Equals, s1)
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
		f := &ast.FuncCallExpr{
			FnName: model.NewCIStr("SUBSTRING"),
			Args:   []ast.ExprNode{ast.NewValueExpr(v.str), ast.NewValueExpr(v.pos)},
		}
		if v.len != -1 {
			f.Args = append(f.Args, ast.NewValueExpr(v.len))
		}
		_, err := Eval(ctx, f)
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestConvert(c *C) {
	ctx := mock.NewContext()
	tbl := []struct {
		str    string
		cs     string
		result string
	}{
		{"haha", "utf8", "haha"},
		{"haha", "ascii", "haha"},
	}
	for _, v := range tbl {
		f := &ast.FuncCallExpr{
			FnName: model.NewCIStr("CONVERT"),
			Args: []ast.ExprNode{
				ast.NewValueExpr(v.str),
				ast.NewValueExpr(v.cs),
			},
		}

		r, err := Eval(ctx, f)
		c.Assert(err, IsNil)
		s, ok := r.(string)
		c.Assert(ok, IsTrue)
		c.Assert(s, Equals, v.result)
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
		f := &ast.FuncCallExpr{
			FnName: model.NewCIStr("CONVERT"),
			Args: []ast.ExprNode{
				ast.NewValueExpr(v.str),
				ast.NewValueExpr(v.cs),
			},
		}

		_, err := Eval(ctx, f)
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestSubstringIndex(c *C) {
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
	ctx := mock.NewContext()
	for _, v := range tbl {
		f := &ast.FuncCallExpr{
			FnName: model.NewCIStr("SUBSTRING_INDEX"),
			Args:   []ast.ExprNode{ast.NewValueExpr(v.str), ast.NewValueExpr(v.delim), ast.NewValueExpr(v.count)},
		}
		r, err := Eval(ctx, f)
		c.Assert(err, IsNil)
		s, ok := r.(string)
		c.Assert(ok, IsTrue)
		c.Assert(s, Equals, v.result)
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
		f := &ast.FuncCallExpr{
			FnName: model.NewCIStr("SUBSTRING_INDEX"),
			Args:   []ast.ExprNode{ast.NewValueExpr(v.str), ast.NewValueExpr(v.delim), ast.NewValueExpr(v.count)},
		}
		r, err := Eval(ctx, f)
		c.Assert(err, NotNil)
		c.Assert(r, IsNil)
	}
}

func (s *testEvaluatorSuite) TestLocate(c *C) {
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
	ctx := mock.NewContext()
	for _, v := range tbl {
		f := &ast.FuncCallExpr{
			FnName: model.NewCIStr("LOCATE"),
			Args:   []ast.ExprNode{ast.NewValueExpr(v.subStr), ast.NewValueExpr(v.Str)},
		}
		r, err := Eval(ctx, f)
		c.Assert(err, IsNil)
		s, ok := r.(int64)
		c.Assert(ok, IsTrue)
		c.Assert(s, Equals, v.result)
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
		f := &ast.FuncCallExpr{
			FnName: model.NewCIStr("LOCATE"),
			Args:   []ast.ExprNode{ast.NewValueExpr(v.subStr), ast.NewValueExpr(v.Str), ast.NewValueExpr(v.pos)},
		}
		r, err := Eval(ctx, f)
		c.Assert(err, IsNil)
		s, ok := r.(int64)
		c.Assert(ok, IsTrue)
		c.Assert(s, Equals, v.result)
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
		f := &ast.FuncCallExpr{
			FnName: model.NewCIStr("LOCATE"),
			Args:   []ast.ExprNode{ast.NewValueExpr(v.subStr), ast.NewValueExpr(v.Str)},
		}
		r, _ := Eval(ctx, f)
		c.Assert(r, IsNil)
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
		f := &ast.FuncCallExpr{
			FnName: model.NewCIStr("LOCATE"),
			Args:   []ast.ExprNode{ast.NewValueExpr(v.subStr), ast.NewValueExpr(v.Str), ast.NewValueExpr(v.pos)},
		}
		r, _ := Eval(ctx, f)
		c.Assert(r, IsNil)
	}
}

func (s *testEvaluatorSuite) TestTrim(c *C) {
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
	ctx := mock.NewContext()
	for _, v := range tbl {
		f := &ast.FuncCallExpr{
			FnName: model.NewCIStr("TRIM"),
			Args: []ast.ExprNode{
				ast.NewValueExpr(v.str),
				ast.NewValueExpr(v.remstr),
				ast.NewValueExpr(v.dir),
			},
		}
		r, err := Eval(ctx, f)
		c.Assert(err, IsNil)
		c.Assert(r, Equals, v.result)
	}
}
