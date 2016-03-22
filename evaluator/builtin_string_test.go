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
	"github.com/pingcap/tidb/mysql"
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

	for _, t := range tbl {
		v, err := builtinStrcmp(t.Input, nil)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t.Expect)
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

	for _, t := range tbl {
		v, err := builtinReplace(t.Input, nil)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t.Expect)
	}
}
