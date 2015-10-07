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

package builtin

import (
	"errors"
	"strings"
	"time"

	. "github.com/pingcap/check"
	mysql "github.com/pingcap/tidb/mysqldef"
)

func (s *testBuiltinSuite) TestLength(c *C) {
	v, err := builtinLength([]interface{}{nil}, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

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

	for _, t := range tbl {
		v, err = builtinLength([]interface{}{t.Input}, nil)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t.Expected)
	}
}

func (s *testBuiltinSuite) TestConcat(c *C) {
	args := []interface{}{nil}

	v, err := builtinConcat(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	args = []interface{}{"a", "b", "c"}
	v, err = builtinConcat(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "abc")

	args = []interface{}{"a", "b", nil, "c"}
	v, err = builtinConcat(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	args = []interface{}{errors.New("must error")}
	_, err = builtinConcat(args, nil)
	c.Assert(err, NotNil)
}

func (s *testBuiltinSuite) TestConcatWS(c *C) {
	args := []interface{}{nil}

	v, err := builtinConcatWS(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	args = []interface{}{"|", "a", nil, "b", "c"}
	v, err = builtinConcatWS(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "a|b|c")

	args = []interface{}{errors.New("must error")}
	_, err = builtinConcatWS(args, nil)
	c.Assert(err, NotNil)
}

func (s *testBuiltinSuite) TestLeft(c *C) {
	args := []interface{}{"abcdefg", int64(2)}
	v, err := builtinLeft(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "ab")

	args = []interface{}{"abcdefg", int64(-1)}
	v, err = builtinLeft(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "")

	args = []interface{}{"abcdefg", int64(100)}
	v, err = builtinLeft(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "abcdefg")

	args = []interface{}{1, int64(1)}
	_, err = builtinLeft(args, nil)
	c.Assert(err, NotNil)

	args = []interface{}{"abcdefg", "xxx"}
	_, err = builtinLeft(args, nil)
	c.Assert(err, NotNil)
}

func (s *testBuiltinSuite) TestRepeat(c *C) {
	args := []interface{}{"a", int64(2)}
	v, err := builtinRepeat(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "aa")

	args = []interface{}{"a", uint64(2)}
	v, err = builtinRepeat(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "aa")

	args = []interface{}{"a", int64(-1)}
	v, err = builtinRepeat(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "")

	args = []interface{}{"a", int64(0)}
	v, err = builtinRepeat(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "")

	args = []interface{}{"a", uint64(0)}
	v, err = builtinRepeat(args, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "")
}

func (s *testBuiltinSuite) TestLowerAndUpper(c *C) {
	v, err := builtinLower([]interface{}{nil}, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	v, err = builtinUpper([]interface{}{nil}, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	tbl := []struct {
		Input  interface{}
		Expect string
	}{
		{"abc", "abc"},
		{1, "1"},
	}

	for _, t := range tbl {
		args := []interface{}{t.Input}
		v, err = builtinLower(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t.Expect)

		v, err = builtinUpper(args, nil)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, strings.ToUpper(t.Expect))
	}
}
