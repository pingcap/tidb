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

package expressions

import (
	. "github.com/pingcap/check"
)

func (s *testBuiltinSuite) TestLength(c *C) {
	v, err := builtinLength([]interface{}{nil}, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)

	v, err = builtinLength([]interface{}{"abc"}, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(3))

	_, err = builtinLength([]interface{}{1}, nil)
	c.Assert(err, NotNil)
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

	args = []interface{}{mockExpr{}}
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

	args = []interface{}{mockExpr{}}
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
