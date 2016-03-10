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
	. "github.com/pingcap/check"
)

func (s *testEvaluatorSuite) TestAbs(c *C) {
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{int64(1), int64(1)},
		{uint64(1), uint64(1)},
		{int64(-1), int64(1)},
		{float64(3.14), float64(3.14)},
		{float64(-3.14), float64(3.14)},
	}

	for _, t := range tbl {
		v, err := builtinAbs([]interface{}{t.Arg}, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.Ret)
	}
}

func (s *testEvaluatorSuite) TestRand(c *C) {
	v, err := builtinRand([]interface{}{}, nil)
	c.Assert(err, IsNil)
	c.Assert(v, Less, float64(1))
	c.Assert(v, GreaterEqual, float64(0))
}

func (s *testEvaluatorSuite) TestPow(c *C) {
	tbl := []struct {
		Arg []interface{}
		Ret float64
	}{
		{[]interface{}{1, 3}, 1},
		{[]interface{}{2, 2}, 4},
		{[]interface{}{4, 0.5}, 2},
		{[]interface{}{4, -2}, 0.0625},
	}

	for _, t := range tbl {
		v, err := builtinPow(t.Arg, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.Ret)
	}

	errTbl := []struct {
		Arg []interface{}
	}{
		{[]interface{}{"test", "test"}},
		{[]interface{}{nil, nil}},
		{[]interface{}{1, "test"}},
		{[]interface{}{1, nil}},
	}
	for _, t := range errTbl {
		_, err := builtinPow(t.Arg, nil)
		c.Assert(err, NotNil)
	}

}
