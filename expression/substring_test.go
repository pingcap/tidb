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
	. "github.com/pingcap/check"
)

var _ = Suite(&testSubstringSuite{})

type testSubstringSuite struct {
}

func (s *testSubstringSuite) TestSubstring(c *C) {
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
	for _, v := range tbl {
		f := FunctionSubstring{
			StrExpr: &Value{Val: v.str},
			Pos:     &Value{Val: v.pos},
		}
		if v.slen != -1 {
			f.Len = &Value{Val: v.slen}
		}
		c.Assert(f.IsStatic(), Equals, true)

		fs := f.String()
		c.Assert(len(fs), Greater, 0)

		f1 := f.Clone()

		r, err := f.Eval(nil, nil)
		c.Assert(err, IsNil)
		s, ok := r.(string)
		c.Assert(ok, Equals, true)
		c.Assert(s, Equals, v.result)

		r1, err := f1.Eval(nil, nil)
		c.Assert(err, IsNil)
		s1, ok := r1.(string)
		c.Assert(ok, Equals, true)
		c.Assert(s, Equals, s1)
	}
	errTbl := []struct {
		str    interface{}
		pos    interface{}
		len    interface{}
		result string
	}{
		{1, 5, -1, "ratically"},
		{"foobarbar", "4", -1, "barbar"},
		{"Quadratically", 5, "6", "ratica"},
	}
	for _, v := range errTbl {
		f := FunctionSubstring{
			StrExpr: &Value{Val: v.str},
			Pos:     &Value{Val: v.pos},
		}
		if v.len != -1 {
			f.Len = &Value{Val: v.len}
		}
		_, err := f.Eval(nil, nil)
		c.Assert(err, NotNil)
	}
}

func (s *testSubstringSuite) TestSubstringIndex(c *C) {
	tbl := []struct {
		str    string
		delim  string
		count  int64
		result string
	}{
		{"www.mysql.com", ".", 2, "www.mysql"},
		{"www.mysql.com", ".", -2, "mysql.com"},
		{"www.mysql.com", ".", 20, "www.mysql.com"},
		{"www.mysql.com", ".", -20, "www.mysql.com"},
		{"www.mysql.com", "_", 2, "www.mysql.com"},
		{"www.mysql.com", "_", 0, ""},
	}
	for _, v := range tbl {
		f := FunctionSubstringIndex{
			StrExpr: &Value{Val: v.str},
			Delim:   &Value{Val: v.delim},
			Count:   &Value{Val: v.count},
		}
		c.Assert(f.IsStatic(), Equals, true)

		fs := f.String()
		c.Assert(len(fs), Greater, 0)

		f1 := f.Clone()

		r, err := f.Eval(nil, nil)
		c.Assert(err, IsNil)
		s, ok := r.(string)
		c.Assert(ok, Equals, true)
		c.Assert(s, Equals, v.result)

		r1, err := f1.Eval(nil, nil)
		c.Assert(err, IsNil)
		s1, ok := r1.(string)
		c.Assert(ok, Equals, true)
		c.Assert(s, Equals, s1)
	}
}
