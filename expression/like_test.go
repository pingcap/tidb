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
	"github.com/juju/errors"
	. "github.com/pingcap/check"
)

var _ = Suite(&testLikeSuite{})

type testLikeSuite struct {
}

func (*testLikeSuite) TestLike(c *C) {
	tbl := []struct {
		pattern string
		input   string
		escape  byte
		match   bool
	}{
		{"", "a", '\\', false},
		{"a", "a", '\\', true},
		{"a", "b", '\\', false},
		{"aA", "aA", '\\', true},
		{"_", "a", '\\', true},
		{"_", "ab", '\\', false},
		{"__", "b", '\\', false},
		{"_ab", "AAB", '\\', true},
		{"%", "abcd", '\\', true},
		{"%", "", '\\', true},
		{"%a", "AAA", '\\', true},
		{"%b", "AAA", '\\', false},
		{"b%", "BBB", '\\', true},
		{"%a%", "BBB", '\\', false},
		{"%a%", "BAB", '\\', true},
		{"a%", "BBB", '\\', false},
		{`\%a`, `%a`, '\\', true},
		{`\%a`, `aa`, '\\', false},
		{`\_a`, `_a`, '\\', true},
		{`\_a`, `aa`, '\\', false},
		{`\\_a`, `\xa`, '\\', true},
		{`\a\b`, `\a\b`, '\\', true},
		{"%%_", `abc`, '\\', true},
		{`+_a`, `_a`, '+', true},
		{`+%a`, `%a`, '+', true},
		{`\%a`, `%a`, '+', false},
		{`++a`, `+a`, '+', true},
		{`++_a`, `+xa`, '+', true},
	}
	for _, v := range tbl {
		patChars, patTypes := compilePattern(v.pattern, v.escape)
		match := doMatch(v.input, patChars, patTypes)
		c.Assert(match, Equals, v.match, Commentf("%v", v))
	}
}

func (*testLikeSuite) TestEval(c *C) {
	pattern := &PatternLike{
		Expr: &Value{
			Val: "aA",
		},
		Pattern: &Value{
			Val: "aA",
		},

		Escape: '\\',
	}
	cloned := pattern.Clone()
	pattern = cloned.(*PatternLike)
	c.Assert(pattern.IsStatic(), IsTrue)
	c.Assert(pattern.String(), Not(Equals), "")
	val, err := pattern.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(val, IsTrue)
	pattern.Not = true
	val, err = pattern.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(val, IsFalse)

	pattern.Pattern = mockExpr{isStatic: false, err: errors.Errorf("test error")}
	c.Assert(pattern.Clone(), NotNil)
	_, err = pattern.Eval(nil, nil)
	c.Assert(err, NotNil)
	pattern.Pattern = mockExpr{isStatic: false, val: nil}
	_, err = pattern.Eval(nil, nil)
	c.Assert(err, IsNil)
	pattern.Pattern = mockExpr{isStatic: false, val: 123}
	_, err = pattern.Eval(nil, nil)
	c.Assert(err, IsNil)

	pattern.Expr = mockExpr{isStatic: false, err: errors.Errorf("test error")}
	c.Assert(pattern.Clone(), NotNil)
	_, err = pattern.Eval(nil, nil)
	c.Assert(err, NotNil)
	pattern.Expr = mockExpr{isStatic: false, val: 123}
	_, err = pattern.Eval(nil, nil)
	c.Assert(err, IsNil)
	pattern.Expr = mockExpr{isStatic: false, val: nil}
	_, err = pattern.Eval(nil, nil)
	c.Assert(err, IsNil)

	// Testcase for "LIKE BINARY xxx"
	pattern = &PatternLike{
		Expr:    mockExpr{isStatic: true, val: "slien"},
		Pattern: mockExpr{isStatic: true, val: []byte("%E%")},
		Escape:  '\\',
	}
	v, err := pattern.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsTrue)
	pattern = &PatternLike{
		Expr:    mockExpr{isStatic: true, val: "slin"},
		Pattern: mockExpr{isStatic: true, val: []byte("%E%")},
		Escape:  '\\',
	}
	v, err = pattern.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsFalse)
}
