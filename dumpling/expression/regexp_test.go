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

var _ = Suite(&testRegexpSuite{})

type testRegexpSuite struct {
}

func (rs *testRegexpSuite) TestT(c *C) {
	tbl := []struct {
		pattern string
		input   string
		match   bool
	}{
		{"^$", "a", false},
		{"a", "a", true},
		{"a", "b", false},
		{"aA", "aA", true},
		{".", "a", true},
		{"^.$", "ab", false},
		{"..", "b", false},
		{".ab", "aab", true},
		{".*", "abcd", true},
	}
	for _, v := range tbl {
		pattern := &PatternRegexp{
			Pattern: &Value{v.pattern},
			Expr:    &Value{v.input},
		}
		match, err := pattern.Eval(nil, nil)
		c.Assert(err, IsNil)
		c.Assert(match.(bool), Equals, v.match, Commentf("%v", v))
	}

	pat := &PatternRegexp{
		Pattern: &mockExpr{err: errors.Errorf("test error")},
		Expr:    &mockExpr{err: errors.Errorf("test error")},
	}
	c.Assert(pat.Clone(), NotNil)
	_, err := pat.Eval(nil, nil)
	c.Assert(err, NotNil)
	pat.Expr = &Value{"^$"}
	c.Assert(pat.Clone(), NotNil)
	_, err = pat.Eval(nil, nil)
	c.Assert(err, NotNil)
	pat.Pattern = &Value{"a"}
	c.Assert(pat.Clone(), NotNil)
	_, err = pat.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(pat.IsStatic(), IsTrue)
	c.Assert(pat.String(), Not(Equals), "")
	pat.Not = true
	_, err = pat.Eval(nil, nil)
	c.Assert(err, IsNil)

	pat.Re = nil
	pat.Pattern = &Value{123}
	_, err = pat.Eval(nil, nil)
	c.Assert(err, IsNil)
	pat.Re = nil
	pat.Pattern = &Value{"["}
	_, err = pat.Eval(nil, nil)
	c.Assert(err, NotNil)
	pat.Pattern = &Value{nil}
	_, err = pat.Eval(nil, nil)
	c.Assert(err, IsNil)

	pat.Sexpr = nil
	pat.Expr = &Value{123}
	_, err = pat.Eval(nil, nil)
	c.Assert(err, IsNil)
	pat.Sexpr = nil
	pat.Expr = &Value{nil}
	_, err = pat.Eval(nil, nil)
	c.Assert(err, IsNil)
}
