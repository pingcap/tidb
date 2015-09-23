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

var _ = Suite(&testCaseSuite{})

type testCaseSuite struct {
}

func (s *testCaseSuite) TestCaseWhen(c *C) {
	when0 := &WhenClause{mockExpr{val: 1}, mockExpr{val: "str1"}}
	when1 := &WhenClause{mockExpr{val: 2}, mockExpr{val: "str2"}}
	whens0 := []*WhenClause{when0, when1}

	tbl := []struct {
		value      Expression
		whens      []*WhenClause
		elseClause Expression
		ret        interface{}
	}{
		{mockExpr{val: 1}, whens0, nil, "str1"},
		{mockExpr{val: 2}, whens0, nil, "str2"},
		{mockExpr{val: 3}, whens0, nil, nil},
		{mockExpr{val: 3}, whens0, mockExpr{val: "str3"}, "str3"},
	}

	for _, t := range tbl {
		f := FunctionCase{t.value, t.whens, t.elseClause}
		v, err := f.Eval(nil, nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t.ret)
	}

	f := FunctionCase{mockExpr{val: 4}, whens0, mockExpr{val: "else"}}
	cf := f.Clone()
	c.Assert(cf, NotNil)
	v, err := f.Eval(nil, nil)
	c.Assert(err, IsNil)
	cv, err := cf.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, cv)
	c.Assert(f.IsStatic(), IsFalse)

	str := f.String()
	c.Assert(len(str), Greater, 0)

	when2 := &WhenClause{mockExpr{val: 2, isStatic: true}, mockExpr{val: "str2", isStatic: true}}
	whens1 := []*WhenClause{when2}
	f = FunctionCase{mockExpr{val: 4, isStatic: true}, whens1, mockExpr{val: "else", isStatic: true}}
	c.Assert(f.IsStatic(), IsTrue)
}
