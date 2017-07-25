// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestAndAnd(c *C) {
	defer testleak.AfterTest(c)()

	sc := s.ctx.GetSessionVars().StmtCtx
	origin := sc.IgnoreTruncate
	defer func() {
		sc.IgnoreTruncate = origin
	}()
	sc.IgnoreTruncate = true

	cases := []struct {
		args     []interface{}
		expected int64
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{1, 1}, 1, false, false},
		{[]interface{}{1, 0}, 0, false, false},
		{[]interface{}{0, 1}, 0, false, false},
		{[]interface{}{0, 0}, 0, false, false},
		{[]interface{}{2, -1}, 1, false, false},
		{[]interface{}{"a", "1"}, 0, false, false},
		{[]interface{}{"1a", "1"}, 1, false, false},
		{[]interface{}{0, nil}, 0, false, false},
		{[]interface{}{nil, 0}, 0, false, false},
		{[]interface{}{nil, 1}, 0, true, false},

		{[]interface{}{errors.New("must error"), 1}, 0, false, true},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.LogicAnd, primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		d, err := f.Eval(nil)
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetInt64(), Equals, t.expected)
			}
		}
	}

	// Test incorrect parameter count.
	_, err := newFunctionForTest(s.ctx, ast.LogicAnd, Zero)
	c.Assert(err, NotNil)

	f, err := funcs[ast.LogicAnd].getFunction([]Expression{Zero, Zero}, s.ctx)
	c.Assert(err, IsNil)
	c.Assert(f.isDeterministic(), IsTrue)
}
