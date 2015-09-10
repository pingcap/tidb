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
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
)

var _ = Suite(&testPatternInSuite{})

type testPatternInSuite struct {
}

func (t *testPatternInSuite) TestPatternIn(c *C) {
	e := &PatternIn{
		Expr: Value{1},
		List: []expression.Expression{Value{1}, Value{2}, Value{3}},
	}

	v, err := e.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsTrue)

	c.Assert(e.IsStatic(), IsTrue)

	str := e.String()
	c.Assert(len(str), Greater, 0)

	ec, err := e.Clone()
	c.Assert(err, IsNil)

	e2, ok := ec.(*PatternIn)
	c.Assert(ok, IsTrue)

	vv, err := e2.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(vv, IsTrue)

	str = e2.String()
	c.Assert(len(str), Greater, 0)

	e2.List = []expression.Expression{&Ident{model.NewCIStr("c1")}, &Ident{model.NewCIStr("c2")}}

	c.Assert(e2.IsStatic(), IsFalse)

	e2.Expr = &Ident{model.NewCIStr("c1")}

	c.Assert(e2.IsStatic(), IsFalse)

	e2.Not = true

	str = e2.String()
	c.Assert(len(str), Greater, 0)

	e2.Expr = Value{1}
	e2.List = []expression.Expression{Value{}}

	vvv, err := e2.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(vvv, IsNil)

	e2.List = nil

	vv, err = e2.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(vv, IsTrue)

	e2.Expr = Value{}

	vvv, err = e2.Eval(nil, nil)
	c.Assert(err, IsNil)
	c.Assert(vvv, IsNil)

	sel := newMockStatement()
	e2.Sel = sel

	str = e2.String()
	c.Assert(len(str), Greater, 0)

	e2.Not = false

	str = e2.String()
	c.Assert(len(str), Greater, 0)

	e2.Expr = Value{1}
	args := make(map[interface{}]interface{})

	_, err = e2.Eval(nil, args)
	c.Assert(err, NotNil)

	sel.SetFieldOffset(1)
	e2.Sel = sel

	vv, err = e2.Eval(nil, args)
	c.Assert(err, IsNil)
	c.Assert(vv, IsTrue)

	args[e2] = []interface{}{1, 2}

	vv, err = e2.Eval(nil, args)
	c.Assert(err, IsNil)
	c.Assert(vv, IsTrue)

	delete(args, e2)
	e2.Expr = newTestRow(1, 2)
	sel.SetFieldOffset(2)
	_, err = e2.Eval(nil, args)
	c.Assert(err, IsNil)

	e2.Expr = newTestRow(1, 2, 3)
	sel.SetFieldOffset(2)
	_, err = e2.Eval(nil, args)
	c.Assert(err, NotNil)

	delete(args, e2)
	e2.Sel = nil
	e2.List = []expression.Expression{newTestRow(1, 2, 3)}
	_, err = e2.Eval(nil, args)
	c.Assert(err, IsNil)

	e2.List = []expression.Expression{Value{1}}
	_, err = e2.Eval(nil, args)
	c.Assert(err, NotNil)
}
