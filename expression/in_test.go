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

package expression_test

import (
	. "github.com/pingcap/check"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&testPatternInSuite{})

type testPatternInSuite struct {
}

func (t *testPatternInSuite) TestPatternIn(c *C) {
	e := &expression.PatternIn{
		Expr: expression.Value{Val: 1},
		List: []expression.Expression{expression.Value{Val: 1}, expression.Value{Val: 2}, expression.Value{Val: 3}},
	}

	ctx := mock.NewContext()
	v, err := e.Eval(ctx, nil)
	c.Assert(err, IsNil)
	c.Assert(v, IsTrue)

	c.Assert(e.IsStatic(), IsTrue)

	str := e.String()
	c.Assert(len(str), Greater, 0)

	ec := e.Clone()

	e2, ok := ec.(*expression.PatternIn)
	c.Assert(ok, IsTrue)

	vv, err := e2.Eval(ctx, nil)
	c.Assert(err, IsNil)
	c.Assert(vv, IsTrue)

	str = e2.String()
	c.Assert(len(str), Greater, 0)

	e2.List = []expression.Expression{&expression.Ident{CIStr: model.NewCIStr("c1")}, &expression.Ident{CIStr: model.NewCIStr("c2")}}

	c.Assert(e2.IsStatic(), IsFalse)

	e2.Expr = &expression.Ident{CIStr: model.NewCIStr("c1")}

	c.Assert(e2.IsStatic(), IsFalse)

	e2.Not = true

	str = e2.String()
	c.Assert(len(str), Greater, 0)

	e2.Expr = expression.Value{Val: 1}
	e2.List = []expression.Expression{expression.Value{}}

	vvv, err := e2.Eval(ctx, nil)
	c.Assert(err, IsNil)
	c.Assert(vvv, IsNil)

	e2.List = nil

	vv, err = e2.Eval(ctx, nil)
	c.Assert(err, IsNil)
	c.Assert(vv, IsTrue)

	e2.Expr = expression.Value{}

	vvv, err = e2.Eval(ctx, nil)
	c.Assert(err, IsNil)
	c.Assert(vvv, IsNil)

	sel := newMockSubQuery([][]interface{}{{1, 2}}, []string{"id", "name"})
	e2.Sel = sel

	str = e2.String()
	c.Assert(len(str), Greater, 0)

	e2.Not = false

	str = e2.String()
	c.Assert(len(str), Greater, 0)

	e2.Expr = expression.Value{Val: 1}
	args := make(map[interface{}]interface{})

	_, err = e2.Eval(ctx, args)
	c.Assert(err, NotNil)

	e2.Sel = newMockSubQuery([][]interface{}{{1}, {2}}, []string{"id"})

	vv, err = e2.Eval(ctx, args)
	c.Assert(err, IsNil)
	c.Assert(vv, IsTrue)

	e2.Sel.SetValue([]interface{}{1, 2})

	vv, err = e2.Eval(ctx, args)
	c.Assert(err, IsNil)
	c.Assert(vv, IsTrue)

	e2.Sel.SetValue(nil)
	e2.Expr = expression.NewTestRow(1, 2)
	e2.Sel = newMockSubQuery([][]interface{}{{1, 2}}, []string{"id", "name"})
	_, err = e2.Eval(ctx, args)
	c.Assert(err, IsNil)

	e2.Expr = expression.NewTestRow(1, 2, 3)

	_, err = e2.Eval(ctx, args)
	c.Assert(err, NotNil)

	e2.Sel.SetValue(nil)
	e2.Sel = nil
	e2.List = []expression.Expression{expression.NewTestRow(1, 2, 3)}
	_, err = e2.Eval(ctx, args)
	c.Assert(err, IsNil)

	e2.List = []expression.Expression{expression.Value{Val: 1}}
	_, err = e2.Eval(ctx, args)
	c.Assert(err, NotNil)
}
