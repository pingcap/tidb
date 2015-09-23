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
	"errors"

	. "github.com/pingcap/check"

	"github.com/pingcap/tidb/expression/builtin"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testBetweenSuite{})

type testBetweenSuite struct {
}

func (s *testBetweenSuite) TestBetween(c *C) {
	table := []struct {
		Expr   Expression
		Left   int
		Right  int
		Not    bool
		Result int64 // 0 for false, 1 for true
	}{
		{Value{1}, 2, 3, false, 0},
		{Value{1}, 2, 3, true, 1},
		{&Call{
			F:    "count",
			Args: []Expression{Value{1}},
		}, 2, 3, false, 0},
		{&Call{
			F:    "count",
			Args: []Expression{Value{1}},
		}, 2, 3, true, 1},
	}

	m := map[interface{}]interface{}{
		builtin.ExprEvalArgAggEmpty: struct{}{},
	}

	for _, t := range table {
		b, err := NewBetween(t.Expr, Value{t.Left}, Value{t.Right}, t.Not)
		c.Assert(err, IsNil)
		c.Assert(len(b.String()), Greater, 0)
		c.Assert(b.Clone(), NotNil)
		c.Assert(b.IsStatic(), Equals, !ContainAggregateFunc(t.Expr))

		v, err := b.Eval(nil, m)

		val, err := types.ToBool(v)
		c.Assert(err, IsNil)
		c.Assert(val, Equals, t.Result)
	}

	f := func(isErr bool) Expression {
		if isErr {
			return mockExpr{isStatic: true, err: errors.New("test error")}
		}
		return mockExpr{isStatic: true, err: nil}
	}

	tableError := []struct {
		Expr  Expression
		Left  Expression
		Right Expression
	}{
		{f(true), f(false), f(false)},
		{f(false), f(true), f(false)},
		{f(false), f(false), f(true)},
	}

	for _, t := range tableError {
		_, err := NewBetween(t.Expr, t.Left, t.Right, false)
		c.Assert(err, NotNil)

		b := Between{Expr: t.Expr, Left: t.Left, Right: t.Right, Not: false}
		c.Assert(b.Clone(), NotNil)
		_, err = b.Eval(nil, nil)
		c.Assert(err, NotNil)
	}

	tableError = []struct {
		Expr  Expression
		Left  Expression
		Right Expression
	}{
		{NewTestRow(1, 2), f(false), f(false)},
		{f(false), NewTestRow(1, 2), f(false)},
		{f(false), f(false), NewTestRow(1, 2)},
	}

	for _, t := range tableError {
		b := Between{Expr: t.Expr, Left: t.Left, Right: t.Right, Not: false}

		_, err := b.Eval(nil, nil)
		c.Assert(err, NotNil)
	}
}
