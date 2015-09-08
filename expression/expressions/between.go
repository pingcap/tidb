// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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
	"fmt"

	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/opcode"
)

var _ expression.Expression = (*Between)(nil)

// Between is for "between and" or "not between and" expression.
type Between struct {
	// Expr is the expression to be checked.
	Expr expression.Expression
	// Left is the expression for minimal value in the range.
	Left expression.Expression
	// Right is the expression for maximum value in the range.
	Right expression.Expression
	// Not is true, the expression is "not between and".
	Not bool
}

// Clone implements the Expression Clone interface.
func (b *Between) Clone() (expression.Expression, error) {
	expr, err := b.Expr.Clone()
	if err != nil {
		return nil, err
	}

	left, err := b.Left.Clone()
	if err != nil {
		return nil, err
	}

	right, err := b.Right.Clone()
	if err != nil {
		return nil, err
	}

	return &Between{Expr: expr, Left: left, Right: right, Not: b.Not}, nil
}

// IsStatic implements the Expression IsStatic interface.
func (b *Between) IsStatic() bool {
	return b.Expr.IsStatic() && b.Left.IsStatic() && b.Right.IsStatic()
}

// String implements the Expression String interface.
func (b *Between) String() string {
	not := ""
	if b.Not {
		not = "NOT "
	}
	return fmt.Sprintf("%s %sBETWEEN %s AND %s", b.Expr, not, b.Left, b.Right)
}

// Eval implements the Expression Eval interface.
func (b *Between) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	v, err := b.Expr.Eval(ctx, args)
	if err != nil {
		return nil, err
	}

	lv, err := b.Left.Eval(ctx, args)
	if err != nil {
		return nil, err
	}

	rv, err := b.Right.Eval(ctx, args)
	if err != nil {
		return nil, err
	}

	var l, r expression.Expression
	op := opcode.AndAnd

	if b.Not {
		// v < lv || v > rv
		op = opcode.OrOr
		l = NewBinaryOperation(opcode.LT, Value{v}, Value{lv})
		r = NewBinaryOperation(opcode.GT, Value{v}, Value{rv})
	} else {
		// v >= lv && v <= rv
		l = NewBinaryOperation(opcode.GE, Value{v}, Value{lv})
		r = NewBinaryOperation(opcode.LE, Value{v}, Value{rv})
	}

	ret := NewBinaryOperation(op, l, r)

	return ret.Eval(ctx, args)
}

// convert Between to binary operation, if cannot convert, return itself directly
// sometimes we can convert between expression to binary operation directly
// like "a between l and r" -> "a >= l && a <= r"
// but this conversion is not always correct if the Expr has an aggregate function,
// e.g, "sum(10) between 10 and 15", if we convert this between to binary operation,
// we will get (sum(10) >= 10 && sum(10) <= 15), the sum(10) is the same Call expression,
// so here we will evaluate sum(10) twice, and get the wrong result.
func (b *Between) convert() expression.Expression {
	if ContainAggregateFunc(b.Expr) {
		return b
	}

	var (
		l, r expression.Expression
	)

	op := opcode.AndAnd
	if b.Not {
		// v < lv || v > rv
		op = opcode.OrOr
		l = NewBinaryOperation(opcode.LT, b.Expr, b.Left)
		r = NewBinaryOperation(opcode.GT, b.Expr, b.Right)
	} else {
		// v >= lv && v <= rv
		l = NewBinaryOperation(opcode.GE, b.Expr, b.Left)
		r = NewBinaryOperation(opcode.LE, b.Expr, b.Right)
	}

	ret := NewBinaryOperation(op, l, r)

	return ret
}

// NewBetween creates a Between expression for "a between c and d" or "a not between c and d".
// If a doesn't contain any aggregate function, we can split it with logic and comparison expressions.
// For example:
//	a between 10 and 15 -> a >= 10 && a <= 15
//	a not between 10 and 15 -> a < 10 || b > 15
func NewBetween(expr, lo, hi expression.Expression, not bool) (expression.Expression, error) {
	e, err := staticExpr(expr)
	if err != nil {
		return nil, err
	}

	l, err := staticExpr(lo)
	if err != nil {
		return nil, err
	}

	h, err := staticExpr(hi)
	if err != nil {
		return nil, err
	}

	b := &Between{Expr: e, Left: l, Right: h, Not: not}

	ret := b.convert()

	return staticExpr(ret)
}
