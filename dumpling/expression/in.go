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

package expression

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"

	"github.com/pingcap/tidb/util/types"
)

var (
	_ Expression = (*PatternIn)(nil)
)

// PatternIn is the expression for in operator, like "expr in (1, 2, 3)" or "expr in (select c from t)".
type PatternIn struct {
	// Expr is the value expression to be compared.
	Expr Expression
	// List is the list expression in compare list.
	List []Expression
	// Not is true, the expression is "not in".
	Not bool
	// Sel is the sub query.
	Sel SubQuery
}

// Clone implements the Expression Clone interface.
func (n *PatternIn) Clone() Expression {
	expr := n.Expr.Clone()
	list := cloneExpressionList(n.List)
	return &PatternIn{
		Expr: expr,
		List: list,
		Not:  n.Not,
		Sel:  n.Sel,
	}
}

// IsStatic implements the Expression IsStatic interface.
func (n *PatternIn) IsStatic() bool {
	if !n.Expr.IsStatic() || n.Sel != nil {
		return false
	}

	for _, v := range n.List {
		if !v.IsStatic() {
			return false
		}
	}
	return true
}

// String implements the Expression String interface.
func (n *PatternIn) String() string {
	if n.Sel == nil {
		a := []string{}
		for _, v := range n.List {
			a = append(a, v.String())
		}
		if n.Not {
			return fmt.Sprintf("%s NOT IN (%s)", n.Expr, strings.Join(a, ","))
		}

		return fmt.Sprintf("%s IN (%s)", n.Expr, strings.Join(a, ","))
	}

	if n.Not {
		return fmt.Sprintf("%s NOT IN %s", n.Expr, n.Sel)
	}

	return fmt.Sprintf("%s IN %s", n.Expr, n.Sel)
}

func (n *PatternIn) checkInList(in interface{}, list []interface{}) (interface{}, error) {
	hasNull := false
	for _, v := range list {
		if types.IsNil(v) {
			hasNull = true
			continue
		}

		r, err := types.Compare(in, v)
		if err != nil {
			return nil, err
		}

		if r == 0 {
			return !n.Not, nil
		}
	}

	if hasNull {
		// if no matched but we got null in In, return null
		// e.g 1 in (null, 2, 3) returns null
		return nil, nil
	}

	return n.Not, nil
}

func evalExprList(ctx context.Context, args map[interface{}]interface{}, list []Expression) ([]interface{}, error) {
	var err error
	values := make([]interface{}, len(list))
	for i := range values {
		values[i], err = list[i].Eval(ctx, args)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return values, nil
}

// Eval implements the Expression Eval interface.
func (n *PatternIn) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	lhs, err := n.Expr.Eval(ctx, args)
	if err != nil {
		return nil, err
	}

	if types.IsNil(lhs) {
		return nil, nil
	}

	if n.Sel == nil {
		err1 := hasSameColumnCount(ctx, n.Expr, n.List...)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}

		var values []interface{}
		values, err1 = evalExprList(ctx, args, n.List)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}

		return n.checkInList(lhs, values)
	}

	if !n.Sel.UseOuterQuery() && n.Sel.Value() != nil {
		return n.checkInList(lhs, n.Sel.Value().([]interface{}))
	}

	var res []interface{}
	if err = hasSameColumnCount(ctx, n.Expr, n.Sel); err != nil {
		return nil, errors.Trace(err)
	}

	res, err = n.Sel.EvalRows(ctx, args, -1)
	if err != nil {
		return nil, errors.Trace(err)
	}

	n.Sel.SetValue(res)

	return n.checkInList(lhs, res)
}

// Accept implements Expression Accept interface.
func (n *PatternIn) Accept(v Visitor) (Expression, error) {
	return v.VisitPatternIn(n)
}
