// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ expression.Expression = (*PatternIn)(nil)
)

// PatternIn is the expression for in operator, like "expr in (1, 2, 3)" or "expr in (select c from t)".
type PatternIn struct {
	// Expr is the value expression to be compared.
	Expr expression.Expression
	// List is the list expression in compare list.
	List []expression.Expression
	// Not is true, the expression is "not in".
	Not bool
	// Sel is the select statement.
	Sel plan.Planner
}

// Clone implements the Expression Clone interface.
func (n *PatternIn) Clone() (expression.Expression, error) {
	expr, err := n.Expr.Clone()
	if err != nil {
		return nil, err
	}

	list, err := cloneExpressionList(n.List)
	if err != nil {
		return nil, err
	}

	return &PatternIn{
		Expr: expr,
		List: list,
		Not:  n.Not,
		Sel:  n.Sel,
	}, nil
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
		return fmt.Sprintf("%s NOT IN (%s)", n.Expr, n.Sel)
	}

	return fmt.Sprintf("%s IN (%s)", n.Expr, n.Sel)
}

func (n *PatternIn) evalInList(ctx context.Context, args map[interface{}]interface{},
	in interface{}, list []expression.Expression) (interface{}, error) {
	hasNull := false
	for _, v := range list {
		b := NewBinaryOperation(opcode.EQ, Value{in}, v)

		eVal, err := b.Eval(ctx, args)
		if err != nil {
			return nil, err
		}

		if eVal == nil {
			hasNull = true
			continue
		}

		r, err := types.ToBool(eVal)
		if err != nil {
			return nil, err
		}

		if r == 1 {
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

// Eval implements the Expression Eval interface.
func (n *PatternIn) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	lhs, err := n.Expr.Eval(ctx, args)
	if err != nil {
		return nil, err
	}

	if lhs == nil {
		return nil, nil
	}

	if n.Sel == nil {
		return n.evalInList(ctx, args, lhs, n.List)
	}

	var res []expression.Expression
	if ev, ok := args[n]; !ok {
		// select not yet evaluated
		r, err := n.Sel.Plan(ctx)
		if err != nil {
			return nil, err
		}

		if g, e := len(r.GetFields()), 1; g != e {
			return false, errors.Errorf("IN (%s): mismatched field count, have %d, need %d", n.Sel, g, e)
		}

		res = make([]expression.Expression, 0)
		// evaluate select and save its result for later in expression check
		err = r.Do(ctx, func(id interface{}, data []interface{}) (more bool, err error) {
			res = append(res, Value{data[0]})
			args[n] = res
			return true, nil
		})
		if err != nil {
			return nil, err
		}

		args[n] = res
	} else {
		res = ev.([]expression.Expression)
	}

	return n.evalInList(ctx, args, lhs, res)
}
