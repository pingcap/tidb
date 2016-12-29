// Copyright 2016 PingCAP, Inc.
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

package xeval

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

func (e *Evaluator) evalControlFuncs(expr *tipb.Expr) (d types.Datum, err error) {
	switch expr.GetTp() {
	case tipb.ExprType_Case:
		return e.evalCaseWhen(expr)
	case tipb.ExprType_If:
		return e.evalIf(expr)
	case tipb.ExprType_NullIf:
		return e.evalNullIf(expr)
	case tipb.ExprType_IfNull:
		return e.evalIfNull(expr)
	}
	return
}

func (e *Evaluator) evalCaseWhen(expr *tipb.Expr) (d types.Datum, err error) {
	l := len(expr.Children)
	for i := 0; i < l-1; i += 2 {
		child, err := e.Eval(expr.Children[i])
		if err != nil {
			return d, errors.Trace(err)
		}
		if child.IsNull() {
			continue
		}
		x, err := child.ToBool(e.sc)
		if err != nil {
			return d, errors.Trace(err)
		}
		if x == 1 {
			ans, err := e.Eval(expr.Children[i+1])
			if err != nil {
				return d, errors.Trace(err)
			}
			return ans, nil
		}
	}
	if l%2 == 1 { // Else statement
		ans, err := e.Eval(expr.Children[l-1])
		if err != nil {
			return d, errors.Trace(err)
		}
		return ans, nil
	}
	return d, nil
}

func (e *Evaluator) evalIf(expr *tipb.Expr) (d types.Datum, err error) {
	if len(expr.Children) != 3 {
		err = ErrInvalid.Gen("IF needs 3 operands but got %d", len(expr.Children))
		return
	}
	condTrue := false
	child1, err := e.Eval(expr.Children[0])
	if err != nil {
		return d, errors.Trace(err)
	}
	if !child1.IsNull() {
		x, err := child1.ToBool(e.sc)
		if err != nil {
			return d, errors.Trace(err)
		}
		if x == 1 {
			condTrue = true
		}
	}
	if condTrue {
		d, err = e.Eval(expr.Children[1])
	} else {
		d, err = e.Eval(expr.Children[2])
	}
	return d, errors.Trace(err)
}

func (e *Evaluator) evalNullIf(expr *tipb.Expr) (d types.Datum, err error) {
	left, right, err := e.evalTwoChildren(expr)
	if err != nil {
		return d, errors.Trace(err)
	}
	if left.IsNull() || right.IsNull() {
		return left, nil
	}
	x, err := left.CompareDatum(e.sc, right)
	if err != nil {
		return d, errors.Trace(err)
	}
	if x == 0 {
		return d, nil
	}
	return left, nil
}

func (e *Evaluator) evalIfNull(expr *tipb.Expr) (d types.Datum, err error) {
	left, right, err := e.evalTwoChildren(expr)
	if err != nil {
		return d, errors.Trace(err)
	}
	if left.IsNull() {
		return right, nil
	}
	return left, nil
}
