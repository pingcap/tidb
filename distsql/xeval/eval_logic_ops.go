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

// evalLogicOps computes LogicAnd, LogicOr, LogicXor results of two operands.
func (e *Evaluator) evalLogicOps(expr *tipb.Expr) (types.Datum, error) {
	if expr.GetTp() == tipb.ExprType_Not {
		return e.evalNot(expr)
	}
	left, right, err := e.getTwoChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	switch op := expr.GetTp(); op {
	case tipb.ExprType_And:
		return e.evalAnd(left, right)
	case tipb.ExprType_Or:
		return e.evalOr(left, right)
	case tipb.ExprType_Xor:
		return e.evalXor(left, right)
	default:
		return types.Datum{}, errors.Errorf("Unknown binop type: %v", op)
	}
}

// evalBool evaluates expr as bool value.
func (e *Evaluator) evalBool(expr *tipb.Expr) (int64, error) {
	v, err := e.Eval(expr)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if v.IsNull() {
		return compareResultNull, nil
	}

	return v.ToBool(e.sc)
}

// evalAnd computes result of (X && Y). It works in a short-cut way.
func (e *Evaluator) evalAnd(left, right *tipb.Expr) (types.Datum, error) {
	var d types.Datum

	leftBool, err := e.evalBool(left)
	if err != nil {
		return d, errors.Trace(err)
	}
	if leftBool == 0 {
		d.SetInt64(0)
		return d, nil
	}

	rightBool, err := e.evalBool(right)
	if err != nil {
		return d, errors.Trace(err)
	}
	if rightBool == 0 {
		d.SetInt64(0)
		return d, nil
	}

	if leftBool == compareResultNull || rightBool == compareResultNull {
		return d, nil
	}

	d.SetInt64(1)
	return d, nil
}

// evalOr computes result of (X || Y). It works in a short-cut way.
func (e *Evaluator) evalOr(left, right *tipb.Expr) (types.Datum, error) {
	var d types.Datum
	leftBool, err := e.evalBool(left)
	if err != nil {
		return d, errors.Trace(err)
	}
	if leftBool == 1 {
		d.SetInt64(1)
		return d, nil
	}
	rightBool, err := e.evalBool(right)
	if err != nil {
		return d, errors.Trace(err)
	}
	if rightBool == 1 {
		d.SetInt64(1)
		return d, nil
	}

	if leftBool == compareResultNull || rightBool == compareResultNull {
		return d, nil
	}

	d.SetInt64(0)
	return d, nil
}

// evalXor computes result of (X XOR Y). It works in a short-cut way.
func (e *Evaluator) evalXor(left, right *tipb.Expr) (types.Datum, error) {
	var d types.Datum
	leftBool, err := e.evalBool(left)
	if err != nil {
		return d, errors.Trace(err)
	}
	if leftBool == compareResultNull {
		return d, nil
	}
	rightBool, err := e.evalBool(left)
	if err != nil {
		return d, errors.Trace(err)
	}
	if rightBool == compareResultNull {
		return d, nil
	}
	if leftBool == rightBool {
		d.SetInt64(0)
		return d, nil
	}
	d.SetInt64(1)
	return d, nil
}

// evalNot computes result of (!X).
func (e *Evaluator) evalNot(expr *tipb.Expr) (types.Datum, error) {
	if len(expr.Children) != 1 {
		return types.Datum{}, ErrInvalid.Gen("NOT need 1 operand, got %d", len(expr.Children))
	}
	d, err := e.Eval(expr.Children[0])
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if d.IsNull() {
		return d, nil
	}
	boolVal, err := d.ToBool(e.sc)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if boolVal == 1 {
		return types.NewIntDatum(0), nil
	}
	return types.NewIntDatum(1), nil
}
