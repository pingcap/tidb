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
	leftBool, rightBool, err := e.evalTwoBoolChildren(expr)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	switch op := expr.GetTp(); op {
	case tipb.ExprType_And:
		return e.evalAnd(leftBool, rightBool)
	case tipb.ExprType_Or:
		return e.evalOr(leftBool, rightBool)
	default:
		return types.Datum{}, errors.Errorf("Unknown binop type: %v", op)
	}
}

// evalAnd computes result of (X && Y).
func (e *Evaluator) evalAnd(leftBool, rightBool int64) (types.Datum, error) {
	var d types.Datum
	if leftBool == 0 || rightBool == 0 {
		d.SetInt64(0)
		return d, nil
	}
	if leftBool == compareResultNull || rightBool == compareResultNull {
		d.SetNull()
		return d, nil
	}
	d.SetInt64(1)
	return d, nil
}

// evalOr computes result of (X || Y).
func (e *Evaluator) evalOr(leftBool, rightBool int64) (types.Datum, error) {
	var d types.Datum
	if leftBool == 1 || rightBool == 1 {
		d.SetInt64(1)
		return d, nil
	}
	if leftBool == compareResultNull || rightBool == compareResultNull {
		d.SetNull()
		return d, nil
	}
	d.SetInt64(0)
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
	boolVal, err := d.ToBool()
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if boolVal == 1 {
		return types.NewIntDatum(0), nil
	}
	return types.NewIntDatum(1), nil
}
