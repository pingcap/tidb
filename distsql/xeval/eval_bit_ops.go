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

func (e *Evaluator) evalBitOps(expr *tipb.Expr) (types.Datum, error) {
	if expr.GetTp() == tipb.ExprType_BitNeg {
		var result types.Datum
		if len(expr.Children) != 1 {
			err := ErrInvalid.Gen("need 2 operands but got %d", len(expr.Children))
			return result, err
		}
		operand, err := e.Eval(expr.Children[0])
		if err != nil {
			return types.Datum{}, errors.Trace(err)
		}
		a, err := types.CoerceArithmetic(operand)
		if err != nil {
			return result, errors.Trace(err)
		}
		return types.ComputeBitNeg(a)
	}
	var result types.Datum
	left, right, err := e.evalTwoChildren(expr)
	if err != nil {
		return result, errors.Trace(err)
	}
	return ComputeBit(expr.GetTp(), left, right)
}

// ComputeBit computes the logic operation on two datums.
func ComputeBit(op tipb.ExprType, left, right types.Datum) (types.Datum, error) {
	var result types.Datum
	a, err := types.CoerceArithmetic(left)
	if err != nil {
		return result, errors.Trace(err)
	}

	b, err := types.CoerceArithmetic(right)
	if err != nil {
		return result, errors.Trace(err)
	}
	a, b, err = types.CoerceDatum(a, b)
	if err != nil {
		return result, errors.Trace(err)
	}
	if a.IsNull() || b.IsNull() {
		return result, nil
	}

	switch op {
	case tipb.ExprType_BitAnd:
		return types.ComputeBitAnd(a, b)
	case tipb.ExprType_BitOr:
		return types.ComputeBitOr(a, b)
	case tipb.ExprType_BitXor:
		return types.ComputeBitXor(a, b)
	case tipb.ExprType_LeftShift:
		return types.ComputeLeftShift(a, b)
	case tipb.ExprType_RighShift:
		return types.ComputeRightShift(a, b)
	default:
		return result, errors.Errorf("Unknown binop type: %v", op)
	}
}
