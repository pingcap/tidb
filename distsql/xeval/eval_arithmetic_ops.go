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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

func (e *Evaluator) evalArithmeticOps(expr *tipb.Expr) (types.Datum, error) {
	var result types.Datum
	left, right, err := e.evalTwoChildren(expr)
	if err != nil {
		return result, errors.Trace(err)
	}
	return ComputeArithmetic(e.sc, expr.GetTp(), left, right)
}

// ComputeArithmetic computes the arithmetic operation on two datums.
func ComputeArithmetic(sc *variable.StatementContext, op tipb.ExprType, left types.Datum, right types.Datum) (types.Datum, error) {
	var result types.Datum
	a, err := types.CoerceArithmetic(sc, left)
	if err != nil {
		return result, errors.Trace(err)
	}

	b, err := types.CoerceArithmetic(sc, right)
	if err != nil {
		return result, errors.Trace(err)
	}
	a, b, err = types.CoerceDatum(sc, a, b)
	if err != nil {
		return result, errors.Trace(err)
	}
	if a.IsNull() || b.IsNull() {
		return result, nil
	}

	switch op {
	case tipb.ExprType_Plus:
		return types.ComputePlus(a, b)
	case tipb.ExprType_Div:
		return types.ComputeDiv(sc, a, b)
	case tipb.ExprType_Minus:
		return types.ComputeMinus(a, b)
	case tipb.ExprType_Mul:
		return types.ComputeMul(a, b)
	case tipb.ExprType_IntDiv:
		return types.ComputeIntDiv(sc, a, b)
	case tipb.ExprType_Mod:
		return types.ComputeMod(sc, a, b)
	default:
		return result, errors.Errorf("Unknown binop type: %v", op)
	}
}
