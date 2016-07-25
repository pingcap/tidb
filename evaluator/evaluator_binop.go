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

package evaluator

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

const (
	zeroI64 int64 = 0
	oneI64  int64 = 1
)

func (e *Evaluator) binaryOperation(o *ast.BinaryOperationExpr) bool {
	switch o.Op {
	case opcode.AndAnd, opcode.OrOr, opcode.LogicXor:
		return e.handleLogicOperation(o)
	case opcode.LT, opcode.LE, opcode.GE, opcode.GT, opcode.EQ, opcode.NE, opcode.NullEQ:
		return e.handleComparisonOp(o)
	case opcode.RightShift, opcode.LeftShift, opcode.And, opcode.Or, opcode.Xor:
		return e.handleBitOp(o)
	case opcode.Plus, opcode.Minus, opcode.Mod, opcode.Div, opcode.Mul, opcode.IntDiv:
		return e.handleArithmeticOp(o)
	default:
		e.err = ErrInvalidOperation
		return false
	}
}

func (e *Evaluator) handleLogicOperation(o *ast.BinaryOperationExpr) bool {
	switch o.Op {
	case opcode.AndAnd:
		return e.handleAndAnd(o)
	case opcode.OrOr:
		return e.handleOrOr(o)
	case opcode.LogicXor:
		return e.handleXor(o)
	default:
		e.err = ErrInvalidOperation.Gen("unkown operator %s", o.Op)
		return false
	}
}

func (e *Evaluator) handleAndAnd(o *ast.BinaryOperationExpr) bool {
	leftDatum := o.L.GetDatum()
	rightDatum := o.R.GetDatum()
	if !leftDatum.IsNull() {
		x, err := leftDatum.ToBool()
		if err != nil {
			e.err = errors.Trace(err)
			return false
		} else if x == 0 {
			// false && any other types is false
			o.SetInt64(x)
			return true
		}
	}
	if !rightDatum.IsNull() {
		y, err := rightDatum.ToBool()
		if err != nil {
			e.err = errors.Trace(err)
			return false
		} else if y == 0 {
			o.SetInt64(y)
			return true
		}
	}
	if leftDatum.IsNull() || rightDatum.IsNull() {
		o.SetNull()
		return true
	}
	o.SetInt64(int64(1))
	return true
}

func (e *Evaluator) handleOrOr(o *ast.BinaryOperationExpr) bool {
	leftDatum := o.L.GetDatum()
	if !leftDatum.IsNull() {
		x, err := leftDatum.ToBool()
		if err != nil {
			e.err = errors.Trace(err)
			return false
		} else if x == 1 {
			// true || any other types is true.
			o.SetInt64(x)
			return true
		}
	}
	righDatum := o.R.GetDatum()
	if !righDatum.IsNull() {
		y, err := righDatum.ToBool()
		if err != nil {
			e.err = errors.Trace(err)
			return false
		} else if y == 1 {
			o.SetInt64(y)
			return true
		}
	}
	if leftDatum.IsNull() || righDatum.IsNull() {
		o.SetNull()
		return true
	}
	o.SetInt64(int64(0))
	return true
}

func (e *Evaluator) handleXor(o *ast.BinaryOperationExpr) bool {
	leftDatum := o.L.GetDatum()
	righDatum := o.R.GetDatum()
	if leftDatum.IsNull() || righDatum.IsNull() {
		o.SetNull()
		return true
	}
	x, err := leftDatum.ToBool()
	if err != nil {
		e.err = errors.Trace(err)
		return false
	}

	y, err := righDatum.ToBool()
	if err != nil {
		e.err = errors.Trace(err)
		return false
	}
	if x == y {
		o.SetInt64(int64(0))
	} else {
		o.SetInt64(int64(1))
	}
	return true
}

func (e *Evaluator) handleComparisonOp(o *ast.BinaryOperationExpr) bool {
	a, b := types.CoerceDatum(*o.L.GetDatum(), *o.R.GetDatum())
	if a.IsNull() || b.IsNull() {
		// for <=>, if a and b are both nil, return true.
		// if a or b is nil, return false.
		if o.Op == opcode.NullEQ {
			if a.IsNull() && b.IsNull() {
				o.SetInt64(oneI64)
			} else {
				o.SetInt64(zeroI64)
			}
		} else {
			o.SetNull()
		}
		return true
	}

	n, err := a.CompareDatum(b)

	if err != nil {
		e.err = errors.Trace(err)
		return false
	}

	r, err := getCompResult(o.Op, n)
	if err != nil {
		e.err = errors.Trace(err)
		return false
	}
	if r {
		o.SetInt64(oneI64)
	} else {
		o.SetInt64(zeroI64)
	}
	return true
}

func getCompResult(op opcode.Op, value int) (bool, error) {
	switch op {
	case opcode.LT:
		return value < 0, nil
	case opcode.LE:
		return value <= 0, nil
	case opcode.GE:
		return value >= 0, nil
	case opcode.GT:
		return value > 0, nil
	case opcode.EQ:
		return value == 0, nil
	case opcode.NE:
		return value != 0, nil
	case opcode.NullEQ:
		return value == 0, nil
	default:
		return false, ErrInvalidOperation.Gen("invalid op %v in comparision operation", op)
	}
}

func (e *Evaluator) handleBitOp(o *ast.BinaryOperationExpr) bool {
	a, b := types.CoerceDatum(*o.L.GetDatum(), *o.R.GetDatum())

	if a.IsNull() || b.IsNull() {
		o.SetNull()
		return true
	}

	x, err := a.ToInt64()
	if err != nil {
		e.err = errors.Trace(err)
		return false
	}

	y, err := b.ToInt64()
	if err != nil {
		e.err = errors.Trace(err)
		return false
	}

	// use a int64 for bit operator, return uint64
	switch o.Op {
	case opcode.And:
		o.SetUint64(uint64(x & y))
	case opcode.Or:
		o.SetUint64(uint64(x | y))
	case opcode.Xor:
		o.SetUint64(uint64(x ^ y))
	case opcode.RightShift:
		o.SetUint64(uint64(x) >> uint64(y))
	case opcode.LeftShift:
		o.SetUint64(uint64(x) << uint64(y))
	default:
		e.err = ErrInvalidOperation.Gen("invalid op %v in bit operation", o.Op)
		return false
	}
	return true
}

func (e *Evaluator) handleArithmeticOp(o *ast.BinaryOperationExpr) bool {
	a, err := types.CoerceArithmetic(*o.L.GetDatum())
	if err != nil {
		e.err = errors.Trace(err)
		return false
	}

	b, err := types.CoerceArithmetic(*o.R.GetDatum())
	if err != nil {
		e.err = errors.Trace(err)
		return false
	}

	a, b = types.CoerceDatum(a, b)
	if a.IsNull() || b.IsNull() {
		o.SetNull()
		return true
	}

	var result types.Datum
	switch o.Op {
	case opcode.Plus:
		result, e.err = types.ComputePlus(a, b)
	case opcode.Minus:
		result, e.err = types.ComputeMinus(a, b)
	case opcode.Mul:
		result, e.err = types.ComputeMul(a, b)
	case opcode.Div:
		result, e.err = types.ComputeDiv(a, b)
	case opcode.Mod:
		result, e.err = types.ComputeMod(a, b)
	case opcode.IntDiv:
		result, e.err = types.ComputeIntDiv(a, b)
	default:
		e.err = ErrInvalidOperation.Gen("invalid op %v in arithmetic operation", o.Op)
		return false
	}
	o.SetDatum(result)
	return e.err == nil
}
