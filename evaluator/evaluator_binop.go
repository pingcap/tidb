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
	"math"

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
	if leftDatum.Kind() != types.KindNull {
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
	if rightDatum.Kind() != types.KindNull {
		y, err := rightDatum.ToBool()
		if err != nil {
			e.err = errors.Trace(err)
			return false
		} else if y == 0 {
			o.SetInt64(y)
			return true
		}
	}
	if leftDatum.Kind() == types.KindNull || rightDatum.Kind() == types.KindNull {
		o.SetNull()
		return true
	}
	o.SetInt64(int64(1))
	return true
}

func (e *Evaluator) handleOrOr(o *ast.BinaryOperationExpr) bool {
	leftDatum := o.L.GetDatum()
	righDatum := o.R.GetDatum()
	if leftDatum.Kind() != types.KindNull {
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
	if righDatum.Kind() != types.KindNull {
		y, err := righDatum.ToBool()
		if err != nil {
			e.err = errors.Trace(err)
			return false
		} else if y == 1 {
			o.SetInt64(y)
			return true
		}
	}
	if leftDatum.Kind() == types.KindNull || righDatum.Kind() == types.KindNull {
		o.SetNull()
		return true
	}
	o.SetInt64(int64(0))
	return true
}

func (e *Evaluator) handleXor(o *ast.BinaryOperationExpr) bool {
	leftDatum := o.L.GetDatum()
	righDatum := o.R.GetDatum()
	if leftDatum.Kind() == types.KindNull || righDatum.Kind() == types.KindNull {
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
	if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
		// for <=>, if a and b are both nil, return true.
		// if a or b is nil, return false.
		if o.Op == opcode.NullEQ {
			if a.Kind() == types.KindNull && b.Kind() == types.KindNull {
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

	if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
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
	a, err := coerceArithmetic(*o.L.GetDatum())
	if err != nil {
		e.err = errors.Trace(err)
		return false
	}

	b, err := coerceArithmetic(*o.R.GetDatum())
	if err != nil {
		e.err = errors.Trace(err)
		return false
	}

	a, b = types.CoerceDatum(a, b)
	if a.Kind() == types.KindNull || b.Kind() == types.KindNull {
		o.SetNull()
		return true
	}

	var result types.Datum
	switch o.Op {
	case opcode.Plus:
		result, e.err = computePlus(a, b)
	case opcode.Minus:
		result, e.err = computeMinus(a, b)
	case opcode.Mul:
		result, e.err = computeMul(a, b)
	case opcode.Div:
		result, e.err = computeDiv(a, b)
	case opcode.Mod:
		result, e.err = computeMod(a, b)
	case opcode.IntDiv:
		result, e.err = computeIntDiv(a, b)
	default:
		e.err = ErrInvalidOperation.Gen("invalid op %v in arithmetic operation", o.Op)
		return false
	}
	o.SetDatum(result)
	return e.err == nil
}

func computePlus(a, b types.Datum) (d types.Datum, err error) {
	switch a.Kind() {
	case types.KindInt64:
		switch b.Kind() {
		case types.KindInt64:
			r, err1 := types.AddInt64(a.GetInt64(), b.GetInt64())
			d.SetInt64(r)
			return d, errors.Trace(err1)
		case types.KindUint64:
			r, err1 := types.AddInteger(b.GetUint64(), a.GetInt64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case types.KindUint64:
		switch b.Kind() {
		case types.KindInt64:
			r, err1 := types.AddInteger(a.GetUint64(), b.GetInt64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		case types.KindUint64:
			r, err1 := types.AddUint64(a.GetUint64(), b.GetUint64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case types.KindFloat64:
		switch b.Kind() {
		case types.KindFloat64:
			r := a.GetFloat64() + b.GetFloat64()
			d.SetFloat64(r)
			return d, nil
		}
	case types.KindMysqlDecimal:
		switch b.Kind() {
		case types.KindMysqlDecimal:
			r := a.GetMysqlDecimal().Add(b.GetMysqlDecimal())
			d.SetMysqlDecimal(r)
			return d, nil
		}
	}
	_, err = types.InvOp2(a.GetValue(), b.GetValue(), opcode.Plus)
	return d, err
}

func computeMinus(a, b types.Datum) (d types.Datum, err error) {
	switch a.Kind() {
	case types.KindInt64:
		switch b.Kind() {
		case types.KindInt64:
			r, err1 := types.SubInt64(a.GetInt64(), b.GetInt64())
			d.SetInt64(r)
			return d, errors.Trace(err1)
		case types.KindUint64:
			r, err1 := types.SubIntWithUint(a.GetInt64(), b.GetUint64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case types.KindUint64:
		switch b.Kind() {
		case types.KindInt64:
			r, err1 := types.SubUintWithInt(a.GetUint64(), b.GetInt64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		case types.KindUint64:
			r, err1 := types.SubUint64(a.GetUint64(), b.GetUint64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case types.KindFloat64:
		switch b.Kind() {
		case types.KindFloat64:
			r := a.GetFloat64() - b.GetFloat64()
			d.SetFloat64(r)
			return d, nil
		}
	case types.KindMysqlDecimal:
		switch b.Kind() {
		case types.KindMysqlDecimal:
			r := a.GetMysqlDecimal().Sub(b.GetMysqlDecimal())
			d.SetMysqlDecimal(r)
			return d, nil
		}
	}
	_, err = types.InvOp2(a.GetValue(), b.GetValue(), opcode.Minus)
	return d, errors.Trace(err)
}

func computeMul(a, b types.Datum) (d types.Datum, err error) {
	switch a.Kind() {
	case types.KindInt64:
		switch b.Kind() {
		case types.KindInt64:
			r, err1 := types.MulInt64(a.GetInt64(), b.GetInt64())
			d.SetInt64(r)
			return d, errors.Trace(err1)
		case types.KindUint64:
			r, err1 := types.MulInteger(b.GetUint64(), a.GetInt64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case types.KindUint64:
		switch b.Kind() {
		case types.KindInt64:
			r, err1 := types.MulInteger(a.GetUint64(), b.GetInt64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		case types.KindUint64:
			r, err1 := types.MulUint64(a.GetUint64(), b.GetUint64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case types.KindFloat64:
		switch b.Kind() {
		case types.KindFloat64:
			r := a.GetFloat64() * b.GetFloat64()
			d.SetFloat64(r)
			return d, nil
		}
	case types.KindMysqlDecimal:
		switch b.Kind() {
		case types.KindMysqlDecimal:
			r := a.GetMysqlDecimal().Mul(b.GetMysqlDecimal())
			d.SetMysqlDecimal(r)
			return d, nil
		}
	}

	_, err = types.InvOp2(a.GetValue(), b.GetValue(), opcode.Mul)
	return d, errors.Trace(err)
}

func computeDiv(a, b types.Datum) (d types.Datum, err error) {
	// MySQL support integer divison Div and division operator /
	// we use opcode.Div for division operator and will use another for integer division later.
	// for division operator, we will use float64 for calculation.
	switch a.Kind() {
	case types.KindFloat64:
		y, err1 := b.ToFloat64()
		if err1 != nil {
			return d, errors.Trace(err1)
		}

		if y == 0 {
			return d, nil
		}

		x := a.GetFloat64()
		d.SetFloat64(x / y)
		return d, nil
	default:
		// the scale of the result is the scale of the first operand plus
		// the value of the div_precision_increment system variable (which is 4 by default)
		// we will use 4 here
		xa, err1 := a.ToDecimal()
		if err != nil {
			return d, errors.Trace(err1)
		}

		xb, err1 := b.ToDecimal()
		if err1 != nil {
			return d, errors.Trace(err1)
		}
		if f, _ := xb.Float64(); f == 0 {
			// division by zero return null
			return d, nil
		}

		d.SetMysqlDecimal(xa.Div(xb))
		return d, nil
	}
}

func computeMod(a, b types.Datum) (d types.Datum, err error) {
	switch a.Kind() {
	case types.KindInt64:
		x := a.GetInt64()
		switch b.Kind() {
		case types.KindInt64:
			y := b.GetInt64()
			if y == 0 {
				return d, nil
			}
			d.SetInt64(x % y)
			return d, nil
		case types.KindUint64:
			y := b.GetUint64()
			if y == 0 {
				return d, nil
			} else if x < 0 {
				d.SetInt64(-int64(uint64(-x) % y))
				// first is int64, return int64.
				return d, nil
			}
			d.SetInt64(int64(uint64(x) % y))
			return d, nil
		}
	case types.KindUint64:
		x := a.GetUint64()
		switch b.Kind() {
		case types.KindInt64:
			y := b.GetInt64()
			if y == 0 {
				return d, nil
			} else if y < 0 {
				// first is uint64, return uint64.
				d.SetUint64(uint64(x % uint64(-y)))
				return d, nil
			}
			d.SetUint64(x % uint64(y))
			return d, nil
		case types.KindUint64:
			y := b.GetUint64()
			if y == 0 {
				return d, nil
			}
			d.SetUint64(x % y)
			return d, nil
		}
	case types.KindFloat64:
		x := a.GetFloat64()
		switch b.Kind() {
		case types.KindFloat64:
			y := b.GetFloat64()
			if y == 0 {
				return d, nil
			}
			d.SetFloat64(math.Mod(x, y))
			return d, nil
		}
	case types.KindMysqlDecimal:
		x := a.GetMysqlDecimal()
		switch b.Kind() {
		case types.KindMysqlDecimal:
			y := b.GetMysqlDecimal()
			xf, _ := x.Float64()
			yf, _ := y.Float64()
			if yf == 0 {
				return d, nil
			}
			d.SetFloat64(math.Mod(xf, yf))
			return d, nil
		}
	}
	_, err = types.InvOp2(a.GetValue(), b.GetValue(), opcode.Mod)
	return d, errors.Trace(err)
}

func computeIntDiv(a, b types.Datum) (d types.Datum, err error) {
	switch a.Kind() {
	case types.KindInt64:
		x := a.GetInt64()
		switch b.Kind() {
		case types.KindInt64:
			y := b.GetInt64()
			if y == 0 {
				return d, nil
			}
			r, err1 := types.DivInt64(x, y)
			d.SetInt64(r)
			return d, errors.Trace(err1)
		case types.KindUint64:
			y := b.GetUint64()
			if y == 0 {
				return d, nil
			}
			r, err1 := types.DivIntWithUint(x, y)
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case types.KindUint64:
		x := a.GetUint64()
		switch b.Kind() {
		case types.KindInt64:
			y := b.GetInt64()
			if y == 0 {
				return d, nil
			}
			r, err1 := types.DivUintWithInt(x, y)
			d.SetUint64(r)
			return d, errors.Trace(err1)
		case types.KindUint64:
			y := b.GetUint64()
			if y == 0 {
				return d, nil
			}
			d.SetUint64(x / y)
			return d, nil
		}
	}

	// if any is none integer, use decimal to calculate
	x, err := a.ToDecimal()
	if err != nil {
		return d, errors.Trace(err)
	}

	y, err := b.ToDecimal()
	if err != nil {
		return d, errors.Trace(err)
	}

	if f, _ := y.Float64(); f == 0 {
		return d, nil
	}

	d.SetInt64(x.Div(y).IntPart())
	return d, nil
}

func coerceArithmetic(a types.Datum) (d types.Datum, err error) {
	switch a.Kind() {
	case types.KindString, types.KindBytes:
		// MySQL will convert string to float for arithmetic operation
		f, err := types.StrToFloat(a.GetString())
		if err != nil {
			return d, errors.Trace(err)
		}
		d.SetFloat64(f)
		return d, errors.Trace(err)
	case types.KindMysqlTime:
		// if time has no precision, return int64
		t := a.GetMysqlTime()
		de := t.ToNumber()
		if t.Fsp == 0 {
			d.SetInt64(de.IntPart())
			return d, nil
		}
		d.SetMysqlDecimal(de)
		return d, nil
	case types.KindMysqlDuration:
		// if duration has no precision, return int64
		du := a.GetMysqlDuration()
		de := du.ToNumber()
		if du.Fsp == 0 {
			d.SetInt64(de.IntPart())
			return d, nil
		}
		d.SetMysqlDecimal(de)
		return d, nil
	case types.KindMysqlHex:
		d.SetFloat64(a.GetMysqlHex().ToNumber())
		return d, nil
	case types.KindMysqlBit:
		d.SetFloat64(a.GetMysqlBit().ToNumber())
		return d, nil
	case types.KindMysqlEnum:
		d.SetFloat64(a.GetMysqlEnum().ToNumber())
		return d, nil
	case types.KindMysqlSet:
		d.SetFloat64(a.GetMysqlSet().ToNumber())
		return d, nil
	default:
		return a, nil
	}
}
