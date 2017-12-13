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

package types

import (
	"math"

	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
)

// CoerceArithmetic converts datum to appropriate datum for arithmetic computing.
func CoerceArithmetic(sc *stmtctx.StatementContext, a Datum) (d Datum, err error) {
	switch a.Kind() {
	case KindString, KindBytes:
		// MySQL will convert string to float for arithmetic operation
		f, err := StrToFloat(sc, a.GetString())
		if err != nil {
			return d, errors.Trace(err)
		}
		d.SetFloat64(f)
		return d, errors.Trace(err)
	case KindMysqlTime:
		// if time has no precision, return int64
		t := a.GetMysqlTime()
		de := t.ToNumber()
		if t.Fsp == 0 {
			iVal, err := de.ToInt()
			if err != nil {
				return d, errors.Trace(err)
			}
			d.SetInt64(iVal)
			return d, nil
		}
		d.SetMysqlDecimal(de)
		return d, nil
	case KindMysqlDuration:
		// if duration has no precision, return int64
		du := a.GetMysqlDuration()
		de := du.ToNumber()
		if du.Fsp == 0 {
			iVal, err := de.ToInt()
			if err != nil {
				return d, errors.Trace(err)
			}
			d.SetInt64(iVal)
			return d, nil
		}
		d.SetMysqlDecimal(de)
		return d, nil
	case KindBinaryLiteral, KindMysqlBit:
		val, err1 := a.GetBinaryLiteral().ToInt()
		d.SetUint64(val)
		return d, err1
	case KindMysqlEnum:
		d.SetFloat64(a.GetMysqlEnum().ToNumber())
		return d, nil
	case KindMysqlSet:
		d.SetFloat64(a.GetMysqlSet().ToNumber())
		return d, nil
	default:
		return a, nil
	}
}

// ComputePlus computes the result of a+b.
func ComputePlus(a, b Datum) (d Datum, err error) {
	switch a.Kind() {
	case KindInt64:
		switch b.Kind() {
		case KindInt64:
			r, err1 := AddInt64(a.GetInt64(), b.GetInt64())
			d.SetInt64(r)
			return d, errors.Trace(err1)
		case KindUint64:
			r, err1 := AddInteger(b.GetUint64(), a.GetInt64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case KindUint64:
		switch b.Kind() {
		case KindInt64:
			r, err1 := AddInteger(a.GetUint64(), b.GetInt64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		case KindUint64:
			r, err1 := AddUint64(a.GetUint64(), b.GetUint64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case KindFloat64:
		switch b.Kind() {
		case KindFloat64:
			r := a.GetFloat64() + b.GetFloat64()
			d.SetFloat64(r)
			return d, nil
		}
	case KindMysqlDecimal:
		switch b.Kind() {
		case KindMysqlDecimal:
			r := new(MyDecimal)
			err = DecimalAdd(a.GetMysqlDecimal(), b.GetMysqlDecimal(), r)
			d.SetMysqlDecimal(r)
			d.SetFrac(mathutil.Max(a.Frac(), b.Frac()))
			return d, err
		}
	}
	_, err = InvOp2(a.GetValue(), b.GetValue(), opcode.Plus)
	return d, err
}

// ComputeMinus computes the result of a-b.
func ComputeMinus(a, b Datum) (d Datum, err error) {
	switch a.Kind() {
	case KindInt64:
		switch b.Kind() {
		case KindInt64:
			r, err1 := SubInt64(a.GetInt64(), b.GetInt64())
			d.SetInt64(r)
			return d, errors.Trace(err1)
		case KindUint64:
			r, err1 := SubIntWithUint(a.GetInt64(), b.GetUint64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case KindUint64:
		switch b.Kind() {
		case KindInt64:
			r, err1 := SubUintWithInt(a.GetUint64(), b.GetInt64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		case KindUint64:
			r, err1 := SubUint64(a.GetUint64(), b.GetUint64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case KindFloat64:
		switch b.Kind() {
		case KindFloat64:
			r := a.GetFloat64() - b.GetFloat64()
			d.SetFloat64(r)
			return d, nil
		}
	case KindMysqlDecimal:
		switch b.Kind() {
		case KindMysqlDecimal:
			r := new(MyDecimal)
			err = DecimalSub(a.GetMysqlDecimal(), b.GetMysqlDecimal(), r)
			d.SetMysqlDecimal(r)
			return d, err
		}
	}
	_, err = InvOp2(a.GetValue(), b.GetValue(), opcode.Minus)
	return d, errors.Trace(err)
}

// ComputeMul computes the result of a*b.
func ComputeMul(a, b Datum) (d Datum, err error) {
	switch a.Kind() {
	case KindInt64:
		switch b.Kind() {
		case KindInt64:
			r, err1 := MulInt64(a.GetInt64(), b.GetInt64())
			d.SetInt64(r)
			return d, errors.Trace(err1)
		case KindUint64:
			r, err1 := MulInteger(b.GetUint64(), a.GetInt64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case KindUint64:
		switch b.Kind() {
		case KindInt64:
			r, err1 := MulInteger(a.GetUint64(), b.GetInt64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		case KindUint64:
			r, err1 := MulUint64(a.GetUint64(), b.GetUint64())
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case KindFloat64:
		switch b.Kind() {
		case KindFloat64:
			r := a.GetFloat64() * b.GetFloat64()
			d.SetFloat64(r)
			return d, nil
		}
	case KindMysqlDecimal:
		switch b.Kind() {
		case KindMysqlDecimal:
			r := new(MyDecimal)
			err = DecimalMul(a.GetMysqlDecimal(), b.GetMysqlDecimal(), r)
			d.SetMysqlDecimal(r)
			return d, nil
		}
	}

	_, err = InvOp2(a.GetValue(), b.GetValue(), opcode.Mul)
	return d, errors.Trace(err)
}

// ComputeDiv computes the result of a/b.
func ComputeDiv(sc *stmtctx.StatementContext, a, b Datum) (d Datum, err error) {
	// MySQL support integer division Div and division operator /
	// we use opcode.Div for division operator and will use another for integer division later.
	// for division operator, we will use float64 for calculation.
	switch a.Kind() {
	case KindFloat64:
		y, err1 := b.ToFloat64(sc)
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
		xa, err1 := a.ToDecimal(sc)
		if err != nil {
			return d, errors.Trace(err1)
		}

		xb, err1 := b.ToDecimal(sc)
		if err1 != nil {
			return d, errors.Trace(err1)
		}
		// division by zero return null
		to := new(MyDecimal)
		err = DecimalDiv(xa, xb, to, DivFracIncr)
		if err != ErrDivByZero {
			d.SetMysqlDecimal(to)
		} else {
			err = nil
		}
		return d, err
	}
}

// ComputeMod computes the result of a mod b.
func ComputeMod(sc *stmtctx.StatementContext, a, b Datum) (d Datum, err error) {
	switch a.Kind() {
	case KindInt64:
		x := a.GetInt64()
		switch b.Kind() {
		case KindInt64:
			y := b.GetInt64()
			if y == 0 {
				return d, nil
			}
			d.SetInt64(x % y)
			return d, nil
		case KindUint64:
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
	case KindUint64:
		x := a.GetUint64()
		switch b.Kind() {
		case KindInt64:
			y := b.GetInt64()
			if y == 0 {
				return d, nil
			} else if y < 0 {
				// first is uint64, return uint64.
				d.SetUint64(x % uint64(-y))
				return d, nil
			}
			d.SetUint64(x % uint64(y))
			return d, nil
		case KindUint64:
			y := b.GetUint64()
			if y == 0 {
				return d, nil
			}
			d.SetUint64(x % y)
			return d, nil
		}
	case KindFloat64:
		x := a.GetFloat64()
		switch b.Kind() {
		case KindFloat64:
			y := b.GetFloat64()
			if y == 0 {
				return d, nil
			}
			d.SetFloat64(math.Mod(x, y))
			return d, nil
		}
	case KindMysqlDecimal:
		x := a.GetMysqlDecimal()
		switch b.Kind() {
		case KindMysqlDecimal:
			y := b.GetMysqlDecimal()
			to := new(MyDecimal)
			err = DecimalMod(x, y, to)
			if err != ErrDivByZero {
				d.SetMysqlDecimal(to)
			} else {
				// div by zero returns nil without error.
				err = nil
			}
			return d, err
		}
	}
	_, err = InvOp2(a.GetValue(), b.GetValue(), opcode.Mod)
	return d, errors.Trace(err)
}

// ComputeIntDiv computes the result of a / b, both a and b are integer.
func ComputeIntDiv(sc *stmtctx.StatementContext, a, b Datum) (d Datum, err error) {
	switch a.Kind() {
	case KindInt64:
		x := a.GetInt64()
		switch b.Kind() {
		case KindInt64:
			y := b.GetInt64()
			if y == 0 {
				return d, nil
			}
			r, err1 := DivInt64(x, y)
			d.SetInt64(r)
			return d, errors.Trace(err1)
		case KindUint64:
			y := b.GetUint64()
			if y == 0 {
				return d, nil
			}
			r, err1 := DivIntWithUint(x, y)
			d.SetUint64(r)
			return d, errors.Trace(err1)
		}
	case KindUint64:
		x := a.GetUint64()
		switch b.Kind() {
		case KindInt64:
			y := b.GetInt64()
			if y == 0 {
				return d, nil
			}
			r, err1 := DivUintWithInt(x, y)
			d.SetUint64(r)
			return d, errors.Trace(err1)
		case KindUint64:
			y := b.GetUint64()
			if y == 0 {
				return d, nil
			}
			d.SetUint64(x / y)
			return d, nil
		}
	}

	// If either is not integer, use decimal to calculate
	x, err := a.ToDecimal(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	y, err := b.ToDecimal(sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	to := new(MyDecimal)
	err = DecimalDiv(x, y, to, DivFracIncr)
	if err == ErrDivByZero {
		return d, nil
	}
	iVal, err1 := to.ToInt()
	if err == nil {
		err = err1
	}
	d.SetInt64(iVal)
	return d, nil
}

// decimal2RoundUint converts a MyDecimal to an uint64 after rounding.
func decimal2RoundUint(x *MyDecimal) (uint64, error) {
	roundX := new(MyDecimal)
	err := x.Round(roundX, 0, ModeHalfEven)
	if err != nil {
		return 0, errors.Trace(err)
	}
	var uintX uint64
	if roundX.IsNegative() {
		var intX int64
		intX, err = roundX.ToInt()
		if err != nil && err != ErrTruncated {
			return 0, errors.Trace(err)
		}
		uintX = uint64(intX)
	} else {
		uintX, err = roundX.ToUint()
		if err != nil && err != ErrTruncated {
			return 0, errors.Trace(err)
		}
	}

	return uintX, nil
}

// ComputeBitAnd computes the result of a & b.
func ComputeBitAnd(sc *stmtctx.StatementContext, a, b Datum) (d Datum, err error) {
	aKind, bKind := a.Kind(), b.Kind()
	if (aKind == KindInt64 || aKind == KindUint64) && (bKind == KindInt64 || bKind == KindUint64) {
		d.SetUint64(a.GetUint64() & b.GetUint64())
		return
	}
	// If either is not integer, we round the operands and then use uint64 to calculate.
	x, err := convertNonInt2RoundUint64(sc, a)
	if err != nil {
		return d, errors.Trace(err)
	}

	y, err := convertNonInt2RoundUint64(sc, b)
	if err != nil {
		return d, errors.Trace(err)
	}

	d.SetUint64(x & y)
	return d, nil
}

// ComputeBitOr computes the result of a | b.
func ComputeBitOr(sc *stmtctx.StatementContext, a, b Datum) (d Datum, err error) {
	aKind, bKind := a.Kind(), b.Kind()
	if (aKind == KindInt64 || aKind == KindUint64) && (bKind == KindInt64 || bKind == KindUint64) {
		d.SetUint64(a.GetUint64() | b.GetUint64())
		return
	}
	// If either is not integer, we round the operands and then use uint64 to calculate.
	x, err := convertNonInt2RoundUint64(sc, a)
	if err != nil {
		return d, errors.Trace(err)
	}

	y, err := convertNonInt2RoundUint64(sc, b)
	if err != nil {
		return d, errors.Trace(err)
	}

	d.SetUint64(x | y)
	return d, nil
}

// ComputeBitNeg computes the result of ~a.
func ComputeBitNeg(sc *stmtctx.StatementContext, a Datum) (d Datum, err error) {
	aKind := a.Kind()
	if aKind == KindInt64 || aKind == KindUint64 {
		d.SetUint64(^a.GetUint64())
		return
	}
	// If either is not integer, we round the operands and then use uint64 to calculate.
	x, err := convertNonInt2RoundUint64(sc, a)
	if err != nil {
		return d, errors.Trace(err)
	}

	d.SetUint64(^x)
	return d, nil
}

// ComputeBitXor computes the result of a ^ b.
func ComputeBitXor(sc *stmtctx.StatementContext, a, b Datum) (d Datum, err error) {
	aKind, bKind := a.Kind(), b.Kind()
	if (aKind == KindInt64 || aKind == KindUint64) && (bKind == KindInt64 || bKind == KindUint64) {
		d.SetUint64(a.GetUint64() ^ b.GetUint64())
		return
	}
	// If either is not integer, we round the operands and then use uint64 to calculate.
	x, err := convertNonInt2RoundUint64(sc, a)
	if err != nil {
		return d, errors.Trace(err)
	}

	y, err := convertNonInt2RoundUint64(sc, b)
	if err != nil {
		return d, errors.Trace(err)
	}

	d.SetUint64(x ^ y)
	return d, nil
}

// ComputeLeftShift computes the result of a >> b.
func ComputeLeftShift(sc *stmtctx.StatementContext, a, b Datum) (d Datum, err error) {
	aKind, bKind := a.Kind(), b.Kind()
	if (aKind == KindInt64 || aKind == KindUint64) && (bKind == KindInt64 || bKind == KindUint64) {
		d.SetUint64(a.GetUint64() << b.GetUint64())
		return
	}
	// If either is not integer, we round the operands and then use uint64 to calculate.
	x, err := convertNonInt2RoundUint64(sc, a)
	if err != nil {
		return d, errors.Trace(err)
	}

	y, err := convertNonInt2RoundUint64(sc, b)
	if err != nil {
		return d, errors.Trace(err)
	}

	d.SetUint64(x << y)
	return d, nil
}

// ComputeRightShift computes the result of a << b.
func ComputeRightShift(sc *stmtctx.StatementContext, a, b Datum) (d Datum, err error) {
	aKind, bKind := a.Kind(), b.Kind()
	if (aKind == KindInt64 || aKind == KindUint64) && (bKind == KindInt64 || bKind == KindUint64) {
		d.SetUint64(a.GetUint64() >> b.GetUint64())
		return
	}
	// If either is not integer, we round the operands and then use uint64 to calculate.
	x, err := convertNonInt2RoundUint64(sc, a)
	if err != nil {
		return d, errors.Trace(err)
	}

	y, err := convertNonInt2RoundUint64(sc, b)
	if err != nil {
		return d, errors.Trace(err)
	}

	d.SetUint64(x >> y)
	return d, nil
}

// convertNonInt2RoundUint64 converts a non-integer to an uint64
func convertNonInt2RoundUint64(sc *stmtctx.StatementContext, x Datum) (d uint64, err error) {
	decimalX, err := x.ToDecimal(sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	d, err = decimal2RoundUint(decimalX)
	if err != nil {
		return d, errors.Trace(err)
	}
	return
}
