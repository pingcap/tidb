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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/parser/opcode"
)

// CoerceArithmetic converts datum to appropriate datum for arithmetic computing.
func CoerceArithmetic(a Datum) (d Datum, err error) {
	switch a.Kind() {
	case KindString, KindBytes:
		// MySQL will convert string to float for arithmetic operation
		f, err := StrToFloat(a.GetString())
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
			d.SetInt64(de.IntPart())
			return d, nil
		}
		d.SetMysqlDecimal(de)
		return d, nil
	case KindMysqlDuration:
		// if duration has no precision, return int64
		du := a.GetMysqlDuration()
		de := du.ToNumber()
		if du.Fsp == 0 {
			d.SetInt64(de.IntPart())
			return d, nil
		}
		d.SetMysqlDecimal(de)
		return d, nil
	case KindMysqlHex:
		d.SetFloat64(a.GetMysqlHex().ToNumber())
		return d, nil
	case KindMysqlBit:
		d.SetFloat64(a.GetMysqlBit().ToNumber())
		return d, nil
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
			r := a.GetMysqlDecimal().Add(b.GetMysqlDecimal())
			d.SetMysqlDecimal(r)
			return d, nil
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
			r := a.GetMysqlDecimal().Sub(b.GetMysqlDecimal())
			d.SetMysqlDecimal(r)
			return d, nil
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
			r := a.GetMysqlDecimal().Mul(b.GetMysqlDecimal())
			d.SetMysqlDecimal(r)
			return d, nil
		}
	}

	_, err = InvOp2(a.GetValue(), b.GetValue(), opcode.Mul)
	return d, errors.Trace(err)
}

// ComputeDiv computes the result of a/b.
func ComputeDiv(a, b Datum) (d Datum, err error) {
	// MySQL support integer division Div and division operator /
	// we use opcode.Div for division operator and will use another for integer division later.
	// for division operator, we will use float64 for calculation.
	switch a.Kind() {
	case KindFloat64:
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

// ComputeMod computes the result of a mod b.
func ComputeMod(a, b Datum) (d Datum, err error) {
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
				d.SetUint64(uint64(x % uint64(-y)))
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
			xf, _ := x.Float64()
			yf, _ := y.Float64()
			if yf == 0 {
				return d, nil
			}
			d.SetFloat64(math.Mod(xf, yf))
			return d, nil
		}
	}
	_, err = InvOp2(a.GetValue(), b.GetValue(), opcode.Mod)
	return d, errors.Trace(err)
}

// ComputeIntDiv computes the result of a / b, both a and b are integer.
func ComputeIntDiv(a, b Datum) (d Datum, err error) {
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
