// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package expression

import (
	"fmt"
	"math"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

var _ Expression = (*BinaryOperation)(nil)

// BinaryOperation is for binary operation like 1 + 1, 1 - 1, etc.
type BinaryOperation struct {
	// Op is the operator code for BinaryOperation.
	Op opcode.Op
	// L is the left expression in BinaryOperation.
	L Expression
	// R is the right expression in BinaryOperation.
	R Expression
}

// NewBinaryOperation creates a binary expression with op as the operator code and
// x, y as the left and right operator expression.
func NewBinaryOperation(op opcode.Op, x, y Expression) Expression {
	b := &BinaryOperation{op, x, y}

	var ok bool
	// Normalize relational comparison operation like format "ident op expr"
	// if left is Ident operation, return directly
	if _, ok = b.L.(*Ident); ok {
		return b
	}

	var r Expression
	// left is not Ident expression, check right
	if r, ok = b.R.(*Ident); !ok {
		return b
	}

	// Normalize expr relOp indent: Ident InRelOp expr
	switch b.Op {
	case opcode.LT:
		return &BinaryOperation{opcode.GT, r, b.L}
	case opcode.LE:
		return &BinaryOperation{opcode.GE, r, b.L}
	case opcode.GT:
		return &BinaryOperation{opcode.LT, r, b.L}
	case opcode.GE:
		return &BinaryOperation{opcode.LE, r, b.L}
	case opcode.EQ, opcode.NE, opcode.NullEQ:
		return &BinaryOperation{b.Op, r, b.L}
	default:
		return b
	}
}

// IsIdentCompareVal checks left expression is Ident expression and right is Value expression
// for relational comparison, then returns left identifier name and right value.
func (o *BinaryOperation) IsIdentCompareVal() (bool, string, interface{}, error) {
	id, ok := o.L.(*Ident)
	if !ok {
		return false, "", nil, nil
	}
	switch o.Op {
	case opcode.LT, opcode.LE,
		opcode.GT, opcode.GE,
		opcode.EQ, opcode.NE:
	default:
		return false, "", nil, nil
	}
	if !o.R.IsStatic() {
		return false, "", nil, nil
	}
	value, err := o.R.Eval(nil, nil)
	if err != nil {
		return false, "", nil, errors.Trace(err)
	}
	return true, id.L, value, nil
}

// Clone implements the Expression Clone interface.
func (o *BinaryOperation) Clone() Expression {
	l := o.L.Clone()
	r := o.R.Clone()
	return NewBinaryOperation(o.Op, l, r)
}

// IsStatic implements the Expression IsStatic interface.
func (o *BinaryOperation) IsStatic() bool {
	return o.L.IsStatic() && o.R.IsStatic()
}

// String implements the Expression String interface.
func (o *BinaryOperation) String() string {
	return fmt.Sprintf("%s %s %s", o.L, o.Op, o.R)
}

func (o *BinaryOperation) traceErr(err error) error {
	if err == nil {
		return nil
	}

	return errors.Errorf("eval %s err: %v", o, err)
}

// Eval implements the Expression Eval interface.
func (o *BinaryOperation) Eval(ctx context.Context, args map[interface{}]interface{}) (r interface{}, err error) {
	defer func() {
		if e := recover(); e != nil {
			r, err = nil, errors.Errorf("%v", e)
			err = o.traceErr(err)
		}
	}()

	// all operands must have same column.
	if err := hasSameColumnCount(ctx, o.L, o.R); err != nil {
		return nil, o.traceErr(err)
	}

	// row constructor only supports comparison operation.
	switch o.Op {
	case opcode.LT, opcode.LE, opcode.GE, opcode.GT, opcode.EQ, opcode.NE, opcode.NullEQ:
	default:
		if err := CheckOneColumn(ctx, o.L); err != nil {
			return nil, o.traceErr(err)
		}
	}

	switch o.Op {
	case opcode.AndAnd, opcode.OrOr, opcode.LogicXor:
		return o.evalLogicOp(ctx, args)
	case opcode.LT, opcode.LE, opcode.GE, opcode.GT, opcode.EQ, opcode.NE, opcode.NullEQ:
		return o.evalComparisonOp(ctx, args)
	case opcode.RightShift, opcode.LeftShift, opcode.And, opcode.Or, opcode.Xor:
		// TODO: MySQL doesn't support and not, we should remove it later.
		return o.evalBitOp(ctx, args)
	case opcode.Plus, opcode.Minus, opcode.Mod, opcode.Div, opcode.Mul, opcode.IntDiv:
		return o.evalArithmeticOp(ctx, args)
	default:
		panic("should never happen")
	}
}

// Accept implements Expression Accept interface.
func (o *BinaryOperation) Accept(v Visitor) (Expression, error) {
	return v.VisitBinaryOperation(o)
}

func (o *BinaryOperation) get2(ctx context.Context, args map[interface{}]interface{}) (x, y interface{}, err error) {
	x, err = o.L.Eval(ctx, args)
	if err != nil {
		err = o.traceErr(err)
		return
	}
	y, err = o.R.Eval(ctx, args)
	if err != nil {
		err = o.traceErr(err)
		return
	}

	x, y = types.Coerce(x, y)
	return x, y, nil
}

func (o *BinaryOperation) evalAndAnd(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	a, err := o.L.Eval(ctx, args)
	if err != nil {
		return nil, o.traceErr(err)
	}

	if !types.IsNil(a) {
		var x int64
		x, err = types.ToBool(a)
		if err != nil {
			return nil, o.traceErr(err)
		} else if x == 0 {
			// false && any other types is false
			return x, nil
		}
	}

	b, err := o.R.Eval(ctx, args)
	if err != nil {
		return nil, o.traceErr(err)
	}

	if !types.IsNil(b) {
		var y int64
		y, err = types.ToBool(b)
		if err != nil {
			return nil, o.traceErr(err)
		} else if y == 0 {
			return y, nil
		}
	}

	// here x and y are all not false
	// if a or b is nil
	if types.IsNil(a) || types.IsNil(b) {
		return nil, nil
	}

	return int64(1), nil
}

func (o *BinaryOperation) evalOrOr(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	a, err := o.L.Eval(ctx, args)
	if err != nil {
		return nil, o.traceErr(err)
	}

	var (
		x int64
		y int64
	)

	if !types.IsNil(a) {
		x, err = types.ToBool(a)
		if err != nil {
			return nil, o.traceErr(err)
		} else if x == 1 {
			// true || any other types is true
			return x, nil
		}
	}

	b, err := o.R.Eval(ctx, args)
	if err != nil {
		return nil, o.traceErr(err)
	}

	if !types.IsNil(b) {
		y, err = types.ToBool(b)
		if err != nil {
			return nil, o.traceErr(err)
		} else if y == 1 {
			return y, nil
		}
	}

	// here x and y are all not true
	// if a or b is nil
	if types.IsNil(a) || types.IsNil(b) {
		return nil, nil
	}

	return int64(0), nil
}

func (o *BinaryOperation) evalLogicXor(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	a, err := o.L.Eval(ctx, args)
	if err != nil || types.IsNil(a) {
		return nil, o.traceErr(err)
	}

	x, err := types.ToBool(a)
	if err != nil {
		return nil, o.traceErr(err)
	}

	b, err := o.R.Eval(ctx, args)
	if err != nil || types.IsNil(b) {
		return nil, o.traceErr(err)
	}

	y, err := types.ToBool(b)
	if err != nil {
		return nil, o.traceErr(err)
	}

	if x == y {
		return int64(0), nil
	}

	return int64(1), nil
}

func (o *BinaryOperation) errorf(format string, args ...interface{}) error {
	err := errors.Errorf(format, args...)
	return o.traceErr(err)
}

// Operator: &, ~, |, ^, <<, >>
// See https://dev.mysql.com/doc/refman/5.7/en/bit-functions.html
func (o *BinaryOperation) evalBitOp(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	a, b, err := o.get2(ctx, args)
	if err != nil {
		return nil, err
	}

	if types.IsNil(a) || types.IsNil(b) {
		return nil, nil
	}

	x, err := types.ToInt64(a)
	if err != nil {
		return nil, o.traceErr(err)
	}

	y, err := types.ToInt64(b)
	if err != nil {
		return nil, o.traceErr(err)
	}

	// use a int64 for bit operator, return uint64
	switch o.Op {
	case opcode.And:
		return uint64(x & y), nil
	case opcode.Or:
		return uint64(x | y), nil
	case opcode.Xor:
		return uint64(x ^ y), nil
	case opcode.RightShift:
		return uint64(x) >> uint64(y), nil
	case opcode.LeftShift:
		return uint64(x) << uint64(y), nil
	default:
		return nil, o.errorf("invalid op %v in bit operation", o.Op)
	}
}

// Operator: &&, ||, logic XOR
// See https://dev.mysql.com/doc/refman/5.7/en/logical-operators.html
func (o *BinaryOperation) evalLogicOp(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	switch o.Op {
	case opcode.AndAnd:
		return o.evalAndAnd(ctx, args)
	case opcode.OrOr:
		return o.evalOrOr(ctx, args)
	case opcode.LogicXor:
		return o.evalLogicXor(ctx, args)
	default:
		return nil, o.errorf("invalid op %v in logic operation", o.Op)
	}
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
		return false, errors.Errorf("invalid op %v in comparision operation", op)
	}
}

// operator: >=, >, <=, <, !=, <>, = <=>, etc.
// see https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html
func (o *BinaryOperation) evalComparisonOp(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	//TODO: support <=> later
	a, b, err := o.get2(ctx, args)
	if err != nil {
		return nil, err
	}

	if types.IsNil(a) || types.IsNil(b) {
		// for <=>, if a and b are both nil, return true.
		// if a or b is nil, return false.
		if o.Op == opcode.NullEQ {
			return types.IsNil(a) && types.IsNil(b), nil
		}

		return nil, nil
	}

	n, err := types.Compare(a, b)
	if err != nil {
		return nil, o.traceErr(err)
	}

	r, err := getCompResult(o.Op, n)
	if err != nil {
		return nil, o.errorf(err.Error())
	}

	return r, nil
}

func (o *BinaryOperation) evalPlus(a interface{}, b interface{}) (interface{}, error) {
	switch x := a.(type) {
	case int64:
		switch y := b.(type) {
		case int64:
			return types.AddInt64(x, y)
		case uint64:
			return types.AddInteger(y, x)
		}
	case uint64:
		switch y := b.(type) {
		case int64:
			return types.AddInteger(x, y)
		case uint64:
			return types.AddUint64(x, y)
		}
	case float64:
		switch y := b.(type) {
		case float64:
			return x + y, nil
		}
	case mysql.Decimal:
		switch y := b.(type) {
		case mysql.Decimal:
			return x.Add(y), nil
		}
	}

	return types.InvOp2(a, b, opcode.Plus)
}

func (o *BinaryOperation) evalMinus(a interface{}, b interface{}) (interface{}, error) {
	switch x := a.(type) {
	case int64:
		switch y := b.(type) {
		case int64:
			return types.SubInt64(x, y)
		case uint64:
			return types.SubIntWithUint(x, y)
		}
	case uint64:
		switch y := b.(type) {
		case int64:
			return types.SubUintWithInt(x, y)
		case uint64:
			return types.SubUint64(x, y)
		}
	case float64:
		switch y := b.(type) {
		case float64:
			return x - y, nil
		}
	case mysql.Decimal:
		switch y := b.(type) {
		case mysql.Decimal:
			return x.Sub(y), nil
		}
	}

	return types.InvOp2(a, b, opcode.Minus)
}

func (o *BinaryOperation) evalMul(a interface{}, b interface{}) (interface{}, error) {
	switch x := a.(type) {
	case int64:
		switch y := b.(type) {
		case int64:
			return types.MulInt64(x, y)
		case uint64:
			return types.MulInteger(y, x)
		}
	case uint64:
		switch y := b.(type) {
		case int64:
			return types.MulInteger(x, y)
		case uint64:
			return types.MulUint64(x, y)
		}
	case float64:
		switch y := b.(type) {
		case float64:
			return x * y, nil
		}
	case mysql.Decimal:
		switch y := b.(type) {
		case mysql.Decimal:
			return x.Mul(y), nil
		}
	}

	return types.InvOp2(a, b, opcode.Mul)
}

func (o *BinaryOperation) evalDiv(a interface{}, b interface{}) (interface{}, error) {
	// MySQL support integer divison Div and division operator /
	// we use opcode.Div for division operator and will use another for integer division later.
	// for division operator, we will use float64 for calculation.
	switch x := a.(type) {
	case float64:
		y, err := types.ToFloat64(b)
		if err != nil {
			return nil, err
		}

		if y == 0 {
			return nil, nil
		}

		return x / y, nil
	default:
		// the scale of the result is the scale of the first operand plus
		// the value of the div_precision_increment system variable (which is 4 by default)
		// we will use 4 here

		xa, err := types.ToDecimal(a)
		if err != nil {
			return nil, o.traceErr(err)
		}

		xb, err := types.ToDecimal(b)
		if err != nil {
			return nil, o.traceErr(err)
		}
		if f, _ := xb.Float64(); f == 0 {
			// division by zero return null
			return nil, nil
		}

		return xa.Div(xb), nil
	}
}

// See https://dev.mysql.com/doc/refman/5.7/en/arithmetic-functions.html#operator_div
func (o *BinaryOperation) evalIntDiv(a interface{}, b interface{}) (interface{}, error) {
	switch x := a.(type) {
	case int64:
		switch y := b.(type) {
		case int64:
			if y == 0 {
				return nil, nil
			}
			return types.DivInt64(x, y)
		case uint64:
			if y == 0 {
				return nil, nil
			}
			return types.DivIntWithUint(x, y)
		}
	case uint64:
		switch y := b.(type) {
		case int64:
			if y == 0 {
				return nil, nil
			}
			return types.DivUintWithInt(x, y)
		case uint64:
			if y == 0 {
				return nil, nil
			}
			return x / y, nil
		}
	}

	// if any is none integer, use decimal to calculate
	x, err := types.ToDecimal(a)
	if err != nil {
		return nil, o.traceErr(err)
	}

	y, err := types.ToDecimal(b)
	if err != nil {
		return nil, o.traceErr(err)
	}

	if f, _ := y.Float64(); f == 0 {
		return nil, nil
	}

	return x.Div(y).IntPart(), nil
}

func (o *BinaryOperation) evalMod(a interface{}, b interface{}) (interface{}, error) {
	switch x := a.(type) {
	case int64:
		switch y := b.(type) {
		case int64:
			if y == 0 {
				return nil, nil
			}
			return x % y, nil
		case uint64:
			if y == 0 {
				return nil, nil
			} else if x < 0 {
				// first is int64, return int64.
				return -int64(uint64(-x) % y), nil
			}
			return int64(uint64(x) % y), nil
		}
	case uint64:
		switch y := b.(type) {
		case int64:
			if y == 0 {
				return nil, nil
			} else if y < 0 {
				// first is uint64, return uint64.
				return uint64(x % uint64(-y)), nil
			}
			return x % uint64(y), nil
		case uint64:
			if y == 0 {
				return nil, nil
			}
			return x % y, nil
		}
	case float64:
		switch y := b.(type) {
		case float64:
			if y == 0 {
				return nil, nil
			}
			return math.Mod(x, y), nil
		}
	case mysql.Decimal:
		switch y := b.(type) {
		case mysql.Decimal:
			xf, _ := x.Float64()
			yf, _ := y.Float64()
			if yf == 0 {
				return nil, nil
			}
			return math.Mod(xf, yf), nil
		}
	}

	return types.InvOp2(a, b, opcode.Mod)
}

func (o *BinaryOperation) coerceArithmetic(a interface{}) (interface{}, error) {
	switch x := a.(type) {
	case string:
		// MySQL will convert string to float for arithmetic operation
		f, err := types.StrToFloat(x)
		if err != nil {
			return nil, err
		}
		return f, err
	case mysql.Time:
		// if time has no precision, return int64
		v := x.ToNumber()
		if x.Fsp == 0 {
			return v.IntPart(), nil
		}
		return v, nil
	case mysql.Duration:
		// if duration has no precision, return int64
		v := x.ToNumber()
		if x.Fsp == 0 {
			return v.IntPart(), nil
		}
		return v, nil
	case []byte:
		// []byte is the same as string, converted to float for arithmetic operator.
		f, err := types.StrToFloat(string(x))
		if err != nil {
			return nil, err
		}
		return f, err
	case mysql.Hex:
		return x.ToNumber(), nil
	case mysql.Bit:
		return x.ToNumber(), nil
	case mysql.Enum:
		return x.ToNumber(), nil
	case mysql.Set:
		return x.ToNumber(), nil
	default:
		return x, nil
	}
}

func (o *BinaryOperation) coerceArithmetic2(a interface{}, b interface{}) (x interface{}, y interface{}, err error) {
	x, err = o.coerceArithmetic(a)
	if err != nil {
		return
	}

	y, err = o.coerceArithmetic(b)
	if err != nil {
		return
	}

	x, y = types.Coerce(x, y)
	return x, y, nil
}

// Operator: DIV / - % MOD + *
// See https://dev.mysql.com/doc/refman/5.7/en/arithmetic-functions.html#operator_divide
func (o *BinaryOperation) evalArithmeticOp(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	a, b, err := o.get2(ctx, args)
	if err != nil {
		return nil, err
	}

	a = types.RawData(a)
	b = types.RawData(b)

	if a == nil || b == nil {
		// TODO: for <=>, if a and b are both nil, return true
		return nil, nil
	}

	if a, b, err = o.coerceArithmetic2(a, b); err != nil {
		return nil, o.traceErr(err)
	}

	// TODO: support logic division DIV
	switch o.Op {
	case opcode.Plus:
		return o.evalPlus(a, b)
	case opcode.Minus:
		return o.evalMinus(a, b)
	case opcode.Mul:
		return o.evalMul(a, b)
	case opcode.Div:
		return o.evalDiv(a, b)
	case opcode.Mod:
		return o.evalMod(a, b)
	case opcode.IntDiv:
		return o.evalIntDiv(a, b)
	default:
		return nil, o.errorf("invalid op %v in arithmetic operation", o.Op)
	}
}
