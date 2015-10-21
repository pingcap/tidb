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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ Expression = (*UnaryOperation)(nil)
)

// UnaryOperation is the expression for unary operator.
type UnaryOperation struct {
	// Op is the operator opcode.
	Op opcode.Op
	// V is the unary expression.
	V Expression
}

// NewUnaryOperation creates an unary expression.
func NewUnaryOperation(op opcode.Op, l Expression) Expression {
	// extract expression in parenthese, e.g, ((expr)) -> expr
	for {
		pe, ok := l.(*PExpr)
		if ok {
			l = pe.Expr
			continue
		}

		break
	}

	// if op is Not and l is relational comparison expression,
	// we can invert relational operator.
	if op == opcode.Not {
		b, ok := l.(*BinaryOperation)
		if ok {
			switch b.Op {
			case opcode.EQ:
				b.Op = opcode.NE
				return b
			case opcode.NE:
				b.Op = opcode.EQ
				return b
			case opcode.GT:
				b.Op = opcode.LE
				return b
			case opcode.GE:
				b.Op = opcode.LT
				return b
			case opcode.LT:
				b.Op = opcode.GE
				return b
			case opcode.LE:
				b.Op = opcode.GT
				return b
			}
		}

		u, ok := l.(*UnaryOperation)
		if ok && u.Op == opcode.Not { // !!x: x
			return u.V
		}
	}

	return &UnaryOperation{op, l}
}

// Clone implements the Expression Clone interface.
func (u *UnaryOperation) Clone() Expression {
	v := u.V.Clone()
	return &UnaryOperation{Op: u.Op, V: v}
}

// IsStatic implements the Expression IsStatic interface.
func (u *UnaryOperation) IsStatic() bool {
	return u.V.IsStatic()
}

// String implements the Expression String interface.
func (u *UnaryOperation) String() string {
	switch u.V.(type) {
	case *BinaryOperation:
		return fmt.Sprintf("%s(%s)", u.Op, u.V)
	case *ExistsSubQuery:
		switch u.Op {
		case opcode.Not:
			return fmt.Sprintf("NOT %s", u.V)
		default:
			return fmt.Sprintf("%s%s", u.Op, u.V)
		}
	default:
		switch u.Op {
		case opcode.Not:
			// we must handle not oeprator, e.g
			// if not using (),  "not null is null" will output "!null is null".
			// the ! and not's precedences are not equal
			// "not null is null" will returns 0, but "!null is null" will return 1
			return fmt.Sprintf("%s(%s)", u.Op, u.V)
		default:
			return fmt.Sprintf("%s%s", u.Op, u.V)
		}
	}
}

// Eval implements the Expression Eval interface.
func (u *UnaryOperation) Eval(ctx context.Context, args map[interface{}]interface{}) (r interface{}, err error) {
	defer func() {
		if e := recover(); e != nil {
			r, err = nil, errors.Errorf("%v", e)
		}
	}()

	switch op := u.Op; op {
	case opcode.Not:
		a := Eval(u.V, ctx, args)
		a = types.RawData(a)
		if a == nil {
			return
		}
		n, err := types.ToBool(a)
		if err != nil {
			return types.UndOp(a, op)
		} else if n == 0 {
			return int64(1), nil
		}
		return int64(0), nil
	case opcode.BitNeg:
		a := Eval(u.V, ctx, args)
		a = types.RawData(a)
		if a == nil {
			return
		}
		// for bit operation, we will use int64 first, then return uint64
		n, err := types.ToInt64(a)
		if err != nil {
			return types.UndOp(a, op)
		}

		return uint64(^n), nil
	case opcode.Plus:
		a := Eval(u.V, ctx, args)
		a = types.RawData(a)
		if a == nil {
			return
		}
		switch x := a.(type) {
		case nil:
			return nil, nil
		case bool:
			if x {
				return int64(1), nil
			}
			return int64(0), nil
		case float32:
			return +x, nil
		case float64:
			return +x, nil
		case int:
			return +x, nil
		case int8:
			return +x, nil
		case int16:
			return +x, nil
		case int32:
			return +x, nil
		case int64:
			return +x, nil
		case uint:
			return +x, nil
		case uint8:
			return +x, nil
		case uint16:
			return +x, nil
		case uint32:
			return +x, nil
		case uint64:
			return +x, nil
		case mysql.Duration:
			return x, nil
		case mysql.Time:
			return x, nil
		case string:
			return x, nil
		case mysql.Decimal:
			return x, nil
		case []byte:
			return x, nil
		case mysql.Hex:
			return x, nil
		case mysql.Bit:
			return x, nil
		case mysql.Enum:
			return x, nil
		case mysql.Set:
			return x, nil
		default:
			return types.UndOp(a, op)
		}
	case opcode.Minus:
		a := Eval(u.V, ctx, args)
		a = types.RawData(a)
		if a == nil {
			return
		}

		switch x := a.(type) {
		case nil:
			return nil, nil
		case bool:
			if x {
				return int64(-1), nil
			}
			return int64(0), nil
		case float32:
			return -x, nil
		case float64:
			return -x, nil
		case int:
			return -x, nil
		case int8:
			return -x, nil
		case int16:
			return -x, nil
		case int32:
			return -x, nil
		case int64:
			return -x, nil
		case uint:
			return -int64(x), nil
		case uint8:
			return -int64(x), nil
		case uint16:
			return -int64(x), nil
		case uint32:
			return -int64(x), nil
		case uint64:
			// TODO: check overflow and do more test for unsigned type
			return -int64(x), nil
		case mysql.Duration:
			return mysql.ZeroDecimal.Sub(x.ToNumber()), nil
		case mysql.Time:
			return mysql.ZeroDecimal.Sub(x.ToNumber()), nil
		case string:
			f, err := types.StrToFloat(x)
			return -f, err
		case mysql.Decimal:
			f, _ := x.Float64()
			return mysql.NewDecimalFromFloat(-f), nil
		case []byte:
			f, err := types.StrToFloat(string(x))
			return -f, err
		case mysql.Hex:
			return -x.ToNumber(), nil
		case mysql.Bit:
			return -x.ToNumber(), nil
		case mysql.Enum:
			return -x.ToNumber(), nil
		case mysql.Set:
			return -x.ToNumber(), nil
		default:
			return types.UndOp(a, op)
		}
	default:
		panic("should never happen")
	}
}

// Accept implements Expression Accept interface.
func (u *UnaryOperation) Accept(v Visitor) (Expression, error) {
	return v.VisitUnaryOperation(u)
}
