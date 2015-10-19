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
	"errors"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression/builtin"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testBinOpSuite{})

type testBinOpSuite struct {
}

func (s *testBinOpSuite) SetUpSuite(c *C) {
}

func (s *testBinOpSuite) TestComparisonOp(c *C) {
	tbl := []struct {
		lhs    interface{}
		op     opcode.Op
		rhs    interface{}
		result int64 // 0 for false, 1 for true
	}{
		// test EQ
		{1, opcode.EQ, 2, 0},
		{false, opcode.EQ, false, 1},
		{false, opcode.EQ, true, 0},
		{true, opcode.EQ, true, 1},
		{true, opcode.EQ, false, 0},
		{"1", opcode.EQ, true, 1},
		{"1", opcode.EQ, false, 0},

		// test NEQ
		{1, opcode.NE, 2, 1},
		{false, opcode.NE, false, 0},
		{false, opcode.NE, true, 1},
		{true, opcode.NE, true, 0},
		{"1", opcode.NE, true, 0},
		{"1", opcode.NE, false, 1},

		// test GT, GE
		{1, opcode.GT, 0, 1},
		{1, opcode.GT, 1, 0},
		{1, opcode.GE, 1, 1},
		{3.14, opcode.GT, 3, 1},
		{3.14, opcode.GE, 3.14, 1},

		// test LT, LE
		{1, opcode.LT, 2, 1},
		{1, opcode.LT, 1, 0},
		{1, opcode.LE, 1, 1},
	}

	for _, t := range tbl {
		expr := NewBinaryOperation(t.op, Value{t.lhs}, Value{t.rhs})
		v, err := expr.Eval(nil, nil)
		c.Assert(err, IsNil)
		val, err := types.ToBool(v)
		c.Assert(err, IsNil)
		c.Assert(val, Equals, t.result)
	}

	// test nil
	nilTbl := []struct {
		lhs interface{}
		op  opcode.Op
		rhs interface{}
	}{
		{nil, opcode.EQ, nil},
		{nil, opcode.EQ, 1},
		{nil, opcode.NE, nil},
		{nil, opcode.NE, 1},
		{nil, opcode.LT, nil},
		{nil, opcode.LT, 1},
		{nil, opcode.LE, nil},
		{nil, opcode.LE, 1},
		{nil, opcode.GT, nil},
		{nil, opcode.GT, 1},
		{nil, opcode.GE, nil},
		{nil, opcode.GE, 1},
	}

	for _, t := range nilTbl {
		expr := NewBinaryOperation(t.op, Value{t.lhs}, Value{t.rhs})
		v, err := expr.Eval(nil, nil)
		c.Assert(err, IsNil)
		c.Assert(v, IsNil)
	}

	// test error
	mock := mockExpr{
		isStatic: false,
		err:      errors.New("must error"),
	}

	errTbl := []struct {
		arg interface{}
		op  opcode.Op
	}{
		{1, opcode.EQ},
		{1.1, opcode.NE},
		{mysql.NewDecimalFromInt(1, 0), opcode.LT},
		{1, opcode.LE},
		{1.1, opcode.GT},
		{1, opcode.GE},
	}

	for _, t := range errTbl {
		expr := NewBinaryOperation(t.op, Value{t.arg}, mock)
		_, err := expr.Eval(nil, nil)
		c.Assert(err, NotNil)
		c.Assert(expr.Clone(), NotNil)

		expr = NewBinaryOperation(t.op, mock, Value{t.arg})
		_, err = expr.Eval(nil, nil)
		c.Assert(err, NotNil)

		c.Assert(expr.Clone(), NotNil)
	}

	mock.err = nil
	mock.val = "abc"

	for _, t := range errTbl {
		expr := NewBinaryOperation(t.op, Value{t.arg}, mock)
		_, err := expr.Eval(nil, nil)
		c.Assert(err, NotNil)

		expr = NewBinaryOperation(t.op, mock, Value{t.arg})
		_, err = expr.Eval(nil, nil)
		c.Assert(err, NotNil)
	}

	expr := BinaryOperation{Op: opcode.Plus, L: Value{1}, R: Value{1}}
	_, err := expr.evalComparisonOp(nil, nil)
	c.Assert(err, NotNil)
}

func (s *testBinOpSuite) TestIdentRelOp(c *C) {
	tbl := []struct {
		lhs Expression
		op  opcode.Op
		rhs Expression
	}{
		{&Ident{}, opcode.LT, Value{1}},
		{&Ident{}, opcode.LE, Value{1}},
		{&Ident{}, opcode.GT, Value{1}},
		{&Ident{}, opcode.GE, Value{1}},
		{&Ident{}, opcode.EQ, Value{1}},
		{&Ident{}, opcode.NE, Value{1}},
		{&Ident{}, opcode.Plus, Value{1}},
	}

	m := map[interface{}]interface{}{
		builtin.ExprEvalArgAggEmpty: struct{}{},
	}

	for _, t := range tbl {
		expr := NewBinaryOperation(t.op, t.lhs, t.rhs)
		_, err := expr.Eval(nil, m)
		c.Assert(err, IsNil)

		expr = NewBinaryOperation(t.op, t.rhs, t.lhs)
		_, err = expr.Eval(nil, m)
		c.Assert(err, IsNil)
	}

	f := func(name string) *Ident {
		return &Ident{
			CIStr: model.NewCIStr(name),
		}
	}

	// IsIdentRelOpVal
	relTbl := []struct {
		lhs Expression
		op  opcode.Op
		rhs Expression
		ret bool
	}{
		{f("id"), opcode.LT, Value{1}, true},
		{f("id"), opcode.LE, Value{1}, true},
		{f("id"), opcode.GT, Value{1}, true},
		{f("id"), opcode.GE, Value{1}, true},
		{f("id"), opcode.EQ, Value{1}, true},
		{f("id"), opcode.NE, Value{1}, true},
		{f("id"), opcode.Plus, Value{1}, false},
		{f("db.id"), opcode.NE, Value{1}, true},
		{f("id"), opcode.LT, f("name"), false},
		{Value{1}, opcode.NE, Value{1}, false},
	}

	for i, t := range relTbl {
		expr := &BinaryOperation{Op: t.op, L: t.lhs, R: t.rhs}
		b, _, _, err := expr.IsIdentCompareVal()
		c.Assert(err, IsNil)
		c.Assert(b, Equals, t.ret, Commentf("%d", i))
	}
}

func (s *testBinOpSuite) TestLogicOp(c *C) {
	tbl := []struct {
		lhs interface{}
		op  opcode.Op
		rhs interface{}
		ret interface{}
	}{
		{nil, opcode.AndAnd, 1, nil},
		{nil, opcode.AndAnd, 0, 0},
		{nil, opcode.OrOr, 1, 1},
		{nil, opcode.OrOr, 0, nil},
		{nil, opcode.LogicXor, 1, nil},
		{nil, opcode.LogicXor, 0, nil},
		{1, opcode.AndAnd, 0, 0},
		{1, opcode.AndAnd, 1, 1},
		{1, opcode.OrOr, 0, 1},
		{1, opcode.OrOr, 1, 1},
		{0, opcode.OrOr, 0, 0},
		{1, opcode.LogicXor, 0, 1},
		{1, opcode.LogicXor, 1, 0},
		{0, opcode.LogicXor, 0, 0},
		{0, opcode.LogicXor, 1, 1},
	}

	for _, t := range tbl {
		expr := NewBinaryOperation(t.op, Value{t.lhs}, Value{t.rhs})
		v, err := expr.Eval(nil, nil)
		c.Assert(err, IsNil)
		switch x := t.ret.(type) {
		case nil:
			c.Assert(v, IsNil)
		case int:
			c.Assert(v, DeepEquals, int64(x))
		}

		c.Assert(expr.Clone(), NotNil)
	}

	// test error
	mock := mockExpr{
		isStatic: false,
		err:      errors.New("must error"),
	}

	errTbl := []struct {
		arg interface{}
		op  opcode.Op
	}{
		{1, opcode.AndAnd},
		{0, opcode.OrOr},
		{1, opcode.LogicXor},
	}

	for _, t := range errTbl {
		expr := NewBinaryOperation(t.op, Value{t.arg}, mock)
		_, err := expr.Eval(nil, nil)
		c.Assert(err, NotNil)

		expr = NewBinaryOperation(t.op, mock, Value{t.arg})
		_, err = expr.Eval(nil, nil)
		c.Assert(err, NotNil)
	}

	mock.err = nil
	mock.val = errors.New("invalid value type")

	for _, t := range errTbl {
		expr := NewBinaryOperation(t.op, Value{t.arg}, mock)
		_, err := expr.Eval(nil, nil)
		c.Assert(err, NotNil)

		expr = NewBinaryOperation(t.op, mock, Value{t.arg})
		_, err = expr.Eval(nil, nil)
		c.Assert(err, NotNil)
	}

	expr := BinaryOperation{
		L:  Value{1},
		R:  Value{1},
		Op: opcode.Plus,
	}

	_, err := expr.evalLogicOp(nil, nil)
	c.Assert(err, NotNil)
}

func (s *testBinOpSuite) TestBitOp(c *C) {
	tbl := []struct {
		lhs interface{}
		op  opcode.Op
		rhs interface{}
		ret interface{}
	}{
		{1, opcode.And, 1, 1},
		{1, opcode.Or, 1, 1},
		{1, opcode.Xor, 1, 0},
		{1, opcode.LeftShift, 1, 2},
		{2, opcode.RightShift, 1, 1},
		{nil, opcode.And, 1, nil},
		{1, opcode.And, nil, nil},
		{nil, opcode.Or, 1, nil},
		{nil, opcode.Xor, 1, nil},
		{nil, opcode.LeftShift, 1, nil},
		{nil, opcode.RightShift, 1, nil},
	}

	for _, t := range tbl {
		expr := NewBinaryOperation(t.op, Value{t.lhs}, Value{t.rhs})
		v, err := expr.Eval(nil, nil)
		c.Assert(err, IsNil)

		switch x := t.ret.(type) {
		case nil:
			c.Assert(v, IsNil)
		case int:
			c.Assert(v, DeepEquals, uint64(x))
		}
	}

	// test error
	errTbl := []struct {
		arg interface{}
		op  opcode.Op
	}{
		{1, opcode.And},
		{1, opcode.Or},
		{1, opcode.Xor},
		{1, opcode.LeftShift},
		{1, opcode.RightShift},
	}

	mock := mockExpr{
		val: errors.New("error object"),
	}
	for _, t := range errTbl {
		expr := NewBinaryOperation(t.op, Value{t.arg}, mock)
		_, err := expr.Eval(nil, nil)
		c.Assert(err, NotNil)

		expr = NewBinaryOperation(t.op, mock, Value{t.arg})
		_, err = expr.Eval(nil, nil)
		c.Assert(err, NotNil)
	}

	mock.val = nil
	mock.err = errors.New("must error")

	expr := &BinaryOperation{
		Op: opcode.And,
		L:  Value{1},
		R:  mock,
	}
	_, err := expr.Eval(nil, nil)
	c.Assert(err, NotNil)

	expr.Op = opcode.Plus
	expr.R = Value{1}
	_, err = expr.evalBitOp(nil, nil)
	c.Assert(err, NotNil)
}

func (s *testBinOpSuite) TestNumericOp(c *C) {
	tbl := []struct {
		lhs interface{}
		op  opcode.Op
		rhs interface{}
		ret interface{}
	}{
		// plus
		{1, opcode.Plus, 1, 2},
		{1, opcode.Plus, uint64(1), 2},
		{1, opcode.Plus, "1", 2},
		{1, opcode.Plus, mysql.NewDecimalFromInt(1, 0), 2},
		{uint64(1), opcode.Plus, 1, 2},
		{uint64(1), opcode.Plus, uint64(1), 2},
		{1, opcode.Plus, []byte("1"), 2},
		{1, opcode.Plus, mysql.Hex{Value: 1}, 2},
		{1, opcode.Plus, mysql.Bit{Value: 1, Width: 1}, 2},
		{1, opcode.Plus, mysql.Enum{Name: "a", Value: 1}, 2},
		{1, opcode.Plus, mysql.Set{Name: "a", Value: 1}, 2},

		// minus
		{1, opcode.Minus, 1, 0},
		{1, opcode.Minus, uint64(1), 0},
		{1, opcode.Minus, float64(1), 0},
		{1, opcode.Minus, mysql.NewDecimalFromInt(1, 0), 0},
		{uint64(1), opcode.Minus, 1, 0},
		{uint64(1), opcode.Minus, uint64(1), 0},
		{mysql.NewDecimalFromInt(1, 0), opcode.Minus, 1, 0},
		{"1", opcode.Minus, []byte("1"), 0},

		// mul
		{1, opcode.Mul, 1, 1},
		{1, opcode.Mul, uint64(1), 1},
		{1, opcode.Mul, float64(1), 1},
		{1, opcode.Mul, mysql.NewDecimalFromInt(1, 0), 1},
		{uint64(1), opcode.Mul, 1, 1},
		{uint64(1), opcode.Mul, uint64(1), 1},
		{mysql.Time{}, opcode.Mul, 0, 0},
		{mysql.ZeroDuration, opcode.Mul, 0, 0},
		{mysql.Time{Time: time.Now(), Fsp: 0, Type: mysql.TypeDatetime}, opcode.Mul, 0, 0},
		{mysql.Time{Time: time.Now(), Fsp: 6, Type: mysql.TypeDatetime}, opcode.Mul, 0, 0},
		{mysql.Duration{Duration: 100000000, Fsp: 6}, opcode.Mul, 0, 0},

		// div
		{1, opcode.Div, float64(1), 1},
		{1, opcode.Div, float64(0), nil},
		{1, opcode.Div, 2, 0.5},
		{1, opcode.Div, 0, nil},

		// int div
		{1, opcode.IntDiv, 2, 0},
		{1, opcode.IntDiv, uint64(2), 0},
		{1, opcode.IntDiv, 0, nil},
		{1, opcode.IntDiv, uint64(0), nil},
		{uint64(1), opcode.IntDiv, 2, 0},
		{uint64(1), opcode.IntDiv, uint64(2), 0},
		{uint64(1), opcode.IntDiv, 0, nil},
		{uint64(1), opcode.IntDiv, uint64(0), nil},
		{1.0, opcode.IntDiv, 2.0, 0},
		{1.0, opcode.IntDiv, 0, nil},

		// mod
		{10, opcode.Mod, 2, 0},
		{10, opcode.Mod, uint64(2), 0},
		{10, opcode.Mod, 0, nil},
		{10, opcode.Mod, uint64(0), nil},
		{-10, opcode.Mod, uint64(2), 0},
		{uint64(10), opcode.Mod, 2, 0},
		{uint64(10), opcode.Mod, uint64(2), 0},
		{uint64(10), opcode.Mod, 0, nil},
		{uint64(10), opcode.Mod, uint64(0), nil},
		{uint64(10), opcode.Mod, -2, 0},
		{float64(10), opcode.Mod, 2, 0},
		{float64(10), opcode.Mod, 0, nil},
		{mysql.NewDecimalFromInt(10, 0), opcode.Mod, 2, 0},
		{mysql.NewDecimalFromInt(10, 0), opcode.Mod, 0, nil},
	}

	for _, t := range tbl {
		expr := NewBinaryOperation(t.op, Value{t.lhs}, Value{t.rhs})
		v, err := expr.Eval(nil, nil)
		c.Assert(err, IsNil)
		switch v.(type) {
		case nil:
			c.Assert(t.ret, IsNil)
		default:
			// we use float64 as the result type check for all.
			f, err := types.ToFloat64(v)
			c.Assert(err, IsNil)

			r, err := types.ToFloat64(t.ret)
			c.Assert(err, IsNil)
			c.Assert(r, Equals, f)
		}
	}

	// test error
	expr := &BinaryOperation{}
	_, err := expr.evalPlus(1, 1)
	c.Assert(err, NotNil)

	_, err = expr.evalMinus(1, 1)
	c.Assert(err, NotNil)

	_, err = expr.evalMul(1, 1)
	c.Assert(err, NotNil)

	_, err = expr.evalDiv("abc", 1)
	c.Assert(err, NotNil)

	_, err = expr.evalDiv(float64(1), "abc")
	c.Assert(err, NotNil)

	_, err = expr.evalDiv(1, "abc")
	c.Assert(err, NotNil)

	_, err = expr.evalIntDiv("abc", 1)
	c.Assert(err, NotNil)

	_, err = expr.evalIntDiv(1, "abc")
	c.Assert(err, NotNil)

	_, err = expr.evalMod("abc", 1)
	c.Assert(err, NotNil)

	expr.L = Value{1}
	expr.R = Value{1}
	_, err = expr.evalArithmeticOp(nil, nil)
	c.Assert(err, NotNil)

	expr.L = mockExpr{err: errors.New("must error")}
	_, err = expr.evalArithmeticOp(nil, nil)
	c.Assert(err, NotNil)

	expr.L = Value{"abc"}
	expr.R = Value{1}
	_, err = expr.evalArithmeticOp(nil, nil)
	c.Assert(err, NotNil)

	expr.L = Value{[]byte("abc")}
	expr.R = Value{1}
	_, err = expr.evalArithmeticOp(nil, nil)
	c.Assert(err, NotNil)

	expr.L = Value{1}
	expr.R = Value{"abc"}
	_, err = expr.evalArithmeticOp(nil, nil)
	c.Assert(err, NotNil)

	expr.Op = 0
	_, err = expr.Eval(nil, nil)
	c.Assert(err, NotNil)

	expr.L = NewTestRow(1, 2)
	expr.R = Value{nil}
	expr.Op = opcode.Plus

	_, err = expr.Eval(nil, nil)
	c.Assert(err, NotNil)

	expr.Op = opcode.LE
	_, err = expr.Eval(nil, nil)
	c.Assert(err, NotNil)

	expr.R = NewTestRow(1, 2)
	expr.Op = opcode.Plus
	_, err = expr.Eval(nil, nil)
	c.Assert(err, NotNil)

	expr.L = NewTestRow(1, 2)
	expr.R = NewTestRow(1, 2)
	expr.Op = opcode.Plus
	_, err = expr.Eval(nil, nil)
	c.Assert(err, NotNil)
}
