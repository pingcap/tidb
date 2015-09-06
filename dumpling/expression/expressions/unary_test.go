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

package expressions

import (
	"errors"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/expression"
	mysql "github.com/pingcap/tidb/mysqldef"
	"github.com/pingcap/tidb/parser/opcode"
)

var _ = Suite(&testUnaryOperationSuite{})

type testUnaryOperationSuite struct {
}

func (s *testUnaryOperationSuite) TestUnaryOp(c *C) {
	tbl := []struct {
		arg    interface{}
		op     opcode.Op
		result interface{}
	}{
		// test NOT.
		{1, opcode.Not, int8(0)},
		{0, opcode.Not, int8(1)},
		{nil, opcode.Not, nil},

		// test BitNeg.
		{nil, opcode.BitNeg, nil},
		{-1, opcode.BitNeg, uint64(0)},

		// test Plus.
		{nil, opcode.Plus, nil},
		{float32(1.0), opcode.Plus, float32(1.0)},
		{float64(1.0), opcode.Plus, float64(1.0)},
		{int(1), opcode.Plus, int(1)},
		{int8(1), opcode.Plus, int8(1)},
		{int16(1), opcode.Plus, int16(1)},
		{int32(1), opcode.Plus, int32(1)},
		{int64(1), opcode.Plus, int64(1)},
		{uint(1), opcode.Plus, uint(1)},
		{uint8(1), opcode.Plus, uint8(1)},
		{uint16(1), opcode.Plus, uint16(1)},
		{uint32(1), opcode.Plus, uint32(1)},
		{uint64(1), opcode.Plus, uint64(1)},
		{"1.0", opcode.Plus, "1.0"},

		// test Minus.
		{nil, opcode.Minus, nil},
		{float32(1.0), opcode.Minus, float32(-1.0)},
		{float64(1.0), opcode.Minus, float64(-1.0)},
		{int(1), opcode.Minus, int(-1)},
		{int8(1), opcode.Minus, int8(-1)},
		{int16(1), opcode.Minus, int16(-1)},
		{int32(1), opcode.Minus, int32(-1)},
		{int64(1), opcode.Minus, int64(-1)},
		{uint(1), opcode.Minus, -int64(1)},
		{uint8(1), opcode.Minus, -int64(1)},
		{uint16(1), opcode.Minus, -int64(1)},
		{uint32(1), opcode.Minus, -int64(1)},
		{uint64(1), opcode.Minus, -int64(1)},
		{"1.0", opcode.Minus, -1.0},
	}

	for _, t := range tbl {
		expr := NewUnaryOperation(t.op, Value{t.arg})

		exprc, err := expr.Clone()
		c.Assert(err, IsNil)

		result, err := exprc.Eval(nil, nil)
		c.Assert(err, IsNil)
		c.Assert(result, Equals, t.result)
	}

	tbl = []struct {
		arg    interface{}
		op     opcode.Op
		result interface{}
	}{
		{mysql.NewDecimalFromInt(1, 0), opcode.Plus, mysql.NewDecimalFromInt(1, 0)},
		{mysql.Duration{time.Duration(838*3600 + 59*60 + 59), mysql.DefaultFsp}, opcode.Plus, mysql.Duration{time.Duration(838*3600 + 59*60 + 59), mysql.DefaultFsp}},
		{mysql.Time{time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), mysql.TypeDatetime, 0}, opcode.Plus, mysql.Time{time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), mysql.TypeDatetime, 0}},

		{mysql.NewDecimalFromInt(1, 0), opcode.Minus, mysql.NewDecimalFromInt(-1, 0)},
		{mysql.ZeroDuration, opcode.Minus, mysql.NewDecimalFromInt(0, 0)},
		{mysql.Time{time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), mysql.TypeDatetime, 0}, opcode.Minus, mysql.NewDecimalFromInt(-20091110230000, 0)},
	}

	for _, t := range tbl {
		expr := NewUnaryOperation(t.op, Value{t.arg})

		exprc, err := expr.Clone()
		c.Assert(err, IsNil)

		result, err := exprc.Eval(nil, nil)
		c.Assert(err, IsNil)

		ret, err := evalCompare(result, t.result)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, 0)
	}

	// test String().
	strTbl := []struct {
		expr     expression.Expression
		op       opcode.Op
		isStatic bool
	}{
		{NewBinaryOperation(opcode.EQ, Value{1}, Value{1}), opcode.Plus, true},
		{NewUnaryOperation(opcode.Not, Value{1}), opcode.Not, true},
		{Value{1}, opcode.Not, true},
		{Value{1}, opcode.Plus, true},
		{&PExpr{Value{1}}, opcode.Not, true},

		{NewBinaryOperation(opcode.EQ, Value{1}, Value{1}), opcode.Not, true},
		{NewBinaryOperation(opcode.NE, Value{1}, Value{1}), opcode.Not, true},
		{NewBinaryOperation(opcode.GT, Value{1}, Value{1}), opcode.Not, true},
		{NewBinaryOperation(opcode.GE, Value{1}, Value{1}), opcode.Not, true},
		{NewBinaryOperation(opcode.LT, Value{1}, Value{1}), opcode.Not, true},
		{NewBinaryOperation(opcode.LE, Value{1}, Value{1}), opcode.Not, true},
	}

	for _, t := range strTbl {
		expr := NewUnaryOperation(t.op, t.expr)

		c.Assert(expr.IsStatic(), Equals, t.isStatic)

		str := expr.String()
		c.Assert(len(str), Greater, 0)

	}

	// test error.
	errTbl := []struct {
		arg interface{}
		op  opcode.Op
	}{
		{mockExpr{}, opcode.Not},
		{mockExpr{}, opcode.BitNeg},
		{mockExpr{}, opcode.Plus},
		{false, opcode.Plus},
		{mockExpr{}, opcode.Minus},
		{false, opcode.Minus},
		{mockExpr{}, opcode.EQ},
	}

	// test error clone
	expr := NewUnaryOperation(opcode.Not, mockExpr{err: errors.New("must error")})
	_, err := expr.Clone()
	c.Assert(err, NotNil)

	for _, t := range errTbl {
		expr := NewUnaryOperation(t.op, Value{t.arg})
		_, err := expr.Eval(nil, nil)
		c.Assert(err, NotNil)
	}
}
