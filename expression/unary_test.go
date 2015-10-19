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

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
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
		{1, opcode.Not, int64(0)},
		{0, opcode.Not, int64(1)},
		{nil, opcode.Not, nil},
		{mysql.Hex{Value: 0}, opcode.Not, int64(1)},
		{mysql.Bit{Value: 0, Width: 1}, opcode.Not, int64(1)},
		{mysql.Enum{Name: "a", Value: 1}, opcode.Not, int64(0)},
		{mysql.Set{Name: "a", Value: 1}, opcode.Not, int64(0)},

		// test BitNeg.
		{nil, opcode.BitNeg, nil},
		{-1, opcode.BitNeg, uint64(0)},

		// test Plus.
		{nil, opcode.Plus, nil},
		{float32(1.0), opcode.Plus, float32(1.0)},
		{float64(1.0), opcode.Plus, float64(1.0)},
		{int(1), opcode.Plus, int(1)},
		{int64(1), opcode.Plus, int64(1)},
		{uint64(1), opcode.Plus, uint64(1)},
		{"1.0", opcode.Plus, "1.0"},
		{[]byte("1.0"), opcode.Plus, []byte("1.0")},
		{mysql.Hex{Value: 1}, opcode.Plus, mysql.Hex{Value: 1}},
		{mysql.Bit{Value: 1, Width: 1}, opcode.Plus, mysql.Bit{Value: 1, Width: 1}},
		{true, opcode.Plus, int64(1)},
		{false, opcode.Plus, int64(0)},
		{mysql.Enum{Name: "a", Value: 1}, opcode.Plus, mysql.Enum{Name: "a", Value: 1}},
		{mysql.Set{Name: "a", Value: 1}, opcode.Plus, mysql.Set{Name: "a", Value: 1}},

		// test Minus.
		{nil, opcode.Minus, nil},
		{float32(1.0), opcode.Minus, float32(-1.0)},
		{float64(1.0), opcode.Minus, float64(-1.0)},
		{int(1), opcode.Minus, int(-1)},
		{int64(1), opcode.Minus, int64(-1)},
		{uint(1), opcode.Minus, -int64(1)},
		{uint8(1), opcode.Minus, -int64(1)},
		{uint16(1), opcode.Minus, -int64(1)},
		{uint32(1), opcode.Minus, -int64(1)},
		{uint64(1), opcode.Minus, -int64(1)},
		{"1.0", opcode.Minus, -1.0},
		{[]byte("1.0"), opcode.Minus, -1.0},
		{mysql.Hex{Value: 1}, opcode.Minus, -1.0},
		{mysql.Bit{Value: 1, Width: 1}, opcode.Minus, -1.0},
		{true, opcode.Minus, int64(-1)},
		{false, opcode.Minus, int64(0)},
		{mysql.Enum{Name: "a", Value: 1}, opcode.Minus, -1.0},
		{mysql.Set{Name: "a", Value: 1}, opcode.Minus, -1.0},
	}

	for _, t := range tbl {
		expr := NewUnaryOperation(t.op, Value{t.arg})

		exprc := expr.Clone()

		result, err := exprc.Eval(nil, nil)
		c.Assert(err, IsNil)
		c.Assert(result, DeepEquals, t.result)
	}

	tbl = []struct {
		arg    interface{}
		op     opcode.Op
		result interface{}
	}{
		{mysql.NewDecimalFromInt(1, 0), opcode.Plus, mysql.NewDecimalFromInt(1, 0)},
		{mysql.Duration{Duration: time.Duration(838*3600 + 59*60 + 59), Fsp: mysql.DefaultFsp}, opcode.Plus,
			mysql.Duration{Duration: time.Duration(838*3600 + 59*60 + 59), Fsp: mysql.DefaultFsp}},
		{mysql.Time{Time: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), Type: mysql.TypeDatetime, Fsp: 0}, opcode.Plus, mysql.Time{Time: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), Type: mysql.TypeDatetime, Fsp: 0}},

		{mysql.NewDecimalFromInt(1, 0), opcode.Minus, mysql.NewDecimalFromInt(-1, 0)},
		{mysql.ZeroDuration, opcode.Minus, mysql.NewDecimalFromInt(0, 0)},
		{mysql.Time{Time: time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC), Type: mysql.TypeDatetime, Fsp: 0}, opcode.Minus, mysql.NewDecimalFromInt(-20091110230000, 0)},
	}

	for _, t := range tbl {
		expr := NewUnaryOperation(t.op, Value{t.arg})

		exprc := expr.Clone()

		result, err := exprc.Eval(nil, nil)
		c.Assert(err, IsNil)

		ret, err := types.Compare(result, t.result)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, 0)
	}

	// test String().
	strTbl := []struct {
		expr     Expression
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
		{mockExpr{}, opcode.Minus},
		{mockExpr{}, opcode.EQ},
	}

	// test error clone
	expr := NewUnaryOperation(opcode.Not, mockExpr{err: errors.New("must error")})
	c.Assert(expr.Clone(), NotNil)

	for _, t := range errTbl {
		expr := NewUnaryOperation(t.op, Value{t.arg})
		_, err := expr.Eval(nil, nil)
		c.Assert(err, NotNil)
	}
}
