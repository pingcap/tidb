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
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
	"time"
)

var _ = Suite(&testEvaluatorSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testEvaluatorSuite struct {
}

func parseExpr(c *C, expr string) ast.ExprNode {
	lexer := parser.NewLexer("select " + expr)
	parser.YYParse(lexer)
	if parser.YYParse(lexer) != 0 || len(lexer.Errors()) != 0 {
		c.Fatal(lexer.Errors()[0], expr)
	}
	stmt := lexer.Stmts()[0].(*ast.SelectStmt)
	return stmt.Fields.Fields[0].Expr
}

type testCase struct {
	exprStr   string
	resultStr string
}

func (s *testEvaluatorSuite) runTests(c *C, cases []testCase) {
	ctx := mock.NewContext()
	for _, ca := range cases {
		expr := parseExpr(c, ca.exprStr)
		val, err := Eval(ctx, expr)
		c.Assert(err, IsNil)
		valStr := fmt.Sprintf("%v", val)
		c.Assert(valStr, Equals, ca.resultStr)
	}
}

func (s *testEvaluatorSuite) TestBetween(c *C) {
	cases := []testCase{
		{exprStr: "1 between 2 and 3", resultStr: "0"},
		{exprStr: "1 not between 2 and 3", resultStr: "1"},
	}
	s.runTests(c, cases)
}

func (s *testEvaluatorSuite) TestBinopComparison(c *C) {
	ctx := mock.NewContext()
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
		expr := &ast.BinaryOperationExpr{Op: t.op, L: ast.NewValueExpr(t.lhs), R: ast.NewValueExpr(t.rhs)}
		v, err := Eval(ctx, expr)
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
		expr := &ast.BinaryOperationExpr{Op: t.op, L: ast.NewValueExpr(t.lhs), R: ast.NewValueExpr(t.rhs)}
		v, err := Eval(ctx, expr)
		c.Assert(err, IsNil)
		c.Assert(v, IsNil)
	}
}

func (s *testEvaluatorSuite) TestBinopLogic(c *C) {
	ctx := mock.NewContext()
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
		expr := &ast.BinaryOperationExpr{Op: t.op, L: ast.NewValueExpr(t.lhs), R: ast.NewValueExpr(t.rhs)}
		v, err := Eval(ctx, expr)
		c.Assert(err, IsNil)
		switch x := t.ret.(type) {
		case nil:
			c.Assert(v, IsNil)
		case int:
			c.Assert(v, DeepEquals, int64(x))
		}
	}
}

func (s *testEvaluatorSuite) TestBinopBitop(c *C) {
	ctx := mock.NewContext()
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
		expr := &ast.BinaryOperationExpr{Op: t.op, L: ast.NewValueExpr(t.lhs), R: ast.NewValueExpr(t.rhs)}
		v, err := Eval(ctx, expr)
		c.Assert(err, IsNil)

		switch x := t.ret.(type) {
		case nil:
			c.Assert(v, IsNil)
		case int:
			c.Assert(v, DeepEquals, uint64(x))
		}
	}
}

func (s *testEvaluatorSuite) TestBinopNumeric(c *C) {
	ctx := mock.NewContext()
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
		expr := &ast.BinaryOperationExpr{Op: t.op, L: ast.NewValueExpr(t.lhs), R: ast.NewValueExpr(t.rhs)}
		v, err := Eval(ctx, expr)
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
}

func (s *testEvaluatorSuite) TestCaseWhen(c *C) {
	cases := []testCase{
		{
			exprStr:   "case 1 when 1 then 'str1' when 2 then 'str2' end",
			resultStr: "str1",
		},
		{
			exprStr:   "case 2 when 1 then 'str1' when 2 then 'str2' end",
			resultStr: "str2",
		},
		{
			exprStr:   "case 3 when 1 then 'str1' when 2 then 'str2' end",
			resultStr: "<nil>",
		},
		{
			exprStr:   "case 4 when 1 then 'str1' when 2 then 'str2' else 'str3' end",
			resultStr: "str3",
		},
	}
	s.runTests(c, cases)
}

func (s *testEvaluatorSuite) TestConvert(c *C) {
	ctx := mock.NewContext()
	tbl := []struct {
		str    string
		cs     string
		result string
	}{
		{"haha", "utf8", "haha"},
		{"haha", "ascii", "haha"},
	}
	for _, v := range tbl {
		f := &ast.FuncConvertExpr{
			Expr:    ast.NewValueExpr(v.str),
			Charset: v.cs,
		}

		c.Assert(f.IsStatic(), Equals, true)

		fs := f.String()
		c.Assert(len(fs), Greater, 0)

		r, err := Eval(ctx, f)
		c.Assert(err, IsNil)
		s, ok := r.(string)
		c.Assert(ok, Equals, true)
		c.Assert(s, Equals, v.result)
	}

	// Test case for error
	errTbl := []struct {
		str    interface{}
		cs     string
		result string
	}{
		{"haha", "wrongcharset", "haha"},
	}
	for _, v := range errTbl {
		f := &ast.FuncConvertExpr{
			Expr:    ast.NewValueExpr(v.str),
			Charset: v.cs,
		}

		_, err := Eval(ctx, f)
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestDateArith(c *C) {
	c.Skip("to be implement")
	ctx := mock.NewContext()
	input := "2011-11-11 10:10:10"
	e := &ast.FuncDateArithExpr{
		Op:   ast.DateAdd,
		Date: ast.NewValueExpr(input),
		DateArithInterval: ast.DateArithInterval{
			Interval: ast.NewValueExpr(1),
			Unit:     "DAY",
		},
	}
	c.Assert(e.IsStatic(), IsTrue)
	_, err := Eval(ctx, e)
	c.Assert(err, IsNil)

	// Test null.
	nullTbl := []struct {
		Op       ast.DateArithType
		Unit     string
		Date     interface{}
		Interval interface{}
	}{
		{ast.DateAdd, "DAY", nil, "1"},
		{ast.DateAdd, "DAY", input, nil},
	}
	for _, t := range nullTbl {
		e := &ast.FuncDateArithExpr{
			Op:   t.Op,
			Date: ast.NewValueExpr(t.Date),
			DateArithInterval: ast.DateArithInterval{
				Interval: ast.NewValueExpr(t.Interval),
				Unit:     t.Unit,
			},
		}
		v, err := Eval(ctx, e)
		c.Assert(err, IsNil)
		c.Assert(v, IsNil)
		e.Op = ast.DateSub
		v, err = Eval(ctx, e)
		c.Assert(err, IsNil)
		c.Assert(v, IsNil)
	}

	// Test eval.
	tbl := []struct {
		Unit      string
		Interval  interface{}
		AddExpect string
		SubExpect string
	}{
		{"MICROSECOND", "1000", "2011-11-11 10:10:10.001000", "2011-11-11 10:10:09.999000"},
		{"MICROSECOND", 1000, "2011-11-11 10:10:10.001000", "2011-11-11 10:10:09.999000"},
		{"SECOND", "10", "2011-11-11 10:10:20", "2011-11-11 10:10:00"},
		{"MINUTE", "10", "2011-11-11 10:20:10", "2011-11-11 10:00:10"},
		{"HOUR", "10", "2011-11-11 20:10:10", "2011-11-11 00:10:10"},
		{"DAY", "11", "2011-11-22 10:10:10", "2011-10-31 10:10:10"},
		{"WEEK", "2", "2011-11-25 10:10:10", "2011-10-28 10:10:10"},
		{"MONTH", "2", "2012-01-11 10:10:10", "2011-09-11 10:10:10"},
		{"QUARTER", "4", "2012-11-11 10:10:10", "2010-11-11 10:10:10"},
		{"YEAR", "2", "2013-11-11 10:10:10", "2009-11-11 10:10:10"},
		{"SECOND_MICROSECOND", "10.00100000", "2011-11-11 10:10:20.100000", "2011-11-11 10:09:59.900000"},
		{"SECOND_MICROSECOND", "10.0010000000", "2011-11-11 10:10:30", "2011-11-11 10:09:50"},
		{"SECOND_MICROSECOND", "10.0010000010", "2011-11-11 10:10:30.000010", "2011-11-11 10:09:49.999990"},
		{"MINUTE_MICROSECOND", "10:10.100", "2011-11-11 10:20:20.100000", "2011-11-11 09:59:59.900000"},
		{"MINUTE_SECOND", "10:10", "2011-11-11 10:20:20", "2011-11-11 10:00:00"},
		{"HOUR_MICROSECOND", "10:10:10.100", "2011-11-11 20:20:20.100000", "2011-11-10 23:59:59.900000"},
		{"HOUR_SECOND", "10:10:10", "2011-11-11 20:20:20", "2011-11-11 00:00:00"},
		{"HOUR_MINUTE", "10:10", "2011-11-11 20:20:10", "2011-11-11 00:00:10"},
		{"DAY_MICROSECOND", "11 10:10:10.100", "2011-11-22 20:20:20.100000", "2011-10-30 23:59:59.900000"},
		{"DAY_SECOND", "11 10:10:10", "2011-11-22 20:20:20", "2011-10-31 00:00:00"},
		{"DAY_MINUTE", "11 10:10", "2011-11-22 20:20:10", "2011-10-31 00:00:10"},
		{"DAY_HOUR", "11 10", "2011-11-22 20:10:10", "2011-10-31 00:10:10"},
		{"YEAR_MONTH", "11-1", "2022-12-11 10:10:10", "2000-10-11 10:10:10"},
		{"YEAR_MONTH", "11-11", "2023-10-11 10:10:10", "1999-12-11 10:10:10"},
	}
	for _, t := range tbl {
		e := &ast.FuncDateArithExpr{
			Op:   ast.DateAdd,
			Date: ast.NewValueExpr(input),
			DateArithInterval: ast.DateArithInterval{
				Interval: ast.NewValueExpr(t.Interval),
				Unit:     t.Unit,
			},
		}
		v, err := Eval(ctx, e)
		c.Assert(err, IsNil)
		value, ok := v.(mysql.Time)
		c.Assert(ok, IsTrue)
		c.Assert(value.String(), Equals, t.AddExpect)

		e.Op = ast.DateSub
		v, err = Eval(ctx, e)
		c.Assert(err, IsNil)
		value, ok = v.(mysql.Time)
		c.Assert(ok, IsTrue)
		c.Assert(value.String(), Equals, t.SubExpect)
	}

	// Test eval for adddate and subdate with days form
	tblDays := []struct {
		Interval  interface{}
		AddExpect string
		SubExpect string
	}{
		{"20", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{19.88, "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"19.88", "2011-11-30 10:10:10", "2011-10-23 10:10:10"},
		{"20-11", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"20,11", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"1000", "2014-08-07 10:10:10", "2009-02-14 10:10:10"},
		{"true", "2011-11-12 10:10:10", "2011-11-10 10:10:10"},
	}
	for _, t := range tblDays {
		e := &ast.FuncDateArithExpr{
			Op:   ast.DateAdd,
			Date: ast.NewValueExpr(input),
			DateArithInterval: ast.DateArithInterval{
				Interval: ast.NewValueExpr(t.Interval),
				Unit:     "day",
				Form:     ast.DateArithDaysForm,
			},
		}
		v, err := Eval(ctx, e)
		c.Assert(err, IsNil)
		value, ok := v.(mysql.Time)
		c.Assert(ok, IsTrue)
		c.Assert(value.String(), Equals, t.AddExpect)

		e.Op = ast.DateSub
		v, err = Eval(ctx, e)
		c.Assert(err, IsNil)
		value, ok = v.(mysql.Time)
		c.Assert(ok, IsTrue)
		c.Assert(value.String(), Equals, t.SubExpect)
	}

	// Test error.
	errInput := "20111111 10:10:10"
	errTbl := []struct {
		Unit     string
		Interval interface{}
	}{
		{"MICROSECOND", "abc1000"},
		{"MICROSECOND", ""},
		{"SECOND_MICROSECOND", "10"},
		{"MINUTE_MICROSECOND", "10.0000"},
		{"MINUTE_MICROSECOND", "10:10:10.0000"},

		// MySQL support, but tidb not.
		{"HOUR_MICROSECOND", "10:10.0000"},
		{"YEAR_MONTH", "10 1"},
	}
	for _, t := range errTbl {
		e := &ast.FuncDateArithExpr{
			Op:   ast.DateAdd,
			Date: ast.NewValueExpr(input),
			DateArithInterval: ast.DateArithInterval{
				Interval: ast.NewValueExpr(t.Interval),
				Unit:     t.Unit,
			},
		}
		_, err := Eval(ctx, e)
		c.Assert(err, NotNil)
		e.Date = ast.NewValueExpr(errInput)
		v, err := Eval(ctx, e)
		c.Assert(err, NotNil, Commentf("%s", v))

		e.Op = ast.DateSub
		_, err = Eval(ctx, e)
		c.Assert(err, NotNil)
		e.Date = ast.NewValueExpr(errInput)
		v, err = Eval(ctx, e)
		c.Assert(err, NotNil, Commentf("%s", v))
	}
}
