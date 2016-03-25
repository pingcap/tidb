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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testEvaluatorSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testEvaluatorSuite struct {
}

func parseExpr(c *C, expr string) ast.ExprNode {
	s, err := parser.ParseOneStmt("select "+expr, "", "")
	c.Assert(err, IsNil)
	stmt := s.(*ast.SelectStmt)
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
		c.Assert(valStr, Equals, ca.resultStr, Commentf("for %s", ca.exprStr))
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

	// When expression value changed, result set back to null.
	valExpr := ast.NewValueExpr(1)
	whenClause := &ast.WhenClause{Expr: ast.NewValueExpr(1), Result: ast.NewValueExpr(1)}
	caseExpr := &ast.CaseExpr{
		Value:       valExpr,
		WhenClauses: []*ast.WhenClause{whenClause},
	}
	ctx := mock.NewContext()
	v, err := Eval(ctx, caseExpr)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(1))
	valExpr.SetValue(4)
	v, err = Eval(ctx, caseExpr)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)
}

func (s *testEvaluatorSuite) TestCall(c *C) {
	ctx := mock.NewContext()

	// Test case for correct number of arguments
	expr := &ast.FuncCallExpr{
		FnName: model.NewCIStr("date"),
		Args:   []ast.ExprNode{ast.NewValueExpr("2015-12-21 11:11:11")},
	}
	v, err := Eval(ctx, expr)
	c.Assert(err, IsNil)
	value, ok := v.(mysql.Time)
	c.Assert(ok, IsTrue)
	c.Assert(value.String(), Equals, "2015-12-21")

	// Test case for unlimited upper bound
	expr = &ast.FuncCallExpr{
		FnName: model.NewCIStr("concat"),
		Args: []ast.ExprNode{ast.NewValueExpr("Ti"),
			ast.NewValueExpr("D"), ast.NewValueExpr("B")},
	}
	v, err = Eval(ctx, expr)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, "TiDB")

	// Test case for unknown function
	expr = &ast.FuncCallExpr{
		FnName: model.NewCIStr("unknown"),
		Args:   []ast.ExprNode{},
	}
	_, err = Eval(ctx, expr)
	c.Assert(err, NotNil)

	// Test case for invalid number of arguments, violating the lower bound
	expr = &ast.FuncCallExpr{
		FnName: model.NewCIStr("date"),
		Args:   []ast.ExprNode{},
	}
	_, err = Eval(ctx, expr)
	c.Assert(err, NotNil)

	// Test case for invalid number of arguments, violating the upper bound
	expr = &ast.FuncCallExpr{
		FnName: model.NewCIStr("date"),
		Args: []ast.ExprNode{ast.NewValueExpr("2015-12-21"),
			ast.NewValueExpr("2015-12-22")},
	}
	_, err = Eval(ctx, expr)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestCast(c *C) {
	f := types.NewFieldType(mysql.TypeLonglong)

	expr := &ast.FuncCastExpr{
		Expr: ast.NewValueExpr(1),
		Tp:   f,
	}
	ctx := mock.NewContext()
	v, err := Eval(ctx, expr)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, int64(1))

	f.Flag |= mysql.UnsignedFlag
	v, err = Eval(ctx, expr)
	c.Assert(err, IsNil)
	c.Assert(v, Equals, uint64(1))

	f.Tp = mysql.TypeString
	f.Charset = charset.CharsetBin
	v, err = Eval(ctx, expr)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, []byte("1"))

	f.Tp = mysql.TypeString
	f.Charset = "utf8"
	v, err = Eval(ctx, expr)
	c.Assert(err, IsNil)
	c.Assert(v, DeepEquals, "1")

	expr.Expr = ast.NewValueExpr(nil)
	v, err = Eval(ctx, expr)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)
}

func (s *testEvaluatorSuite) TestExtract(c *C) {
	str := "2011-11-11 10:10:10.123456"
	tbl := []struct {
		Unit   string
		Expect int64
	}{
		{"MICROSECOND", 123456},
		{"SECOND", 10},
		{"MINUTE", 10},
		{"HOUR", 10},
		{"DAY", 11},
		{"WEEK", 45},
		{"MONTH", 11},
		{"QUARTER", 4},
		{"YEAR", 2011},
		{"SECOND_MICROSECOND", 10123456},
		{"MINUTE_MICROSECOND", 1010123456},
		{"MINUTE_SECOND", 1010},
		{"HOUR_MICROSECOND", 101010123456},
		{"HOUR_SECOND", 101010},
		{"HOUR_MINUTE", 1010},
		{"DAY_MICROSECOND", 11101010123456},
		{"DAY_SECOND", 11101010},
		{"DAY_MINUTE", 111010},
		{"DAY_HOUR", 1110},
		{"YEAR_MONTH", 201111},
	}
	ctx := mock.NewContext()
	for _, t := range tbl {
		e := &ast.FuncCallExpr{
			FnName: model.NewCIStr("EXTRACT"),
			Args:   []ast.ExprNode{ast.NewValueExpr(t.Unit), ast.NewValueExpr(str)},
		}
		v, err := Eval(ctx, e)
		c.Assert(err, IsNil)
		c.Assert(v, Equals, t.Expect)
	}

	// Test nil
	e := &ast.FuncCallExpr{
		FnName: model.NewCIStr("EXTRACT"),
		Args:   []ast.ExprNode{ast.NewValueExpr("SECOND"), ast.NewValueExpr(nil)},
	}

	v, err := Eval(ctx, e)
	c.Assert(err, IsNil)
	c.Assert(v, IsNil)
}

func (s *testEvaluatorSuite) TestPatternIn(c *C) {
	cases := []testCase{
		{
			exprStr:   "1 not in (1, 2, 3)",
			resultStr: "0",
		},
		{
			exprStr:   "1 in (1, 2, 3)",
			resultStr: "1",
		},
		{
			exprStr:   "1 in (2, 3)",
			resultStr: "0",
		},
		{
			exprStr:   "NULL in (2, 3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "NULL not in (2, 3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "NULL in (NULL, 3)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "1 in (1, NULL)",
			resultStr: "1",
		},
		{
			exprStr:   "1 in (NULL, 1)",
			resultStr: "1",
		},
		{
			exprStr:   "2 in (1, NULL)",
			resultStr: "<nil>",
		},
		{
			exprStr:   "(-(23)++46/51*+51) in (+23)",
			resultStr: "0",
		},
	}
	s.runTests(c, cases)
}

func (s *testEvaluatorSuite) TestIsNull(c *C) {
	cases := []testCase{
		{
			exprStr:   "1 IS NULL",
			resultStr: "0",
		},
		{
			exprStr:   "1 IS NOT NULL",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NULL",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NOT NULL",
			resultStr: "0",
		},
	}
	s.runTests(c, cases)
}

func (s *testEvaluatorSuite) TestIsTruth(c *C) {
	cases := []testCase{
		{
			exprStr:   "1 IS TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "2 IS TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "0 IS TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "NULL IS TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "1 IS FALSE",
			resultStr: "0",
		},
		{
			exprStr:   "2 IS FALSE",
			resultStr: "0",
		},
		{
			exprStr:   "0 IS FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NOT FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "1 IS NOT TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "2 IS NOT TRUE",
			resultStr: "0",
		},
		{
			exprStr:   "0 IS NOT TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "NULL IS NOT TRUE",
			resultStr: "1",
		},
		{
			exprStr:   "1 IS NOT FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "2 IS NOT FALSE",
			resultStr: "1",
		},
		{
			exprStr:   "0 IS NOT FALSE",
			resultStr: "0",
		},
		{
			exprStr:   "NULL IS NOT FALSE",
			resultStr: "1",
		},
	}
	s.runTests(c, cases)
}

func (s *testEvaluatorSuite) TestLike(c *C) {
	tbl := []struct {
		pattern string
		input   string
		escape  byte
		match   bool
	}{
		{"", "a", '\\', false},
		{"a", "a", '\\', true},
		{"a", "b", '\\', false},
		{"aA", "aA", '\\', true},
		{"_", "a", '\\', true},
		{"_", "ab", '\\', false},
		{"__", "b", '\\', false},
		{"_ab", "AAB", '\\', true},
		{"%", "abcd", '\\', true},
		{"%", "", '\\', true},
		{"%a", "AAA", '\\', true},
		{"%b", "AAA", '\\', false},
		{"b%", "BBB", '\\', true},
		{"%a%", "BBB", '\\', false},
		{"%a%", "BAB", '\\', true},
		{"a%", "BBB", '\\', false},
		{`\%a`, `%a`, '\\', true},
		{`\%a`, `aa`, '\\', false},
		{`\_a`, `_a`, '\\', true},
		{`\_a`, `aa`, '\\', false},
		{`\\_a`, `\xa`, '\\', true},
		{`\a\b`, `\a\b`, '\\', true},
		{"%%_", `abc`, '\\', true},
		{`+_a`, `_a`, '+', true},
		{`+%a`, `%a`, '+', true},
		{`\%a`, `%a`, '+', false},
		{`++a`, `+a`, '+', true},
		{`++_a`, `+xa`, '+', true},
	}
	for _, v := range tbl {
		patChars, patTypes := compilePattern(v.pattern, v.escape)
		match := doMatch(v.input, patChars, patTypes)
		c.Assert(match, Equals, v.match, Commentf("%v", v))
	}
	cases := []testCase{
		{
			exprStr:   "'a' LIKE ''",
			resultStr: "0",
		},
		{
			exprStr:   "'a' LIKE 'a'",
			resultStr: "1",
		},
		{
			exprStr:   "'a' LIKE 'b'",
			resultStr: "0",
		},
		{
			exprStr:   "'aA' LIKE 'Aa'",
			resultStr: "1",
		},
		{
			exprStr:   "'aAb' LIKE 'Aa%'",
			resultStr: "1",
		},
		{
			exprStr:   "'aAb' LIKE 'Aa_'",
			resultStr: "1",
		},
	}
	s.runTests(c, cases)
}

func (s *testEvaluatorSuite) TestRegexp(c *C) {
	tbl := []struct {
		pattern string
		input   string
		match   int64
	}{
		{"^$", "a", 0},
		{"a", "a", 1},
		{"a", "b", 0},
		{"aA", "aA", 1},
		{".", "a", 1},
		{"^.$", "ab", 0},
		{"..", "b", 0},
		{".ab", "aab", 1},
		{".*", "abcd", 1},
	}
	ctx := mock.NewContext()
	for _, v := range tbl {
		pattern := &ast.PatternRegexpExpr{
			Pattern: ast.NewValueExpr(v.pattern),
			Expr:    ast.NewValueExpr(v.input),
		}
		match, err := Eval(ctx, pattern)
		c.Assert(err, IsNil)
		c.Assert(match, Equals, v.match, Commentf("%v", v))
	}
}

func (s *testEvaluatorSuite) TestUnaryOp(c *C) {
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
		{float64(1.0), opcode.Plus, float64(1.0)},
		{int64(1), opcode.Plus, int64(1)},
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
		{float64(1.0), opcode.Minus, float64(-1.0)},
		{int64(1), opcode.Minus, int64(-1)},
		{int64(1), opcode.Minus, int64(-1)},
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
	ctx := mock.NewContext()
	expr := &ast.UnaryOperationExpr{}
	for i, t := range tbl {
		expr.Op = t.op
		expr.V = ast.NewValueExpr(t.arg)
		result, err := Eval(ctx, expr)
		c.Assert(err, IsNil)
		c.Assert(result, DeepEquals, t.result, Commentf("%d", i))
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
		expr := &ast.UnaryOperationExpr{Op: t.op, V: ast.NewValueExpr(t.arg)}

		result, err := Eval(ctx, expr)
		c.Assert(err, IsNil)

		ret, err := types.Compare(result, t.result)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, 0)
	}
}

func (s *testEvaluatorSuite) TestColumnNameExpr(c *C) {
	ctx := mock.NewContext()
	value1 := ast.NewValueExpr(1)
	rf := &ast.ResultField{Expr: value1}
	expr := &ast.ColumnNameExpr{Refer: rf}

	result, err := Eval(ctx, expr)
	c.Assert(err, IsNil)
	c.Assert(result, Equals, int64(1))

	value2 := ast.NewValueExpr(2)
	rf.Expr = value2
	result, err = Eval(ctx, expr)
	c.Assert(err, IsNil)
	c.Assert(result, Equals, int64(2))
}

func (s *testEvaluatorSuite) TestAggFuncAvg(c *C) {
	ctx := mock.NewContext()
	avg := &ast.AggregateFuncExpr{
		F: ast.AggFuncAvg,
	}
	avg.CurrentGroup = "emptyGroup"
	result, err := Eval(ctx, avg)
	c.Assert(err, IsNil)
	// Empty group should return nil.
	c.Assert(result, IsNil)

	avg.Args = []ast.ExprNode{ast.NewValueExpr(2)}
	avg.Update()
	avg.Args = []ast.ExprNode{ast.NewValueExpr(4)}
	avg.Update()

	result, err = Eval(ctx, avg)
	c.Assert(err, IsNil)
	expect, _ := mysql.ConvertToDecimal(3)
	v, ok := result.(mysql.Decimal)
	c.Assert(ok, IsTrue)
	c.Assert(v.Equals(expect), IsTrue)
}

func (s *testEvaluatorSuite) TestGetTimeValue(c *C) {
	v, err := GetTimeValue(nil, "2012-12-12 00:00:00", mysql.TypeTimestamp, mysql.MinFsp)
	c.Assert(err, IsNil)

	timeValue, ok := v.(mysql.Time)
	c.Assert(ok, IsTrue)
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	ctx := mock.NewContext()
	variable.BindSessionVars(ctx)
	sessionVars := variable.GetSessionVars(ctx)

	sessionVars.Systems["timestamp"] = ""
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, mysql.MinFsp)
	c.Assert(err, IsNil)

	timeValue, ok = v.(mysql.Time)
	c.Assert(ok, IsTrue)
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	sessionVars.Systems["timestamp"] = "0"
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, mysql.MinFsp)
	c.Assert(err, IsNil)

	timeValue, ok = v.(mysql.Time)
	c.Assert(ok, IsTrue)
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	delete(sessionVars.Systems, "timestamp")
	v, err = GetTimeValue(ctx, "2012-12-12 00:00:00", mysql.TypeTimestamp, mysql.MinFsp)
	c.Assert(err, IsNil)

	timeValue, ok = v.(mysql.Time)
	c.Assert(ok, IsTrue)
	c.Assert(timeValue.String(), Equals, "2012-12-12 00:00:00")

	sessionVars.Systems["timestamp"] = "1234"

	tbl := []struct {
		Expr interface{}
		Ret  interface{}
	}{
		{"2012-12-12 00:00:00", "2012-12-12 00:00:00"},
		{CurrentTimestamp, time.Unix(1234, 0).Format(mysql.TimeFormat)},
		{ZeroTimestamp, "0000-00-00 00:00:00"},
		{ast.NewValueExpr("2012-12-12 00:00:00"), "2012-12-12 00:00:00"},
		{ast.NewValueExpr(int64(0)), "0000-00-00 00:00:00"},
		{ast.NewValueExpr(nil), nil},
		{&ast.FuncCallExpr{FnName: model.NewCIStr(CurrentTimestamp)}, CurrentTimestamp},
		{&ast.UnaryOperationExpr{Op: opcode.Minus, V: ast.NewValueExpr(int64(0))}, "0000-00-00 00:00:00"},
	}

	for i, t := range tbl {
		comment := Commentf("expr: %d", i)
		v, err := GetTimeValue(ctx, t.Expr, mysql.TypeTimestamp, mysql.MinFsp)
		c.Assert(err, IsNil)

		switch x := v.(type) {
		case mysql.Time:
			c.Assert(x.String(), DeepEquals, t.Ret, comment)
		default:
			c.Assert(x, DeepEquals, t.Ret, comment)
		}
	}

	errTbl := []struct {
		Expr interface{}
	}{
		{"2012-13-12 00:00:00"},
		{ast.NewValueExpr("2012-13-12 00:00:00")},
		{ast.NewValueExpr(int64(1))},
		{&ast.FuncCallExpr{FnName: model.NewCIStr("xxx")}},
		{&ast.UnaryOperationExpr{Op: opcode.Minus, V: ast.NewValueExpr(int64(1))}},
	}

	for _, t := range errTbl {
		_, err := GetTimeValue(ctx, t.Expr, mysql.TypeTimestamp, mysql.MinFsp)
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestIsCurrentTimeExpr(c *C) {
	v := IsCurrentTimeExpr(ast.NewValueExpr("abc"))
	c.Assert(v, IsFalse)

	v = IsCurrentTimeExpr(&ast.FuncCallExpr{FnName: model.NewCIStr("CURRENT_TIMESTAMP")})
	c.Assert(v, IsTrue)
}
