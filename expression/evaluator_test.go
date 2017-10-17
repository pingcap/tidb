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
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
)

var _ = Suite(&testEvaluatorSuite{})

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testEvaluatorSuite struct {
	*parser.Parser
	ctx context.Context
}

func (s *testEvaluatorSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	s.ctx = mock.NewContext()
}

func (s *testEvaluatorSuite) TearDownSuite(c *C) {
}

func (s *testEvaluatorSuite) kindToFieldType(kind byte) types.FieldType {
	ft := types.FieldType{}
	switch kind {
	case types.KindNull:
		ft.Tp = mysql.TypeNull
	case types.KindInt64:
		ft.Tp = mysql.TypeLonglong
	case types.KindUint64:
		ft.Tp = mysql.TypeLonglong
		ft.Flag |= mysql.UnsignedFlag
	case types.KindMinNotNull:
		ft.Tp = mysql.TypeLonglong
	case types.KindMaxValue:
		ft.Tp = mysql.TypeLonglong
	case types.KindFloat32:
		ft.Tp = mysql.TypeDouble
	case types.KindFloat64:
		ft.Tp = mysql.TypeDouble
	case types.KindString:
		ft.Tp = mysql.TypeVarString
	case types.KindBytes:
		ft.Tp = mysql.TypeVarString
	case types.KindMysqlEnum:
		ft.Tp = mysql.TypeEnum
	case types.KindMysqlSet:
		ft.Tp = mysql.TypeSet
	case types.KindInterface:
		ft.Tp = mysql.TypeVarString
	case types.KindMysqlDecimal:
		ft.Tp = mysql.TypeNewDecimal
	case types.KindMysqlDuration:
		ft.Tp = mysql.TypeDuration
	case types.KindMysqlTime:
		ft.Tp = mysql.TypeDatetime
	case types.KindBinaryLiteral:
		ft.Tp = mysql.TypeVarString
	case types.KindMysqlBit:
		ft.Tp = mysql.TypeBit
	}
	return ft
}

func (s *testEvaluatorSuite) datumsToConstants(datums []types.Datum) []Expression {
	constants := make([]Expression, 0, len(datums))
	for _, d := range datums {
		ft := s.kindToFieldType(d.Kind())
		ft.Flen, ft.Decimal = types.UnspecifiedLength, types.UnspecifiedLength
		constants = append(constants, &Constant{Value: d, RetType: &ft})
	}
	return constants
}

func (s *testEvaluatorSuite) primitiveValsToConstants(args []interface{}) []Expression {
	cons := s.datumsToConstants(types.MakeDatums(args...))
	for i, arg := range args {
		types.DefaultTypeForValue(arg, cons[i].GetType())
	}
	return cons
}

func (s *testEvaluatorSuite) TestSleep(c *C) {
	defer testleak.AfterTest(c)()
	ctx := mock.NewContext()
	sessVars := ctx.GetSessionVars()

	fc := funcs[ast.Sleep]
	// non-strict model
	sessVars.StrictSQLMode = false
	d := make([]types.Datum, 1)
	f, err := fc.getFunction(ctx, s.datumsToConstants(d))
	c.Assert(err, IsNil)
	ret, isNull, err := f.evalInt(nil)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	c.Assert(ret, Equals, int64(0))
	d[0].SetInt64(-1)
	f, err = fc.getFunction(ctx, s.datumsToConstants(d))
	c.Assert(err, IsNil)
	ret, isNull, err = f.evalInt(nil)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	c.Assert(ret, Equals, int64(0))

	// for error case under the strict model
	sessVars.StrictSQLMode = true
	d[0].SetNull()
	_, err = fc.getFunction(ctx, s.datumsToConstants(d))
	c.Assert(err, IsNil)
	ret, isNull, err = f.evalInt(nil)
	c.Assert(err, NotNil)
	c.Assert(isNull, IsFalse)
	d[0].SetFloat64(-2.5)
	_, err = fc.getFunction(ctx, s.datumsToConstants(d))
	c.Assert(err, IsNil)
	ret, isNull, err = f.evalInt(nil)
	c.Assert(err, NotNil)
	c.Assert(isNull, IsFalse)

	// strict model
	d[0].SetFloat64(0.5)
	start := time.Now()
	f, err = fc.getFunction(ctx, s.datumsToConstants(d))
	c.Assert(err, IsNil)
	ret, isNull, err = f.evalInt(nil)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	c.Assert(ret, Equals, int64(0))
	sub := time.Since(start)
	c.Assert(sub.Nanoseconds(), GreaterEqual, int64(0.5*1e9))

	// quit when context canceled.
	d[0].SetFloat64(2)
	f, err = fc.getFunction(ctx, s.datumsToConstants(d))
	c.Assert(err, IsNil)
	start = time.Now()
	go func() {
		time.Sleep(1 * time.Second)
		ctx.Cancel()
	}()
	ret, isNull, err = f.evalInt(nil)
	sub = time.Since(start)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	c.Assert(ret, Equals, int64(1))
	c.Assert(sub.Nanoseconds(), LessEqual, int64(2*1e9))
	c.Assert(sub.Nanoseconds(), GreaterEqual, int64(1*1e9))
}

func (s *testEvaluatorSuite) TestBinopComparison(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		lhs    interface{}
		op     string
		rhs    interface{}
		result int64 // 0 for false, 1 for true
	}{
		// test EQ
		{1, ast.EQ, 2, 0},
		{false, ast.EQ, false, 1},
		{false, ast.EQ, true, 0},
		{true, ast.EQ, true, 1},
		{true, ast.EQ, false, 0},
		{"1", ast.EQ, true, 1},
		{"1", ast.EQ, false, 0},

		// test NEQ
		{1, ast.NE, 2, 1},
		{false, ast.NE, false, 0},
		{false, ast.NE, true, 1},
		{true, ast.NE, true, 0},
		{"1", ast.NE, true, 0},
		{"1", ast.NE, false, 1},

		// test GT, GE
		{1, ast.GT, 0, 1},
		{1, ast.GT, 1, 0},
		{1, ast.GE, 1, 1},
		{3.14, ast.GT, 3, 1},
		{3.14, ast.GE, 3.14, 1},

		// test LT, LE
		{1, ast.LT, 2, 1},
		{1, ast.LT, 1, 0},
		{1, ast.LE, 1, 1},
	}
	for _, t := range tbl {
		fc := funcs[t.op]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.lhs, t.rhs)))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		val, err := v.ToBool(s.ctx.GetSessionVars().StmtCtx)
		c.Assert(err, IsNil)
		c.Assert(val, Equals, t.result)
	}

	// test nil
	nilTbl := []struct {
		lhs interface{}
		op  string
		rhs interface{}
	}{
		{nil, ast.EQ, nil},
		{nil, ast.EQ, 1},
		{nil, ast.NE, nil},
		{nil, ast.NE, 1},
		{nil, ast.LT, nil},
		{nil, ast.LT, 1},
		{nil, ast.LE, nil},
		{nil, ast.LE, 1},
		{nil, ast.GT, nil},
		{nil, ast.GT, 1},
		{nil, ast.GE, nil},
		{nil, ast.GE, 1},
	}

	for _, t := range nilTbl {
		fc := funcs[t.op]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.lhs, t.rhs)))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(v.Kind(), Equals, types.KindNull)
	}
}

func (s *testEvaluatorSuite) TestBinopLogic(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		lhs interface{}
		op  string
		rhs interface{}
		ret interface{}
	}{
		{nil, ast.LogicAnd, 1, nil},
		{nil, ast.LogicAnd, 0, 0},
		{nil, ast.LogicOr, 1, 1},
		{nil, ast.LogicOr, 0, nil},
		{nil, ast.LogicXor, 1, nil},
		{nil, ast.LogicXor, 0, nil},
		{1, ast.LogicAnd, 0, 0},
		{1, ast.LogicAnd, 1, 1},
		{1, ast.LogicOr, 0, 1},
		{1, ast.LogicOr, 1, 1},
		{0, ast.LogicOr, 0, 0},
		{1, ast.LogicXor, 0, 1},
		{1, ast.LogicXor, 1, 0},
		{0, ast.LogicXor, 0, 0},
		{0, ast.LogicXor, 1, 1},
	}
	for _, t := range tbl {
		fc := funcs[t.op]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.lhs, t.rhs)))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		switch x := t.ret.(type) {
		case nil:
			c.Assert(v.Kind(), Equals, types.KindNull)
		case int:
			c.Assert(v, testutil.DatumEquals, types.NewDatum(int64(x)))
		}
	}
}

func (s *testEvaluatorSuite) TestBinopBitop(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		lhs interface{}
		op  string
		rhs interface{}
		ret interface{}
	}{
		{1, ast.And, 1, 1},
		{1, ast.Or, 1, 1},
		{1, ast.Xor, 1, 0},
		{1, ast.LeftShift, 1, 2},
		{2, ast.RightShift, 1, 1},
		{nil, ast.And, 1, nil},
		{1, ast.And, nil, nil},
		{nil, ast.Or, 1, nil},
		{nil, ast.Xor, 1, nil},
		{nil, ast.LeftShift, 1, nil},
		{nil, ast.RightShift, 1, nil},
	}

	for _, t := range tbl {
		fc := funcs[t.op]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.lhs, t.rhs)))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)

		switch x := t.ret.(type) {
		case nil:
			c.Assert(v.Kind(), Equals, types.KindNull)
		case int:
			c.Assert(v, testutil.DatumEquals, types.NewDatum(uint64(x)))
		}
	}
}

func (s *testEvaluatorSuite) TestBinopNumeric(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		lhs interface{}
		op  string
		rhs interface{}
		ret interface{}
	}{
		// plus
		{1, ast.Plus, 1, 2},
		{1, ast.Plus, uint64(1), 2},
		{1, ast.Plus, "1", 2},
		{1, ast.Plus, types.NewDecFromInt(1), 2},
		{uint64(1), ast.Plus, 1, 2},
		{uint64(1), ast.Plus, uint64(1), 2},
		{uint64(1), ast.Plus, -1, 0},
		{1, ast.Plus, []byte("1"), 2},
		{1, ast.Plus, types.NewBinaryLiteralFromUint(1, -1), 2},
		{1, ast.Plus, types.Enum{Name: "a", Value: 1}, 2},
		{1, ast.Plus, types.Set{Name: "a", Value: 1}, 2},

		// minus
		{1, ast.Minus, 1, 0},
		{1, ast.Minus, uint64(1), 0},
		{1, ast.Minus, float64(1), 0},
		{1, ast.Minus, types.NewDecFromInt(1), 0},
		{uint64(1), ast.Minus, 1, 0},
		{uint64(1), ast.Minus, uint64(1), 0},
		{types.NewDecFromInt(1), ast.Minus, 1, 0},
		{"1", ast.Minus, []byte("1"), 0},

		// mul
		{1, ast.Mul, 1, 1},
		{1, ast.Mul, uint64(1), 1},
		{1, ast.Mul, float64(1), 1},
		{1, ast.Mul, types.NewDecFromInt(1), 1},
		{uint64(1), ast.Mul, 1, 1},
		{uint64(1), ast.Mul, uint64(1), 1},
		{types.Time{Time: types.FromDate(0, 0, 0, 0, 0, 0, 0)}, ast.Mul, 0, 0},
		{types.ZeroDuration, ast.Mul, 0, 0},
		{types.Time{Time: types.FromGoTime(time.Now()), Fsp: 0, Type: mysql.TypeDatetime}, ast.Mul, 0, 0},
		{types.Time{Time: types.FromGoTime(time.Now()), Fsp: 6, Type: mysql.TypeDatetime}, ast.Mul, 0, 0},
		{types.Duration{Duration: 100000000, Fsp: 6}, ast.Mul, 0, 0},

		// div
		{1, ast.Div, float64(1), 1},
		{1, ast.Div, float64(0), nil},
		{1, ast.Div, 2, 0.5},
		{1, ast.Div, 0, nil},

		// int div
		{1, ast.IntDiv, 2, 0},
		{1, ast.IntDiv, uint64(2), 0},
		{1, ast.IntDiv, 0, nil},
		{1, ast.IntDiv, uint64(0), nil},
		{uint64(1), ast.IntDiv, 2, 0},
		{uint64(1), ast.IntDiv, uint64(2), 0},
		{uint64(1), ast.IntDiv, 0, nil},
		{uint64(1), ast.IntDiv, uint64(0), nil},
		{1.0, ast.IntDiv, 2.0, 0},

		// mod
		{10, ast.Mod, 2, 0},
		{10, ast.Mod, uint64(2), 0},
		{10, ast.Mod, 0, nil},
		{10, ast.Mod, uint64(0), nil},
		{-10, ast.Mod, uint64(2), 0},
		{uint64(10), ast.Mod, 2, 0},
		{uint64(10), ast.Mod, uint64(2), 0},
		{uint64(10), ast.Mod, 0, nil},
		{uint64(10), ast.Mod, uint64(0), nil},
		{uint64(10), ast.Mod, -2, 0},
		{float64(10), ast.Mod, 2, 0},
		{float64(10), ast.Mod, 0, nil},
		{types.NewDecFromInt(10), ast.Mod, 2, 0},
		{types.NewDecFromInt(10), ast.Mod, 0, nil},
	}

	for _, t := range tbl {
		fc := funcs[t.op]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.lhs, t.rhs)))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		switch v.Kind() {
		case types.KindNull:
			c.Assert(t.ret, IsNil)
		default:
			// we use float64 as the result type check for all.
			sc := s.ctx.GetSessionVars().StmtCtx
			f, err := v.ToFloat64(sc)
			c.Assert(err, IsNil)
			d := types.NewDatum(t.ret)
			r, err := d.ToFloat64(sc)
			c.Assert(err, IsNil)
			c.Assert(r, Equals, f)
		}
	}

	testcases := []struct {
		lhs interface{}
		op  string
		rhs interface{}
	}{
		// div
		{1, ast.Div, float64(0)},
		{1, ast.Div, 0},
		// int div
		{1, ast.IntDiv, 0},
		{1, ast.IntDiv, uint64(0)},
		{uint64(1), ast.IntDiv, 0},
		{uint64(1), ast.IntDiv, uint64(0)},
		// mod
		{10, ast.Mod, 0},
		{10, ast.Mod, uint64(0)},
		{uint64(10), ast.Mod, 0},
		{uint64(10), ast.Mod, uint64(0)},
		{float64(10), ast.Mod, 0},
		{types.NewDecFromInt(10), ast.Mod, 0},
	}

	oldInSelectStmt := s.ctx.GetSessionVars().StmtCtx.InSelectStmt
	s.ctx.GetSessionVars().StmtCtx.InSelectStmt = false
	oldSQLMode := s.ctx.GetSessionVars().SQLMode
	s.ctx.GetSessionVars().SQLMode |= mysql.ModeErrorForDivisionByZero
	oldInInsertStmt := s.ctx.GetSessionVars().StmtCtx.InInsertStmt
	s.ctx.GetSessionVars().StmtCtx.InInsertStmt = true
	for _, t := range testcases {
		fc := funcs[t.op]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.lhs, t.rhs)))
		c.Assert(err, IsNil)
		_, err = evalBuiltinFunc(f, nil)
		c.Assert(err, NotNil)
	}

	oldDividedByZeroAsWarning := s.ctx.GetSessionVars().StmtCtx.DividedByZeroAsWarning
	s.ctx.GetSessionVars().StmtCtx.DividedByZeroAsWarning = true
	for _, t := range testcases {
		fc := funcs[t.op]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.lhs, t.rhs)))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(v.Kind(), Equals, types.KindNull)
	}

	s.ctx.GetSessionVars().StmtCtx.InSelectStmt = oldInSelectStmt
	s.ctx.GetSessionVars().SQLMode = oldSQLMode
	s.ctx.GetSessionVars().StmtCtx.InInsertStmt = oldInInsertStmt
	s.ctx.GetSessionVars().StmtCtx.DividedByZeroAsWarning = oldDividedByZeroAsWarning
}

func (s *testEvaluatorSuite) TestExtract(c *C) {
	defer testleak.AfterTest(c)()
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
	for _, t := range tbl {
		fc := funcs[ast.Extract]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.Unit, str)))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, types.NewDatum(t.Expect))
	}

	// Test nil
	fc := funcs[ast.Extract]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums("SECOND", nil)))
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(v.Kind(), Equals, types.KindNull)
}

func (s *testEvaluatorSuite) TestLike(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
		input   string
		pattern string
		match   int
	}{
		{"a", "", 0},
		{"a", "a", 1},
		{"a", "b", 0},
		{"aA", "Aa", 0},
		{"aAb", "Aa%", 0},
		{"aAb", "aA_", 1},
	}
	for _, tt := range tests {
		fc := funcs[ast.Like]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tt.input, tt.pattern, 0)))
		c.Assert(err, IsNil)
		r, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(r, testutil.DatumEquals, types.NewDatum(tt.match))
	}
}

func (s *testEvaluatorSuite) TestRegexp(c *C) {
	defer testleak.AfterTest(c)()
	tests := []struct {
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
	for _, tt := range tests {
		fc := funcs[ast.Regexp]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tt.input, tt.pattern)))
		c.Assert(err, IsNil)
		match, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(match, testutil.DatumEquals, types.NewDatum(tt.match), Commentf("%v", tt))
	}
}

func (s *testEvaluatorSuite) TestUnaryOp(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		arg    interface{}
		op     string
		result interface{}
	}{
		// test NOT.
		{1, ast.UnaryNot, int64(0)},
		{0, ast.UnaryNot, int64(1)},
		{nil, ast.UnaryNot, nil},
		{types.NewBinaryLiteralFromUint(0, -1), ast.UnaryNot, int64(1)},
		{types.NewBinaryLiteralFromUint(1, -1), ast.UnaryNot, int64(0)},
		{types.Enum{Name: "a", Value: 1}, ast.UnaryNot, int64(0)},
		{types.Set{Name: "a", Value: 1}, ast.UnaryNot, int64(0)},

		// test BitNeg.
		{nil, ast.BitNeg, nil},
		{-1, ast.BitNeg, uint64(0)},

		// test Minus.
		{nil, ast.UnaryMinus, nil},
		{float64(1.0), ast.UnaryMinus, float64(-1.0)},
		{int64(1), ast.UnaryMinus, int64(-1)},
		{int64(1), ast.UnaryMinus, int64(-1)},
		{uint64(1), ast.UnaryMinus, -int64(1)},
		{"1.0", ast.UnaryMinus, -1.0},
		{[]byte("1.0"), ast.UnaryMinus, -1.0},
		{types.NewBinaryLiteralFromUint(1, -1), ast.UnaryMinus, -1.0},
		{true, ast.UnaryMinus, int64(-1)},
		{false, ast.UnaryMinus, int64(0)},
		{types.Enum{Name: "a", Value: 1}, ast.UnaryMinus, -1.0},
		{types.Set{Name: "a", Value: 1}, ast.UnaryMinus, -1.0},
	}
	for i, t := range tbl {
		fc := funcs[t.op]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.arg)))
		c.Assert(err, IsNil)
		result, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)
		c.Assert(result, testutil.DatumEquals, types.NewDatum(t.result), Commentf("%d", i))
	}

	tbl = []struct {
		arg    interface{}
		op     string
		result interface{}
	}{
		{types.NewDecFromInt(1), ast.UnaryMinus, types.NewDecFromInt(-1)},
		{types.ZeroDuration, ast.UnaryMinus, new(types.MyDecimal)},
		{types.Time{Time: types.FromGoTime(time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)), Type: mysql.TypeDatetime, Fsp: 0}, ast.UnaryMinus, types.NewDecFromInt(-20091110230000)},
	}

	for _, t := range tbl {
		fc := funcs[t.op]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(t.arg)))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		result, err := evalBuiltinFunc(f, nil)
		c.Assert(err, IsNil)

		expect := types.NewDatum(t.result)
		ret, err := result.CompareDatum(s.ctx.GetSessionVars().StmtCtx, &expect)
		c.Assert(err, IsNil)
		c.Assert(ret, Equals, 0, Commentf("%v %s", t.arg, t.op))
	}
}

func (s *testEvaluatorSuite) TestMod(c *C) {
	fc := funcs[ast.Mod]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(234, 10)))
	c.Assert(err, IsNil)
	r, err := evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(r, testutil.DatumEquals, types.NewIntDatum(4))
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(29, 9)))
	c.Assert(err, IsNil)
	r, err = evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(r, testutil.DatumEquals, types.NewIntDatum(2))
	f, err = fc.getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(34.5, 3)))
	c.Assert(err, IsNil)
	r, err = evalBuiltinFunc(f, nil)
	c.Assert(err, IsNil)
	c.Assert(r, testutil.DatumEquals, types.NewDatum(1.5))
}
