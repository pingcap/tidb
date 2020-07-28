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
	"math"
	"runtime"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tipb/go-tipb"
)

func (s *testEvaluatorSuite) TestAbs(c *C) {
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{int64(1), int64(1)},
		{uint64(1), uint64(1)},
		{int64(-1), int64(1)},
		{float64(3.14), float64(3.14)},
		{float64(-3.14), float64(3.14)},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.Abs]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Arg"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestCeil(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	tmpIT := sc.IgnoreTruncate
	sc.IgnoreTruncate = true
	defer func() {
		sc.IgnoreTruncate = tmpIT
	}()

	type testCase struct {
		arg    interface{}
		expect interface{}
		isNil  bool
		getErr bool
	}

	cases := []testCase{
		{nil, nil, true, false},
		{int64(1), int64(1), false, false},
		{float64(1.23), float64(2), false, false},
		{float64(-1.23), float64(-1), false, false},
		{"1.23", float64(2), false, false},
		{"-1.23", float64(-1), false, false},
		{"tidb", float64(0), false, false},
		{"1tidb", float64(1), false, false}}

	expressions := []Expression{
		&Constant{
			Value:   types.NewDatum(0),
			RetType: types.NewFieldType(mysql.TypeTiny),
		},
		&Constant{
			Value:   types.NewFloat64Datum(float64(12.34)),
			RetType: types.NewFieldType(mysql.TypeFloat),
		},
	}

	runCasesOn := func(funcName string, cases []testCase, exps []Expression) {
		for _, test := range cases {
			f, err := newFunctionForTest(s.ctx, funcName, s.primitiveValsToConstants([]interface{}{test.arg})...)
			c.Assert(err, IsNil)

			result, err := f.Eval(chunk.Row{})
			if test.getErr {
				c.Assert(err, NotNil)
			} else {
				c.Assert(err, IsNil)
				if test.isNil {
					c.Assert(result.Kind(), Equals, types.KindNull)
				} else {
					c.Assert(result, testutil.DatumEquals, types.NewDatum(test.expect))
				}
			}
		}

		for _, exp := range exps {
			_, err := funcs[funcName].getFunction(s.ctx, []Expression{exp})
			c.Assert(err, IsNil)
		}
	}

	runCasesOn(ast.Ceil, cases, expressions)
	runCasesOn(ast.Ceiling, cases, expressions)
}

func (s *testEvaluatorSuite) TestExp(c *C) {
	tests := []struct {
		args       interface{}
		expect     float64
		isNil      bool
		getWarning bool
		errMsg     string
	}{
		{nil, 0, true, false, ""},
		{int64(1), 2.718281828459045, false, false, ""},
		{float64(1.23), 3.4212295362896734, false, false, ""},
		{float64(-1.23), 0.2922925776808594, false, false, ""},
		{float64(0), 1, false, false, ""},
		{"0", 1, false, false, ""},
		{"tidb", 0, false, true, ""},
		{float64(100000), 0, false, true, "[types:1690]DOUBLE value is out of range in 'exp(100000)'"},
	}

	if runtime.GOARCH == "ppc64le" {
		tests[1].expect = 2.7182818284590455
	}

	for _, test := range tests {
		preWarningCnt := s.ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(s.ctx, ast.Exp, s.primitiveValsToConstants([]interface{}{test.args})...)
		c.Assert(err, IsNil)

		result, err := f.Eval(chunk.Row{})
		if test.getWarning {
			if test.errMsg != "" {
				c.Assert(err, NotNil)
				c.Assert(err.Error(), Equals, test.errMsg)
			} else {
				c.Assert(err, IsNil)
				c.Assert(s.ctx.GetSessionVars().StmtCtx.WarningCount(), Equals, preWarningCnt+1)
			}
		} else {
			c.Assert(err, IsNil)
			if test.isNil {
				c.Assert(result.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(result.GetFloat64(), Equals, test.expect)
			}
		}
	}

	_, err := funcs[ast.Exp].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestFloor(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	tmpIT := sc.IgnoreTruncate
	sc.IgnoreTruncate = true
	defer func() {
		sc.IgnoreTruncate = tmpIT
	}()

	genDuration := func(h, m, s int64) types.Duration {
		duration := time.Duration(h)*time.Hour +
			time.Duration(m)*time.Minute +
			time.Duration(s)*time.Second

		return types.Duration{Duration: duration, Fsp: types.DefaultFsp}
	}

	genTime := func(y, m, d int) types.Time {
		return types.NewTime(types.FromDate(y, m, d, 0, 0, 0, 0), mysql.TypeDatetime, types.DefaultFsp)
	}

	for _, test := range []struct {
		arg    interface{}
		expect interface{}
		isNil  bool
		getErr bool
	}{
		{nil, nil, true, false},
		{int64(1), int64(1), false, false},
		{float64(1.23), float64(1), false, false},
		{float64(-1.23), float64(-2), false, false},
		{"1.23", float64(1), false, false},
		{"-1.23", float64(-2), false, false},
		{"-1.b23", float64(-1), false, false},
		{"abce", float64(0), false, false},
		{genDuration(12, 59, 59), float64(125959), false, false},
		{genDuration(0, 12, 34), float64(1234), false, false},
		{genTime(2017, 7, 19), float64(20170719000000), false, false},
	} {
		f, err := newFunctionForTest(s.ctx, ast.Floor, s.primitiveValsToConstants([]interface{}{test.arg})...)
		c.Assert(err, IsNil)

		result, err := f.Eval(chunk.Row{})
		if test.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if test.isNil {
				c.Assert(result.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(result, testutil.DatumEquals, types.NewDatum(test.expect))
			}
		}
	}

	for _, exp := range []Expression{
		&Constant{
			Value:   types.NewDatum(0),
			RetType: types.NewFieldType(mysql.TypeTiny),
		},
		&Constant{
			Value:   types.NewFloat64Datum(float64(12.34)),
			RetType: types.NewFieldType(mysql.TypeFloat),
		},
	} {
		_, err := funcs[ast.Floor].getFunction(s.ctx, []Expression{exp})
		c.Assert(err, IsNil)
	}
}

func (s *testEvaluatorSuite) TestLog(c *C) {
	tests := []struct {
		args       []interface{}
		expect     float64
		isNil      bool
		getWarning bool
	}{
		{[]interface{}{nil}, 0, true, false},
		{[]interface{}{nil, nil}, 0, true, false},
		{[]interface{}{int64(100)}, 4.605170185988092, false, false},
		{[]interface{}{float64(100)}, 4.605170185988092, false, false},
		{[]interface{}{int64(10), int64(100)}, 2, false, false},
		{[]interface{}{float64(10), float64(100)}, 2, false, false},
		{[]interface{}{float64(-1)}, 0, true, false},
		{[]interface{}{float64(1), float64(2)}, 0, true, false},
		{[]interface{}{float64(0.5), float64(0.25)}, 2, false, false},
		{[]interface{}{"abc"}, 0, false, true},
	}

	for _, test := range tests {
		preWarningCnt := s.ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(s.ctx, ast.Log, s.primitiveValsToConstants(test.args)...)
		c.Assert(err, IsNil)

		result, err := f.Eval(chunk.Row{})
		if test.getWarning {
			c.Assert(err, IsNil)
			c.Assert(s.ctx.GetSessionVars().StmtCtx.WarningCount(), Equals, preWarningCnt+1)
		} else {
			c.Assert(err, IsNil)
			if test.isNil {
				c.Assert(result.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(result.GetFloat64(), Equals, test.expect)
			}
		}
	}

	_, err := funcs[ast.Log].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestLog2(c *C) {
	tests := []struct {
		args       interface{}
		expect     float64
		isNil      bool
		getWarning bool
	}{
		{nil, 0, true, false},
		{int64(16), 4, false, false},
		{float64(16), 4, false, false},
		{int64(5), 2.321928094887362, false, false},
		{int64(-1), 0, true, false},
		{"4abc", 0, false, true},
	}

	for _, test := range tests {
		preWarningCnt := s.ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(s.ctx, ast.Log2, s.primitiveValsToConstants([]interface{}{test.args})...)
		c.Assert(err, IsNil)

		result, err := f.Eval(chunk.Row{})
		if test.getWarning {
			c.Assert(err, IsNil)
			c.Assert(s.ctx.GetSessionVars().StmtCtx.WarningCount(), Equals, preWarningCnt+1)
		} else {
			c.Assert(err, IsNil)
			if test.isNil {
				c.Assert(result.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(result.GetFloat64(), Equals, test.expect)
			}
		}
	}

	_, err := funcs[ast.Log2].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestLog10(c *C) {
	tests := []struct {
		args       interface{}
		expect     float64
		isNil      bool
		getWarning bool
	}{
		{nil, 0, true, false},
		{int64(100), 2, false, false},
		{float64(100), 2, false, false},
		{int64(101), 2.0043213737826426, false, false},
		{int64(-1), 0, true, false},
		{"100abc", 0, false, true},
	}

	for _, test := range tests {
		preWarningCnt := s.ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(s.ctx, ast.Log10, s.primitiveValsToConstants([]interface{}{test.args})...)
		c.Assert(err, IsNil)

		result, err := f.Eval(chunk.Row{})
		if test.getWarning {
			c.Assert(err, IsNil)
			c.Assert(s.ctx.GetSessionVars().StmtCtx.WarningCount(), Equals, preWarningCnt+1)
		} else {
			c.Assert(err, IsNil)
			if test.isNil {
				c.Assert(result.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(result.GetFloat64(), Equals, test.expect)
			}
		}
	}

	_, err := funcs[ast.Log10].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestRand(c *C) {
	fc := funcs[ast.Rand]
	f, err := fc.getFunction(s.ctx, nil)
	c.Assert(err, IsNil)
	v, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(v.GetFloat64(), Less, float64(1))
	c.Assert(v.GetFloat64(), GreaterEqual, float64(0))

	// issue 3211
	f2, err := fc.getFunction(s.ctx, []Expression{&Constant{Value: types.NewIntDatum(20160101), RetType: types.NewFieldType(mysql.TypeLonglong)}})
	c.Assert(err, IsNil)
	randGen := NewWithSeed(20160101)
	for i := 0; i < 3; i++ {
		v, err = evalBuiltinFunc(f2, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v.GetFloat64(), Equals, randGen.Gen())
	}
}

func (s *testEvaluatorSuite) TestPow(c *C) {
	tbl := []struct {
		Arg []interface{}
		Ret float64
	}{
		{[]interface{}{1, 3}, 1},
		{[]interface{}{2, 2}, 4},
		{[]interface{}{4, 0.5}, 2},
		{[]interface{}{4, -2}, 0.0625},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.Pow]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Arg"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}

	errTbl := []struct {
		Arg []interface{}
	}{
		{[]interface{}{"test", "test"}},
		{[]interface{}{1, "test"}},
		{[]interface{}{10, 700}}, // added overflow test
	}

	errDtbl := tblToDtbl(errTbl)
	for i, t := range errDtbl {
		fc := funcs[ast.Pow]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Arg"]))
		c.Assert(err, IsNil)
		_, err = evalBuiltinFunc(f, chunk.Row{})
		if i == 2 {
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, "[types:1690]DOUBLE value is out of range in 'pow(10, 700)'")
		} else {
			c.Assert(err, IsNil)
		}
	}
	c.Assert(int(s.ctx.GetSessionVars().StmtCtx.WarningCount()), Equals, 3)
}

func (s *testEvaluatorSuite) TestRound(c *C) {
	newDec := types.NewDecFromStringForTest
	tbl := []struct {
		Arg []interface{}
		Ret interface{}
	}{
		{[]interface{}{-1.23}, -1},
		{[]interface{}{-1.23, 0}, -1},
		{[]interface{}{-1.58}, -2},
		{[]interface{}{1.58}, 2},
		{[]interface{}{1.298, 1}, 1.3},
		{[]interface{}{1.298}, 1},
		{[]interface{}{1.298, 0}, 1},
		{[]interface{}{23.298, -1}, 20},
		{[]interface{}{newDec("-1.23")}, newDec("-1")},
		{[]interface{}{newDec("-1.23"), 1}, newDec("-1.2")},
		{[]interface{}{newDec("-1.58")}, newDec("-2")},
		{[]interface{}{newDec("1.58")}, newDec("2")},
		{[]interface{}{newDec("1.58"), 1}, newDec("1.6")},
		{[]interface{}{newDec("23.298"), -1}, newDec("20")},
		{[]interface{}{nil, 2}, nil},
		{[]interface{}{1, -2012}, 0},
		{[]interface{}{1, -201299999999999}, 0},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.Round]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Arg"]))
		c.Assert(err, IsNil)
		switch f.(type) {
		case *builtinRoundWithFracIntSig:
			c.Assert(f.PbCode(), Equals, tipb.ScalarFuncSig_RoundWithFracInt)
		case *builtinRoundWithFracDecSig:
			c.Assert(f.PbCode(), Equals, tipb.ScalarFuncSig_RoundWithFracDec)
		case *builtinRoundWithFracRealSig:
			c.Assert(f.PbCode(), Equals, tipb.ScalarFuncSig_RoundWithFracReal)
		case *builtinRoundIntSig:
			c.Assert(f.PbCode(), Equals, tipb.ScalarFuncSig_RoundInt)
		case *builtinRoundDecSig:
			c.Assert(f.PbCode(), Equals, tipb.ScalarFuncSig_RoundDec)
		case *builtinRoundRealSig:
			c.Assert(f.PbCode(), Equals, tipb.ScalarFuncSig_RoundReal)
		}
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestTruncate(c *C) {
	newDec := types.NewDecFromStringForTest
	tbl := []struct {
		Arg []interface{}
		Ret interface{}
	}{
		{[]interface{}{-1.23, 0}, -1},
		{[]interface{}{1.58, 0}, 1},
		{[]interface{}{1.298, 1}, 1.2},
		{[]interface{}{123.2, -1}, 120},
		{[]interface{}{123.2, 100}, 123.2},
		{[]interface{}{123.2, -100}, 0},
		{[]interface{}{123.2, -100}, 0},
		{[]interface{}{1.797693134862315708145274237317043567981e+308, 2},
			1.797693134862315708145274237317043567981e+308},
		{[]interface{}{newDec("-1.23"), 0}, newDec("-1")},
		{[]interface{}{newDec("-1.23"), 1}, newDec("-1.2")},
		{[]interface{}{newDec("-11.23"), -1}, newDec("-10")},
		{[]interface{}{newDec("1.58"), 0}, newDec("1")},
		{[]interface{}{newDec("1.58"), 1}, newDec("1.5")},
		{[]interface{}{newDec("11.58"), -1}, newDec("10")},
		{[]interface{}{newDec("23.298"), -1}, newDec("20")},
		{[]interface{}{newDec("23.298"), -100}, newDec("0")},
		{[]interface{}{newDec("23.298"), 100}, newDec("23.298")},
		{[]interface{}{nil, 2}, nil},
		{[]interface{}{uint64(9223372036854775808), -10}, 9223372030000000000},
		{[]interface{}{9223372036854775807, -7}, 9223372036850000000},
		{[]interface{}{uint64(18446744073709551615), -10}, uint64(18446744070000000000)},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.Truncate]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Arg"]))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestCRC32(c *C) {
	tbl := []struct {
		Arg []interface{}
		Ret interface{}
	}{
		{[]interface{}{nil}, nil},
		{[]interface{}{""}, 0},
		{[]interface{}{-1}, 808273962},
		{[]interface{}{"-1"}, 808273962},
		{[]interface{}{"mysql"}, 2501908538},
		{[]interface{}{"MySQL"}, 3259397556},
		{[]interface{}{"hello"}, 907060870},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.CRC32]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Arg"]))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestConv(c *C) {
	cases := []struct {
		args     []interface{}
		expected interface{}
		isNil    bool
		getErr   bool
	}{
		{[]interface{}{"a", 16, 2}, "1010", false, false},
		{[]interface{}{"6E", 18, 8}, "172", false, false},
		{[]interface{}{"-17", 10, -18}, "-H", false, false},
		{[]interface{}{"-17", 10, 18}, "2D3FGB0B9CG4BD1H", false, false},
		{[]interface{}{nil, 10, 10}, "0", true, false},
		{[]interface{}{"+18aZ", 7, 36}, "1", false, false},
		{[]interface{}{"18446744073709551615", -10, 16}, "7FFFFFFFFFFFFFFF", false, false},
		{[]interface{}{"12F", -10, 16}, "C", false, false},
		{[]interface{}{"  FF ", 16, 10}, "255", false, false},
		{[]interface{}{"TIDB", 10, 8}, "0", false, false},
		{[]interface{}{"aa", 10, 2}, "0", false, false},
		{[]interface{}{" A", -10, 16}, "0", false, false},
		{[]interface{}{"a6a", 10, 8}, "0", false, false},
		{[]interface{}{"a6a", 1, 8}, "0", true, false},
	}

	for _, t := range cases {
		f, err := newFunctionForTest(s.ctx, ast.Conv, s.primitiveValsToConstants(t.args)...)
		c.Assert(err, IsNil)
		tp := f.GetType()
		c.Assert(tp.Tp, Equals, mysql.TypeVarString)
		c.Assert(tp.Charset, Equals, charset.CharsetUTF8MB4)
		c.Assert(tp.Collate, Equals, charset.CollationUTF8MB4)
		c.Assert(tp.Flag, Equals, uint(0))

		d, err := f.Eval(chunk.Row{})
		if t.getErr {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetString(), Equals, t.expected)
			}
		}
	}

	v := []struct {
		s    string
		base int64
		ret  string
	}{
		{"-123456D1f", 5, "-1234"},
		{"+12azD", 16, "12a"},
		{"+", 12, ""},
	}
	for _, t := range v {
		r := getValidPrefix(t.s, t.base)
		c.Assert(r, Equals, t.ret)
	}

	_, err := funcs[ast.Conv].getFunction(s.ctx, []Expression{NewZero(), NewZero(), NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestSign(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	tmpIT := sc.IgnoreTruncate
	sc.IgnoreTruncate = true
	defer func() {
		sc.IgnoreTruncate = tmpIT
	}()

	for _, t := range []struct {
		num []interface{}
		ret interface{}
	}{
		{[]interface{}{nil}, nil},
		{[]interface{}{1}, int64(1)},
		{[]interface{}{0}, int64(0)},
		{[]interface{}{-1}, int64(-1)},
		{[]interface{}{0.4}, int64(1)},
		{[]interface{}{-0.4}, int64(-1)},
		{[]interface{}{"1"}, int64(1)},
		{[]interface{}{"-1"}, int64(-1)},
		{[]interface{}{"1a"}, int64(1)},
		{[]interface{}{"-1a"}, int64(-1)},
		{[]interface{}{"a"}, int64(0)},
		{[]interface{}{uint64(9223372036854775808)}, int64(1)},
	} {
		fc := funcs[ast.Sign]
		f, err := fc.getFunction(s.ctx, s.primitiveValsToConstants(t.num))
		c.Assert(err, IsNil, Commentf("%v", t))
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil, Commentf("%v", t))
		c.Assert(v, testutil.DatumEquals, types.NewDatum(t.ret), Commentf("%v", t))
	}
}

func (s *testEvaluatorSuite) TestDegrees(c *C) {
	sc := s.ctx.GetSessionVars().StmtCtx
	sc.IgnoreTruncate = false
	cases := []struct {
		args       interface{}
		expected   float64
		isNil      bool
		getWarning bool
	}{
		{nil, 0, true, false},
		{int64(0), float64(0), false, false},
		{int64(1), float64(57.29577951308232), false, false},
		{float64(1), float64(57.29577951308232), false, false},
		{float64(math.Pi), float64(180), false, false},
		{float64(-math.Pi / 2), float64(-90), false, false},
		{"", float64(0), false, false},
		{"-2", float64(-114.59155902616465), false, false},
		{"abc", float64(0), false, true},
		{"+1abc", 57.29577951308232, false, true},
	}

	for _, t := range cases {
		preWarningCnt := s.ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(s.ctx, ast.Degrees, s.primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)
		d, err := f.Eval(chunk.Row{})
		if t.getWarning {
			c.Assert(err, IsNil)
			c.Assert(s.ctx.GetSessionVars().StmtCtx.WarningCount(), Equals, preWarningCnt+1)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetFloat64(), Equals, t.expected)
			}
		}
	}
	_, err := funcs[ast.Degrees].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestSqrt(c *C) {
	tbl := []struct {
		Arg []interface{}
		Ret interface{}
	}{
		{[]interface{}{nil}, nil},
		{[]interface{}{int64(1)}, float64(1)},
		{[]interface{}{float64(4)}, float64(2)},
		{[]interface{}{"4"}, float64(2)},
		{[]interface{}{"9"}, float64(3)},
		{[]interface{}{"-16"}, nil},
	}

	for _, t := range tbl {
		fc := funcs[ast.Sqrt]
		f, err := fc.getFunction(s.ctx, s.primitiveValsToConstants(t.Arg))
		c.Assert(err, IsNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, types.NewDatum(t.Ret), Commentf("%v", t))
	}
}

func (s *testEvaluatorSuite) TestPi(c *C) {
	f, err := funcs[ast.PI].getFunction(s.ctx, nil)
	c.Assert(err, IsNil)

	pi, err := evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(pi, testutil.DatumEquals, types.NewDatum(math.Pi))
}

func (s *testEvaluatorSuite) TestRadians(c *C) {
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{0, float64(0)},
		{float64(180), float64(math.Pi)},
		{-360, -2 * float64(math.Pi)},
		{"180", float64(math.Pi)},
	}

	Dtbl := tblToDtbl(tbl)
	for _, t := range Dtbl {
		fc := funcs[ast.Radians]
		f, err := fc.getFunction(s.ctx, s.datumsToConstants(t["Arg"]))
		c.Assert(err, IsNil)
		c.Assert(f, NotNil)
		v, err := evalBuiltinFunc(f, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}

	invalidArg := "notNum"
	fc := funcs[ast.Radians]
	f, err := fc.getFunction(s.ctx, s.datumsToConstants([]types.Datum{types.NewDatum(invalidArg)}))
	c.Assert(err, IsNil)
	_, err = evalBuiltinFunc(f, chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(int(s.ctx.GetSessionVars().StmtCtx.WarningCount()), Equals, 1)
}

func (s *testEvaluatorSuite) TestSin(c *C) {
	cases := []struct {
		args       interface{}
		expected   float64
		isNil      bool
		getWarning bool
	}{
		{nil, 0, true, false},
		{int64(0), float64(0), false, false},
		{math.Pi, float64(math.Sin(math.Pi)), false, false}, // Pie ==> 0
		{-math.Pi, float64(math.Sin(-math.Pi)), false, false},
		{math.Pi / 2, float64(math.Sin(math.Pi / 2)), false, false}, // Pie/2 ==> 1
		{-math.Pi / 2, float64(math.Sin(-math.Pi / 2)), false, false},
		{math.Pi / 6, float64(math.Sin(math.Pi / 6)), false, false}, // Pie/6(30 degrees) ==> 0.5
		{-math.Pi / 6, float64(math.Sin(-math.Pi / 6)), false, false},
		{math.Pi * 2, float64(math.Sin(math.Pi * 2)), false, false},
		{string("adfsdfgs"), 0, false, true},
		{"0.000", 0, false, false},
	}

	for _, t := range cases {
		preWarningCnt := s.ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(s.ctx, ast.Sin, s.primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)

		d, err := f.Eval(chunk.Row{})
		if t.getWarning {
			c.Assert(err, IsNil)
			c.Assert(s.ctx.GetSessionVars().StmtCtx.WarningCount(), Equals, preWarningCnt+1)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetFloat64(), Equals, t.expected)
			}
		}
	}

	_, err := funcs[ast.Sin].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestCos(c *C) {
	cases := []struct {
		args       interface{}
		expected   float64
		isNil      bool
		getWarning bool
	}{
		{nil, 0, true, false},
		{int64(0), float64(1), false, false},
		{math.Pi, float64(-1), false, false}, // cos pi equals -1
		{-math.Pi, float64(-1), false, false},
		{math.Pi / 2, float64(math.Cos(math.Pi / 2)), false, false}, // Pi/2 is some near 0 (6.123233995736766e-17) but not 0. Even in math it is 0.
		{-math.Pi / 2, float64(math.Cos(-math.Pi / 2)), false, false},
		{"0.000", float64(1), false, false}, // string value case
		{"sdfgsfsdf", float64(0), false, true},
	}

	for _, t := range cases {
		preWarningCnt := s.ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(s.ctx, ast.Cos, s.primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)

		d, err := f.Eval(chunk.Row{})
		if t.getWarning {
			c.Assert(err, IsNil)
			c.Assert(s.ctx.GetSessionVars().StmtCtx.WarningCount(), Equals, preWarningCnt+1)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetFloat64(), Equals, t.expected)
			}
		}
	}

	_, err := funcs[ast.Cos].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestAcos(c *C) {
	tests := []struct {
		args       interface{}
		expect     float64
		isNil      bool
		getWarning bool
	}{
		{nil, 0, true, false},
		{float64(1), 0, false, false},
		{float64(2), 0, true, false},
		{float64(-1), 3.141592653589793, false, false},
		{float64(-2), 0, true, false},
		{"tidb", 0, false, true},
	}

	for _, test := range tests {
		preWarningCnt := s.ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(s.ctx, ast.Acos, s.primitiveValsToConstants([]interface{}{test.args})...)
		c.Assert(err, IsNil)

		result, err := f.Eval(chunk.Row{})
		if test.getWarning {
			c.Assert(err, IsNil)
			c.Assert(s.ctx.GetSessionVars().StmtCtx.WarningCount(), Equals, preWarningCnt+1)
		} else {
			c.Assert(err, IsNil)
			if test.isNil {
				c.Assert(result.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(result.GetFloat64(), Equals, test.expect)
			}
		}
	}

	_, err := funcs[ast.Acos].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestAsin(c *C) {
	tests := []struct {
		args       interface{}
		expect     float64
		isNil      bool
		getWarning bool
	}{
		{nil, 0, true, false},
		{float64(1), 1.5707963267948966, false, false},
		{float64(2), 0, true, false},
		{float64(-1), -1.5707963267948966, false, false},
		{float64(-2), 0, true, false},
		{"tidb", 0, false, true},
	}

	for _, test := range tests {
		preWarningCnt := s.ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(s.ctx, ast.Asin, s.primitiveValsToConstants([]interface{}{test.args})...)
		c.Assert(err, IsNil)

		result, err := f.Eval(chunk.Row{})
		if test.getWarning {
			c.Assert(err, IsNil)
			c.Assert(s.ctx.GetSessionVars().StmtCtx.WarningCount(), Equals, preWarningCnt+1)
		} else {
			c.Assert(err, IsNil)
			if test.isNil {
				c.Assert(result.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(result.GetFloat64(), Equals, test.expect)
			}
		}
	}

	_, err := funcs[ast.Asin].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestAtan(c *C) {
	tests := []struct {
		args       []interface{}
		expect     float64
		isNil      bool
		getWarning bool
	}{
		{[]interface{}{nil}, 0, true, false},
		{[]interface{}{nil, nil}, 0, true, false},
		{[]interface{}{float64(1)}, 0.7853981633974483, false, false},
		{[]interface{}{float64(-1)}, -0.7853981633974483, false, false},
		{[]interface{}{float64(0), float64(-2)}, float64(math.Pi), false, false},
		{[]interface{}{"tidb"}, 0, false, true},
	}

	for _, test := range tests {
		preWarningCnt := s.ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(s.ctx, ast.Atan, s.primitiveValsToConstants(test.args)...)
		c.Assert(err, IsNil)

		result, err := f.Eval(chunk.Row{})
		if test.getWarning {
			c.Assert(err, IsNil)
			c.Assert(s.ctx.GetSessionVars().StmtCtx.WarningCount(), Equals, preWarningCnt+1)
		} else {
			c.Assert(err, IsNil)
			if test.isNil {
				c.Assert(result.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(result.GetFloat64(), Equals, test.expect)
			}
		}
	}

	_, err := funcs[ast.Atan].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestTan(c *C) {
	cases := []struct {
		args       interface{}
		expected   float64
		isNil      bool
		getWarning bool
	}{
		{nil, 0, true, false},
		{int64(0), float64(0), false, false},
		{math.Pi / 4, float64(1), false, false},
		{-math.Pi / 4, float64(-1), false, false},
		{math.Pi * 3 / 4, math.Tan(math.Pi * 3 / 4), false, false}, //in mysql and golang, it equals -1.0000000000000002, not -1
		{"0.000", float64(0), false, false},
		{"sdfgsdfg", 0, false, true},
	}

	for _, t := range cases {
		preWarningCnt := s.ctx.GetSessionVars().StmtCtx.WarningCount()
		f, err := newFunctionForTest(s.ctx, ast.Tan, s.primitiveValsToConstants([]interface{}{t.args})...)
		c.Assert(err, IsNil)

		d, err := f.Eval(chunk.Row{})
		if t.getWarning {
			c.Assert(err, IsNil)
			c.Assert(s.ctx.GetSessionVars().StmtCtx.WarningCount(), Equals, preWarningCnt+1)
		} else {
			c.Assert(err, IsNil)
			if t.isNil {
				c.Assert(d.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(d.GetFloat64(), Equals, t.expected)
			}
		}
	}

	_, err := funcs[ast.Tan].getFunction(s.ctx, []Expression{NewZero()})
	c.Assert(err, IsNil)
}

func (s *testEvaluatorSuite) TestCot(c *C) {
	tests := []struct {
		args   interface{}
		expect float64
		isNil  bool
		getErr bool
		errMsg string
	}{
		{nil, 0, true, false, ""},
		{float64(0), 0, false, true, "[types:1690]DOUBLE value is out of range in 'cot(0)'"},
		{float64(-1), -0.6420926159343308, false, false, ""},
		{float64(1), 0.6420926159343308, false, false, ""},
		{math.Pi / 4, 1 / math.Tan(math.Pi/4), false, false, ""},
		{math.Pi / 2, 1 / math.Tan(math.Pi/2), false, false, ""},
		{math.Pi, 1 / math.Tan(math.Pi), false, false, ""},
		{"tidb", 0, false, true, ""},
	}

	for _, test := range tests {
		f, err := newFunctionForTest(s.ctx, ast.Cot, s.primitiveValsToConstants([]interface{}{test.args})...)
		c.Assert(err, IsNil)

		result, err := f.Eval(chunk.Row{})
		if test.getErr {
			c.Assert(err, NotNil)
			if test.errMsg != "" {
				c.Assert(err.Error(), Equals, test.errMsg)
			}
		} else {
			c.Assert(err, IsNil)
			if test.isNil {
				c.Assert(result.Kind(), Equals, types.KindNull)
			} else {
				c.Assert(result.GetFloat64(), Equals, test.expect)
			}
		}
	}

	_, err := funcs[ast.Cot].getFunction(s.ctx, []Expression{NewOne()})
	c.Assert(err, IsNil)
}
