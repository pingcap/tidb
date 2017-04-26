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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
)

func (s *testEvaluatorSuite) TestAbs(c *C) {
	defer testleak.AfterTest(c)()
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
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestCeil(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{int64(1), int64(1)},
		{float64(1.23), float64(2)},
		{float64(-1.23), float64(-1)},
		{"1.23", float64(2)},
		{"-1.23", float64(-1)},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.Ceil]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t["Ret"][0], Commentf("arg:%v", t["Arg"]))
	}
}

func (s *testEvaluatorSuite) TestExp(c *C) {
	defer testleak.AfterTest(c)()
	for _, t := range []struct {
		num interface{}
		ret interface{}
		err Checker
	}{
		{int64(1), float64(2.718281828459045), IsNil},
		{float64(1.23), float64(3.4212295362896734), IsNil},
		{float64(-1.23), float64(0.2922925776808594), IsNil},
		{float64(-1), float64(0.36787944117144233), IsNil},
		{float64(0), float64(1), IsNil},
		{"1.23", float64(3.4212295362896734), IsNil},
		{"-1.23", float64(0.2922925776808594), IsNil},
		{"0", float64(1), IsNil},
		{nil, nil, IsNil},
		{"abce", nil, NotNil},
		{"", nil, NotNil},
	} {
		fc := funcs[ast.Exp]
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(t.num)), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, t.err)
		c.Assert(v, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}

func (s *testEvaluatorSuite) TestFloor(c *C) {
	defer testleak.AfterTest(c)()

	sc := s.ctx.GetSessionVars().StmtCtx
	tmpIT := sc.IgnoreTruncate
	sc.IgnoreTruncate = true
	defer func() {
		sc.IgnoreTruncate = tmpIT
	}()

	for _, t := range []struct {
		num interface{}
		ret interface{}
		err Checker
	}{
		{nil, nil, IsNil},
		{int64(1), int64(1), IsNil},
		{float64(1.23), float64(1), IsNil},
		{float64(-1.23), float64(-2), IsNil},
		{"1.23", float64(1), IsNil},
		{"-1.23", float64(-2), IsNil},
		{"-1.b23", float64(-1), IsNil},
		{"abce", float64(0), IsNil},
	} {
		fc := funcs[ast.Floor]
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(t.num)), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, t.err)
		c.Assert(v, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}

func (s *testEvaluatorSuite) TestLog(c *C) {
	defer testleak.AfterTest(c)()

	tbl := []struct {
		Arg []interface{}
		Ret interface{}
	}{
		{[]interface{}{int64(2)}, float64(0.6931471805599453)},

		{[]interface{}{int64(2), int64(65536)}, float64(16)},
		{[]interface{}{int64(10), int64(100)}, float64(2)},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.Log]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t["Ret"][0], Commentf("arg:%v", t["Arg"]))
	}

	nullTbl := []struct {
		Arg []interface{}
	}{
		{[]interface{}{int64(-2)}},
		{[]interface{}{int64(1), int64(100)}},
	}

	nullDtbl := tblToDtbl(nullTbl)

	for _, t := range nullDtbl {
		fc := funcs[ast.Log]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v.Kind(), Equals, types.KindNull)
	}
}

func (s *testEvaluatorSuite) TestRand(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.Rand]
	f, err := fc.getFunction(nil, s.ctx)
	c.Assert(err, IsNil)
	v, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(v.GetFloat64(), Less, float64(1))
	c.Assert(v.GetFloat64(), GreaterEqual, float64(0))
}

func (s *testEvaluatorSuite) TestPow(c *C) {
	defer testleak.AfterTest(c)()
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
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}

	errTbl := []struct {
		Arg []interface{}
	}{
		{[]interface{}{"test", "test"}},
		{[]interface{}{nil, nil}},
		{[]interface{}{1, "test"}},
		{[]interface{}{1, nil}},
		{[]interface{}{10, 700}}, // added overflow test
	}

	errDtbl := tblToDtbl(errTbl)
	for _, t := range errDtbl {
		fc := funcs[ast.Pow]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		_, err = f.eval(nil)
		c.Assert(err, NotNil)
	}
}

func (s *testEvaluatorSuite) TestRound(c *C) {
	defer testleak.AfterTest(c)()
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
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.Round]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestTruncate(c *C) {
	defer testleak.AfterTest(c)()
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
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.Truncate]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestCRC32(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg []interface{}
		Ret uint64
	}{
		{[]interface{}{"mysql"}, 2501908538},
		{[]interface{}{"MySQL"}, 3259397556},
		{[]interface{}{"hello"}, 907060870},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.CRC32]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestConv(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg []interface{}
		Ret interface{}
	}{
		{[]interface{}{"a", 16, 2}, "1010"},
		{[]interface{}{"6E", 18, 8}, "172"},
		{[]interface{}{"-17", 10, -18}, "-H"},
		{[]interface{}{"-17", 10, 18}, "2D3FGB0B9CG4BD1H"},
		{[]interface{}{nil, 10, 10}, nil},
		{[]interface{}{"+18aZ", 7, 36}, 1},
		{[]interface{}{"18446744073709551615", -10, 16}, "7FFFFFFFFFFFFFFF"},
		{[]interface{}{"12F", -10, 16}, "C"},
		{[]interface{}{"  FF ", 16, 10}, "255"},
		{[]interface{}{"TIDB", 10, 8}, "0"},
		{[]interface{}{"aa", 10, 2}, "0"},
		{[]interface{}{" A", -10, 16}, "0"},
		{[]interface{}{"a6a", 10, 8}, "0"},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.Conv]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
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
}

func (s *testEvaluatorSuite) TestSign(c *C) {
	defer testleak.AfterTest(c)()

	for _, t := range []struct {
		num interface{}
		ret interface{}
		err Checker
	}{
		{nil, nil, IsNil},
		{1, 1, IsNil},
		{0, 0, IsNil},
		{-1, -1, IsNil},
		{0.4, 1, IsNil},
		{-0.4, -1, IsNil},
		{"1", 1, IsNil},
		{"-1", -1, IsNil},
		{"1a", 1, NotNil},
		{"-1a", -1, NotNil},
		{"a", 0, NotNil},
		{uint64(9223372036854775808), 1, IsNil},
	} {
		fc := funcs[ast.Sign]
		f, err := fc.getFunction(datumsToConstants(types.MakeDatums(t.num)), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, t.err)
		c.Assert(v, testutil.DatumEquals, types.NewDatum(t.ret))
	}
}

func (s *testEvaluatorSuite) TestDegrees(c *C) {
	defer testleak.AfterTest(c)()

	sc := s.ctx.GetSessionVars().StmtCtx
	tmpIT := sc.IgnoreTruncate
	sc.IgnoreTruncate = true
	defer func() {
		sc.IgnoreTruncate = tmpIT
	}()

	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{int64(0), float64(0)},
		{int64(1), float64(57.29577951308232)},
		{float64(1), float64(57.29577951308232)},
		{float64(math.Pi), float64(180)},
		{float64(-math.Pi / 2), float64(-90)},
		{"", float64(0)},
		{"-2", float64(-114.59155902616465)},
		{"abc", float64(0)},
		{"+1abc", float64(57.29577951308232)},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.Degrees]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t["Ret"][0], Commentf("arg:%v", t["arg"]))
	}
}

func (s *testEvaluatorSuite) TestSqrt(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{int64(1), float64(1)},
		{float64(4), float64(2)},
		{"4", float64(2)},
		{"9", float64(3)},
		{"-16", nil},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.Sqrt]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t["Ret"][0], Commentf("arg:%v", t["Arg"]))
	}
}

func (s *testEvaluatorSuite) TestPi(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.PI]
	f, _ := fc.getFunction(nil, s.ctx)
	pi, err := f.eval(nil)
	c.Assert(err, IsNil)
	c.Assert(pi, testutil.DatumEquals, types.NewDatum(math.Pi))
}

func (s *testEvaluatorSuite) TestRadians(c *C) {
	defer testleak.AfterTest(c)()
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
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}

	invalidArg := "notNum"
	fc := funcs[ast.Radians]
	f, err := fc.getFunction(datumsToConstants([]types.Datum{types.NewDatum(invalidArg)}), s.ctx)
	c.Assert(err, IsNil)
	_, err = f.eval(nil)
	c.Assert(err, NotNil)
}

func (s *testEvaluatorSuite) TestSin(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{int64(0), float64(0)},
		{math.Pi, float64(math.Sin(math.Pi))}, // Pie ==> 0
		{-math.Pi, float64(math.Sin(-math.Pi))},
		{math.Pi / 2, float64(math.Sin(math.Pi / 2))}, // Pie/2 ==> 1
		{-math.Pi / 2, float64(math.Sin(-math.Pi / 2))},
		{math.Pi / 6, float64(math.Sin(math.Pi / 6))}, // Pie/6(30 degrees) ==> 0.5
		{-math.Pi / 6, float64(math.Sin(-math.Pi / 6))},
		{math.Pi * 2, float64(math.Sin(math.Pi * 2))},
		{"0.000", float64(0)}, // string value case
	}

	Dtbl := tblToDtbl(tbl)
	for _, t := range Dtbl {
		fc := funcs[ast.Sin]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestCos(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{int64(0), float64(1)},
		{math.Pi, float64(-1)}, // cos pi equals -1
		{-math.Pi, float64(-1)},
		{math.Pi / 2, float64(math.Cos(math.Pi / 2))}, // Pi/2 is some near 0 (6.123233995736766e-17) but not 0. Even in math it is 0.
		{-math.Pi / 2, float64(math.Cos(-math.Pi / 2))},
		{"0.000", float64(1)}, // string value case
	}

	Dtbl := tblToDtbl(tbl)
	for _, t := range Dtbl {
		fc := funcs[ast.Cos]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Log(t)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestAcos(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{int64(1), float64(0)},
		{float64(1.0001), nil},
		{"1", float64(0)},
	}

	Dtbl := tblToDtbl(tbl)
	for _, t := range Dtbl {
		fc := funcs[ast.Acos]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestAsin(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{int64(0), float64(0)},
		{float64(1.0001), nil},
		{"0", float64(0)},
		{"1.0", math.Pi / 2},
	}

	Dtbl := tblToDtbl(tbl)

	for _, t := range Dtbl {
		fc := funcs[ast.Asin]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t["Ret"][0], Commentf("arg:%v", t["Arg"]))
	}
}

func (s *testEvaluatorSuite) TestAtan(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg []interface{}
		Ret interface{}
	}{
		{[]interface{}{nil}, nil},
		{[]interface{}{nil, nil}, nil},
		{[]interface{}{int64(0), "aaa"}, float64(0)},
		{[]interface{}{int64(0)}, float64(0)},
		{[]interface{}{"0", "1"}, float64(0)},
		{[]interface{}{"0.0", "-2.0"}, float64(math.Pi)},
	}

	Dtbl := tblToDtbl(tbl)

	for idx, t := range Dtbl {
		fc := funcs[ast.Atan]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t["Ret"][0], Commentf("[%v] - arg:%v", idx, t["Arg"]))
	}
}

func (s *testEvaluatorSuite) TestTan(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{int64(0), float64(0)},
		{math.Pi / 4, float64(1)},
		{-math.Pi / 4, float64(-1)},
		{math.Pi * 3 / 4, math.Tan(math.Pi * 3 / 4)}, //in mysql and golang, it equals -1.0000000000000002, not -1
		{"0.000", float64(0)},
	}

	Dtbl := tblToDtbl(tbl)
	for _, t := range Dtbl {
		fc := funcs[ast.Tan]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Log(t)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}

func (s *testEvaluatorSuite) TestCot(c *C) {
	defer testleak.AfterTest(c)()
	tbl := []struct {
		Arg interface{}
		Ret interface{}
	}{
		{nil, nil},
		{math.Pi / 4, math.Cos(math.Pi/4) / math.Sin(math.Pi/4)}, // cot pi/4 does not return 1 actually
		{-math.Pi / 4, math.Cos(-math.Pi/4) / math.Sin(-math.Pi/4)},
		{math.Pi * 3 / 4, math.Cos(math.Pi*3/4) / math.Sin(math.Pi*3/4)},
		{"3.1415926", math.Cos(3.1415926) / math.Sin(3.1415926)},
	}

	Dtbl := tblToDtbl(tbl)
	for _, t := range Dtbl {
		fc := funcs[ast.Cot]
		f, err := fc.getFunction(datumsToConstants(t["Arg"]), s.ctx)
		c.Assert(err, IsNil)
		v, err := f.eval(nil)
		c.Assert(err, IsNil)
		c.Log(t)
		c.Assert(v, testutil.DatumEquals, t["Ret"][0])
	}
}
