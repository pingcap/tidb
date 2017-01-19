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
	tbl := []struct {
		Arg []interface{}
		Ret float64
	}{
		{[]interface{}{-1.23}, -1},
		{[]interface{}{-1.23, 0}, -1},
		{[]interface{}{-1.58}, -2},
		{[]interface{}{1.58}, 2},
		{[]interface{}{1.298, 1}, 1.3},
		{[]interface{}{1.298}, 1},
		{[]interface{}{1.298, 0}, 1},
		{[]interface{}{23.298, -1}, 20},
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
		v, err := builtinSqrt(t["Arg"], s.ctx)
		c.Assert(err, IsNil)
		c.Assert(v, DeepEquals, t["Ret"][0], Commentf("arg:%v", t["Arg"]))
	}
}
