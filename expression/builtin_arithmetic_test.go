// Copyright 2017 PingCAP, Inc.
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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
)

func (s *testEvaluatorSuite) TestSetFlenDecimal4RealOrDecimal(c *C) {
	defer testleak.AfterTest(c)()

	ret := &types.FieldType{}
	a := &types.FieldType{
		Decimal: 1,
		Flen:    3,
	}
	b := &types.FieldType{
		Decimal: 0,
		Flen:    2,
	}
	setFlenDecimal4RealOrDecimal(ret, a, b, true)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, 6)

	b.Flen = 65
	setFlenDecimal4RealOrDecimal(ret, a, b, true)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, mysql.MaxRealWidth)
	setFlenDecimal4RealOrDecimal(ret, a, b, false)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, mysql.MaxDecimalWidth)

	b.Flen = types.UnspecifiedLength
	setFlenDecimal4RealOrDecimal(ret, a, b, true)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, types.UnspecifiedLength)

	b.Decimal = types.UnspecifiedLength
	setFlenDecimal4RealOrDecimal(ret, a, b, true)
	c.Assert(ret.Decimal, Equals, types.UnspecifiedLength)
	c.Assert(ret.Flen, Equals, types.UnspecifiedLength)
}

func (s *testEvaluatorSuite) TestSetFlenDecimal4Int(c *C) {
	defer testleak.AfterTest(c)()

	ret := &types.FieldType{}
	a := &types.FieldType{
		Decimal: 1,
		Flen:    3,
	}
	b := &types.FieldType{
		Decimal: 0,
		Flen:    2,
	}
	setFlenDecimal4Int(ret, a, b)
	c.Assert(ret.Decimal, Equals, 0)
	c.Assert(ret.Flen, Equals, mysql.MaxIntWidth)

	b.Flen = mysql.MaxIntWidth + 1
	setFlenDecimal4Int(ret, a, b)
	c.Assert(ret.Decimal, Equals, 0)
	c.Assert(ret.Flen, Equals, mysql.MaxIntWidth)

	b.Flen = types.UnspecifiedLength
	setFlenDecimal4Int(ret, a, b)
	c.Assert(ret.Decimal, Equals, 0)
	c.Assert(ret.Flen, Equals, mysql.MaxIntWidth)
}

func (s *testEvaluatorSuite) TestArithmeticPlus(c *C) {
	defer testleak.AfterTest(c)()

	// case: 1
	args := []interface{}{int64(12), int64(1)}

	bf, err := funcs[ast.Plus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	intSig, ok := bf.(*builtinArithmeticPlusIntSig)
	c.Assert(ok, IsTrue)
	c.Assert(intSig, NotNil)

	intResult, isNull, err := intSig.evalInt(nil)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	c.Assert(intResult, Equals, int64(13))

	// case 2
	args = []interface{}{float64(1.01001), float64(-0.01)}

	bf, err = funcs[ast.Plus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	realSig, ok := bf.(*builtinArithmeticPlusRealSig)
	c.Assert(ok, IsTrue)
	c.Assert(realSig, NotNil)

	realResult, isNull, err := realSig.evalReal(nil)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	c.Assert(realResult, Equals, float64(1.00001))

	// case 3
	args = []interface{}{nil, float64(-0.11101)}

	bf, err = funcs[ast.Plus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	realSig, ok = bf.(*builtinArithmeticPlusRealSig)
	c.Assert(ok, IsTrue)
	c.Assert(realSig, NotNil)

	realResult, isNull, err = realSig.evalReal(nil)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	c.Assert(realResult, Equals, float64(0))

	// case 4
	args = []interface{}{nil, nil}

	bf, err = funcs[ast.Plus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	realSig, ok = bf.(*builtinArithmeticPlusRealSig)
	c.Assert(ok, IsTrue)
	c.Assert(realSig, NotNil)

	realResult, isNull, err = realSig.evalReal(nil)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	c.Assert(realResult, Equals, float64(0))
}

func (s *testEvaluatorSuite) TestArithmeticMinus(c *C) {
	defer testleak.AfterTest(c)()

	// case: 1
	args := []interface{}{int64(12), int64(1)}

	bf, err := funcs[ast.Minus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	intSig, ok := bf.(*builtinArithmeticMinusIntSig)
	c.Assert(ok, IsTrue)
	c.Assert(intSig, NotNil)

	intResult, isNull, err := intSig.evalInt(nil)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	c.Assert(intResult, Equals, int64(11))

	// case 2
	args = []interface{}{float64(1.01001), float64(-0.01)}

	bf, err = funcs[ast.Minus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	realSig, ok := bf.(*builtinArithmeticMinusRealSig)
	c.Assert(ok, IsTrue)
	c.Assert(realSig, NotNil)

	realResult, isNull, err := realSig.evalReal(nil)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsFalse)
	c.Assert(realResult, Equals, float64(1.02001))

	// case 3
	args = []interface{}{nil, float64(-0.11101)}

	bf, err = funcs[ast.Minus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	realSig, ok = bf.(*builtinArithmeticMinusRealSig)
	c.Assert(ok, IsTrue)
	c.Assert(realSig, NotNil)

	realResult, isNull, err = realSig.evalReal(nil)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	c.Assert(realResult, Equals, float64(0))

	// case 4
	args = []interface{}{float64(1.01), nil}

	bf, err = funcs[ast.Minus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	realSig, ok = bf.(*builtinArithmeticMinusRealSig)
	c.Assert(ok, IsTrue)
	c.Assert(realSig, NotNil)

	realResult, isNull, err = realSig.evalReal(nil)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	c.Assert(realResult, Equals, float64(0))

	// case 5
	args = []interface{}{nil, nil}

	bf, err = funcs[ast.Minus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	realSig, ok = bf.(*builtinArithmeticMinusRealSig)
	c.Assert(ok, IsTrue)
	c.Assert(realSig, NotNil)

	realResult, isNull, err = realSig.evalReal(nil)
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	c.Assert(realResult, Equals, float64(0))
}

func (s *testEvaluatorSuite) TestArithmeticMultiply(c *C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		args   []interface{}
		expect interface{}
		err    error
	}{
		{
			args:   []interface{}{int64(11), int64(11)},
			expect: int64(121),
		},
		{
			args:   []interface{}{uint64(11), uint64(11)},
			expect: int64(121),
		},
		{
			args:   []interface{}{float64(11), float64(11)},
			expect: float64(121),
		},
		{
			args:   []interface{}{nil, float64(-0.11101)},
			expect: nil,
		},
		{
			args:   []interface{}{float64(1.01), nil},
			expect: nil,
		},
		{
			args:   []interface{}{nil, nil},
			expect: nil,
		},
	}

	for _, tc := range testCases {
		sig, err := funcs[ast.Mul].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(sig, NotNil)
		val, err := evalBuiltinFunc(sig, nil)
		c.Assert(err, IsNil)
		c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect))
	}
}

func (s *testEvaluatorSuite) TestArithmeticDivide(c *C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		args   []interface{}
		expect interface{}
	}{
		{
			args:   []interface{}{float64(11.1111111), float64(11.1)},
			expect: float64(1.001001),
		},
		{
			args:   []interface{}{float64(11.1111111), float64(0)},
			expect: nil,
		},
		{
			args:   []interface{}{int64(11), int64(11)},
			expect: float64(1),
		},
		{
			args:   []interface{}{int64(11), int64(2)},
			expect: float64(5.5),
		},
		{
			args:   []interface{}{int64(11), int64(0)},
			expect: nil,
		},
		{
			args:   []interface{}{uint64(11), uint64(11)},
			expect: float64(1),
		},
		{
			args:   []interface{}{uint64(11), uint64(2)},
			expect: float64(5.5),
		},
		{
			args:   []interface{}{uint64(11), uint64(0)},
			expect: nil,
		},
		{
			args:   []interface{}{nil, float64(-0.11101)},
			expect: nil,
		},
		{
			args:   []interface{}{float64(1.01), nil},
			expect: nil,
		},
		{
			args:   []interface{}{nil, nil},
			expect: nil,
		},
	}

	for _, tc := range testCases {
		sig, err := funcs[ast.Div].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(sig, NotNil)
		val, err := evalBuiltinFunc(sig, nil)
		c.Assert(err, IsNil)
		c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect))
	}
}

func (s *testEvaluatorSuite) TestArithmeticIntDivide(c *C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		args   []interface{}
		expect interface{}
	}{
		{
			args:   []interface{}{int64(13), int64(11)},
			expect: int64(1),
		},
		{
			args:   []interface{}{int64(-13), int64(11)},
			expect: int64(-1),
		},
		{
			args:   []interface{}{int64(13), int64(-11)},
			expect: int64(-1),
		},
		{
			args:   []interface{}{int64(-13), int64(-11)},
			expect: int64(1),
		},
		{
			args:   []interface{}{int64(33), int64(11)},
			expect: int64(3),
		},
		{
			args:   []interface{}{int64(-33), int64(11)},
			expect: int64(-3),
		},
		{
			args:   []interface{}{int64(33), int64(-11)},
			expect: int64(-3),
		},
		{
			args:   []interface{}{int64(-33), int64(-11)},
			expect: int64(3),
		},
		{
			args:   []interface{}{int64(11), int64(0)},
			expect: nil,
		},
		{
			args:   []interface{}{int64(-11), int64(0)},
			expect: nil,
		},
		{
			args:   []interface{}{float64(11.01), float64(1.1)},
			expect: int64(10),
		},
		{
			args:   []interface{}{float64(-11.01), float64(1.1)},
			expect: int64(-10),
		},
		{
			args:   []interface{}{float64(11.01), float64(-1.1)},
			expect: int64(-10),
		},
		{
			args:   []interface{}{float64(-11.01), float64(-1.1)},
			expect: int64(10),
		},
		{
			args:   []interface{}{nil, float64(-0.11101)},
			expect: nil,
		},
		{
			args:   []interface{}{float64(1.01), nil},
			expect: nil,
		},
		{
			args:   []interface{}{nil, int64(-1001)},
			expect: nil,
		},
		{
			args:   []interface{}{int64(101), nil},
			expect: nil,
		},
		{
			args:   []interface{}{nil, nil},
			expect: nil,
		},
	}

	for _, tc := range testCases {
		sig, err := funcs[ast.IntDiv].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(sig, NotNil)
		val, err := evalBuiltinFunc(sig, nil)
		c.Assert(err, IsNil)
		c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect))
	}
}

func (s *testEvaluatorSuite) TestArithmeticMod(c *C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		args   []interface{}
		expect interface{}
	}{
		{
			args:   []interface{}{int64(13), int64(11)},
			expect: int64(2),
		},
		{
			args:   []interface{}{int64(-13), int64(11)},
			expect: int64(-2),
		},
		{
			args:   []interface{}{int64(13), int64(-11)},
			expect: int64(2),
		},
		{
			args:   []interface{}{int64(-13), int64(-11)},
			expect: int64(-2),
		},
		{
			args:   []interface{}{int64(33), int64(11)},
			expect: int64(0),
		},
		{
			args:   []interface{}{int64(-33), int64(11)},
			expect: int64(0),
		},
		{
			args:   []interface{}{int64(33), int64(-11)},
			expect: int64(0),
		},
		{
			args:   []interface{}{int64(-33), int64(-11)},
			expect: int64(0),
		},
		{
			args:   []interface{}{int64(11), int64(0)},
			expect: nil,
		},
		{
			args:   []interface{}{int64(-11), int64(0)},
			expect: nil,
		},
		{
			args:   []interface{}{int64(1), float64(1.1)},
			expect: float64(1),
		},
		{
			args:   []interface{}{int64(-1), float64(1.1)},
			expect: float64(-1),
		},
		{
			args:   []interface{}{int64(1), float64(-1.1)},
			expect: float64(1),
		},
		{
			args:   []interface{}{int64(-1), float64(-1.1)},
			expect: float64(-1),
		},
		{
			args:   []interface{}{nil, float64(-0.11101)},
			expect: nil,
		},
		{
			args:   []interface{}{float64(1.01), nil},
			expect: nil,
		},
		{
			args:   []interface{}{nil, int64(-1001)},
			expect: nil,
		},
		{
			args:   []interface{}{int64(101), nil},
			expect: nil,
		},
		{
			args:   []interface{}{nil, nil},
			expect: nil,
		},
		{
			args:   []interface{}{"1231", 12},
			expect: 7,
		},
		{
			args:   []interface{}{"1231", "12"},
			expect: float64(7),
		},
		{
			args:   []interface{}{types.Duration{Duration: 45296 * time.Second}, 122},
			expect: 114,
		},
		{
			args:   []interface{}{types.Set{Value: 7, Name: "abc"}, "12"},
			expect: float64(7),
		},
	}

	for _, tc := range testCases {
		sig, err := funcs[ast.Mod].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(sig, NotNil)
		val, err := evalBuiltinFunc(sig, nil)
		c.Assert(err, IsNil)
		c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect))
	}
}
