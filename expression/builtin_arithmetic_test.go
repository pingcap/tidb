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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tipb/go-tipb"
)

func (s *testEvaluatorSuite) TestSetFlenDecimal4RealOrDecimal(c *C) {
	ret := &types.FieldType{}
	a := &types.FieldType{
		Decimal: 1,
		Flen:    3,
	}
	b := &types.FieldType{
		Decimal: 0,
		Flen:    2,
	}
	setFlenDecimal4RealOrDecimal(ret, a, b, true, false)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, 6)

	b.Flen = 65
	setFlenDecimal4RealOrDecimal(ret, a, b, true, false)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, mysql.MaxRealWidth)
	setFlenDecimal4RealOrDecimal(ret, a, b, false, false)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, mysql.MaxDecimalWidth)

	b.Flen = types.UnspecifiedLength
	setFlenDecimal4RealOrDecimal(ret, a, b, true, false)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, types.UnspecifiedLength)

	b.Decimal = types.UnspecifiedLength
	setFlenDecimal4RealOrDecimal(ret, a, b, true, false)
	c.Assert(ret.Decimal, Equals, types.UnspecifiedLength)
	c.Assert(ret.Flen, Equals, types.UnspecifiedLength)

	ret = &types.FieldType{}
	a = &types.FieldType{
		Decimal: 1,
		Flen:    3,
	}
	b = &types.FieldType{
		Decimal: 0,
		Flen:    2,
	}
	setFlenDecimal4RealOrDecimal(ret, a, b, true, true)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, 8)

	b.Flen = 65
	setFlenDecimal4RealOrDecimal(ret, a, b, true, true)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, mysql.MaxRealWidth)
	setFlenDecimal4RealOrDecimal(ret, a, b, false, true)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, mysql.MaxDecimalWidth)

	b.Flen = types.UnspecifiedLength
	setFlenDecimal4RealOrDecimal(ret, a, b, true, true)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, types.UnspecifiedLength)

	b.Decimal = types.UnspecifiedLength
	setFlenDecimal4RealOrDecimal(ret, a, b, true, true)
	c.Assert(ret.Decimal, Equals, types.UnspecifiedLength)
	c.Assert(ret.Flen, Equals, types.UnspecifiedLength)
}

func (s *testEvaluatorSuite) TestSetFlenDecimal4Int(c *C) {
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
	// case: 1
	args := []interface{}{int64(12), int64(1)}

	bf, err := funcs[ast.Plus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	intSig, ok := bf.(*builtinArithmeticPlusIntSig)
	c.Assert(ok, IsTrue)
	c.Assert(intSig, NotNil)

	intResult, isNull, err := intSig.evalInt(chunk.Row{})
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

	realResult, isNull, err := realSig.evalReal(chunk.Row{})
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

	realResult, isNull, err = realSig.evalReal(chunk.Row{})
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

	realResult, isNull, err = realSig.evalReal(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	c.Assert(realResult, Equals, float64(0))

	// case 5
	hexStr, err := types.ParseHexStr("0x20000000000000")
	c.Assert(err, IsNil)
	args = []interface{}{hexStr, int64(1)}

	bf, err = funcs[ast.Plus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	intSig, ok = bf.(*builtinArithmeticPlusIntSig)
	c.Assert(ok, IsTrue)
	c.Assert(intSig, NotNil)

	intResult, _, err = intSig.evalInt(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(intResult, Equals, int64(9007199254740993))
}

func (s *testEvaluatorSuite) TestArithmeticMinus(c *C) {
	// case: 1
	args := []interface{}{int64(12), int64(1)}

	bf, err := funcs[ast.Minus].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(args...)))
	c.Assert(err, IsNil)
	c.Assert(bf, NotNil)
	intSig, ok := bf.(*builtinArithmeticMinusIntSig)
	c.Assert(ok, IsTrue)
	c.Assert(intSig, NotNil)

	intResult, isNull, err := intSig.evalInt(chunk.Row{})
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

	realResult, isNull, err := realSig.evalReal(chunk.Row{})
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

	realResult, isNull, err = realSig.evalReal(chunk.Row{})
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

	realResult, isNull, err = realSig.evalReal(chunk.Row{})
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

	realResult, isNull, err = realSig.evalReal(chunk.Row{})
	c.Assert(err, IsNil)
	c.Assert(isNull, IsTrue)
	c.Assert(realResult, Equals, float64(0))
}

func (s *testEvaluatorSuite) TestArithmeticMultiply(c *C) {
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
		val, err := evalBuiltinFunc(sig, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect))
	}
}

func (s *testEvaluatorSuite) TestArithmeticDivide(c *C) {
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
		switch sig.(type) {
		case *builtinArithmeticIntDivideIntSig:
			c.Assert(sig.PbCode(), Equals, tipb.ScalarFuncSig_IntDivideInt)
		case *builtinArithmeticIntDivideDecimalSig:
			c.Assert(sig.PbCode(), Equals, tipb.ScalarFuncSig_IntDivideDecimal)
		}
		val, err := evalBuiltinFunc(sig, chunk.Row{})
		c.Assert(err, IsNil)
		c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect))
	}
}

func (s *testEvaluatorSuite) TestArithmeticIntDivide(c *C) {
	testCases := []struct {
		args   []interface{}
		expect []interface{}
	}{
		{
			args:   []interface{}{int64(13), int64(11)},
			expect: []interface{}{int64(1), nil},
		},
		{
			args:   []interface{}{int64(-13), int64(11)},
			expect: []interface{}{int64(-1), nil},
		},
		{
			args:   []interface{}{int64(13), int64(-11)},
			expect: []interface{}{int64(-1), nil},
		},
		{
			args:   []interface{}{int64(-13), int64(-11)},
			expect: []interface{}{int64(1), nil},
		},
		{
			args:   []interface{}{int64(33), int64(11)},
			expect: []interface{}{int64(3), nil},
		},
		{
			args:   []interface{}{int64(-33), int64(11)},
			expect: []interface{}{int64(-3), nil},
		},
		{
			args:   []interface{}{int64(33), int64(-11)},
			expect: []interface{}{int64(-3), nil},
		},
		{
			args:   []interface{}{int64(-33), int64(-11)},
			expect: []interface{}{int64(3), nil},
		},
		{
			args:   []interface{}{int64(11), int64(0)},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{int64(-11), int64(0)},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{float64(11.01), float64(1.1)},
			expect: []interface{}{int64(10), nil},
		},
		{
			args:   []interface{}{float64(-11.01), float64(1.1)},
			expect: []interface{}{int64(-10), nil},
		},
		{
			args:   []interface{}{float64(11.01), float64(-1.1)},
			expect: []interface{}{int64(-10), nil},
		},
		{
			args:   []interface{}{float64(-11.01), float64(-1.1)},
			expect: []interface{}{int64(10), nil},
		},
		{
			args:   []interface{}{nil, float64(-0.11101)},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{float64(1.01), nil},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{nil, int64(-1001)},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{int64(101), nil},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{nil, nil},
			expect: []interface{}{nil, nil},
		},
		{
			args:   []interface{}{float64(123456789100000.0), float64(-0.00001)},
			expect: []interface{}{nil, "*BIGINT value is out of range in '\\(123456789100000 DIV -0.00001\\)'"},
		},
		{
			args:   []interface{}{int64(-9223372036854775808), float64(-1)},
			expect: []interface{}{nil, "*BIGINT value is out of range in '\\(-9223372036854775808 DIV -1\\)'"},
		},
		{
			args:   []interface{}{uint64(1), float64(-2)},
			expect: []interface{}{0, nil},
		},
		{
			args:   []interface{}{uint64(1), float64(-1)},
			expect: []interface{}{nil, "*BIGINT UNSIGNED value is out of range in '\\(1 DIV -1\\)'"},
		},
	}

	for _, tc := range testCases {
		sig, err := funcs[ast.IntDiv].getFunction(s.ctx, s.datumsToConstants(types.MakeDatums(tc.args...)))
		c.Assert(err, IsNil)
		c.Assert(sig, NotNil)
		val, err := evalBuiltinFunc(sig, chunk.Row{})
		if tc.expect[1] == nil {
			c.Assert(err, IsNil)
			c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect[0]))
		} else {
			c.Assert(err, ErrorMatches, tc.expect[1])
		}
	}
}

func (s *testEvaluatorSuite) TestArithmeticMod(c *C) {
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
		val, err := evalBuiltinFunc(sig, chunk.Row{})
		switch sig.(type) {
		case *builtinArithmeticModRealSig:
			c.Assert(sig.PbCode(), Equals, tipb.ScalarFuncSig_ModReal)
		case *builtinArithmeticModIntUnsignedUnsignedSig:
			c.Assert(sig.PbCode(), Equals, tipb.ScalarFuncSig_ModIntUnsignedUnsigned)
		case *builtinArithmeticModIntUnsignedSignedSig:
			c.Assert(sig.PbCode(), Equals, tipb.ScalarFuncSig_ModIntUnsignedSigned)
		case *builtinArithmeticModIntSignedUnsignedSig:
			c.Assert(sig.PbCode(), Equals, tipb.ScalarFuncSig_ModIntSignedUnsigned)
		case *builtinArithmeticModIntSignedSignedSig:
			c.Assert(sig.PbCode(), Equals, tipb.ScalarFuncSig_ModIntSignedSigned)
		case *builtinArithmeticModDecimalSig:
			c.Assert(sig.PbCode(), Equals, tipb.ScalarFuncSig_ModDecimal)
		}
		c.Assert(err, IsNil)
		c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect))
	}
}
