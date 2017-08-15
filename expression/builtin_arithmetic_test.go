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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
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
	testCases := []struct {
		args   []interface{}
		expect interface{}
	}{
		{
			args:   []interface{}{int64(12), int64(1)},
			expect: int64(13),
		},
		{
			args:   []interface{}{float64(1.01001), float64(-0.01)},
			expect: float64(1.00001),
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
		sig, err := funcs[ast.Plus].getFunction(datumsToConstants(types.MakeDatums(tc.args...)), s.ctx)
		c.Assert(err, IsNil)
		c.Assert(sig, NotNil)
		c.Assert(sig.isDeterministic(), Equals, true)
		val, err := sig.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect))
	}
}

func (s *testEvaluatorSuite) TestArithmeticMinus(c *C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		args   []interface{}
		expect interface{}
	}{
		{
			args:   []interface{}{int64(12), int64(1)},
			expect: int64(11),
		},
		{
			args:   []interface{}{float64(1.01001), float64(-0.01)},
			expect: float64(1.02001),
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
		sig, err := funcs[ast.Minus].getFunction(datumsToConstants(types.MakeDatums(tc.args...)), s.ctx)
		c.Assert(err, IsNil)
		c.Assert(sig, NotNil)
		c.Assert(sig.isDeterministic(), Equals, true)
		val, err := sig.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect))
	}
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
		sig, err := funcs[ast.Mul].getFunction(datumsToConstants(types.MakeDatums(tc.args...)), s.ctx)
		c.Assert(err, IsNil)
		c.Assert(sig, NotNil)
		c.Assert(sig.isDeterministic(), Equals, true)
		val, err := sig.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect))
	}
}
