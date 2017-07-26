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

func (s *testEvaluatorSuite) TestPlusGetEvalTp(c *C) {
	defer testleak.AfterTest(c)()
	fieldTps := []*types.FieldType{
		types.NewFieldType(mysql.TypeTiny),
		types.NewFieldType(mysql.TypeShort),
		types.NewFieldType(mysql.TypeInt24),
		types.NewFieldType(mysql.TypeLong),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeBit),
		types.NewFieldType(mysql.TypeYear),
		types.NewFieldType(mysql.TypeDuration),
		types.NewFieldType(mysql.TypeDatetime),
		types.NewFieldType(mysql.TypeNewDecimal),
		types.NewFieldType(mysql.TypeDecimal),
		types.NewFieldType(mysql.TypeFloat),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeNull),
		types.NewFieldType(mysql.TypeTimestamp),
		types.NewFieldType(mysql.TypeDate),
		types.NewFieldType(mysql.TypeNewDate),
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeJSON),
		types.NewFieldType(mysql.TypeEnum),
		types.NewFieldType(mysql.TypeSet),
		types.NewFieldType(mysql.TypeTinyBlob),
		types.NewFieldType(mysql.TypeMediumBlob),
		types.NewFieldType(mysql.TypeLongBlob), types.NewFieldType(mysql.TypeBlob),
		types.NewFieldType(mysql.TypeVarString),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeGeometry),
	}
	plusFunctionClass, ok := funcs[ast.Plus].(*arithmeticPlusFunctionClass)
	c.Assert(ok, Equals, true)
	for i, ft := range fieldTps {
		retEvalTp := plusFunctionClass.getEvalTp(ft)
		if i < 8 {
			c.Assert(retEvalTp, Equals, tpInt)
		} else if i < 10 {
			c.Assert(retEvalTp, Equals, tpDecimal, Commentf("for field type: \"%s\"", ft))
		} else {
			c.Assert(retEvalTp, Equals, tpReal)
		}
	}
}

func (s *testEvaluatorSuite) TestPlusSetFlenDecimal4RealOrDecimal(c *C) {
	defer testleak.AfterTest(c)()
	plusFunctionClass, ok := funcs[ast.Plus].(*arithmeticPlusFunctionClass)
	c.Assert(ok, Equals, true)

	ret := &types.FieldType{}
	a := &types.FieldType{
		Decimal: 1,
		Flen:    3,
	}
	b := &types.FieldType{
		Decimal: 0,
		Flen:    2,
	}
	plusFunctionClass.setFlenDecimal4RealOrDecimal(ret, a, b, true)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, 5)

	b.Flen = 65
	plusFunctionClass.setFlenDecimal4RealOrDecimal(ret, a, b, true)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, mysql.MaxRealWidth)
	plusFunctionClass.setFlenDecimal4RealOrDecimal(ret, a, b, false)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, mysql.MaxDecimalWidth)

	b.Flen = types.UnspecifiedLength
	plusFunctionClass.setFlenDecimal4RealOrDecimal(ret, a, b, true)
	c.Assert(ret.Decimal, Equals, 1)
	c.Assert(ret.Flen, Equals, types.UnspecifiedLength)

	b.Decimal = types.UnspecifiedLength
	plusFunctionClass.setFlenDecimal4RealOrDecimal(ret, a, b, true)
	c.Assert(ret.Decimal, Equals, types.UnspecifiedLength)
	c.Assert(ret.Flen, Equals, types.UnspecifiedLength)
}

func (s *testEvaluatorSuite) TestPlusSetFlenDecimal4Int(c *C) {
	defer testleak.AfterTest(c)()
	plusFunctionClass, ok := funcs[ast.Plus].(*arithmeticPlusFunctionClass)
	c.Assert(ok, Equals, true)

	ret := &types.FieldType{}
	a := &types.FieldType{
		Decimal: 1,
		Flen:    3,
	}
	b := &types.FieldType{
		Decimal: 0,
		Flen:    2,
	}
	plusFunctionClass.setFlenDecimal4Int(ret, a, b)
	c.Assert(ret.Decimal, Equals, 0)
	c.Assert(ret.Flen, Equals, 4)

	b.Flen = mysql.MaxIntWidth + 1
	plusFunctionClass.setFlenDecimal4Int(ret, a, b)
	c.Assert(ret.Decimal, Equals, 0)
	c.Assert(ret.Flen, Equals, mysql.MaxIntWidth)

	b.Flen = types.UnspecifiedLength
	plusFunctionClass.setFlenDecimal4Int(ret, a, b)
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
		val, err := sig.eval(nil)
		c.Assert(err, IsNil)
		c.Assert(val, testutil.DatumEquals, types.NewDatum(tc.expect))
	}
}
