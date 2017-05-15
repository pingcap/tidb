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
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
	"math"
)

var bitCountCases = []struct {
	origin interface{}
	count  interface{}
}{
	{int64(8), int64(1)},
	{int64(29), int64(4)},
	{int64(0), int64(0)},
	{int64(-1), int64(64)},
	{int64(-11), int64(62)},
	{int64(-1000), int64(56)},
	{float64(1.1), int64(1)},
	{float64(3.1), int64(2)},
	{float64(-1.1), int64(64)},
	{float64(-3.1), int64(63)},
	{uint64(math.MaxUint64), int64(64)},
	{string("xxx"), int64(0)},
	{nil, nil},
}

func (s *testEvaluatorSuite) TestBitCount(c *C) {
	defer testleak.AfterTest(c)()
	fc := funcs[ast.BitCount]
	for _, test := range bitCountCases {
		in := types.NewDatum(test.origin)
		f, _ := fc.getFunction(datumsToConstants([]types.Datum{in}), s.ctx)
		count, err := f.eval(nil)
		c.Assert(err, IsNil)
		if count.IsNull() {
			c.Assert(test.count, IsNil)
			continue
		}
		sc := new(variable.StatementContext)
		sc.IgnoreTruncate = true
		res, err := count.ToInt64(sc)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, test.count)
	}
}

func (s *testEvaluatorSuite) TestCastFuncSig(c *C) {
	defer testleak.AfterTest(c)()
	ctx := s.ctx
	var sig builtinFunc

	// Test cast as Decimal.
	castToDecCases := []struct {
		before *Column
		after  *types.MyDecimal
		row    []types.Datum
	}{
		// cast int as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			types.NewDecFromInt(1),
			[]types.Datum{types.NewIntDatum(1)},
		},
		// cast string as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			types.NewDecFromInt(1),
			[]types.Datum{types.NewStringDatum("1")},
		},
		// cast real as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			types.NewDecFromInt(1),
			[]types.Datum{types.NewFloat64Datum(1)},
		},
	}
	for i, t := range castToDecCases {
		args := []Expression{t.before}
		decFunc := baseDecimalBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
		switch i {
		case 0:
			sig = &builtinCastIntAsDecimalSig{decFunc}
		case 1:
			sig = &builtinCastStringAsDecimalSig{decFunc}
		case 2:
			sig = &builtinCastRealAsDecimalSig{decFunc}
		}
		res, isNull, err := sig.evalDecimal(t.row)
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res.Compare(t.after), Equals, 0)
	}

	// Test cast as int.
	castToIntCases := []struct {
		before *Column
		after  int64
		row    []types.Datum
	}{
		// cast string as int.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			1,
			[]types.Datum{types.NewStringDatum("1")},
		},
		// cast decimal as int.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			1,
			[]types.Datum{types.NewDecimalDatum(types.NewDecFromInt(1))},
		},
		// cast real as int.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			1,
			[]types.Datum{types.NewFloat64Datum(1)},
		},
	}
	for i, t := range castToIntCases {
		args := []Expression{t.before}
		intFunc := baseIntBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
		switch i {
		case 0:
			sig = &builtinCastStringAsIntSig{intFunc}
		case 1:
			sig = &builtinCastDecimalAsIntSig{intFunc}
		case 2:
			sig = &builtinCastRealAsIntSig{intFunc}
		}
		res, isNull, err := sig.evalInt(t.row)
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, t.after)
	}

	// Test cast as real.
	castToRealCases := []struct {
		before *Column
		after  float64
		row    []types.Datum
	}{
		// cast string as real.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			1.1,
			[]types.Datum{types.NewStringDatum("1.1")},
		},
		// cast decimal as real.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			1.1,
			[]types.Datum{types.NewDecimalDatum(types.NewDecFromFloatForTest(1.1))},
		},
		// cast int as real.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			1,
			[]types.Datum{types.NewIntDatum(1)},
		},
	}
	for i, t := range castToRealCases {
		args := []Expression{t.before}
		realFunc := baseRealBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
		switch i {
		case 0:
			sig = &builtinCastStringAsRealSig{realFunc}
		case 1:
			sig = &builtinCastDecimalAsRealSig{realFunc}
		case 2:
			sig = &builtinCastIntAsRealSig{realFunc}
		}
		res, isNull, err := sig.evalReal(t.row)
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, t.after)
	}

	// Test cast as string.
	castToStringCases := []struct {
		before *Column
		after  string
		row    []types.Datum
	}{
		// cast real as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			"1",
			[]types.Datum{types.NewFloat64Datum(1)},
		},
		// cast decimal as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			"1",
			[]types.Datum{types.NewDecimalDatum(types.NewDecFromInt(1))},
		},
		// cast int as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			"1",
			[]types.Datum{types.NewIntDatum(1)},
		},
	}
	for i, t := range castToStringCases {
		args := []Expression{t.before}
		stringFunc := baseStringBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
		switch i {
		case 0:
			sig = &builtinCastRealAsStringSig{stringFunc}
		case 1:
			sig = &builtinCastDecimalAsStringSig{stringFunc}
		case 2:
			sig = &builtinCastIntAsStringSig{stringFunc}
		}
		res, isNull, err := sig.evalString(t.row)
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, t.after)
	}

	// null case
	args := []Expression{&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0}}
	row := []types.Datum{types.NewDatum(nil)}
	sig = &builtinCastRealAsStringSig{baseStringBuiltinFunc{newBaseBuiltinFunc(args, ctx)}}
	sRes, isNull, err := sig.evalString(row)
	c.Assert(sRes, Equals, "")
	c.Assert(isNull, Equals, true)
	c.Assert(err, IsNil)

	// test hybridType cases.
	args = []Expression{&Constant{types.NewDatum(types.Enum{Name: "a", Value: 0}), types.NewFieldType(mysql.TypeEnum)}}
	sig = &builtinCastStringAsIntSig{baseIntBuiltinFunc{newBaseBuiltinFunc(args, ctx)}}
	iRes, isNull, err := sig.evalInt(nil)
	c.Assert(isNull, Equals, false)
	c.Assert(err, IsNil)
	c.Assert(iRes, Equals, int64(0))

	time, err := types.ParseTime("2000-12-11 12:11:10", mysql.TypeDatetime, 0)
	c.Assert(err, IsNil)
	args = []Expression{&Constant{types.NewDatum(time), types.NewFieldType(mysql.TypeDatetime)}}
	sig = &builtinCastStringAsIntSig{baseIntBuiltinFunc{newBaseBuiltinFunc(args, ctx)}}
	iRes, isNull, err = sig.evalInt(nil)
	c.Assert(isNull, Equals, false)
	c.Assert(err, IsNil)
	c.Assert(iRes, Equals, int64(20001211121110))
}
