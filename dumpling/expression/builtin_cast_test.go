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
	"fmt"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
	"time"
)

func (s *testEvaluatorSuite) TestCast(c *C) {
	defer testleak.AfterTest(c)()
	ctx, sc := s.ctx, s.ctx.GetSessionVars().StmtCtx

	// Test `cast as char[(N)]` and `cast as binary[(N)]`.
	originIgnoreTruncate := sc.IgnoreTruncate
	sc.IgnoreTruncate = true
	defer func() {
		sc.IgnoreTruncate = originIgnoreTruncate
	}()

	tp := types.NewFieldType(mysql.TypeString)
	tp.Flen = 5

	// cast(str as char(N)), N < len([]rune(str)).
	// cast("你好world" as char(5))
	f := NewCastFunc(tp, &Constant{Value: types.NewDatum("你好world"), RetType: types.NewFieldType(mysql.TypeString)}, ctx)
	tp.Charset = charset.CharsetUTF8
	res, err := f.Eval(nil)
	c.Assert(err, IsNil)
	c.Assert(res.GetString(), Equals, "你好wor")

	// cast(str as char(N)), N > len([]rune(str)).
	// cast("a" as char(5))
	f = NewCastFunc(tp, &Constant{Value: types.NewDatum("a"), RetType: types.NewFieldType(mysql.TypeString)}, ctx)
	res, err = f.Eval(nil)
	c.Assert(err, IsNil)
	c.Assert(len(res.GetString()), Equals, 1)
	c.Assert(res.GetString(), Equals, "a")

	// cast(str as binary(N)), N < len(str).
	// cast("你好world" as binary(5))
	str := "你好world"
	tp.Flag |= mysql.BinaryFlag
	tp.Charset = charset.CharsetBin
	tp.Collate = charset.CollationBin
	f = NewCastFunc(tp, &Constant{Value: types.NewDatum(str), RetType: types.NewFieldType(mysql.TypeString)}, ctx)
	res, err = f.Eval(nil)
	c.Assert(err, IsNil)
	c.Assert(res.GetString(), Equals, str[:5])

	// cast(str as binary(N)), N > len([]byte(str)).
	// cast("a" as binary(5))
	f = NewCastFunc(tp, &Constant{Value: types.NewDatum("a"), RetType: types.NewFieldType(mysql.TypeString)}, ctx)
	res, err = f.Eval(nil)
	c.Assert(err, IsNil)
	c.Assert(len(res.GetString()), Equals, 5)
	c.Assert(res.GetString(), Equals, string([]byte{'a', 0x00, 0x00, 0x00, 0x00}))
}

func (s *testEvaluatorSuite) TestCastFuncSig(c *C) {
	defer testleak.AfterTest(c)()
	ctx := s.ctx
	var sig builtinFunc

	// durationDatum indicates duration "12:59:59".
	durationDatum := types.Datum{}
	duration := types.Duration{
		Duration: time.Duration(12*time.Hour + 59*time.Minute + 59*time.Second),
		Fsp:      types.DefaultFsp}
	durationDatum.SetMysqlDuration(duration)

	// timeDuration indicates datetime "curYear-curMonth-curDay 12:59:59".
	timeDatum := types.Datum{}
	year, month, day := time.Now().Date()
	tm := types.Time{
		Time: types.FromDate(year, int(month), day, 12, 59, 59, 0),
		Type: mysql.TypeDatetime,
		Fsp:  types.DefaultFsp}
	timeDatum.SetMysqlTime(tm)

	curDateInt := int64(year*10000 + int(month)*100 + day)
	curTimeInt := int64(curDateInt*1000000 + 125959)
	curTimeString := fmt.Sprintf("%4d-%02d-%02d 12:59:59", year, int(month), day)

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
		// cast Time as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			types.NewDecFromInt(curTimeInt),
			[]types.Datum{timeDatum},
		},
		// cast Duration as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			types.NewDecFromInt(125959),
			[]types.Datum{durationDatum},
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
		case 3:
			sig = &builtinCastTimeAsDecimalSig{decFunc}
		case 4:
			sig = &builtinCastDurationAsDecimalSig{decFunc}
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
		// cast Time as int.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			curTimeInt,
			[]types.Datum{timeDatum},
		},
		// cast Duration as int.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			125959,
			[]types.Datum{durationDatum},
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
		case 3:
			sig = &builtinCastTimeAsIntSig{intFunc}
		case 4:
			sig = &builtinCastDurationAsIntSig{intFunc}
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
		// cast Time as real.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			float64(curTimeInt),
			[]types.Datum{timeDatum},
		},
		// cast Duration as real.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			125959,
			[]types.Datum{durationDatum},
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
		case 3:
			sig = &builtinCastTimeAsRealSig{realFunc}
		case 4:
			sig = &builtinCastDurationAsRealSig{realFunc}
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
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			curTimeString,
			[]types.Datum{timeDatum},
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			"12:59:59",
			[]types.Datum{durationDatum},
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
		case 3:
			sig = &builtinCastTimeAsStringSig{stringFunc}
		case 4:
			sig = &builtinCastDurationAsStringSig{stringFunc}
		}
		res, isNull, err := sig.evalString(t.row)
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, t.after)
	}

	castToTimeCases := []struct {
		before *Column
		after  types.Time
		row    []types.Datum
	}{
		// cast real as Time.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			tm,
			[]types.Datum{types.NewFloat64Datum(float64(curTimeInt))},
		},
		// cast decimal as Time.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			tm,
			[]types.Datum{types.NewDecimalDatum(types.NewDecFromInt(curTimeInt))},
		},
		// cast int as Time.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			tm,
			[]types.Datum{types.NewIntDatum(curTimeInt)},
		},
		// cast string as Time.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			tm,
			[]types.Datum{types.NewStringDatum(curTimeString)},
		},
		// cast Duration as Time.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			tm,
			[]types.Datum{durationDatum},
		},
	}
	for i, t := range castToTimeCases {
		args := []Expression{t.before}
		timeFunc := baseTimeBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
		tp, fsp := mysql.TypeDatetime, types.DefaultFsp
		switch i {
		case 0:
			sig = &builtinCastRealAsTimeSig{timeFunc, tp, fsp}
		case 1:
			sig = &builtinCastDecimalAsTimeSig{timeFunc, tp}
		case 2:
			sig = &builtinCastIntAsTimeSig{timeFunc}
		case 3:
			sig = &builtinCastStringAsTimeSig{timeFunc, tp, fsp}
		case 4:
			sig = &builtinCastDurationAsTimeSig{timeFunc, tp}
		}
		res, isNull, err := sig.evalTime(t.row)
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res.String(), Equals, t.after.String())
	}

	castToDurationCases := []struct {
		before *Column
		after  types.Duration
		row    []types.Datum
	}{
		// cast real as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			duration,
			[]types.Datum{types.NewFloat64Datum(125959)},
		},
		// cast decimal as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			duration,
			[]types.Datum{types.NewDecimalDatum(types.NewDecFromInt(125959))},
		},
		// cast int as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			duration,
			[]types.Datum{types.NewIntDatum(125959)},
		},
		// cast string as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			duration,
			[]types.Datum{types.NewStringDatum("12:59:59")},
		},
		// cast Time as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			duration,
			[]types.Datum{timeDatum},
		},
	}
	for i, t := range castToDurationCases {
		args := []Expression{t.before}
		durationFunc := baseDurationBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
		fsp := types.DefaultFsp
		switch i {
		case 0:
			sig = &builtinCastRealAsDurationSig{durationFunc, fsp}
		case 1:
			sig = &builtinCastDecimalAsDurationSig{durationFunc}
		case 2:
			sig = &builtinCastIntAsDurationSig{durationFunc, fsp}
		case 3:
			sig = &builtinCastStringAsDurationSig{durationFunc, fsp}
		case 4:
			sig = &builtinCastTimeAsDurationSig{durationFunc}
		}
		res, isNull, err := sig.evalDuration(t.row)
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res.String(), Equals, t.after.String())
	}

	// null case
	args := []Expression{&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0}}
	row := []types.Datum{types.NewDatum(nil)}
	sig = &builtinCastRealAsStringSig{baseStringBuiltinFunc{newBaseBuiltinFunc(args, ctx)}}
	sRes, isNull, err := sig.evalString(row)
	c.Assert(sRes, Equals, "0")
	c.Assert(isNull, Equals, true)
	c.Assert(err, IsNil)

	// test hybridType cases.
	args = []Expression{&Constant{types.NewDatum(types.Enum{Name: "a", Value: 0}), types.NewFieldType(mysql.TypeEnum)}}
	sig = &builtinCastStringAsIntSig{baseIntBuiltinFunc{newBaseBuiltinFunc(args, ctx)}}
	iRes, isNull, err := sig.evalInt(nil)
	c.Assert(isNull, Equals, false)
	c.Assert(err, IsNil)
	c.Assert(iRes, Equals, int64(0))
}
