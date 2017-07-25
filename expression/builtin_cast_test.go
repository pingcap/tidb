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
	"math"
	"strconv"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
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

	// cast(bad_string as decimal)
	for _, s := range []string{"hello", ""} {
		f = NewCastFunc(tp, &Constant{Value: types.NewDatum(s), RetType: types.NewFieldType(mysql.TypeDecimal)}, ctx)
		res, err = f.Eval(nil)
		c.Assert(err, IsNil)
	}

	// cast(1234 as char(0))
	tp.Flen = 0
	tp.Charset = charset.CharsetUTF8
	f = NewCastFunc(tp, &Constant{Value: types.NewDatum(1234), RetType: types.NewFieldType(mysql.TypeString)}, ctx)
	res, err = f.Eval(nil)
	c.Assert(err, IsNil)
	c.Assert(len(res.GetString()), Equals, 0)
	c.Assert(res.GetString(), Equals, "")
}

var (
	year, month, day     = time.Now().Date()
	curDateInt           = int64(year*10000 + int(month)*100 + day)
	curTimeInt           = int64(curDateInt*1000000 + 125959)
	curTimeWithFspReal   = float64(curTimeInt) + 0.555
	curTimeString        = fmt.Sprintf("%4d-%02d-%02d 12:59:59", year, int(month), day)
	curTimeWithFspString = fmt.Sprintf("%4d-%02d-%02d 12:59:59.555000", year, int(month), day)
	tm                   = types.Time{
		Time: types.FromDate(year, int(month), day, 12, 59, 59, 0),
		Type: mysql.TypeDatetime,
		Fsp:  types.DefaultFsp}
	tmWithFsp = types.Time{
		Time: types.FromDate(year, int(month), day, 12, 59, 59, 555000),
		Type: mysql.TypeDatetime,
		Fsp:  types.MaxFsp}
	// timeDatum indicates datetime "curYear-curMonth-curDay 12:59:59".
	timeDatum = types.NewDatum(tm)
	// timeWithFspDatum indicates datetime "curYear-curMonth-curDay 12:59:59.555000".
	timeWithFspDatum = types.NewDatum(tmWithFsp)
	duration         = types.Duration{
		Duration: time.Duration(12*time.Hour + 59*time.Minute + 59*time.Second),
		Fsp:      types.DefaultFsp}
	// durationDatum indicates duration "12:59:59".
	durationDatum   = types.NewDatum(duration)
	durationWithFsp = types.Duration{
		Duration: time.Duration(12*time.Hour + 59*time.Minute + 59*time.Second + 555*time.Millisecond),
		Fsp:      3}
	// durationWithFspDatum indicates duration "12:59:59.555"
	durationWithFspDatum = types.NewDatum(durationWithFsp)
	dt                   = types.Time{
		Time: types.FromDate(year, int(month), day, 0, 0, 0, 0),
		Type: mysql.TypeDate,
		Fsp:  types.DefaultFsp}
)

func (s *testEvaluatorSuite) TestCastFuncSig(c *C) {
	defer testleak.AfterTest(c)()
	ctx, sc := s.ctx, s.ctx.GetSessionVars().StmtCtx
	originIgnoreTruncate := sc.IgnoreTruncate
	sc.IgnoreTruncate = true
	defer func() {
		sc.IgnoreTruncate = originIgnoreTruncate
	}()
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
		decFunc.tp = types.NewFieldType(mysql.TypeNewDecimal)
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
		case 5:
			sig = &builtinCastDecimalAsDecimalSig{decFunc}
		}
		res, isNull, err := sig.evalDecimal(t.row)
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res.Compare(t.after), Equals, 0)
	}

	castToDecCases2 := []struct {
		before  *Column
		flen    int
		decimal int
		after   *types.MyDecimal
		row     []types.Datum
	}{
		// cast int as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			7,
			3,
			types.NewDecFromStringForTest("1234.000"),
			[]types.Datum{types.NewIntDatum(1234)},
		},
		// cast string as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			7,
			3,
			types.NewDecFromStringForTest("1234.000"),
			[]types.Datum{types.NewStringDatum("1234")},
		},
		// cast real as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			8,
			4,
			types.NewDecFromStringForTest("1234.1230"),
			[]types.Datum{types.NewFloat64Datum(1234.123)},
		},
		// cast Time as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			15,
			1,
			types.NewDecFromStringForTest(strconv.FormatInt(curTimeInt, 10) + ".0"),
			[]types.Datum{timeDatum},
		},
		// cast Duration as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			7,
			1,
			types.NewDecFromStringForTest("125959.0"),
			[]types.Datum{durationDatum},
		},
		// cast decimal as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			7,
			3,
			types.NewDecFromStringForTest("1234.000"),
			[]types.Datum{types.NewDecimalDatum(types.NewDecFromStringForTest("1234"))},
		},
	}

	for i, t := range castToDecCases2 {
		args := []Expression{t.before}
		tp := types.NewFieldType(mysql.TypeNewDecimal)
		tp.Flen, tp.Decimal = t.flen, t.decimal
		decFunc := baseDecimalBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
		decFunc.tp = tp
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
		case 5:
			sig = &builtinCastDecimalAsDecimalSig{decFunc}
		}
		res, isNull, err := sig.evalDecimal(t.row)
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res.ToString(), DeepEquals, t.after.ToString())
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
		// cast time as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			curTimeString,
			[]types.Datum{timeDatum},
		},
		// cast duration as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			"12:59:59",
			[]types.Datum{durationDatum},
		},
		// cast string as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			"1234",
			[]types.Datum{types.NewStringDatum("1234")},
		},
	}
	for i, t := range castToStringCases {
		tp := types.NewFieldType(mysql.TypeVarString)
		tp.Charset = charset.CharsetBin
		args := []Expression{t.before}
		stringFunc := baseStringBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
		stringFunc.tp = tp
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
		case 5:
			sig = &builtinCastStringAsStringSig{stringFunc}
		}
		res, isNull, err := sig.evalString(t.row)
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res, Equals, t.after)
	}

	// Test cast as string.
	castToStringCases2 := []struct {
		before *Column
		after  string
		flen   int
		row    []types.Datum
	}{
		// cast real as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			"123",
			3,
			[]types.Datum{types.NewFloat64Datum(1234.123)},
		},
		// cast decimal as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			"123",
			3,
			[]types.Datum{types.NewDecimalDatum(types.NewDecFromStringForTest("1234.123"))},
		},
		// cast int as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			"123",
			3,
			[]types.Datum{types.NewIntDatum(1234)},
		},
		// cast time as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			curTimeString[:3],
			3,
			[]types.Datum{timeDatum},
		},
		// cast duration as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			"12:",
			3,
			[]types.Datum{durationDatum},
		},
		// cast string as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			"你好w",
			3,
			[]types.Datum{types.NewStringDatum("你好world")},
		},
	}
	for i, t := range castToStringCases2 {
		args := []Expression{t.before}
		tp := types.NewFieldType(mysql.TypeVarString)
		tp.Flen, tp.Charset = t.flen, charset.CharsetBin
		stringFunc := baseStringBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
		stringFunc.tp = tp
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
		case 5:
			stringFunc.tp.Charset = charset.CharsetUTF8
			sig = &builtinCastStringAsStringSig{stringFunc}
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
		// cast Time as Time.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			tm,
			[]types.Datum{timeDatum},
		},
	}
	for i, t := range castToTimeCases {
		args := []Expression{t.before}
		tp := types.NewFieldType(mysql.TypeDatetime)
		tp.Decimal = types.DefaultFsp
		timeFunc := baseTimeBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
		timeFunc.tp = tp
		switch i {
		case 0:
			sig = &builtinCastRealAsTimeSig{timeFunc}
		case 1:
			sig = &builtinCastDecimalAsTimeSig{timeFunc}
		case 2:
			sig = &builtinCastIntAsTimeSig{timeFunc}
		case 3:
			sig = &builtinCastStringAsTimeSig{timeFunc}
		case 4:
			sig = &builtinCastDurationAsTimeSig{timeFunc}
		case 5:
			sig = &builtinCastTimeAsTimeSig{timeFunc}
		}
		res, isNull, err := sig.evalTime(t.row)
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res.String(), Equals, t.after.String())
	}

	castToTimeCases2 := []struct {
		before *Column
		after  types.Time
		row    []types.Datum
		fsp    int
		tp     byte
	}{
		// cast real as Time(0).
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			dt,
			[]types.Datum{types.NewFloat64Datum(float64(curTimeInt))},
			types.DefaultFsp,
			mysql.TypeDate,
		},
		// cast decimal as Date.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			dt,
			[]types.Datum{types.NewDecimalDatum(types.NewDecFromInt(curTimeInt))},
			types.DefaultFsp,
			mysql.TypeDate,
		},
		// cast int as Datetime(6).
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			tm,
			[]types.Datum{types.NewIntDatum(curTimeInt)},
			types.MaxFsp,
			mysql.TypeDatetime,
		},
		// cast string as Datetime(6).
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			tm,
			[]types.Datum{types.NewStringDatum(curTimeString)},
			types.MaxFsp,
			mysql.TypeDatetime,
		},
		// cast Duration as Date.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			dt,
			[]types.Datum{durationDatum},
			types.DefaultFsp,
			mysql.TypeDate,
		},
		// cast Time as Date.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			dt,
			[]types.Datum{timeDatum},
			types.DefaultFsp,
			mysql.TypeDate,
		},
	}
	for i, t := range castToTimeCases2 {
		args := []Expression{t.before}
		tp := types.NewFieldType(t.tp)
		tp.Decimal = t.fsp
		timeFunc := baseTimeBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
		timeFunc.tp = tp
		switch i {
		case 0:
			sig = &builtinCastRealAsTimeSig{timeFunc}
		case 1:
			sig = &builtinCastDecimalAsTimeSig{timeFunc}
		case 2:
			sig = &builtinCastIntAsTimeSig{timeFunc}
		case 3:
			sig = &builtinCastStringAsTimeSig{timeFunc}
		case 4:
			sig = &builtinCastDurationAsTimeSig{timeFunc}
		case 5:
			sig = &builtinCastTimeAsTimeSig{timeFunc}
		}
		res, isNull, err := sig.evalTime(t.row)
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		resAfter := t.after.String()
		if t.fsp > 0 {
			resAfter += "."
			for i := 0; i < t.fsp; i++ {
				resAfter += "0"
			}
		}
		c.Assert(res.String(), Equals, resAfter)
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
		// cast Duration as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			duration,
			[]types.Datum{durationDatum},
		},
	}
	for i, t := range castToDurationCases {
		args := []Expression{t.before}
		tp := types.NewFieldType(mysql.TypeDuration)
		tp.Decimal = types.DefaultFsp
		durationFunc := baseDurationBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
		durationFunc.tp = tp
		switch i {
		case 0:
			sig = &builtinCastRealAsDurationSig{durationFunc}
		case 1:
			sig = &builtinCastDecimalAsDurationSig{durationFunc}
		case 2:
			sig = &builtinCastIntAsDurationSig{durationFunc}
		case 3:
			sig = &builtinCastStringAsDurationSig{durationFunc}
		case 4:
			sig = &builtinCastTimeAsDurationSig{durationFunc}
		case 5:
			sig = &builtinCastDurationAsDurationSig{durationFunc}
		}
		res, isNull, err := sig.evalDuration(t.row)
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res.String(), Equals, t.after.String())
	}

	castToDurationCases2 := []struct {
		before *Column
		after  types.Duration
		row    []types.Datum
		fsp    int
	}{
		// cast real as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			duration,
			[]types.Datum{types.NewFloat64Datum(125959)},
			1,
		},
		// cast decimal as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			duration,
			[]types.Datum{types.NewDecimalDatum(types.NewDecFromInt(125959))},
			2,
		},
		// cast int as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			duration,
			[]types.Datum{types.NewIntDatum(125959)},
			3,
		},
		// cast string as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			duration,
			[]types.Datum{types.NewStringDatum("12:59:59")},
			4,
		},
		// cast Time as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			duration,
			[]types.Datum{timeDatum},
			5,
		},
		// cast Duration as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			duration,
			[]types.Datum{durationDatum},
			6,
		},
	}
	for i, t := range castToDurationCases2 {
		args := []Expression{t.before}
		tp := types.NewFieldType(mysql.TypeDuration)
		tp.Decimal = t.fsp
		durationFunc := baseDurationBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
		durationFunc.tp = tp
		switch i {
		case 0:
			sig = &builtinCastRealAsDurationSig{durationFunc}
		case 1:
			sig = &builtinCastDecimalAsDurationSig{durationFunc}
		case 2:
			sig = &builtinCastIntAsDurationSig{durationFunc}
		case 3:
			sig = &builtinCastStringAsDurationSig{durationFunc}
		case 4:
			sig = &builtinCastTimeAsDurationSig{durationFunc}
		case 5:
			sig = &builtinCastDurationAsDurationSig{durationFunc}
		}
		res, isNull, err := sig.evalDuration(t.row)
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		resAfter := t.after.String()
		if t.fsp > 0 {
			resAfter += "."
			for j := 0; j < t.fsp; j++ {
				resAfter += "0"
			}
		}
		c.Assert(res.String(), Equals, resAfter)
	}

	// null case
	args := []Expression{&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0}}
	row := []types.Datum{types.NewDatum(nil)}
	bf := baseStringBuiltinFunc{newBaseBuiltinFunc(args, ctx)}
	bf.tp = types.NewFieldType(mysql.TypeVarString)
	sig = &builtinCastRealAsStringSig{bf}
	sRes, isNull, err := sig.evalString(row)
	c.Assert(sRes, Equals, "")
	c.Assert(isNull, Equals, true)
	c.Assert(err, IsNil)

	// test hybridType case.
	args = []Expression{&Constant{types.NewDatum(types.Enum{Name: "a", Value: 0}), types.NewFieldType(mysql.TypeEnum)}}
	sig = &builtinCastStringAsIntSig{baseIntBuiltinFunc{newBaseBuiltinFunc(args, ctx)}}
	iRes, isNull, err := sig.evalInt(nil)
	c.Assert(isNull, Equals, false)
	c.Assert(err, IsNil)
	c.Assert(iRes, Equals, int64(0))
}

// TestWrapWithCastAsTypesClasses tests WrapWithCastAsInt/Real/String/Decimal.
func (s *testEvaluatorSuite) TestWrapWithCastAsTypesClasses(c *C) {
	defer testleak.AfterTest(c)()
	ctx, sc := s.ctx, s.ctx.GetSessionVars().StmtCtx

	cases := []struct {
		expr      Expression
		row       []types.Datum
		intRes    int64
		realRes   float64
		decRes    *types.MyDecimal
		stringRes string
	}{
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLong), Index: 0},
			[]types.Datum{types.NewDatum(123)},
			123, 123, types.NewDecFromInt(123), "123",
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			[]types.Datum{types.NewDatum(123.555)},
			124, 123.555, types.NewDecFromFloatForTest(123.555), "123.555",
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			[]types.Datum{types.NewDatum(123.123)},
			123, 123.123, types.NewDecFromFloatForTest(123.123), "123.123",
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			[]types.Datum{types.NewDecimalDatum(types.NewDecFromStringForTest("123.123"))},
			123, 123.123, types.NewDecFromFloatForTest(123.123), "123.123",
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			[]types.Datum{types.NewDecimalDatum(types.NewDecFromStringForTest("123.555"))},
			124, 123.555, types.NewDecFromFloatForTest(123.555), "123.555",
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeVarString), Index: 0},
			[]types.Datum{types.NewStringDatum("123.123")},
			123, 123.123, types.NewDecFromStringForTest("123.123"), "123.123",
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			[]types.Datum{timeDatum},
			curTimeInt, float64(curTimeInt), types.NewDecFromInt(curTimeInt), curTimeString,
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			[]types.Datum{timeWithFspDatum},
			int64(curDateInt*1000000 + 130000), curTimeWithFspReal, types.NewDecFromFloatForTest(curTimeWithFspReal), curTimeWithFspString,
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			[]types.Datum{durationDatum},
			125959, 125959, types.NewDecFromFloatForTest(125959), "12:59:59",
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			[]types.Datum{durationWithFspDatum},
			130000, 125959.555, types.NewDecFromFloatForTest(125959.555), "12:59:59.555",
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeEnum), Index: 0},
			[]types.Datum{types.NewDatum(types.Enum{Name: "a", Value: 123})},
			123, 123, types.NewDecFromStringForTest("123"), "a",
		},
		{
			&Constant{RetType: types.NewFieldType(mysql.TypeVarString), Value: types.NewDatum(types.Hex{Value: 0x61})},
			nil,
			97, 97, types.NewDecFromInt(97), "a",
		},
	}
	for _, t := range cases {
		// Test wrapping with CastAsInt.
		intExpr, err := WrapWithCastAsInt(t.expr, ctx)
		c.Assert(err, IsNil)
		c.Assert(intExpr.GetTypeClass(), Equals, types.ClassInt)
		_, ok := intExpr.(*ScalarFunction)
		c.Assert(ok, Equals, t.expr.GetTypeClass() != types.ClassInt)
		intRes, isNull, err := intExpr.EvalInt(t.row, sc)
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(intRes, Equals, t.intRes)

		// Test wrapping with CastAsReal.
		realExpr, err := WrapWithCastAsReal(t.expr, ctx)
		c.Assert(err, IsNil)
		c.Assert(realExpr.GetTypeClass(), Equals, types.ClassReal)
		_, ok = realExpr.(*ScalarFunction)
		c.Assert(ok, Equals, t.expr.GetTypeClass() != types.ClassReal)
		realRes, isNull, err := realExpr.EvalReal(t.row, sc)
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(realRes, Equals, t.realRes)

		// Test wrapping with CastAsDecimal.
		decExpr, err := WrapWithCastAsDecimal(t.expr, ctx)
		c.Assert(err, IsNil)
		c.Assert(decExpr.GetTypeClass(), Equals, types.ClassDecimal)
		_, ok = decExpr.(*ScalarFunction)
		c.Assert(ok, Equals, t.expr.GetTypeClass() != types.ClassDecimal)
		decRes, isNull, err := decExpr.EvalDecimal(t.row, sc)
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(decRes.Compare(t.decRes), Equals, 0)

		// Test wrapping with CastAsString.
		strExpr, err := WrapWithCastAsString(t.expr, ctx)
		c.Assert(err, IsNil)
		c.Assert(strExpr.GetTypeClass(), Equals, types.ClassString)
		_, ok = strExpr.(*ScalarFunction)
		c.Assert(ok, Equals, t.expr.GetTypeClass() != types.ClassString)
		strRes, isNull, err := strExpr.EvalString(t.row, sc)
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(strRes, Equals, t.stringRes)
	}

	unsignedIntExpr := &Column{RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag, Flen: mysql.MaxIntWidth, Decimal: 0}, Index: 0}

	// test cast unsigned int as string.
	strExpr, err := WrapWithCastAsString(unsignedIntExpr, ctx)
	c.Assert(err, IsNil)
	c.Assert(strExpr.GetTypeClass(), Equals, types.ClassString)
	strRes, isNull, err := strExpr.EvalString([]types.Datum{types.NewUintDatum(math.MaxUint64)}, sc)
	c.Assert(err, IsNil)
	c.Assert(strRes, Equals, strconv.FormatUint(math.MaxUint64, 10))
	c.Assert(isNull, Equals, false)

	strRes, isNull, err = strExpr.EvalString([]types.Datum{types.NewUintDatum(1234)}, sc)
	c.Assert(err, IsNil)
	c.Assert(isNull, Equals, false)
	c.Assert(strRes, Equals, strconv.FormatUint(uint64(1234), 10))

	// test cast unsigned int as decimal.
	decExpr, err := WrapWithCastAsDecimal(unsignedIntExpr, ctx)
	c.Assert(err, IsNil)
	c.Assert(decExpr.GetTypeClass(), Equals, types.ClassDecimal)
	decRes, isNull, err := decExpr.EvalDecimal([]types.Datum{types.NewUintDatum(uint64(1234))}, sc)
	c.Assert(err, IsNil)
	c.Assert(isNull, Equals, false)
	c.Assert(decRes.Compare(types.NewDecFromUint(uint64(1234))), Equals, 0)

	// test cast unsigned int as Time.
	timeExpr, err := WrapWithCastAsTime(unsignedIntExpr, types.NewFieldType(mysql.TypeDatetime), ctx)
	c.Assert(err, IsNil)
	c.Assert(timeExpr.GetType().Tp, Equals, mysql.TypeDatetime)
	timeRes, isNull, err := timeExpr.EvalTime([]types.Datum{types.NewUintDatum(uint64(curTimeInt))}, sc)
	c.Assert(err, IsNil)
	c.Assert(isNull, Equals, false)
	c.Assert(timeRes.Compare(tm), Equals, 0)
}

func (s *testEvaluatorSuite) TestWrapWithCastAsTime(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		expr Expression
		tp   *types.FieldType
		res  types.Time
	}{
		{
			&Constant{RetType: types.NewFieldType(mysql.TypeLong), Value: types.NewIntDatum(curTimeInt)},
			types.NewFieldType(mysql.TypeDate),
			dt,
		},
		{
			&Constant{RetType: types.NewFieldType(mysql.TypeDouble), Value: types.NewFloat64Datum(float64(curTimeInt))},
			types.NewFieldType(mysql.TypeDatetime),
			tm,
		},
		{
			&Constant{RetType: types.NewFieldType(mysql.TypeNewDecimal), Value: types.NewDecimalDatum(types.NewDecFromInt(curTimeInt))},
			types.NewFieldType(mysql.TypeDate),
			dt,
		},
		{
			&Constant{RetType: types.NewFieldType(mysql.TypeVarString), Value: types.NewStringDatum(curTimeString)},
			types.NewFieldType(mysql.TypeDatetime),
			tm,
		},
		{
			&Constant{RetType: types.NewFieldType(mysql.TypeDatetime), Value: timeDatum},
			types.NewFieldType(mysql.TypeDate),
			dt,
		},
		{
			&Constant{RetType: types.NewFieldType(mysql.TypeDuration), Value: durationDatum},
			types.NewFieldType(mysql.TypeDatetime),
			tm,
		},
	}
	for _, t := range cases {
		expr, err := WrapWithCastAsTime(t.expr, t.tp, s.ctx)
		c.Assert(err, IsNil)
		_, ok := expr.(*ScalarFunction)
		c.Assert(ok, Equals, t.expr.GetType().Tp != t.tp.Tp)
		res, isNull, err := expr.EvalTime(nil, s.ctx.GetSessionVars().StmtCtx)
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(res.Type, Equals, t.tp.Tp)
		c.Assert(res.Compare(t.res), Equals, 0)
	}
}

func (s *testEvaluatorSuite) TestWrapWithCastAsDuration(c *C) {
	defer testleak.AfterTest(c)()
	cases := []struct {
		expr Expression
	}{
		{
			&Constant{RetType: types.NewFieldType(mysql.TypeLong), Value: types.NewIntDatum(125959)},
		},
		{
			&Constant{RetType: types.NewFieldType(mysql.TypeDouble), Value: types.NewFloat64Datum(125959)},
		},
		{
			&Constant{RetType: types.NewFieldType(mysql.TypeNewDecimal), Value: types.NewDecimalDatum(types.NewDecFromInt(125959))},
		},
		{
			&Constant{RetType: types.NewFieldType(mysql.TypeVarString), Value: types.NewStringDatum("125959")},
		},
		{
			&Constant{RetType: types.NewFieldType(mysql.TypeDatetime), Value: timeDatum},
		},
		{
			&Constant{RetType: types.NewFieldType(mysql.TypeDuration), Value: durationDatum},
		},
	}
	for _, t := range cases {
		expr, err := WrapWithCastAsDuration(t.expr, s.ctx)
		c.Assert(err, IsNil)
		_, ok := expr.(*ScalarFunction)
		c.Assert(ok, Equals, t.expr.GetType().Tp != mysql.TypeDuration)
		res, isNull, err := expr.EvalDuration(nil, s.ctx.GetSessionVars().StmtCtx)
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(res.Compare(duration), Equals, 0)
	}
}
