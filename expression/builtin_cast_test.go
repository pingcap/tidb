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
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/testleak"
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
	tp.Charset = charset.CharsetUTF8
	f := BuildCastFunction(ctx, &Constant{Value: types.NewDatum("你好world"), RetType: tp}, tp)
	res, err := f.Eval(nil)
	c.Assert(err, IsNil)
	c.Assert(res.GetString(), Equals, "你好wor")

	// cast(str as char(N)), N > len([]rune(str)).
	// cast("a" as char(5))
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("a"), RetType: types.NewFieldType(mysql.TypeString)}, tp)
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
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum(str), RetType: types.NewFieldType(mysql.TypeString)}, tp)
	res, err = f.Eval(nil)
	c.Assert(err, IsNil)
	c.Assert(res.GetString(), Equals, str[:5])

	// cast(str as binary(N)), N > len([]byte(str)).
	// cast("a" as binary(5))
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("a"), RetType: types.NewFieldType(mysql.TypeString)}, tp)
	res, err = f.Eval(nil)
	c.Assert(err, IsNil)
	c.Assert(len(res.GetString()), Equals, 5)
	c.Assert(res.GetString(), Equals, string([]byte{'a', 0x00, 0x00, 0x00, 0x00}))

	origSc := sc
	sc.InSelectStmt = true
	sc.OverflowAsWarning = true

	// cast('18446744073709551616' as unsigned);
	tp1 := &types.FieldType{
		Tp:      mysql.TypeLonglong,
		Flag:    mysql.BinaryFlag,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
		Flen:    mysql.MaxIntWidth,
	}
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("18446744073709551616"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err = f.Eval(nil)
	c.Assert(err, IsNil)
	c.Assert(res.GetUint64() == math.MaxUint64, IsTrue)

	warnings := sc.GetWarnings()
	lastWarn := warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn), IsTrue)

	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("-1"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err = f.Eval(nil)
	c.Assert(err, IsNil)
	c.Assert(res.GetUint64() == 18446744073709551615, IsTrue)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrCastNegIntAsUnsigned, lastWarn), IsTrue)

	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("-18446744073709551616"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err = f.Eval(nil)
	c.Assert(err, IsNil)
	t := math.MinInt64
	// 9223372036854775808
	c.Assert(res.GetUint64() == uint64(t), IsTrue)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn), IsTrue)

	// cast('18446744073709551616' as signed);
	mask := ^mysql.UnsignedFlag
	tp1.Flag &= uint(mask)
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("18446744073709551616"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err = f.Eval(nil)
	c.Assert(err, IsNil)
	c.Check(res.GetInt64(), Equals, int64(-1))

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn), IsTrue)

	// cast('18446744073709551614' as signed);
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("18446744073709551614"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err = f.Eval(nil)
	c.Assert(err, IsNil)
	c.Check(res.GetInt64(), Equals, int64(-2))

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrCastAsSignedOverflow, lastWarn), IsTrue)

	// create table t1(s1 time);
	// insert into t1 values('11:11:11');
	// select cast(s1 as decimal(7, 2)) from t1;
	ft := &types.FieldType{
		Tp:      mysql.TypeNewDecimal,
		Flag:    mysql.BinaryFlag | mysql.UnsignedFlag,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
		Flen:    7,
		Decimal: 2,
	}
	f = BuildCastFunction(ctx, &Constant{Value: timeDatum, RetType: types.NewFieldType(mysql.TypeDatetime)}, ft)
	res, err = f.Eval(nil)
	c.Assert(err, IsNil)
	resDecimal := new(types.MyDecimal)
	resDecimal.FromString([]byte("99999.99"))
	c.Assert(res.GetMysqlDecimal().Compare(resDecimal), Equals, 0)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(types.ErrOverflow, lastWarn), IsTrue)
	sc = origSc

	// cast(bad_string as decimal)
	for _, s := range []string{"hello", ""} {
		f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum(s), RetType: types.NewFieldType(mysql.TypeDecimal)}, tp)
		res, err = f.Eval(nil)
		c.Assert(err, IsNil)
	}

	// cast(1234 as char(0))
	tp.Flen = 0
	tp.Charset = charset.CharsetUTF8
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum(1234), RetType: types.NewFieldType(mysql.TypeString)}, tp)
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

	// jsonInt indicates json(3)
	jsonInt = types.NewDatum(json.CreateBinary(int64(3)))

	// jsonTime indicates "CURRENT_DAY 12:59:59"
	jsonTime = types.NewDatum(json.CreateBinary(tm.String()))

	// jsonDuration indicates
	jsonDuration = types.NewDatum(json.CreateBinary(duration.String()))
)

func (s *testEvaluatorSuite) TestCastFuncSig(c *C) {
	defer testleak.AfterTest(c)()
	ctx, sc := s.ctx, s.ctx.GetSessionVars().StmtCtx
	originIgnoreTruncate := sc.IgnoreTruncate
	originTZ := sc.TimeZone
	sc.IgnoreTruncate = true
	sc.TimeZone = time.Local
	defer func() {
		sc.IgnoreTruncate = originIgnoreTruncate
		sc.TimeZone = originTZ
	}()
	var sig builtinFunc

	// Test cast as Decimal.
	castToDecCases := []struct {
		before *Column
		after  *types.MyDecimal
		row    types.DatumRow
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
		decFunc := newBaseBuiltinFunc(ctx, args)
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
		row     types.DatumRow
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
		decFunc := newBaseBuiltinFunc(ctx, args)
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
		row    types.DatumRow
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
		// cast JSON as int.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeJSON), Index: 0},
			3,
			[]types.Datum{jsonInt},
		},
	}
	for i, t := range castToIntCases {
		args := []Expression{t.before}
		intFunc := newBaseBuiltinFunc(ctx, args)
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
		case 5:
			sig = &builtinCastJSONAsIntSig{intFunc}
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
		row    types.DatumRow
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
		// cast JSON as real.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeJSON), Index: 0},
			3.0,
			[]types.Datum{jsonInt},
		},
	}
	for i, t := range castToRealCases {
		args := []Expression{t.before}
		realFunc := newBaseBuiltinFunc(ctx, args)
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
		case 5:
			sig = &builtinCastJSONAsRealSig{realFunc}
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
		row    types.DatumRow
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
		// cast JSON as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeJSON), Index: 0},
			"3",
			[]types.Datum{jsonInt},
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
		stringFunc := newBaseBuiltinFunc(ctx, args)
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
			sig = &builtinCastJSONAsStringSig{stringFunc}
		case 6:
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
		row    types.DatumRow
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
		stringFunc := newBaseBuiltinFunc(ctx, args)
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
		row    types.DatumRow
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
		// cast JSON as Time.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeJSON), Index: 0},
			tm,
			[]types.Datum{jsonTime},
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
		timeFunc := newBaseBuiltinFunc(ctx, args)
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
			sig = &builtinCastJSONAsTimeSig{timeFunc}
		case 6:
			sig = &builtinCastTimeAsTimeSig{timeFunc}
		}
		res, isNull, err := sig.evalTime(t.row)
		c.Assert(isNull, Equals, false)
		c.Assert(err, IsNil)
		c.Assert(res.TimeZone, Equals, sc.TimeZone)
		c.Assert(res.String(), Equals, t.after.String())
	}

	castToTimeCases2 := []struct {
		before *Column
		after  types.Time
		row    types.DatumRow
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
		timeFunc := newBaseBuiltinFunc(ctx, args)
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
		c.Assert(res.TimeZone, Equals, sc.TimeZone)
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
		row    types.DatumRow
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
		// cast JSON as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeJSON), Index: 0},
			duration,
			[]types.Datum{jsonDuration},
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
		durationFunc := newBaseBuiltinFunc(ctx, args)
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
			sig = &builtinCastJSONAsDurationSig{durationFunc}
		case 6:
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
		row    types.DatumRow
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
		durationFunc := newBaseBuiltinFunc(ctx, args)
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
	row := types.DatumRow{types.NewDatum(nil)}
	bf := newBaseBuiltinFunc(ctx, args)
	bf.tp = types.NewFieldType(mysql.TypeVarString)
	sig = &builtinCastRealAsStringSig{bf}
	sRes, isNull, err := sig.evalString(row)
	c.Assert(sRes, Equals, "")
	c.Assert(isNull, Equals, true)
	c.Assert(err, IsNil)

	// test hybridType case.
	args = []Expression{&Constant{Value: types.NewDatum(types.Enum{Name: "a", Value: 0}), RetType: types.NewFieldType(mysql.TypeEnum)}}
	sig = &builtinCastStringAsIntSig{newBaseBuiltinFunc(ctx, args)}
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
		row       types.DatumRow
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
			&Constant{RetType: types.NewFieldType(mysql.TypeVarString), Value: types.NewBinaryLiteralDatum(types.NewBinaryLiteralFromUint(0x61, -1))},
			nil,
			97, 97, types.NewDecFromInt(0x61), "a",
		},
	}
	for _, t := range cases {
		// Test wrapping with CastAsInt.
		intExpr := WrapWithCastAsInt(ctx, t.expr)
		c.Assert(intExpr.GetType().EvalType(), Equals, types.ETInt)
		intRes, isNull, err := intExpr.EvalInt(t.row, sc)
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(intRes, Equals, t.intRes)

		// Test wrapping with CastAsReal.
		realExpr := WrapWithCastAsReal(ctx, t.expr)
		c.Assert(realExpr.GetType().EvalType(), Equals, types.ETReal)
		realRes, isNull, err := realExpr.EvalReal(t.row, sc)
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(realRes, Equals, t.realRes)

		// Test wrapping with CastAsDecimal.
		decExpr := WrapWithCastAsDecimal(ctx, t.expr)
		c.Assert(decExpr.GetType().EvalType(), Equals, types.ETDecimal)
		decRes, isNull, err := decExpr.EvalDecimal(t.row, sc)
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(decRes.Compare(t.decRes), Equals, 0)

		// Test wrapping with CastAsString.
		strExpr := WrapWithCastAsString(ctx, t.expr)
		c.Assert(strExpr.GetType().EvalType().IsStringKind(), IsTrue)
		strRes, isNull, err := strExpr.EvalString(t.row, sc)
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(strRes, Equals, t.stringRes)
	}

	unsignedIntExpr := &Column{RetType: &types.FieldType{Tp: mysql.TypeLonglong, Flag: mysql.UnsignedFlag, Flen: mysql.MaxIntWidth, Decimal: 0}, Index: 0}

	// test cast unsigned int as string.
	strExpr := WrapWithCastAsString(ctx, unsignedIntExpr)
	c.Assert(strExpr.GetType().EvalType().IsStringKind(), IsTrue)
	strRes, isNull, err := strExpr.EvalString(types.DatumRow{types.NewUintDatum(math.MaxUint64)}, sc)
	c.Assert(err, IsNil)
	c.Assert(strRes, Equals, strconv.FormatUint(math.MaxUint64, 10))
	c.Assert(isNull, Equals, false)

	strRes, isNull, err = strExpr.EvalString(types.DatumRow{types.NewUintDatum(1234)}, sc)
	c.Assert(err, IsNil)
	c.Assert(isNull, Equals, false)
	c.Assert(strRes, Equals, strconv.FormatUint(uint64(1234), 10))

	// test cast unsigned int as decimal.
	decExpr := WrapWithCastAsDecimal(ctx, unsignedIntExpr)
	c.Assert(decExpr.GetType().EvalType(), Equals, types.ETDecimal)
	decRes, isNull, err := decExpr.EvalDecimal(types.DatumRow{types.NewUintDatum(uint64(1234))}, sc)
	c.Assert(err, IsNil)
	c.Assert(isNull, Equals, false)
	c.Assert(decRes.Compare(types.NewDecFromUint(uint64(1234))), Equals, 0)

	// test cast unsigned int as Time.
	timeExpr := WrapWithCastAsTime(ctx, unsignedIntExpr, types.NewFieldType(mysql.TypeDatetime))
	c.Assert(timeExpr.GetType().Tp, Equals, mysql.TypeDatetime)
	timeRes, isNull, err := timeExpr.EvalTime(types.DatumRow{types.NewUintDatum(uint64(curTimeInt))}, sc)
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
		expr := WrapWithCastAsTime(s.ctx, t.expr, t.tp)
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
		expr := WrapWithCastAsDuration(s.ctx, t.expr)
		res, isNull, err := expr.EvalDuration(nil, s.ctx.GetSessionVars().StmtCtx)
		c.Assert(err, IsNil)
		c.Assert(isNull, Equals, false)
		c.Assert(res.Compare(duration), Equals, 0)
	}
}
