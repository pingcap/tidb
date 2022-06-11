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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestCastFunctions(t *testing.T) {
	ctx := createContext(t)

	sc := ctx.GetSessionVars().StmtCtx

	// Test `cast as char[(N)]` and `cast as binary[(N)]`.
	originIgnoreTruncate := sc.IgnoreTruncate
	originTruncateAsWarning := sc.TruncateAsWarning
	sc.IgnoreTruncate = false
	sc.TruncateAsWarning = true
	defer func() {
		sc.IgnoreTruncate = originIgnoreTruncate
		sc.TruncateAsWarning = originTruncateAsWarning
	}()

	tp := types.NewFieldType(mysql.TypeString)
	tp.SetFlen(5)

	// cast(str as char(N)), N < len([]rune(str)).
	// cast("你好world" as char(5))
	tp.SetCharset(charset.CharsetUTF8)
	f := BuildCastFunction(ctx, &Constant{Value: types.NewDatum("你好world"), RetType: tp}, tp)
	res, err := f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, "你好wor", res.GetString())

	// cast(str as char(N)), N > len([]rune(str)).
	// cast("a" as char(5))
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("a"), RetType: types.NewFieldType(mysql.TypeString)}, tp)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, 1, len(res.GetString()))
	require.Equal(t, "a", res.GetString())

	// cast(str as binary(N)), N < len(str).
	// cast("你好world" as binary(5))
	str := "你好world"

	tp.AddFlag(mysql.BinaryFlag)
	tp.SetCharset(charset.CharsetBin)
	tp.SetCollate(charset.CollationBin)
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum(str), RetType: types.NewFieldType(mysql.TypeString)}, tp)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, str[:5], res.GetString())

	// cast(str as binary(N)), N > len([]byte(str)).
	// cast("a" as binary(5))
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("a"), RetType: types.NewFieldType(mysql.TypeString)}, tp)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, 5, len(res.GetString()))
	require.Equal(t, string([]byte{'a', 0x00, 0x00, 0x00, 0x00}), res.GetString())

	// cast(str as binary(N)), N > len([]byte(str)).
	// cast("a" as binary(4294967295))
	tp.SetFlen(4294967295)
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("a"), RetType: types.NewFieldType(mysql.TypeString)}, tp)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.True(t, res.IsNull())
	warnings := sc.GetWarnings()
	lastWarn := warnings[len(warnings)-1]
	require.Truef(t, terror.ErrorEqual(errWarnAllowedPacketOverflowed, lastWarn.Err), "err %v", lastWarn.Err)

	origSc := sc
	oldInSelectStmt := sc.InSelectStmt
	sc.InSelectStmt = true
	defer func() {
		sc.InSelectStmt = oldInSelectStmt
	}()
	sc.OverflowAsWarning = true

	// cast('18446744073709551616' as unsigned);
	tp1 := types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.BinaryFlag).SetFlen(mysql.MaxIntWidth).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin).BuildP()
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("18446744073709551616"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.True(t, res.GetUint64() == math.MaxUint64)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	require.Truef(t, terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn.Err), "err %v", lastWarn.Err)

	originFlag := tp1.GetFlag()
	tp1.AddFlag(mysql.UnsignedFlag)
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("-1"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.True(t, res.GetUint64() == 18446744073709551615)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	require.Truef(t, terror.ErrorEqual(types.ErrCastNegIntAsUnsigned, lastWarn.Err), "err %v", lastWarn.Err)
	tp1.SetFlag(originFlag)

	previousWarnings := len(sc.GetWarnings())
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("-1"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.True(t, res.GetInt64() == -1)
	require.True(t, len(sc.GetWarnings()) == previousWarnings)

	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("-18446744073709551616"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	limit := math.MinInt64
	// 9223372036854775808
	require.True(t, res.GetUint64() == uint64(limit))

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	require.Truef(t, terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn.Err), "err %v", lastWarn.Err)

	// cast('125e342.83' as unsigned)
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("125e342.83"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.True(t, res.GetUint64() == 125)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	require.Truef(t, terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn.Err), "err %v", lastWarn.Err)

	// cast('1e9223372036854775807' as unsigned)
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("1e9223372036854775807"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.True(t, res.GetUint64() == 1)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	require.Truef(t, terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn.Err), "err %v", lastWarn.Err)

	// cast('18446744073709551616' as signed);
	mask := ^mysql.UnsignedFlag
	tp1.SetFlag(tp1.GetFlag() & mask)
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("18446744073709551616"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, int64(-1), res.GetInt64())

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	require.Truef(t, terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn.Err), "err %v", lastWarn.Err)

	// cast('18446744073709551614' as signed);
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("18446744073709551614"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, int64(-2), res.GetInt64())

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	require.Truef(t, terror.ErrorEqual(types.ErrCastAsSignedOverflow, lastWarn.Err), "err %v", lastWarn.Err)

	// cast('125e342.83' as signed)
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("125e342.83"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.True(t, res.GetInt64() == 125)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	require.Truef(t, terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn.Err), "err %v", lastWarn.Err)

	// cast('1e9223372036854775807' as signed)
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum("1e9223372036854775807"), RetType: types.NewFieldType(mysql.TypeString)}, tp1)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.True(t, res.GetInt64() == 1)

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	require.Truef(t, terror.ErrorEqual(types.ErrTruncatedWrongVal, lastWarn.Err), "err %v", lastWarn.Err)

	// create table t1(s1 time);
	// insert into t1 values('11:11:11');
	// select cast(s1 as decimal(7, 2)) from t1;
	ft := types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).SetFlag(mysql.BinaryFlag | mysql.UnsignedFlag).SetFlen(7).SetDecimal(2).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin).BuildP()
	f = BuildCastFunction(ctx, &Constant{Value: timeDatum, RetType: types.NewFieldType(mysql.TypeDatetime)}, ft)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	resDecimal := new(types.MyDecimal)
	err = resDecimal.FromString([]byte("99999.99"))
	require.NoError(t, err)
	require.Equal(t, 0, res.GetMysqlDecimal().Compare(resDecimal))

	warnings = sc.GetWarnings()
	lastWarn = warnings[len(warnings)-1]
	require.Truef(t, terror.ErrorEqual(types.ErrOverflow, lastWarn.Err), "err %v", lastWarn.Err)
	sc = origSc

	// create table tt(a bigint unsigned);
	// insert into tt values(18446744073709551615);
	// select cast(a as decimal(65, 0)) from tt;
	ft = types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).SetFlag(mysql.BinaryFlag).SetFlen(65).SetCharset(charset.CharsetBin).SetCollate(charset.CollationBin).BuildP()
	rt := types.NewFieldType(mysql.TypeLonglong)
	rt.SetFlag(mysql.BinaryFlag | mysql.UnsignedFlag)
	f = BuildCastFunction(ctx, &Constant{Value: types.NewUintDatum(18446744073709551615), RetType: rt}, ft)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	u, err := res.GetMysqlDecimal().ToUint()
	require.NoError(t, err)
	require.True(t, u == 18446744073709551615)

	// cast(bad_string as decimal)
	for _, s := range []string{"hello", ""} {
		f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum(s), RetType: types.NewFieldType(mysql.TypeNewDecimal)}, tp)
		res, err = f.Eval(chunk.Row{})
		require.NoError(t, err)
	}

	// cast(1234 as char(0))
	tp.SetFlen(0)
	tp.SetCharset(charset.CharsetUTF8)
	f = BuildCastFunction(ctx, &Constant{Value: types.NewDatum(1234), RetType: types.NewFieldType(mysql.TypeString)}, tp)
	res, err = f.Eval(chunk.Row{})
	require.NoError(t, err)
	require.Equal(t, 0, len(res.GetString()))
	require.Equal(t, "", res.GetString())
}

var (
	year, month, day     = time.Now().In(time.UTC).Date()
	curDateInt           = int64(year*10000 + int(month)*100 + day)
	curTimeInt           = curDateInt*1000000 + 125959
	curTimeWithFspReal   = float64(curTimeInt) + 0.555
	curTimeString        = fmt.Sprintf("%4d-%02d-%02d 12:59:59", year, int(month), day)
	curTimeWithFspString = fmt.Sprintf("%4d-%02d-%02d 12:59:59.555000", year, int(month), day)
	tm                   = types.NewTime(types.FromDate(year, int(month), day, 12, 59, 59, 0), mysql.TypeDatetime, types.DefaultFsp)
	tmWithFsp            = types.NewTime(types.FromDate(year, int(month), day, 12, 59, 59, 555000), mysql.TypeDatetime, types.MaxFsp)
	// timeDatum indicates datetime "curYear-curMonth-curDay 12:59:59".
	timeDatum = types.NewDatum(tm)
	// timeWithFspDatum indicates datetime "curYear-curMonth-curDay 12:59:59.555000".
	timeWithFspDatum = types.NewDatum(tmWithFsp)
	duration         = types.Duration{
		Duration: 12*time.Hour + 59*time.Minute + 59*time.Second,
		Fsp:      types.DefaultFsp}
	// durationDatum indicates duration "12:59:59".
	durationDatum   = types.NewDatum(duration)
	durationWithFsp = types.Duration{
		Duration: 12*time.Hour + 59*time.Minute + 59*time.Second + 555*time.Millisecond,
		Fsp:      3}
	// durationWithFspDatum indicates duration "12:59:59.555"
	durationWithFspDatum = types.NewDatum(durationWithFsp)
	dt                   = types.NewTime(types.FromDate(year, int(month), day, 0, 0, 0, 0), mysql.TypeDate, types.DefaultFsp)

	// jsonInt indicates json(3)
	jsonInt = types.NewDatum(json.CreateBinary(int64(3)))

	// jsonTime indicates "CURRENT_DAY 12:59:59"
	jsonTime = types.NewDatum(json.CreateBinary(tm.String()))

	// jsonDuration indicates
	jsonDuration = types.NewDatum(json.CreateBinary(duration.String()))
)

func TestCastFuncSig(t *testing.T) {
	ctx := createContext(t)

	sc := ctx.GetSessionVars().StmtCtx
	originIgnoreTruncate := sc.IgnoreTruncate
	originTZ := sc.TimeZone
	sc.IgnoreTruncate = true
	sc.TimeZone = time.UTC
	defer func() {
		sc.IgnoreTruncate = originIgnoreTruncate
		sc.TimeZone = originTZ
	}()
	var sig builtinFunc

	durationColumn := &Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0}
	durationColumn.RetType.SetDecimal(types.DefaultFsp)
	// Test cast as decimal.
	castToDecCases := []struct {
		before *Column
		after  *types.MyDecimal
		row    chunk.MutRow
	}{
		// cast int as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			types.NewDecFromInt(1),
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}),
		},
		// cast string as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			types.NewDecFromInt(1),
			chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum("1")}),
		},
		// cast real as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			types.NewDecFromInt(1),
			chunk.MutRowFromDatums([]types.Datum{types.NewFloat64Datum(1)}),
		},
		// cast Time as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			types.NewDecFromInt(curTimeInt),
			chunk.MutRowFromDatums([]types.Datum{timeDatum}),
		},
		// cast Duration as decimal.
		{
			durationColumn,
			types.NewDecFromInt(125959),
			chunk.MutRowFromDatums([]types.Datum{durationDatum}),
		},
	}
	for i, c := range castToDecCases {
		args := []Expression{c.before}
		b, err := newBaseBuiltinFunc(ctx, "", args, 0)
		require.NoError(t, err)
		decFunc := newBaseBuiltinCastFunc(b, false)
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
		res, isNull, err := sig.evalDecimal(c.row.ToRow())
		require.Equal(t, false, isNull)
		require.NoError(t, err)
		require.Equal(t, 0, res.Compare(c.after))
	}

	durationColumn.RetType.SetDecimal(1)
	castToDecCases2 := []struct {
		before  *Column
		flen    int
		decimal int
		after   *types.MyDecimal
		row     chunk.MutRow
	}{
		// cast int as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			7,
			3,
			types.NewDecFromStringForTest("1234.000"),
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1234)}),
		},
		// cast string as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			7,
			3,
			types.NewDecFromStringForTest("1234.000"),
			chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum("1234")}),
		},
		// cast real as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			8,
			4,
			types.NewDecFromStringForTest("1234.1230"),
			chunk.MutRowFromDatums([]types.Datum{types.NewFloat64Datum(1234.123)}),
		},
		// cast Time as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			15,
			1,
			types.NewDecFromStringForTest(strconv.FormatInt(curTimeInt, 10) + ".0"),
			chunk.MutRowFromDatums([]types.Datum{timeDatum}),
		},
		// cast Duration as decimal.
		{
			durationColumn,
			7,
			1,
			types.NewDecFromStringForTest("125959.0"),
			chunk.MutRowFromDatums([]types.Datum{durationDatum}),
		},
		// cast decimal as decimal.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			7,
			3,
			types.NewDecFromStringForTest("1234.000"),
			chunk.MutRowFromDatums([]types.Datum{types.NewDecimalDatum(types.NewDecFromStringForTest("1234"))}),
		},
	}

	for i, c := range castToDecCases2 {
		args := []Expression{c.before}
		tp := types.NewFieldType(mysql.TypeNewDecimal)
		tp.SetFlen(c.flen)
		tp.SetDecimal(c.decimal)
		b, err := newBaseBuiltinFunc(ctx, "", args, 0)
		require.NoError(t, err)
		decFunc := newBaseBuiltinCastFunc(b, false)
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
		res, isNull, err := sig.evalDecimal(c.row.ToRow())
		require.Equal(t, false, isNull)
		require.NoError(t, err)
		require.Equal(t, c.after.ToString(), res.ToString())
	}

	durationColumn.RetType.SetDecimal(0)
	// Test cast as int.
	castToIntCases := []struct {
		before *Column
		after  int64
		row    chunk.MutRow
	}{
		// cast string as int.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			1,
			chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum("1")}),
		},
		// cast decimal as int.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			1,
			chunk.MutRowFromDatums([]types.Datum{types.NewDecimalDatum(types.NewDecFromInt(1))}),
		},
		// cast real as int.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			2,
			chunk.MutRowFromDatums([]types.Datum{types.NewFloat64Datum(2.5)}),
		},
		// cast Time as int.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			curTimeInt,
			chunk.MutRowFromDatums([]types.Datum{timeDatum}),
		},
		// cast Duration as int.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			125959,
			chunk.MutRowFromDatums([]types.Datum{durationDatum}),
		},
		// cast JSON as int.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeJSON), Index: 0},
			3,
			chunk.MutRowFromDatums([]types.Datum{jsonInt}),
		},
	}
	for i, c := range castToIntCases {
		args := []Expression{c.before}
		b, err := newBaseBuiltinFunc(ctx, "", args, 0)
		require.NoError(t, err)
		intFunc := newBaseBuiltinCastFunc(b, false)
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
		res, isNull, err := sig.evalInt(c.row.ToRow())
		require.False(t, isNull)
		require.NoError(t, err)
		require.Equal(t, c.after, res)
	}

	// Test cast as real.
	castToRealCases := []struct {
		before *Column
		after  float64
		row    chunk.MutRow
	}{
		// cast string as real.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			1.1,
			chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum("1.1")}),
		},
		// cast decimal as real.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			1.1,
			chunk.MutRowFromDatums([]types.Datum{types.NewDecimalDatum(types.NewDecFromFloatForTest(1.1))}),
		},
		// cast int as real.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			1,
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}),
		},
		// cast Time as real.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			float64(curTimeInt),
			chunk.MutRowFromDatums([]types.Datum{timeDatum}),
		},
		// cast Duration as real.
		{
			durationColumn,
			125959,
			chunk.MutRowFromDatums([]types.Datum{durationDatum}),
		},
		// cast JSON as real.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeJSON), Index: 0},
			3.0,
			chunk.MutRowFromDatums([]types.Datum{jsonInt}),
		},
	}
	for i, c := range castToRealCases {
		args := []Expression{c.before}
		b, err := newBaseBuiltinFunc(ctx, "", args, 0)
		require.NoError(t, err)
		realFunc := newBaseBuiltinCastFunc(b, false)
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
		res, isNull, err := sig.evalReal(c.row.ToRow())
		require.False(t, isNull)
		require.NoError(t, err)
		require.Equal(t, c.after, res)
	}

	// Test cast as string.
	castToStringCases := []struct {
		before *Column
		after  string
		row    chunk.MutRow
	}{
		// cast real as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			"1",
			chunk.MutRowFromDatums([]types.Datum{types.NewFloat64Datum(1)}),
		},
		// cast decimal as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			"1",
			chunk.MutRowFromDatums([]types.Datum{types.NewDecimalDatum(types.NewDecFromInt(1))}),
		},
		// cast int as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			"1",
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1)}),
		},
		// cast time as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			curTimeString,
			chunk.MutRowFromDatums([]types.Datum{timeDatum}),
		},
		// cast duration as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			"12:59:59",
			chunk.MutRowFromDatums([]types.Datum{durationDatum}),
		},
		// cast JSON as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeJSON), Index: 0},
			"3",
			chunk.MutRowFromDatums([]types.Datum{jsonInt}),
		},
		// cast string as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			"1234",
			chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum("1234")}),
		},
	}
	for i, c := range castToStringCases {
		tp := types.NewFieldType(mysql.TypeVarString)
		tp.SetCharset(charset.CharsetBin)
		args := []Expression{c.before}
		stringFunc, err := newBaseBuiltinFunc(ctx, "", args, 0)
		require.NoError(t, err)
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
		res, isNull, err := sig.evalString(c.row.ToRow())
		require.False(t, isNull)
		require.NoError(t, err)
		require.Equal(t, c.after, res)
	}

	// Test cast as string.
	castToStringCases2 := []struct {
		before *Column
		after  string
		flen   int
		row    chunk.MutRow
	}{
		// cast real as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			"123",
			3,
			chunk.MutRowFromDatums([]types.Datum{types.NewFloat64Datum(1234.123)}),
		},
		// cast decimal as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			"123",
			3,
			chunk.MutRowFromDatums([]types.Datum{types.NewDecimalDatum(types.NewDecFromStringForTest("1234.123"))}),
		},
		// cast int as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			"123",
			3,
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(1234)}),
		},
		// cast time as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			curTimeString[:3],
			3,
			chunk.MutRowFromDatums([]types.Datum{timeDatum}),
		},
		// cast duration as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			"12:",
			3,
			chunk.MutRowFromDatums([]types.Datum{durationDatum}),
		},
		// cast string as string.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			"你好w",
			3,
			chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum("你好world")}),
		},
	}
	for i, c := range castToStringCases2 {
		args := []Expression{c.before}
		tp := types.NewFieldType(mysql.TypeVarString)
		tp.SetFlen(c.flen)
		tp.SetCharset(charset.CharsetBin)
		stringFunc, err := newBaseBuiltinFunc(ctx, "", args, 0)
		require.NoError(t, err)
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
			stringFunc.tp.SetCharset(charset.CharsetUTF8)
			sig = &builtinCastStringAsStringSig{stringFunc}
		}
		res, isNull, err := sig.evalString(c.row.ToRow())
		require.False(t, isNull)
		require.NoError(t, err)
		require.Equal(t, c.after, res)
	}

	castToTimeCases := []struct {
		before *Column
		after  types.Time
		row    chunk.MutRow
	}{
		// cast real as Time.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			tm,
			chunk.MutRowFromDatums([]types.Datum{types.NewFloat64Datum(float64(curTimeInt))}),
		},
		// cast decimal as Time.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			tm,
			chunk.MutRowFromDatums([]types.Datum{types.NewDecimalDatum(types.NewDecFromInt(curTimeInt))}),
		},
		// cast int as Time.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			tm,
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(curTimeInt)}),
		},
		// cast string as Time.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			tm,
			chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum(curTimeString)}),
		},
		// cast Duration as Time.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			tm,
			chunk.MutRowFromDatums([]types.Datum{durationDatum}),
		},
		// cast JSON as Time.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeJSON), Index: 0},
			tm,
			chunk.MutRowFromDatums([]types.Datum{jsonTime}),
		},
		// cast Time as Time.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			tm,
			chunk.MutRowFromDatums([]types.Datum{timeDatum}),
		},
	}
	for i, c := range castToTimeCases {
		args := []Expression{c.before}
		tp := types.NewFieldType(mysql.TypeDatetime)
		tp.SetDecimal(types.DefaultFsp)
		timeFunc, err := newBaseBuiltinFunc(ctx, "", args, 0)
		require.NoError(t, err)
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
		res, isNull, err := sig.evalTime(c.row.ToRow())
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, c.after.String(), res.String())
	}

	castToTimeCases2 := []struct {
		before *Column
		after  types.Time
		fsp    int
		tp     byte
		row    chunk.MutRow
	}{
		// cast real as Time(0).
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			dt,
			types.DefaultFsp,
			mysql.TypeDate,
			chunk.MutRowFromDatums([]types.Datum{types.NewFloat64Datum(float64(curTimeInt))}),
		},
		// cast decimal as Date.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			dt,
			types.DefaultFsp,
			mysql.TypeDate,
			chunk.MutRowFromDatums([]types.Datum{types.NewDecimalDatum(types.NewDecFromInt(curTimeInt))}),
		},
		// cast int as Datetime(6).
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			tm,
			types.MaxFsp,
			mysql.TypeDatetime,
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(curTimeInt)}),
		},
		// cast string as Datetime(6).
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			tm,
			types.MaxFsp,
			mysql.TypeDatetime,
			chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum(curTimeString)}),
		},
		// cast Duration as Date.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			dt,
			types.DefaultFsp,
			mysql.TypeDate,
			chunk.MutRowFromDatums([]types.Datum{durationDatum}),
		},
		// cast Time as Date.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			dt,
			types.DefaultFsp,
			mysql.TypeDate,
			chunk.MutRowFromDatums([]types.Datum{timeDatum}),
		},
	}
	for i, c := range castToTimeCases2 {
		args := []Expression{c.before}
		tp := types.NewFieldType(c.tp)
		tp.SetDecimal(c.fsp)
		timeFunc, err := newBaseBuiltinFunc(ctx, "", args, 0)
		require.NoError(t, err)
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
		res, isNull, err := sig.evalTime(c.row.ToRow())
		require.Equal(t, false, isNull)
		require.NoError(t, err)
		resAfter := c.after.String()
		if c.fsp > 0 {
			resAfter += "."
			for i := 0; i < c.fsp; i++ {
				resAfter += "0"
			}
		}
		require.Equal(t, resAfter, res.String())
	}

	castToDurationCases := []struct {
		before *Column
		after  types.Duration
		row    chunk.MutRow
	}{
		// cast real as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			duration,
			chunk.MutRowFromDatums([]types.Datum{types.NewFloat64Datum(125959)}),
		},
		// cast decimal as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			duration,
			chunk.MutRowFromDatums([]types.Datum{types.NewDecimalDatum(types.NewDecFromInt(125959))}),
		},
		// cast int as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			duration,
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(125959)}),
		},
		// cast string as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			duration,
			chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum("12:59:59")}),
		},
		// cast Time as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			duration,
			chunk.MutRowFromDatums([]types.Datum{timeDatum}),
		},
		// cast JSON as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeJSON), Index: 0},
			duration,
			chunk.MutRowFromDatums([]types.Datum{jsonDuration}),
		},
		// cast Duration as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			duration,
			chunk.MutRowFromDatums([]types.Datum{durationDatum}),
		},
	}
	for i, c := range castToDurationCases {
		args := []Expression{c.before}
		tp := types.NewFieldType(mysql.TypeDuration)
		tp.SetDecimal(types.DefaultFsp)
		durationFunc, err := newBaseBuiltinFunc(ctx, "", args, 0)
		require.NoError(t, err)
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
		res, isNull, err := sig.evalDuration(c.row.ToRow())
		require.False(t, isNull)
		require.NoError(t, err)
		require.Equal(t, c.after.String(), res.String())
	}

	castToDurationCases2 := []struct {
		before *Column
		after  types.Duration
		row    chunk.MutRow
		fsp    int
	}{
		// cast real as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			duration,
			chunk.MutRowFromDatums([]types.Datum{types.NewFloat64Datum(125959)}),
			1,
		},
		// cast decimal as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			duration,
			chunk.MutRowFromDatums([]types.Datum{types.NewDecimalDatum(types.NewDecFromInt(125959))}),
			2,
		},
		// cast int as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLonglong), Index: 0},
			duration,
			chunk.MutRowFromDatums([]types.Datum{types.NewIntDatum(125959)}),
			3,
		},
		// cast string as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0},
			duration,
			chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum("12:59:59")}),
			4,
		},
		// cast Time as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			duration,
			chunk.MutRowFromDatums([]types.Datum{timeDatum}),
			5,
		},
		// cast Duration as Duration.
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0},
			duration,
			chunk.MutRowFromDatums([]types.Datum{durationDatum}),
			6,
		},
	}
	for i, c := range castToDurationCases2 {
		args := []Expression{c.before}
		tp := types.NewFieldType(mysql.TypeDuration)
		tp.SetDecimal(c.fsp)
		durationFunc, err := newBaseBuiltinFunc(ctx, "", args, 0)
		require.NoError(t, err)
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
		res, isNull, err := sig.evalDuration(c.row.ToRow())
		require.False(t, isNull)
		require.NoError(t, err)
		resAfter := c.after.String()
		if c.fsp > 0 {
			resAfter += "."
			for j := 0; j < c.fsp; j++ {
				resAfter += "0"
			}
		}
		require.Equal(t, resAfter, res.String())
	}

	// null case
	args := []Expression{&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0}}
	row := chunk.MutRowFromDatums([]types.Datum{types.NewDatum(nil)})
	bf, err := newBaseBuiltinFunc(ctx, "", args, 0)
	require.NoError(t, err)
	bf.tp = types.NewFieldType(mysql.TypeVarString)
	sig = &builtinCastRealAsStringSig{bf}
	sRes, isNull, err := sig.evalString(row.ToRow())
	require.Equal(t, "", sRes)
	require.Equal(t, true, isNull)
	require.NoError(t, err)

	// test hybridType case.
	args = []Expression{&Constant{Value: types.NewDatum(types.Enum{Name: "a", Value: 0}), RetType: types.NewFieldType(mysql.TypeEnum)}}
	b, err := newBaseBuiltinFunc(ctx, "", args, 0)
	require.NoError(t, err)
	sig = &builtinCastStringAsIntSig{newBaseBuiltinCastFunc(b, false)}
	iRes, isNull, err := sig.evalInt(chunk.Row{})
	require.Equal(t, false, isNull)
	require.NoError(t, err)
	require.Equal(t, int64(0), iRes)
}

func TestCastJSONAsDecimalSig(t *testing.T) {
	ctx := createContext(t)
	sc := ctx.GetSessionVars().StmtCtx
	originIgnoreTruncate := sc.IgnoreTruncate
	sc.IgnoreTruncate = true
	defer func() {
		sc.IgnoreTruncate = originIgnoreTruncate
	}()

	col := &Column{RetType: types.NewFieldType(mysql.TypeJSON), Index: 0}
	b, err := newBaseBuiltinFunc(ctx, "", []Expression{col}, 0)
	require.NoError(t, err)
	decFunc := newBaseBuiltinCastFunc(b, false)
	decFunc.tp = types.NewFieldType(mysql.TypeNewDecimal)
	decFunc.tp.SetFlen(60)
	decFunc.tp.SetDecimal(2)
	sig := &builtinCastJSONAsDecimalSig{decFunc}

	var tests = []struct {
		In  string
		Out *types.MyDecimal
	}{
		{`{}`, types.NewDecFromStringForTest("0")},
		{`[]`, types.NewDecFromStringForTest("0")},
		{`3`, types.NewDecFromStringForTest("3")},
		{`-3`, types.NewDecFromStringForTest("-3")},
		{`4.5`, types.NewDecFromStringForTest("4.5")},
		{`"1234"`, types.NewDecFromStringForTest("1234")},
		// test truncate
		{`"1234.1234"`, types.NewDecFromStringForTest("1234.12")},
		{`"1234.4567"`, types.NewDecFromStringForTest("1234.46")},
		// test big decimal
		{`"1234567890123456789012345678901234567890123456789012345"`, types.NewDecFromStringForTest("1234567890123456789012345678901234567890123456789012345")},
	}
	for _, tt := range tests {
		j, err := json.ParseBinaryFromString(tt.In)
		require.NoError(t, err)
		row := chunk.MutRowFromDatums([]types.Datum{types.NewDatum(j)})
		res, isNull, err := sig.evalDecimal(row.ToRow())
		require.Equal(t, false, isNull)
		require.NoError(t, err)
		require.Equal(t, 0, res.Compare(tt.Out))
	}
}

// TestWrapWithCastAsTypesClasses tests WrapWithCastAsInt/Real/String/decimal.
func TestWrapWithCastAsTypesClasses(t *testing.T) {
	ctx := createContext(t)

	durationColumn0 := &Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0}
	durationColumn0.RetType.SetDecimal(types.DefaultFsp)
	durationColumn3 := &Column{RetType: types.NewFieldType(mysql.TypeDuration), Index: 0}
	durationColumn3.RetType.SetDecimal(3)
	cases := []struct {
		expr      Expression
		row       chunk.MutRow
		intRes    int64
		realRes   float64
		decRes    *types.MyDecimal
		stringRes string
	}{
		{
			&Column{RetType: types.NewFieldType(mysql.TypeLong), Index: 0},
			chunk.MutRowFromDatums([]types.Datum{types.NewDatum(123)}),
			123, 123, types.NewDecFromInt(123), "123",
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			chunk.MutRowFromDatums([]types.Datum{types.NewDatum(123.555)}),
			124, 123.555, types.NewDecFromFloatForTest(123.555), "123.555",
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDouble), Index: 0},
			chunk.MutRowFromDatums([]types.Datum{types.NewDatum(123.123)}),
			123, 123.123, types.NewDecFromFloatForTest(123.123), "123.123",
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			chunk.MutRowFromDatums([]types.Datum{types.NewDecimalDatum(types.NewDecFromStringForTest("123.123"))}),
			123, 123.123, types.NewDecFromFloatForTest(123.123), "123.123",
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeNewDecimal), Index: 0},
			chunk.MutRowFromDatums([]types.Datum{types.NewDecimalDatum(types.NewDecFromStringForTest("123.555"))}),
			124, 123.555, types.NewDecFromFloatForTest(123.555), "123.555",
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeVarString), Index: 0},
			chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum("123.123")}),
			123, 123.123, types.NewDecFromStringForTest("123.123"), "123.123",
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			chunk.MutRowFromDatums([]types.Datum{timeDatum}),
			curTimeInt, float64(curTimeInt), types.NewDecFromInt(curTimeInt), curTimeString,
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeDatetime), Index: 0},
			chunk.MutRowFromDatums([]types.Datum{timeWithFspDatum}),
			curDateInt*1000000 + 130000, curTimeWithFspReal, types.NewDecFromFloatForTest(curTimeWithFspReal), curTimeWithFspString,
		},
		{
			durationColumn0,
			chunk.MutRowFromDatums([]types.Datum{durationDatum}),
			125959, 125959, types.NewDecFromFloatForTest(125959), "12:59:59",
		},
		{
			durationColumn3,
			chunk.MutRowFromDatums([]types.Datum{durationWithFspDatum}),
			130000, 125959.555, types.NewDecFromFloatForTest(125959.555), "12:59:59.555",
		},
		{
			&Column{RetType: types.NewFieldType(mysql.TypeEnum), Index: 0},
			chunk.MutRowFromDatums([]types.Datum{types.NewDatum(types.Enum{Name: "a", Value: 123})}),
			123, 123, types.NewDecFromStringForTest("123"), "a",
		},
		{
			&Constant{RetType: types.NewFieldType(mysql.TypeVarString), Value: types.NewBinaryLiteralDatum(types.NewBinaryLiteralFromUint(0x61, -1))},
			chunk.MutRowFromDatums([]types.Datum{types.NewDatum(nil)}),
			97, 97, types.NewDecFromInt(0x61), "a",
		},
	}
	for i, c := range cases {
		// Test wrapping with CastAsInt.
		intExpr := WrapWithCastAsInt(ctx, c.expr)
		require.Equal(t, types.ETInt, intExpr.GetType().EvalType())
		intRes, isNull, err := intExpr.EvalInt(ctx, c.row.ToRow())
		require.NoErrorf(t, err, "cast[%v]: %#v", i, t)
		require.Equal(t, false, isNull)
		require.Equal(t, c.intRes, intRes)

		// Test wrapping with CastAsReal.
		realExpr := WrapWithCastAsReal(ctx, c.expr)
		require.Equal(t, types.ETReal, realExpr.GetType().EvalType())
		realRes, isNull, err := realExpr.EvalReal(ctx, c.row.ToRow())
		require.NoError(t, err)
		require.Equal(t, false, isNull)
		require.Equalf(t, c.realRes, realRes, "cast[%v]: %#v", i, t)

		// Test wrapping with CastAsDecimal.
		decExpr := WrapWithCastAsDecimal(ctx, c.expr)
		require.Equal(t, types.ETDecimal, decExpr.GetType().EvalType())
		decRes, isNull, err := decExpr.EvalDecimal(ctx, c.row.ToRow())
		require.NoError(t, err, "case[%v]: %#v\n", i, t)
		require.Equal(t, false, isNull)
		require.Equalf(t, 0, decRes.Compare(c.decRes), "case[%v]: %#v\n", i, t)

		// Test wrapping with CastAsString.
		strExpr := WrapWithCastAsString(ctx, c.expr)
		require.True(t, strExpr.GetType().EvalType().IsStringKind())
		strRes, isNull, err := strExpr.EvalString(ctx, c.row.ToRow())
		require.NoError(t, err)
		require.Equal(t, false, isNull)
		require.Equal(t, c.stringRes, strRes)
	}

	unsignedIntExpr := &Column{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).SetFlen(mysql.MaxIntWidth).BuildP(), Index: 0}

	// test cast unsigned int as string.
	strExpr := WrapWithCastAsString(ctx, unsignedIntExpr)
	require.True(t, strExpr.GetType().EvalType().IsStringKind())
	strRes, isNull, err := strExpr.EvalString(ctx, chunk.MutRowFromDatums([]types.Datum{types.NewUintDatum(math.MaxUint64)}).ToRow())
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(math.MaxUint64, 10), strRes)
	require.Equal(t, false, isNull)

	strRes, isNull, err = strExpr.EvalString(ctx, chunk.MutRowFromDatums([]types.Datum{types.NewUintDatum(1234)}).ToRow())
	require.NoError(t, err)
	require.Equal(t, false, isNull)
	require.Equal(t, strconv.FormatUint(uint64(1234), 10), strRes)

	// test cast unsigned int as decimal.
	decExpr := WrapWithCastAsDecimal(ctx, unsignedIntExpr)
	require.Equal(t, types.ETDecimal, decExpr.GetType().EvalType())
	decRes, isNull, err := decExpr.EvalDecimal(ctx, chunk.MutRowFromDatums([]types.Datum{types.NewUintDatum(uint64(1234))}).ToRow())
	require.NoError(t, err)
	require.Equal(t, false, isNull)
	require.Equal(t, 0, decRes.Compare(types.NewDecFromUint(uint64(1234))))

	// test cast unsigned int as Time.
	timeExpr := WrapWithCastAsTime(ctx, unsignedIntExpr, types.NewFieldType(mysql.TypeDatetime))
	require.Equal(t, mysql.TypeDatetime, timeExpr.GetType().GetType())
	timeRes, isNull, err := timeExpr.EvalTime(ctx, chunk.MutRowFromDatums([]types.Datum{types.NewUintDatum(uint64(curTimeInt))}).ToRow())
	require.NoError(t, err)
	require.Equal(t, false, isNull)
	require.Equal(t, 0, timeRes.Compare(tm))
}

func TestWrapWithCastAsTime(t *testing.T) {
	ctx := createContext(t)
	sc := ctx.GetSessionVars().StmtCtx
	save := sc.TimeZone
	sc.TimeZone = time.UTC
	defer func() {
		sc.TimeZone = save
	}()
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
	for d, c := range cases {
		expr := WrapWithCastAsTime(ctx, c.expr, c.tp)
		res, isNull, err := expr.EvalTime(ctx, chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, false, isNull)
		require.Equal(t, c.tp.GetType(), res.Type())
		require.Zerof(t, res.Compare(c.res), "case %d res = %s, expect = %s", d, res, c.res)
	}
}

func TestWrapWithCastAsDuration(t *testing.T) {
	ctx := createContext(t)

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
	for _, c := range cases {
		expr := WrapWithCastAsDuration(ctx, c.expr)
		res, isNull, err := expr.EvalDuration(ctx, chunk.Row{})
		require.NoError(t, err)
		require.Equal(t, false, isNull)
		require.Zero(t, res.Compare(duration))
	}
}

func TestWrapWithCastAsString(t *testing.T) {
	ctx := createContext(t)

	cases := []struct {
		expr Expression
		err  bool
		ret  string
	}{
		{
			&Constant{RetType: types.NewFieldTypeWithCollation(mysql.TypeVarString, charset.CollationBin, 1), Value: types.NewBinaryLiteralDatum([]byte{0x91})},
			true,
			"",
		},
		{
			&Constant{RetType: types.NewFieldTypeWithCollation(mysql.TypeVarString, charset.CollationBin, 1), Value: types.NewBinaryLiteralDatum([]byte{0x61})},
			false,
			"a",
		},
	}
	for _, c := range cases {
		expr := BuildCastFunction(ctx, c.expr, types.NewFieldType(mysql.TypeVarString))
		res, _, err := expr.EvalString(ctx, chunk.Row{})
		if c.err {
			require.Error(t, err)
		} else {
			require.Equal(t, c.ret, res)
		}
	}

	expr := BuildCastFunction(ctx, &Constant{RetType: types.NewFieldType(mysql.TypeEnum)}, types.NewFieldType(mysql.TypeVarString))
	require.NotContains(t, expr.String(), "to_binary")
}

func TestWrapWithCastAsJSON(t *testing.T) {
	ctx := createContext(t)

	input := &Column{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeJSON).BuildP()}
	expr := WrapWithCastAsJSON(ctx, input)

	output, ok := expr.(*Column)
	require.True(t, ok)
	require.Equal(t, input, output)
}

func TestCastIntAsIntVec(t *testing.T) {
	cast, input, result := genCastIntAsInt()
	require.NoError(t, cast.vecEvalInt(input, result))
	i64s := result.Int64s()
	it := chunk.NewIterator4Chunk(input)
	i := 0
	for row := it.Begin(); row != it.End(); row = it.Next() {
		v, _, err := cast.evalInt(row)
		require.NoError(t, err)
		require.Equal(t, i64s[i], v)
		i++
	}

	cast.inUnion = true
	cast.getRetTp().AddFlag(mysql.UnsignedFlag)
	require.NoError(t, cast.vecEvalInt(input, result))
	i64s = result.Int64s()
	it = chunk.NewIterator4Chunk(input)
	i = 0
	for row := it.Begin(); row != it.End(); row = it.Next() {
		v, _, err := cast.evalInt(row)
		require.NoError(t, err)
		require.Equal(t, i64s[i], v)
		i++
	}
}

// for issue https://github.com/pingcap/tidb/issues/16825
func TestCastStringAsDecimalSigWithUnsignedFlagInUnion(t *testing.T) {
	col := &Column{RetType: types.NewFieldType(mysql.TypeString), Index: 0}
	b, err := newBaseBuiltinFunc(mock.NewContext(), "", []Expression{col}, 0)
	require.NoError(t, err)
	// set `inUnion` to `true`
	decFunc := newBaseBuiltinCastFunc(b, true)
	decFunc.tp = types.NewFieldType(mysql.TypeNewDecimal)
	// set the `UnsignedFlag` bit
	decFunc.tp.AddFlag(mysql.UnsignedFlag)
	cast := &builtinCastStringAsDecimalSig{decFunc}

	cases := []struct {
		row chunk.MutRow
		res *types.MyDecimal
	}{
		// if `inUnion` is `true`, the result of cast a positive decimal string to unsigned decimal should be normal
		{
			chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum("1")}),
			types.NewDecFromInt(1),
		},
		// if `inUnion` is `true`, the result of cast a negative decimal string to unsigned decimal should be 0
		{
			chunk.MutRowFromDatums([]types.Datum{types.NewStringDatum("-1")}),
			types.NewDecFromInt(0),
		},
	}

	for _, c := range cases {
		res, isNull, err := cast.evalDecimal(c.row.ToRow())
		require.Equal(t, false, isNull)
		require.NoError(t, err)
		require.Equal(t, 0, res.Compare(c.res))
	}
}

func TestCastConstAsDecimalFieldType(t *testing.T) {
	type testCase struct {
		input         *Constant
		resultFlen    int
		resultDecimal int
	}
	allTestCase := []testCase{
		// test int
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP(), Value: types.NewIntDatum(0)}, 1, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP(), Value: types.NewIntDatum(1)}, 1, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP(), Value: types.NewIntDatum(-1)}, 1, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP(), Value: types.NewIntDatum(11111)}, 5, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP(), Value: types.NewIntDatum(-11111)}, 5, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP(), Value: types.NewIntDatum(1111111111)}, 10, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP(), Value: types.NewIntDatum(-1111111111)}, 10, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP(), Value: types.NewIntDatum(111111111111111)}, 15, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP(), Value: types.NewIntDatum(-111111111111111)}, 15, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP(), Value: types.NewIntDatum(9223372036854775807)}, 19, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).BuildP(), Value: types.NewIntDatum(-9223372036854775808)}, 19, 0},
		// test uint
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(), Value: types.NewUintDatum(0)}, 1, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(), Value: types.NewUintDatum(1)}, 1, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(), Value: types.NewUintDatum(11111)}, 5, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(), Value: types.NewUintDatum(1111111111)}, 10, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(), Value: types.NewUintDatum(111111111111111)}, 15, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(), Value: types.NewUintDatum(9223372036854775807)}, 19, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(), Value: types.NewUintDatum(18446744073709551615)}, 20, 0},
		// test decimal, use origin fieldType
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).SetFlen(10).SetDecimal(5).BuildP(), Value: types.NewDecimalDatum(types.NewDecFromStringForTest("12345"))}, 10, 5},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).SetFlen(2).SetDecimal(1).BuildP(), Value: types.NewDecimalDatum(types.NewDecFromStringForTest("1"))}, 2, 1},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeNewDecimal).SetFlen(30).SetDecimal(0).BuildP(), Value: types.NewDecimalDatum(types.NewDecFromStringForTest("12345"))}, 30, 0},
		// test real
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDouble).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewFloat64Datum(1.234)}, 4, 3},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDouble).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewFloat64Datum(1.23456789)}, 9, 8},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDouble).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewFloat64Datum(-1234567890.123456789)}, 17, 7},           // float precision lost
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDouble).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewFloat64Datum(-1234567890.1234567890123456789)}, 17, 7}, // float precision lost
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDouble).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewFloat64Datum(1e10)}, 11, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDouble).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewFloat64Datum(1e20)}, 21, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDouble).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewFloat64Datum(1e40)}, 41, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDouble).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewFloat64Datum(1e60)}, 61, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDouble).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewFloat64Datum(1e80)}, 65, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDouble).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewFloat64Datum(1e-10)}, 10, 10},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDouble).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewFloat64Datum(1e-20)}, 20, 20},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDouble).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewFloat64Datum(1e-40)}, 40, 30},
		// test string
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewStringDatum("123.456")}, 6, 3},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewStringDatum("123.4560")}, 7, 4},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewStringDatum("123.456000000")}, 12, 9},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewStringDatum("123abcde")}, 3, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewStringDatum("1e80")}, 65, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewStringDatum("1e-40")}, 40, 30},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewStringDatum("-1234567890.123456789")}, 19, 9},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlen(types.UnspecifiedLength).SetDecimal(types.UnspecifiedLength).BuildP(), Value: types.NewStringDatum("-1234567890.1234567890123456789")}, 29, 19},
		// test time
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDuration).SetFlen(types.UnspecifiedLength).SetDecimal(3).BuildP(), Value: types.NewDurationDatum(types.NewDuration(10, 10, 10, 110, 3))}, 9, 3},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDuration).SetFlen(types.UnspecifiedLength).SetDecimal(6).BuildP(), Value: types.NewDurationDatum(types.NewDuration(10, 10, 10, 110, 6))}, 12, 6},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDuration).SetFlen(types.UnspecifiedLength).SetDecimal(0).BuildP(), Value: types.NewDurationDatum(types.NewDuration(10, 10, 10, 110, 0))}, 6, 0},
		// test timestamp
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeTimestamp).SetFlen(types.UnspecifiedLength).SetDecimal(0).BuildP(), Value: types.NewTimeDatum(types.NewTime(types.FromDate(2020, 10, 10, 10, 10, 10, 110), mysql.TypeTimestamp, 0))}, 14, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeTimestamp).SetFlen(types.UnspecifiedLength).SetDecimal(3).BuildP(), Value: types.NewTimeDatum(types.NewTime(types.FromDate(2020, 10, 10, 10, 10, 10, 110), mysql.TypeTimestamp, 0))}, 17, 3},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeTimestamp).SetFlen(types.UnspecifiedLength).SetDecimal(6).BuildP(), Value: types.NewTimeDatum(types.NewTime(types.FromDate(2020, 10, 10, 10, 10, 10, 110), mysql.TypeTimestamp, 0))}, 20, 6},
		// test datetime
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDatetime).SetFlen(types.UnspecifiedLength).SetDecimal(0).BuildP(), Value: types.NewTimeDatum(types.NewTime(types.FromDate(2020, 10, 10, 10, 10, 10, 110), mysql.TypeDatetime, 0))}, 14, 0},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDatetime).SetFlen(types.UnspecifiedLength).SetDecimal(3).BuildP(), Value: types.NewTimeDatum(types.NewTime(types.FromDate(2020, 10, 10, 10, 10, 10, 110), mysql.TypeDatetime, 0))}, 17, 3},
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDatetime).SetFlen(types.UnspecifiedLength).SetDecimal(6).BuildP(), Value: types.NewTimeDatum(types.NewTime(types.FromDate(2020, 10, 10, 10, 10, 10, 110), mysql.TypeDatetime, 0))}, 20, 6},
		// test date
		{&Constant{RetType: types.NewFieldTypeBuilder().SetType(mysql.TypeDate).SetFlen(types.UnspecifiedLength).SetDecimal(0).BuildP(), Value: types.NewTimeDatum(types.NewTime(types.FromDate(2020, 10, 10, 10, 10, 10, 110), mysql.TypeDate, 0))}, 8, 0},
	}
	ctx := createContext(t)
	for _, tc := range allTestCase {
		expr := WrapWithCastAsDecimal(ctx, tc.input)
		require.Equal(t, tc.resultFlen, expr.GetType().GetFlen())
		require.Equal(t, tc.resultDecimal, expr.GetType().GetDecimal())
	}
}
