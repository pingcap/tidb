// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

func convertToTime(sc *variable.StatementContext, arg types.Datum, tp byte) (d types.Datum, err error) {
	f := types.NewFieldType(tp)
	f.Decimal = types.MaxFsp

	d, err = arg.ConvertTo(sc, f)
	if err != nil {
		d.SetNull()
		return d, errors.Trace(err)
	}

	if d.IsNull() {
		return d, nil
	}

	if d.Kind() != types.KindMysqlTime {
		d.SetNull()
		return d, errors.Errorf("need time type, but got %T", d.GetValue())
	}
	return d, nil
}

func convertToDuration(sc *variable.StatementContext, arg types.Datum, fsp int) (d types.Datum, err error) {
	f := types.NewFieldType(mysql.TypeDuration)
	f.Decimal = fsp

	d, err = arg.ConvertTo(sc, f)
	if err != nil {
		d.SetNull()
		return d, errors.Trace(err)
	}

	if d.IsNull() {
		return d, nil
	}

	if d.Kind() != types.KindMysqlDuration {
		d.SetNull()
		return d, errors.Errorf("need duration type, but got %T", d.GetValue())
	}
	return d, nil
}

func builtinDate(args []types.Datum, ctx context.Context) (types.Datum, error) {
	return convertToTime(ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
}

func convertDatumToTime(sc *variable.StatementContext, d types.Datum) (t types.Time, err error) {
	if d.Kind() != types.KindMysqlTime {
		d, err = convertToTime(sc, d, mysql.TypeDatetime)
		if err != nil {
			return t, errors.Trace(err)
		}
	}
	return d.GetMysqlTime(), nil
}

func builtinTimeDiff(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	t1, err := convertDatumToTime(sc, args[0])
	if err != nil {
		return d, errors.Trace(err)
	}
	t2, err := convertDatumToTime(sc, args[1])
	if err != nil {
		return d, errors.Trace(err)
	}

	t := t1.Sub(&t2)
	d.SetMysqlDuration(t)
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func builtinDateFormat(args []types.Datum, ctx context.Context) (types.Datum, error) {
	var d types.Datum
	date, err := convertToTime(ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDatetime)
	if err != nil {
		return d, errors.Trace(err)
	}

	t := date.GetMysqlTime()
	str, err := t.DateFormat(args[1].GetString())
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetString(str)
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_day
// Day is a synonym for DayOfMonth.
func builtinDay(args []types.Datum, ctx context.Context) (types.Datum, error) {
	return builtinDayOfMonth(args, ctx)
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_hour
func builtinHour(args []types.Datum, ctx context.Context) (types.Datum, error) {
	d, err := convertToDuration(ctx.GetSessionVars().StmtCtx, args[0], types.MaxFsp)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	// No need to check type here.
	h := int64(d.GetMysqlDuration().Hour())
	d.SetInt64(h)
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_minute
func builtinMinute(args []types.Datum, ctx context.Context) (types.Datum, error) {
	d, err := convertToDuration(ctx.GetSessionVars().StmtCtx, args[0], types.MaxFsp)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	// No need to check type here.
	m := int64(d.GetMysqlDuration().Minute())
	d.SetInt64(m)
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_second
func builtinSecond(args []types.Datum, ctx context.Context) (types.Datum, error) {
	d, err := convertToDuration(ctx.GetSessionVars().StmtCtx, args[0], types.MaxFsp)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	// No need to check type here.
	s := int64(d.GetMysqlDuration().Second())
	d.SetInt64(s)
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_microsecond
func builtinMicroSecond(args []types.Datum, ctx context.Context) (types.Datum, error) {
	d, err := convertToDuration(ctx.GetSessionVars().StmtCtx, args[0], types.MaxFsp)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	// No need to check type here.
	m := int64(d.GetMysqlDuration().MicroSecond())
	d.SetInt64(m)
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_month
func builtinMonth(args []types.Datum, ctx context.Context) (types.Datum, error) {
	d, err := convertToTime(ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	// No need to check type here.
	t := d.GetMysqlTime()
	i := int64(0)
	if t.IsZero() {
		d.SetInt64(i)
		return d, nil
	}
	i = int64(t.Time.Month())
	d.SetInt64(i)
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_monthname
func builtinMonthName(args []types.Datum, ctx context.Context) (types.Datum, error) {
	d, err := builtinMonth(args, ctx)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	mon := int(d.GetInt64())
	if mon <= 0 || mon > len(types.MonthNames) {
		d.SetNull()
		if mon == 0 {
			return d, nil
		}
		return d, errors.Errorf("no name for invalid month: %d.", mon)
	}
	d.SetString(types.MonthNames[mon-1])

	return d, nil
}

func builtinNow(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	// TODO: if NOW is used in stored function or trigger, NOW will return the beginning time
	// of the execution.
	fsp := 0
	sc := ctx.GetSessionVars().StmtCtx
	if len(args) == 1 && !args[0].IsNull() {
		if fsp, err = checkFsp(sc, args[0]); err != nil {
			d.SetNull()
			return d, errors.Trace(err)
		}
	}

	tr, err := types.RoundFrac(time.Now(), int(fsp))
	if err != nil {
		d.SetNull()
		return d, errors.Trace(err)
	}

	t := types.Time{
		Time: types.FromGoTime(tr),
		Type: mysql.TypeDatetime,
		// set unspecified for later round
		Fsp: fsp,
	}

	d.SetMysqlTime(t)
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayname
func builtinDayName(args []types.Datum, ctx context.Context) (types.Datum, error) {
	d, err := builtinWeekDay(args, ctx)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}
	weekday := d.GetInt64()
	if (weekday < 0) || (weekday >= int64(len(types.WeekdayNames))) {
		d.SetNull()
		return d, errors.Errorf("no name for invalid weekday: %d.", weekday)
	}
	d.SetString(types.WeekdayNames[weekday])
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofmonth
func builtinDayOfMonth(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	// TODO: some invalid format like 2000-00-00 will return 0 too.
	d, err = convertToTime(ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	// No need to check type here.
	t := d.GetMysqlTime()
	if t.IsZero() {
		d.SetInt64(int64(0))
		return d, nil
	}

	d.SetInt64(int64(t.Time.Day()))
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofweek
func builtinDayOfWeek(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	d, err = convertToTime(ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	// No need to check type here.
	t := d.GetMysqlTime()
	if t.IsZero() {
		d.SetNull()
		// TODO: log warning or return error?
		return d, nil
	}

	// 1 is Sunday, 2 is Monday, .... 7 is Saturday
	d.SetInt64(int64(t.Time.Weekday() + 1))
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofyear
func builtinDayOfYear(args []types.Datum, ctx context.Context) (types.Datum, error) {
	d, err := convertToTime(ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	t := d.GetMysqlTime()
	if t.Time.Month() == 0 || t.Time.Day() == 0 {
		// TODO: log warning or return error?
		d.SetNull()
		return d, nil
	}

	yd := int64(t.Time.YearDay())
	d.SetInt64(yd)
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_week
func builtinWeek(args []types.Datum, ctx context.Context) (types.Datum, error) {
	d, err := convertToTime(ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	// No need to check type here.
	t := d.GetMysqlTime()
	if t.IsZero() {
		// TODO: log warning or return error?
		d.SetNull()
		return d, nil
	}

	var mode int
	if len(args) > 1 {
		v, err := args[1].ToInt64(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return d, errors.Trace(err)
		}
		mode = int(v)
	}

	week := t.Time.Week(mode)
	wi := int64(week)
	d.SetInt64(wi)
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_weekday
func builtinWeekDay(args []types.Datum, ctx context.Context) (types.Datum, error) {
	d, err := convertToTime(ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	// No need to check type here.
	t := d.GetMysqlTime()
	if t.IsZero() {
		// TODO: log warning or return error?
		d.SetNull()
		return d, nil
	}

	// Monday is 0, ... Sunday = 6 in MySQL
	// but in go, Sunday is 0, ... Saturday is 6
	// w will do a conversion.
	w := (int64(t.Time.Weekday()) + 6) % 7
	d.SetInt64(w)
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_weekofyear
func builtinWeekOfYear(args []types.Datum, ctx context.Context) (types.Datum, error) {
	// WeekOfYear is equivalent to to Week(date, 3)
	d := types.Datum{}
	d.SetInt64(3)
	return builtinWeek([]types.Datum{args[0], d}, ctx)
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_year
func builtinYear(args []types.Datum, ctx context.Context) (types.Datum, error) {
	d, err := convertToTime(ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	// No need to check type here.
	t := d.GetMysqlTime()
	if t.IsZero() {
		d.SetInt64(0)
		return d, nil
	}

	d.SetInt64(int64(t.Time.Year()))
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func builtinYearWeek(args []types.Datum, ctx context.Context) (types.Datum, error) {
	d, err := convertToTime(ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	// No need to check type here.
	t := d.GetMysqlTime()
	if t.Time.Month() == 0 || t.Time.Day() == 0 {
		d.SetNull()
		// TODO: log warning or return error?
		return d, nil
	}

	var mode int64
	if len(args) > 1 {
		v, err := args[1].ToInt64(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return d, errors.Trace(err)
		}
		mode = v
	}

	year, week := t.Time.YearWeek(int(mode))
	d.SetInt64(int64(week + year*100))
	if d.GetInt64() < 0 {
		d.SetInt64(math.MaxUint32)
	}
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-unixtime
func builtinFromUnixTime(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	unixTimeStamp, err := args[0].ToDecimal(sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	// 0 <= unixTimeStamp <= INT32_MAX
	if unixTimeStamp.IsNegative() {
		return
	}
	integralPart, err := unixTimeStamp.ToInt()
	if err == types.ErrTruncated {
		err = nil
	}
	if err != nil {
		return d, errors.Trace(err)
	}
	if integralPart > int64(math.MaxInt32) {
		return
	}
	// Split the integral part and fractional part of a decimal timestamp.
	// e.g. for timestamp 12345.678,
	// first get the integral part 12345,
	// then (12345.678 - 12345) * (10^9) to get the decimal part and convert it to nanosecond precision.
	integerDecimalTp := new(types.MyDecimal).FromInt(integralPart)
	fracDecimalTp := new(types.MyDecimal)
	err = types.DecimalSub(unixTimeStamp, integerDecimalTp, fracDecimalTp)
	if err != nil {
		return d, errors.Trace(err)
	}
	nano := new(types.MyDecimal).FromInt(int64(time.Second))
	x := new(types.MyDecimal)
	err = types.DecimalMul(fracDecimalTp, nano, x)
	if err != nil {
		return d, errors.Trace(err)
	}
	fractionalPart, err := x.ToInt() // here fractionalPart is result multiplying the original fractional part by 10^9.
	if err == types.ErrTruncated {
		err = nil
	}
	if err != nil {
		return d, errors.Trace(err)
	}

	_, fracDigitsNumber := unixTimeStamp.PrecisionAndFrac()
	fsp := fracDigitsNumber
	if fracDigitsNumber > types.MaxFsp {
		fsp = types.MaxFsp
	}
	tr, err := types.RoundFrac(time.Unix(integralPart, fractionalPart), fsp)
	if err != nil {
		return d, errors.Trace(err)
	}

	t := types.Time{
		Time: types.FromGoTime(tr),
		Type: mysql.TypeDatetime,
		Fsp:  fsp,
	}
	if args[0].Kind() == types.KindString { // Keep consistent with MySQL.
		t.Fsp = types.MaxFsp
	}
	d.SetMysqlTime(t)
	if len(args) == 1 {
		return
	}
	return builtinDateFormat([]types.Datum{d, args[1]}, ctx)
}

// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_str-to-date
func builtinStrToDate(args []types.Datum, _ context.Context) (types.Datum, error) {
	date := args[0].GetString()
	format := args[1].GetString()
	var (
		d types.Datum
		t types.Time
	)

	succ := t.StrToDate(date, format)
	if !succ {
		d.SetNull()
		return d, nil
	}

	d.SetMysqlTime(t)
	return d, nil
}

func builtinSysDate(args []types.Datum, ctx context.Context) (types.Datum, error) {
	// SYSDATE is not the same as NOW if NOW is used in a stored function or trigger.
	// But here we can just think they are the same because we don't support stored function
	// and trigger now.
	return builtinNow(args, ctx)
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_curdate
func builtinCurrentDate(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	year, month, day := time.Now().Date()
	t := types.Time{
		Time: types.FromDate(year, int(month), day, 0, 0, 0, 0),
		Type: mysql.TypeDate, Fsp: 0}
	d.SetMysqlTime(t)
	return d, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_curtime
func builtinCurrentTime(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	fsp := 0
	sc := ctx.GetSessionVars().StmtCtx
	if len(args) == 1 && !args[0].IsNull() {
		if fsp, err = checkFsp(sc, args[0]); err != nil {
			d.SetNull()
			return d, errors.Trace(err)
		}
	}
	d.SetString(time.Now().Format("15:04:05.000000"))
	return convertToDuration(ctx.GetSessionVars().StmtCtx, d, fsp)
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time
func builtinTime(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	if args[0].IsNull() {
		return
	}

	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	idx := strings.Index(str, ".")
	fsp := 0
	if idx != -1 {
		fsp = len(str) - idx - 1
	}
	sc := ctx.GetSessionVars().StmtCtx
	fspD := types.NewIntDatum(int64(fsp))
	if fsp, err = checkFsp(sc, fspD); err != nil {
		return d, errors.Trace(err)
	}

	return convertToDuration(sc, args[0], fsp)
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-date
func builtinUTCDate(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	year, month, day := time.Now().UTC().Date()
	t := types.Time{
		Time: types.FromGoTime(time.Date(year, month, day, 0, 0, 0, 0, time.UTC)),
		Type: mysql.TypeDate, Fsp: types.UnspecifiedFsp}
	d.SetMysqlTime(t)
	return d, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func builtinExtract(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	unit := args[0].GetString()
	vd := args[1]

	if vd.IsNull() {
		d.SetNull()
		return d, nil
	}

	f := types.NewFieldType(mysql.TypeDatetime)
	f.Decimal = types.MaxFsp
	val, err := vd.ConvertTo(ctx.GetSessionVars().StmtCtx, f)
	if err != nil {
		d.SetNull()
		return d, errors.Trace(err)
	}
	if val.IsNull() {
		d.SetNull()
		return d, nil
	}

	if val.Kind() != types.KindMysqlTime {
		d.SetNull()
		return d, errors.Errorf("need time type, but got %T", val)
	}
	t := val.GetMysqlTime()
	n, err1 := types.ExtractTimeNum(unit, t)
	if err1 != nil {
		d.SetNull()
		return d, errors.Trace(err1)
	}
	d.SetInt64(n)
	return d, nil
}

func checkFsp(sc *variable.StatementContext, arg types.Datum) (int, error) {
	fsp, err := arg.ToInt64(sc)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if int(fsp) > types.MaxFsp {
		return 0, errors.Errorf("Too big precision %d specified. Maximum is 6.", fsp)
	} else if fsp < 0 {
		return 0, errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
	}
	return int(fsp), nil
}

func builtinDateArith(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	// Op is used for distinguishing date_add and date_sub.
	// args[0] -> Op
	// args[1] -> Date
	// args[2] -> DateArithInterval
	// health check for date and interval
	if args[1].IsNull() {
		return d, nil
	}
	nodeDate := args[1]
	nodeInterval := args[2].GetInterface().(ast.DateArithInterval)
	nodeIntervalIntervalDatum := nodeInterval.Interval.GetDatum()
	if nodeIntervalIntervalDatum.IsNull() {
		return d, nil
	}
	// parse date
	fieldType := mysql.TypeDate
	var resultField *types.FieldType
	switch nodeDate.Kind() {
	case types.KindMysqlTime:
		x := nodeDate.GetMysqlTime()
		if (x.Type == mysql.TypeDatetime) || (x.Type == mysql.TypeTimestamp) {
			fieldType = mysql.TypeDatetime
		}
	case types.KindString:
		x := nodeDate.GetString()
		if !types.IsDateFormat(x) {
			fieldType = mysql.TypeDatetime
		}
	case types.KindInt64:
		x := nodeDate.GetInt64()
		if t, err1 := types.ParseTimeFromInt64(x); err1 == nil {
			if (t.Type == mysql.TypeDatetime) || (t.Type == mysql.TypeTimestamp) {
				fieldType = mysql.TypeDatetime
			}
		}
	}
	sc := ctx.GetSessionVars().StmtCtx
	if types.IsClockUnit(nodeInterval.Unit) {
		fieldType = mysql.TypeDatetime
	}
	resultField = types.NewFieldType(fieldType)
	resultField.Decimal = types.MaxFsp
	value, err := nodeDate.ConvertTo(ctx.GetSessionVars().StmtCtx, resultField)
	if err != nil {
		return d, errInvalidOperation.Gen("DateArith invalid args, need date but get %T", nodeDate)
	}
	if value.IsNull() {
		return d, errInvalidOperation.Gen("DateArith invalid args, need date but get %v", value.GetValue())
	}
	if value.Kind() != types.KindMysqlTime {
		return d, errInvalidOperation.Gen("DateArith need time type, but got %T", value.GetValue())
	}
	result := value.GetMysqlTime()
	// parse interval
	var interval string
	if strings.ToLower(nodeInterval.Unit) == "day" {
		day, err1 := parseDayInterval(sc, *nodeIntervalIntervalDatum)
		if err1 != nil {
			return d, errInvalidOperation.Gen("DateArith invalid day interval, need int but got %T", nodeIntervalIntervalDatum.GetString())
		}
		interval = fmt.Sprintf("%d", day)
	} else {
		if nodeIntervalIntervalDatum.Kind() == types.KindString {
			interval = fmt.Sprintf("%v", nodeIntervalIntervalDatum.GetString())
		} else {
			ii, err1 := nodeIntervalIntervalDatum.ToInt64(sc)
			if err1 != nil {
				return d, errors.Trace(err1)
			}
			interval = fmt.Sprintf("%v", ii)
		}
	}
	year, month, day, duration, err := types.ExtractTimeValue(nodeInterval.Unit, interval)
	if err != nil {
		return d, errors.Trace(err)
	}
	op := args[0].GetInterface().(ast.DateArithType)
	if op == ast.DateSub {
		year, month, day, duration = -year, -month, -day, -duration
	}
	t, err := result.Time.GoTime()
	if err != nil {
		return d, errors.Trace(err)
	}
	t = t.Add(duration)
	t = t.AddDate(int(year), int(month), int(day))
	if t.Nanosecond() == 0 {
		result.Fsp = 0
	}
	result.Time = types.FromGoTime(t)
	d.SetMysqlTime(result)
	return d, nil
}

var reg = regexp.MustCompile(`[\d]+`)

func parseDayInterval(sc *variable.StatementContext, value types.Datum) (int64, error) {
	switch value.Kind() {
	case types.KindString:
		vs := value.GetString()
		s := strings.ToLower(vs)
		if s == "false" {
			return 0, nil
		} else if s == "true" {
			return 1, nil
		}
		value.SetString(reg.FindString(vs))
	}
	return value.ToInt64(sc)
}
