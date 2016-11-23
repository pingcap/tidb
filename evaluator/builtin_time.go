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

package evaluator

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

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

func abbrDayOfMonth(arg types.Datum, ctx context.Context) (types.Datum, error) {
	day, err := builtinDayOfMonth([]types.Datum{arg}, ctx)
	if err != nil || arg.IsNull() {
		return types.Datum{}, errors.Trace(err)
	}
	var str string
	switch day.GetInt64() {
	case 1, 21, 31:
		str = "st"
	case 2, 22:
		str = "nd"
	case 3, 23:
		str = "rd"
	default:
		str = "th"
	}

	d := types.NewStringDatum(fmt.Sprintf("%d%s", day.GetInt64(), str))
	return d, nil
}

func to12Hour(arg types.Datum, ctx context.Context) (types.Datum, error) {
	d, err := builtinTime([]types.Datum{arg}, ctx)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	var str string
	var hour int
	if d.GetMysqlDuration().Hour() > 12 {
		hour = 12
		str = "PM"
	} else {
		// If the hour value is 0 in 24-hour time system,
		// then the hour value is 23 in 12-hour time system.
		if d.GetMysqlDuration().Hour() == 0 {
			hour = -12
		}
		str = "AM"
	}
	duration := types.Duration{
		Duration: d.GetMysqlDuration().Duration - time.Duration(hour)*time.Hour,
		Fsp:      0}
	str = fmt.Sprintf("%v %s", duration, str)
	d.SetString(str)
	return d, nil
}

func convertDateFormat(ctx context.Context, arg types.Datum, b byte) (types.Datum, error) {
	var d types.Datum
	var err error

	switch b {
	case 'b':
		d, err = builtinMonthName([]types.Datum{arg}, ctx)
		if err == nil && !d.IsNull() {
			d.SetString(d.GetString()[:3])
		}
	case 'M':
		d, err = builtinMonthName([]types.Datum{arg}, ctx)
	case 'm':
		d, err = builtinMonth([]types.Datum{arg}, ctx)
		if err == nil && !d.IsNull() {
			d.SetString(fmt.Sprintf("%02d", d.GetInt64()))
		}
	case 'c':
		d, err = builtinMonth([]types.Datum{arg}, ctx)
	case 'D':
		d, err = abbrDayOfMonth(arg, ctx)
	case 'd':
		d, err = builtinDayOfMonth([]types.Datum{arg}, ctx)
		if err == nil && !d.IsNull() {
			d.SetString(fmt.Sprintf("%02d", d.GetInt64()))
		}
	case 'e':
		d, err = builtinDayOfMonth([]types.Datum{arg}, ctx)
	case 'j':
		d, err = builtinDayOfYear([]types.Datum{arg}, ctx)
		if err == nil {
			d.SetString(fmt.Sprintf("%03d", d.GetInt64()))
		}
	case 'H', 'k':
		d, err = builtinHour([]types.Datum{arg}, ctx)
		if err == nil && b == 'H' && !d.IsNull() {
			d.SetString(fmt.Sprintf("%02d", d.GetInt64()))
		}
	case 'h', 'I', 'l':
		d, err = builtinHour([]types.Datum{arg}, ctx)
		if err == nil && !d.IsNull() {
			if d.GetInt64() > 12 {
				d.SetInt64(d.GetInt64() - 12)
			} else if d.GetInt64() == 0 {
				d.SetInt64(12)
			}
			d.SetString(fmt.Sprintf("%02d", d.GetInt64()))
		}
	case 'i':
		d, err = builtinMinute([]types.Datum{arg}, ctx)
		if err == nil && !d.IsNull() {
			d.SetString(fmt.Sprintf("%02d", d.GetInt64()))
		}
	case 'p':
		d, err = builtinHour([]types.Datum{arg}, ctx)
		if err == nil && !d.IsNull() {
			if d.GetInt64() < 12 {
				d.SetString("AM")
				break
			}
			d.SetString("PM")
		}
	case 'r':
		d, err = to12Hour(arg, ctx)
	case 'T':
		d, err = builtinTime([]types.Datum{arg}, ctx)
		if err == nil && !d.IsNull() {
			duration := types.Duration{
				Duration: d.GetMysqlDuration().Duration,
				Fsp:      0}
			d.SetMysqlDuration(duration)
		}
	case 'S', 's':
		d, err = builtinSecond([]types.Datum{arg}, ctx)
		if err == nil && !d.IsNull() {
			d.SetString(fmt.Sprintf("%02d", d.GetInt64()))
		}
	case 'f':
		d, err = builtinMicroSecond([]types.Datum{arg}, ctx)
		if err == nil && !d.IsNull() {
			d.SetString(fmt.Sprintf("%06d", d.GetInt64()))
		}
	case 'U':
		d, err = builtinWeek([]types.Datum{arg, types.NewIntDatum(0)}, ctx)
		if err == nil && !d.IsNull() {
			d.SetString(fmt.Sprintf("%02d", d.GetInt64()))
		}
	case 'u':
		d, err = builtinWeek([]types.Datum{arg, types.NewIntDatum(1)}, ctx)
		if err == nil && !d.IsNull() {
			d.SetString(fmt.Sprintf("%02d", d.GetInt64()))
		}
	case 'V':
		d, err = builtinWeek([]types.Datum{arg, types.NewIntDatum(2)}, ctx)
		if err == nil && !d.IsNull() {
			d.SetString(fmt.Sprintf("%02d", d.GetInt64()))
		}
	case 'v':
		d, err = builtinWeek([]types.Datum{arg, types.NewIntDatum(3)}, ctx)
		if err == nil && !d.IsNull() {
			d.SetString(fmt.Sprintf("%02d", d.GetInt64()))
		}
	case 'a':
		d, err = builtinDayName([]types.Datum{arg}, ctx)
		if err == nil && !d.IsNull() {
			d.SetString(d.GetString()[:3])
		}
	case 'W':
		d, err = builtinDayName([]types.Datum{arg}, ctx)
	case 'w':
		d, err = builtinDayOfWeek([]types.Datum{arg}, ctx)
		if err == nil && !d.IsNull() {
			d.SetInt64(d.GetInt64() - 1)
		}
	case 'X':
		d, err = builtinYearWeek([]types.Datum{arg, types.NewIntDatum(2)}, ctx)
		if err == nil && !d.IsNull() {
			if d.GetInt64() == math.MaxUint32 {
				break
			}
			str := fmt.Sprintf("%04d", d.GetInt64())
			d.SetString(fmt.Sprintf("%04s", str[:4]))
		}
	case 'x':
		d, err = builtinYearWeek([]types.Datum{arg, types.NewIntDatum(3)}, ctx)
		if err == nil && !d.IsNull() {
			if d.GetInt64() == math.MaxUint32 {
				break
			}
			str := fmt.Sprintf("%04d", d.GetInt64())
			d.SetString(fmt.Sprintf("%04s", str[:4]))
		}
	case 'Y':
		d, err = builtinYear([]types.Datum{arg}, ctx)
		if err == nil && !d.IsNull() {
			d.SetString(fmt.Sprintf("%04d", d.GetInt64()))
		}
	case 'y':
		d, err = builtinYear([]types.Datum{arg}, ctx)
		if err == nil && !d.IsNull() {
			str := fmt.Sprintf("%04d", d.GetInt64())
			d.SetString(fmt.Sprintf("%02s", str[2:]))
		}
	default:
		d.SetString(string(b))
	}

	if err == nil && !d.IsNull() {
		d.SetString(fmt.Sprintf("%v", d.GetValue()))
	}
	return d, errors.Trace(err)
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func builtinDateFormat(args []types.Datum, ctx context.Context) (types.Datum, error) {
	var (
		isPercent bool
		ret       []byte
		d         types.Datum
	)

	// TODO: Some invalid format like 2000-00-01(the month is 0) will return null.
	for _, b := range []byte(args[1].GetString()) {
		if isPercent {
			if b == '%' {
				ret = append(ret, b)
			} else {
				str, err := convertDateFormat(ctx, args[0], b)
				if err != nil {
					return types.Datum{}, errors.Trace(err)
				}
				if str.IsNull() {
					return types.Datum{}, nil
				}
				ret = append(ret, str.GetString()...)
			}
			isPercent = false
			continue
		}
		if b == '%' {
			isPercent = true
		} else {
			ret = append(ret, b)
		}
	}
	d.SetString(string(ret))
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
	i = int64(t.Month())
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

func builtinNow(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	// TODO: if NOW is used in stored function or trigger, NOW will return the beginning time
	// of the execution.
	fsp := 0
	if len(args) == 1 && !args[0].IsNull() {
		if fsp, err = checkFsp(args[0]); err != nil {
			d.SetNull()
			return d, errors.Trace(err)
		}
	}

	t := types.Time{
		Time: time.Now(),
		Type: mysql.TypeDatetime,
		// set unspecified for later round
		Fsp: types.UnspecifiedFsp,
	}

	tr, err := t.RoundFrac(int(fsp))
	if err != nil {
		d.SetNull()
		return d, errors.Trace(err)
	}
	d.SetMysqlTime(tr)
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

	d.SetInt64(int64(t.Day()))
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
	d.SetInt64(int64(t.Weekday()) + 1)
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofyear
func builtinDayOfYear(args []types.Datum, ctx context.Context) (types.Datum, error) {
	d, err := convertToTime(ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	t := d.GetMysqlTime()
	if t.IsZero() {
		// TODO: log warning or return error?
		d.SetNull()
		return d, nil
	}

	yd := int64(t.YearDay())
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

	// TODO: support multi mode for week
	_, week := t.ISOWeek()
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
	w := (int64(t.Weekday()) + 6) % 7
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

	d.SetInt64(int64(t.Year()))
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
	if t.IsZero() {
		d.SetNull()
		// TODO: log warning or return error?
		return d, nil
	}

	// TODO: support multi mode for week
	year, week := t.ISOWeek()
	d.SetInt64(int64(year*100 + week))
	if d.GetInt64() < 0 {
		d.SetInt64(math.MaxUint32)
	}
	return d, nil
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-unixtime
func builtinFromUnixTime(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	unixTimeStamp, err := args[0].ToDecimal()
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
	t := types.Time{
		Time: time.Unix(integralPart, fractionalPart),
		Type: mysql.TypeDatetime,
		Fsp:  types.UnspecifiedFsp,
	}
	_, fracDigitsNumber := unixTimeStamp.PrecisionAndFrac()
	fsp := fracDigitsNumber
	if fracDigitsNumber > types.MaxFsp {
		fsp = types.MaxFsp
	}
	t, err = t.RoundFrac(fsp)
	if err != nil {
		return d, errors.Trace(err)
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

// strToDate converts date string according to format, returns true on success,
// the value will be stored in argument t.
func strToDate(t *time.Time, date string, format string) bool {
	date = skipWhiteSpace(date)
	format = skipWhiteSpace(format)

	token, formatRemain, succ := getFormatToken(format)
	if !succ {
		return false
	}

	if token == "" {
		return date == ""
	}

	dateRemain, succ := matchDateWithToken(t, date, token)
	if !succ {
		return false
	}

	return strToDate(t, dateRemain, formatRemain)
}

// getFormatToken takes one format control token from the string.
// format "%d %H %m" will get token "%d" and the remain is " %H %m".
func getFormatToken(format string) (token string, remain string, succ bool) {
	if len(format) == 0 {
		return "", "", true
	}

	// Just one character.
	if len(format) == 1 {
		if format[0] == '%' {
			return "", "", false
		}
		return format, "", true
	}

	// More than one character.
	if format[0] == '%' {
		return format[:2], format[2:], true
	}

	return format[:1], format[1:], true
}

func skipWhiteSpace(input string) string {
	for i, c := range input {
		if !unicode.IsSpace(c) {
			return input[i:]
		}
	}
	return ""
}

var weekdayAbbrev = map[string]time.Weekday{
	"Sun": time.Sunday,
	"Mon": time.Monday,
	"Tue": time.Tuesday,
	"Wed": time.Wednesday,
	"Thu": time.Tuesday,
	"Fri": time.Friday,
	"Sat": time.Saturday,
}

var monthAbbrev = map[string]time.Month{
	"Jan": time.January,
	"Feb": time.February,
	"Mar": time.March,
	"Apr": time.April,
	"May": time.May,
	"Jun": time.June,
	"Jul": time.July,
	"Aug": time.August,
	"Sep": time.September,
	"Oct": time.October,
	"Nov": time.November,
	"Dec": time.December,
}

type dateFormatParser func(t *time.Time, date string) (remain string, succ bool)

var dateFormatParserTable = map[string]dateFormatParser{
	"%a": abbreviatedWeekday,
	"%b": abbreviatedMonth,
	"%c": monthNumeric,
	"%D": dayOfMonthWithSuffix,
	"%Y": yearNumericFourDigits,
	"%m": monthNumericTwoDigits,
	"%d": dayOfMonthNumericTwoDigits,
	"%H": hour24TwoDigits,
	"%i": minutesNumeric,
	"%s": secondsNumeric,
}

func matchDateWithToken(t *time.Time, date string, token string) (remain string, succ bool) {
	if parse, ok := dateFormatParserTable[token]; ok {
		return parse(t, date)
	}

	if strings.HasPrefix(date, token) {
		return date[len(token):], true
	}
	return date, false
}

func parseTwoDigits(input string) (int, bool) {
	if len(input) < 2 {
		return 0, false
	}

	v, err := strconv.ParseUint(input[:2], 10, 64)
	if err != nil {
		return int(v), false
	}
	return int(v), true
}

func hour24TwoDigits(t *time.Time, input string) (string, bool) {
	v, succ := parseTwoDigits(input)
	if !succ || v >= 24 {
		return input, false
	}
	timeSetHour(t, v)
	return input[2:], true
}

func secondsNumeric(t *time.Time, input string) (string, bool) {
	v, succ := parseTwoDigits(input)
	if !succ || v >= 60 {
		return input, false
	}
	timeSetSecond(t, v)
	return input[2:], true
}

func minutesNumeric(t *time.Time, input string) (string, bool) {
	v, succ := parseTwoDigits(input)
	if !succ || v >= 60 {
		return input, false
	}
	timeSetMinute(t, v)
	return input[2:], true
}

func dayOfMonthNumericTwoDigits(t *time.Time, input string) (string, bool) {
	v, succ := parseTwoDigits(input)
	if !succ || v >= 32 {
		return input, false
	}
	timeSetDay(t, v)
	return input[2:], true
}

func yearNumericFourDigits(t *time.Time, input string) (string, bool) {
	if len(input) < 4 {
		return input, false
	}

	v, err := strconv.ParseUint(input[:4], 10, 64)
	if err != nil {
		return input, false
	}
	timeSetYear(t, int(v))
	return input[4:], true
}

func monthNumericTwoDigits(t *time.Time, input string) (string, bool) {
	v, succ := parseTwoDigits(input)
	if !succ || v > 12 {
		return input, false
	}

	timeSetMonth(t, time.Month(v))
	return input[2:], true
}

func abbreviatedWeekday(t *time.Time, input string) (string, bool) {
	if len(input) >= 3 {
		dayName := input[:3]
		if _, ok := weekdayAbbrev[dayName]; ok {
			// TODO: We need refact mysql time to support this.
			return input, false
		}
	}
	return input, false
}

func abbreviatedMonth(t *time.Time, input string) (string, bool) {
	if len(input) >= 3 {
		monthName := input[:3]
		if month, ok := monthAbbrev[monthName]; ok {
			timeSetMonth(t, month)
			return input[len(monthName):], true
		}
	}
	return input, false
}

func monthNumeric(t *time.Time, input string) (string, bool) {
	// TODO: This code is ugly!
	for i := 12; i >= 0; i-- {
		str := strconv.FormatInt(int64(i), 10)
		if strings.HasPrefix(input, str) {
			timeSetMonth(t, time.Month(i))
			return input[len(str):], true
		}
	}

	return input, false
}

// 0th 1st 2nd 3rd ...
func dayOfMonthWithSuffix(t *time.Time, input string) (string, bool) {
	month, remain := parseOrdinalNumbers(input)
	if month >= 0 {
		timeSetMonth(t, time.Month(month))
		return remain, true
	}
	return input, false
}

func parseOrdinalNumbers(input string) (value int, remain string) {
	for i, c := range input {
		if !unicode.IsDigit(c) {
			v, err := strconv.ParseUint(input[:i], 10, 64)
			if err != nil {
				return -1, input
			}
			value = int(v)
			break
		}
	}
	switch {
	case strings.HasPrefix(remain, "st"):
		if value == 1 {
			remain = remain[2:]
			return
		}
	case strings.HasPrefix(remain, "nd"):
		if value == 2 {
			remain = remain[2:]
			return
		}
	case strings.HasPrefix(remain, "th"):
		remain = remain[2:]
		return
	}
	return -1, input
}

func timeSetYear(t *time.Time, year int) {
	_, month, day := t.Date()
	hour, min, sec := t.Clock()
	nsec := t.Nanosecond()
	loc := t.Location()
	*t = time.Date(year, month, day, hour, min, sec, nsec, loc)
}

func timeSetMonth(t *time.Time, month time.Month) {
	year, _, day := t.Date()
	hour, min, sec := t.Clock()
	nsec := t.Nanosecond()
	loc := t.Location()
	*t = time.Date(year, month, day, hour, min, sec, nsec, loc)
}

func timeSetDay(t *time.Time, day int) {
	year, month, _ := t.Date()
	hour, min, sec := t.Clock()
	nsec := t.Nanosecond()
	loc := t.Location()
	*t = time.Date(year, month, day, hour, min, sec, nsec, loc)
}

func timeSetHour(t *time.Time, hour int) {
	year, month, day := t.Date()
	_, min, sec := t.Clock()
	nsec := t.Nanosecond()
	loc := t.Location()
	*t = time.Date(year, month, day, hour, min, sec, nsec, loc)
}

func timeSetMinute(t *time.Time, min int) {
	year, month, day := t.Date()
	hour, _, sec := t.Clock()
	nsec := t.Nanosecond()
	loc := t.Location()
	*t = time.Date(year, month, day, hour, min, sec, nsec, loc)
}

func timeSetSecond(t *time.Time, sec int) {
	year, month, day := t.Date()
	hour, min, _ := t.Clock()
	nsec := t.Nanosecond()
	loc := t.Location()
	*t = time.Date(year, month, day, hour, min, sec, nsec, loc)
}

// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_str-to-date
func builtinStrToDate(args []types.Datum, _ context.Context) (types.Datum, error) {
	date := args[0].GetString()
	format := args[1].GetString()
	var (
		d      types.Datum
		goTime time.Time
	)
	goTime = types.ZeroTime
	if !strToDate(&goTime, date, format) {
		d.SetNull()
		return d, nil
	}

	t := types.Time{
		Time: goTime,
		Type: mysql.TypeDatetime,
		Fsp:  types.UnspecifiedFsp,
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
		Time: time.Date(year, month, day, 0, 0, 0, 0, time.Local),
		Type: mysql.TypeDate, Fsp: 0}
	d.SetMysqlTime(t)
	return d, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_curtime
func builtinCurrentTime(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	fsp := 0
	if len(args) == 1 && !args[0].IsNull() {
		if fsp, err = checkFsp(args[0]); err != nil {
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
	fspD := types.NewIntDatum(int64(fsp))
	if fsp, err = checkFsp(fspD); err != nil {
		return d, errors.Trace(err)
	}

	return convertToDuration(ctx.GetSessionVars().StmtCtx, args[0], fsp)
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-date
func builtinUTCDate(args []types.Datum, _ context.Context) (d types.Datum, err error) {
	year, month, day := time.Now().UTC().Date()
	t := types.Time{
		Time: time.Date(year, month, day, 0, 0, 0, 0, time.UTC),
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

func checkFsp(arg types.Datum) (int, error) {
	fsp, err := arg.ToInt64()
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
	if types.IsClockUnit(nodeInterval.Unit) {
		fieldType = mysql.TypeDatetime
	}
	resultField = types.NewFieldType(fieldType)
	resultField.Decimal = types.MaxFsp
	value, err := nodeDate.ConvertTo(ctx.GetSessionVars().StmtCtx, resultField)
	if err != nil {
		return d, ErrInvalidOperation.Gen("DateArith invalid args, need date but get %T", nodeDate)
	}
	if value.IsNull() {
		return d, ErrInvalidOperation.Gen("DateArith invalid args, need date but get %v", value.GetValue())
	}
	if value.Kind() != types.KindMysqlTime {
		return d, ErrInvalidOperation.Gen("DateArith need time type, but got %T", value.GetValue())
	}
	result := value.GetMysqlTime()
	// parse interval
	var interval string
	if strings.ToLower(nodeInterval.Unit) == "day" {
		day, err1 := parseDayInterval(*nodeIntervalIntervalDatum)
		if err1 != nil {
			return d, ErrInvalidOperation.Gen("DateArith invalid day interval, need int but got %T", nodeIntervalIntervalDatum.GetString())
		}
		interval = fmt.Sprintf("%d", day)
	} else {
		if nodeIntervalIntervalDatum.Kind() == types.KindString {
			interval = fmt.Sprintf("%v", nodeIntervalIntervalDatum.GetString())
		} else {
			ii, err1 := nodeIntervalIntervalDatum.ToInt64()
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
	result.Time = result.Time.Add(duration)
	result.Time = result.Time.AddDate(int(year), int(month), int(day))
	if result.Time.Nanosecond() == 0 {
		result.Fsp = 0
	}
	d.SetMysqlTime(result)
	return d, nil
}

var reg = regexp.MustCompile(`[\d]+`)

func parseDayInterval(value types.Datum) (int64, error) {
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
	return value.ToInt64()
}
