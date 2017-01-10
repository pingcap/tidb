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

var (
	_ functionClass = &dateFunctionClass{}
	_ functionClass = &dateDiffFunctionClass{}
	_ functionClass = &timeDiffFunctionClass{}
	_ functionClass = &dateFormatFunctionClass{}
	_ functionClass = &dayFunctionClass{}
	_ functionClass = &hourFunctionClass{}
	_ functionClass = &minuteFunctionClass{}
	_ functionClass = &secondFunctionClass{}
	_ functionClass = &microSecondFunctionClass{}
	_ functionClass = &monthFunctionClass{}
	_ functionClass = &monthNameFunctionClass{}
	_ functionClass = &nowFunctionClass{}
	_ functionClass = &dayNameFunctionClass{}
	_ functionClass = &dayOfMonthFunctionClass{}
	_ functionClass = &dayOfWeekFunctionClass{}
	_ functionClass = &dayOfYearFunctionClass{}
	_ functionClass = &weekFunctionClass{}
	_ functionClass = &weekDayFunctionClass{}
	_ functionClass = &weekOfYearFunctionClass{}
	_ functionClass = &yearFunctionClass{}
	_ functionClass = &yearWeekFunctionClass{}
	_ functionClass = &fromUnixTimeFunctionClass{}
	_ functionClass = &strToDateFunctionClass{}
	_ functionClass = &sysDateFunctionClass{}
	_ functionClass = &currentDateFunctionClass{}
	_ functionClass = &currentTimeFunctionClass{}
	_ functionClass = &timeFunctionClass{}
	_ functionClass = &utcDateFunctionClass{}
	_ functionClass = &extractFunctionClass{}
	_ functionClass = &arithmeticFunctionClass{}
	_ functionClass = &unixTimestampFunctionClass{}
)

var (
	_ builtinFunc = &builtinDateSig{}
	_ builtinFunc = &builtinDateDiffSig{}
	_ builtinFunc = &builtinTimeDiffSig{}
	_ builtinFunc = &builtinDateFormatSig{}
	_ builtinFunc = &builtinDaySig{}
	_ builtinFunc = &builtinHourSig{}
	_ builtinFunc = &builtinMinuteSig{}
	_ builtinFunc = &builtinSecondSig{}
	_ builtinFunc = &builtinMicroSecondSig{}
	_ builtinFunc = &builtinMonthSig{}
	_ builtinFunc = &builtinMonthNameSig{}
	_ builtinFunc = &builtinNowSig{}
	_ builtinFunc = &builtinDayNameSig{}
	_ builtinFunc = &builtinDayOfMonthSig{}
	_ builtinFunc = &builtinDayOfWeekSig{}
	_ builtinFunc = &builtinDayOfYearSig{}
	_ builtinFunc = &builtinWeekSig{}
	_ builtinFunc = &builtinWeekDaySig{}
	_ builtinFunc = &builtinWeekOfYearSig{}
	_ builtinFunc = &builtinYearSig{}
	_ builtinFunc = &builtinYearWeekSig{}
	_ builtinFunc = &builtinFromUnixTimeSig{}
	_ builtinFunc = &builtinStrToDateSig{}
	_ builtinFunc = &builtinSysDateSig{}
	_ builtinFunc = &builtinCurrentDateSig{}
	_ builtinFunc = &builtinCurrentTimeSig{}
	_ builtinFunc = &builtinTimeSig{}
	_ builtinFunc = &builtinUTCDateSig{}
	_ builtinFunc = &builtinExtractSig{}
	_ builtinFunc = &builtinArithmeticSig{}
	_ builtinFunc = &builtinUnixTimestampSig{}
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

type dateFunctionClass struct {
	baseFunctionClass
}

func (c *dateFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinDateSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinDateSig struct {
	baseBuiltinFunc
}

func (b *builtinDateSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinDate(args, b.ctx)
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date
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

type dateDiffFunctionClass struct {
	baseFunctionClass
}

func (c *dateDiffFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinDateDiffSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinDateDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinDateDiffSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinDateDiff(args, b.ctx)
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_datediff
func builtinDateDiff(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	t1, err := convertDatumToTime(sc, args[0])
	if err != nil {
		return d, errors.Trace(err)
	}

	t2, err := convertDatumToTime(sc, args[1])
	if err != nil {
		return d, errors.Trace(err)
	}

	if t1.Time.Month() == 0 || t1.Time.Day() == 0 || t2.Time.Month() == 0 || t2.Time.Day() == 0 {
		return d, nil
	}

	r := types.DateDiff(t1.Time, t2.Time)
	d.SetInt64(int64(r))
	return d, nil
}

type timeDiffFunctionClass struct {
	baseFunctionClass
}

func (c *timeDiffFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinTimeDiffSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeDiffSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinTimeDiff(args, b.ctx)
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
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

type dateFormatFunctionClass struct {
	baseFunctionClass
}

func (c *dateFormatFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinDateFormatSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinDateFormatSig struct {
	baseBuiltinFunc
}

func (b *builtinDateFormatSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinDateFormat(args, b.ctx)
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

type dayFunctionClass struct {
	baseFunctionClass
}

func (c *dayFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinDaySig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinDaySig struct {
	baseBuiltinFunc
}

func (b *builtinDaySig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinDay(args, b.ctx)
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_day
// Day is a synonym for DayOfMonth.
func builtinDay(args []types.Datum, ctx context.Context) (types.Datum, error) {
	return builtinDayOfMonth(args, ctx)
}

type hourFunctionClass struct {
	baseFunctionClass
}

func (c *hourFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinHourSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinHourSig struct {
	baseBuiltinFunc
}

func (b *builtinHourSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinHour(args, b.ctx)
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

type minuteFunctionClass struct {
	baseFunctionClass
}

func (c *minuteFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinMinuteSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinMinuteSig struct {
	baseBuiltinFunc
}

func (b *builtinMinuteSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinMinute(args, b.ctx)
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

type secondFunctionClass struct {
	baseFunctionClass
}

func (c *secondFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinSecondSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinSecondSig struct {
	baseBuiltinFunc
}

func (b *builtinSecondSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinSecond(args, b.ctx)
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

type microSecondFunctionClass struct {
	baseFunctionClass
}

func (c *microSecondFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinMicroSecondSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinMicroSecondSig struct {
	baseBuiltinFunc
}

func (b *builtinMicroSecondSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinMicroSecond(args, b.ctx)
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

type monthFunctionClass struct {
	baseFunctionClass
}

func (c *monthFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinMonthSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinMonthSig struct {
	baseBuiltinFunc
}

func (b *builtinMonthSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinMonth(args, b.ctx)
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

type monthNameFunctionClass struct {
	baseFunctionClass
}

func (c *monthNameFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinMonthNameSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinMonthNameSig struct {
	baseBuiltinFunc
}

func (b *builtinMonthNameSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinMonthName(args, b.ctx)
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

type nowFunctionClass struct {
	baseFunctionClass
}

func (c *nowFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinNowSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinNowSig struct {
	baseBuiltinFunc
}

func (b *builtinNowSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinNow(args, b.ctx)
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_now
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

type dayNameFunctionClass struct {
	baseFunctionClass
}

func (c *dayNameFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinDayNameSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinDayNameSig struct {
	baseBuiltinFunc
}

func (b *builtinDayNameSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinDayName(args, b.ctx)
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

type dayOfMonthFunctionClass struct {
	baseFunctionClass
}

func (c *dayOfMonthFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinDayOfMonthSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinDayOfMonthSig struct {
	baseBuiltinFunc
}

func (b *builtinDayOfMonthSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinDayOfMonth(args, b.ctx)
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

type dayOfWeekFunctionClass struct {
	baseFunctionClass
}

func (c *dayOfWeekFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinDayOfWeekSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinDayOfWeekSig struct {
	baseBuiltinFunc
}

func (b *builtinDayOfWeekSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinDayOfWeek(args, b.ctx)
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

type dayOfYearFunctionClass struct {
	baseFunctionClass
}

func (c *dayOfYearFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinDayOfYearSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinDayOfYearSig struct {
	baseBuiltinFunc
}

func (b *builtinDayOfYearSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinDayOfYear(args, b.ctx)
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

type weekFunctionClass struct {
	baseFunctionClass
}

func (c *weekFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinWeekSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinWeekSig struct {
	baseBuiltinFunc
}

func (b *builtinWeekSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinWeek(args, b.ctx)
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

type weekDayFunctionClass struct {
	baseFunctionClass
}

func (c *weekDayFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinWeekDaySig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinWeekDaySig struct {
	baseBuiltinFunc
}

func (b *builtinWeekDaySig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinWeekDay(args, b.ctx)
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

type weekOfYearFunctionClass struct {
	baseFunctionClass
}

func (c *weekOfYearFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinWeekOfYearSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinWeekOfYearSig struct {
	baseBuiltinFunc
}

func (b *builtinWeekOfYearSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinWeekOfYear(args, b.ctx)
}

// See http://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_weekofyear
func builtinWeekOfYear(args []types.Datum, ctx context.Context) (types.Datum, error) {
	// WeekOfYear is equivalent to to Week(date, 3)
	d := types.Datum{}
	d.SetInt64(3)
	return builtinWeek([]types.Datum{args[0], d}, ctx)
}

type yearFunctionClass struct {
	baseFunctionClass
}

func (c *yearFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinYearSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinYearSig struct {
	baseBuiltinFunc
}

func (b *builtinYearSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinYear(args, b.ctx)
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

type yearWeekFunctionClass struct {
	baseFunctionClass
}

func (c *yearWeekFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinYearWeekSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinYearWeekSig struct {
	baseBuiltinFunc
}

func (b *builtinYearWeekSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinYearWeek(args, b.ctx)
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

type fromUnixTimeFunctionClass struct {
	baseFunctionClass
}

func (c *fromUnixTimeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinFromUnixTimeSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinFromUnixTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinFromUnixTimeSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinFromUnixTime(args, b.ctx)
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

type strToDateFunctionClass struct {
	baseFunctionClass
}

func (c *strToDateFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinStrToDateSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinStrToDateSig struct {
	baseBuiltinFunc
}

func (b *builtinStrToDateSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinStrToDate(args, b.ctx)
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

type sysDateFunctionClass struct {
	baseFunctionClass
}

func (c *sysDateFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinSysDateSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinSysDateSig struct {
	baseBuiltinFunc
}

func (b *builtinSysDateSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinSysDate(args, b.ctx)
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sysdate
func builtinSysDate(args []types.Datum, ctx context.Context) (types.Datum, error) {
	// SYSDATE is not the same as NOW if NOW is used in a stored function or trigger.
	// But here we can just think they are the same because we don't support stored function
	// and trigger now.
	return builtinNow(args, ctx)
}

type currentDateFunctionClass struct {
	baseFunctionClass
}

func (c *currentDateFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinCurrentDateSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinCurrentDateSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentDateSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinCurrentDate(args, b.ctx)
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

type currentTimeFunctionClass struct {
	baseFunctionClass
}

func (c *currentTimeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinCurrentTimeSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinCurrentTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentTimeSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinCurrentTime(args, b.ctx)
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

type timeFunctionClass struct {
	baseFunctionClass
}

func (c *timeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinTimeSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinTime(args, b.ctx)
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

type utcDateFunctionClass struct {
	baseFunctionClass
}

func (c *utcDateFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinUTCDateSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinUTCDateSig struct {
	baseBuiltinFunc
}

func (b *builtinUTCDateSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinUTCDate(args, b.ctx)
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

type extractFunctionClass struct {
	baseFunctionClass
}

func (c *extractFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinExtractSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinExtractSig struct {
	baseBuiltinFunc
}

func (b *builtinExtractSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinExtract(args, b.ctx)
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

type dateArithFunctionClass struct {
	baseFunctionClass

	op ast.DateArithType
}

func (c *dateArithFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinDateArithSig{newBaseBuiltinFunc(args, ctx), c.op}, errors.Trace(c.verifyArgs(args))
}

type builtinDateArithSig struct {
	baseBuiltinFunc

	op ast.DateArithType
}

func (b *builtinDateArithSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return dateArithFuncFactory(b.op)(args, b.ctx)
}

func dateArithFuncFactory(op ast.DateArithType) BuiltinFunc {
	return func(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
		// args[0] -> Date
		// args[1] -> Interval Value
		// args[2] -> Interval Unit
		// health check for date and interval
		if args[0].IsNull() || args[1].IsNull() {
			return d, nil
		}
		nodeDate := args[0]
		nodeIntervalValue := args[1]
		nodeIntervalUnit := args[2].GetString()
		if nodeIntervalValue.IsNull() {
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
		if types.IsClockUnit(nodeIntervalUnit) {
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
		if strings.ToLower(nodeIntervalUnit) == "day" {
			day, err1 := parseDayInterval(sc, nodeIntervalValue)
			if err1 != nil {
				return d, errInvalidOperation.Gen("DateArith invalid day interval, need int but got %T", nodeIntervalValue.GetString())
			}
			interval = fmt.Sprintf("%d", day)
		} else {
			if nodeIntervalValue.Kind() == types.KindString {
				interval = fmt.Sprintf("%v", nodeIntervalValue.GetString())
			} else {
				ii, err1 := nodeIntervalValue.ToInt64(sc)
				if err1 != nil {
					return d, errors.Trace(err1)
				}
				interval = fmt.Sprintf("%v", ii)
			}
		}
		year, month, day, duration, err := types.ExtractTimeValue(nodeIntervalUnit, interval)
		if err != nil {
			return d, errors.Trace(err)
		}
		if op == ast.DateArithSub {
			year, month, day, duration = -year, -month, -day, -duration
		}
		// TODO: Consider time_zone variable.
		t, err := result.Time.GoTime(time.Local)
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

type unixTimestampFunctionClass struct {
	baseFunctionClass
}

func (c *unixTimestampFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinUnixTimestampSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinUnixTimestampSig struct {
	baseBuiltinFunc
}

func (b *builtinUnixTimestampSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinUnixTimestamp(args, b.ctx)
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func builtinUnixTimestamp(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	if len(args) == 0 {
		now := time.Now().Unix()
		d.SetInt64(now)
		return
	}

	var (
		t  types.Time
		t1 time.Time
	)
	switch args[0].Kind() {
	case types.KindString:
		t, err = types.ParseTime(args[0].GetString(), mysql.TypeDatetime, types.MaxFsp)
		if err != nil {
			return d, errors.Trace(err)
		}
	case types.KindInt64, types.KindUint64:
		t, err = types.ParseTimeFromInt64(args[0].GetInt64())
		if err != nil {
			return d, errors.Trace(err)
		}
	}

	t1, err = t.Time.GoTime(getTimeZone(ctx))
	if err != nil {
		d.SetInt64(0)
		return d, nil
	}

	if t.Time.Microsecond() > 0 {
		var dec types.MyDecimal
		dec.FromFloat64(float64(t1.Unix()) + float64(t.Time.Microsecond())/1e6)
		d.SetMysqlDecimal(&dec)
	} else {
		d.SetInt64(t1.Unix())
	}
	return
}

func getTimeZone(ctx context.Context) *time.Location {
	ret := ctx.GetSessionVars().TimeZone
	if ret == nil {
		ret = time.Local
	}
	return ret
}
