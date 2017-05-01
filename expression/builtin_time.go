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
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

const ( // GET_FORMAT first argument.
	dateFormat     = "DATE"
	datetimeFormat = "DATETIME"
	timeFormat     = "TIME"
)

const ( // GET_FORMAT location.
	usaLocation      = "USA"
	jisLocation      = "JIS"
	isoLocation      = "ISO"
	eurLocation      = "EUR"
	internalLocation = "INTERNAL"
)

var (
	_ functionClass = &dateFunctionClass{}
	_ functionClass = &dateDiffFunctionClass{}
	_ functionClass = &timeDiffFunctionClass{}
	_ functionClass = &dateFormatFunctionClass{}
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
	_ functionClass = &getFormatFunctionClass{}
	_ functionClass = &strToDateFunctionClass{}
	_ functionClass = &sysDateFunctionClass{}
	_ functionClass = &currentDateFunctionClass{}
	_ functionClass = &currentTimeFunctionClass{}
	_ functionClass = &timeFunctionClass{}
	_ functionClass = &utcDateFunctionClass{}
	_ functionClass = &utcTimestampFunctionClass{}
	_ functionClass = &extractFunctionClass{}
	_ functionClass = &arithmeticFunctionClass{}
	_ functionClass = &unixTimestampFunctionClass{}
	_ functionClass = &addTimeFunctionClass{}
	_ functionClass = &convertTzFunctionClass{}
	_ functionClass = &makeDateFunctionClass{}
	_ functionClass = &makeTimeFunctionClass{}
	_ functionClass = &periodAddFunctionClass{}
	_ functionClass = &periodDiffFunctionClass{}
	_ functionClass = &quarterFunctionClass{}
	_ functionClass = &secToTimeFunctionClass{}
	_ functionClass = &subTimeFunctionClass{}
	_ functionClass = &timeFormatFunctionClass{}
	_ functionClass = &timeToSecFunctionClass{}
	_ functionClass = &timestampAddFunctionClass{}
	_ functionClass = &toDaysFunctionClass{}
	_ functionClass = &toSecondsFunctionClass{}
	_ functionClass = &utcTimeFunctionClass{}
	_ functionClass = &timestampFunctionClass{}
)

var (
	_ builtinFunc = &builtinDateSig{}
	_ builtinFunc = &builtinDateDiffSig{}
	_ builtinFunc = &builtinTimeDiffSig{}
	_ builtinFunc = &builtinDateFormatSig{}
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
	_ builtinFunc = &builtinGetFormatSig{}
	_ builtinFunc = &builtinStrToDateSig{}
	_ builtinFunc = &builtinSysDateSig{}
	_ builtinFunc = &builtinCurrentDateSig{}
	_ builtinFunc = &builtinCurrentTimeSig{}
	_ builtinFunc = &builtinTimeSig{}
	_ builtinFunc = &builtinUTCDateSig{}
	_ builtinFunc = &builtinUTCTimestampSig{}
	_ builtinFunc = &builtinExtractSig{}
	_ builtinFunc = &builtinArithmeticSig{}
	_ builtinFunc = &builtinUnixTimestampSig{}
	_ builtinFunc = &builtinAddTimeSig{}
	_ builtinFunc = &builtinConvertTzSig{}
	_ builtinFunc = &builtinMakeDateSig{}
	_ builtinFunc = &builtinMakeTimeSig{}
	_ builtinFunc = &builtinPeriodAddSig{}
	_ builtinFunc = &builtinPeriodDiffSig{}
	_ builtinFunc = &builtinQuarterSig{}
	_ builtinFunc = &builtinSecToTimeSig{}
	_ builtinFunc = &builtinSubTimeSig{}
	_ builtinFunc = &builtinTimeFormatSig{}
	_ builtinFunc = &builtinTimeToSecSig{}
	_ builtinFunc = &builtinTimestampAddSig{}
	_ builtinFunc = &builtinToDaysSig{}
	_ builtinFunc = &builtinToSecondsSig{}
	_ builtinFunc = &builtinUTCTimeSig{}
	_ builtinFunc = &builtinTimestampSig{}
)

func convertTimeToMysqlTime(t time.Time, fsp int) (types.Time, error) {
	tr, err := types.RoundFrac(t, int(fsp))
	if err != nil {
		return types.Time{}, errors.Trace(err)
	}

	return types.Time{
		Time: types.FromGoTime(tr),
		Type: mysql.TypeDatetime,
		Fsp:  fsp,
	}, nil
}

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

// eval evals a builtinDateSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date
func (b *builtinDateSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return convertToTime(b.ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
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

// eval evals a builtinDateDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_datediff
func (b *builtinDateDiffSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() || args[1].IsNull() {
		return d, nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
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

// eval evals a builtinTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinTimeDiffSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() || args[1].IsNull() {
		return d, nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
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

// eval evals a builtinDateFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func (b *builtinDateFormatSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	return builtinDateFormat(args, b.ctx)
}

// builtinDateFormat ...
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func builtinDateFormat(args []types.Datum, ctx context.Context) (types.Datum, error) {
	var d types.Datum
	date, err := convertToTime(ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDatetime)
	if err != nil {
		return d, errors.Trace(err)
	}

	if date.IsNull() {
		return d, nil
	}
	t := date.GetMysqlTime()
	str, err := t.DateFormat(args[1].GetString())
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetString(str)
	return d, nil
}

type fromDaysFunctionClass struct {
	baseFunctionClass
}

func (c *fromDaysFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinFromDaysSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinFromDaysSig struct {
	baseBuiltinFunc
}

// eval evals a builtinFromDaysSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-days
func (b *builtinFromDaysSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	days, err := args[0].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	d.SetMysqlTime(types.TimeFromDays(days))
	return d, nil
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

// eval evals a builtinHourSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_hour
func (b *builtinHourSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d, err := convertToDuration(b.ctx.GetSessionVars().StmtCtx, args[0], types.MaxFsp)
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

// eval evals a builtinMinuteSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_minute
func (b *builtinMinuteSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d, err := convertToDuration(b.ctx.GetSessionVars().StmtCtx, args[0], types.MaxFsp)
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

// eval evals a builtinSecondSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_second
func (b *builtinSecondSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d, err := convertToDuration(b.ctx.GetSessionVars().StmtCtx, args[0], types.MaxFsp)
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

// eval evals a builtinMicroSecondSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_microsecond
func (b *builtinMicroSecondSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d, err := convertToDuration(b.ctx.GetSessionVars().StmtCtx, args[0], types.MaxFsp)
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

// builtinMonth ...
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_month
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

// builtinMonthName ...
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_monthname
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

// builtinNow ...
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_now
func builtinNow(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	// TODO: if NOW is used in stored function or trigger, NOW will return the beginning time
	// of the execution.
	fsp := 0
	sc := ctx.GetSessionVars().StmtCtx
	if len(args) == 1 && !args[0].IsNull() {
		if fsp, err = checkFsp(sc, args[0]); err != nil {
			return d, errors.Trace(err)
		}
	}

	t, err := convertTimeToMysqlTime(time.Now(), fsp)
	if err != nil {
		return d, errors.Trace(err)
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

// eval evals a builtinDayNameSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayname
func (b *builtinDayNameSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d, err := builtinWeekDay(args, b.ctx)
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

// eval evals a builtinDayOfMonthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofmonth
func (b *builtinDayOfMonthSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// TODO: some invalid format like 2000-00-00 will return 0 too.
	d, err = convertToTime(b.ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
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

// eval evals a builtinDayOfWeekSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofweek
func (b *builtinDayOfWeekSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d, err = convertToTime(b.ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
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

// eval evals a builtinDayOfYearSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofyear
func (b *builtinDayOfYearSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d, err := convertToTime(b.ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	t := d.GetMysqlTime()
	if t.InvalidZero() {
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

// builtinWeek ...
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_week
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

// builtinWeekDay ...
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_weekday
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

// eval evals a builtinWeekOfYearSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_weekofyear
func (b *builtinWeekOfYearSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// WeekOfYear is equivalent to to Week(date, 3)
	d := types.Datum{}
	d.SetInt64(3)
	return builtinWeek([]types.Datum{args[0], d}, b.ctx)
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

// eval evals a builtinYearSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_year
func (b *builtinYearSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d, err := convertToTime(b.ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
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

// eval evals a builtinYearWeekSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d, err := convertToTime(b.ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	// No need to check type here.
	t := d.GetMysqlTime()
	if t.InvalidZero() {
		d.SetNull()
		// TODO: log warning or return error?
		return d, nil
	}

	var mode int64
	if len(args) > 1 {
		v, err := args[1].ToInt64(b.ctx.GetSessionVars().StmtCtx)
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

// eval evals a builtinFromUnixTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-unixtime
func (b *builtinFromUnixTimeSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	sc := b.ctx.GetSessionVars().StmtCtx
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

	t, err := convertTimeToMysqlTime(time.Unix(integralPart, fractionalPart), fsp)
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
	return builtinDateFormat([]types.Datum{d, args[1]}, b.ctx)
}

type getFormatFunctionClass struct {
	baseFunctionClass
}

func (c *getFormatFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinGetFormatSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinGetFormatSig struct {
	baseBuiltinFunc
}

// eval evals a builtinGetFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_get-format
func (b *builtinGetFormatSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	t := args[0].GetString()
	l := args[1].GetString()
	switch t {
	case dateFormat:
		switch l {
		case usaLocation:
			d.SetString("%m.%d.%Y")
		case jisLocation:
			d.SetString("%Y-%m-%d")
		case isoLocation:
			d.SetString("%Y-%m-%d")
		case eurLocation:
			d.SetString("%d.%m.%Y")
		case internalLocation:
			d.SetString("%Y%m%d")
		}
	case datetimeFormat:
		switch l {
		case usaLocation:
			d.SetString("%Y-%m-%d %H.%i.%s")
		case jisLocation:
			d.SetString("%Y-%m-%d %H:%i:%s")
		case isoLocation:
			d.SetString("%Y-%m-%d %H:%i:%s")
		case eurLocation:
			d.SetString("%Y-%m-%d %H.%i.%s")
		case internalLocation:
			d.SetString("%Y%m%d%H%i%s")
		}
	case timeFormat:
		switch l {
		case usaLocation:
			d.SetString("%h:%i:%s %p")
		case jisLocation:
			d.SetString("%H:%i:%s")
		case isoLocation:
			d.SetString("%H:%i:%s")
		case eurLocation:
			d.SetString("%H.%i.%s")
		case internalLocation:
			d.SetString("%H%i%s")
		}
	}

	return d, nil
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

// eval evals a builtinStrToDateSig.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_str-to-date
func (b *builtinStrToDateSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
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

// eval evals a builtinSysDateSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sysdate
func (b *builtinSysDateSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// SYSDATE is not the same as NOW if NOW is used in a stored function or trigger.
	// But here we can just think they are the same because we don't support stored function
	// and trigger now.
	return builtinNow(args, b.ctx)
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

// eval evals a builtinCurrentDateSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_curdate
func (b *builtinCurrentDateSig) eval(_ []types.Datum) (d types.Datum, err error) {
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

// eval evals a builtinCurrentTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_curtime
func (b *builtinCurrentTimeSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	fsp := 0
	sc := b.ctx.GetSessionVars().StmtCtx
	if len(args) == 1 && !args[0].IsNull() {
		if fsp, err = checkFsp(sc, args[0]); err != nil {
			d.SetNull()
			return d, errors.Trace(err)
		}
	}
	d.SetString(time.Now().Format("15:04:05.000000"))
	return convertToDuration(b.ctx.GetSessionVars().StmtCtx, d, fsp)
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

// eval evals a builtinTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time
func (b *builtinTimeSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
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
	sc := b.ctx.GetSessionVars().StmtCtx
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

// eval evals a builtinUTCDateSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-date
func (b *builtinUTCDateSig) eval(_ []types.Datum) (d types.Datum, err error) {
	year, month, day := time.Now().UTC().Date()
	t := types.Time{
		Time: types.FromGoTime(time.Date(year, month, day, 0, 0, 0, 0, time.UTC)),
		Type: mysql.TypeDate, Fsp: types.UnspecifiedFsp}
	d.SetMysqlTime(t)
	return d, nil
}

type utcTimestampFunctionClass struct {
	baseFunctionClass
}

func (c *utcTimestampFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinUTCTimestampSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinUTCTimestampSig struct {
	baseBuiltinFunc
}

// eval evals a builtinUTCTimestampSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}

	fsp := 0
	sc := b.ctx.GetSessionVars().StmtCtx
	if len(args) == 1 && !args[0].IsNull() {
		if fsp, err = checkFsp(sc, args[0]); err != nil {
			return d, errors.Trace(err)
		}
	}

	t, err := convertTimeToMysqlTime(time.Now().UTC(), fsp)
	if err != nil {
		return d, errors.Trace(err)
	}

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

// eval evals a builtinExtractSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func (b *builtinExtractSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	unit := args[0].GetString()
	vd := args[1]

	if vd.IsNull() {
		d.SetNull()
		return d, nil
	}

	f := types.NewFieldType(mysql.TypeDatetime)
	f.Decimal = types.MaxFsp
	val, err := vd.ConvertTo(b.ctx.GetSessionVars().StmtCtx, f)
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

func (b *builtinDateArithSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
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
	sc := b.ctx.GetSessionVars().StmtCtx
	if types.IsClockUnit(nodeIntervalUnit) {
		fieldType = mysql.TypeDatetime
	}
	resultField = types.NewFieldType(fieldType)
	resultField.Decimal = types.MaxFsp
	value, err := nodeDate.ConvertTo(b.ctx.GetSessionVars().StmtCtx, resultField)
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
	if b.op == ast.DateArithSub {
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

type timestampDiffFunctionClass struct {
	baseFunctionClass
}

func (c *timestampDiffFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinTimestampDiffSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinTimestampDiffSig struct {
	baseBuiltinFunc
}

// eval evals a builtinTimestampDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampdiff
func (b *builtinTimestampDiffSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[1].IsNull() || args[2].IsNull() {
		return d, nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	t1, err := convertDatumToTime(sc, args[1])
	if err != nil {
		return d, errorOrWarning(err, b.ctx)
	}
	t2, err := convertDatumToTime(sc, args[2])
	if err != nil {
		return d, errorOrWarning(err, b.ctx)
	}
	if t1.InvalidZero() || t2.InvalidZero() {
		return d, errorOrWarning(types.ErrInvalidTimeFormat, b.ctx)
	}

	v := types.TimestampDiff(args[0].GetString(), t1, t2)
	d.SetInt64(v)
	return
}

// errorOrWarning reports error or warning depend on the context.
func errorOrWarning(err error, ctx context.Context) error {
	sc := ctx.GetSessionVars().StmtCtx
	// TODO: Use better name, such as sc.IsInsert instead of sc.IgnoreTruncate.
	if ctx.GetSessionVars().StrictSQLMode && !sc.IgnoreTruncate {
		return errors.Trace(types.ErrInvalidTimeFormat)
	}
	sc.AppendWarning(err)
	return nil
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

// eval evals a builtinUnixTimestampSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
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
	case types.KindMysqlTime:
		t = args[0].GetMysqlTime()
	case types.KindNull:
		return
	default:
		return d, errors.Errorf("Unknown args type for unix_timestamp %d", args[0].Kind())
	}

	t1, err = t.Time.GoTime(getTimeZone(b.ctx))
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

type timestampFunctionClass struct {
	baseFunctionClass
}

func (c *timestampFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinTimestampSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinTimestampSig struct {
	baseBuiltinFunc
}

// eval evals a builtinTimestampSig.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_timestamp
func (b *builtinTimestampSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() {
		return
	}
	var arg0 types.Time
	switch tp := args[0].Kind(); tp {
	case types.KindInt64, types.KindUint64:
		arg0, err = types.ParseDatetimeFromNum(args[0].GetInt64())
		if err != nil {
			return d, errors.Trace(err)
		}
	case types.KindString, types.KindBytes, types.KindMysqlDecimal, types.KindFloat32, types.KindFloat64:
		s, err1 := args[0].ToString()
		if err1 != nil {
			return d, errors.Trace(err1)
		}
		arg0, err = types.ParseTime(s, mysql.TypeDatetime, getFsp(s))
		if err != nil {
			return d, errors.Trace(err)
		}
	case types.KindMysqlTime:
		arg0 = args[0].GetMysqlTime()
	default:
		return d, errors.Errorf("Unknown args type for timestamp %d", tp)
	}
	if len(args) == 1 {
		d.SetMysqlTime(arg0)
		return
	}
	// If exists args[1].
	s, err := args[1].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	arg1, err := types.ParseDuration(s, getFsp(s))
	if err != nil {
		return d, errors.Trace(err)
	}
	tmpDuration := arg0.Add(&arg1)
	result, err := tmpDuration.ConvertToTime(arg0.Type)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetMysqlTime(result)
	return
}

func getFsp(s string) (fsp int) {
	fsp = len(s) - strings.Index(s, ".") - 1
	if fsp == len(s) {
		fsp = 0
	} else if fsp > 6 {
		fsp = 6
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

type addTimeFunctionClass struct {
	baseFunctionClass
}

func (c *addTimeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinAddTimeSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinAddTimeSig struct {
	baseBuiltinFunc
}

// eval evals a builtinAddTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("ADDTIME")
}

type convertTzFunctionClass struct {
	baseFunctionClass
}

func (c *convertTzFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinConvertTzSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinConvertTzSig struct {
	baseBuiltinFunc
}

// eval evals a builtinConvertTzSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_convert-tz
func (b *builtinConvertTzSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("CONVERT_TZ")
}

type makeDateFunctionClass struct {
	baseFunctionClass
}

func (c *makeDateFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinMakeDateSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinMakeDateSig struct {
	baseBuiltinFunc
}

// eval evals a builtinMakeDateSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_makedate
func (b *builtinMakeDateSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() || args[1].IsNull() {
		return d, nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	year, err := args[0].ToInt64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	dayOfYear, err := args[1].ToInt64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	if dayOfYear <= 0 || year < 0 || year > 9999 {
		return d, nil
	}
	if year < 70 {
		year += 2000
	} else if year < 100 {
		year += 1900
	}
	startTime := types.Time{
		Time: types.FromDate(int(year), 1, 1, 0, 0, 0, 0),
		Type: mysql.TypeDate,
		Fsp:  0,
	}
	retTimestamp := types.TimestampDiff("DAY", types.ZeroDate, startTime)
	if retTimestamp == 0 {
		return d, errorOrWarning(types.ErrInvalidTimeFormat, b.ctx)
	}
	ret := types.TimeFromDays(retTimestamp + dayOfYear - 1)
	if ret.IsZero() || ret.Time.Year() > 9999 {
		return d, nil
	}
	d.SetMysqlTime(ret)
	return d, nil
}

type makeTimeFunctionClass struct {
	baseFunctionClass
}

func (c *makeTimeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinMakeTimeSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinMakeTimeSig struct {
	baseBuiltinFunc
}

// eval evals a builtinMakeTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_maketime
func (b *builtinMakeTimeSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	// MAKETIME(hour, minute, second)
	if args[0].IsNull() || args[1].IsNull() || args[2].IsNull() {
		return d, nil
	}

	var (
		hour     int64
		minute   int64
		second   float64
		overflow bool
	)
	sc := b.ctx.GetSessionVars().StmtCtx
	hour, _ = args[0].ToInt64(sc)
	// MySQL TIME datatype: https://dev.mysql.com/doc/refman/5.7/en/time.html
	// ranges from '-838:59:59.000000' to '838:59:59.000000'
	if hour < -838 {
		hour = -838
		overflow = true
	} else if hour > 838 {
		hour = 838
		overflow = true
	}

	minute, _ = args[1].ToInt64(sc)
	if minute < 0 || minute >= 60 {
		return d, nil
	}

	second, _ = args[2].ToFloat64(sc)
	if second < 0 || second >= 60 {
		return d, nil
	}
	if hour == -838 || hour == 838 {
		if second > 59 {
			second = 59
		}
	}
	if overflow {
		minute = 59
		second = 59
	}

	var dur types.Duration
	fsp := types.MaxFsp
	if args[2].Kind() != types.KindString {
		sec, _ := args[2].ToString()
		secs := strings.Split(sec, ".")
		if len(secs) <= 1 {
			fsp = 0
		} else if len(secs[1]) < fsp {
			fsp = len(secs[1])
		}
	}
	dur, err = types.ParseDuration(fmt.Sprintf("%02d:%02d:%v", hour, minute, second), fsp)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetMysqlDuration(dur)
	return d, nil
}

type periodAddFunctionClass struct {
	baseFunctionClass
}

func (c *periodAddFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinPeriodAddSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinPeriodAddSig struct {
	baseBuiltinFunc
}

// eval evals a builtinPeriodAddSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-add
func (b *builtinPeriodAddSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}

	if args[0].IsNull() || args[1].IsNull() {
		return d, errors.Trace(err)
	}

	sc := b.ctx.GetSessionVars().StmtCtx
	period, err := args[0].ToInt64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	//Check zero
	if period <= 0 {
		d.SetInt64(0)
		return d, nil
	}

	y := period / 100
	m := period % 100

	// YYMM, 00-69 year: 2000-2069
	// YYMM, 70-99 year: 1970-1999
	if y <= 69 {
		y += 2000
	} else if y <= 99 {
		y += 1900
	}

	months, err := args[1].ToInt64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	sum := time.Month(m + months)
	// TODO: Consider time_zone variable.
	t := time.Date(int(y), sum, 1, 0, 0, 0, 0, time.Local)

	ret := int64(t.Year())*100 + int64(t.Month())
	d.SetInt64(ret)
	return d, nil
}

type periodDiffFunctionClass struct {
	baseFunctionClass
}

func (c *periodDiffFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinPeriodDiffSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinPeriodDiffSig struct {
	baseBuiltinFunc
}

// eval evals a builtinPeriodDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-diff
func (b *builtinPeriodDiffSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("PERIOD_DIFF")
}

type quarterFunctionClass struct {
	baseFunctionClass
}

func (c *quarterFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinQuarterSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinQuarterSig struct {
	baseBuiltinFunc
}

// eval evals a builtinQuarterSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_quarter
func (b *builtinQuarterSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}

	d, err = builtinMonth(args, b.ctx)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	mon := int(d.GetInt64())
	switch mon {
	case 0:
		// An undocumented behavior of the mysql implementation
		d.SetInt64(0)
	case 1, 2, 3:
		d.SetInt64(1)
	case 4, 5, 6:
		d.SetInt64(2)
	case 7, 8, 9:
		d.SetInt64(3)
	case 10, 11, 12:
		d.SetInt64(4)
	default:
		d.SetNull()
		return d, errors.Errorf("no quarter for invalid month: %d.", mon)
	}

	return d, nil
}

type secToTimeFunctionClass struct {
	baseFunctionClass
}

func (c *secToTimeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinSecToTimeSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinSecToTimeSig struct {
	baseBuiltinFunc
}

// eval evals a builtinSecToTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sec-to-time
func (b *builtinSecToTimeSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() {
		return d, nil
	}

	var (
		hour          int64
		minute        int64
		second        int64
		demical       float64
		secondDemical float64
		negative      string
	)

	sc := b.ctx.GetSessionVars().StmtCtx
	secondsFloat, err := args[0].ToFloat64(sc)
	if err != nil {
		if args[0].Kind() == types.KindString && terror.ErrorEqual(err, types.ErrTruncated) {
			secondsFloat = float64(0)
		} else {
			return d, errors.Trace(err)
		}
	}

	if secondsFloat < 0 {
		negative = "-"
		secondsFloat = math.Abs(secondsFloat)
	}
	seconds := int64(secondsFloat)
	demical = secondsFloat - float64(seconds)

	hour = seconds / 3600
	if hour > 838 {
		hour = 838
		minute = 59
		second = 59
	} else {
		minute = seconds % 3600 / 60
		second = seconds % 60
	}
	secondDemical = float64(second) + demical

	var dur types.Duration
	fsp := types.MaxFsp
	if args[0].Kind() != types.KindString {
		sec, _ := args[0].ToString()
		secs := strings.Split(sec, ".")
		if len(secs) <= 1 {
			fsp = 0
		} else if len(secs[1]) < fsp {
			fsp = len(secs[1])
		}
	}
	dur, err = types.ParseDuration(fmt.Sprintf("%s%02d:%02d:%v", negative, hour, minute, secondDemical), fsp)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetMysqlDuration(dur)
	return d, nil
}

type subTimeFunctionClass struct {
	baseFunctionClass
}

func (c *subTimeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinSubTimeSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinSubTimeSig struct {
	baseBuiltinFunc
}

// eval evals a builtinSubTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("SUB_TIME")
}

type timeFormatFunctionClass struct {
	baseFunctionClass
}

func (c *timeFormatFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinTimeFormatSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinTimeFormatSig struct {
	baseBuiltinFunc
}

// eval evals a builtinTimeFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-format
func (b *builtinTimeFormatSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("TIME_FORMAT")
}

type timeToSecFunctionClass struct {
	baseFunctionClass
}

func (c *timeToSecFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinTimeToSecSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinTimeToSecSig struct {
	baseBuiltinFunc
}

// eval evals a builtinTimeToSecSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-to-sec
func (b *builtinTimeToSecSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}

	d, err = convertToDuration(b.ctx.GetSessionVars().StmtCtx, args[0], 0)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	t := d.GetMysqlDuration()
	// TODO: select TIME_TO_SEC('-2:-2:-2') not handle well.
	if t.Compare(types.ZeroDuration) < 0 {
		d.SetInt64(int64(-1 * (t.Hour()*3600 + t.Minute()*60 + t.Second())))
	} else {
		d.SetInt64(int64(t.Hour()*3600 + t.Minute()*60 + t.Second()))
	}

	return d, nil
}

type timestampAddFunctionClass struct {
	baseFunctionClass
}

func (c *timestampAddFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinTimestampAddSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinTimestampAddSig struct {
	baseBuiltinFunc
}

// eval evals a builtinTimestampAddSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampadd
func (b *builtinTimestampAddSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	unit, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[1].IsNull() || args[2].IsNull() {
		return d, nil
	}
	v := args[1].GetInt64()
	sc := b.ctx.GetSessionVars().StmtCtx
	date, err := convertDatumToTime(sc, args[2])
	if err != nil {
		return d, errorOrWarning(err, b.ctx)
	}
	tm, err := date.Time.GoTime(time.Local)
	if err != nil {
		return d, errors.Trace(err)
	}
	var tb time.Time
	fsp := types.DefaultFsp
	switch unit {
	case "MICROSECOND":
		tb = tm.Add(time.Duration(v) * time.Microsecond)
		fsp = types.MaxFsp
	case "SECOND":
		tb = tm.Add(time.Duration(v) * time.Second)
	case "MINUTE":
		tb = tm.Add(time.Duration(v) * time.Minute)
	case "HOUR":
		tb = tm.Add(time.Duration(v) * time.Hour)
	case "DAY":
		tb = tm.AddDate(0, 0, int(v))
	case "WEEK":
		tb = tm.AddDate(0, 0, 7*int(v))
	case "MONTH":
		tb = tm.AddDate(0, int(v), 0)
	case "QUARTER":
		tb = tm.AddDate(0, 3*int(v), 0)
	case "YEAR":
		tb = tm.AddDate(int(v), 0, 0)
	default:
		return d, errors.Trace(types.ErrInvalidTimeFormat)
	}
	r := types.Time{Time: types.FromGoTime(tb), Type: mysql.TypeDatetime, Fsp: fsp}
	err = r.Check()
	if err != nil {
		return d, errorOrWarning(err, b.ctx)
	}
	d.SetMysqlTime(r)
	return d, nil
}

type toDaysFunctionClass struct {
	baseFunctionClass
}

func (c *toDaysFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinToDaysSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinToDaysSig struct {
	baseBuiltinFunc
}

// eval evals a builtinToDaysSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-days
func (b *builtinToDaysSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() {
		return d, nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	date, err := convertDatumToTime(sc, args[0])
	if err != nil {
		return d, errorOrWarning(err, b.ctx)
	}
	ret := types.TimestampDiff("DAY", types.ZeroDate, date)
	if ret == 0 {
		return d, errorOrWarning(types.ErrInvalidTimeFormat, b.ctx)
	}
	d.SetInt64(ret)
	return d, nil
}

type toSecondsFunctionClass struct {
	baseFunctionClass
}

func (c *toSecondsFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinToSecondsSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinToSecondsSig struct {
	baseBuiltinFunc
}

// eval evals a builtinToSecondsSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-seconds
func (b *builtinToSecondsSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() {
		return d, nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	date, err := convertDatumToTime(sc, args[0])
	if err != nil {
		return d, errorOrWarning(err, b.ctx)
	}
	ret := types.TimestampDiff("SECOND", types.ZeroDate, date)
	if ret == 0 {
		return d, errorOrWarning(types.ErrInvalidTimeFormat, b.ctx)
	}
	d.SetInt64(ret)
	return d, nil
}

type utcTimeFunctionClass struct {
	baseFunctionClass
}

func (c *utcTimeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinUTCTimeSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinUTCTimeSig struct {
	baseBuiltinFunc
}

// eval evals a builtinUTCTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeSig) eval(row []types.Datum) (d types.Datum, err error) {
	const utctimeFormat = "15:04:05.000000"
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	fsp := 0
	sc := b.ctx.GetSessionVars().StmtCtx
	if len(args) == 1 && !args[0].IsNull() {
		if fsp, err = checkFsp(sc, args[0]); err != nil {
			d.SetNull()
			return d, errors.Trace(err)
		}
	}
	d.SetString(time.Now().UTC().Format(utctimeFormat))
	return convertToDuration(b.ctx.GetSessionVars().StmtCtx, d, fsp)
}
