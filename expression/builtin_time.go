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
	"strconv"
	"strings"
	"time"

	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
	log "github.com/sirupsen/logrus"
)

const ( // GET_FORMAT first argument.
	dateFormat      = "DATE"
	datetimeFormat  = "DATETIME"
	timestampFormat = "TIMESTAMP"
	timeFormat      = "TIME"
)

const ( // GET_FORMAT location.
	usaLocation      = "USA"
	jisLocation      = "JIS"
	isoLocation      = "ISO"
	eurLocation      = "EUR"
	internalLocation = "INTERNAL"
)

var (
	// durationPattern checks whether a string matchs the format of duration.
	durationPattern = regexp.MustCompile(`^\s*[-]?(((\d{1,2}\s+)?0*\d{0,3}(:0*\d{1,2}){0,2})|(\d{1,7}))?(\.\d*)?\s*$`)

	// timestampPattern checks whether a string matchs the format of timestamp.
	timestampPattern = regexp.MustCompile(`^\s*0*\d{1,4}([^\d]0*\d{1,2}){2}\s+(0*\d{0,2}([^\d]0*\d{1,2}){2})?(\.\d*)?\s*$`)

	// datePattern determine whether to match the format of date.
	datePattern = regexp.MustCompile(`^\s*((0*\d{1,4}([^\d]0*\d{1,2}){2})|(\d{2,4}(\d{2}){2}))\s*$`)
)

var (
	_ functionClass = &dateFunctionClass{}
	_ functionClass = &dateLiteralFunctionClass{}
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
	_ functionClass = &timeLiteralFunctionClass{}
	_ functionClass = &utcDateFunctionClass{}
	_ functionClass = &utcTimestampFunctionClass{}
	_ functionClass = &extractFunctionClass{}
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
	_ functionClass = &timestampLiteralFunctionClass{}
	_ functionClass = &lastDayFunctionClass{}
	_ functionClass = &addDateFunctionClass{}
	_ functionClass = &subDateFunctionClass{}
)

var (
	_ builtinFunc = &builtinDateSig{}
	_ builtinFunc = &builtinDateLiteralSig{}
	_ builtinFunc = &builtinDateDiffSig{}
	_ builtinFunc = &builtinNullTimeDiffSig{}
	_ builtinFunc = &builtinTimeStringTimeDiffSig{}
	_ builtinFunc = &builtinDurationStringTimeDiffSig{}
	_ builtinFunc = &builtinDurationDurationTimeDiffSig{}
	_ builtinFunc = &builtinStringTimeTimeDiffSig{}
	_ builtinFunc = &builtinStringDurationTimeDiffSig{}
	_ builtinFunc = &builtinStringStringTimeDiffSig{}
	_ builtinFunc = &builtinTimeTimeTimeDiffSig{}
	_ builtinFunc = &builtinDateFormatSig{}
	_ builtinFunc = &builtinHourSig{}
	_ builtinFunc = &builtinMinuteSig{}
	_ builtinFunc = &builtinSecondSig{}
	_ builtinFunc = &builtinMicroSecondSig{}
	_ builtinFunc = &builtinMonthSig{}
	_ builtinFunc = &builtinMonthNameSig{}
	_ builtinFunc = &builtinNowWithArgSig{}
	_ builtinFunc = &builtinNowWithoutArgSig{}
	_ builtinFunc = &builtinDayNameSig{}
	_ builtinFunc = &builtinDayOfMonthSig{}
	_ builtinFunc = &builtinDayOfWeekSig{}
	_ builtinFunc = &builtinDayOfYearSig{}
	_ builtinFunc = &builtinWeekWithModeSig{}
	_ builtinFunc = &builtinWeekWithoutModeSig{}
	_ builtinFunc = &builtinWeekDaySig{}
	_ builtinFunc = &builtinWeekOfYearSig{}
	_ builtinFunc = &builtinYearSig{}
	_ builtinFunc = &builtinYearWeekWithModeSig{}
	_ builtinFunc = &builtinYearWeekWithoutModeSig{}
	_ builtinFunc = &builtinGetFormatSig{}
	_ builtinFunc = &builtinSysDateWithFspSig{}
	_ builtinFunc = &builtinSysDateWithoutFspSig{}
	_ builtinFunc = &builtinCurrentDateSig{}
	_ builtinFunc = &builtinCurrentTime0ArgSig{}
	_ builtinFunc = &builtinCurrentTime1ArgSig{}
	_ builtinFunc = &builtinTimeSig{}
	_ builtinFunc = &builtinTimeLiteralSig{}
	_ builtinFunc = &builtinUTCDateSig{}
	_ builtinFunc = &builtinUTCTimestampWithArgSig{}
	_ builtinFunc = &builtinUTCTimestampWithoutArgSig{}
	_ builtinFunc = &builtinAddDatetimeAndDurationSig{}
	_ builtinFunc = &builtinAddDatetimeAndStringSig{}
	_ builtinFunc = &builtinAddTimeDateTimeNullSig{}
	_ builtinFunc = &builtinAddStringAndDurationSig{}
	_ builtinFunc = &builtinAddStringAndStringSig{}
	_ builtinFunc = &builtinAddTimeStringNullSig{}
	_ builtinFunc = &builtinAddDurationAndDurationSig{}
	_ builtinFunc = &builtinAddDurationAndStringSig{}
	_ builtinFunc = &builtinAddTimeDurationNullSig{}
	_ builtinFunc = &builtinAddDateAndDurationSig{}
	_ builtinFunc = &builtinAddDateAndStringSig{}
	_ builtinFunc = &builtinSubDatetimeAndDurationSig{}
	_ builtinFunc = &builtinSubDatetimeAndStringSig{}
	_ builtinFunc = &builtinSubTimeDateTimeNullSig{}
	_ builtinFunc = &builtinSubStringAndDurationSig{}
	_ builtinFunc = &builtinSubStringAndStringSig{}
	_ builtinFunc = &builtinSubTimeStringNullSig{}
	_ builtinFunc = &builtinSubDurationAndDurationSig{}
	_ builtinFunc = &builtinSubDurationAndStringSig{}
	_ builtinFunc = &builtinSubTimeDurationNullSig{}
	_ builtinFunc = &builtinSubDateAndDurationSig{}
	_ builtinFunc = &builtinSubDateAndStringSig{}
	_ builtinFunc = &builtinUnixTimestampCurrentSig{}
	_ builtinFunc = &builtinUnixTimestampIntSig{}
	_ builtinFunc = &builtinUnixTimestampDecSig{}
	_ builtinFunc = &builtinConvertTzSig{}
	_ builtinFunc = &builtinMakeDateSig{}
	_ builtinFunc = &builtinMakeTimeSig{}
	_ builtinFunc = &builtinPeriodAddSig{}
	_ builtinFunc = &builtinPeriodDiffSig{}
	_ builtinFunc = &builtinQuarterSig{}
	_ builtinFunc = &builtinSecToTimeSig{}
	_ builtinFunc = &builtinTimeToSecSig{}
	_ builtinFunc = &builtinTimestampAddSig{}
	_ builtinFunc = &builtinToDaysSig{}
	_ builtinFunc = &builtinToSecondsSig{}
	_ builtinFunc = &builtinUTCTimeWithArgSig{}
	_ builtinFunc = &builtinUTCTimeWithoutArgSig{}
	_ builtinFunc = &builtinTimestamp1ArgSig{}
	_ builtinFunc = &builtinTimestamp2ArgsSig{}
	_ builtinFunc = &builtinTimestampLiteralSig{}
	_ builtinFunc = &builtinLastDaySig{}
	_ builtinFunc = &builtinStrToDateDateSig{}
	_ builtinFunc = &builtinStrToDateDatetimeSig{}
	_ builtinFunc = &builtinStrToDateDurationSig{}
	_ builtinFunc = &builtinFromUnixTime1ArgSig{}
	_ builtinFunc = &builtinFromUnixTime2ArgSig{}
	_ builtinFunc = &builtinExtractDatetimeSig{}
	_ builtinFunc = &builtinExtractDurationSig{}
	_ builtinFunc = &builtinAddDateStringStringSig{}
	_ builtinFunc = &builtinAddDateStringIntSig{}
	_ builtinFunc = &builtinAddDateIntStringSig{}
	_ builtinFunc = &builtinAddDateIntIntSig{}
	_ builtinFunc = &builtinAddDateDatetimeStringSig{}
	_ builtinFunc = &builtinAddDateDatetimeIntSig{}
	_ builtinFunc = &builtinSubDateStringStringSig{}
	_ builtinFunc = &builtinSubDateStringIntSig{}
	_ builtinFunc = &builtinSubDateIntStringSig{}
	_ builtinFunc = &builtinSubDateIntIntSig{}
	_ builtinFunc = &builtinSubDateDatetimeStringSig{}
	_ builtinFunc = &builtinSubDateDatetimeIntSig{}
)

func convertTimeToMysqlTime(t time.Time, fsp int) (types.Time, error) {
	tr, err := types.RoundFrac(t, fsp)
	if err != nil {
		return types.Time{}, errors.Trace(err)
	}

	return types.Time{
		Time: types.FromGoTime(tr),
		Type: mysql.TypeDatetime,
		Fsp:  fsp,
	}, nil
}

type dateFunctionClass struct {
	baseFunctionClass
}

func (c *dateFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETDatetime)
	bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, 10, 0
	sig := &builtinDateSig{bf}
	return sig, nil
}

type builtinDateSig struct {
	baseBuiltinFunc
}

// evalTime evals DATE(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date
func (b *builtinDateSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	expr, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}

	if expr.IsZero() {
		return types.Time{}, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(expr.String())))
	}

	expr.Time = types.FromDate(expr.Time.Year(), expr.Time.Month(), expr.Time.Day(), 0, 0, 0, 0)
	expr.Type = mysql.TypeDate
	return expr, false, nil
}

type dateLiteralFunctionClass struct {
	baseFunctionClass
}

func (c *dateLiteralFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	con, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for date literal")
	}
	dt, err := con.Eval(nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	str := dt.GetString()
	if !datePattern.MatchString(str) {
		return nil, types.ErrIncorrectDatetimeValue.GenByArgs(str)
	}
	tm, err := types.ParseDate(ctx.GetSessionVars().StmtCtx, str)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, []Expression{}, types.ETDatetime)
	bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, 10, 0
	sig := &builtinDateLiteralSig{bf, tm}
	return sig, nil
}

type builtinDateLiteralSig struct {
	baseBuiltinFunc
	literal types.Time
}

// evalTime evals DATE 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinDateLiteralSig) evalTime(row types.Row) (types.Time, bool, error) {
	mode := b.getCtx().GetSessionVars().SQLMode
	if mode.HasNoZeroDateMode() && b.literal.IsZero() {
		return b.literal, true, types.ErrIncorrectDatetimeValue.GenByArgs(b.literal.String())
	}
	if mode.HasNoZeroInDateMode() && (b.literal.InvalidZero() && !b.literal.IsZero()) {
		return b.literal, true, types.ErrIncorrectDatetimeValue.GenByArgs(b.literal.String())
	}
	return b.literal, false, nil
}

type dateDiffFunctionClass struct {
	baseFunctionClass
}

func (c *dateDiffFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime, types.ETDatetime)
	sig := &builtinDateDiffSig{bf}
	return sig, nil
}

type builtinDateDiffSig struct {
	baseBuiltinFunc
}

// evalInt evals a builtinDateDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_datediff
func (b *builtinDateDiffSig) evalInt(row types.Row) (int64, bool, error) {
	ctx := b.ctx.GetSessionVars().StmtCtx
	lhs, isNull, err := b.args[0].EvalTime(row, ctx)
	if isNull || err != nil {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	rhs, isNull, err := b.args[1].EvalTime(row, ctx)
	if isNull || err != nil {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	if invalidLHS, invalidRHS := lhs.InvalidZero(), rhs.InvalidZero(); invalidLHS || invalidRHS {
		if invalidLHS {
			err = handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(lhs.String()))
		}
		if invalidRHS {
			err = handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(rhs.String()))
		}
		return 0, true, errors.Trace(err)
	}
	return int64(types.DateDiff(lhs.Time, rhs.Time)), false, nil
}

type timeDiffFunctionClass struct {
	baseFunctionClass
}

func (c *timeDiffFunctionClass) getArgEvalTp(fieldTp *types.FieldType) types.EvalType {
	argTp := types.ETString
	switch tp := fieldTp.EvalType(); tp {
	case types.ETDuration, types.ETDatetime, types.ETTimestamp:
		argTp = tp
	}
	return argTp
}

func (c *timeDiffFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	arg0FieldTp, arg1FieldTp := args[0].GetType(), args[1].GetType()
	arg0Tp, arg1Tp := c.getArgEvalTp(arg0FieldTp), c.getArgEvalTp(arg1FieldTp)
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, arg0Tp, arg1Tp)
	bf.tp.Decimal = mathutil.Max(arg0FieldTp.Decimal, arg1FieldTp.Decimal)

	var sig builtinFunc
	// arg0 and arg1 must be the same time type(compatible), or timediff will return NULL.
	// TODO: we don't really need Duration type, actually in MySQL, it use Time class to represent
	// all the time type, and use filed type to distinguish datetime, date, timestamp or time(duration).
	// With the duration type, we are hard to port all the MySQL behavior.
	switch arg0Tp {
	case types.ETDuration:
		switch arg1Tp {
		case types.ETDuration:
			sig = &builtinDurationDurationTimeDiffSig{bf}
		case types.ETDatetime, types.ETTimestamp:
			sig = &builtinNullTimeDiffSig{bf}
		default:
			sig = &builtinDurationStringTimeDiffSig{bf}
		}
	case types.ETDatetime, types.ETTimestamp:
		switch arg1Tp {
		case types.ETDuration:
			sig = &builtinNullTimeDiffSig{bf}
		case types.ETDatetime, types.ETTimestamp:
			sig = &builtinTimeTimeTimeDiffSig{bf}
		default:
			sig = &builtinTimeStringTimeDiffSig{bf}
		}
	default:
		switch arg1Tp {
		case types.ETDuration:
			sig = &builtinStringDurationTimeDiffSig{bf}
		case types.ETDatetime, types.ETTimestamp:
			sig = &builtinStringTimeTimeDiffSig{bf}
		default:
			sig = &builtinStringStringTimeDiffSig{bf}
		}
	}
	return sig, nil
}

type builtinDurationDurationTimeDiffSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinDurationDurationTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinDurationDurationTimeDiffSig) evalDuration(row types.Row) (d types.Duration, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	lhs, isNull, err := b.args[0].EvalDuration(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	rhs, isNull, err := b.args[1].EvalDuration(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	d, isNull, err = calculateDurationTimeDiff(sc, lhs, rhs)
	return d, isNull, errors.Trace(err)
}

type builtinTimeTimeTimeDiffSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinTimeTimeTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinTimeTimeTimeDiffSig) evalDuration(row types.Row) (d types.Duration, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	lhs, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	rhs, isNull, err := b.args[1].EvalTime(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	d, isNull, err = calculateTimeDiff(sc, lhs, rhs)
	return d, isNull, errors.Trace(err)
}

type builtinDurationStringTimeDiffSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinDurationStringTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinDurationStringTimeDiffSig) evalDuration(row types.Row) (d types.Duration, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	lhs, isNull, err := b.args[0].EvalDuration(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	rhsStr, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	rhs, _, isDuration, err := convertStringToDuration(sc, rhsStr, b.tp.Decimal)
	if err != nil || !isDuration {
		return d, true, errors.Trace(err)
	}

	d, isNull, err = calculateDurationTimeDiff(sc, lhs, rhs)
	return d, isNull, errors.Trace(err)
}

type builtinStringDurationTimeDiffSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinStringDurationTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinStringDurationTimeDiffSig) evalDuration(row types.Row) (d types.Duration, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	lhsStr, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	rhs, isNull, err := b.args[1].EvalDuration(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	lhs, _, isDuration, err := convertStringToDuration(sc, lhsStr, b.tp.Decimal)
	if err != nil || !isDuration {
		return d, true, errors.Trace(err)
	}

	d, isNull, err = calculateDurationTimeDiff(sc, lhs, rhs)
	return d, isNull, errors.Trace(err)
}

// calculateTimeDiff calculates interval difference of two types.Time.
func calculateTimeDiff(sc *stmtctx.StatementContext, lhs, rhs types.Time) (d types.Duration, isNull bool, err error) {
	d = lhs.Sub(&rhs)
	d.Duration, err = types.TruncateOverflowMySQLTime(d.Duration)
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = sc.HandleTruncate(err)
	}
	return d, err != nil, errors.Trace(err)
}

// calculateTimeDiff calculates interval difference of two types.Duration.
func calculateDurationTimeDiff(sc *stmtctx.StatementContext, lhs, rhs types.Duration) (d types.Duration, isNull bool, err error) {
	d, err = lhs.Sub(rhs)
	if err != nil {
		return d, true, errors.Trace(err)
	}

	d.Duration, err = types.TruncateOverflowMySQLTime(d.Duration)
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = sc.HandleTruncate(err)
	}
	return d, err != nil, errors.Trace(err)
}

type builtinTimeStringTimeDiffSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinTimeStringTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinTimeStringTimeDiffSig) evalDuration(row types.Row) (d types.Duration, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	lhs, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	rhsStr, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	_, rhs, isDuration, err := convertStringToDuration(sc, rhsStr, b.tp.Decimal)
	if err != nil || isDuration {
		return d, true, errors.Trace(err)
	}

	d, isNull, err = calculateTimeDiff(sc, lhs, rhs)
	return d, isNull, errors.Trace(err)
}

type builtinStringTimeTimeDiffSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinStringTimeTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinStringTimeTimeDiffSig) evalDuration(row types.Row) (d types.Duration, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	lhsStr, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	rhs, isNull, err := b.args[1].EvalTime(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	_, lhs, isDuration, err := convertStringToDuration(sc, lhsStr, b.tp.Decimal)
	if err != nil || isDuration {
		return d, true, errors.Trace(err)
	}

	d, isNull, err = calculateTimeDiff(sc, lhs, rhs)
	return d, isNull, errors.Trace(err)
}

type builtinStringStringTimeDiffSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinStringStringTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinStringStringTimeDiffSig) evalDuration(row types.Row) (d types.Duration, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	lhs, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	rhs, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	fsp := b.tp.Decimal
	lhsDur, lhsTime, lhsIsDuration, err := convertStringToDuration(sc, lhs, fsp)
	if err != nil {
		return d, true, errors.Trace(err)
	}

	rhsDur, rhsTime, rhsIsDuration, err := convertStringToDuration(sc, rhs, fsp)
	if err != nil {
		return d, true, errors.Trace(err)
	}

	if lhsIsDuration != rhsIsDuration {
		return d, true, nil
	}

	if lhsIsDuration {
		d, isNull, err = calculateDurationTimeDiff(sc, lhsDur, rhsDur)
	} else {
		d, isNull, err = calculateTimeDiff(sc, lhsTime, rhsTime)
	}

	return d, isNull, errors.Trace(err)
}

type builtinNullTimeDiffSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinNullTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinNullTimeDiffSig) evalDuration(row types.Row) (d types.Duration, isNull bool, err error) {
	return d, true, nil
}

// convertStringToDuration converts string to duration, it return types.Time because in some case
// it will converts string to datetime.
func convertStringToDuration(sc *stmtctx.StatementContext, str string, fsp int) (d types.Duration, t types.Time,
	isDuration bool, err error) {
	if n := strings.IndexByte(str, '.'); n >= 0 {
		lenStrFsp := len(str[n+1:])
		if lenStrFsp <= types.MaxFsp {
			fsp = mathutil.Max(lenStrFsp, fsp)
		}
	}
	return types.StrToDuration(sc, str, fsp)
}

type dateFormatFunctionClass struct {
	baseFunctionClass
}

func (c *dateFormatFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETDatetime, types.ETString)
	// worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
	bf.tp.Flen = (args[1].GetType().Flen + 1) / 2 * 11
	sig := &builtinDateFormatSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_DateFormatSig)
	return sig, nil
}

type builtinDateFormatSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinDateFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func (b *builtinDateFormatSig) evalString(row types.Row) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	t, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	if t.InvalidZero() {
		return "", true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(t.String())))
	}
	formatMask, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}

	res, err := t.DateFormat(formatMask)
	return res, isNull, errors.Trace(err)
}

type fromDaysFunctionClass struct {
	baseFunctionClass
}

func (c *fromDaysFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETInt)
	bf.tp.Flen, bf.tp.Decimal = 10, 0
	sig := &builtinFromDaysSig{bf}
	return sig, nil
}

type builtinFromDaysSig struct {
	baseBuiltinFunc
}

// evalTime evals FROM_DAYS(N).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-days
func (b *builtinFromDaysSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx

	n, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	return types.TimeFromDays(n), false, nil
}

type hourFunctionClass struct {
	baseFunctionClass
}

func (c *hourFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDuration)
	bf.tp.Flen, bf.tp.Decimal = 3, 0
	sig := &builtinHourSig{bf}
	return sig, nil
}

type builtinHourSig struct {
	baseBuiltinFunc
}

// evalInt evals HOUR(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_hour
func (b *builtinHourSig) evalInt(row types.Row) (int64, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(row, b.ctx.GetSessionVars().StmtCtx)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.Hour()), false, nil
}

type minuteFunctionClass struct {
	baseFunctionClass
}

func (c *minuteFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDuration)
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinMinuteSig{bf}
	return sig, nil
}

type builtinMinuteSig struct {
	baseBuiltinFunc
}

// evalInt evals MINUTE(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_minute
func (b *builtinMinuteSig) evalInt(row types.Row) (int64, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(row, b.ctx.GetSessionVars().StmtCtx)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.Minute()), false, nil
}

type secondFunctionClass struct {
	baseFunctionClass
}

func (c *secondFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDuration)
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinSecondSig{bf}
	return sig, nil
}

type builtinSecondSig struct {
	baseBuiltinFunc
}

// evalInt evals SECOND(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_second
func (b *builtinSecondSig) evalInt(row types.Row) (int64, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(row, b.ctx.GetSessionVars().StmtCtx)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.Second()), false, nil
}

type microSecondFunctionClass struct {
	baseFunctionClass
}

func (c *microSecondFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDuration)
	bf.tp.Flen, bf.tp.Decimal = 6, 0
	sig := &builtinMicroSecondSig{bf}
	return sig, nil
}

type builtinMicroSecondSig struct {
	baseBuiltinFunc
}

// evalInt evals MICROSECOND(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_microsecond
func (b *builtinMicroSecondSig) evalInt(row types.Row) (int64, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(row, b.ctx.GetSessionVars().StmtCtx)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.MicroSecond()), false, nil
}

type monthFunctionClass struct {
	baseFunctionClass
}

func (c *monthFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinMonthSig{bf}
	return sig, nil
}

type builtinMonthSig struct {
	baseBuiltinFunc
}

// evalInt evals MONTH(date).
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_month
func (b *builtinMonthSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	date, isNull, err := b.args[0].EvalTime(row, sc)

	if isNull || err != nil {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}

	if date.IsZero() {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(date.String())))
	}

	return int64(date.Time.Month()), false, nil
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_monthname
type monthNameFunctionClass struct {
	baseFunctionClass
}

func (c *monthNameFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETDatetime)
	bf.tp.Flen = 10
	sig := &builtinMonthNameSig{bf}
	return sig, nil
}

type builtinMonthNameSig struct {
	baseBuiltinFunc
}

func (b *builtinMonthNameSig) evalString(row types.Row) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	mon := arg.Time.Month()
	if arg.IsZero() || mon < 0 || mon > len(types.MonthNames) {
		return "", true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(arg.String())))
	} else if mon == 0 {
		return "", true, nil
	}
	return types.MonthNames[mon-1], false, nil
}

type dayNameFunctionClass struct {
	baseFunctionClass
}

func (c *dayNameFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETDatetime)
	bf.tp.Flen = 10
	sig := &builtinDayNameSig{bf}
	return sig, nil
}

type builtinDayNameSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinDayNameSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayname
func (b *builtinDayNameSig) evalString(row types.Row) (string, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	if arg.InvalidZero() {
		return "", true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(arg.String())))
	}
	// Monday is 0, ... Sunday = 6 in MySQL
	// but in go, Sunday is 0, ... Saturday is 6
	// w will do a conversion.
	res := (int64(arg.Time.Weekday()) + 6) % 7
	return types.WeekdayNames[res], false, nil
}

type dayOfMonthFunctionClass struct {
	baseFunctionClass
}

func (c *dayOfMonthFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen = 2
	sig := &builtinDayOfMonthSig{bf}
	return sig, nil
}

type builtinDayOfMonthSig struct {
	baseBuiltinFunc
}

// evalInt evals a builtinDayOfMonthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofmonth
func (b *builtinDayOfMonthSig) evalInt(row types.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	if arg.IsZero() {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(arg.String())))
	}
	return int64(arg.Time.Day()), false, nil
}

type dayOfWeekFunctionClass struct {
	baseFunctionClass
}

func (c *dayOfWeekFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen = 1
	sig := &builtinDayOfWeekSig{bf}
	return sig, nil
}

type builtinDayOfWeekSig struct {
	baseBuiltinFunc
}

// evalInt evals a builtinDayOfWeekSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofweek
func (b *builtinDayOfWeekSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	if arg.InvalidZero() {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(arg.String())))
	}
	// 1 is Sunday, 2 is Monday, .... 7 is Saturday
	return int64(arg.Time.Weekday() + 1), false, nil
}

type dayOfYearFunctionClass struct {
	baseFunctionClass
}

func (c *dayOfYearFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen = 3
	sig := &builtinDayOfYearSig{bf}
	return sig, nil
}

type builtinDayOfYearSig struct {
	baseBuiltinFunc
}

// evalInt evals a builtinDayOfYearSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofyear
func (b *builtinDayOfYearSig) evalInt(row types.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	if arg.InvalidZero() {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(arg.String())))
	}

	return int64(arg.Time.YearDay()), false, nil
}

type weekFunctionClass struct {
	baseFunctionClass
}

func (c *weekFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	argTps := []types.EvalType{types.ETDatetime}
	if len(args) == 2 {
		argTps = append(argTps, types.ETInt)
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)

	bf.tp.Flen, bf.tp.Decimal = 2, 0

	var sig builtinFunc
	if len(args) == 2 {
		sig = &builtinWeekWithModeSig{bf}
	} else {
		sig = &builtinWeekWithoutModeSig{bf}
	}
	return sig, nil
}

type builtinWeekWithModeSig struct {
	baseBuiltinFunc
}

// evalInt evals WEEK(date, mode).
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_week
func (b *builtinWeekWithModeSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	date, isNull, err := b.args[0].EvalTime(row, sc)

	if isNull || err != nil {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}

	if date.IsZero() {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(date.String())))
	}

	mode, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	week := date.Time.Week(int(mode))
	return int64(week), false, nil
}

type builtinWeekWithoutModeSig struct {
	baseBuiltinFunc
}

// evalInt evals WEEK(date).
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_week
func (b *builtinWeekWithoutModeSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	date, isNull, err := b.args[0].EvalTime(row, sc)

	if isNull || err != nil {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}

	if date.IsZero() {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(date.String())))
	}

	week := date.Time.Week(0)
	return int64(week), false, nil
}

type weekDayFunctionClass struct {
	baseFunctionClass
}

func (c *weekDayFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen = 1

	sig := &builtinWeekDaySig{bf}
	return sig, nil
}

type builtinWeekDaySig struct {
	baseBuiltinFunc
}

// evalInt evals WEEKDAY(date).
func (b *builtinWeekDaySig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	date, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}

	if date.IsZero() {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(date.String())))
	}

	return int64(date.Time.Weekday()+6) % 7, false, nil
}

type weekOfYearFunctionClass struct {
	baseFunctionClass
}

func (c *weekOfYearFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinWeekOfYearSig{bf}
	return sig, nil
}

type builtinWeekOfYearSig struct {
	baseBuiltinFunc
}

// evalInt evals WEEKOFYEAR(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_weekofyear
func (b *builtinWeekOfYearSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	date, isNull, err := b.args[0].EvalTime(row, sc)

	if isNull || err != nil {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}

	if date.IsZero() {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(date.String())))
	}

	week := date.Time.Week(3)
	return int64(week), false, nil
}

type yearFunctionClass struct {
	baseFunctionClass
}

func (c *yearFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen, bf.tp.Decimal = 4, 0
	sig := &builtinYearSig{bf}
	return sig, nil
}

type builtinYearSig struct {
	baseBuiltinFunc
}

// evalInt evals YEAR(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_year
func (b *builtinYearSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	date, isNull, err := b.args[0].EvalTime(row, sc)

	if isNull || err != nil {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}

	if date.IsZero() {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(date.String())))
	}

	return int64(date.Time.Year()), false, nil
}

type yearWeekFunctionClass struct {
	baseFunctionClass
}

func (c *yearWeekFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := []types.EvalType{types.ETDatetime}
	if len(args) == 2 {
		argTps = append(argTps, types.ETInt)
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTps...)

	bf.tp.Flen, bf.tp.Decimal = 6, 0

	var sig builtinFunc
	if len(args) == 2 {
		sig = &builtinYearWeekWithModeSig{bf}
	} else {
		sig = &builtinYearWeekWithoutModeSig{bf}
	}
	return sig, nil
}

type builtinYearWeekWithModeSig struct {
	baseBuiltinFunc
}

// evalInt evals YEARWEEK(date,mode).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithModeSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	date, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	if date.IsZero() {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(date.String())))
	}

	mode, isNull, err := b.args[1].EvalInt(row, sc)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	if isNull {
		mode = 0
	}

	year, week := date.Time.YearWeek(int(mode))
	result := int64(week + year*100)
	if result < 0 {
		return int64(math.MaxUint32), false, nil
	}
	return result, false, nil
}

type builtinYearWeekWithoutModeSig struct {
	baseBuiltinFunc
}

// evalInt evals YEARWEEK(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithoutModeSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	date, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}

	if date.InvalidZero() {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(date.String())))
	}

	year, week := date.Time.YearWeek(0)
	result := int64(week + year*100)
	if result < 0 {
		return int64(math.MaxUint32), false, nil
	}
	return result, false, nil
}

type fromUnixTimeFunctionClass struct {
	baseFunctionClass
}

func (c *fromUnixTimeFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	retTp, argTps := types.ETDatetime, make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETDecimal)
	if len(args) == 2 {
		retTp = types.ETString
		argTps = append(argTps, types.ETString)
	}

	_, isArg0Con := args[0].(*Constant)
	isArg0Str := args[0].GetType().EvalType() == types.ETString
	bf := newBaseBuiltinFuncWithTp(ctx, args, retTp, argTps...)
	if len(args) == 1 {
		if isArg0Str {
			bf.tp.Decimal = types.MaxFsp
		} else if isArg0Con {
			arg0, _, err1 := args[0].EvalDecimal(nil, ctx.GetSessionVars().StmtCtx)
			if err1 != nil {
				return sig, errors.Trace(err1)
			}
			fsp := int(arg0.GetDigitsFrac())
			if fsp > types.MaxFsp {
				fsp = types.MaxFsp
			}
			bf.tp.Decimal = fsp
		}
		sig = &builtinFromUnixTime1ArgSig{bf}
	} else {
		bf.tp.Flen = args[1].GetType().Flen
		sig = &builtinFromUnixTime2ArgSig{bf}
	}
	return sig, nil
}

func evalFromUnixTime(ctx context.Context, fsp int, row types.Row, arg Expression) (res types.Time, isNull bool, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	unixTimeStamp, isNull, err := arg.EvalDecimal(row, sc)
	if err != nil || isNull {
		return res, isNull, errors.Trace(err)
	}
	// 0 <= unixTimeStamp <= INT32_MAX
	if unixTimeStamp.IsNegative() {
		return res, true, nil
	}
	integralPart, err := unixTimeStamp.ToInt()
	if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) {
		return res, true, errors.Trace(err)
	}
	if integralPart > int64(math.MaxInt32) {
		return res, true, nil
	}
	// Split the integral part and fractional part of a decimal timestamp.
	// e.g. for timestamp 12345.678,
	// first get the integral part 12345,
	// then (12345.678 - 12345) * (10^9) to get the decimal part and convert it to nanosecond precision.
	integerDecimalTp := new(types.MyDecimal).FromInt(integralPart)
	fracDecimalTp := new(types.MyDecimal)
	err = types.DecimalSub(unixTimeStamp, integerDecimalTp, fracDecimalTp)
	if err != nil {
		return res, true, errors.Trace(err)
	}
	nano := new(types.MyDecimal).FromInt(int64(time.Second))
	x := new(types.MyDecimal)
	err = types.DecimalMul(fracDecimalTp, nano, x)
	if err != nil {
		return res, true, errors.Trace(err)
	}
	fractionalPart, err := x.ToInt() // here fractionalPart is result multiplying the original fractional part by 10^9.
	if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) {
		return res, true, errors.Trace(err)
	}
	fracDigitsNumber := int(unixTimeStamp.GetDigitsFrac())
	if fsp < 0 {
		fsp = types.MaxFsp
	}
	fsp = mathutil.Max(fracDigitsNumber, fsp)
	if fsp > types.MaxFsp {
		fsp = types.MaxFsp
	}

	tmp := time.Unix(integralPart, fractionalPart).In(sc.TimeZone)
	t, err := convertTimeToMysqlTime(tmp, fsp)
	if err != nil {
		return res, true, errors.Trace(err)
	}
	return t, false, nil
}

type builtinFromUnixTime1ArgSig struct {
	baseBuiltinFunc
}

// evalTime evals a builtinFromUnixTime1ArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-unixtime
func (b *builtinFromUnixTime1ArgSig) evalTime(row types.Row) (res types.Time, isNull bool, err error) {
	return evalFromUnixTime(b.ctx, b.tp.Decimal, row, b.args[0])
}

type builtinFromUnixTime2ArgSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinFromUnixTime2ArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-unixtime
func (b *builtinFromUnixTime2ArgSig) evalString(row types.Row) (res string, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	format, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	t, isNull, err := evalFromUnixTime(b.ctx, b.tp.Decimal, row, b.args[0])
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	res, err = t.DateFormat(format)
	return res, err != nil, errors.Trace(err)
}

type getFormatFunctionClass struct {
	baseFunctionClass
}

func (c *getFormatFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETString)
	bf.tp.Flen = 17
	sig := &builtinGetFormatSig{bf}
	return sig, nil
}

type builtinGetFormatSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinGetFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_get-format
func (b *builtinGetFormatSig) evalString(row types.Row) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	t, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	l, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}

	var res string
	switch t {
	case dateFormat:
		switch l {
		case usaLocation:
			res = "%m.%d.%Y"
		case jisLocation:
			res = "%Y-%m-%d"
		case isoLocation:
			res = "%Y-%m-%d"
		case eurLocation:
			res = "%d.%m.%Y"
		case internalLocation:
			res = "%Y%m%d"
		}
	case datetimeFormat, timestampFormat:
		switch l {
		case usaLocation:
			res = "%Y-%m-%d %H.%i.%s"
		case jisLocation:
			res = "%Y-%m-%d %H:%i:%s"
		case isoLocation:
			res = "%Y-%m-%d %H:%i:%s"
		case eurLocation:
			res = "%Y-%m-%d %H.%i.%s"
		case internalLocation:
			res = "%Y%m%d%H%i%s"
		}
	case timeFormat:
		switch l {
		case usaLocation:
			res = "%h:%i:%s %p"
		case jisLocation:
			res = "%H:%i:%s"
		case isoLocation:
			res = "%H:%i:%s"
		case eurLocation:
			res = "%H.%i.%s"
		case internalLocation:
			res = "%H%i%s"
		}
	}

	return res, false, nil
}

type strToDateFunctionClass struct {
	baseFunctionClass
}

func (c *strToDateFunctionClass) getRetTp(arg Expression, ctx context.Context) (tp byte, fsp int) {
	tp = mysql.TypeDatetime
	if _, ok := arg.(*Constant); !ok {
		return tp, types.MaxFsp
	}
	strArg := WrapWithCastAsString(ctx, arg)
	format, isNull, err := strArg.EvalString(nil, ctx.GetSessionVars().StmtCtx)
	if err != nil || isNull {
		return
	}
	isDuration, isDate := types.GetFormatType(format)
	if isDuration && !isDate {
		tp = mysql.TypeDuration
	} else if !isDuration && isDate {
		tp = mysql.TypeDate
	}
	if strings.Contains(format, "%f") {
		fsp = types.MaxFsp
	}
	return
}

// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_str-to-date
func (c *strToDateFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	retTp, fsp := c.getRetTp(args[1], ctx)
	switch retTp {
	case mysql.TypeDate:
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETString, types.ETString)
		bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, types.MinFsp
		sig = &builtinStrToDateDateSig{bf}
	case mysql.TypeDatetime:
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETString, types.ETString)
		if fsp == types.MinFsp {
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeWidthNoFsp, types.MinFsp
		} else {
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeWidthWithFsp, types.MaxFsp
		}
		sig = &builtinStrToDateDatetimeSig{bf}
	case mysql.TypeDuration:
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, types.ETString, types.ETString)
		if fsp == types.MinFsp {
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthNoFsp, types.MinFsp
		} else {
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthWithFsp, types.MaxFsp
		}
		sig = &builtinStrToDateDurationSig{bf}
	}
	return sig, nil
}

type builtinStrToDateDateSig struct {
	baseBuiltinFunc
}

func (b *builtinStrToDateDateSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	date, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, isNull, errors.Trace(err)
	}
	format, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, isNull, errors.Trace(err)
	}
	var t types.Time
	succ := t.StrToDate(sc, date, format)
	if !succ {
		return types.Time{}, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(t.String()))
	}
	t.Type, t.Fsp = mysql.TypeDate, types.MinFsp
	return t, false, nil
}

type builtinStrToDateDatetimeSig struct {
	baseBuiltinFunc
}

func (b *builtinStrToDateDatetimeSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	date, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, isNull, errors.Trace(err)
	}
	format, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, isNull, errors.Trace(err)
	}
	var t types.Time
	succ := t.StrToDate(sc, date, format)
	if !succ {
		return types.Time{}, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(t.String()))
	}
	t.Type, t.Fsp = mysql.TypeDatetime, b.tp.Decimal
	return t, false, nil
}

type builtinStrToDateDurationSig struct {
	baseBuiltinFunc
}

// TODO: If the NO_ZERO_DATE or NO_ZERO_IN_DATE SQL mode is enabled, zero dates or part of dates are disallowed.
// In that case, STR_TO_DATE() returns NULL and generates a warning.
func (b *builtinStrToDateDurationSig) evalDuration(row types.Row) (types.Duration, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	date, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return types.Duration{}, isNull, errors.Trace(err)
	}
	format, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return types.Duration{}, isNull, errors.Trace(err)
	}
	var t types.Time
	succ := t.StrToDate(sc, date, format)
	if !succ {
		return types.Duration{}, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(t.String()))
	}
	t.Fsp = b.tp.Decimal
	dur, err := t.ConvertToDuration()
	return dur, err != nil, errors.Trace(err)
}

type sysDateFunctionClass struct {
	baseFunctionClass
}

func (c *sysDateFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := []types.EvalType{}
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)
	bf.tp.Flen, bf.tp.Decimal = 19, 0

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinSysDateWithFspSig{bf}
	} else {
		sig = &builtinSysDateWithoutFspSig{bf}
	}
	return sig, nil
}

type builtinSysDateWithFspSig struct {
	baseBuiltinFunc
}

// evalTime evals SYSDATE(fsp).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sysdate
func (b *builtinSysDateWithFspSig) evalTime(row types.Row) (d types.Time, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	fsp, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return types.Time{}, isNull, errors.Trace(err)
	}

	result, err := convertTimeToMysqlTime(time.Now(), int(fsp))
	if err != nil {
		return types.Time{}, true, errors.Trace(err)
	}
	return result, false, nil
}

type builtinSysDateWithoutFspSig struct {
	baseBuiltinFunc
}

// evalTime evals SYSDATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sysdate
func (b *builtinSysDateWithoutFspSig) evalTime(row types.Row) (d types.Time, isNull bool, err error) {
	result, err := convertTimeToMysqlTime(time.Now(), 0)
	if err != nil {
		return types.Time{}, true, errors.Trace(err)
	}
	return result, false, nil
}

type currentDateFunctionClass struct {
	baseFunctionClass
}

func (c *currentDateFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime)
	bf.tp.Flen, bf.tp.Decimal = 10, 0
	sig := &builtinCurrentDateSig{bf}
	return sig, nil
}

type builtinCurrentDateSig struct {
	baseBuiltinFunc
}

// evalTime evals CURDATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_curdate
func (b *builtinCurrentDateSig) evalTime(row types.Row) (d types.Time, isNull bool, err error) {
	year, month, day := time.Now().Date()
	result := types.Time{
		Time: types.FromDate(year, int(month), day, 0, 0, 0, 0),
		Type: mysql.TypeDate,
		Fsp:  0}
	return result, false, nil
}

type currentTimeFunctionClass struct {
	baseFunctionClass
}

func (c *currentTimeFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	if len(args) == 0 {
		bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration)
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthNoFsp, types.MinFsp
		sig = &builtinCurrentTime0ArgSig{bf}
		return sig, nil
	}
	// args[0] must be a constant which should not be null.
	_, ok := args[0].(*Constant)
	fsp := int64(types.MaxFsp)
	if ok {
		fsp, _, err = args[0].EvalInt(nil, ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if fsp > int64(types.MaxFsp) {
			return nil, errors.Errorf("Too-big precision %v specified for 'curtime'. Maximum is %v.", fsp, types.MaxFsp)
		} else if fsp < int64(types.MinFsp) {
			return nil, errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
		}
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, types.ETInt)
	bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthWithFsp, int(fsp)
	sig = &builtinCurrentTime1ArgSig{bf}
	return sig, nil
}

type builtinCurrentTime0ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentTime0ArgSig) evalDuration(row types.Row) (types.Duration, bool, error) {
	res, err := types.ParseDuration(time.Now().Format(types.TimeFormat), types.MinFsp)
	if err != nil {
		return types.Duration{}, true, errors.Trace(err)
	}
	return res, false, nil
}

type builtinCurrentTime1ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentTime1ArgSig) evalDuration(row types.Row) (types.Duration, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	fsp, _, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return types.Duration{}, true, errors.Trace(err)
	}
	res, err := types.ParseDuration(time.Now().Format(types.TimeFSPFormat), int(fsp))
	if err != nil {
		return types.Duration{}, true, errors.Trace(err)
	}
	return res, false, nil
}

type timeFunctionClass struct {
	baseFunctionClass
}

func (c *timeFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, types.ETString)
	sig := &builtinTimeSig{bf}
	return sig, nil
}

type builtinTimeSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time.
func (b *builtinTimeSig) evalDuration(row types.Row) (res types.Duration, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	expr, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}

	fsp := 0
	if idx := strings.Index(expr, "."); idx != -1 {
		fsp = len(expr) - idx - 1
	}

	if fsp, err = types.CheckFsp(fsp); err != nil {
		return res, isNull, errors.Trace(err)
	}

	res, err = types.ParseDuration(expr, fsp)
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = sc.HandleTruncate(err)
	}
	return res, isNull, errors.Trace(err)
}

type timeLiteralFunctionClass struct {
	baseFunctionClass
}

func (c *timeLiteralFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	con, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for time literal")
	}
	dt, err := con.Eval(nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	str := dt.GetString()
	if !isDuration(str) {
		return nil, types.ErrIncorrectDatetimeValue.GenByArgs(str)
	}
	duration, err := types.ParseDuration(str, getFsp(str))
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, []Expression{}, types.ETDuration)
	bf.tp.Flen, bf.tp.Decimal = 10, duration.Fsp
	if duration.Fsp > 0 {
		bf.tp.Flen += 1 + duration.Fsp
	}
	sig := &builtinTimeLiteralSig{bf, duration}
	return sig, nil
}

type builtinTimeLiteralSig struct {
	baseBuiltinFunc
	duration types.Duration
}

// evalDuration evals TIME 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinTimeLiteralSig) evalDuration(row types.Row) (types.Duration, bool, error) {
	return b.duration, false, nil
}

type utcDateFunctionClass struct {
	baseFunctionClass
}

func (c *utcDateFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime)
	bf.tp.Flen, bf.tp.Decimal = 10, 0
	sig := &builtinUTCDateSig{bf}
	return sig, nil
}

type builtinUTCDateSig struct {
	baseBuiltinFunc
}

// evalTime evals UTC_DATE, UTC_DATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-date
func (b *builtinUTCDateSig) evalTime(row types.Row) (types.Time, bool, error) {
	year, month, day := time.Now().UTC().Date()
	result := types.Time{
		Time: types.FromGoTime(time.Date(year, month, day, 0, 0, 0, 0, time.UTC)),
		Type: mysql.TypeDate,
		Fsp:  types.UnspecifiedFsp}
	return result, false, nil
}

type utcTimestampFunctionClass struct {
	baseFunctionClass
}

func getFlenAndDecimal4UTCTimestampAndNow(sc *stmtctx.StatementContext, arg Expression) (flen, decimal int) {
	if constant, ok := arg.(*Constant); ok {
		fsp, isNull, err := constant.EvalInt(nil, sc)
		if isNull || err != nil || fsp > int64(types.MaxFsp) {
			decimal = types.MaxFsp
		} else if fsp < int64(types.MinFsp) {
			decimal = types.MinFsp
		} else {
			decimal = int(fsp)
		}
	}
	if decimal > 0 {
		flen = 19 + 1 + decimal
	} else {
		flen = 19
	}
	return flen, decimal
}

func (c *utcTimestampFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)

	if len(args) == 1 {
		bf.tp.Flen, bf.tp.Decimal = getFlenAndDecimal4UTCTimestampAndNow(bf.ctx.GetSessionVars().StmtCtx, args[0])
	} else {
		bf.tp.Flen, bf.tp.Decimal = 19, 0
	}

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinUTCTimestampWithArgSig{bf}
	} else {
		sig = &builtinUTCTimestampWithoutArgSig{bf}
	}
	return sig, nil
}

func evalUTCTimestampWithFsp(fsp int) (types.Time, bool, error) {
	result, err := convertTimeToMysqlTime(time.Now().UTC(), fsp)
	if err != nil {
		return types.Time{}, true, errors.Trace(err)
	}
	return result, false, nil
}

type builtinUTCTimestampWithArgSig struct {
	baseBuiltinFunc
}

// evalTime evals UTC_TIMESTAMP(fsp).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithArgSig) evalTime(row types.Row) (types.Time, bool, error) {
	num, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	if !isNull && num > int64(types.MaxFsp) {
		return types.Time{}, true, errors.Errorf("Too-big precision %v specified for 'utc_timestamp'. Maximum is %v.", num, types.MaxFsp)
	}
	if !isNull && num < int64(types.MinFsp) {
		return types.Time{}, true, errors.Errorf("Invalid negative %d specified, must in [0, 6].", num)
	}

	result, isNull, err := evalUTCTimestampWithFsp(int(num))
	return result, isNull, errors.Trace(err)
}

type builtinUTCTimestampWithoutArgSig struct {
	baseBuiltinFunc
}

// evalTime evals UTC_TIMESTAMP().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithoutArgSig) evalTime(row types.Row) (types.Time, bool, error) {
	result, isNull, err := evalUTCTimestampWithFsp(0)
	return result, isNull, errors.Trace(err)
}

type nowFunctionClass struct {
	baseFunctionClass
}

func (c *nowFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)

	if len(args) == 1 {
		bf.tp.Flen, bf.tp.Decimal = getFlenAndDecimal4UTCTimestampAndNow(bf.ctx.GetSessionVars().StmtCtx, args[0])
	} else {
		bf.tp.Flen, bf.tp.Decimal = 19, 0
	}

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinNowWithArgSig{bf}
	} else {
		sig = &builtinNowWithoutArgSig{bf}
	}
	return sig, nil
}

func evalNowWithFsp(ctx context.Context, fsp int) (types.Time, bool, error) {
	sysTs, err := getSystemTimestamp(ctx)
	if err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	result, err := convertTimeToMysqlTime(sysTs, fsp)
	if err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	err = result.ConvertTimeZone(time.Local, ctx.GetSessionVars().GetTimeZone())
	if err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	return result, false, nil
}

type builtinNowWithArgSig struct {
	baseBuiltinFunc
}

// evalTime evals NOW(fsp)
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_now
func (b *builtinNowWithArgSig) evalTime(row types.Row) (types.Time, bool, error) {
	fsp, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)

	if err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	if isNull {
		fsp = 0
	} else if fsp > int64(types.MaxFsp) {
		return types.Time{}, true, errors.Errorf("Too-big precision %v specified for 'now'. Maximum is %v.", fsp, types.MaxFsp)
	} else if fsp < int64(types.MinFsp) {
		return types.Time{}, true, errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
	}

	result, isNull, err := evalNowWithFsp(b.ctx, int(fsp))
	return result, isNull, errors.Trace(err)
}

type builtinNowWithoutArgSig struct {
	baseBuiltinFunc
}

// evalTime evals NOW()
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_now
func (b *builtinNowWithoutArgSig) evalTime(row types.Row) (types.Time, bool, error) {
	result, isNull, err := evalNowWithFsp(b.ctx, 0)
	return result, isNull, errors.Trace(err)
}

type extractFunctionClass struct {
	baseFunctionClass
}

func (c *extractFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	datetimeUnits := map[string]struct{}{
		"DAY":             {},
		"WEEK":            {},
		"MONTH":           {},
		"QUARTER":         {},
		"YEAR":            {},
		"DAY_MICROSECOND": {},
		"DAY_SECOND":      {},
		"DAY_MINUTE":      {},
		"DAY_HOUR":        {},
		"YEAR_MONTH":      {},
	}
	isDatetimeUnit := true
	args[0] = WrapWithCastAsString(ctx, args[0])
	if _, isCon := args[0].(*Constant); isCon {
		unit, _, err1 := args[0].EvalString(nil, ctx.GetSessionVars().StmtCtx)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		_, isDatetimeUnit = datetimeUnits[unit]
	}
	var bf baseBuiltinFunc
	if isDatetimeUnit {
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETDatetime)
		sig = &builtinExtractDatetimeSig{bf}
	} else {
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETDuration)
		sig = &builtinExtractDurationSig{bf}
	}
	return sig, nil
}

type builtinExtractDatetimeSig struct {
	baseBuiltinFunc
}

// evalInt evals a builtinExtractDatetimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func (b *builtinExtractDatetimeSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	unit, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	dt, isNull, err := b.args[1].EvalTime(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	res, err := types.ExtractDatetimeNum(&dt, unit)
	return res, err != nil, errors.Trace(err)
}

type builtinExtractDurationSig struct {
	baseBuiltinFunc
}

// evalInt evals a builtinExtractDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func (b *builtinExtractDurationSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	unit, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	dur, isNull, err := b.args[1].EvalDuration(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	res, err := types.ExtractDurationNum(&dur, unit)
	return res, err != nil, errors.Trace(err)
}

// baseDateArithmitical is the base class for all "builtinAddDateXXXSig" and "builtinSubDateXXXSig",
// which provides parameter getter and date arithmetical calculate functions.
type baseDateArithmitical struct {
	// intervalRegexp is "*Regexp" used to extract string interval for "DAY" unit.
	intervalRegexp *regexp.Regexp
}

func newDateArighmeticalUtil() baseDateArithmitical {
	return baseDateArithmitical{
		intervalRegexp: regexp.MustCompile(`[\d]+`),
	}
}

func (du *baseDateArithmitical) getDateFromString(ctx context.Context, args []Expression, row types.Row, unit string) (types.Time, bool, error) {
	sc := ctx.GetSessionVars().StmtCtx
	dateStr, isNull, err := args[0].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	dateTp := mysql.TypeDate
	if !types.IsDateFormat(dateStr) || types.IsClockUnit(unit) {
		dateTp = mysql.TypeDatetime
	}

	date, err := types.ParseTime(sc, dateStr, dateTp, types.MaxFsp)
	return date, err != nil, errors.Trace(handleInvalidTimeError(ctx, err))
}

func (du *baseDateArithmitical) getDateFromInt(ctx context.Context, args []Expression, row types.Row, unit string) (types.Time, bool, error) {
	sc := ctx.GetSessionVars().StmtCtx
	dateInt, isNull, err := args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	date, err := types.ParseTimeFromInt64(sc, dateInt)
	if err != nil {
		return types.Time{}, true, errors.Trace(handleInvalidTimeError(ctx, err))
	}

	dateTp := mysql.TypeDate
	if date.Type == mysql.TypeDatetime || date.Type == mysql.TypeTimestamp || types.IsClockUnit(unit) {
		dateTp = mysql.TypeDatetime
	}
	date.Type = dateTp
	return date, false, nil
}

func (du *baseDateArithmitical) getDateFromDatetime(ctx context.Context, args []Expression, row types.Row, unit string) (types.Time, bool, error) {
	date, isNull, err := args[0].EvalTime(row, ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	dateTp := mysql.TypeDate
	if date.Type == mysql.TypeDatetime || date.Type == mysql.TypeTimestamp || types.IsClockUnit(unit) {
		dateTp = mysql.TypeDatetime
	}
	date.Type = dateTp
	return date, false, nil
}

func (du *baseDateArithmitical) getIntervalFromString(ctx context.Context, args []Expression, row types.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalString(row, ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	// unit "DAY" has to be specially handled.
	if strings.ToLower(unit) == "day" {
		if strings.ToLower(interval) == "true" {
			interval = "1"
		} else if strings.ToLower(interval) == "false" {
			interval = "0"
		} else {
			interval = du.intervalRegexp.FindString(interval)
		}
	}
	return interval, false, nil
}

func (du *baseDateArithmitical) getIntervalFromInt(ctx context.Context, args []Expression, row types.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalInt(row, ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	return strconv.FormatInt(interval, 10), false, nil
}

func (du *baseDateArithmitical) add(ctx context.Context, date types.Time, interval string, unit string) (types.Time, bool, error) {
	year, month, day, dur, err := types.ExtractTimeValue(unit, interval)
	if err != nil {
		return types.Time{}, true, errors.Trace(handleInvalidTimeError(ctx, err))
	}

	goTime, err := date.Time.GoTime(time.Local)
	if err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	goTime = goTime.Add(dur)
	goTime = goTime.AddDate(int(year), int(month), int(day))

	if goTime.Nanosecond() == 0 {
		date.Fsp = 0
	}

	date.Time = types.FromGoTime(goTime)
	return date, false, nil
}

func (du *baseDateArithmitical) sub(ctx context.Context, date types.Time, interval string, unit string) (types.Time, bool, error) {
	year, month, day, dur, err := types.ExtractTimeValue(unit, interval)
	if err != nil {
		return types.Time{}, true, errors.Trace(handleInvalidTimeError(ctx, err))
	}
	year, month, day, dur = -year, -month, -day, -dur

	goTime, err := date.Time.GoTime(time.Local)
	if err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	goTime = goTime.Add(dur)
	goTime = goTime.AddDate(int(year), int(month), int(day))

	if goTime.Nanosecond() == 0 {
		date.Fsp = 0
	}

	date.Time = types.FromGoTime(goTime)
	return date, false, nil
}

type addDateFunctionClass struct {
	baseFunctionClass
}

func (c *addDateFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	dateEvalTp := args[0].GetType().EvalType()
	if dateEvalTp != types.ETString && dateEvalTp != types.ETInt {
		dateEvalTp = types.ETDatetime
	}

	intervalEvalTp := args[1].GetType().EvalType()
	if intervalEvalTp != types.ETString {
		intervalEvalTp = types.ETInt
	}

	argTps := []types.EvalType{dateEvalTp, intervalEvalTp, types.ETString}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)
	bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeFullWidth, types.UnspecifiedLength

	switch {
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETString:
		sig = &builtinAddDateStringStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETInt:
		sig = &builtinAddDateStringIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETString:
		sig = &builtinAddDateIntStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETInt:
		sig = &builtinAddDateIntIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETString:
		sig = &builtinAddDateDatetimeStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETInt:
		sig = &builtinAddDateDatetimeIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	}
	return sig, nil
}

type builtinAddDateStringStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringStringSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	unit, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, errors.Trace(err)
}

type builtinAddDateStringIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringIntSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	unit, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, errors.Trace(err)
}

type builtinAddDateIntStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntStringSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	unit, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, errors.Trace(err)
}

type builtinAddDateIntIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntIntSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	unit, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, errors.Trace(err)
}

type builtinAddDateDatetimeStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeStringSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	unit, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, errors.Trace(err)
}

type builtinAddDateDatetimeIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeIntSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	unit, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, errors.Trace(err)
}

type subDateFunctionClass struct {
	baseFunctionClass
}

func (c *subDateFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	dateEvalTp := args[0].GetType().EvalType()
	if dateEvalTp != types.ETString && dateEvalTp != types.ETInt {
		dateEvalTp = types.ETDatetime
	}

	intervalEvalTp := args[1].GetType().EvalType()
	if intervalEvalTp != types.ETString {
		intervalEvalTp = types.ETInt
	}

	argTps := []types.EvalType{dateEvalTp, intervalEvalTp, types.ETString}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, argTps...)
	bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeFullWidth, types.UnspecifiedLength

	switch {
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETString:
		sig = &builtinSubDateStringStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETInt:
		sig = &builtinSubDateStringIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETString:
		sig = &builtinSubDateIntStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETInt:
		sig = &builtinSubDateIntIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETString:
		sig = &builtinSubDateDatetimeStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETInt:
		sig = &builtinSubDateDatetimeIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	}
	return sig, nil
}

type builtinSubDateStringStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateStringStringSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	unit, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, errors.Trace(err)
}

type builtinSubDateStringIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateStringIntSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	unit, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, errors.Trace(err)
}

type builtinSubDateIntStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntStringSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	unit, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, errors.Trace(err)
}

type builtinSubDateIntIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntIntSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	unit, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, errors.Trace(err)
}

type builtinSubDateDatetimeStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeStringSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	unit, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, errors.Trace(err)
}

type builtinSubDateDatetimeIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeIntSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	unit, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, errors.Trace(err)
}

type timestampDiffFunctionClass struct {
	baseFunctionClass
}

func (c *timestampDiffFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETDatetime, types.ETDatetime)
	sig := &builtinTimestampDiffSig{bf}
	return sig, nil
}

type builtinTimestampDiffSig struct {
	baseBuiltinFunc
}

// evalInt evals a builtinTimestampDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampdiff
func (b *builtinTimestampDiffSig) evalInt(row types.Row) (int64, bool, error) {
	ctx := b.getCtx().GetSessionVars().StmtCtx
	unit, isNull, err := b.args[0].EvalString(row, ctx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	lhs, isNull, err := b.args[1].EvalTime(row, ctx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(handleInvalidTimeError(b.getCtx(), err))
	}
	rhs, isNull, err := b.args[2].EvalTime(row, ctx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(handleInvalidTimeError(b.getCtx(), err))
	}
	if invalidLHS, invalidRHS := lhs.InvalidZero(), rhs.InvalidZero(); invalidLHS || invalidRHS {
		if invalidLHS {
			err = handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(lhs.String()))
		}
		if invalidRHS {
			err = handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(rhs.String()))
		}
		return 0, true, errors.Trace(err)
	}
	return types.TimestampDiff(unit, lhs, rhs), false, nil
}

type unixTimestampFunctionClass struct {
	baseFunctionClass
}

func (c *unixTimestampFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	var argTps []types.EvalType
	var retTp types.EvalType
	var retFLen, retDecimal int

	if len(args) == 0 {
		retTp, retDecimal = types.ETInt, 0
	} else {
		argTps = []types.EvalType{types.ETDatetime}
		argType := args[0].GetType()
		argEvaltp := argType.EvalType()
		if argEvaltp == types.ETString {
			// Treat types.ETString as unspecified decimal.
			retDecimal = types.UnspecifiedLength
		} else {
			retDecimal = argType.Decimal
		}
		if retDecimal > 6 || retDecimal == types.UnspecifiedLength {
			retDecimal = 6
		}
		if retDecimal == 0 {
			retTp = types.ETInt
		} else {
			retTp = types.ETDecimal
		}
	}
	if retTp == types.ETInt {
		retFLen = 11
	} else if retTp == types.ETDecimal {
		retFLen = 12 + retDecimal
	} else {
		panic("Unexpected retTp")
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, retTp, argTps...)
	bf.tp.Flen = retFLen
	bf.tp.Decimal = retDecimal

	var sig builtinFunc
	if len(args) == 0 {
		sig = &builtinUnixTimestampCurrentSig{bf}
	} else if retTp == types.ETInt {
		sig = &builtinUnixTimestampIntSig{bf}
	} else if retTp == types.ETDecimal {
		sig = &builtinUnixTimestampDecSig{bf}
	} else {
		panic("Unexpected retTp")
	}

	return sig, nil
}

// goTimeToMysqlUnixTimestamp converts go time into MySQL's Unix timestamp.
// MySQL's Unix timestamp ranges in int32. Values out of range should be rewritten to 0.
func goTimeToMysqlUnixTimestamp(t time.Time, decimal int) (*types.MyDecimal, error) {
	nanoSeconds := t.UnixNano()
	if nanoSeconds < 0 || (nanoSeconds/1e3) >= (math.MaxInt32+1)*1e6 {
		return new(types.MyDecimal), nil
	}
	dec := new(types.MyDecimal)
	// Here we don't use float to prevent precision lose.
	dec.FromInt(nanoSeconds)
	err := dec.Shift(-9)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = dec.Round(dec, decimal, types.ModeHalfEven)
	return dec, errors.Trace(err)
}

type builtinUnixTimestampCurrentSig struct {
	baseBuiltinFunc
}

// evalInt evals a UNIX_TIMESTAMP().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampCurrentSig) evalInt(row types.Row) (int64, bool, error) {
	dec, err := goTimeToMysqlUnixTimestamp(time.Now(), 1)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	intVal, err := dec.ToInt()
	terror.Log(errors.Trace(err))
	return intVal, false, nil
}

type builtinUnixTimestampIntSig struct {
	baseBuiltinFunc
}

// evalInt evals a UNIX_TIMESTAMP(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampIntSig) evalInt(row types.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if err != nil && terror.ErrorEqual(types.ErrInvalidTimeFormat, err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}
	t, err := val.Time.GoTime(getTimeZone(b.getCtx()))
	if err != nil {
		return 0, false, nil
	}
	dec, err := goTimeToMysqlUnixTimestamp(t, 1)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	intVal, err := dec.ToInt()
	terror.Log(errors.Trace(err))
	return intVal, false, nil
}

type builtinUnixTimestampDecSig struct {
	baseBuiltinFunc
}

// evalDecimal evals a UNIX_TIMESTAMP(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampDecSig) evalDecimal(row types.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		// Return 0 for invalid date time.
		return new(types.MyDecimal), isNull, nil
	}
	t, err := val.Time.GoTime(getTimeZone(b.getCtx()))
	if err != nil {
		return new(types.MyDecimal), false, nil
	}
	result, err := goTimeToMysqlUnixTimestamp(t, b.tp.Decimal)
	return result, err != nil, errors.Trace(err)
}

type timestampFunctionClass struct {
	baseFunctionClass
}

func (c *timestampFunctionClass) getDefaultFsp(tp *types.FieldType) int {
	if tp.Tp == mysql.TypeDatetime || tp.Tp == mysql.TypeDate || tp.Tp == mysql.TypeDuration ||
		tp.Tp == mysql.TypeTimestamp || tp.Tp == mysql.TypeNewDate {
		return tp.Decimal
	}
	switch cls := tp.EvalType(); cls {
	case types.ETInt:
		return types.MinFsp
	case types.ETReal, types.ETDatetime, types.ETTimestamp, types.ETDuration, types.ETJson, types.ETString:
		return types.MaxFsp
	case types.ETDecimal:
		if tp.Decimal < types.MaxFsp {
			return tp.Decimal
		}
		return types.MaxFsp
	}
	return types.MaxFsp
}

func (c *timestampFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	evalTps, argLen := []types.EvalType{types.ETString}, len(args)
	if argLen == 2 {
		evalTps = append(evalTps, types.ETString)
	}
	fsp := c.getDefaultFsp(args[0].GetType())
	if argLen == 2 {
		fsp = mathutil.Max(fsp, c.getDefaultFsp(args[1].GetType()))
	}
	isFloat := false
	switch args[0].GetType().Tp {
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeDecimal:
		isFloat = true
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, evalTps...)
	bf.tp.Decimal, bf.tp.Flen = fsp, 19
	if fsp != 0 {
		bf.tp.Flen += 1 + fsp
	}
	var sig builtinFunc
	if argLen == 2 {
		sig = &builtinTimestamp2ArgsSig{bf, isFloat}
	} else {
		sig = &builtinTimestamp1ArgSig{bf, isFloat}
	}
	return sig, nil
}

type builtinTimestamp1ArgSig struct {
	baseBuiltinFunc

	isFloat bool
}

// evalTime evals a builtinTimestamp1ArgSig.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_timestamp
func (b *builtinTimestamp1ArgSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	s, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, isNull, errors.Trace(err)
	}
	var tm types.Time
	if b.isFloat {
		tm, err = types.ParseTimeFromFloatString(sc, s, mysql.TypeDatetime, getFsp(s))
	} else {
		tm, err = types.ParseTime(sc, s, mysql.TypeDatetime, getFsp(s))
	}
	if err != nil {
		return types.Time{}, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	return tm, false, nil
}

type builtinTimestamp2ArgsSig struct {
	baseBuiltinFunc

	isFloat bool
}

// evalTime evals a builtinTimestamp2ArgsSig.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_timestamp
func (b *builtinTimestamp2ArgsSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, isNull, errors.Trace(err)
	}
	var tm types.Time
	if b.isFloat {
		tm, err = types.ParseTimeFromFloatString(sc, arg0, mysql.TypeDatetime, getFsp(arg0))
	} else {
		tm, err = types.ParseTime(sc, arg0, mysql.TypeDatetime, getFsp(arg0))
	}
	if err != nil {
		return types.Time{}, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	arg1, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, isNull, errors.Trace(err)
	}
	if !isDuration(arg1) {
		return types.Time{}, true, nil
	}
	duration, err := types.ParseDuration(arg1, getFsp(arg1))
	if err != nil {
		return types.Time{}, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	tmp, err := tm.Add(duration)
	if err != nil {
		return types.Time{}, true, errors.Trace(err)
	}
	return tmp, false, nil
}

type timestampLiteralFunctionClass struct {
	baseFunctionClass
}

func (c *timestampLiteralFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	con, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for timestamp literal")
	}
	dt, err := con.Eval(nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	str, err := dt.ToString()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !timestampPattern.MatchString(str) {
		return nil, types.ErrIncorrectDatetimeValue.GenByArgs(str)
	}
	tm, err := types.ParseTime(ctx.GetSessionVars().StmtCtx, str, mysql.TypeTimestamp, getFsp(str))
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, []Expression{}, types.ETDatetime)
	bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeWidthNoFsp, tm.Fsp
	if tm.Fsp > 0 {
		bf.tp.Flen += tm.Fsp + 1
	}
	sig := &builtinTimestampLiteralSig{bf, tm}
	return sig, nil
}

type builtinTimestampLiteralSig struct {
	baseBuiltinFunc
	tm types.Time
}

// evalTime evals TIMESTAMP 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinTimestampLiteralSig) evalTime(row types.Row) (types.Time, bool, error) {
	return b.tm, false, nil
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

// getFsp4TimeAddSub is used to in function 'ADDTIME' and 'SUBTIME' to evaluate `fsp` for the
// second parameter. It's used only if the second parameter is of string type. It's different
// from getFsp in that the result of getFsp4TimeAddSub is either 6 or 0.
func getFsp4TimeAddSub(s string) int {
	if len(s)-strings.Index(s, ".")-1 == len(s) {
		return types.MinFsp
	}
	for _, c := range s[strings.Index(s, ".")+1:] {
		if c != '0' {
			return types.MaxFsp
		}
	}
	return types.MinFsp
}

// getBf4TimeAddSub parses input types, generates baseBuiltinFunc and set related attributes for
// builtin function 'ADDTIME' and 'SUBTIME'
func getBf4TimeAddSub(ctx context.Context, args []Expression) (tp1, tp2 *types.FieldType, bf baseBuiltinFunc) {
	tp1, tp2 = args[0].GetType(), args[1].GetType()
	var argTp1, argTp2, retTp types.EvalType
	switch tp1.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		argTp1, retTp = types.ETDatetime, types.ETDatetime
	case mysql.TypeDuration:
		argTp1, retTp = types.ETDuration, types.ETDuration
	case mysql.TypeDate:
		argTp1, retTp = types.ETDuration, types.ETString
	default:
		argTp1, retTp = types.ETString, types.ETString
	}
	switch tp2.Tp {
	case mysql.TypeDatetime, mysql.TypeDuration:
		argTp2 = types.ETDuration
	default:
		argTp2 = types.ETString
	}
	bf = newBaseBuiltinFuncWithTp(ctx, args, retTp, argTp1, argTp2)
	bf.tp.Decimal = tp1.Decimal
	if retTp == types.ETString {
		bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeString, mysql.MaxDatetimeWidthWithFsp, types.UnspecifiedLength
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

// isDuration returns a boolean indicating whether the str matches the format of duration.
// See https://dev.mysql.com/doc/refman/5.7/en/time.html
func isDuration(str string) bool {
	return durationPattern.MatchString(str)
}

// strDatetimeAddDuration adds duration to datetime string, returns a string value.
func strDatetimeAddDuration(sc *stmtctx.StatementContext, d string, arg1 types.Duration) (string, error) {
	arg0, err := types.ParseTime(sc, d, mysql.TypeDatetime, types.MaxFsp)
	if err != nil {
		return "", errors.Trace(err)
	}
	ret, err := arg0.Add(arg1)
	if err != nil {
		return "", errors.Trace(err)
	}
	fsp := types.MaxFsp
	if ret.Time.Microsecond() == 0 {
		fsp = types.MinFsp
	}
	ret.Fsp = fsp
	return ret.String(), nil
}

// strDurationAddDuration adds duration to duration string, returns a string value.
func strDurationAddDuration(sc *stmtctx.StatementContext, d string, arg1 types.Duration) (string, error) {
	arg0, err := types.ParseDuration(d, types.MaxFsp)
	if err != nil {
		return "", errors.Trace(err)
	}
	tmpDuration, err := arg0.Add(arg1)
	if err != nil {
		return "", errors.Trace(err)
	}
	tmpDuration.Fsp = types.MaxFsp
	if tmpDuration.MicroSecond() == 0 {
		tmpDuration.Fsp = types.MinFsp
	}
	return tmpDuration.String(), nil
}

// strDatetimeSubDuration subtracts duration from datetime string, returns a string value.
func strDatetimeSubDuration(sc *stmtctx.StatementContext, d string, arg1 types.Duration) (string, error) {
	arg0, err := types.ParseTime(sc, d, mysql.TypeDatetime, types.MaxFsp)
	if err != nil {
		return "", errors.Trace(err)
	}
	arg1time, err := arg1.ConvertToTime(uint8(getFsp(arg1.String())))
	if err != nil {
		return "", errors.Trace(err)
	}
	tmpDuration := arg0.Sub(&arg1time)
	fsp := types.MaxFsp
	if tmpDuration.MicroSecond() == 0 {
		fsp = types.MinFsp
	}
	resultDuration, err := tmpDuration.ConvertToTime(mysql.TypeDatetime)
	if err != nil {
		return "", errors.Trace(err)
	}
	resultDuration.Fsp = fsp
	return resultDuration.String(), nil
}

// strDurationSubDuration subtracts duration from duration string, returns a string value.
func strDurationSubDuration(sc *stmtctx.StatementContext, d string, arg1 types.Duration) (string, error) {
	arg0, err := types.ParseDuration(d, types.MaxFsp)
	if err != nil {
		return "", errors.Trace(err)
	}
	tmpDuration, err := arg0.Sub(arg1)
	if err != nil {
		return "", errors.Trace(err)
	}
	tmpDuration.Fsp = types.MaxFsp
	if tmpDuration.MicroSecond() == 0 {
		tmpDuration.Fsp = types.MinFsp
	}
	return tmpDuration.String(), nil
}

type addTimeFunctionClass struct {
	baseFunctionClass
}

func (c *addTimeFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	tp1, tp2, bf := getBf4TimeAddSub(ctx, args)
	switch tp1.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinAddDatetimeAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeDateTimeNullSig{bf}
		default:
			sig = &builtinAddDatetimeAndStringSig{bf}
		}
	case mysql.TypeDate:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinAddDateAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeStringNullSig{bf}
		default:
			sig = &builtinAddDateAndStringSig{bf}
		}
	case mysql.TypeDuration:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinAddDurationAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeDurationNullSig{bf}
		default:
			sig = &builtinAddDurationAndStringSig{bf}
		}
	default:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinAddStringAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeStringNullSig{bf}
		default:
			sig = &builtinAddStringAndStringSig{bf}
		}
	}
	return sig, nil
}

type builtinAddTimeDateTimeNullSig struct {
	baseBuiltinFunc
}

// evalTime evals a builtinAddTimeDateTimeNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeDateTimeNullSig) evalTime(row types.Row) (types.Time, bool, error) {
	return types.ZeroDatetime, true, nil
}

type builtinAddDatetimeAndDurationSig struct {
	baseBuiltinFunc
}

// evalTime evals a builtinAddDatetimeAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDatetimeAndDurationSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalDuration(row, sc)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, errors.Trace(err)
	}
	result, err := arg0.Add(arg1)
	return result, err != nil, errors.Trace(err)
}

type builtinAddDatetimeAndStringSig struct {
	baseBuiltinFunc
}

// evalTime evals a builtinAddDatetimeAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDatetimeAndStringSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, errors.Trace(err)
	}
	s, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, errors.Trace(err)
	}
	if !isDuration(s) {
		return types.ZeroDatetime, true, nil
	}
	arg1, err := types.ParseDuration(s, getFsp(s))
	if err != nil {
		return types.ZeroDatetime, true, errors.Trace(err)
	}
	result, err := arg0.Add(arg1)
	return result, err != nil, errors.Trace(err)
}

type builtinAddTimeDurationNullSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinAddTimeDurationNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeDurationNullSig) evalDuration(row types.Row) (types.Duration, bool, error) {
	return types.ZeroDuration, true, nil
}

type builtinAddDurationAndDurationSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDurationAndDurationSig) evalDuration(row types.Row) (types.Duration, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalDuration(row, sc)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalDuration(row, sc)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, errors.Trace(err)
	}
	result, err := arg0.Add(arg1)
	if err != nil {
		return types.ZeroDuration, true, errors.Trace(err)
	}
	return result, false, nil
}

type builtinAddDurationAndStringSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinAddDurationAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDurationAndStringSig) evalDuration(row types.Row) (types.Duration, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalDuration(row, sc)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, errors.Trace(err)
	}
	s, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, errors.Trace(err)
	}
	if !isDuration(s) {
		return types.ZeroDuration, true, nil
	}
	arg1, err := types.ParseDuration(s, getFsp(s))
	if err != nil {
		return types.ZeroDuration, true, errors.Trace(err)
	}
	result, err := arg0.Add(arg1)
	if err != nil {
		return types.ZeroDuration, true, errors.Trace(err)
	}
	return result, false, nil
}

type builtinAddTimeStringNullSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeStringNullSig) evalString(row types.Row) (string, bool, error) {
	return "", true, nil
}

type builtinAddStringAndDurationSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinAddStringAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddStringAndDurationSig) evalString(row types.Row) (result string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	var (
		arg0 string
		arg1 types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	arg1, isNull, err = b.args[1].EvalDuration(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	if isDuration(arg0) {
		result, err = strDurationAddDuration(sc, arg0, arg1)
		if err != nil {
			return "", true, errors.Trace(err)
		}
		return result, false, nil
	}
	result, err = strDatetimeAddDuration(sc, arg0, arg1)
	return result, err != nil, errors.Trace(err)
}

type builtinAddStringAndStringSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinAddStringAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddStringAndStringSig) evalString(row types.Row) (result string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	var (
		arg0, arg1Str string
		arg1          types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	arg1Str, isNull, err = b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	arg1, err = types.ParseDuration(arg1Str, getFsp4TimeAddSub(arg1Str))
	if err != nil {
		return "", true, errors.Trace(err)
	}
	if isDuration(arg0) {
		result, err = strDurationAddDuration(sc, arg0, arg1)
		if err != nil {
			return "", true, errors.Trace(err)
		}
		return result, false, nil
	}
	result, err = strDatetimeAddDuration(sc, arg0, arg1)
	return result, err != nil, errors.Trace(err)
}

type builtinAddDateAndDurationSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDateAndDurationSig) evalString(row types.Row) (string, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalDuration(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalDuration(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	result, err := arg0.Add(arg1)
	return result.String(), err != nil, errors.Trace(err)
}

type builtinAddDateAndStringSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinAddDateAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDateAndStringSig) evalString(row types.Row) (string, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalDuration(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	s, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	if !isDuration(s) {
		return "", true, nil
	}
	arg1, err := types.ParseDuration(s, getFsp4TimeAddSub(s))
	if err != nil {
		return "", true, errors.Trace(err)
	}
	result, err := arg0.Add(arg1)
	return result.String(), err != nil, errors.Trace(err)
}

type convertTzFunctionClass struct {
	baseFunctionClass
}

func (c *convertTzFunctionClass) getDecimal(ctx context.Context, arg Expression) int {
	decimal := types.MaxFsp
	if dt, isConstant := arg.(*Constant); isConstant {
		switch arg.GetType().EvalType() {
		case types.ETInt:
			decimal = 0
		case types.ETReal, types.ETDecimal:
			decimal = arg.GetType().Decimal
		case types.ETString:
			str, isNull, err := dt.EvalString(nil, ctx.GetSessionVars().StmtCtx)
			if err == nil && !isNull {
				decimal = types.DateFSP(str)
			}
		}
	}
	if decimal > types.MaxFsp {
		return types.MaxFsp
	}
	if decimal < types.MinFsp {
		return types.MinFsp
	}
	return decimal
}

func (c *convertTzFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	// tzRegex holds the regex to check whether a string is a time zone.
	tzRegex, err := regexp.Compile(`(^(\+|-)(0?[0-9]|1[0-2]):[0-5]?\d$)|(^\+13:00$)`)
	if err != nil {
		return nil, errors.Trace(err)
	}

	decimal := c.getDecimal(ctx, args[0])
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETDatetime, types.ETString, types.ETString)
	bf.tp.Decimal = decimal
	sig := &builtinConvertTzSig{
		baseBuiltinFunc: bf,
		timezoneRegex:   tzRegex,
	}
	return sig, nil
}

type builtinConvertTzSig struct {
	baseBuiltinFunc
	timezoneRegex *regexp.Regexp
}

// evalTime evals CONVERT_TZ(dt,from_tz,to_tz).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_convert-tz
func (b *builtinConvertTzSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	dt, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	fromTzStr, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	toTzStr, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	fromTzMatched := b.timezoneRegex.MatchString(fromTzStr)
	toTzMatched := b.timezoneRegex.MatchString(toTzStr)

	if !fromTzMatched && !toTzMatched {
		fromTz, err := time.LoadLocation(fromTzStr)
		if err != nil {
			return types.Time{}, true, errors.Trace(err)
		}

		toTz, err := time.LoadLocation(toTzStr)
		if err != nil {
			return types.Time{}, true, errors.Trace(err)
		}

		t, err := dt.Time.GoTime(fromTz)
		if err != nil {
			return types.Time{}, true, errors.Trace(err)
		}

		return types.Time{
			Time: types.FromGoTime(t.In(toTz)),
			Type: mysql.TypeDatetime,
			Fsp:  b.tp.Decimal,
		}, false, nil
	}
	if fromTzMatched && toTzMatched {
		t, err := dt.Time.GoTime(time.Local)
		if err != nil {
			return types.Time{}, true, errors.Trace(err)
		}

		return types.Time{
			Time: types.FromGoTime(t.Add(timeZone2Duration(toTzStr) - timeZone2Duration(fromTzStr))),
			Type: mysql.TypeDatetime,
			Fsp:  b.tp.Decimal,
		}, false, nil
	}
	return types.Time{}, true, nil
}

type makeDateFunctionClass struct {
	baseFunctionClass
}

func (c *makeDateFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETInt, types.ETInt)
	tp := bf.tp
	tp.Tp, tp.Flen, tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, 0
	sig := &builtinMakeDateSig{bf}
	return sig, nil
}

type builtinMakeDateSig struct {
	baseBuiltinFunc
}

// evalTime evaluates a builtinMakeDateSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_makedate
func (b *builtinMakeDateSig) evalTime(row types.Row) (d types.Time, isNull bool, err error) {
	args := b.getArgs()
	sc := b.ctx.GetSessionVars().StmtCtx
	var year, dayOfYear int64
	year, isNull, err = args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return d, true, errors.Trace(err)
	}
	dayOfYear, isNull, err = args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return d, true, errors.Trace(err)
	}
	if dayOfYear <= 0 || year < 0 || year > 9999 {
		return d, true, nil
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
		return d, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(startTime.String())))
	}
	ret := types.TimeFromDays(retTimestamp + dayOfYear - 1)
	if ret.IsZero() || ret.Time.Year() > 9999 {
		return d, true, nil
	}
	return ret, false, nil
}

type makeTimeFunctionClass struct {
	baseFunctionClass
}

func (c *makeTimeFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	tp, flen, decimal := args[2].GetType().EvalType(), 10, 0
	switch tp {
	case types.ETInt:
	case types.ETReal, types.ETDecimal:
		decimal = args[2].GetType().Decimal
		if decimal > 6 || decimal == types.UnspecifiedLength {
			decimal = 6
		}
		if decimal > 0 {
			flen += 1 + decimal
		}
	default:
		flen, decimal = 17, 6
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, types.ETInt, types.ETInt, types.ETReal)
	bf.tp.Flen, bf.tp.Decimal = flen, decimal
	sig := &builtinMakeTimeSig{bf}
	return sig, nil
}

type builtinMakeTimeSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinMakeTimeIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_maketime
func (b *builtinMakeTimeSig) evalDuration(row types.Row) (types.Duration, bool, error) {
	sc, dur := b.ctx.GetSessionVars().StmtCtx, types.ZeroDuration
	dur.Fsp = types.MaxFsp
	hour, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return dur, isNull, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	minute, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return dur, isNull, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	if minute < 0 || minute >= 60 {
		return dur, true, nil
	}
	second, isNull, err := b.args[2].EvalReal(row, sc)
	if isNull || err != nil {
		return dur, isNull, errors.Trace(err)
	}
	if second < 0 || second >= 60 {
		return dur, true, nil
	}
	var overflow bool
	// MySQL TIME datatype: https://dev.mysql.com/doc/refman/5.7/en/time.html
	// ranges from '-838:59:59.000000' to '838:59:59.000000'
	if hour < -838 {
		hour = -838
		overflow = true
	} else if hour > 838 {
		hour = 838
		overflow = true
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
	fsp := b.tp.Decimal
	dur, err = types.ParseDuration(fmt.Sprintf("%02d:%02d:%v", hour, minute, second), fsp)
	if err != nil {
		return dur, true, errors.Trace(err)
	}
	return dur, false, nil
}

type periodAddFunctionClass struct {
	baseFunctionClass
}

func (c *periodAddFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	bf.tp.Flen = 6
	sig := &builtinPeriodAddSig{bf}
	return sig, nil
}

// period2Month converts a period to months, in which period is represented in the format of YYMM or YYYYMM.
// Note that the period argument is not a date value.
func period2Month(period uint64) uint64 {
	if period == 0 {
		return 0
	}

	year, month := period/100, period%100
	if year < 70 {
		year += 2000
	} else if year < 100 {
		year += 1900
	}

	return year*12 + month - 1
}

// month2Period converts a month to a period.
func month2Period(month uint64) uint64 {
	if month == 0 {
		return 0
	}

	year := month / 12
	if year < 70 {
		year += 2000
	} else if year < 100 {
		year += 1900
	}

	return year*100 + month%12 + 1
}

type builtinPeriodAddSig struct {
	baseBuiltinFunc
}

// evalInt evals PERIOD_ADD(P,N).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-add
func (b *builtinPeriodAddSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	p, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, true, errors.Trace(err)
	}

	if p == 0 {
		return 0, false, nil
	}

	n, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, true, errors.Trace(err)
	}

	sumMonth := int64(period2Month(uint64(p))) + n
	return int64(month2Period(uint64(sumMonth))), false, nil
}

type periodDiffFunctionClass struct {
	baseFunctionClass
}

func (c *periodDiffFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	bf.tp.Flen = 6
	sig := &builtinPeriodDiffSig{bf}
	return sig, nil
}

type builtinPeriodDiffSig struct {
	baseBuiltinFunc
}

// evalInt evals PERIOD_DIFF(P1,P2).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-diff
func (b *builtinPeriodDiffSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	p1, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	p2, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	return int64(period2Month(uint64(p1)) - period2Month(uint64(p2))), false, nil
}

type quarterFunctionClass struct {
	baseFunctionClass
}

func (c *quarterFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	bf.tp.Flen = 1

	sig := &builtinQuarterSig{bf}
	return sig, nil
}

type builtinQuarterSig struct {
	baseBuiltinFunc
}

// evalInt evals QUARTER(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_quarter
func (b *builtinQuarterSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	date, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}

	if date.IsZero() {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(date.String())))
	}

	return int64((date.Time.Month() + 2) / 3), false, nil
}

type secToTimeFunctionClass struct {
	baseFunctionClass
}

func (c *secToTimeFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	var retFlen, retFsp int
	argType := args[0].GetType()
	argEvalTp := argType.EvalType()
	if argEvalTp == types.ETString {
		retFsp = types.UnspecifiedLength
	} else {
		retFsp = argType.Decimal
	}
	if retFsp > types.MaxFsp || retFsp == types.UnspecifiedLength {
		retFsp = types.MaxFsp
	} else if retFsp < types.MinFsp {
		retFsp = types.MinFsp
	}
	retFlen = 10
	if retFsp > 0 {
		retFlen += 1 + retFsp
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, types.ETReal)
	bf.tp.Flen, bf.tp.Decimal = retFlen, retFsp
	sig := &builtinSecToTimeSig{bf}
	return sig, nil
}

type builtinSecToTimeSig struct {
	baseBuiltinFunc
}

// evalDuration evals SEC_TO_TIME(seconds).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sec-to-time
func (b *builtinSecToTimeSig) evalDuration(row types.Row) (types.Duration, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	secondsFloat, isNull, err := b.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return types.Duration{}, isNull, errors.Trace(err)
	}
	var (
		hour          int64
		minute        int64
		second        int64
		demical       float64
		secondDemical float64
		negative      string
	)

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
	dur, err = types.ParseDuration(fmt.Sprintf("%s%02d:%02d:%v", negative, hour, minute, secondDemical), b.tp.Decimal)
	if err != nil {
		return types.Duration{}, err != nil, errors.Trace(err)
	}
	return dur, false, nil
}

type subTimeFunctionClass struct {
	baseFunctionClass
}

func (c *subTimeFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	tp1, tp2, bf := getBf4TimeAddSub(ctx, args)
	switch tp1.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinSubDatetimeAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeDateTimeNullSig{bf}
		default:
			sig = &builtinSubDatetimeAndStringSig{bf}
		}
	case mysql.TypeDate:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinSubDateAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeStringNullSig{bf}
		default:
			sig = &builtinSubDateAndStringSig{bf}
		}
	case mysql.TypeDuration:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinSubDurationAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeDurationNullSig{bf}
		default:
			sig = &builtinSubDurationAndStringSig{bf}
		}
	default:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinSubStringAndDurationSig{bf}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeStringNullSig{bf}
		default:
			sig = &builtinSubStringAndStringSig{bf}
		}
	}
	return sig, nil
}

type builtinSubDatetimeAndDurationSig struct {
	baseBuiltinFunc
}

// evalTime evals a builtinSubDatetimeAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDatetimeAndDurationSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalDuration(row, sc)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, errors.Trace(err)
	}
	arg1time, err := arg1.ConvertToTime(mysql.TypeDatetime)
	if err != nil {
		return arg1time, true, errors.Trace(err)
	}
	tmpDuration := arg0.Sub(&arg1time)
	result, err := tmpDuration.ConvertToTime(arg0.Type)
	return result, err != nil, errors.Trace(err)
}

type builtinSubDatetimeAndStringSig struct {
	baseBuiltinFunc
}

// evalTime evals a builtinSubDatetimeAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDatetimeAndStringSig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, errors.Trace(err)
	}
	s, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, errors.Trace(err)
	}
	if err != nil {
		return types.ZeroDatetime, true, errors.Trace(err)
	}
	if !isDuration(s) {
		return types.ZeroDatetime, true, nil
	}
	arg1, err := types.ParseDuration(s, getFsp(s))
	if err != nil {
		return types.ZeroDatetime, true, errors.Trace(err)
	}
	arg1time, err := arg1.ConvertToTime(mysql.TypeDatetime)
	if err != nil {
		return types.ZeroDatetime, true, errors.Trace(err)
	}
	tmpDuration := arg0.Sub(&arg1time)
	result, err := tmpDuration.ConvertToTime(mysql.TypeDatetime)
	return result, err != nil, errors.Trace(err)
}

type builtinSubTimeDateTimeNullSig struct {
	baseBuiltinFunc
}

// evalTime evals a builtinSubTimeDateTimeNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeDateTimeNullSig) evalTime(row types.Row) (types.Time, bool, error) {
	return types.ZeroDatetime, true, nil
}

type builtinSubStringAndDurationSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinSubStringAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubStringAndDurationSig) evalString(row types.Row) (result string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	var (
		arg0 string
		arg1 types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	arg1, isNull, err = b.args[1].EvalDuration(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	if isDuration(arg0) {
		result, err = strDurationSubDuration(sc, arg0, arg1)
		if err != nil {
			return "", true, errors.Trace(err)
		}
		return result, false, nil
	}
	result, err = strDatetimeSubDuration(sc, arg0, arg1)
	return result, err != nil, errors.Trace(err)
}

type builtinSubStringAndStringSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinAddStringAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubStringAndStringSig) evalString(row types.Row) (result string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	var (
		s, arg0 string
		arg1    types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	s, isNull, err = b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	arg1, err = types.ParseDuration(s, getFsp4TimeAddSub(s))
	if err != nil {
		return "", true, errors.Trace(err)
	}
	if isDuration(arg0) {
		result, err = strDurationSubDuration(sc, arg0, arg1)
		if err != nil {
			return "", true, errors.Trace(err)
		}
		return result, false, nil
	}
	result, err = strDatetimeSubDuration(sc, arg0, arg1)
	return result, err != nil, errors.Trace(err)
}

type builtinSubTimeStringNullSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinSubTimeStringNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeStringNullSig) evalString(row types.Row) (string, bool, error) {
	return "", true, nil
}

type builtinSubDurationAndDurationSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDurationAndDurationSig) evalDuration(row types.Row) (types.Duration, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalDuration(row, sc)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalDuration(row, sc)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, errors.Trace(err)
	}
	result, err := arg0.Sub(arg1)
	if err != nil {
		return types.ZeroDuration, true, errors.Trace(err)
	}
	return result, false, nil
}

type builtinSubDurationAndStringSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinAddDurationAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDurationAndStringSig) evalDuration(row types.Row) (types.Duration, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalDuration(row, sc)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, errors.Trace(err)
	}
	s, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, errors.Trace(err)
	}
	if !isDuration(s) {
		return types.ZeroDuration, true, nil
	}
	arg1, err := types.ParseDuration(s, getFsp(s))
	if err != nil {
		return types.ZeroDuration, true, errors.Trace(err)
	}
	result, err := arg0.Sub(arg1)
	return result, err != nil, errors.Trace(err)
}

type builtinSubTimeDurationNullSig struct {
	baseBuiltinFunc
}

// evalTime evals a builtinSubTimeDurationNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeDurationNullSig) evalDuration(row types.Row) (types.Duration, bool, error) {
	return types.ZeroDuration, true, nil
}

type builtinSubDateAndDurationSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinAddDateAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDateAndDurationSig) evalString(row types.Row) (string, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalDuration(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalDuration(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	result, err := arg0.Sub(arg1)
	return result.String(), err != nil, errors.Trace(err)
}

type builtinSubDateAndStringSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinAddDateAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDateAndStringSig) evalString(row types.Row) (string, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalDuration(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	s, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	if !isDuration(s) {
		return "", true, nil
	}
	arg1, err := types.ParseDuration(s, getFsp4TimeAddSub(s))
	if err != nil {
		return "", true, errors.Trace(err)
	}
	result, err := arg0.Sub(arg1)
	if err != nil {
		return "", true, errors.Trace(err)
	}
	return result.String(), false, nil
}

type timeFormatFunctionClass struct {
	baseFunctionClass
}

func (c *timeFormatFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETDuration, types.ETString)
	// worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
	bf.tp.Flen = (args[1].GetType().Flen + 1) / 2 * 11
	sig := &builtinTimeFormatSig{bf}
	return sig, nil
}

type builtinTimeFormatSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinTimeFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-format
func (b *builtinTimeFormatSig) evalString(row types.Row) (string, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(row, b.ctx.GetSessionVars().StmtCtx)
	// if err != nil, then dur is ZeroDuration, outputs 00:00:00 in this case which follows the behavior of mysql.
	if err != nil {
		log.Warnf("Expression.EvalDuration() in time_format() failed, due to %s", err.Error())
	}
	if isNull {
		return "", isNull, errors.Trace(err)
	}
	formatMask, isNull, err := b.args[1].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil || isNull {
		return "", isNull, errors.Trace(err)
	}
	res, err := b.formatTime(dur, formatMask, b.ctx)
	return res, isNull, errors.Trace(err)
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-format
func (b *builtinTimeFormatSig) formatTime(t types.Duration, formatMask string, ctx context.Context) (res string, err error) {
	t2 := types.Time{
		Time: types.FromDate(0, 0, 0, t.Hour(), t.Minute(), t.Second(), t.MicroSecond()),
		Type: mysql.TypeDate, Fsp: 0}

	str, err := t2.DateFormat(formatMask)
	return str, errors.Trace(err)
}

type timeToSecFunctionClass struct {
	baseFunctionClass
}

func (c *timeToSecFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDuration)
	bf.tp.Flen = 10
	sig := &builtinTimeToSecSig{bf}
	return sig, nil
}

type builtinTimeToSecSig struct {
	baseBuiltinFunc
}

// evalInt evals TIME_TO_SEC(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-to-sec
func (b *builtinTimeToSecSig) evalInt(row types.Row) (int64, bool, error) {
	ctx := b.getCtx().GetSessionVars().StmtCtx
	duration, isNull, err := b.args[0].EvalDuration(row, ctx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	var sign int
	if duration.Duration >= 0 {
		sign = 1
	} else {
		sign = -1
	}
	return int64(sign * (duration.Hour()*3600 + duration.Minute()*60 + duration.Second())), false, nil
}

type timestampAddFunctionClass struct {
	baseFunctionClass
}

func (c *timestampAddFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETString, types.ETString, types.ETInt, types.ETDatetime)
	bf.tp = &types.FieldType{Tp: mysql.TypeString, Flen: mysql.MaxDatetimeWidthNoFsp, Decimal: types.UnspecifiedLength}
	sig := &builtinTimestampAddSig{bf}
	return sig, nil

}

type builtinTimestampAddSig struct {
	baseBuiltinFunc
}

// evalString evals a builtinTimestampAddSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampadd
func (b *builtinTimestampAddSig) evalString(row types.Row) (string, bool, error) {
	ctx := b.getCtx().GetSessionVars().StmtCtx
	unit, isNull, err := b.args[0].EvalString(row, ctx)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	v, isNull, err := b.args[1].EvalInt(row, ctx)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	arg, isNull, err := b.args[2].EvalTime(row, ctx)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	tm1, err := arg.Time.GoTime(time.Local)
	if err != nil {
		return "", isNull, errors.Trace(err)
	}
	var tb time.Time
	fsp := types.DefaultFsp
	switch unit {
	case "MICROSECOND":
		tb = tm1.Add(time.Duration(v) * time.Microsecond)
		fsp = types.MaxFsp
	case "SECOND":
		tb = tm1.Add(time.Duration(v) * time.Second)
	case "MINUTE":
		tb = tm1.Add(time.Duration(v) * time.Minute)
	case "HOUR":
		tb = tm1.Add(time.Duration(v) * time.Hour)
	case "DAY":
		tb = tm1.AddDate(0, 0, int(v))
	case "WEEK":
		tb = tm1.AddDate(0, 0, 7*int(v))
	case "MONTH":
		tb = tm1.AddDate(0, int(v), 0)
	case "QUARTER":
		tb = tm1.AddDate(0, 3*int(v), 0)
	case "YEAR":
		tb = tm1.AddDate(int(v), 0, 0)
	default:
		return "", true, errors.Trace(types.ErrInvalidTimeFormat)
	}
	r := types.Time{Time: types.FromGoTime(tb), Type: mysql.TypeDatetime, Fsp: fsp}
	if err = r.Check(); err != nil {
		return "", true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	return r.String(), false, nil
}

type toDaysFunctionClass struct {
	baseFunctionClass
}

func (c *toDaysFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	sig := &builtinToDaysSig{bf}
	return sig, nil
}

type builtinToDaysSig struct {
	baseBuiltinFunc
}

// evalInt evals a builtinToDaysSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-days
func (b *builtinToDaysSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg, isNull, err := b.args[0].EvalTime(row, sc)

	if isNull || err != nil {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	ret := types.TimestampDiff("DAY", types.ZeroDate, arg)
	if ret == 0 {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(arg.String())))
	}
	return ret, false, nil
}

type toSecondsFunctionClass struct {
	baseFunctionClass
}

func (c *toSecondsFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETDatetime)
	sig := &builtinToSecondsSig{bf}
	return sig, nil
}

type builtinToSecondsSig struct {
	baseBuiltinFunc
}

// evalInt evals a builtinToSecondsSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-seconds
func (b *builtinToSecondsSig) evalInt(row types.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	ret := types.TimestampDiff("SECOND", types.ZeroDate, arg)
	if ret == 0 {
		return 0, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(arg.String())))
	}
	return ret, false, nil
}

type utcTimeFunctionClass struct {
	baseFunctionClass
}

func (c *utcTimeFunctionClass) getFlenAndDecimal4UTCTime(sc *stmtctx.StatementContext, args []Expression) (flen, decimal int) {
	if len(args) == 0 {
		flen, decimal = 8, 0
		return
	}
	if constant, ok := args[0].(*Constant); ok {
		fsp, isNull, err := constant.EvalInt(nil, sc)
		if isNull || err != nil || fsp > int64(types.MaxFsp) {
			decimal = types.MaxFsp
		} else if fsp < int64(types.MinFsp) {
			decimal = types.MinFsp
		} else {
			decimal = int(fsp)
		}
	}
	if decimal > 0 {
		flen = 8 + 1 + decimal
	} else {
		flen = 8
	}
	return flen, decimal
}

func (c *utcTimeFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDuration, argTps...)
	bf.tp.Flen, bf.tp.Decimal = c.getFlenAndDecimal4UTCTime(bf.ctx.GetSessionVars().StmtCtx, args)

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinUTCTimeWithArgSig{bf}
	} else {
		sig = &builtinUTCTimeWithoutArgSig{bf}
	}
	return sig, nil
}

type builtinUTCTimeWithoutArgSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinUTCTimeWithoutArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithoutArgSig) evalDuration(row types.Row) (types.Duration, bool, error) {
	v, err := types.ParseDuration(time.Now().UTC().Format(types.TimeFormat), 0)
	return v, false, err
}

type builtinUTCTimeWithArgSig struct {
	baseBuiltinFunc
}

// evalDuration evals a builtinUTCTimeWithArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithArgSig) evalDuration(row types.Row) (types.Duration, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	fsp, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return types.Duration{}, isNull, errors.Trace(err)
	}
	if fsp > int64(types.MaxFsp) {
		return types.Duration{}, true, errors.Errorf("Too-big precision %v specified for 'utc_time'. Maximum is %v.", fsp, types.MaxFsp)
	}
	if fsp < int64(types.MinFsp) {
		return types.Duration{}, true, errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
	}
	v, err := types.ParseDuration(time.Now().UTC().Format(types.TimeFSPFormat), int(fsp))
	return v, false, err
}

type lastDayFunctionClass struct {
	baseFunctionClass
}

func (c *lastDayFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETDatetime, types.ETDatetime)
	bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, types.DefaultFsp
	sig := &builtinLastDaySig{bf}
	return sig, nil
}

type builtinLastDaySig struct {
	baseBuiltinFunc
}

// evalTime evals a builtinLastDaySig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_last-day
func (b *builtinLastDaySig) evalTime(row types.Row) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg, isNull, err := b.args[0].EvalTime(row, sc)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	tm := arg.Time
	year, month, day := tm.Year(), tm.Month(), 30
	if year == 0 && month == 0 && tm.Day() == 0 {
		return types.Time{}, true, errors.Trace(handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(arg.String())))
	}
	if month == 1 || month == 3 || month == 5 ||
		month == 7 || month == 8 || month == 10 || month == 12 {
		day = 31
	} else if month == 2 {
		day = 28
		if tm.IsLeapYear() {
			day = 29
		}
	}
	ret := types.Time{
		Time: types.FromDate(year, month, day, 0, 0, 0, 0),
		Type: mysql.TypeDate,
		Fsp:  types.DefaultFsp,
	}
	return ret, false, nil
}
