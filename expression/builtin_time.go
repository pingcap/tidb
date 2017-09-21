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

	log "github.com/Sirupsen/logrus"
	"github.com/cznic/mathutil"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
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

// handleInvalidTimeError reports error or warning depend on the context.
func handleInvalidTimeError(ctx context.Context, err error) error {
	if err == nil || !(terror.ErrorEqual(err, types.ErrInvalidTimeFormat) || types.ErrIncorrectDatetimeValue.Equal(err)) {
		return err
	}
	sc := ctx.GetSessionVars().StmtCtx
	if ctx.GetSessionVars().StrictSQLMode && (sc.InInsertStmt || sc.InUpdateOrDeleteStmt) {
		return err
	}
	sc.AppendWarning(err)
	return nil
}

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

func convertToTimeWithFsp(sc *variable.StatementContext, arg types.Datum, tp byte, fsp int) (d types.Datum, err error) {
	if fsp > types.MaxFsp {
		fsp = types.MaxFsp
	}

	f := types.NewFieldType(tp)
	f.Decimal = fsp

	d, err = arg.ConvertTo(sc, f)
	if err != nil {
		d.SetNull()
		return d, errors.Trace(err)
	}

	if d.IsNull() {
		return
	}

	if d.Kind() != types.KindMysqlTime {
		d.SetNull()
		return d, errors.Errorf("need time type, but got %T", d.GetValue())
	}
	return
}

func convertToTime(sc *variable.StatementContext, arg types.Datum, tp byte) (d types.Datum, err error) {
	return convertToTimeWithFsp(sc, arg, tp, types.MaxFsp)
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
		return
	}

	if d.Kind() != types.KindMysqlDuration {
		d.SetNull()
		return d, errors.Errorf("need duration type, but got %T", d.GetValue())
	}
	return
}

type dateFunctionClass struct {
	baseFunctionClass
}

func (c *dateFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDatetime, tpDatetime)
	bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, 10, 0
	sig := &builtinDateSig{baseTimeBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinDateSig struct {
	baseTimeBuiltinFunc
}

// evalTime evals DATE(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date
func (b *builtinDateSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	constant, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for date literal")
	}
	str := constant.Value.GetString()
	if !datePattern.MatchString(str) {
		return nil, types.ErrIncorrectDatetimeValue.GenByArgs(str)
	}
	tm, err := types.ParseDate(str)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp([]Expression{}, ctx, tpDatetime)
	bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, 10, 0
	sig := &builtinDateLiteralSig{baseTimeBuiltinFunc{bf}, tm}
	return sig.setSelf(sig), nil
}

type builtinDateLiteralSig struct {
	baseTimeBuiltinFunc
	literal types.Time
}

// evalTime evals DATE 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinDateLiteralSig) evalTime(row []types.Datum) (types.Time, bool, error) {
	mode := b.getCtx().GetSessionVars().SQLMode
	if mode.HasNoZeroDateMode() && b.literal.IsZero() {
		return b.literal, true, types.ErrIncorrectDatetimeValue.GenByArgs(b.literal.String())
	}
	if mode.HasNoZeroInDateMode() && (b.literal.InvalidZero() && !b.literal.IsZero()) {
		return b.literal, true, types.ErrIncorrectDatetimeValue.GenByArgs(b.literal.String())
	}
	return b.literal, false, nil
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

func (c *dateDiffFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDatetime, tpDatetime)
	sig := &builtinDateDiffSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinDateDiffSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinDateDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_datediff
func (b *builtinDateDiffSig) evalInt(row []types.Datum) (int64, bool, error) {
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

func (c *timeDiffFunctionClass) getArgEvalTp(fieldTp *types.FieldType) evalTp {
	argTp := tpString
	switch tp := fieldTp2EvalTp(fieldTp); tp {
	case tpDuration, tpDatetime, tpTimestamp:
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDuration, arg0Tp, arg1Tp)
	bf.tp.Decimal = mathutil.Max(arg0FieldTp.Decimal, arg1FieldTp.Decimal)

	var sig builtinFunc
	// arg0 and arg1 must be the same time type(compatible), or timediff will return NULL.
	// TODO: we don't really need Duration type, actually in MySQL, it use Time class to represent
	// all the time type, and use filed type to distinguish datetime, date, timestamp or time(duration).
	// With the duration type, we are hard to port all the MySQL behavior.
	switch arg0Tp {
	case tpDuration:
		switch arg1Tp {
		case tpDuration:
			sig = &builtinDurationDurationTimeDiffSig{baseDurationBuiltinFunc{bf}}
		case tpDatetime, tpTimestamp:
			sig = &builtinNullTimeDiffSig{baseDurationBuiltinFunc{bf}}
		default:
			sig = &builtinDurationStringTimeDiffSig{baseDurationBuiltinFunc{bf}}
		}
	case tpDatetime, tpTimestamp:
		switch arg1Tp {
		case tpDuration:
			sig = &builtinNullTimeDiffSig{baseDurationBuiltinFunc{bf}}
		case tpDatetime, tpTimestamp:
			sig = &builtinTimeTimeTimeDiffSig{baseDurationBuiltinFunc{bf}}
		default:
			sig = &builtinTimeStringTimeDiffSig{baseDurationBuiltinFunc{bf}}
		}
	default:
		switch arg1Tp {
		case tpDuration:
			sig = &builtinStringDurationTimeDiffSig{baseDurationBuiltinFunc{bf}}
		case tpDatetime, tpTimestamp:
			sig = &builtinStringTimeTimeDiffSig{baseDurationBuiltinFunc{bf}}
		default:
			sig = &builtinStringStringTimeDiffSig{baseDurationBuiltinFunc{bf}}
		}
	}
	return sig.setSelf(sig), nil
}

type builtinDurationDurationTimeDiffSig struct {
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinDurationDurationTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinDurationDurationTimeDiffSig) evalDuration(row []types.Datum) (d types.Duration, isNull bool, err error) {
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
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinTimeTimeTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinTimeTimeTimeDiffSig) evalDuration(row []types.Datum) (d types.Duration, isNull bool, err error) {
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
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinDurationStringTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinDurationStringTimeDiffSig) evalDuration(row []types.Datum) (d types.Duration, isNull bool, err error) {
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
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinStringDurationTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinStringDurationTimeDiffSig) evalDuration(row []types.Datum) (d types.Duration, isNull bool, err error) {
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
func calculateTimeDiff(sc *variable.StatementContext, lhs, rhs types.Time) (d types.Duration, isNull bool, err error) {
	d = lhs.Sub(&rhs)
	d.Duration, err = types.TruncateOverflowMySQLTime(d.Duration)
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = sc.HandleTruncate(err)
	}
	return d, err != nil, errors.Trace(err)
}

// calculateTimeDiff calculates interval difference of two types.Duration.
func calculateDurationTimeDiff(sc *variable.StatementContext, lhs, rhs types.Duration) (d types.Duration, isNull bool, err error) {
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
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinTimeStringTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinTimeStringTimeDiffSig) evalDuration(row []types.Datum) (d types.Duration, isNull bool, err error) {
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
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinStringTimeTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinStringTimeTimeDiffSig) evalDuration(row []types.Datum) (d types.Duration, isNull bool, err error) {
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
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinStringStringTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinStringStringTimeDiffSig) evalDuration(row []types.Datum) (d types.Duration, isNull bool, err error) {
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
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinNullTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinNullTimeDiffSig) evalDuration(row []types.Datum) (d types.Duration, isNull bool, err error) {
	return d, true, nil
}

// convertStringToDuration converts string to duration, it return types.Time because in some case
// it will converts string to datetime.
func convertStringToDuration(sc *variable.StatementContext, str string, fsp int) (d types.Duration, t types.Time,
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpDatetime, tpString)
	// worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
	bf.tp.Flen = (args[1].GetType().Flen + 1) / 2 * 11
	sig := &builtinDateFormatSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil

}

type builtinDateFormatSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinDateFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func (b *builtinDateFormatSig) evalString(row []types.Datum) (string, bool, error) {
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

// builtinDateFormat ...
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func builtinDateFormat(ctx context.Context, args []types.Datum) (d types.Datum, err error) {
	date, err := convertToTime(ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDatetime)
	if err != nil {
		return d, errors.Trace(err)
	}

	if date.IsNull() {
		return
	}
	t := date.GetMysqlTime()
	str, err := t.DateFormat(args[1].GetString())
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetString(str)
	return
}

type fromDaysFunctionClass struct {
	baseFunctionClass
}

func (c *fromDaysFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDatetime, tpInt)
	bf.tp.Flen, bf.tp.Decimal = 10, 0
	sig := &builtinFromDaysSig{baseTimeBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinFromDaysSig struct {
	baseTimeBuiltinFunc
}

// evalTime evals FROM_DAYS(N).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-days
func (b *builtinFromDaysSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDuration)
	bf.tp.Flen, bf.tp.Decimal = 3, 0
	sig := &builtinHourSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinHourSig struct {
	baseIntBuiltinFunc
}

// evalInt evals HOUR(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_hour
func (b *builtinHourSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDuration)
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinMinuteSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinMinuteSig struct {
	baseIntBuiltinFunc
}

// evalInt evals MINUTE(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_minute
func (b *builtinMinuteSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDuration)
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinSecondSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinSecondSig struct {
	baseIntBuiltinFunc
}

// evalInt evals SECOND(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_second
func (b *builtinSecondSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDuration)
	bf.tp.Flen, bf.tp.Decimal = 6, 0
	sig := &builtinMicroSecondSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinMicroSecondSig struct {
	baseIntBuiltinFunc
}

// evalInt evals MICROSECOND(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_microsecond
func (b *builtinMicroSecondSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDatetime)
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinMonthSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinMonthSig struct {
	baseIntBuiltinFunc
}

// evalInt evals MONTH(date).
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_month
func (b *builtinMonthSig) evalInt(row []types.Datum) (int64, bool, error) {
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

// builtinMonth ...
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_month
func builtinMonth(args []types.Datum, ctx context.Context) (d types.Datum, err error) {
	d, err = convertToTime(ctx.GetSessionVars().StmtCtx, args[0], mysql.TypeDate)
	if err != nil || d.IsNull() {
		return d, errors.Trace(err)
	}

	// No need to check type here.
	t := d.GetMysqlTime()
	i := int64(0)
	if t.IsZero() {
		d.SetInt64(i)
		return
	}
	i = int64(t.Time.Month())
	d.SetInt64(i)
	return
}

// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_monthname
type monthNameFunctionClass struct {
	baseFunctionClass
}

func (c *monthNameFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpDatetime)
	bf.tp.Flen = 10
	sig := &builtinMonthNameSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinMonthNameSig struct {
	baseStringBuiltinFunc
}

func (b *builtinMonthNameSig) evalString(row []types.Datum) (string, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpDatetime)
	bf.tp.Flen = 10
	sig := &builtinDayNameSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinDayNameSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinDayNameSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayname
func (b *builtinDayNameSig) evalString(row []types.Datum) (string, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDatetime)
	bf.tp.Flen = 2
	sig := &builtinDayOfMonthSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinDayOfMonthSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinDayOfMonthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofmonth
func (b *builtinDayOfMonthSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDatetime)
	bf.tp.Flen = 1
	sig := &builtinDayOfWeekSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinDayOfWeekSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinDayOfWeekSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofweek
func (b *builtinDayOfWeekSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDatetime)
	bf.tp.Flen = 3
	sig := &builtinDayOfYearSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinDayOfYearSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinDayOfYearSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofyear
func (b *builtinDayOfYearSig) evalInt(row []types.Datum) (int64, bool, error) {
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

	argTps := []evalTp{tpDatetime}
	if len(args) == 2 {
		argTps = append(argTps, tpInt)
	}

	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, argTps...)

	bf.tp.Flen, bf.tp.Decimal = 2, 0

	var sig builtinFunc
	if len(args) == 2 {
		sig = &builtinWeekWithModeSig{baseIntBuiltinFunc{bf}}
	} else {
		sig = &builtinWeekWithoutModeSig{baseIntBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), nil
}

type builtinWeekWithModeSig struct {
	baseIntBuiltinFunc
}

// evalInt evals WEEK(date, mode).
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_week
func (b *builtinWeekWithModeSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	baseIntBuiltinFunc
}

// evalInt evals WEEK(date).
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_week
func (b *builtinWeekWithoutModeSig) evalInt(row []types.Datum) (int64, bool, error) {
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

	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDatetime)
	bf.tp.Flen = 1

	sig := &builtinWeekDaySig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinWeekDaySig struct {
	baseIntBuiltinFunc
}

// evalInt evals WEEKDAY(date).
func (b *builtinWeekDaySig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDatetime)
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinWeekOfYearSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinWeekOfYearSig struct {
	baseIntBuiltinFunc
}

// evalInt evals WEEKOFYEAR(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_weekofyear
func (b *builtinWeekOfYearSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDatetime)
	bf.tp.Flen, bf.tp.Decimal = 4, 0
	sig := &builtinYearSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinYearSig struct {
	baseIntBuiltinFunc
}

// evalInt evals YEAR(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_year
func (b *builtinYearSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	argTps := []evalTp{tpDatetime}
	if len(args) == 2 {
		argTps = append(argTps, tpInt)
	}

	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, argTps...)

	bf.tp.Flen, bf.tp.Decimal = 6, 0

	var sig builtinFunc
	if len(args) == 2 {
		sig = &builtinYearWeekWithModeSig{baseIntBuiltinFunc{bf}}
	} else {
		sig = &builtinYearWeekWithoutModeSig{baseIntBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), nil
}

type builtinYearWeekWithModeSig struct {
	baseIntBuiltinFunc
}

// evalInt evals YEARWEEK(date,mode).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithModeSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	baseIntBuiltinFunc
}

// evalInt evals YEARWEEK(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithoutModeSig) evalInt(row []types.Datum) (int64, bool, error) {
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

	retTp, argTps := tpDatetime, make([]evalTp, 0, len(args))
	argTps = append(argTps, tpDecimal)
	if len(args) == 2 {
		retTp = tpString
		argTps = append(argTps, tpString)
	}

	_, isArg0Con := args[0].(*Constant)
	isArg0Str := fieldTp2EvalTp(args[0].GetType()) == tpString
	bf := newBaseBuiltinFuncWithTp(args, ctx, retTp, argTps...)
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
		sig = &builtinFromUnixTime1ArgSig{baseTimeBuiltinFunc{bf}}
	} else {
		bf.tp.Flen = args[1].GetType().Flen
		sig = &builtinFromUnixTime2ArgSig{baseStringBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), nil
}

func evalFromUnixTime(ctx context.Context, fsp int, row []types.Datum, arg Expression) (res types.Time, isNull bool, err error) {
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
	baseTimeBuiltinFunc
}

// evalTime evals a builtinFromUnixTime1ArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-unixtime
func (b *builtinFromUnixTime1ArgSig) evalTime(row []types.Datum) (res types.Time, isNull bool, err error) {
	return evalFromUnixTime(b.ctx, b.tp.Decimal, row, b.args[0])
}

type builtinFromUnixTime2ArgSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinFromUnixTime2ArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-unixtime
func (b *builtinFromUnixTime2ArgSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpString)
	bf.tp.Flen = 17
	sig := &builtinGetFormatSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinGetFormatSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinGetFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_get-format
func (b *builtinGetFormatSig) evalString(row []types.Datum) (string, bool, error) {
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
	strArg := WrapWithCastAsString(arg, ctx)
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
		bf := newBaseBuiltinFuncWithTp(args, ctx, tpDatetime, tpString, tpString)
		bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, types.MinFsp
		sig = &builtinStrToDateDateSig{baseTimeBuiltinFunc{bf}}
	case mysql.TypeDatetime:
		bf := newBaseBuiltinFuncWithTp(args, ctx, tpDatetime, tpString, tpString)
		if fsp == types.MinFsp {
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeWidthNoFsp, types.MinFsp
		} else {
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeWidthWithFsp, types.MaxFsp
		}
		sig = &builtinStrToDateDatetimeSig{baseTimeBuiltinFunc{bf}}
	case mysql.TypeDuration:
		bf := newBaseBuiltinFuncWithTp(args, ctx, tpDuration, tpString, tpString)
		if fsp == types.MinFsp {
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthNoFsp, types.MinFsp
		} else {
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthWithFsp, types.MaxFsp
		}
		sig = &builtinStrToDateDurationSig{baseDurationBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), nil
}

type builtinStrToDateDateSig struct {
	baseTimeBuiltinFunc
}

func (b *builtinStrToDateDateSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	succ := t.StrToDate(date, format)
	if !succ {
		return types.Time{}, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(t.String()))
	}
	t.Type, t.Fsp = mysql.TypeDate, types.MinFsp
	return t, false, nil
}

type builtinStrToDateDatetimeSig struct {
	baseTimeBuiltinFunc
}

func (b *builtinStrToDateDatetimeSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	succ := t.StrToDate(date, format)
	if !succ {
		return types.Time{}, true, handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenByArgs(t.String()))
	}
	t.Type, t.Fsp = mysql.TypeDatetime, b.tp.Decimal
	return t, false, nil
}

type builtinStrToDateDurationSig struct {
	baseDurationBuiltinFunc
}

// TODO: If the NO_ZERO_DATE or NO_ZERO_IN_DATE SQL mode is enabled, zero dates or part of dates are disallowed.
// In that case, STR_TO_DATE() returns NULL and generates a warning.
func (b *builtinStrToDateDurationSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
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
	succ := t.StrToDate(date, format)
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
	argTps := []evalTp{}
	if len(args) == 1 {
		argTps = append(argTps, tpInt)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDatetime, argTps...)
	bf.tp.Flen, bf.tp.Decimal = 19, 0
	bf.foldable = false

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinSysDateWithFspSig{baseTimeBuiltinFunc{bf}}
	} else {
		sig = &builtinSysDateWithoutFspSig{baseTimeBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), nil
}

type builtinSysDateWithFspSig struct {
	baseTimeBuiltinFunc
}

// evalTime evals SYSDATE(fsp).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sysdate
func (b *builtinSysDateWithFspSig) evalTime(row []types.Datum) (d types.Time, isNull bool, err error) {
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
	baseTimeBuiltinFunc
}

// evalTime evals SYSDATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sysdate
func (b *builtinSysDateWithoutFspSig) evalTime(row []types.Datum) (d types.Time, isNull bool, err error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDatetime)
	bf.tp.Flen, bf.tp.Decimal = 10, 0
	bf.foldable = false
	sig := &builtinCurrentDateSig{baseTimeBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinCurrentDateSig struct {
	baseTimeBuiltinFunc
}

// evalTime evals CURDATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_curdate
func (b *builtinCurrentDateSig) evalTime(row []types.Datum) (d types.Time, isNull bool, err error) {
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
		bf := newBaseBuiltinFuncWithTp(args, ctx, tpDuration)
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthNoFsp, types.MinFsp
		sig = &builtinCurrentTime0ArgSig{baseDurationBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDuration, tpInt)
	bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthWithFsp, int(fsp)
	sig = &builtinCurrentTime1ArgSig{baseDurationBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinCurrentTime0ArgSig struct {
	baseDurationBuiltinFunc
}

func (b *builtinCurrentTime0ArgSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
	res, err := types.ParseDuration(time.Now().Format(types.TimeFormat), types.MinFsp)
	if err != nil {
		return types.Duration{}, true, errors.Trace(err)
	}
	return res, false, nil
}

type builtinCurrentTime1ArgSig struct {
	baseDurationBuiltinFunc
}

func (b *builtinCurrentTime1ArgSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
	fsp, _, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDuration, tpString)
	sig := &builtinTimeSig{baseDurationBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinTimeSig struct {
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time.
func (b *builtinTimeSig) evalDuration(row []types.Datum) (res types.Duration, isNull bool, err error) {
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
	constant, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for time literal")
	}
	str := constant.Value.GetString()
	if !isDuration(str) {
		return nil, types.ErrIncorrectDatetimeValue.GenByArgs(str)
	}
	duration, err := types.ParseDuration(str, getFsp(str))
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp([]Expression{}, ctx, tpDuration)
	bf.tp.Flen, bf.tp.Decimal = 10, duration.Fsp
	if duration.Fsp > 0 {
		bf.tp.Flen += 1 + duration.Fsp
	}
	sig := &builtinTimeLiteralSig{baseDurationBuiltinFunc{bf}, duration}
	return sig.setSelf(sig), nil
}

type builtinTimeLiteralSig struct {
	baseDurationBuiltinFunc
	duration types.Duration
}

// evalDuration evals TIME 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinTimeLiteralSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
	return b.duration, false, nil
}

type utcDateFunctionClass struct {
	baseFunctionClass
}

func (c *utcDateFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDatetime)
	bf.tp.Flen, bf.tp.Decimal = 10, 0
	bf.foldable = false
	sig := &builtinUTCDateSig{baseTimeBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinUTCDateSig struct {
	baseTimeBuiltinFunc
}

// evalTime evals UTC_DATE, UTC_DATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-date
func (b *builtinUTCDateSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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

func getFlenAndDecimal4UTCTimestampAndNow(sc *variable.StatementContext, arg Expression) (flen, decimal int) {
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
	argTps := make([]evalTp, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, tpInt)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDatetime, argTps...)

	if len(args) == 1 {
		bf.tp.Flen, bf.tp.Decimal = getFlenAndDecimal4UTCTimestampAndNow(bf.ctx.GetSessionVars().StmtCtx, args[0])
	} else {
		bf.tp.Flen, bf.tp.Decimal = 19, 0
	}
	bf.foldable = false

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinUTCTimestampWithArgSig{baseTimeBuiltinFunc{bf}}
	} else {
		sig = &builtinUTCTimestampWithoutArgSig{baseTimeBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), nil
}

func evalUTCTimestampWithFsp(fsp int) (types.Time, bool, error) {
	result, err := convertTimeToMysqlTime(time.Now().UTC(), fsp)
	if err != nil {
		return types.Time{}, true, errors.Trace(err)
	}
	return result, false, nil
}

type builtinUTCTimestampWithArgSig struct {
	baseTimeBuiltinFunc
}

// evalTime evals UTC_TIMESTAMP(fsp).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithArgSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseTimeBuiltinFunc
}

// evalTime evals UTC_TIMESTAMP().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithoutArgSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	argTps := make([]evalTp, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, tpInt)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDatetime, argTps...)

	if len(args) == 1 {
		bf.tp.Flen, bf.tp.Decimal = getFlenAndDecimal4UTCTimestampAndNow(bf.ctx.GetSessionVars().StmtCtx, args[0])
	} else {
		bf.tp.Flen, bf.tp.Decimal = 19, 0
	}

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinNowWithArgSig{baseTimeBuiltinFunc{bf}}
	} else {
		sig = &builtinNowWithoutArgSig{baseTimeBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), nil
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
	baseTimeBuiltinFunc
}

// evalTime evals NOW(fsp)
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_now
func (b *builtinNowWithArgSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseTimeBuiltinFunc
}

// evalTime evals NOW()
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_now
func (b *builtinNowWithoutArgSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	args[0] = WrapWithCastAsString(args[0], ctx)
	if _, isCon := args[0].(*Constant); isCon {
		unit, _, err1 := args[0].EvalString(nil, ctx.GetSessionVars().StmtCtx)
		if err1 != nil {
			return nil, errors.Trace(err1)
		}
		_, isDatetimeUnit = datetimeUnits[unit]
	}
	var bf baseBuiltinFunc
	if isDatetimeUnit {
		bf = newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString, tpDatetime)
		sig = &builtinExtractDatetimeSig{baseIntBuiltinFunc{bf}}
	} else {
		bf = newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString, tpDuration)
		sig = &builtinExtractDurationSig{baseIntBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), nil
}

type builtinExtractDatetimeSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinExtractDatetimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func (b *builtinExtractDatetimeSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	baseIntBuiltinFunc
}

// evalInt evals a builtinExtractDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func (b *builtinExtractDurationSig) evalInt(row []types.Datum) (int64, bool, error) {
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

func (du *baseDateArithmitical) getDateFromString(ctx context.Context, args []Expression, row []types.Datum, unit string) (types.Time, bool, error) {
	dateStr, isNull, err := args[0].EvalString(row, ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	dateTp := mysql.TypeDate
	if !types.IsDateFormat(dateStr) || types.IsClockUnit(unit) {
		dateTp = mysql.TypeDatetime
	}

	date, err := types.ParseTime(dateStr, dateTp, types.MaxFsp)
	return date, err != nil, errors.Trace(handleInvalidTimeError(ctx, err))
}

func (du *baseDateArithmitical) getDateFromInt(ctx context.Context, args []Expression, row []types.Datum, unit string) (types.Time, bool, error) {
	dateInt, isNull, err := args[0].EvalInt(row, ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return types.Time{}, true, errors.Trace(err)
	}

	date, err := types.ParseTimeFromInt64(dateInt)
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

func (du *baseDateArithmitical) getDateFromDatetime(ctx context.Context, args []Expression, row []types.Datum, unit string) (types.Time, bool, error) {
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

func (du *baseDateArithmitical) getIntervalFromString(ctx context.Context, args []Expression, row []types.Datum, unit string) (string, bool, error) {
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

func (du *baseDateArithmitical) getIntervalFromInt(ctx context.Context, args []Expression, row []types.Datum, unit string) (string, bool, error) {
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

	dateEvalTp := fieldTp2EvalTp(args[0].GetType())
	if dateEvalTp != tpString && dateEvalTp != tpInt {
		dateEvalTp = tpDatetime
	}

	intervalEvalTp := fieldTp2EvalTp(args[1].GetType())
	if intervalEvalTp != tpString {
		intervalEvalTp = tpInt
	}

	argTps := []evalTp{dateEvalTp, intervalEvalTp, tpString}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDatetime, argTps...)
	bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeFullWidth, types.UnspecifiedLength

	switch {
	case dateEvalTp == tpString && intervalEvalTp == tpString:
		sig = &builtinAddDateStringStringSig{
			baseTimeBuiltinFunc:  baseTimeBuiltinFunc{bf},
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == tpString && intervalEvalTp == tpInt:
		sig = &builtinAddDateStringIntSig{
			baseTimeBuiltinFunc:  baseTimeBuiltinFunc{bf},
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == tpInt && intervalEvalTp == tpString:
		sig = &builtinAddDateIntStringSig{
			baseTimeBuiltinFunc:  baseTimeBuiltinFunc{bf},
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == tpInt && intervalEvalTp == tpInt:
		sig = &builtinAddDateIntIntSig{
			baseTimeBuiltinFunc:  baseTimeBuiltinFunc{bf},
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == tpDatetime && intervalEvalTp == tpString:
		sig = &builtinAddDateDatetimeStringSig{
			baseTimeBuiltinFunc:  baseTimeBuiltinFunc{bf},
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == tpDatetime && intervalEvalTp == tpInt:
		sig = &builtinAddDateDatetimeIntSig{
			baseTimeBuiltinFunc:  baseTimeBuiltinFunc{bf},
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	}
	return sig.setSelf(sig), nil
}

type builtinAddDateStringStringSig struct {
	baseTimeBuiltinFunc
	baseDateArithmitical
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringStringSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseTimeBuiltinFunc
	baseDateArithmitical
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringIntSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseTimeBuiltinFunc
	baseDateArithmitical
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntStringSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseTimeBuiltinFunc
	baseDateArithmitical
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntIntSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseTimeBuiltinFunc
	baseDateArithmitical
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeStringSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseTimeBuiltinFunc
	baseDateArithmitical
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeIntSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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

	dateEvalTp := fieldTp2EvalTp(args[0].GetType())
	if dateEvalTp != tpString && dateEvalTp != tpInt {
		dateEvalTp = tpDatetime
	}

	intervalEvalTp := fieldTp2EvalTp(args[1].GetType())
	if intervalEvalTp != tpString {
		intervalEvalTp = tpInt
	}

	argTps := []evalTp{dateEvalTp, intervalEvalTp, tpString}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDatetime, argTps...)
	bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeFullWidth, types.UnspecifiedLength

	switch {
	case dateEvalTp == tpString && intervalEvalTp == tpString:
		sig = &builtinSubDateStringStringSig{
			baseTimeBuiltinFunc:  baseTimeBuiltinFunc{bf},
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == tpString && intervalEvalTp == tpInt:
		sig = &builtinSubDateStringIntSig{
			baseTimeBuiltinFunc:  baseTimeBuiltinFunc{bf},
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == tpInt && intervalEvalTp == tpString:
		sig = &builtinSubDateIntStringSig{
			baseTimeBuiltinFunc:  baseTimeBuiltinFunc{bf},
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == tpInt && intervalEvalTp == tpInt:
		sig = &builtinSubDateIntIntSig{
			baseTimeBuiltinFunc:  baseTimeBuiltinFunc{bf},
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == tpDatetime && intervalEvalTp == tpString:
		sig = &builtinSubDateDatetimeStringSig{
			baseTimeBuiltinFunc:  baseTimeBuiltinFunc{bf},
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	case dateEvalTp == tpDatetime && intervalEvalTp == tpInt:
		sig = &builtinSubDateDatetimeIntSig{
			baseTimeBuiltinFunc:  baseTimeBuiltinFunc{bf},
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
	}
	return sig.setSelf(sig), nil
}

type builtinSubDateStringStringSig struct {
	baseTimeBuiltinFunc
	baseDateArithmitical
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateStringStringSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseTimeBuiltinFunc
	baseDateArithmitical
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateStringIntSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseTimeBuiltinFunc
	baseDateArithmitical
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntStringSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseTimeBuiltinFunc
	baseDateArithmitical
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntIntSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseTimeBuiltinFunc
	baseDateArithmitical
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeStringSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseTimeBuiltinFunc
	baseDateArithmitical
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeIntSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString, tpDatetime, tpDatetime)
	sig := &builtinTimestampDiffSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinTimestampDiffSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinTimestampDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampdiff
func (b *builtinTimestampDiffSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	var argTps []evalTp
	var retTp evalTp
	var retFLen, retDecimal int

	if len(args) == 0 {
		retTp, retDecimal = tpInt, 0
	} else {
		argTps = []evalTp{tpDatetime}
		argType := args[0].GetType()
		argEvaltp := fieldTp2EvalTp(argType)
		if argEvaltp == tpString {
			// Treat tpString as unspecified decimal.
			retDecimal = types.UnspecifiedLength
		} else {
			retDecimal = argType.Decimal
		}
		if retDecimal > 6 || retDecimal == types.UnspecifiedLength {
			retDecimal = 6
		}
		if retDecimal == 0 {
			retTp = tpInt
		} else {
			retTp = tpDecimal
		}
	}
	if retTp == tpInt {
		retFLen = 11
	} else if retTp == tpDecimal {
		retFLen = 12 + retDecimal
	} else {
		panic("Unexpected retTp")
	}

	bf := newBaseBuiltinFuncWithTp(args, ctx, retTp, argTps...)
	bf.foldable = false
	bf.tp.Flen = retFLen
	bf.tp.Decimal = retDecimal

	var sig builtinFunc
	if len(args) == 0 {
		sig = &builtinUnixTimestampCurrentSig{baseIntBuiltinFunc{bf}}
	} else if retTp == tpInt {
		sig = &builtinUnixTimestampIntSig{baseIntBuiltinFunc{bf}}
	} else if retTp == tpDecimal {
		sig = &builtinUnixTimestampDecSig{baseDecimalBuiltinFunc{bf}}
	} else {
		panic("Unexpected retTp")
	}

	return sig.setSelf(sig), nil
}

// goTimeToMysqlUnixTimestamp converts go time into MySQL's Unix timestamp.
// MySQL's Unix timestamp ranges in int32. Values out of range should be rewritten to 0.
func goTimeToMysqlUnixTimestamp(t time.Time, decimal int) *types.MyDecimal {
	nanoSeconds := t.UnixNano()
	if nanoSeconds < 0 || (nanoSeconds/1e3) >= (math.MaxInt32+1)*1e6 {
		return new(types.MyDecimal)
	}
	dec := new(types.MyDecimal)
	// Here we don't use float to prevent precision lose.
	dec.FromInt(nanoSeconds)
	dec.Shift(-9)
	dec.Round(dec, decimal, types.ModeHalfEven)
	return dec
}

type builtinUnixTimestampCurrentSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a UNIX_TIMESTAMP().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampCurrentSig) evalInt(row []types.Datum) (int64, bool, error) {
	dec := goTimeToMysqlUnixTimestamp(time.Now(), 1)
	intVal, _ := dec.ToInt() // Ignore truncate errors.
	return intVal, false, nil
}

type builtinUnixTimestampIntSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a UNIX_TIMESTAMP(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		// Return 0 for invalid date time.
		return 0, isNull, nil
	}
	t, err := val.Time.GoTime(getTimeZone(b.getCtx()))
	if err != nil {
		return 0, false, nil
	}
	dec := goTimeToMysqlUnixTimestamp(t, 1)
	intVal, _ := dec.ToInt() // Ignore truncate errors.
	return intVal, false, nil
}

type builtinUnixTimestampDecSig struct {
	baseDecimalBuiltinFunc
}

// evalDecimal evals a UNIX_TIMESTAMP(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampDecSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalTime(row, b.getCtx().GetSessionVars().StmtCtx)
	if isNull || err != nil {
		// Return 0 for invalid date time.
		return new(types.MyDecimal), isNull, nil
	}
	t, err := val.Time.GoTime(getTimeZone(b.getCtx()))
	if err != nil {
		return new(types.MyDecimal), false, nil
	}
	return goTimeToMysqlUnixTimestamp(t, b.tp.Decimal), false, nil
}

type timestampFunctionClass struct {
	baseFunctionClass
}

func (c *timestampFunctionClass) getDefaultFsp(tp *types.FieldType) int {
	if tp.Tp == mysql.TypeDatetime || tp.Tp == mysql.TypeDate || tp.Tp == mysql.TypeDuration ||
		tp.Tp == mysql.TypeTimestamp || tp.Tp == mysql.TypeNewDate {
		return tp.Decimal
	}
	switch cls := tp.ToClass(); cls {
	case types.ClassInt:
		return types.MinFsp
	case types.ClassReal, types.ClassString:
		return types.MaxFsp
	case types.ClassDecimal:
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
	evalTps, argLen := []evalTp{tpString}, len(args)
	if argLen == 2 {
		evalTps = append(evalTps, tpString)
	}
	fsp := c.getDefaultFsp(args[0].GetType())
	if argLen == 2 {
		fsp = mathutil.Max(fsp, c.getDefaultFsp(args[1].GetType()))
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDatetime, evalTps...)
	bf.tp.Decimal, bf.tp.Flen = fsp, 19
	if fsp != 0 {
		bf.tp.Flen += 1 + fsp
	}
	var sig builtinFunc
	if argLen == 2 {
		sig = &builtinTimestamp2ArgsSig{baseTimeBuiltinFunc{bf}}
	} else {
		sig = &builtinTimestamp1ArgSig{baseTimeBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), nil
}

type builtinTimestamp1ArgSig struct {
	baseTimeBuiltinFunc
}

// evalTime evals a builtinTimestamp1ArgSig.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_timestamp
func (b *builtinTimestamp1ArgSig) evalTime(row []types.Datum) (types.Time, bool, error) {
	s, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return types.Time{}, isNull, errors.Trace(err)
	}
	tm, err := types.ParseTime(s, mysql.TypeDatetime, getFsp(s))
	if err != nil {
		return types.Time{}, true, errors.Trace(handleInvalidTimeError(b.ctx, err))
	}
	return tm, false, nil
}

type builtinTimestamp2ArgsSig struct {
	baseTimeBuiltinFunc
}

// evalTime evals a builtinTimestamp2ArgsSig.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_timestamp
func (b *builtinTimestamp2ArgsSig) evalTime(row []types.Datum) (types.Time, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return types.Time{}, isNull, errors.Trace(err)
	}
	tm, err := types.ParseTime(arg0, mysql.TypeDatetime, getFsp(arg0))
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
	constant, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for timestamp literal")
	}
	str := constant.Value.GetString()
	if !timestampPattern.MatchString(str) {
		return nil, types.ErrIncorrectDatetimeValue.GenByArgs(str)
	}
	tm, err := types.ParseTime(str, mysql.TypeTimestamp, getFsp(str))
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp([]Expression{}, ctx, tpDatetime)
	bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeWidthNoFsp, tm.Fsp
	if tm.Fsp > 0 {
		bf.tp.Flen += tm.Fsp + 1
	}
	sig := &builtinTimestampLiteralSig{baseTimeBuiltinFunc{bf}, tm}
	return sig.setSelf(sig), nil
}

type builtinTimestampLiteralSig struct {
	baseTimeBuiltinFunc
	tm types.Time
}

// evalTime evals TIMESTAMP 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinTimestampLiteralSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	var argTp1, argTp2, retTp evalTp
	switch tp1.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		argTp1, retTp = tpDatetime, tpDatetime
	case mysql.TypeDuration:
		argTp1, retTp = tpDuration, tpDuration
	case mysql.TypeDate:
		argTp1, retTp = tpDuration, tpString
	default:
		argTp1, retTp = tpString, tpString
	}
	switch tp2.Tp {
	case mysql.TypeDatetime, mysql.TypeDuration:
		argTp2 = tpDuration
	default:
		argTp2 = tpString
	}
	bf = newBaseBuiltinFuncWithTp(args, ctx, retTp, argTp1, argTp2)
	bf.tp.Decimal = tp1.Decimal
	if retTp == tpString {
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
func strDatetimeAddDuration(d string, arg1 types.Duration) (string, error) {
	arg0, err := types.ParseTime(d, mysql.TypeDatetime, types.MaxFsp)
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
func strDurationAddDuration(d string, arg1 types.Duration) (string, error) {
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
func strDatetimeSubDuration(d string, arg1 types.Duration) (string, error) {
	arg0, err := types.ParseTime(d, mysql.TypeDatetime, types.MaxFsp)
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
func strDurationSubDuration(d string, arg1 types.Duration) (string, error) {
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
			sig = &builtinAddDatetimeAndDurationSig{baseTimeBuiltinFunc{bf}}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeDateTimeNullSig{baseTimeBuiltinFunc{bf}}
		default:
			sig = &builtinAddDatetimeAndStringSig{baseTimeBuiltinFunc{bf}}
		}
	case mysql.TypeDate:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinAddDateAndDurationSig{baseStringBuiltinFunc{bf}}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeStringNullSig{baseStringBuiltinFunc{bf}}
		default:
			sig = &builtinAddDateAndStringSig{baseStringBuiltinFunc{bf}}
		}
	case mysql.TypeDuration:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinAddDurationAndDurationSig{baseDurationBuiltinFunc{bf}}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeDurationNullSig{baseDurationBuiltinFunc{bf}}
		default:
			sig = &builtinAddDurationAndStringSig{baseDurationBuiltinFunc{bf}}
		}
	default:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinAddStringAndDurationSig{baseStringBuiltinFunc{bf}}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeStringNullSig{baseStringBuiltinFunc{bf}}
		default:
			sig = &builtinAddStringAndStringSig{baseStringBuiltinFunc{bf}}
		}
	}
	return sig.setSelf(sig), nil
}

type builtinAddTimeDateTimeNullSig struct {
	baseTimeBuiltinFunc
}

// evalTime evals a builtinAddTimeDateTimeNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeDateTimeNullSig) evalTime(row []types.Datum) (types.Time, bool, error) {
	return types.ZeroDatetime, true, nil
}

type builtinAddDatetimeAndDurationSig struct {
	baseTimeBuiltinFunc
}

// evalTime evals a builtinAddDatetimeAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDatetimeAndDurationSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseTimeBuiltinFunc
}

// evalTime evals a builtinAddDatetimeAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDatetimeAndStringSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinAddTimeDurationNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeDurationNullSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
	return types.ZeroDuration, true, nil
}

type builtinAddDurationAndDurationSig struct {
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDurationAndDurationSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
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
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinAddDurationAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDurationAndStringSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
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
	baseStringBuiltinFunc
}

// evalString evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeStringNullSig) evalString(row []types.Datum) (string, bool, error) {
	return "", true, nil
}

type builtinAddStringAndDurationSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinAddStringAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddStringAndDurationSig) evalString(row []types.Datum) (result string, isNull bool, err error) {
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
		result, err = strDurationAddDuration(arg0, arg1)
		if err != nil {
			return "", true, errors.Trace(err)
		}
		return result, false, nil
	}
	result, err = strDatetimeAddDuration(arg0, arg1)
	return result, err != nil, errors.Trace(err)
}

type builtinAddStringAndStringSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinAddStringAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddStringAndStringSig) evalString(row []types.Datum) (result string, isNull bool, err error) {
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
		result, err = strDurationAddDuration(arg0, arg1)
		if err != nil {
			return "", true, errors.Trace(err)
		}
		return result, false, nil
	}
	result, err = strDatetimeAddDuration(arg0, arg1)
	return result, err != nil, errors.Trace(err)
}

type builtinAddDateAndDurationSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDateAndDurationSig) evalString(row []types.Datum) (string, bool, error) {
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
	baseStringBuiltinFunc
}

// evalString evals a builtinAddDateAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDateAndStringSig) evalString(row []types.Datum) (string, bool, error) {
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
		switch fieldTp2EvalTp(arg.GetType()) {
		case tpInt:
			decimal = 0
		case tpReal, tpDecimal:
			decimal = arg.GetType().Decimal
		case tpString:
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDatetime, tpDatetime, tpString, tpString)
	bf.tp.Decimal = decimal
	sig := &builtinConvertTzSig{
		baseTimeBuiltinFunc: baseTimeBuiltinFunc{bf},
		timezoneRegex:       tzRegex,
	}
	return sig.setSelf(sig), nil
}

type builtinConvertTzSig struct {
	baseTimeBuiltinFunc
	timezoneRegex *regexp.Regexp
}

// evalTime evals CONVERT_TZ(dt,from_tz,to_tz).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_convert-tz
func (b *builtinConvertTzSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDatetime, tpInt, tpInt)
	tp := bf.tp
	tp.Tp, tp.Flen, tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, 0
	sig := &builtinMakeDateSig{baseTimeBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinMakeDateSig struct {
	baseTimeBuiltinFunc
}

// evalTime evaluates a builtinMakeDateSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_makedate
func (b *builtinMakeDateSig) evalTime(row []types.Datum) (d types.Time, isNull bool, err error) {
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
	tp, flen, decimal := fieldTp2EvalTp(args[2].GetType()), 10, 0
	switch tp {
	case tpInt:
	case tpReal, tpDecimal:
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDuration, tpInt, tpInt, tpReal)
	bf.tp.Flen, bf.tp.Decimal = flen, decimal
	sig := &builtinMakeTimeSig{baseDurationBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinMakeTimeSig struct {
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinMakeTimeIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_maketime
func (b *builtinMakeTimeSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
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

	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	bf.tp.Flen = 6
	sig := &builtinPeriodAddSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
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
	baseIntBuiltinFunc
}

// evalInt evals PERIOD_ADD(P,N).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-add
func (b *builtinPeriodAddSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	bf.tp.Flen = 6
	sig := &builtinPeriodDiffSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinPeriodDiffSig struct {
	baseIntBuiltinFunc
}

// evalInt evals PERIOD_DIFF(P1,P2).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-diff
func (b *builtinPeriodDiffSig) evalInt(row []types.Datum) (int64, bool, error) {
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

	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDatetime)
	bf.tp.Flen = 1

	sig := &builtinQuarterSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinQuarterSig struct {
	baseIntBuiltinFunc
}

// evalInt evals QUARTER(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_quarter
func (b *builtinQuarterSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	argEvalTp := fieldTp2EvalTp(argType)
	if argEvalTp == tpString {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDuration, tpReal)
	bf.tp.Flen, bf.tp.Decimal = retFlen, retFsp
	sig := &builtinSecToTimeSig{baseDurationBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinSecToTimeSig struct {
	baseDurationBuiltinFunc
}

// evalDuration evals SEC_TO_TIME(seconds).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sec-to-time
func (b *builtinSecToTimeSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
	ctx := b.getCtx().GetSessionVars().StmtCtx
	secondsFloat, isNull, err := b.args[0].EvalReal(row, ctx)
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
			sig = &builtinSubDatetimeAndDurationSig{baseTimeBuiltinFunc{bf}}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeDateTimeNullSig{baseTimeBuiltinFunc{bf}}
		default:
			sig = &builtinSubDatetimeAndStringSig{baseTimeBuiltinFunc{bf}}
		}
	case mysql.TypeDate:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinSubDateAndDurationSig{baseStringBuiltinFunc{bf}}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeStringNullSig{baseStringBuiltinFunc{bf}}
		default:
			sig = &builtinSubDateAndStringSig{baseStringBuiltinFunc{bf}}
		}
	case mysql.TypeDuration:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinSubDurationAndDurationSig{baseDurationBuiltinFunc{bf}}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeDurationNullSig{baseDurationBuiltinFunc{bf}}
		default:
			sig = &builtinSubDurationAndStringSig{baseDurationBuiltinFunc{bf}}
		}
	default:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinSubStringAndDurationSig{baseStringBuiltinFunc{bf}}
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeStringNullSig{baseStringBuiltinFunc{bf}}
		default:
			sig = &builtinSubStringAndStringSig{baseStringBuiltinFunc{bf}}
		}
	}
	return sig.setSelf(sig), nil
}

type builtinSubDatetimeAndDurationSig struct {
	baseTimeBuiltinFunc
}

// evalTime evals a builtinSubDatetimeAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDatetimeAndDurationSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseTimeBuiltinFunc
}

// evalTime evals a builtinSubDatetimeAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDatetimeAndStringSig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
	baseTimeBuiltinFunc
}

// evalTime evals a builtinSubTimeDateTimeNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeDateTimeNullSig) evalTime(row []types.Datum) (types.Time, bool, error) {
	return types.ZeroDatetime, true, nil
}

type builtinSubStringAndDurationSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinSubStringAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubStringAndDurationSig) evalString(row []types.Datum) (result string, isNull bool, err error) {
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
		result, err = strDurationSubDuration(arg0, arg1)
		if err != nil {
			return "", true, errors.Trace(err)
		}
		return result, false, nil
	}
	result, err = strDatetimeSubDuration(arg0, arg1)
	return result, err != nil, errors.Trace(err)
}

type builtinSubStringAndStringSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinAddStringAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubStringAndStringSig) evalString(row []types.Datum) (result string, isNull bool, err error) {
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
		result, err = strDurationSubDuration(arg0, arg1)
		if err != nil {
			return "", true, errors.Trace(err)
		}
		return result, false, nil
	}
	result, err = strDatetimeSubDuration(arg0, arg1)
	return result, err != nil, errors.Trace(err)
}

type builtinSubTimeStringNullSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinSubTimeStringNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeStringNullSig) evalString(row []types.Datum) (string, bool, error) {
	return "", true, nil
}

type builtinSubDurationAndDurationSig struct {
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDurationAndDurationSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
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
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinAddDurationAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDurationAndStringSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
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
	baseDurationBuiltinFunc
}

// evalTime evals a builtinSubTimeDurationNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeDurationNullSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
	return types.ZeroDuration, true, nil
}

type builtinSubDateAndDurationSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinAddDateAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDateAndDurationSig) evalString(row []types.Datum) (string, bool, error) {
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
	baseStringBuiltinFunc
}

// evalString evals a builtinAddDateAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDateAndStringSig) evalString(row []types.Datum) (string, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpDuration, tpString)
	// worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
	bf.tp.Flen = (args[1].GetType().Flen + 1) / 2 * 11
	sig := &builtinTimeFormatSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinTimeFormatSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinTimeFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-format
func (b *builtinTimeFormatSig) evalString(row []types.Datum) (string, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDuration)
	bf.tp.Flen = 10
	sig := &builtinTimeToSecSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinTimeToSecSig struct {
	baseIntBuiltinFunc
}

// evalInt evals TIME_TO_SEC(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-to-sec
func (b *builtinTimeToSecSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpInt, tpDatetime)
	bf.tp = &types.FieldType{Tp: mysql.TypeString, Flen: mysql.MaxDatetimeWidthNoFsp, Decimal: types.UnspecifiedLength}
	sig := &builtinTimestampAddSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil

}

type builtinTimestampAddSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinTimestampAddSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampadd
func (b *builtinTimestampAddSig) evalString(row []types.Datum) (string, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDatetime)
	sig := &builtinToDaysSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinToDaysSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinToDaysSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-days
func (b *builtinToDaysSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDatetime)
	sig := &builtinToSecondsSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinToSecondsSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinToSecondsSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-seconds
func (b *builtinToSecondsSig) evalInt(row []types.Datum) (int64, bool, error) {
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

func (c *utcTimeFunctionClass) getFlenAndDecimal4UTCTime(sc *variable.StatementContext, args []Expression) (flen, decimal int) {
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
	argTps := make([]evalTp, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, tpInt)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDuration, argTps...)
	bf.tp.Flen, bf.tp.Decimal = c.getFlenAndDecimal4UTCTime(bf.ctx.GetSessionVars().StmtCtx, args)

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinUTCTimeWithArgSig{baseDurationBuiltinFunc{bf}}
	} else {
		sig = &builtinUTCTimeWithoutArgSig{baseDurationBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), nil
}

type builtinUTCTimeWithoutArgSig struct {
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinUTCTimeWithoutArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithoutArgSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
	// the types.ParseDuration here would never fail, so the err returned can be ignored.
	v, _ := types.ParseDuration(time.Now().UTC().Format(types.TimeFormat), 0)
	return v, false, nil
}

type builtinUTCTimeWithArgSig struct {
	baseDurationBuiltinFunc
}

// evalDuration evals a builtinUTCTimeWithArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithArgSig) evalDuration(row []types.Datum) (types.Duration, bool, error) {
	fsp, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return types.Duration{}, isNull, errors.Trace(err)
	}
	if fsp > int64(types.MaxFsp) {
		return types.Duration{}, true, errors.Errorf("Too-big precision %v specified for 'utc_time'. Maximum is %v.", fsp, types.MaxFsp)
	}
	if fsp < int64(types.MinFsp) {
		return types.Duration{}, true, errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
	}
	// the types.ParseDuration here would never fail, so the err returned can be ignored.
	v, _ := types.ParseDuration(time.Now().UTC().Format(types.TimeFSPFormat), int(fsp))
	return v, false, nil
}

type lastDayFunctionClass struct {
	baseFunctionClass
}

func (c *lastDayFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDatetime, tpDatetime)
	bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, types.DefaultFsp
	sig := &builtinLastDaySig{baseTimeBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinLastDaySig struct {
	baseTimeBuiltinFunc
}

// evalTime evals a builtinLastDaySig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_last-day
func (b *builtinLastDaySig) evalTime(row []types.Datum) (types.Time, bool, error) {
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
