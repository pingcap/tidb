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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
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
	_ builtinFuncNew = &builtinUnixTimestampIntSig{}
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
	_ builtinFunc = &builtinAddDateStringRealSig{}
	_ builtinFunc = &builtinAddDateStringDecimalSig{}
	_ builtinFunc = &builtinAddDateIntStringSig{}
	_ builtinFunc = &builtinAddDateIntIntSig{}
	_ builtinFunc = &builtinAddDateIntRealSig{}
	_ builtinFunc = &builtinAddDateIntDecimalSig{}
	_ builtinFunc = &builtinAddDateDatetimeStringSig{}
	_ builtinFunc = &builtinAddDateDatetimeIntSig{}
	_ builtinFunc = &builtinAddDateDatetimeRealSig{}
	_ builtinFunc = &builtinAddDateDatetimeDecimalSig{}
	_ builtinFunc = &builtinSubDateStringStringSig{}
	_ builtinFunc = &builtinSubDateStringIntSig{}
	_ builtinFunc = &builtinSubDateStringRealSig{}
	_ builtinFunc = &builtinSubDateStringDecimalSig{}
	_ builtinFunc = &builtinSubDateIntStringSig{}
	_ builtinFunc = &builtinSubDateIntIntSig{}
	_ builtinFunc = &builtinSubDateIntRealSig{}
	_ builtinFunc = &builtinSubDateIntDecimalSig{}
	_ builtinFunc = &builtinSubDateDatetimeStringSig{}
	_ builtinFunc = &builtinSubDateDatetimeIntSig{}
	_ builtinFunc = &builtinSubDateDatetimeRealSig{}
	_ builtinFunc = &builtinSubDateDatetimeDecimalSig{}
)

func convertTimeToMysqlTime(t time.Time, fsp int8, roundMode types.RoundMode) (types.Time, error) {
	var tr time.Time
	var err error
	if roundMode == types.ModeTruncate {
		tr, err = types.TruncateFrac(t, fsp)
	} else {
		tr, err = types.RoundFrac(t, fsp)
	}
	if err != nil {
		return types.ZeroTime, err
	}

	return types.NewTime(types.FromGoTime(tr), mysql.TypeDatetime, fsp), nil
}

type dateFunctionClass struct {
	baseFunctionClass
}

func (c *dateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, 10, 0
	sig := &builtinDateSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Date)
	return sig, nil
}

type builtinDateSig struct {
	baseBuiltinFunc
}

func (b *builtinDateSig) Clone() builtinFunc {
	newSig := &builtinDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals DATE(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date
func (b *builtinDateSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	expr, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
	}

	if expr.IsZero() {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, expr.String()))
	}

	expr.SetCoreTime(types.FromDate(expr.Year(), expr.Month(), expr.Day(), 0, 0, 0, 0))
	expr.SetType(mysql.TypeDate)
	return expr, false, nil
}

type dateLiteralFunctionClass struct {
	baseFunctionClass
}

func (c *dateLiteralFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	con, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for date literal")
	}
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		return nil, err
	}
	str := dt.GetString()
	if !datePattern.MatchString(str) {
		return nil, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, str)
	}
	tm, err := types.ParseDate(ctx.GetSessionVars().StmtCtx, str)
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, []Expression{}, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, 10, 0
	sig := &builtinDateLiteralSig{bf, tm}
	return sig, nil
}

type builtinDateLiteralSig struct {
	baseBuiltinFunc
	literal types.Time
}

func (b *builtinDateLiteralSig) Clone() builtinFunc {
	newSig := &builtinDateLiteralSig{literal: b.literal}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals DATE 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinDateLiteralSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	mode := b.ctx.GetSessionVars().SQLMode
	if mode.HasNoZeroDateMode() && b.literal.IsZero() {
		return b.literal, true, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, b.literal.String())
	}
	if mode.HasNoZeroInDateMode() && (b.literal.InvalidZero() && !b.literal.IsZero()) {
		return b.literal, true, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, b.literal.String())
	}
	return b.literal, false, nil
}

type dateDiffFunctionClass struct {
	baseFunctionClass
}

func (c *dateDiffFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	sig := &builtinDateDiffSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_DateDiff)
	return sig, nil
}

type builtinDateDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinDateDiffSig) Clone() builtinFunc {
	newSig := &builtinDateDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinDateDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_datediff
func (b *builtinDateDiffSig) evalInt(row chunk.Row) (int64, bool, error) {
	lhs, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}
	rhs, isNull, err := b.args[1].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}
	if invalidLHS, invalidRHS := lhs.InvalidZero(), rhs.InvalidZero(); invalidLHS || invalidRHS {
		if invalidLHS {
			err = handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, lhs.String()))
		}
		if invalidRHS {
			err = handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, rhs.String()))
		}
		return 0, true, err
	}
	return int64(types.DateDiff(lhs.CoreTime(), rhs.CoreTime())), false, nil
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

func (c *timeDiffFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	arg0FieldTp, arg1FieldTp := args[0].GetType(), args[1].GetType()
	arg0Tp, arg1Tp := c.getArgEvalTp(arg0FieldTp), c.getArgEvalTp(arg1FieldTp)
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, arg0Tp, arg1Tp)
	if err != nil {
		return nil, err
	}

	arg0Dec, err := getExpressionFsp(ctx, args[0])
	if err != nil {
		return nil, err
	}
	arg1Dec, err := getExpressionFsp(ctx, args[1])
	if err != nil {
		return nil, err
	}
	bf.tp.Decimal = mathutil.Max(arg0Dec, arg1Dec)

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
			sig.setPbCode(tipb.ScalarFuncSig_DurationDurationTimeDiff)
		case types.ETDatetime, types.ETTimestamp:
			sig = &builtinNullTimeDiffSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullTimeDiff)
		default:
			sig = &builtinDurationStringTimeDiffSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_DurationStringTimeDiff)
		}
	case types.ETDatetime, types.ETTimestamp:
		switch arg1Tp {
		case types.ETDuration:
			sig = &builtinNullTimeDiffSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullTimeDiff)
		case types.ETDatetime, types.ETTimestamp:
			sig = &builtinTimeTimeTimeDiffSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_TimeTimeTimeDiff)
		default:
			sig = &builtinTimeStringTimeDiffSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_TimeStringTimeDiff)
		}
	default:
		switch arg1Tp {
		case types.ETDuration:
			sig = &builtinStringDurationTimeDiffSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_StringDurationTimeDiff)
		case types.ETDatetime, types.ETTimestamp:
			sig = &builtinStringTimeTimeDiffSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_StringTimeTimeDiff)
		default:
			sig = &builtinStringStringTimeDiffSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_StringStringTimeDiff)
		}
	}
	return sig, nil
}

type builtinDurationDurationTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinDurationDurationTimeDiffSig) Clone() builtinFunc {
	newSig := &builtinDurationDurationTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinDurationDurationTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinDurationDurationTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhs, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhs, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	d, isNull, err = calculateDurationTimeDiff(b.ctx, lhs, rhs)
	return d, isNull, err
}

type builtinTimeTimeTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeTimeTimeDiffSig) Clone() builtinFunc {
	newSig := &builtinTimeTimeTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinTimeTimeTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinTimeTimeTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhs, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhs, isNull, err := b.args[1].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	sc := b.ctx.GetSessionVars().StmtCtx
	d, isNull, err = calculateTimeDiff(sc, lhs, rhs)
	return d, isNull, err
}

type builtinDurationStringTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinDurationStringTimeDiffSig) Clone() builtinFunc {
	newSig := &builtinDurationStringTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinDurationStringTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinDurationStringTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhs, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhsStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	sc := b.ctx.GetSessionVars().StmtCtx
	rhs, _, isDuration, err := convertStringToDuration(sc, rhsStr, int8(b.tp.Decimal))
	if err != nil || !isDuration {
		return d, true, err
	}

	d, isNull, err = calculateDurationTimeDiff(b.ctx, lhs, rhs)
	return d, isNull, err
}

type builtinStringDurationTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinStringDurationTimeDiffSig) Clone() builtinFunc {
	newSig := &builtinStringDurationTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinStringDurationTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinStringDurationTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhsStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhs, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	sc := b.ctx.GetSessionVars().StmtCtx
	lhs, _, isDuration, err := convertStringToDuration(sc, lhsStr, int8(b.tp.Decimal))
	if err != nil || !isDuration {
		return d, true, err
	}

	d, isNull, err = calculateDurationTimeDiff(b.ctx, lhs, rhs)
	return d, isNull, err
}

// calculateTimeDiff calculates interval difference of two types.Time.
func calculateTimeDiff(sc *stmtctx.StatementContext, lhs, rhs types.Time) (d types.Duration, isNull bool, err error) {
	d = lhs.Sub(sc, &rhs)
	d.Duration, err = types.TruncateOverflowMySQLTime(d.Duration)
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = sc.HandleTruncate(err)
	}
	return d, err != nil, err
}

// calculateDurationTimeDiff calculates interval difference of two types.Duration.
func calculateDurationTimeDiff(ctx sessionctx.Context, lhs, rhs types.Duration) (d types.Duration, isNull bool, err error) {
	d, err = lhs.Sub(rhs)
	if err != nil {
		return d, true, err
	}

	d.Duration, err = types.TruncateOverflowMySQLTime(d.Duration)
	if types.ErrTruncatedWrongVal.Equal(err) {
		sc := ctx.GetSessionVars().StmtCtx
		err = sc.HandleTruncate(err)
	}
	return d, err != nil, err
}

type builtinTimeStringTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeStringTimeDiffSig) Clone() builtinFunc {
	newSig := &builtinTimeStringTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinTimeStringTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinTimeStringTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhs, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhsStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	sc := b.ctx.GetSessionVars().StmtCtx
	_, rhs, isDuration, err := convertStringToDuration(sc, rhsStr, int8(b.tp.Decimal))
	if err != nil || isDuration {
		return d, true, err
	}

	d, isNull, err = calculateTimeDiff(sc, lhs, rhs)
	return d, isNull, err
}

type builtinStringTimeTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinStringTimeTimeDiffSig) Clone() builtinFunc {
	newSig := &builtinStringTimeTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinStringTimeTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinStringTimeTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhsStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhs, isNull, err := b.args[1].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	sc := b.ctx.GetSessionVars().StmtCtx
	_, lhs, isDuration, err := convertStringToDuration(sc, lhsStr, int8(b.tp.Decimal))
	if err != nil || isDuration {
		return d, true, err
	}

	d, isNull, err = calculateTimeDiff(sc, lhs, rhs)
	return d, isNull, err
}

type builtinStringStringTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinStringStringTimeDiffSig) Clone() builtinFunc {
	newSig := &builtinStringStringTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinStringStringTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinStringStringTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhs, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhs, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	sc := b.ctx.GetSessionVars().StmtCtx
	fsp := int8(b.tp.Decimal)
	lhsDur, lhsTime, lhsIsDuration, err := convertStringToDuration(sc, lhs, fsp)
	if err != nil {
		return d, true, err
	}

	rhsDur, rhsTime, rhsIsDuration, err := convertStringToDuration(sc, rhs, fsp)
	if err != nil {
		return d, true, err
	}

	if lhsIsDuration != rhsIsDuration {
		return d, true, nil
	}

	if lhsIsDuration {
		d, isNull, err = calculateDurationTimeDiff(b.ctx, lhsDur, rhsDur)
	} else {
		d, isNull, err = calculateTimeDiff(sc, lhsTime, rhsTime)
	}

	return d, isNull, err
}

type builtinNullTimeDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinNullTimeDiffSig) Clone() builtinFunc {
	newSig := &builtinNullTimeDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinNullTimeDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timediff
func (b *builtinNullTimeDiffSig) evalDuration(row chunk.Row) (d types.Duration, isNull bool, err error) {
	return d, true, nil
}

// convertStringToDuration converts string to duration, it return types.Time because in some case
// it will converts string to datetime.
func convertStringToDuration(sc *stmtctx.StatementContext, str string, fsp int8) (d types.Duration, t types.Time,
	isDuration bool, err error) {
	if n := strings.IndexByte(str, '.'); n >= 0 {
		lenStrFsp := len(str[n+1:])
		if lenStrFsp <= int(types.MaxFsp) {
			fsp = mathutil.MaxInt8(int8(lenStrFsp), fsp)
		}
	}
	return types.StrToDuration(sc, str, fsp)
}

type dateFormatFunctionClass struct {
	baseFunctionClass
}

func (c *dateFormatFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETDatetime, types.ETString)
	if err != nil {
		return nil, err
	}
	// worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
	bf.tp.Flen = (args[1].GetType().Flen + 1) / 2 * 11
	sig := &builtinDateFormatSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_DateFormatSig)
	return sig, nil
}

type builtinDateFormatSig struct {
	baseBuiltinFunc
}

func (b *builtinDateFormatSig) Clone() builtinFunc {
	newSig := &builtinDateFormatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinDateFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-format
func (b *builtinDateFormatSig) evalString(row chunk.Row) (string, bool, error) {
	t, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, handleInvalidTimeError(b.ctx, err)
	}
	formatMask, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	// MySQL compatibility, #11203
	// If format mask is 0 then return 0 without warnings
	if formatMask == "0" {
		return "0", false, nil
	}

	if t.InvalidZero() {
		// MySQL compatibility, #11203
		// 0 | 0.0 should be converted to null without warnings
		n, err := t.ToNumber().ToInt()
		isOriginalIntOrDecimalZero := err == nil && n == 0
		// Args like "0000-00-00", "0000-00-00 00:00:00" set Fsp to 6
		isOriginalStringZero := t.Fsp() > 0
		if isOriginalIntOrDecimalZero && !isOriginalStringZero {
			return "", true, nil
		}
		return "", true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}

	res, err := t.DateFormat(formatMask)
	return res, isNull, err
}

type fromDaysFunctionClass struct {
	baseFunctionClass
}

func (c *fromDaysFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = 10, 0
	sig := &builtinFromDaysSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_FromDays)
	return sig, nil
}

type builtinFromDaysSig struct {
	baseBuiltinFunc
}

func (b *builtinFromDaysSig) Clone() builtinFunc {
	newSig := &builtinFromDaysSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals FROM_DAYS(N).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-days
func (b *builtinFromDaysSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	n, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	return types.TimeFromDays(n), false, nil
}

type hourFunctionClass struct {
	baseFunctionClass
}

func (c *hourFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDuration)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = 3, 0
	sig := &builtinHourSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Hour)
	return sig, nil
}

type builtinHourSig struct {
	baseBuiltinFunc
}

func (b *builtinHourSig) Clone() builtinFunc {
	newSig := &builtinHourSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals HOUR(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_hour
func (b *builtinHourSig) evalInt(row chunk.Row) (int64, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.Hour()), false, nil
}

type minuteFunctionClass struct {
	baseFunctionClass
}

func (c *minuteFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDuration)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinMinuteSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Minute)
	return sig, nil
}

type builtinMinuteSig struct {
	baseBuiltinFunc
}

func (b *builtinMinuteSig) Clone() builtinFunc {
	newSig := &builtinMinuteSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals MINUTE(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_minute
func (b *builtinMinuteSig) evalInt(row chunk.Row) (int64, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.Minute()), false, nil
}

type secondFunctionClass struct {
	baseFunctionClass
}

func (c *secondFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDuration)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinSecondSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Second)
	return sig, nil
}

type builtinSecondSig struct {
	baseBuiltinFunc
}

func (b *builtinSecondSig) Clone() builtinFunc {
	newSig := &builtinSecondSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals SECOND(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_second
func (b *builtinSecondSig) evalInt(row chunk.Row) (int64, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.Second()), false, nil
}

type microSecondFunctionClass struct {
	baseFunctionClass
}

func (c *microSecondFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDuration)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = 6, 0
	sig := &builtinMicroSecondSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_MicroSecond)
	return sig, nil
}

type builtinMicroSecondSig struct {
	baseBuiltinFunc
}

func (b *builtinMicroSecondSig) Clone() builtinFunc {
	newSig := &builtinMicroSecondSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals MICROSECOND(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_microsecond
func (b *builtinMicroSecondSig) evalInt(row chunk.Row) (int64, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.MicroSecond()), false, nil
}

type monthFunctionClass struct {
	baseFunctionClass
}

func (c *monthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinMonthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Month)
	return sig, nil
}

type builtinMonthSig struct {
	baseBuiltinFunc
}

func (b *builtinMonthSig) Clone() builtinFunc {
	newSig := &builtinMonthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals MONTH(date).
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_month
func (b *builtinMonthSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	if date.IsZero() {
		if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() {
			return 0, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
		}
		return 0, false, nil
	}

	return int64(date.Month()), false, nil
}

// monthNameFunctionClass see https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_monthname
type monthNameFunctionClass struct {
	baseFunctionClass
}

func (c *monthNameFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Charset, bf.tp.Collate = ctx.GetSessionVars().GetCharsetInfo()
	bf.tp.Flen = 10
	sig := &builtinMonthNameSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_MonthName)
	return sig, nil
}

type builtinMonthNameSig struct {
	baseBuiltinFunc
}

func (b *builtinMonthNameSig) Clone() builtinFunc {
	newSig := &builtinMonthNameSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMonthNameSig) evalString(row chunk.Row) (string, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return "", true, handleInvalidTimeError(b.ctx, err)
	}
	mon := arg.Month()
	if (arg.IsZero() && b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode()) || mon < 0 || mon > len(types.MonthNames) {
		return "", true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	} else if mon == 0 || arg.IsZero() {
		return "", true, nil
	}
	return types.MonthNames[mon-1], false, nil
}

type dayNameFunctionClass struct {
	baseFunctionClass
}

func (c *dayNameFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Charset, bf.tp.Collate = ctx.GetSessionVars().GetCharsetInfo()
	bf.tp.Flen = 10
	sig := &builtinDayNameSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_DayName)
	return sig, nil
}

type builtinDayNameSig struct {
	baseBuiltinFunc
}

func (b *builtinDayNameSig) Clone() builtinFunc {
	newSig := &builtinDayNameSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinDayNameSig) evalIndex(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if arg.InvalidZero() {
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	// Monday is 0, ... Sunday = 6 in MySQL
	// but in go, Sunday is 0, ... Saturday is 6
	// w will do a conversion.
	res := (int64(arg.Weekday()) + 6) % 7
	return res, false, nil
}

// evalString evals a builtinDayNameSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayname
func (b *builtinDayNameSig) evalString(row chunk.Row) (string, bool, error) {
	idx, isNull, err := b.evalIndex(row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return types.WeekdayNames[idx], false, nil
}

func (b *builtinDayNameSig) evalReal(row chunk.Row) (float64, bool, error) {
	idx, isNull, err := b.evalIndex(row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return float64(idx), false, nil
}

func (b *builtinDayNameSig) evalInt(row chunk.Row) (int64, bool, error) {
	idx, isNull, err := b.evalIndex(row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return idx, false, nil
}

type dayOfMonthFunctionClass struct {
	baseFunctionClass
}

func (c *dayOfMonthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 2
	sig := &builtinDayOfMonthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_DayOfMonth)
	return sig, nil
}

type builtinDayOfMonthSig struct {
	baseBuiltinFunc
}

func (b *builtinDayOfMonthSig) Clone() builtinFunc {
	newSig := &builtinDayOfMonthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinDayOfMonthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofmonth
func (b *builtinDayOfMonthSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}
	if arg.IsZero() {
		if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() {
			return 0, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
		}
		return 0, false, nil
	}
	return int64(arg.Day()), false, nil
}

type dayOfWeekFunctionClass struct {
	baseFunctionClass
}

func (c *dayOfWeekFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 1
	sig := &builtinDayOfWeekSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_DayOfWeek)
	return sig, nil
}

type builtinDayOfWeekSig struct {
	baseBuiltinFunc
}

func (b *builtinDayOfWeekSig) Clone() builtinFunc {
	newSig := &builtinDayOfWeekSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinDayOfWeekSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofweek
func (b *builtinDayOfWeekSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}
	if arg.InvalidZero() {
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	// 1 is Sunday, 2 is Monday, .... 7 is Saturday
	return int64(arg.Weekday() + 1), false, nil
}

type dayOfYearFunctionClass struct {
	baseFunctionClass
}

func (c *dayOfYearFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 3
	sig := &builtinDayOfYearSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_DayOfYear)
	return sig, nil
}

type builtinDayOfYearSig struct {
	baseBuiltinFunc
}

func (b *builtinDayOfYearSig) Clone() builtinFunc {
	newSig := &builtinDayOfYearSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinDayOfYearSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayofyear
func (b *builtinDayOfYearSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, handleInvalidTimeError(b.ctx, err)
	}
	if arg.InvalidZero() {
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}

	return int64(arg.YearDay()), false, nil
}

type weekFunctionClass struct {
	baseFunctionClass
}

func (c *weekFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTps := []types.EvalType{types.ETDatetime}
	if len(args) == 2 {
		argTps = append(argTps, types.ETInt)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}

	bf.tp.Flen, bf.tp.Decimal = 2, 0

	var sig builtinFunc
	if len(args) == 2 {
		sig = &builtinWeekWithModeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_WeekWithMode)
	} else {
		sig = &builtinWeekWithoutModeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_WeekWithoutMode)
	}
	return sig, nil
}

type builtinWeekWithModeSig struct {
	baseBuiltinFunc
}

func (b *builtinWeekWithModeSig) Clone() builtinFunc {
	newSig := &builtinWeekWithModeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals WEEK(date, mode).
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_week
func (b *builtinWeekWithModeSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	if date.IsZero() {
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
	}

	mode, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	week := date.Week(int(mode))
	return int64(week), false, nil
}

type builtinWeekWithoutModeSig struct {
	baseBuiltinFunc
}

func (b *builtinWeekWithoutModeSig) Clone() builtinFunc {
	newSig := &builtinWeekWithoutModeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals WEEK(date).
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_week
func (b *builtinWeekWithoutModeSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	if date.IsZero() {
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
	}

	mode := 0
	modeStr, ok := b.ctx.GetSessionVars().GetSystemVar(variable.DefaultWeekFormat)
	if ok && modeStr != "" {
		mode, err = strconv.Atoi(modeStr)
		if err != nil {
			return 0, true, handleInvalidTimeError(b.ctx, types.ErrInvalidWeekModeFormat.GenWithStackByArgs(modeStr))
		}
	}

	week := date.Week(mode)
	return int64(week), false, nil
}

type weekDayFunctionClass struct {
	baseFunctionClass
}

func (c *weekDayFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 1

	sig := &builtinWeekDaySig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_WeekDay)
	return sig, nil
}

type builtinWeekDaySig struct {
	baseBuiltinFunc
}

func (b *builtinWeekDaySig) Clone() builtinFunc {
	newSig := &builtinWeekDaySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals WEEKDAY(date).
func (b *builtinWeekDaySig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	if date.IsZero() {
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
	}

	return int64(date.Weekday()+6) % 7, false, nil
}

type weekOfYearFunctionClass struct {
	baseFunctionClass
}

func (c *weekOfYearFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = 2, 0
	sig := &builtinWeekOfYearSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_WeekOfYear)
	return sig, nil
}

type builtinWeekOfYearSig struct {
	baseBuiltinFunc
}

func (b *builtinWeekOfYearSig) Clone() builtinFunc {
	newSig := &builtinWeekOfYearSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals WEEKOFYEAR(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_weekofyear
func (b *builtinWeekOfYearSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	if date.IsZero() {
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
	}

	week := date.Week(3)
	return int64(week), false, nil
}

type yearFunctionClass struct {
	baseFunctionClass
}

func (c *yearFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = 4, 0
	sig := &builtinYearSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Year)
	return sig, nil
}

type builtinYearSig struct {
	baseBuiltinFunc
}

func (b *builtinYearSig) Clone() builtinFunc {
	newSig := &builtinYearSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals YEAR(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_year
func (b *builtinYearSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	if date.IsZero() {
		if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() {
			return 0, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
		}
		return 0, false, nil
	}
	return int64(date.Year()), false, nil
}

type yearWeekFunctionClass struct {
	baseFunctionClass
}

func (c *yearWeekFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETDatetime}
	if len(args) == 2 {
		argTps = append(argTps, types.ETInt)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}

	bf.tp.Flen, bf.tp.Decimal = 6, 0

	var sig builtinFunc
	if len(args) == 2 {
		sig = &builtinYearWeekWithModeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_YearWeekWithMode)
	} else {
		sig = &builtinYearWeekWithoutModeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_YearWeekWithoutMode)
	}
	return sig, nil
}

type builtinYearWeekWithModeSig struct {
	baseBuiltinFunc
}

func (b *builtinYearWeekWithModeSig) Clone() builtinFunc {
	newSig := &builtinYearWeekWithModeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals YEARWEEK(date,mode).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithModeSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, handleInvalidTimeError(b.ctx, err)
	}
	if date.IsZero() {
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
	}

	mode, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	if isNull {
		mode = 0
	}

	year, week := date.YearWeek(int(mode))
	result := int64(week + year*100)
	if result < 0 {
		return int64(math.MaxUint32), false, nil
	}
	return result, false, nil
}

type builtinYearWeekWithoutModeSig struct {
	baseBuiltinFunc
}

func (b *builtinYearWeekWithoutModeSig) Clone() builtinFunc {
	newSig := &builtinYearWeekWithoutModeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals YEARWEEK(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithoutModeSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	if date.InvalidZero() {
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
	}

	year, week := date.YearWeek(0)
	result := int64(week + year*100)
	if result < 0 {
		return int64(math.MaxUint32), false, nil
	}
	return result, false, nil
}

type fromUnixTimeFunctionClass struct {
	baseFunctionClass
}

func (c *fromUnixTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	retTp, argTps := types.ETDatetime, make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETDecimal)
	if len(args) == 2 {
		retTp = types.ETString
		argTps = append(argTps, types.ETString)
	}

	isArg0Str := args[0].GetType().EvalType() == types.ETString
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, retTp, argTps...)
	if err != nil {
		return nil, err
	}

	if len(args) > 1 {
		bf.tp.Flen = args[1].GetType().Flen
		sig = &builtinFromUnixTime2ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_FromUnixTime2Arg)
		return sig, nil
	}

	// Calculate the time fsp.
	bf.tp.Decimal = int(types.MaxFsp)
	if !isArg0Str {
		if args[0].GetType().Decimal != types.UnspecifiedLength {
			bf.tp.Decimal = mathutil.Min(bf.tp.Decimal, args[0].GetType().Decimal)
		}
	}

	sig = &builtinFromUnixTime1ArgSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_FromUnixTime1Arg)
	return sig, nil
}

func evalFromUnixTime(ctx sessionctx.Context, fsp int8, unixTimeStamp *types.MyDecimal) (res types.Time, isNull bool, err error) {
	// 0 <= unixTimeStamp <= INT32_MAX
	if unixTimeStamp.IsNegative() {
		return res, true, nil
	}
	integralPart, err := unixTimeStamp.ToInt()
	if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) {
		return res, true, err
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
		return res, true, err
	}
	nano := new(types.MyDecimal).FromInt(int64(time.Second))
	x := new(types.MyDecimal)
	err = types.DecimalMul(fracDecimalTp, nano, x)
	if err != nil {
		return res, true, err
	}
	fractionalPart, err := x.ToInt() // here fractionalPart is result multiplying the original fractional part by 10^9.
	if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) {
		return res, true, err
	}
	if fsp < 0 {
		fsp = types.MaxFsp
	}

	sc := ctx.GetSessionVars().StmtCtx
	tmp := time.Unix(integralPart, fractionalPart).In(sc.TimeZone)
	t, err := convertTimeToMysqlTime(tmp, fsp, types.ModeHalfEven)
	if err != nil {
		return res, true, err
	}
	return t, false, nil
}

type builtinFromUnixTime1ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinFromUnixTime1ArgSig) Clone() builtinFunc {
	newSig := &builtinFromUnixTime1ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinFromUnixTime1ArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-unixtime
func (b *builtinFromUnixTime1ArgSig) evalTime(row chunk.Row) (res types.Time, isNull bool, err error) {
	unixTimeStamp, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if err != nil || isNull {
		return res, isNull, err
	}
	return evalFromUnixTime(b.ctx, int8(b.tp.Decimal), unixTimeStamp)
}

type builtinFromUnixTime2ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinFromUnixTime2ArgSig) Clone() builtinFunc {
	newSig := &builtinFromUnixTime2ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinFromUnixTime2ArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_from-unixtime
func (b *builtinFromUnixTime2ArgSig) evalString(row chunk.Row) (res string, isNull bool, err error) {
	format, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	unixTimeStamp, isNull, err := b.args[0].EvalDecimal(b.ctx, row)
	if err != nil || isNull {
		return "", isNull, err
	}
	t, isNull, err := evalFromUnixTime(b.ctx, int8(b.tp.Decimal), unixTimeStamp)
	if isNull || err != nil {
		return "", isNull, err
	}
	res, err = t.DateFormat(format)
	return res, err != nil, err
}

type getFormatFunctionClass struct {
	baseFunctionClass
}

func (c *getFormatFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 17
	sig := &builtinGetFormatSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_GetFormat)
	return sig, nil
}

type builtinGetFormatSig struct {
	baseBuiltinFunc
}

func (b *builtinGetFormatSig) Clone() builtinFunc {
	newSig := &builtinGetFormatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinGetFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_get-format
func (b *builtinGetFormatSig) evalString(row chunk.Row) (string, bool, error) {
	t, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	l, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	res := b.getFormat(t, l)
	return res, false, nil
}

type strToDateFunctionClass struct {
	baseFunctionClass
}

func (c *strToDateFunctionClass) getRetTp(ctx sessionctx.Context, arg Expression) (tp byte, fsp int8) {
	tp = mysql.TypeDatetime
	if _, ok := arg.(*Constant); !ok {
		return tp, types.MaxFsp
	}
	strArg := WrapWithCastAsString(ctx, arg)
	format, isNull, err := strArg.EvalString(ctx, chunk.Row{})
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

// getFunction see https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_str-to-date
func (c *strToDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	retTp, fsp := c.getRetTp(ctx, args[1])
	switch retTp {
	case mysql.TypeDate:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETString, types.ETString)
		if err != nil {
			return nil, err
		}
		bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, int(types.MinFsp)
		sig = &builtinStrToDateDateSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_StrToDateDate)
	case mysql.TypeDatetime:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETString, types.ETString)
		if err != nil {
			return nil, err
		}
		if fsp == types.MinFsp {
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeWidthNoFsp, int(types.MinFsp)
		} else {
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeWidthWithFsp, int(types.MaxFsp)
		}
		sig = &builtinStrToDateDatetimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_StrToDateDatetime)
	case mysql.TypeDuration:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, types.ETString, types.ETString)
		if err != nil {
			return nil, err
		}
		if fsp == types.MinFsp {
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthNoFsp, int(types.MinFsp)
		} else {
			bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthWithFsp, int(types.MaxFsp)
		}
		sig = &builtinStrToDateDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_StrToDateDuration)
	}
	return sig, nil
}

type builtinStrToDateDateSig struct {
	baseBuiltinFunc
}

func (b *builtinStrToDateDateSig) Clone() builtinFunc {
	newSig := &builtinStrToDateDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinStrToDateDateSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	date, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	format, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	var t types.Time
	sc := b.ctx.GetSessionVars().StmtCtx
	succ := t.StrToDate(sc, date, format)
	if !succ {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() && (t.Year() == 0 || t.Month() == 0 || t.Day() == 0) {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	t.SetType(mysql.TypeDate)
	t.SetFsp(types.MinFsp)
	return t, false, nil
}

type builtinStrToDateDatetimeSig struct {
	baseBuiltinFunc
}

func (b *builtinStrToDateDatetimeSig) Clone() builtinFunc {
	newSig := &builtinStrToDateDatetimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinStrToDateDatetimeSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	date, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	format, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	var t types.Time
	sc := b.ctx.GetSessionVars().StmtCtx
	succ := t.StrToDate(sc, date, format)
	if !succ {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() && (t.Year() == 0 || t.Month() == 0 || t.Day() == 0) {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	t.SetType(mysql.TypeDatetime)
	t.SetFsp(int8(b.tp.Decimal))
	return t, false, nil
}

type builtinStrToDateDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinStrToDateDurationSig) Clone() builtinFunc {
	newSig := &builtinStrToDateDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration
// TODO: If the NO_ZERO_DATE or NO_ZERO_IN_DATE SQL mode is enabled, zero dates or part of dates are disallowed.
// In that case, STR_TO_DATE() returns NULL and generates a warning.
func (b *builtinStrToDateDurationSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	date, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	format, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	var t types.Time
	sc := b.ctx.GetSessionVars().StmtCtx
	succ := t.StrToDate(sc, date, format)
	if !succ {
		return types.Duration{}, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() && (t.Year() == 0 || t.Month() == 0 || t.Day() == 0) {
		return types.Duration{}, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	t.SetFsp(int8(b.tp.Decimal))
	dur, err := t.ConvertToDuration()
	return dur, err != nil, err
}

type sysDateFunctionClass struct {
	baseFunctionClass
}

func (c *sysDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var argTps = make([]types.EvalType, 0)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = 19, 0

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinSysDateWithFspSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_SysDateWithFsp)
	} else {
		sig = &builtinSysDateWithoutFspSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_SysDateWithoutFsp)
	}
	return sig, nil
}

type builtinSysDateWithFspSig struct {
	baseBuiltinFunc
}

func (b *builtinSysDateWithFspSig) Clone() builtinFunc {
	newSig := &builtinSysDateWithFspSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SYSDATE(fsp).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sysdate
func (b *builtinSysDateWithFspSig) evalTime(row chunk.Row) (d types.Time, isNull bool, err error) {
	fsp, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}

	loc := b.ctx.GetSessionVars().Location()
	now := time.Now().In(loc)
	result, err := convertTimeToMysqlTime(now, int8(fsp), types.ModeHalfEven)
	if err != nil {
		return types.ZeroTime, true, err
	}
	return result, false, nil
}

type builtinSysDateWithoutFspSig struct {
	baseBuiltinFunc
}

func (b *builtinSysDateWithoutFspSig) Clone() builtinFunc {
	newSig := &builtinSysDateWithoutFspSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SYSDATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sysdate
func (b *builtinSysDateWithoutFspSig) evalTime(row chunk.Row) (d types.Time, isNull bool, err error) {
	tz := b.ctx.GetSessionVars().Location()
	now := time.Now().In(tz)
	result, err := convertTimeToMysqlTime(now, 0, types.ModeHalfEven)
	if err != nil {
		return types.ZeroTime, true, err
	}
	return result, false, nil
}

type currentDateFunctionClass struct {
	baseFunctionClass
}

func (c *currentDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = 10, 0
	sig := &builtinCurrentDateSig{bf}
	return sig, nil
}

type builtinCurrentDateSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentDateSig) Clone() builtinFunc {
	newSig := &builtinCurrentDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals CURDATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_curdate
func (b *builtinCurrentDateSig) evalTime(row chunk.Row) (d types.Time, isNull bool, err error) {
	tz := b.ctx.GetSessionVars().Location()
	nowTs, err := getStmtTimestamp(b.ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}
	year, month, day := nowTs.In(tz).Date()
	result := types.NewTime(types.FromDate(year, int(month), day, 0, 0, 0, 0), mysql.TypeDate, 0)
	return result, false, nil
}

type currentTimeFunctionClass struct {
	baseFunctionClass
}

func (c *currentTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	if len(args) == 0 {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration)
		if err != nil {
			return nil, err
		}
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthNoFsp, int(types.MinFsp)
		sig = &builtinCurrentTime0ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CurrentTime0Arg)
		return sig, nil
	}
	// args[0] must be a constant which should not be null.
	_, ok := args[0].(*Constant)
	fsp := int64(types.MaxFsp)
	if ok {
		fsp, _, err = args[0].EvalInt(ctx, chunk.Row{})
		if err != nil {
			return nil, err
		}
		if fsp > int64(types.MaxFsp) {
			return nil, errors.Errorf("Too-big precision %v specified for 'curtime'. Maximum is %v.", fsp, types.MaxFsp)
		} else if fsp < int64(types.MinFsp) {
			return nil, errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
		}
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthWithFsp, int(fsp)
	sig = &builtinCurrentTime1ArgSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_CurrentTime1Arg)
	return sig, nil
}

type builtinCurrentTime0ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentTime0ArgSig) Clone() builtinFunc {
	newSig := &builtinCurrentTime0ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCurrentTime0ArgSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	tz := b.ctx.GetSessionVars().Location()
	nowTs, err := getStmtTimestamp(b.ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	dur := nowTs.In(tz).Format(types.TimeFormat)
	res, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, dur, types.MinFsp)
	if err != nil {
		return types.Duration{}, true, err
	}
	return res, false, nil
}

type builtinCurrentTime1ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinCurrentTime1ArgSig) Clone() builtinFunc {
	newSig := &builtinCurrentTime1ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinCurrentTime1ArgSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	fsp, _, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return types.Duration{}, true, err
	}
	tz := b.ctx.GetSessionVars().Location()
	nowTs, err := getStmtTimestamp(b.ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	dur := nowTs.In(tz).Format(types.TimeFSPFormat)
	res, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, dur, int8(fsp))
	if err != nil {
		return types.Duration{}, true, err
	}
	return res, false, nil
}

type timeFunctionClass struct {
	baseFunctionClass
}

func (c *timeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Decimal, err = getExpressionFsp(ctx, args[0])
	if err != nil {
		return nil, err
	}
	sig := &builtinTimeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Time)
	return sig, nil
}

type builtinTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeSig) Clone() builtinFunc {
	newSig := &builtinTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinTimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time.
func (b *builtinTimeSig) evalDuration(row chunk.Row) (res types.Duration, isNull bool, err error) {
	expr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	fsp := 0
	if idx := strings.Index(expr, "."); idx != -1 {
		fsp = len(expr) - idx - 1
	}

	var tmpFsp int8
	if tmpFsp, err = types.CheckFsp(fsp); err != nil {
		return res, isNull, err
	}
	fsp = int(tmpFsp)

	sc := b.ctx.GetSessionVars().StmtCtx
	res, err = types.ParseDuration(sc, expr, int8(fsp))
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = sc.HandleTruncate(err)
	}
	return res, isNull, err
}

type timeLiteralFunctionClass struct {
	baseFunctionClass
}

func (c *timeLiteralFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	con, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for time literal")
	}
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		return nil, err
	}
	str := dt.GetString()
	if !isDuration(str) {
		return nil, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, str)
	}
	duration, err := types.ParseDuration(ctx.GetSessionVars().StmtCtx, str, types.GetFsp(str))
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, []Expression{}, types.ETDuration)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = 10, int(duration.Fsp)
	if int(duration.Fsp) > 0 {
		bf.tp.Flen += 1 + int(duration.Fsp)
	}
	sig := &builtinTimeLiteralSig{bf, duration}
	return sig, nil
}

type builtinTimeLiteralSig struct {
	baseBuiltinFunc
	duration types.Duration
}

func (b *builtinTimeLiteralSig) Clone() builtinFunc {
	newSig := &builtinTimeLiteralSig{duration: b.duration}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals TIME 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinTimeLiteralSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	return b.duration, false, nil
}

type utcDateFunctionClass struct {
	baseFunctionClass
}

func (c *utcDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = 10, 0
	sig := &builtinUTCDateSig{bf}
	return sig, nil
}

type builtinUTCDateSig struct {
	baseBuiltinFunc
}

func (b *builtinUTCDateSig) Clone() builtinFunc {
	newSig := &builtinUTCDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals UTC_DATE, UTC_DATE().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-date
func (b *builtinUTCDateSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	nowTs, err := getStmtTimestamp(b.ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}
	year, month, day := nowTs.UTC().Date()
	result := types.NewTime(types.FromGoTime(time.Date(year, month, day, 0, 0, 0, 0, time.UTC)), mysql.TypeDate, types.UnspecifiedFsp)
	return result, false, nil
}

type utcTimestampFunctionClass struct {
	baseFunctionClass
}

func getFlenAndDecimal4UTCTimestampAndNow(ctx sessionctx.Context, arg Expression) (flen, decimal int) {
	if constant, ok := arg.(*Constant); ok {
		fsp, isNull, err := constant.EvalInt(ctx, chunk.Row{})
		if isNull || err != nil || fsp > int64(types.MaxFsp) {
			decimal = int(types.MaxFsp)
		} else if fsp < int64(types.MinFsp) {
			decimal = int(types.MinFsp)
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

func (c *utcTimestampFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, argTps...)
	if err != nil {
		return nil, err
	}

	if len(args) == 1 {
		bf.tp.Flen, bf.tp.Decimal = getFlenAndDecimal4UTCTimestampAndNow(bf.ctx, args[0])
	} else {
		bf.tp.Flen, bf.tp.Decimal = 19, 0
	}

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinUTCTimestampWithArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UTCTimestampWithArg)
	} else {
		sig = &builtinUTCTimestampWithoutArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UTCTimestampWithoutArg)
	}
	return sig, nil
}

func evalUTCTimestampWithFsp(ctx sessionctx.Context, fsp int8) (types.Time, bool, error) {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}
	result, err := convertTimeToMysqlTime(nowTs.UTC(), fsp, types.ModeHalfEven)
	if err != nil {
		return types.ZeroTime, true, err
	}
	return result, false, nil
}

type builtinUTCTimestampWithArgSig struct {
	baseBuiltinFunc
}

func (b *builtinUTCTimestampWithArgSig) Clone() builtinFunc {
	newSig := &builtinUTCTimestampWithArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals UTC_TIMESTAMP(fsp).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithArgSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	num, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return types.ZeroTime, true, err
	}

	if !isNull && num > int64(types.MaxFsp) {
		return types.ZeroTime, true, errors.Errorf("Too-big precision %v specified for 'utc_timestamp'. Maximum is %v.", num, types.MaxFsp)
	}
	if !isNull && num < int64(types.MinFsp) {
		return types.ZeroTime, true, errors.Errorf("Invalid negative %d specified, must in [0, 6].", num)
	}

	result, isNull, err := evalUTCTimestampWithFsp(b.ctx, int8(num))
	return result, isNull, err
}

type builtinUTCTimestampWithoutArgSig struct {
	baseBuiltinFunc
}

func (b *builtinUTCTimestampWithoutArgSig) Clone() builtinFunc {
	newSig := &builtinUTCTimestampWithoutArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals UTC_TIMESTAMP().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithoutArgSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	result, isNull, err := evalUTCTimestampWithFsp(b.ctx, int8(0))
	return result, isNull, err
}

type nowFunctionClass struct {
	baseFunctionClass
}

func (c *nowFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, argTps...)
	if err != nil {
		return nil, err
	}

	if len(args) == 1 {
		bf.tp.Flen, bf.tp.Decimal = getFlenAndDecimal4UTCTimestampAndNow(bf.ctx, args[0])
	} else {
		bf.tp.Flen, bf.tp.Decimal = 19, 0
	}

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinNowWithArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_NowWithArg)
	} else {
		sig = &builtinNowWithoutArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_NowWithoutArg)
	}
	return sig, nil
}

func evalNowWithFsp(ctx sessionctx.Context, fsp int8) (types.Time, bool, error) {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}

	// In MySQL's implementation, now() will truncate the result instead of rounding it.
	// Results below are from MySQL 5.7, which can prove it.
	// mysql> select now(6), now(3), now();
	//	+----------------------------+-------------------------+---------------------+
	//	| now(6)                     | now(3)                  | now()               |
	//	+----------------------------+-------------------------+---------------------+
	//	| 2019-03-25 15:57:56.612966 | 2019-03-25 15:57:56.612 | 2019-03-25 15:57:56 |
	//	+----------------------------+-------------------------+---------------------+
	result, err := convertTimeToMysqlTime(nowTs, fsp, types.ModeTruncate)
	if err != nil {
		return types.ZeroTime, true, err
	}

	err = result.ConvertTimeZone(time.Local, ctx.GetSessionVars().Location())
	if err != nil {
		return types.ZeroTime, true, err
	}

	return result, false, nil
}

type builtinNowWithArgSig struct {
	baseBuiltinFunc
}

func (b *builtinNowWithArgSig) Clone() builtinFunc {
	newSig := &builtinNowWithArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals NOW(fsp)
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_now
func (b *builtinNowWithArgSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	fsp, isNull, err := b.args[0].EvalInt(b.ctx, row)

	if err != nil {
		return types.ZeroTime, true, err
	}

	if isNull {
		fsp = 0
	} else if fsp > int64(types.MaxFsp) {
		return types.ZeroTime, true, errors.Errorf("Too-big precision %v specified for 'now'. Maximum is %v.", fsp, types.MaxFsp)
	} else if fsp < int64(types.MinFsp) {
		return types.ZeroTime, true, errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
	}

	result, isNull, err := evalNowWithFsp(b.ctx, int8(fsp))
	return result, isNull, err
}

type builtinNowWithoutArgSig struct {
	baseBuiltinFunc
}

func (b *builtinNowWithoutArgSig) Clone() builtinFunc {
	newSig := &builtinNowWithoutArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals NOW()
// see: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_now
func (b *builtinNowWithoutArgSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	result, isNull, err := evalNowWithFsp(b.ctx, int8(0))
	return result, isNull, err
}

type extractFunctionClass struct {
	baseFunctionClass
}

func (c *extractFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
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
		unit, _, err1 := args[0].EvalString(ctx, chunk.Row{})
		if err1 != nil {
			return nil, err1
		}
		_, isDatetimeUnit = datetimeUnits[unit]
	}
	var bf baseBuiltinFunc
	if isDatetimeUnit {
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETDatetime)
		if err != nil {
			return nil, err
		}
		sig = &builtinExtractDatetimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ExtractDatetime)
	} else {
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETDuration)
		if err != nil {
			return nil, err
		}
		sig = &builtinExtractDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ExtractDuration)
	}
	return sig, nil
}

type builtinExtractDatetimeSig struct {
	baseBuiltinFunc
}

func (b *builtinExtractDatetimeSig) Clone() builtinFunc {
	newSig := &builtinExtractDatetimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinExtractDatetimeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func (b *builtinExtractDatetimeSig) evalInt(row chunk.Row) (int64, bool, error) {
	unit, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	dt, isNull, err := b.args[1].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	res, err := types.ExtractDatetimeNum(&dt, unit)
	return res, err != nil, err
}

type builtinExtractDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinExtractDurationSig) Clone() builtinFunc {
	newSig := &builtinExtractDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinExtractDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func (b *builtinExtractDurationSig) evalInt(row chunk.Row) (int64, bool, error) {
	unit, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	dur, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	res, err := types.ExtractDurationNum(&dur, unit)
	return res, err != nil, err
}

// baseDateArithmitical is the base class for all "builtinAddDateXXXSig" and "builtinSubDateXXXSig",
// which provides parameter getter and date arithmetical calculate functions.
type baseDateArithmitical struct {
	// intervalRegexp is "*Regexp" used to extract string interval for "DAY" unit.
	intervalRegexp *regexp.Regexp
}

func newDateArighmeticalUtil() baseDateArithmitical {
	return baseDateArithmitical{
		intervalRegexp: regexp.MustCompile(`-?[\d]+`),
	}
}

func (du *baseDateArithmitical) getDateFromString(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	dateStr, isNull, err := args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	dateTp := mysql.TypeDate
	if !types.IsDateFormat(dateStr) || types.IsClockUnit(unit) {
		dateTp = mysql.TypeDatetime
	}

	sc := ctx.GetSessionVars().StmtCtx
	date, err := types.ParseTime(sc, dateStr, dateTp, types.MaxFsp)
	return date, err != nil, handleInvalidTimeError(ctx, err)
}

func (du *baseDateArithmitical) getDateFromInt(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	dateInt, isNull, err := args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	sc := ctx.GetSessionVars().StmtCtx
	date, err := types.ParseTimeFromInt64(sc, dateInt)
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}

	dateTp := mysql.TypeDate
	if date.Type() == mysql.TypeDatetime || date.Type() == mysql.TypeTimestamp || types.IsClockUnit(unit) {
		dateTp = mysql.TypeDatetime
	}
	date.SetType(dateTp)
	return date, false, nil
}

func (du *baseDateArithmitical) getDateFromDatetime(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	date, isNull, err := args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	dateTp := mysql.TypeDate
	if date.Type() == mysql.TypeDatetime || date.Type() == mysql.TypeTimestamp || types.IsClockUnit(unit) {
		dateTp = mysql.TypeDatetime
	}
	date.SetType(dateTp)
	return date, false, nil
}

func (du *baseDateArithmitical) getIntervalFromString(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	// unit "DAY" and "HOUR" has to be specially handled.
	if toLower := strings.ToLower(unit); toLower == "day" || toLower == "hour" {
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

func (du *baseDateArithmitical) getIntervalFromDecimal(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	decimal, isNull, err := args[1].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	interval := decimal.String()

	switch strings.ToUpper(unit) {
	case "HOUR_MINUTE", "MINUTE_SECOND", "YEAR_MONTH", "DAY_HOUR", "DAY_MINUTE",
		"DAY_SECOND", "DAY_MICROSECOND", "HOUR_MICROSECOND", "HOUR_SECOND", "MINUTE_MICROSECOND", "SECOND_MICROSECOND":
		neg := false
		if interval != "" && interval[0] == '-' {
			neg = true
			interval = interval[1:]
		}
		switch strings.ToUpper(unit) {
		case "HOUR_MINUTE", "MINUTE_SECOND":
			interval = strings.Replace(interval, ".", ":", -1)
		case "YEAR_MONTH":
			interval = strings.Replace(interval, ".", "-", -1)
		case "DAY_HOUR":
			interval = strings.Replace(interval, ".", " ", -1)
		case "DAY_MINUTE":
			interval = "0 " + strings.Replace(interval, ".", ":", -1)
		case "DAY_SECOND":
			interval = "0 00:" + strings.Replace(interval, ".", ":", -1)
		case "DAY_MICROSECOND":
			interval = "0 00:00:" + interval
		case "HOUR_MICROSECOND":
			interval = "00:00:" + interval
		case "HOUR_SECOND":
			interval = "00:" + strings.Replace(interval, ".", ":", -1)
		case "MINUTE_MICROSECOND":
			interval = "00:" + interval
		case "SECOND_MICROSECOND":
			/* keep interval as original decimal */
		}
		if neg {
			interval = "-" + interval
		}
	case "SECOND":
		// interval is already like the %f format.
	default:
		// YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, MICROSECOND
		castExpr := WrapWithCastAsString(ctx, WrapWithCastAsInt(ctx, args[1]))
		interval, isNull, err = castExpr.EvalString(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
	}

	return interval, false, nil
}

func (du *baseDateArithmitical) getIntervalFromInt(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	return strconv.FormatInt(interval, 10), false, nil
}

func (du *baseDateArithmitical) getIntervalFromReal(ctx sessionctx.Context, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	return strconv.FormatFloat(interval, 'f', args[1].GetType().Decimal, 64), false, nil
}

func (du *baseDateArithmitical) add(ctx sessionctx.Context, date types.Time, interval string, unit string) (types.Time, bool, error) {
	year, month, day, nano, err := types.ParseDurationValue(unit, interval)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}

	goTime, err := date.GoTime(time.Local)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}

	goTime = goTime.Add(time.Duration(nano))
	goTime = types.AddDate(year, month, day, goTime)

	if goTime.Nanosecond() == 0 {
		date.SetFsp(0)
	} else {
		date.SetFsp(6)
	}

	if goTime.Year() < 0 || goTime.Year() > 9999 {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}

	date.SetCoreTime(types.FromGoTime(goTime))
	overflow, err := types.DateTimeIsOverflow(ctx.GetSessionVars().StmtCtx, date)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}
	if overflow {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}
	return date, false, nil
}

func (du *baseDateArithmitical) addDuration(ctx sessionctx.Context, d types.Duration, interval string, unit string) (types.Duration, bool, error) {
	dur, err := types.ExtractDurationValue(unit, interval)
	if err != nil {
		return types.ZeroDuration, true, handleInvalidTimeError(ctx, err)
	}
	retDur, err := d.Add(dur)
	if err != nil {
		return types.ZeroDuration, true, err
	}
	return retDur, false, nil
}

func (du *baseDateArithmitical) subDuration(ctx sessionctx.Context, d types.Duration, interval string, unit string) (types.Duration, bool, error) {
	dur, err := types.ExtractDurationValue(unit, interval)
	if err != nil {
		return types.ZeroDuration, true, handleInvalidTimeError(ctx, err)
	}
	retDur, err := d.Sub(dur)
	if err != nil {
		return types.ZeroDuration, true, err
	}
	return retDur, false, nil
}

func (du *baseDateArithmitical) sub(ctx sessionctx.Context, date types.Time, interval string, unit string) (types.Time, bool, error) {
	year, month, day, nano, err := types.ParseDurationValue(unit, interval)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}
	year, month, day, nano = -year, -month, -day, -nano

	goTime, err := date.GoTime(time.Local)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}

	duration := time.Duration(nano)
	goTime = goTime.Add(duration)
	goTime = types.AddDate(year, month, day, goTime)

	if goTime.Nanosecond() == 0 {
		date.SetFsp(0)
	} else {
		date.SetFsp(6)
	}

	if goTime.Year() < 0 || goTime.Year() > 9999 {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}

	date.SetCoreTime(types.FromGoTime(goTime))
	overflow, err := types.DateTimeIsOverflow(ctx.GetSessionVars().StmtCtx, date)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}
	if overflow {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}
	return date, false, nil
}

func (du *baseDateArithmitical) vecGetDateFromInt(b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	dates := result.Times()
	i64s := buf.Int64s()
	sc := b.ctx.GetSessionVars().StmtCtx
	isClockUnit := types.IsClockUnit(unit)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		date, err := types.ParseTimeFromInt64(sc, i64s[i])
		if err != nil {
			err = handleInvalidTimeError(b.ctx, err)
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}

		dateTp := mysql.TypeDate
		if date.Type() == mysql.TypeDatetime || date.Type() == mysql.TypeTimestamp || isClockUnit {
			dateTp = mysql.TypeDatetime
		}
		date.SetType(dateTp)
		dates[i] = date
	}
	return nil
}

func (du *baseDateArithmitical) vecGetDateFromString(b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	dates := result.Times()
	sc := b.ctx.GetSessionVars().StmtCtx
	isClockUnit := types.IsClockUnit(unit)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		dateStr := buf.GetString(i)
		dateTp := mysql.TypeDate
		if !types.IsDateFormat(dateStr) || isClockUnit {
			dateTp = mysql.TypeDatetime
		}

		date, err := types.ParseTime(sc, dateStr, dateTp, types.MaxFsp)
		if err != nil {
			err = handleInvalidTimeError(b.ctx, err)
			if err != nil {
				return err
			}
			result.SetNull(i, true)
		} else {
			dates[i] = date
		}

	}
	return nil
}

func (du *baseDateArithmitical) vecGetDateFromDatetime(b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeTime(n, false)
	if err := b.args[0].VecEvalTime(b.ctx, input, result); err != nil {
		return err
	}

	dates := result.Times()
	isClockUnit := types.IsClockUnit(unit)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		dateTp := mysql.TypeDate
		if dates[i].Type() == mysql.TypeDatetime || dates[i].Type() == mysql.TypeTimestamp || isClockUnit {
			dateTp = mysql.TypeDatetime
		}
		dates[i].SetType(dateTp)
	}
	return nil
}

func (du *baseDateArithmitical) vecGetIntervalFromString(b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	amendInterval := func(val string) string {
		return val
	}
	if unitLower := strings.ToLower(unit); unitLower == "day" || unitLower == "hour" {
		amendInterval = func(val string) string {
			if intervalLower := strings.ToLower(val); intervalLower == "true" {
				return "1"
			} else if intervalLower == "false" {
				return "0"
			}
			return du.intervalRegexp.FindString(val)
		}
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		result.AppendString(amendInterval(buf.GetString(i)))
	}
	return nil
}

func (du *baseDateArithmitical) vecGetIntervalFromDecimal(b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalDecimal(b.ctx, input, buf); err != nil {
		return err
	}

	isCompoundUnit := false
	amendInterval := func(val string, row *chunk.Row) (string, bool, error) {
		return val, false, nil
	}
	switch unitUpper := strings.ToUpper(unit); unitUpper {
	case "HOUR_MINUTE", "MINUTE_SECOND", "YEAR_MONTH", "DAY_HOUR", "DAY_MINUTE",
		"DAY_SECOND", "DAY_MICROSECOND", "HOUR_MICROSECOND", "HOUR_SECOND", "MINUTE_MICROSECOND", "SECOND_MICROSECOND":
		isCompoundUnit = true
		switch strings.ToUpper(unit) {
		case "HOUR_MINUTE", "MINUTE_SECOND":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return strings.Replace(val, ".", ":", -1), false, nil
			}
		case "YEAR_MONTH":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return strings.Replace(val, ".", "-", -1), false, nil
			}
		case "DAY_HOUR":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return strings.Replace(val, ".", " ", -1), false, nil
			}
		case "DAY_MINUTE":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return "0 " + strings.Replace(val, ".", ":", -1), false, nil
			}
		case "DAY_SECOND":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return "0 00:" + strings.Replace(val, ".", ":", -1), false, nil
			}
		case "DAY_MICROSECOND":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return "0 00:00:" + val, false, nil
			}
		case "HOUR_MICROSECOND":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return "00:00:" + val, false, nil
			}
		case "HOUR_SECOND":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return "00:" + strings.Replace(val, ".", ":", -1), false, nil
			}
		case "MINUTE_MICROSECOND":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return "00:" + val, false, nil
			}
		case "SECOND_MICROSECOND":
			/* keep interval as original decimal */
		}
	case "SECOND":
		/* keep interval as original decimal */
	default:
		// YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, MICROSECOND
		castExpr := WrapWithCastAsString(b.ctx, WrapWithCastAsInt(b.ctx, b.args[1]))
		amendInterval = func(_ string, row *chunk.Row) (string, bool, error) {
			interval, isNull, err := castExpr.EvalString(b.ctx, *row)
			return interval, isNull || err != nil, err
		}
	}

	result.ReserveString(n)
	decs := buf.Decimals()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		interval := decs[i].String()
		row := input.GetRow(i)
		isNeg := false
		if isCompoundUnit && interval != "" && interval[0] == '-' {
			isNeg = true
			interval = interval[1:]
		}
		interval, isNull, err := amendInterval(interval, &row)
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
			continue
		}
		if isCompoundUnit && isNeg {
			interval = "-" + interval
		}
		result.AppendString(interval)
	}
	return nil
}

func (du *baseDateArithmitical) vecGetIntervalFromInt(b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := buf.Int64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
		} else {
			result.AppendString(strconv.FormatInt(i64s[i], 10))
		}
	}
	return nil
}

func (du *baseDateArithmitical) vecGetIntervalFromReal(b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	f64s := buf.Float64s()
	prec := b.args[1].GetType().Decimal
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
		} else {
			result.AppendString(strconv.FormatFloat(f64s[i], 'f', prec, 64))
		}
	}
	return nil
}

type addDateFunctionClass struct {
	baseFunctionClass
}

func (c *addDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	dateEvalTp := args[0].GetType().EvalType()
	if dateEvalTp != types.ETString && dateEvalTp != types.ETInt && dateEvalTp != types.ETDuration {
		dateEvalTp = types.ETDatetime
	}

	intervalEvalTp := args[1].GetType().EvalType()
	if intervalEvalTp != types.ETString && intervalEvalTp != types.ETDecimal && intervalEvalTp != types.ETReal {
		intervalEvalTp = types.ETInt
	}

	argTps := []types.EvalType{dateEvalTp, intervalEvalTp, types.ETString}
	var bf baseBuiltinFunc
	if dateEvalTp == types.ETDuration {
		unit, _, err := args[2].EvalString(ctx, chunk.Row{})
		if err != nil {
			return nil, err
		}
		internalFsp := 0
		switch unit {
		// If the unit has micro second, then the fsp must be the MaxFsp.
		case "MICROSECOND", "SECOND_MICROSECOND", "MINUTE_MICROSECOND", "HOUR_MICROSECOND", "DAY_MICROSECOND":
			internalFsp = int(types.MaxFsp)
		// If the unit is second, the fsp is related with the arg[1]'s.
		case "SECOND":
			internalFsp = int(types.MaxFsp)
			if intervalEvalTp != types.ETString {
				internalFsp = mathutil.Min(args[1].GetType().Decimal, int(types.MaxFsp))
			}
			// Otherwise, the fsp should be 0.
		}
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, argTps...)
		if err != nil {
			return nil, err
		}
		arg0Dec, err := getExpressionFsp(ctx, args[0])
		if err != nil {
			return nil, err
		}
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthWithFsp, mathutil.Max(arg0Dec, internalFsp)
	} else {
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, argTps...)
		if err != nil {
			return nil, err
		}
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeFullWidth, types.UnspecifiedLength
	}

	switch {
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETString:
		sig = &builtinAddDateStringStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateStringString)
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETInt:
		sig = &builtinAddDateStringIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateStringInt)
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETReal:
		sig = &builtinAddDateStringRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateStringReal)
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddDateStringDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateStringDecimal)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETString:
		sig = &builtinAddDateIntStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateIntString)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETInt:
		sig = &builtinAddDateIntIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateIntInt)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETReal:
		sig = &builtinAddDateIntRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateIntReal)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddDateIntDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateIntDecimal)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETString:
		sig = &builtinAddDateDatetimeStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateDatetimeString)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETInt:
		sig = &builtinAddDateDatetimeIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateDatetimeInt)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETReal:
		sig = &builtinAddDateDatetimeRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateDatetimeReal)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddDateDatetimeDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateDatetimeDecimal)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETString:
		sig = &builtinAddDateDurationStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateDurationString)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETInt:
		sig = &builtinAddDateDurationIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateDurationInt)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETReal:
		sig = &builtinAddDateDurationRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateDurationReal)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddDateDurationDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_AddDateDurationDecimal)
	}
	return sig, nil
}

type builtinAddDateStringStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateStringStringSig) Clone() builtinFunc {
	newSig := &builtinAddDateStringStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateStringIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateStringIntSig) Clone() builtinFunc {
	newSig := &builtinAddDateStringIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateStringRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateStringRealSig) Clone() builtinFunc {
	newSig := &builtinAddDateStringRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateStringDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateStringDecimalSig) Clone() builtinFunc {
	newSig := &builtinAddDateStringDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateStringDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateIntStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateIntStringSig) Clone() builtinFunc {
	newSig := &builtinAddDateIntStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateIntIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateIntIntSig) Clone() builtinFunc {
	newSig := &builtinAddDateIntIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateIntRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateIntRealSig) Clone() builtinFunc {
	newSig := &builtinAddDateIntRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateIntDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateIntDecimalSig) Clone() builtinFunc {
	newSig := &builtinAddDateIntDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateIntDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDatetimeStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDatetimeStringSig) Clone() builtinFunc {
	newSig := &builtinAddDateDatetimeStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDatetimeIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDatetimeIntSig) Clone() builtinFunc {
	newSig := &builtinAddDateDatetimeIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDatetimeRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDatetimeRealSig) Clone() builtinFunc {
	newSig := &builtinAddDateDatetimeRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDatetimeDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDatetimeDecimalSig) Clone() builtinFunc {
	newSig := &builtinAddDateDatetimeDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals ADDDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
func (b *builtinAddDateDatetimeDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.add(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDurationStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDurationStringSig) Clone() builtinFunc {
	newSig := &builtinAddDateDurationStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinAddDateDurationStringSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.addDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDurationIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDurationIntSig) Clone() builtinFunc {
	newSig := &builtinAddDateDurationIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinAddDateDurationIntSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}
	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.addDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDurationDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDurationDecimalSig) Clone() builtinFunc {
	newSig := &builtinAddDateDurationDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinAddDateDurationDecimalSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}
	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.addDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinAddDateDurationRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinAddDateDurationRealSig) Clone() builtinFunc {
	newSig := &builtinAddDateDurationRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinAddDateDurationRealSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}
	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.addDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type subDateFunctionClass struct {
	baseFunctionClass
}

func (c *subDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	dateEvalTp := args[0].GetType().EvalType()
	if dateEvalTp != types.ETString && dateEvalTp != types.ETInt && dateEvalTp != types.ETDuration {
		dateEvalTp = types.ETDatetime
	}

	intervalEvalTp := args[1].GetType().EvalType()
	if intervalEvalTp != types.ETString && intervalEvalTp != types.ETDecimal && intervalEvalTp != types.ETReal {
		intervalEvalTp = types.ETInt
	}

	argTps := []types.EvalType{dateEvalTp, intervalEvalTp, types.ETString}
	var bf baseBuiltinFunc
	if dateEvalTp == types.ETDuration {
		unit, _, err := args[2].EvalString(ctx, chunk.Row{})
		if err != nil {
			return nil, err
		}
		internalFsp := 0
		switch unit {
		// If the unit has micro second, then the fsp must be the MaxFsp.
		case "MICROSECOND", "SECOND_MICROSECOND", "MINUTE_MICROSECOND", "HOUR_MICROSECOND", "DAY_MICROSECOND":
			internalFsp = int(types.MaxFsp)
		// If the unit is second, the fsp is related with the arg[1]'s.
		case "SECOND":
			internalFsp = int(types.MaxFsp)
			if intervalEvalTp != types.ETString {
				internalFsp = mathutil.Min(args[1].GetType().Decimal, int(types.MaxFsp))
			}
			// Otherwise, the fsp should be 0.
		}
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, argTps...)
		if err != nil {
			return nil, err
		}
		arg0Dec, err := getExpressionFsp(ctx, args[0])
		if err != nil {
			return nil, err
		}
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDurationWidthWithFsp, mathutil.Max(arg0Dec, internalFsp)
	} else {
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, argTps...)
		if err != nil {
			return nil, err
		}
		bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeFullWidth, types.UnspecifiedLength
	}

	switch {
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETString:
		sig = &builtinSubDateStringStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateStringString)
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETInt:
		sig = &builtinSubDateStringIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateStringInt)
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETReal:
		sig = &builtinSubDateStringRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateStringReal)
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETDecimal:
		sig = &builtinSubDateStringDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateStringDecimal)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETString:
		sig = &builtinSubDateIntStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateIntString)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETInt:
		sig = &builtinSubDateIntIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateIntInt)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETReal:
		sig = &builtinSubDateIntRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateIntReal)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETDecimal:
		sig = &builtinSubDateIntDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateIntDecimal)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETString:
		sig = &builtinSubDateDatetimeStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateDatetimeString)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETInt:
		sig = &builtinSubDateDatetimeIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateDatetimeInt)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETReal:
		sig = &builtinSubDateDatetimeRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateDatetimeReal)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETDecimal:
		sig = &builtinSubDateDatetimeDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateDatetimeDecimal)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETString:
		sig = &builtinSubDateDurationStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateDurationString)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETInt:
		sig = &builtinSubDateDurationIntSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateDurationInt)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETReal:
		sig = &builtinSubDateDurationRealSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateDurationReal)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETDecimal:
		sig = &builtinSubDateDurationDecimalSig{
			baseBuiltinFunc:      bf,
			baseDateArithmitical: newDateArighmeticalUtil(),
		}
		sig.setPbCode(tipb.ScalarFuncSig_SubDateDurationDecimal)
	}
	return sig, nil
}

type builtinSubDateStringStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateStringStringSig) Clone() builtinFunc {
	newSig := &builtinSubDateStringStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateStringStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateStringIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateStringIntSig) Clone() builtinFunc {
	newSig := &builtinSubDateStringIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateStringIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateStringRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateStringRealSig) Clone() builtinFunc {
	newSig := &builtinSubDateStringRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateStringRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateStringDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateStringDecimalSig) Clone() builtinFunc {
	newSig := &builtinSubDateStringDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSubDateStringDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateIntStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateIntStringSig) Clone() builtinFunc {
	newSig := &builtinSubDateIntStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateIntIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateIntIntSig) Clone() builtinFunc {
	newSig := &builtinSubDateIntIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateIntRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateIntRealSig) Clone() builtinFunc {
	newSig := &builtinSubDateIntRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDatetimeStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

type builtinSubDateIntDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateIntDecimalSig) Clone() builtinFunc {
	newSig := &builtinSubDateIntDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateIntDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

func (b *builtinSubDateDatetimeStringSig) Clone() builtinFunc {
	newSig := &builtinSubDateDatetimeStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDatetimeIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDatetimeIntSig) Clone() builtinFunc {
	newSig := &builtinSubDateDatetimeIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeIntSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDatetimeRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDatetimeRealSig) Clone() builtinFunc {
	newSig := &builtinSubDateDatetimeRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeRealSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDatetimeDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDatetimeDecimalSig) Clone() builtinFunc {
	newSig := &builtinSubDateDatetimeDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals SUBDATE(date,INTERVAL expr unit).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
func (b *builtinSubDateDatetimeDecimalSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromDatetime(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.sub(b.ctx, date, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDurationStringSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDurationStringSig) Clone() builtinFunc {
	newSig := &builtinSubDateDurationStringSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSubDateDurationStringSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	interval, isNull, err := b.getIntervalFromString(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.subDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDurationIntSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDurationIntSig) Clone() builtinFunc {
	newSig := &builtinSubDateDurationIntSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSubDateDurationIntSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	interval, isNull, err := b.getIntervalFromInt(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.subDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDurationDecimalSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDurationDecimalSig) Clone() builtinFunc {
	newSig := &builtinSubDateDurationDecimalSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSubDateDurationDecimalSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	interval, isNull, err := b.getIntervalFromDecimal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.subDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type builtinSubDateDurationRealSig struct {
	baseBuiltinFunc
	baseDateArithmitical
}

func (b *builtinSubDateDurationRealSig) Clone() builtinFunc {
	newSig := &builtinSubDateDurationRealSig{baseDateArithmitical: b.baseDateArithmitical}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinSubDateDurationRealSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}
	interval, isNull, err := b.getIntervalFromReal(b.ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.subDuration(b.ctx, dur, interval, unit)
	return result, isNull || err != nil, err
}

type timestampDiffFunctionClass struct {
	baseFunctionClass
}

func (c *timestampDiffFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETDatetime, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	sig := &builtinTimestampDiffSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_TimestampDiff)
	return sig, nil
}

type builtinTimestampDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinTimestampDiffSig) Clone() builtinFunc {
	newSig := &builtinTimestampDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinTimestampDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampdiff
func (b *builtinTimestampDiffSig) evalInt(row chunk.Row) (int64, bool, error) {
	unit, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	lhs, isNull, err := b.args[1].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, handleInvalidTimeError(b.ctx, err)
	}
	rhs, isNull, err := b.args[2].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, handleInvalidTimeError(b.ctx, err)
	}
	if invalidLHS, invalidRHS := lhs.InvalidZero(), rhs.InvalidZero(); invalidLHS || invalidRHS {
		if invalidLHS {
			err = handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, lhs.String()))
		}
		if invalidRHS {
			err = handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, rhs.String()))
		}
		return 0, true, err
	}
	return types.TimestampDiff(unit, lhs, rhs), false, nil
}

type unixTimestampFunctionClass struct {
	baseFunctionClass
}

func (c *unixTimestampFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var (
		argTps              []types.EvalType
		retTp               types.EvalType
		retFLen, retDecimal int
	)

	if len(args) == 0 {
		retTp, retDecimal = types.ETInt, 0
	} else {
		argTps = []types.EvalType{types.ETDatetime}
		argType := args[0].GetType()
		argEvaltp := argType.EvalType()
		if argEvaltp == types.ETString {
			// Treat types.ETString as unspecified decimal.
			retDecimal = types.UnspecifiedLength
			if cnst, ok := args[0].(*Constant); ok {
				tmpStr, _, err := cnst.EvalString(ctx, chunk.Row{})
				if err != nil {
					return nil, err
				}
				retDecimal = 0
				if dotIdx := strings.LastIndex(tmpStr, "."); dotIdx >= 0 {
					retDecimal = len(tmpStr) - dotIdx - 1
				}
			}
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

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, retTp, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = retFLen
	bf.tp.Decimal = retDecimal

	var sig builtinFunc
	if len(args) == 0 {
		sig = &builtinUnixTimestampCurrentSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnixTimestampCurrent)
	} else if retTp == types.ETInt {
		sig = &builtinUnixTimestampIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnixTimestampInt)
	} else if retTp == types.ETDecimal {
		sig = &builtinUnixTimestampDecSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnixTimestampDec)
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
		return nil, err
	}

	// In MySQL's implementation, unix_timestamp() will truncate the result instead of rounding it.
	// Results below are from MySQL 5.7, which can prove it.
	//	mysql> select unix_timestamp(), unix_timestamp(now(0)), now(0), unix_timestamp(now(3)), now(3), now(6);
	//	+------------------+------------------------+---------------------+------------------------+-------------------------+----------------------------+
	//	| unix_timestamp() | unix_timestamp(now(0)) | now(0)              | unix_timestamp(now(3)) | now(3)                  | now(6)                     |
	//	+------------------+------------------------+---------------------+------------------------+-------------------------+----------------------------+
	//	|       1553503194 |             1553503194 | 2019-03-25 16:39:54 |         1553503194.992 | 2019-03-25 16:39:54.992 | 2019-03-25 16:39:54.992969 |
	//	+------------------+------------------------+---------------------+------------------------+-------------------------+----------------------------+
	err = dec.Round(dec, decimal, types.ModeTruncate)
	return dec, err
}

type builtinUnixTimestampCurrentSig struct {
	baseBuiltinFunc
}

func (b *builtinUnixTimestampCurrentSig) Clone() builtinFunc {
	newSig := &builtinUnixTimestampCurrentSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a UNIX_TIMESTAMP().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampCurrentSig) evalInt(row chunk.Row) (int64, bool, error) {
	nowTs, err := getStmtTimestamp(b.ctx)
	if err != nil {
		return 0, true, err
	}
	dec, err := goTimeToMysqlUnixTimestamp(nowTs, 1)
	if err != nil {
		return 0, true, err
	}
	intVal, err := dec.ToInt()
	if !terror.ErrorEqual(err, types.ErrTruncated) {
		terror.Log(err)
	}
	return intVal, false, nil
}

type builtinUnixTimestampIntSig struct {
	baseBuiltinFunc
}

func (b *builtinUnixTimestampIntSig) Clone() builtinFunc {
	newSig := &builtinUnixTimestampIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a UNIX_TIMESTAMP(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	return b.evalIntWithCtx(b.ctx, row)
}

func (b *builtinUnixTimestampIntSig) evalIntWithCtx(ctx sessionctx.Context, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.TimeStr, val), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}

	tz := ctx.GetSessionVars().Location()
	t, err := val.GoTime(tz)
	if err != nil {
		return 0, false, nil
	}
	dec, err := goTimeToMysqlUnixTimestamp(t, 1)
	if err != nil {
		return 0, true, err
	}
	intVal, err := dec.ToInt()
	if !terror.ErrorEqual(err, types.ErrTruncated) {
		terror.Log(err)
	}
	return intVal, false, nil
}

type builtinUnixTimestampDecSig struct {
	baseBuiltinFunc
}

func (b *builtinUnixTimestampDecSig) Clone() builtinFunc {
	newSig := &builtinUnixTimestampDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a UNIX_TIMESTAMP(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampDecSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		// Return 0 for invalid date time.
		return new(types.MyDecimal), isNull, nil
	}
	t, err := val.GoTime(getTimeZone(b.ctx))
	if err != nil {
		return new(types.MyDecimal), false, nil
	}
	result, err := goTimeToMysqlUnixTimestamp(t, b.tp.Decimal)
	return result, err != nil, err
}

type timestampFunctionClass struct {
	baseFunctionClass
}

func (c *timestampFunctionClass) getDefaultFsp(tp *types.FieldType) int8 {
	if tp.Tp == mysql.TypeDatetime || tp.Tp == mysql.TypeDate || tp.Tp == mysql.TypeDuration ||
		tp.Tp == mysql.TypeTimestamp {
		return int8(tp.Decimal)
	}
	switch cls := tp.EvalType(); cls {
	case types.ETInt:
		return types.MinFsp
	case types.ETReal, types.ETDatetime, types.ETTimestamp, types.ETDuration, types.ETJson, types.ETString:
		return types.MaxFsp
	case types.ETDecimal:
		if tp.Decimal < int(types.MaxFsp) {
			return int8(tp.Decimal)
		}
		return types.MaxFsp
	}
	return types.MaxFsp
}

func (c *timestampFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	evalTps, argLen := []types.EvalType{types.ETString}, len(args)
	if argLen == 2 {
		evalTps = append(evalTps, types.ETString)
	}
	fsp := c.getDefaultFsp(args[0].GetType())
	if argLen == 2 {
		fsp = mathutil.MaxInt8(fsp, c.getDefaultFsp(args[1].GetType()))
	}
	isFloat := false
	switch args[0].GetType().Tp {
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		isFloat = true
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, evalTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.Decimal, bf.tp.Flen = -1, 19
	if fsp != 0 {
		bf.tp.Flen += 1 + int(fsp)
	}
	var sig builtinFunc
	if argLen == 2 {
		sig = &builtinTimestamp2ArgsSig{bf, isFloat}
		sig.setPbCode(tipb.ScalarFuncSig_Timestamp2Args)
	} else {
		sig = &builtinTimestamp1ArgSig{bf, isFloat}
		sig.setPbCode(tipb.ScalarFuncSig_Timestamp1Arg)
	}
	return sig, nil
}

type builtinTimestamp1ArgSig struct {
	baseBuiltinFunc

	isFloat bool
}

func (b *builtinTimestamp1ArgSig) Clone() builtinFunc {
	newSig := &builtinTimestamp1ArgSig{isFloat: b.isFloat}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTimestamp1ArgSig.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_timestamp
func (b *builtinTimestamp1ArgSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	s, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	var tm types.Time
	sc := b.ctx.GetSessionVars().StmtCtx
	if b.isFloat {
		tm, err = types.ParseTimeFromFloatString(sc, s, mysql.TypeDatetime, types.GetFsp(s))
	} else {
		tm, err = types.ParseTime(sc, s, mysql.TypeDatetime, types.GetFsp(s))
	}
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
	}
	return tm, false, nil
}

type builtinTimestamp2ArgsSig struct {
	baseBuiltinFunc

	isFloat bool
}

func (b *builtinTimestamp2ArgsSig) Clone() builtinFunc {
	newSig := &builtinTimestamp2ArgsSig{isFloat: b.isFloat}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTimestamp2ArgsSig.
// See https://dev.mysql.com/doc/refman/5.5/en/date-and-time-functions.html#function_timestamp
func (b *builtinTimestamp2ArgsSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	var tm types.Time
	sc := b.ctx.GetSessionVars().StmtCtx
	if b.isFloat {
		tm, err = types.ParseTimeFromFloatString(sc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
	} else {
		tm, err = types.ParseTime(sc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
	}
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
	}
	arg1, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	if !isDuration(arg1) {
		return types.ZeroTime, true, nil
	}
	duration, err := types.ParseDuration(sc, arg1, types.GetFsp(arg1))
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
	}
	tmp, err := tm.Add(sc, duration)
	if err != nil {
		return types.ZeroTime, true, err
	}
	return tmp, false, nil
}

type timestampLiteralFunctionClass struct {
	baseFunctionClass
}

func (c *timestampLiteralFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	con, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for timestamp literal")
	}
	dt, err := con.Eval(chunk.Row{})
	if err != nil {
		return nil, err
	}
	str, err := dt.ToString()
	if err != nil {
		return nil, err
	}
	if !timestampPattern.MatchString(str) {
		return nil, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, str)
	}
	tm, err := types.ParseTime(ctx.GetSessionVars().StmtCtx, str, mysql.TypeTimestamp, types.GetFsp(str))
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, []Expression{}, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = mysql.MaxDatetimeWidthNoFsp, int(tm.Fsp())
	if tm.Fsp() > 0 {
		bf.tp.Flen += int(tm.Fsp()) + 1
	}
	sig := &builtinTimestampLiteralSig{bf, tm}
	return sig, nil
}

type builtinTimestampLiteralSig struct {
	baseBuiltinFunc
	tm types.Time
}

func (b *builtinTimestampLiteralSig) Clone() builtinFunc {
	newSig := &builtinTimestampLiteralSig{tm: b.tm}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals TIMESTAMP 'stringLit'.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
func (b *builtinTimestampLiteralSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	return b.tm, false, nil
}

// getFsp4TimeAddSub is used to in function 'ADDTIME' and 'SUBTIME' to evaluate `fsp` for the
// second parameter. It's used only if the second parameter is of string type. It's different
// from getFsp in that the result of getFsp4TimeAddSub is either 6 or 0.
func getFsp4TimeAddSub(s string) int8 {
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
func getBf4TimeAddSub(ctx sessionctx.Context, funcName string, args []Expression) (tp1, tp2 *types.FieldType, bf baseBuiltinFunc, err error) {
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
	arg0Dec, err := getExpressionFsp(ctx, args[0])
	if err != nil {
		return
	}
	arg1Dec, err := getExpressionFsp(ctx, args[1])
	if err != nil {
		return
	}

	bf, err = newBaseBuiltinFuncWithTp(ctx, funcName, args, retTp, argTp1, argTp2)
	if err != nil {
		return
	}
	bf.tp.Decimal = mathutil.Min(mathutil.Max(arg0Dec, arg1Dec), int(types.MaxFsp))
	if retTp == types.ETString {
		bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeString, mysql.MaxDatetimeWidthWithFsp, types.UnspecifiedLength
	}
	return
}

func getTimeZone(ctx sessionctx.Context) *time.Location {
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
		return "", err
	}
	ret, err := arg0.Add(sc, arg1)
	if err != nil {
		return "", err
	}
	fsp := types.MaxFsp
	if ret.Microsecond() == 0 {
		fsp = types.MinFsp
	}
	ret.SetFsp(fsp)
	return ret.String(), nil
}

// strDurationAddDuration adds duration to duration string, returns a string value.
func strDurationAddDuration(sc *stmtctx.StatementContext, d string, arg1 types.Duration) (string, error) {
	arg0, err := types.ParseDuration(sc, d, types.MaxFsp)
	if err != nil {
		return "", err
	}
	tmpDuration, err := arg0.Add(arg1)
	if err != nil {
		return "", err
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
		return "", err
	}
	arg1time, err := arg1.ConvertToTime(sc, uint8(types.GetFsp(arg1.String())))
	if err != nil {
		return "", err
	}
	tmpDuration := arg0.Sub(sc, &arg1time)
	fsp := types.MaxFsp
	if tmpDuration.MicroSecond() == 0 {
		fsp = types.MinFsp
	}
	resultDuration, err := tmpDuration.ConvertToTime(sc, mysql.TypeDatetime)
	if err != nil {
		return "", err
	}
	resultDuration.SetFsp(fsp)
	return resultDuration.String(), nil
}

// strDurationSubDuration subtracts duration from duration string, returns a string value.
func strDurationSubDuration(sc *stmtctx.StatementContext, d string, arg1 types.Duration) (string, error) {
	arg0, err := types.ParseDuration(sc, d, types.MaxFsp)
	if err != nil {
		return "", err
	}
	tmpDuration, err := arg0.Sub(arg1)
	if err != nil {
		return "", err
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

func (c *addTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	tp1, tp2, bf, err := getBf4TimeAddSub(ctx, c.funcName, args)
	if err != nil {
		return nil, err
	}
	switch tp1.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinAddDatetimeAndDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AddDatetimeAndDuration)
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeDateTimeNullSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AddTimeDateTimeNull)
		default:
			sig = &builtinAddDatetimeAndStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AddDatetimeAndString)
		}
	case mysql.TypeDate:
		bf.tp.Charset, bf.tp.Collate = ctx.GetSessionVars().GetCharsetInfo()
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinAddDateAndDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AddDateAndDuration)
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeStringNullSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AddTimeStringNull)
		default:
			sig = &builtinAddDateAndStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AddDateAndString)
		}
	case mysql.TypeDuration:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinAddDurationAndDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AddDurationAndDuration)
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeDurationNullSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AddTimeDurationNull)
		default:
			sig = &builtinAddDurationAndStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AddDurationAndString)
		}
	default:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinAddStringAndDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AddStringAndDuration)
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinAddTimeStringNullSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AddTimeStringNull)
		default:
			sig = &builtinAddStringAndStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AddStringAndString)
		}
	}
	return sig, nil
}

type builtinAddTimeDateTimeNullSig struct {
	baseBuiltinFunc
}

func (b *builtinAddTimeDateTimeNullSig) Clone() builtinFunc {
	newSig := &builtinAddTimeDateTimeNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinAddTimeDateTimeNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeDateTimeNullSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	return types.ZeroDatetime, true, nil
}

type builtinAddDatetimeAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinAddDatetimeAndDurationSig) Clone() builtinFunc {
	newSig := &builtinAddDatetimeAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinAddDatetimeAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDatetimeAndDurationSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	result, err := arg0.Add(b.ctx.GetSessionVars().StmtCtx, arg1)
	return result, err != nil, err
}

type builtinAddDatetimeAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinAddDatetimeAndStringSig) Clone() builtinFunc {
	newSig := &builtinAddDatetimeAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinAddDatetimeAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDatetimeAndStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	s, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	if !isDuration(s) {
		return types.ZeroDatetime, true, nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	arg1, err := types.ParseDuration(sc, s, types.GetFsp(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return types.ZeroDatetime, true, nil
		}
		return types.ZeroDatetime, true, err
	}
	result, err := arg0.Add(sc, arg1)
	return result, err != nil, err
}

type builtinAddTimeDurationNullSig struct {
	baseBuiltinFunc
}

func (b *builtinAddTimeDurationNullSig) Clone() builtinFunc {
	newSig := &builtinAddTimeDurationNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinAddTimeDurationNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeDurationNullSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	return types.ZeroDuration, true, nil
}

type builtinAddDurationAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinAddDurationAndDurationSig) Clone() builtinFunc {
	newSig := &builtinAddDurationAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDurationAndDurationSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	result, err := arg0.Add(arg1)
	if err != nil {
		return types.ZeroDuration, true, err
	}
	return result, false, nil
}

type builtinAddDurationAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinAddDurationAndStringSig) Clone() builtinFunc {
	newSig := &builtinAddDurationAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinAddDurationAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDurationAndStringSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	s, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	if !isDuration(s) {
		return types.ZeroDuration, true, nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	arg1, err := types.ParseDuration(sc, s, types.GetFsp(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return types.ZeroDuration, true, nil
		}
		return types.ZeroDuration, true, err
	}
	result, err := arg0.Add(arg1)
	if err != nil {
		return types.ZeroDuration, true, err
	}
	return result, false, nil
}

type builtinAddTimeStringNullSig struct {
	baseBuiltinFunc
}

func (b *builtinAddTimeStringNullSig) Clone() builtinFunc {
	newSig := &builtinAddTimeStringNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddTimeStringNullSig) evalString(row chunk.Row) (string, bool, error) {
	return "", true, nil
}

type builtinAddStringAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinAddStringAndDurationSig) Clone() builtinFunc {
	newSig := &builtinAddStringAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinAddStringAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddStringAndDurationSig) evalString(row chunk.Row) (result string, isNull bool, err error) {
	var (
		arg0 string
		arg1 types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1, isNull, err = b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	if isDuration(arg0) {
		result, err = strDurationAddDuration(sc, arg0, arg1)
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				return "", true, nil
			}
			return "", true, err
		}
		return result, false, nil
	}
	result, err = strDatetimeAddDuration(sc, arg0, arg1)
	return result, err != nil, err
}

type builtinAddStringAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinAddStringAndStringSig) Clone() builtinFunc {
	newSig := &builtinAddStringAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinAddStringAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddStringAndStringSig) evalString(row chunk.Row) (result string, isNull bool, err error) {
	var (
		arg0, arg1Str string
		arg1          types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1Type := b.args[1].GetType()
	if mysql.HasBinaryFlag(arg1Type.Flag) {
		return "", true, nil
	}
	arg1Str, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	arg1, err = types.ParseDuration(sc, arg1Str, getFsp4TimeAddSub(arg1Str))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return "", true, nil
		}
		return "", true, err
	}
	if isDuration(arg0) {
		result, err = strDurationAddDuration(sc, arg0, arg1)
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				return "", true, nil
			}
			return "", true, err
		}
		return result, false, nil
	}
	result, err = strDatetimeAddDuration(sc, arg0, arg1)
	return result, err != nil, err
}

type builtinAddDateAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinAddDateAndDurationSig) Clone() builtinFunc {
	newSig := &builtinAddDateAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinAddDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDateAndDurationSig) evalString(row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	result, err := arg0.Add(arg1)
	return result.String(), err != nil, err
}

type builtinAddDateAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinAddDateAndStringSig) Clone() builtinFunc {
	newSig := &builtinAddDateAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinAddDateAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_addtime
func (b *builtinAddDateAndStringSig) evalString(row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	s, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	if !isDuration(s) {
		return "", true, nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	arg1, err := types.ParseDuration(sc, s, getFsp4TimeAddSub(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return "", true, nil
		}
		return "", true, err
	}
	result, err := arg0.Add(arg1)
	return result.String(), err != nil, err
}

type convertTzFunctionClass struct {
	baseFunctionClass
}

func (c *convertTzFunctionClass) getDecimal(ctx sessionctx.Context, arg Expression) int {
	decimal := int(types.MaxFsp)
	if dt, isConstant := arg.(*Constant); isConstant {
		switch arg.GetType().EvalType() {
		case types.ETInt:
			decimal = 0
		case types.ETReal, types.ETDecimal:
			decimal = arg.GetType().Decimal
		case types.ETString:
			str, isNull, err := dt.EvalString(ctx, chunk.Row{})
			if err == nil && !isNull {
				decimal = types.DateFSP(str)
			}
		}
	}
	if decimal > int(types.MaxFsp) {
		return int(types.MaxFsp)
	}
	if decimal < int(types.MinFsp) {
		return int(types.MinFsp)
	}
	return decimal
}

func (c *convertTzFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	// tzRegex holds the regex to check whether a string is a time zone.
	tzRegex, err := regexp.Compile(`(^(\+|-)(0?[0-9]|1[0-2]):[0-5]?\d$)|(^\+13:00$)`)
	if err != nil {
		return nil, err
	}

	decimal := c.getDecimal(ctx, args[0])
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETDatetime, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.Decimal = decimal
	sig := &builtinConvertTzSig{
		baseBuiltinFunc: bf,
		timezoneRegex:   tzRegex,
	}
	sig.setPbCode(tipb.ScalarFuncSig_ConvertTz)
	return sig, nil
}

type builtinConvertTzSig struct {
	baseBuiltinFunc
	timezoneRegex *regexp.Regexp
}

func (b *builtinConvertTzSig) Clone() builtinFunc {
	newSig := &builtinConvertTzSig{timezoneRegex: b.timezoneRegex}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals CONVERT_TZ(dt,from_tz,to_tz).
// `CONVERT_TZ` function returns NULL if the arguments are invalid.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_convert-tz
func (b *builtinConvertTzSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	dt, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, nil
	}

	fromTzStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, nil
	}

	toTzStr, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, nil
	}

	return b.convertTz(dt, fromTzStr, toTzStr)
}

func (b *builtinConvertTzSig) convertTz(dt types.Time, fromTzStr, toTzStr string) (types.Time, bool, error) {
	if fromTzStr == "" || toTzStr == "" {
		return types.ZeroTime, true, nil
	}
	fromTzMatched := b.timezoneRegex.MatchString(fromTzStr)
	toTzMatched := b.timezoneRegex.MatchString(toTzStr)

	if !fromTzMatched && !toTzMatched {
		fromTz, err := time.LoadLocation(fromTzStr)
		if err != nil {
			return types.ZeroTime, true, nil
		}

		toTz, err := time.LoadLocation(toTzStr)
		if err != nil {
			return types.ZeroTime, true, nil
		}

		t, err := dt.GoTime(fromTz)
		if err != nil {
			return types.ZeroTime, true, nil
		}

		return types.NewTime(types.FromGoTime(t.In(toTz)), mysql.TypeDatetime, int8(b.tp.Decimal)), false, nil
	}
	if fromTzMatched && toTzMatched {
		t, err := dt.GoTime(time.Local)
		if err != nil {
			return types.ZeroTime, true, nil
		}

		return types.NewTime(types.FromGoTime(t.Add(timeZone2Duration(toTzStr)-timeZone2Duration(fromTzStr))), mysql.TypeDatetime, int8(b.tp.Decimal)), false, nil
	}
	return types.ZeroTime, true, nil
}

type makeDateFunctionClass struct {
	baseFunctionClass
}

func (c *makeDateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	tp := bf.tp
	tp.Tp, tp.Flen, tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, 0
	sig := &builtinMakeDateSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_MakeDate)
	return sig, nil
}

type builtinMakeDateSig struct {
	baseBuiltinFunc
}

func (b *builtinMakeDateSig) Clone() builtinFunc {
	newSig := &builtinMakeDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evaluates a builtinMakeDateSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_makedate
func (b *builtinMakeDateSig) evalTime(row chunk.Row) (d types.Time, isNull bool, err error) {
	args := b.getArgs()
	var year, dayOfYear int64
	year, isNull, err = args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return d, true, err
	}
	dayOfYear, isNull, err = args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return d, true, err
	}
	if dayOfYear <= 0 || year < 0 || year > 9999 {
		return d, true, nil
	}
	if year < 70 {
		year += 2000
	} else if year < 100 {
		year += 1900
	}
	startTime := types.NewTime(types.FromDate(int(year), 1, 1, 0, 0, 0, 0), mysql.TypeDate, 0)
	retTimestamp := types.TimestampDiff("DAY", types.ZeroDate, startTime)
	if retTimestamp == 0 {
		return d, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, startTime.String()))
	}
	ret := types.TimeFromDays(retTimestamp + dayOfYear - 1)
	if ret.IsZero() || ret.Year() > 9999 {
		return d, true, nil
	}
	return ret, false, nil
}

type makeTimeFunctionClass struct {
	baseFunctionClass
}

func (c *makeTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
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
	// MySQL will cast the first and second arguments to INT, and the third argument to DECIMAL.
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, types.ETInt, types.ETInt, types.ETReal)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = flen, decimal
	sig := &builtinMakeTimeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_MakeTime)
	return sig, nil
}

type builtinMakeTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinMakeTimeSig) Clone() builtinFunc {
	newSig := &builtinMakeTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMakeTimeSig) makeTime(hour int64, minute int64, second float64, hourUnsignedFlag bool) (types.Duration, error) {
	var overflow bool
	// MySQL TIME datatype: https://dev.mysql.com/doc/refman/5.7/en/time.html
	// ranges from '-838:59:59.000000' to '838:59:59.000000'
	if hour < 0 && hourUnsignedFlag {
		hour = 838
		overflow = true
	}
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
	return types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, fmt.Sprintf("%02d:%02d:%v", hour, minute, second), int8(fsp))
}

// evalDuration evals a builtinMakeTimeIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_maketime
func (b *builtinMakeTimeSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	dur := types.ZeroDuration
	dur.Fsp = types.MaxFsp
	hour, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return dur, isNull, err
	}
	minute, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return dur, isNull, err
	}
	if minute < 0 || minute >= 60 {
		return dur, true, nil
	}
	second, isNull, err := b.args[2].EvalReal(b.ctx, row)
	if isNull || err != nil {
		return dur, isNull, err
	}
	if second < 0 || second >= 60 {
		return dur, true, nil
	}
	dur, err = b.makeTime(hour, minute, second, mysql.HasUnsignedFlag(b.args[0].GetType().Flag))
	if err != nil {
		return dur, true, err
	}
	return dur, false, nil
}

type periodAddFunctionClass struct {
	baseFunctionClass
}

func (c *periodAddFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 6
	sig := &builtinPeriodAddSig{bf}
	return sig, nil
}

// validPeriod checks if this period is valid, it comes from MySQL 8.0+.
func validPeriod(p int64) bool {
	return !(p < 0 || p%100 == 0 || p%100 > 12)
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

func (b *builtinPeriodAddSig) Clone() builtinFunc {
	newSig := &builtinPeriodAddSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals PERIOD_ADD(P,N).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-add
func (b *builtinPeriodAddSig) evalInt(row chunk.Row) (int64, bool, error) {
	p, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	n, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	// in MySQL, if p is invalid but n is NULL, the result is NULL, so we have to check if n is NULL first.
	if !validPeriod(p) {
		return 0, false, errIncorrectArgs.GenWithStackByArgs("period_add")
	}

	sumMonth := int64(period2Month(uint64(p))) + n
	return int64(month2Period(uint64(sumMonth))), false, nil
}

type periodDiffFunctionClass struct {
	baseFunctionClass
}

func (c *periodDiffFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 6
	sig := &builtinPeriodDiffSig{bf}
	return sig, nil
}

type builtinPeriodDiffSig struct {
	baseBuiltinFunc
}

func (b *builtinPeriodDiffSig) Clone() builtinFunc {
	newSig := &builtinPeriodDiffSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals PERIOD_DIFF(P1,P2).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-diff
func (b *builtinPeriodDiffSig) evalInt(row chunk.Row) (int64, bool, error) {
	p1, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	p2, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if !validPeriod(p1) {
		return 0, false, errIncorrectArgs.GenWithStackByArgs("period_diff")
	}

	if !validPeriod(p2) {
		return 0, false, errIncorrectArgs.GenWithStackByArgs("period_diff")
	}

	return int64(period2Month(uint64(p1)) - period2Month(uint64(p2))), false, nil
}

type quarterFunctionClass struct {
	baseFunctionClass
}

func (c *quarterFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 1

	sig := &builtinQuarterSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Quarter)
	return sig, nil
}

type builtinQuarterSig struct {
	baseBuiltinFunc
}

func (b *builtinQuarterSig) Clone() builtinFunc {
	newSig := &builtinQuarterSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals QUARTER(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_quarter
func (b *builtinQuarterSig) evalInt(row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}

	if date.IsZero() {
		// MySQL compatibility, #11203
		// 0 | 0.0 should be converted to 0 value (not null)
		n, err := date.ToNumber().ToInt()
		isOriginalIntOrDecimalZero := err == nil && n == 0
		// Args like "0000-00-00", "0000-00-00 00:00:00" set Fsp to 6
		isOriginalStringZero := date.Fsp() > 0
		if isOriginalIntOrDecimalZero && !isOriginalStringZero {
			return 0, false, nil
		}
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
	}

	return int64((date.Month() + 2) / 3), false, nil
}

type secToTimeFunctionClass struct {
	baseFunctionClass
}

func (c *secToTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var retFlen, retFsp int
	argType := args[0].GetType()
	argEvalTp := argType.EvalType()
	if argEvalTp == types.ETString {
		retFsp = types.UnspecifiedLength
	} else {
		retFsp = argType.Decimal
	}
	if retFsp > int(types.MaxFsp) || retFsp == int(types.UnspecifiedFsp) {
		retFsp = int(types.MaxFsp)
	} else if retFsp < int(types.MinFsp) {
		retFsp = int(types.MinFsp)
	}
	retFlen = 10
	if retFsp > 0 {
		retFlen += 1 + retFsp
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, types.ETReal)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = retFlen, retFsp
	sig := &builtinSecToTimeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_SecToTime)
	return sig, nil
}

type builtinSecToTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinSecToTimeSig) Clone() builtinFunc {
	newSig := &builtinSecToTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals SEC_TO_TIME(seconds).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_sec-to-time
func (b *builtinSecToTimeSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	secondsFloat, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
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
	dur, err = types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, fmt.Sprintf("%s%02d:%02d:%s", negative, hour, minute, strconv.FormatFloat(secondDemical, 'f', -1, 64)), int8(b.tp.Decimal))
	if err != nil {
		return types.Duration{}, err != nil, err
	}
	return dur, false, nil
}

type subTimeFunctionClass struct {
	baseFunctionClass
}

func (c *subTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	tp1, tp2, bf, err := getBf4TimeAddSub(ctx, c.funcName, args)
	if err != nil {
		return nil, err
	}
	switch tp1.Tp {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinSubDatetimeAndDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubDatetimeAndDuration)
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeDateTimeNullSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubTimeDateTimeNull)
		default:
			sig = &builtinSubDatetimeAndStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubDatetimeAndString)
		}
	case mysql.TypeDate:
		bf.tp.Charset, bf.tp.Collate = ctx.GetSessionVars().GetCharsetInfo()
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinSubDateAndDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubDateAndDuration)
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeStringNullSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubTimeStringNull)
		default:
			sig = &builtinSubDateAndStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubDateAndString)
		}
	case mysql.TypeDuration:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinSubDurationAndDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubDurationAndDuration)
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeDurationNullSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubTimeDurationNull)
		default:
			sig = &builtinSubDurationAndStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubDurationAndString)
		}
	default:
		switch tp2.Tp {
		case mysql.TypeDuration:
			sig = &builtinSubStringAndDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubStringAndDuration)
		case mysql.TypeDatetime, mysql.TypeTimestamp:
			sig = &builtinSubTimeStringNullSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubTimeStringNull)
		default:
			sig = &builtinSubStringAndStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_SubStringAndString)
		}
	}
	return sig, nil
}

type builtinSubDatetimeAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinSubDatetimeAndDurationSig) Clone() builtinFunc {
	newSig := &builtinSubDatetimeAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinSubDatetimeAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDatetimeAndDurationSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	arg1time, err := arg1.ConvertToTime(sc, mysql.TypeDatetime)
	if err != nil {
		return arg1time, true, err
	}
	tmpDuration := arg0.Sub(sc, &arg1time)
	result, err := tmpDuration.ConvertToTime(sc, arg0.Type())
	return result, err != nil, err
}

type builtinSubDatetimeAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinSubDatetimeAndStringSig) Clone() builtinFunc {
	newSig := &builtinSubDatetimeAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinSubDatetimeAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDatetimeAndStringSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	s, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	if err != nil {
		return types.ZeroDatetime, true, err
	}
	if !isDuration(s) {
		return types.ZeroDatetime, true, nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	arg1, err := types.ParseDuration(sc, s, types.GetFsp(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return types.ZeroDatetime, true, nil
		}
		return types.ZeroDatetime, true, err
	}
	arg1time, err := arg1.ConvertToTime(sc, mysql.TypeDatetime)
	if err != nil {
		return types.ZeroDatetime, true, err
	}
	tmpDuration := arg0.Sub(sc, &arg1time)
	result, err := tmpDuration.ConvertToTime(sc, mysql.TypeDatetime)
	return result, err != nil, err
}

type builtinSubTimeDateTimeNullSig struct {
	baseBuiltinFunc
}

func (b *builtinSubTimeDateTimeNullSig) Clone() builtinFunc {
	newSig := &builtinSubTimeDateTimeNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinSubTimeDateTimeNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeDateTimeNullSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	return types.ZeroDatetime, true, nil
}

type builtinSubStringAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinSubStringAndDurationSig) Clone() builtinFunc {
	newSig := &builtinSubStringAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubStringAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubStringAndDurationSig) evalString(row chunk.Row) (result string, isNull bool, err error) {
	var (
		arg0 string
		arg1 types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1, isNull, err = b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	if isDuration(arg0) {
		result, err = strDurationSubDuration(sc, arg0, arg1)
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				return "", true, nil
			}
			return "", true, err
		}
		return result, false, nil
	}
	result, err = strDatetimeSubDuration(sc, arg0, arg1)
	return result, err != nil, err
}

type builtinSubStringAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinSubStringAndStringSig) Clone() builtinFunc {
	newSig := &builtinSubStringAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubStringAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubStringAndStringSig) evalString(row chunk.Row) (result string, isNull bool, err error) {
	var (
		s, arg0 string
		arg1    types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1Type := b.args[1].GetType()
	if mysql.HasBinaryFlag(arg1Type.Flag) {
		return "", true, nil
	}
	s, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	arg1, err = types.ParseDuration(sc, s, getFsp4TimeAddSub(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return "", true, nil
		}
		return "", true, err
	}
	if isDuration(arg0) {
		result, err = strDurationSubDuration(sc, arg0, arg1)
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				return "", true, nil
			}
			return "", true, err
		}
		return result, false, nil
	}
	result, err = strDatetimeSubDuration(sc, arg0, arg1)
	return result, err != nil, err
}

type builtinSubTimeStringNullSig struct {
	baseBuiltinFunc
}

func (b *builtinSubTimeStringNullSig) Clone() builtinFunc {
	newSig := &builtinSubTimeStringNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubTimeStringNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeStringNullSig) evalString(row chunk.Row) (string, bool, error) {
	return "", true, nil
}

type builtinSubDurationAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinSubDurationAndDurationSig) Clone() builtinFunc {
	newSig := &builtinSubDurationAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinSubDurationAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDurationAndDurationSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	result, err := arg0.Sub(arg1)
	if err != nil {
		return types.ZeroDuration, true, err
	}
	return result, false, nil
}

type builtinSubDurationAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinSubDurationAndStringSig) Clone() builtinFunc {
	newSig := &builtinSubDurationAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinSubDurationAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDurationAndStringSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	s, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	if !isDuration(s) {
		return types.ZeroDuration, true, nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	arg1, err := types.ParseDuration(sc, s, types.GetFsp(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return types.ZeroDuration, true, nil
		}
		return types.ZeroDuration, true, err
	}
	result, err := arg0.Sub(arg1)
	return result, err != nil, err
}

type builtinSubTimeDurationNullSig struct {
	baseBuiltinFunc
}

func (b *builtinSubTimeDurationNullSig) Clone() builtinFunc {
	newSig := &builtinSubTimeDurationNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinSubTimeDurationNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubTimeDurationNullSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	return types.ZeroDuration, true, nil
}

type builtinSubDateAndDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinSubDateAndDurationSig) Clone() builtinFunc {
	newSig := &builtinSubDateAndDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubDateAndDurationSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDateAndDurationSig) evalString(row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	result, err := arg0.Sub(arg1)
	return result.String(), err != nil, err
}

type builtinSubDateAndStringSig struct {
	baseBuiltinFunc
}

func (b *builtinSubDateAndStringSig) Clone() builtinFunc {
	newSig := &builtinSubDateAndStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubDateAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubDateAndStringSig) evalString(row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	s, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	if !isDuration(s) {
		return "", true, nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	arg1, err := types.ParseDuration(sc, s, getFsp4TimeAddSub(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			sc.AppendWarning(err)
			return "", true, nil
		}
		return "", true, err
	}
	result, err := arg0.Sub(arg1)
	if err != nil {
		return "", true, err
	}
	return result.String(), false, nil
}

type timeFormatFunctionClass struct {
	baseFunctionClass
}

func (c *timeFormatFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETDuration, types.ETString)
	if err != nil {
		return nil, err
	}
	// worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
	bf.tp.Flen = (args[1].GetType().Flen + 1) / 2 * 11
	sig := &builtinTimeFormatSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_TimeFormat)
	return sig, nil
}

type builtinTimeFormatSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeFormatSig) Clone() builtinFunc {
	newSig := &builtinTimeFormatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTimeFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-format
func (b *builtinTimeFormatSig) evalString(row chunk.Row) (string, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	// if err != nil, then dur is ZeroDuration, outputs 00:00:00 in this case which follows the behavior of mysql.
	if err != nil {
		logutil.BgLogger().Warn("time_format.args[0].EvalDuration failed", zap.Error(err))
	}
	if isNull {
		return "", isNull, err
	}
	formatMask, isNull, err := b.args[1].EvalString(b.ctx, row)
	if err != nil || isNull {
		return "", isNull, err
	}
	res, err := b.formatTime(b.ctx, dur, formatMask)
	return res, isNull, err
}

// formatTime see https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-format
func (b *builtinTimeFormatSig) formatTime(ctx sessionctx.Context, t types.Duration, formatMask string) (res string, err error) {
	return t.DurationFormat(formatMask)
}

type timeToSecFunctionClass struct {
	baseFunctionClass
}

func (c *timeToSecFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDuration)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 10
	sig := &builtinTimeToSecSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_TimeToSec)
	return sig, nil
}

type builtinTimeToSecSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeToSecSig) Clone() builtinFunc {
	newSig := &builtinTimeToSecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals TIME_TO_SEC(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-to-sec
func (b *builtinTimeToSecSig) evalInt(row chunk.Row) (int64, bool, error) {
	duration, isNull, err := b.args[0].EvalDuration(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
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

func (c *timestampAddFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp = &types.FieldType{Tp: mysql.TypeString, Flen: mysql.MaxDatetimeWidthNoFsp, Decimal: types.UnspecifiedLength}
	sig := &builtinTimestampAddSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_TimestampAdd)
	return sig, nil

}

type builtinTimestampAddSig struct {
	baseBuiltinFunc
}

func (b *builtinTimestampAddSig) Clone() builtinFunc {
	newSig := &builtinTimestampAddSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTimestampAddSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampadd
func (b *builtinTimestampAddSig) evalString(row chunk.Row) (string, bool, error) {
	unit, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	v, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg, isNull, err := b.args[2].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	tm1, err := arg.GoTime(time.Local)
	if err != nil {
		return "", isNull, err
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
		return "", true, types.ErrWrongValue.GenWithStackByArgs(types.TimeStr, unit)
	}
	r := types.NewTime(types.FromGoTime(tb), b.resolveType(arg.Type(), unit), fsp)
	if err = r.Check(b.ctx.GetSessionVars().StmtCtx); err != nil {
		return "", true, handleInvalidTimeError(b.ctx, err)
	}
	return r.String(), false, nil
}

func (b *builtinTimestampAddSig) resolveType(typ uint8, unit string) uint8 {
	// The approach below is from MySQL.
	// The field type for the result of an Item_date function is defined as
	// follows:
	//
	//- If first arg is a MYSQL_TYPE_DATETIME result is MYSQL_TYPE_DATETIME
	//- If first arg is a MYSQL_TYPE_DATE and the interval type uses hours,
	//	minutes, seconds or microsecond then type is MYSQL_TYPE_DATETIME.
	//- Otherwise the result is MYSQL_TYPE_STRING
	//	(This is because you can't know if the string contains a DATE, MYSQL_TIME
	//	or DATETIME argument)
	if typ == mysql.TypeDate && (unit == "HOUR" || unit == "MINUTE" || unit == "SECOND" || unit == "MICROSECOND") {
		return mysql.TypeDatetime
	}
	return typ
}

type toDaysFunctionClass struct {
	baseFunctionClass
}

func (c *toDaysFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	sig := &builtinToDaysSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_ToDays)
	return sig, nil
}

type builtinToDaysSig struct {
	baseBuiltinFunc
}

func (b *builtinToDaysSig) Clone() builtinFunc {
	newSig := &builtinToDaysSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinToDaysSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-days
func (b *builtinToDaysSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}
	ret := types.TimestampDiff("DAY", types.ZeroDate, arg)
	if ret == 0 {
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	return ret, false, nil
}

type toSecondsFunctionClass struct {
	baseFunctionClass
}

func (c *toSecondsFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	sig := &builtinToSecondsSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_ToSeconds)
	return sig, nil
}

type builtinToSecondsSig struct {
	baseBuiltinFunc
}

func (b *builtinToSecondsSig) Clone() builtinFunc {
	newSig := &builtinToSecondsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinToSecondsSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-seconds
func (b *builtinToSecondsSig) evalInt(row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(b.ctx, err)
	}
	ret := types.TimestampDiff("SECOND", types.ZeroDate, arg)
	if ret == 0 {
		return 0, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	return ret, false, nil
}

type utcTimeFunctionClass struct {
	baseFunctionClass
}

func (c *utcTimeFunctionClass) getFlenAndDecimal4UTCTime(ctx sessionctx.Context, args []Expression) (flen, decimal int) {
	if len(args) == 0 {
		flen, decimal = 8, 0
		return
	}
	if constant, ok := args[0].(*Constant); ok {
		fsp, isNull, err := constant.EvalInt(ctx, chunk.Row{})
		if isNull || err != nil || fsp > int64(types.MaxFsp) {
			decimal = int(types.MaxFsp)
		} else if fsp < int64(types.MinFsp) {
			decimal = int(types.MinFsp)
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

func (c *utcTimeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, 1)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen, bf.tp.Decimal = c.getFlenAndDecimal4UTCTime(bf.ctx, args)

	var sig builtinFunc
	if len(args) == 1 {
		sig = &builtinUTCTimeWithArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UTCTimeWithArg)
	} else {
		sig = &builtinUTCTimeWithoutArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UTCTimeWithoutArg)
	}
	return sig, nil
}

type builtinUTCTimeWithoutArgSig struct {
	baseBuiltinFunc
}

func (b *builtinUTCTimeWithoutArgSig) Clone() builtinFunc {
	newSig := &builtinUTCTimeWithoutArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinUTCTimeWithoutArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithoutArgSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	nowTs, err := getStmtTimestamp(b.ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	v, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, nowTs.UTC().Format(types.TimeFormat), 0)
	return v, false, err
}

type builtinUTCTimeWithArgSig struct {
	baseBuiltinFunc
}

func (b *builtinUTCTimeWithArgSig) Clone() builtinFunc {
	newSig := &builtinUTCTimeWithArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDuration evals a builtinUTCTimeWithArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithArgSig) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	fsp, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	if fsp > int64(types.MaxFsp) {
		return types.Duration{}, true, errors.Errorf("Too-big precision %v specified for 'utc_time'. Maximum is %v.", fsp, types.MaxFsp)
	}
	if fsp < int64(types.MinFsp) {
		return types.Duration{}, true, errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
	}
	nowTs, err := getStmtTimestamp(b.ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	v, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, nowTs.UTC().Format(types.TimeFSPFormat), int8(fsp))
	return v, false, err
}

type lastDayFunctionClass struct {
	baseFunctionClass
}

func (c *lastDayFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, int(types.DefaultFsp)
	sig := &builtinLastDaySig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LastDay)
	return sig, nil
}

type builtinLastDaySig struct {
	baseBuiltinFunc
}

func (b *builtinLastDaySig) Clone() builtinFunc {
	newSig := &builtinLastDaySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinLastDaySig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_last-day
func (b *builtinLastDaySig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(b.ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
	}
	tm := arg
	year, month := tm.Year(), tm.Month()
	if arg.InvalidZero() {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	lastDay := types.GetLastDay(year, month)
	ret := types.NewTime(types.FromDate(year, month, lastDay, 0, 0, 0, 0), mysql.TypeDate, types.DefaultFsp)
	return ret, false, nil
}

// getExpressionFsp calculates the fsp from given expression.
func getExpressionFsp(ctx sessionctx.Context, expression Expression) (int, error) {
	constExp, isConstant := expression.(*Constant)
	if isConstant && types.IsString(expression.GetType().Tp) && !isTemporalColumn(expression) {
		str, isNil, err := constExp.EvalString(ctx, chunk.Row{})
		if isNil || err != nil {
			return 0, err
		}
		return int(types.GetFsp(str)), nil
	}
	return mathutil.Min(expression.GetType().Decimal, int(types.MaxFsp)), nil
}

// tidbParseTsoFunctionClass extracts physical time from a tso
type tidbParseTsoFunctionClass struct {
	baseFunctionClass
}

func (c *tidbParseTsoFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := args[0].GetType().EvalType()
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, argTp, types.ETInt)
	if err != nil {
		return nil, err
	}

	bf.tp.Tp, bf.tp.Flen, bf.tp.Decimal = mysql.TypeDate, mysql.MaxDateWidth, int(types.DefaultFsp)
	sig := &builtinTidbParseTsoSig{bf}
	return sig, nil
}

type builtinTidbParseTsoSig struct {
	baseBuiltinFunc
}

func (b *builtinTidbParseTsoSig) Clone() builtinFunc {
	newSig := &builtinTidbParseTsoSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTidbParseTsoSig.
func (b *builtinTidbParseTsoSig) evalTime(row chunk.Row) (types.Time, bool, error) {
	arg, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil || arg <= 0 {
		return types.ZeroTime, true, handleInvalidTimeError(b.ctx, err)
	}

	t := oracle.GetTimeFromTS(uint64(arg))
	result := types.NewTime(types.FromGoTime(t), mysql.TypeDatetime, types.MaxFsp)
	err = result.ConvertTimeZone(time.Local, b.ctx.GetSessionVars().Location())
	if err != nil {
		return types.ZeroTime, true, err
	}
	return result, false, nil
}
