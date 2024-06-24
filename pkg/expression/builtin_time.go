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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package expression

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression/contextopt"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/parser"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/oracle"
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
	// durationPattern checks whether a string matches the format of duration.
	durationPattern = regexp.MustCompile(`^\s*[-]?(((\d{1,2}\s+)?0*\d{0,3}(:0*\d{1,2}){0,2})|(\d{1,7}))?(\.\d*)?\s*$`)

	// timestampPattern checks whether a string matches the format of timestamp.
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
	_ functionClass = &addSubDateFunctionClass{}
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
	_ builtinFunc = &builtinExtractDatetimeFromStringSig{}
	_ builtinFunc = &builtinExtractDatetimeSig{}
	_ builtinFunc = &builtinExtractDurationSig{}
	_ builtinFunc = &builtinAddSubDateAsStringSig{}
	_ builtinFunc = &builtinAddSubDateDatetimeAnySig{}
	_ builtinFunc = &builtinAddSubDateDurationAnySig{}
)

func convertTimeToMysqlTime(t time.Time, fsp int, roundMode types.RoundMode) (types.Time, error) {
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

func (c *dateFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDate()
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
func (b *builtinDateSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	expr, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}

	if expr.IsZero() && sqlMode(ctx).HasNoZeroDateMode() {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, expr.String()))
	}

	expr.SetCoreTime(types.FromDate(expr.Year(), expr.Month(), expr.Day(), 0, 0, 0, 0))
	expr.SetType(mysql.TypeDate)
	return expr, false, nil
}

type dateLiteralFunctionClass struct {
	baseFunctionClass
}

func (c *dateLiteralFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	con, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for date literal")
	}
	dt, err := con.Eval(ctx.GetEvalCtx(), chunk.Row{})
	if err != nil {
		return nil, err
	}
	str := dt.GetString()
	if !datePattern.MatchString(str) {
		return nil, types.ErrWrongValue.GenWithStackByArgs(types.DateStr, str)
	}
	tm, err := types.ParseDate(ctx.GetEvalCtx().TypeCtx(), str)
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, []Expression{}, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDate()
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
func (b *builtinDateLiteralSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	mode := sqlMode(ctx)
	if mode.HasNoZeroDateMode() && b.literal.IsZero() {
		return b.literal, true, types.ErrWrongValue.GenWithStackByArgs(types.DateStr, b.literal.String())
	}
	if mode.HasNoZeroInDateMode() && (b.literal.InvalidZero() && !b.literal.IsZero()) {
		return b.literal, true, types.ErrWrongValue.GenWithStackByArgs(types.DateStr, b.literal.String())
	}
	return b.literal, false, nil
}

type dateDiffFunctionClass struct {
	baseFunctionClass
}

func (c *dateDiffFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
func (b *builtinDateDiffSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	lhs, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}
	rhs, isNull, err := b.args[1].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}
	if invalidLHS, invalidRHS := lhs.InvalidZero(), rhs.InvalidZero(); invalidLHS || invalidRHS {
		if invalidLHS {
			err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, lhs.String()))
		}
		if invalidRHS {
			err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, rhs.String()))
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

func (c *timeDiffFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	arg0FieldTp, arg1FieldTp := args[0].GetType(ctx.GetEvalCtx()), args[1].GetType(ctx.GetEvalCtx())
	arg0Tp, arg1Tp := c.getArgEvalTp(arg0FieldTp), c.getArgEvalTp(arg1FieldTp)
	arg0Dec, err := getExpressionFsp(ctx, args[0])
	if err != nil {
		return nil, err
	}
	arg1Dec, err := getExpressionFsp(ctx, args[1])
	if err != nil {
		return nil, err
	}
	fsp := mathutil.Max(arg0Dec, arg1Dec)
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, arg0Tp, arg1Tp)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForTime(fsp)

	var sig builtinFunc
	// arg0 and arg1 must be the same time type(compatible), or timediff will return NULL.
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
func (b *builtinDurationDurationTimeDiffSig) evalDuration(ctx EvalContext, row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhs, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhs, isNull, err := b.args[1].EvalDuration(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	d, isNull, err = calculateDurationTimeDiff(ctx, lhs, rhs)
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
func (b *builtinTimeTimeTimeDiffSig) evalDuration(ctx EvalContext, row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhs, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhs, isNull, err := b.args[1].EvalTime(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	tc := typeCtx(ctx)
	d, isNull, err = calculateTimeDiff(tc, lhs, rhs)
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
func (b *builtinDurationStringTimeDiffSig) evalDuration(ctx EvalContext, row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhs, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhsStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	tc := typeCtx(ctx)
	rhs, _, isDuration, err := convertStringToDuration(tc, rhsStr, b.tp.GetDecimal())
	if err != nil || !isDuration {
		return d, true, err
	}

	d, isNull, err = calculateDurationTimeDiff(ctx, lhs, rhs)
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
func (b *builtinStringDurationTimeDiffSig) evalDuration(ctx EvalContext, row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhsStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhs, isNull, err := b.args[1].EvalDuration(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	tc := typeCtx(ctx)
	lhs, _, isDuration, err := convertStringToDuration(tc, lhsStr, b.tp.GetDecimal())
	if err != nil || !isDuration {
		return d, true, err
	}

	d, isNull, err = calculateDurationTimeDiff(ctx, lhs, rhs)
	return d, isNull, err
}

// calculateTimeDiff calculates interval difference of two types.Time.
func calculateTimeDiff(tc types.Context, lhs, rhs types.Time) (d types.Duration, isNull bool, err error) {
	d = lhs.Sub(tc, &rhs)
	d.Duration, err = types.TruncateOverflowMySQLTime(d.Duration)
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = tc.HandleTruncate(err)
	}
	return d, err != nil, err
}

// calculateDurationTimeDiff calculates interval difference of two types.Duration.
func calculateDurationTimeDiff(ctx EvalContext, lhs, rhs types.Duration) (d types.Duration, isNull bool, err error) {
	d, err = lhs.Sub(rhs)
	if err != nil {
		return d, true, err
	}

	d.Duration, err = types.TruncateOverflowMySQLTime(d.Duration)
	if types.ErrTruncatedWrongVal.Equal(err) {
		tc := typeCtx(ctx)
		err = tc.HandleTruncate(err)
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
func (b *builtinTimeStringTimeDiffSig) evalDuration(ctx EvalContext, row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhs, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhsStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	tc := typeCtx(ctx)
	_, rhs, isDuration, err := convertStringToDuration(tc, rhsStr, b.tp.GetDecimal())
	if err != nil || isDuration {
		return d, true, err
	}

	d, isNull, err = calculateTimeDiff(tc, lhs, rhs)
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
func (b *builtinStringTimeTimeDiffSig) evalDuration(ctx EvalContext, row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhsStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhs, isNull, err := b.args[1].EvalTime(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	tc := typeCtx(ctx)
	_, lhs, isDuration, err := convertStringToDuration(tc, lhsStr, b.tp.GetDecimal())
	if err != nil || isDuration {
		return d, true, err
	}

	d, isNull, err = calculateTimeDiff(tc, lhs, rhs)
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
func (b *builtinStringStringTimeDiffSig) evalDuration(ctx EvalContext, row chunk.Row) (d types.Duration, isNull bool, err error) {
	lhs, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	rhs, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	tc := typeCtx(ctx)
	fsp := b.tp.GetDecimal()
	lhsDur, lhsTime, lhsIsDuration, err := convertStringToDuration(tc, lhs, fsp)
	if err != nil {
		return d, true, err
	}

	rhsDur, rhsTime, rhsIsDuration, err := convertStringToDuration(tc, rhs, fsp)
	if err != nil {
		return d, true, err
	}

	if lhsIsDuration != rhsIsDuration {
		return d, true, nil
	}

	if lhsIsDuration {
		d, isNull, err = calculateDurationTimeDiff(ctx, lhsDur, rhsDur)
	} else {
		d, isNull, err = calculateTimeDiff(tc, lhsTime, rhsTime)
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
func (b *builtinNullTimeDiffSig) evalDuration(ctx EvalContext, row chunk.Row) (d types.Duration, isNull bool, err error) {
	return d, true, nil
}

// convertStringToDuration converts string to duration, it return types.Time because in some case
// it will converts string to datetime.
func convertStringToDuration(tc types.Context, str string, fsp int) (d types.Duration, t types.Time,
	isDuration bool, err error) {
	if n := strings.IndexByte(str, '.'); n >= 0 {
		lenStrFsp := len(str[n+1:])
		if lenStrFsp <= types.MaxFsp {
			fsp = mathutil.Max(lenStrFsp, fsp)
		}
	}
	return types.StrToDuration(tc, str, fsp)
}

type dateFormatFunctionClass struct {
	baseFunctionClass
}

func (c *dateFormatFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETDatetime, types.ETString)
	if err != nil {
		return nil, err
	}
	// worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
	bf.tp.SetFlen((args[1].GetType(ctx.GetEvalCtx()).GetFlen() + 1) / 2 * 11)
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
func (b *builtinDateFormatSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	t, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return "", isNull, handleInvalidTimeError(ctx, err)
	}
	formatMask, isNull, err := b.args[1].EvalString(ctx, row)
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
		return "", true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}

	res, err := t.DateFormat(formatMask)
	return res, isNull, err
}

type fromDaysFunctionClass struct {
	baseFunctionClass
}

func (c *fromDaysFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDate()
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
// See https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_from-days
func (b *builtinFromDaysSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	n, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}
	ret := types.TimeFromDays(n)
	// the maximum date value is 9999-12-31 in mysql 5.8.
	if ret.Year() > 9999 {
		return types.ZeroTime, true, nil
	}
	return ret, false, nil
}

type hourFunctionClass struct {
	baseFunctionClass
}

func (c *hourFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDuration)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(3)
	bf.tp.SetDecimal(0)
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
func (b *builtinHourSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.Hour()), false, nil
}

type minuteFunctionClass struct {
	baseFunctionClass
}

func (c *minuteFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDuration)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(2)
	bf.tp.SetDecimal(0)
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
func (b *builtinMinuteSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.Minute()), false, nil
}

type secondFunctionClass struct {
	baseFunctionClass
}

func (c *secondFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDuration)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(2)
	bf.tp.SetDecimal(0)
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
func (b *builtinSecondSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.Second()), false, nil
}

type microSecondFunctionClass struct {
	baseFunctionClass
}

func (c *microSecondFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDuration)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(6)
	bf.tp.SetDecimal(0)
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
func (b *builtinMicroSecondSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(ctx, row)
	// ignore error and return NULL
	if isNull || err != nil {
		return 0, true, nil
	}
	return int64(dur.MicroSecond()), false, nil
}

type monthFunctionClass struct {
	baseFunctionClass
}

func (c *monthFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(2)
	bf.tp.SetDecimal(0)
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
func (b *builtinMonthSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}

	return int64(date.Month()), false, nil
}

// monthNameFunctionClass see https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_monthname
type monthNameFunctionClass struct {
	baseFunctionClass
}

func (c *monthNameFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(10)
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

func (b *builtinMonthNameSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return "", true, handleInvalidTimeError(ctx, err)
	}
	mon := arg.Month()
	if (arg.IsZero() && sqlMode(ctx).HasNoZeroDateMode()) || mon < 0 || mon > len(types.MonthNames) {
		return "", true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	} else if mon == 0 || arg.IsZero() {
		return "", true, nil
	}
	return types.MonthNames[mon-1], false, nil
}

type dayNameFunctionClass struct {
	baseFunctionClass
}

func (c *dayNameFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(10)
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

func (b *builtinDayNameSig) evalIndex(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if arg.InvalidZero() {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	// Monday is 0, ... Sunday = 6 in MySQL
	// but in go, Sunday is 0, ... Saturday is 6
	// w will do a conversion.
	res := (int64(arg.Weekday()) + 6) % 7
	return res, false, nil
}

// evalString evals a builtinDayNameSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayname
func (b *builtinDayNameSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	idx, isNull, err := b.evalIndex(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return types.WeekdayNames[idx], false, nil
}

func (b *builtinDayNameSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	idx, isNull, err := b.evalIndex(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return float64(idx), false, nil
}

func (b *builtinDayNameSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	idx, isNull, err := b.evalIndex(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return idx, false, nil
}

type dayOfMonthFunctionClass struct {
	baseFunctionClass
}

func (c *dayOfMonthFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(2)
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
func (b *builtinDayOfMonthSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}
	return int64(arg.Day()), false, nil
}

type dayOfWeekFunctionClass struct {
	baseFunctionClass
}

func (c *dayOfWeekFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)
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
func (b *builtinDayOfWeekSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}
	if arg.InvalidZero() {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	// 1 is Sunday, 2 is Monday, .... 7 is Saturday
	return int64(arg.Weekday() + 1), false, nil
}

type dayOfYearFunctionClass struct {
	baseFunctionClass
}

func (c *dayOfYearFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(3)
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
func (b *builtinDayOfYearSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, isNull, handleInvalidTimeError(ctx, err)
	}
	if arg.InvalidZero() {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}

	return int64(arg.YearDay()), false, nil
}

type weekFunctionClass struct {
	baseFunctionClass
}

func (c *weekFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
	bf.tp.SetFlen(2)
	bf.tp.SetDecimal(0)

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
func (b *builtinWeekWithModeSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}

	if date.IsZero() || date.InvalidZero() {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
	}

	mode, isNull, err := b.args[1].EvalInt(ctx, row)
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
func (b *builtinWeekWithoutModeSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}

	if date.IsZero() || date.InvalidZero() {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
	}

	mode := 0
	if modeStr := ctx.GetDefaultWeekFormatMode(); modeStr != "" {
		mode, err = strconv.Atoi(modeStr)
		if err != nil {
			return 0, true, handleInvalidTimeError(ctx, types.ErrInvalidWeekModeFormat.GenWithStackByArgs(modeStr))
		}
	}

	week := date.Week(mode)
	return int64(week), false, nil
}

type weekDayFunctionClass struct {
	baseFunctionClass
}

func (c *weekDayFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)

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
func (b *builtinWeekDaySig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}

	if date.IsZero() || date.InvalidZero() {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
	}

	return int64(date.Weekday()+6) % 7, false, nil
}

type weekOfYearFunctionClass struct {
	baseFunctionClass
}

func (c *weekOfYearFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(2)
	bf.tp.SetDecimal(0)
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
func (b *builtinWeekOfYearSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}

	if date.IsZero() || date.InvalidZero() {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
	}

	week := date.Week(3)
	return int64(week), false, nil
}

type yearFunctionClass struct {
	baseFunctionClass
}

func (c *yearFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(4)
	bf.tp.SetDecimal(0)
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
func (b *builtinYearSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}
	return int64(date.Year()), false, nil
}

type yearWeekFunctionClass struct {
	baseFunctionClass
}

func (c *yearWeekFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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

	bf.tp.SetFlen(6)
	bf.tp.SetDecimal(0)

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
func (b *builtinYearWeekWithModeSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, isNull, handleInvalidTimeError(ctx, err)
	}
	if date.IsZero() || date.InvalidZero() {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
	}

	mode, isNull, err := b.args[1].EvalInt(ctx, row)
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
func (b *builtinYearWeekWithoutModeSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}

	if date.InvalidZero() {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
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

func (c *fromUnixTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	retTp, argTps := types.ETDatetime, make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETDecimal)
	if len(args) == 2 {
		retTp = types.ETString
		argTps = append(argTps, types.ETString)
	}

	arg0Tp := args[0].GetType(ctx.GetEvalCtx())
	isArg0Str := arg0Tp.EvalType() == types.ETString
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, retTp, argTps...)
	if err != nil {
		return nil, err
	}

	if fieldString(arg0Tp.GetType()) {
		//Improve string cast Unix Time precision
		x, ok := (bf.getArgs()[0]).(*ScalarFunction)
		if ok {
			//used to adjust FromUnixTime precision #Fixbug35184
			if x.FuncName.L == ast.Cast {
				if x.RetType.GetDecimal() == 0 && (x.RetType.GetType() == mysql.TypeNewDecimal) {
					x.RetType.SetDecimal(6)
					fieldLen := mathutil.Min(x.RetType.GetFlen()+6, mysql.MaxDecimalWidth)
					x.RetType.SetFlen(fieldLen)
				}
			}
		}
	}

	if len(args) > 1 {
		sig = &builtinFromUnixTime2ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_FromUnixTime2Arg)
		return sig, nil
	}

	// Calculate the time fsp.
	fsp := types.MaxFsp
	if !isArg0Str {
		if arg0Tp.GetDecimal() != types.UnspecifiedLength {
			fsp = mathutil.Min(bf.tp.GetDecimal(), arg0Tp.GetDecimal())
		}
	}
	bf.setDecimalAndFlenForDatetime(fsp)

	sig = &builtinFromUnixTime1ArgSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_FromUnixTime1Arg)
	return sig, nil
}

func evalFromUnixTime(ctx EvalContext, fsp int, unixTimeStamp *types.MyDecimal) (res types.Time, isNull bool, err error) {
	// 0 <= unixTimeStamp <= 32536771199.999999
	if unixTimeStamp.IsNegative() {
		return res, true, nil
	}
	integralPart, err := unixTimeStamp.ToInt()
	if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) && !terror.ErrorEqual(err, types.ErrOverflow) {
		return res, true, err
	}
	// The max integralPart should not be larger than 32536771199.
	// Refer to https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-28.html
	if integralPart > 32536771199 {
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

	tc := typeCtx(ctx)
	tmp := time.Unix(integralPart, fractionalPart).In(tc.Location())
	t, err := convertTimeToMysqlTime(tmp, fsp, types.ModeHalfUp)
	if err != nil {
		return res, true, err
	}
	return t, false, nil
}

// fieldString returns true if precision cannot be determined
func fieldString(fieldType byte) bool {
	switch fieldType {
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		return true
	default:
		return false
	}
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
func (b *builtinFromUnixTime1ArgSig) evalTime(ctx EvalContext, row chunk.Row) (res types.Time, isNull bool, err error) {
	unixTimeStamp, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if err != nil || isNull {
		return res, isNull, err
	}
	return evalFromUnixTime(ctx, b.tp.GetDecimal(), unixTimeStamp)
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
func (b *builtinFromUnixTime2ArgSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	format, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	unixTimeStamp, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if err != nil || isNull {
		return "", isNull, err
	}
	t, isNull, err := evalFromUnixTime(ctx, b.tp.GetDecimal(), unixTimeStamp)
	if isNull || err != nil {
		return "", isNull, err
	}
	res, err = t.DateFormat(format)
	return res, err != nil, err
}

type getFormatFunctionClass struct {
	baseFunctionClass
}

func (c *getFormatFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(17)
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
func (b *builtinGetFormatSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	t, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	l, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	res := b.getFormat(t, l)
	return res, false, nil
}

type strToDateFunctionClass struct {
	baseFunctionClass
}

func (c *strToDateFunctionClass) getRetTp(ctx BuildContext, arg Expression) (tp byte, fsp int) {
	tp = mysql.TypeDatetime
	if _, ok := arg.(*Constant); !ok {
		return tp, types.MaxFsp
	}
	strArg := WrapWithCastAsString(ctx, arg)
	format, isNull, err := strArg.EvalString(ctx.GetEvalCtx(), chunk.Row{})
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
func (c *strToDateFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
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
		bf.setDecimalAndFlenForDate()
		sig = &builtinStrToDateDateSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_StrToDateDate)
	case mysql.TypeDatetime:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETString, types.ETString)
		if err != nil {
			return nil, err
		}
		bf.setDecimalAndFlenForDatetime(fsp)
		sig = &builtinStrToDateDatetimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_StrToDateDatetime)
	case mysql.TypeDuration:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, types.ETString, types.ETString)
		if err != nil {
			return nil, err
		}
		bf.setDecimalAndFlenForTime(fsp)
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

func (b *builtinStrToDateDateSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	date, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	format, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	var t types.Time
	tc := typeCtx(ctx)
	succ := t.StrToDate(tc, date, format)
	if !succ {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	if sqlMode(ctx).HasNoZeroDateMode() && (t.Year() == 0 || t.Month() == 0 || t.Day() == 0) {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValueForType.GenWithStackByArgs(types.DateTimeStr, date, ast.StrToDate))
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

func (b *builtinStrToDateDatetimeSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	date, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	format, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	var t types.Time
	tc := typeCtx(ctx)
	succ := t.StrToDate(tc, date, format)
	if !succ {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	if sqlMode(ctx).HasNoZeroDateMode() && (t.Year() == 0 || t.Month() == 0 || t.Day() == 0) {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	t.SetType(mysql.TypeDatetime)
	t.SetFsp(b.tp.GetDecimal())
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
func (b *builtinStrToDateDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	date, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	format, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	var t types.Time
	tc := typeCtx(ctx)
	succ := t.StrToDate(tc, date, format)
	if !succ {
		return types.Duration{}, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String()))
	}
	t.SetFsp(b.tp.GetDecimal())
	dur, err := t.ConvertToDuration()
	return dur, err != nil, err
}

type sysDateFunctionClass struct {
	baseFunctionClass
}

func (c *sysDateFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	fsp, err := getFspByIntArg(ctx, args)
	if err != nil {
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
	bf.setDecimalAndFlenForDatetime(fsp)
	// Illegal parameters have been filtered out in the parser, so the result is always not null.
	bf.tp.SetFlag(bf.tp.GetFlag() | mysql.NotNullFlag)

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
func (b *builtinSysDateWithFspSig) evalTime(ctx EvalContext, row chunk.Row) (val types.Time, isNull bool, err error) {
	fsp, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}

	loc := location(ctx)
	now := time.Now().In(loc)
	result, err := convertTimeToMysqlTime(now, int(fsp), types.ModeHalfUp)
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
func (b *builtinSysDateWithoutFspSig) evalTime(ctx EvalContext, row chunk.Row) (val types.Time, isNull bool, err error) {
	tz := location(ctx)
	now := time.Now().In(tz)
	result, err := convertTimeToMysqlTime(now, 0, types.ModeHalfUp)
	if err != nil {
		return types.ZeroTime, true, err
	}
	return result, false, nil
}

type currentDateFunctionClass struct {
	baseFunctionClass
}

func (c *currentDateFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDate()
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
func (b *builtinCurrentDateSig) evalTime(ctx EvalContext, row chunk.Row) (val types.Time, isNull bool, err error) {
	tz := location(ctx)
	nowTs, err := getStmtTimestamp(ctx)
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

func (c *currentTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	fsp, err := getFspByIntArg(ctx, args)
	if err != nil {
		return nil, err
	}
	var argTps = make([]types.EvalType, 0)
	if len(args) == 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, argTps...)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForTime(fsp)
	// 1. no sign.
	// 2. hour is in the 2-digit range.
	bf.tp.SetFlen(bf.tp.GetFlen() - 2)
	if len(args) == 0 {
		sig = &builtinCurrentTime0ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CurrentTime0Arg)
		return sig, nil
	}
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

func (b *builtinCurrentTime0ArgSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	tz := location(ctx)
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	dur := nowTs.In(tz).Format(types.TimeFormat)
	res, _, err := types.ParseDuration(typeCtx(ctx), dur, types.MinFsp)
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

func (b *builtinCurrentTime1ArgSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	fsp, _, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return types.Duration{}, true, err
	}
	tz := location(ctx)
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	dur := nowTs.In(tz).Format(types.TimeFSPFormat)
	tc := typeCtx(ctx)
	res, _, err := types.ParseDuration(tc, dur, int(fsp))
	if err != nil {
		return types.Duration{}, true, err
	}
	return res, false, nil
}

type timeFunctionClass struct {
	baseFunctionClass
}

func (c *timeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, err
	}
	fsp, err := getExpressionFsp(ctx, args[0])
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForTime(fsp)
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
func (b *builtinTimeSig) evalDuration(ctx EvalContext, row chunk.Row) (res types.Duration, isNull bool, err error) {
	expr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	fsp := 0
	if idx := strings.Index(expr, "."); idx != -1 {
		fsp = len(expr) - idx - 1
	}

	var tmpFsp int
	if tmpFsp, err = types.CheckFsp(fsp); err != nil {
		return res, isNull, err
	}
	fsp = tmpFsp

	tc := typeCtx(ctx)
	res, _, err = types.ParseDuration(tc, expr, fsp)
	if types.ErrTruncatedWrongVal.Equal(err) {
		err = tc.HandleTruncate(err)
	}
	return res, isNull, err
}

type timeLiteralFunctionClass struct {
	baseFunctionClass
}

func (c *timeLiteralFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	con, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for time literal")
	}
	dt, err := con.Eval(ctx.GetEvalCtx(), chunk.Row{})
	if err != nil {
		return nil, err
	}
	str := dt.GetString()
	if !isDuration(str) {
		return nil, types.ErrWrongValue.GenWithStackByArgs(types.TimeStr, str)
	}
	duration, _, err := types.ParseDuration(ctx.GetEvalCtx().TypeCtx(), str, types.GetFsp(str))
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, []Expression{}, types.ETDuration)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForTime(duration.Fsp)
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
func (b *builtinTimeLiteralSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	return b.duration, false, nil
}

type utcDateFunctionClass struct {
	baseFunctionClass
}

func (c *utcDateFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDate()
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
func (b *builtinUTCDateSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	nowTs, err := getStmtTimestamp(ctx)
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

func (c *utcTimestampFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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

	fsp, err := getFspByIntArg(ctx, args)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDatetime(fsp)
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

func evalUTCTimestampWithFsp(ctx EvalContext, fsp int) (types.Time, bool, error) {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}
	result, err := convertTimeToMysqlTime(nowTs.UTC(), fsp, types.ModeHalfUp)
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
func (b *builtinUTCTimestampWithArgSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	num, isNull, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return types.ZeroTime, true, err
	}

	if !isNull && num > int64(types.MaxFsp) {
		return types.ZeroTime, true, errors.Errorf("Too-big precision %v specified for 'utc_timestamp'. Maximum is %v", num, types.MaxFsp)
	}
	if !isNull && num < int64(types.MinFsp) {
		return types.ZeroTime, true, errors.Errorf("Invalid negative %d specified, must in [0, 6]", num)
	}

	result, isNull, err := evalUTCTimestampWithFsp(ctx, int(num))
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
func (b *builtinUTCTimestampWithoutArgSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	result, isNull, err := evalUTCTimestampWithFsp(ctx, 0)
	return result, isNull, err
}

type nowFunctionClass struct {
	baseFunctionClass
}

func (c *nowFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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

	fsp, err := getFspByIntArg(ctx, args)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDatetime(fsp)

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

// GetStmtTimestamp directly calls getTimeZone with timezone
func GetStmtTimestamp(ctx EvalContext) (time.Time, error) {
	tz := getTimeZone(ctx)
	tVal, err := getStmtTimestamp(ctx)
	if err != nil {
		return tVal, err
	}
	return tVal.In(tz), nil
}

func evalNowWithFsp(ctx EvalContext, fsp int) (types.Time, bool, error) {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}

	failpoint.Inject("injectNow", func(val failpoint.Value) {
		nowTs = time.Unix(int64(val.(int)), 0)
	})

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

	err = result.ConvertTimeZone(time.Local, location(ctx))
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
func (b *builtinNowWithArgSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	fsp, isNull, err := b.args[0].EvalInt(ctx, row)

	if err != nil {
		return types.ZeroTime, true, err
	}

	if isNull {
		fsp = 0
	} else if fsp > int64(types.MaxFsp) {
		return types.ZeroTime, true, errors.Errorf("Too-big precision %v specified for 'now'. Maximum is %v", fsp, types.MaxFsp)
	} else if fsp < int64(types.MinFsp) {
		return types.ZeroTime, true, errors.Errorf("Invalid negative %d specified, must in [0, 6]", fsp)
	}

	result, isNull, err := evalNowWithFsp(ctx, int(fsp))
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
func (b *builtinNowWithoutArgSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	result, isNull, err := evalNowWithFsp(ctx, 0)
	return result, isNull, err
}

type extractFunctionClass struct {
	baseFunctionClass
}

func (c *extractFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	args[0] = WrapWithCastAsString(ctx, args[0])
	unit, _, err := args[0].EvalString(ctx.GetEvalCtx(), chunk.Row{})
	if err != nil {
		return nil, err
	}
	isClockUnit := types.IsClockUnit(unit)
	isDateUnit := types.IsDateUnit(unit)
	var bf baseBuiltinFunc
	if isClockUnit && isDateUnit {
		// For unit DAY_MICROSECOND/DAY_SECOND/DAY_MINUTE/DAY_HOUR, the interpretation of the second argument depends on its evaluation type:
		// 1. Datetime/timestamp are interpreted as datetime. For example:
		// extract(day_second from datetime('2001-01-01 02:03:04')) = 120304
		// Note that MySQL 5.5+ has a bug of no day portion in the result (20304) for this case, see https://bugs.mysql.com/bug.php?id=73240.
		// 2. Time is interpreted as is. For example:
		// extract(day_second from time('02:03:04')) = 20304
		// Note that time shouldn't be implicitly cast to datetime, or else the date portion will be padded with the current date and this will adjust time portion accordingly.
		// 3. Otherwise, string/int/float are interpreted as arbitrarily either datetime or time, depending on which fits. For example:
		// extract(day_second from '2001-01-01 02:03:04') = 1020304 // datetime
		// extract(day_second from 20010101020304) = 1020304 // datetime
		// extract(day_second from '01 02:03:04') = 260304 // time
		if args[1].GetType(ctx.GetEvalCtx()).EvalType() == types.ETDatetime || args[1].GetType(ctx.GetEvalCtx()).EvalType() == types.ETTimestamp {
			bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETDatetime)
			if err != nil {
				return nil, err
			}
			sig = &builtinExtractDatetimeSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_ExtractDatetime)
		} else if args[1].GetType(ctx.GetEvalCtx()).EvalType() == types.ETDuration {
			bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETDuration)
			if err != nil {
				return nil, err
			}
			sig = &builtinExtractDurationSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_ExtractDuration)
		} else {
			bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETString)
			if err != nil {
				return nil, err
			}
			bf.args[1].GetType(ctx.GetEvalCtx()).SetDecimal(int(types.MaxFsp))
			sig = &builtinExtractDatetimeFromStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_ExtractDatetimeFromString)
		}
	} else if isClockUnit {
		// Clock units interpret the second argument as time.
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETDuration)
		if err != nil {
			return nil, err
		}
		sig = &builtinExtractDurationSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ExtractDuration)
	} else {
		// Date units interpret the second argument as datetime.
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETDatetime)
		if err != nil {
			return nil, err
		}
		sig = &builtinExtractDatetimeSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ExtractDatetime)
	}
	return sig, nil
}

type builtinExtractDatetimeFromStringSig struct {
	baseBuiltinFunc
}

func (b *builtinExtractDatetimeFromStringSig) Clone() builtinFunc {
	newSig := &builtinExtractDatetimeFromStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinExtractDatetimeFromStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
func (b *builtinExtractDatetimeFromStringSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	unit, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	dtStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	tc := typeCtx(ctx)
	if types.IsClockUnit(unit) && types.IsDateUnit(unit) {
		dur, _, err := types.ParseDuration(tc, dtStr, types.GetFsp(dtStr))
		if err != nil {
			return 0, true, err
		}
		res, err := types.ExtractDurationNum(&dur, unit)
		if err != nil {
			return 0, true, err
		}
		dt, err := types.ParseDatetime(tc, dtStr)
		if err != nil {
			return res, false, nil
		}
		if dt.Hour() == dur.Hour() && dt.Minute() == dur.Minute() && dt.Second() == dur.Second() && dt.Year() > 0 {
			res, err = types.ExtractDatetimeNum(&dt, unit)
		}
		return res, err != nil, err
	}

	panic("Unexpected unit for extract")
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
func (b *builtinExtractDatetimeSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	unit, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	dt, isNull, err := b.args[1].EvalTime(ctx, row)
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
func (b *builtinExtractDurationSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	unit, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	dur, isNull, err := b.args[1].EvalDuration(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	res, err := types.ExtractDurationNum(&dur, unit)
	return res, err != nil, err
}

// baseDateArithmetical is the base class for all "builtinAddDateXXXSig" and "builtinSubDateXXXSig",
// which provides parameter getter and date arithmetical calculate functions.
type baseDateArithmetical struct {
	// intervalRegexp is "*Regexp" used to extract string interval for "DAY" unit.
	intervalRegexp *regexp.Regexp
}

func newDateArithmeticalUtil() baseDateArithmetical {
	return baseDateArithmetical{
		intervalRegexp: regexp.MustCompile(`^[+-]?[\d]+`),
	}
}

func (du *baseDateArithmetical) getDateFromString(ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	dateStr, isNull, err := args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	dateTp := mysql.TypeDate
	if !types.IsDateFormat(dateStr) || types.IsClockUnit(unit) {
		dateTp = mysql.TypeDatetime
	}

	tc := typeCtx(ctx)
	date, err := types.ParseTime(tc, dateStr, dateTp, types.MaxFsp)
	if err != nil {
		err = handleInvalidTimeError(ctx, err)
		if err != nil {
			return types.ZeroTime, true, err
		}
		return date, true, handleInvalidTimeError(ctx, err)
	} else if sqlMode(ctx).HasNoZeroDateMode() && (date.Year() == 0 || date.Month() == 0 || date.Day() == 0) {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, dateStr))
	}
	return date, false, handleInvalidTimeError(ctx, err)
}

func (du *baseDateArithmetical) getDateFromInt(ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	dateInt, isNull, err := args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	tc := typeCtx(ctx)
	date, err := types.ParseTimeFromInt64(tc, dateInt)
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}

	// The actual date.Type() might be date or datetime.
	// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
	if types.IsClockUnit(unit) {
		date.SetType(mysql.TypeDatetime)
	}
	return date, false, nil
}

func (du *baseDateArithmetical) getDateFromReal(ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	dateReal, isNull, err := args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	tc := typeCtx(ctx)
	date, err := types.ParseTimeFromFloat64(tc, dateReal)
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}

	// The actual date.Type() might be date or datetime.
	// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
	if types.IsClockUnit(unit) {
		date.SetType(mysql.TypeDatetime)
	}
	return date, false, nil
}

func (du *baseDateArithmetical) getDateFromDecimal(ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	dateDec, isNull, err := args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	tc := typeCtx(ctx)
	date, err := types.ParseTimeFromDecimal(tc, dateDec)
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}

	// The actual date.Type() might be date or datetime.
	// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
	if types.IsClockUnit(unit) {
		date.SetType(mysql.TypeDatetime)
	}
	return date, false, nil
}

func (du *baseDateArithmetical) getDateFromDatetime(ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	date, isNull, err := args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	// The actual date.Type() might be date, datetime or timestamp.
	// Datetime is treated as is.
	// Timestamp is treated as datetime, as MySQL manual says: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-add
	// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
	if types.IsClockUnit(unit) || date.Type() == mysql.TypeTimestamp {
		date.SetType(mysql.TypeDatetime)
	}
	return date, false, nil
}

func (du *baseDateArithmetical) getIntervalFromString(ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	ec := errCtx(ctx)
	interval, err = du.intervalReformatString(ec, interval, unit)
	return interval, false, err
}

func (du *baseDateArithmetical) intervalReformatString(ec errctx.Context, str string, unit string) (interval string, err error) {
	switch strings.ToUpper(unit) {
	case "MICROSECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR":
		str = strings.TrimSpace(str)
		// a single unit value has to be specially handled.
		interval = du.intervalRegexp.FindString(str)
		if interval == "" {
			interval = "0"
		}

		if interval != str {
			err = ec.HandleError(types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", str))
		}
	case "SECOND":
		// The unit SECOND is specially handled, for example:
		// date + INTERVAL "1e2" SECOND = date + INTERVAL 100 second
		// date + INTERVAL "1.6" SECOND = date + INTERVAL 1.6 second
		// But:
		// date + INTERVAL "1e2" MINUTE = date + INTERVAL 1 MINUTE
		// date + INTERVAL "1.6" MINUTE = date + INTERVAL 1 MINUTE
		var dec types.MyDecimal
		if err = dec.FromString([]byte(str)); err != nil {
			truncatedErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", str)
			err = ec.HandleErrorWithAlias(err, truncatedErr, truncatedErr)
		}
		interval = string(dec.ToString())
	default:
		interval = str
	}
	return interval, err
}

func (du *baseDateArithmetical) intervalDecimalToString(ec errctx.Context, dec *types.MyDecimal) (string, error) {
	var rounded types.MyDecimal
	err := dec.Round(&rounded, 0, types.ModeHalfUp)
	if err != nil {
		return "", err
	}

	intVal, err := rounded.ToInt()
	if err != nil {
		if err = ec.HandleError(types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", dec.String())); err != nil {
			return "", err
		}
	}

	return strconv.FormatInt(intVal, 10), nil
}

func (du *baseDateArithmetical) getIntervalFromDecimal(ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
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
			interval = strings.ReplaceAll(interval, ".", ":")
		case "YEAR_MONTH":
			interval = strings.ReplaceAll(interval, ".", "-")
		case "DAY_HOUR":
			interval = strings.ReplaceAll(interval, ".", " ")
		case "DAY_MINUTE":
			interval = "0 " + strings.ReplaceAll(interval, ".", ":")
		case "DAY_SECOND":
			interval = "0 00:" + strings.ReplaceAll(interval, ".", ":")
		case "DAY_MICROSECOND":
			interval = "0 00:00:" + interval
		case "HOUR_MICROSECOND":
			interval = "00:00:" + interval
		case "HOUR_SECOND":
			interval = "00:" + strings.ReplaceAll(interval, ".", ":")
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
		ec := errCtx(ctx)
		interval, err = du.intervalDecimalToString(ec, decimal)
		if err != nil {
			return "", true, err
		}
	}

	return interval, false, nil
}

func (du *baseDateArithmetical) getIntervalFromInt(ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	if mysql.HasUnsignedFlag(args[1].GetType(ctx).GetFlag()) {
		return strconv.FormatUint(uint64(interval), 10), false, nil
	}

	return strconv.FormatInt(interval, 10), false, nil
}

func (du *baseDateArithmetical) getIntervalFromReal(ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	interval, isNull, err := args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	return strconv.FormatFloat(interval, 'f', args[1].GetType(ctx).GetDecimal(), 64), false, nil
}

func (du *baseDateArithmetical) add(ctx EvalContext, date types.Time, interval, unit string, resultFsp int) (types.Time, bool, error) {
	year, month, day, nano, _, err := types.ParseDurationValue(unit, interval)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}
	return du.addDate(ctx, date, year, month, day, nano, resultFsp)
}

func (du *baseDateArithmetical) addDate(ctx EvalContext, date types.Time, year, month, day, nano int64, resultFsp int) (types.Time, bool, error) {
	goTime, err := date.GoTime(time.UTC)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}

	goTime = goTime.Add(time.Duration(nano))
	goTime, err = types.AddDate(year, month, day, goTime)
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}

	// Adjust fsp as required by outer - always respect type inference.
	date.SetFsp(resultFsp)

	// fix https://github.com/pingcap/tidb/issues/11329
	if goTime.Year() == 0 {
		hour, minute, second := goTime.Clock()
		date.SetCoreTime(types.FromDate(0, 0, 0, hour, minute, second, goTime.Nanosecond()/1000))
		return date, false, nil
	}

	date.SetCoreTime(types.FromGoTime(goTime))
	tc := typeCtx(ctx)
	overflow, err := types.DateTimeIsOverflow(tc, date)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}
	if overflow {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}
	return date, false, nil
}

type funcDurationOp func(d, interval types.Duration) (types.Duration, error)

func (du *baseDateArithmetical) opDuration(ctx EvalContext, op funcDurationOp, d types.Duration, interval string, unit string, resultFsp int) (types.Duration, bool, error) {
	dur, err := types.ExtractDurationValue(unit, interval)
	if err != nil {
		return types.ZeroDuration, true, handleInvalidTimeError(ctx, err)
	}
	retDur, err := op(d, dur)
	if err != nil {
		return types.ZeroDuration, true, err
	}
	// Adjust fsp as required by outer - always respect type inference.
	retDur.Fsp = resultFsp
	return retDur, false, nil
}

func (du *baseDateArithmetical) addDuration(ctx EvalContext, d types.Duration, interval string, unit string, resultFsp int) (types.Duration, bool, error) {
	add := func(d, interval types.Duration) (types.Duration, error) {
		return d.Add(interval)
	}
	return du.opDuration(ctx, add, d, interval, unit, resultFsp)
}

func (du *baseDateArithmetical) subDuration(ctx EvalContext, d types.Duration, interval string, unit string, resultFsp int) (types.Duration, bool, error) {
	sub := func(d, interval types.Duration) (types.Duration, error) {
		return d.Sub(interval)
	}
	return du.opDuration(ctx, sub, d, interval, unit, resultFsp)
}

func (du *baseDateArithmetical) sub(ctx EvalContext, date types.Time, interval string, unit string, resultFsp int) (types.Time, bool, error) {
	year, month, day, nano, _, err := types.ParseDurationValue(unit, interval)
	if err := handleInvalidTimeError(ctx, err); err != nil {
		return types.ZeroTime, true, err
	}
	return du.addDate(ctx, date, -year, -month, -day, -nano, resultFsp)
}

func (du *baseDateArithmetical) vecGetDateFromInt(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	dates := result.Times()
	i64s := buf.Int64s()
	tc := typeCtx(ctx)
	isClockUnit := types.IsClockUnit(unit)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		date, err := types.ParseTimeFromInt64(tc, i64s[i])
		if err != nil {
			err = handleInvalidTimeError(ctx, err)
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}

		// The actual date.Type() might be date or datetime.
		// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
		if isClockUnit {
			date.SetType(mysql.TypeDatetime)
		}
		dates[i] = date
	}
	return nil
}

func (du *baseDateArithmetical) vecGetDateFromReal(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	dates := result.Times()
	f64s := buf.Float64s()
	tc := typeCtx(ctx)
	isClockUnit := types.IsClockUnit(unit)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		date, err := types.ParseTimeFromFloat64(tc, f64s[i])
		if err != nil {
			err = handleInvalidTimeError(ctx, err)
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}

		// The actual date.Type() might be date or datetime.
		// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
		if isClockUnit {
			date.SetType(mysql.TypeDatetime)
		}
		dates[i] = date
	}
	return nil
}

func (du *baseDateArithmetical) vecGetDateFromDecimal(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	dates := result.Times()
	tc := typeCtx(ctx)
	isClockUnit := types.IsClockUnit(unit)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		dec := buf.GetDecimal(i)
		date, err := types.ParseTimeFromDecimal(tc, dec)
		if err != nil {
			err = handleInvalidTimeError(ctx, err)
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}

		// The actual date.Type() might be date or datetime.
		// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
		if isClockUnit {
			date.SetType(mysql.TypeDatetime)
		}
		dates[i] = date
	}
	return nil
}

func (du *baseDateArithmetical) vecGetDateFromString(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	dates := result.Times()
	tc := typeCtx(ctx)
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

		date, err := types.ParseTime(tc, dateStr, dateTp, types.MaxFsp)
		if err != nil {
			err = handleInvalidTimeError(ctx, err)
			if err != nil {
				return err
			}
			result.SetNull(i, true)
		} else if sqlMode(ctx).HasNoZeroDateMode() && (date.Year() == 0 || date.Month() == 0 || date.Day() == 0) {
			err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, dateStr))
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

func (du *baseDateArithmetical) vecGetDateFromDatetime(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeTime(n, false)
	if err := b.args[0].VecEvalTime(ctx, input, result); err != nil {
		return err
	}

	dates := result.Times()
	isClockUnit := types.IsClockUnit(unit)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		// The actual date[i].Type() might be date, datetime or timestamp.
		// Datetime is treated as is.
		// Timestamp is treated as datetime, as MySQL manual says: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-add
		// When the unit contains clock, the date part is treated as datetime even though it might be actually a date.
		if isClockUnit || dates[i].Type() == mysql.TypeTimestamp {
			dates[i].SetType(mysql.TypeDatetime)
		}
	}
	return nil
}

func (du *baseDateArithmetical) vecGetIntervalFromString(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	ec := errCtx(ctx)
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		interval, err := du.intervalReformatString(ec, buf.GetString(i), unit)
		if err != nil {
			return err
		}
		result.AppendString(interval)
	}
	return nil
}

func (du *baseDateArithmetical) vecGetIntervalFromDecimal(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalDecimal(ctx, input, buf); err != nil {
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
				return strings.ReplaceAll(val, ".", ":"), false, nil
			}
		case "YEAR_MONTH":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return strings.ReplaceAll(val, ".", "-"), false, nil
			}
		case "DAY_HOUR":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return strings.ReplaceAll(val, ".", " "), false, nil
			}
		case "DAY_MINUTE":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return "0 " + strings.ReplaceAll(val, ".", ":"), false, nil
			}
		case "DAY_SECOND":
			amendInterval = func(val string, _ *chunk.Row) (string, bool, error) {
				return "0 00:" + strings.ReplaceAll(val, ".", ":"), false, nil
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
				return "00:" + strings.ReplaceAll(val, ".", ":"), false, nil
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
		amendInterval = func(_ string, row *chunk.Row) (string, bool, error) {
			dec, isNull, err := b.args[1].EvalDecimal(ctx, *row)
			if isNull || err != nil {
				return "", true, err
			}

			str, err := du.intervalDecimalToString(errCtx(ctx), dec)
			if err != nil {
				return "", true, err
			}

			return str, false, nil
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

func (du *baseDateArithmetical) vecGetIntervalFromInt(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	i64s := buf.Int64s()
	unsigned := mysql.HasUnsignedFlag(b.args[1].GetType(ctx).GetFlag())
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
		} else if unsigned {
			result.AppendString(strconv.FormatUint(uint64(i64s[i]), 10))
		} else {
			result.AppendString(strconv.FormatInt(i64s[i], 10))
		}
	}
	return nil
}

func (du *baseDateArithmetical) vecGetIntervalFromReal(b *baseBuiltinFunc, ctx EvalContext, input *chunk.Chunk, unit string, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	f64s := buf.Float64s()
	prec := b.args[1].GetType(ctx).GetDecimal()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
		} else {
			result.AppendString(strconv.FormatFloat(f64s[i], 'f', prec, 64))
		}
	}
	return nil
}

type funcTimeOpForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, date types.Time, interval, unit string, resultFsp int) (types.Time, bool, error)

func addTime(da *baseDateArithmetical, ctx EvalContext, date types.Time, interval, unit string, resultFsp int) (types.Time, bool, error) {
	return da.add(ctx, date, interval, unit, resultFsp)
}

func subTime(da *baseDateArithmetical, ctx EvalContext, date types.Time, interval, unit string, resultFsp int) (types.Time, bool, error) {
	return da.sub(ctx, date, interval, unit, resultFsp)
}

type funcDurationOpForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, d types.Duration, interval, unit string, resultFsp int) (types.Duration, bool, error)

func addDuration(da *baseDateArithmetical, ctx EvalContext, d types.Duration, interval, unit string, resultFsp int) (types.Duration, bool, error) {
	return da.addDuration(ctx, d, interval, unit, resultFsp)
}

func subDuration(da *baseDateArithmetical, ctx EvalContext, d types.Duration, interval, unit string, resultFsp int) (types.Duration, bool, error) {
	return da.subDuration(ctx, d, interval, unit, resultFsp)
}

type funcSetPbCodeOp func(b builtinFunc, add, sub tipb.ScalarFuncSig)

func setAdd(b builtinFunc, add, sub tipb.ScalarFuncSig) {
	b.setPbCode(add)
}

func setSub(b builtinFunc, add, sub tipb.ScalarFuncSig) {
	b.setPbCode(sub)
}

type funcGetDateForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error)

func getDateFromString(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	return da.getDateFromString(ctx, args, row, unit)
}

func getDateFromInt(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	return da.getDateFromInt(ctx, args, row, unit)
}

func getDateFromReal(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	return da.getDateFromReal(ctx, args, row, unit)
}

func getDateFromDecimal(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (types.Time, bool, error) {
	return da.getDateFromDecimal(ctx, args, row, unit)
}

type funcVecGetDateForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error

func vecGetDateFromString(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetDateFromString(b, ctx, input, unit, result)
}

func vecGetDateFromInt(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetDateFromInt(b, ctx, input, unit, result)
}

func vecGetDateFromReal(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetDateFromReal(b, ctx, input, unit, result)
}

func vecGetDateFromDecimal(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetDateFromDecimal(b, ctx, input, unit, result)
}

type funcGetIntervalForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error)

func getIntervalFromString(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	return da.getIntervalFromString(ctx, args, row, unit)
}

func getIntervalFromInt(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	return da.getIntervalFromInt(ctx, args, row, unit)
}

func getIntervalFromReal(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	return da.getIntervalFromReal(ctx, args, row, unit)
}

func getIntervalFromDecimal(da *baseDateArithmetical, ctx EvalContext, args []Expression, row chunk.Row, unit string) (string, bool, error) {
	return da.getIntervalFromDecimal(ctx, args, row, unit)
}

type funcVecGetIntervalForDateAddSub func(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error

func vecGetIntervalFromString(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetIntervalFromString(b, ctx, input, unit, result)
}

func vecGetIntervalFromInt(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetIntervalFromInt(b, ctx, input, unit, result)
}

func vecGetIntervalFromReal(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetIntervalFromReal(b, ctx, input, unit, result)
}

func vecGetIntervalFromDecimal(da *baseDateArithmetical, ctx EvalContext, b *baseBuiltinFunc, input *chunk.Chunk, unit string, result *chunk.Column) error {
	return da.vecGetIntervalFromDecimal(b, ctx, input, unit, result)
}

type addSubDateFunctionClass struct {
	baseFunctionClass
	timeOp      funcTimeOpForDateAddSub
	durationOp  funcDurationOpForDateAddSub
	setPbCodeOp funcSetPbCodeOp
}

func (c *addSubDateFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	dateEvalTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	// Some special evaluation type treatment.
	// Note that it could be more elegant if we always evaluate datetime for int, real, decimal and string, by leveraging existing implicit casts.
	// However, MySQL has a weird behavior for date_add(string, ...), whose result depends on the content of the first argument.
	// E.g., date_add('2000-01-02 00:00:00', interval 1 day) evaluates to '2021-01-03 00:00:00' (which is normal),
	// whereas date_add('2000-01-02', interval 1 day) evaluates to '2000-01-03' instead of '2021-01-03 00:00:00'.
	// This requires a customized parsing of the content of the first argument, by recognizing if it is a pure date format or contains HMS part.
	// So implicit casts are not viable here.
	if dateEvalTp == types.ETTimestamp {
		dateEvalTp = types.ETDatetime
	} else if dateEvalTp == types.ETJson {
		dateEvalTp = types.ETString
	}

	intervalEvalTp := args[1].GetType(ctx.GetEvalCtx()).EvalType()
	if intervalEvalTp == types.ETJson {
		intervalEvalTp = types.ETString
	} else if intervalEvalTp != types.ETString && intervalEvalTp != types.ETDecimal && intervalEvalTp != types.ETReal {
		intervalEvalTp = types.ETInt
	}

	unit, _, err := args[2].EvalString(ctx.GetEvalCtx(), chunk.Row{})
	if err != nil {
		return nil, err
	}

	resultTp := mysql.TypeVarString
	resultEvalTp := types.ETString
	if args[0].GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeDate {
		if !types.IsClockUnit(unit) {
			// First arg is date and unit contains no HMS, return date.
			resultTp = mysql.TypeDate
			resultEvalTp = types.ETDatetime
		} else {
			// First arg is date and unit contains HMS, return datetime.
			resultTp = mysql.TypeDatetime
			resultEvalTp = types.ETDatetime
		}
	} else if dateEvalTp == types.ETDuration {
		if types.IsDateUnit(unit) && unit != "DAY_MICROSECOND" {
			// First arg is time and unit contains YMD (except DAY_MICROSECOND), return datetime.
			resultTp = mysql.TypeDatetime
			resultEvalTp = types.ETDatetime
		} else {
			// First arg is time and unit contains no YMD or is DAY_MICROSECOND, return time.
			resultTp = mysql.TypeDuration
			resultEvalTp = types.ETDuration
		}
	} else if dateEvalTp == types.ETDatetime {
		// First arg is datetime or timestamp, return datetime.
		resultTp = mysql.TypeDatetime
		resultEvalTp = types.ETDatetime
	}

	argTps := []types.EvalType{dateEvalTp, intervalEvalTp, types.ETString}
	var bf baseBuiltinFunc
	bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, resultEvalTp, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetType(resultTp)

	var resultFsp int
	if types.IsMicrosecondUnit(unit) {
		resultFsp = types.MaxFsp
	} else {
		intervalFsp := types.MinFsp
		if unit == "SECOND" {
			if intervalEvalTp == types.ETString || intervalEvalTp == types.ETReal {
				intervalFsp = types.MaxFsp
			} else {
				intervalFsp = mathutil.Min(types.MaxFsp, args[1].GetType(ctx.GetEvalCtx()).GetDecimal())
			}
		}
		resultFsp = mathutil.Min(types.MaxFsp, mathutil.Max(args[0].GetType(ctx.GetEvalCtx()).GetDecimal(), intervalFsp))
	}
	switch resultTp {
	case mysql.TypeDate:
		bf.setDecimalAndFlenForDate()
	case mysql.TypeDuration:
		bf.setDecimalAndFlenForTime(resultFsp)
	case mysql.TypeDatetime:
		bf.setDecimalAndFlenForDatetime(resultFsp)
	case mysql.TypeVarString:
		bf.tp.SetFlen(mysql.MaxDatetimeFullWidth)
		bf.tp.SetDecimal(types.MinFsp)
	}

	switch {
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETString:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromString,
			vecGetDate:           vecGetDateFromString,
			getInterval:          getIntervalFromString,
			vecGetInterval:       vecGetIntervalFromString,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateStringString, tipb.ScalarFuncSig_SubDateStringString)
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETInt:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromString,
			vecGetDate:           vecGetDateFromString,
			getInterval:          getIntervalFromInt,
			vecGetInterval:       vecGetIntervalFromInt,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateStringInt, tipb.ScalarFuncSig_SubDateStringInt)
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETReal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromString,
			vecGetDate:           vecGetDateFromString,
			getInterval:          getIntervalFromReal,
			vecGetInterval:       vecGetIntervalFromReal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateStringReal, tipb.ScalarFuncSig_SubDateStringReal)
	case dateEvalTp == types.ETString && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromString,
			vecGetDate:           vecGetDateFromString,
			getInterval:          getIntervalFromDecimal,
			vecGetInterval:       vecGetIntervalFromDecimal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateStringDecimal, tipb.ScalarFuncSig_SubDateStringDecimal)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETString:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromInt,
			vecGetDate:           vecGetDateFromInt,
			getInterval:          getIntervalFromString,
			vecGetInterval:       vecGetIntervalFromString,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateIntString, tipb.ScalarFuncSig_SubDateIntString)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETInt:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromInt,
			vecGetDate:           vecGetDateFromInt,
			getInterval:          getIntervalFromInt,
			vecGetInterval:       vecGetIntervalFromInt,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateIntInt, tipb.ScalarFuncSig_SubDateIntInt)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETReal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromInt,
			vecGetDate:           vecGetDateFromInt,
			getInterval:          getIntervalFromReal,
			vecGetInterval:       vecGetIntervalFromReal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateIntReal, tipb.ScalarFuncSig_SubDateIntReal)
	case dateEvalTp == types.ETInt && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromInt,
			vecGetDate:           vecGetDateFromInt,
			getInterval:          getIntervalFromDecimal,
			vecGetInterval:       vecGetIntervalFromDecimal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateIntDecimal, tipb.ScalarFuncSig_SubDateIntDecimal)
	case dateEvalTp == types.ETReal && intervalEvalTp == types.ETString:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromReal,
			vecGetDate:           vecGetDateFromReal,
			getInterval:          getIntervalFromString,
			vecGetInterval:       vecGetIntervalFromString,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateRealString, tipb.ScalarFuncSig_SubDateRealString)
	case dateEvalTp == types.ETReal && intervalEvalTp == types.ETInt:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromReal,
			vecGetDate:           vecGetDateFromReal,
			getInterval:          getIntervalFromInt,
			vecGetInterval:       vecGetIntervalFromInt,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateRealInt, tipb.ScalarFuncSig_SubDateRealInt)
	case dateEvalTp == types.ETReal && intervalEvalTp == types.ETReal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromReal,
			vecGetDate:           vecGetDateFromReal,
			getInterval:          getIntervalFromReal,
			vecGetInterval:       vecGetIntervalFromReal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateRealReal, tipb.ScalarFuncSig_SubDateRealReal)
	case dateEvalTp == types.ETReal && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromReal,
			vecGetDate:           vecGetDateFromReal,
			getInterval:          getIntervalFromDecimal,
			vecGetInterval:       vecGetIntervalFromDecimal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateRealDecimal, tipb.ScalarFuncSig_SubDateRealDecimal)
	case dateEvalTp == types.ETDecimal && intervalEvalTp == types.ETString:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromDecimal,
			vecGetDate:           vecGetDateFromDecimal,
			getInterval:          getIntervalFromString,
			vecGetInterval:       vecGetIntervalFromString,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDecimalString, tipb.ScalarFuncSig_SubDateDecimalString)
	case dateEvalTp == types.ETDecimal && intervalEvalTp == types.ETInt:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromDecimal,
			vecGetDate:           vecGetDateFromDecimal,
			getInterval:          getIntervalFromInt,
			vecGetInterval:       vecGetIntervalFromInt,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDecimalInt, tipb.ScalarFuncSig_SubDateDecimalInt)
	case dateEvalTp == types.ETDecimal && intervalEvalTp == types.ETReal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromDecimal,
			vecGetDate:           vecGetDateFromDecimal,
			getInterval:          getIntervalFromReal,
			vecGetInterval:       vecGetIntervalFromReal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDecimalReal, tipb.ScalarFuncSig_SubDateDecimalReal)
	case dateEvalTp == types.ETDecimal && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddSubDateAsStringSig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getDate:              getDateFromDecimal,
			vecGetDate:           vecGetDateFromDecimal,
			getInterval:          getIntervalFromDecimal,
			vecGetInterval:       vecGetIntervalFromDecimal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDecimalDecimal, tipb.ScalarFuncSig_SubDateDecimalDecimal)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETString:
		sig = &builtinAddSubDateDatetimeAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromString,
			vecGetInterval:       vecGetIntervalFromString,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDatetimeString, tipb.ScalarFuncSig_SubDateDatetimeString)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETInt:
		sig = &builtinAddSubDateDatetimeAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromInt,
			vecGetInterval:       vecGetIntervalFromInt,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDatetimeInt, tipb.ScalarFuncSig_SubDateDatetimeInt)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETReal:
		sig = &builtinAddSubDateDatetimeAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromReal,
			vecGetInterval:       vecGetIntervalFromReal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDatetimeReal, tipb.ScalarFuncSig_SubDateDatetimeReal)
	case dateEvalTp == types.ETDatetime && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddSubDateDatetimeAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromDecimal,
			vecGetInterval:       vecGetIntervalFromDecimal,
			timeOp:               c.timeOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDatetimeDecimal, tipb.ScalarFuncSig_SubDateDatetimeDecimal)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETString:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromString,
			vecGetInterval:       vecGetIntervalFromString,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationString, tipb.ScalarFuncSig_SubDateDurationString)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETInt:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromInt,
			vecGetInterval:       vecGetIntervalFromInt,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationInt, tipb.ScalarFuncSig_SubDateDurationInt)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETReal:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromReal,
			vecGetInterval:       vecGetIntervalFromReal,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationReal, tipb.ScalarFuncSig_SubDateDurationReal)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETDecimal:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromDecimal,
			vecGetInterval:       vecGetIntervalFromDecimal,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationDecimal, tipb.ScalarFuncSig_SubDateDurationDecimal)
	}
	return sig, nil
}

type builtinAddSubDateAsStringSig struct {
	baseBuiltinFunc
	baseDateArithmetical
	getDate        funcGetDateForDateAddSub
	vecGetDate     funcVecGetDateForDateAddSub
	getInterval    funcGetIntervalForDateAddSub
	vecGetInterval funcVecGetIntervalForDateAddSub
	timeOp         funcTimeOpForDateAddSub
}

func (b *builtinAddSubDateAsStringSig) Clone() builtinFunc {
	newSig := &builtinAddSubDateAsStringSig{
		baseDateArithmetical: b.baseDateArithmetical,
		getDate:              b.getDate,
		vecGetDate:           b.vecGetDate,
		getInterval:          b.getInterval,
		vecGetInterval:       b.vecGetInterval,
		timeOp:               b.timeOp,
	}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinAddSubDateAsStringSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	unit, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime.String(), true, err
	}

	date, isNull, err := b.getDate(&b.baseDateArithmetical, ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime.String(), true, err
	}
	if date.InvalidZero() {
		return types.ZeroTime.String(), true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String()))
	}

	interval, isNull, err := b.getInterval(&b.baseDateArithmetical, ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime.String(), true, err
	}

	result, isNull, err := b.timeOp(&b.baseDateArithmetical, ctx, date, interval, unit, b.tp.GetDecimal())
	if result.Microsecond() == 0 {
		result.SetFsp(types.MinFsp)
	} else {
		result.SetFsp(types.MaxFsp)
	}

	return result.String(), isNull, err
}

type builtinAddSubDateDatetimeAnySig struct {
	baseBuiltinFunc
	baseDateArithmetical
	getInterval    funcGetIntervalForDateAddSub
	vecGetInterval funcVecGetIntervalForDateAddSub
	timeOp         funcTimeOpForDateAddSub
}

func (b *builtinAddSubDateDatetimeAnySig) Clone() builtinFunc {
	newSig := &builtinAddSubDateDatetimeAnySig{
		baseDateArithmetical: b.baseDateArithmetical,
		getInterval:          b.getInterval,
		vecGetInterval:       b.vecGetInterval,
		timeOp:               b.timeOp,
	}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinAddSubDateDatetimeAnySig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	date, isNull, err := b.getDateFromDatetime(ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getInterval(&b.baseDateArithmetical, ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	result, isNull, err := b.timeOp(&b.baseDateArithmetical, ctx, date, interval, unit, b.tp.GetDecimal())
	return result, isNull || err != nil, err
}

type builtinAddSubDateDurationAnySig struct {
	baseBuiltinFunc
	baseDateArithmetical
	getInterval    funcGetIntervalForDateAddSub
	vecGetInterval funcVecGetIntervalForDateAddSub
	timeOp         funcTimeOpForDateAddSub
	durationOp     funcDurationOpForDateAddSub
}

func (b *builtinAddSubDateDurationAnySig) Clone() builtinFunc {
	newSig := &builtinAddSubDateDurationAnySig{
		baseDateArithmetical: b.baseDateArithmetical,
		getInterval:          b.getInterval,
		vecGetInterval:       b.vecGetInterval,
		timeOp:               b.timeOp,
		durationOp:           b.durationOp,
	}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinAddSubDateDurationAnySig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	unit, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	d, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	interval, isNull, err := b.getInterval(&b.baseDateArithmetical, ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}

	tc := typeCtx(ctx)
	t, err := d.ConvertToTime(tc, mysql.TypeDatetime)
	if err != nil {
		return types.ZeroTime, true, err
	}
	result, isNull, err := b.timeOp(&b.baseDateArithmetical, ctx, t, interval, unit, b.tp.GetDecimal())
	return result, isNull || err != nil, err
}

func (b *builtinAddSubDateDurationAnySig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	unit, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	dur, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	interval, isNull, err := b.getInterval(&b.baseDateArithmetical, ctx, b.args, row, unit)
	if isNull || err != nil {
		return types.ZeroDuration, true, err
	}

	result, isNull, err := b.durationOp(&b.baseDateArithmetical, ctx, dur, interval, unit, b.tp.GetDecimal())
	return result, isNull || err != nil, err
}

type timestampDiffFunctionClass struct {
	baseFunctionClass
}

func (c *timestampDiffFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
func (b *builtinTimestampDiffSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	unit, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	lhs, isNull, err := b.args[1].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, isNull, handleInvalidTimeError(ctx, err)
	}
	rhs, isNull, err := b.args[2].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, isNull, handleInvalidTimeError(ctx, err)
	}
	if invalidLHS, invalidRHS := lhs.InvalidZero(), rhs.InvalidZero(); invalidLHS || invalidRHS {
		if invalidLHS {
			err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, lhs.String()))
		}
		if invalidRHS {
			err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, rhs.String()))
		}
		return 0, true, err
	}
	return types.TimestampDiff(unit, lhs, rhs), false, nil
}

type unixTimestampFunctionClass struct {
	baseFunctionClass
}

func (c *unixTimestampFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
		argType := args[0].GetType(ctx.GetEvalCtx())
		argEvaltp := argType.EvalType()
		if argEvaltp == types.ETString {
			// Treat types.ETString as unspecified decimal.
			retDecimal = types.UnspecifiedLength
			if cnst, ok := args[0].(*Constant); ok {
				tmpStr, _, err := cnst.EvalString(ctx.GetEvalCtx(), chunk.Row{})
				if err != nil {
					return nil, err
				}
				retDecimal = 0
				if dotIdx := strings.LastIndex(tmpStr, "."); dotIdx >= 0 {
					retDecimal = len(tmpStr) - dotIdx - 1
				}
			}
		} else {
			retDecimal = argType.GetDecimal()
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
	bf.tp.SetFlenUnderLimit(retFLen)
	bf.tp.SetDecimalUnderLimit(retDecimal)

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
// MySQL's Unix timestamp ranges from '1970-01-01 00:00:01.000000' UTC to '3001-01-18 23:59:59.999999' UTC. Values out of range should be rewritten to 0.
// https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_unix-timestamp
func goTimeToMysqlUnixTimestamp(t time.Time, decimal int) (*types.MyDecimal, error) {
	microSeconds := t.UnixMicro()
	// Prior to MySQL 8.0.28 (or any 32-bit platform), the valid range of argument values is the same as for the TIMESTAMP data type:
	// '1970-01-01 00:00:01.000000' UTC to '2038-01-19 03:14:07.999999' UTC.
	// After 8.0.28, the range has been extended to '1970-01-01 00:00:01.000000' UTC to '3001-01-18 23:59:59.999999' UTC
	// The magic value of '3001-01-18 23:59:59.999999' comes from the maximum supported timestamp on windows. Though TiDB
	// doesn't support windows, this value is used here to keep the compatibility with MySQL
	if microSeconds < 1e6 || microSeconds > 32536771199999999 {
		return new(types.MyDecimal), nil
	}
	dec := new(types.MyDecimal)
	// Here we don't use float to prevent precision lose.
	dec.FromUint(uint64(microSeconds))
	err := dec.Shift(-6)
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
func (b *builtinUnixTimestampCurrentSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	nowTs, err := getStmtTimestamp(ctx)
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
func (b *builtinUnixTimestampIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if err != nil && terror.ErrorEqual(types.ErrWrongValue.GenWithStackByArgs(types.TimeStr, val), err) {
		// Return 0 for invalid date time.
		return 0, false, nil
	}
	if isNull {
		return 0, true, nil
	}

	tz := location(ctx)
	t, err := val.AdjustedGoTime(tz)
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
func (b *builtinUnixTimestampDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		// Return 0 for invalid date time.
		return new(types.MyDecimal), isNull, nil
	}
	t, err := val.GoTime(getTimeZone(ctx))
	if err != nil {
		return new(types.MyDecimal), false, nil
	}
	result, err := goTimeToMysqlUnixTimestamp(t, b.tp.GetDecimal())
	return result, err != nil, err
}

type timestampFunctionClass struct {
	baseFunctionClass
}

func (c *timestampFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	evalTps, argLen := []types.EvalType{types.ETString}, len(args)
	if argLen == 2 {
		evalTps = append(evalTps, types.ETString)
	}
	fsp, err := getExpressionFsp(ctx, args[0])
	if err != nil {
		return nil, err
	}
	if argLen == 2 {
		fsp2, err := getExpressionFsp(ctx, args[1])
		if err != nil {
			return nil, err
		}
		if fsp2 > fsp {
			fsp = fsp2
		}
	}
	isFloat := false
	switch args[0].GetType(ctx.GetEvalCtx()).GetType() {
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal, mysql.TypeLonglong:
		isFloat = true
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, evalTps...)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDatetime(fsp)
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
func (b *builtinTimestamp1ArgSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	s, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	var tm types.Time
	tc := typeCtx(ctx)
	if b.isFloat {
		tm, err = types.ParseTimeFromFloatString(tc, s, mysql.TypeDatetime, types.GetFsp(s))
	} else {
		tm, err = types.ParseTime(tc, s, mysql.TypeDatetime, types.GetFsp(s))
	}
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
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
func (b *builtinTimestamp2ArgsSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	var tm types.Time
	tc := typeCtx(ctx)
	if b.isFloat {
		tm, err = types.ParseTimeFromFloatString(tc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
	} else {
		tm, err = types.ParseTime(tc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
	}
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	if tm.Year() == 0 {
		// MySQL won't evaluate add for date with zero year.
		// See https://github.com/mysql/mysql-server/blob/5.7/sql/item_timefunc.cc#L2805
		return types.ZeroTime, true, nil
	}
	arg1, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, isNull, err
	}
	if !isDuration(arg1) {
		return types.ZeroTime, true, nil
	}
	duration, _, err := types.ParseDuration(tc, arg1, types.GetFsp(arg1))
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	tmp, err := tm.Add(tc, duration)
	if err != nil {
		return types.ZeroTime, true, err
	}
	return tmp, false, nil
}

type timestampLiteralFunctionClass struct {
	baseFunctionClass
}

func (c *timestampLiteralFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	con, ok := args[0].(*Constant)
	if !ok {
		panic("Unexpected parameter for timestamp literal")
	}
	dt, err := con.Eval(ctx.GetEvalCtx(), chunk.Row{})
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
	tm, err := types.ParseTime(ctx.GetEvalCtx().TypeCtx(), str, mysql.TypeDatetime, types.GetFsp(str))
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, []Expression{}, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDatetime(tm.Fsp())
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
func (b *builtinTimestampLiteralSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	return b.tm, false, nil
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
func getBf4TimeAddSub(ctx BuildContext, funcName string, args []Expression) (tp1, tp2 *types.FieldType, bf baseBuiltinFunc, err error) {
	tp1, tp2 = args[0].GetType(ctx.GetEvalCtx()), args[1].GetType(ctx.GetEvalCtx())
	var argTp1, argTp2, retTp types.EvalType
	switch tp1.GetType() {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		argTp1, retTp = types.ETDatetime, types.ETDatetime
	case mysql.TypeDuration:
		argTp1, retTp = types.ETDuration, types.ETDuration
	case mysql.TypeDate:
		argTp1, retTp = types.ETDuration, types.ETString
	default:
		argTp1, retTp = types.ETString, types.ETString
	}
	switch tp2.GetType() {
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
	switch retTp {
	case types.ETDatetime:
		bf.setDecimalAndFlenForDatetime(mathutil.Min(mathutil.Max(arg0Dec, arg1Dec), types.MaxFsp))
	case types.ETDuration:
		bf.setDecimalAndFlenForTime(mathutil.Min(mathutil.Max(arg0Dec, arg1Dec), types.MaxFsp))
	case types.ETString:
		bf.tp.SetType(mysql.TypeString)
		bf.tp.SetFlen(mysql.MaxDatetimeWidthWithFsp)
		bf.tp.SetDecimal(types.UnspecifiedLength)
	}
	return
}

func getTimeZone(ctx EvalContext) *time.Location {
	ret := location(ctx)
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
func strDatetimeAddDuration(tc types.Context, d string, arg1 types.Duration) (result string, isNull bool, err error) {
	arg0, err := types.ParseTime(tc, d, mysql.TypeDatetime, types.MaxFsp)
	if err != nil {
		// Return a warning regardless of the sql_mode, this is compatible with MySQL.
		tc.AppendWarning(err)
		return "", true, nil
	}
	ret, err := arg0.Add(tc, arg1)
	if err != nil {
		return "", false, err
	}
	fsp := types.MaxFsp
	if ret.Microsecond() == 0 {
		fsp = types.MinFsp
	}
	ret.SetFsp(fsp)
	return ret.String(), false, nil
}

// strDurationAddDuration adds duration to duration string, returns a string value.
func strDurationAddDuration(tc types.Context, d string, arg1 types.Duration) (string, error) {
	arg0, _, err := types.ParseDuration(tc, d, types.MaxFsp)
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
func strDatetimeSubDuration(tc types.Context, d string, arg1 types.Duration) (result string, isNull bool, err error) {
	arg0, err := types.ParseTime(tc, d, mysql.TypeDatetime, types.MaxFsp)
	if err != nil {
		// Return a warning regardless of the sql_mode, this is compatible with MySQL.
		tc.AppendWarning(err)
		return "", true, nil
	}
	resultTime, err := arg0.Add(tc, arg1.Neg())
	if err != nil {
		return "", false, err
	}
	fsp := types.MaxFsp
	if resultTime.Microsecond() == 0 {
		fsp = types.MinFsp
	}
	resultTime.SetFsp(fsp)
	return resultTime.String(), false, nil
}

// strDurationSubDuration subtracts duration from duration string, returns a string value.
func strDurationSubDuration(tc types.Context, d string, arg1 types.Duration) (string, error) {
	arg0, _, err := types.ParseDuration(tc, d, types.MaxFsp)
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

func (c *addTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	tp1, tp2, bf, err := getBf4TimeAddSub(ctx, c.funcName, args)
	if err != nil {
		return nil, err
	}
	switch tp1.GetType() {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		switch tp2.GetType() {
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
		charset, collate := ctx.GetCharsetInfo()
		bf.tp.SetCharset(charset)
		bf.tp.SetCollate(collate)
		switch tp2.GetType() {
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
		switch tp2.GetType() {
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
		switch tp2.GetType() {
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
func (b *builtinAddTimeDateTimeNullSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
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
func (b *builtinAddDatetimeAndDurationSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	result, err := arg0.Add(typeCtx(ctx), arg1)
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
func (b *builtinAddDatetimeAndStringSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	s, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	if !isDuration(s) {
		return types.ZeroDatetime, true, nil
	}
	tc := typeCtx(ctx)
	arg1, _, err := types.ParseDuration(tc, s, types.GetFsp(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			tc.AppendWarning(err)
			return types.ZeroDatetime, true, nil
		}
		return types.ZeroDatetime, true, err
	}
	result, err := arg0.Add(tc, arg1)
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
func (b *builtinAddTimeDurationNullSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
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
func (b *builtinAddDurationAndDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(ctx, row)
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
func (b *builtinAddDurationAndStringSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	s, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	if !isDuration(s) {
		return types.ZeroDuration, true, nil
	}
	tc := typeCtx(ctx)
	arg1, _, err := types.ParseDuration(tc, s, types.GetFsp(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			tc.AppendWarning(err)
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
func (b *builtinAddTimeStringNullSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
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
func (b *builtinAddStringAndDurationSig) evalString(ctx EvalContext, row chunk.Row) (result string, isNull bool, err error) {
	var (
		arg0 string
		arg1 types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1, isNull, err = b.args[1].EvalDuration(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	tc := typeCtx(ctx)
	if isDuration(arg0) {
		result, err = strDurationAddDuration(tc, arg0, arg1)
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				tc.AppendWarning(err)
				return "", true, nil
			}
			return "", true, err
		}
		return result, false, nil
	}
	result, isNull, err = strDatetimeAddDuration(tc, arg0, arg1)
	return result, isNull, err
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
func (b *builtinAddStringAndStringSig) evalString(ctx EvalContext, row chunk.Row) (result string, isNull bool, err error) {
	var (
		arg0, arg1Str string
		arg1          types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1Type := b.args[1].GetType(ctx)
	if mysql.HasBinaryFlag(arg1Type.GetFlag()) {
		return "", true, nil
	}
	arg1Str, isNull, err = b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	tc := typeCtx(ctx)
	arg1, _, err = types.ParseDuration(tc, arg1Str, getFsp4TimeAddSub(arg1Str))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			tc.AppendWarning(err)
			return "", true, nil
		}
		return "", true, err
	}

	check := arg1Str
	_, check, err = parser.Number(parser.Space0(check))
	if err == nil {
		check, err = parser.Char(check, '-')
		if strings.Compare(check, "") != 0 && err == nil {
			return "", true, nil
		}
	}

	if isDuration(arg0) {
		result, err = strDurationAddDuration(tc, arg0, arg1)
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				tc.AppendWarning(err)
				return "", true, nil
			}
			return "", true, err
		}
		return result, false, nil
	}
	result, isNull, err = strDatetimeAddDuration(tc, arg0, arg1)
	return result, isNull, err
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
func (b *builtinAddDateAndDurationSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(ctx, row)
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
func (b *builtinAddDateAndStringSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	s, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	if !isDuration(s) {
		return "", true, nil
	}
	tc := typeCtx(ctx)
	arg1, _, err := types.ParseDuration(tc, s, getFsp4TimeAddSub(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			tc.AppendWarning(err)
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

func (c *convertTzFunctionClass) getDecimal(ctx BuildContext, arg Expression) int {
	decimal := types.MaxFsp
	if dt, isConstant := arg.(*Constant); isConstant {
		switch arg.GetType(ctx.GetEvalCtx()).EvalType() {
		case types.ETInt:
			decimal = 0
		case types.ETReal, types.ETDecimal:
			decimal = arg.GetType(ctx.GetEvalCtx()).GetDecimal()
		case types.ETString:
			str, isNull, err := dt.EvalString(ctx.GetEvalCtx(), chunk.Row{})
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

func (c *convertTzFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	// tzRegex holds the regex to check whether a string is a time zone.
	tzRegex, err := regexp.Compile(`(^[-+](0?[0-9]|1[0-3]):[0-5]?\d$)|(^\+14:00?$)`)
	if err != nil {
		return nil, err
	}

	decimal := c.getDecimal(ctx, args[0])
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETDatetime, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDatetime(decimal)
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
func (b *builtinConvertTzSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	dt, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, nil
	}
	if dt.InvalidZero() {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, dt.String()))
	}
	fromTzStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, nil
	}

	toTzStr, isNull, err := b.args[2].EvalString(ctx, row)
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

	var fromTz, toTz *time.Location
	var err error

	if fromTzMatched {
		fromTz = time.FixedZone(fromTzStr, timeZone2int(fromTzStr))
	} else {
		if strings.EqualFold(fromTzStr, "SYSTEM") {
			fromTzStr = "Local"
		}
		fromTz, err = time.LoadLocation(fromTzStr)
		if err != nil {
			return types.ZeroTime, true, nil
		}
	}

	t, err := dt.AdjustedGoTime(fromTz)
	if err != nil {
		return types.ZeroTime, true, nil
	}
	t = t.In(time.UTC)

	if toTzMatched {
		toTz = time.FixedZone(toTzStr, timeZone2int(toTzStr))
	} else {
		if strings.EqualFold(toTzStr, "SYSTEM") {
			toTzStr = "Local"
		}
		toTz, err = time.LoadLocation(toTzStr)
		if err != nil {
			return types.ZeroTime, true, nil
		}
	}

	return types.NewTime(types.FromGoTime(t.In(toTz)), mysql.TypeDatetime, b.tp.GetDecimal()), false, nil
}

type makeDateFunctionClass struct {
	baseFunctionClass
}

func (c *makeDateFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDate()
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
func (b *builtinMakeDateSig) evalTime(ctx EvalContext, row chunk.Row) (d types.Time, isNull bool, err error) {
	args := b.getArgs()
	var year, dayOfYear int64
	year, isNull, err = args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return d, true, err
	}
	dayOfYear, isNull, err = args[1].EvalInt(ctx, row)
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
		return d, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, startTime.String()))
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

func (c *makeTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	tp, decimal := args[2].GetType(ctx.GetEvalCtx()).EvalType(), 0
	switch tp {
	case types.ETInt:
	case types.ETReal, types.ETDecimal:
		decimal = args[2].GetType(ctx.GetEvalCtx()).GetDecimal()
		if decimal > 6 || decimal == types.UnspecifiedLength {
			decimal = 6
		}
	default:
		decimal = 6
	}
	// MySQL will cast the first and second arguments to INT, and the third argument to DECIMAL.
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, types.ETInt, types.ETInt, types.ETReal)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForTime(decimal)
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

func (b *builtinMakeTimeSig) makeTime(ctx types.Context, hour int64, minute int64, second float64, hourUnsignedFlag bool) (types.Duration, error) {
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
	if (hour == -838 || hour == 838) && minute == 59 && second > 59 {
		overflow = true
	}
	if overflow {
		minute = 59
		second = 59
	}
	fsp := b.tp.GetDecimal()
	d, _, err := types.ParseDuration(ctx, fmt.Sprintf("%02d:%02d:%v", hour, minute, second), fsp)
	return d, err
}

// evalDuration evals a builtinMakeTimeIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_maketime
func (b *builtinMakeTimeSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	dur := types.ZeroDuration
	dur.Fsp = types.MaxFsp
	hour, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return dur, isNull, err
	}
	minute, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return dur, isNull, err
	}
	if minute < 0 || minute >= 60 {
		return dur, true, nil
	}
	second, isNull, err := b.args[2].EvalReal(ctx, row)
	if isNull || err != nil {
		return dur, isNull, err
	}
	if second < 0 || second >= 60 {
		return dur, true, nil
	}
	dur, err = b.makeTime(typeCtx(ctx), hour, minute, second, mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()))
	if err != nil {
		return dur, true, err
	}
	return dur, false, nil
}

type periodAddFunctionClass struct {
	baseFunctionClass
}

func (c *periodAddFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(6)
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
func (b *builtinPeriodAddSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	p, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	n, isNull, err := b.args[1].EvalInt(ctx, row)
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

func (c *periodDiffFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(6)
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
func (b *builtinPeriodDiffSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	p1, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	p2, isNull, err := b.args[1].EvalInt(ctx, row)
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

func (c *quarterFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)

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
func (b *builtinQuarterSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	date, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}

	return int64((date.Month() + 2) / 3), false, nil
}

type secToTimeFunctionClass struct {
	baseFunctionClass
}

func (c *secToTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var retFsp int
	argType := args[0].GetType(ctx.GetEvalCtx())
	argEvalTp := argType.EvalType()
	if argEvalTp == types.ETString {
		retFsp = types.UnspecifiedLength
	} else {
		retFsp = argType.GetDecimal()
	}
	if retFsp > types.MaxFsp || retFsp == types.UnspecifiedFsp {
		retFsp = types.MaxFsp
	} else if retFsp < types.MinFsp {
		retFsp = types.MinFsp
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDuration, types.ETReal)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForTime(retFsp)
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
func (b *builtinSecToTimeSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	secondsFloat, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	var (
		hour          uint64
		minute        uint64
		second        uint64
		demical       float64
		secondDemical float64
		negative      string
	)

	if secondsFloat < 0 {
		negative = "-"
		secondsFloat = math.Abs(secondsFloat)
	}
	seconds := uint64(secondsFloat)
	demical = secondsFloat - float64(seconds)

	hour = seconds / 3600
	if hour > 838 {
		hour = 838
		minute = 59
		second = 59
		demical = 0
		tc := typeCtx(ctx)
		err = tc.HandleTruncate(errTruncatedWrongValue.GenWithStackByArgs("time", strconv.FormatFloat(secondsFloat, 'f', -1, 64)))
		if err != nil {
			return types.Duration{}, err != nil, err
		}
	} else {
		minute = seconds % 3600 / 60
		second = seconds % 60
	}
	secondDemical = float64(second) + demical

	var dur types.Duration
	dur, _, err = types.ParseDuration(typeCtx(ctx), fmt.Sprintf("%s%02d:%02d:%s", negative, hour, minute, strconv.FormatFloat(secondDemical, 'f', -1, 64)), b.tp.GetDecimal())
	if err != nil {
		return types.Duration{}, err != nil, err
	}
	return dur, false, nil
}

type subTimeFunctionClass struct {
	baseFunctionClass
}

func (c *subTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	tp1, tp2, bf, err := getBf4TimeAddSub(ctx, c.funcName, args)
	if err != nil {
		return nil, err
	}
	switch tp1.GetType() {
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		switch tp2.GetType() {
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
		charset, collate := ctx.GetCharsetInfo()
		bf.tp.SetCharset(charset)
		bf.tp.SetCollate(collate)
		switch tp2.GetType() {
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
		switch tp2.GetType() {
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
		switch tp2.GetType() {
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
func (b *builtinSubDatetimeAndDurationSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	tc := typeCtx(ctx)
	result, err := arg0.Add(tc, arg1.Neg())
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
func (b *builtinSubDatetimeAndStringSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	arg0, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	s, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroDatetime, isNull, err
	}
	if !isDuration(s) {
		return types.ZeroDatetime, true, nil
	}
	tc := typeCtx(ctx)
	arg1, _, err := types.ParseDuration(tc, s, types.GetFsp(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			tc.AppendWarning(err)
			return types.ZeroDatetime, true, nil
		}
		return types.ZeroDatetime, true, err
	}
	result, err := arg0.Add(tc, arg1.Neg())
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
func (b *builtinSubTimeDateTimeNullSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
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
func (b *builtinSubStringAndDurationSig) evalString(ctx EvalContext, row chunk.Row) (result string, isNull bool, err error) {
	var (
		arg0 string
		arg1 types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1, isNull, err = b.args[1].EvalDuration(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	tc := typeCtx(ctx)
	if isDuration(arg0) {
		result, err = strDurationSubDuration(tc, arg0, arg1)
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				tc.AppendWarning(err)
				return "", true, nil
			}
			return "", true, err
		}
		return result, false, nil
	}
	result, isNull, err = strDatetimeSubDuration(tc, arg0, arg1)
	return result, isNull, err
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
func (b *builtinSubStringAndStringSig) evalString(ctx EvalContext, row chunk.Row) (result string, isNull bool, err error) {
	var (
		s, arg0 string
		arg1    types.Duration
	)
	arg0, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1Type := b.args[1].GetType(ctx)
	if mysql.HasBinaryFlag(arg1Type.GetFlag()) {
		return "", true, nil
	}
	s, isNull, err = b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	tc := typeCtx(ctx)
	arg1, _, err = types.ParseDuration(tc, s, getFsp4TimeAddSub(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			tc.AppendWarning(err)
			return "", true, nil
		}
		return "", true, err
	}
	if isDuration(arg0) {
		result, err = strDurationSubDuration(tc, arg0, arg1)
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				tc.AppendWarning(err)
				return "", true, nil
			}
			return "", true, err
		}
		return result, false, nil
	}
	result, isNull, err = strDatetimeSubDuration(tc, arg0, arg1)
	return result, isNull, err
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
func (b *builtinSubTimeStringNullSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
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
func (b *builtinSubDurationAndDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(ctx, row)
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
func (b *builtinSubDurationAndStringSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	s, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroDuration, isNull, err
	}
	if !isDuration(s) {
		return types.ZeroDuration, true, nil
	}
	tc := typeCtx(ctx)
	arg1, _, err := types.ParseDuration(tc, s, types.GetFsp(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			tc.AppendWarning(err)
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
func (b *builtinSubTimeDurationNullSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
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
func (b *builtinSubDateAndDurationSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg1, isNull, err := b.args[1].EvalDuration(ctx, row)
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
func (b *builtinSubDateAndStringSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	arg0, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	s, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	if !isDuration(s) {
		return "", true, nil
	}
	tc := typeCtx(ctx)
	arg1, _, err := types.ParseDuration(tc, s, getFsp4TimeAddSub(s))
	if err != nil {
		if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
			tc.AppendWarning(err)
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

func (c *timeFormatFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETDuration, types.ETString)
	if err != nil {
		return nil, err
	}
	// worst case: formatMask=%r%r%r...%r, each %r takes 11 characters
	bf.tp.SetFlen((args[1].GetType(ctx.GetEvalCtx()).GetFlen() + 1) / 2 * 11)
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
func (b *builtinTimeFormatSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	dur, isNull, err := b.args[0].EvalDuration(ctx, row)
	// if err != nil, then dur is ZeroDuration, outputs 00:00:00 in this case which follows the behavior of mysql.
	if err != nil {
		logutil.BgLogger().Warn("time_format.args[0].EvalDuration failed", zap.Error(err))
	}
	if isNull {
		return "", isNull, err
	}
	formatMask, isNull, err := b.args[1].EvalString(ctx, row)
	if err != nil || isNull {
		return "", isNull, err
	}
	res, err := b.formatTime(dur, formatMask)
	return res, isNull, err
}

// formatTime see https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-format
func (b *builtinTimeFormatSig) formatTime(t types.Duration, formatMask string) (res string, err error) {
	return t.DurationFormat(formatMask)
}

type timeToSecFunctionClass struct {
	baseFunctionClass
}

func (c *timeToSecFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDuration)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(10)
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
func (b *builtinTimeToSecSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	duration, isNull, err := b.args[0].EvalDuration(ctx, row)
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

func (c *timestampAddFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETReal, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	flen := mysql.MaxDatetimeWidthNoFsp
	con, ok := args[0].(*Constant)
	if !ok {
		return nil, errors.New("should not happened")
	}
	unit, null, err := con.EvalString(ctx.GetEvalCtx(), chunk.Row{})
	if null || err != nil {
		return nil, errors.New("should not happened")
	}
	if unit == ast.TimeUnitMicrosecond.String() {
		flen = mysql.MaxDatetimeWidthWithFsp
	}

	bf.tp.SetFlen(flen)
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

var (
	minDatetimeInGoTime, _ = types.MinDatetime.GoTime(time.Local)
	minDatetimeNanos       = float64(minDatetimeInGoTime.Unix())*1e9 + float64(minDatetimeInGoTime.Nanosecond())
	maxDatetimeInGoTime, _ = types.MaxDatetime.GoTime(time.Local)
	maxDatetimeNanos       = float64(maxDatetimeInGoTime.Unix())*1e9 + float64(maxDatetimeInGoTime.Nanosecond())
	minDatetimeMonths      = float64(types.MinDatetime.Year()*12 + types.MinDatetime.Month() - 1) // 0001-01-01 00:00:00
	maxDatetimeMonths      = float64(types.MaxDatetime.Year()*12 + types.MaxDatetime.Month() - 1) // 9999-12-31 00:00:00
)

func validAddTime(nano1 float64, nano2 float64) bool {
	return nano1+nano2 >= minDatetimeNanos && nano1+nano2 <= maxDatetimeNanos
}

func validAddMonth(month1 float64, year, month int) bool {
	tmp := month1 + float64(year)*12 + float64(month-1)
	return tmp >= minDatetimeMonths && tmp <= maxDatetimeMonths
}

func addUnitToTime(unit string, t time.Time, v float64) (time.Time, bool, error) {
	s := math.Trunc(v * 1000000)
	// round to the nearest int
	v = math.Round(v)
	var tb time.Time
	nano := float64(t.Unix())*1e9 + float64(t.Nanosecond())
	switch unit {
	case "MICROSECOND":
		if !validAddTime(v*float64(time.Microsecond), nano) {
			return tb, true, nil
		}
		tb = t.Add(time.Duration(v) * time.Microsecond)
	case "SECOND":
		if !validAddTime(s*float64(time.Microsecond), nano) {
			return tb, true, nil
		}
		tb = t.Add(time.Duration(s) * time.Microsecond)
	case "MINUTE":
		if !validAddTime(v*float64(time.Minute), nano) {
			return tb, true, nil
		}
		tb = t.Add(time.Duration(v) * time.Minute)
	case "HOUR":
		if !validAddTime(v*float64(time.Hour), nano) {
			return tb, true, nil
		}
		tb = t.Add(time.Duration(v) * time.Hour)
	case "DAY":
		if !validAddTime(v*24*float64(time.Hour), nano) {
			return tb, true, nil
		}
		tb = t.AddDate(0, 0, int(v))
	case "WEEK":
		if !validAddTime(v*24*7*float64(time.Hour), nano) {
			return tb, true, nil
		}
		tb = t.AddDate(0, 0, 7*int(v))
	case "MONTH":
		if !validAddMonth(v, t.Year(), int(t.Month())) {
			return tb, true, nil
		}
		tb = t.AddDate(0, int(v), 0)

		// For corner case: timestampadd(month,1,date '2024-01-31') = "2024-02-29", timestampadd(month,1,date '2024-01-30') = "2024-02-29"
		// `tb.Month()` refers to the actual result, `t.Month()+v` refers to the expect result.
		// Actual result may be greater than expect result, we need to judge and modify it.
		for int(tb.Month())%12 != (int(t.Month())+int(v))%12 {
			tb = tb.AddDate(0, 0, -1)
		}
	case "QUARTER":
		if !validAddMonth(v*3, t.Year(), int(t.Month())) {
			return tb, true, nil
		}
		tb = t.AddDate(0, 3*int(v), 0)
	case "YEAR":
		if !validAddMonth(v*12, t.Year(), int(t.Month())) {
			return tb, true, nil
		}
		tb = t.AddDate(int(v), 0, 0)
	default:
		return tb, false, types.ErrWrongValue.GenWithStackByArgs(types.TimeStr, unit)
	}
	return tb, false, nil
}

// evalString evals a builtinTimestampAddSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampadd
func (b *builtinTimestampAddSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	unit, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	v, isNull, err := b.args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	arg, isNull, err := b.args[2].EvalTime(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	tm1, err := arg.GoTime(time.Local)
	if err != nil {
		tc := typeCtx(ctx)
		tc.AppendWarning(err)
		return "", true, nil
	}
	tb, overflow, err := addUnitToTime(unit, tm1, v)
	if err != nil {
		return "", true, err
	}
	if overflow {
		return "", true, handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime"))
	}
	fsp := types.DefaultFsp
	// use MaxFsp when microsecond is not zero
	if tb.Nanosecond()/1000 != 0 {
		fsp = types.MaxFsp
	}
	r := types.NewTime(types.FromGoTime(tb), b.resolveType(arg.Type(), unit), fsp)
	if err = r.Check(typeCtx(ctx)); err != nil {
		return "", true, handleInvalidTimeError(ctx, err)
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

func (c *toDaysFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
func (b *builtinToDaysSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(ctx, row)

	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}
	if arg.InvalidZero() {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	ret := types.TimestampDiff("DAY", types.ZeroDate, arg)
	if ret == 0 {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	return ret, false, nil
}

type toSecondsFunctionClass struct {
	baseFunctionClass
}

func (c *toSecondsFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
func (b *builtinToSecondsSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return 0, true, handleInvalidTimeError(ctx, err)
	}
	if arg.InvalidZero() {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	ret := types.TimestampDiff("SECOND", types.ZeroDate, arg)
	if ret == 0 {
		return 0, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	return ret, false, nil
}

type utcTimeFunctionClass struct {
	baseFunctionClass
}

func (c *utcTimeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
	fsp, err := getFspByIntArg(ctx, args)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForTime(fsp)
	// 1. no sign.
	// 2. hour is in the 2-digit range.
	bf.tp.SetFlen(bf.tp.GetFlen() - 2)

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
func (b *builtinUTCTimeWithoutArgSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	v, _, err := types.ParseDuration(typeCtx(ctx), nowTs.UTC().Format(types.TimeFormat), 0)
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
func (b *builtinUTCTimeWithArgSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	fsp, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return types.Duration{}, isNull, err
	}
	if fsp > int64(types.MaxFsp) {
		return types.Duration{}, true, errors.Errorf("Too-big precision %v specified for 'utc_time'. Maximum is %v", fsp, types.MaxFsp)
	}
	if fsp < int64(types.MinFsp) {
		return types.Duration{}, true, errors.Errorf("Invalid negative %d specified, must in [0, 6]", fsp)
	}
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return types.Duration{}, true, err
	}
	v, _, err := types.ParseDuration(typeCtx(ctx), nowTs.UTC().Format(types.TimeFSPFormat), int(fsp))
	return v, false, err
}

type lastDayFunctionClass struct {
	baseFunctionClass
}

func (c *lastDayFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDate()
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
func (b *builtinLastDaySig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	arg, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	tm := arg
	year, month := tm.Year(), tm.Month()
	if tm.Month() == 0 || (tm.Day() == 0 && sqlMode(ctx).HasNoZeroDateMode()) {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String()))
	}
	lastDay := types.GetLastDay(year, month)
	ret := types.NewTime(types.FromDate(year, month, lastDay, 0, 0, 0, 0), mysql.TypeDate, types.DefaultFsp)
	return ret, false, nil
}

// getExpressionFsp calculates the fsp from given expression.
// This function must by called before calling newBaseBuiltinFuncWithTp.
func getExpressionFsp(ctx BuildContext, expression Expression) (int, error) {
	constExp, isConstant := expression.(*Constant)
	if isConstant {
		str, isNil, err := constExp.EvalString(ctx.GetEvalCtx(), chunk.Row{})
		if isNil || err != nil {
			return 0, err
		}
		return types.GetFsp(str), nil
	}
	warpExpr := WrapWithCastAsTime(ctx, expression, types.NewFieldType(mysql.TypeDatetime))
	return mathutil.Min(warpExpr.GetType(ctx.GetEvalCtx()).GetDecimal(), types.MaxFsp), nil
}

// tidbParseTsoFunctionClass extracts physical time from a tso
type tidbParseTsoFunctionClass struct {
	baseFunctionClass
}

func (c *tidbParseTsoFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, argTp, types.ETInt)
	if err != nil {
		return nil, err
	}

	bf.tp.SetType(mysql.TypeDatetime)
	bf.tp.SetFlen(mysql.MaxDateWidth)
	bf.tp.SetDecimal(types.DefaultFsp)
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
func (b *builtinTidbParseTsoSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	arg, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil || arg <= 0 {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}

	t := oracle.GetTimeFromTS(uint64(arg))
	result := types.NewTime(types.FromGoTime(t), mysql.TypeDatetime, types.MaxFsp)
	err = result.ConvertTimeZone(time.Local, location(ctx))
	if err != nil {
		return types.ZeroTime, true, err
	}
	return result, false, nil
}

// tidbParseTsoFunctionClass extracts logical time from a tso
type tidbParseTsoLogicalFunctionClass struct {
	baseFunctionClass
}

func (c *tidbParseTsoLogicalFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}

	sig := &builtinTidbParseTsoLogicalSig{bf}
	return sig, nil
}

type builtinTidbParseTsoLogicalSig struct {
	baseBuiltinFunc
}

func (b *builtinTidbParseTsoLogicalSig) Clone() builtinFunc {
	newSig := &builtinTidbParseTsoLogicalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalTime evals a builtinTidbParseTsoLogicalSig.
func (b *builtinTidbParseTsoLogicalSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil || arg <= 0 {
		return 0, true, err
	}

	t := oracle.ExtractLogical(uint64(arg))
	return t, false, nil
}

// tidbBoundedStalenessFunctionClass reads a time window [a, b] and compares it with the latest SafeTS
// to determine which TS to use in a read only transaction.
type tidbBoundedStalenessFunctionClass struct {
	baseFunctionClass
}

func (c *tidbBoundedStalenessFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDatetime, types.ETDatetime, types.ETDatetime)
	if err != nil {
		return nil, err
	}
	bf.setDecimalAndFlenForDatetime(3)
	sig := &builtinTiDBBoundedStalenessSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinTiDBBoundedStalenessSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
	contextopt.KVStorePropReader
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinTiDBBoundedStalenessSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps() |
		b.KVStorePropReader.RequiredOptionalEvalProps()
}

func (b *builtinTiDBBoundedStalenessSig) Clone() builtinFunc {
	newSig := &builtinTidbParseTsoSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinTiDBBoundedStalenessSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	store, err := b.GetKVStore(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}

	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return types.ZeroTime, true, err
	}

	leftTime, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	rightTime, isNull, err := b.args[1].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
	}
	if invalidLeftTime, invalidRightTime := leftTime.InvalidZero(), rightTime.InvalidZero(); invalidLeftTime || invalidRightTime {
		if invalidLeftTime {
			err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, leftTime.String()))
		}
		if invalidRightTime {
			err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, rightTime.String()))
		}
		return types.ZeroTime, true, err
	}
	timeZone := getTimeZone(ctx)
	minTime, err := leftTime.GoTime(timeZone)
	if err != nil {
		return types.ZeroTime, true, err
	}
	maxTime, err := rightTime.GoTime(timeZone)
	if err != nil {
		return types.ZeroTime, true, err
	}
	if minTime.After(maxTime) {
		return types.ZeroTime, true, nil
	}
	// Because the minimum unit of a TSO is millisecond, so we only need fsp to be 3.
	return types.NewTime(types.FromGoTime(calAppropriateTime(minTime, maxTime, GetStmtMinSafeTime(vars.StmtCtx, store, timeZone))), mysql.TypeDatetime, 3), false, nil
}

// GetStmtMinSafeTime get minSafeTime
func GetStmtMinSafeTime(sc *stmtctx.StatementContext, store kv.Storage, timeZone *time.Location) time.Time {
	var minSafeTS uint64
	txnScope := config.GetTxnScopeFromConfig()
	if store != nil {
		minSafeTS = store.GetMinSafeTS(txnScope)
	}
	// Inject mocked SafeTS for test.
	failpoint.Inject("injectSafeTS", func(val failpoint.Value) {
		injectTS := val.(int)
		minSafeTS = uint64(injectTS)
	})
	// Try to get from the stmt cache to make sure this function is deterministic.
	minSafeTS = sc.GetOrStoreStmtCache(stmtctx.StmtSafeTSCacheKey, minSafeTS).(uint64)
	return oracle.GetTimeFromTS(minSafeTS).In(timeZone)
}

// CalAppropriateTime directly calls calAppropriateTime
func CalAppropriateTime(minTime, maxTime, minSafeTime time.Time) time.Time {
	return calAppropriateTime(minTime, maxTime, minSafeTime)
}

// For a SafeTS t and a time range [t1, t2]:
//  1. If t < t1, we will use t1 as the result,
//     and with it, a read request may fail because it's an unreached SafeTS.
//  2. If t1 <= t <= t2, we will use t as the result, and with it,
//     a read request won't fail.
//  2. If t2 < t, we will use t2 as the result,
//     and with it, a read request won't fail because it's bigger than the latest SafeTS.
func calAppropriateTime(minTime, maxTime, minSafeTime time.Time) time.Time {
	if minSafeTime.Before(minTime) || minSafeTime.After(maxTime) {
		logutil.BgLogger().Debug("calAppropriateTime",
			zap.Time("minTime", minTime),
			zap.Time("maxTime", maxTime),
			zap.Time("minSafeTime", minSafeTime))
		if minSafeTime.Before(minTime) {
			return minTime
		} else if minSafeTime.After(maxTime) {
			return maxTime
		}
	}
	logutil.BgLogger().Debug("calAppropriateTime",
		zap.Time("minTime", minTime),
		zap.Time("maxTime", maxTime),
		zap.Time("minSafeTime", minSafeTime))
	return minSafeTime
}

// getFspByIntArg is used by some time functions to get the result fsp. If len(expr) == 0, then the fsp is not explicit set, use 0 as default.
func getFspByIntArg(ctx BuildContext, exps []Expression) (int, error) {
	if len(exps) == 0 {
		return 0, nil
	}
	if len(exps) != 1 {
		return 0, errors.Errorf("Should not happen, the num of argument should be 1, but got %d", len(exps))
	}
	_, ok := exps[0].(*Constant)
	if ok {
		fsp, isNuLL, err := exps[0].EvalInt(ctx.GetEvalCtx(), chunk.Row{})
		if err != nil || isNuLL {
			// If isNULL, it may be a bug of parser. Return 0 to be compatible with old version.
			return 0, err
		}
		if fsp > int64(types.MaxFsp) {
			return 0, errors.Errorf("Too-big precision %v specified for 'curtime'. Maximum is %v", fsp, types.MaxFsp)
		} else if fsp < int64(types.MinFsp) {
			return 0, errors.Errorf("Invalid negative %d specified, must in [0, 6]", fsp)
		}
		return int(fsp), nil
	}
	// Should no happen. But our tests may generate non-constant input.
	return 0, nil
}

type tidbCurrentTsoFunctionClass struct {
	baseFunctionClass
}

func (c *tidbCurrentTsoFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinTiDBCurrentTsoSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinTiDBCurrentTsoSig struct {
	baseBuiltinFunc
	contextopt.SessionVarsPropReader
}

func (b *builtinTiDBCurrentTsoSig) Clone() builtinFunc {
	newSig := &builtinTiDBCurrentTsoSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinTiDBCurrentTsoSig) RequiredOptionalEvalProps() OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps()
}

// evalInt evals currentTSO().
func (b *builtinTiDBCurrentTsoSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	tso, _ := sessionVars.GetSessionOrGlobalSystemVar(context.Background(), "tidb_current_ts")
	itso, _ := strconv.ParseInt(tso, 10, 64)
	return itso, false, nil
}
