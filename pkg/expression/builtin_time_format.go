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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"math"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

type dateFormatFunctionClass struct {
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
					fieldLen := min(x.RetType.GetFlen()+6, mysql.MaxDecimalWidth)
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
			fsp = min(bf.tp.GetDecimal(), arg0Tp.GetDecimal())
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

type timestampDiffFunctionClass struct {
	baseFunctionClass
}

type addTimeFunctionClass struct {
	baseFunctionClass
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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

		var err error
		tb, err = types.AddDate(0, int64(v), 0, t)
		if err != nil {
			return tb, false, err
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
