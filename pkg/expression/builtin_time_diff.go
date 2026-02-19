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
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// for issue 59417, should return NULL when month or day is zero and sql_mode contains NO_ZERO_IN_DATE
	if !expr.IsZero() && expr.InvalidZero() && sqlMode(ctx).HasNoZeroInDateMode() {
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	fsp := max(arg0Dec, arg1Dec)
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
			fsp = max(lenStrFsp, fsp)
		}
	}
	return types.StrToDuration(tc, str, fsp)
}
