// Copyright 2019 PingCAP, Inc.
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
	"strconv"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
