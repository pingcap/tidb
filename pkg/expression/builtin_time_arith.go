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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

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
	if err != nil {
		return types.ZeroTime, true, handleInvalidTimeError(ctx, err)
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
				intervalFsp = min(types.MaxFsp, args[1].GetType(ctx.GetEvalCtx()).GetDecimal())
			}
		}
		resultFsp = min(types.MaxFsp, max(args[0].GetType(ctx.GetEvalCtx()).GetDecimal(), intervalFsp))
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
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETString && resultTp == mysql.TypeDuration:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromString,
			vecGetInterval:       vecGetIntervalFromString,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationString, tipb.ScalarFuncSig_SubDateDurationString)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETInt && resultTp == mysql.TypeDuration:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromInt,
			vecGetInterval:       vecGetIntervalFromInt,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationInt, tipb.ScalarFuncSig_SubDateDurationInt)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETReal && resultTp == mysql.TypeDuration:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromReal,
			vecGetInterval:       vecGetIntervalFromReal,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationReal, tipb.ScalarFuncSig_SubDateDurationReal)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETDecimal && resultTp == mysql.TypeDuration:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromDecimal,
			vecGetInterval:       vecGetIntervalFromDecimal,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationDecimal, tipb.ScalarFuncSig_SubDateDurationDecimal)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETString && resultTp == mysql.TypeDatetime:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromString,
			vecGetInterval:       vecGetIntervalFromString,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationStringDatetime, tipb.ScalarFuncSig_SubDateDurationStringDatetime)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETInt && resultTp == mysql.TypeDatetime:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromInt,
			vecGetInterval:       vecGetIntervalFromInt,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationIntDatetime, tipb.ScalarFuncSig_SubDateDurationIntDatetime)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETReal && resultTp == mysql.TypeDatetime:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromReal,
			vecGetInterval:       vecGetIntervalFromReal,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationRealDatetime, tipb.ScalarFuncSig_SubDateDurationRealDatetime)
	case dateEvalTp == types.ETDuration && intervalEvalTp == types.ETDecimal && resultTp == mysql.TypeDatetime:
		sig = &builtinAddSubDateDurationAnySig{
			baseBuiltinFunc:      bf,
			baseDateArithmetical: newDateArithmeticalUtil(),
			getInterval:          getIntervalFromDecimal,
			vecGetInterval:       vecGetIntervalFromDecimal,
			timeOp:               c.timeOp,
			durationOp:           c.durationOp,
		}
		c.setPbCodeOp(sig, tipb.ScalarFuncSig_AddDateDurationDecimalDatetime, tipb.ScalarFuncSig_SubDateDurationDecimalDatetime)
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

