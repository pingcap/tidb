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
	"strings"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func (b *builtinDateDiffSig) vectorized() bool {
	return true
}

func (b *builtinDateDiffSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err = b.args[0].VecEvalTime(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err = b.args[1].VecEvalTime(ctx, input, buf1); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf0, buf1)
	args0 := buf0.Times()
	args1 := buf1.Times()
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if invalidArg0, invalidArg1 := args0[i].InvalidZero(), args1[i].InvalidZero(); invalidArg0 || invalidArg1 {
			if invalidArg0 {
				err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, args0[i].String()))
			}
			if invalidArg1 {
				err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, args1[i].String()))
			}
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = int64(types.DateDiff(args0[i].CoreTime(), args1[i].CoreTime()))
	}
	return nil
}

func (b *builtinCurrentDateSig) vectorized() bool {
	return true
}

func (b *builtinCurrentDateSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return err
	}

	tz := location(ctx)
	year, month, day := nowTs.In(tz).Date()
	timeValue := types.NewTime(types.FromDate(year, int(month), day, 0, 0, 0, 0), mysql.TypeDate, 0)

	n := input.NumRows()
	result.ResizeTime(n, false)
	times := result.Times()
	for i := range n {
		times[i] = timeValue
	}
	return nil
}

func (b *builtinMakeTimeSig) vectorized() bool {
	return true
}

func (b *builtinMakeTimeSig) getVecIntParam(ctx EvalContext, arg Expression, input *chunk.Chunk, col *chunk.Column) (err error) {
	if arg.GetType(ctx).EvalType() == types.ETReal {
		err = arg.VecEvalReal(ctx, input, col)
		if err != nil {
			return err
		}
		f64s := col.Float64s()
		i64s := col.Int64s()
		n := input.NumRows()
		for i := range n {
			i64s[i] = int64(f64s[i])
		}
		return nil
	}
	err = arg.VecEvalInt(ctx, input, col)
	return err
}

func (b *builtinMakeTimeSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeInt64(n, false)
	hoursBuf := result
	var err error
	if err = b.getVecIntParam(ctx, b.args[0], input, hoursBuf); err != nil {
		return err
	}
	minutesBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(minutesBuf)
	if err = b.getVecIntParam(ctx, b.args[1], input, minutesBuf); err != nil {
		return err
	}
	secondsBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(secondsBuf)
	if err = b.args[2].VecEvalReal(ctx, input, secondsBuf); err != nil {
		return err
	}
	hours := hoursBuf.Int64s()
	minutes := minutesBuf.Int64s()
	seconds := secondsBuf.Float64s()
	durs := result.GoDurations()
	result.MergeNulls(minutesBuf, secondsBuf)
	hourUnsignedFlag := mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag())
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if minutes[i] < 0 || minutes[i] >= 60 || seconds[i] < 0 || seconds[i] >= 60 {
			result.SetNull(i, true)
			continue
		}
		dur, err := b.makeTime(typeCtx(ctx), hours[i], minutes[i], seconds[i], hourUnsignedFlag)
		if err != nil {
			return err
		}
		durs[i] = dur.Duration
	}
	return nil
}

func (b *builtinDayOfYearSig) vectorized() bool {
	return true
}

func (b *builtinDayOfYearSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if ds[i].InvalidZero() {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, ds[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = int64(ds[i].YearDay())
	}
	return nil
}

func (b *builtinFromUnixTime1ArgSig) vectorized() bool {
	return true
}

func (b *builtinFromUnixTime1ArgSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDecimal(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	ts := result.Times()
	ds := buf.Decimals()
	fsp := b.tp.GetDecimal()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		t, isNull, err := evalFromUnixTime(ctx, fsp, &ds[i])
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
			continue
		}
		ts[i] = t
	}
	return nil
}

func (b *builtinYearWeekWithModeSig) vectorized() bool {
	return true
}

// vecEvalInt evals YEARWEEK(date,mode).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithModeSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	if err := b.args[0].VecEvalTime(ctx, input, buf1); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	if err := b.args[1].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf1)
	i64s := result.Int64s()
	ds := buf1.Times()
	ms := buf2.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		date := ds[i]
		if date.IsZero() {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		mode := int(ms[i])
		if buf2.IsNull(i) {
			mode = 0
		}
		year, week := date.YearWeek(mode)
		i64s[i] = int64(week + year*100)
		if i64s[i] < 0 {
			i64s[i] = int64(math.MaxUint32)
		}
	}
	return nil
}

func (b *builtinTimestampDiffSig) vectorized() bool {
	return true
}

// vecEvalInt evals a builtinTimestampDiffSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampdiff
func (b *builtinTimestampDiffSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	unitBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(unitBuf)
	if err := b.args[0].VecEvalString(ctx, input, unitBuf); err != nil {
		return err
	}
	lhsBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(lhsBuf)
	if err := b.args[1].VecEvalTime(ctx, input, lhsBuf); err != nil {
		return err
	}
	rhsBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(rhsBuf)
	if err := b.args[2].VecEvalTime(ctx, input, rhsBuf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(unitBuf, lhsBuf, rhsBuf)
	i64s := result.Int64s()
	lhs := lhsBuf.Times()
	rhs := rhsBuf.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if invalidLHS, invalidRHS := lhs[i].InvalidZero(), rhs[i].InvalidZero(); invalidLHS || invalidRHS {
			if invalidLHS {
				err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, lhs[i].String()))
			}
			if invalidRHS {
				err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, rhs[i].String()))
			}
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = types.TimestampDiff(unitBuf.GetString(i), lhs[i], rhs[i])
	}
	return nil
}

func (b *builtinUnixTimestampIntSig) vectorized() bool {
	return true
}

// vecEvalInt evals a UNIX_TIMESTAMP(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_unix-timestamp
func (b *builtinUnixTimestampIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	result.ResizeInt64(n, false)
	i64s := result.Int64s()

	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		var isNull bool
		for i := range n {
			i64s[i], isNull, err = b.evalInt(ctx, input.GetRow(i))
			if err != nil {
				return err
			}
			if isNull {
				result.SetNull(i, true)
			}
		}
	} else {
		result.MergeNulls(buf)
		for i := range n {
			if result.IsNull(i) {
				continue
			}

			t, err := buf.GetTime(i).AdjustedGoTime(getTimeZone(ctx))
			if err != nil {
				i64s[i] = 0
				continue
			}
			dec, err := goTimeToMysqlUnixTimestamp(t, 1)
			if err != nil {
				return err
			}
			intVal, err := dec.ToInt()
			if !terror.ErrorEqual(err, types.ErrTruncated) {
				terror.Log(err)
			}
			i64s[i] = intVal
		}
	}

	return nil
}

func (b *builtinCurrentTime0ArgSig) vectorized() bool {
	return true
}

func (b *builtinCurrentTime0ArgSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return err
	}
	tz := location(ctx)
	dur := nowTs.In(tz).Format(types.TimeFormat)
	res, _, err := types.ParseDuration(typeCtx(ctx), dur, types.MinFsp)
	if err != nil {
		return err
	}
	result.ResizeGoDuration(n, false)
	durations := result.GoDurations()
	for i := range n {
		durations[i] = res.Duration
	}
	return nil
}

func (b *builtinTimeSig) vectorized() bool {
	return true
}

func (b *builtinTimeSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeGoDuration(n, false)
	result.MergeNulls(buf)
	ds := result.GoDurations()
	tc := typeCtx(ctx)
	for i := range n {
		if result.IsNull(i) {
			continue
		}

		fsp := 0
		expr := buf.GetString(i)
		if idx := strings.Index(expr, "."); idx != -1 {
			fsp = len(expr) - idx - 1
		}

		var tmpFsp int
		if tmpFsp, err = types.CheckFsp(fsp); err != nil {
			return err
		}
		fsp = tmpFsp

		res, _, err := types.ParseDuration(tc, expr, fsp)
		if types.ErrTruncatedWrongVal.Equal(err) {
			err = tc.HandleTruncate(err)
		}
		if err != nil {
			return err
		}
		ds[i] = res.Duration
	}
	return nil
}

func (b *builtinDateLiteralSig) vectorized() bool {
	return true
}

func (b *builtinDateLiteralSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	mode := sqlMode(ctx)
	if mode.HasNoZeroDateMode() && b.literal.IsZero() {
		return types.ErrWrongValue.GenWithStackByArgs(types.DateStr, b.literal.String())
	}
	if mode.HasNoZeroInDateMode() && (b.literal.InvalidZero() && !b.literal.IsZero()) {
		return types.ErrWrongValue.GenWithStackByArgs(types.DateStr, b.literal.String())
	}

	result.ResizeTime(n, false)
	times := result.Times()
	for i := range times {
		times[i] = b.literal
	}
	return nil
}

func (b *builtinTimeLiteralSig) vectorized() bool {
	return true
}

func (b *builtinTimeLiteralSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeGoDuration(n, false)
	d64s := result.GoDurations()
	for i := range n {
		d64s[i] = b.duration.Duration
	}
	return nil
}

func (b *builtinMonthNameSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	ds := buf.Times()
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		mon := ds[i].Month()
		if (ds[i].IsZero() && sqlMode(ctx).HasNoZeroDateMode()) || mon < 0 || mon > len(types.MonthNames) {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, ds[i].String())); err != nil {
				return err
			}
			result.AppendNull()
			continue
		} else if mon == 0 || ds[i].IsZero() {
			result.AppendNull()
			continue
		}
		result.AppendString(types.MonthNames[mon-1])
	}
	return nil
}

func (b *builtinMonthNameSig) vectorized() bool {
	return true
}

func (b *builtinDayOfWeekSig) vectorized() bool {
	return true
}

func (b *builtinDayOfWeekSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if ds[i].InvalidZero() {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, ds[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = int64(ds[i].Weekday() + 1)
	}
	return nil
}

func (b *builtinCurrentTime1ArgSig) vectorized() bool {
	return true
}

func (b *builtinCurrentTime1ArgSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return err
	}
	tz := location(ctx)
	dur := nowTs.In(tz).Format(types.TimeFSPFormat)
	tc := typeCtx(ctx)
	i64s := buf.Int64s()
	result.ResizeGoDuration(n, false)
	durations := result.GoDurations()
	for i := range n {
		res, _, err := types.ParseDuration(tc, dur, int(i64s[i]))
		if err != nil {
			return err
		}
		durations[i] = res.Duration
	}
	return nil
}

func (b *builtinUTCTimestampWithoutArgSig) vectorized() bool {
	return true
}

// vecEvalTime evals UTC_TIMESTAMP().
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithoutArgSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	res, isNull, err := evalUTCTimestampWithFsp(ctx, types.DefaultFsp)
	if err != nil {
		return err
	}
	if isNull {
		result.ResizeTime(n, true)
		return nil
	}
	result.ResizeTime(n, false)
	t64s := result.Times()
	for i := range n {
		t64s[i] = res
	}
	return nil
}

func (b *builtinConvertTzSig) vectorized() bool {
	return true
}

func (b *builtinConvertTzSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalTime(ctx, input, result); err != nil {
		return err
	}

	fromTzBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(fromTzBuf)
	if err := b.args[1].VecEvalString(ctx, input, fromTzBuf); err != nil {
		return err
	}

	toTzBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(toTzBuf)
	if err := b.args[2].VecEvalString(ctx, input, toTzBuf); err != nil {
		return err
	}

	result.MergeNulls(fromTzBuf, toTzBuf)
	ts := result.Times()
	var isNull bool
	for i := range n {
		if result.IsNull(i) {
			continue
		}

		ts[i], isNull, err = b.convertTz(ts[i], fromTzBuf.GetString(i), toTzBuf.GetString(i))
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
		}
	}
	return nil
}

func (b *builtinTimestamp1ArgSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	times := result.Times()
	tc := typeCtx(ctx)
	var tm types.Time
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		s := buf.GetString(i)

		if b.isFloat {
			tm, err = types.ParseTimeFromFloatString(tc, s, mysql.TypeDatetime, types.GetFsp(s))
		} else {
			tm, err = types.ParseTime(tc, s, mysql.TypeDatetime, types.GetFsp(s))
		}
		if err != nil {
			if err = handleInvalidTimeError(ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		times[i] = tm
	}
	return nil
}

func (b *builtinTimestamp1ArgSig) vectorized() bool {
	return true
}

func (b *builtinTimestamp2ArgsSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf0, buf1)
	times := result.Times()
	tc := typeCtx(ctx)
	var tm types.Time
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		arg0 := buf0.GetString(i)
		arg1 := buf1.GetString(i)

		if b.isFloat {
			tm, err = types.ParseTimeFromFloatString(tc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
		} else {
			tm, err = types.ParseTime(tc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
		}
		if err != nil {
			if err = handleInvalidTimeError(ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		if tm.Year() == 0 {
			// MySQL won't evaluate add for date with zero year.
			// See https://github.com/mysql/mysql-server/blob/5.7/sql/item_timefunc.cc#L2805
			result.SetNull(i, true)
			continue
		}

		if !isDuration(arg1) {
			result.SetNull(i, true)
			continue
		}

		duration, _, err := types.ParseDuration(tc, arg1, types.GetFsp(arg1))
		if err != nil {
			if err = handleInvalidTimeError(ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		tmp, err := tm.Add(tc, duration)
		if err != nil {
			return err
		}
		times[i] = tmp
	}
	return nil
}

func (b *builtinTimestamp2ArgsSig) vectorized() bool {
	return true
}

func (b *builtinDayOfMonthSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(ds[i].Day())
	}
	return nil
}

func (b *builtinDayOfMonthSig) vectorized() bool {
	return true
}

func (b *builtinAddSubDateAsStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	unit, isNull, err := b.args[2].EvalString(ctx, chunk.Row{})
	if err != nil {
		return err
	}
	if isNull {
		result.ReserveString(n)
		result.SetNulls(0, n, true)
		return nil
	}

	intervalBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(intervalBuf)
	if err := b.vecGetInterval(&b.baseDateArithmetical, ctx, &b.baseBuiltinFunc, input, unit, intervalBuf); err != nil {
		return err
	}

	dateBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dateBuf)
	if err := b.vecGetDate(&b.baseDateArithmetical, ctx, &b.baseBuiltinFunc, input, unit, dateBuf); err != nil {
		return err
	}

	result.ReserveString(n)

	dateBuf.MergeNulls(intervalBuf)
	for i := range n {
		if dateBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		resDate, isNull, err := b.timeOp(&b.baseDateArithmetical, ctx, dateBuf.Times()[i], intervalBuf.GetString(i), unit, b.tp.GetDecimal())
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
		} else {
			if resDate.Microsecond() == 0 {
				resDate.SetFsp(types.MinFsp)
			} else {
				resDate.SetFsp(types.MaxFsp)
			}
			result.AppendString(resDate.String())
		}
	}
	return nil
}

func (b *builtinAddSubDateAsStringSig) vectorized() bool {
	return true
}

func (b *builtinAddSubDateDatetimeAnySig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	unit, isNull, err := b.args[2].EvalString(ctx, chunk.Row{})
	if err != nil {
		return err
	}
	if isNull {
		result.ResizeTime(n, true)
		return nil
	}

	intervalBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(intervalBuf)
	if err := b.vecGetInterval(&b.baseDateArithmetical, ctx, &b.baseBuiltinFunc, input, unit, intervalBuf); err != nil {
		return err
	}

	if err := b.vecGetDateFromDatetime(&b.baseBuiltinFunc, ctx, input, unit, result); err != nil {
		return err
	}

	result.MergeNulls(intervalBuf)
	resDates := result.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		resDate, isNull, err := b.timeOp(&b.baseDateArithmetical, ctx, resDates[i], intervalBuf.GetString(i), unit, b.tp.GetDecimal())
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
		} else {
			resDates[i] = resDate
		}
	}
	return nil
}

func (b *builtinAddSubDateDatetimeAnySig) vectorized() bool {
	return true
}

func (b *builtinAddSubDateDurationAnySig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	unit, isNull, err := b.args[2].EvalString(ctx, chunk.Row{})
	if err != nil {
		return err
	}
	if isNull {
		result.ResizeTime(n, true)
		return nil
	}

	intervalBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(intervalBuf)
	if err := b.vecGetInterval(&b.baseDateArithmetical, ctx, &b.baseBuiltinFunc, input, unit, intervalBuf); err != nil {
		return err
	}

	durBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(durBuf)
	if err := b.args[0].VecEvalDuration(ctx, input, durBuf); err != nil {
		return err
	}

	goDurations := durBuf.GoDurations()
	result.ResizeTime(n, false)
	result.MergeNulls(durBuf, intervalBuf)
	resDates := result.Times()
	iterDuration := types.Duration{Fsp: types.MaxFsp}
	tc := typeCtx(ctx)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		iterDuration.Duration = goDurations[i]
		t, err := iterDuration.ConvertToTime(tc, mysql.TypeDatetime)
		if err != nil {
			result.SetNull(i, true)
		}
		resDate, isNull, err := b.timeOp(&b.baseDateArithmetical, ctx, t, intervalBuf.GetString(i), unit, b.tp.GetDecimal())
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
		} else {
			resDates[i] = resDate
		}
	}
	return nil
}

func (b *builtinAddSubDateDurationAnySig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	unit, isNull, err := b.args[2].EvalString(ctx, chunk.Row{})
	if err != nil {
		return err
	}
	if isNull {
		result.ResizeGoDuration(n, true)
		return nil
	}

	intervalBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(intervalBuf)
	if err := b.vecGetInterval(&b.baseDateArithmetical, ctx, &b.baseBuiltinFunc, input, unit, intervalBuf); err != nil {
		return err
	}

	result.ResizeGoDuration(n, false)
	if err := b.args[0].VecEvalDuration(ctx, input, result); err != nil {
		return err
	}

	result.MergeNulls(intervalBuf)
	resDurations := result.GoDurations()
	iterDuration := types.Duration{Fsp: types.MaxFsp}
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		iterDuration.Duration = resDurations[i]
		resDuration, isNull, err := b.durationOp(&b.baseDateArithmetical, ctx, iterDuration, intervalBuf.GetString(i), unit, b.tp.GetDecimal())
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
		} else {
			resDurations[i] = resDuration.Duration
		}
	}
	return nil
}

func (b *builtinAddSubDateDurationAnySig) vectorized() bool {
	return true
}
