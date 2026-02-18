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
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func (b *builtinExtractDatetimeSig) vectorized() bool {
	return true
}

func (b *builtinExtractDatetimeSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	unit, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(unit)
	if err := b.args[0].VecEvalString(ctx, input, unit); err != nil {
		return err
	}
	dt, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dt)
	if err = b.args[1].VecEvalTime(ctx, input, dt); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(unit, dt)
	i64s := result.Int64s()
	tmIs := dt.Times()
	var t types.Time
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		unitI := unit.GetString(i)
		t = tmIs[i]
		i64s[i], err = types.ExtractDatetimeNum(&t, unitI)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *builtinExtractDurationSig) vectorized() bool {
	return true
}

func (b *builtinExtractDurationSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	unit, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(unit)
	if err := b.args[0].VecEvalString(ctx, input, unit); err != nil {
		return err
	}
	dur, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dur)
	if err = b.args[1].VecEvalDuration(ctx, input, dur); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(unit, dur)
	i64s := result.Int64s()
	durIs := dur.GoDurations()
	var duration types.Duration
	duration.Fsp = b.args[1].GetType(ctx).GetDecimal()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		unitI := unit.GetString(i)
		duration.Duration = durIs[i]
		i64s[i], err = types.ExtractDurationNum(&duration, unitI)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *builtinStrToDateDurationSig) vectorized() bool {
	return true
}

func (b *builtinStrToDateDurationSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufStrings, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufStrings)
	if err := b.args[0].VecEvalString(ctx, input, bufStrings); err != nil {
		return err
	}

	bufFormats, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufFormats)
	if err := b.args[1].VecEvalString(ctx, input, bufFormats); err != nil {
		return err
	}

	result.ResizeGoDuration(n, false)
	result.MergeNulls(bufStrings, bufFormats)
	d64s := result.GoDurations()
	tc := typeCtx(ctx)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		var t types.Time
		succ := t.StrToDate(tc, bufStrings.GetString(i), bufFormats.GetString(i))
		if !succ {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		t.SetFsp(b.tp.GetDecimal())
		dur, err := t.ConvertToDuration()
		if err != nil {
			return err
		}
		d64s[i] = dur.Duration
	}
	return nil
}

func (b *builtinToSecondsSig) vectorized() bool {
	return true
}

// vecEvalInt evals a builtinToSecondsSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-seconds
func (b *builtinToSecondsSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		arg := ds[i]
		ret := types.TimestampDiff("SECOND", types.ZeroDate, arg)
		if ret == 0 {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = ret
	}
	return nil
}

func (b *builtinMinuteSig) vectorized() bool {
	return true
}

func (b *builtinMinuteSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return vecEvalIntByRows(ctx, b, input, result)
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(buf.GetDuration(i, types.UnspecifiedFsp).Minute())
	}
	return nil
}

func (b *builtinSecondSig) vectorized() bool {
	return true
}

func (b *builtinSecondSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return vecEvalIntByRows(ctx, b, input, result)
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(buf.GetDuration(i, types.UnspecifiedFsp).Second())
	}
	return nil
}

func (b *builtinNowWithoutArgSig) vectorized() bool {
	return true
}

func (b *builtinNowWithoutArgSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	nowTs, isNull, err := evalNowWithFsp(ctx, 0)
	if err != nil {
		return err
	}
	if isNull {
		result.ResizeTime(n, true)
		return nil
	}
	result.ResizeTime(n, false)
	times := result.Times()
	for i := range n {
		times[i] = nowTs
	}
	return nil
}

func (b *builtinTimestampLiteralSig) vectorized() bool {
	return true
}

func (b *builtinTimestampLiteralSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeTime(n, false)
	times := result.Times()
	for i := range times {
		times[i] = b.tm
	}
	return nil
}

func (b *builtinMakeDateSig) vectorized() bool {
	return true
}

func (b *builtinMakeDateSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[0].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf1, buf2)

	times := result.Times()
	years := buf1.Int64s()
	days := buf2.Int64s()

	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if days[i] <= 0 || years[i] < 0 || years[i] > 9999 {
			result.SetNull(i, true)
			continue
		}
		if years[i] < 70 {
			years[i] += 2000
		} else if years[i] < 100 {
			years[i] += 1900
		}
		startTime := types.NewTime(types.FromDate(int(years[i]), 1, 1, 0, 0, 0, 0), mysql.TypeDate, 0)
		retTimestamp := types.TimestampDiff("DAY", types.ZeroDate, startTime)
		if retTimestamp == 0 {
			if err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, startTime.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		ret := types.TimeFromDays(retTimestamp + days[i] - 1)
		if ret.IsZero() || ret.Year() > 9999 {
			result.SetNull(i, true)
			continue
		}
		times[i] = ret
	}
	return nil
}

func (b *builtinWeekOfYearSig) vectorized() bool {
	return true
}

func (b *builtinWeekOfYearSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalTime(ctx, input, buf); err != nil {
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
		if ds[i].IsZero() {
			if err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, ds[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		week := ds[i].Week(3)
		i64s[i] = int64(week)
	}
	return nil
}

func (b *builtinUTCTimestampWithArgSig) vectorized() bool {
	return true
}

// vecEvalTime evals UTC_TIMESTAMP(fsp).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithArgSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	t64s := result.Times()
	i64s := buf.Int64s()
	result.MergeNulls(buf)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		fsp := i64s[i]
		if fsp > int64(math.MaxInt32) || fsp < int64(types.MinFsp) {
			return types.ErrSyntax.GenWithStack(util.SyntaxErrorPrefix)
		}
		if fsp > int64(types.MaxFsp) {
			return types.ErrTooBigPrecision.GenWithStackByArgs(fsp, "utc_timestamp", types.MaxFsp)
		}
		res, isNull, err := evalUTCTimestampWithFsp(ctx, int(fsp))
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
			continue
		}
		t64s[i] = res
	}
	return nil
}

func (b *builtinTimeToSecSig) vectorized() bool {
	return true
}

// vecEvalInt evals TIME_TO_SEC(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-to-sec
func (b *builtinTimeToSecSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	fsp := b.args[0].GetType(ctx).GetDecimal()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		var sign int
		duration := buf.GetDuration(i, fsp)
		if duration.Duration >= 0 {
			sign = 1
		} else {
			sign = -1
		}
		i64s[i] = int64(sign * (duration.Hour()*3600 + duration.Minute()*60 + duration.Second()))
	}
	return nil
}

func (b *builtinStrToDateDatetimeSig) vectorized() bool {
	return true
}

func (b *builtinStrToDateDatetimeSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	dateBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dateBuf)
	if err = b.args[0].VecEvalString(ctx, input, dateBuf); err != nil {
		return err
	}

	formatBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(formatBuf)
	if err = b.args[1].VecEvalString(ctx, input, formatBuf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(dateBuf, formatBuf)
	times := result.Times()
	tc := typeCtx(ctx)
	hasNoZeroDateMode := sqlMode(ctx).HasNoZeroDateMode()
	fsp := b.tp.GetDecimal()

	for i := range n {
		if result.IsNull(i) {
			continue
		}
		var t types.Time
		succ := t.StrToDate(tc, dateBuf.GetString(i), formatBuf.GetString(i))
		if !succ {
			if err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		if hasNoZeroDateMode && (t.Year() == 0 || t.Month() == 0 || t.Day() == 0) {
			if err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		t.SetType(mysql.TypeDatetime)
		t.SetFsp(fsp)
		times[i] = t
	}
	return nil
}

func (b *builtinUTCDateSig) vectorized() bool {
	return true
}

func (b *builtinUTCDateSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return err
	}
	year, month, day := nowTs.UTC().Date()
	utcDate := types.NewTime(types.FromGoTime(time.Date(year, month, day, 0, 0, 0, 0, time.UTC)), mysql.TypeDate, types.UnspecifiedFsp)

	n := input.NumRows()
	result.ResizeTime(n, false)
	times := result.Times()
	for i := range n {
		times[i] = utcDate
	}
	return nil
}

func (b *builtinWeekWithoutModeSig) vectorized() bool {
	return true
}

func (b *builtinWeekWithoutModeSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()

	mode := 0
	if modeStr := ctx.GetDefaultWeekFormatMode(); modeStr != "" {
		mode, err = strconv.Atoi(modeStr)
		if err != nil {
			return handleInvalidTimeError(ctx, types.ErrInvalidWeekModeFormat.GenWithStackByArgs(modeStr))
		}
	}
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

		week := date.Week(mode)
		i64s[i] = int64(week)
	}
	return nil
}

func (b *builtinUnixTimestampDecSig) vectorized() bool {
	return true
}

func (b *builtinUnixTimestampDecSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeDecimal(n, false)
	ts := result.Decimals()
	timeBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(timeBuf)
	if err := b.args[0].VecEvalTime(ctx, input, timeBuf); err != nil {
		for i := range n {
			temp, isNull, err := b.evalDecimal(ctx, input.GetRow(i))
			if err != nil {
				return err
			}
			ts[i] = *temp
			if isNull {
				result.SetNull(i, true)
			}
		}
	} else {
		result.MergeNulls(timeBuf)
		for i := range n {
			if result.IsNull(i) {
				continue
			}
			t, err := timeBuf.GetTime(i).GoTime(getTimeZone(ctx))
			if err != nil {
				ts[i] = *new(types.MyDecimal)
				continue
			}
			tmp, err := goTimeToMysqlUnixTimestamp(t, b.tp.GetDecimal())
			if err != nil {
				return err
			}
			ts[i] = *tmp
		}
	}
	return nil
}

func (b *builtinPeriodAddSig) vectorized() bool {
	return true
}

// vecEvalInt evals PERIOD_ADD(P,N).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-add
func (b *builtinPeriodAddSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}
	i64s := result.Int64s()
	result.MergeNulls(buf)
	ns := buf.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}

		// in MySQL, if p is invalid but n is NULL, the result is NULL, so we have to check if n is NULL first.
		if !validPeriod(i64s[i]) {
			return errIncorrectArgs.GenWithStackByArgs("period_add")
		}
		sumMonth := int64(period2Month(uint64(i64s[i]))) + ns[i]
		i64s[i] = int64(month2Period(uint64(sumMonth)))
	}
	return nil
}

func (b *builtinTimestampAddSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinTimestampAddSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampadd
func (b *builtinTimestampAddSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalReal(ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalTime(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf1.Float64s()
	ds := buf2.Times()
	for i := range n {
		if buf.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		unit := buf.GetString(i)
		v := nums[i]
		arg := ds[i]

		tm1, err := arg.GoTime(time.Local)
		if err != nil {
			tc := typeCtx(ctx)
			tc.AppendWarning(err)
			result.AppendNull()
			continue
		}
		tb, overflow, err := addUnitToTime(unit, tm1, v)
		if err != nil {
			return err
		}
		if overflow {
			if err = handleInvalidTimeError(ctx, types.ErrDatetimeFunctionOverflow.GenWithStackByArgs("datetime")); err != nil {
				return err
			}
			result.AppendNull()
			continue
		}
		fsp := types.DefaultFsp
		// use MaxFsp when microsecond is not zero
		if tb.Nanosecond()/1000 != 0 {
			fsp = types.MaxFsp
		}
		r := types.NewTime(types.FromGoTime(tb), b.resolveType(arg.Type(), unit), fsp)
		if err = r.Check(typeCtx(ctx)); err != nil {
			if err = handleInvalidTimeError(ctx, err); err != nil {
				return err
			}
			result.AppendNull()
			continue
		}
		result.AppendString(r.String())
	}
	return nil
}

func (b *builtinToDaysSig) vectorized() bool {
	return true
}

// vecEvalInt evals a builtinToDaysSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-days
func (b *builtinToDaysSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		arg := ds[i]
		ret := types.TimestampDiff("DAY", types.ZeroDate, arg)
		if ret == 0 {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, arg.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = ret
	}
	return nil
}

func (b *builtinDateFormatSig) vectorized() bool {
	return true
}

func (b *builtinDateFormatSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	dateBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dateBuf)
	if err := b.args[0].VecEvalTime(ctx, input, dateBuf); err != nil {
		return err
	}
	times := dateBuf.Times()

	formatBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(formatBuf)
	if err := b.args[1].VecEvalString(ctx, input, formatBuf); err != nil {
		return err
	}

	result.ReserveString(n)

	for i := range times {
		t := times[i]
		if dateBuf.IsNull(i) || formatBuf.IsNull(i) {
			result.AppendNull()
			continue
		}

		formatMask := formatBuf.GetString(i)
		// MySQL compatibility, #11203
		// If format mask is 0 then return 0 without warnings
		if formatMask == "0" {
			result.AppendString("0")
			continue
		}

		if t.InvalidZero() {
			// MySQL compatibility, #11203
			// 0 | 0.0 should be converted to null without warnings
			n, err := t.ToNumber().ToInt()
			isOriginalIntOrDecimalZero := err == nil && n == 0
			// Args like "0000-00-00", "0000-00-00 00:00:00" set Fsp to 6
			isOriginalStringZero := t.Fsp() > 0

			result.AppendNull()
			if isOriginalIntOrDecimalZero && !isOriginalStringZero {
				continue
			}
			if errHandled := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String())); errHandled != nil {
				return errHandled
			}
			continue
		}
		res, err := t.DateFormat(formatMask)
		if err != nil {
			return err
		}
		result.AppendString(res)
	}
	return nil
}

func (b *builtinHourSig) vectorized() bool {
	return true
}

func (b *builtinHourSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return vecEvalIntByRows(ctx, b, input, result)
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(buf.GetDuration(i, int(types.UnspecifiedFsp)).Hour())
	}
	return nil
}

func (b *builtinSecToTimeSig) vectorized() bool {
	return true
}

func (b *builtinSecToTimeSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}
	args := buf.Float64s()
	result.ResizeGoDuration(n, false)
	result.MergeNulls(buf)
	durations := result.GoDurations()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		secondsFloat := args[i]
		negative := ""
		if secondsFloat < 0 {
			negative = "-"
			secondsFloat = math.Abs(secondsFloat)
		}
		seconds := uint64(secondsFloat)
		demical := secondsFloat - float64(seconds)
		var minute, second uint64
		hour := seconds / 3600
		if hour > 838 {
			hour = 838
			minute = 59
			second = 59
			demical = 0
			tc := typeCtx(ctx)
			err = tc.HandleTruncate(errTruncatedWrongValue.GenWithStackByArgs("time", strconv.FormatFloat(secondsFloat, 'f', -1, 64)))
			if err != nil {
				return err
			}
		} else {
			minute = seconds % 3600 / 60
			second = seconds % 60
		}
		secondDemical := float64(second) + demical
		duration, _, err := types.ParseDuration(typeCtx(ctx), fmt.Sprintf("%s%02d:%02d:%s", negative, hour, minute, strconv.FormatFloat(secondDemical, 'f', -1, 64)), b.tp.GetDecimal())
		if err != nil {
			return err
		}
		durations[i] = duration.Duration
	}
	return nil
}

func (b *builtinUTCTimeWithoutArgSig) vectorized() bool {
	return true
}

// vecEvalDuration evals a builtinUTCTimeWithoutArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithoutArgSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return err
	}
	res, _, err := types.ParseDuration(typeCtx(ctx), nowTs.UTC().Format(types.TimeFormat), types.DefaultFsp)
	if err != nil {
		return err
	}
	result.ResizeGoDuration(n, false)
	d64s := result.GoDurations()
	for i := range n {
		d64s[i] = res.Duration
	}
	return nil
}
