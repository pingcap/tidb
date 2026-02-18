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
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/tikv/client-go/v2/oracle"
)

func (b *builtinMonthSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	for i := range input.NumRows() {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(ds[i].Month())
	}
	return nil
}

func (b *builtinMonthSig) vectorized() bool {
	return true
}

func (b *builtinYearSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	for i := range input.NumRows() {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(ds[i].Year())
	}
	return nil
}

func (b *builtinYearSig) vectorized() bool {
	return true
}

func (b *builtinDateSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalTime(ctx, input, result); err != nil {
		return err
	}
	times := result.Times()
	for i := range times {
		if result.IsNull(i) {
			continue
		}
		if times[i].IsZero() && sqlMode(ctx).HasNoZeroDateMode() {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, times[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		// for issue 59417, should return NULL when month or day is zero and sql_mode contains NO_ZERO_IN_DATE
		if !times[i].IsZero() && times[i].InvalidZero() && sqlMode(ctx).HasNoZeroInDateMode() {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, times[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}

		times[i].SetCoreTime(types.FromDate(times[i].Year(), times[i].Month(), times[i].Day(), 0, 0, 0, 0))
		times[i].SetType(mysql.TypeDate)
	}
	return nil
}

func (b *builtinDateSig) vectorized() bool {
	return true
}

func (b *builtinFromUnixTime2ArgSig) vectorized() bool {
	return true
}

func (b *builtinFromUnixTime2ArgSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err = b.args[0].VecEvalDecimal(ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err = b.args[1].VecEvalString(ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	ds := buf1.Decimals()
	fsp := b.tp.GetDecimal()
	for i := range n {
		if buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		t, isNull, err := evalFromUnixTime(ctx, fsp, &ds[i])
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
			continue
		}
		res, err := t.DateFormat(buf2.GetString(i))
		if err != nil {
			return err
		}
		result.AppendString(res)
	}
	return nil
}

func (b *builtinSysDateWithoutFspSig) vectorized() bool {
	return true
}

func (b *builtinSysDateWithoutFspSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	loc := location(ctx)
	now := time.Now().In(loc)

	result.ResizeTime(n, false)
	times := result.Times()
	t, err := convertTimeToMysqlTime(now, 0, types.ModeHalfUp)
	if err != nil {
		return err
	}
	for i := range n {
		times[i] = t
	}
	return nil
}

func (b *builtinExtractDatetimeFromStringSig) vectorized() bool {
	// TODO: to fix https://github.com/pingcap/tidb/issues/9716 in vectorized evaluation.
	return false
}

func (b *builtinExtractDatetimeFromStringSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	if err := b.args[1].VecEvalTime(ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	ds := buf1.Times()
	result.MergeNulls(buf, buf1)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		res, err := types.ExtractDatetimeNum(&ds[i], buf.GetString(i))
		if err != nil {
			return err
		}
		i64s[i] = res
	}
	return nil
}

func (b *builtinDayNameSig) vectorized() bool {
	return true
}

func (b *builtinDayNameSig) vecEvalIndex(ctx EvalContext, input *chunk.Chunk, apply func(i int, res int), applyNull func(i int)) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}

	ds := buf.Times()
	for i := range n {
		if buf.IsNull(i) {
			applyNull(i)
			continue
		}
		if ds[i].InvalidZero() {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, ds[i].String())); err != nil {
				return err
			}
			applyNull(i)
			continue
		}
		// Monday is 0, ... Sunday = 6 in MySQL
		// but in go, Sunday is 0, ... Saturday is 6
		// we will do a conversion.
		res := (int(ds[i].Weekday()) + 6) % 7
		apply(i, res)
	}
	return nil
}

// vecEvalString evals a builtinDayNameSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayname
func (b *builtinDayNameSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveString(n)

	return b.vecEvalIndex(ctx, input, func(i, res int) {
		result.AppendString(types.WeekdayNames[res])
	}, func(i int) {
		result.AppendNull()
	})
}

func (b *builtinDayNameSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeFloat64(n, false)
	f64s := result.Float64s()

	return b.vecEvalIndex(ctx, input, func(i, res int) {
		f64s[i] = float64(res)
	}, func(i int) {
		result.SetNull(i, true)
	})
}

func (b *builtinDayNameSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()

	return b.vecEvalIndex(ctx, input, func(i, res int) {
		i64s[i] = int64(res)
	}, func(i int) {
		result.SetNull(i, true)
	})
}

func (b *builtinWeekDaySig) vectorized() bool {
	return true
}

func (b *builtinWeekDaySig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	for i := range input.NumRows() {
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
		i64s[i] = int64((ds[i].Weekday() + 6) % 7)
	}
	return nil
}

func (b *builtinTimeFormatSig) vectorized() bool {
	return true
}

func (b *builtinTimeFormatSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		// If err != nil, then dur is ZeroDuration, outputs 00:00:00
		// in this case which follows the behavior of mysql.
		// Use the non-vectorized method to handle this kind of error.
		return vecEvalStringByRows(b, ctx, input, result)
	}

	buf1, err1 := b.bufAllocator.get()
	if err1 != nil {
		return err1
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := range n {
		if buf.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}
		res, err := b.formatTime(buf.GetDuration(i, 0), buf1.GetString(i))
		if err != nil {
			return err
		}
		result.AppendString(res)
	}
	return nil
}

func (b *builtinUTCTimeWithArgSig) vectorized() bool {
	return true
}

// vecEvalDuration evals a builtinUTCTimeWithArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithArgSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	utc := nowTs.UTC().Format(types.TimeFSPFormat)
	tc := typeCtx(ctx)
	result.ResizeGoDuration(n, false)
	d64s := result.GoDurations()
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
			return types.ErrTooBigPrecision.GenWithStackByArgs(fsp, "utc_time", types.MaxFsp)
		}
		res, _, err := types.ParseDuration(tc, utc, int(fsp))
		if err != nil {
			return err
		}
		d64s[i] = res.Duration
	}
	return nil
}

func (b *builtinUnixTimestampCurrentSig) vectorized() bool {
	return true
}

func (b *builtinUnixTimestampCurrentSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	nowTs, err := getStmtTimestamp(ctx)
	if err != nil {
		return err
	}
	dec, err := goTimeToMysqlUnixTimestamp(nowTs, 1)
	if err != nil {
		return err
	}
	intVal, err := dec.ToInt()
	if !terror.ErrorEqual(err, types.ErrTruncated) {
		terror.Log(err)
	}
	n := input.NumRows()
	result.ResizeInt64(n, false)
	intRes := result.Int64s()
	for i := range n {
		intRes[i] = intVal
	}
	return nil
}

func (b *builtinYearWeekWithoutModeSig) vectorized() bool {
	return true
}

// vecEvalInt evals YEARWEEK(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithoutModeSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		date := ds[i]
		if date.InvalidZero() {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, date.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		year, week := date.YearWeek(0)
		i64s[i] = int64(week + year*100)
		if i64s[i] < 0 {
			i64s[i] = int64(math.MaxUint32)
		}
	}
	return nil
}

func (b *builtinPeriodDiffSig) vectorized() bool {
	return true
}

// vecEvalInt evals PERIOD_DIFF(P1,P2).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-diff
func (b *builtinPeriodDiffSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	periods := buf.Int64s()
	result.MergeNulls(buf)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if !validPeriod(i64s[i]) || !validPeriod(periods[i]) {
			return errIncorrectArgs.GenWithStackByArgs("period_diff")
		}
		i64s[i] = int64(period2Month(uint64(i64s[i])) - period2Month(uint64(periods[i])))
	}
	return nil
}

func (b *builtinNowWithArgSig) vectorized() bool {
	return true
}

func (b *builtinNowWithArgSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufFsp, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufFsp)
	if err = b.args[0].VecEvalInt(ctx, input, bufFsp); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	times := result.Times()
	fsps := bufFsp.Int64s()

	for i := range n {
		fsp := 0
		if !bufFsp.IsNull(i) {
			if fsps[i] > int64(math.MaxInt32) || fsps[i] < int64(types.MinFsp) {
				return types.ErrSyntax.GenWithStack(util.SyntaxErrorPrefix)
			}
			if fsps[i] > int64(types.MaxFsp) {
				return types.ErrTooBigPrecision.GenWithStackByArgs(fsps[i], "now", types.MaxFsp)
			}
			fsp = int(fsps[i])
		}

		t, isNull, err := evalNowWithFsp(ctx, fsp)
		if err != nil {
			return err
		}
		if isNull {
			result.SetNull(i, true)
			continue
		}

		times[i] = t
	}
	return nil
}

func (b *builtinGetFormatSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinGetFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_get-format
func (b *builtinGetFormatSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err = b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err = b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := range n {
		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}
		format := buf0.GetString(i)
		location := buf1.GetString(i)
		res := b.getFormat(format, location)
		result.AppendString(res)
	}
	return nil
}

func (b *builtinGetFormatSig) getFormat(format, location string) string {
	// for Issue 59420, location should be case insensitive
	location = strings.ToUpper(location)

	res := ""
	switch format {
	case dateFormat:
		switch location {
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
		switch location {
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
		switch location {
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
	return res
}

func (b *builtinLastDaySig) vectorized() bool {
	return true
}

func (b *builtinLastDaySig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalTime(ctx, input, result); err != nil {
		return err
	}
	times := result.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		tm := times[i]
		year, month := tm.Year(), tm.Month()
		if tm.Month() == 0 || (tm.Day() == 0 && sqlMode(ctx).HasNoZeroDateMode()) {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, times[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		lastDay := types.GetLastDay(year, month)
		times[i] = types.NewTime(types.FromDate(year, month, lastDay, 0, 0, 0, 0), mysql.TypeDate, types.DefaultFsp)
	}
	return nil
}

func (b *builtinStrToDateDateSig) vectorized() bool {
	return true
}

func (b *builtinStrToDateDateSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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

	result.ResizeTime(n, false)
	result.MergeNulls(bufStrings, bufFormats)
	times := result.Times()
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
		if sqlMode(ctx).HasNoZeroDateMode() && (t.Year() == 0 || t.Month() == 0 || t.Day() == 0) {
			if err := handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, t.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		t.SetType(mysql.TypeDate)
		t.SetFsp(types.MinFsp)
		times[i] = t
	}
	return nil
}

func (b *builtinSysDateWithFspSig) vectorized() bool {
	return true
}

func (b *builtinSysDateWithFspSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	loc := location(ctx)
	now := time.Now().In(loc)

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	times := result.Times()
	ds := buf.Int64s()

	for i := range n {
		if result.IsNull(i) {
			continue
		}
		t, err := convertTimeToMysqlTime(now, int(ds[i]), types.ModeHalfUp)
		if err != nil {
			return err
		}
		times[i] = t
	}
	return nil
}

func (b *builtinTidbParseTsoSig) vectorized() bool {
	return true
}

func (b *builtinTidbParseTsoSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}
	args := buf.Int64s()
	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	times := result.Times()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if args[i] <= 0 {
			result.SetNull(i, true)
			continue
		}
		t := oracle.GetTimeFromTS(uint64(args[i]))
		r := types.NewTime(types.FromGoTime(t), mysql.TypeDatetime, types.MaxFsp)
		if err := r.ConvertTimeZone(time.Local, location(ctx)); err != nil {
			return err
		}
		times[i] = r
	}
	return nil
}

func (b *builtinTiDBBoundedStalenessSig) vectorized() bool {
	return true
}

func (b *builtinTiDBBoundedStalenessSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	store, err := b.GetKVStore(ctx)
	if err != nil {
		return err
	}

	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}

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
	args0 := buf0.Times()
	args1 := buf1.Times()
	timeZone := getTimeZone(ctx)
	minSafeTime := GetStmtMinSafeTime(vars.StmtCtx, store, timeZone)
	result.ResizeTime(n, false)
	result.MergeNulls(buf0, buf1)
	times := result.Times()
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
		minTime, err := args0[i].GoTime(timeZone)
		if err != nil {
			return err
		}
		maxTime, err := args1[i].GoTime(timeZone)
		if err != nil {
			return err
		}
		if minTime.After(maxTime) {
			result.SetNull(i, true)
			continue
		}
		// Because the minimum unit of a TSO is millisecond, so we only need fsp to be 3.
		times[i] = types.NewTime(types.FromGoTime(calAppropriateTime(minTime, maxTime, minSafeTime)), mysql.TypeDatetime, 3)
	}
	return nil
}

func (b *builtinFromDaysSig) vectorized() bool {
	return true
}

func (b *builtinFromDaysSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	ts := result.Times()
	i64s := buf.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		ts[i] = types.TimeFromDays(i64s[i])
	}
	return nil
}

func (b *builtinMicroSecondSig) vectorized() bool {
	return true
}

func (b *builtinMicroSecondSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		i64s[i] = int64(buf.GetDuration(i, int(types.UnspecifiedFsp)).MicroSecond())
	}
	return nil
}

func (b *builtinQuarterSig) vectorized() bool {
	return true
}

// vecEvalInt evals QUARTER(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_quarter
func (b *builtinQuarterSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
		date := ds[i]
		i64s[i] = int64((date.Month() + 2) / 3)
	}
	return nil
}

func (b *builtinWeekWithModeSig) vectorized() bool {
	return true
}

func (b *builtinWeekWithModeSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	if err := b.args[0].VecEvalTime(ctx, input, buf1); err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)

	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	if err := b.args[1].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)

	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	ds := buf1.Times()
	ms := buf2.Int64s()
	for i := range n {
		if buf1.IsNull(i) {
			result.SetNull(i, true)
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
		if buf2.IsNull(i) {
			result.SetNull(i, true)
			continue
		}
		mode := int(ms[i])
		week := date.Week(mode)
		i64s[i] = int64(week)
	}
	return nil
}
