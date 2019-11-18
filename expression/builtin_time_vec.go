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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"math"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinMonthSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := 0; i < input.NumRows(); i++ {
		if result.IsNull(i) {
			continue
		}
		if ds[i].IsZero() {
			if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() {
				if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(ds[i].String())); err != nil {
					return err
				}
				result.SetNull(i, true)
				continue
			}
			i64s[i] = 0
			continue
		}
		i64s[i] = int64(ds[i].Time.Month())
	}
	return nil
}

func (b *builtinMonthSig) vectorized() bool {
	return true
}

func (b *builtinYearSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := 0; i < input.NumRows(); i++ {
		if result.IsNull(i) {
			continue
		}
		if ds[i].IsZero() {
			if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() {
				if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(ds[i].String())); err != nil {
					return err
				}
				result.SetNull(i, true)
				continue
			}
			i64s[i] = 0
			continue
		}
		i64s[i] = int64(ds[i].Time.Year())
	}
	return nil
}

func (b *builtinYearSig) vectorized() bool {
	return true
}

func (b *builtinDateSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalTime(b.ctx, input, result); err != nil {
		return err
	}
	times := result.Times()
	for i := 0; i < len(times); i++ {
		if result.IsNull(i) {
			continue
		}
		if times[i].IsZero() {
			if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(times[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
		} else {
			times[i].Time = types.FromDate(times[i].Time.Year(), times[i].Time.Month(), times[i].Time.Day(), 0, 0, 0, 0)
			times[i].Type = mysql.TypeDate
		}
	}
	return nil
}

func (b *builtinDateSig) vectorized() bool {
	return true
}

func (b *builtinFromUnixTime2ArgSig) vectorized() bool {
	return true
}

func (b *builtinFromUnixTime2ArgSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err = b.args[0].VecEvalDecimal(b.ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err = b.args[1].VecEvalString(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	ds := buf1.Decimals()
	fsp := int8(b.tp.Decimal)
	for i := 0; i < n; i++ {
		if buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}
		t, isNull, err := evalFromUnixTime(b.ctx, fsp, &ds[i])
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

func (b *builtinSysDateWithoutFspSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	loc := b.ctx.GetSessionVars().Location()
	now := time.Now().In(loc)

	result.ResizeTime(n, false)
	times := result.Times()
	t, err := convertTimeToMysqlTime(now, 0, types.ModeHalfEven)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		times[i] = t
	}
	return nil
}

func (b *builtinExtractDatetimeSig) vectorized() bool {
	return false
}

func (b *builtinExtractDatetimeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinAddDateIntIntSig) vectorized() bool {
	return false
}

func (b *builtinAddDateIntIntSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinAddDateDatetimeDecimalSig) vectorized() bool {
	return false
}

func (b *builtinAddDateDatetimeDecimalSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinDayNameSig) vectorized() bool {
	return true
}

func (b *builtinDayNameSig) vecEvalIndex(input *chunk.Chunk, apply func(i, res int), applyNull func(i int)) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}

	ds := buf.Times()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			applyNull(i)
			continue
		}
		if ds[i].InvalidZero() {
			if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(ds[i].String())); err != nil {
				return err
			}
			applyNull(i)
			continue
		}
		// Monday is 0, ... Sunday = 6 in MySQL
		// but in go, Sunday is 0, ... Saturday is 6
		// we will do a conversion.
		res := (int(ds[i].Time.Weekday()) + 6) % 7
		apply(i, res)
	}
	return nil
}

// evalString evals a builtinDayNameSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_dayname
func (b *builtinDayNameSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveString(n)

	return b.vecEvalIndex(input,
		func(i, res int) {
			result.AppendString(types.WeekdayNames[res])
		},
		func(i int) {
			result.AppendNull()
		},
	)
}

func (b *builtinDayNameSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeFloat64(n, false)
	f64s := result.Float64s()

	return b.vecEvalIndex(input,
		func(i, res int) {
			f64s[i] = float64(res)
		},
		func(i int) {
			result.SetNull(i, true)
		},
	)
}

func (b *builtinDayNameSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()

	return b.vecEvalIndex(input,
		func(i, res int) {
			i64s[i] = int64(res)
		},
		func(i int) {
			result.SetNull(i, true)
		},
	)
}

func (b *builtinWeekDaySig) vectorized() bool {
	return true
}

func (b *builtinWeekDaySig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := 0; i < input.NumRows(); i++ {
		if result.IsNull(i) {
			continue
		}
		if ds[i].IsZero() {
			if err = handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(ds[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = int64((ds[i].Time.Weekday() + 6) % 7)
	}
	return nil
}

func (b *builtinTimeFormatSig) vectorized() bool {
	return true
}

func (b *builtinTimeFormatSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDuration, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDuration(b.ctx, input, buf); err != nil {
		return err
	}

	buf1, err1 := b.bufAllocator.get(types.ETString, n)
	if err1 != nil {
		return err1
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}
		res, err := b.formatTime(b.ctx, buf.GetDuration(i, 0), buf1.GetString(i))
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
func (b *builtinUTCTimeWithArgSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}
	nowTs, err := getStmtTimestamp(b.ctx)
	if err != nil {
		return err
	}
	utc := nowTs.UTC().Format(types.TimeFSPFormat)
	stmtCtx := b.ctx.GetSessionVars().StmtCtx
	result.ResizeGoDuration(n, false)
	d64s := result.GoDurations()
	i64s := buf.Int64s()
	result.MergeNulls(buf)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		fsp := i64s[i]
		if fsp > int64(types.MaxFsp) {
			return errors.Errorf("Too-big precision %v specified for 'utc_time'. Maximum is %v.", fsp, types.MaxFsp)
		}
		if fsp < int64(types.MinFsp) {
			return errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
		}
		res, err := types.ParseDuration(stmtCtx, utc, int8(fsp))
		if err != nil {
			return err
		}
		d64s[i] = res.Duration
	}
	return nil
}

func (b *builtinSubDateIntIntSig) vectorized() bool {
	return false
}

func (b *builtinSubDateIntIntSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinUnixTimestampCurrentSig) vectorized() bool {
	return false
}

func (b *builtinUnixTimestampCurrentSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDateIntRealSig) vectorized() bool {
	return false
}

func (b *builtinSubDateIntRealSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinYearWeekWithoutModeSig) vectorized() bool {
	return true
}

// evalInt evals YEARWEEK(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithoutModeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		date := ds[i]
		if date.InvalidZero() {
			if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		year, week := date.Time.YearWeek(0)
		i64s[i] = int64(week + year*100)
		if i64s[i] < 0 {
			i64s[i] = int64(math.MaxUint32)
		}
	}
	return nil
}

func (b *builtinAddDateStringRealSig) vectorized() bool {
	return false
}

func (b *builtinAddDateStringRealSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDateStringDecimalSig) vectorized() bool {
	return false
}

func (b *builtinSubDateStringDecimalSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinPeriodDiffSig) vectorized() bool {
	return true
}

// evalInt evals PERIOD_DIFF(P1,P2).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-diff
func (b *builtinPeriodDiffSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	i64s := result.Int64s()
	periods := buf.Int64s()
	result.MergeNulls(buf)
	for i := 0; i < n; i++ {
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

func (b *builtinNowWithArgSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	bufFsp, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(bufFsp)
	if err = b.args[0].VecEvalInt(b.ctx, input, bufFsp); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	times := result.Times()
	fsps := bufFsp.Int64s()

	for i := 0; i < n; i++ {
		fsp := int8(0)
		if !bufFsp.IsNull(i) {
			if fsps[i] > int64(types.MaxFsp) {
				return errors.Errorf("Too-big precision %v specified for 'now'. Maximum is %v.", fsps[i], types.MaxFsp)
			}
			if fsps[i] < int64(types.MinFsp) {
				return errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsps[i])
			}
			fsp = int8(fsps[i])
		}

		t, isNull, err := evalNowWithFsp(b.ctx, fsp)
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

func (b *builtinSubDateStringRealSig) vectorized() bool {
	return false
}

func (b *builtinSubDateStringRealSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDateDatetimeIntSig) vectorized() bool {
	return false
}

func (b *builtinSubDateDatetimeIntSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDateDurationDecimalSig) vectorized() bool {
	return false
}

func (b *builtinSubDateDurationDecimalSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGetFormatSig) vectorized() bool {
	return true
}

// vecEvalString evals a builtinGetFormatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_get-format
func (b *builtinGetFormatSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err = b.args[0].VecEvalString(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err = b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
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
	return false
}

func (b *builtinLastDaySig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinAddDateStringDecimalSig) vectorized() bool {
	return false
}

func (b *builtinAddDateStringDecimalSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinAddDateDatetimeRealSig) vectorized() bool {
	return false
}

func (b *builtinAddDateDatetimeRealSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubTimeDurationNullSig) vectorized() bool {
	return false
}

func (b *builtinSubTimeDurationNullSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinStrToDateDateSig) vectorized() bool {
	return false
}

func (b *builtinStrToDateDateSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinAddDateStringIntSig) vectorized() bool {
	return false
}

func (b *builtinAddDateStringIntSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSysDateWithFspSig) vectorized() bool {
	return true
}

func (b *builtinSysDateWithFspSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	loc := b.ctx.GetSessionVars().Location()
	now := time.Now().In(loc)

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	times := result.Times()
	ds := buf.Int64s()

	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		t, err := convertTimeToMysqlTime(now, int8(ds[i]), types.ModeHalfEven)
		if err != nil {
			return err
		}
		times[i] = t
	}
	return nil
}

func (b *builtinAddDateDurationIntSig) vectorized() bool {
	return false
}

func (b *builtinAddDateDurationIntSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDateIntStringSig) vectorized() bool {
	return false
}

func (b *builtinSubDateIntStringSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinTidbParseTsoSig) vectorized() bool {
	return false
}

func (b *builtinTidbParseTsoSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinAddDateDurationStringSig) vectorized() bool {
	return false
}

func (b *builtinAddDateDurationStringSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubStringAndDurationSig) vectorized() bool {
	return false
}

func (b *builtinSubStringAndDurationSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinFromDaysSig) vectorized() bool {
	return true
}

func (b *builtinFromDaysSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	ts := result.Times()
	i64s := buf.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		ts[i] = types.TimeFromDays(i64s[i])
	}
	return nil
}

func (b *builtinMicroSecondSig) vectorized() bool {
	return false
}

func (b *builtinMicroSecondSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDatetimeAndStringSig) vectorized() bool {
	return false
}

func (b *builtinSubDatetimeAndStringSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDateStringStringSig) vectorized() bool {
	return false
}

func (b *builtinSubDateStringStringSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinQuarterSig) vectorized() bool {
	return true
}

// evalInt evals QUARTER(date).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_quarter
func (b *builtinQuarterSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		date := ds[i]
		if date.IsZero() {
			if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = int64((date.Time.Month() + 2) / 3)
	}
	return nil
}

func (b *builtinWeekWithModeSig) vectorized() bool {
	return false
}

func (b *builtinWeekWithModeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinExtractDurationSig) vectorized() bool {
	return false
}

func (b *builtinExtractDurationSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinStrToDateDurationSig) vectorized() bool {
	return false
}

func (b *builtinStrToDateDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubTimeDateTimeNullSig) vectorized() bool {
	return false
}

func (b *builtinSubTimeDateTimeNullSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinToSecondsSig) vectorized() bool {
	return true
}

// evalInt evals a builtinToSecondsSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-seconds
func (b *builtinToSecondsSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		arg := ds[i]
		ret := types.TimestampDiff("SECOND", types.ZeroDate, arg)
		if ret == 0 {
			if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = ret
	}
	return nil
}

func (b *builtinSubDurationAndStringSig) vectorized() bool {
	return false
}

func (b *builtinSubDurationAndStringSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDateAndStringSig) vectorized() bool {
	return false
}

func (b *builtinSubDateAndStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinMinuteSig) vectorized() bool {
	return true
}

func (b *builtinMinuteSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDuration, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(buf.GetDuration(i, int(types.UnspecifiedFsp)).Minute())
	}
	return nil
}

func (b *builtinSecondSig) vectorized() bool {
	return true
}

func (b *builtinSecondSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDuration, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(buf.GetDuration(i, int(types.UnspecifiedFsp)).Second())
	}
	return nil
}

func (b *builtinNowWithoutArgSig) vectorized() bool {
	return true
}

func (b *builtinNowWithoutArgSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	nowTs, isNull, err := evalNowWithFsp(b.ctx, int8(0))
	if err != nil {
		return err
	}
	if isNull {
		result.ResizeTime(n, true)
		return nil
	}
	result.ResizeTime(n, false)
	times := result.Times()
	for i := 0; i < n; i++ {
		times[i] = nowTs
	}
	return nil
}

func (b *builtinTimestampLiteralSig) vectorized() bool {
	return false
}

func (b *builtinTimestampLiteralSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinAddDateIntDecimalSig) vectorized() bool {
	return false
}

func (b *builtinAddDateIntDecimalSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinMakeDateSig) vectorized() bool {
	return true
}

func (b *builtinMakeDateSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf1, buf2)

	times := result.Times()
	years := buf1.Int64s()
	days := buf2.Int64s()

	for i := 0; i < n; i++ {
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
		startTime := types.Time{
			Time: types.FromDate(int(years[i]), 1, 1, 0, 0, 0, 0),
			Type: mysql.TypeDate,
			Fsp:  0,
		}
		retTimestamp := types.TimestampDiff("DAY", types.ZeroDate, startTime)
		if retTimestamp == 0 {
			if err = handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(startTime.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		ret := types.TimeFromDays(retTimestamp + days[i] - 1)
		if ret.IsZero() || ret.Time.Year() > 9999 {
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

func (b *builtinWeekOfYearSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		if err = handleInvalidTimeError(b.ctx, err); err != nil {
			return err
		}
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if ds[i].IsZero() {
			if err = handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(ds[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		week := ds[i].Time.Week(3)
		i64s[i] = int64(week)
	}
	return nil
}

func (b *builtinUTCTimestampWithArgSig) vectorized() bool {
	return true
}

// vecEvalTime evals UTC_TIMESTAMP(fsp).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-timestamp
func (b *builtinUTCTimestampWithArgSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
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
	t64s := result.Times()
	i64s := buf.Int64s()
	result.MergeNulls(buf)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		fsp := i64s[i]
		if fsp > int64(types.MaxFsp) {
			return errors.Errorf("Too-big precision %v specified for 'utc_timestamp'. Maximum is %v.", fsp, types.MaxFsp)
		}
		if fsp < int64(types.MinFsp) {
			return errors.Errorf("Invalid negative %d specified, must in [0, 6].", fsp)
		}
		res, isNull, err := evalUTCTimestampWithFsp(b.ctx, int8(fsp))
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

func (b *builtinAddDateIntRealSig) vectorized() bool {
	return false
}

func (b *builtinAddDateIntRealSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDurationAndDurationSig) vectorized() bool {
	return false
}

func (b *builtinSubDurationAndDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinTimeToSecSig) vectorized() bool {
	return true
}

// evalInt evals TIME_TO_SEC(time).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_time-to-sec
func (b *builtinTimeToSecSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDuration, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	fsp := b.args[0].GetType().Decimal
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		var sign int
		duration := buf.GetDuration(i, int(fsp))
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

func (b *builtinStrToDateDatetimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	dateBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dateBuf)
	if err = b.args[0].VecEvalString(b.ctx, input, dateBuf); err != nil {
		return err
	}

	formatBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(formatBuf)
	if err = b.args[1].VecEvalString(b.ctx, input, formatBuf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(dateBuf, formatBuf)
	times := result.Times()
	sc := b.ctx.GetSessionVars().StmtCtx
	hasNoZeroDateMode := b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode()
	fsp := int8(b.tp.Decimal)

	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		var t types.Time
		succ := t.StrToDate(sc, dateBuf.GetString(i), formatBuf.GetString(i))
		if !succ {
			if err = handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		if hasNoZeroDateMode && (t.Time.Year() == 0 || t.Time.Month() == 0 || t.Time.Day() == 0) {
			if err = handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(t.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		t.Type, t.Fsp = mysql.TypeDatetime, fsp
		times[i] = t
	}
	return nil
}

func (b *builtinUTCDateSig) vectorized() bool {
	return true
}

func (b *builtinUTCDateSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	nowTs, err := getStmtTimestamp(b.ctx)
	if err != nil {
		return err
	}
	year, month, day := nowTs.UTC().Date()
	utcDate := types.Time{
		Time: types.FromGoTime(time.Date(year, month, day, 0, 0, 0, 0, time.UTC)),
		Type: mysql.TypeDate,
		Fsp:  types.UnspecifiedFsp}

	n := input.NumRows()
	result.ResizeTime(n, false)
	times := result.Times()
	for i := 0; i < n; i++ {
		times[i] = utcDate
	}
	return nil
}

func (b *builtinWeekWithoutModeSig) vectorized() bool {
	return false
}

func (b *builtinWeekWithoutModeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinUnixTimestampDecSig) vectorized() bool {
	return false
}

func (b *builtinUnixTimestampDecSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinPeriodAddSig) vectorized() bool {
	return true
}

// evalInt evals PERIOD_ADD(P,N).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_period-add
func (b *builtinPeriodAddSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}
	i64s := result.Int64s()
	result.MergeNulls(buf)
	ns := buf.Int64s()
	for i := 0; i < n; i++ {
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

// evalString evals a builtinTimestampAddSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_timestampadd
func (b *builtinTimestampAddSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	buf2, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	if err := b.args[2].VecEvalTime(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ReserveString(n)
	nums := buf1.Int64s()
	ds := buf2.Times()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf1.IsNull(i) || buf2.IsNull(i) {
			result.AppendNull()
			continue
		}

		unit := buf.GetString(i)
		v := nums[i]
		arg := ds[i]

		tm1, err := arg.Time.GoTime(time.Local)
		if err != nil {
			return err
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
			return types.ErrInvalidTimeFormat.GenWithStackByArgs(unit)
		}
		r := types.Time{Time: types.FromGoTime(tb), Type: b.resolveType(arg.Type, unit), Fsp: fsp}
		if err = r.Check(b.ctx.GetSessionVars().StmtCtx); err != nil {
			if err = handleInvalidTimeError(b.ctx, err); err != nil {
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

// evalInt evals a builtinToDaysSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_to-days
func (b *builtinToDaysSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		arg := ds[i]
		ret := types.TimestampDiff("DAY", types.ZeroDate, arg)
		if ret == 0 {
			if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(arg.String())); err != nil {
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
	return false
}

func (b *builtinDateFormatSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinHourSig) vectorized() bool {
	return true
}

func (b *builtinHourSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDuration, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.GoDurations()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(ds[i].Hours())
	}
	return nil
}

func (b *builtinAddDateDurationRealSig) vectorized() bool {
	return false
}

func (b *builtinAddDateDurationRealSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSecToTimeSig) vectorized() bool {
	return false
}

func (b *builtinSecToTimeSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubStringAndStringSig) vectorized() bool {
	return true
}

// evalString evals a builtinSubStringAndStringSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subtime
func (b *builtinSubStringAndStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	var (
		s, arg0 string
		arg1    types.Duration
	)

	n := input.NumRows()
	buf0, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err = b.args[0].VecEvalString(b.ctx, input, buf0); err != nil {
		return err
	}

	arg1Type := b.args[1].GetType()
	buf1, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err = b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ReserveString(n)
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < n; i++ {
		if buf0.IsNull(i) || buf1.IsNull(i) || mysql.HasBinaryFlag(arg1Type.Flag) {
			result.AppendNull()
			continue
		}

		arg0 = buf0.GetString(i)
		s = buf1.GetString(i)

		arg1, err = types.ParseDuration(sc, s, getFsp4TimeAddSub(s))
		if err != nil {
			if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
				sc.AppendWarning(err)
				result.AppendNull()
				continue
			}
			return err
		}
		if isDuration(arg0) {
			res, err := strDurationSubDuration(sc, arg0, arg1)
			if err != nil {
				if terror.ErrorEqual(err, types.ErrTruncatedWrongVal) {
					sc.AppendWarning(err)
					result.AppendNull()
					continue
				}
				return err
			}
			result.AppendString(res)
			continue
		}
		res, err := strDatetimeSubDuration(sc, arg0, arg1)
		if err != nil {
			return err
		}
		result.AppendString(res)
	}

	return nil
}

func (b *builtinUTCTimeWithoutArgSig) vectorized() bool {
	return true
}

// vecEvalDuration evals a builtinUTCTimeWithoutArgSig.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_utc-time
func (b *builtinUTCTimeWithoutArgSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	nowTs, err := getStmtTimestamp(b.ctx)
	if err != nil {
		return err
	}
	res, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, nowTs.UTC().Format(types.TimeFormat), types.DefaultFsp)
	if err != nil {
		return err
	}
	result.ResizeGoDuration(n, false)
	d64s := result.GoDurations()
	for i := 0; i < n; i++ {
		d64s[i] = res.Duration
	}
	return nil
}

func (b *builtinSubDateIntDecimalSig) vectorized() bool {
	return false
}

func (b *builtinSubDateIntDecimalSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinDateDiffSig) vectorized() bool {
	return false
}

func (b *builtinDateDiffSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCurrentDateSig) vectorized() bool {
	return true
}

func (b *builtinCurrentDateSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	nowTs, err := getStmtTimestamp(b.ctx)
	if err != nil {
		return err
	}

	tz := b.ctx.GetSessionVars().Location()
	year, month, day := nowTs.In(tz).Date()
	timeValue := types.Time{
		Time: types.FromDate(year, int(month), day, 0, 0, 0, 0),
		Type: mysql.TypeDate,
		Fsp:  0}

	n := input.NumRows()
	result.ResizeTime(n, false)
	times := result.Times()
	for i := 0; i < n; i++ {
		times[i] = timeValue
	}
	return nil
}

func (b *builtinAddDateStringStringSig) vectorized() bool {
	return false
}

func (b *builtinAddDateStringStringSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinAddDateIntStringSig) vectorized() bool {
	return false
}

func (b *builtinAddDateIntStringSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinAddDateDatetimeStringSig) vectorized() bool {
	return false
}

func (b *builtinAddDateDatetimeStringSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinMakeTimeSig) vectorized() bool {
	return false
}

func (b *builtinMakeTimeSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDateAndDurationSig) vectorized() bool {
	return false
}

func (b *builtinSubDateAndDurationSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinDayOfYearSig) vectorized() bool {
	return true
}

func (b *builtinDayOfYearSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		if err := handleInvalidTimeError(b.ctx, err); err != nil {
			return err
		}
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if ds[i].InvalidZero() {
			if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(ds[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = int64(ds[i].Time.YearDay())
	}
	return nil
}

func (b *builtinFromUnixTime1ArgSig) vectorized() bool {
	return true
}

func (b *builtinFromUnixTime1ArgSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDecimal(b.ctx, input, buf); err != nil {
		return err
	}
	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	ts := result.Times()
	ds := buf.Decimals()
	fsp := int8(b.tp.Decimal)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		t, isNull, err := evalFromUnixTime(b.ctx, fsp, &ds[i])
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

func (b *builtinSubDateDurationIntSig) vectorized() bool {
	return false
}

func (b *builtinSubDateDurationIntSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinYearWeekWithModeSig) vectorized() bool {
	return true
}

// vecEvalInt evals YEARWEEK(date,mode).
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_yearweek
func (b *builtinYearWeekWithModeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	if err := b.args[0].VecEvalTime(b.ctx, input, buf1); err != nil {
		return err
	}
	buf2, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	if err := b.args[1].VecEvalInt(b.ctx, input, buf2); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf1)
	i64s := result.Int64s()
	ds := buf1.Times()
	ms := buf2.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		date := ds[i]
		if date.IsZero() {
			if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(date.String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		mode := int(ms[i])
		if buf2.IsNull(i) {
			mode = 0
		}
		year, week := date.Time.YearWeek(mode)
		i64s[i] = int64(week + year*100)
		if i64s[i] < 0 {
			i64s[i] = int64(math.MaxUint32)
		}
	}
	return nil
}

func (b *builtinTimestampDiffSig) vectorized() bool {
	return false
}

func (b *builtinTimestampDiffSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinUnixTimestampIntSig) vectorized() bool {
	return false
}

func (b *builtinUnixTimestampIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinAddDateDurationDecimalSig) vectorized() bool {
	return false
}

func (b *builtinAddDateDurationDecimalSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDateDatetimeRealSig) vectorized() bool {
	return false
}

func (b *builtinSubDateDatetimeRealSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDateDurationRealSig) vectorized() bool {
	return false
}

func (b *builtinSubDateDurationRealSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCurrentTime0ArgSig) vectorized() bool {
	return true
}

func (b *builtinCurrentTime0ArgSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	nowTs, err := getStmtTimestamp(b.ctx)
	if err != nil {
		return err
	}
	tz := b.ctx.GetSessionVars().Location()
	dur := nowTs.In(tz).Format(types.TimeFormat)
	res, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, dur, types.MinFsp)
	if err != nil {
		return err
	}
	result.ResizeGoDuration(n, false)
	durations := result.GoDurations()
	for i := 0; i < n; i++ {
		durations[i] = res.Duration
	}
	return nil
}

func (b *builtinTimeSig) vectorized() bool {
	return false
}

func (b *builtinTimeSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinAddDateDatetimeIntSig) vectorized() bool {
	return false
}

func (b *builtinAddDateDatetimeIntSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDateStringIntSig) vectorized() bool {
	return false
}

func (b *builtinSubDateStringIntSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinDateLiteralSig) vectorized() bool {
	return false
}

func (b *builtinDateLiteralSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinTimeLiteralSig) vectorized() bool {
	return false
}

func (b *builtinTimeLiteralSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDateDurationStringSig) vectorized() bool {
	return false
}

func (b *builtinSubDateDurationStringSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubTimeStringNullSig) vectorized() bool {
	return false
}

func (b *builtinSubTimeStringNullSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinMonthNameSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	ds := buf.Times()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		mon := ds[i].Time.Month()
		if (ds[i].IsZero() && b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode()) || mon < 0 || mon > len(types.MonthNames) {
			if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(ds[i].String())); err != nil {
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

func (b *builtinSubDateDatetimeStringSig) vectorized() bool {
	return false
}

func (b *builtinSubDateDatetimeStringSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDateDatetimeDecimalSig) vectorized() bool {
	return false
}

func (b *builtinSubDateDatetimeDecimalSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSubDatetimeAndDurationSig) vectorized() bool {
	return false
}

func (b *builtinSubDatetimeAndDurationSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinDayOfWeekSig) vectorized() bool {
	return true
}

func (b *builtinDayOfWeekSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if ds[i].InvalidZero() {
			if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(ds[i].String())); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		i64s[i] = int64(ds[i].Time.Weekday() + 1)
	}
	return nil
}

func (b *builtinCurrentTime1ArgSig) vectorized() bool {
	return true
}

func (b *builtinCurrentTime1ArgSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	nowTs, err := getStmtTimestamp(b.ctx)
	if err != nil {
		return err
	}
	tz := b.ctx.GetSessionVars().Location()
	dur := nowTs.In(tz).Format(types.TimeFSPFormat)
	stmtCtx := b.ctx.GetSessionVars().StmtCtx
	i64s := buf.Int64s()
	result.ResizeGoDuration(n, false)
	durations := result.GoDurations()
	for i := 0; i < n; i++ {
		res, err := types.ParseDuration(stmtCtx, dur, int8(i64s[i]))
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
func (b *builtinUTCTimestampWithoutArgSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	res, isNull, err := evalUTCTimestampWithFsp(b.ctx, types.DefaultFsp)
	if err != nil {
		return err
	}
	if isNull {
		result.ResizeTime(n, true)
		return nil
	}
	result.ResizeTime(n, false)
	t64s := result.Times()
	for i := 0; i < n; i++ {
		t64s[i] = res
	}
	return nil
}

func (b *builtinConvertTzSig) vectorized() bool {
	return false
}

func (b *builtinConvertTzSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinTimestamp1ArgSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
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
	times := result.Times()
	sc := b.ctx.GetSessionVars().StmtCtx
	var tm types.Time
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		s := buf.GetString(i)

		if b.isFloat {
			tm, err = types.ParseTimeFromFloatString(sc, s, mysql.TypeDatetime, types.GetFsp(s))
		} else {
			tm, err = types.ParseTime(sc, s, mysql.TypeDatetime, types.GetFsp(s))
		}
		if err != nil {
			if err = handleInvalidTimeError(b.ctx, err); err != nil {
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

func (b *builtinTimestamp2ArgsSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(b.ctx, input, buf0); err != nil {
		return err
	}

	buf1, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf0, buf1)
	times := result.Times()
	sc := b.ctx.GetSessionVars().StmtCtx
	var tm types.Time
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		arg0 := buf0.GetString(i)
		arg1 := buf1.GetString(i)

		if b.isFloat {
			tm, err = types.ParseTimeFromFloatString(sc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
		} else {
			tm, err = types.ParseTime(sc, arg0, mysql.TypeDatetime, types.GetFsp(arg0))
		}
		if err != nil {
			if err = handleInvalidTimeError(b.ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}

		if !isDuration(arg1) {
			result.SetNull(i, true)
			continue
		}

		duration, err := types.ParseDuration(sc, arg1, types.GetFsp(arg1))
		if err != nil {
			if err = handleInvalidTimeError(b.ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		tmp, err := tm.Add(sc, duration)
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

func (b *builtinDayOfMonthSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	ds := buf.Times()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if ds[i].IsZero() {
			if b.ctx.GetSessionVars().SQLMode.HasNoZeroDateMode() {
				if err := handleInvalidTimeError(b.ctx, types.ErrIncorrectDatetimeValue.GenWithStackByArgs(ds[i].String())); err != nil {
					return err
				}
				result.SetNull(i, true)
				continue
			}
			i64s[i] = 0
			continue
		}
		i64s[i] = int64(ds[i].Time.Day())
	}
	return nil
}

func (b *builtinDayOfMonthSig) vectorized() bool {
	return true
}
