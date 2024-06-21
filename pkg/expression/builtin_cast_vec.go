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
	"errors"
	"math"
	"strconv"
	"strings"
	gotime "time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func (b *builtinCastIntAsDurationSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeGoDuration(n, false)
	result.MergeNulls(buf)
	i64s := buf.Int64s()
	ds := result.GoDurations()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		dur, err := types.NumberToDuration(i64s[i], b.tp.GetDecimal())
		if err != nil {
			if types.ErrOverflow.Equal(err) || types.ErrTruncatedWrongVal.Equal(err) {
				ec := errCtx(ctx)
				err = ec.HandleError(err)
			}
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		ds[i] = dur.Duration
	}
	return nil
}

func (*builtinCastIntAsDurationSig) vectorized() bool {
	return true
}

func (b *builtinCastIntAsIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}
	if b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) {
		i64s := result.Int64s()
		// the null array of result is set by its child args[0],
		// so we can skip it here to make this loop simpler to improve its performance.
		for i := range i64s {
			if i64s[i] < 0 {
				i64s[i] = 0
			}
		}
	}
	return nil
}

func (*builtinCastIntAsIntSig) vectorized() bool {
	return true
}

func (b *builtinCastIntAsRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeFloat64(n, false)
	result.MergeNulls(buf)

	i64s := buf.Int64s()
	rs := result.Float64s()

	hasUnsignedFlag0 := mysql.HasUnsignedFlag(b.tp.GetFlag())
	hasUnsignedFlag1 := mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag())

	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if !hasUnsignedFlag0 && !hasUnsignedFlag1 {
			rs[i] = float64(i64s[i])
		} else if b.inUnion && !hasUnsignedFlag1 && i64s[i] < 0 {
			// Round up to 0 if the value is negative but the expression eval type is unsigned in `UNION` statement
			// NOTE: the following expressions are equal (so choose the more efficient one):
			// `b.inUnion && hasUnsignedFlag0 && !hasUnsignedFlag1 && i64s[i] < 0`
			// `b.inUnion && !hasUnsignedFlag1 && i64s[i] < 0`
			rs[i] = 0
		} else {
			// recall that, int to float is different from uint to float
			rs[i] = float64(uint64(i64s[i]))
		}
	}
	return nil
}

func (*builtinCastIntAsRealSig) vectorized() bool {
	return true
}

func (b *builtinCastRealAsRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	f64s := result.Float64s()
	conditionUnionAndUnsigned := b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag())
	if !conditionUnionAndUnsigned {
		return nil
	}
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if f64s[i] < 0 {
			f64s[i] = 0
		}
	}
	return nil
}

func (*builtinCastRealAsRealSig) vectorized() bool {
	return true
}

func (*builtinCastTimeAsJSONSig) vectorized() bool {
	return true
}

func (b *builtinCastTimeAsJSONSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveJSON(n)
	tms := buf.Times()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		tp := tms[i].Type()
		if tp == mysql.TypeDatetime || tp == mysql.TypeTimestamp {
			tms[i].SetFsp(types.MaxFsp)
		}
		result.AppendJSON(types.CreateBinaryJSON(tms[i]))
	}
	return nil
}

func (*builtinCastRealAsStringSig) vectorized() bool {
	return true
}

func (b *builtinCastRealAsStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}

	bits := 64
	if b.args[0].GetType(ctx).GetType() == mysql.TypeFloat {
		// b.args[0].EvalReal() casts the value from float32 to float64, for example:
		// float32(208.867) is cast to float64(208.86700439)
		// If we strconv.FormatFloat the value with 64bits, the result is incorrect!
		bits = 32
	}

	var isNull bool
	var res string
	f64s := buf.Float64s()
	result.ReserveString(n)
	tc := typeCtx(ctx)
	for i, v := range f64s {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		res, err = types.ProduceStrWithSpecifiedTp(strconv.FormatFloat(v, 'f', -1, bits), b.tp, tc, false)
		if err != nil {
			return err
		}
		res, isNull, err = padZeroForBinaryType(res, b.tp, ctx)
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
			continue
		}
		result.AppendString(res)
	}
	return nil
}

func (*builtinCastDecimalAsStringSig) vectorized() bool {
	return true
}

func (b *builtinCastDecimalAsStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(ctx, input, buf); err != nil {
		return err
	}

	tc := typeCtx(ctx)
	vas := buf.Decimals()
	result.ReserveString(n)
	for i, v := range vas {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		res, e := types.ProduceStrWithSpecifiedTp(string(v.ToString()), b.tp, tc, false)
		if e != nil {
			return e
		}
		str, b, e1 := padZeroForBinaryType(res, b.tp, ctx)
		if e1 != nil {
			return e1
		}
		if b {
			result.AppendNull()
			continue
		}
		result.AppendString(str)
	}
	return nil
}

func (*builtinCastTimeAsDecimalSig) vectorized() bool {
	return true
}

func (b *builtinCastTimeAsDecimalSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)
	times := buf.Times()
	decs := result.Decimals()
	tc, ec := typeCtx(ctx), errCtx(ctx)
	dec := new(types.MyDecimal)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		*dec = types.MyDecimal{}
		times[i].FillNumber(dec)
		dec, err = types.ProduceDecWithSpecifiedTp(tc, dec, b.tp)
		if err = ec.HandleError(err); err != nil {
			return err
		}
		decs[i] = *dec
	}
	return nil
}

func (*builtinCastDurationAsIntSig) vectorized() bool {
	return true
}

func (b *builtinCastDurationAsIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	var duration types.Duration
	ds := buf.GoDurations()
	fsp := b.args[0].GetType(ctx).GetDecimal()
	isYear := b.tp.GetType() == mysql.TypeYear
	tc := typeCtx(ctx)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		duration.Duration = ds[i]
		duration.Fsp = fsp

		if isYear {
			i64s[i], err = duration.ConvertToYear(tc)
		} else {
			var dur types.Duration
			dur, err = duration.RoundFrac(types.DefaultFsp, location(ctx))
			if err != nil {
				return err
			}
			i64s[i], err = dur.ToNumber().ToInt()
		}

		if err != nil {
			return err
		}
	}
	return nil
}

func (*builtinCastIntAsTimeSig) vectorized() bool {
	return true
}

func (b *builtinCastIntAsTimeSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	times := result.Times()
	i64s := buf.Int64s()
	tc := typeCtx(ctx)
	fsp := b.tp.GetDecimal()

	var tm types.Time
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			continue
		}

		if b.args[0].GetType(ctx).GetType() == mysql.TypeYear {
			tm, err = types.ParseTimeFromYear(i64s[i])
		} else {
			tm, err = types.ParseTimeFromNum(tc, i64s[i], b.tp.GetType(), fsp)
		}

		if err != nil {
			if err = handleInvalidTimeError(ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		times[i] = tm
		if b.tp.GetType() == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			times[i].SetCoreTime(types.FromDate(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0))
		}
	}
	return nil
}

func (*builtinCastRealAsJSONSig) vectorized() bool {
	return true
}

func (b *builtinCastRealAsJSONSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}
	f64s := buf.Float64s()
	result.ReserveJSON(n)
	for i := 0; i < n; i++ {
		// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.```
		if buf.IsNull(i) {
			result.AppendNull()
		} else {
			result.AppendJSON(types.CreateBinaryJSON(f64s[i]))
		}
	}
	return nil
}

func (*builtinCastJSONAsRealSig) vectorized() bool {
	return true
}

func (b *builtinCastJSONAsRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeFloat64(n, false)
	result.MergeNulls(buf)
	f64s := result.Float64s()
	tc := typeCtx(ctx)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		f64s[i], err = types.ConvertJSONToFloat(tc, buf.GetJSON(i))
		if err != nil {
			return err
		}
	}
	return nil
}

func (*builtinCastJSONAsTimeSig) vectorized() bool {
	return true
}

func (b *builtinCastJSONAsTimeSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	times := result.Times()

	tc := typeCtx(ctx)
	ts, err := getStmtTimestamp(ctx)
	if err != nil {
		ts = gotime.Now()
	}
	fsp := b.tp.GetDecimal()

	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		val := buf.GetJSON(i)
		if err != nil {
			return err
		}

		switch val.TypeCode {
		case types.JSONTypeCodeDate, types.JSONTypeCodeDatetime, types.JSONTypeCodeTimestamp:
			tm := val.GetTimeWithFsp(b.tp.GetDecimal())
			times[i] = tm
			times[i].SetType(b.tp.GetType())
			if b.tp.GetType() == mysql.TypeDate {
				// Truncate hh:mm:ss part if the type is Date.
				times[i].SetCoreTime(types.FromDate(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0))
			}
		case types.JSONTypeCodeDuration:
			duration := val.GetDuration()

			tc := typeCtx(ctx)
			tm, err := duration.ConvertToTimeWithTimestamp(tc, b.tp.GetType(), ts)
			if err != nil {
				if err = handleInvalidTimeError(ctx, err); err != nil {
					return err
				}
				result.SetNull(i, true)
				continue
			}
			tm, err = tm.RoundFrac(tc, fsp)
			if err != nil {
				return err
			}
			times[i] = tm
		case types.JSONTypeCodeString:
			s, err := val.Unquote()
			if err != nil {
				return err
			}
			tm, err := types.ParseTime(tc, s, b.tp.GetType(), fsp)
			if err != nil {
				if err = handleInvalidTimeError(ctx, err); err != nil {
					return err
				}
				result.SetNull(i, true)
				continue
			}
			times[i] = tm
			if b.tp.GetType() == mysql.TypeDate {
				// Truncate hh:mm:ss part if the type is Date.
				times[i].SetCoreTime(types.FromDate(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0))
			}
		default:
			err = types.ErrTruncatedWrongVal.GenWithStackByArgs(types.TypeStr(b.tp.GetType()), val.String())
			if err = handleInvalidTimeError(ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
		}
	}
	return nil
}

func (*builtinCastRealAsTimeSig) vectorized() bool {
	return true
}

func (b *builtinCastRealAsTimeSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	times := result.Times()
	f64s := buf.Float64s()
	tc := typeCtx(ctx)
	fsp := b.tp.GetDecimal()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			continue
		}
		fv := strconv.FormatFloat(f64s[i], 'f', -1, 64)
		if fv == "0" {
			times[i] = types.ZeroTime
			continue
		}
		tm, err := types.ParseTimeFromFloatString(tc, fv, b.tp.GetType(), fsp)
		if err != nil {
			if err = handleInvalidTimeError(ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		times[i] = tm
		if b.tp.GetType() == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			times[i].SetCoreTime(types.FromDate(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0))
		}
	}
	return nil
}

func (*builtinCastDecimalAsDecimalSig) vectorized() bool {
	return true
}

func (b *builtinCastDecimalAsDecimalSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalDecimal(ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	decs := result.Decimals()
	tc, ec := typeCtx(ctx), errCtx(ctx)
	conditionUnionAndUnsigned := b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag())
	dec := new(types.MyDecimal)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		*dec = types.MyDecimal{}
		if !(conditionUnionAndUnsigned && decs[i].IsNegative()) {
			*dec = decs[i]
		}
		dec, err := types.ProduceDecWithSpecifiedTp(tc, dec, b.tp)
		if err = ec.HandleError(err); err != nil {
			return err
		}
		decs[i] = *dec
	}
	return nil
}

func (*builtinCastDurationAsTimeSig) vectorized() bool {
	return true
}

func (b *builtinCastDurationAsTimeSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	var duration types.Duration
	ds := buf.GoDurations()
	times := result.Times()
	tc := typeCtx(ctx)
	ts, err := getStmtTimestamp(ctx)
	if err != nil {
		ts = gotime.Now()
	}
	fsp := b.tp.GetDecimal()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		duration.Duration = ds[i]
		duration.Fsp = fsp
		tm, err := duration.ConvertToTimeWithTimestamp(tc, b.tp.GetType(), ts)
		if err != nil {
			if err = handleInvalidTimeError(ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		tm, err = tm.RoundFrac(tc, fsp)
		if err != nil {
			return err
		}
		times[i] = tm
	}
	return nil
}

func (*builtinCastIntAsStringSig) vectorized() bool {
	return true
}

func (b *builtinCastIntAsStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	tp := b.args[0].GetType(ctx)
	isUnsigned := mysql.HasUnsignedFlag(tp.GetFlag())
	isYearType := tp.GetType() == mysql.TypeYear
	result.ReserveString(n)
	i64s := buf.Int64s()
	for i := 0; i < n; i++ {
		var str string
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		if !isUnsigned {
			str = strconv.FormatInt(i64s[i], 10)
		} else {
			str = strconv.FormatUint(uint64(i64s[i]), 10)
		}
		if isYearType && str == "0" {
			str = "0000"
		}
		str, err = types.ProduceStrWithSpecifiedTp(str, b.tp, typeCtx(ctx), false)
		if err != nil {
			return err
		}
		var d bool
		str, d, err = padZeroForBinaryType(str, b.tp, ctx)
		if err != nil {
			return err
		}
		if d {
			result.AppendNull()
		} else {
			result.AppendString(str)
		}
	}
	return nil
}

func (*builtinCastRealAsIntSig) vectorized() bool {
	return true
}

func (b *builtinCastRealAsIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	f64s := buf.Float64s()
	unsigned := mysql.HasUnsignedFlag(b.tp.GetFlag())
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		if !unsigned {
			i64s[i], err = types.ConvertFloatToInt(f64s[i], types.IntergerSignedLowerBound(mysql.TypeLonglong), types.IntergerSignedUpperBound(mysql.TypeLonglong), mysql.TypeLonglong)
		} else if b.inUnion && f64s[i] < 0 {
			i64s[i] = 0
		} else {
			var uintVal uint64
			tc := typeCtx(ctx)
			uintVal, err = types.ConvertFloatToUint(tc.Flags(), f64s[i], types.IntergerUnsignedUpperBound(mysql.TypeLonglong), mysql.TypeLonglong)
			i64s[i] = int64(uintVal)
		}
		if types.ErrOverflow.Equal(err) {
			ec := errCtx(ctx)
			err = ec.HandleError(err)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (*builtinCastTimeAsRealSig) vectorized() bool {
	return true
}

func (b *builtinCastTimeAsRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeFloat64(n, false)
	result.MergeNulls(buf)
	times := buf.Times()
	f64s := result.Float64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		f64, err := times[i].ToNumber().ToFloat64()
		if err != nil {
			if types.ErrOverflow.Equal(err) {
				ec := errCtx(ctx)
				err = ec.HandleError(err)
			}
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		f64s[i] = f64
	}
	return nil
}

func (*builtinCastStringAsJSONSig) vectorized() bool {
	return true
}

func (b *builtinCastStringAsJSONSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveJSON(n)
	typ := b.args[0].GetType(ctx)
	if types.IsBinaryStr(typ) {
		var res types.BinaryJSON
		for i := 0; i < n; i++ {
			if buf.IsNull(i) {
				result.AppendNull()
				continue
			}

			val := buf.GetBytes(i)
			resultBuf := val
			if typ.GetType() == mysql.TypeString && typ.GetFlen() > 0 {
				// only for BINARY: the tailing zero should also be in the opaque json
				resultBuf = make([]byte, typ.GetFlen())
				copy(resultBuf, val)
			}

			res = types.CreateBinaryJSON(types.Opaque{
				TypeCode: b.args[0].GetType(ctx).GetType(),
				Buf:      resultBuf,
			})
			result.AppendJSON(res)
		}
	} else if mysql.HasParseToJSONFlag(b.tp.GetFlag()) {
		var res types.BinaryJSON
		for i := 0; i < n; i++ {
			if buf.IsNull(i) {
				result.AppendNull()
				continue
			}
			res, err = types.ParseBinaryJSONFromString(buf.GetString(i))
			if err != nil {
				return err
			}
			result.AppendJSON(res)
		}
	} else {
		for i := 0; i < n; i++ {
			if buf.IsNull(i) {
				result.AppendNull()
				continue
			}
			result.AppendJSON(types.CreateBinaryJSON(buf.GetString(i)))
		}
	}
	return nil
}

func (*builtinCastRealAsDecimalSig) vectorized() bool {
	return true
}

func (b *builtinCastRealAsDecimalSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)
	bufreal := buf.Float64s()
	resdecimal := result.Decimals()
	tc, ec := typeCtx(ctx), errCtx(ctx)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if !b.inUnion || bufreal[i] >= 0 {
			if err = resdecimal[i].FromFloat64(bufreal[i]); err != nil {
				if types.ErrOverflow.Equal(err) {
					warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", b.args[0])
					err = ec.HandleErrorWithAlias(err, err, warnErr)
				} else if types.ErrTruncated.Equal(err) {
					// This behavior is consistent with MySQL.
					err = nil
				}
				if err != nil {
					return err
				}
			}
		}
		dec, err := types.ProduceDecWithSpecifiedTp(tc, &resdecimal[i], b.tp)
		if err = ec.HandleError(err); err != nil {
			return err
		}
		resdecimal[i] = *dec
	}
	return nil
}

func (*builtinCastStringAsIntSig) vectorized() bool {
	return true
}

func (b *builtinCastStringAsIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if b.args[0].GetType(ctx).Hybrid() || IsBinaryLiteral(b.args[0]) {
		return b.args[0].VecEvalInt(ctx, input, result)
	}

	// Take the implicit evalInt path if possible.
	if CanImplicitEvalInt(b.args[0]) {
		return b.args[0].VecEvalInt(ctx, input, result)
	}

	result.ResizeInt64(n, false)
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.MergeNulls(buf)
	tc := typeCtx(ctx)
	i64s := result.Int64s()
	isUnsigned := mysql.HasUnsignedFlag(b.tp.GetFlag())
	unionUnsigned := isUnsigned && b.inUnion
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		var (
			res  int64
			ures uint64
		)
		val := strings.TrimSpace(buf.GetString(i))
		isNegative := len(val) > 1 && val[0] == '-'
		if !isNegative {
			ures, err = types.StrToUint(tc, val, true)
			if !isUnsigned && err == nil && ures > uint64(math.MaxInt64) {
				tc.AppendWarning(types.ErrCastAsSignedOverflow)
			}
			res = int64(ures)
		} else if unionUnsigned {
			res = 0
		} else {
			res, err = types.StrToInt(tc, val, true)
			if err == nil && isUnsigned {
				// If overflow, don't append this warnings
				tc.AppendWarning(types.ErrCastNegIntAsUnsigned)
			}
		}
		res, err = b.handleOverflow(ctx, res, val, err, isNegative)
		if err != nil {
			return err
		}
		i64s[i] = res
	}
	return nil
}

func (*builtinCastStringAsDurationSig) vectorized() bool {
	return true
}

func (b *builtinCastStringAsDurationSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		dur, isNull, err := types.ParseDuration(tc, buf.GetString(i), b.tp.GetDecimal())
		if err != nil {
			if types.ErrTruncatedWrongVal.Equal(err) {
				err = tc.HandleTruncate(err)
			}
			if err != nil {
				return err
			}
			if isNull {
				result.SetNull(i, true)
				continue
			}
		}
		ds[i] = dur.Duration
	}
	return nil
}

func (*builtinCastDurationAsDecimalSig) vectorized() bool {
	return true
}

func (b *builtinCastDurationAsDecimalSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)
	d64s := result.Decimals()
	var duration types.Duration
	ds := buf.GoDurations()
	tc, ec := typeCtx(ctx), errCtx(ctx)
	fsp := b.args[0].GetType(ctx).GetDecimal()
	if fsp, err = types.CheckFsp(fsp); err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		duration.Duration = ds[i]
		duration.Fsp = fsp
		res, err := types.ProduceDecWithSpecifiedTp(tc, duration.ToNumber(), b.tp)
		if err = ec.HandleError(err); err != nil {
			return err
		}
		d64s[i] = *res
	}
	return nil
}

func (*builtinCastIntAsDecimalSig) vectorized() bool {
	return true
}

func (b *builtinCastIntAsDecimalSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	isUnsignedTp := mysql.HasUnsignedFlag(b.tp.GetFlag())
	isUnsignedArgs0 := mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag())
	nums := buf.Int64s()
	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)
	decs := result.Decimals()
	tc, ec := typeCtx(ctx), errCtx(ctx)
	dec := new(types.MyDecimal)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		*dec = types.MyDecimal{}
		if !isUnsignedTp && !isUnsignedArgs0 {
			dec.FromInt(nums[i])
		} else if b.inUnion && !isUnsignedArgs0 && nums[i] < 0 {
			dec.FromUint(0)
		} else {
			dec.FromUint(uint64(nums[i]))
		}

		dec, err = types.ProduceDecWithSpecifiedTp(tc, dec, b.tp)
		if err = ec.HandleError(err); err != nil {
			return err
		}
		decs[i] = *dec
	}
	return nil
}

func (*builtinCastIntAsJSONSig) vectorized() bool {
	return true
}

func (b *builtinCastIntAsJSONSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}
	nums := buf.Int64s()
	result.ReserveJSON(n)
	if mysql.HasIsBooleanFlag(b.args[0].GetType(ctx).GetFlag()) {
		for i := 0; i < n; i++ {
			if buf.IsNull(i) {
				result.AppendNull()
			} else {
				result.AppendJSON(types.CreateBinaryJSON(nums[i] != 0))
			}
		}
	} else if mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()) {
		for i := 0; i < n; i++ {
			if buf.IsNull(i) {
				result.AppendNull()
			} else {
				result.AppendJSON(types.CreateBinaryJSON(uint64(nums[i])))
			}
		}
	} else {
		for i := 0; i < n; i++ {
			if buf.IsNull(i) {
				result.AppendNull()
			} else {
				result.AppendJSON(types.CreateBinaryJSON(nums[i]))
			}
		}
	}

	return nil
}

func (*builtinCastJSONAsJSONSig) vectorized() bool {
	return true
}

func (b *builtinCastJSONAsJSONSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalJSON(ctx, input, result)
}

func (*builtinCastJSONAsStringSig) vectorized() bool {
	return true
}

func (b *builtinCastJSONAsStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	tc := typeCtx(ctx)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		s, err := types.ProduceStrWithSpecifiedTp(buf.GetJSON(i).String(), b.tp, tc, false)
		if err != nil {
			return err
		}
		result.AppendString(s)
	}
	return nil
}

func (*builtinCastDurationAsRealSig) vectorized() bool {
	return true
}

func (b *builtinCastDurationAsRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeFloat64(n, false)
	result.MergeNulls(buf)
	f64s := result.Float64s()

	var duration types.Duration
	fsp := b.args[0].GetType(ctx).GetDecimal()
	if fsp, err = types.CheckFsp(fsp); err != nil {
		return err
	}
	ds := buf.GoDurations()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		duration.Duration = ds[i]
		duration.Fsp = fsp
		if f64s[i], err = duration.ToNumber().ToFloat64(); err != nil {
			return err
		}
	}
	return nil
}

func (*builtinCastJSONAsIntSig) vectorized() bool {
	return true
}

func (b *builtinCastJSONAsIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	tc, ec := typeCtx(ctx), errCtx(ctx)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		i64s[i], err = types.ConvertJSONToInt64(tc, buf.GetJSON(i), mysql.HasUnsignedFlag(b.tp.GetFlag()))
		if err = ec.HandleError(err); err != nil {
			return err
		}
	}
	return nil
}

func (*builtinCastRealAsDurationSig) vectorized() bool {
	return true
}

func (b *builtinCastRealAsDurationSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeGoDuration(n, false)
	result.MergeNulls(buf)
	f64s := buf.Float64s()
	ds := result.GoDurations()
	tc := typeCtx(ctx)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		dur, _, err := types.ParseDuration(tc, strconv.FormatFloat(f64s[i], 'f', -1, 64), b.tp.GetDecimal())
		if err != nil {
			if types.ErrTruncatedWrongVal.Equal(err) {
				err = tc.HandleTruncate(err)
				if err != nil {
					return err
				}
				// ErrTruncatedWrongVal needs to be considered NULL.
				result.SetNull(i, true)
				continue
			}
			return err
		}
		ds[i] = dur.Duration
	}
	return nil
}

func (*builtinCastTimeAsDurationSig) vectorized() bool {
	return true
}

func (b *builtinCastTimeAsDurationSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	arg0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(arg0)
	if err := b.args[0].VecEvalTime(ctx, input, arg0); err != nil {
		return err
	}
	arg0s := arg0.Times()
	result.ResizeGoDuration(n, false)
	result.MergeNulls(arg0)
	ds := result.GoDurations()
	for i, t := range arg0s {
		if result.IsNull(i) {
			continue
		}
		d, err := t.ConvertToDuration()
		if err != nil {
			return err
		}
		d, err = d.RoundFrac(b.tp.GetDecimal(), location(ctx))
		if err != nil {
			return err
		}
		ds[i] = d.Duration
	}
	return nil
}

func (*builtinCastDurationAsDurationSig) vectorized() bool {
	return true
}

func (b *builtinCastDurationAsDurationSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	var err error
	if err = b.args[0].VecEvalDuration(ctx, input, result); err != nil {
		return err
	}

	res := result.GoDurations()
	dur := &types.Duration{
		Fsp: types.UnspecifiedFsp,
	}
	var rd types.Duration
	for i, v := range res {
		if result.IsNull(i) {
			continue
		}
		dur.Duration = v
		rd, err = dur.RoundFrac(b.tp.GetDecimal(), location(ctx))
		if err != nil {
			return err
		}
		res[i] = rd.Duration
	}
	return nil
}

func (*builtinCastDurationAsStringSig) vectorized() bool {
	return true
}

func (b *builtinCastDurationAsStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return err
	}

	var res string
	var isNull bool
	tc := typeCtx(ctx)
	result.ReserveString(n)
	fsp := b.args[0].GetType(ctx).GetDecimal()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		res, err = types.ProduceStrWithSpecifiedTp(buf.GetDuration(i, fsp).String(), b.tp, tc, false)
		if err != nil {
			return err
		}
		res, isNull, err = padZeroForBinaryType(res, b.tp, ctx)
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
			continue
		}
		result.AppendString(res)
	}
	return nil
}

func (*builtinCastDecimalAsRealSig) vectorized() bool {
	return true
}

func (b *builtinCastDecimalAsRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeFloat64(n, false)
	result.MergeNulls(buf)

	d := buf.Decimals()
	rs := result.Float64s()

	inUnionAndUnsigned := b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag())
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if inUnionAndUnsigned && d[i].IsNegative() {
			rs[i] = 0
			continue
		}
		res, err := d[i].ToFloat64()
		if err != nil {
			if types.ErrOverflow.Equal(err) {
				ec := errCtx(ctx)
				err = ec.HandleError(err)
			}
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		rs[i] = res
	}
	return nil
}

func (*builtinCastDecimalAsTimeSig) vectorized() bool {
	return true
}

func (b *builtinCastDecimalAsTimeSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	times := result.Times()
	decimals := buf.Decimals()
	tc := typeCtx(ctx)
	fsp := b.tp.GetDecimal()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			continue
		}
		tm, err := types.ParseTimeFromFloatString(tc, string(decimals[i].ToString()), b.tp.GetType(), fsp)
		if err != nil {
			if err = handleInvalidTimeError(ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		times[i] = tm
		if b.tp.GetType() == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			times[i].SetCoreTime(types.FromDate(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0))
		}
	}
	return nil
}

func (*builtinCastTimeAsIntSig) vectorized() bool {
	return true
}

func (b *builtinCastTimeAsIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	times := buf.Times()
	i64s := result.Int64s()
	tc := typeCtx(ctx)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		t, err := times[i].RoundFrac(tc, types.DefaultFsp)
		if err != nil {
			return err
		}
		i64s[i], err = t.ToNumber().ToInt()
		if err != nil {
			return err
		}
	}
	return nil
}

func (*builtinCastTimeAsTimeSig) vectorized() bool {
	return true
}

func (b *builtinCastTimeAsTimeSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalTime(ctx, input, result); err != nil {
		return err
	}

	times := result.Times()
	tc := typeCtx(ctx)
	fsp := b.tp.GetDecimal()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		res, err := times[i].Convert(tc, b.tp.GetType())
		if err != nil {
			if err = handleInvalidTimeError(ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		tm, err := res.RoundFrac(tc, fsp)
		if err != nil {
			return err
		}
		times[i] = tm
		if b.tp.GetType() == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			times[i].SetCoreTime(types.FromDate(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0))
			times[i].SetType(b.tp.GetType())
		}
	}
	return nil
}

func (*builtinCastTimeAsStringSig) vectorized() bool {
	return true
}

func (b *builtinCastTimeAsStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(ctx, input, buf); err != nil {
		return err
	}

	var res string
	var isNull bool
	tc := typeCtx(ctx)
	vas := buf.Times()
	result.ReserveString(n)
	for i, v := range vas {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		res, err = types.ProduceStrWithSpecifiedTp(v.String(), b.tp, tc, false)
		if err != nil {
			return err
		}
		res, isNull, err = padZeroForBinaryType(res, b.tp, ctx)
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
			continue
		}
		result.AppendString(res)
	}
	return nil
}

func (*builtinCastJSONAsDecimalSig) vectorized() bool {
	return true
}

func (b *builtinCastJSONAsDecimalSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalJSON(ctx, input, buf); err != nil {
		return err
	}
	tc, ec := typeCtx(ctx), errCtx(ctx)
	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)
	res := result.Decimals()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		tempres, err := types.ConvertJSONToDecimal(tc, buf.GetJSON(i))
		if err != nil {
			return err
		}
		tempres, err = types.ProduceDecWithSpecifiedTp(tc, tempres, b.tp)
		if err = ec.HandleError(err); err != nil {
			return err
		}
		res[i] = *tempres
	}
	return nil
}

func (*builtinCastStringAsRealSig) vectorized() bool {
	return true
}

func (b *builtinCastStringAsRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if IsBinaryLiteral(b.args[0]) {
		return b.args[0].VecEvalReal(ctx, input, result)
	}

	// Take the implicit evalReal path if possible.
	if CanImplicitEvalReal(b.args[0]) {
		return b.args[0].VecEvalReal(ctx, input, result)
	}

	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeFloat64(n, false)
	result.MergeNulls(buf)
	ret := result.Float64s()
	tc := typeCtx(ctx)

	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		res, err := types.StrToFloat(tc, buf.GetString(i), true)
		if err != nil {
			return err
		}
		if b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && res < 0 {
			res = 0
		}
		res, err = types.ProduceFloatWithSpecifiedTp(res, b.tp)
		if err != nil {
			return err
		}
		ret[i] = res
	}
	return nil
}

func (*builtinCastStringAsDecimalSig) vectorized() bool {
	return true
}

func (b *builtinCastStringAsDecimalSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if IsBinaryLiteral(b.args[0]) {
		return b.args[0].VecEvalDecimal(ctx, input, result)
	}
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)
	res := result.Decimals()
	tc, ec := typeCtx(ctx), errCtx(ctx)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		val := strings.TrimSpace(buf.GetString(i))
		isNegative := len(val) > 0 && val[0] == '-'
		dec := new(types.MyDecimal)
		if !(b.inUnion && mysql.HasUnsignedFlag(b.tp.GetFlag()) && isNegative) {
			if err := tc.HandleTruncate(dec.FromString([]byte(val))); err != nil {
				return err
			}
			dec, err := types.ProduceDecWithSpecifiedTp(tc, dec, b.tp)
			if err = ec.HandleError(err); err != nil {
				return err
			}
			res[i] = *dec
		}
	}
	return nil
}

func (*builtinCastStringAsTimeSig) vectorized() bool {
	return true
}

func (b *builtinCastStringAsTimeSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
	fsp := b.tp.GetDecimal()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		tm, err := types.ParseTime(tc, buf.GetString(i), b.tp.GetType(), fsp)
		if err != nil {
			if errors.Is(err, strconv.ErrSyntax) || errors.Is(err, strconv.ErrRange) {
				err = types.ErrIncorrectDatetimeValue.GenWithStackByArgs(buf.GetString(i))
			}
			if err = handleInvalidTimeError(ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		if tm.IsZero() && sqlMode(ctx).HasNoZeroDateMode() {
			err = handleInvalidTimeError(ctx, types.ErrWrongValue.GenWithStackByArgs(types.DateTimeStr, tm.String()))
			if err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		times[i] = tm
		if b.tp.GetType() == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			times[i].SetCoreTime(types.FromDate(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0))
		}
	}
	return nil
}

func (*builtinCastDecimalAsIntSig) vectorized() bool {
	return true
}

func (b *builtinCastDecimalAsIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	d64s := buf.Decimals()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		// Round is needed for both unsigned and signed.
		to := d64s[i]
		err = d64s[i].Round(&to, 0, types.ModeHalfUp)
		if err != nil {
			return err
		}

		if !mysql.HasUnsignedFlag(b.tp.GetFlag()) {
			i64s[i], err = to.ToInt()
		} else if b.inUnion && to.IsNegative() {
			i64s[i] = 0
		} else {
			var uintRes uint64
			uintRes, err = to.ToUint()
			i64s[i] = int64(uintRes)
		}

		if types.ErrOverflow.Equal(err) {
			ec := errCtx(ctx)
			warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", d64s[i])
			err = ec.HandleErrorWithAlias(err, err, warnErr)
		}

		if err != nil {
			return err
		}
	}
	return nil
}

func (*builtinCastDecimalAsDurationSig) vectorized() bool {
	return true
}

func (b *builtinCastDecimalAsDurationSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeGoDuration(n, false)
	result.MergeNulls(buf)
	args := buf.Decimals()
	ds := result.GoDurations()
	tc := typeCtx(ctx)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		dur, _, err := types.ParseDuration(tc, string(args[i].ToString()), b.tp.GetDecimal())
		if err != nil {
			if types.ErrTruncatedWrongVal.Equal(err) {
				err = tc.HandleTruncate(err)
				if err != nil {
					return err
				}
				// ErrTruncatedWrongVal needs to be considered NULL.
				result.SetNull(i, true)
				continue
			}
			return err
		}
		ds[i] = dur.Duration
	}
	return nil
}

func (*builtinCastStringAsStringSig) vectorized() bool {
	return true
}

func (b *builtinCastStringAsStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	var res string
	var isNull bool
	tc := typeCtx(ctx)
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		res, err = types.ProduceStrWithSpecifiedTp(buf.GetString(i), b.tp, tc, false)
		if err != nil {
			return err
		}
		res, isNull, err = padZeroForBinaryType(res, b.tp, ctx)
		if err != nil {
			return err
		}
		if isNull {
			result.AppendNull()
			continue
		}
		result.AppendString(res)
	}
	return nil
}

func (*builtinCastJSONAsDurationSig) vectorized() bool {
	return true
}

func (b *builtinCastJSONAsDurationSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(ctx, input, buf); err != nil {
		return err
	}

	tc := typeCtx(ctx)

	result.ResizeGoDuration(n, false)
	result.MergeNulls(buf)
	var dur types.Duration
	ds := result.GoDurations()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		val := buf.GetJSON(i)

		switch val.TypeCode {
		case types.JSONTypeCodeDate, types.JSONTypeCodeDatetime, types.JSONTypeCodeTimestamp:
			time := val.GetTimeWithFsp(b.tp.GetDecimal())
			d, err := time.ConvertToDuration()
			if err != nil {
				return err
			}
			d, err = d.RoundFrac(b.tp.GetDecimal(), location(ctx))
			if err != nil {
				return err
			}
			ds[i] = d.Duration
		case types.JSONTypeCodeDuration:
			dur = val.GetDuration()
			ds[i] = dur.Duration
		case types.JSONTypeCodeString:
			s, err := buf.GetJSON(i).Unquote()
			if err != nil {
				return err
			}
			dur, _, err = types.ParseDuration(tc, s, b.tp.GetDecimal())
			if types.ErrTruncatedWrongVal.Equal(err) {
				err = tc.HandleTruncate(err)
			}
			if err != nil {
				return err
			}
			ds[i] = dur.Duration
		default:
			err = types.ErrTruncatedWrongVal.GenWithStackByArgs(types.TypeStr(b.tp.GetType()), val.String())
			err = tc.HandleTruncate(err)
			if err != nil {
				return err
			}

			result.SetNull(i, true)
			continue
		}
	}
	return nil
}

func (*builtinCastDecimalAsJSONSig) vectorized() bool {
	return true
}

func (b *builtinCastDecimalAsJSONSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDecimal(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveJSON(n)
	f64s := buf.Decimals()
	var f float64
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
		f, err = f64s[i].ToFloat64()
		if err != nil {
			return err
		}
		result.AppendJSON(types.CreateBinaryJSON(f))
	}
	return nil
}

func (*builtinCastDurationAsJSONSig) vectorized() bool {
	return true
}

func (b *builtinCastDurationAsJSONSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveJSON(n)
	var dur types.Duration
	dur.Fsp = types.MaxFsp
	ds := buf.GoDurations()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		dur.Duration = ds[i]
		result.AppendJSON(types.CreateBinaryJSON(dur))
	}
	return nil
}
