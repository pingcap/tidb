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
	"strconv"
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
	for i := range n {
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

	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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

	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
