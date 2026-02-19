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
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)


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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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

	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		dur.Duration = ds[i]
		result.AppendJSON(types.CreateBinaryJSON(dur))
	}
	return nil
}
