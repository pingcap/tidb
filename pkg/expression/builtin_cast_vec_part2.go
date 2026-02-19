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
	"strings"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)


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
	for i := range n {
		if result.IsNull(i) {
			continue
		}

		if !unsigned {
			i64s[i], err = types.ConvertFloatToInt(f64s[i], types.IntegerSignedLowerBound(mysql.TypeLonglong), types.IntegerSignedUpperBound(mysql.TypeLonglong), mysql.TypeLonglong)
		} else if b.inUnion && f64s[i] < 0 {
			i64s[i] = 0
		} else {
			var uintVal uint64
			tc := typeCtx(ctx)
			uintVal, err = types.ConvertFloatToUint(tc.Flags(), f64s[i], types.IntegerUnsignedUpperBound(mysql.TypeLonglong), mysql.TypeLonglong)
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
	for i := range n {
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
		for i := range n {
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
		for i := range n {
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
		for i := range n {
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
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if !b.inUnion || bufreal[i] >= 0 {
			if err = resdecimal[i].FromFloat64(bufreal[i]); err != nil {
				if types.ErrOverflow.Equal(err) {
					warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", b.args[0].StringWithCtx(ctx, perrors.RedactLogDisable))
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
		for i := range n {
			if buf.IsNull(i) {
				result.AppendNull()
			} else {
				result.AppendJSON(types.CreateBinaryJSON(nums[i] != 0))
			}
		}
	} else if mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()) || b.args[0].GetType(ctx).GetType() == mysql.TypeYear {
		for i := range n {
			if buf.IsNull(i) {
				result.AppendNull()
			} else {
				result.AppendJSON(types.CreateBinaryJSON(uint64(nums[i])))
			}
		}
	} else {
		for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
	for i := range n {
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
