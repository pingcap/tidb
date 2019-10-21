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
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinCastIntAsDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
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
		dur, err := types.NumberToDuration(i64s[i], int8(b.tp.Decimal))
		if err != nil {
			if types.ErrOverflow.Equal(err) {
				err = b.ctx.GetSessionVars().StmtCtx.HandleOverflow(err, err)
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

func (b *builtinCastIntAsDurationSig) vectorized() bool {
	return true
}

func (b *builtinCastIntAsIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}
	if b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag) {
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

func (b *builtinCastIntAsIntSig) vectorized() bool {
	return true
}

func (b *builtinCastIntAsRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeFloat64(n, false)
	result.MergeNulls(buf)

	i64s := buf.Int64s()
	rs := result.Float64s()

	hasUnsignedFlag0 := mysql.HasUnsignedFlag(b.tp.Flag)
	hasUnsignedFlag1 := mysql.HasUnsignedFlag(b.args[0].GetType().Flag)

	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if !hasUnsignedFlag0 && !hasUnsignedFlag1 {
			rs[i] = float64(i64s[i])
		} else if b.inUnion && i64s[i] < 0 {
			rs[i] = 0
		} else {
			// recall that, int to float is different from uint to float
			rs[i] = float64(uint64(i64s[i]))
		}
	}
	return nil
}

func (b *builtinCastIntAsRealSig) vectorized() bool {
	return true
}

func (b *builtinCastRealAsRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	f64s := result.Float64s()
	conditionUnionAndUnsigned := b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag)
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

func (b *builtinCastRealAsRealSig) vectorized() bool {
	return true
}

func (b *builtinCastTimeAsJSONSig) vectorized() bool {
	return true
}

func (b *builtinCastTimeAsJSONSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveJSON(n)
	tms := buf.Times()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		tp := tms[i].Type
		if tp == mysql.TypeDatetime || tp == mysql.TypeTimestamp {
			tms[i].Fsp = types.MaxFsp
		}
		result.AppendJSON(json.CreateBinary(tms[i].String()))
	}
	return nil
}

func (b *builtinCastRealAsStringSig) vectorized() bool {
	return true
}

func (b *builtinCastRealAsStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}

	bits := 64
	if b.args[0].GetType().Tp == mysql.TypeFloat {
		// b.args[0].EvalReal() casts the value from float32 to float64, for example:
		// float32(208.867) is cast to float64(208.86700439)
		// If we strconv.FormatFloat the value with 64bits, the result is incorrect!
		bits = 32
	}

	var isNull bool
	var res string
	f64s := buf.Float64s()
	result.ReserveString(n)
	sc := b.ctx.GetSessionVars().StmtCtx
	for i, v := range f64s {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		res, err = types.ProduceStrWithSpecifiedTp(strconv.FormatFloat(v, 'f', -1, bits), b.tp, sc, false)
		if err != nil {
			return err
		}
		res, isNull, err = padZeroForBinaryType(res, b.tp, b.ctx)
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

func (b *builtinCastDecimalAsStringSig) vectorized() bool {
	return true
}

func (b *builtinCastDecimalAsStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(b.ctx, input, buf); err != nil {
		return err
	}

	sc := b.ctx.GetSessionVars().StmtCtx
	vas := buf.Decimals()
	result.ReserveString(n)
	for i, v := range vas {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		res, e := types.ProduceStrWithSpecifiedTp(string(v.ToString()), b.tp, sc, false)
		if e != nil {
			return e
		}
		str, b, e1 := padZeroForBinaryType(res, b.tp, b.ctx)
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

func (b *builtinCastTimeAsDecimalSig) vectorized() bool {
	return true
}

func (b *builtinCastTimeAsDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}
	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)
	times := buf.Times()
	decs := result.Decimals()
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		times[i].FillNumber(&decs[i])
		if _, err := types.ProduceDecWithSpecifiedTp(&decs[i], b.tp, sc); err != nil {
			return err
		}
	}
	return nil
}

func (b *builtinCastDurationAsIntSig) vectorized() bool {
	return false
}

func (b *builtinCastDurationAsIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastIntAsTimeSig) vectorized() bool {
	return false
}

func (b *builtinCastIntAsTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastRealAsJSONSig) vectorized() bool {
	return true
}

func (b *builtinCastRealAsJSONSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}
	f64s := buf.Float64s()
	result.ReserveJSON(n)
	for i := 0; i < n; i++ {
		// FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.```
		if buf.IsNull(i) {
			result.AppendNull()
		} else {
			result.AppendJSON(json.CreateBinary(f64s[i]))
		}
	}
	return nil
}

func (b *builtinCastJSONAsRealSig) vectorized() bool {
	return false
}

func (b *builtinCastJSONAsRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastJSONAsTimeSig) vectorized() bool {
	return false
}

func (b *builtinCastJSONAsTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastRealAsTimeSig) vectorized() bool {
	return false
}

func (b *builtinCastRealAsTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastDecimalAsDecimalSig) vectorized() bool {
	return false
}

func (b *builtinCastDecimalAsDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastDurationAsTimeSig) vectorized() bool {
	return false
}

func (b *builtinCastDurationAsTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastIntAsStringSig) vectorized() bool {
	return true
}

func (b *builtinCastIntAsStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	isUnsigned := mysql.HasUnsignedFlag(b.args[0].GetType().Flag)
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
		str, err = types.ProduceStrWithSpecifiedTp(str, b.tp, b.ctx.GetSessionVars().StmtCtx, false)
		if err != nil {
			return err
		}
		var d bool
		str, d, err = padZeroForBinaryType(str, b.tp, b.ctx)
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

func (b *builtinCastRealAsIntSig) vectorized() bool {
	return true
}

func (b *builtinCastRealAsIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	f64s := buf.Float64s()
	unsigned := mysql.HasUnsignedFlag(b.tp.Flag)
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
			sc := b.ctx.GetSessionVars().StmtCtx
			uintVal, err = types.ConvertFloatToUint(sc, f64s[i], types.IntergerUnsignedUpperBound(mysql.TypeLonglong), mysql.TypeLonglong)
			i64s[i] = int64(uintVal)
		}

		if err != nil {
			return err
		}
	}
	return nil
}

func (b *builtinCastTimeAsRealSig) vectorized() bool {
	return true
}

func (b *builtinCastTimeAsRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
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
				err = b.ctx.GetSessionVars().StmtCtx.HandleOverflow(err, err)
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

func (b *builtinCastStringAsJSONSig) vectorized() bool {
	return true
}

func (b *builtinCastStringAsJSONSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveJSON(n)
	hasParse := mysql.HasParseToJSONFlag(b.tp.Flag)
	if hasParse {
		var res json.BinaryJSON
		for i := 0; i < n; i++ {
			if buf.IsNull(i) {
				result.AppendNull()
				continue
			}
			res, err = json.ParseBinaryFromString(buf.GetString(i))
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
			result.AppendJSON(json.CreateBinary(buf.GetString(i)))
		}
	}
	return nil
}

func (b *builtinCastRealAsDecimalSig) vectorized() bool {
	return false
}

func (b *builtinCastRealAsDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastStringAsIntSig) vectorized() bool {
	return false
}

func (b *builtinCastStringAsIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastStringAsDurationSig) vectorized() bool {
	return false
}

func (b *builtinCastStringAsDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastDurationAsDecimalSig) vectorized() bool {
	return false
}

func (b *builtinCastDurationAsDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastIntAsDecimalSig) vectorized() bool {
	return false
}

func (b *builtinCastIntAsDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastIntAsJSONSig) vectorized() bool {
	return true
}

func (b *builtinCastIntAsJSONSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}
	nums := buf.Int64s()
	result.ReserveJSON(n)
	if mysql.HasIsBooleanFlag(b.args[0].GetType().Flag) {
		for i := 0; i < n; i++ {
			if buf.IsNull(i) {
				result.AppendNull()
			} else {
				result.AppendJSON(json.CreateBinary(nums[i] != 0))
			}
		}
	} else if mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		for i := 0; i < n; i++ {
			if buf.IsNull(i) {
				result.AppendNull()
			} else {
				result.AppendJSON(json.CreateBinary(uint64(nums[i])))
			}
		}
	} else {
		for i := 0; i < n; i++ {
			if buf.IsNull(i) {
				result.AppendNull()
			} else {
				result.AppendJSON(json.CreateBinary(nums[i]))
			}
		}
	}

	return nil
}

func (b *builtinCastJSONAsJSONSig) vectorized() bool {
	return true
}

func (b *builtinCastJSONAsJSONSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalJSON(b.ctx, input, result)
}

func (b *builtinCastJSONAsStringSig) vectorized() bool {
	return true
}

func (b *builtinCastJSONAsStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(b.ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(buf.GetJSON(i).String())
	}
	return nil
}

func (b *builtinCastDurationAsRealSig) vectorized() bool {
	return false
}

func (b *builtinCastDurationAsRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastJSONAsIntSig) vectorized() bool {
	return true
}

func (b *builtinCastJSONAsIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		i64s[i], err = types.ConvertJSONToInt(sc, buf.GetJSON(i), mysql.HasUnsignedFlag(b.tp.Flag))
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *builtinCastRealAsDurationSig) vectorized() bool {
	return false
}

func (b *builtinCastRealAsDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastTimeAsDurationSig) vectorized() bool {
	return true
}

func (b *builtinCastTimeAsDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	arg0, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(arg0)
	if err := b.args[0].VecEvalTime(b.ctx, input, arg0); err != nil {
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
		d, err = d.RoundFrac(int8(b.tp.Decimal))
		if err != nil {
			return err
		}
		ds[i] = d.Duration
	}
	return nil
}

func (b *builtinCastDurationAsDurationSig) vectorized() bool {
	return true
}

func (b *builtinCastDurationAsDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	var err error
	if err = b.args[0].VecEvalDuration(b.ctx, input, result); err != nil {
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
		rd, err = dur.RoundFrac(int8(b.tp.Decimal))
		if err != nil {
			return err
		}
		res[i] = rd.Duration
	}
	return nil
}

func (b *builtinCastDurationAsStringSig) vectorized() bool {
	return true
}

func (b *builtinCastDurationAsStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDuration, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDuration(b.ctx, input, buf); err != nil {
		return err
	}

	var res string
	var isNull bool
	sc := b.ctx.GetSessionVars().StmtCtx
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		res, err = types.ProduceStrWithSpecifiedTp(buf.GetDuration(i, 0).String(), b.tp, sc, false)
		if err != nil {
			return err
		}
		res, isNull, err = padZeroForBinaryType(res, b.tp, b.ctx)
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

func (b *builtinCastDecimalAsRealSig) vectorized() bool {
	return true
}

func (b *builtinCastDecimalAsRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeFloat64(n, false)
	result.MergeNulls(buf)

	d := buf.Decimals()
	rs := result.Float64s()

	inUnionAndUnsigned := b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag)
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
				err = b.ctx.GetSessionVars().StmtCtx.HandleOverflow(err, err)
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

func (b *builtinCastDecimalAsTimeSig) vectorized() bool {
	return false
}

func (b *builtinCastDecimalAsTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastTimeAsIntSig) vectorized() bool {
	return true
}

func (b *builtinCastTimeAsIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
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
	times := buf.Times()
	i64s := result.Int64s()
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		t, err := times[i].RoundFrac(sc, types.DefaultFsp)
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

func (b *builtinCastTimeAsTimeSig) vectorized() bool {
	return false
}

func (b *builtinCastTimeAsTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastTimeAsStringSig) vectorized() bool {
	return true
}

func (b *builtinCastTimeAsStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETTimestamp, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}

	var res string
	var isNull bool
	sc := b.ctx.GetSessionVars().StmtCtx
	vas := buf.Times()
	result.ReserveString(n)
	for i, v := range vas {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		res, err = types.ProduceStrWithSpecifiedTp(v.String(), b.tp, sc, false)
		if err != nil {
			return err
		}
		res, isNull, err = padZeroForBinaryType(res, b.tp, b.ctx)
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

func (b *builtinCastJSONAsDecimalSig) vectorized() bool {
	return false
}

func (b *builtinCastJSONAsDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastStringAsRealSig) vectorized() bool {
	return false
}

func (b *builtinCastStringAsRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastStringAsDecimalSig) vectorized() bool {
	return false
}

func (b *builtinCastStringAsDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastStringAsTimeSig) vectorized() bool {
	return false
}

func (b *builtinCastStringAsTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastDecimalAsIntSig) vectorized() bool {
	return false
}

func (b *builtinCastDecimalAsIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastDecimalAsDurationSig) vectorized() bool {
	return false
}

func (b *builtinCastDecimalAsDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastStringAsStringSig) vectorized() bool {
	return true
}

func (b *builtinCastStringAsStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	var res string
	var isNull bool
	sc := b.ctx.GetSessionVars().StmtCtx
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		res, err = types.ProduceStrWithSpecifiedTp(buf.GetString(i), b.tp, sc, false)
		if err != nil {
			return err
		}
		res, isNull, err = padZeroForBinaryType(res, b.tp, b.ctx)
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

func (b *builtinCastJSONAsDurationSig) vectorized() bool {
	return false
}

func (b *builtinCastJSONAsDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCastDecimalAsJSONSig) vectorized() bool {
	return true
}

func (b *builtinCastDecimalAsJSONSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDecimal(b.ctx, input, buf); err != nil {
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
		result.AppendJSON(json.CreateBinary(f))
	}
	return nil
}

func (b *builtinCastDurationAsJSONSig) vectorized() bool {
	return false
}

func (b *builtinCastDurationAsJSONSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}
