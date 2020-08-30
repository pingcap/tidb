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
	"strconv"
	"strings"

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

		tp := tms[i].Type()
		if tp == mysql.TypeDatetime || tp == mysql.TypeTimestamp {
			tms[i].SetFsp(types.MaxFsp)
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
	dec := new(types.MyDecimal)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		*dec = types.MyDecimal{}
		times[i].FillNumber(dec)
		dec, err = types.ProduceDecWithSpecifiedTp(dec, b.tp, sc)
		if err != nil {
			return err
		}
		decs[i] = *dec
	}
	return nil
}

func (b *builtinCastDurationAsIntSig) vectorized() bool {
	return true
}

func (b *builtinCastDurationAsIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDuration, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDuration(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	var duration types.Duration
	ds := buf.GoDurations()
	fsp := int8(b.args[0].GetType().Decimal)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		duration.Duration = ds[i]
		duration.Fsp = fsp
		dur, err := duration.RoundFrac(types.DefaultFsp)
		if err != nil {
			return err
		}
		i64s[i], err = dur.ToNumber().ToInt()
		if err != nil {
			return err
		}
	}
	return nil

}

func (b *builtinCastIntAsTimeSig) vectorized() bool {
	return true
}

func (b *builtinCastIntAsTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
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
	result.MergeNulls(buf)
	times := result.Times()
	i64s := buf.Int64s()
	stmt := b.ctx.GetSessionVars().StmtCtx
	fsp := int8(b.tp.Decimal)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			continue
		}
		tm, err := types.ParseTimeFromNum(stmt, i64s[i], b.tp.Tp, fsp)
		if err != nil {
			if err = handleInvalidTimeError(b.ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		times[i] = tm
		if b.tp.Tp == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			times[i].SetCoreTime(types.FromDate(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0))
		}
	}
	return nil
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
	return true
}

func (b *builtinCastJSONAsRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeFloat64(n, false)
	result.MergeNulls(buf)
	f64s := result.Float64s()
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		f64s[i], err = types.ConvertJSONToFloat(sc, buf.GetJSON(i))
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *builtinCastJSONAsTimeSig) vectorized() bool {
	return true
}

func (b *builtinCastJSONAsTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	times := result.Times()
	stmtCtx := b.ctx.GetSessionVars().StmtCtx
	fsp := int8(b.tp.Decimal)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		s, err := buf.GetJSON(i).Unquote()
		if err != nil {
			return err
		}
		tm, err := types.ParseTime(stmtCtx, s, b.tp.Tp, fsp)
		if err != nil {
			if err = handleInvalidTimeError(b.ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		times[i] = tm
		if b.tp.Tp == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			times[i].SetCoreTime(types.FromDate(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0))
		}
	}
	return nil
}

func (b *builtinCastRealAsTimeSig) vectorized() bool {
	return true
}

func (b *builtinCastRealAsTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	times := result.Times()
	f64s := buf.Float64s()
	stmt := b.ctx.GetSessionVars().StmtCtx
	fsp := int8(b.tp.Decimal)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			continue
		}
		fv := strconv.FormatFloat(f64s[i], 'f', -1, 64)
		if fv == "0" {
			times[i] = types.ZeroTime
			continue
		}
		tm, err := types.ParseTime(stmt, fv, b.tp.Tp, fsp)
		if err != nil {
			if err = handleInvalidTimeError(b.ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		times[i] = tm
		if b.tp.Tp == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			times[i].SetCoreTime(types.FromDate(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0))
		}
	}
	return nil
}

func (b *builtinCastDecimalAsDecimalSig) vectorized() bool {
	return true
}

func (b *builtinCastDecimalAsDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalDecimal(b.ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	decs := result.Decimals()
	sc := b.ctx.GetSessionVars().StmtCtx
	conditionUnionAndUnsigned := b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag)
	dec := new(types.MyDecimal)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		*dec = types.MyDecimal{}
		if !(conditionUnionAndUnsigned && decs[i].IsNegative()) {
			*dec = decs[i]
		}
		dec, err := types.ProduceDecWithSpecifiedTp(dec, b.tp, sc)
		if err != nil {
			return err
		}
		decs[i] = *dec
	}
	return nil
}

func (b *builtinCastDurationAsTimeSig) vectorized() bool {
	return true
}

func (b *builtinCastDurationAsTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDuration, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDuration(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	var duration types.Duration
	ds := buf.GoDurations()
	times := result.Times()
	stmtCtx := b.ctx.GetSessionVars().StmtCtx
	fsp := int8(b.tp.Decimal)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		duration.Duration = ds[i]
		duration.Fsp = fsp
		tm, err := duration.ConvertToTime(stmtCtx, b.tp.Tp)
		if err != nil {
			if err = handleInvalidTimeError(b.ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		tm, err = tm.RoundFrac(stmtCtx, fsp)
		if err != nil {
			return err
		}
		times[i] = tm
	}
	return nil
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
		if types.ErrOverflow.Equal(err) {
			err = b.ctx.GetSessionVars().StmtCtx.HandleOverflow(err, err)
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
	return true
}

func (b *builtinCastRealAsDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}
	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)
	bufreal := buf.Float64s()
	resdecimal := result.Decimals()
	for i := 0; i < n; i++ {
		if !b.inUnion || bufreal[i] >= 0 {
			if err = resdecimal[i].FromFloat64(bufreal[i]); err != nil {
				if types.ErrOverflow.Equal(err) {
					warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", b.args[0])
					err = b.ctx.GetSessionVars().StmtCtx.HandleOverflow(err, warnErr)
				} else if types.ErrTruncated.Equal(err) {
					// This behavior is consistent with MySQL.
					err = nil
				}
				if err != nil {
					return err
				}
			}
		}
		dec, err := types.ProduceDecWithSpecifiedTp(&resdecimal[i], b.tp, b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return err
		}
		resdecimal[i] = *dec
	}
	return nil
}

func (b *builtinCastStringAsIntSig) vectorized() bool {
	return true
}

func (b *builtinCastStringAsIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if b.args[0].GetType().Hybrid() || IsBinaryLiteral(b.args[0]) {
		return b.args[0].VecEvalInt(b.ctx, input, result)
	}
	result.ResizeInt64(n, false)
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	result.MergeNulls(buf)
	sc := b.ctx.GetSessionVars().StmtCtx
	i64s := result.Int64s()
	isUnsigned := mysql.HasUnsignedFlag(b.tp.Flag)
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
			ures, err = types.StrToUint(sc, val, true)
			if !isUnsigned && err == nil && ures > uint64(math.MaxInt64) {
				sc.AppendWarning(types.ErrCastAsSignedOverflow)
			}
			res = int64(ures)
		} else if unionUnsigned {
			res = 0
		} else {
			res, err = types.StrToInt(sc, val, true)
			if err == nil && isUnsigned {
				// If overflow, don't append this warnings
				sc.AppendWarning(types.ErrCastNegIntAsUnsigned)
			}
		}
		res, err = b.handleOverflow(res, val, err, isNegative)
		if err != nil {
			return err
		}
		i64s[i] = res
	}
	return nil
}

func (b *builtinCastStringAsDurationSig) vectorized() bool {
	return true
}

func (b *builtinCastStringAsDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	result.ResizeGoDuration(n, false)
	result.MergeNulls(buf)
	ds := result.GoDurations()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		dur, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, buf.GetString(i), int8(b.tp.Decimal))
		if err != nil {
			if types.ErrTruncatedWrongVal.Equal(err) {
				err = b.ctx.GetSessionVars().StmtCtx.HandleTruncate(err)
			}
			if err != nil {
				return err
			}
			if dur == types.ZeroDuration {
				result.SetNull(i, true)
				continue
			}
		}
		ds[i] = dur.Duration
	}
	return nil
}

func (b *builtinCastDurationAsDecimalSig) vectorized() bool {
	return true
}

func (b *builtinCastDurationAsDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDuration, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDuration(b.ctx, input, buf); err != nil {
		return err
	}
	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)
	d64s := result.Decimals()
	var duration types.Duration
	ds := buf.GoDurations()
	sc := b.ctx.GetSessionVars().StmtCtx
	fsp := int8(b.args[0].GetType().Decimal)
	if fsp, err = types.CheckFsp(int(fsp)); err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		duration.Duration = ds[i]
		duration.Fsp = fsp
		res, err := types.ProduceDecWithSpecifiedTp(duration.ToNumber(), b.tp, sc)
		if err != nil {
			return err
		}
		d64s[i] = *res
	}
	return nil
}

func (b *builtinCastIntAsDecimalSig) vectorized() bool {
	return true
}

func (b *builtinCastIntAsDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	isUnsignedTp := mysql.HasUnsignedFlag(b.tp.Flag)
	isUnsignedArgs0 := mysql.HasUnsignedFlag(b.args[0].GetType().Flag)
	nums := buf.Int64s()
	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)
	decs := result.Decimals()
	sc := b.ctx.GetSessionVars().StmtCtx
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

		dec, err = types.ProduceDecWithSpecifiedTp(dec, b.tp, sc)
		if err != nil {
			return err
		}
		decs[i] = *dec
	}
	return nil
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
	return true
}

func (b *builtinCastDurationAsRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDuration, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeFloat64(n, false)
	result.MergeNulls(buf)
	f64s := result.Float64s()

	var duration types.Duration
	fsp := int8(b.args[0].GetType().Decimal)
	if fsp, err = types.CheckFsp(int(fsp)); err != nil {
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
	return true
}

func (b *builtinCastRealAsDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}
	result.ResizeGoDuration(n, false)
	result.MergeNulls(buf)
	f64s := buf.Float64s()
	ds := result.GoDurations()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		dur, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, strconv.FormatFloat(f64s[i], 'f', -1, 64), int8(b.tp.Decimal))
		if err != nil {
			return err
		}
		ds[i] = dur.Duration
	}
	return nil
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
	return true
}

func (b *builtinCastDecimalAsTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeTime(n, false)
	result.MergeNulls(buf)
	times := result.Times()
	decimals := buf.Decimals()
	stmt := b.ctx.GetSessionVars().StmtCtx
	fsp := int8(b.tp.Decimal)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			continue
		}
		tm, err := types.ParseTimeFromFloatString(stmt, string(decimals[i].ToString()), b.tp.Tp, fsp)
		if err != nil {
			if err = handleInvalidTimeError(b.ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		times[i] = tm
		if b.tp.Tp == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			times[i].SetCoreTime(types.FromDate(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0))
		}
	}
	return nil
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
	return true
}

func (b *builtinCastTimeAsTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalTime(b.ctx, input, result); err != nil {
		return err
	}

	times := result.Times()
	stmt := b.ctx.GetSessionVars().StmtCtx
	fsp := int8(b.tp.Decimal)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		res, err := times[i].Convert(stmt, b.tp.Tp)
		if err != nil {
			if err = handleInvalidTimeError(b.ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		tm, err := res.RoundFrac(stmt, fsp)
		if err != nil {
			return err
		}
		times[i] = tm
		if b.tp.Tp == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			times[i].SetCoreTime(types.FromDate(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0))
			times[i].SetType(b.tp.Tp)
		}
	}
	return nil
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
	return true
}

func (b *builtinCastJSONAsDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalJSON(b.ctx, input, buf); err != nil {
		return err
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)
	res := result.Decimals()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		tempres, err := types.ConvertJSONToDecimal(sc, buf.GetJSON(i))
		if err != nil {
			return err
		}
		tempres, err = types.ProduceDecWithSpecifiedTp(tempres, b.tp, sc)
		if err != nil {
			return err
		}
		res[i] = *tempres
	}
	return nil
}

func (b *builtinCastStringAsRealSig) vectorized() bool {
	return true
}

func (b *builtinCastStringAsRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if IsBinaryLiteral(b.args[0]) {
		return b.args[0].VecEvalReal(b.ctx, input, result)
	}
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeFloat64(n, false)
	result.MergeNulls(buf)
	ret := result.Float64s()
	sc := b.ctx.GetSessionVars().StmtCtx

	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		res, err := types.StrToFloat(sc, buf.GetString(i), true)
		if err != nil {
			return err
		}
		if b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag) && res < 0 {
			res = 0
		}
		res, err = types.ProduceFloatWithSpecifiedTp(res, b.tp, sc)
		if err != nil {
			return err
		}
		ret[i] = res
	}
	return nil
}

func (b *builtinCastStringAsDecimalSig) vectorized() bool {
	return true
}

func (b *builtinCastStringAsDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	if IsBinaryLiteral(b.args[0]) {
		return b.args[0].VecEvalDecimal(b.ctx, input, result)
	}
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)
	res := result.Decimals()
	stmtCtx := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		val := strings.TrimSpace(buf.GetString(i))
		isNegative := len(val) > 0 && val[0] == '-'
		dec := new(types.MyDecimal)
		if !(b.inUnion && mysql.HasUnsignedFlag(b.tp.Flag) && isNegative) {
			if err := stmtCtx.HandleTruncate(dec.FromString([]byte(val))); err != nil {
				return err
			}
			dec, err := types.ProduceDecWithSpecifiedTp(dec, b.tp, stmtCtx)
			if err != nil {
				return err
			}
			res[i] = *dec
		}
	}
	return nil
}

func (b *builtinCastStringAsTimeSig) vectorized() bool {
	return true
}

func (b *builtinCastStringAsTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
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
	stmtCtx := b.ctx.GetSessionVars().StmtCtx
	fsp := int8(b.tp.Decimal)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		tm, err := types.ParseTime(stmtCtx, buf.GetString(i), b.tp.Tp, fsp)
		if err != nil {
			if err = handleInvalidTimeError(b.ctx, err); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		times[i] = tm
		if b.tp.Tp == mysql.TypeDate {
			// Truncate hh:mm:ss part if the type is Date.
			times[i].SetCoreTime(types.FromDate(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0))
		}
	}
	return nil
}

func (b *builtinCastDecimalAsIntSig) vectorized() bool {
	return true
}

func (b *builtinCastDecimalAsIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(b.ctx, input, buf); err != nil {
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
		err = d64s[i].Round(&to, 0, types.ModeHalfEven)
		if err != nil {
			return err
		}

		if !mysql.HasUnsignedFlag(b.tp.Flag) {
			i64s[i], err = to.ToInt()
		} else if b.inUnion && to.IsNegative() {
			i64s[i] = 0
		} else {
			var uintRes uint64
			uintRes, err = to.ToUint()
			i64s[i] = int64(uintRes)
		}

		if types.ErrOverflow.Equal(err) {
			warnErr := types.ErrTruncatedWrongVal.GenWithStackByArgs("DECIMAL", d64s[i])
			err = b.ctx.GetSessionVars().StmtCtx.HandleOverflow(err, warnErr)
		}

		if err != nil {
			return err
		}
	}
	return nil
}

func (b *builtinCastDecimalAsDurationSig) vectorized() bool {
	return true
}

func (b *builtinCastDecimalAsDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeGoDuration(n, false)
	result.MergeNulls(buf)
	args := buf.Decimals()
	ds := result.GoDurations()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		dur, err := types.ParseDuration(b.ctx.GetSessionVars().StmtCtx, string(args[i].ToString()), int8(b.tp.Decimal))
		if err != nil {
			if types.ErrTruncatedWrongVal.Equal(err) {
				err = b.ctx.GetSessionVars().StmtCtx.HandleTruncate(err)
			}
			if err != nil {
				return err
			}
			if dur == types.ZeroDuration {
				result.SetNull(i, true)
				continue
			}
		}
		ds[i] = dur.Duration
	}
	return nil
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
	return true
}

func (b *builtinCastJSONAsDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalJSON(b.ctx, input, buf); err != nil {
		return err
	}

	ctx := b.ctx.GetSessionVars().StmtCtx
	result.ResizeGoDuration(n, false)
	result.MergeNulls(buf)
	var dur types.Duration
	ds := result.GoDurations()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		s, err := buf.GetJSON(i).Unquote()
		if err != nil {
			return nil
		}
		dur, err = types.ParseDuration(ctx, s, int8(b.tp.Decimal))
		if types.ErrTruncatedWrongVal.Equal(err) {
			err = ctx.HandleTruncate(err)
		}
		if err != nil {
			return err
		}
		ds[i] = dur.Duration
	}
	return nil
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
	return true
}

func (b *builtinCastDurationAsJSONSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDuration, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalDuration(b.ctx, input, buf); err != nil {
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
		result.AppendJSON(json.CreateBinary(dur.String()))
	}
	return nil
}
