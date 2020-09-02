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
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// vecEvalDecimal evals a builtinGreatestDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(b.ctx, input, result); err != nil {
		return err
	}

	d64s := result.Decimals()
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalDecimal(b.ctx, input, buf); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if result.IsNull(i) {
				continue
			} else if buf.IsNull(i) {
				result.SetNull(i, true)
				continue
			}
			v := buf.GetDecimal(i)
			if v.Compare(&d64s[i]) > 0 {
				d64s[i] = *v
			}
		}
	}
	return nil
}

func (b *builtinGreatestDecimalSig) vectorized() bool {
	return true
}

func (b *builtinLeastDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(b.ctx, input, result); err != nil {
		return err
	}

	d64s := result.Decimals()
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalDecimal(b.ctx, input, buf); err != nil {
			return err
		}

		result.MergeNulls(buf)
		for i := 0; i < n; i++ {
			if result.IsNull(i) {
				continue
			}
			v := buf.GetDecimal(i)
			if v.Compare(&d64s[i]) < 0 {
				d64s[i] = *v
			}
		}
	}
	return nil
}

func (b *builtinLeastDecimalSig) vectorized() bool {
	return true
}

func (b *builtinLeastIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}

	i64s := result.Int64s()
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalInt(b.ctx, input, buf); err != nil {
			return err
		}

		result.MergeNulls(buf)
		for i := 0; i < n; i++ {
			if result.IsNull(i) {
				continue
			}
			v := buf.GetInt64(i)
			if v < i64s[i] {
				i64s[i] = v
			}
		}
	}
	return nil
}

func (b *builtinLeastIntSig) vectorized() bool {
	return true
}

func (b *builtinGreatestIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}

	i64s := result.Int64s()
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalInt(b.ctx, input, buf); err != nil {
			return err
		}

		result.MergeNulls(buf)
		v := buf.Int64s()
		for i := 0; i < n; i++ {
			if result.IsNull(i) {
				continue
			}
			if v[i] > i64s[i] {
				i64s[i] = v[i]
			}
		}
	}
	return nil
}

func (b *builtinGreatestIntSig) vectorized() bool {
	return true
}

func (b *builtinGEIntSig) vectorized() bool {
	return true
}

func (b *builtinGEIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err = b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err = b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType().Flag), mysql.HasUnsignedFlag(b.args[1].GetType().Flag), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfGE(result.Int64s())
	return nil
}

func (b *builtinLeastRealSig) vectorized() bool {
	return true
}

func (b *builtinLeastRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}

	f64s := result.Float64s()
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalReal(b.ctx, input, buf); err != nil {
			return err
		}

		result.MergeNulls(buf)
		v := buf.Float64s()
		for i := 0; i < n; i++ {
			if result.IsNull(i) {
				continue
			}
			if v[i] < f64s[i] {
				f64s[i] = v[i]
			}
		}
	}
	return nil
}

func (b *builtinLeastStringSig) vectorized() bool {
	return true
}

func (b *builtinLeastStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalString(b.ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	buf1, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)

	buf2, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)

	src := result
	arg := buf1
	dst := buf2
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalString(b.ctx, input, arg); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if src.IsNull(i) || arg.IsNull(i) {
				dst.AppendNull()
				continue
			}
			srcStr := src.GetString(i)
			argStr := arg.GetString(i)
			if types.CompareString(srcStr, argStr, b.collation) < 0 {
				dst.AppendString(srcStr)
			} else {
				dst.AppendString(argStr)
			}
		}
		src, dst = dst, src
		arg.ReserveString(n)
		dst.ReserveString(n)
	}
	if len(b.args)%2 == 0 {
		src.CopyConstruct(result)
	}
	return nil
}

func (b *builtinEQIntSig) vectorized() bool {
	return true
}

func (b *builtinEQIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType().Flag), mysql.HasUnsignedFlag(b.args[1].GetType().Flag), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfEQ(result.Int64s())
	return nil
}

func (b *builtinNEIntSig) vectorized() bool {
	return true
}

func (b *builtinNEIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType().Flag), mysql.HasUnsignedFlag(b.args[1].GetType().Flag), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfNE(result.Int64s())
	return nil
}

func (b *builtinGTIntSig) vectorized() bool {
	return true
}

func (b *builtinGTIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType().Flag), mysql.HasUnsignedFlag(b.args[1].GetType().Flag), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfGT(result.Int64s())
	return nil
}

func (b *builtinNullEQIntSig) vectorized() bool {
	return true
}

func (b *builtinNullEQIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	result.ResizeInt64(n, false)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType().Flag), mysql.HasUnsignedFlag(b.args[1].GetType().Flag), buf0, buf1, result)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		isNull0 := buf0.IsNull(i)
		isNull1 := buf1.IsNull(i)
		if isNull0 && isNull1 {
			i64s[i] = 1
		} else if isNull0 || isNull1 || i64s[i] != 0 {
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinIntervalIntSig) vectorized() bool {
	return true
}

func (b *builtinIntervalIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	var err error
	if err = b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}
	i64s := result.Int64s()
	var idx int
	for i, v := range i64s {
		if result.IsNull(i) {
			result.SetNull(i, false)
			i64s[i] = -1
			continue
		}
		idx, err = b.binSearch(v, mysql.HasUnsignedFlag(b.args[0].GetType().Flag), b.args[1:], input.GetRow(i))
		if err != nil {
			return err
		}
		i64s[i] = int64(idx)
	}
	return nil
}

func (b *builtinIntervalRealSig) vectorized() bool {
	return true
}

func (b *builtinIntervalRealSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}

	f64s := buf.Float64s()
	result.ResizeInt64(n, false)
	res := result.Int64s()
	var idx int
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			res[i] = -1
			continue
		}
		idx, err = b.binSearch(f64s[i], b.args[1:], input.GetRow(i))
		if err != nil {
			return err
		}
		res[i] = int64(idx)
	}
	return nil
}

func (b *builtinLEIntSig) vectorized() bool {
	return true
}

func (b *builtinLEIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType().Flag), mysql.HasUnsignedFlag(b.args[1].GetType().Flag), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfLE(result.Int64s())
	return nil
}

func (b *builtinLTIntSig) vectorized() bool {
	return true
}

func (b *builtinLTIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType().Flag), mysql.HasUnsignedFlag(b.args[1].GetType().Flag), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfLT(result.Int64s())
	return nil
}

func vecResOfLT(res []int64) {
	n := len(res)
	for i := 0; i < n; i++ {
		if res[i] < 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

func vecResOfNE(res []int64) {
	n := len(res)
	for i := 0; i < n; i++ {
		if res[i] != 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

func vecResOfEQ(res []int64) {
	n := len(res)
	for i := 0; i < n; i++ {
		if res[i] == 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

func vecResOfLE(res []int64) {
	n := len(res)
	for i := 0; i < n; i++ {
		if res[i] <= 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

func vecResOfGT(res []int64) {
	n := len(res)
	for i := 0; i < n; i++ {
		if res[i] > 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

func vecResOfGE(res []int64) {
	n := len(res)
	for i := 0; i < n; i++ {
		if res[i] >= 0 {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
}

//vecCompareInt is vectorized CompareInt()
func vecCompareInt(isUnsigned0, isUnsigned1 bool, largs, rargs, result *chunk.Column) {
	switch {
	case isUnsigned0 && isUnsigned1:
		types.VecCompareUU(largs.Uint64s(), rargs.Uint64s(), result.Int64s())
	case isUnsigned0 && !isUnsigned1:
		types.VecCompareUI(largs.Uint64s(), rargs.Int64s(), result.Int64s())
	case !isUnsigned0 && isUnsigned1:
		types.VecCompareIU(largs.Int64s(), rargs.Uint64s(), result.Int64s())
	case !isUnsigned0 && !isUnsigned1:
		types.VecCompareII(largs.Int64s(), rargs.Int64s(), result.Int64s())
	}
}

func (b *builtinGreatestTimeSig) vectorized() bool {
	return true
}

func (b *builtinGreatestTimeSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	dst, err := b.bufAllocator.get(types.ETTimestamp, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dst)

	sc := b.ctx.GetSessionVars().StmtCtx
	dst.ResizeTime(n, false)
	dstTimes := dst.Times()
	for i := 0; i < n; i++ {
		dstTimes[i] = types.ZeroDatetime
	}
	var argTime types.Time
	for j := 0; j < len(b.args); j++ {
		if err := b.args[j].VecEvalString(b.ctx, input, result); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if result.IsNull(i) || dst.IsNull(i) {
				dst.SetNull(i, true)
				continue
			}
			argTime, err = types.ParseDatetime(sc, result.GetString(i))
			if err != nil {
				if err = handleInvalidTimeError(b.ctx, err); err != nil {
					return err
				}
				continue
			}
			if argTime.Compare(dstTimes[i]) > 0 {
				dstTimes[i] = argTime
			}
		}
	}
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if dst.IsNull(i) {
			result.AppendNull()
		} else {
			result.AppendString(dstTimes[i].String())
		}
	}
	return nil
}

func (b *builtinGreatestRealSig) vectorized() bool {
	return true
}

func (b *builtinGreatestRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}

	f64s := result.Float64s()
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalReal(b.ctx, input, buf); err != nil {
			return err
		}

		result.MergeNulls(buf)
		v := buf.Float64s()
		for i := 0; i < n; i++ {
			if result.IsNull(i) {
				continue
			}
			if v[i] > f64s[i] {
				f64s[i] = v[i]
			}
		}
	}
	return nil
}

func (b *builtinLeastTimeSig) vectorized() bool {
	return true
}

func (b *builtinLeastTimeSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	dst, err := b.bufAllocator.get(types.ETTimestamp, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(dst)

	sc := b.ctx.GetSessionVars().StmtCtx
	dst.ResizeTime(n, false)
	dstTimes := dst.Times()
	for i := 0; i < n; i++ {
		dstTimes[i] = types.NewTime(types.MaxDatetime, mysql.TypeDatetime, types.DefaultFsp)
	}
	var argTime types.Time

	var findInvalidTime []bool = make([]bool, n)
	var invalidValue []string = make([]string, n)

	for j := 0; j < len(b.args); j++ {
		if err := b.args[j].VecEvalString(b.ctx, input, result); err != nil {
			return err
		}
		dst.MergeNulls(result)
		for i := 0; i < n; i++ {
			if dst.IsNull(i) {
				continue
			}
			argTime, err = types.ParseDatetime(sc, result.GetString(i))
			if err != nil {
				if err = handleInvalidTimeError(b.ctx, err); err != nil {
					return err
				} else if !findInvalidTime[i] {
					// Make a deep copy here.
					// Otherwise invalidValue will internally change with result.
					invalidValue[i] = string(result.GetBytes(i))
					findInvalidTime[i] = true
				}
				continue
			}
			if argTime.Compare(dstTimes[i]) < 0 {
				dstTimes[i] = argTime
			}
		}
	}
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if dst.IsNull(i) {
			result.AppendNull()
			continue
		}
		if findInvalidTime[i] {
			result.AppendString(invalidValue[i])
		} else {
			result.AppendString(dstTimes[i].String())
		}
	}
	return nil
}

func (b *builtinGreatestStringSig) vectorized() bool {
	return true
}

func (b *builtinGreatestStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalString(b.ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	buf1, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	buf2, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)

	src := result
	arg := buf1
	dst := buf2
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalString(b.ctx, input, arg); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if src.IsNull(i) || arg.IsNull(i) {
				dst.AppendNull()
				continue
			}
			srcStr := src.GetString(i)
			argStr := arg.GetString(i)
			if types.CompareString(srcStr, argStr, b.collation) > 0 {
				dst.AppendString(srcStr)
			} else {
				dst.AppendString(argStr)
			}
		}
		src, dst = dst, src
		arg.ReserveString(n)
		dst.ReserveString(n)
	}
	if len(b.args)%2 == 0 {
		src.CopyConstruct(result)
	}
	return nil
}
