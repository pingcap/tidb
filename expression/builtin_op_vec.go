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
	"fmt"
	"math"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/chunk"
)

func (b *builtinTimeIsNullSig) vectorized() bool {
	return true
}

func (b *builtinTimeIsNullSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDatetime, numRows)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	if err := b.args[0].VecEvalTime(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(numRows, false)
	i64s := result.Int64s()
	for i := 0; i < numRows; i++ {
		if buf.IsNull(i) {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinLogicOrSig) vectorized() bool {
	return true
}

func (b *builtinLogicOrSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
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
	arg1s := buf.Int64s()

	for i := 0; i < n; i++ {
		isNull0 := result.IsNull(i)
		isNull1 := buf.IsNull(i)
		// Because buf is used to store the conversion of args[0] in place, it could
		// be that args[0] is null and args[1] is nonzero, in which case the result
		// is 1. In these cases, we need to clear the null bit mask of the corresponding
		// row in result.
		// See https://dev.mysql.com/doc/refman/5.7/en/logical-operators.html#operator_or
		isNull := false
		if (!isNull0 && i64s[i] != 0) || (!isNull1 && arg1s[i] != 0) {
			i64s[i] = 1
		} else if isNull0 || isNull1 {
			isNull = true
		} else {
			i64s[i] = 0
		}
		if isNull != isNull0 {
			result.SetNull(i, isNull)
		}
	}
	return nil
}

func (b *builtinBitOrSig) vectorized() bool {
	return true
}

func (b *builtinBitOrSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, numRows)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}
	arg0s := result.Int64s()
	arg1s := buf.Int64s()
	result.MergeNulls(buf)
	for i := 0; i < numRows; i++ {
		arg0s[i] |= arg1s[i]
	}
	return nil
}

func (b *builtinDecimalIsFalseSig) vectorized() bool {
	return true
}

func (b *builtinDecimalIsFalseSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()

	buf, err := b.bufAllocator.get(types.ETDecimal, numRows)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(b.ctx, input, buf); err != nil {
		return err
	}

	decs := buf.Decimals()
	result.ResizeInt64(numRows, false)
	i64s := result.Int64s()

	for i := 0; i < numRows; i++ {
		isNull := buf.IsNull(i)
		if b.keepNull && isNull {
			result.SetNull(i, true)
			continue
		}
		if isNull || !decs[i].IsZero() {
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinIntIsFalseSig) vectorized() bool {
	return true
}

func (b *builtinIntIsFalseSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}
	i64s := result.Int64s()
	for i := 0; i < numRows; i++ {
		isNull := result.IsNull(i)
		if b.keepNull && isNull {
			continue
		}
		if isNull {
			i64s[i] = 0
			result.SetNull(i, false)
		} else if i64s[i] != 0 {
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinUnaryMinusRealSig) vectorized() bool {
	return true
}

func (b *builtinUnaryMinusRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	var err error
	if err = b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	f64s := result.Float64s()
	for i := 0; i < n; i++ {
		f64s[i] = -f64s[i]
	}
	return nil
}

func (b *builtinBitNegSig) vectorized() bool {
	return true
}

func (b *builtinBitNegSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	args := result.Int64s()
	for i := 0; i < n; i++ {
		args[i] = ^args[i]
	}
	return nil
}

func (b *builtinUnaryMinusDecimalSig) vectorized() bool {
	return true
}

func (b *builtinUnaryMinusDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalDecimal(b.ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	decs := result.Decimals()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		decs[i] = *types.DecimalNeg(&decs[i])
	}
	return nil
}

func (b *builtinIntIsNullSig) vectorized() bool {
	return true
}

func (b *builtinIntIsNullSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}

	i64s := result.Int64s()
	for i := 0; i < len(i64s); i++ {
		if result.IsNull(i) {
			i64s[i] = 1
			result.SetNull(i, false)
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinRealIsNullSig) vectorized() bool {
	return true
}

func (b *builtinRealIsNullSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, numRows)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	if err := b.args[0].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(numRows, false)
	i64s := result.Int64s()
	for i := 0; i < numRows; i++ {
		if buf.IsNull(i) {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinUnaryNotRealSig) vectorized() bool {
	return true
}

func (b *builtinUnaryNotRealSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
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

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if f64s[i] == 0 {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinLogicAndSig) vectorized() bool {
	return true
}

func (b *builtinLogicAndSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
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

	i64s := result.Int64s()
	arg1 := buf1.Int64s()

	for i := 0; i < n; i++ {
		isNull0 := result.IsNull(i)
		if !isNull0 && i64s[i] == 0 {
			result.SetNull(i, false)
			continue
		}

		isNull1 := buf1.IsNull(i)
		if !isNull1 && arg1[i] == 0 {
			i64s[i] = 0
			result.SetNull(i, false)
			continue
		}

		if isNull0 || isNull1 {
			result.SetNull(i, true)
			continue
		}

		i64s[i] = 1
	}

	return nil
}

func (b *builtinBitXorSig) vectorized() bool {
	return true
}

func (b *builtinBitXorSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, numRows)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}
	arg0s := result.Int64s()
	arg1s := buf.Int64s()
	result.MergeNulls(buf)
	for i := 0; i < numRows; i++ {
		arg0s[i] ^= arg1s[i]
	}
	return nil
}

func (b *builtinLogicXorSig) vectorized() bool {
	return true
}

func (b *builtinLogicXorSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
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
	arg1s := buf.Int64s()
	// Returns NULL if either operand is NULL.
	// See https://dev.mysql.com/doc/refman/5.7/en/logical-operators.html#operator_xor
	result.MergeNulls(buf)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		arg0 := i64s[i]
		arg1 := arg1s[i]
		if (arg0 != 0 && arg1 != 0) || (arg0 == 0 && arg1 == 0) {
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinBitAndSig) vectorized() bool {
	return true
}

func (b *builtinBitAndSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, numRows)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}
	arg0s := result.Int64s()
	arg1s := buf.Int64s()
	result.MergeNulls(buf)
	for i := 0; i < numRows; i++ {
		arg0s[i] &= arg1s[i]
	}
	return nil
}

func (b *builtinRealIsFalseSig) vectorized() bool {
	return true
}

func (b *builtinRealIsFalseSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, numRows)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(numRows, false)
	i64s := result.Int64s()
	bufI64s := buf.Int64s()
	for i := 0; i < numRows; i++ {
		isNull := buf.IsNull(i)
		if b.keepNull && isNull {
			result.SetNull(i, true)
			continue
		}
		if isNull || bufI64s[i] != 0 {
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinUnaryMinusIntSig) vectorized() bool {
	return true
}

func (b *builtinUnaryMinusIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	args := result.Int64s()
	if mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		for i := 0; i < n; i++ {
			if result.IsNull(i) {
				continue
			}
			if uint64(args[i]) > uint64(-math.MinInt64) {
				return types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("-%v", uint64(args[i])))
			}
			args[i] = -args[i]
		}
	} else {
		for i := 0; i < n; i++ {
			if result.IsNull(i) {
				continue
			}
			if args[i] == math.MinInt64 {
				return types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("-%v", args[i]))
			}
			args[i] = -args[i]
		}
	}
	return nil
}

func (b *builtinUnaryNotDecimalSig) vectorized() bool {
	return true
}

func (b *builtinUnaryNotDecimalSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(b.ctx, input, buf); err != nil {
		return err
	}
	decs := buf.Decimals()

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if decs[i].IsZero() {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinUnaryNotIntSig) vectorized() bool {
	return true
}

func (b *builtinUnaryNotIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}

	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if i64s[i] == 0 {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinDecimalIsNullSig) vectorized() bool {
	return true
}

func (b *builtinDecimalIsNullSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDecimal, numRows)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	if err := b.args[0].VecEvalDecimal(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(numRows, false)
	i64s := result.Int64s()
	for i := 0; i < numRows; i++ {
		if buf.IsNull(i) {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinLeftShiftSig) vectorized() bool {
	return true
}

func (b *builtinLeftShiftSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, numRows)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}
	arg0s := result.Int64s()
	arg1s := buf.Int64s()
	result.MergeNulls(buf)
	for i := 0; i < numRows; i++ {
		arg0s[i] = int64(uint64(arg0s[i]) << uint64(arg1s[i]))
	}
	return nil
}

func (b *builtinRightShiftSig) vectorized() bool {
	return true
}

func (b *builtinRightShiftSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, numRows)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}
	arg0s := result.Int64s()
	arg1s := buf.Int64s()
	result.MergeNulls(buf)
	for i := 0; i < numRows; i++ {
		arg0s[i] = int64(uint64(arg0s[i]) >> uint64(arg1s[i]))
	}
	return nil
}

func (b *builtinRealIsTrueSig) vectorized() bool {
	return true
}

func (b *builtinRealIsTrueSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, numRows)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	if err := b.args[0].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(numRows, false)
	f64s := buf.Float64s()
	i64s := result.Int64s()
	for i := 0; i < numRows; i++ {
		isNull := buf.IsNull(i)
		if b.keepNull && isNull {
			result.SetNull(i, true)
			continue
		}
		if isNull || f64s[i] == 0 {
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinDecimalIsTrueSig) vectorized() bool {
	return true
}

func (b *builtinDecimalIsTrueSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDecimal, numRows)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(b.ctx, input, buf); err != nil {
		return err
	}

	decs := buf.Decimals()
	result.ResizeInt64(numRows, false)
	i64s := result.Int64s()

	for i := 0; i < numRows; i++ {
		isNull := buf.IsNull(i)
		if b.keepNull && isNull {
			result.SetNull(i, true)
			continue
		}
		if isNull || decs[i].IsZero() {
			i64s[i] = 0
		} else {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinIntIsTrueSig) vectorized() bool {
	return true
}

func (b *builtinIntIsTrueSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}
	i64s := result.Int64s()
	for i := 0; i < numRows; i++ {
		isNull := result.IsNull(i)
		if b.keepNull && isNull {
			continue
		}
		if isNull {
			i64s[i] = 0
			result.SetNull(i, false)
		} else if i64s[i] != 0 {
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinDurationIsNullSig) vectorized() bool {
	return true
}

func (b *builtinDurationIsNullSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	numRows := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDuration, numRows)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	if err := b.args[0].VecEvalDuration(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(numRows, false)
	i64s := result.Int64s()
	for i := 0; i < numRows; i++ {
		if buf.IsNull(i) {
			i64s[i] = 1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}
