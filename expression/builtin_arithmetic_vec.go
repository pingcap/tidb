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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"math"
)

func (b *builtinArithmeticMultiplyRealSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticMultiplyRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticDivideDecimalSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticDivideDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticModIntSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticModIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticMinusRealSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticMinusRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticMinusDecimalSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticMinusDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticMinusIntSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticMinusIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticModRealSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticModRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticModDecimalSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticModDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticPlusRealSig) vectorized() bool {
	return true
}

func (b *builtinArithmeticPlusRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	lh, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(lh)

	rh, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(rh)

	if err := b.args[0].VecEvalReal(b.ctx, input, lh); err != nil {
		return err
	}

	if err := b.args[1].VecEvalReal(b.ctx, input, rh); err != nil {
		return err
	}

	result.ResizeFloat64(n, false)
	result.MergeNulls(lh)
	result.MergeNulls(rh)

	lhf64s := lh.Float64s()
	rhf64s := rh.Float64s()
	resf64s := result.Float64s()

	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		lh := lhf64s[i]
		rh := rhf64s[i]

		if (lh > 0 && rh > math.MaxFloat64-lh) || (lh < 0 && rh < -math.MaxFloat64-lh) {
			result.SetNull(i, true)
		} else {
			resf64s[i] = lh + rh
		}
	}
	return nil
}

func (b *builtinArithmeticMultiplyDecimalSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticMultiplyDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticIntDivideDecimalSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticIntDivideDecimalSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticMultiplyIntSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticMultiplyIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticDivideRealSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticDivideRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticIntDivideIntSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticIntDivideIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticPlusIntSig) vectorized() bool {
	return true
}

func (b *builtinArithmeticPlusIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	lh, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(lh)

	rh, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(rh)

	if err := b.args[0].VecEvalInt(b.ctx, input, lh); err != nil {
		return err
	}

	if err := b.args[1].VecEvalInt(b.ctx, input, rh); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(lh)
	result.MergeNulls(rh)

	lhi64s := lh.Int64s()
	rhi64s := rh.Int64s()
	resulti64s := result.Int64s()

	isLHSUnsigned := mysql.HasUnsignedFlag(b.args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(b.args[1].GetType().Flag)

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		b.plusUU(result, lhi64s, rhi64s, resulti64s)
	case isLHSUnsigned && !isRHSUnsigned:
		b.plusUS(result, lhi64s, rhi64s, resulti64s)
	case !isLHSUnsigned && isRHSUnsigned:
		b.plusSU(result, lhi64s, rhi64s, resulti64s)
	case !isLHSUnsigned && !isRHSUnsigned:
		b.plusSS(result, lhi64s, rhi64s, resulti64s)
	}
	return nil
}
func (b *builtinArithmeticPlusIntSig) plusUU(result *chunk.Column, lhi64s, rhi64s, resi64s []int64) {
	for i := 0; i < len(lhi64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lh, rh := lhi64s[i], rhi64s[i]

		if uint64(lh) > math.MaxUint64-uint64(rh) {
			result.SetNull(i, true)
			continue
		}

		resi64s[i] = lh + rh
	}
}

func (b *builtinArithmeticPlusIntSig) plusUS(result *chunk.Column, lhi64s, rhi64s, resi64s []int64) {
	for i := 0; i < len(lhi64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lh, rh := lhi64s[i], rhi64s[i]

		if rh < 0 && uint64(-rh) > uint64(lh) {
			result.SetNull(i, true)
			continue
		}
		if rh > 0 && uint64(lh) > math.MaxUint64-uint64(lh) {
			result.SetNull(i, true)
			continue
		}

		resi64s[i] = lh + rh
	}

}

func (b *builtinArithmeticPlusIntSig) plusSU(result *chunk.Column, lhi64s, rhi64s, resi64s []int64) {
	for i := 0; i < len(lhi64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lh, rh := lhi64s[i], rhi64s[i]

		if lh < 0 && uint64(-lh) > uint64(rh) {
			result.SetNull(i, true)
			continue
		}
		if lh > 0 && uint64(rh) > math.MaxUint64-uint64(lh) {
			result.SetNull(i, true)
			continue
		}

		resi64s[i] = lh + rh
	}
}
func (b *builtinArithmeticPlusIntSig) plusSS(result *chunk.Column, lhi64s, rhi64s, resi64s []int64) {
	for i := 0; i < len(lhi64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lh, rh := lhi64s[i], rhi64s[i]

		if (lh > 0 && rh > math.MaxInt64-lh) || (lh < 0 && rh < math.MinInt64-lh) {
			result.SetNull(i, true)
			continue
		}

		resi64s[i] = lh + rh
	}
}

func (b *builtinArithmeticPlusDecimalSig) vectorized() bool {
	return true
}

func (b *builtinArithmeticPlusDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	lh, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(lh)

	rh, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(rh)

	if err := b.args[0].VecEvalDecimal(b.ctx, input, lh); err != nil {
		return err
	}

	if err := b.args[1].VecEvalDecimal(b.ctx, input, rh); err != nil {
		return err
	}

	result.ResizeDecimal(n, false)
	result.MergeNulls(lh)
	result.MergeNulls(rh)

	lhDecimals := lh.Decimals()
	rhDecimals := rh.Decimals()
	resDecimals := result.Decimals()

	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		lh := lhDecimals[i]
		rh := rhDecimals[i]

		c := &types.MyDecimal{}
		err = types.DecimalAdd(&lh, &rh, c)
		if err != nil {
			result.SetNull(i, true)
		} else {
			resDecimals[i] = *c
		}

	}
	return nil
}

func (b *builtinArithmeticMultiplyIntUnsignedSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticMultiplyIntUnsignedSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}
