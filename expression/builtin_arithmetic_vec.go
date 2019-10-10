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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (b *builtinArithmeticMultiplyRealSig) vectorized() bool {
	return true
}

func (b *builtinArithmeticMultiplyRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}

	result.MergeNulls(buf)
	x := result.Float64s()
	y := buf.Float64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		x[i] = x[i] * y[i]
		if math.IsInf(x[i], 0) {
			return types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s * %s)", b.args[0].String(), b.args[1].String()))
		}
	}
	return nil
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
	return true
}

func (b *builtinArithmeticMinusRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}

	result.MergeNulls(buf)
	x := result.Float64s()
	y := buf.Float64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if (x[i] > 0 && -y[i] > math.MaxFloat64-x[i]) || (x[i] < 0 && -y[i] < -math.MaxFloat64-x[i]) {
			return types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s - %s)", b.args[0].String(), b.args[1].String()))
		}
		x[i] = x[i] - y[i]
	}
	return nil
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
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}

	result.MergeNulls(buf)
	x := result.Float64s()
	y := buf.Float64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if (x[i] > 0 && y[i] > math.MaxFloat64-x[i]) || (x[i] < 0 && y[i] < -math.MaxFloat64-x[i]) {
			return types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s + %s)", b.args[0].String(), b.args[1].String()))
		}
		x[i] = x[i] + y[i]
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
	return true
}

func (b *builtinArithmeticDivideRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}

	result.MergeNulls(buf)
	x := result.Float64s()
	y := buf.Float64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if y[i] == 0 {
			if err := handleDivisionByZeroError(b.ctx); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}

		x[i] = x[i] / y[i]
		if math.IsInf(x[i], 0) {
			return types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s / %s)", b.args[0].String(), b.args[1].String()))
		}
	}
	return nil
}

func (b *builtinArithmeticIntDivideIntSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticIntDivideIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticPlusIntSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticPlusIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticPlusDecimalSig) vectorized() bool {
	return true
}

func (b *builtinArithmeticPlusDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalDecimal(b.ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalDecimal(b.ctx, input, buf); err != nil {
		return err
	}

	result.MergeNulls(buf)
	x := result.Decimals()
	y := buf.Decimals()
	to := new(types.MyDecimal)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if err = types.DecimalAdd(&x[i], &y[i], to); err != nil {
			return err
		}
		x[i] = *to
	}
	return nil
}

func (b *builtinArithmeticMultiplyIntUnsignedSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticMultiplyIntUnsignedSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}
