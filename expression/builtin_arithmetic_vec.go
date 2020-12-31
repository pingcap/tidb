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
	"github.com/pingcap/parser/terror"
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
	return true
}

func (b *builtinArithmeticDivideDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
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
	var to types.MyDecimal
	var frac int
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		err = types.DecimalDiv(&x[i], &y[i], &to, types.DivFracIncr)
		if err == types.ErrDivByZero {
			if err = handleDivisionByZeroError(b.ctx); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		} else if err == types.ErrTruncated {
			if err = sc.HandleTruncate(errTruncatedWrongValue.GenWithStackByArgs("DECIMAL", to)); err != nil {
				return err
			}
		} else if err == nil {
			_, frac = to.PrecisionAndFrac()
			if frac < b.baseBuiltinFunc.tp.Decimal {
				if err = to.Round(&to, b.baseBuiltinFunc.tp.Decimal, types.ModeHalfEven); err != nil {
					return err
				}
			}
		} else {
			return err
		}
		x[i] = to
	}
	return nil
}

func (b *builtinArithmeticModIntSig) vectorized() bool {
	return true
}

func (b *builtinArithmeticModIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	lh, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(lh)

	if err := b.args[0].VecEvalInt(b.ctx, input, lh); err != nil {
		return err
	}
	// reuse result as rh to avoid buf allocate
	if err := b.args[1].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}

	isLHSUnsigned := mysql.HasUnsignedFlag(b.args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(b.args[1].GetType().Flag)

	rh := result
	switch {
	case isLHSUnsigned && isRHSUnsigned:
		err = b.modUU(lh, rh)
	case isLHSUnsigned && !isRHSUnsigned:
		err = b.modUS(lh, rh)
	case !isLHSUnsigned && isRHSUnsigned:
		err = b.modSU(lh, rh)
	case !isLHSUnsigned && !isRHSUnsigned:
		err = b.modSS(lh, rh)
	}
	return err
}
func (b *builtinArithmeticModIntSig) modUU(lh, rh *chunk.Column) error {
	lhi64s := lh.Int64s()
	rhi64s := rh.Int64s()

	for i := 0; i < len(lhi64s); i++ {
		if rh.IsNull(i) {
			continue
		}
		if rhi64s[i] == 0 {
			if err := handleDivisionByZeroError(b.ctx); err != nil {
				return err
			}
			rh.SetNull(i, true)
			continue
		}
		if lh.IsNull(i) {
			rh.SetNull(i, true)
			continue
		}
		lhVar, rhVar := lhi64s[i], rhi64s[i]
		rhi64s[i] = int64(uint64(lhVar) % uint64(rhVar))
	}
	return nil
}
func (b *builtinArithmeticModIntSig) modUS(lh, rh *chunk.Column) error {
	lhi64s := lh.Int64s()
	rhi64s := rh.Int64s()

	for i := 0; i < len(lhi64s); i++ {
		if rh.IsNull(i) {
			continue
		}
		if rhi64s[i] == 0 {
			if err := handleDivisionByZeroError(b.ctx); err != nil {
				return err
			}
			rh.SetNull(i, true)
			continue
		}
		if lh.IsNull(i) {
			rh.SetNull(i, true)
			continue
		}
		lhVar, rhVar := lhi64s[i], rhi64s[i]
		if rhVar < 0 {
			rhi64s[i] = int64(uint64(lhVar) % uint64(-rhVar))
		} else {
			rhi64s[i] = int64(uint64(lhVar) % uint64(rhVar))
		}
	}
	return nil
}
func (b *builtinArithmeticModIntSig) modSU(lh, rh *chunk.Column) error {
	lhi64s := lh.Int64s()
	rhi64s := rh.Int64s()

	for i := 0; i < len(lhi64s); i++ {
		if rh.IsNull(i) {
			continue
		}
		if rhi64s[i] == 0 {
			if err := handleDivisionByZeroError(b.ctx); err != nil {
				return err
			}
			rh.SetNull(i, true)
			continue
		}
		if lh.IsNull(i) {
			rh.SetNull(i, true)
			continue
		}
		lhVar, rhVar := lhi64s[i], rhi64s[i]
		if lhVar < 0 {
			rhi64s[i] = -int64(uint64(-lhVar) % uint64(rhVar))
		} else {
			rhi64s[i] = int64(uint64(lhVar) % uint64(rhVar))
		}
	}
	return nil
}
func (b *builtinArithmeticModIntSig) modSS(lh, rh *chunk.Column) error {
	lhi64s := lh.Int64s()
	rhi64s := rh.Int64s()

	for i := 0; i < len(lhi64s); i++ {
		if rh.IsNull(i) {
			continue
		}
		if rhi64s[i] == 0 {
			if err := handleDivisionByZeroError(b.ctx); err != nil {
				return err
			}
			rh.SetNull(i, true)
			continue
		}
		if lh.IsNull(i) {
			rh.SetNull(i, true)
			continue
		}
		lhVar, rhVar := lhi64s[i], rhi64s[i]
		rhi64s[i] = lhVar % rhVar
	}
	return nil
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
	return true
}

func (b *builtinArithmeticMinusDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
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
	var to types.MyDecimal
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if err = types.DecimalSub(&x[i], &y[i], &to); err != nil {
			return err
		}
		x[i] = to
	}
	return nil
}

func (b *builtinArithmeticMinusIntSig) vectorized() bool {
	return true
}

func (b *builtinArithmeticMinusIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	lh, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(lh)

	if err := b.args[0].VecEvalInt(b.ctx, input, lh); err != nil {
		return err
	}

	if err := b.args[1].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}

	result.MergeNulls(lh)

	rh := result
	lhi64s := lh.Int64s()
	rhi64s := rh.Int64s()
	resulti64s := result.Int64s()

	forceToSigned := b.ctx.GetSessionVars().SQLMode.HasNoUnsignedSubtractionMode()
	isLHSUnsigned := mysql.HasUnsignedFlag(b.args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(b.args[1].GetType().Flag)

	switch {
	case forceToSigned && isLHSUnsigned && isRHSUnsigned:
		err = b.minusFUU(result, lhi64s, rhi64s, resulti64s)
	case forceToSigned && isLHSUnsigned && !isRHSUnsigned:
		err = b.minusFUS(result, lhi64s, rhi64s, resulti64s)
	case forceToSigned && !isLHSUnsigned && isRHSUnsigned:
		err = b.minusFSU(result, lhi64s, rhi64s, resulti64s)
	case forceToSigned && !isLHSUnsigned && !isRHSUnsigned:
		err = b.minusSS(result, lhi64s, rhi64s, resulti64s)
	case !forceToSigned && isLHSUnsigned && isRHSUnsigned:
		err = b.minusUU(result, lhi64s, rhi64s, resulti64s)
	case !forceToSigned && isLHSUnsigned && !isRHSUnsigned:
		err = b.minusUS(result, lhi64s, rhi64s, resulti64s)
	case !forceToSigned && !isLHSUnsigned && isRHSUnsigned:
		err = b.minusSU(result, lhi64s, rhi64s, resulti64s)
	case !forceToSigned && !isLHSUnsigned && !isRHSUnsigned:
		err = b.minusSS(result, lhi64s, rhi64s, resulti64s)
	}
	return err
}
func (b *builtinArithmeticMinusIntSig) minusFUU(result *chunk.Column, lhi64s, rhi64s, resulti64s []int64) error {
	for i := 0; i < len(lhi64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lh, rh := lhi64s[i], rhi64s[i]

		if lh < 0 {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", b.args[0].String(), b.args[1].String()))
		}

		if rh < 0 {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", b.args[0].String(), b.args[1].String()))
		}

		if (lh > 0 && -rh > math.MaxInt64-lh) || (lh < 0 && -rh < math.MinInt64-lh) {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s + %s)", b.args[0].String(), b.args[1].String()))
		}

		resulti64s[i] = lh - rh
	}
	return nil
}

func (b *builtinArithmeticMinusIntSig) minusFUS(result *chunk.Column, lhi64s, rhi64s, resulti64s []int64) error {
	for i := 0; i < len(lhi64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lh, rh := lhi64s[i], rhi64s[i]

		if lh < 0 {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", b.args[0].String(), b.args[1].String()))
		}

		if (lh > 0 && -rh > math.MaxInt64-lh) || (lh < 0 && -rh < math.MinInt64-lh) {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s + %s)", b.args[0].String(), b.args[1].String()))
		}

		resulti64s[i] = lh - rh
	}
	return nil
}

func (b *builtinArithmeticMinusIntSig) minusFSU(result *chunk.Column, lhi64s, rhi64s, resulti64s []int64) error {
	for i := 0; i < len(lhi64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lh, rh := lhi64s[i], rhi64s[i]

		if rh < 0 {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", b.args[0].String(), b.args[1].String()))
		}

		if (lh > 0 && -rh > math.MaxInt64-lh) || (lh < 0 && -rh < math.MinInt64-lh) {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s + %s)", b.args[0].String(), b.args[1].String()))
		}

		resulti64s[i] = lh - rh
	}
	return nil
}
func (b *builtinArithmeticMinusIntSig) minusUU(result *chunk.Column, lhi64s, rhi64s, resulti64s []int64) error {
	for i := 0; i < len(lhi64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lh, rh := lhi64s[i], rhi64s[i]

		if uint64(lh) < uint64(rh) {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", b.args[0].String(), b.args[1].String()))
		}

		resulti64s[i] = lh - rh
	}
	return nil
}

func (b *builtinArithmeticMinusIntSig) minusUS(result *chunk.Column, lhi64s, rhi64s, resulti64s []int64) error {
	for i := 0; i < len(lhi64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lh, rh := lhi64s[i], rhi64s[i]

		if rh >= 0 && uint64(lh) < uint64(rh) {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", b.args[0].String(), b.args[1].String()))
		}
		if rh < 0 && uint64(lh) > math.MaxUint64-uint64(-rh) {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", b.args[0].String(), b.args[1].String()))
		}

		resulti64s[i] = lh - rh
	}
	return nil
}

func (b *builtinArithmeticMinusIntSig) minusSU(result *chunk.Column, lhi64s, rhi64s, resulti64s []int64) error {
	for i := 0; i < len(lhi64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lh, rh := lhi64s[i], rhi64s[i]

		if uint64(lh-math.MinInt64) < uint64(rh) {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", b.args[0].String(), b.args[1].String()))
		}

		resulti64s[i] = lh - rh
	}
	return nil
}
func (b *builtinArithmeticMinusIntSig) minusSS(result *chunk.Column, lhi64s, rhi64s, resulti64s []int64) error {
	for i := 0; i < len(lhi64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lh, rh := lhi64s[i], rhi64s[i]

		if (lh > 0 && -rh > math.MaxInt64-lh) || (lh < 0 && -rh < math.MinInt64-lh) {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s - %s)", b.args[0].String(), b.args[1].String()))
		}

		resulti64s[i] = lh - rh
	}
	return nil
}

func (b *builtinArithmeticModRealSig) vectorized() bool {
	return true
}

func (b *builtinArithmeticModRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	x := result.Float64s()
	y := buf.Float64s()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.SetNull(i, true)
			continue
		}
		if y[i] == 0 {
			if err := handleDivisionByZeroError(b.ctx); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		if result.IsNull(i) {
			continue
		}

		x[i] = math.Mod(x[i], y[i])
	}
	return nil
}

func (b *builtinArithmeticModDecimalSig) vectorized() bool {
	return true
}

func (b *builtinArithmeticModDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
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
	var to types.MyDecimal
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		err = types.DecimalMod(&x[i], &y[i], &to)
		if err == types.ErrDivByZero {
			if err := handleDivisionByZeroError(b.ctx); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		} else if err != nil {
			return err
		}

		x[i] = to
	}
	return nil
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
	return true
}

func (b *builtinArithmeticMultiplyDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
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
	var to types.MyDecimal
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		err = types.DecimalMul(&x[i], &y[i], &to)
		if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) {
			return err
		}
		x[i] = to
	}
	return nil
}

func (b *builtinArithmeticIntDivideDecimalSig) vectorized() bool {
	return true
}

func (b *builtinArithmeticIntDivideDecimalSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	sc := b.ctx.GetSessionVars().StmtCtx
	n := input.NumRows()
	var err error
	var buf [2]*chunk.Column
	var num [2][]types.MyDecimal
	for i, arg := range b.args {
		if buf[i], err = b.bufAllocator.get(types.ETDecimal, n); err != nil {
			return err
		}
		defer b.bufAllocator.put(buf[i])

		err = arg.VecEvalDecimal(b.ctx, input, buf[i])
		if err != nil {
			return err
		}

		num[i] = buf[i].Decimals()
	}

	isLHSUnsigned := mysql.HasUnsignedFlag(b.args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(b.args[1].GetType().Flag)
	isUnsigned := isLHSUnsigned || isRHSUnsigned

	result.ResizeInt64(n, false)
	result.MergeNulls(buf[0], buf[1])
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		c := &types.MyDecimal{}
		err = types.DecimalDiv(&num[0][i], &num[1][i], c, types.DivFracIncr)
		if err == types.ErrDivByZero {
			if err = handleDivisionByZeroError(b.ctx); err != nil {
				return err
			}
			result.SetNull(i, true)
			continue
		}
		if err == types.ErrTruncated {
			err = sc.HandleTruncate(errTruncatedWrongValue.GenWithStackByArgs("DECIMAL", c))
		} else if err == types.ErrOverflow {
			newErr := errTruncatedWrongValue.GenWithStackByArgs("DECIMAL", c)
			err = sc.HandleOverflow(newErr, newErr)
		}
		if err != nil {
			return err
		}

		if isUnsigned {
			val, err := c.ToUint()
			// err returned by ToUint may be ErrTruncated or ErrOverflow, only handle ErrOverflow, ignore ErrTruncated.
			if err == types.ErrOverflow {
				v, err := c.ToInt()
				// when the final result is at (-1, 0], it should be return 0 instead of the error
				if v == 0 && err == types.ErrTruncated {
					i64s[i] = int64(0)
					continue
				}
				return types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s DIV %s)", num[0][i].String(), num[1][i].String()))
			}
			i64s[i] = int64(val)
		} else {
			i64s[i], err = c.ToInt()
			// err returned by ToInt may be ErrTruncated or ErrOverflow, only handle ErrOverflow, ignore ErrTruncated.
			if err == types.ErrOverflow {
				return types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s DIV %s)", num[0][i].String(), num[1][i].String()))
			}
		}
	}
	return nil
}

func (b *builtinArithmeticMultiplyIntSig) vectorized() bool {
	return true
}

func (b *builtinArithmeticMultiplyIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
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

	x := result.Int64s()
	y := buf.Int64s()
	result.MergeNulls(buf)
	var tmp int64
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		tmp = x[i] * y[i]
		if x[i] != 0 && tmp/x[i] != y[i] {
			result.SetNull(i, true)
			return types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s * %s)", b.args[0].String(), b.args[1].String()))
		}

		x[i] = tmp
	}

	return nil
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
	return true
}

func (b *builtinArithmeticIntDivideIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	lhsBuf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(lhsBuf)

	if err := b.args[0].VecEvalInt(b.ctx, input, lhsBuf); err != nil {
		return err
	}

	// reuse result as rhsBuf to avoid buf allocate
	if err := b.args[1].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}

	result.MergeNulls(lhsBuf)

	rhsBuf := result
	lhsI64s := lhsBuf.Int64s()
	rhsI64s := rhsBuf.Int64s()
	resultI64s := result.Int64s()

	isLHSUnsigned := mysql.HasUnsignedFlag(b.args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(b.args[1].GetType().Flag)

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		err = b.divideUU(result, lhsI64s, rhsI64s, resultI64s)
	case isLHSUnsigned && !isRHSUnsigned:
		err = b.divideUS(result, lhsI64s, rhsI64s, resultI64s)
	case !isLHSUnsigned && isRHSUnsigned:
		err = b.divideSU(result, lhsI64s, rhsI64s, resultI64s)
	case !isLHSUnsigned && !isRHSUnsigned:
		err = b.divideSS(result, lhsI64s, rhsI64s, resultI64s)
	}
	return err
}

func (b *builtinArithmeticIntDivideIntSig) divideUU(result *chunk.Column, lhsI64s, rhsI64s, resultI64s []int64) error {
	for i := 0; i < len(lhsI64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lhs, rhs := lhsI64s[i], rhsI64s[i]

		if rhs == 0 {
			if err := handleDivisionByZeroError(b.ctx); err != nil {
				return err
			}
			result.SetNull(i, true)
		} else {
			resultI64s[i] = int64(uint64(lhs) / uint64(rhs))
		}

	}
	return nil
}

func (b *builtinArithmeticIntDivideIntSig) divideUS(result *chunk.Column, lhsI64s, rhsI64s, resultI64s []int64) error {
	for i := 0; i < len(lhsI64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lhs, rhs := lhsI64s[i], rhsI64s[i]

		if rhs == 0 {
			if err := handleDivisionByZeroError(b.ctx); err != nil {
				return err
			}
			result.SetNull(i, true)
		} else {
			val, err := types.DivUintWithInt(uint64(lhs), rhs)
			if err != nil {
				return err
			}
			resultI64s[i] = int64(val)
		}
	}
	return nil
}

func (b *builtinArithmeticIntDivideIntSig) divideSU(result *chunk.Column, lhsI64s, rhsI64s, resultI64s []int64) error {
	for i := 0; i < len(lhsI64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lhs, rhs := lhsI64s[i], rhsI64s[i]

		if rhs == 0 {
			if err := handleDivisionByZeroError(b.ctx); err != nil {
				return err
			}
			result.SetNull(i, true)
		} else {
			val, err := types.DivIntWithUint(lhs, uint64(rhs))
			if err != nil {
				return err
			}
			resultI64s[i] = int64(val)
		}
	}
	return nil
}

func (b *builtinArithmeticIntDivideIntSig) divideSS(result *chunk.Column, lhsI64s, rhsI64s, resultI64s []int64) error {
	for i := 0; i < len(lhsI64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lhs, rhs := lhsI64s[i], rhsI64s[i]

		if rhs == 0 {
			if err := handleDivisionByZeroError(b.ctx); err != nil {
				return err
			}
			result.SetNull(i, true)
		} else {
			val, err := types.DivInt64(lhs, rhs)
			if err != nil {
				return err
			}
			resultI64s[i] = val
		}
	}
	return nil
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

	if err := b.args[0].VecEvalInt(b.ctx, input, lh); err != nil {
		return err
	}

	// reuse result as rh to avoid buf allocate
	if err := b.args[1].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}

	result.MergeNulls(lh)

	rh := result
	lhi64s := lh.Int64s()
	rhi64s := rh.Int64s()
	resulti64s := result.Int64s()

	isLHSUnsigned := mysql.HasUnsignedFlag(b.args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(b.args[1].GetType().Flag)

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		err = b.plusUU(result, lhi64s, rhi64s, resulti64s)
	case isLHSUnsigned && !isRHSUnsigned:
		err = b.plusUS(result, lhi64s, rhi64s, resulti64s)
	case !isLHSUnsigned && isRHSUnsigned:
		err = b.plusSU(result, lhi64s, rhi64s, resulti64s)
	case !isLHSUnsigned && !isRHSUnsigned:
		err = b.plusSS(result, lhi64s, rhi64s, resulti64s)
	}
	return err
}
func (b *builtinArithmeticPlusIntSig) plusUU(result *chunk.Column, lhi64s, rhi64s, resulti64s []int64) error {
	for i := 0; i < len(lhi64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lh, rh := lhi64s[i], rhi64s[i]

		if uint64(lh) > math.MaxUint64-uint64(rh) {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", b.args[0].String(), b.args[1].String()))
		}

		resulti64s[i] = lh + rh
	}
	return nil
}

func (b *builtinArithmeticPlusIntSig) plusUS(result *chunk.Column, lhi64s, rhi64s, resulti64s []int64) error {
	for i := 0; i < len(lhi64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lh, rh := lhi64s[i], rhi64s[i]

		if rh < 0 && uint64(-rh) > uint64(lh) {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", b.args[0].String(), b.args[1].String()))
		}
		if rh > 0 && uint64(lh) > math.MaxUint64-uint64(lh) {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", b.args[0].String(), b.args[1].String()))
		}

		resulti64s[i] = lh + rh
	}
	return nil
}

func (b *builtinArithmeticPlusIntSig) plusSU(result *chunk.Column, lhi64s, rhi64s, resulti64s []int64) error {
	for i := 0; i < len(lhi64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lh, rh := lhi64s[i], rhi64s[i]

		if lh < 0 && uint64(-lh) > uint64(rh) {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", b.args[0].String(), b.args[1].String()))
		}
		if lh > 0 && uint64(rh) > math.MaxUint64-uint64(lh) {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", b.args[0].String(), b.args[1].String()))
		}

		resulti64s[i] = lh + rh
	}
	return nil
}
func (b *builtinArithmeticPlusIntSig) plusSS(result *chunk.Column, lhi64s, rhi64s, resulti64s []int64) error {
	for i := 0; i < len(lhi64s); i++ {
		if result.IsNull(i) {
			continue
		}
		lh, rh := lhi64s[i], rhi64s[i]

		if (lh > 0 && rh > math.MaxInt64-lh) || (lh < 0 && rh < math.MinInt64-lh) {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s + %s)", b.args[0].String(), b.args[1].String()))
		}

		resulti64s[i] = lh + rh
	}
	return nil
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
	return true
}

func (b *builtinArithmeticMultiplyIntUnsignedSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
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

	x := result.Uint64s()
	y := buf.Uint64s()
	result.MergeNulls(buf)
	var res uint64
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}

		res = x[i] * y[i]
		if x[i] != 0 && res/x[i] != y[i] {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s * %s)", b.args[0].String(), b.args[1].String()))
		}
		x[i] = res
	}
	return nil
}
