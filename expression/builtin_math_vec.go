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
	"hash/crc32"
	"math"
	"strconv"

	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/chunk"
)

func (b *builtinLog1ArgSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if f64s[i] <= 0 {
			result.SetNull(i, true)
		} else {
			f64s[i] = math.Log(f64s[i])
		}
	}
	return nil
}

func (b *builtinLog1ArgSig) vectorized() bool {
	return true
}

func (b *builtinLog2Sig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if f64s[i] <= 0 {
			result.SetNull(i, true)
		} else {
			f64s[i] = math.Log2(f64s[i])
		}
	}
	return nil
}

func (b *builtinLog2Sig) vectorized() bool {
	return true
}

func (b *builtinLog10Sig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if f64s[i] <= 0 {
			result.SetNull(i, true)
		} else {
			f64s[i] = math.Log10(f64s[i])
		}
	}
	return nil
}

func (b *builtinLog10Sig) vectorized() bool {
	return true
}

func (b *builtinSqrtSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if f64s[i] < 0 {
			result.SetNull(i, true)
		} else {
			f64s[i] = math.Sqrt(f64s[i])
		}
	}
	return nil
}

func (b *builtinSqrtSig) vectorized() bool {
	return true
}

func (b *builtinAcosSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if f64s[i] < -1 || f64s[i] > 1 {
			result.SetNull(i, true)
		} else {
			f64s[i] = math.Acos(f64s[i])
		}
	}
	return nil
}

func (b *builtinAcosSig) vectorized() bool {
	return true
}

func (b *builtinAsinSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if f64s[i] < -1 || f64s[i] > 1 {
			result.SetNull(i, true)
		} else {
			f64s[i] = math.Asin(f64s[i])
		}
	}
	return nil
}

func (b *builtinAsinSig) vectorized() bool {
	return true
}

func (b *builtinAtan1ArgSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = math.Atan(f64s[i])
	}
	return nil
}

func (b *builtinAtan1ArgSig) vectorized() bool {
	return true
}

func (b *builtinAtan2ArgsSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}

	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}

	f64s := result.Float64s()
	arg := buf.Float64s()

	result.MergeNulls(buf)

	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = math.Atan2(f64s[i], arg[i])
	}
	return nil
}

func (b *builtinAtan2ArgsSig) vectorized() bool {
	return true
}

func (b *builtinCosSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = math.Cos(f64s[i])
	}
	return nil
}

func (b *builtinCosSig) vectorized() bool {
	return true
}

func (b *builtinCotSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		tan := math.Tan(f64s[i])
		if tan != 0 {
			cot := 1 / tan
			if !math.IsInf(cot, 0) && !math.IsNaN(cot) {
				f64s[i] = cot
			}
			continue
		}
		if err := types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("cot(%s)", strconv.FormatFloat(f64s[i], 'f', -1, 64))); err != nil {
			return err
		}
	}
	return nil
}

func (b *builtinCotSig) vectorized() bool {
	return true
}

func (b *builtinDegreesSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = f64s[i] * 180 / math.Pi
	}
	return nil
}

func (b *builtinDegreesSig) vectorized() bool {
	return true
}

func (b *builtinExpSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		exp := math.Exp(f64s[i])
		if math.IsInf(exp, 0) || math.IsNaN(exp) {
			s := fmt.Sprintf("exp(%s)", b.args[0].String())
			if err := types.ErrOverflow.GenWithStackByArgs("DOUBLE", s); err != nil {
				return err
			}
		}
		f64s[i] = exp
	}
	return nil
}

func (b *builtinExpSig) vectorized() bool {
	return true
}

func (b *builtinRadiansSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = f64s[i] * math.Pi / 180
	}
	return nil
}

func (b *builtinRadiansSig) vectorized() bool {
	return true
}

func (b *builtinSinSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = math.Sin(f64s[i])
	}
	return nil
}

func (b *builtinSinSig) vectorized() bool {
	return true
}

func (b *builtinTanSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = math.Tan(f64s[i])
	}
	return nil
}

func (b *builtinTanSig) vectorized() bool {
	return true
}

func (b *builtinAbsDecSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalDecimal(b.ctx, input, result); err != nil {
		return err
	}
	zero := new(types.MyDecimal)
	buf := new(types.MyDecimal)
	d64s := result.Decimals()
	for i := 0; i < len(d64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if d64s[i].IsNegative() {
			if err := types.DecimalSub(zero, &d64s[i], buf); err != nil {
				return err
			}
			d64s[i] = *buf
		}
	}
	return nil
}

func (b *builtinAbsDecSig) vectorized() bool {
	return true
}

func (b *builtinRoundDecSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalDecimal(b.ctx, input, result); err != nil {
		return err
	}
	d64s := result.Decimals()
	buf := new(types.MyDecimal)
	for i := 0; i < len(d64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if err := d64s[i].Round(buf, 0, types.ModeHalfEven); err != nil {
			return err
		}
		d64s[i] = *buf
	}
	return nil
}

func (b *builtinRoundDecSig) vectorized() bool {
	return true
}

func (b *builtinPowSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[0].VecEvalReal(b.ctx, input, buf1); err != nil {
		return err
	}

	if err := b.args[1].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}

	x := buf1.Float64s()
	y := result.Float64s()
	result.MergeNulls(buf1)
	f64s := result.Float64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		power := math.Pow(x[i], y[i])
		if math.IsInf(power, -1) || math.IsInf(power, 1) || math.IsNaN(power) {
			return types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("pow(%s, %s)", strconv.FormatFloat(x[i], 'f', -1, 64), strconv.FormatFloat(y[i], 'f', -1, 64)))
		}
		f64s[i] = power
	}
	return nil
}

func (b *builtinPowSig) vectorized() bool {
	return true
}

func (b *builtinFloorRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = math.Floor(f64s[i])
	}
	return nil
}

func (b *builtinFloorRealSig) vectorized() bool {
	return true
}

func (b *builtinLog2ArgsSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	buf1, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalReal(b.ctx, input, buf1); err != nil {
		return err
	}

	d := result.Float64s()
	x := buf1.Float64s()
	result.MergeNulls(buf1)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if d[i] <= 0 || d[i] == 1 || x[i] <= 0 {
			result.SetNull(i, true)
		}
		d[i] = math.Log(x[i]) / math.Log(d[i])
	}
	return nil
}

func (b *builtinLog2ArgsSig) vectorized() bool {
	return true
}

func (b *builtinCeilRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = math.Ceil(f64s[i])
	}
	return nil
}

func (b *builtinCeilRealSig) vectorized() bool {
	return true
}

func (b *builtinRoundRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		if result.IsNull(i) {
			continue
		}
		f64s[i] = types.Round(f64s[i], 0)
	}
	return nil
}

func (b *builtinRoundRealSig) vectorized() bool {
	return true
}

func (b *builtinRoundWithFracRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	buf1, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	x := result.Float64s()
	d := buf1.Int64s()
	result.MergeNulls(buf1)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		x[i] = types.Round(x[i], int(d[i]))
	}
	return nil
}

func (b *builtinRoundWithFracRealSig) vectorized() bool {
	return true
}

func (b *builtinTruncateRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	n := input.NumRows()
	buf1, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}

	result.MergeNulls(buf1)
	x := result.Float64s()
	d := buf1.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		x[i] = types.Truncate(x[i], int(d[i]))
	}
	return nil
}

func (b *builtinTruncateRealSig) vectorized() bool {
	return true
}

func (b *builtinAbsRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalReal(b.ctx, input, result); err != nil {
		return err
	}
	f64s := result.Float64s()
	for i := 0; i < len(f64s); i++ {
		f64s[i] = math.Abs(f64s[i])
	}
	return nil
}

func (b *builtinAbsRealSig) vectorized() bool {
	return true
}

func (b *builtinAbsIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		return err
	}
	i64s := result.Int64s()
	for i := 0; i < len(i64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if i64s[i] == math.MinInt64 {
			return types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("abs(%d)", i64s[i]))
		}
		if i64s[i] < 0 {
			i64s[i] = -i64s[i]
		}
	}
	return nil
}

func (b *builtinAbsIntSig) vectorized() bool {
	return true
}

func (b *builtinRoundIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalInt(b.ctx, input, result)
}

func (b *builtinRoundIntSig) vectorized() bool {
	return true
}

func (b *builtinRoundWithFracIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
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
	frac := buf.Int64s()
	result.MergeNulls(buf)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = int64(types.Round(float64(i64s[i]), int(frac[i])))
	}
	return nil
}

func (b *builtinRoundWithFracIntSig) vectorized() bool {
	return true
}
func (b *builtinCRC32Sig) vectorized() bool {
	return true
}

func (b *builtinCRC32Sig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	result.MergeNulls(buf)
	for i := range i64s {
		if !buf.IsNull(i) {
			i64s[i] = int64(crc32.ChecksumIEEE(buf.GetBytes(i)))
		}
	}
	return nil
}

func (b *builtinPISig) vectorized() bool {
	return true
}

func (b *builtinPISig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeFloat64(n, false)
	f64s := result.Float64s()
	for i := range f64s {
		f64s[i] = math.Pi
	}
	return nil
}

func (b *builtinRandSig) vectorized() bool {
	return true
}

func (b *builtinRandSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeFloat64(n, false)
	f64s := result.Float64s()
	b.mu.Lock()
	for i := range f64s {
		f64s[i] = b.mysqlRng.Gen()
	}
	b.mu.Unlock()
	return nil
}

func (b *builtinRandWithSeedFirstGenSig) vectorized() bool {
	return true
}

func (b *builtinRandWithSeedFirstGenSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeFloat64(n, false)
	i64s := buf.Int64s()
	f64s := result.Float64s()
	for i := 0; i < n; i++ {
		// When the seed is null we need to use 0 as the seed.
		// The behavior same as MySQL.
		rng := NewWithSeed(0)
		if !buf.IsNull(i) {
			rng = NewWithSeed(i64s[i])
		}
		f64s[i] = rng.Gen()
	}
	return nil
}

func (b *builtinCeilIntToDecSig) vectorized() bool {
	return true
}

func (b *builtinCeilIntToDecSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)

	i64s := buf.Int64s()
	d := result.Decimals()
	isUnsigned := mysql.HasUnsignedFlag(b.args[0].GetType().Flag)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if isUnsigned || i64s[i] >= 0 {
			d[i] = *types.NewDecFromUint(uint64(i64s[i]))
			continue
		}
		d[i] = *types.NewDecFromInt(i64s[i])
	}
	return nil
}

func (b *builtinTruncateIntSig) vectorized() bool {
	return true
}

func (b *builtinTruncateIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
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
	result.MergeNulls(buf)
	i64s := result.Int64s()
	buf64s := buf.Int64s()

	for i := 0; i < len(i64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if buf64s[i] < 0 {
			shift := int64(math.Pow10(int(-buf64s[i])))
			i64s[i] = i64s[i] / shift * shift
		}
	}
	return nil
}

func (b *builtinTruncateUintSig) vectorized() bool {
	return true
}

func (b *builtinTruncateUintSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
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
	result.MergeNulls(buf)
	i64s := result.Int64s()
	buf64s := buf.Int64s()

	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if buf64s[i] < 0 {
			shift := uint64(math.Pow10(int(-buf64s[i])))
			i64s[i] = int64(uint64(i64s[i]) / shift * shift)
		}
	}
	return nil
}

func (b *builtinCeilDecToDecSig) vectorized() bool {
	return true
}

func (b *builtinCeilDecToDecSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalDecimal(b.ctx, input, result); err != nil {
		return err
	}
	ds := result.Decimals()

	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		rst := new(types.MyDecimal)
		if err := ds[i].Round(rst, 0, types.ModeTruncate); err != nil {
			return err
		}
		if !ds[i].IsNegative() && rst.Compare(&ds[i]) != 0 {
			if err := types.DecimalAdd(rst, types.NewDecFromInt(1), rst); err != nil {
				return err
			}
		}
		ds[i] = *rst
	}
	return nil
}

func (b *builtinFloorDecToDecSig) vectorized() bool {
	return true
}

func (b *builtinFloorDecToDecSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalDecimal(b.ctx, input, result); err != nil {
		return err
	}
	ds := result.Decimals()

	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		rst := new(types.MyDecimal)
		if !ds[i].IsNegative() {
			if err := ds[i].Round(rst, 0, types.ModeTruncate); err != nil {
				return err
			}
		} else {
			if err := ds[i].Round(rst, 0, types.ModeTruncate); err != nil {
				return err
			}
			if rst.Compare(&ds[i]) != 0 {
				if err := types.DecimalSub(rst, types.NewDecFromInt(1), rst); err != nil {
					return err
				}
			}
		}
		ds[i] = *rst
	}
	return nil
}

func (b *builtinTruncateDecimalSig) vectorized() bool {
	return true
}

func (b *builtinTruncateDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalDecimal(b.ctx, input, result); err != nil {
		return err
	}
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}
	result.MergeNulls(buf)
	ds := result.Decimals()
	i64s := buf.Int64s()
	ft := b.getRetTp().Decimal
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		result := new(types.MyDecimal)
		if err := ds[i].Round(result, mathutil.Min(int(i64s[i]), ft), types.ModeTruncate); err != nil {
			return err
		}
		ds[i] = *result
	}
	return nil
}

func (b *builtinRoundWithFracDecSig) vectorized() bool {
	return true
}

func (b *builtinRoundWithFracDecSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalDecimal(b.ctx, input, result); err != nil {
		return err
	}
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	result.MergeNulls(buf)
	tmp := new(types.MyDecimal)
	d64s := result.Decimals()
	i64s := buf.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		// TODO: reuse d64[i] and remove the temporary variable tmp.
		if err := d64s[i].Round(tmp, mathutil.Min(int(i64s[i]), b.tp.Decimal), types.ModeHalfEven); err != nil {
			return err
		}
		d64s[i] = *tmp
	}
	return nil
}

func (b *builtinFloorIntToDecSig) vectorized() bool {
	return true
}

func (b *builtinFloorIntToDecSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}

	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)

	i64s := buf.Int64s()
	d := result.Decimals()
	isUnsigned := mysql.HasUnsignedFlag(b.args[0].GetType().Flag)
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		if isUnsigned || i64s[i] >= 0 {
			d[i] = *types.NewDecFromUint(uint64(i64s[i]))
			continue
		}
		d[i] = *types.NewDecFromInt(i64s[i])
	}
	return nil
}

func (b *builtinSignSig) vectorized() bool {
	return true
}

func (b *builtinSignSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(b.ctx, input, buf); err != nil {
		return err
	}
	args := buf.Float64s()
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := 0; i < len(i64s); i++ {
		if result.IsNull(i) {
			continue
		}
		if args[i] > 0 {
			i64s[i] = 1
		} else if args[i] < 0 {
			i64s[i] = -1
		} else {
			i64s[i] = 0
		}
	}
	return nil
}

func (b *builtinConvSig) vectorized() bool {
	return true
}

func (b *builtinConvSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	buf2, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	buf3, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf3)
	if err := b.args[0].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}
	if err := b.args[1].VecEvalInt(b.ctx, input, buf2); err != nil {
		return err
	}
	if err := b.args[2].VecEvalInt(b.ctx, input, buf3); err != nil {
		return err
	}
	result.ReserveString(n)
	fromBase := buf2.Int64s()
	toBase := buf3.Int64s()
	for i := 0; i < n; i++ {
		if buf1.IsNull(i) || buf2.IsNull(i) || buf3.IsNull(i) {
			result.AppendNull()
			continue
		}
		res, isNull, err := b.conv(buf1.GetString(i), fromBase[i], toBase[i])
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

func (b *builtinAbsUIntSig) vectorized() bool {
	return true
}

func (b *builtinAbsUIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalInt(b.ctx, input, result)
}

func (b *builtinCeilDecToIntSig) vectorized() bool {
	return true
}

func (b *builtinCeilDecToIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
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

	d := buf.Decimals()
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		i64s[i], err = d[i].ToInt()
		if err == types.ErrTruncated {
			err = nil
			if !d[i].IsNegative() {
				i64s[i] = i64s[i] + 1
			}
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *builtinCeilIntToIntSig) vectorized() bool {
	return true
}

func (b *builtinCeilIntToIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalInt(b.ctx, input, result)
}

func (b *builtinFloorIntToIntSig) vectorized() bool {
	return true
}

func (b *builtinFloorIntToIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalInt(b.ctx, input, result)
}

func (b *builtinFloorDecToIntSig) vectorized() bool {
	return true
}

func (b *builtinFloorDecToIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
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

	d := buf.Decimals()
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		i64s[i], err = d[i].ToInt()
		if err == types.ErrTruncated {
			err = nil
			if d[i].IsNegative() {
				i64s[i]--
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}
