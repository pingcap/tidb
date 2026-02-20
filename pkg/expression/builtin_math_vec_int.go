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
	"fmt"
	"hash/crc32"
	"math"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mathutil"
)

func (b *builtinAbsIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}
	i64s := result.Int64s()
	for i := range i64s {
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

func (b *builtinRoundIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalInt(ctx, input, result)
}

func (b *builtinRoundIntSig) vectorized() bool {
	return true
}

func (b *builtinRoundWithFracIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	i64s := result.Int64s()
	frac := buf.Int64s()
	result.MergeNulls(buf)
	for i := range n {
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

func (b *builtinCRC32Sig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
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

func (b *builtinPISig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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

func (b *builtinRandSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeFloat64(n, false)
	f64s := result.Float64s()
	for i := range f64s {
		f64s[i] = b.mysqlRng.Gen()
	}
	return nil
}

func (b *builtinRandWithSeedFirstGenSig) vectorized() bool {
	return true
}

func (b *builtinRandWithSeedFirstGenSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeFloat64(n, false)
	i64s := buf.Int64s()
	f64s := result.Float64s()
	for i := range n {
		// When the seed is null we need to use 0 as the seed.
		// The behavior same as MySQL.
		rng := mathutil.NewWithSeed(0)
		if !buf.IsNull(i) {
			rng = mathutil.NewWithSeed(i64s[i])
		}
		f64s[i] = rng.Gen()
	}
	return nil
}

func (b *builtinCeilIntToDecSig) vectorized() bool {
	return true
}

func (b *builtinCeilIntToDecSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)

	i64s := buf.Int64s()
	d := result.Decimals()
	isUnsigned := mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag())
	for i := range n {
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

func (b *builtinTruncateIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}

	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	if err := b.args[1].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}
	result.MergeNulls(buf)
	i64s := result.Int64s()
	buf64s := buf.Int64s()

	if mysql.HasUnsignedFlag(b.args[1].GetType(ctx).GetFlag()) {
		return nil
	}

	for i := range i64s {
		if result.IsNull(i) {
			continue
		}
		if buf64s[i] < 0 {
			// -MinInt = MinInt, special case
			if buf64s[i] == mathutil.MinInt {
				i64s[i] = 0
			} else {
				shift := int64(math.Pow10(int(-buf64s[i])))
				i64s[i] = i64s[i] / shift * shift
			}
		}
	}
	return nil
}

func (b *builtinTruncateUintSig) vectorized() bool {
	return true
}

func (b *builtinTruncateUintSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}

	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	if err := b.args[1].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}
	result.MergeNulls(buf)
	i64s := result.Int64s()
	buf64s := buf.Int64s()

	if mysql.HasUnsignedFlag(b.args[1].GetType(ctx).GetFlag()) {
		return nil
	}

	for i := range n {
		if result.IsNull(i) {
			continue
		}

		if buf64s[i] < 0 {
			// -MinInt = MinInt, special case
			if buf64s[i] == mathutil.MinInt {
				i64s[i] = 0
			} else {
				shift := uint64(math.Pow10(int(-buf64s[i])))
				i64s[i] = int64(uint64(i64s[i]) / shift * shift)
			}
		}
	}
	return nil
}

func (b *builtinCeilDecToDecSig) vectorized() bool {
	return true
}

func (b *builtinCeilDecToDecSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalDecimal(ctx, input, result); err != nil {
		return err
	}
	ds := result.Decimals()

	for i := range n {
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

func (b *builtinFloorDecToDecSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalDecimal(ctx, input, result); err != nil {
		return err
	}
	ds := result.Decimals()

	for i := range n {
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

func (b *builtinTruncateDecimalSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalDecimal(ctx, input, result); err != nil {
		return err
	}
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}
	result.MergeNulls(buf)
	ds := result.Decimals()
	i64s := buf.Int64s()
	ft := b.getRetTp().GetDecimal()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		result := new(types.MyDecimal)
		if err := ds[i].Round(result, min(int(i64s[i]), ft), types.ModeTruncate); err != nil {
			return err
		}
		ds[i] = *result
	}
	return nil
}

func (b *builtinRoundWithFracDecSig) vectorized() bool {
	return true
}

func (b *builtinRoundWithFracDecSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalDecimal(ctx, input, result); err != nil {
		return err
	}
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[1].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.MergeNulls(buf)
	tmp := new(types.MyDecimal)
	d64s := result.Decimals()
	i64s := buf.Int64s()
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		// TODO: reuse d64[i] and remove the temporary variable tmp.
		if err := d64s[i].Round(tmp, min(int(i64s[i]), b.tp.GetDecimal()), types.ModeHalfUp); err != nil {
			return err
		}
		d64s[i] = *tmp
	}
	return nil
}

func (b *builtinFloorIntToDecSig) vectorized() bool {
	return true
}

func (b *builtinFloorIntToDecSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeDecimal(n, false)
	result.MergeNulls(buf)

	i64s := buf.Int64s()
	d := result.Decimals()
	isUnsigned := mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag())
	for i := range n {
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

func (b *builtinSignSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}
	args := buf.Float64s()
	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	for i := range i64s {
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
	// TODO: change the vecEval match hybrid type fixing. Then open the vectorized evaluation.
	return false
}

func (b *builtinConvSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	buf2, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)
	buf3, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf3)
	if err := b.args[0].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}
	if err := b.args[1].VecEvalInt(ctx, input, buf2); err != nil {
		return err
	}
	if err := b.args[2].VecEvalInt(ctx, input, buf3); err != nil {
		return err
	}
	result.ReserveString(n)
	fromBase := buf2.Int64s()
	toBase := buf3.Int64s()
	for i := range n {
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

func (b *builtinAbsUIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalInt(ctx, input, result)
}

func (b *builtinCeilDecToIntSig) vectorized() bool {
	return true
}

func (b *builtinCeilDecToIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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

	d := buf.Decimals()
	i64s := result.Int64s()
	for i := range n {
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

func (b *builtinCeilIntToIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalInt(ctx, input, result)
}

func (b *builtinFloorIntToIntSig) vectorized() bool {
	return true
}

func (b *builtinFloorIntToIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return b.args[0].VecEvalInt(ctx, input, result)
}

func (b *builtinFloorDecToIntSig) vectorized() bool {
	return true
}

func (b *builtinFloorDecToIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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

	d := buf.Decimals()
	i64s := result.Int64s()
	for i := range n {
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
