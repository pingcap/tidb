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
func (b *builtinLTStringSig) vectorized() bool {
	return false
}

func (b *builtinLTStringSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGEIntSig) vectorized() bool {
	return false
}

func (b *builtinGEIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
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
	return false
}

func (b *builtinLeastStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinEQJSONSig) vectorized() bool {
	return false
}

func (b *builtinEQJSONSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNEDecimalSig) vectorized() bool {
	return false
}

func (b *builtinNEDecimalSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNullEQJSONSig) vectorized() bool {
	return false
}

func (b *builtinNullEQJSONSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLTTimeSig) vectorized() bool {
	return false
}

func (b *builtinLTTimeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinEQDurationSig) vectorized() bool {
	return false
}

func (b *builtinEQDurationSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinEQIntSig) vectorized() bool {
	return false
}

func (b *builtinEQIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNEIntSig) vectorized() bool {
	return false
}

func (b *builtinNEIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLERealSig) vectorized() bool {
	return false
}

func (b *builtinLERealSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGTIntSig) vectorized() bool {
	return false
}

func (b *builtinGTIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGTJSONSig) vectorized() bool {
	return false
}

func (b *builtinGTJSONSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNETimeSig) vectorized() bool {
	return false
}

func (b *builtinNETimeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCoalesceDurationSig) vectorized() bool {
	return false
}

func (b *builtinCoalesceDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLTJSONSig) vectorized() bool {
	return false
}

func (b *builtinLTJSONSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNEDurationSig) vectorized() bool {
	return false
}

func (b *builtinNEDurationSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNullEQIntSig) vectorized() bool {
	return false
}

func (b *builtinNullEQIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNullEQRealSig) vectorized() bool {
	return true
}

func (b *builtinNullEQRealSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)

	buf1, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)

	if err := b.args[0].VecEvalReal(b.ctx, input, buf0); err != nil {
		return err
	}
	arg0 := buf0.Float64s()

	if err := b.args[1].VecEvalReal(b.ctx, input, buf1); err != nil {
		return err
	}
	arg1 := buf1.Float64s()

	result.ResizeInt64(n, false)
	i64s := result.Int64s()

	for i := 0; i < n; i++ {
		isNull0 := buf0.IsNull(i)
		isNull1 := buf1.IsNull(i)
		switch {
		case isNull0 && isNull1:
			i64s[i] = 1
		case isNull0 != isNull1:
			break
		case types.CompareFloat64(arg0[i], arg1[i]) == 0:
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinNullEQTimeSig) vectorized() bool {
	return false
}

func (b *builtinNullEQTimeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCoalesceRealSig) vectorized() bool {
	return false
}

func (b *builtinCoalesceRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCoalesceStringSig) vectorized() bool {
	return false
}

func (b *builtinCoalesceStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
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

func (b *builtinLTRealSig) vectorized() bool {
	return false
}

func (b *builtinLTRealSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGEStringSig) vectorized() bool {
	return false
}

func (b *builtinGEStringSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinEQStringSig) vectorized() bool {
	return false
}

func (b *builtinEQStringSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLEIntSig) vectorized() bool {
	return false
}

func (b *builtinLEIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLEDecimalSig) vectorized() bool {
	return false
}

func (b *builtinLEDecimalSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinEQTimeSig) vectorized() bool {
	return false
}

func (b *builtinEQTimeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNullEQDecimalSig) vectorized() bool {
	return true
}

func (b *builtinNullEQDecimalSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalDecimal(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalDecimal(b.ctx, input, buf1); err != nil {
		return err
	}
	args0 := buf0.Decimals()
	args1 := buf1.Decimals()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		isNull0 := buf0.IsNull(i)
		isNull1 := buf1.IsNull(i)
		switch {
		case isNull0 && isNull1:
			i64s[i] = 1
		case isNull0 != isNull1:
			i64s[i] = 0
		case args0[i].Compare(&args1[i]) == 0:
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinCoalesceDecimalSig) vectorized() bool {
	return false
}

func (b *builtinCoalesceDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGEDurationSig) vectorized() bool {
	return false
}

func (b *builtinGEDurationSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNullEQStringSig) vectorized() bool {
	return true
}

func (b *builtinNullEQStringSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(b.ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < len(i64s); i++ {
		isNull0 := buf0.IsNull(i)
		isNull1 := buf1.IsNull(i)
		switch {
		case isNull0 && isNull1:
			i64s[i] = 1
		case isNull0 != isNull1:
			i64s[i] = 0
		case types.CompareString(buf0.GetString(i), buf1.GetString(i)) == 0:
			i64s[i] = 1
		}
	}
	return nil
}

func (b *builtinLTIntSig) vectorized() bool {
	return false
}

func (b *builtinLTIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLEJSONSig) vectorized() bool {
	return false
}

func (b *builtinLEJSONSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCoalesceIntSig) vectorized() bool {
	return false
}

func (b *builtinCoalesceIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGEJSONSig) vectorized() bool {
	return false
}

func (b *builtinGEJSONSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLETimeSig) vectorized() bool {
	return false
}

func (b *builtinLETimeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGETimeSig) vectorized() bool {
	return false
}

func (b *builtinGETimeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNEStringSig) vectorized() bool {
	return false
}

func (b *builtinNEStringSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGreatestTimeSig) vectorized() bool {
	return false
}

func (b *builtinGreatestTimeSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLTDecimalSig) vectorized() bool {
	return false
}

func (b *builtinLTDecimalSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGTDurationSig) vectorized() bool {
	return false
}

func (b *builtinGTDurationSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNEJSONSig) vectorized() bool {
	return false
}

func (b *builtinNEJSONSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCoalesceTimeSig) vectorized() bool {
	return false
}

func (b *builtinCoalesceTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGTDecimalSig) vectorized() bool {
	return false
}

func (b *builtinGTDecimalSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLEStringSig) vectorized() bool {
	return false
}

func (b *builtinLEStringSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLEDurationSig) vectorized() bool {
	return false
}

func (b *builtinLEDurationSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCoalesceJSONSig) vectorized() bool {
	return false
}

func (b *builtinCoalesceJSONSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGTRealSig) vectorized() bool {
	return false
}

func (b *builtinGTRealSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGTTimeSig) vectorized() bool {
	return false
}

func (b *builtinGTTimeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGEDecimalSig) vectorized() bool {
	return false
}

func (b *builtinGEDecimalSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinEQRealSig) vectorized() bool {
	return false
}

func (b *builtinEQRealSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
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

func (b *builtinGTStringSig) vectorized() bool {
	return false
}

func (b *builtinGTStringSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLeastTimeSig) vectorized() bool {
	return false
}

func (b *builtinLeastTimeSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNERealSig) vectorized() bool {
	return false
}

func (b *builtinNERealSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGERealSig) vectorized() bool {
	return false
}

func (b *builtinGERealSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinEQDecimalSig) vectorized() bool {
	return false
}

func (b *builtinEQDecimalSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinNullEQDurationSig) vectorized() bool {
	return false
}

func (b *builtinNullEQDurationSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGreatestStringSig) vectorized() bool {
	return false
}

func (b *builtinGreatestStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLTDurationSig) vectorized() bool {
	return false
}

func (b *builtinLTDurationSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}
