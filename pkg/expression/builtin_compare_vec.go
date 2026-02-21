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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// vecEvalDecimal evals a builtinGreatestDecimalSig.
// See http://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_greatest
func (b *builtinGreatestDecimalSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(ctx, input, result); err != nil {
		return err
	}

	d64s := result.Decimals()
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalDecimal(ctx, input, buf); err != nil {
			return err
		}
		result.MergeNulls(buf)
		for i := range n {
			if result.IsNull(i) {
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

func (b *builtinLeastDecimalSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalDecimal(ctx, input, result); err != nil {
		return err
	}

	d64s := result.Decimals()
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalDecimal(ctx, input, buf); err != nil {
			return err
		}

		result.MergeNulls(buf)
		for i := range n {
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

func (b *builtinLeastIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}

	i64s := result.Int64s()
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalInt(ctx, input, buf); err != nil {
			return err
		}

		result.MergeNulls(buf)
		for i := range n {
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

func (b *builtinGreatestIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		return err
	}

	i64s := result.Int64s()
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalInt(ctx, input, buf); err != nil {
			return err
		}

		result.MergeNulls(buf)
		v := buf.Int64s()
		for i := range n {
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

func (b *builtinGEIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err = b.args[0].VecEvalInt(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err = b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()), mysql.HasUnsignedFlag(b.args[1].GetType(ctx).GetFlag()), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfGE(result.Int64s())
	return nil
}

func (b *builtinLeastRealSig) vectorized() bool {
	return true
}

func (b *builtinLeastRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalReal(ctx, input, result); err != nil {
		return err
	}

	f64s := result.Float64s()
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalReal(ctx, input, buf); err != nil {
			return err
		}

		result.MergeNulls(buf)
		v := buf.Float64s()
		for i := range n {
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

func (b *builtinLeastStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalString(ctx, input, result); err != nil {
		return err
	}

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

	src := result
	arg := buf1
	dst := buf2
	dst.ReserveString(n)
	for j := 1; j < len(b.args); j++ {
		if err := b.args[j].VecEvalString(ctx, input, arg); err != nil {
			return err
		}
		for i := range n {
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

func (b *builtinEQIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()), mysql.HasUnsignedFlag(b.args[1].GetType(ctx).GetFlag()), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfEQ(result.Int64s())
	return nil
}

func (b *builtinNEIntSig) vectorized() bool {
	return true
}

func (b *builtinNEIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()), mysql.HasUnsignedFlag(b.args[1].GetType(ctx).GetFlag()), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfNE(result.Int64s())
	return nil
}

func (b *builtinGTIntSig) vectorized() bool {
	return true
}

func (b *builtinGTIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()), mysql.HasUnsignedFlag(b.args[1].GetType(ctx).GetFlag()), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfGT(result.Int64s())
	return nil
}

func (b *builtinNullEQIntSig) vectorized() bool {
	return true
}

func (b *builtinNullEQIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	result.ResizeInt64(n, false)
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()), mysql.HasUnsignedFlag(b.args[1].GetType(ctx).GetFlag()), buf0, buf1, result)
	i64s := result.Int64s()
	for i := range n {
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

func (b *builtinIntervalIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	var err error
	if err = b.args[0].VecEvalInt(ctx, input, result); err != nil {
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
		if b.hasNullable {
			idx, err = b.linearSearch(ctx, v, mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()), b.args[1:], input.GetRow(i))
		} else {
			idx, err = b.binSearch(ctx, v, mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()), b.args[1:], input.GetRow(i))
		}
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

func (b *builtinIntervalRealSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err = b.args[0].VecEvalReal(ctx, input, buf); err != nil {
		return err
	}

	f64s := buf.Float64s()
	result.ResizeInt64(n, false)
	res := result.Int64s()
	var idx int
	for i := range n {
		if buf.IsNull(i) {
			res[i] = -1
			continue
		}
		if b.hasNullable {
			idx, err = b.linearSearch(ctx, f64s[i], b.args[1:], input.GetRow(i))
		} else {
			idx, err = b.binSearch(ctx, f64s[i], b.args[1:], input.GetRow(i))
		}
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

func (b *builtinLEIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()), mysql.HasUnsignedFlag(b.args[1].GetType(ctx).GetFlag()), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfLE(result.Int64s())
	return nil
}

func (b *builtinLTIntSig) vectorized() bool {
	return true
}

func (b *builtinLTIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var err error
	var buf0, buf1 *chunk.Column
	buf0, err = b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalInt(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err = b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	vecCompareInt(mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()), mysql.HasUnsignedFlag(b.args[1].GetType(ctx).GetFlag()), buf0, buf1, result)
	result.MergeNulls(buf0, buf1)
	vecResOfLT(result.Int64s())
	return nil
}

