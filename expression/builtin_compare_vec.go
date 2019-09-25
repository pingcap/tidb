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
