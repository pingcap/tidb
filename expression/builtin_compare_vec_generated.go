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

func (b *builtinCoalesceIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeInt64(n, true)
	i64s := result.Int64s()
	buf1, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	for j := 0; j < len(b.args); j++ {

		if err := b.args[j].VecEvalInt(b.ctx, input, buf1); err != nil {
			return err
		}
		args := buf1.Int64s()
		for i := 0; i < n; i++ {
			if !buf1.IsNull(i) && result.IsNull(i) {
				i64s[i] = args[i]
				result.SetNull(i, buf1.IsNull(i))
				continue
			}
			if !result.IsNull(i) {
				continue
			}
			result.SetNull(i, true)
		}
	}
	return nil
}

func (b *builtinCoalesceIntSig) vectorized() bool {
	return true
}

func (b *builtinCoalesceRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeFloat64(n, true)
	i64s := result.Float64s()
	buf1, err := b.bufAllocator.get(types.ETReal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	for j := 0; j < len(b.args); j++ {

		if err := b.args[j].VecEvalReal(b.ctx, input, buf1); err != nil {
			return err
		}
		args := buf1.Float64s()
		for i := 0; i < n; i++ {
			if !buf1.IsNull(i) && result.IsNull(i) {
				i64s[i] = args[i]
				result.SetNull(i, buf1.IsNull(i))
				continue
			}
			if !result.IsNull(i) {
				continue
			}
			result.SetNull(i, true)
		}
	}
	return nil
}

func (b *builtinCoalesceRealSig) vectorized() bool {
	return true
}

func (b *builtinCoalesceDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeDecimal(n, true)
	i64s := result.Decimals()
	buf1, err := b.bufAllocator.get(types.ETDecimal, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	for j := 0; j < len(b.args); j++ {

		if err := b.args[j].VecEvalDecimal(b.ctx, input, buf1); err != nil {
			return err
		}
		args := buf1.Decimals()
		for i := 0; i < n; i++ {
			if !buf1.IsNull(i) && result.IsNull(i) {
				i64s[i] = args[i]
				result.SetNull(i, buf1.IsNull(i))
				continue
			}
			if !result.IsNull(i) {
				continue
			}
			result.SetNull(i, true)
		}
	}
	return nil
}

func (b *builtinCoalesceDecimalSig) vectorized() bool {
	return true
}

func (b *builtinCoalesceStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	result.ReserveString(n)

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

	buf2.ReserveString(len(b.args) * n)
	for j := 0; j < len(b.args); j++ {
		if err := b.args[j].VecEvalString(b.ctx, input, buf1); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if buf1.IsNull(i) {
				buf2.AppendNull()
				continue
			}
			buf2.AppendString(buf1.GetString(i))
		}
	}
	for i := 0; i < n; i++ {
		for j := 0; j < len(b.args); j++ {
			index := i + j*n
			if !buf2.IsNull(index) {
				result.AppendString(buf2.GetString(index))
				break
			}
			if j == len(b.args)-1 && buf2.IsNull(index) {
				result.AppendNull()
			}
		}
	}
	return nil
}

func (b *builtinCoalesceStringSig) vectorized() bool {
	return true
}

func (b *builtinCoalesceTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeTime(n, true)
	i64s := result.Times()
	buf1, err := b.bufAllocator.get(types.ETDatetime, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	for j := 0; j < len(b.args); j++ {

		if err := b.args[j].VecEvalTime(b.ctx, input, buf1); err != nil {
			return err
		}
		args := buf1.Times()
		for i := 0; i < n; i++ {
			if !buf1.IsNull(i) && result.IsNull(i) {
				i64s[i] = args[i]
				result.SetNull(i, buf1.IsNull(i))
				continue
			}
			if !result.IsNull(i) {
				continue
			}
			result.SetNull(i, true)
		}
	}
	return nil
}

func (b *builtinCoalesceTimeSig) vectorized() bool {
	return true
}

func (b *builtinCoalesceDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeGoDuration(n, true)
	i64s := result.GoDurations()
	buf1, err := b.bufAllocator.get(types.ETDuration, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	for j := 0; j < len(b.args); j++ {

		if err := b.args[j].VecEvalDuration(b.ctx, input, buf1); err != nil {
			return err
		}
		args := buf1.GoDurations()
		for i := 0; i < n; i++ {
			if !buf1.IsNull(i) && result.IsNull(i) {
				i64s[i] = args[i]
				result.SetNull(i, buf1.IsNull(i))
				continue
			}
			if !result.IsNull(i) {
				continue
			}
			result.SetNull(i, true)
		}
	}
	return nil
}

func (b *builtinCoalesceDurationSig) vectorized() bool {
	return true
}

func (b *builtinCoalesceJSONSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	result.ReserveJSON(n)

	buf1, err := b.bufAllocator.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)

	buf2, err := b.bufAllocator.get(types.ETJson, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf2)

	buf2.ReserveJSON(len(b.args) * n)
	for j := 0; j < len(b.args); j++ {
		if err := b.args[j].VecEvalJSON(b.ctx, input, buf1); err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if buf1.IsNull(i) {
				buf2.AppendNull()
				continue
			}
			buf2.AppendJSON(buf1.GetJSON(i))
		}
	}
	for i := 0; i < n; i++ {
		for j := 0; j < len(b.args); j++ {
			index := i + j*n
			if !buf2.IsNull(index) {
				result.AppendJSON(buf2.GetJSON(index))
				break
			}
			if j == len(b.args)-1 && buf2.IsNull(index) {
				result.AppendNull()
			}
		}
	}
	return nil
}

func (b *builtinCoalesceJSONSig) vectorized() bool {
	return true
}
