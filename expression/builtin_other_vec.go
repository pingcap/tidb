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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/stringutil"
)

func (b *builtinValuesIntSig) vectorized() bool {
	return false
}

func (b *builtinValuesIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinValuesDurationSig) vectorized() bool {
	return false
}

func (b *builtinValuesDurationSig) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinRowSig) vectorized() bool {
	return true
}

func (b *builtinRowSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	panic("builtinRowSig.vecEvalString() should never be called.")
}

func (b *builtinValuesRealSig) vectorized() bool {
	return false
}

func (b *builtinValuesRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinValuesStringSig) vectorized() bool {
	return false
}

func (b *builtinValuesStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinValuesTimeSig) vectorized() bool {
	return false
}

func (b *builtinValuesTimeSig) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinValuesJSONSig) vectorized() bool {
	return false
}

func (b *builtinValuesJSONSig) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

// bitCount returns the number of bits that are set in the argument 'value'.
func bitCount(value int64) int64 {
	value = value - ((value >> 1) & 0x5555555555555555)
	value = (value & 0x3333333333333333) + ((value >> 2) & 0x3333333333333333)
	value = (value & 0x0f0f0f0f0f0f0f0f) + ((value >> 4) & 0x0f0f0f0f0f0f0f0f)
	value = value + (value >> 8)
	value = value + (value >> 16)
	value = value + (value >> 32)
	value = value & 0x7f
	return value
}
func (b *builtinBitCountSig) vectorized() bool {
	return true
}
func (b *builtinBitCountSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		if types.ErrOverflow.Equal(err) {
			result.ResizeInt64(n, false)
			i64s := result.Int64s()
			for i := 0; i < n; i++ {
				res, isNull, err := b.evalInt(input.GetRow(i))
				if err != nil {
					return err
				}
				result.SetNull(i, isNull)
				i64s[i] = res
			}
			return nil
		}
		return err
	}
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		i64s[i] = bitCount(i64s[i])
	}
	return nil
}

func (b *builtinGetParamStringSig) vectorized() bool {
	return true
}

func (b *builtinGetParamStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	sessionVars := b.ctx.GetSessionVars()
	n := input.NumRows()
	idx, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(idx)
	if err := b.args[0].VecEvalInt(b.ctx, input, idx); err != nil {
		return err
	}
	idxIs := idx.Int64s()
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if idx.IsNull(i) {
			result.AppendNull()
			continue
		}
		idxI := idxIs[i]
		v := sessionVars.PreparedParams[idxI]
		str, err := v.ToString()
		if err != nil {
			result.AppendNull()
			continue
		}
		result.AppendString(str)
	}
	return nil
}

func (b *builtinSetVarSig) vectorized() bool {
	return true
}

func (b *builtinSetVarSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
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
	result.ReserveString(n)
	sessionVars := b.ctx.GetSessionVars()
	sessionVars.UsersLock.Lock()
	defer sessionVars.UsersLock.Unlock()
	_, collation := sessionVars.GetCharsetInfo()
	for i := 0; i < n; i++ {
		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}
		varName := strings.ToLower(buf0.GetString(i))
		res := buf1.GetString(i)
		sessionVars.Users[varName] = types.NewCollationStringDatum(stringutil.Copy(res), collation, collate.DefaultLen)
		result.AppendString(res)
	}
	return nil
}

func (b *builtinValuesDecimalSig) vectorized() bool {
	return false
}

func (b *builtinValuesDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGetVarSig) vectorized() bool {
	return true
}

func (b *builtinGetVarSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(b.ctx, input, buf0); err != nil {
		return err
	}
	result.ReserveString(n)
	sessionVars := b.ctx.GetSessionVars()
	sessionVars.UsersLock.Lock()
	defer sessionVars.UsersLock.Unlock()
	for i := 0; i < n; i++ {
		if buf0.IsNull(i) {
			result.AppendNull()
			continue
		}
		varName := strings.ToLower(buf0.GetString(i))
		if v, ok := sessionVars.Users[varName]; ok {
			result.AppendString(v.GetString())
			continue
		}
		result.AppendNull()
	}
	return nil
}
