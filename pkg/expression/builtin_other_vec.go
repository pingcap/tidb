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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

func (b *builtinValuesIntSig) vectorized() bool {
	return false
}

func (b *builtinValuesIntSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinValuesDurationSig) vectorized() bool {
	return false
}

func (b *builtinValuesDurationSig) vecEvalDuration(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinRowSig) vectorized() bool {
	return true
}

func (b *builtinRowSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	panic("builtinRowSig.vecEvalString() should never be called.")
}

func (b *builtinValuesRealSig) vectorized() bool {
	return false
}

func (b *builtinValuesRealSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinValuesStringSig) vectorized() bool {
	return false
}

func (b *builtinValuesStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinValuesTimeSig) vectorized() bool {
	return false
}

func (b *builtinValuesTimeSig) vecEvalTime(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinValuesJSONSig) vectorized() bool {
	return false
}

func (b *builtinValuesJSONSig) vecEvalJSON(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
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
func (b *builtinBitCountSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalInt(ctx, input, result); err != nil {
		if types.ErrOverflow.Equal(err) {
			result.ResizeInt64(n, false)
			i64s := result.Int64s()
			for i := 0; i < n; i++ {
				res, isNull, err := b.evalInt(ctx, input.GetRow(i))
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

func (b *builtinGetParamStringSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}
	n := input.NumRows()
	idx, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(idx)
	if err := b.args[0].VecEvalInt(ctx, input, idx); err != nil {
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
		v := sessionVars.PlanCacheParams.GetParamValue(int(idxI))
		str, err := v.ToString()
		if err != nil {
			result.AppendNull()
			continue
		}
		result.AppendString(str)
	}
	return nil
}

func (b *builtinSetStringVarSig) vectorized() bool {
	return true
}

func (b *builtinSetStringVarSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(ctx, input, buf1); err != nil {
		return err
	}
	result.ReserveString(n)
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}
	_, collation := sessionVars.GetCharsetInfo()
	for i := 0; i < n; i++ {
		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}
		varName := strings.ToLower(buf0.GetString(i))
		res := buf1.GetString(i)
		sessionVars.SetUserVarVal(varName, types.NewCollationStringDatum(stringutil.Copy(res), collation))
		result.AppendString(res)
	}
	return nil
}

func (b *builtinSetIntVarSig) vectorized() bool {
	return true
}

func (b *builtinSetIntVarSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(ctx, input, buf1); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.SetNull(i, true)
			continue
		}
		varName := strings.ToLower(buf0.GetString(i))
		res := buf1.GetInt64(i)
		sessionVars.SetUserVarVal(varName, types.NewIntDatum(res))
		i64s[i] = res
	}
	return nil
}

func (b *builtinSetRealVarSig) vectorized() bool {
	return true
}

func (b *builtinSetRealVarSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalReal(ctx, input, buf1); err != nil {
		return err
	}
	result.ResizeFloat64(n, false)
	f64s := result.Float64s()
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.SetNull(i, true)
			continue
		}
		varName := strings.ToLower(buf0.GetString(i))
		res := buf1.GetFloat64(i)
		sessionVars.SetUserVarVal(varName, types.NewFloat64Datum(res))
		f64s[i] = res
	}
	return nil
}

func (b *builtinSetDecimalVarSig) vectorized() bool {
	return true
}

func (b *builtinSetDecimalVarSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalDecimal(ctx, input, buf1); err != nil {
		return err
	}
	result.ResizeDecimal(n, false)
	decs := result.Decimals()
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if buf0.IsNull(i) || buf1.IsNull(i) {
			result.SetNull(i, true)
			continue
		}
		varName := strings.ToLower(buf0.GetString(i))
		res := buf1.GetDecimal(i)
		sessionVars.SetUserVarVal(varName, types.NewDecimalDatum(res))
		decs[i] = *res
	}
	return nil
}

func (b *builtinValuesDecimalSig) vectorized() bool {
	return false
}

func (b *builtinValuesDecimalSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGetStringVarSig) vectorized() bool {
	return true
}

func (b *builtinGetStringVarSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	result.ReserveString(n)
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if buf0.IsNull(i) {
			result.AppendNull()
			continue
		}
		varName := strings.ToLower(buf0.GetString(i))
		if v, ok := sessionVars.GetUserVarVal(varName); ok {
			res, err := v.ToString()
			if err != nil {
				return err
			}
			result.AppendString(res)
			continue
		}
		result.AppendNull()
	}
	return nil
}

func (b *builtinGetIntVarSig) vectorized() bool {
	return true
}

func (b *builtinGetIntVarSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	result.ResizeInt64(n, false)
	result.MergeNulls(buf0)
	i64s := result.Int64s()
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		varName := strings.ToLower(buf0.GetString(i))
		if v, ok := sessionVars.GetUserVarVal(varName); ok {
			i64s[i] = v.GetInt64()
			continue
		}
		result.SetNull(i, true)
	}
	return nil
}

func (b *builtinGetRealVarSig) vectorized() bool {
	return true
}

// NOTE: get/set variable vectorized eval was disabled. See more in
// https://github.com/pingcap/tidb/pull/8412
func (b *builtinGetRealVarSig) vecEvalReal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	result.ResizeFloat64(n, false)
	result.MergeNulls(buf0)
	f64s := result.Float64s()
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		varName := strings.ToLower(buf0.GetString(i))
		if v, ok := sessionVars.GetUserVarVal(varName); ok {
			d, err := v.ToFloat64(typeCtx(ctx))
			if err != nil {
				return err
			}
			f64s[i] = d
			continue
		}
		result.SetNull(i, true)
	}
	return nil
}

func (b *builtinGetDecimalVarSig) vectorized() bool {
	return true
}

// NOTE: get/set variable vectorized eval was disabled. See more in
// https://github.com/pingcap/tidb/pull/8412
func (b *builtinGetDecimalVarSig) vecEvalDecimal(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf0, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf0)
	if err := b.args[0].VecEvalString(ctx, input, buf0); err != nil {
		return err
	}
	result.ResizeDecimal(n, false)
	result.MergeNulls(buf0)
	decs := result.Decimals()
	sessionVars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if result.IsNull(i) {
			continue
		}
		varName := strings.ToLower(buf0.GetString(i))
		if v, ok := sessionVars.GetUserVarVal(varName); ok {
			d, err := v.ToDecimal(typeCtx(ctx))
			if err != nil {
				return err
			}
			decs[i] = *d
			continue
		}
		result.SetNull(i, true)
	}
	return nil
}
