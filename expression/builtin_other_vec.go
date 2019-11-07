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
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
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
	return false
}

func (b *builtinRowSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
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

func (b *builtinBitCountSig) vectorized() bool {
	return true
}

func (b *builtinBitCountSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if err := b.args[0].VecEvalInt(b.ctx, input, result); err != nil {
		if err != nil && types.ErrOverflow.Equal(err) {
			return err
		}
		return err
	}
	f64s := result.Int64s()
	for i := 0; i < n; i++ {
		var count int64
		if result.IsNull(i) {
			continue
		}
		for ; f64s[i] != 0; f64s[i] = (f64s[i] - 1) & f64s[i] {
			count++
		}
		f64s[i] = count
	}
	return nil
}

func (b *builtinGetParamStringSig) vectorized() bool {
	return false
}

func (b *builtinGetParamStringSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSetVarSig) vectorized() bool {
	return false
}

func (b *builtinSetVarSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinValuesDecimalSig) vectorized() bool {
	return false
}

func (b *builtinValuesDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGetVarSig) vectorized() bool {
	return false
}

func (b *builtinGetVarSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}
