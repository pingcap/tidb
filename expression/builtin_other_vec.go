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

func (b *builtinInDecimalSig) vectorized() bool {
	return false
}

func (b *builtinInDecimalSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinInTimeSig) vectorized() bool {
	return false
}

func (b *builtinInTimeSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinInJSONSig) vectorized() bool {
	return false
}

func (b *builtinInJSONSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
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

func (b *builtinInIntSig) vectorized() bool {
	return false
}

func (b *builtinInIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinInRealSig) vectorized() bool {
	return false
}

func (b *builtinInRealSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinBitCountSig) vectorized() bool {
	return false
}

func (b *builtinBitCountSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
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

func (b *builtinInStringSig) vectorized() bool {
	return false
}

func (b *builtinInStringSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinInDurationSig) vectorized() bool {
	return false
}

func (b *builtinInDurationSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinGetVarSig) vectorized() bool {
	return false
}

func (b *builtinGetVarSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}
