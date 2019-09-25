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

func (b *builtinTimeIsNullSig) vectorized() bool {
	return false
}

func (b *builtinTimeIsNullSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLogicOrSig) vectorized() bool {
	return false
}

func (b *builtinLogicOrSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinBitOrSig) vectorized() bool {
	return false
}

func (b *builtinBitOrSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinDecimalIsFalseSig) vectorized() bool {
	return false
}

func (b *builtinDecimalIsFalseSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinIntIsFalseSig) vectorized() bool {
	return false
}

func (b *builtinIntIsFalseSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinUnaryMinusRealSig) vectorized() bool {
	return false
}

func (b *builtinUnaryMinusRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinBitNegSig) vectorized() bool {
	return false
}

func (b *builtinBitNegSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinUnaryMinusDecimalSig) vectorized() bool {
	return false
}

func (b *builtinUnaryMinusDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinIntIsNullSig) vectorized() bool {
	return false
}

func (b *builtinIntIsNullSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinRealIsNullSig) vectorized() bool {
	return false
}

func (b *builtinRealIsNullSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinUnaryNotRealSig) vectorized() bool {
	return false
}

func (b *builtinUnaryNotRealSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLogicAndSig) vectorized() bool {
	return false
}

func (b *builtinLogicAndSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinBitXorSig) vectorized() bool {
	return false
}

func (b *builtinBitXorSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLogicXorSig) vectorized() bool {
	return false
}

func (b *builtinLogicXorSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinBitAndSig) vectorized() bool {
	return false
}

func (b *builtinBitAndSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinRealIsFalseSig) vectorized() bool {
	return false
}

func (b *builtinRealIsFalseSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinUnaryMinusIntSig) vectorized() bool {
	return false
}

func (b *builtinUnaryMinusIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinUnaryNotDecimalSig) vectorized() bool {
	return false
}

func (b *builtinUnaryNotDecimalSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinUnaryNotIntSig) vectorized() bool {
	return false
}

func (b *builtinUnaryNotIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinDecimalIsNullSig) vectorized() bool {
	return false
}

func (b *builtinDecimalIsNullSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinLeftShiftSig) vectorized() bool {
	return false
}

func (b *builtinLeftShiftSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinRightShiftSig) vectorized() bool {
	return false
}

func (b *builtinRightShiftSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinRealIsTrueSig) vectorized() bool {
	return false
}

func (b *builtinRealIsTrueSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinDecimalIsTrueSig) vectorized() bool {
	return false
}

func (b *builtinDecimalIsTrueSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinIntIsTrueSig) vectorized() bool {
	return false
}

func (b *builtinIntIsTrueSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinDurationIsNullSig) vectorized() bool {
	return false
}

func (b *builtinDurationIsNullSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}
