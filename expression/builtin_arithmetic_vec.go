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

func (b *builtinArithmeticMultiplyRealSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticMultiplyRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticDivideDecimalSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticDivideDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticModIntSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticModIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticMinusRealSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticMinusRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticMinusDecimalSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticMinusDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticMinusIntSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticMinusIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticModRealSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticModRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticModDecimalSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticModDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticPlusRealSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticPlusRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticMultiplyDecimalSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticMultiplyDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticIntDivideDecimalSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticIntDivideDecimalSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticMultiplyIntSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticMultiplyIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticDivideRealSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticDivideRealSig) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticIntDivideIntSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticIntDivideIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticPlusIntSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticPlusIntSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticPlusDecimalSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticPlusDecimalSig) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinArithmeticMultiplyIntUnsignedSig) vectorized() bool {
	return false
}

func (b *builtinArithmeticMultiplyIntUnsignedSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}
