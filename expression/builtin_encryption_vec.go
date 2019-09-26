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

func (b *builtinAesDecryptSig) vectorized() bool {
	return false
}

func (b *builtinAesDecryptSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinAesEncryptIVSig) vectorized() bool {
	return false
}

func (b *builtinAesEncryptIVSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinDecodeSig) vectorized() bool {
	return false
}

func (b *builtinDecodeSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinEncodeSig) vectorized() bool {
	return false
}

func (b *builtinEncodeSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinAesDecryptIVSig) vectorized() bool {
	return false
}

func (b *builtinAesDecryptIVSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinRandomBytesSig) vectorized() bool {
	return false
}

func (b *builtinRandomBytesSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinMD5Sig) vectorized() bool {
	return false
}

func (b *builtinMD5Sig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSHA2Sig) vectorized() bool {
	return false
}

func (b *builtinSHA2Sig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinCompressSig) vectorized() bool {
	return false
}

func (b *builtinCompressSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinAesEncryptSig) vectorized() bool {
	return false
}

func (b *builtinAesEncryptSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinPasswordSig) vectorized() bool {
	return false
}

func (b *builtinPasswordSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinSHA1Sig) vectorized() bool {
	return false
}

func (b *builtinSHA1Sig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinUncompressSig) vectorized() bool {
	return false
}

func (b *builtinUncompressSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}

func (b *builtinUncompressedLengthSig) vectorized() bool {
	return false
}

func (b *builtinUncompressedLengthSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("not implemented")
}
