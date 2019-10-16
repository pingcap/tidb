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
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/encrypt"
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
	return true
}

func (b *builtinEncodeSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	buf1, err1 := b.bufAllocator.get(types.ETString, n)
	if err1 != nil {
		return err1
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalString(b.ctx, input, buf1); err != nil {
		return err
	}
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}
		decodeStr := buf.GetString(i)
		passwordStr := buf1.GetString(i)
		dataStr, err := encrypt.SQLEncode(decodeStr, passwordStr)
		if err != nil {
			return err
		}
		result.AppendString(dataStr)
	}
	return nil
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
	return true
}

func (b *builtinSHA2Sig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	buf1, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf1)
	if err := b.args[1].VecEvalInt(b.ctx, input, buf1); err != nil {
		return err
	}
	result.ReserveString(n)
	i64s := buf1.Int64s()
	var (
		hasher    *hash.Hash
		hasher256 = sha256.New()
		hasher224 = sha256.New224()
		hasher384 = sha512.New384()
		hasher512 = sha512.New()
	)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}
		hashLength := i64s[i]
		hasher = nil
		switch int(hashLength) {
		case SHA0, SHA256:
			hasher = &hasher256
		case SHA224:
			hasher = &hasher224
		case SHA384:
			hasher = &hasher384
		case SHA512:
			hasher = &hasher512
		}
		if hasher == nil {
			result.AppendNull()
			continue
		}
		str := buf.GetString(i)
		_, err = (*hasher).Write([]byte(str))
		if err != nil {
			return err
		}
		result.AppendString(fmt.Sprintf("%x", (*hasher).Sum(nil)))
		(*hasher).Reset()
	}
	return nil
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
