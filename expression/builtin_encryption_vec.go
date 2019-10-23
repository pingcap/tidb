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
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
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
	return true
}

func (b *builtinDecodeSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
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
		dataStr := buf.GetString(i)
		passwordStr := buf1.GetString(i)
		decodeStr, err := encrypt.SQLDecode(dataStr, passwordStr)
		if err != nil {
			return err
		}
		result.AppendString(decodeStr)
	}
	return nil
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
	return true
}

func (b *builtinMD5Sig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(n)
	digest := md5.New()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		cryptByte := buf.GetBytes(i)
		_, err := digest.Write(cryptByte)
		if err != nil {
			return err
		}
		result.AppendString(fmt.Sprintf("%x", digest.Sum(nil)))
		digest.Reset()
	}
	return nil
}

func (b *builtinSHA2Sig) vectorized() bool {
	return true
}

// vecEvalString evals SHA2(str, hash_length).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_sha2
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
		hasher224 hash.Hash
		hasher256 hash.Hash
		hasher384 hash.Hash
		hasher512 hash.Hash
	)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) || buf1.IsNull(i) {
			result.AppendNull()
			continue
		}
		hashLength := i64s[i]
		var hasher hash.Hash
		switch int(hashLength) {
		case SHA0, SHA256:
			if hasher256 == nil {
				hasher256 = sha256.New()
			}
			hasher = hasher256
		case SHA224:
			if hasher224 == nil {
				hasher224 = sha256.New224()
			}
			hasher = hasher224
		case SHA384:
			if hasher384 == nil {
				hasher384 = sha512.New384()
			}
			hasher = hasher384
		case SHA512:
			if hasher512 == nil {
				hasher512 = sha512.New()
			}
			hasher = hasher512
		}
		if hasher == nil {
			result.AppendNull()
			continue
		}

		str := buf.GetBytes(i)
		_, err = hasher.Write(str)
		if err != nil {
			return err
		}
		result.AppendString(fmt.Sprintf("%x", hasher.Sum(nil)))
		hasher.Reset()
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
	return true
}

func (b *builtinPasswordSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendString("")
			continue
		}
		pass := buf.GetString(i)
		if len(pass) == 0 {
			result.AppendString("")
			continue
		}
		// We should append a warning here because function "PASSWORD" is deprecated since MySQL 5.7.6.
		// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_password
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errDeprecatedSyntaxNoReplacement.GenWithStackByArgs("PASSWORD"))

		result.AppendString(auth.EncodePassword(pass))
	}
	return nil
}

func (b *builtinSHA1Sig) vectorized() bool {
	return true
}

func (b *builtinSHA1Sig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(b.ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(n)
	hasher := sha1.New()
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		str := buf.GetBytes(i)
		_, err = hasher.Write(str)
		if err != nil {
			return err
		}
		result.AppendString(fmt.Sprintf("%x", hasher.Sum(nil)))
		hasher.Reset()
	}
	return nil
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
