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
	"bytes"
	"crypto/aes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/chunk"
	"github.com/pingcap/tidb/v4/util/encrypt"
	"github.com/pingcap/tidb/v4/util/hack"
)

func (b *builtinAesDecryptSig) vectorized() bool {
	return true
}

func (b *builtinAesDecryptSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	strBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(strBuf)
	if err := b.args[0].VecEvalString(b.ctx, input, strBuf); err != nil {
		return err
	}

	keyBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(keyBuf)
	if err := b.args[1].VecEvalString(b.ctx, input, keyBuf); err != nil {
		return err
	}

	if b.modeName != "ecb" {
		return errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}

	isWarning := !b.ivRequired && len(b.args) == 3
	isConstKey := b.args[1].ConstItem(b.ctx.GetSessionVars().StmtCtx)

	var key []byte
	if isConstKey {
		key = encrypt.DeriveKeyMySQL(keyBuf.GetBytes(0), b.keySize)
	}

	result.ReserveString(n)
	stmtCtx := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < n; i++ {
		// According to doc: If either function argument is NULL, the function returns NULL.
		if strBuf.IsNull(i) || keyBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		if isWarning {
			// For modes that do not require init_vector, it is ignored and a warning is generated if it is specified.
			stmtCtx.AppendWarning(errWarnOptionIgnored.GenWithStackByArgs("IV"))
		}
		if !isConstKey {
			key = encrypt.DeriveKeyMySQL(keyBuf.GetBytes(i), b.keySize)
		}
		// ANNOTATION:
		// we can't use GetBytes here because GetBytes return raw memory in strBuf,
		// and the memory will be modified in AESEncryptWithECB & AESDecryptWithECB
		str := []byte(strBuf.GetString(i))
		plainText, err := encrypt.AESDecryptWithECB(str, key)
		if err != nil {
			result.AppendNull()
			continue
		}
		result.AppendBytes(plainText)
	}

	return nil
}

func (b *builtinAesEncryptIVSig) vectorized() bool {
	return true
}

// evalString evals AES_ENCRYPT(str, key_str, iv).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesEncryptIVSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	strBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(strBuf)
	if err := b.args[0].VecEvalString(b.ctx, input, strBuf); err != nil {
		return err
	}

	keyBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(keyBuf)
	if err := b.args[1].VecEvalString(b.ctx, input, keyBuf); err != nil {
		return err
	}

	ivBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(ivBuf)
	if err := b.args[2].VecEvalString(b.ctx, input, ivBuf); err != nil {
		return err
	}

	isCBC := false
	isOFB := false
	isCFB := false
	switch b.modeName {
	case "cbc":
		isCBC = true
	case "ofb":
		isOFB = true
	case "cfb":
		isCFB = true
	default:
		return errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}

	isConst := b.args[1].ConstItem(b.ctx.GetSessionVars().StmtCtx)
	var key []byte
	if isConst {
		key = encrypt.DeriveKeyMySQL(keyBuf.GetBytes(0), b.keySize)
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		// According to doc: If either function argument is NULL, the function returns NULL.
		if strBuf.IsNull(i) || keyBuf.IsNull(i) || ivBuf.IsNull(i) {
			result.AppendNull()
			continue
		}

		iv := ivBuf.GetBytes(i)
		if len(iv) < aes.BlockSize {
			return errIncorrectArgs.GenWithStack("The initialization vector supplied to aes_encrypt is too short. Must be at least %d bytes long", aes.BlockSize)
		}
		// init_vector must be 16 bytes or longer (bytes in excess of 16 are ignored)
		iv = iv[0:aes.BlockSize]
		if !isConst {
			key = encrypt.DeriveKeyMySQL(keyBuf.GetBytes(i), b.keySize)
		}
		var cipherText []byte

		// ANNOTATION:
		// we can't use GetBytes here because GetBytes return raw memory in strBuf,
		// and the memory will be modified in AESEncryptWithCBC & AESEncryptWithOFB & AESEncryptWithCFB
		if isCBC {
			cipherText, err = encrypt.AESEncryptWithCBC([]byte(strBuf.GetString(i)), key, iv)
		}
		if isOFB {
			cipherText, err = encrypt.AESEncryptWithOFB([]byte(strBuf.GetString(i)), key, iv)
		}
		if isCFB {
			cipherText, err = encrypt.AESEncryptWithCFB([]byte(strBuf.GetString(i)), key, iv)
		}
		if err != nil {
			result.AppendNull()
			continue
		}
		result.AppendBytes(cipherText)
	}
	return nil
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
	return true
}

// evalString evals AES_DECRYPT(crypt_str, key_key, iv).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesDecryptIVSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	strBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(strBuf)
	if err := b.args[0].VecEvalString(b.ctx, input, strBuf); err != nil {
		return err
	}

	keyBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(keyBuf)
	if err := b.args[1].VecEvalString(b.ctx, input, keyBuf); err != nil {
		return err
	}

	ivBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(ivBuf)
	if err := b.args[2].VecEvalString(b.ctx, input, ivBuf); err != nil {
		return err
	}

	isCBC := false
	isOFB := false
	isCFB := false
	switch b.modeName {
	case "cbc":
		isCBC = true
	case "ofb":
		isOFB = true
	case "cfb":
		isCFB = true
	default:
		return errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}

	isConst := b.args[1].ConstItem(b.ctx.GetSessionVars().StmtCtx)
	var key []byte
	if isConst {
		key = encrypt.DeriveKeyMySQL(keyBuf.GetBytes(0), b.keySize)
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		// According to doc: If either function argument is NULL, the function returns NULL.
		if strBuf.IsNull(i) || keyBuf.IsNull(i) || ivBuf.IsNull(i) {
			result.AppendNull()
			continue
		}

		iv := ivBuf.GetBytes(i)
		if len(iv) < aes.BlockSize {
			return errIncorrectArgs.GenWithStack("The initialization vector supplied to aes_decrypt is too short. Must be at least %d bytes long", aes.BlockSize)
		}
		// init_vector must be 16 bytes or longer (bytes in excess of 16 are ignored)
		iv = iv[0:aes.BlockSize]
		if !isConst {
			key = encrypt.DeriveKeyMySQL(keyBuf.GetBytes(i), b.keySize)
		}
		var plainText []byte

		// ANNOTATION:
		// we can't use GetBytes here because GetBytes return raw memory in strBuf,
		// and the memory will be modified in AESDecryptWithCBC & AESDecryptWithOFB & AESDecryptWithCFB
		if isCBC {
			plainText, err = encrypt.AESDecryptWithCBC([]byte(strBuf.GetString(i)), key, iv)
		}
		if isOFB {
			plainText, err = encrypt.AESDecryptWithOFB([]byte(strBuf.GetString(i)), key, iv)
		}
		if isCFB {
			plainText, err = encrypt.AESDecryptWithCFB([]byte(strBuf.GetString(i)), key, iv)
		}
		if err != nil {
			result.AppendNull()
			continue
		}
		result.AppendBytes(plainText)
	}
	return nil
}

func (b *builtinRandomBytesSig) vectorized() bool {
	return true
}

func (b *builtinRandomBytesSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get(types.ETInt, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalInt(b.ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(n)
	i64s := buf.Int64s()
	var dst bytes.Buffer
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		byteLen := i64s[i]
		if byteLen < 1 || byteLen > 1024 {
			return types.ErrOverflow.GenWithStackByArgs("length", "random_bytes")
		}
		if n, err := io.CopyN(&dst, rand.Reader, byteLen); err != nil {
			return err
		} else if n != byteLen {
			return errors.New("fail to generate random bytes")
		}
		result.AppendBytes(dst.Bytes())
		dst.Reset()
	}
	return nil
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
	return true
}

var (
	defaultByteSliceSize = 1024
	bytePool             = sync.Pool{
		New: func() interface{} {
			return make([]byte, defaultByteSliceSize)
		},
	}
)

func allocByteSlice(n int) []byte {
	if n > defaultByteSliceSize {
		return make([]byte, n)
	}
	return bytePool.Get().([]byte)
}

func deallocateByteSlice(b []byte) {
	if cap(b) <= defaultByteSliceSize {
		bytePool.Put(b)
	}
}

// evalString evals COMPRESS(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_compress
func (b *builtinCompressSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
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
			result.AppendNull()
			continue
		}

		str := buf.GetString(i)

		// According to doc: Empty strings are stored as empty strings.
		if len(str) == 0 {
			result.AppendString("")
		}

		compressed, err := deflate(hack.Slice(str))
		if err != nil {
			result.AppendNull()
			continue
		}

		resultLength := 4 + len(compressed)

		// append "." if ends with space
		shouldAppendSuffix := compressed[len(compressed)-1] == 32
		if shouldAppendSuffix {
			resultLength++
		}

		buffer := allocByteSlice(resultLength)
		defer deallocateByteSlice(buffer)
		buffer = buffer[:resultLength]

		binary.LittleEndian.PutUint32(buffer, uint32(len(str)))
		copy(buffer[4:], compressed)

		if shouldAppendSuffix {
			buffer[len(buffer)-1] = '.'
		}

		result.AppendBytes(buffer)
	}
	return nil
}

func (b *builtinAesEncryptSig) vectorized() bool {
	return true
}

// evalString evals AES_ENCRYPT(str, key_str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesEncryptSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	strBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(strBuf)
	if err := b.args[0].VecEvalString(b.ctx, input, strBuf); err != nil {
		return err
	}

	keyBuf, err := b.bufAllocator.get(types.ETString, n)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(keyBuf)
	if err := b.args[1].VecEvalString(b.ctx, input, keyBuf); err != nil {
		return err
	}

	if b.modeName != "ecb" {
		// For modes that do not require init_vector, it is ignored and a warning is generated if it is specified.
		return errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}

	isWarning := !b.ivRequired && len(b.args) == 3
	isConst := b.args[1].ConstItem(b.ctx.GetSessionVars().StmtCtx)
	var key []byte
	if isConst {
		key = encrypt.DeriveKeyMySQL(keyBuf.GetBytes(0), b.keySize)
	}

	sc := b.ctx.GetSessionVars().StmtCtx
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		// According to doc: If either function argument is NULL, the function returns NULL.
		if strBuf.IsNull(i) || keyBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		if isWarning {
			sc.AppendWarning(errWarnOptionIgnored.GenWithStackByArgs("IV"))
		}
		if !isConst {
			key = encrypt.DeriveKeyMySQL(keyBuf.GetBytes(i), b.keySize)
		}

		// NOTE: we can't use GetBytes, because in AESEncryptWithECB padding is automatically
		//       added to str and this will damange the data layout in chunk.Column
		str := []byte(strBuf.GetString(i))
		cipherText, err := encrypt.AESEncryptWithECB(str, key)
		if err != nil {
			result.AppendNull()
			continue
		}
		result.AppendBytes(cipherText)
	}

	return nil
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
	return true
}

// evalString evals UNCOMPRESS(compressed_string).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_uncompress
func (b *builtinUncompressSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
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
	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		payload := buf.GetString(i)

		if len(payload) == 0 {
			result.AppendString("")
			continue
		}
		if len(payload) <= 4 {
			// corrupted
			sc.AppendWarning(errZlibZData)
			result.AppendNull()
			continue
		}
		length := binary.LittleEndian.Uint32([]byte(payload[0:4]))
		bytes, err := inflate([]byte(payload[4:]))
		if err != nil {
			sc.AppendWarning(errZlibZData)
			result.AppendNull()
			continue
		}
		if length < uint32(len(bytes)) {
			sc.AppendWarning(errZlibZBuf)
			result.AppendNull()
			continue
		}

		result.AppendBytes(bytes)
	}

	return nil
}

func (b *builtinUncompressedLengthSig) vectorized() bool {
	return true
}

func (b *builtinUncompressedLengthSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	sc := b.ctx.GetSessionVars().StmtCtx
	nr := input.NumRows()
	payloadBuf, err := b.bufAllocator.get(types.ETString, nr)
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(payloadBuf)
	if err := b.args[0].VecEvalString(b.ctx, input, payloadBuf); err != nil {
		return err
	}

	result.ResizeInt64(nr, false)
	result.MergeNulls(payloadBuf)
	i64s := result.Int64s()
	for i := 0; i < nr; i++ {
		if result.IsNull(i) {
			continue
		}
		str := payloadBuf.GetString(i)
		if len(str) == 0 {
			i64s[i] = 0
			continue
		}
		if len(str) <= 4 {
			sc.AppendWarning(errZlibZData)
			i64s[i] = 0
			continue
		}
		i64s[i] = int64(binary.LittleEndian.Uint32([]byte(str)[0:4]))
	}
	return nil
}
