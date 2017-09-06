// Copyright 2017 PingCAP, Inc.
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
	"compress/zlib"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"hash"
	"io"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util/encrypt"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ functionClass = &aesDecryptFunctionClass{}
	_ functionClass = &aesEncryptFunctionClass{}
	_ functionClass = &compressFunctionClass{}
	_ functionClass = &decodeFunctionClass{}
	_ functionClass = &desDecryptFunctionClass{}
	_ functionClass = &desEncryptFunctionClass{}
	_ functionClass = &encodeFunctionClass{}
	_ functionClass = &encryptFunctionClass{}
	_ functionClass = &md5FunctionClass{}
	_ functionClass = &oldPasswordFunctionClass{}
	_ functionClass = &passwordFunctionClass{}
	_ functionClass = &randomBytesFunctionClass{}
	_ functionClass = &sha1FunctionClass{}
	_ functionClass = &sha2FunctionClass{}
	_ functionClass = &uncompressFunctionClass{}
	_ functionClass = &uncompressedLengthFunctionClass{}
	_ functionClass = &validatePasswordStrengthFunctionClass{}
)

var (
	_ builtinFunc = &builtinAesDecryptSig{}
	_ builtinFunc = &builtinAesEncryptSig{}
	_ builtinFunc = &builtinCompressSig{}
	_ builtinFunc = &builtinMD5Sig{}
	_ builtinFunc = &builtinPasswordSig{}
	_ builtinFunc = &builtinRandomBytesSig{}
	_ builtinFunc = &builtinSHA1Sig{}
	_ builtinFunc = &builtinSHA2Sig{}
	_ builtinFunc = &builtinUncompressSig{}
	_ builtinFunc = &builtinUncompressedLengthSig{}
)

// TODO: support other mode
const (
	aes128ecb          string = "aes-128-ecb"
	aes128ecbBlobkSize int    = 16
)

type aesDecryptFunctionClass struct {
	baseFunctionClass
}

func (c *aesDecryptFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(c.verifyArgs(args))
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpString)
	bf.tp.Flen = args[0].GetType().Flen // At most.
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinAesDecryptSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinAesDecryptSig struct {
	baseStringBuiltinFunc
}

// evalString evals AES_DECRYPT(crypt_str, key_key).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesDecryptSig) evalString(row []types.Datum) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	cryptStr, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}

	keyStr, isNull, err := b.args[1].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}

	// TODO: Support other modes.
	key := encrypt.DeriveKeyMySQL([]byte(keyStr), aes128ecbBlobkSize)
	plainText, err := encrypt.AESDecryptWithECB([]byte(cryptStr), key)
	if err != nil {
		return "", true, nil
	}
	return string(plainText), false, nil
}

type aesEncryptFunctionClass struct {
	baseFunctionClass
}

func (c *aesEncryptFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(c.verifyArgs(args))
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpString)
	bf.tp.Flen = aes128ecbBlobkSize * (args[0].GetType().Flen/aes128ecbBlobkSize + 1) // At most.
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinAesEncryptSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinAesEncryptSig struct {
	baseStringBuiltinFunc
}

// evalString evals AES_ENCRYPT(str, key_str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesEncryptSig) evalString(row []types.Datum) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	str, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}

	keyStr, isNull, err := b.args[1].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}

	// TODO: Support other modes.
	key := encrypt.DeriveKeyMySQL([]byte(keyStr), aes128ecbBlobkSize)
	cipherText, err := encrypt.AESEncryptWithECB([]byte(str), key)
	if err != nil {
		return "", true, nil
	}
	return string(cipherText), false, nil
}

type decodeFunctionClass struct {
	baseFunctionClass
}

func (c *decodeFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("DECODE")
}

type desDecryptFunctionClass struct {
	baseFunctionClass
}

func (c *desDecryptFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("DES_DECRYPT")
}

type desEncryptFunctionClass struct {
	baseFunctionClass
}

func (c *desEncryptFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("DES_ENCRYPT")
}

type encodeFunctionClass struct {
	baseFunctionClass
}

func (c *encodeFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("ENCODE")
}

type encryptFunctionClass struct {
	baseFunctionClass
}

func (c *encryptFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("ENCRYPT")
}

type oldPasswordFunctionClass struct {
	baseFunctionClass
}

func (c *oldPasswordFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("OLD_PASSWORD")
}

type passwordFunctionClass struct {
	baseFunctionClass
}

func (c *passwordFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	bf.tp.Flen = mysql.PWDHashLen + 1
	sig := &builtinPasswordSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinPasswordSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinPasswordSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_password
func (b *builtinPasswordSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	pass, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", false, errors.Trace(err)
	}

	if len(pass) == 0 {
		return "", false, nil
	}

	return auth.EncodePassword(pass), false, nil
}

type randomBytesFunctionClass struct {
	baseFunctionClass
}

func (c *randomBytesFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpInt)
	bf.tp.Flen = 1024 // Max allowed random bytes
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinRandomBytesSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinRandomBytesSig struct {
	baseStringBuiltinFunc
}

// evalString evals RANDOM_BYTES(len).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_random-bytes
func (b *builtinRandomBytesSig) evalString(row []types.Datum) (string, bool, error) {
	len, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	if len < 1 || len > 1024 {
		return "", false, types.ErrOverflow.GenByArgs("length", "random_bytes")
	}
	buf := make([]byte, len)
	if n, err := rand.Read(buf); err != nil {
		return "", false, errors.Trace(err)
	} else if int64(n) != len {
		return "", false, errors.New("fail to generate random bytes")
	}
	return string(buf), false, nil
}

type md5FunctionClass struct {
	baseFunctionClass
}

func (c *md5FunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	bf.tp.Flen = 32
	sig := &builtinMD5Sig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinMD5Sig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinMD5Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_md5
func (b *builtinMD5Sig) evalString(row []types.Datum) (string, bool, error) {
	arg, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	sum := md5.Sum([]byte(arg))
	hexStr := fmt.Sprintf("%x", sum)
	return hexStr, false, nil
}

type sha1FunctionClass struct {
	baseFunctionClass
}

func (c *sha1FunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	bf.tp.Flen = 40
	sig := &builtinSHA1Sig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinSHA1Sig struct {
	baseStringBuiltinFunc
}

// evalString evals SHA1(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_sha1
// The value is returned as a string of 40 hexadecimal digits, or NULL if the argument was NULL.
func (b *builtinSHA1Sig) evalString(row []types.Datum) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	hasher := sha1.New()
	hasher.Write([]byte(str))
	return fmt.Sprintf("%x", hasher.Sum(nil)), false, nil
}

type sha2FunctionClass struct {
	baseFunctionClass
}

func (c *sha2FunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpInt)
	bf.tp.Flen = 128 // sha512
	sig := &builtinSHA2Sig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinSHA2Sig struct {
	baseStringBuiltinFunc
}

// Supported hash length of SHA-2 family
const (
	SHA0   int = 0
	SHA224 int = 224
	SHA256 int = 256
	SHA384 int = 384
	SHA512 int = 512
)

// evalString evals SHA2(str, hash_length).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_sha2
func (b *builtinSHA2Sig) evalString(row []types.Datum) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	hashLength, isNull, err := b.args[1].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	var hasher hash.Hash
	switch int(hashLength) {
	case SHA0, SHA256:
		hasher = sha256.New()
	case SHA224:
		hasher = sha256.New224()
	case SHA384:
		hasher = sha512.New384()
	case SHA512:
		hasher = sha512.New()
	}
	if hasher == nil {
		return "", true, nil
	}

	hasher.Write([]byte(str))
	return fmt.Sprintf("%x", hasher.Sum(nil)), false, nil
}

// deflate compresses a string using the DEFLATE format.
func deflate(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	w := zlib.NewWriter(&buffer)
	if _, err := w.Write([]byte(data)); err != nil {
		return nil, errors.Trace(err)
	}
	if err := w.Close(); err != nil {
		return nil, errors.Trace(err)
	}
	return buffer.Bytes(), nil
}

// inflate uncompresses a string using the DEFLATE format.
func inflate(compressStr []byte) ([]byte, error) {
	reader := bytes.NewReader(compressStr)
	var out bytes.Buffer
	r, err := zlib.NewReader(reader)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := io.Copy(&out, r); err != nil {
		return nil, errors.Trace(err)
	}
	r.Close()
	return out.Bytes(), nil
}

type compressFunctionClass struct {
	baseFunctionClass
}

func (c *compressFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	srcLen := args[0].GetType().Flen
	compressBound := srcLen + (srcLen >> 12) + (srcLen >> 14) + (srcLen >> 25) + 13
	if compressBound > mysql.MaxBlobWidth {
		compressBound = mysql.MaxBlobWidth
	}
	bf.tp.Flen = compressBound
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinCompressSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinCompressSig struct {
	baseStringBuiltinFunc
}

// evalString evals COMPRESS(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_compress
func (b *builtinCompressSig) evalString(row []types.Datum) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}

	// According to doc: Empty strings are stored as empty strings.
	if len(str) == 0 {
		return "", false, nil
	}

	compressed, err := deflate([]byte(str))
	if err != nil {
		return "", true, nil
	}

	resultLength := 4 + len(compressed)

	// append "." if ends with space
	shouldAppendSuffix := compressed[len(compressed)-1] == 32
	if shouldAppendSuffix {
		resultLength++
	}

	buffer := make([]byte, resultLength)
	binary.LittleEndian.PutUint32(buffer, uint32(len(str)))
	copy(buffer[4:], compressed)

	if shouldAppendSuffix {
		buffer[len(buffer)-1] = '.'
	}

	return string(buffer), false, nil
}

type uncompressFunctionClass struct {
	baseFunctionClass
}

func (c *uncompressFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	bf.tp.Flen = mysql.MaxBlobWidth
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinUncompressSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinUncompressSig struct {
	baseStringBuiltinFunc
}

// evalString evals UNCOMPRESS(compressed_string).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_uncompress
func (b *builtinUncompressSig) evalString(row []types.Datum) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	payload, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	if len(payload) == 0 {
		return "", false, nil
	}
	if len(payload) <= 4 {
		// corrupted
		sc.AppendWarning(errZlibZData)
		return "", true, nil
	}
	bytes, err := inflate([]byte(payload[4:]))
	if err != nil {
		sc.AppendWarning(errZlibZData)
		return "", true, nil
	}
	return string(bytes), false, nil
}

type uncompressedLengthFunctionClass struct {
	baseFunctionClass
}

func (c *uncompressedLengthFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	bf.tp.Flen = 10
	sig := &builtinUncompressedLengthSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinUncompressedLengthSig struct {
	baseIntBuiltinFunc
}

// evalInt evals UNCOMPRESSED_LENGTH(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_uncompressed-length
func (b *builtinUncompressedLengthSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	payload, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return 0, true, errors.Trace(err)
	}
	if len(payload) == 0 {
		return 0, false, nil
	}
	if len(payload) <= 4 {
		// corrupted
		sc.AppendWarning(errZlibZData)
		return 0, false, nil
	}
	len := binary.LittleEndian.Uint32([]byte(payload)[0:4])
	return int64(len), false, nil
}

type validatePasswordStrengthFunctionClass struct {
	baseFunctionClass
}

func (c *validatePasswordStrengthFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	return nil, errFunctionNotExists.GenByArgs("VALIDATE_PASSWORD_STRENGTH")
}
