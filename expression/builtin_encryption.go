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
	"github.com/pingcap/tidb/util"
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
	_ builtinFunc = &builtinDecodeSig{}
	_ builtinFunc = &builtinDesDecryptSig{}
	_ builtinFunc = &builtinDesEncryptSig{}
	_ builtinFunc = &builtinEncodeSig{}
	_ builtinFunc = &builtinEncryptSig{}
	_ builtinFunc = &builtinMD5Sig{}
	_ builtinFunc = &builtinOldPasswordSig{}
	_ builtinFunc = &builtinPasswordSig{}
	_ builtinFunc = &builtinRandomBytesSig{}
	_ builtinFunc = &builtinSHA1Sig{}
	_ builtinFunc = &builtinSHA2Sig{}
	_ builtinFunc = &builtinUncompressSig{}
	_ builtinFunc = &builtinUncompressedLengthSig{}
	_ builtinFunc = &builtinValidatePasswordStrengthSig{}
)

// TODO: support other mode
const (
	aes128ecb          string = "aes-128-ecb"
	aes128ecbBlobkSize int    = 16
)

type aesDecryptFunctionClass struct {
	baseFunctionClass
}

func (c *aesDecryptFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(c.verifyArgs(args))
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (c *aesEncryptFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(c.verifyArgs(args))
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (c *decodeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinDecodeSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinDecodeSig struct {
	baseBuiltinFunc
}

// eval evals a builtinDecodeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_decode
func (b *builtinDecodeSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("DECODE")
}

type desDecryptFunctionClass struct {
	baseFunctionClass
}

func (c *desDecryptFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinDesDecryptSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinDesDecryptSig struct {
	baseBuiltinFunc
}

// eval evals a builtinDesDecryptSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_des-decrypt
func (b *builtinDesDecryptSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("DES_DECRYPT")
}

type desEncryptFunctionClass struct {
	baseFunctionClass
}

func (c *desEncryptFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinDesEncryptSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinDesEncryptSig struct {
	baseBuiltinFunc
}

// eval evals a builtinDesEncryptSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_des-encrypt
func (b *builtinDesEncryptSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("DES_ENCRYPT")
}

type encodeFunctionClass struct {
	baseFunctionClass
}

func (c *encodeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinEncodeSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinEncodeSig struct {
	baseBuiltinFunc
}

// eval evals a builtinEncodeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_encode
func (b *builtinEncodeSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("ENCODE")
}

type encryptFunctionClass struct {
	baseFunctionClass
}

func (c *encryptFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinEncryptSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinEncryptSig struct {
	baseBuiltinFunc
}

// eval evals a builtinEncryptSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_encrypt
func (b *builtinEncryptSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("ENCRYPT")
}

type oldPasswordFunctionClass struct {
	baseFunctionClass
}

func (c *oldPasswordFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinOldPasswordSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinOldPasswordSig struct {
	baseBuiltinFunc
}

// eval evals a builtinOldPasswordSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_old-password
func (b *builtinOldPasswordSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("OLD_PASSWORD")
}

type passwordFunctionClass struct {
	baseFunctionClass
}

func (c *passwordFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

	return util.EncodePassword(pass), false, nil
}

type randomBytesFunctionClass struct {
	baseFunctionClass
}

func (c *randomBytesFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinRandomBytesSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinRandomBytesSig struct {
	baseBuiltinFunc
}

// eval evals a builtinRandomBytesSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_random-bytes
func (b *builtinRandomBytesSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	arg := args[0]
	if arg.IsNull() {
		return d, nil
	}
	size := arg.GetInt64()
	if size < 1 || size > 1024 {
		return d, mysql.NewErr(mysql.ErrDataOutOfRange, "length", "random_bytes")
	}
	buf := make([]byte, size)
	if n, err := rand.Read(buf); err != nil {
		return d, errors.Trace(err)
	} else if int64(n) != size {
		return d, errors.New("fail to generate random bytes")
	}
	d.SetBytes(buf)
	return d, nil
}

type md5FunctionClass struct {
	baseFunctionClass
}

func (c *md5FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (c *sha1FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (c *sha2FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (c *compressFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (c *uncompressFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (c *uncompressedLengthFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (c *validatePasswordStrengthFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinValidatePasswordStrengthSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinValidatePasswordStrengthSig struct {
	baseBuiltinFunc
}

// eval evals a builtinValidatePasswordStrengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_validate-password-strength
func (b *builtinValidatePasswordStrengthSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("VALIDATE_PASSWORD_STRENGTH")
}
