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
	"fmt"
	"hash"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
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
	aes128ecb string = "aes-128-ecb"
)

type aesDecryptFunctionClass struct {
	baseFunctionClass
}

func (c *aesDecryptFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinAesDecryptSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = true
	return bt, errors.Trace(err)
}

type builtinAesDecryptSig struct {
	baseBuiltinFunc
}

// eval evals a builtinAesDecryptSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesDecryptSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	for _, arg := range args {
		// If either function argument is NULL, the function returns NULL.
		if arg.IsNull() {
			return d, nil
		}
	}

	cryptStr, err := args[0].ToBytes()
	if err != nil {
		return d, errors.Trace(err)
	}
	key, err := args[1].ToBytes()
	if err != nil {
		return d, errors.Trace(err)
	}
	key = handleAESKey(key, aes128ecb)
	// By default these functions implement AES with a 128-bit key length.
	// TODO: We only support aes-128-ecb now. We should support other mode latter.
	data, err := encrypt.AESDecryptWithECB(cryptStr, key)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetString(string(data))
	return d, nil
}

type aesEncryptFunctionClass struct {
	baseFunctionClass
}

func (c *aesEncryptFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinAesEncryptSig{newBaseBuiltinFunc(args, ctx)}
	return bt, errors.Trace(err)
}

type builtinAesEncryptSig struct {
	baseBuiltinFunc
}

// eval evals a builtinAesEncryptSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
// We only support aes-128-ecb mode.
// TODO: support other mode.
func (b *builtinAesEncryptSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	for _, arg := range args {
		// If either function argument is NULL, the function returns NULL.
		if arg.IsNull() {
			return
		}
	}
	str, err := args[0].ToBytes()
	if err != nil {
		return d, errors.Trace(err)
	}
	key, err := args[1].ToBytes()
	if err != nil {
		return d, errors.Trace(err)
	}
	key = handleAESKey(key, aes128ecb)
	crypted, err := encrypt.AESEncryptWithECB(str, key)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetString(string(crypted))
	return
}

// handleAESKey transforms an arbitrary long key into a fixed length AES key.
func handleAESKey(key []byte, mode string) []byte {
	// TODO: get key size according to mode
	keySize := 16
	rKey := make([]byte, keySize)
	rIdx := 0
	for _, k := range key {
		if rIdx == keySize {
			rIdx = 0
		}
		rKey[rIdx] ^= k
		rIdx++
	}
	return rKey
}

type compressFunctionClass struct {
	baseFunctionClass
}

func (c *compressFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinCompressSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinCompressSig struct {
	baseBuiltinFunc
}

// eval evals a builtinCompressSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_compress
func (b *builtinCompressSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}

	arg := args[0]
	if arg.IsNull() {
		return d, nil
	}

	compressStr, err := arg.ToBytes()
	if err != nil {
		return d, errors.Trace(err)
	}

	var in bytes.Buffer
	w := zlib.NewWriter(&in)
	w.Write(compressStr)
	w.Close()
	d.SetBytes(in.Bytes())
	return d, nil
}

type decodeFunctionClass struct {
	baseFunctionClass
}

func (c *decodeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinDecodeSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
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
	return &builtinDesDecryptSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
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
	return &builtinDesEncryptSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
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
	return &builtinEncodeSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
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
	return &builtinEncryptSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinEncryptSig struct {
	baseBuiltinFunc
}

// eval evals a builtinEncryptSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_encrypt
func (b *builtinEncryptSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("ENCRYPT")
}

type md5FunctionClass struct {
	baseFunctionClass
}

func (c *md5FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinMD5Sig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinMD5Sig struct {
	baseBuiltinFunc
}

// eval evals a builtinMD5Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_md5
func (b *builtinMD5Sig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// This function takes one argument.
	arg := args[0]
	if arg.IsNull() {
		return
	}
	bin, err := arg.ToBytes()
	if err != nil {
		return d, errors.Trace(err)
	}
	sum := md5.Sum(bin)
	hexStr := fmt.Sprintf("%x", sum)
	d.SetString(hexStr)
	return d, nil
}

type oldPasswordFunctionClass struct {
	baseFunctionClass
}

func (c *oldPasswordFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinOldPasswordSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
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
	return &builtinPasswordSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinPasswordSig struct {
	baseBuiltinFunc
}

// eval evals a builtinPasswordSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_password
func (b *builtinPasswordSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("PASSWORD")
}

type randomBytesFunctionClass struct {
	baseFunctionClass
}

func (c *randomBytesFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinRandomBytesSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
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

type sha1FunctionClass struct {
	baseFunctionClass
}

func (c *sha1FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinSHA1Sig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinSHA1Sig struct {
	baseBuiltinFunc
}

// eval evals a builtinSHA1Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_sha1
// The value is returned as a string of 40 hexadecimal digits, or NULL if the argument was NULL.
func (b *builtinSHA1Sig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// SHA/SHA1 function only accept 1 parameter
	arg := args[0]
	if arg.IsNull() {
		return d, nil
	}
	bin, err := arg.ToBytes()
	if err != nil {
		return d, errors.Trace(err)
	}
	hasher := sha1.New()
	hasher.Write(bin)
	data := fmt.Sprintf("%x", hasher.Sum(nil))
	d.SetString(data)
	return d, nil
}

type sha2FunctionClass struct {
	baseFunctionClass
}

func (c *sha2FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinSHA2Sig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinSHA2Sig struct {
	baseBuiltinFunc
}

// Supported hash length of SHA-2 family
const (
	SHA0   int = 0
	SHA224 int = 224
	SHA256 int = 256
	SHA384 int = 384
	SHA512 int = 512
)

// eval evals a builtinSHA2Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_sha2
func (b *builtinSHA2Sig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	for _, arg := range args {
		if arg.IsNull() {
			return d, nil
		}
	}
	// Meaning of each argument:
	// args[0]: the cleartext string to be hashed
	// args[1]: desired bit length of result
	bin, err := args[0].ToBytes()
	if err != nil {
		return d, errors.Trace(err)
	}
	hashLength, err := args[1].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
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
	if hasher != nil {
		hasher.Write(bin)
		data := fmt.Sprintf("%x", hasher.Sum(nil))
		d.SetString(data)
	}
	return d, nil
}

type uncompressFunctionClass struct {
	baseFunctionClass
}

func (c *uncompressFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinUncompressSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinUncompressSig struct {
	baseBuiltinFunc
}

// eval evals a builtinUncompressSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_uncompress
func (b *builtinUncompressSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("UNCOMPRESS")
}

type uncompressedLengthFunctionClass struct {
	baseFunctionClass
}

func (c *uncompressedLengthFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinUncompressedLengthSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinUncompressedLengthSig struct {
	baseBuiltinFunc
}

// eval evals a builtinUncompressedLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_uncompressed-length
func (b *builtinUncompressedLengthSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("UNCOMPRESSED_LENGTH")
}

type validatePasswordStrengthFunctionClass struct {
	baseFunctionClass
}

func (c *validatePasswordStrengthFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinValidatePasswordStrengthSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinValidatePasswordStrengthSig struct {
	baseBuiltinFunc
}

// eval evals a builtinValidatePasswordStrengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_validate-password-strength
func (b *builtinValidatePasswordStrengthSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("VALIDATE_PASSWORD_STRENGTH")
}
