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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"crypto/aes" // #nosec G501
	// #nosec G505
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/encrypt"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &aesDecryptFunctionClass{}
	_ functionClass = &aesEncryptFunctionClass{}
	_ functionClass = &compressFunctionClass{}
	_ functionClass = &decodeFunctionClass{}
	_ functionClass = &encodeFunctionClass{}
	_ functionClass = &md5FunctionClass{}
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
	_ builtinFunc = &builtinAesDecryptIVSig{}
	_ builtinFunc = &builtinAesEncryptSig{}
	_ builtinFunc = &builtinAesEncryptIVSig{}
	_ builtinFunc = &builtinCompressSig{}
	_ builtinFunc = &builtinMD5Sig{}
	_ builtinFunc = &builtinPasswordSig{}
	_ builtinFunc = &builtinRandomBytesSig{}
	_ builtinFunc = &builtinSHA1Sig{}
	_ builtinFunc = &builtinSHA2Sig{}
	_ builtinFunc = &builtinUncompressSig{}
	_ builtinFunc = &builtinUncompressedLengthSig{}
	_ builtinFunc = &builtinValidatePasswordStrengthSig{}
)

// aesModeAttr indicates that the key length and iv attribute for specific block_encryption_mode.
// keySize is the key length in bits and mode is the encryption mode.
// ivRequired indicates that initialization vector is required or not.
// nolint:structcheck
type aesModeAttr struct {
	modeName   string
	keySize    int
	ivRequired bool
}

var aesModes = map[string]*aesModeAttr{
	// TODO support more modes, permitted mode values are: ECB, CBC, CFB1, CFB8, CFB128, OFB
	"aes-128-ecb": {"ecb", 16, false},
	"aes-192-ecb": {"ecb", 24, false},
	"aes-256-ecb": {"ecb", 32, false},
	"aes-128-cbc": {"cbc", 16, true},
	"aes-192-cbc": {"cbc", 24, true},
	"aes-256-cbc": {"cbc", 32, true},
	"aes-128-ofb": {"ofb", 16, true},
	"aes-192-ofb": {"ofb", 24, true},
	"aes-256-ofb": {"ofb", 32, true},
	"aes-128-cfb": {"cfb", 16, true},
	"aes-192-cfb": {"cfb", 24, true},
	"aes-256-cfb": {"cfb", 32, true},
}

type aesDecryptFunctionClass struct {
	baseFunctionClass
}

func (c *aesDecryptFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, c.verifyArgs(args)
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(args[0].GetType(ctx.GetEvalCtx()).GetFlen()) // At most.
	types.SetBinChsClnFlag(bf.tp)

	blockMode := ctx.GetBlockEncryptionMode()
	mode, exists := aesModes[strings.ToLower(blockMode)]
	if !exists {
		return nil, errors.Errorf("unsupported block encryption mode - %v", blockMode)
	}
	if mode.ivRequired {
		if len(args) != 3 {
			return nil, ErrIncorrectParameterCount.GenWithStackByArgs("aes_decrypt")
		}
		sig := &builtinAesDecryptIVSig{bf, mode}
		sig.setPbCode(tipb.ScalarFuncSig_AesDecryptIV)
		return sig, nil
	}
	sig := &builtinAesDecryptSig{bf, mode}
	sig.setPbCode(tipb.ScalarFuncSig_AesDecrypt)
	return sig, nil
}

type builtinAesDecryptSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesDecryptSig) Clone() builtinFunc {
	newSig := &builtinAesDecryptSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_DECRYPT(crypt_str, key_key).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesDecryptSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	cryptStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	keyStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if !b.ivRequired && len(b.args) == 3 {
		tc := typeCtx(ctx)
		// For modes that do not require init_vector, it is ignored and a warning is generated if it is specified.
		tc.AppendWarning(errWarnOptionIgnored.FastGenByArgs("IV"))
	}

	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var plainText []byte
	switch b.modeName {
	case "ecb":
		plainText, err = encrypt.AESDecryptWithECB([]byte(cryptStr), key)
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(plainText), false, nil
}

type builtinAesDecryptIVSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesDecryptIVSig) Clone() builtinFunc {
	newSig := &builtinAesDecryptIVSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_DECRYPT(crypt_str, key_key, iv).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesDecryptIVSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	cryptStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	keyStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	iv, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if len(iv) < aes.BlockSize {
		return "", true, errIncorrectArgs.GenWithStack("The initialization vector supplied to aes_decrypt is too short. Must be at least %d bytes long", aes.BlockSize)
	}
	// init_vector must be 16 bytes or longer (bytes in excess of 16 are ignored)
	iv = iv[0:aes.BlockSize]

	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var plainText []byte
	switch b.modeName {
	case "cbc":
		plainText, err = encrypt.AESDecryptWithCBC([]byte(cryptStr), key, []byte(iv))
	case "ofb":
		plainText, err = encrypt.AESDecryptWithOFB([]byte(cryptStr), key, []byte(iv))
	case "cfb":
		plainText, err = encrypt.AESDecryptWithCFB([]byte(cryptStr), key, []byte(iv))
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(plainText), false, nil
}

type aesEncryptFunctionClass struct {
	baseFunctionClass
}

func (c *aesEncryptFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, c.verifyArgs(args)
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(aes.BlockSize * (args[0].GetType(ctx.GetEvalCtx()).GetFlen()/aes.BlockSize + 1)) // At most.
	types.SetBinChsClnFlag(bf.tp)

	blockMode := ctx.GetBlockEncryptionMode()
	mode, exists := aesModes[strings.ToLower(blockMode)]
	if !exists {
		return nil, errors.Errorf("unsupported block encryption mode - %v", blockMode)
	}
	if mode.ivRequired {
		if len(args) != 3 {
			return nil, ErrIncorrectParameterCount.GenWithStackByArgs("aes_encrypt")
		}
		sig := &builtinAesEncryptIVSig{bf, mode}
		sig.setPbCode(tipb.ScalarFuncSig_AesEncryptIV)
		return sig, nil
	}
	sig := &builtinAesEncryptSig{bf, mode}
	sig.setPbCode(tipb.ScalarFuncSig_AesEncrypt)
	return sig, nil
}

type builtinAesEncryptSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesEncryptSig) Clone() builtinFunc {
	newSig := &builtinAesEncryptSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_ENCRYPT(str, key_str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesEncryptSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	keyStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if !b.ivRequired && len(b.args) == 3 {
		tc := typeCtx(ctx)
		// For modes that do not require init_vector, it is ignored and a warning is generated if it is specified.
		tc.AppendWarning(errWarnOptionIgnored.FastGenByArgs("IV"))
	}

	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var cipherText []byte
	switch b.modeName {
	case "ecb":
		cipherText, err = encrypt.AESEncryptWithECB([]byte(str), key)
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(cipherText), false, nil
}

type builtinAesEncryptIVSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesEncryptIVSig) Clone() builtinFunc {
	newSig := &builtinAesEncryptIVSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_ENCRYPT(str, key_str, iv).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesEncryptIVSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	keyStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	iv, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if len(iv) < aes.BlockSize {
		return "", true, errIncorrectArgs.GenWithStack("The initialization vector supplied to aes_encrypt is too short. Must be at least %d bytes long", aes.BlockSize)
	}
	// init_vector must be 16 bytes or longer (bytes in excess of 16 are ignored)
	iv = iv[0:aes.BlockSize]

	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var cipherText []byte
	switch b.modeName {
	case "cbc":
		cipherText, err = encrypt.AESEncryptWithCBC([]byte(str), key, []byte(iv))
	case "ofb":
		cipherText, err = encrypt.AESEncryptWithOFB([]byte(str), key, []byte(iv))
	case "cfb":
		cipherText, err = encrypt.AESEncryptWithCFB([]byte(str), key, []byte(iv))
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(cipherText), false, nil
}

type decodeFunctionClass struct {
	baseFunctionClass
}

func (c *decodeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}

	bf.tp.SetFlen(args[0].GetType(ctx.GetEvalCtx()).GetFlen())
	sig := &builtinDecodeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Decode)
	return sig, nil
}

type builtinDecodeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinDecodeSig) Clone() builtinFunc {
	newSig := &builtinDecodeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals DECODE(str, password_str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_decode
func (b *builtinDecodeSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	dataStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	passwordStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	decodeStr, err := encrypt.SQLDecode(dataStr, passwordStr)
	return decodeStr, false, err
}

type encodeFunctionClass struct {
	baseFunctionClass
}

func (c *encodeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}

	bf.tp.SetFlen(args[0].GetType(ctx.GetEvalCtx()).GetFlen())
	sig := &builtinEncodeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Encode)
	return sig, nil
}

type builtinEncodeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinEncodeSig) Clone() builtinFunc {
	newSig := &builtinEncodeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals ENCODE(crypt_str, password_str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_encode
func (b *builtinEncodeSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	decodeStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	passwordStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	dataStr, err := encrypt.SQLEncode(decodeStr, passwordStr)
	return dataStr, false, err
}
