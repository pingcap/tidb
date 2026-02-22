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
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type passwordFunctionClass struct {
	baseFunctionClass
}

func (c *passwordFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(mysql.PWDHashLen + 1)
	sig := &builtinPasswordSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Password)
	return sig, nil
}

type builtinPasswordSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinPasswordSig) Clone() builtinFunc {
	newSig := &builtinPasswordSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinPasswordSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_password
func (b *builtinPasswordSig) evalString(ctx EvalContext, row chunk.Row) (val string, isNull bool, err error) {
	pass, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	if len(pass) == 0 {
		return "", false, nil
	}

	// We should append a warning here because function "PASSWORD" is deprecated since MySQL 5.7.6.
	// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_password
	tc := typeCtx(ctx)
	tc.AppendWarning(errDeprecatedSyntaxNoReplacement.FastGenByArgs("PASSWORD", ""))

	return auth.EncodePassword(pass), false, nil
}

type randomBytesFunctionClass struct {
	baseFunctionClass
}

func (c *randomBytesFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1024) // Max allowed random bytes
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinRandomBytesSig{bf}
	return sig, nil
}

type builtinRandomBytesSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRandomBytesSig) Clone() builtinFunc {
	newSig := &builtinRandomBytesSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals RANDOM_BYTES(len).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_random-bytes
func (b *builtinRandomBytesSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if val < 1 || val > 1024 {
		return "", false, types.ErrOverflow.GenWithStackByArgs("length", "random_bytes")
	}
	buf := make([]byte, val)
	//nolint: gosec
	if n, err := rand.Read(buf); err != nil {
		return "", true, err
	} else if int64(n) != val {
		return "", false, errors.New("fail to generate random bytes")
	}
	return string(buf), false, nil
}

type md5FunctionClass struct {
	baseFunctionClass
}

func (c *md5FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(32)
	sig := &builtinMD5Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_MD5)
	return sig, nil
}

type builtinMD5Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMD5Sig) Clone() builtinFunc {
	newSig := &builtinMD5Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinMD5Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_md5
func (b *builtinMD5Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	arg, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	sum := md5.Sum([]byte(arg)) // #nosec G401
	hexStr := hex.EncodeToString(sum[:])

	return hexStr, false, nil
}

type sha1FunctionClass struct {
	baseFunctionClass
}

func (c *sha1FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(40)
	sig := &builtinSHA1Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_SHA1)
	return sig, nil
}

type builtinSHA1Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSHA1Sig) Clone() builtinFunc {
	newSig := &builtinSHA1Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SHA1(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_sha1
// The value is returned as a string of 40 hexadecimal digits, or NULL if the argument was NULL.
func (b *builtinSHA1Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	hasher := sha1.New() // #nosec G401
	_, err = hasher.Write([]byte(str))
	if err != nil {
		return "", true, err
	}
	return fmt.Sprintf("%x", hasher.Sum(nil)), false, nil
}

type sha2FunctionClass struct {
	baseFunctionClass
}

func (c *sha2FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(128) // sha512
	sig := &builtinSHA2Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_SHA2)
	return sig, nil
}

type builtinSHA2Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSHA2Sig) Clone() builtinFunc {
	newSig := &builtinSHA2Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type sm3FunctionClass struct {
	baseFunctionClass
}

func (c *sm3FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(40)
	sig := &builtinSM3Sig{bf}
	return sig, nil
}

type builtinSM3Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSM3Sig) Clone() builtinFunc {
	newSig := &builtinSM3Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals Sm3Hash(str).
// The value is returned as a string of 70 hexadecimal digits, or NULL if the argument was NULL.
func (b *builtinSM3Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	hasher := auth.NewSM3()
	_, err = hasher.Write([]byte(str))
	if err != nil {
		return "", true, err
	}
	return fmt.Sprintf("%x", hasher.Sum(nil)), false, nil
}

// Supported hash length of SHA-2 family
const (
	SHA0   = 0
	SHA224 = 224
	SHA256 = 256
	SHA384 = 384
	SHA512 = 512
)

// evalString evals SHA2(str, hash_length).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_sha2
func (b *builtinSHA2Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	hashLength, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
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

	_, err = hasher.Write([]byte(str))
	if err != nil {
		return "", true, err
	}
	return fmt.Sprintf("%x", hasher.Sum(nil)), false, nil
}
