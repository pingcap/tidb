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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/encrypt"
	"github.com/pingcap/tidb/pkg/util/zeropool"
)

func (b *builtinSM3Sig) vectorized() bool {
	return true
}

// vecEvalString evals Sm3Hash(str).
func (b *builtinSM3Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return errors.Trace(err)
	}
	result.ReserveString(n)
	hasher := auth.NewSM3()
	for i := range n {
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

func (b *builtinCompressSig) vectorized() bool {
	return true
}

var (
	defaultByteSliceSize = 1024
	bytePool             = zeropool.New[[]byte](func() []byte {
		return make([]byte, defaultByteSliceSize)
	})
)

func allocByteSlice(n int) []byte {
	if n > defaultByteSliceSize {
		return make([]byte, n)
	}
	return bytePool.Get()
}

func deallocateByteSlice(b []byte) {
	if cap(b) <= defaultByteSliceSize {
		bytePool.Put(b)
	}
}

// evalString evals COMPRESS(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_compress
func (b *builtinCompressSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	for i := range n {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}

		strBytes := buf.GetBytes(i)

		// According to doc: Empty strings are stored as empty strings.
		if len(strBytes) == 0 {
			result.AppendString("")
			continue
		}

		compressed, err := deflate(strBytes)
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
		//nolint: revive
		defer deallocateByteSlice(buffer)
		buffer = buffer[:resultLength]

		binary.LittleEndian.PutUint32(buffer, uint32(len(strBytes)))
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
func (b *builtinAesEncryptSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	strBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(strBuf)
	if err := b.args[0].VecEvalString(ctx, input, strBuf); err != nil {
		return err
	}

	keyBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(keyBuf)
	if err := b.args[1].VecEvalString(ctx, input, keyBuf); err != nil {
		return err
	}

	if b.modeName != "ecb" {
		// For modes that do not require init_vector, it is ignored and a warning is generated if it is specified.
		return errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}

	isWarning := !b.ivRequired && len(b.args) == 3
	isConst := b.args[1].ConstLevel() >= ConstOnlyInContext
	var key []byte
	if isConst {
		key = encrypt.DeriveKeyMySQL(keyBuf.GetBytes(0), b.keySize)
	}

	tc := typeCtx(ctx)
	result.ReserveString(n)
	for i := range n {
		// According to doc: If either function argument is NULL, the function returns NULL.
		if strBuf.IsNull(i) || keyBuf.IsNull(i) {
			result.AppendNull()
			continue
		}
		if isWarning {
			tc.AppendWarning(errWarnOptionIgnored.FastGenByArgs("IV"))
		}
		if !isConst {
			key = encrypt.DeriveKeyMySQL(keyBuf.GetBytes(i), b.keySize)
		}

		// NOTE: we can't use GetBytes, because in AESEncryptWithECB padding is automatically
		//       added to str and this will damage the data layout in chunk.Column
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

func (b *builtinPasswordSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(n)
	for i := range n {
		if buf.IsNull(i) {
			result.AppendString("")
			continue
		}

		passBytes := buf.GetBytes(i)
		if len(passBytes) == 0 {
			result.AppendString("")
			continue
		}

		// We should append a warning here because function "PASSWORD" is deprecated since MySQL 5.7.6.
		// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_password
		tc := typeCtx(ctx)
		tc.AppendWarning(errDeprecatedSyntaxNoReplacement.FastGenByArgs("PASSWORD", ""))

		result.AppendString(auth.EncodePasswordBytes(passBytes))
	}
	return nil
}

func (b *builtinSHA1Sig) vectorized() bool {
	return true
}

func (b *builtinSHA1Sig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}
	result.ReserveString(n)
	hasher := sha1.New() // #nosec G401
	for i := range n {
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
func (b *builtinUncompressSig) vecEvalString(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ReserveString(n)
	tc := typeCtx(ctx)
	for i := range n {
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
			tc.AppendWarning(errZlibZData)
			result.AppendNull()
			continue
		}
		length := binary.LittleEndian.Uint32([]byte(payload[0:4]))
		bytes, err := inflate([]byte(payload[4:]))
		if err != nil {
			tc.AppendWarning(errZlibZData)
			result.AppendNull()
			continue
		}
		if length < uint32(len(bytes)) {
			tc.AppendWarning(errZlibZBuf)
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

func (b *builtinUncompressedLengthSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	tc := typeCtx(ctx)
	nr := input.NumRows()
	payloadBuf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(payloadBuf)
	if err := b.args[0].VecEvalString(ctx, input, payloadBuf); err != nil {
		return err
	}

	result.ResizeInt64(nr, false)
	result.MergeNulls(payloadBuf)
	i64s := result.Int64s()
	for i := range nr {
		if result.IsNull(i) {
			continue
		}
		str := payloadBuf.GetString(i)
		if len(str) == 0 {
			i64s[i] = 0
			continue
		}
		if len(str) <= 4 {
			tc.AppendWarning(errZlibZData)
			i64s[i] = 0
			continue
		}
		i64s[i] = int64(binary.LittleEndian.Uint32([]byte(str)[0:4]))
	}
	return nil
}

func (b *builtinValidatePasswordStrengthSig) vectorized() bool {
	return true
}

func (b *builtinValidatePasswordStrengthSig) vecEvalInt(ctx EvalContext, input *chunk.Chunk, result *chunk.Column) error {
	user, err := b.CurrentUser(ctx)
	if err != nil {
		return err
	}

	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return err
	}

	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(ctx, input, buf); err != nil {
		return err
	}

	result.ResizeInt64(n, false)
	result.MergeNulls(buf)
	i64s := result.Int64s()
	globalVars := vars.GlobalVarsAccessor
	enableValidation := false
	validation, err := globalVars.GetGlobalSysVar(vardef.ValidatePasswordEnable)
	if err != nil {
		return err
	}
	enableValidation = variable.TiDBOptOn(validation)
	for i := range n {
		if result.IsNull(i) {
			continue
		}
		if !enableValidation {
			i64s[i] = 0
		} else if score, isNull, err := b.validateStr(buf.GetString(i), user, &globalVars); err != nil {
			return err
		} else if !isNull {
			i64s[i] = score
		} else {
			result.SetNull(i, true)
		}
	}
	return nil
}
