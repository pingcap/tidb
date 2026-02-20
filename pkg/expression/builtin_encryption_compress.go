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
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"io"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	pwdValidator "github.com/pingcap/tidb/pkg/util/password-validation"
	"github.com/pingcap/tipb/go-tipb"
)

// deflate compresses a string using the DEFLATE format.
func deflate(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	w := zlib.NewWriter(&buffer)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// inflate uncompresses a string using the DEFLATE format.
func inflate(compressStr []byte) ([]byte, error) {
	reader := bytes.NewReader(compressStr)
	var out bytes.Buffer
	r, err := zlib.NewReader(reader)
	if err != nil {
		return nil, err
	}
	/* #nosec G110 */
	if _, err = io.Copy(&out, r); err != nil {
		return nil, err
	}
	err = r.Close()
	return out.Bytes(), err
}

type compressFunctionClass struct {
	baseFunctionClass
}

func (c *compressFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	srcLen := args[0].GetType(ctx.GetEvalCtx()).GetFlen()
	compressBound := min(srcLen+(srcLen>>12)+(srcLen>>14)+(srcLen>>25)+13, mysql.MaxBlobWidth)
	bf.tp.SetFlen(compressBound)
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinCompressSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Compress)
	return sig, nil
}

type builtinCompressSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCompressSig) Clone() builtinFunc {
	newSig := &builtinCompressSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals COMPRESS(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_compress
func (b *builtinCompressSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
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

func (c *uncompressFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(mysql.MaxBlobWidth)
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinUncompressSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Uncompress)
	return sig, nil
}

type builtinUncompressSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUncompressSig) Clone() builtinFunc {
	newSig := &builtinUncompressSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals UNCOMPRESS(compressed_string).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_uncompress
func (b *builtinUncompressSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	tc := typeCtx(ctx)
	payload, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if len(payload) == 0 {
		return "", false, nil
	}
	if len(payload) <= 4 {
		// corrupted
		tc.AppendWarning(errZlibZData)
		return "", true, nil
	}
	length := binary.LittleEndian.Uint32([]byte(payload[0:4]))
	bytes, err := inflate([]byte(payload[4:]))
	if err != nil {
		tc.AppendWarning(errZlibZData)
		return "", true, nil
	}
	if length < uint32(len(bytes)) {
		tc.AppendWarning(errZlibZBuf)
		return "", true, nil
	}
	return string(bytes), false, nil
}

type uncompressedLengthFunctionClass struct {
	baseFunctionClass
}

func (c *uncompressedLengthFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(10)
	sig := &builtinUncompressedLengthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_UncompressedLength)
	return sig, nil
}

type builtinUncompressedLengthSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUncompressedLengthSig) Clone() builtinFunc {
	newSig := &builtinUncompressedLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals UNCOMPRESSED_LENGTH(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_uncompressed-length
func (b *builtinUncompressedLengthSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	tc := typeCtx(ctx)
	payload, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	if len(payload) == 0 {
		return 0, false, nil
	}
	if len(payload) <= 4 {
		// corrupted
		tc.AppendWarning(errZlibZData)
		return 0, false, nil
	}
	return int64(binary.LittleEndian.Uint32([]byte(payload)[0:4])), false, nil
}

type validatePasswordStrengthFunctionClass struct {
	baseFunctionClass
}

func (c *validatePasswordStrengthFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(21)
	sig := &builtinValidatePasswordStrengthSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinValidatePasswordStrengthSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
	expropt.CurrentUserPropReader
}

func (b *builtinValidatePasswordStrengthSig) Clone() builtinFunc {
	newSig := &builtinValidatePasswordStrengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinValidatePasswordStrengthSig) RequiredOptionalEvalProps() exprctx.OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps() |
		b.CurrentUserPropReader.RequiredOptionalEvalProps()
}

// evalInt evals VALIDATE_PASSWORD_STRENGTH(str).
// See https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_validate-password-strength
func (b *builtinValidatePasswordStrengthSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	user, err := b.CurrentUser(ctx)
	if err != nil {
		return 0, true, err
	}

	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	globalVars := vars.GlobalVarsAccessor
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return 0, true, err
	} else if utf8.RuneCountInString(str) < 4 {
		return 0, false, nil
	}
	if validation, err := globalVars.GetGlobalSysVar(vardef.ValidatePasswordEnable); err != nil {
		return 0, true, err
	} else if !variable.TiDBOptOn(validation) {
		return 0, false, nil
	}
	return b.validateStr(str, user, &globalVars)
}

func (b *builtinValidatePasswordStrengthSig) validateStr(str string, user *auth.UserIdentity, globalVars *variable.GlobalVarAccessor) (int64, bool, error) {
	if warn, err := pwdValidator.ValidateUserNameInPassword(str, user, globalVars); err != nil {
		return 0, true, err
	} else if len(warn) > 0 {
		return 0, false, nil
	}
	if warn, err := pwdValidator.ValidatePasswordLowPolicy(str, globalVars); err != nil {
		return 0, true, err
	} else if len(warn) > 0 {
		return 25, false, nil
	}
	if warn, err := pwdValidator.ValidatePasswordMediumPolicy(str, globalVars); err != nil {
		return 0, true, err
	} else if len(warn) > 0 {
		return 50, false, nil
	}
	if ok, err := pwdValidator.ValidateDictionaryPassword(str, globalVars); err != nil {
		return 0, true, err
	} else if !ok {
		return 75, false, nil
	}
	return 100, false, nil
}
