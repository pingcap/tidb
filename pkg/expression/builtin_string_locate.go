// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tipb/go-tipb"
)

type locateFunctionClass struct {
	baseFunctionClass
}

func (c *locateFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	hasStartPos, argTps := len(args) == 3, []types.EvalType{types.ETString, types.ETString}
	if hasStartPos {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	var sig builtinFunc
	// Locate is multibyte safe.
	useBinary := bf.collation == charset.CollationBin
	switch {
	case hasStartPos && useBinary:
		sig = &builtinLocate3ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Locate3Args)
	case hasStartPos:
		sig = &builtinLocate3ArgsUTF8Sig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Locate3ArgsUTF8)
	case useBinary:
		sig = &builtinLocate2ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Locate2Args)
	default:
		sig = &builtinLocate2ArgsUTF8Sig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Locate2ArgsUTF8)
	}
	return sig, nil
}

type builtinLocate2ArgsSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLocate2ArgsSig) Clone() builtinFunc {
	newSig := &builtinLocate2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str), case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate2ArgsSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	subStrLen := len(subStr)
	if subStrLen == 0 {
		return 1, false, nil
	}
	ret, idx := 0, strings.Index(str, subStr)
	if idx != -1 {
		ret = idx + 1
	}
	return int64(ret), false, nil
}

type builtinLocate2ArgsUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLocate2ArgsUTF8Sig) Clone() builtinFunc {
	newSig := &builtinLocate2ArgsUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate2ArgsUTF8Sig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if int64(utf8.RuneCountInString(subStr)) == 0 {
		return 1, false, nil
	}

	return locateStringWithCollation(str, subStr, b.collation), false, nil
}

type builtinLocate3ArgsSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLocate3ArgsSig) Clone() builtinFunc {
	newSig := &builtinLocate3ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str,pos), case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate3ArgsSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	pos, isNull, err := b.args[2].EvalInt(ctx, row)
	// Transfer the argument which starts from 1 to real index which starts from 0.
	pos--
	if isNull || err != nil {
		return 0, isNull, err
	}
	subStrLen := len(subStr)
	if pos < 0 || pos > int64(len(str)-subStrLen) {
		return 0, false, nil
	} else if subStrLen == 0 {
		return pos + 1, false, nil
	}
	slice := str[pos:]
	idx := strings.Index(slice, subStr)
	if idx != -1 {
		return pos + int64(idx) + 1, false, nil
	}
	return 0, false, nil
}

type builtinLocate3ArgsUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLocate3ArgsUTF8Sig) Clone() builtinFunc {
	newSig := &builtinLocate3ArgsUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str,pos).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate3ArgsUTF8Sig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if collate.IsCICollation(b.collation) {
		subStr = strings.ToLower(subStr)
		str = strings.ToLower(str)
	}
	pos, isNull, err := b.args[2].EvalInt(ctx, row)
	// Transfer the argument which starts from 1 to real index which starts from 0.
	pos--
	if isNull || err != nil {
		return 0, isNull, err
	}
	subStrLen := utf8.RuneCountInString(subStr)
	if pos < 0 || pos > int64(utf8.RuneCountInString(str)-subStrLen) {
		return 0, false, nil
	} else if subStrLen == 0 {
		return pos + 1, false, nil
	}
	slice := str[runeByteIndex(str, int(pos)):]

	idx := locateStringWithCollation(slice, subStr, b.collation)
	if idx != 0 {
		return pos + idx, false, nil
	}
	return 0, false, nil
}

type hexFunctionClass struct {
	baseFunctionClass
}

func (c *hexFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	switch argTp {
	case types.ETString, types.ETDatetime, types.ETTimestamp, types.ETDuration, types.ETJson:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
		bf.tp.SetFlen(types.UnspecifiedLength)
		if err != nil {
			return nil, err
		}
		argLen := args[0].GetType(ctx.GetEvalCtx()).GetFlen()
		// Use UTF8MB4 as default.
		if argLen != types.UnspecifiedLength {
			bf.tp.SetFlen(argLen * 4 * 2)
		}
		sig := &builtinHexStrArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_HexStrArg)
		return sig, nil
	case types.ETInt, types.ETReal, types.ETDecimal:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
		bf.tp.SetFlen(types.UnspecifiedLength)
		if err != nil {
			return nil, err
		}
		argLen := args[0].GetType(ctx.GetEvalCtx()).GetFlen()
		if argLen != types.UnspecifiedLength {
			bf.tp.SetFlen(argLen * 2)
		}
		charset, collate := ctx.GetCharsetInfo()
		bf.tp.SetCharset(charset)
		bf.tp.SetCollate(collate)
		sig := &builtinHexIntArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_HexIntArg)
		return sig, nil
	default:
		return nil, errors.Errorf("Hex invalid args, need int or string but get %T", args[0].GetType(ctx.GetEvalCtx()))
	}
}

type builtinHexStrArgSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinHexStrArgSig) Clone() builtinFunc {
	newSig := &builtinHexStrArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinHexStrArgSig, corresponding to hex(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexStrArgSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	d, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	return strings.ToUpper(hex.EncodeToString(hack.Slice(d))), false, nil
}

type builtinHexIntArgSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinHexIntArgSig) Clone() builtinFunc {
	newSig := &builtinHexIntArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinHexIntArgSig, corresponding to hex(N)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexIntArgSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	x, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return strings.ToUpper(fmt.Sprintf("%x", uint64(x))), false, nil
}

type unhexFunctionClass struct {
	baseFunctionClass
}

func (c *unhexFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var retFlen int

	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argType := args[0].GetType(ctx.GetEvalCtx())
	argEvalTp := argType.EvalType()
	switch argEvalTp {
	case types.ETString, types.ETDatetime, types.ETTimestamp, types.ETDuration, types.ETJson:
		// Use UTF8MB4 as default charset, so there're (flen * 4 + 1) / 2 byte-pairs.
		retFlen = (argType.GetFlen()*4 + 1) / 2
	case types.ETInt, types.ETReal, types.ETDecimal:
		// For number value, there're (flen + 1) / 2 byte-pairs.
		retFlen = (argType.GetFlen() + 1) / 2
	default:
		return nil, errors.Errorf("Unhex invalid args, need int or string but get %s", argType)
	}
	if argType.GetFlen() == types.UnspecifiedLength {
		retFlen = types.UnspecifiedLength
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(retFlen)
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinUnHexSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_UnHex)
	return sig, nil
}

type builtinUnHexSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUnHexSig) Clone() builtinFunc {
	newSig := &builtinUnHexSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUnHexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_unhex
func (b *builtinUnHexSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	var bs []byte

	d, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	// Add a '0' to the front, if the length is not the multiple of 2
	if len(d)%2 != 0 {
		d = "0" + d
	}
	bs, err = hex.DecodeString(d)
	if err != nil {
		return "", true, nil
	}
	return string(bs), false, nil
}

const spaceChars = " "

type trimFunctionClass struct {
	baseFunctionClass
}

// getFunction sets trim built-in function signature.
// The syntax of trim in mysql is 'TRIM([{BOTH | LEADING | TRAILING} [remstr] FROM] str), TRIM([remstr FROM] str)',
// but we will convert it into trim(str), trim(str, remstr) and trim(str, remstr, direction) in AST.
