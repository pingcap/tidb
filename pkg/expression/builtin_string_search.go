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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tipb/go-tipb"
)

func (c *strcmpFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(2)
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinStrcmpSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Strcmp)
	return sig, nil
}

type builtinStrcmpSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinStrcmpSig) Clone() builtinFunc {
	newSig := &builtinStrcmpSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinStrcmpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html
func (b *builtinStrcmpSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	var (
		left, right string
		isNull      bool
		err         error
	)

	left, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	right, isNull, err = b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	res := types.CompareString(left, right, b.collation)
	return int64(res), false, nil
}

type replaceFunctionClass struct {
	baseFunctionClass
}

func (c *replaceFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(c.fixLength(ctx.GetEvalCtx(), args))
	for _, a := range args {
		SetBinFlagOrBinStr(a.GetType(ctx.GetEvalCtx()), bf.tp)
	}
	sig := &builtinReplaceSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Replace)
	return sig, nil
}

// fixLength calculate the flen of the return type.
func (c *replaceFunctionClass) fixLength(ctx EvalContext, args []Expression) int {
	charLen := args[0].GetType(ctx).GetFlen()
	oldStrLen := args[1].GetType(ctx).GetFlen()
	diff := args[2].GetType(ctx).GetFlen() - oldStrLen
	if diff > 0 && oldStrLen > 0 {
		charLen += (charLen / oldStrLen) * diff
	}
	return charLen
}

type builtinReplaceSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinReplaceSig) Clone() builtinFunc {
	newSig := &builtinReplaceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinReplaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_replace
func (b *builtinReplaceSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	var str, oldStr, newStr string

	str, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	oldStr, isNull, err = b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	newStr, isNull, err = b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	if oldStr == "" {
		return str, false, nil
	}
	return strings.ReplaceAll(str, oldStr, newStr), false, nil
}

type convertFunctionClass struct {
	baseFunctionClass
}

func (c *convertFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}

	charsetArg, ok := args[1].(*Constant)
	if !ok {
		// `args[1]` is limited by parser to be a constant string,
		// should never go into here.
		return nil, errIncorrectArgs.GenWithStackByArgs("charset")
	}
	transcodingName := charsetArg.Value.GetString()
	bf.tp.SetCharset(strings.ToLower(transcodingName))
	// Quoted about the behavior of syntax CONVERT(expr, type) to CHAR():
	// In all cases, the string has the default collation for the character set.
	// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
	// Here in syntax CONVERT(expr USING transcoding_name), behavior is kept the same,
	// picking the default collation of target charset.
	str1, err1 := charset.GetDefaultCollation(bf.tp.GetCharset())
	bf.tp.SetCollate(str1)
	if err1 != nil {
		return nil, errUnknownCharacterSet.GenWithStackByArgs(transcodingName)
	}
	// convert function should always derive to CoercibilityImplicit
	bf.SetCoercibility(CoercibilityImplicit)
	if bf.tp.GetCharset() == charset.CharsetASCII {
		bf.SetRepertoire(ASCII)
	} else {
		bf.SetRepertoire(UNICODE)
	}
	// Result will be a binary string if converts charset to BINARY.
	// See https://dev.mysql.com/doc/refman/5.7/en/charset-binary-set.html
	if types.IsBinaryStr(bf.tp) {
		types.SetBinChsClnFlag(bf.tp)
	} else {
		bf.tp.DelFlag(mysql.BinaryFlag)
	}

	bf.tp.SetFlen(mysql.MaxBlobWidth)
	sig := &builtinConvertSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Convert)
	return sig, nil
}

type builtinConvertSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinConvertSig) Clone() builtinFunc {
	newSig := &builtinConvertSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals CONVERT(expr USING transcoding_name).
// Syntax CONVERT(expr, type) is parsed as cast expr so not handled here.
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
func (b *builtinConvertSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	expr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	argTp, resultTp := b.args[0].GetType(ctx), b.tp
	if !charset.IsSupportedEncoding(resultTp.GetCharset()) {
		return "", false, errUnknownCharacterSet.GenWithStackByArgs(resultTp.GetCharset())
	}
	if types.IsBinaryStr(argTp) {
		// Convert charset binary -> utf8. If it meets error, NULL is returned.
		enc := charset.FindEncoding(resultTp.GetCharset())
		ret, err := enc.Transform(nil, hack.Slice(expr), charset.OpDecodeReplace)
		return string(ret), err != nil, nil
	} else if types.IsBinaryStr(resultTp) {
		// Convert charset utf8 -> binary.
		enc := charset.FindEncoding(argTp.GetCharset())
		ret, err := enc.Transform(nil, hack.Slice(expr), charset.OpEncode)
		return string(ret), false, err
	}
	enc := charset.FindEncoding(resultTp.GetCharset())
	if !enc.IsValid(hack.Slice(expr)) {
		replace, _ := enc.Transform(nil, hack.Slice(expr), charset.OpReplaceNoErr)
		return string(replace), false, nil
	}
	return expr, false, nil
}

type substringFunctionClass struct {
	baseFunctionClass
}

func (c *substringFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := []types.EvalType{types.ETString, types.ETInt}
	if len(args) == 3 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}

	argType := args[0].GetType(ctx.GetEvalCtx())
	bf.tp.SetFlen(argType.GetFlen())
	SetBinFlagOrBinStr(argType, bf.tp)

	var sig builtinFunc
	switch {
	case len(args) == 3 && types.IsBinaryStr(argType):
		sig = &builtinSubstring3ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Substring3Args)
	case len(args) == 3:
		sig = &builtinSubstring3ArgsUTF8Sig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Substring3ArgsUTF8)
	case len(args) == 2 && types.IsBinaryStr(argType):
		sig = &builtinSubstring2ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Substring2Args)
	case len(args) == 2:
		sig = &builtinSubstring2ArgsUTF8Sig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Substring2ArgsUTF8)
	default:
		// Should never happens.
		return nil, errors.Errorf("SUBSTR invalid arg length, expect 2 or 3 but got: %v", len(args))
	}
	return sig, nil
}

type builtinSubstring2ArgsSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubstring2ArgsSig) Clone() builtinFunc {
	newSig := &builtinSubstring2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring2ArgsSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	length := int64(len(str))
	if pos < 0 {
		pos += length
	} else {
		pos--
	}
	if pos > length || pos < 0 {
		pos = length
	}
	return str[pos:], false, nil
}

type builtinSubstring2ArgsUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubstring2ArgsUTF8Sig) Clone() builtinFunc {
	newSig := &builtinSubstring2ArgsUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring2ArgsUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runeCount := int64(utf8.RuneCountInString(str))
	if pos < 0 {
		pos += runeCount
	} else {
		pos--
	}
	if pos > runeCount || pos < 0 {
		pos = runeCount
	}
	return str[runeByteIndex(str, int(pos)):], false, nil
}

type builtinSubstring3ArgsSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubstring3ArgsSig) Clone() builtinFunc {
	newSig := &builtinSubstring3ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	length, isNull, err := b.args[2].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	byteLen := int64(len(str))
	if pos < 0 {
		pos += byteLen
	} else {
		pos--
	}
	if pos > byteLen || pos < 0 {
		pos = byteLen
	}
	end := pos + length
	if end < pos {
		return "", false, nil
	} else if end < byteLen {
		return str[pos:end], false, nil
	}
	return str[pos:], false, nil
}

type builtinSubstring3ArgsUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubstring3ArgsUTF8Sig) Clone() builtinFunc {
	newSig := &builtinSubstring3ArgsUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	length, isNull, err := b.args[2].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	numRunes := int64(utf8.RuneCountInString(str))
	if pos < 0 {
		pos += numRunes
	} else {
		pos--
	}
	if pos > numRunes || pos < 0 {
		pos = numRunes
	}
	end := pos + length
	if end < pos {
		return "", false, nil
	}
	startByte := runeByteIndex(str, int(pos))
	if end < numRunes {
		endByte := runeByteIndex(str, int(end))
		return str[startByte:endByte], false, nil
	}
	return str[startByte:], false, nil
}

type substringIndexFunctionClass struct {
	baseFunctionClass
}

func (c *substringIndexFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType(ctx.GetEvalCtx())
	bf.tp.SetFlen(argType.GetFlen())
	SetBinFlagOrBinStr(argType, bf.tp)
	sig := &builtinSubstringIndexSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_SubstringIndex)
	return sig, nil
}

type builtinSubstringIndexSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSubstringIndexSig) Clone() builtinFunc {
	newSig := &builtinSubstringIndexSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubstringIndexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
func (b *builtinSubstringIndexSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	var (
		str, delim string
		count      int64
	)
	str, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	delim, isNull, err = b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	count, isNull, err = b.args[2].EvalInt(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	if len(delim) == 0 {
		return "", false, nil
	}
	// when count > MaxInt64, returns whole string.
	if count < 0 && mysql.HasUnsignedFlag(b.args[2].GetType(ctx).GetFlag()) {
		return str, false, nil
	}

	strs := strings.Split(str, delim)
	start, end := int64(0), int64(len(strs))
	if count > 0 {
		// If count is positive, everything to the left of the final delimiter (counting from the left) is returned.
		if count < end {
			end = count
		}
	} else {
		// If count is negative, everything to the right of the final delimiter (counting from the right) is returned.
		count = -count
		if count < 0 {
			// -count overflows max int64, returns whole string.
			return str, false, nil
		}

		if count < end {
			start = end - count
		}
	}
	substrs := strs[start:end]
	return strings.Join(substrs, delim), false, nil
}

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
