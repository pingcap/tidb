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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/types"
	"golang.org/x/text/transform"
)

var (
	_ functionClass = &lengthFunctionClass{}
	_ functionClass = &asciiFunctionClass{}
	_ functionClass = &concatFunctionClass{}
	_ functionClass = &concatWSFunctionClass{}
	_ functionClass = &leftFunctionClass{}
	_ functionClass = &repeatFunctionClass{}
	_ functionClass = &lowerFunctionClass{}
	_ functionClass = &reverseFunctionClass{}
	_ functionClass = &spaceFunctionClass{}
	_ functionClass = &upperFunctionClass{}
	_ functionClass = &strcmpFunctionClass{}
	_ functionClass = &replaceFunctionClass{}
	_ functionClass = &convertFunctionClass{}
	_ functionClass = &substringFunctionClass{}
	_ functionClass = &substringIndexFunctionClass{}
	_ functionClass = &locateFunctionClass{}
	_ functionClass = &hexFunctionClass{}
	_ functionClass = &unhexFunctionClass{}
	_ functionClass = &trimFunctionClass{}
	_ functionClass = &lTrimFunctionClass{}
	_ functionClass = &rTrimFunctionClass{}
	_ functionClass = &lpadFunctionClass{}
	_ functionClass = &rpadFunctionClass{}
	_ functionClass = &bitLengthFunctionClass{}
	_ functionClass = &charFunctionClass{}
	_ functionClass = &charLengthFunctionClass{}
	_ functionClass = &findInSetFunctionClass{}
	_ functionClass = &fieldFunctionClass{}
	_ functionClass = &makeSetFunctionClass{}
	_ functionClass = &octFunctionClass{}
	_ functionClass = &ordFunctionClass{}
	_ functionClass = &quoteFunctionClass{}
	_ functionClass = &binFunctionClass{}
	_ functionClass = &eltFunctionClass{}
	_ functionClass = &exportSetFunctionClass{}
	_ functionClass = &formatFunctionClass{}
	_ functionClass = &fromBase64FunctionClass{}
	_ functionClass = &toBase64FunctionClass{}
	_ functionClass = &insertFuncFunctionClass{}
	_ functionClass = &instrFunctionClass{}
	_ functionClass = &loadFileFunctionClass{}
)

var (
	_ builtinFunc = &builtinLengthSig{}
	_ builtinFunc = &builtinASCIISig{}
	_ builtinFunc = &builtinConcatSig{}
	_ builtinFunc = &builtinConcatWSSig{}
	_ builtinFunc = &builtinLeftBinarySig{}
	_ builtinFunc = &builtinLeftSig{}
	_ builtinFunc = &builtinRightBinarySig{}
	_ builtinFunc = &builtinRightSig{}
	_ builtinFunc = &builtinRepeatSig{}
	_ builtinFunc = &builtinLowerSig{}
	_ builtinFunc = &builtinReverseSig{}
	_ builtinFunc = &builtinReverseBinarySig{}
	_ builtinFunc = &builtinSpaceSig{}
	_ builtinFunc = &builtinUpperSig{}
	_ builtinFunc = &builtinStrcmpSig{}
	_ builtinFunc = &builtinReplaceSig{}
	_ builtinFunc = &builtinConvertSig{}
	_ builtinFunc = &builtinSubstringBinary2ArgsSig{}
	_ builtinFunc = &builtinSubstringBinary3ArgsSig{}
	_ builtinFunc = &builtinSubstring2ArgsSig{}
	_ builtinFunc = &builtinSubstring3ArgsSig{}
	_ builtinFunc = &builtinSubstringIndexSig{}
	_ builtinFunc = &builtinLocate2ArgsSig{}
	_ builtinFunc = &builtinLocate3ArgsSig{}
	_ builtinFunc = &builtinLocateBinary2ArgsSig{}
	_ builtinFunc = &builtinLocateBinary3ArgsSig{}
	_ builtinFunc = &builtinHexStrArgSig{}
	_ builtinFunc = &builtinHexIntArgSig{}
	_ builtinFunc = &builtinUnHexSig{}
	_ builtinFunc = &builtinTrim1ArgSig{}
	_ builtinFunc = &builtinTrim2ArgsSig{}
	_ builtinFunc = &builtinTrim3ArgsSig{}
	_ builtinFunc = &builtinLTrimSig{}
	_ builtinFunc = &builtinRTrimSig{}
	_ builtinFunc = &builtinLpadSig{}
	_ builtinFunc = &builtinLpadBinarySig{}
	_ builtinFunc = &builtinRpadSig{}
	_ builtinFunc = &builtinRpadBinarySig{}
	_ builtinFunc = &builtinBitLengthSig{}
	_ builtinFunc = &builtinCharSig{}
	_ builtinFunc = &builtinCharLengthSig{}
	_ builtinFunc = &builtinFindInSetSig{}
	_ builtinFunc = &builtinMakeSetSig{}
	_ builtinFunc = &builtinOctSig{}
	_ builtinFunc = &builtinOrdSig{}
	_ builtinFunc = &builtinQuoteSig{}
	_ builtinFunc = &builtinBinSig{}
	_ builtinFunc = &builtinEltSig{}
	_ builtinFunc = &builtinExportSetSig{}
	_ builtinFunc = &builtinFormatSig{}
	_ builtinFunc = &builtinFromBase64Sig{}
	_ builtinFunc = &builtinToBase64Sig{}
	_ builtinFunc = &builtinInsertFuncSig{}
	_ builtinFunc = &builtinInstrSig{}
	_ builtinFunc = &builtinInstrBinarySig{}
	_ builtinFunc = &builtinLoadFileSig{}
)

func reverseBytes(origin []byte) []byte {
	for i, length := 0, len(origin); i < length/2; i++ {
		origin[i], origin[length-i-1] = origin[length-i-1], origin[i]
	}
	return origin
}

func reverseRunes(origin []rune) []rune {
	for i, length := 0, len(origin); i < length/2; i++ {
		origin[i], origin[length-i-1] = origin[length-i-1], origin[i]
	}
	return origin
}

// setBinFlagOrBinStr sets resTp to binary string if argTp is a binary string,
// if not, sets the binary flag of resTp to true if argTp has binary flag.
func setBinFlagOrBinStr(argTp *types.FieldType, resTp *types.FieldType) {
	if types.IsBinaryStr(argTp) {
		types.SetBinChsClnFlag(resTp)
	} else if mysql.HasBinaryFlag(argTp.Flag) {
		resTp.Flag |= mysql.BinaryFlag
	}
}

type lengthFunctionClass struct {
	baseFunctionClass
}

func (c *lengthFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 10
	sig := &builtinLengthSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinLengthSig struct {
	baseIntBuiltinFunc
}

// evalInt evaluates a builtinLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html
func (b *builtinLengthSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return int64(len([]byte(val))), false, nil
}

type asciiFunctionClass struct {
	baseFunctionClass
}

func (c *asciiFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 3
	sig := &builtinASCIISig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinASCIISig struct {
	baseIntBuiltinFunc
}

// eval evals a builtinASCIISig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ascii
func (b *builtinASCIISig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if len(val) == 0 {
		return 0, false, nil
	}
	return int64(val[0]), false, nil
}

type concatFunctionClass struct {
	baseFunctionClass
}

func (c *concatFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := make([]evalTp, 0, len(args))
	for i := 0; i < len(args); i++ {
		argTps = append(argTps, tpString)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, argTps...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for i := range args {
		argType := args[i].GetType()
		setBinFlagOrBinStr(argType, bf.tp)

		if argType.Flen < 0 {
			bf.tp.Flen = mysql.MaxBlobWidth
			log.Warningf("Not Expected: `Flen` of arg[%v] in CONCAT is -1.", i)
		}
		bf.tp.Flen += argType.Flen
	}
	if bf.tp.Flen >= mysql.MaxBlobWidth {
		bf.tp.Flen = mysql.MaxBlobWidth
	}
	sig := &builtinConcatSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinConcatSig struct {
	baseStringBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat
func (b *builtinConcatSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var s []byte
	for _, a := range b.getArgs() {
		d, isNull, err = a.EvalString(row, b.ctx.GetSessionVars().StmtCtx)
		if isNull || err != nil {
			return d, isNull, errors.Trace(err)
		}
		s = append(s, []byte(d)...)
	}
	return string(s), false, nil
}

type concatWSFunctionClass struct {
	baseFunctionClass
}

func (c *concatWSFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := make([]evalTp, 0, len(args))
	for i := 0; i < len(args); i++ {
		argTps = append(argTps, tpString)
	}

	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, argTps...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for i := range args {
		argType := args[i].GetType()
		setBinFlagOrBinStr(argType, bf.tp)

		// skip seperator param
		if i != 0 {
			if argType.Flen < 0 {
				bf.tp.Flen = mysql.MaxBlobWidth
				log.Warningf("Not Expected: `Flen` of arg[%v] in CONCAT_WS is -1.", i)
			}
			bf.tp.Flen += argType.Flen
		}
	}

	// add seperator
	argsLen := len(args) - 1
	bf.tp.Flen += argsLen - 1

	if bf.tp.Flen >= mysql.MaxBlobWidth {
		bf.tp.Flen = mysql.MaxBlobWidth
	}

	sig := &builtinConcatWSSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinConcatWSSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinConcatWSSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat-ws
func (b *builtinConcatWSSig) evalString(row []types.Datum) (string, bool, error) {
	args := b.getArgs()
	strs := make([]string, 0, len(args))
	var sep string
	for i, arg := range args {
		val, isNull, err := arg.EvalString(row, b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return val, isNull, errors.Trace(err)
		}

		if isNull {
			// If the separator is NULL, the result is NULL.
			if i == 0 {
				return val, isNull, nil
			}
			// CONCAT_WS() does not skip empty strings. However,
			// it does skip any NULL values after the separator argument.
			continue
		}

		if i == 0 {
			sep = val
			continue
		}
		strs = append(strs, val)
	}

	// TODO: check whether the length of result is larger than Flen
	return strings.Join(strs, sep), false, nil
}

type leftFunctionClass struct {
	baseFunctionClass
}

func (c *leftFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	setBinFlagOrBinStr(argType, bf.tp)
	if types.IsBinaryStr(argType) {
		sig := &builtinLeftBinarySig{baseStringBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	}
	sig := &builtinLeftSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinLeftBinarySig struct {
	baseStringBuiltinFunc
}

// evalString evals LEFT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func (b *builtinLeftBinarySig) evalString(row []types.Datum) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	left, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	leftLength := int(left)
	if strLength := len(str); leftLength > strLength {
		leftLength = strLength
	} else if leftLength < 0 {
		leftLength = 0
	}
	return str[:leftLength], false, nil
}

type builtinLeftSig struct {
	baseStringBuiltinFunc
}

// evalString evals LEFT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func (b *builtinLeftSig) evalString(row []types.Datum) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	left, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	runes, leftLength := []rune(str), int(left)
	if runeLength := len(runes); leftLength > runeLength {
		leftLength = runeLength
	} else if leftLength < 0 {
		leftLength = 0
	}
	return string(runes[:leftLength]), false, nil
}

type rightFunctionClass struct {
	baseFunctionClass
}

func (c *rightFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	setBinFlagOrBinStr(argType, bf.tp)
	if types.IsBinaryStr(argType) {
		sig := &builtinRightBinarySig{baseStringBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	}
	sig := &builtinRightSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinRightBinarySig struct {
	baseStringBuiltinFunc
}

// evalString evals RIGHT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_right
func (b *builtinRightBinarySig) evalString(row []types.Datum) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	right, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	strLength, rightLength := len(str), int(right)
	if rightLength > strLength {
		rightLength = strLength
	} else if rightLength < 0 {
		rightLength = 0
	}
	return str[strLength-rightLength:], false, nil
}

type builtinRightSig struct {
	baseStringBuiltinFunc
}

// evalString evals RIGHT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_right
func (b *builtinRightSig) evalString(row []types.Datum) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	right, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	runes := []rune(str)
	strLength, rightLength := len(runes), int(right)
	if rightLength > strLength {
		rightLength = strLength
	} else if rightLength < 0 {
		rightLength = 0
	}
	return string(runes[strLength-rightLength:]), false, nil
}

type repeatFunctionClass struct {
	baseFunctionClass
}

func (c *repeatFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = mysql.MaxBlobWidth
	setBinFlagOrBinStr(args[0].GetType(), bf.tp)
	sig := &builtinRepeatSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinRepeatSig struct {
	baseStringBuiltinFunc
}

// eval evals a builtinRepeatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_repeat
func (b *builtinRepeatSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}

	num, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	if num < 1 {
		return "", false, nil
	}
	if num > math.MaxInt32 {
		num = math.MaxInt32
	}

	if int64(len(str)) > int64(b.tp.Flen)/num {
		return "", true, nil
	}
	return strings.Repeat(str, int(num)), false, nil
}

type lowerFunctionClass struct {
	baseFunctionClass
}

func (c *lowerFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	argTp := args[0].GetType()
	bf.tp.Flen = argTp.Flen
	setBinFlagOrBinStr(argTp, bf.tp)
	sig := &builtinLowerSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinLowerSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinLowerSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lower
func (b *builtinLowerSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	if types.IsBinaryStr(b.args[0].GetType()) {
		return d, false, nil
	}

	return strings.ToLower(d), false, nil
}

type reverseFunctionClass struct {
	baseFunctionClass
}

func (c *reverseFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	*bf.tp = *args[0].GetType()
	var sig builtinFunc
	if types.IsBinaryStr(bf.tp) {
		sig = &builtinReverseBinarySig{baseStringBuiltinFunc{bf}}
	} else {
		sig = &builtinReverseSig{baseStringBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), nil
}

type builtinReverseBinarySig struct {
	baseStringBuiltinFunc
}

// evalString evals a REVERSE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseBinarySig) evalString(row []types.Datum) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	reversed := reverseBytes([]byte(str))
	return string(reversed), false, nil
}

type builtinReverseSig struct {
	baseStringBuiltinFunc
}

// evalString evals a REVERSE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseSig) evalString(row []types.Datum) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	reversed := reverseRunes([]rune(str))
	return string(reversed), false, nil
}

type spaceFunctionClass struct {
	baseFunctionClass
}

func (c *spaceFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = mysql.MaxBlobWidth
	sig := &builtinSpaceSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinSpaceSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinSpaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_space
func (b *builtinSpaceSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var x int64

	x, isNull, err = b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	if x > mysql.MaxBlobWidth {
		return d, true, nil
	}
	if x < 0 {
		x = 0
	}
	return strings.Repeat(" ", int(x)), false, nil
}

type upperFunctionClass struct {
	baseFunctionClass
}

func (c *upperFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	argTp := args[0].GetType()
	bf.tp.Flen = argTp.Flen
	setBinFlagOrBinStr(argTp, bf.tp)
	sig := &builtinUpperSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinUpperSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinUpperSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func (b *builtinUpperSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}

	if types.IsBinaryStr(b.args[0].GetType()) {
		return d, false, nil
	}

	return strings.ToUpper(d), false, nil
}

type strcmpFunctionClass struct {
	baseFunctionClass
}

func (c *strcmpFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 2
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinStrcmpSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinStrcmpSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinStrcmpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html
func (b *builtinStrcmpSig) evalInt(row []types.Datum) (int64, bool, error) {
	var (
		left, right string
		isNull      bool
		err         error
	)

	sc := b.ctx.GetSessionVars().StmtCtx
	left, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	right, isNull, err = b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	res := types.CompareString(left, right)
	return int64(res), false, nil
}

type replaceFunctionClass struct {
	baseFunctionClass
}

func (c *replaceFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = c.fixLength(args)
	for _, a := range args {
		setBinFlagOrBinStr(a.GetType(), bf.tp)
	}
	sig := &builtinReplaceSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

// fixLength calculate the Flen of the return type.
func (c *replaceFunctionClass) fixLength(args []Expression) int {
	charLen := args[0].GetType().Flen
	oldStrLen := args[1].GetType().Flen
	diff := args[2].GetType().Flen - oldStrLen
	if diff > 0 && oldStrLen > 0 {
		charLen += (charLen / oldStrLen) * diff
	}
	return charLen
}

type builtinReplaceSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinReplaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_replace
func (b *builtinReplaceSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var str, oldStr, newStr string

	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	oldStr, isNull, err = b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	newStr, isNull, err = b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	if oldStr == "" {
		return str, false, nil
	}
	return strings.Replace(str, oldStr, newStr, -1), false, nil
}

type convertFunctionClass struct {
	baseFunctionClass
}

func (c *convertFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinConvertSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinConvertSig struct {
	baseBuiltinFunc
}

// eval evals a builtinConvertSig.
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
func (b *builtinConvertSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// Casting nil to any type returns nil
	if args[0].Kind() != types.KindString {
		return d, nil
	}

	str := args[0].GetString()
	Charset := args[1].GetString()

	if strings.ToLower(Charset) == "ascii" {
		d.SetString(str)
		return d, nil
	} else if strings.ToLower(Charset) == "utf8mb4" {
		d.SetString(str)
		return d, nil
	}

	encoding, _ := charset.Lookup(Charset)
	if encoding == nil {
		return d, errors.Errorf("unknown encoding: %s", Charset)
	}

	target, _, err := transform.String(encoding.NewDecoder(), str)
	if err != nil {
		log.Errorf("Convert %s to %s with error: %v", str, Charset, err)
		return d, errors.Trace(err)
	}
	d.SetString(target)
	return d, nil
}

type substringFunctionClass struct {
	baseFunctionClass
}

func (c *substringFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := []evalTp{tpString, tpInt}
	if len(args) == 3 {
		argTps = append(argTps, tpInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, argTps...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	setBinFlagOrBinStr(argType, bf.tp)

	var sig builtinFunc
	switch {
	case len(args) == 3 && types.IsBinaryStr(argType):
		sig = &builtinSubstringBinary3ArgsSig{baseStringBuiltinFunc{bf}}
	case len(args) == 3:
		sig = &builtinSubstring3ArgsSig{baseStringBuiltinFunc{bf}}
	case len(args) == 2 && types.IsBinaryStr(argType):
		sig = &builtinSubstringBinary2ArgsSig{baseStringBuiltinFunc{bf}}
	case len(args) == 2:
		sig = &builtinSubstring2ArgsSig{baseStringBuiltinFunc{bf}}
	default:
		// Should never happens.
		return nil, errors.Errorf("SUBSTR invalid arg length, expect 2 or 3 but got: %v", len(args))
	}
	return sig.setSelf(sig), nil
}

type builtinSubstringBinary2ArgsSig struct {
	baseStringBuiltinFunc
}

// evalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstringBinary2ArgsSig) evalString(row []types.Datum) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	pos, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
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

type builtinSubstring2ArgsSig struct {
	baseStringBuiltinFunc
}

// evalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring2ArgsSig) evalString(row []types.Datum) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	pos, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	runes := []rune(str)
	length := int64(len(runes))
	if pos < 0 {
		pos += length
	} else {
		pos--
	}
	if pos > length || pos < 0 {
		pos = length
	}
	return string(runes[pos:]), false, nil
}

type builtinSubstringBinary3ArgsSig struct {
	baseStringBuiltinFunc
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstringBinary3ArgsSig) evalString(row []types.Datum) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	pos, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	length, isNull, err := b.args[2].EvalInt(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
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

type builtinSubstring3ArgsSig struct {
	baseStringBuiltinFunc
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsSig) evalString(row []types.Datum) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	pos, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	length, isNull, err := b.args[2].EvalInt(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	runes := []rune(str)
	numRunes := int64(len(runes))
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
	} else if end < numRunes {
		return string(runes[pos:end]), false, nil
	}
	return string(runes[pos:]), false, nil
}

type substringIndexFunctionClass struct {
	baseFunctionClass
}

func (c *substringIndexFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpString, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	setBinFlagOrBinStr(argType, bf.tp)
	sig := &builtinSubstringIndexSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinSubstringIndexSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinSubstringIndexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
func (b *builtinSubstringIndexSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var (
		str, delim string
		count      int64
	)
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	delim, isNull, err = b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	count, isNull, err = b.args[2].EvalInt(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	if len(delim) == 0 {
		return "", false, nil
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

func (c *locateFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	hasStartPos, argTps := len(args) == 3, []evalTp{tpString, tpString}
	if hasStartPos {
		argTps = append(argTps, tpInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, argTps...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var sig builtinFunc
	// Loacte is multibyte safe, and is case-sensitive only if at least one argument is a binary string.
	hasBianryInput := types.IsBinaryStr(args[0].GetType()) || types.IsBinaryStr(args[1].GetType())
	switch {
	case hasStartPos && hasBianryInput:
		sig = &builtinLocateBinary3ArgsSig{baseIntBuiltinFunc{bf}}
		break
	case hasStartPos:
		sig = &builtinLocate3ArgsSig{baseIntBuiltinFunc{bf}}
		break
	case hasBianryInput:
		sig = &builtinLocateBinary2ArgsSig{baseIntBuiltinFunc{bf}}
		break
	default:
		sig = &builtinLocate2ArgsSig{baseIntBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), nil
}

type builtinLocateBinary2ArgsSig struct {
	baseIntBuiltinFunc
}

// evalInt evals LOCATE(substr,str), case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocateBinary2ArgsSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	subStr, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	str, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
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

type builtinLocate2ArgsSig struct {
	baseIntBuiltinFunc
}

// evalInt evals LOCATE(substr,str), non case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate2ArgsSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	subStr, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	str, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if int64(len([]rune(subStr))) == 0 {
		return 1, false, nil
	}
	slice := string([]rune(strings.ToLower(str)))
	ret, idx := 0, strings.Index(slice, strings.ToLower(subStr))
	if idx != -1 {
		ret = utf8.RuneCountInString(slice[:idx]) + 1
	}
	return int64(ret), false, nil
}

type builtinLocateBinary3ArgsSig struct {
	baseIntBuiltinFunc
}

// evalInt evals LOCATE(substr,str,pos), case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocateBinary3ArgsSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	subStr, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	str, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	pos, isNull, err := b.args[2].EvalInt(row, sc)
	// Transfer the argument which starts from 1 to real index which starts from 0.
	pos--
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
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

type builtinLocate3ArgsSig struct {
	baseIntBuiltinFunc
}

// evalInt evals LOCATE(substr,str,pos), non case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate3ArgsSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	subStr, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	str, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	pos, isNull, err := b.args[2].EvalInt(row, sc)
	// Transfer the argument which starts from 1 to real index which starts from 0.
	pos--
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	subStrLen := len([]rune(subStr))
	if pos < 0 || pos > int64(len([]rune(strings.ToLower(str)))-subStrLen) {
		return 0, false, nil
	} else if subStrLen == 0 {
		return pos + 1, false, nil
	}
	slice := string([]rune(strings.ToLower(str))[pos:])
	idx := strings.Index(slice, strings.ToLower(subStr))
	if idx != -1 {
		return pos + int64(utf8.RuneCountInString(slice[:idx])) + 1, false, nil
	}
	return 0, false, nil
}

type hexFunctionClass struct {
	baseFunctionClass
}

func (c *hexFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	switch t := args[0].GetTypeClass(); t {
	case types.ClassString:
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Use UTF-8 as default
		bf.tp.Flen = args[0].GetType().Flen * 3 * 2
		sig := &builtinHexStrArgSig{baseStringBuiltinFunc{bf}}
		return sig.setSelf(sig), nil

	case types.ClassInt, types.ClassReal, types.ClassDecimal:
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpInt)
		if err != nil {
			return nil, errors.Trace(err)
		}
		bf.tp.Flen = args[0].GetType().Flen * 2
		sig := &builtinHexIntArgSig{baseStringBuiltinFunc{bf}}
		return sig.setSelf(sig), nil

	default:
		return nil, errors.Errorf("Hex invalid args, need int or string but get %T", t)
	}
}

type builtinHexStrArgSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinHexStrArgSig, corresponding to hex(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexStrArgSig) evalString(row []types.Datum) (string, bool, error) {
	d, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	return strings.ToUpper(hex.EncodeToString(hack.Slice(d))), false, nil
}

type builtinHexIntArgSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinHexIntArgSig, corresponding to hex(N)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexIntArgSig) evalString(row []types.Datum) (string, bool, error) {
	x, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}
	return strings.ToUpper(fmt.Sprintf("%x", uint64(x))), false, nil
}

type unhexFunctionClass struct {
	baseFunctionClass
}

func (c *unhexFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	var retFlen int

	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argType := args[0].GetType()
	switch t := args[0].GetTypeClass(); t {
	case types.ClassString:
		// Use UTF-8 as default charset, so there're (Flen * 3 + 1) / 2 byte-pairs
		retFlen = (argType.Flen*3 + 1) / 2

	case types.ClassInt, types.ClassReal, types.ClassDecimal:
		// For number value, there're (Flen + 1) / 2 byte-pairs
		retFlen = (argType.Flen + 1) / 2

	default:
		return nil, errors.Errorf("Unhex invalid args, need int or string but get %T", t)
	}

	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = retFlen
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinUnHexSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinUnHexSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinUnHexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_unhex
func (b *builtinUnHexSig) evalString(row []types.Datum) (string, bool, error) {
	var bs []byte

	d, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
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

const spaceChars = "\n\t\r "

type trimFunctionClass struct {
	baseFunctionClass
}

// The syntax of trim in mysql is 'TRIM([{BOTH | LEADING | TRAILING} [remstr] FROM] str), TRIM([remstr FROM] str)',
// but we wil convert it into trim(str), trim(str, remstr) and trim(str, remstr, direction) in AST.
func (c *trimFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	switch len(args) {
	case 1:
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
		if err != nil {
			return nil, errors.Trace(err)
		}
		argType := args[0].GetType()
		bf.tp.Flen = argType.Flen
		setBinFlagOrBinStr(argType, bf.tp)
		sig := &builtinTrim1ArgSig{baseStringBuiltinFunc{bf}}
		return sig.setSelf(sig), nil

	case 2:
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpString)
		if err != nil {
			return nil, errors.Trace(err)
		}
		argType := args[0].GetType()
		setBinFlagOrBinStr(argType, bf.tp)
		sig := &builtinTrim2ArgsSig{baseStringBuiltinFunc{bf}}
		return sig.setSelf(sig), nil

	case 3:
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpString, tpInt)
		if err != nil {
			return nil, errors.Trace(err)
		}
		argType := args[0].GetType()
		bf.tp.Flen = argType.Flen
		setBinFlagOrBinStr(argType, bf.tp)
		sig := &builtinTrim3ArgsSig{baseStringBuiltinFunc{bf}}
		return sig.setSelf(sig), nil

	default:
		return nil, errors.Trace(c.verifyArgs(args))
	}
}

type builtinTrim1ArgSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinTrim1ArgSig, corresponding to trim(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim1ArgSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	return strings.Trim(d, spaceChars), false, nil
}

type builtinTrim2ArgsSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinTrim2ArgsSig, corresponding to trim(str, remstr)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim2ArgsSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var str, remstr string

	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	remstr, isNull, err = b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	d = trimLeft(str, remstr)
	d = trimRight(d, remstr)
	return d, false, nil
}

type builtinTrim3ArgsSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinTrim3ArgsSig, corresponding to trim(str, remstr, direction)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim3ArgsSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	var (
		str, remstr  string
		x            int64
		direction    ast.TrimDirectionType
		isRemStrNull bool
	)
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err = b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	remstr, isRemStrNull, err = b.args[1].EvalString(row, sc)
	if err != nil {
		return d, isNull, errors.Trace(err)
	}
	x, isNull, err = b.args[2].EvalInt(row, sc)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	direction = ast.TrimDirectionType(x)
	if direction == ast.TrimLeading {
		if isRemStrNull {
			d = strings.TrimLeft(str, spaceChars)
		} else {
			d = trimLeft(str, remstr)
		}
	} else if direction == ast.TrimTrailing {
		if isRemStrNull {
			d = strings.TrimRight(str, spaceChars)
		} else {
			d = trimRight(str, remstr)
		}
	} else {
		if isRemStrNull {
			d = strings.Trim(str, spaceChars)
		} else {
			d = trimLeft(str, remstr)
			d = trimRight(d, remstr)
		}
	}
	return d, false, nil
}

type lTrimFunctionClass struct {
	baseFunctionClass
}

func (c *lTrimFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	setBinFlagOrBinStr(argType, bf.tp)
	sig := &builtinLTrimSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinLTrimSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinLTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ltrim
func (b *builtinLTrimSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	return strings.TrimLeft(d, spaceChars), false, nil
}

type rTrimFunctionClass struct {
	baseFunctionClass
}

func (c *rTrimFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	argType := args[0].GetType()
	bf.tp.Flen = argType.Flen
	setBinFlagOrBinStr(argType, bf.tp)
	sig := &builtinRTrimSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinRTrimSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinRTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rtrim
func (b *builtinRTrimSig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return d, isNull, errors.Trace(err)
	}
	return strings.TrimRight(d, spaceChars), false, nil
}

func trimLeft(str, remstr string) string {
	for {
		x := strings.TrimPrefix(str, remstr)
		if len(x) == len(str) {
			return x
		}
		str = x
	}
}

func trimRight(str, remstr string) string {
	for {
		x := strings.TrimSuffix(str, remstr)
		if len(x) == len(str) {
			return x
		}
		str = x
	}
}

func getFlen4LpadAndRpad(sc *variable.StatementContext, arg Expression) int {
	if constant, ok := arg.(*Constant); ok {
		length, isNull, err := constant.EvalInt(nil, sc)
		if err != nil {
			log.Errorf("getFlen4LpadAndRpad with error: %v", err.Error())
		}
		if isNull || err != nil || length > mysql.MaxBlobWidth {
			return mysql.MaxBlobWidth
		}
		return int(length)
	}
	return mysql.MaxBlobWidth
}

type lpadFunctionClass struct {
	baseFunctionClass
}

func (c *lpadFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpInt, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = getFlen4LpadAndRpad(bf.ctx.GetSessionVars().StmtCtx, args[1])
	setBinFlagOrBinStr(args[0].GetType(), bf.tp)
	setBinFlagOrBinStr(args[2].GetType(), bf.tp)
	if types.IsBinaryStr(args[0].GetType()) || types.IsBinaryStr(args[2].GetType()) {
		sig := &builtinLpadBinarySig{baseStringBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	}
	if bf.tp.Flen *= 4; bf.tp.Flen > mysql.MaxBlobWidth {
		bf.tp.Flen = mysql.MaxBlobWidth
	}
	sig := &builtinLpadSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinLpadBinarySig struct {
	baseStringBuiltinFunc
}

// evalString evals LPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_lpad
func (b *builtinLpadBinarySig) evalString(row []types.Datum) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	byteLength := len(str)

	length, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	targetLength := int(length)

	padStr, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	padLength := len(padStr)

	if targetLength < 0 || targetLength > b.tp.Flen || (byteLength < targetLength && padLength == 0) {
		return "", true, nil
	}

	if tailLen := targetLength - byteLength; tailLen > 0 {
		repeatCount := tailLen/padLength + 1
		str = strings.Repeat(padStr, repeatCount)[:tailLen] + str
	}
	return str[:targetLength], false, nil
}

type builtinLpadSig struct {
	baseStringBuiltinFunc
}

// evalString evals LPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_lpad
func (b *builtinLpadSig) evalString(row []types.Datum) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	runeLength := len([]rune(str))

	length, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	targetLength := int(length)

	padStr, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	padLength := len([]rune(padStr))

	if targetLength < 0 || targetLength*4 > b.tp.Flen || (runeLength < targetLength && padLength == 0) {
		return "", true, nil
	}

	if tailLen := targetLength - runeLength; tailLen > 0 {
		repeatCount := tailLen/padLength + 1
		str = string([]rune(strings.Repeat(padStr, repeatCount))[:tailLen]) + str
	}
	return string([]rune(str)[:targetLength]), false, nil
}

type rpadFunctionClass struct {
	baseFunctionClass
}

func (c *rpadFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpInt, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = getFlen4LpadAndRpad(bf.ctx.GetSessionVars().StmtCtx, args[1])
	if err != nil {
		return nil, errors.Trace(err)
	}
	setBinFlagOrBinStr(args[0].GetType(), bf.tp)
	setBinFlagOrBinStr(args[2].GetType(), bf.tp)
	if types.IsBinaryStr(args[0].GetType()) || types.IsBinaryStr(args[2].GetType()) {
		sig := &builtinRpadBinarySig{baseStringBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	}
	if bf.tp.Flen *= 4; bf.tp.Flen > mysql.MaxBlobWidth {
		bf.tp.Flen = mysql.MaxBlobWidth
	}
	sig := &builtinRpadSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinRpadBinarySig struct {
	baseStringBuiltinFunc
}

// evalString evals RPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadBinarySig) evalString(row []types.Datum) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	byteLength := len(str)

	length, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	targetLength := int(length)

	padStr, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	padLength := len(padStr)

	if targetLength < 0 || targetLength > b.tp.Flen || (byteLength < targetLength && padLength == 0) {
		return "", true, nil
	}

	if tailLen := targetLength - byteLength; tailLen > 0 {
		repeatCount := tailLen/padLength + 1
		str = str + strings.Repeat(padStr, repeatCount)
	}
	return str[:targetLength], false, nil
}

type builtinRpadSig struct {
	baseStringBuiltinFunc
}

// evalString evals RPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadSig) evalString(row []types.Datum) (string, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	runeLength := len([]rune(str))

	length, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	targetLength := int(length)

	padStr, isNull, err := b.args[2].EvalString(row, sc)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	padLength := len([]rune(padStr))

	if targetLength < 0 || targetLength*4 > b.tp.Flen || (runeLength < targetLength && padLength == 0) {
		return "", true, nil
	}

	if tailLen := targetLength - runeLength; tailLen > 0 {
		repeatCount := tailLen/padLength + 1
		str = str + strings.Repeat(padStr, repeatCount)
	}
	return string([]rune(str)[:targetLength]), false, nil
}

type bitLengthFunctionClass struct {
	baseFunctionClass
}

func (c *bitLengthFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 10
	sig := &builtinBitLengthSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinBitLengthSig struct {
	baseIntBuiltinFunc
}

// evalInt evaluates a builtinBitLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_bit-length
func (b *builtinBitLengthSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	return int64(len(val) * 8), false, nil
}

type charFunctionClass struct {
	baseFunctionClass
}

func (c *charFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTps := make([]evalTp, 0, len(args))
	for i := 0; i < len(args)-1; i++ {
		argTps = append(argTps, tpInt)
	}
	argTps = append(argTps, tpString)
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, argTps...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 4 * (len(args) - 1)
	types.SetBinChsClnFlag(bf.tp)

	sig := &builtinCharSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinCharSig struct {
	baseStringBuiltinFunc
}

func (b *builtinCharSig) convertToBytes(ints []int64) []byte {
	buffer := bytes.NewBuffer([]byte{})
	for i := len(ints) - 1; i >= 0; i-- {
		for count, val := 0, ints[i]; count < 4; count++ {
			buffer.WriteByte(byte(val & 0xff))
			if val >>= 8; val == 0 {
				break
			}
		}
	}
	return reverseBytes(buffer.Bytes())
}

// evalString evals CHAR(N,... [USING charset_name]).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char.
func (b *builtinCharSig) evalString(row []types.Datum) (string, bool, error) {
	bigints := make([]int64, 0, len(b.args)-1)

	sc := b.ctx.GetSessionVars().StmtCtx
	for i := 0; i < len(b.args)-1; i++ {
		val, IsNull, err := b.args[i].EvalInt(row, sc)
		if err != nil {
			return "", true, errors.Trace(err)
		}
		if IsNull {
			continue
		}
		bigints = append(bigints, val)
	}
	// The last argument represents the charset name after "using".
	// Use default charset utf8 if it is nil.
	argCharset, IsNull, err := b.args[len(b.args)-1].EvalString(row, sc)
	if err != nil {
		return "", true, errors.Trace(err)
	}

	result := string(b.convertToBytes(bigints))
	charsetLabel := strings.ToLower(argCharset)
	if IsNull || charsetLabel == "ascii" || strings.HasPrefix(charsetLabel, "utf8") {
		return result, false, nil
	}

	encoding, charsetName := charset.Lookup(charsetLabel)
	if encoding == nil {
		return "", true, errors.Errorf("unknown encoding: %s", argCharset)
	}

	oldStr := result
	result, _, err = transform.String(encoding.NewDecoder(), result)
	if err != nil {
		log.Errorf("Convert %s to %s with error: %v", oldStr, charsetName, err.Error())
		return "", true, errors.Trace(err)
	}
	return result, false, nil
}

type charLengthFunctionClass struct {
	baseFunctionClass
}

func (c *charLengthFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if argsErr := c.verifyArgs(args); argsErr != nil {
		return nil, errors.Trace(argsErr)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinCharLengthSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinCharLengthSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinCharLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char-length
func (b *builtinCharLengthSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return int64(len([]rune(val))), false, nil
}

type findInSetFunctionClass struct {
	baseFunctionClass
}

func (c *findInSetFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinFindInSetSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinFindInSetSig struct {
	baseBuiltinFunc
}

// eval evals a builtinFindInSetSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_find-in-set
// TODO: This function can be optimized by using bit arithmetic when the first argument is
// a constant string and the second is a column of type SET.
func (b *builtinFindInSetSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// args[0] -> Str
	// args[1] -> StrList
	if args[0].IsNull() || args[1].IsNull() {
		return
	}

	str, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	strlst, err := args[1].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}

	d.SetInt64(0)
	if len(strlst) == 0 {
		return
	}
	for i, s := range strings.Split(strlst, ",") {
		if s == str {
			d.SetInt64(int64(i + 1))
			return
		}
	}
	return
}

type fieldFunctionClass struct {
	baseFunctionClass
}

func (c *fieldFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinFieldSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinFieldSig struct {
	baseBuiltinFunc
}

// eval evals a builtinFieldSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
// Returns the index (position) of arg0 in the arg1, arg2, arg3, ... list.
// Returns 0 if arg0 is not found.
// If arg0 is NULL, the return value is 0 because NULL fails equality comparison with any value.
// If all arguments are strings, all arguments are compared as strings.
// If all arguments are numbers, they are compared as numbers.
// Otherwise, the arguments are compared as double.
func (b *builtinFieldSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d.SetInt64(0)
	if args[0].IsNull() {
		return
	}
	var (
		pos                  int64
		allString, allNumber bool
		newArgs              []types.Datum
	)
	allString, allNumber = true, true
	for i := 0; i < len(args) && (allString || allNumber); i++ {
		switch args[i].Kind() {
		case types.KindInt64, types.KindUint64, types.KindFloat32, types.KindFloat64, types.KindMysqlDecimal:
			allString = false
		case types.KindString, types.KindBytes:
			allNumber = false
		default:
			allString, allNumber = false, false
		}
	}
	newArgs, err = argsToSpecifiedType(args, allString, allNumber, b.ctx)
	if err != nil {
		return d, errors.Trace(err)
	}
	arg0, sc := newArgs[0], b.ctx.GetSessionVars().StmtCtx
	for i, curArg := range newArgs[1:] {
		cmpResult, _ := arg0.CompareDatum(sc, curArg)
		if cmpResult == 0 {
			pos = int64(i + 1)
			break
		}
	}
	d.SetInt64(pos)
	return d, errors.Trace(err)
}

// argsToSpecifiedType converts the type of all arguments in args into string type or double type.
func argsToSpecifiedType(args []types.Datum, allString bool, allNumber bool, ctx context.Context) (newArgs []types.Datum, err error) {
	if allNumber { // If all arguments are numbers, they can be compared directly without type converting.
		return args, nil
	}
	sc := ctx.GetSessionVars().StmtCtx
	newArgs = make([]types.Datum, len(args))
	for i, arg := range args {
		if allString {
			str, err := arg.ToString()
			if err != nil {
				return newArgs, errors.Trace(err)
			}
			newArgs[i] = types.NewStringDatum(str)
		} else {
			// If error occurred when convert arg to float64, ignore it and set f as 0.
			f, _ := arg.ToFloat64(sc)
			newArgs[i] = types.NewFloat64Datum(f)
		}
	}
	return
}

type makeSetFunctionClass struct {
	baseFunctionClass
}

func (c *makeSetFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinMakeSetSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinMakeSetSig struct {
	baseBuiltinFunc
}

// eval evals a builtinMakeSetSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_make-set
func (b *builtinMakeSetSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() {
		d.SetNull()
		return
	}
	var (
		arg0 int64
		sets []string
	)

	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, err = args[0].ToInt64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	for i := 1; i < len(args); i++ {
		if args[i].IsNull() {
			continue
		}
		if arg0&(1<<uint(i-1)) > 0 {
			str, err1 := args[i].ToString()
			if err1 != nil {
				return d, errors.Trace(err1)
			}
			sets = append(sets, str)
		}
	}

	d.SetString(strings.Join(sets, ","))
	return d, errors.Trace(err)
}

type octFunctionClass struct {
	baseFunctionClass
}

func (c *octFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinOctSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinOctSig struct {
	baseBuiltinFunc
}

// eval evals a builtinOctSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_oct
func (b *builtinOctSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	var (
		negative bool
		overflow bool
	)
	arg := args[0]
	if arg.IsNull() {
		return d, nil
	}
	n, err := arg.ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	n = getValidPrefix(strings.TrimSpace(n), 10)
	if len(n) == 0 {
		d.SetString("0")
		return d, nil
	}
	if n[0] == '-' {
		negative = true
		n = n[1:]
	}
	val, err := strconv.ParseUint(n, 10, 64)
	if err != nil {
		if numError, ok := err.(*strconv.NumError); ok {
			if numError.Err == strconv.ErrRange {
				overflow = true
			} else {
				return d, errors.Trace(err)
			}
		} else {
			return d, errors.Trace(err)
		}
	}

	if negative && !overflow {
		val = -val
	}
	str := strconv.FormatUint(val, 8)
	d.SetString(str)
	return d, nil
}

type ordFunctionClass struct {
	baseFunctionClass
}

func (c *ordFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 10
	sig := &builtinOrdSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinOrdSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinOrdSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ord
func (b *builtinOrdSig) evalInt(row []types.Datum) (int64, bool, error) {
	str, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if len(str) == 0 {
		return 0, false, nil
	}

	_, size := utf8.DecodeRuneInString(str)
	leftMost := str[:size]
	var result int64
	var factor int64 = 1
	for i := len(leftMost) - 1; i >= 0; i-- {
		result += int64(leftMost[i]) * factor
		factor *= 256
	}

	return result, false, nil
}

type quoteFunctionClass struct {
	baseFunctionClass
}

func (c *quoteFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinQuoteSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinQuoteSig struct {
	baseBuiltinFunc
}

// eval evals a builtinQuoteSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_quote
func (b *builtinQuoteSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() {
		return
	}
	var (
		str    string
		buffer bytes.Buffer
	)
	str, err = args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	runes := []rune(str)
	buffer.WriteRune('\'')
	for i := 0; i < len(runes); i++ {
		switch runes[i] {
		case '\\', '\'':
			buffer.WriteRune('\\')
			buffer.WriteRune(runes[i])
		case 0:
			buffer.WriteRune('\\')
			buffer.WriteRune('0')
		case '\032':
			buffer.WriteRune('\\')
			buffer.WriteRune('Z')
		default:
			buffer.WriteRune(runes[i])
		}
	}
	buffer.WriteRune('\'')
	d.SetString(buffer.String())
	return d, errors.Trace(err)
}

type binFunctionClass struct {
	baseFunctionClass
}

func (c *binFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 64
	sig := &builtinBinSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinBinSig struct {
	baseStringBuiltinFunc
}

// evalString evals BIN(N).
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_bin
func (b *builtinBinSig) evalString(row []types.Datum) (string, bool, error) {
	val, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	return fmt.Sprintf("%b", uint64(val)), false, nil
}

type eltFunctionClass struct {
	baseFunctionClass
}

func (c *eltFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if argsErr := c.verifyArgs(args); argsErr != nil {
		return nil, errors.Trace(argsErr)
	}
	argTps := make([]evalTp, 0, len(args))
	argTps = append(argTps, tpInt)
	for i := 1; i < len(args); i++ {
		argTps = append(argTps, tpString)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, argTps...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, arg := range args[1:] {
		argType := arg.GetType()
		if types.IsBinaryStr(argType) {
			types.SetBinChsClnFlag(bf.tp)
		}
		if argType.Flen > bf.tp.Flen {
			bf.tp.Flen = argType.Flen
		}
	}
	sig := &builtinEltSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinEltSig struct {
	baseStringBuiltinFunc
}

// evalString evals a builtinEltSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_elt
func (b *builtinEltSig) evalString(row []types.Datum) (string, bool, error) {
	idx, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	if idx < 1 || idx >= int64(len(b.args)) {
		return "", true, nil
	}
	arg, isNull, err := b.args[idx].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	return arg, false, nil
}

type exportSetFunctionClass struct {
	baseFunctionClass
}

func (c *exportSetFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinExportSetSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinExportSetSig struct {
	baseBuiltinFunc
}

// eval evals a builtinExportSetSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_export-set
func (b *builtinExportSetSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	var (
		bits         uint64
		on           string
		off          string
		separator    = ","
		numberOfBits = 64
	)
	switch len(args) {
	case 5:
		var arg int64
		arg, err = args[4].ToInt64(b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return d, errors.Trace(err)
		}
		if arg >= 0 && arg < 64 {
			numberOfBits = int(arg)
		}
		fallthrough
	case 4:
		separator, err = args[3].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		fallthrough
	case 3:
		arg, err := args[0].ToInt64(b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return d, errors.Trace(err)
		}
		bits = uint64(arg)
		on, err = args[1].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
		off, err = args[2].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
	}
	var result string
	for i := 0; i < numberOfBits; i++ {
		if bits&1 > 0 {
			result += on
		} else {
			result += off
		}
		bits >>= 1
		if i < numberOfBits-1 {
			result += separator
		}
	}
	d.SetString(result)
	return d, nil
}

type formatFunctionClass struct {
	baseFunctionClass
}

func (c *formatFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinFormatSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinFormatSig struct {
	baseBuiltinFunc
}

// eval evals a builtinFormatSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_format
func (b *builtinFormatSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() {
		d.SetNull()
		return
	}
	arg0, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	arg1, err := args[1].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	var arg2 string

	if len(args) == 2 {
		arg2 = "en_US"
	} else if len(args) == 3 {
		arg2, err = args[2].ToString()
		if err != nil {
			return d, errors.Trace(err)
		}
	}

	formatString, err := mysql.GetLocaleFormatFunction(arg2)(arg0, arg1)
	if err != nil {
		return d, errors.Trace(err)
	}

	d.SetString(formatString)
	return d, nil
}

type fromBase64FunctionClass struct {
	baseFunctionClass
}

func (c *fromBase64FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = mysql.MaxBlobWidth
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinFromBase64Sig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinFromBase64Sig struct {
	baseStringBuiltinFunc
}

// evalString evals FROM_BASE64(str).
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_from-base64
func (b *builtinFromBase64Sig) evalString(row []types.Datum) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return "", true, errors.Trace(err)
	}
	str = strings.Replace(str, "\t", "", -1)
	str = strings.Replace(str, " ", "", -1)
	result, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		// When error happens, take `from_base64("asc")` as an example, we should return NULL.
		return "", true, nil
	}
	return string(result), false, nil
}

type toBase64FunctionClass struct {
	baseFunctionClass
}

func (c *toBase64FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = base64NeededEncodedLength(bf.args[0].GetType().Flen)
	sig := &builtinToBase64Sig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinToBase64Sig struct {
	baseStringBuiltinFunc
}

// base64NeededEncodedLength return the base64 encoded string length.
func base64NeededEncodedLength(n int) int {
	// Returns -1 indicate the result will overflow.
	if strconv.IntSize == 64 {
		// len(arg)            -> len(to_base64(arg))
		// 6827690988321067803 -> 9223372036854775804
		// 6827690988321067804 -> -9223372036854775808
		if n > 6827690988321067803 {
			return -1
		}
	} else {
		// len(arg)   -> len(to_base64(arg))
		// 1589695686 -> 2147483645
		// 1589695687 -> -2147483646
		if n > 1589695686 {
			return -1
		}
	}

	length := (n + 2) / 3 * 4
	return length + (length-1)/76
}

// evalString evals a builtinToBase64Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_to-base64
func (b *builtinToBase64Sig) evalString(row []types.Datum) (d string, isNull bool, err error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	str, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return "", isNull, errors.Trace(err)
	}

	if b.tp.Flen == -1 || b.tp.Flen > mysql.MaxBlobWidth {
		return "", true, nil
	}

	//encode
	strBytes := []byte(str)
	result := base64.StdEncoding.EncodeToString(strBytes)
	//A newline is added after each 76 characters of encoded output to divide long output into multiple lines.
	count := len(result)
	if count > 76 {
		resultArr := splitToSubN(result, 76)
		result = strings.Join(resultArr, "\n")
	}

	return result, false, nil
}

// splitToSubN splits a string every n runes into a string[]
func splitToSubN(s string, n int) []string {
	subs := make([]string, 0, len(s)/n+1)
	for len(s) > n {
		subs = append(subs, s[:n])
		s = s[n:]
	}
	subs = append(subs, s)
	return subs
}

type insertFuncFunctionClass struct {
	baseFunctionClass
}

func (c *insertFuncFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinInsertFuncSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinInsertFuncSig struct {
	baseBuiltinFunc
}

// eval evals a builtinInsertFuncSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_insert
func (b *builtinInsertFuncSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}

	// Returns NULL if any argument is NULL.
	if args[0].IsNull() || args[1].IsNull() || args[2].IsNull() || args[3].IsNull() {
		return
	}

	str0, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	str := []rune(str0)
	strLen := len(str)

	posInt64, err := args[1].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	pos := int(posInt64)

	lenInt64, err := args[2].ToInt64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	length := int(lenInt64)

	newstr, err := args[3].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}

	var s string
	if pos < 1 || pos > strLen {
		s = str0
	} else if length > strLen-pos+1 || length < 0 {
		s = string(str[0:pos-1]) + newstr
	} else {
		s = string(str[0:pos-1]) + newstr + string(str[pos+length-1:])
	}

	d.SetString(s)
	return d, nil
}

type instrFunctionClass struct {
	baseFunctionClass
}

func (c *instrFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString, tpString)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 11
	if types.IsBinaryStr(bf.args[0].GetType()) || types.IsBinaryStr(bf.args[1].GetType()) {
		sig := &builtinInstrBinarySig{baseIntBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	}
	sig := &builtinInstrSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinInstrSig struct{ baseIntBuiltinFunc }
type builtinInstrBinarySig struct{ baseIntBuiltinFunc }

// evalInt evals INSTR(str,substr), case insensitive
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_instr
func (b *builtinInstrSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	str, IsNull, err := b.args[0].EvalString(row, sc)
	if IsNull || err != nil {
		return 0, true, errors.Trace(err)
	}
	str = strings.ToLower(str)

	substr, IsNull, err := b.args[1].EvalString(row, sc)
	if IsNull || err != nil {
		return 0, true, errors.Trace(err)
	}
	substr = strings.ToLower(substr)

	idx := strings.Index(str, substr)
	if idx == -1 {
		return 0, false, nil
	}
	return int64(utf8.RuneCountInString(str[:idx]) + 1), false, nil
}

// evalInt evals INSTR(str,substr), case sensitive
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_instr
func (b *builtinInstrBinarySig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	str, IsNull, err := b.args[0].EvalString(row, sc)
	if IsNull || err != nil {
		return 0, true, errors.Trace(err)
	}

	substr, IsNull, err := b.args[1].EvalString(row, sc)
	if IsNull || err != nil {
		return 0, true, errors.Trace(err)
	}

	idx := strings.Index(str, substr)
	if idx == -1 {
		return 0, false, nil
	}
	return int64(idx + 1), false, nil
}

type loadFileFunctionClass struct {
	baseFunctionClass
}

func (c *loadFileFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinLoadFileSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), nil
}

type builtinLoadFileSig struct {
	baseBuiltinFunc
}

// eval evals a builtinLoadFileSig.
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_load-file
func (b *builtinLoadFileSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("load_file")
}
