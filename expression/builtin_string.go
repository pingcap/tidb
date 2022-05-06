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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
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
	_ functionClass = &insertFunctionClass{}
	_ functionClass = &instrFunctionClass{}
	_ functionClass = &loadFileFunctionClass{}
	_ functionClass = &weightStringFunctionClass{}
)

var (
	_ builtinFunc = &builtinLengthSig{}
	_ builtinFunc = &builtinASCIISig{}
	_ builtinFunc = &builtinConcatSig{}
	_ builtinFunc = &builtinConcatWSSig{}
	_ builtinFunc = &builtinLeftSig{}
	_ builtinFunc = &builtinLeftUTF8Sig{}
	_ builtinFunc = &builtinRightSig{}
	_ builtinFunc = &builtinRightUTF8Sig{}
	_ builtinFunc = &builtinRepeatSig{}
	_ builtinFunc = &builtinLowerUTF8Sig{}
	_ builtinFunc = &builtinLowerSig{}
	_ builtinFunc = &builtinReverseUTF8Sig{}
	_ builtinFunc = &builtinReverseSig{}
	_ builtinFunc = &builtinSpaceSig{}
	_ builtinFunc = &builtinUpperUTF8Sig{}
	_ builtinFunc = &builtinUpperSig{}
	_ builtinFunc = &builtinStrcmpSig{}
	_ builtinFunc = &builtinReplaceSig{}
	_ builtinFunc = &builtinConvertSig{}
	_ builtinFunc = &builtinSubstring2ArgsSig{}
	_ builtinFunc = &builtinSubstring3ArgsSig{}
	_ builtinFunc = &builtinSubstring2ArgsUTF8Sig{}
	_ builtinFunc = &builtinSubstring3ArgsUTF8Sig{}
	_ builtinFunc = &builtinSubstringIndexSig{}
	_ builtinFunc = &builtinLocate2ArgsUTF8Sig{}
	_ builtinFunc = &builtinLocate3ArgsUTF8Sig{}
	_ builtinFunc = &builtinLocate2ArgsSig{}
	_ builtinFunc = &builtinLocate3ArgsSig{}
	_ builtinFunc = &builtinHexStrArgSig{}
	_ builtinFunc = &builtinHexIntArgSig{}
	_ builtinFunc = &builtinUnHexSig{}
	_ builtinFunc = &builtinTrim1ArgSig{}
	_ builtinFunc = &builtinTrim2ArgsSig{}
	_ builtinFunc = &builtinTrim3ArgsSig{}
	_ builtinFunc = &builtinLTrimSig{}
	_ builtinFunc = &builtinRTrimSig{}
	_ builtinFunc = &builtinLpadUTF8Sig{}
	_ builtinFunc = &builtinLpadSig{}
	_ builtinFunc = &builtinRpadUTF8Sig{}
	_ builtinFunc = &builtinRpadSig{}
	_ builtinFunc = &builtinBitLengthSig{}
	_ builtinFunc = &builtinCharSig{}
	_ builtinFunc = &builtinCharLengthUTF8Sig{}
	_ builtinFunc = &builtinFindInSetSig{}
	_ builtinFunc = &builtinMakeSetSig{}
	_ builtinFunc = &builtinOctIntSig{}
	_ builtinFunc = &builtinOctStringSig{}
	_ builtinFunc = &builtinOrdSig{}
	_ builtinFunc = &builtinQuoteSig{}
	_ builtinFunc = &builtinBinSig{}
	_ builtinFunc = &builtinEltSig{}
	_ builtinFunc = &builtinExportSet3ArgSig{}
	_ builtinFunc = &builtinExportSet4ArgSig{}
	_ builtinFunc = &builtinExportSet5ArgSig{}
	_ builtinFunc = &builtinFormatWithLocaleSig{}
	_ builtinFunc = &builtinFormatSig{}
	_ builtinFunc = &builtinFromBase64Sig{}
	_ builtinFunc = &builtinToBase64Sig{}
	_ builtinFunc = &builtinInsertSig{}
	_ builtinFunc = &builtinInsertUTF8Sig{}
	_ builtinFunc = &builtinInstrUTF8Sig{}
	_ builtinFunc = &builtinInstrSig{}
	_ builtinFunc = &builtinFieldRealSig{}
	_ builtinFunc = &builtinFieldIntSig{}
	_ builtinFunc = &builtinFieldStringSig{}
	_ builtinFunc = &builtinWeightStringSig{}
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

// SetBinFlagOrBinStr sets resTp to binary string if argTp is a binary string,
// if not, sets the binary flag of resTp to true if argTp has binary flag.
// We need to check if the tp is enum or set, if so, don't add binary flag directly unless it has binary flag.
func SetBinFlagOrBinStr(argTp *types.FieldType, resTp *types.FieldType) {
	nonEnumOrSet := !(argTp.GetType() == mysql.TypeEnum || argTp.GetType() == mysql.TypeSet)
	if types.IsBinaryStr(argTp) {
		types.SetBinChsClnFlag(resTp)
	} else if mysql.HasBinaryFlag(argTp.GetFlag()) || (!types.IsNonBinaryStr(argTp) && nonEnumOrSet) {
		resTp.AddFlag(mysql.BinaryFlag)
	}
}

// addBinFlag add the binary flag to `tp` if its charset is binary
func addBinFlag(tp *types.FieldType) {
	SetBinFlagOrBinStr(tp, tp)
}

type lengthFunctionClass struct {
	baseFunctionClass
}

func (c *lengthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(10)
	sig := &builtinLengthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Length)
	return sig, nil
}

type builtinLengthSig struct {
	baseBuiltinFunc
}

func (b *builtinLengthSig) Clone() builtinFunc {
	newSig := &builtinLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evaluates a builtinLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html
func (b *builtinLengthSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(len([]byte(val))), false, nil
}

type asciiFunctionClass struct {
	baseFunctionClass
}

func (c *asciiFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(3)
	sig := &builtinASCIISig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_ASCII)
	return sig, nil
}

type builtinASCIISig struct {
	baseBuiltinFunc
}

func (b *builtinASCIISig) Clone() builtinFunc {
	newSig := &builtinASCIISig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinASCIISig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ascii
func (b *builtinASCIISig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if len(val) == 0 {
		return 0, false, nil
	}
	return int64(val[0]), false, nil
}

type concatFunctionClass struct {
	baseFunctionClass
}

func (c *concatFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args); i++ {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	addBinFlag(bf.tp)
	bf.tp.SetFlen(0)
	for i := range args {
		argType := args[i].GetType()

		if argType.GetFlen() < 0 {
			bf.tp.SetFlen(mysql.MaxBlobWidth)
			logutil.BgLogger().Debug("unexpected `Flen` value(-1) in CONCAT's args", zap.Int("arg's index", i))
		}
		bf.tp.SetFlen(bf.tp.GetFlen() + argType.GetFlen())
	}
	if bf.tp.GetFlen() >= mysql.MaxBlobWidth {
		bf.tp.SetFlen(mysql.MaxBlobWidth)
	}

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sig := &builtinConcatSig{bf, maxAllowedPacket}
	sig.setPbCode(tipb.ScalarFuncSig_Concat)
	return sig, nil
}

type builtinConcatSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinConcatSig) Clone() builtinFunc {
	newSig := &builtinConcatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a builtinConcatSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat
func (b *builtinConcatSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	var s []byte
	for _, a := range b.getArgs() {
		d, isNull, err = a.EvalString(b.ctx, row)
		if isNull || err != nil {
			return d, isNull, err
		}
		if uint64(len(s)+len(d)) > b.maxAllowedPacket {
			return "", true, handleAllowedPacketOverflowed(b.ctx, "concat", b.maxAllowedPacket)
		}
		s = append(s, []byte(d)...)
	}
	return string(s), false, nil
}

type concatWSFunctionClass struct {
	baseFunctionClass
}

func (c *concatWSFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args); i++ {
		argTps = append(argTps, types.ETString)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(0)

	addBinFlag(bf.tp)
	for i := range args {
		argType := args[i].GetType()

		// skip separator param
		if i != 0 {
			if argType.GetFlen() < 0 {
				bf.tp.SetFlen(mysql.MaxBlobWidth)
				logutil.BgLogger().Debug("unexpected `Flen` value(-1) in CONCAT_WS's args", zap.Int("arg's index", i))
			}
			bf.tp.SetFlen(bf.tp.GetFlen() + argType.GetFlen())
		}
	}

	// add separator
	sepsLen := len(args) - 2
	bf.tp.SetFlen(bf.tp.GetFlen() + sepsLen*args[0].GetType().GetFlen())

	if bf.tp.GetFlen() >= mysql.MaxBlobWidth {
		bf.tp.SetFlen(mysql.MaxBlobWidth)
	}

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sig := &builtinConcatWSSig{bf, maxAllowedPacket}
	sig.setPbCode(tipb.ScalarFuncSig_ConcatWS)
	return sig, nil
}

type builtinConcatWSSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinConcatWSSig) Clone() builtinFunc {
	newSig := &builtinConcatWSSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a CONCAT_WS(separator,str1,str2,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_concat-ws
func (b *builtinConcatWSSig) evalString(row chunk.Row) (string, bool, error) {
	args := b.getArgs()
	strs := make([]string, 0, len(args))
	var sep string
	var targetLength int

	N := len(args)
	if N > 0 {
		val, isNull, err := args[0].EvalString(b.ctx, row)
		if err != nil || isNull {
			// If the separator is NULL, the result is NULL.
			return val, isNull, err
		}
		sep = val
	}
	for i := 1; i < N; i++ {
		val, isNull, err := args[i].EvalString(b.ctx, row)
		if err != nil {
			return val, isNull, err
		}
		if isNull {
			// CONCAT_WS() does not skip empty strings. However,
			// it does skip any NULL values after the separator argument.
			continue
		}

		targetLength += len(val)
		if i > 1 {
			targetLength += len(sep)
		}
		if uint64(targetLength) > b.maxAllowedPacket {
			return "", true, handleAllowedPacketOverflowed(b.ctx, "concat_ws", b.maxAllowedPacket)
		}
		strs = append(strs, val)
	}

	str := strings.Join(strs, sep)
	// todo check whether the length of result is larger than flen
	// if b.tp.flen != types.UnspecifiedLength && len(str) > b.tp.flen {
	//	return "", true, nil
	// }
	return str, false, nil
}

type leftFunctionClass struct {
	baseFunctionClass
}

func (c *leftFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType()
	bf.tp.SetFlen(argType.GetFlen())
	SetBinFlagOrBinStr(argType, bf.tp)
	if types.IsBinaryStr(argType) {
		sig := &builtinLeftSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Left)
		return sig, nil
	}
	sig := &builtinLeftUTF8Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LeftUTF8)
	return sig, nil
}

type builtinLeftSig struct {
	baseBuiltinFunc
}

func (b *builtinLeftSig) Clone() builtinFunc {
	newSig := &builtinLeftSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals LEFT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func (b *builtinLeftSig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	left, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	leftLength := int(left)
	if strLength := len(str); leftLength > strLength {
		leftLength = strLength
	} else if leftLength < 0 {
		leftLength = 0
	}
	return str[:leftLength], false, nil
}

type builtinLeftUTF8Sig struct {
	baseBuiltinFunc
}

func (b *builtinLeftUTF8Sig) Clone() builtinFunc {
	newSig := &builtinLeftUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals LEFT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func (b *builtinLeftUTF8Sig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	left, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
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

func (c *rightFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType()
	bf.tp.SetFlen(argType.GetFlen())
	SetBinFlagOrBinStr(argType, bf.tp)
	if types.IsBinaryStr(argType) {
		sig := &builtinRightSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Right)
		return sig, nil
	}
	sig := &builtinRightUTF8Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_RightUTF8)
	return sig, nil
}

type builtinRightSig struct {
	baseBuiltinFunc
}

func (b *builtinRightSig) Clone() builtinFunc {
	newSig := &builtinRightSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals RIGHT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_right
func (b *builtinRightSig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	right, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	strLength, rightLength := len(str), int(right)
	if rightLength > strLength {
		rightLength = strLength
	} else if rightLength < 0 {
		rightLength = 0
	}
	return str[strLength-rightLength:], false, nil
}

type builtinRightUTF8Sig struct {
	baseBuiltinFunc
}

func (b *builtinRightUTF8Sig) Clone() builtinFunc {
	newSig := &builtinRightUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals RIGHT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_right
func (b *builtinRightUTF8Sig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	right, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
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

func (c *repeatFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(mysql.MaxBlobWidth)
	SetBinFlagOrBinStr(args[0].GetType(), bf.tp)
	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinRepeatSig{bf, maxAllowedPacket}
	sig.setPbCode(tipb.ScalarFuncSig_Repeat)
	return sig, nil
}

type builtinRepeatSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinRepeatSig) Clone() builtinFunc {
	newSig := &builtinRepeatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a builtinRepeatSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_repeat
func (b *builtinRepeatSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	byteLength := len(str)

	num, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	if num < 1 {
		return "", false, nil
	}
	if num > math.MaxInt32 {
		num = math.MaxInt32
	}

	if uint64(byteLength)*uint64(num) > b.maxAllowedPacket {
		return "", true, handleAllowedPacketOverflowed(b.ctx, "repeat", b.maxAllowedPacket)
	}

	return strings.Repeat(str, int(num)), false, nil
}

type lowerFunctionClass struct {
	baseFunctionClass
}

func (c *lowerFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	argTp := args[0].GetType()
	bf.tp.SetFlen(argTp.GetFlen())
	SetBinFlagOrBinStr(argTp, bf.tp)
	var sig builtinFunc
	if types.IsBinaryStr(argTp) {
		sig = &builtinLowerSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Lower)
	} else {
		sig = &builtinLowerUTF8Sig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_LowerUTF8)
	}
	return sig, nil
}

type builtinLowerUTF8Sig struct {
	baseBuiltinFunc
}

func (b *builtinLowerUTF8Sig) Clone() builtinFunc {
	newSig := &builtinLowerUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLowerUTF8Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lower
func (b *builtinLowerUTF8Sig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	enc := charset.FindEncoding(b.args[0].GetType().GetCharset())
	return enc.ToLower(d), false, nil
}

type builtinLowerSig struct {
	baseBuiltinFunc
}

func (b *builtinLowerSig) Clone() builtinFunc {
	newSig := &builtinLowerSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLowerSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lower
func (b *builtinLowerSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	return d, false, nil
}

type reverseFunctionClass struct {
	baseFunctionClass
}

func (c *reverseFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}

	argTp := args[0].GetType()
	bf.tp.SetFlen(args[0].GetType().GetFlen())
	addBinFlag(bf.tp)
	var sig builtinFunc
	if types.IsBinaryStr(argTp) {
		sig = &builtinReverseSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Reverse)
	} else {
		sig = &builtinReverseUTF8Sig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ReverseUTF8)
	}
	return sig, nil
}

type builtinReverseSig struct {
	baseBuiltinFunc
}

func (b *builtinReverseSig) Clone() builtinFunc {
	newSig := &builtinReverseSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a REVERSE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseSig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	reversed := reverseBytes([]byte(str))
	return string(reversed), false, nil
}

type builtinReverseUTF8Sig struct {
	baseBuiltinFunc
}

func (b *builtinReverseUTF8Sig) Clone() builtinFunc {
	newSig := &builtinReverseUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a REVERSE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseUTF8Sig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	reversed := reverseRunes([]rune(str))
	return string(reversed), false, nil
}

type spaceFunctionClass struct {
	baseFunctionClass
}

func (c *spaceFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetSessionVars().GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(mysql.MaxBlobWidth)
	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinSpaceSig{bf, maxAllowedPacket}
	sig.setPbCode(tipb.ScalarFuncSig_Space)
	return sig, nil
}

type builtinSpaceSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinSpaceSig) Clone() builtinFunc {
	newSig := &builtinSpaceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a builtinSpaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_space
func (b *builtinSpaceSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	var x int64

	x, isNull, err = b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	if x < 0 {
		x = 0
	}
	if uint64(x) > b.maxAllowedPacket {
		return d, true, handleAllowedPacketOverflowed(b.ctx, "space", b.maxAllowedPacket)
	}
	if x > mysql.MaxBlobWidth {
		return d, true, nil
	}
	return strings.Repeat(" ", int(x)), false, nil
}

type upperFunctionClass struct {
	baseFunctionClass
}

func (c *upperFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	argTp := args[0].GetType()
	bf.tp.SetFlen(argTp.GetFlen())
	SetBinFlagOrBinStr(argTp, bf.tp)
	var sig builtinFunc
	if types.IsBinaryStr(argTp) {
		sig = &builtinUpperSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Upper)
	} else {
		sig = &builtinUpperUTF8Sig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UpperUTF8)
	}
	return sig, nil
}

type builtinUpperUTF8Sig struct {
	baseBuiltinFunc
}

func (b *builtinUpperUTF8Sig) Clone() builtinFunc {
	newSig := &builtinUpperUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUpperUTF8Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func (b *builtinUpperUTF8Sig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	enc := charset.FindEncoding(b.args[0].GetType().GetCharset())
	return enc.ToUpper(d), false, nil
}

type builtinUpperSig struct {
	baseBuiltinFunc
}

func (b *builtinUpperSig) Clone() builtinFunc {
	newSig := &builtinUpperSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUpperSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func (b *builtinUpperSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	return d, false, nil
}

type strcmpFunctionClass struct {
	baseFunctionClass
}

func (c *strcmpFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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
}

func (b *builtinStrcmpSig) Clone() builtinFunc {
	newSig := &builtinStrcmpSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinStrcmpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html
func (b *builtinStrcmpSig) evalInt(row chunk.Row) (int64, bool, error) {
	var (
		left, right string
		isNull      bool
		err         error
	)

	left, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	right, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	res := types.CompareString(left, right, b.collation)
	return int64(res), false, nil
}

type replaceFunctionClass struct {
	baseFunctionClass
}

func (c *replaceFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(c.fixLength(args))
	for _, a := range args {
		SetBinFlagOrBinStr(a.GetType(), bf.tp)
	}
	sig := &builtinReplaceSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Replace)
	return sig, nil
}

// fixLength calculate the flen of the return type.
func (c *replaceFunctionClass) fixLength(args []Expression) int {
	charLen := args[0].GetType().GetFlen()
	oldStrLen := args[1].GetType().GetFlen()
	diff := args[2].GetType().GetFlen() - oldStrLen
	if diff > 0 && oldStrLen > 0 {
		charLen += (charLen / oldStrLen) * diff
	}
	return charLen
}

type builtinReplaceSig struct {
	baseBuiltinFunc
}

func (b *builtinReplaceSig) Clone() builtinFunc {
	newSig := &builtinReplaceSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinReplaceSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_replace
func (b *builtinReplaceSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	var str, oldStr, newStr string

	str, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	oldStr, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	newStr, isNull, err = b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	if oldStr == "" {
		return str, false, nil
	}
	return strings.Replace(str, oldStr, newStr, -1), false, nil
}

type convertFunctionClass struct {
	baseFunctionClass
}

func (c *convertFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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
}

func (b *builtinConvertSig) Clone() builtinFunc {
	newSig := &builtinConvertSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals CONVERT(expr USING transcoding_name).
// Syntax CONVERT(expr, type) is parsed as cast expr so not handled here.
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
func (b *builtinConvertSig) evalString(row chunk.Row) (string, bool, error) {
	expr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	argTp, resultTp := b.args[0].GetType(), b.tp
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

func (c *substringFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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

	argType := args[0].GetType()
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
}

func (b *builtinSubstring2ArgsSig) Clone() builtinFunc {
	newSig := &builtinSubstring2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring2ArgsSig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
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
}

func (b *builtinSubstring2ArgsUTF8Sig) Clone() builtinFunc {
	newSig := &builtinSubstring2ArgsUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos), SUBSTR(str FROM pos), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring2ArgsUTF8Sig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
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

type builtinSubstring3ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinSubstring3ArgsSig) Clone() builtinFunc {
	newSig := &builtinSubstring3ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsSig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	length, isNull, err := b.args[2].EvalInt(b.ctx, row)
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
}

func (b *builtinSubstring3ArgsUTF8Sig) Clone() builtinFunc {
	newSig := &builtinSubstring3ArgsUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SUBSTR(str,pos,len), SUBSTR(str FROM pos FOR len), SUBSTR() is a synonym for SUBSTRING().
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substr
func (b *builtinSubstring3ArgsUTF8Sig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	length, isNull, err := b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
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

func (c *substringIndexFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType()
	bf.tp.SetFlen(argType.GetFlen())
	SetBinFlagOrBinStr(argType, bf.tp)
	sig := &builtinSubstringIndexSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_SubstringIndex)
	return sig, nil
}

type builtinSubstringIndexSig struct {
	baseBuiltinFunc
}

func (b *builtinSubstringIndexSig) Clone() builtinFunc {
	newSig := &builtinSubstringIndexSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinSubstringIndexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
func (b *builtinSubstringIndexSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	var (
		str, delim string
		count      int64
	)
	str, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	delim, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	count, isNull, err = b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	if len(delim) == 0 {
		return "", false, nil
	}
	// when count > MaxInt64, returns whole string.
	if count < 0 && mysql.HasUnsignedFlag(b.args[2].GetType().GetFlag()) {
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

func (c *locateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
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
}

func (b *builtinLocate2ArgsSig) Clone() builtinFunc {
	newSig := &builtinLocate2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str), case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate2ArgsSig) evalInt(row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.args[1].EvalString(b.ctx, row)
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
}

func (b *builtinLocate2ArgsUTF8Sig) Clone() builtinFunc {
	newSig := &builtinLocate2ArgsUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate2ArgsUTF8Sig) evalInt(row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if int64(len([]rune(subStr))) == 0 {
		return 1, false, nil
	}

	return locateStringWithCollation(str, subStr, b.collation), false, nil
}

type builtinLocate3ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinLocate3ArgsSig) Clone() builtinFunc {
	newSig := &builtinLocate3ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str,pos), case-sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate3ArgsSig) evalInt(row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	pos, isNull, err := b.args[2].EvalInt(b.ctx, row)
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
}

func (b *builtinLocate3ArgsUTF8Sig) Clone() builtinFunc {
	newSig := &builtinLocate3ArgsUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals LOCATE(substr,str,pos).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
func (b *builtinLocate3ArgsUTF8Sig) evalInt(row chunk.Row) (int64, bool, error) {
	subStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	str, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if collate.IsCICollation(b.collation) {
		subStr = strings.ToLower(subStr)
		str = strings.ToLower(str)
	}
	pos, isNull, err := b.args[2].EvalInt(b.ctx, row)
	// Transfer the argument which starts from 1 to real index which starts from 0.
	pos--
	if isNull || err != nil {
		return 0, isNull, err
	}
	subStrLen := len([]rune(subStr))
	if pos < 0 || pos > int64(len([]rune(str))-subStrLen) {
		return 0, false, nil
	} else if subStrLen == 0 {
		return pos + 1, false, nil
	}
	slice := string([]rune(str)[pos:])

	idx := locateStringWithCollation(slice, subStr, b.collation)
	if idx != 0 {
		return pos + idx, false, nil
	}
	return 0, false, nil
}

type hexFunctionClass struct {
	baseFunctionClass
}

func (c *hexFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	argTp := args[0].GetType().EvalType()
	switch argTp {
	case types.ETString, types.ETDatetime, types.ETTimestamp, types.ETDuration, types.ETJson:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
		if err != nil {
			return nil, err
		}
		argFieldTp := args[0].GetType()
		// Use UTF8MB4 as default.
		bf.tp.SetFlen(argFieldTp.GetFlen() * 4 * 2)
		sig := &builtinHexStrArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_HexStrArg)
		return sig, nil
	case types.ETInt, types.ETReal, types.ETDecimal:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
		if err != nil {
			return nil, err
		}
		bf.tp.SetFlen(args[0].GetType().GetFlen() * 2)
		charset, collate := ctx.GetSessionVars().GetCharsetInfo()
		bf.tp.SetCharset(charset)
		bf.tp.SetCollate(collate)
		sig := &builtinHexIntArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_HexIntArg)
		return sig, nil
	default:
		return nil, errors.Errorf("Hex invalid args, need int or string but get %T", args[0].GetType())
	}
}

type builtinHexStrArgSig struct {
	baseBuiltinFunc
}

func (b *builtinHexStrArgSig) Clone() builtinFunc {
	newSig := &builtinHexStrArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinHexStrArgSig, corresponding to hex(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexStrArgSig) evalString(row chunk.Row) (string, bool, error) {
	d, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	return strings.ToUpper(hex.EncodeToString(hack.Slice(d))), false, nil
}

type builtinHexIntArgSig struct {
	baseBuiltinFunc
}

func (b *builtinHexIntArgSig) Clone() builtinFunc {
	newSig := &builtinHexIntArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinHexIntArgSig, corresponding to hex(N)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_hex
func (b *builtinHexIntArgSig) evalString(row chunk.Row) (string, bool, error) {
	x, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	return strings.ToUpper(fmt.Sprintf("%x", uint64(x))), false, nil
}

type unhexFunctionClass struct {
	baseFunctionClass
}

func (c *unhexFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var retFlen int

	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argType := args[0].GetType()
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
}

func (b *builtinUnHexSig) Clone() builtinFunc {
	newSig := &builtinUnHexSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUnHexSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_unhex
func (b *builtinUnHexSig) evalString(row chunk.Row) (string, bool, error) {
	var bs []byte

	d, isNull, err := b.args[0].EvalString(b.ctx, row)
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
// but we wil convert it into trim(str), trim(str, remstr) and trim(str, remstr, direction) in AST.
func (c *trimFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	switch len(args) {
	case 1:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
		if err != nil {
			return nil, err
		}
		argType := args[0].GetType()
		bf.tp.SetFlen(argType.GetFlen())
		SetBinFlagOrBinStr(argType, bf.tp)
		sig := &builtinTrim1ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Trim1Arg)
		return sig, nil

	case 2:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
		if err != nil {
			return nil, err
		}
		argType := args[0].GetType()
		SetBinFlagOrBinStr(argType, bf.tp)
		sig := &builtinTrim2ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Trim2Args)
		return sig, nil

	case 3:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString, types.ETInt)
		if err != nil {
			return nil, err
		}
		argType := args[0].GetType()
		bf.tp.SetFlen(argType.GetFlen())
		SetBinFlagOrBinStr(argType, bf.tp)
		sig := &builtinTrim3ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Trim3Args)
		return sig, nil

	default:
		return nil, c.verifyArgs(args)
	}
}

type builtinTrim1ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinTrim1ArgSig) Clone() builtinFunc {
	newSig := &builtinTrim1ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTrim1ArgSig, corresponding to trim(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim1ArgSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	return strings.Trim(d, spaceChars), false, nil
}

type builtinTrim2ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinTrim2ArgsSig) Clone() builtinFunc {
	newSig := &builtinTrim2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTrim2ArgsSig, corresponding to trim(str, remstr)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim2ArgsSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	var str, remstr string

	str, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	remstr, isNull, err = b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	d = trimLeft(str, remstr)
	d = trimRight(d, remstr)
	return d, false, nil
}

type builtinTrim3ArgsSig struct {
	baseBuiltinFunc
}

func (b *builtinTrim3ArgsSig) Clone() builtinFunc {
	newSig := &builtinTrim3ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTrim3ArgsSig, corresponding to trim(str, remstr, direction)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim3ArgsSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	var (
		str, remstr  string
		x            int64
		direction    ast.TrimDirectionType
		isRemStrNull bool
	)
	str, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	remstr, isRemStrNull, err = b.args[1].EvalString(b.ctx, row)
	if err != nil || isRemStrNull {
		return d, isRemStrNull, err
	}
	x, isNull, err = b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	direction = ast.TrimDirectionType(x)
	switch direction {
	case ast.TrimLeading:
		d = trimLeft(str, remstr)
	case ast.TrimTrailing:
		d = trimRight(str, remstr)
	default:
		d = trimLeft(str, remstr)
		d = trimRight(d, remstr)

	}
	return d, false, nil
}

type lTrimFunctionClass struct {
	baseFunctionClass
}

func (c *lTrimFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType()
	bf.tp.SetFlen(argType.GetFlen())
	SetBinFlagOrBinStr(argType, bf.tp)
	sig := &builtinLTrimSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LTrim)
	return sig, nil
}

type builtinLTrimSig struct {
	baseBuiltinFunc
}

func (b *builtinLTrimSig) Clone() builtinFunc {
	newSig := &builtinLTrimSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ltrim
func (b *builtinLTrimSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	return strings.TrimLeft(d, spaceChars), false, nil
}

type rTrimFunctionClass struct {
	baseFunctionClass
}

func (c *rTrimFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType()
	bf.tp.SetFlen(argType.GetFlen())
	SetBinFlagOrBinStr(argType, bf.tp)
	sig := &builtinRTrimSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_RTrim)
	return sig, nil
}

type builtinRTrimSig struct {
	baseBuiltinFunc
}

func (b *builtinRTrimSig) Clone() builtinFunc {
	newSig := &builtinRTrimSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinRTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rtrim
func (b *builtinRTrimSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
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

func getFlen4LpadAndRpad(ctx sessionctx.Context, arg Expression) int {
	if constant, ok := arg.(*Constant); ok {
		length, isNull, err := constant.EvalInt(ctx, chunk.Row{})
		if err != nil {
			logutil.BgLogger().Error("eval `Flen` for LPAD/RPAD", zap.Error(err))
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

func (c *lpadFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(getFlen4LpadAndRpad(bf.ctx, args[1]))
	addBinFlag(bf.tp)

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if types.IsBinaryStr(args[0].GetType()) || types.IsBinaryStr(args[2].GetType()) {
		sig := &builtinLpadSig{bf, maxAllowedPacket}
		sig.setPbCode(tipb.ScalarFuncSig_Lpad)
		return sig, nil
	}
	if bf.tp.SetFlen(bf.tp.GetFlen() * 4); bf.tp.GetFlen() > mysql.MaxBlobWidth {
		bf.tp.SetFlen(mysql.MaxBlobWidth)
	}
	sig := &builtinLpadUTF8Sig{bf, maxAllowedPacket}
	sig.setPbCode(tipb.ScalarFuncSig_LpadUTF8)
	return sig, nil
}

type builtinLpadSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinLpadSig) Clone() builtinFunc {
	newSig := &builtinLpadSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals LPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lpad
func (b *builtinLpadSig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	byteLength := len(str)

	length, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	targetLength := int(length)

	if uint64(targetLength) > b.maxAllowedPacket {
		return "", true, handleAllowedPacketOverflowed(b.ctx, "lpad", b.maxAllowedPacket)
	}

	padStr, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	padLength := len(padStr)

	if targetLength < 0 || targetLength > b.tp.GetFlen() || (byteLength < targetLength && padLength == 0) {
		return "", true, nil
	}

	if tailLen := targetLength - byteLength; tailLen > 0 {
		repeatCount := tailLen/padLength + 1
		str = strings.Repeat(padStr, repeatCount)[:tailLen] + str
	}
	return str[:targetLength], false, nil
}

type builtinLpadUTF8Sig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinLpadUTF8Sig) Clone() builtinFunc {
	newSig := &builtinLpadUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals LPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lpad
func (b *builtinLpadUTF8Sig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runeLength := len([]rune(str))

	length, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	targetLength := int(length)

	if uint64(targetLength)*uint64(mysql.MaxBytesOfCharacter) > b.maxAllowedPacket {
		return "", true, handleAllowedPacketOverflowed(b.ctx, "lpad", b.maxAllowedPacket)
	}

	padStr, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	padLength := len([]rune(padStr))

	if targetLength < 0 || targetLength*4 > b.tp.GetFlen() || (runeLength < targetLength && padLength == 0) {
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

func (c *rpadFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(getFlen4LpadAndRpad(bf.ctx, args[1]))
	addBinFlag(bf.tp)

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if types.IsBinaryStr(args[0].GetType()) || types.IsBinaryStr(args[2].GetType()) {
		sig := &builtinRpadSig{bf, maxAllowedPacket}
		sig.setPbCode(tipb.ScalarFuncSig_Rpad)
		return sig, nil
	}
	if bf.tp.SetFlen(bf.tp.GetFlen() * 4); bf.tp.GetFlen() > mysql.MaxBlobWidth {
		bf.tp.SetFlen(mysql.MaxBlobWidth)
	}
	sig := &builtinRpadUTF8Sig{bf, maxAllowedPacket}
	sig.setPbCode(tipb.ScalarFuncSig_RpadUTF8)
	return sig, nil
}

type builtinRpadSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinRpadSig) Clone() builtinFunc {
	newSig := &builtinRpadSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals RPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadSig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	byteLength := len(str)

	length, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	targetLength := int(length)
	if uint64(targetLength) > b.maxAllowedPacket {
		return "", true, handleAllowedPacketOverflowed(b.ctx, "rpad", b.maxAllowedPacket)
	}

	padStr, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	padLength := len(padStr)

	if targetLength < 0 || targetLength > b.tp.GetFlen() || (byteLength < targetLength && padLength == 0) {
		return "", true, nil
	}

	if tailLen := targetLength - byteLength; tailLen > 0 {
		repeatCount := tailLen/padLength + 1
		str = str + strings.Repeat(padStr, repeatCount)
	}
	return str[:targetLength], false, nil
}

type builtinRpadUTF8Sig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinRpadUTF8Sig) Clone() builtinFunc {
	newSig := &builtinRpadUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals RPAD(str,len,padstr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rpad
func (b *builtinRpadUTF8Sig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runeLength := len([]rune(str))

	length, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	targetLength := int(length)

	if uint64(targetLength)*uint64(mysql.MaxBytesOfCharacter) > b.maxAllowedPacket {
		return "", true, handleAllowedPacketOverflowed(b.ctx, "rpad", b.maxAllowedPacket)
	}

	padStr, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	padLength := len([]rune(padStr))

	if targetLength < 0 || targetLength*4 > b.tp.GetFlen() || (runeLength < targetLength && padLength == 0) {
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

func (c *bitLengthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(10)
	sig := &builtinBitLengthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_BitLength)
	return sig, nil
}

type builtinBitLengthSig struct {
	baseBuiltinFunc
}

func (b *builtinBitLengthSig) Clone() builtinFunc {
	newSig := &builtinBitLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evaluates a builtinBitLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_bit-length
func (b *builtinBitLengthSig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(len(val) * 8), false, nil
}

type charFunctionClass struct {
	baseFunctionClass
}

func (c *charFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for i := 0; i < len(args)-1; i++ {
		argTps = append(argTps, types.ETInt)
	}
	argTps = append(argTps, types.ETString)
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	// The last argument represents the charset name after "using".
	if _, ok := args[len(args)-1].(*Constant); !ok {
		// If we got there, there must be something wrong in other places.
		logutil.BgLogger().Warn(fmt.Sprintf("The last argument in char function must be constant, but got %T", args[len(args)-1]))
		return nil, errIncorrectArgs
	}
	charsetName, isNull, err := args[len(args)-1].EvalString(ctx, chunk.Row{})
	if err != nil {
		return nil, err
	}
	if isNull {
		// Use the default charset binary if it is nil.
		bf.tp.SetCharset(charset.CharsetBin)
		bf.tp.SetCollate(charset.CollationBin)
		bf.tp.AddFlag(mysql.BinaryFlag)
	} else {
		bf.tp.SetCharset(charsetName)
		defaultCollate, err := charset.GetDefaultCollation(charsetName)
		if err != nil {
			return nil, err
		}
		bf.tp.SetCollate(defaultCollate)
	}
	bf.tp.SetFlen(4 * (len(args) - 1))

	sig := &builtinCharSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Char)
	return sig, nil
}

type builtinCharSig struct {
	baseBuiltinFunc
}

func (b *builtinCharSig) Clone() builtinFunc {
	newSig := &builtinCharSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
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
func (b *builtinCharSig) evalString(row chunk.Row) (string, bool, error) {
	bigints := make([]int64, 0, len(b.args)-1)

	for i := 0; i < len(b.args)-1; i++ {
		val, IsNull, err := b.args[i].EvalInt(b.ctx, row)
		if err != nil {
			return "", true, err
		}
		if IsNull {
			continue
		}
		bigints = append(bigints, val)
	}

	dBytes := b.convertToBytes(bigints)
	enc := charset.FindEncoding(b.tp.GetCharset())
	res, err := enc.Transform(nil, dBytes, charset.OpDecode)
	if err != nil {
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(err)
		if b.ctx.GetSessionVars().StrictSQLMode {
			return "", true, nil
		}
	}
	return string(res), false, nil
}

type charLengthFunctionClass struct {
	baseFunctionClass
}

func (c *charLengthFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if argsErr := c.verifyArgs(args); argsErr != nil {
		return nil, argsErr
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	if types.IsBinaryStr(args[0].GetType()) {
		sig := &builtinCharLengthBinarySig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CharLength)
		return sig, nil
	}
	sig := &builtinCharLengthUTF8Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_CharLengthUTF8)
	return sig, nil
}

type builtinCharLengthBinarySig struct {
	baseBuiltinFunc
}

func (b *builtinCharLengthBinarySig) Clone() builtinFunc {
	newSig := &builtinCharLengthBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCharLengthUTF8Sig for binary string type.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char-length
func (b *builtinCharLengthBinarySig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(len(val)), false, nil
}

type builtinCharLengthUTF8Sig struct {
	baseBuiltinFunc
}

func (b *builtinCharLengthUTF8Sig) Clone() builtinFunc {
	newSig := &builtinCharLengthUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCharLengthUTF8Sig for non-binary string type.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char-length
func (b *builtinCharLengthUTF8Sig) evalInt(row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(len([]rune(val))), false, nil
}

type findInSetFunctionClass struct {
	baseFunctionClass
}

func (c *findInSetFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(3)
	sig := &builtinFindInSetSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_FindInSet)
	return sig, nil
}

type builtinFindInSetSig struct {
	baseBuiltinFunc
}

func (b *builtinFindInSetSig) Clone() builtinFunc {
	newSig := &builtinFindInSetSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals FIND_IN_SET(str,strlist).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_find-in-set
// TODO: This function can be optimized by using bit arithmetic when the first argument is
// a constant string and the second is a column of type SET.
func (b *builtinFindInSetSig) evalInt(row chunk.Row) (int64, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	strlist, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if len(strlist) == 0 {
		return 0, false, nil
	}

	for i, strInSet := range strings.Split(strlist, ",") {
		if b.ctor.Compare(str, strInSet) == 0 {
			return int64(i + 1), false, nil
		}
	}
	return 0, false, nil
}

type fieldFunctionClass struct {
	baseFunctionClass
}

func (c *fieldFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	isAllString, isAllNumber := true, true
	for i, length := 0, len(args); i < length; i++ {
		argTp := args[i].GetType().EvalType()
		isAllString = isAllString && (argTp == types.ETString)
		isAllNumber = isAllNumber && (argTp == types.ETInt)
	}

	argTps := make([]types.EvalType, len(args))
	argTp := types.ETReal
	if isAllString {
		argTp = types.ETString
	} else if isAllNumber {
		argTp = types.ETInt
	}
	for i, length := 0, len(args); i < length; i++ {
		argTps[i] = argTp
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, argTps...)
	if err != nil {
		return nil, err
	}
	var sig builtinFunc
	switch argTp {
	case types.ETReal:
		sig = &builtinFieldRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_FieldReal)
	case types.ETInt:
		sig = &builtinFieldIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_FieldInt)
	case types.ETString:
		sig = &builtinFieldStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_FieldString)
	}
	return sig, nil
}

type builtinFieldIntSig struct {
	baseBuiltinFunc
}

func (b *builtinFieldIntSig) Clone() builtinFunc {
	newSig := &builtinFieldIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals FIELD(str,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
func (b *builtinFieldIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	str, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return 0, err != nil, err
	}
	for i, length := 1, len(b.args); i < length; i++ {
		stri, isNull, err := b.args[i].EvalInt(b.ctx, row)
		if err != nil {
			return 0, true, err
		}
		if !isNull && str == stri {
			return int64(i), false, nil
		}
	}
	return 0, false, nil
}

type builtinFieldRealSig struct {
	baseBuiltinFunc
}

func (b *builtinFieldRealSig) Clone() builtinFunc {
	newSig := &builtinFieldRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals FIELD(str,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
func (b *builtinFieldRealSig) evalInt(row chunk.Row) (int64, bool, error) {
	str, isNull, err := b.args[0].EvalReal(b.ctx, row)
	if isNull || err != nil {
		return 0, err != nil, err
	}
	for i, length := 1, len(b.args); i < length; i++ {
		stri, isNull, err := b.args[i].EvalReal(b.ctx, row)
		if err != nil {
			return 0, true, err
		}
		if !isNull && str == stri {
			return int64(i), false, nil
		}
	}
	return 0, false, nil
}

type builtinFieldStringSig struct {
	baseBuiltinFunc
}

func (b *builtinFieldStringSig) Clone() builtinFunc {
	newSig := &builtinFieldStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals FIELD(str,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
func (b *builtinFieldStringSig) evalInt(row chunk.Row) (int64, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, err != nil, err
	}
	for i, length := 1, len(b.args); i < length; i++ {
		stri, isNull, err := b.args[i].EvalString(b.ctx, row)
		if err != nil {
			return 0, true, err
		}
		if !isNull && b.ctor.Compare(str, stri) == 0 {
			return int64(i), false, nil
		}
	}
	return 0, false, nil
}

type makeSetFunctionClass struct {
	baseFunctionClass
}

func (c *makeSetFunctionClass) getFlen(ctx sessionctx.Context, args []Expression) int {
	flen, count := 0, 0
	if constant, ok := args[0].(*Constant); ok {
		bits, isNull, err := constant.EvalInt(ctx, chunk.Row{})
		if err == nil && !isNull {
			for i, length := 1, len(args); i < length; i++ {
				if (bits & (1 << uint(i-1))) != 0 {
					flen += args[i].GetType().GetFlen()
					count++
				}
			}
			if count > 0 {
				flen += count - 1
			}
			return flen
		}
	}
	for i, length := 1, len(args); i < length; i++ {
		flen += args[i].GetType().GetFlen()
	}
	return flen + len(args) - 1 - 1
}

func (c *makeSetFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, len(args))
	argTps[0] = types.ETInt
	for i, length := 1, len(args); i < length; i++ {
		argTps[i] = types.ETString
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	addBinFlag(bf.tp)
	bf.tp.SetFlen(c.getFlen(bf.ctx, args))
	if bf.tp.GetFlen() > mysql.MaxBlobWidth {
		bf.tp.SetFlen(mysql.MaxBlobWidth)
	}
	sig := &builtinMakeSetSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_MakeSet)
	return sig, nil
}

type builtinMakeSetSig struct {
	baseBuiltinFunc
}

func (b *builtinMakeSetSig) Clone() builtinFunc {
	newSig := &builtinMakeSetSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals MAKE_SET(bits,str1,str2,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_make-set
func (b *builtinMakeSetSig) evalString(row chunk.Row) (string, bool, error) {
	bits, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	sets := make([]string, 0, len(b.args)-1)
	for i, length := 1, len(b.args); i < length; i++ {
		if (bits & (1 << uint(i-1))) == 0 {
			continue
		}
		str, isNull, err := b.args[i].EvalString(b.ctx, row)
		if err != nil {
			return "", true, err
		}
		if !isNull {
			sets = append(sets, str)
		}
	}

	return strings.Join(sets, ","), false, nil
}

type octFunctionClass struct {
	baseFunctionClass
}

func (c *octFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var sig builtinFunc
	if IsBinaryLiteral(args[0]) || args[0].GetType().EvalType() == types.ETInt {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
		if err != nil {
			return nil, err
		}
		charset, collate := ctx.GetSessionVars().GetCharsetInfo()
		bf.tp.SetCharset(charset)
		bf.tp.SetCollate(collate)

		bf.tp.SetFlen(64)
		bf.tp.SetDecimal(types.UnspecifiedLength)
		sig = &builtinOctIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_OctInt)
	} else {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
		if err != nil {
			return nil, err
		}
		charset, collate := ctx.GetSessionVars().GetCharsetInfo()
		bf.tp.SetCharset(charset)
		bf.tp.SetCollate(collate)
		bf.tp.SetFlen(64)
		bf.tp.SetDecimal(types.UnspecifiedLength)
		sig = &builtinOctStringSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_OctString)
	}

	return sig, nil
}

type builtinOctIntSig struct {
	baseBuiltinFunc
}

func (b *builtinOctIntSig) Clone() builtinFunc {
	newSig := &builtinOctIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals OCT(N).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_oct
func (b *builtinOctIntSig) evalString(row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	return strconv.FormatUint(uint64(val), 8), false, nil
}

type builtinOctStringSig struct {
	baseBuiltinFunc
}

func (b *builtinOctStringSig) Clone() builtinFunc {
	newSig := &builtinOctStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals OCT(N).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_oct
func (b *builtinOctStringSig) evalString(row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	negative, overflow := false, false
	val = getValidPrefix(strings.TrimSpace(val), 10)
	if len(val) == 0 {
		return "0", false, nil
	}

	if val[0] == '-' {
		negative, val = true, val[1:]
	}
	numVal, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		numError, ok := err.(*strconv.NumError)
		if !ok || numError.Err != strconv.ErrRange {
			return "", true, errors.Trace(err)
		}
		overflow = true
	}
	if negative && !overflow {
		numVal = -numVal
	}
	return strconv.FormatUint(numVal, 8), false, nil
}

type ordFunctionClass struct {
	baseFunctionClass
}

func (c *ordFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(10)
	sig := &builtinOrdSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Ord)
	return sig, nil
}

type builtinOrdSig struct {
	baseBuiltinFunc
}

func (b *builtinOrdSig) Clone() builtinFunc {
	newSig := &builtinOrdSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinOrdSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ord
func (b *builtinOrdSig) evalInt(row chunk.Row) (int64, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	strBytes := hack.Slice(str)
	enc := charset.FindEncoding(b.args[0].GetType().GetCharset())
	w := len(charset.EncodingUTF8Impl.Peek(strBytes))
	res, err := enc.Transform(nil, strBytes[:w], charset.OpEncode)
	if err != nil {
		// Fallback to the first byte.
		return calcOrd(strBytes[:1]), false, nil
	}
	// Only the first character is considered.
	return calcOrd(res[:len(enc.Peek(res))]), false, nil
}

func calcOrd(leftMost []byte) int64 {
	var result int64
	var factor int64 = 1
	for i := len(leftMost) - 1; i >= 0; i-- {
		result += int64(leftMost[i]) * factor
		factor *= 256
	}
	return result
}

type quoteFunctionClass struct {
	baseFunctionClass
}

func (c *quoteFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	SetBinFlagOrBinStr(args[0].GetType(), bf.tp)
	bf.tp.SetFlen(2*args[0].GetType().GetFlen() + 2)
	if bf.tp.GetFlen() > mysql.MaxBlobWidth {
		bf.tp.SetFlen(mysql.MaxBlobWidth)
	}
	sig := &builtinQuoteSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Quote)
	return sig, nil
}

type builtinQuoteSig struct {
	baseBuiltinFunc
}

func (b *builtinQuoteSig) Clone() builtinFunc {
	newSig := &builtinQuoteSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals QUOTE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_quote
func (b *builtinQuoteSig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if err != nil {
		return "", true, err
	} else if isNull {
		// If the argument is NULL, the return value is the word "NULL" without enclosing single quotation marks. see ref.
		return "NULL", false, err
	}

	return Quote(str), false, nil
}

// Quote produce a result that can be used as a properly escaped data value in an SQL statement.
func Quote(str string) string {
	runes := []rune(str)
	buffer := bytes.NewBufferString("")
	buffer.WriteRune('\'')
	for i, runeLength := 0, len(runes); i < runeLength; i++ {
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

	return buffer.String()
}

type binFunctionClass struct {
	baseFunctionClass
}

func (c *binFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetSessionVars().GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(64)
	sig := &builtinBinSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Bin)
	return sig, nil
}

type builtinBinSig struct {
	baseBuiltinFunc
}

func (b *builtinBinSig) Clone() builtinFunc {
	newSig := &builtinBinSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals BIN(N).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_bin
func (b *builtinBinSig) evalString(row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	return fmt.Sprintf("%b", uint64(val)), false, nil
}

type eltFunctionClass struct {
	baseFunctionClass
}

func (c *eltFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if argsErr := c.verifyArgs(args); argsErr != nil {
		return nil, argsErr
	}
	argTps := make([]types.EvalType, 0, len(args))
	argTps = append(argTps, types.ETInt)
	for i := 1; i < len(args); i++ {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	for _, arg := range args[1:] {
		argType := arg.GetType()
		if types.IsBinaryStr(argType) {
			types.SetBinChsClnFlag(bf.tp)
		}
		if argType.GetFlen() > bf.tp.GetFlen() {
			bf.tp.SetFlen(argType.GetFlen())
		}
	}
	sig := &builtinEltSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Elt)
	return sig, nil
}

type builtinEltSig struct {
	baseBuiltinFunc
}

func (b *builtinEltSig) Clone() builtinFunc {
	newSig := &builtinEltSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a ELT(N,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_elt
func (b *builtinEltSig) evalString(row chunk.Row) (string, bool, error) {
	idx, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if idx < 1 || idx >= int64(len(b.args)) {
		return "", true, nil
	}
	arg, isNull, err := b.args[idx].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	return arg, false, nil
}

type exportSetFunctionClass struct {
	baseFunctionClass
}

func (c *exportSetFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, 5)
	argTps = append(argTps, types.ETInt, types.ETString, types.ETString)
	if len(args) > 3 {
		argTps = append(argTps, types.ETString)
	}
	if len(args) > 4 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	// Calculate the flen as MySQL does.
	l := args[1].GetType().GetFlen()
	if args[2].GetType().GetFlen() > l {
		l = args[2].GetType().GetFlen()
	}
	sepL := 1
	if len(args) > 3 {
		sepL = args[3].GetType().GetFlen()
	}
	bf.tp.SetFlen((l*64 + sepL*63) * 4)
	switch len(args) {
	case 3:
		sig = &builtinExportSet3ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ExportSet3Arg)
	case 4:
		sig = &builtinExportSet4ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ExportSet4Arg)
	case 5:
		sig = &builtinExportSet5ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ExportSet5Arg)
	}
	return sig, nil
}

// exportSet evals EXPORT_SET(bits,on,off,separator,number_of_bits).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func exportSet(bits int64, on, off, separator string, numberOfBits int64) string {
	result := ""
	for i := uint64(0); i < uint64(numberOfBits); i++ {
		if (bits & (1 << i)) > 0 {
			result += on
		} else {
			result += off
		}
		if i < uint64(numberOfBits)-1 {
			result += separator
		}
	}
	return result
}

type builtinExportSet3ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinExportSet3ArgSig) Clone() builtinFunc {
	newSig := &builtinExportSet3ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals EXPORT_SET(bits,on,off).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet3ArgSig) evalString(row chunk.Row) (string, bool, error) {
	bits, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	on, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	off, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	return exportSet(bits, on, off, ",", 64), false, nil
}

type builtinExportSet4ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinExportSet4ArgSig) Clone() builtinFunc {
	newSig := &builtinExportSet4ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals EXPORT_SET(bits,on,off,separator).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet4ArgSig) evalString(row chunk.Row) (string, bool, error) {
	bits, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	on, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	off, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	separator, isNull, err := b.args[3].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	return exportSet(bits, on, off, separator, 64), false, nil
}

type builtinExportSet5ArgSig struct {
	baseBuiltinFunc
}

func (b *builtinExportSet5ArgSig) Clone() builtinFunc {
	newSig := &builtinExportSet5ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals EXPORT_SET(bits,on,off,separator,number_of_bits).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_export-set
func (b *builtinExportSet5ArgSig) evalString(row chunk.Row) (string, bool, error) {
	bits, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	on, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	off, isNull, err := b.args[2].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	separator, isNull, err := b.args[3].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	numberOfBits, isNull, err := b.args[4].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if numberOfBits < 0 || numberOfBits > 64 {
		numberOfBits = 64
	}

	return exportSet(bits, on, off, separator, numberOfBits), false, nil
}

type formatFunctionClass struct {
	baseFunctionClass
}

func (c *formatFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 2, 3)
	argTps[1] = types.ETInt
	argTp := args[0].GetType().EvalType()
	if argTp == types.ETDecimal || argTp == types.ETInt {
		argTps[0] = types.ETDecimal
	} else {
		argTps[0] = types.ETReal
	}
	if len(args) == 3 {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	charset, colalte := ctx.GetSessionVars().GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(colalte)
	bf.tp.SetFlen(mysql.MaxBlobWidth)
	var sig builtinFunc
	if len(args) == 3 {
		sig = &builtinFormatWithLocaleSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_FormatWithLocale)
	} else {
		sig = &builtinFormatSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Format)
	}
	return sig, nil
}

// formatMaxDecimals limits the maximum number of decimal digits for result of
// function `format`, this value is same as `FORMAT_MAX_DECIMALS` in MySQL source code.
const formatMaxDecimals int64 = 30

// evalNumDecArgsForFormat evaluates first 2 arguments, i.e, x and d, for function `format`.
func evalNumDecArgsForFormat(f builtinFunc, row chunk.Row) (string, string, bool, error) {
	var xStr string
	arg0, arg1 := f.getArgs()[0], f.getArgs()[1]
	ctx := f.getCtx()
	if arg0.GetType().EvalType() == types.ETDecimal {
		x, isNull, err := arg0.EvalDecimal(ctx, row)
		if isNull || err != nil {
			return "", "", isNull, err
		}
		xStr = x.String()
	} else {
		x, isNull, err := arg0.EvalReal(ctx, row)
		if isNull || err != nil {
			return "", "", isNull, err
		}
		xStr = strconv.FormatFloat(x, 'f', -1, 64)
	}
	d, isNull, err := arg1.EvalInt(ctx, row)
	if isNull || err != nil {
		return "", "", isNull, err
	}
	if d < 0 {
		d = 0
	} else if d > formatMaxDecimals {
		d = formatMaxDecimals
	}
	xStr = roundFormatArgs(xStr, int(d))
	dStr := strconv.FormatInt(d, 10)
	return xStr, dStr, false, nil
}

func roundFormatArgs(xStr string, maxNumDecimals int) string {
	if !strings.Contains(xStr, ".") {
		return xStr
	}

	sign := false
	// xStr cannot have '+' prefix now.
	// It is built in `evalNumDecArgsFormat` after evaluating `Evalxxx` method.
	if strings.HasPrefix(xStr, "-") {
		xStr = strings.Trim(xStr, "-")
		sign = true
	}

	xArr := strings.Split(xStr, ".")
	integerPart := xArr[0]
	decimalPart := xArr[1]

	if len(decimalPart) > maxNumDecimals {
		t := []byte(decimalPart)
		carry := false
		if t[maxNumDecimals] >= '5' {
			carry = true
		}
		for i := maxNumDecimals - 1; i >= 0 && carry; i-- {
			if t[i] == '9' {
				t[i] = '0'
			} else {
				t[i] = t[i] + 1
				carry = false
			}
		}
		decimalPart = string(t)
		t = []byte(integerPart)
		for i := len(integerPart) - 1; i >= 0 && carry; i-- {
			if t[i] == '9' {
				t[i] = '0'
			} else {
				t[i] = t[i] + 1
				carry = false
			}
		}
		if carry {
			integerPart = "1" + string(t)
		} else {
			integerPart = string(t)
		}
	}

	xStr = integerPart + "." + decimalPart
	if sign {
		xStr = "-" + xStr
	}
	return xStr
}

type builtinFormatWithLocaleSig struct {
	baseBuiltinFunc
}

func (b *builtinFormatWithLocaleSig) Clone() builtinFunc {
	newSig := &builtinFormatWithLocaleSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals FORMAT(X,D,locale).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_format
func (b *builtinFormatWithLocaleSig) evalString(row chunk.Row) (string, bool, error) {
	x, d, isNull, err := evalNumDecArgsForFormat(b, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	locale, isNull, err := b.args[2].EvalString(b.ctx, row)
	if err != nil {
		return "", false, err
	}
	if isNull {
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errUnknownLocale.GenWithStackByArgs("NULL"))
	} else if !strings.EqualFold(locale, "en_US") { // TODO: support other locales.
		b.ctx.GetSessionVars().StmtCtx.AppendWarning(errUnknownLocale.GenWithStackByArgs(locale))
	}
	locale = "en_US"
	formatString, err := mysql.GetLocaleFormatFunction(locale)(x, d)
	return formatString, false, err
}

type builtinFormatSig struct {
	baseBuiltinFunc
}

func (b *builtinFormatSig) Clone() builtinFunc {
	newSig := &builtinFormatSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals FORMAT(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_format
func (b *builtinFormatSig) evalString(row chunk.Row) (string, bool, error) {
	x, d, isNull, err := evalNumDecArgsForFormat(b, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	formatString, err := mysql.GetLocaleFormatFunction("en_US")(x, d)
	return formatString, false, err
}

type fromBase64FunctionClass struct {
	baseFunctionClass
}

func (c *fromBase64FunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	// The calculation of flen is the same as MySQL.
	if args[0].GetType().GetFlen() == types.UnspecifiedLength {
		bf.tp.SetFlen(types.UnspecifiedLength)
	} else {
		bf.tp.SetFlen(args[0].GetType().GetFlen() * 3)
		if bf.tp.GetFlen() > mysql.MaxBlobWidth {
			bf.tp.SetFlen(mysql.MaxBlobWidth)
		}
	}

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}

	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinFromBase64Sig{bf, maxAllowedPacket}
	sig.setPbCode(tipb.ScalarFuncSig_FromBase64)
	return sig, nil
}

// base64NeededDecodedLength return the base64 decoded string length.
func base64NeededDecodedLength(n int) int {
	// Returns -1 indicate the result will overflow.
	if strconv.IntSize == 64 && n > math.MaxInt64/3 {
		return -1
	}
	if strconv.IntSize == 32 && n > math.MaxInt32/3 {
		return -1
	}
	return n * 3 / 4
}

type builtinFromBase64Sig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinFromBase64Sig) Clone() builtinFunc {
	newSig := &builtinFromBase64Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals FROM_BASE64(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_from-base64
func (b *builtinFromBase64Sig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	needDecodeLen := base64NeededDecodedLength(len(str))
	if needDecodeLen == -1 {
		return "", true, nil
	}
	if needDecodeLen > int(b.maxAllowedPacket) {
		return "", true, handleAllowedPacketOverflowed(b.ctx, "from_base64", b.maxAllowedPacket)
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

func (c *toBase64FunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetSessionVars().GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(base64NeededEncodedLength(bf.args[0].GetType().GetFlen()))

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sig := &builtinToBase64Sig{bf, maxAllowedPacket}
	sig.setPbCode(tipb.ScalarFuncSig_ToBase64)
	return sig, nil
}

type builtinToBase64Sig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinToBase64Sig) Clone() builtinFunc {
	newSig := &builtinToBase64Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
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
func (b *builtinToBase64Sig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	needEncodeLen := base64NeededEncodedLength(len(str))
	if needEncodeLen == -1 {
		return "", true, nil
	}
	if needEncodeLen > int(b.maxAllowedPacket) {
		return "", true, handleAllowedPacketOverflowed(b.ctx, "to_base64", b.maxAllowedPacket)
	}
	if b.tp.GetFlen() == -1 || b.tp.GetFlen() > mysql.MaxBlobWidth {
		b.tp.SetFlen(mysql.MaxBlobWidth)
	}

	// encode
	strBytes := []byte(str)
	result := base64.StdEncoding.EncodeToString(strBytes)
	// A newline is added after each 76 characters of encoded output to divide long output into multiple lines.
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

type insertFunctionClass struct {
	baseFunctionClass
}

func (c *insertFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(mysql.MaxBlobWidth)
	addBinFlag(bf.tp)

	valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
	maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if types.IsBinaryStr(bf.tp) {
		sig = &builtinInsertSig{bf, maxAllowedPacket}
		sig.setPbCode(tipb.ScalarFuncSig_Insert)
	} else {
		sig = &builtinInsertUTF8Sig{bf, maxAllowedPacket}
		sig.setPbCode(tipb.ScalarFuncSig_InsertUTF8)
	}
	return sig, nil
}

type builtinInsertSig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinInsertSig) Clone() builtinFunc {
	newSig := &builtinInsertSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals INSERT(str,pos,len,newstr).
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_insert
func (b *builtinInsertSig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	length, isNull, err := b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	newstr, isNull, err := b.args[3].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	strLength := int64(len(str))
	if pos < 1 || pos > strLength {
		return str, false, nil
	}
	if length > strLength-pos+1 || length < 0 {
		length = strLength - pos + 1
	}

	if uint64(strLength-length+int64(len(newstr))) > b.maxAllowedPacket {
		return "", true, handleAllowedPacketOverflowed(b.ctx, "insert", b.maxAllowedPacket)
	}

	return str[0:pos-1] + newstr + str[pos+length-1:], false, nil
}

type builtinInsertUTF8Sig struct {
	baseBuiltinFunc
	maxAllowedPacket uint64
}

func (b *builtinInsertUTF8Sig) Clone() builtinFunc {
	newSig := &builtinInsertUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals INSERT(str,pos,len,newstr).
// See https://dev.mysql.com/doc/refman/5.6/en/string-functions.html#function_insert
func (b *builtinInsertUTF8Sig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	pos, isNull, err := b.args[1].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	length, isNull, err := b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	newstr, isNull, err := b.args[3].EvalString(b.ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	runes := []rune(str)
	runeLength := int64(len(runes))
	if pos < 1 || pos > runeLength {
		return str, false, nil
	}
	if length > runeLength-pos+1 || length < 0 {
		length = runeLength - pos + 1
	}

	strHead := string(runes[0 : pos-1])
	strTail := string(runes[pos+length-1:])
	if uint64(len(strHead)+len(newstr)+len(strTail)) > b.maxAllowedPacket {
		return "", true, handleAllowedPacketOverflowed(b.ctx, "insert", b.maxAllowedPacket)
	}
	return strHead + newstr + strTail, false, nil
}

type instrFunctionClass struct {
	baseFunctionClass
}

func (c *instrFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(11)
	if bf.collation == charset.CollationBin {
		sig := &builtinInstrSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Instr)
		return sig, nil
	}
	sig := &builtinInstrUTF8Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_InstrUTF8)
	return sig, nil
}

type builtinInstrUTF8Sig struct{ baseBuiltinFunc }

func (b *builtinInstrUTF8Sig) Clone() builtinFunc {
	newSig := &builtinInstrUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type builtinInstrSig struct{ baseBuiltinFunc }

func (b *builtinInstrSig) Clone() builtinFunc {
	newSig := &builtinInstrSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals INSTR(str,substr).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_instr
func (b *builtinInstrUTF8Sig) evalInt(row chunk.Row) (int64, bool, error) {
	str, IsNull, err := b.args[0].EvalString(b.ctx, row)
	if IsNull || err != nil {
		return 0, true, err
	}
	substr, IsNull, err := b.args[1].EvalString(b.ctx, row)
	if IsNull || err != nil {
		return 0, true, err
	}
	if collate.IsCICollation(b.collation) {
		str = strings.ToLower(str)
		substr = strings.ToLower(substr)
	}

	idx := strings.Index(str, substr)
	if idx == -1 {
		return 0, false, nil
	}
	return int64(utf8.RuneCountInString(str[:idx]) + 1), false, nil
}

// evalInt evals INSTR(str,substr), case sensitive.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_instr
func (b *builtinInstrSig) evalInt(row chunk.Row) (int64, bool, error) {
	str, IsNull, err := b.args[0].EvalString(b.ctx, row)
	if IsNull || err != nil {
		return 0, true, err
	}

	substr, IsNull, err := b.args[1].EvalString(b.ctx, row)
	if IsNull || err != nil {
		return 0, true, err
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

func (c *loadFileFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetSessionVars().GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(64)
	sig := &builtinLoadFileSig{bf}
	return sig, nil
}

type builtinLoadFileSig struct {
	baseBuiltinFunc
}

func (b *builtinLoadFileSig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	return "", true, nil
}

func (b *builtinLoadFileSig) Clone() builtinFunc {
	newSig := &builtinLoadFileSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type weightStringPadding byte

const (
	// weightStringPaddingNone is used for WEIGHT_STRING(expr) if the expr is non-numeric.
	weightStringPaddingNone weightStringPadding = iota
	// weightStringPaddingAsChar is used for WEIGHT_STRING(expr AS CHAR(x)) and the expr is non-numeric.
	weightStringPaddingAsChar
	// weightStringPaddingAsBinary is used for WEIGHT_STRING(expr as BINARY(x)) and the expr is not null.
	weightStringPaddingAsBinary
	// weightStringPaddingNull is used for WEIGHT_STRING(expr [AS (CHAR|BINARY)]) for all other cases, it returns null always.
	weightStringPaddingNull
)

type weightStringFunctionClass struct {
	baseFunctionClass
}

func (c *weightStringFunctionClass) verifyArgs(args []Expression) (weightStringPadding, int, error) {
	padding := weightStringPaddingNone
	l := len(args)
	if l != 1 && l != 3 {
		return weightStringPaddingNone, 0, ErrIncorrectParameterCount.GenWithStackByArgs(c.funcName)
	}
	if types.IsTypeNumeric(args[0].GetType().GetType()) {
		padding = weightStringPaddingNull
	}
	length := 0
	if l == 3 {
		if args[1].GetType().EvalType() != types.ETString {
			return weightStringPaddingNone, 0, ErrIncorrectType.GenWithStackByArgs(args[1].String(), c.funcName)
		}
		c1, ok := args[1].(*Constant)
		if !ok {
			return weightStringPaddingNone, 0, ErrIncorrectType.GenWithStackByArgs(args[1].String(), c.funcName)
		}
		switch x := c1.Value.GetString(); x {
		case "CHAR":
			if padding == weightStringPaddingNone {
				padding = weightStringPaddingAsChar
			}
		case "BINARY":
			padding = weightStringPaddingAsBinary
		default:
			return weightStringPaddingNone, 0, ErrIncorrectType.GenWithStackByArgs(x, c.funcName)
		}
		if args[2].GetType().EvalType() != types.ETInt {
			return weightStringPaddingNone, 0, ErrIncorrectType.GenWithStackByArgs(args[2].String(), c.funcName)
		}
		c2, ok := args[2].(*Constant)
		if !ok {
			return weightStringPaddingNone, 0, ErrIncorrectType.GenWithStackByArgs(args[1].String(), c.funcName)
		}
		length = int(c2.Value.GetInt64())
		if length == 0 {
			return weightStringPaddingNone, 0, ErrIncorrectType.GenWithStackByArgs(args[2].String(), c.funcName)
		}
	}
	return padding, length, nil
}

func (c *weightStringFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	padding, length, err := c.verifyArgs(args)
	if err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, len(args))
	argTps[0] = types.ETString

	if len(args) == 3 {
		argTps[1] = types.ETString
		argTps[2] = types.ETInt
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	var sig builtinFunc
	if padding == weightStringPaddingNull {
		sig = &builtinWeightStringNullSig{bf}
	} else {
		valStr, _ := ctx.GetSessionVars().GetSystemVar(variable.MaxAllowedPacket)
		maxAllowedPacket, err := strconv.ParseUint(valStr, 10, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
		sig = &builtinWeightStringSig{bf, padding, length, maxAllowedPacket}
	}
	return sig, nil
}

type builtinWeightStringNullSig struct {
	baseBuiltinFunc
}

func (b *builtinWeightStringNullSig) Clone() builtinFunc {
	newSig := &builtinWeightStringNullSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a WEIGHT_STRING(expr [AS CHAR|BINARY]) when the expr is numeric types, it always returns null.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_weight-string
func (b *builtinWeightStringNullSig) evalString(row chunk.Row) (string, bool, error) {
	return "", true, nil
}

type builtinWeightStringSig struct {
	baseBuiltinFunc

	padding          weightStringPadding
	length           int
	maxAllowedPacket uint64
}

func (b *builtinWeightStringSig) Clone() builtinFunc {
	newSig := &builtinWeightStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.padding = b.padding
	newSig.length = b.length
	newSig.maxAllowedPacket = b.maxAllowedPacket
	return newSig
}

// evalString evals a WEIGHT_STRING(expr [AS (CHAR|BINARY)]) when the expr is non-numeric types.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_weight-string
func (b *builtinWeightStringSig) evalString(row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(b.ctx, row)
	if err != nil {
		return "", false, err
	}
	if isNull {
		return "", true, nil
	}

	var ctor collate.Collator
	// TODO: refactor padding codes after padding is implemented by all collators.
	switch b.padding {
	case weightStringPaddingAsChar:
		runes := []rune(str)
		lenRunes := len(runes)
		if b.length < lenRunes {
			str = string(runes[:b.length])
		} else if b.length > lenRunes {
			if uint64(b.length-lenRunes) > b.maxAllowedPacket {
				return "", true, handleAllowedPacketOverflowed(b.ctx, "weight_string", b.maxAllowedPacket)
			}
			str += strings.Repeat(" ", b.length-lenRunes)
		}
		ctor = collate.GetCollator(b.args[0].GetType().GetCollate())
	case weightStringPaddingAsBinary:
		lenStr := len(str)
		if b.length < lenStr {
			tpInfo := fmt.Sprintf("BINARY(%d)", b.length)
			b.ctx.GetSessionVars().StmtCtx.AppendWarning(errTruncatedWrongValue.GenWithStackByArgs(tpInfo, str))
			str = str[:b.length]
		} else if b.length > lenStr {
			if uint64(b.length-lenStr) > b.maxAllowedPacket {
				return "", true, handleAllowedPacketOverflowed(b.ctx, "cast_as_binary", b.maxAllowedPacket)
			}
			str += strings.Repeat("\x00", b.length-lenStr)
		}
		ctor = collate.GetCollator(charset.CollationBin)
	case weightStringPaddingNone:
		ctor = collate.GetCollator(b.args[0].GetType().GetCollate())
	default:
		return "", false, ErrIncorrectType.GenWithStackByArgs(ast.WeightString, string(b.padding))
	}
	return string(ctor.Key(str)), false, nil
}

const (
	invalidRune rune   = -1
	invalidByte uint16 = 256
)

type translateFunctionClass struct {
	baseFunctionClass
}

// getFunction sets translate built-in function signature.
// The syntax of translate in Oracle is 'TRANSLATE(expr, from_string, to_string)'.
func (c *translateFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType()
	bf.tp.SetFlen(argType.GetFlen())
	SetBinFlagOrBinStr(argType, bf.tp)
	if types.IsBinaryStr(args[0].GetType()) || types.IsBinaryStr(args[1].GetType()) || types.IsBinaryStr(args[2].GetType()) {
		sig := &builtinTranslateBinarySig{bf}
		return sig, nil
	}
	sig := &builtinTranslateUTF8Sig{bf}
	return sig, nil
}

type builtinTranslateBinarySig struct {
	baseBuiltinFunc
}

func (b *builtinTranslateBinarySig) Clone() builtinFunc {
	newSig := &builtinTranslateBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTranslateSig, corresponding to translate(srcStr, fromStr, toStr)
// See https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions196.htm
func (b *builtinTranslateBinarySig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	var (
		srcStr, fromStr, toStr     string
		isFromStrNull, isToStrNull bool
		tgt                        []byte
	)
	srcStr, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	fromStr, isFromStrNull, err = b.args[1].EvalString(b.ctx, row)
	if isFromStrNull || err != nil {
		return d, isFromStrNull, err
	}
	toStr, isToStrNull, err = b.args[2].EvalString(b.ctx, row)
	if isToStrNull || err != nil {
		return d, isToStrNull, err
	}
	mp := buildTranslateMap4Binary([]byte(fromStr), []byte(toStr))
	for _, charSrc := range []byte(srcStr) {
		if charTo, ok := mp[charSrc]; ok {
			if charTo != invalidByte {
				tgt = append(tgt, byte(charTo))
			}
		} else {
			tgt = append(tgt, charSrc)
		}
	}
	return string(tgt), false, nil
}

type builtinTranslateUTF8Sig struct {
	baseBuiltinFunc
}

func (b *builtinTranslateUTF8Sig) Clone() builtinFunc {
	newSig := &builtinTranslateUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTranslateUTF8Sig, corresponding to translate(srcStr, fromStr, toStr)
// See https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions196.htm
func (b *builtinTranslateUTF8Sig) evalString(row chunk.Row) (d string, isNull bool, err error) {
	var (
		srcStr, fromStr, toStr     string
		isFromStrNull, isToStrNull bool
		tgt                        strings.Builder
	)
	srcStr, isNull, err = b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	fromStr, isFromStrNull, err = b.args[1].EvalString(b.ctx, row)
	if isFromStrNull || err != nil {
		return d, isFromStrNull, err
	}
	toStr, isToStrNull, err = b.args[2].EvalString(b.ctx, row)
	if isToStrNull || err != nil {
		return d, isToStrNull, err
	}
	mp := buildTranslateMap4UTF8([]rune(fromStr), []rune(toStr))
	for _, charSrc := range srcStr {
		if charTo, ok := mp[charSrc]; ok {
			if charTo != invalidRune {
				tgt.WriteRune(charTo)
			}
		} else {
			tgt.WriteRune(charSrc)
		}
	}
	return tgt.String(), false, nil
}

func buildTranslateMap4UTF8(from, to []rune) map[rune]rune {
	mp := make(map[rune]rune)
	lenFrom, lenTo := len(from), len(to)
	minLen := lenTo
	if lenFrom < lenTo {
		minLen = lenFrom
	}
	for idx := lenFrom - 1; idx >= lenTo; idx-- {
		mp[from[idx]] = invalidRune
	}
	for idx := minLen - 1; idx >= 0; idx-- {
		mp[from[idx]] = to[idx]
	}
	return mp
}

func buildTranslateMap4Binary(from, to []byte) map[byte]uint16 {
	mp := make(map[byte]uint16)
	lenFrom, lenTo := len(from), len(to)
	minLen := lenTo
	if lenFrom < lenTo {
		minLen = lenFrom
	}
	for idx := lenFrom - 1; idx >= lenTo; idx-- {
		mp[from[idx]] = invalidByte
	}
	for idx := minLen - 1; idx >= 0; idx-- {
		mp[from[idx]] = uint16(to[idx])
	}
	return mp
}
