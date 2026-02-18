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
	"math"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

func (c *lengthFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLengthSig) Clone() builtinFunc {
	newSig := &builtinLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evaluates a builtinLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html
func (b *builtinLengthSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(len([]byte(val))), false, nil
}

type asciiFunctionClass struct {
	baseFunctionClass
}

func (c *asciiFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinASCIISig) Clone() builtinFunc {
	newSig := &builtinASCIISig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinASCIISig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ascii
func (b *builtinASCIISig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
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

func (c *concatFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	addBinFlag(bf.tp)
	bf.tp.SetFlen(0)
	for i := range args {
		argType := args[i].GetType(ctx.GetEvalCtx())

		if argType.GetFlen() < 0 {
			bf.tp.SetFlen(mysql.MaxBlobWidth)
			logutil.BgLogger().Debug("unexpected `Flen` value(-1) in CONCAT's args", zap.Int("arg's index", i))
		}
		bf.tp.SetFlen(bf.tp.GetFlen() + argType.GetFlen())
	}
	if bf.tp.GetFlen() >= mysql.MaxBlobWidth {
		bf.tp.SetFlen(mysql.MaxBlobWidth)
	}

	maxAllowedPacket := ctx.GetEvalCtx().GetMaxAllowedPacket()
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
func (b *builtinConcatSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	//nolint: prealloc
	var s []byte
	for _, a := range b.getArgs() {
		d, isNull, err = a.EvalString(ctx, row)
		if isNull || err != nil {
			return d, isNull, err
		}
		if uint64(len(s)+len(d)) > b.maxAllowedPacket {
			return "", true, handleAllowedPacketOverflowed(ctx, "concat", b.maxAllowedPacket)
		}
		s = append(s, []byte(d)...)
	}
	return string(s), false, nil
}

type concatWSFunctionClass struct {
	baseFunctionClass
}

func (c *concatWSFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETString)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(0)

	addBinFlag(bf.tp)
	for i := range args {
		argType := args[i].GetType(ctx.GetEvalCtx())

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
	bf.tp.SetFlen(bf.tp.GetFlen() + sepsLen*args[0].GetType(ctx.GetEvalCtx()).GetFlen())

	if bf.tp.GetFlen() >= mysql.MaxBlobWidth {
		bf.tp.SetFlen(mysql.MaxBlobWidth)
	}

	maxAllowedPacket := ctx.GetEvalCtx().GetMaxAllowedPacket()
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
func (b *builtinConcatWSSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	args := b.getArgs()
	strs := make([]string, 0, len(args))
	var sep string
	var targetLength int

	N := len(args)
	if N > 0 {
		val, isNull, err := args[0].EvalString(ctx, row)
		if err != nil || isNull {
			// If the separator is NULL, the result is NULL.
			return val, isNull, err
		}
		sep = val
	}
	for i := 1; i < N; i++ {
		val, isNull, err := args[i].EvalString(ctx, row)
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
			return "", true, handleAllowedPacketOverflowed(ctx, "concat_ws", b.maxAllowedPacket)
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

func (c *leftFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType(ctx.GetEvalCtx())
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeftSig) Clone() builtinFunc {
	newSig := &builtinLeftSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals LEFT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func (b *builtinLeftSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	left, isNull, err := b.args[1].EvalInt(ctx, row)
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeftUTF8Sig) Clone() builtinFunc {
	newSig := &builtinLeftUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals LEFT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_left
func (b *builtinLeftUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	left, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	leftLength := int(left)
	if leftLength < 0 {
		leftLength = 0
	}
	byteIdx := 0
	for j := 0; j < leftLength && byteIdx < len(str); j++ {
		_, size := utf8.DecodeRuneInString(str[byteIdx:])
		byteIdx += size
	}
	return str[:byteIdx], false, nil
}

type rightFunctionClass struct {
	baseFunctionClass
}

func (c *rightFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType(ctx.GetEvalCtx())
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRightSig) Clone() builtinFunc {
	newSig := &builtinRightSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals RIGHT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_right
func (b *builtinRightSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	right, isNull, err := b.args[1].EvalInt(ctx, row)
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRightUTF8Sig) Clone() builtinFunc {
	newSig := &builtinRightUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals RIGHT(str,len).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_right
func (b *builtinRightUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	right, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	strLength := utf8.RuneCountInString(str)
	rightLength := int(right)
	if rightLength > strLength {
		rightLength = strLength
	} else if rightLength < 0 {
		rightLength = 0
	}
	return str[runeByteIndex(str, strLength-rightLength):], false, nil
}

type repeatFunctionClass struct {
	baseFunctionClass
}

func (c *repeatFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(mysql.MaxBlobWidth)
	SetBinFlagOrBinStr(args[0].GetType(ctx.GetEvalCtx()), bf.tp)
	maxAllowedPacket := ctx.GetEvalCtx().GetMaxAllowedPacket()
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
func (b *builtinRepeatSig) evalString(ctx EvalContext, row chunk.Row) (val string, isNull bool, err error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	byteLength := len(str)

	num, isNull, err := b.args[1].EvalInt(ctx, row)
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
		return "", true, handleAllowedPacketOverflowed(ctx, "repeat", b.maxAllowedPacket)
	}

	return strings.Repeat(str, int(num)), false, nil
}

type lowerFunctionClass struct {
	baseFunctionClass
}

func (c *lowerFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	argTp := args[0].GetType(ctx.GetEvalCtx())
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLowerUTF8Sig) Clone() builtinFunc {
	newSig := &builtinLowerUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLowerUTF8Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lower
func (b *builtinLowerUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	enc := charset.FindEncoding(b.args[0].GetType(ctx).GetCharset())
	return enc.ToLower(d), false, nil
}

type builtinLowerSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLowerSig) Clone() builtinFunc {
	newSig := &builtinLowerSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLowerSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_lower
func (b *builtinLowerSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	return d, false, nil
}

type reverseFunctionClass struct {
	baseFunctionClass
}

func (c *reverseFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}

	argTp := args[0].GetType(ctx.GetEvalCtx())
	bf.tp.SetFlen(args[0].GetType(ctx.GetEvalCtx()).GetFlen())
	addBinFlag(bf.tp)
	var sig builtinFunc
	if types.IsBinaryStr(argTp) || types.IsTypeBit(argTp) {
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinReverseSig) Clone() builtinFunc {
	newSig := &builtinReverseSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a REVERSE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	reversed := reverseBytes([]byte(str))
	return string(reversed), false, nil
}

type builtinReverseUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinReverseUTF8Sig) Clone() builtinFunc {
	newSig := &builtinReverseUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a REVERSE(str).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_reverse
func (b *builtinReverseUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	reversed := reverseRunes([]rune(str))
	return string(reversed), false, nil
}

type spaceFunctionClass struct {
	baseFunctionClass
}

func (c *spaceFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(mysql.MaxBlobWidth)
	maxAllowedPacket := ctx.GetEvalCtx().GetMaxAllowedPacket()
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
func (b *builtinSpaceSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	var x int64

	x, isNull, err = b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	if x < 0 {
		x = 0
	}
	if uint64(x) > b.maxAllowedPacket {
		return d, true, handleAllowedPacketOverflowed(ctx, "space", b.maxAllowedPacket)
	}
	if x > mysql.MaxBlobWidth {
		return d, true, nil
	}
	return strings.Repeat(" ", int(x)), false, nil
}

type upperFunctionClass struct {
	baseFunctionClass
}

func (c *upperFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	argTp := args[0].GetType(ctx.GetEvalCtx())
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUpperUTF8Sig) Clone() builtinFunc {
	newSig := &builtinUpperUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUpperUTF8Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func (b *builtinUpperUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	enc := charset.FindEncoding(b.args[0].GetType(ctx).GetCharset())
	return enc.ToUpper(d), false, nil
}

type builtinUpperSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUpperSig) Clone() builtinFunc {
	newSig := &builtinUpperSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinUpperSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_upper
func (b *builtinUpperSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}

	return d, false, nil
}

type strcmpFunctionClass struct {
	baseFunctionClass
}
