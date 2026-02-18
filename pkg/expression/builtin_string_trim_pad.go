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
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

func (c *trimFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	switch len(args) {
	case 1:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
		if err != nil {
			return nil, err
		}
		argType := args[0].GetType(ctx.GetEvalCtx())
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
		argType := args[0].GetType(ctx.GetEvalCtx())
		SetBinFlagOrBinStr(argType, bf.tp)
		sig := &builtinTrim2ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Trim2Args)
		return sig, nil

	case 3:
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString, types.ETInt)
		if err != nil {
			return nil, err
		}
		argType := args[0].GetType(ctx.GetEvalCtx())
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTrim1ArgSig) Clone() builtinFunc {
	newSig := &builtinTrim1ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTrim1ArgSig, corresponding to trim(str)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim1ArgSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	return strings.Trim(d, spaceChars), false, nil
}

type builtinTrim2ArgsSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTrim2ArgsSig) Clone() builtinFunc {
	newSig := &builtinTrim2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTrim2ArgsSig, corresponding to trim(str, remstr)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim2ArgsSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	var str, remstr string

	str, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	remstr, isNull, err = b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	d = trimLeft(str, remstr)
	d = trimRight(d, remstr)
	return d, false, nil
}

type builtinTrim3ArgsSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinTrim3ArgsSig) Clone() builtinFunc {
	newSig := &builtinTrim3ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinTrim3ArgsSig, corresponding to trim(str, remstr, direction)
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
func (b *builtinTrim3ArgsSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	var (
		str, remstr  string
		x            int64
		direction    ast.TrimDirectionType
		isRemStrNull bool
	)
	str, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	remstr, isRemStrNull, err = b.args[1].EvalString(ctx, row)
	if err != nil || isRemStrNull {
		return d, isRemStrNull, err
	}
	x, isNull, err = b.args[2].EvalInt(ctx, row)
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

func (c *lTrimFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType(ctx.GetEvalCtx())
	bf.tp.SetFlen(argType.GetFlen())
	SetBinFlagOrBinStr(argType, bf.tp)
	sig := &builtinLTrimSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LTrim)
	return sig, nil
}

type builtinLTrimSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLTrimSig) Clone() builtinFunc {
	newSig := &builtinLTrimSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinLTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_ltrim
func (b *builtinLTrimSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return d, isNull, err
	}
	return strings.TrimLeft(d, spaceChars), false, nil
}

type rTrimFunctionClass struct {
	baseFunctionClass
}

func (c *rTrimFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType(ctx.GetEvalCtx())
	bf.tp.SetFlen(argType.GetFlen())
	SetBinFlagOrBinStr(argType, bf.tp)
	sig := &builtinRTrimSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_RTrim)
	return sig, nil
}

type builtinRTrimSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRTrimSig) Clone() builtinFunc {
	newSig := &builtinRTrimSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinRTrimSig
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_rtrim
func (b *builtinRTrimSig) evalString(ctx EvalContext, row chunk.Row) (d string, isNull bool, err error) {
	d, isNull, err = b.args[0].EvalString(ctx, row)
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

func getFlen4LpadAndRpad(ctx BuildContext, arg Expression) int {
	if constant, ok := arg.(*Constant); ok {
		length, isNull, err := constant.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
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

func (c *lpadFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(getFlen4LpadAndRpad(ctx, args[1]))
	addBinFlag(bf.tp)

	maxAllowedPacket := ctx.GetEvalCtx().GetMaxAllowedPacket()
	if types.IsBinaryStr(args[0].GetType(ctx.GetEvalCtx())) || types.IsBinaryStr(args[2].GetType(ctx.GetEvalCtx())) {
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
func (b *builtinLpadSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	byteLength := len(str)

	length, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	targetLength := int(length)

	if uint64(targetLength) > b.maxAllowedPacket {
		return "", true, handleAllowedPacketOverflowed(ctx, "lpad", b.maxAllowedPacket)
	}

	padStr, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	padLength := len(padStr)

	if targetLength < 0 || targetLength > b.tp.GetFlen() {
		return "", true, nil
	}
	if byteLength < targetLength && padLength == 0 {
		return "", false, nil
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
func (b *builtinLpadUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runeLength := utf8.RuneCountInString(str)

	length, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	targetLength := int(length)

	if uint64(targetLength)*uint64(mysql.MaxBytesOfCharacter) > b.maxAllowedPacket {
		return "", true, handleAllowedPacketOverflowed(ctx, "lpad", b.maxAllowedPacket)
	}

	padStr, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	padLength := utf8.RuneCountInString(padStr)

	if targetLength < 0 || targetLength*4 > b.tp.GetFlen() {
		return "", true, nil
	}
	if runeLength < targetLength && padLength == 0 {
		return "", false, nil
	}

	if tailLen := targetLength - runeLength; tailLen > 0 {
		repeatCount := tailLen/padLength + 1
		repeated := strings.Repeat(padStr, repeatCount)
		str = repeated[:runeByteIndex(repeated, tailLen)] + str
	}
	return str[:runeByteIndex(str, targetLength)], false, nil
}

type rpadFunctionClass struct {
	baseFunctionClass
}

func (c *rpadFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(getFlen4LpadAndRpad(ctx, args[1]))
	addBinFlag(bf.tp)

	maxAllowedPacket := ctx.GetEvalCtx().GetMaxAllowedPacket()
	if types.IsBinaryStr(args[0].GetType(ctx.GetEvalCtx())) || types.IsBinaryStr(args[2].GetType(ctx.GetEvalCtx())) {
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
func (b *builtinRpadSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	byteLength := len(str)

	length, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	targetLength := int(length)
	if uint64(targetLength) > b.maxAllowedPacket {
		return "", true, handleAllowedPacketOverflowed(ctx, "rpad", b.maxAllowedPacket)
	}

	padStr, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	padLength := len(padStr)

	if targetLength < 0 || targetLength > b.tp.GetFlen() {
		return "", true, nil
	}
	if byteLength < targetLength && padLength == 0 {
		return "", false, nil
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
func (b *builtinRpadUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	runeLength := utf8.RuneCountInString(str)

	length, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	targetLength := int(length)

	if uint64(targetLength)*uint64(mysql.MaxBytesOfCharacter) > b.maxAllowedPacket {
		return "", true, handleAllowedPacketOverflowed(ctx, "rpad", b.maxAllowedPacket)
	}

	padStr, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	padLength := utf8.RuneCountInString(padStr)

	if targetLength < 0 || targetLength*4 > b.tp.GetFlen() {
		return "", true, nil
	}
	if runeLength < targetLength && padLength == 0 {
		return "", false, nil
	}

	if tailLen := targetLength - runeLength; tailLen > 0 {
		repeatCount := tailLen/padLength + 1
		str = str + strings.Repeat(padStr, repeatCount)
	}
	return str[:runeByteIndex(str, targetLength)], false, nil
}

type bitLengthFunctionClass struct {
	baseFunctionClass
}

func (c *bitLengthFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinBitLengthSig) Clone() builtinFunc {
	newSig := &builtinBitLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evaluates a builtinBitLengthSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_bit-length
func (b *builtinBitLengthSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(len(val) * 8), false, nil
}

type charFunctionClass struct {
	baseFunctionClass
}

func (c *charFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range len(args) - 1 {
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
		logutil.BgLogger().Warn(fmt.Sprintf("The last argument in char function must be constant, but got %T", args[len(args)-1].StringWithCtx(ctx.GetEvalCtx(), errors.RedactLogDisable)))
		return nil, errIncorrectArgs
	}
	charsetName, isNull, err := args[len(args)-1].EvalString(ctx.GetEvalCtx(), chunk.Row{})
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
func (b *builtinCharSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	bigints := make([]int64, 0, len(b.args)-1)

	for i := range len(b.args) - 1 {
		val, IsNull, err := b.args[i].EvalInt(ctx, row)
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
		tc := typeCtx(ctx)
		tc.AppendWarning(err)
		if sqlMode(ctx).HasStrictMode() {
			return "", true, nil
		}
	}
	return string(res), false, nil
}

type charLengthFunctionClass struct {
	baseFunctionClass
}

func (c *charLengthFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if argsErr := c.verifyArgs(args); argsErr != nil {
		return nil, argsErr
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	if types.IsBinaryStr(args[0].GetType(ctx.GetEvalCtx())) {
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCharLengthBinarySig) Clone() builtinFunc {
	newSig := &builtinCharLengthBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCharLengthUTF8Sig for binary string type.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char-length
func (b *builtinCharLengthBinarySig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(len(val)), false, nil
}

type builtinCharLengthUTF8Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCharLengthUTF8Sig) Clone() builtinFunc {
	newSig := &builtinCharLengthUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCharLengthUTF8Sig for non-binary string type.
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_char-length
func (b *builtinCharLengthUTF8Sig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(utf8.RuneCountInString(val)), false, nil
}

type findInSetFunctionClass struct {
	baseFunctionClass
}

func (c *findInSetFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
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
func (b *builtinFindInSetSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	strlist, isNull, err := b.args[1].EvalString(ctx, row)
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

func (c *fieldFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	isAllString, isAllNumber := true, true
	for i, length := 0, len(args); i < length; i++ {
		argTp := args[i].GetType(ctx.GetEvalCtx()).EvalType()
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFieldIntSig) Clone() builtinFunc {
	newSig := &builtinFieldIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals FIELD(str,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
func (b *builtinFieldIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	str, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, err != nil, err
	}
	for i, length := 1, len(b.args); i < length; i++ {
		stri, isNull, err := b.args[i].EvalInt(ctx, row)
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFieldRealSig) Clone() builtinFunc {
	newSig := &builtinFieldRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals FIELD(str,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
func (b *builtinFieldRealSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	str, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, err != nil, err
	}
	for i, length := 1, len(b.args); i < length; i++ {
		stri, isNull, err := b.args[i].EvalReal(ctx, row)
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFieldStringSig) Clone() builtinFunc {
	newSig := &builtinFieldStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals FIELD(str,str1,str2,str3,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_field
func (b *builtinFieldStringSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, err != nil, err
	}
	for i, length := 1, len(b.args); i < length; i++ {
		stri, isNull, err := b.args[i].EvalString(ctx, row)
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

func (c *makeSetFunctionClass) getFlen(ctx BuildContext, args []Expression) int {
	flen, count := 0, 0
	if constant, ok := args[0].(*Constant); ok {
		bits, isNull, err := constant.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
		if err == nil && !isNull {
			for i, length := 1, len(args); i < length; i++ {
				if (bits & (1 << uint(i-1))) != 0 {
					flen += args[i].GetType(ctx.GetEvalCtx()).GetFlen()
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
		flen += args[i].GetType(ctx.GetEvalCtx()).GetFlen()
	}
	return flen + len(args) - 1 - 1
}

func (c *makeSetFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
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
	bf.tp.SetFlen(c.getFlen(ctx, args))
	if bf.tp.GetFlen() > mysql.MaxBlobWidth {
		bf.tp.SetFlen(mysql.MaxBlobWidth)
	}
	sig := &builtinMakeSetSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_MakeSet)
	return sig, nil
}

type builtinMakeSetSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMakeSetSig) Clone() builtinFunc {
	newSig := &builtinMakeSetSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals MAKE_SET(bits,str1,str2,...).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_make-set
func (b *builtinMakeSetSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	bits, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	sets := make([]string, 0, len(b.args)-1)
	for i, length := 1, len(b.args); i < length; i++ {
		if (bits & (1 << uint(i-1))) == 0 {
			continue
		}
		str, isNull, err := b.args[i].EvalString(ctx, row)
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

func (c *octFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var sig builtinFunc
	if IsBinaryLiteral(args[0]) || args[0].GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
		if err != nil {
			return nil, err
		}
		charset, collate := ctx.GetCharsetInfo()
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
		charset, collate := ctx.GetCharsetInfo()
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinOctIntSig) Clone() builtinFunc {
	newSig := &builtinOctIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals OCT(N).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_oct
func (b *builtinOctIntSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	return strconv.FormatUint(uint64(val), 8), false, nil
}

type builtinOctStringSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinOctStringSig) Clone() builtinFunc {
	newSig := &builtinOctStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals OCT(N).
// See https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_oct
func (b *builtinOctStringSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	// for issue #59446 should return NULL for empty string
	if len(val) == 0 {
		return "", true, nil
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

