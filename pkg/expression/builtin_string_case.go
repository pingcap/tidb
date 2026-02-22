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
	"math"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

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
