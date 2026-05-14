// Copyright 2026 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

type maskFullFunctionClass struct {
	baseFunctionClass
}

func (c *maskFullFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argType := args[0].GetType(ctx.GetEvalCtx())
	evalTp := argType.EvalType()
	argTps := []types.EvalType{evalTp}
	if len(args) == 2 {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, evalTp, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp = argType.Clone()
	switch evalTp {
	case types.ETString:
		maskArgIsBinary := len(args) == 2 && types.IsBinaryStr(args[1].GetType(ctx.GetEvalCtx()))
		if types.IsBinaryStr(argType) || maskArgIsBinary {
			return &builtinMaskFullBinarySig{bf}, nil
		}
		return &builtinMaskFullStringSig{bf}, nil
	case types.ETDatetime, types.ETTimestamp:
		if !types.IsTypeTime(argType.GetType()) {
			return nil, errIncorrectArgs.GenWithStackByArgs("mask_full")
		}
		return &builtinMaskFullTimeSig{bf}, nil
	case types.ETDuration:
		if argType.GetType() != mysql.TypeDuration {
			return nil, errIncorrectArgs.GenWithStackByArgs("mask_full")
		}
		return &builtinMaskFullDurationSig{bf}, nil
	case types.ETInt:
		if argType.GetType() != mysql.TypeYear {
			return nil, errIncorrectArgs.GenWithStackByArgs("mask_full")
		}
		return &builtinMaskFullIntSig{bf}, nil
	default:
		return nil, errIncorrectArgs.GenWithStackByArgs("mask_full")
	}
}

type builtinMaskFullStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMaskFullStringSig) Clone() builtinFunc {
	newSig := &builtinMaskFullStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMaskFullStringSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	maskRune := "X"
	if len(b.args) == 2 {
		mask, isNull, err := b.args[1].EvalString(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
		maskRunes := []rune(mask)
		if len(maskRunes) != 1 {
			return "", true, errIncorrectArgs.GenWithStackByArgs("mask_full")
		}
		maskRune = string(maskRunes[0])
	}
	return strings.Repeat(maskRune, len([]rune(str))), false, nil
}

type builtinMaskFullBinarySig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMaskFullBinarySig) Clone() builtinFunc {
	newSig := &builtinMaskFullBinarySig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMaskFullBinarySig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	mask := "X"
	if len(b.args) == 2 {
		mask, isNull, err = b.args[1].EvalString(ctx, row)
		if isNull || err != nil {
			return "", true, err
		}
		if len(mask) != 1 {
			return "", true, errIncorrectArgs.GenWithStackByArgs("mask_full")
		}
	}
	return strings.Repeat(mask, len(str)), false, nil
}

type builtinMaskFullTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMaskFullTimeSig) Clone() builtinFunc {
	newSig := &builtinMaskFullTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMaskFullTimeSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	_, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}
	tp := b.tp.GetType()
	fsp := b.tp.GetDecimal()
	if tp == mysql.TypeDate {
		return types.NewTime(types.FromDate(1970, 1, 1, 0, 0, 0, 0), mysql.TypeDate, 0), false, nil
	}
	return types.NewTime(types.FromDate(1970, 1, 1, 0, 0, 0, 0), tp, fsp), false, nil
}

type builtinMaskFullDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMaskFullDurationSig) Clone() builtinFunc {
	newSig := &builtinMaskFullDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMaskFullDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	_, isNull, err := b.args[0].EvalDuration(ctx, row)
	if isNull || err != nil {
		return types.Duration{}, true, err
	}
	fsp := b.tp.GetDecimal()
	if fsp == types.UnspecifiedFsp {
		fsp = types.DefaultFsp
	}
	return types.Duration{Fsp: fsp}, false, nil
}

type builtinMaskFullIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMaskFullIntSig) Clone() builtinFunc {
	newSig := &builtinMaskFullIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMaskFullIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	return 0, false, nil
}

type maskNullFunctionClass struct {
	baseFunctionClass
}

func (c *maskNullFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argType := args[0].GetType(ctx.GetEvalCtx())
	evalTp := argType.EvalType()
	bf, err := newBaseBuiltinFuncWithFieldTypes(ctx, c.funcName, args, evalTp, argType.Clone())
	if err != nil {
		return nil, err
	}
	bf.tp = argType.Clone()
	bf.tp.DelFlag(mysql.NotNullFlag)
	switch evalTp {
	case types.ETString:
		return &builtinMaskNullStringSig{bf}, nil
	case types.ETDatetime, types.ETTimestamp:
		if !types.IsTypeTime(argType.GetType()) {
			return nil, errIncorrectArgs.GenWithStackByArgs("mask_null")
		}
		return &builtinMaskNullTimeSig{bf}, nil
	case types.ETDuration:
		if argType.GetType() != mysql.TypeDuration {
			return nil, errIncorrectArgs.GenWithStackByArgs("mask_null")
		}
		return &builtinMaskNullDurationSig{bf}, nil
	case types.ETInt:
		return &builtinMaskNullIntSig{bf}, nil
	case types.ETReal:
		return &builtinMaskNullRealSig{bf}, nil
	case types.ETDecimal:
		return &builtinMaskNullDecimalSig{bf}, nil
	default:
		return nil, errIncorrectArgs.GenWithStackByArgs("mask_null")
	}
}

type builtinMaskNullStringSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMaskNullStringSig) Clone() builtinFunc {
	newSig := &builtinMaskNullStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMaskNullStringSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	_, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil {
		return "", true, err
	}
	if isNull {
		return "", true, nil
	}
	return "", true, nil
}

type builtinMaskNullTimeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMaskNullTimeSig) Clone() builtinFunc {
	newSig := &builtinMaskNullTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMaskNullTimeSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	_, isNull, err := b.args[0].EvalTime(ctx, row)
	if err != nil {
		return types.ZeroTime, true, err
	}
	if isNull {
		return types.ZeroTime, true, nil
	}
	return types.ZeroTime, true, nil
}

type builtinMaskNullDurationSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMaskNullDurationSig) Clone() builtinFunc {
	newSig := &builtinMaskNullDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMaskNullDurationSig) evalDuration(ctx EvalContext, row chunk.Row) (types.Duration, bool, error) {
	_, isNull, err := b.args[0].EvalDuration(ctx, row)
	if err != nil {
		return types.Duration{}, true, err
	}
	if isNull {
		return types.Duration{}, true, nil
	}
	return types.Duration{}, true, nil
}

type builtinMaskNullIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMaskNullIntSig) Clone() builtinFunc {
	newSig := &builtinMaskNullIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMaskNullIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if isNull {
		return 0, true, nil
	}
	return 0, true, nil
}

type builtinMaskNullRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMaskNullRealSig) Clone() builtinFunc {
	newSig := &builtinMaskNullRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMaskNullRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	_, _, err := b.args[0].EvalReal(ctx, row)
	if err != nil {
		return 0, true, err
	}
	// mask_null(col) always return null, so it equal to just check error
	return 0, true, nil
}

type builtinMaskNullDecimalSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMaskNullDecimalSig) Clone() builtinFunc {
	newSig := &builtinMaskNullDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMaskNullDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	_, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if err != nil {
		return nil, true, err
	}
	if isNull {
		return nil, true, nil
	}
	return nil, true, nil
}

type maskPartialFunctionClass struct {
	baseFunctionClass
}

func (c *maskPartialFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	argType := args[0].GetType(ctx.GetEvalCtx())
	bf.tp = argType.Clone()
	if types.IsBinaryStr(argType) || types.IsBinaryStr(args[3].GetType(ctx.GetEvalCtx())) {
		return &builtinMaskPartialSig{bf}, nil
	}
	return &builtinMaskPartialUTF8Sig{bf}, nil
}

type builtinMaskPartialSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMaskPartialSig) Clone() builtinFunc {
	newSig := &builtinMaskPartialSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMaskPartialSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	preserveLeft, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	preserveRight, isNull, err := b.args[2].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pad, isNull, err := b.args[3].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if preserveLeft < 0 || preserveRight < 0 {
		return "", true, errIncorrectArgs.GenWithStackByArgs("mask_partial")
	}
	if len(pad) != 1 {
		return "", true, errIncorrectArgs.GenWithStackByArgs("mask_partial")
	}
	total := int64(len(str))
	if preserveLeft >= total || preserveRight >= total || preserveLeft > total-preserveRight {
		return str, false, nil
	}
	maskLen := int(total - preserveLeft - preserveRight)
	leftEnd := int(preserveLeft)
	rightStart := int(total - preserveRight)
	return str[:leftEnd] + strings.Repeat(pad, maskLen) + str[rightStart:], false, nil
}

type builtinMaskPartialUTF8Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMaskPartialUTF8Sig) Clone() builtinFunc {
	newSig := &builtinMaskPartialUTF8Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMaskPartialUTF8Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	preserveLeft, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	preserveRight, isNull, err := b.args[2].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	pad, isNull, err := b.args[3].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if preserveLeft < 0 || preserveRight < 0 {
		return "", true, errIncorrectArgs.GenWithStackByArgs("mask_partial")
	}
	padRunes := []rune(pad)
	if len(padRunes) != 1 {
		return "", true, errIncorrectArgs.GenWithStackByArgs("mask_partial")
	}
	runes := []rune(str)
	total := int64(len(runes))
	if preserveLeft >= total || preserveRight >= total || preserveLeft > total-preserveRight {
		return str, false, nil
	}
	maskLen := int(total - preserveLeft - preserveRight)
	leftEnd := int(preserveLeft)
	rightStart := int(total - preserveRight)
	maskChar := string(padRunes[0])
	return string(runes[:leftEnd]) + strings.Repeat(maskChar, maskLen) + string(runes[rightStart:]), false, nil
}

type maskDateFunctionClass struct {
	baseFunctionClass
}

func (c *maskDateFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argType := args[0].GetType(ctx.GetEvalCtx())
	if !types.IsTypeTime(argType.GetType()) {
		return nil, errIncorrectArgs.GenWithStackByArgs("mask_date")
	}
	evalTp := argType.EvalType()
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, evalTp, evalTp, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp = argType.Clone()
	return &builtinMaskDateSig{bf}, nil
}

type builtinMaskDateSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMaskDateSig) Clone() builtinFunc {
	newSig := &builtinMaskDateSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinMaskDateSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	_, isNull, err := b.args[0].EvalTime(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}
	dateStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return types.ZeroTime, true, err
	}
	dateVal, err := parseMaskDateLiteral(ctx, dateStr)
	if err != nil {
		return types.ZeroTime, true, err
	}
	tp := b.tp.GetType()
	fsp := b.tp.GetDecimal()
	if tp == mysql.TypeDate {
		return dateVal, false, nil
	}
	return types.NewTime(types.FromDate(dateVal.Year(), dateVal.Month(), dateVal.Day(), 0, 0, 0, 0), tp, fsp), false, nil
}

func parseMaskDateLiteral(ctx EvalContext, dateStr string) (types.Time, error) {
	if len(dateStr) != 10 || dateStr[4] != '-' || dateStr[7] != '-' {
		return types.ZeroTime, errIncorrectArgs.GenWithStackByArgs("mask_date")
	}
	for i, ch := range dateStr {
		if i == 4 || i == 7 {
			continue
		}
		if ch < '0' || ch > '9' {
			return types.ZeroTime, errIncorrectArgs.GenWithStackByArgs("mask_date")
		}
	}
	date, err := types.ParseTime(typeCtx(ctx), dateStr, mysql.TypeDate, 0)
	if err != nil {
		return types.ZeroTime, handleInvalidTimeError(ctx, err)
	}
	return date, nil
}
