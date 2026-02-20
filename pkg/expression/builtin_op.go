// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &logicAndFunctionClass{}
	_ functionClass = &logicOrFunctionClass{}
	_ functionClass = &logicXorFunctionClass{}
	_ functionClass = &isTrueOrFalseFunctionClass{}
	_ functionClass = &unaryMinusFunctionClass{}
	_ functionClass = &isNullFunctionClass{}
	_ functionClass = &unaryNotFunctionClass{}
)

var (
	_ builtinFunc = &builtinLogicAndSig{}
	_ builtinFunc = &builtinLogicOrSig{}
	_ builtinFunc = &builtinLogicXorSig{}
	_ builtinFunc = &builtinRealIsTrueSig{}
	_ builtinFunc = &builtinDecimalIsTrueSig{}
	_ builtinFunc = &builtinIntIsTrueSig{}
	_ builtinFunc = &builtinRealIsFalseSig{}
	_ builtinFunc = &builtinDecimalIsFalseSig{}
	_ builtinFunc = &builtinIntIsFalseSig{}
	_ builtinFunc = &builtinUnaryMinusIntSig{}
	_ builtinFunc = &builtinDecimalIsNullSig{}
	_ builtinFunc = &builtinDurationIsNullSig{}
	_ builtinFunc = &builtinIntIsNullSig{}
	_ builtinFunc = &builtinRealIsNullSig{}
	_ builtinFunc = &builtinStringIsNullSig{}
	_ builtinFunc = &builtinTimeIsNullSig{}
	_ builtinFunc = &builtinUnaryNotRealSig{}
	_ builtinFunc = &builtinUnaryNotDecimalSig{}
	_ builtinFunc = &builtinUnaryNotIntSig{}
)

type logicAndFunctionClass struct {
	baseFunctionClass
}

func (c *logicAndFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, err
	}
	args[0], err = wrapWithIsTrue(ctx, true, args[0], false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	args[1], err = wrapWithIsTrue(ctx, true, args[1], false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinLogicAndSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LogicalAnd)
	sig.tp.SetFlen(1)
	return sig, nil
}

type builtinLogicAndSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLogicAndSig) Clone() builtinFunc {
	newSig := &builtinLogicAndSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLogicAndSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil || (!isNull0 && arg0 == 0) {
		return 0, err != nil, err
	}
	arg1, isNull1, err := b.args[1].EvalInt(ctx, row)
	if err != nil || (!isNull1 && arg1 == 0) {
		return 0, err != nil, err
	}
	if isNull0 || isNull1 {
		return 0, true, nil
	}
	return 1, false, nil
}

type logicOrFunctionClass struct {
	baseFunctionClass
}

func (c *logicOrFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, err
	}
	args[0], err = wrapWithIsTrue(ctx, true, args[0], false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	args[1], err = wrapWithIsTrue(ctx, true, args[1], false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1)
	sig := &builtinLogicOrSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LogicalOr)
	return sig, nil
}

type builtinLogicOrSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLogicOrSig) Clone() builtinFunc {
	newSig := &builtinLogicOrSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLogicOrSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if !isNull0 && arg0 != 0 {
		return 1, false, nil
	}
	arg1, isNull1, err := b.args[1].EvalInt(ctx, row)
	if err != nil {
		return 0, true, err
	}
	if !isNull1 && arg1 != 0 {
		return 1, false, nil
	}
	if isNull0 || isNull1 {
		return 0, true, nil
	}
	return 0, false, nil
}

type logicXorFunctionClass struct {
	baseFunctionClass
}

func (c *logicXorFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, err
	}
	args[0], err = wrapWithIsTrue(ctx, true, args[0], false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	args[1], err = wrapWithIsTrue(ctx, true, args[1], false)
	if err != nil {
		return nil, errors.Trace(err)
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinLogicXorSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LogicalXor)
	sig.tp.SetFlen(1)
	return sig, nil
}

type builtinLogicXorSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLogicXorSig) Clone() builtinFunc {
	newSig := &builtinLogicXorSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLogicXorSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	arg1, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if (arg0 != 0 && arg1 != 0) || (arg0 == 0 && arg1 == 0) {
		return 0, false, nil
	}
	return 1, false, nil
}

type bitAndFunctionClass struct {
	baseFunctionClass
}

func (c *bitAndFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinBitAndSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_BitAndSig)
	sig.tp.AddFlag(mysql.UnsignedFlag)
	return sig, nil
}

type builtinBitAndSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinBitAndSig) Clone() builtinFunc {
	newSig := &builtinBitAndSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinBitAndSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	arg1, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return arg0 & arg1, false, nil
}

type bitOrFunctionClass struct {
	baseFunctionClass
}

func (c *bitOrFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinBitOrSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_BitOrSig)
	sig.tp.AddFlag(mysql.UnsignedFlag)
	return sig, nil
}

type builtinBitOrSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinBitOrSig) Clone() builtinFunc {
	newSig := &builtinBitOrSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinBitOrSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	arg1, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return arg0 | arg1, false, nil
}

type bitXorFunctionClass struct {
	baseFunctionClass
}

func (c *bitXorFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinBitXorSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_BitXorSig)
	sig.tp.AddFlag(mysql.UnsignedFlag)
	return sig, nil
}

type builtinBitXorSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinBitXorSig) Clone() builtinFunc {
	newSig := &builtinBitXorSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinBitXorSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	arg1, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return arg0 ^ arg1, false, nil
}

type leftShiftFunctionClass struct {
	baseFunctionClass
}

func (c *leftShiftFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinLeftShiftSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LeftShift)
	sig.tp.AddFlag(mysql.UnsignedFlag)
	return sig, nil
}

type builtinLeftShiftSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLeftShiftSig) Clone() builtinFunc {
	newSig := &builtinLeftShiftSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLeftShiftSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	arg1, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(uint64(arg0) << uint64(arg1)), false, nil
}

type rightShiftFunctionClass struct {
	baseFunctionClass
}

func (c *rightShiftFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	sig := &builtinRightShiftSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_RightShift)
	sig.tp.AddFlag(mysql.UnsignedFlag)
	return sig, nil
}

type builtinRightShiftSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRightShiftSig) Clone() builtinFunc {
	newSig := &builtinRightShiftSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinRightShiftSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	arg0, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	arg1, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(uint64(arg0) >> uint64(arg1)), false, nil
}

