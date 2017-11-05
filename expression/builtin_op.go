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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"math"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/types"
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
	_ builtinFunc = &builtinUnaryNotSig{}
)

type logicAndFunctionClass struct {
	baseFunctionClass
}

func (c *logicAndFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinLogicAndSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LogicalAnd)
	sig.tp.Flen = 1
	return sig, nil
}

type builtinLogicAndSig struct {
	baseBuiltinFunc
}

func (b *builtinLogicAndSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil || (!isNull0 && arg0 == 0) {
		return 0, err != nil, errors.Trace(err)
	}
	arg1, isNull1, err := b.args[1].EvalInt(row, sc)
	if err != nil || (!isNull1 && arg1 == 0) {
		return 0, err != nil, errors.Trace(err)
	}
	if isNull0 || isNull1 {
		return 0, true, nil
	}
	return 1, false, nil
}

type logicOrFunctionClass struct {
	baseFunctionClass
}

func (c *logicOrFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	bf.tp.Flen = 1
	sig := &builtinLogicOrSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LogicalOr)
	return sig, nil
}

type builtinLogicOrSig struct {
	baseBuiltinFunc
}

func (b *builtinLogicOrSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	if !isNull0 && arg0 != 0 {
		return 1, false, nil
	}
	arg1, isNull1, err := b.args[1].EvalInt(row, sc)
	if err != nil {
		return 0, true, errors.Trace(err)
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

func (c *logicXorFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinLogicXorSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LogicalXor)
	sig.tp.Flen = 1
	return sig, nil
}

type builtinLogicXorSig struct {
	baseBuiltinFunc
}

func (b *builtinLogicXorSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if (arg0 != 0 && arg1 != 0) || (arg0 == 0 && arg1 == 0) {
		return 0, false, nil
	}
	return 1, false, nil
}

type bitAndFunctionClass struct {
	baseFunctionClass
}

func (c *bitAndFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinBitAndSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_BitAndSig)
	sig.tp.Flag |= mysql.UnsignedFlag
	return sig, nil
}

type builtinBitAndSig struct {
	baseBuiltinFunc
}

func (b *builtinBitAndSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return arg0 & arg1, false, nil
}

type bitOrFunctionClass struct {
	baseFunctionClass
}

func (c *bitOrFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinBitOrSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_BitOrSig)
	sig.tp.Flag |= mysql.UnsignedFlag
	return sig, nil
}

type builtinBitOrSig struct {
	baseBuiltinFunc
}

func (b *builtinBitOrSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return arg0 | arg1, false, nil
}

type bitXorFunctionClass struct {
	baseFunctionClass
}

func (c *bitXorFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinBitXorSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_BitXorSig)
	sig.tp.Flag |= mysql.UnsignedFlag
	return sig, nil
}

type builtinBitXorSig struct {
	baseBuiltinFunc
}

func (b *builtinBitXorSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return arg0 ^ arg1, false, nil
}

type leftShiftFunctionClass struct {
	baseFunctionClass
}

func (c *leftShiftFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinLeftShiftSig{bf}
	sig.tp.Flag |= mysql.UnsignedFlag
	return sig, nil
}

type builtinLeftShiftSig struct {
	baseBuiltinFunc
}

func (b *builtinLeftShiftSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return int64(uint64(arg0) << uint64(arg1)), false, nil
}

type rightShiftFunctionClass struct {
	baseFunctionClass
}

func (c *rightShiftFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt, types.ETInt)
	sig := &builtinRightShiftSig{bf}
	sig.tp.Flag |= mysql.UnsignedFlag
	return sig, nil
}

type builtinRightShiftSig struct {
	baseBuiltinFunc
}

func (b *builtinRightShiftSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	arg1, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return int64(uint64(arg0) >> uint64(arg1)), false, nil
}

type isTrueOrFalseFunctionClass struct {
	baseFunctionClass
	op opcode.Op
}

func (c *isTrueOrFalseFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	argTp := args[0].GetType().EvalType()
	if argTp != types.ETReal && argTp != types.ETDecimal {
		argTp = types.ETInt
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTp)
	bf.tp.Flen = 1

	var sig builtinFunc
	switch c.op {
	case opcode.IsTruth:
		switch argTp {
		case types.ETReal:
			sig = &builtinRealIsTrueSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RealIsTrue)
		case types.ETDecimal:
			sig = &builtinDecimalIsTrueSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_DecimalIsTrue)
		case types.ETInt:
			sig = &builtinIntIsTrueSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_IntIsTrue)
		}
	case opcode.IsFalsity:
		switch argTp {
		case types.ETReal:
			sig = &builtinRealIsFalseSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RealIsFalse)
		case types.ETDecimal:
			sig = &builtinDecimalIsFalseSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_DecimalIsFalse)
		case types.ETInt:
			sig = &builtinIntIsFalseSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_IntIsFalse)
		}
	}
	return sig, nil
}

type builtinRealIsTrueSig struct {
	baseBuiltinFunc
}

func (b *builtinRealIsTrueSig) evalInt(row types.Row) (int64, bool, error) {
	input, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	if isNull || input == 0 {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinDecimalIsTrueSig struct {
	baseBuiltinFunc
}

func (b *builtinDecimalIsTrueSig) evalInt(row types.Row) (int64, bool, error) {
	input, isNull, err := b.args[0].EvalDecimal(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	if isNull || input.IsZero() {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinIntIsTrueSig struct {
	baseBuiltinFunc
}

func (b *builtinIntIsTrueSig) evalInt(row types.Row) (int64, bool, error) {
	input, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	if isNull || input == 0 {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinRealIsFalseSig struct {
	baseBuiltinFunc
}

func (b *builtinRealIsFalseSig) evalInt(row types.Row) (int64, bool, error) {
	input, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	if isNull || input != 0 {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinDecimalIsFalseSig struct {
	baseBuiltinFunc
}

func (b *builtinDecimalIsFalseSig) evalInt(row types.Row) (int64, bool, error) {
	input, isNull, err := b.args[0].EvalDecimal(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	if isNull || !input.IsZero() {
		return 0, false, nil
	}
	return 1, false, nil
}

type builtinIntIsFalseSig struct {
	baseBuiltinFunc
}

func (b *builtinIntIsFalseSig) evalInt(row types.Row) (int64, bool, error) {
	input, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	if isNull || input != 0 {
		return 0, false, nil
	}
	return 1, false, nil
}

type bitNegFunctionClass struct {
	baseFunctionClass
}

func (c *bitNegFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt)
	bf.tp.Flag |= mysql.UnsignedFlag
	sig := &builtinBitNegSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_BitNegSig)
	return sig, nil
}

type builtinBitNegSig struct {
	baseBuiltinFunc
}

func (b *builtinBitNegSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return ^arg, false, nil
}

type unaryNotFunctionClass struct {
	baseFunctionClass
}

func (c *unaryNotFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt)
	bf.tp.Flen = 1

	sig := &builtinUnaryNotSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_UnaryNot)
	return sig, nil
}

type builtinUnaryNotSig struct {
	baseBuiltinFunc
}

func (b *builtinUnaryNotSig) evalInt(row types.Row) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if arg != 0 {
		return 0, false, nil
	}
	return 1, false, nil
}

type unaryMinusFunctionClass struct {
	baseFunctionClass
}

func (c *unaryMinusFunctionClass) handleIntOverflow(arg *Constant) (overflow bool) {
	if mysql.HasUnsignedFlag(arg.GetType().Flag) {
		uval := arg.Value.GetUint64()
		// -math.MinInt64 is 9223372036854775808, so if uval is more than 9223372036854775808, like
		// 9223372036854775809, -9223372036854775809 is less than math.MinInt64, overflow occurs.
		if uval > uint64(-math.MinInt64) {
			return true
		}
	} else {
		val := arg.Value.GetInt64()
		// The math.MinInt64 is -9223372036854775808, the math.MaxInt64 is 9223372036854775807,
		// which is less than abs(-9223372036854775808). When val == math.MinInt64, overflow occurs.
		if val == math.MinInt64 {
			return true
		}
	}
	return false
}

// typeInfer infers unaryMinus function return type. when the arg is an int constant and overflow,
// typerInfer will infers the return type as types.ETDecimal, not types.ETInt.
func (c *unaryMinusFunctionClass) typeInfer(argExpr Expression, ctx context.Context) (types.EvalType, bool) {
	tp := argExpr.GetType().EvalType()
	if tp != types.ETInt && tp != types.ETDecimal {
		tp = types.ETReal
	}

	sc := ctx.GetSessionVars().StmtCtx
	overflow := false
	// TODO: Handle float overflow.
	if arg, ok := argExpr.(*Constant); sc.InSelectStmt && ok &&
		tp == types.ETInt {
		overflow = c.handleIntOverflow(arg)
		if overflow {
			tp = types.ETDecimal
		}
	}
	return tp, overflow
}

func (c *unaryMinusFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	argExpr, argExprTp := args[0], args[0].GetType()
	_, intOverflow := c.typeInfer(argExpr, ctx)

	var bf baseBuiltinFunc
	switch argExprTp.EvalType() {
	case types.ETInt:
		if intOverflow {
			bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal)
			sig = &builtinUnaryMinusDecimalSig{bf, true}
			sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusDecimal)
		} else {
			bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETInt)
			sig = &builtinUnaryMinusIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusInt)
		}
		bf.tp.Decimal = 0
	case types.ETDecimal:
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal)
		bf.tp.Decimal = argExprTp.Decimal
		sig = &builtinUnaryMinusDecimalSig{bf, false}
		sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusDecimal)
	case types.ETReal:
		bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
		sig = &builtinUnaryMinusRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusReal)
	default:
		tp := argExpr.GetType().Tp
		if types.IsTypeTime(tp) || tp == mysql.TypeDuration {
			bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETDecimal, types.ETDecimal)
			sig = &builtinUnaryMinusDecimalSig{bf, false}
			sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusDecimal)
		} else {
			bf = newBaseBuiltinFuncWithTp(ctx, args, types.ETReal, types.ETReal)
			sig = &builtinUnaryMinusRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusReal)
		}
	}
	bf.tp.Flen = argExprTp.Flen + 1
	return sig, errors.Trace(err)
}

type builtinUnaryMinusIntSig struct {
	baseBuiltinFunc
}

func (b *builtinUnaryMinusIntSig) evalInt(row types.Row) (res int64, isNull bool, err error) {
	var val int64
	val, isNull, err = b.args[0].EvalInt(row, b.getCtx().GetSessionVars().StmtCtx)
	if err != nil || isNull {
		return val, isNull, errors.Trace(err)
	}

	if mysql.HasUnsignedFlag(b.args[0].GetType().Flag) {
		uval := uint64(val)
		if uval > uint64(-math.MinInt64) {
			return 0, false, types.ErrOverflow.GenByArgs("BIGINT", fmt.Sprintf("-%v", uval))
		} else if uval == uint64(-math.MinInt64) {
			return math.MinInt64, false, nil
		}
	} else if val == math.MinInt64 {
		return 0, false, types.ErrOverflow.GenByArgs("BIGINT", fmt.Sprintf("-%v", val))
	}
	return -val, false, nil
}

type builtinUnaryMinusDecimalSig struct {
	baseBuiltinFunc

	constantArgOverflow bool
}

func (b *builtinUnaryMinusDecimalSig) evalDecimal(row types.Row) (*types.MyDecimal, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx

	var dec *types.MyDecimal
	dec, isNull, err := b.args[0].EvalDecimal(row, sc)
	if err != nil || isNull {
		return dec, isNull, errors.Trace(err)
	}

	to := new(types.MyDecimal)
	err = types.DecimalSub(new(types.MyDecimal), dec, to)
	return to, err != nil, errors.Trace(err)
}

type builtinUnaryMinusRealSig struct {
	baseBuiltinFunc
}

func (b *builtinUnaryMinusRealSig) evalReal(row types.Row) (float64, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	val, isNull, err := b.args[0].EvalReal(row, sc)
	return -val, isNull, errors.Trace(err)
}

type isNullFunctionClass struct {
	baseFunctionClass
}

func (c *isNullFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTp := args[0].GetType().EvalType()
	if argTp == types.ETTimestamp {
		argTp = types.ETDatetime
	} else if argTp == types.ETJson {
		argTp = types.ETString
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTp)
	bf.tp.Flen = 1
	var sig builtinFunc
	switch argTp {
	case types.ETInt:
		sig = &builtinIntIsNullSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IntIsNull)
	case types.ETDecimal:
		sig = &builtinDecimalIsNullSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_DecimalIsNull)
	case types.ETReal:
		sig = &builtinRealIsNullSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_RealIsNull)
	case types.ETDatetime:
		sig = &builtinTimeIsNullSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_TimeIsNull)
	case types.ETDuration:
		sig = &builtinDurationIsNullSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_DurationIsNull)
	case types.ETString:
		sig = &builtinStringIsNullSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_StringIsNull)
	default:
		panic("unexpected types.EvalType")
	}
	return sig, nil
}

type builtinDecimalIsNullSig struct {
	baseBuiltinFunc
}

func evalIsNull(isNull bool, err error) (int64, bool, error) {
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	if isNull {
		return 1, false, nil
	}
	return 0, false, nil
}

func (b *builtinDecimalIsNullSig) evalInt(row types.Row) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalDecimal(row, b.ctx.GetSessionVars().StmtCtx)
	return evalIsNull(isNull, err)
}

type builtinDurationIsNullSig struct {
	baseBuiltinFunc
}

func (b *builtinDurationIsNullSig) evalInt(row types.Row) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalDuration(row, b.ctx.GetSessionVars().StmtCtx)
	return evalIsNull(isNull, err)
}

type builtinIntIsNullSig struct {
	baseBuiltinFunc
}

func (b *builtinIntIsNullSig) evalInt(row types.Row) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	return evalIsNull(isNull, err)
}

type builtinRealIsNullSig struct {
	baseBuiltinFunc
}

func (b *builtinRealIsNullSig) evalInt(row types.Row) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	return evalIsNull(isNull, err)
}

type builtinStringIsNullSig struct {
	baseBuiltinFunc
}

func (b *builtinStringIsNullSig) evalInt(row types.Row) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	return evalIsNull(isNull, err)
}

type builtinTimeIsNullSig struct {
	baseBuiltinFunc
}

func (b *builtinTimeIsNullSig) evalInt(row types.Row) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalTime(row, b.ctx.GetSessionVars().StmtCtx)
	return evalIsNull(isNull, err)
}
