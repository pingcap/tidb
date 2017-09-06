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
	"github.com/pingcap/tidb/util/types"
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	sig := &builtinLogicAndSig{baseIntBuiltinFunc{bf}}
	sig.setPbCode(tipb.ScalarFuncSig_LogicalAnd)
	sig.tp.Flen = 1
	return sig.setSelf(sig), nil
}

type builtinLogicAndSig struct {
	baseIntBuiltinFunc
}

func (b *builtinLogicAndSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil || (!isNull0 && arg0 == 0) {
		return 0, false, errors.Trace(err)
	}
	arg1, isNull1, err := b.args[1].EvalInt(row, sc)
	if err != nil || (!isNull1 && arg1 == 0) {
		return 0, false, errors.Trace(err)
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	bf.tp.Flen = 1
	sig := &builtinLogicOrSig{baseIntBuiltinFunc{bf}}
	sig.setPbCode(tipb.ScalarFuncSig_LogicalOr)
	return sig.setSelf(sig), nil
}

type builtinLogicOrSig struct {
	baseIntBuiltinFunc
}

func (b *builtinLogicOrSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	arg0, isNull0, err := b.args[0].EvalInt(row, sc)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if !isNull0 && arg0 != 0 {
		return 1, false, nil
	}
	arg1, isNull1, err := b.args[1].EvalInt(row, sc)
	if err != nil {
		return 0, false, errors.Trace(err)
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	sig := &builtinLogicXorSig{baseIntBuiltinFunc{bf}}
	sig.setPbCode(tipb.ScalarFuncSig_LogicalXor)
	sig.tp.Flen = 1
	return sig.setSelf(sig), nil
}

type builtinLogicXorSig struct {
	baseIntBuiltinFunc
}

func (b *builtinLogicXorSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	sig := &builtinBitAndSig{baseIntBuiltinFunc{bf}}
	sig.tp.Flag |= mysql.UnsignedFlag
	return sig.setSelf(sig), nil
}

type builtinBitAndSig struct {
	baseIntBuiltinFunc
}

func (b *builtinBitAndSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	sig := &builtinBitOrSig{baseIntBuiltinFunc{bf}}
	sig.tp.Flag |= mysql.UnsignedFlag
	return sig.setSelf(sig), nil
}

type builtinBitOrSig struct {
	baseIntBuiltinFunc
}

func (b *builtinBitOrSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	sig := &builtinBitXorSig{baseIntBuiltinFunc{bf}}
	sig.tp.Flag |= mysql.UnsignedFlag
	return sig.setSelf(sig), nil
}

type builtinBitXorSig struct {
	baseIntBuiltinFunc
}

func (b *builtinBitXorSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	sig := &builtinLeftShiftSig{baseIntBuiltinFunc{bf}}
	sig.tp.Flag |= mysql.UnsignedFlag
	return sig.setSelf(sig), nil
}

type builtinLeftShiftSig struct {
	baseIntBuiltinFunc
}

func (b *builtinLeftShiftSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	sig := &builtinRightShiftSig{baseIntBuiltinFunc{bf}}
	sig.tp.Flag |= mysql.UnsignedFlag
	return sig.setSelf(sig), nil
}

type builtinRightShiftSig struct {
	baseIntBuiltinFunc
}

func (b *builtinRightShiftSig) evalInt(row []types.Datum) (int64, bool, error) {
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

	argTp := tpInt
	switch args[0].GetTypeClass() {
	case types.ClassReal:
		argTp = tpReal
	case types.ClassDecimal:
		argTp = tpDecimal
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, argTp)
	bf.tp.Flen = 1

	var sig builtinFunc
	switch c.op {
	case opcode.IsTruth:
		switch argTp {
		case tpReal:
			sig = &builtinRealIsTrueSig{baseIntBuiltinFunc{bf}}
		case tpDecimal:
			sig = &builtinDecimalIsTrueSig{baseIntBuiltinFunc{bf}}
		case tpInt:
			sig = &builtinIntIsTrueSig{baseIntBuiltinFunc{bf}}
		}
	case opcode.IsFalsity:
		switch argTp {
		case tpReal:
			sig = &builtinRealIsFalseSig{baseIntBuiltinFunc{bf}}
		case tpDecimal:
			sig = &builtinDecimalIsFalseSig{baseIntBuiltinFunc{bf}}
		case tpInt:
			sig = &builtinIntIsFalseSig{baseIntBuiltinFunc{bf}}
		}
	}
	return sig.setSelf(sig), nil
}

type builtinRealIsTrueSig struct {
	baseIntBuiltinFunc
}

func (b *builtinRealIsTrueSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	baseIntBuiltinFunc
}

func (b *builtinDecimalIsTrueSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	baseIntBuiltinFunc
}

func (b *builtinIntIsTrueSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	baseIntBuiltinFunc
}

func (b *builtinRealIsFalseSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	baseIntBuiltinFunc
}

func (b *builtinDecimalIsFalseSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	baseIntBuiltinFunc
}

func (b *builtinIntIsFalseSig) evalInt(row []types.Datum) (int64, bool, error) {
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
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt)
	bf.tp.Flag |= mysql.UnsignedFlag
	sig := &builtinBitNegSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinBitNegSig struct {
	baseIntBuiltinFunc
}

func (b *builtinBitNegSig) evalInt(row []types.Datum) (int64, bool, error) {
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

	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt)
	bf.tp.Flen = 1

	sig := &builtinUnaryNotSig{baseIntBuiltinFunc{bf}}
	sig.setPbCode(tipb.ScalarFuncSig_UnaryNot)
	return sig.setSelf(sig), nil
}

type builtinUnaryNotSig struct {
	baseIntBuiltinFunc
}

func (b *builtinUnaryNotSig) evalInt(row []types.Datum) (int64, bool, error) {
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
// typerInfer will infers the return type as tpDecimal, not tpInt.
func (c *unaryMinusFunctionClass) typeInfer(argExpr Expression, ctx context.Context) (evalTp, bool) {
	tp := tpInt
	switch argExpr.GetTypeClass() {
	case types.ClassString, types.ClassReal:
		tp = tpReal
	case types.ClassDecimal:
		tp = tpDecimal
	}

	sc := ctx.GetSessionVars().StmtCtx
	overflow := false
	// TODO: Handle float overflow.
	if arg, ok := argExpr.(*Constant); sc.InSelectStmt && ok &&
		arg.GetTypeClass() == types.ClassInt {
		overflow = c.handleIntOverflow(arg)
		if overflow {
			tp = tpDecimal
		}
	}
	return tp, overflow
}

func (c *unaryMinusFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	argExpr, argExprTp := args[0], args[0].GetType()
	retTp, intOverflow := c.typeInfer(argExpr, ctx)

	var bf baseBuiltinFunc
	switch argExpr.GetTypeClass() {
	case types.ClassInt:
		if intOverflow {
			bf = newBaseBuiltinFuncWithTp(args, ctx, retTp, tpDecimal)
			sig = &builtinUnaryMinusDecimalSig{baseDecimalBuiltinFunc{bf}, true}
			sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusDecimal)
		} else {
			bf = newBaseBuiltinFuncWithTp(args, ctx, retTp, tpInt)
			sig = &builtinUnaryMinusIntSig{baseIntBuiltinFunc{bf}}
			sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusInt)
		}
		bf.tp.Decimal = 0
	case types.ClassDecimal:
		bf = newBaseBuiltinFuncWithTp(args, ctx, retTp, tpDecimal)
		bf.tp.Decimal = argExprTp.Decimal
		sig = &builtinUnaryMinusDecimalSig{baseDecimalBuiltinFunc{bf}, false}
		sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusDecimal)
	case types.ClassReal:
		bf = newBaseBuiltinFuncWithTp(args, ctx, retTp, tpReal)
		sig = &builtinUnaryMinusRealSig{baseRealBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusReal)
	case types.ClassString:
		tp := argExpr.GetType().Tp
		if types.IsTypeTime(tp) || tp == mysql.TypeDuration {
			bf = newBaseBuiltinFuncWithTp(args, ctx, retTp, tpDecimal)
			sig = &builtinUnaryMinusDecimalSig{baseDecimalBuiltinFunc{bf}, false}
			sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusDecimal)
		} else {
			bf = newBaseBuiltinFuncWithTp(args, ctx, retTp, tpReal)
			sig = &builtinUnaryMinusRealSig{baseRealBuiltinFunc{bf}}
			sig.setPbCode(tipb.ScalarFuncSig_UnaryMinusReal)
		}
	}
	bf.tp.Flen = argExprTp.Flen + 1
	return sig.setSelf(sig), errors.Trace(err)
}

type builtinUnaryMinusIntSig struct {
	baseIntBuiltinFunc
}

func (b *builtinUnaryMinusIntSig) evalInt(row []types.Datum) (res int64, isNull bool, err error) {
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
	return -val, false, errors.Trace(err)
}

type builtinUnaryMinusDecimalSig struct {
	baseDecimalBuiltinFunc

	constantArgOverflow bool
}

func (b *builtinUnaryMinusDecimalSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	sc := b.getCtx().GetSessionVars().StmtCtx

	var dec *types.MyDecimal
	dec, isNull, err := b.args[0].EvalDecimal(row, sc)
	if err != nil || isNull {
		return dec, isNull, errors.Trace(err)
	}

	to := new(types.MyDecimal)
	err = types.DecimalSub(new(types.MyDecimal), dec, to)
	return to, false, errors.Trace(err)
}

type builtinUnaryMinusRealSig struct {
	baseRealBuiltinFunc
}

func (b *builtinUnaryMinusRealSig) evalReal(row []types.Datum) (float64, bool, error) {
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
	tc := args[0].GetType().ToClass()
	var argTp evalTp
	switch tc {
	case types.ClassInt:
		argTp = tpInt
	case types.ClassDecimal:
		argTp = tpDecimal
	case types.ClassReal:
		argTp = tpReal
	default:
		tp := args[0].GetType().Tp
		if types.IsTypeTime(tp) {
			argTp = tpDatetime
		} else if tp == mysql.TypeDuration {
			argTp = tpDuration
		} else {
			argTp = tpString
		}
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, argTp)
	bf.tp.Flen = 1
	var sig builtinFunc
	switch argTp {
	case tpInt:
		sig = &builtinIntIsNullSig{baseIntBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_IntIsNull)
	case tpDecimal:
		sig = &builtinDecimalIsNullSig{baseIntBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_DecimalIsNull)
	case tpReal:
		sig = &builtinRealIsNullSig{baseIntBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_RealIsNull)
	case tpDatetime:
		sig = &builtinTimeIsNullSig{baseIntBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_TimeIsNull)
	case tpDuration:
		sig = &builtinDurationIsNullSig{baseIntBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_DurationIsNull)
	case tpString:
		sig = &builtinStringIsNullSig{baseIntBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_StringIsNull)
	default:
		panic("unexpected evalTp")
	}
	return sig.setSelf(sig), nil
}

type builtinDecimalIsNullSig struct {
	baseIntBuiltinFunc
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

func (b *builtinDecimalIsNullSig) evalInt(row []types.Datum) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalDecimal(row, b.ctx.GetSessionVars().StmtCtx)
	return evalIsNull(isNull, err)
}

type builtinDurationIsNullSig struct {
	baseIntBuiltinFunc
}

func (b *builtinDurationIsNullSig) evalInt(row []types.Datum) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalDuration(row, b.ctx.GetSessionVars().StmtCtx)
	return evalIsNull(isNull, err)
}

type builtinIntIsNullSig struct {
	baseIntBuiltinFunc
}

func (b *builtinIntIsNullSig) evalInt(row []types.Datum) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	return evalIsNull(isNull, err)
}

type builtinRealIsNullSig struct {
	baseIntBuiltinFunc
}

func (b *builtinRealIsNullSig) evalInt(row []types.Datum) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	return evalIsNull(isNull, err)
}

type builtinStringIsNullSig struct {
	baseIntBuiltinFunc
}

func (b *builtinStringIsNullSig) evalInt(row []types.Datum) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	return evalIsNull(isNull, err)
}

type builtinTimeIsNullSig struct {
	baseIntBuiltinFunc
}

func (b *builtinTimeIsNullSig) evalInt(row []types.Datum) (int64, bool, error) {
	_, isNull, err := b.args[0].EvalTime(row, b.ctx.GetSessionVars().StmtCtx)
	return evalIsNull(isNull, err)
}
