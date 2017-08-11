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
)

var (
	_ functionClass = &logicAndFunctionClass{}
	_ functionClass = &logicOrFunctionClass{}
	_ functionClass = &logicXorFunctionClass{}
	_ functionClass = &isTrueOrFalseFunctionClass{}
	_ functionClass = &unaryOpFunctionClass{}
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
	_ builtinFunc = &builtinUnaryOpSig{}
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

func (c *logicAndFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinLogicAndSig{baseIntBuiltinFunc{bf}}
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

func (c *logicOrFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 1
	sig := &builtinLogicOrSig{baseIntBuiltinFunc{bf}}
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

func (c *logicXorFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinLogicXorSig{baseIntBuiltinFunc{bf}}
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

func (c *bitAndFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (c *bitOrFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (c *bitXorFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (c *leftShiftFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (c *rightShiftFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := c.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (c *isTrueOrFalseFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
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
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, argTp)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

func (c *bitNegFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

type unaryOpFunctionClass struct {
	baseFunctionClass

	op opcode.Op
}

func (c *unaryOpFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinUnaryOpSig{newBaseBuiltinFunc(args, ctx), c.op}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinUnaryOpSig struct {
	baseBuiltinFunc

	op opcode.Op
}

func (b *builtinUnaryOpSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	defer func() {
		if er := recover(); er != nil {
			err = errors.Errorf("%v", er)
		}
	}()
	aDatum := args[0]
	if aDatum.IsNull() {
		return
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	switch b.op {
	case opcode.Not:
		var n int64
		n, err = aDatum.ToBool(sc)
		if err != nil {
			err = errors.Trace(err)
		} else if n == 0 {
			d.SetInt64(1)
		} else {
			d.SetInt64(0)
		}
	case opcode.Plus:
		switch aDatum.Kind() {
		case types.KindInt64,
			types.KindUint64,
			types.KindFloat64,
			types.KindFloat32,
			types.KindMysqlDuration,
			types.KindMysqlTime,
			types.KindString,
			types.KindMysqlDecimal,
			types.KindBytes,
			types.KindMysqlHex,
			types.KindMysqlBit,
			types.KindMysqlEnum,
			types.KindMysqlSet:
			d = aDatum
		default:
			return d, errInvalidOperation.Gen("Unsupported type %v for op.Plus", aDatum.Kind())
		}
	default:
		return d, errInvalidOperation.Gen("Unsupported op %v for unary op", b.op)
	}
	return
}

type unaryNotFunctionClass struct {
	baseFunctionClass
}

func (c *unaryNotFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 1

	sig := &builtinUnaryNotSig{baseIntBuiltinFunc{bf}}
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

func (b *unaryMinusFunctionClass) handleIntOverflow(arg *Constant) (overflow bool) {
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
func (b *unaryMinusFunctionClass) typeInfer(argExpr Expression, ctx context.Context) (evalTp, bool) {
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
		overflow = b.handleIntOverflow(arg)
		if overflow {
			tp = tpDecimal
		}
	}
	return tp, overflow
}

func (b *unaryMinusFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	err = b.verifyArgs(args)
	if err != nil {
		return nil, errors.Trace(err)
	}

	argExpr, argExprTp := args[0], args[0].GetType()
	retTp, intOverflow := b.typeInfer(argExpr, ctx)

	var bf baseBuiltinFunc
	switch argExpr.GetTypeClass() {
	case types.ClassInt:
		if intOverflow {
			bf, err = newBaseBuiltinFuncWithTp(args, ctx, retTp, tpDecimal)
			sig = &builtinUnaryMinusDecimalSig{baseDecimalBuiltinFunc{bf}, true}
		} else {
			bf, err = newBaseBuiltinFuncWithTp(args, ctx, retTp, tpInt)
			sig = &builtinUnaryMinusIntSig{baseIntBuiltinFunc{bf}}
		}
		bf.tp.Decimal = 0
	case types.ClassDecimal:
		bf, err = newBaseBuiltinFuncWithTp(args, ctx, retTp, tpDecimal)
		bf.tp.Decimal = argExprTp.Decimal
		sig = &builtinUnaryMinusDecimalSig{baseDecimalBuiltinFunc{bf}, false}
	case types.ClassReal:
		bf, err = newBaseBuiltinFuncWithTp(args, ctx, retTp, tpReal)
		sig = &builtinUnaryMinusRealSig{baseRealBuiltinFunc{bf}}
	case types.ClassString:
		tp := argExpr.GetType().Tp
		if types.IsTypeTime(tp) || tp == mysql.TypeDuration {
			bf, err = newBaseBuiltinFuncWithTp(args, ctx, retTp, tpDecimal)
			sig = &builtinUnaryMinusDecimalSig{baseDecimalBuiltinFunc{bf}, false}
		} else {
			bf, err = newBaseBuiltinFuncWithTp(args, ctx, retTp, tpReal)
			sig = &builtinUnaryMinusRealSig{baseRealBuiltinFunc{bf}}
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

func (b *builtinUnaryMinusRealSig) evalReal(row []types.Datum) (res float64, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	var val float64
	val, isNull, err = b.args[0].EvalReal(row, sc)
	res = -val
	return
}

type isNullFunctionClass struct {
	baseFunctionClass
}

func (c *isNullFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
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
			argTp = tpTime
		} else if tp == mysql.TypeDuration {
			argTp = tpDuration
		} else {
			argTp = tpString
		}
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, argTp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 1
	var sig builtinFunc
	switch argTp {
	case tpInt:
		sig = &builtinIntIsNullSig{baseIntBuiltinFunc{bf}}
	case tpDecimal:
		sig = &builtinDecimalIsNullSig{baseIntBuiltinFunc{bf}}
	case tpReal:
		sig = &builtinRealIsNullSig{baseIntBuiltinFunc{bf}}
	case tpTime:
		sig = &builtinTimeIsNullSig{baseIntBuiltinFunc{bf}}
	case tpDuration:
		sig = &builtinDurationIsNullSig{baseIntBuiltinFunc{bf}}
	case tpString:
		sig = &builtinStringIsNullSig{baseIntBuiltinFunc{bf}}
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
