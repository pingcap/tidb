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
	_ functionClass = &isTrueOpFunctionClass{}
	_ functionClass = &unaryOpFunctionClass{}
	_ functionClass = &unaryMinusFunctionClass{}
	_ functionClass = &isNullFunctionClass{}
)

var (
	_ builtinFunc = &builtinLogicAndSig{}
	_ builtinFunc = &builtinLogicOrSig{}
	_ builtinFunc = &builtinLogicXorSig{}
	_ builtinFunc = &builtinIsTrueOpSig{}
	_ builtinFunc = &builtinUnaryOpSig{}
	_ builtinFunc = &builtinUnaryMinusIntSig{}
	_ builtinFunc = &builtinIsNullSig{}
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

type isTrueOpFunctionClass struct {
	baseFunctionClass

	op opcode.Op
}

func (c *isTrueOpFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinIsTrueOpSig{newBaseBuiltinFunc(args, ctx), c.op}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinIsTrueOpSig struct {
	baseBuiltinFunc

	op opcode.Op
}

func (b *builtinIsTrueOpSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	var boolVal bool
	if !args[0].IsNull() {
		iVal, err := args[0].ToBool(b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return d, errors.Trace(err)
		}
		if (b.op == opcode.IsTruth && iVal == 1) || (b.op == opcode.IsFalsity && iVal == 0) {
			boolVal = true
		}
	}
	d.SetInt64(boolToInt64(boolVal))
	return
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
	case opcode.BitNeg:
		var n int64
		// for bit operation, we will use int64 first, then return uint64
		n, err = aDatum.ToInt64(sc)
		if err != nil {
			return d, errors.Trace(err)
		}
		d.SetUint64(uint64(^n))
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
	if arg, ok := argExpr.(*Constant); sc.IgnoreOverflow && ok &&
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

	argExpr := args[0]
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
	case types.ClassDecimal:
		bf, err = newBaseBuiltinFuncWithTp(args, ctx, retTp, tpDecimal)
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
	sig := &builtinIsNullSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinIsNullSig struct {
	baseBuiltinFunc
}

// eval evals a builtinIsNullSig.
// See https://dev.mysql.com/doc/refman/5.7/en/comparison-operators.html#function_isnull
func (b *builtinIsNullSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	if args[0].IsNull() {
		d.SetInt64(1)
	} else {
		d.SetInt64(0)
	}
	return d, nil
}
