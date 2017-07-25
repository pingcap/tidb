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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ functionClass = &logicAndFunctionClass{}
	_ functionClass = &logicOrFunctionClass{}
	_ functionClass = &logicXorFunctionClass{}
	_ functionClass = &bitOpFunctionClass{}
	_ functionClass = &isTrueOpFunctionClass{}
	_ functionClass = &unaryOpFunctionClass{}
	_ functionClass = &isNullFunctionClass{}
)

var (
	_ builtinFunc = &builtinLogicAndSig{}
	_ builtinFunc = &builtinLogicOrSig{}
	_ builtinFunc = &builtinLogicXorSig{}
	_ builtinFunc = &builtinBitOpSig{}
	_ builtinFunc = &builtinIsTrueOpSig{}
	_ builtinFunc = &builtinUnaryOpSig{}
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
	sig := &builtinLogicOrSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLogicOrSig struct {
	baseBuiltinFunc
}

func (b *builtinLogicOrSig) eval(row []types.Datum) (d types.Datum, err error) {
	leftDatum, err := b.args[0].Eval(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	if !leftDatum.IsNull() {
		var x int64
		x, err = leftDatum.ToBool(sc)
		if err != nil {
			return d, errors.Trace(err)
		} else if x == 1 {
			// false && any other types is false
			d.SetInt64(x)
			return
		}
	}
	rightDatum, err := b.args[1].Eval(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if !rightDatum.IsNull() {
		var y int64
		y, err = rightDatum.ToBool(sc)
		if err != nil {
			return d, errors.Trace(err)
		} else if y == 1 {
			d.SetInt64(y)
			return
		}
	}
	if leftDatum.IsNull() || rightDatum.IsNull() {
		return
	}
	d.SetInt64(int64(0))
	return
}

type logicXorFunctionClass struct {
	baseFunctionClass
}

func (c *logicXorFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinLogicXorSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinLogicXorSig struct {
	baseBuiltinFunc
}

func (b *builtinLogicXorSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	leftDatum := args[0]
	righDatum := args[1]
	if leftDatum.IsNull() || righDatum.IsNull() {
		return
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	x, err := leftDatum.ToBool(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	y, err := righDatum.ToBool(sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	if x == y {
		d.SetInt64(zeroI64)
	} else {
		d.SetInt64(oneI64)
	}
	return
}

type bitOpFunctionClass struct {
	baseFunctionClass

	op opcode.Op
}

func (c *bitOpFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinBitOpSig{newBaseBuiltinFunc(args, ctx), c.op}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinBitOpSig struct {
	baseBuiltinFunc

	op opcode.Op
}

func (s *builtinBitOpSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := s.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	sc := s.ctx.GetSessionVars().StmtCtx
	a, b, err := types.CoerceDatum(sc, args[0], args[1])
	if err != nil {
		return d, errors.Trace(err)
	}
	if a.IsNull() || b.IsNull() {
		return
	}

	x, err := a.ToInt64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	y, err := b.ToInt64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	// use a int64 for bit operator, return uint64
	switch s.op {
	case opcode.And:
		d.SetUint64(uint64(x & y))
	case opcode.Or:
		d.SetUint64(uint64(x | y))
	case opcode.Xor:
		d.SetUint64(uint64(x ^ y))
	case opcode.RightShift:
		d.SetUint64(uint64(x) >> uint64(y))
	case opcode.LeftShift:
		d.SetUint64(uint64(x) << uint64(y))
	default:
		return d, errInvalidOperation.Gen("invalid op %v in bit operation", s.op)
	}
	return
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
	case opcode.Minus:
		switch aDatum.Kind() {
		case types.KindInt64:
			d.SetInt64(-aDatum.GetInt64())
		case types.KindUint64:
			d.SetInt64(-int64(aDatum.GetUint64()))
		case types.KindFloat64:
			d.SetFloat64(-aDatum.GetFloat64())
		case types.KindFloat32:
			d.SetFloat32(-aDatum.GetFloat32())
		case types.KindMysqlDuration:
			dec := new(types.MyDecimal)
			err = types.DecimalSub(new(types.MyDecimal), aDatum.GetMysqlDuration().ToNumber(), dec)
			d.SetMysqlDecimal(dec)
		case types.KindMysqlTime:
			dec := new(types.MyDecimal)
			err = types.DecimalSub(new(types.MyDecimal), aDatum.GetMysqlTime().ToNumber(), dec)
			d.SetMysqlDecimal(dec)
		case types.KindString, types.KindBytes:
			f, err1 := types.StrToFloat(sc, aDatum.GetString())
			err = errors.Trace(err1)
			d.SetFloat64(-f)
		case types.KindMysqlDecimal:
			dec := new(types.MyDecimal)
			err = types.DecimalSub(new(types.MyDecimal), aDatum.GetMysqlDecimal(), dec)
			d.SetMysqlDecimal(dec)
		case types.KindMysqlHex:
			d.SetFloat64(-aDatum.GetMysqlHex().ToNumber())
		case types.KindMysqlBit:
			d.SetFloat64(-aDatum.GetMysqlBit().ToNumber())
		case types.KindMysqlEnum:
			d.SetFloat64(-aDatum.GetMysqlEnum().ToNumber())
		case types.KindMysqlSet:
			d.SetFloat64(-aDatum.GetMysqlSet().ToNumber())
		default:
			return d, errInvalidOperation.Gen("Unsupported type %v for op.Minus", aDatum.Kind())
		}
	default:
		return d, errInvalidOperation.Gen("Unsupported op %v for unary op", b.op)
	}
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
