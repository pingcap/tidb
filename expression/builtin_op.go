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
	_ functionClass = &andandFunctionClass{}
	_ functionClass = &ororFunctionClass{}
	_ functionClass = &logicXorFunctionClass{}
	_ functionClass = &bitOpFunctionClass{}
	_ functionClass = &isTrueOpFunctionClass{}
	_ functionClass = &unaryOpFunctionClass{}
	_ functionClass = &unaryMinusFunctionClass{}
	_ functionClass = &isNullFunctionClass{}
)

var (
	_ builtinFunc = &builtinAndAndSig{}
	_ builtinFunc = &builtinOrOrSig{}
	_ builtinFunc = &builtinLogicXorSig{}
	_ builtinFunc = &builtinBitOpSig{}
	_ builtinFunc = &builtinIsTrueOpSig{}
	_ builtinFunc = &builtinUnaryOpSig{}
	_ builtinFunc = &builtinUnaryMinusIntSig{}
	_ builtinFunc = &builtinIsNullSig{}
)

type andandFunctionClass struct {
	baseFunctionClass
}

func (c *andandFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinAndAndSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinAndAndSig struct {
	baseBuiltinFunc
}

func (b *builtinAndAndSig) eval(row []types.Datum) (d types.Datum, err error) {
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
		} else if x == 0 {
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
		} else if y == 0 {
			d.SetInt64(y)
			return
		}
	}
	if leftDatum.IsNull() || rightDatum.IsNull() {
		return
	}
	d.SetInt64(int64(1))
	return
}

type ororFunctionClass struct {
	baseFunctionClass
}

func (c *ororFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinOrOrSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinOrOrSig struct {
	baseBuiltinFunc
}

func (b *builtinOrOrSig) eval(row []types.Datum) (d types.Datum, err error) {
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
			val := aDatum.GetInt64()
			if val == math.MinInt64 {
				err = types.ErrOverflow.GenByArgs("BIGINT", fmt.Sprintf("-%v", val))
			}
			d.SetInt64(-val)
		case types.KindUint64:
			uval := aDatum.GetUint64()
			if uval > uint64(-math.MinInt64) { // 9223372036854775808
				// -uval will overflow
				err = types.ErrOverflow.GenByArgs("BIGINT", fmt.Sprintf("-%v", uval))
			} else {
				d.SetInt64(-int64(uval))
			}
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

type unaryMinusFunctionClass struct {
	baseFunctionClass
}

func (b *unaryMinusFunctionClass) typeInfer(argExpr Expression, bf *baseBuiltinFunc) bool {
	bf.tp.Init(mysql.TypeLonglong)
	switch argExpr.GetType().Tp {
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeDouble, mysql.TypeFloat,
		mysql.TypeDatetime, mysql.TypeDuration, mysql.TypeTimestamp:
		bf.tp.Tp = mysql.TypeDouble
	case mysql.TypeNewDecimal:
		bf.tp.Tp = mysql.TypeNewDecimal
	}
	types.SetBinChsClnFlag(bf.tp)

	overflow := false
	if arg, ok := argExpr.(*Constant); ok {
		switch arg.Value.Kind() {
		case types.KindUint64:
			uval := arg.Value.GetUint64()
			if uval > uint64(-math.MinInt64) {
				overflow = true
				bf.tp.Tp = mysql.TypeNewDecimal
			}
		case types.KindInt64:
			val := arg.Value.GetInt64()
			if val == math.MinInt64 {
				overflow = true
				bf.tp.Tp = mysql.TypeNewDecimal
			}
		}
	}
	return overflow
}

func (b *unaryMinusFunctionClass) getFunction(args []Expression, ctx context.Context) (sig builtinFunc, err error) {
	bf := newBaseBuiltinFunc(args, ctx)
	argExpr := args[0]
	overflow := b.typeInfer(argExpr, &bf)
	if overflow {
		sig = &builtinUnaryMinusDecimalSig{baseDecimalBuiltinFunc{bf}, true}
		return sig.setSelf(sig), errors.Trace(b.verifyArgs(args))
	}

	switch argExpr.GetTypeClass() {
	case types.ClassInt:
		sig = &builtinUnaryMinusIntSig{baseIntBuiltinFunc{bf}}
	case types.ClassDecimal:
		sig = &builtinUnaryMinusDecimalSig{baseDecimalBuiltinFunc{bf}, false}
	case types.ClassReal:
		sig = &builtinUnaryMinusRealSig{baseRealBuiltinFunc{bf}}
	case types.ClassString:
		tp := argExpr.GetType().Tp
		if types.IsTypeTime(tp) {
			sig = &builtinUnaryMinusDecimalSig{baseDecimalBuiltinFunc{bf}, false}
		} else if tp == mysql.TypeDuration {
			sig = &builtinUnaryMinusDecimalSig{baseDecimalBuiltinFunc{bf}, false}
		} else {
			sig = &builtinUnaryMinusRealSig{baseRealBuiltinFunc{bf}}
		}
	}

	return sig.setSelf(sig), errors.Trace(b.verifyArgs(args))
}

func unaryMinusDecimal(dec *types.MyDecimal) (to *types.MyDecimal, err error) {
	to = new(types.MyDecimal)
	err = types.DecimalSub(new(types.MyDecimal), dec, to)
	return
}

type builtinUnaryMinusIntSig struct {
	baseIntBuiltinFunc
}

// func (b *builtinUnaryMinusIntSig) eval(row []types.Datum) (d types.Datum, err error) {
// 	res, isNull, err := b.self.evalInt(row)
// 	sc := b.getCtx().GetSessionVars().StmtCtx
// 	if !sc.InUpdateOrDeleteStmt && terror.ErrorEqual(err, types.ErrOverflow) {
// 		var (
// 			dec, to *types.MyDecimal
// 		)

// 		dec, isNull, err = b.args[0].EvalDecimal(row, sc)
// 		if err != nil || isNull {
// 			return d, errors.Trace(err)
// 		}
// 		to, err = unaryMinusDecimal(dec)
// 		d.SetMysqlDecimal(to)
// 		b.getRetTp().Tp = mysql.TypeNewDecimal
// 		return
// 	}

// 	if err != nil || isNull {
// 		return d, errors.Trace(err)
// 	}
// 	if mysql.HasUnsignedFlag(b.tp.Flag) {
// 		d.SetUint64(uint64(res))
// 	} else {
// 		d.SetInt64(res)
// 	}
// 	return
// }

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
	return -val, false, err
}

type builtinUnaryMinusDecimalSig struct {
	baseDecimalBuiltinFunc

	constantArgOverflow bool
}

func (b *builtinUnaryMinusDecimalSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	var (
		dec, to *types.MyDecimal
	)

	sc := b.getCtx().GetSessionVars().StmtCtx
	aDatum, err := b.args[0].Eval(row)
	if err != nil || aDatum.IsNull() {
		return nil, aDatum.IsNull(), errors.Trace(err)
	}
	switch aDatum.Kind() {
	case types.KindMysqlTime:
		dec := new(types.MyDecimal)
		err := types.DecimalSub(new(types.MyDecimal), aDatum.GetMysqlTime().ToNumber(), dec)
		return dec, false, err
	case types.KindMysqlDuration:
		dec := new(types.MyDecimal)
		err := types.DecimalSub(new(types.MyDecimal), aDatum.GetMysqlDuration().ToNumber(), dec)
		return dec, false, err
	}

	dec, isNull, err := b.args[0].EvalDecimal(row, sc)
	if err != nil || isNull {
		return dec, isNull, errors.Trace(err)
	}

	if sc.InUpdateOrDeleteStmt && b.constantArgOverflow {
		return dec, false, types.ErrOverflow.GenByArgs("DECIMAL", dec.String())
	}

	to, err = unaryMinusDecimal(dec)
	return to, false, err
}

type builtinUnaryMinusRealSig struct {
	baseRealBuiltinFunc
}

func (b *builtinUnaryMinusRealSig) evalReal(row []types.Datum) (res float64, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	var aDatum types.Datum
	aDatum, err = b.args[0].Eval(row)
	if err != nil || aDatum.IsNull() {
		return res, aDatum.IsNull(), errors.Trace(err)
	}

	switch aDatum.Kind() {
	case types.KindFloat32:
		res = float64(-aDatum.GetFloat32())
	case types.KindFloat64:
		res = float64(-aDatum.GetFloat64())
	case types.KindString, types.KindBytes:
		f, err1 := types.StrToFloat(sc, aDatum.GetString())
		err = errors.Trace(err1)
		res = float64(-f)
	case types.KindMysqlHex:
		res = float64(-aDatum.GetMysqlHex().ToNumber())
	case types.KindMysqlBit:
		res = float64(-aDatum.GetMysqlBit().ToNumber())
	case types.KindMysqlEnum:
		res = float64(-aDatum.GetMysqlEnum().ToNumber())
	case types.KindMysqlSet:
		res = float64(-aDatum.GetMysqlSet().ToNumber())
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
