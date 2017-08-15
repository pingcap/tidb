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
	_ functionClass = &arithmeticPlusFunctionClass{}
	_ functionClass = &arithmeticMinusFunctionClass{}
	_ functionClass = &arithmeticMultiplyFunctionClass{}
	_ functionClass = &arithmeticFunctionClass{}
)

var (
	_ builtinFunc = &builtinArithmeticPlusRealSig{}
	_ builtinFunc = &builtinArithmeticPlusDecimalSig{}
	_ builtinFunc = &builtinArithmeticPlusIntUnsignedSig{}
	_ builtinFunc = &builtinArithmeticPlusIntSig{}
	_ builtinFunc = &builtinArithmeticMinusRealSig{}
	_ builtinFunc = &builtinArithmeticMinusDecimalSig{}
	_ builtinFunc = &builtinArithmeticMinusIntUnsignedSig{}
	_ builtinFunc = &builtinArithmeticMinusIntSig{}
	_ builtinFunc = &builtinArithmeticMultiplyRealSig{}
	_ builtinFunc = &builtinArithmeticMultiplyDecimalSig{}
	_ builtinFunc = &builtinArithmeticMultiplyIntUnsignedSig{}
	_ builtinFunc = &builtinArithmeticMultiplyIntSig{}
	_ builtinFunc = &builtinArithmeticSig{}
)

// numericContextResultType returns TypeClass for numeric function's parameters.
// the returned TypeClass should be one of: ClassInt, ClassDecimal, ClassReal
func numericContextResultType(ft *types.FieldType) types.TypeClass {
	if types.IsTypeTemporal(ft.Tp) {
		if ft.Decimal > 0 {
			return types.ClassDecimal
		}
		return types.ClassInt
	}
	if ft.ToClass() == types.ClassString {
		return types.ClassReal
	}
	return ft.ToClass()
}

// setFlenDecimal4Int is called to set proper `Flen` and `Decimal` of return
// type according to the two input parameter's types.
func setFlenDecimal4Int(retTp, a, b *types.FieldType) {
	retTp.Decimal = 0
	retTp.Flen = mysql.MaxIntWidth
}

// setFlenDecimal4Real is called to set proper `Flen` and `Decimal` of return
// type according to the two input parameter's types.
func setFlenDecimal4RealOrDecimal(retTp, a, b *types.FieldType, isReal bool) {
	if a.Decimal != types.UnspecifiedLength && b.Decimal != types.UnspecifiedLength {
		retTp.Decimal = int(math.Max(float64(a.Decimal), float64(b.Decimal)))
		if a.Flen == types.UnspecifiedLength || b.Flen == types.UnspecifiedLength {
			retTp.Flen = types.UnspecifiedLength
			return
		}
		digitsInt := int(math.Max(float64(a.Flen-a.Decimal), float64(b.Flen-b.Decimal)))
		retTp.Flen = digitsInt + retTp.Decimal + 3
		if isReal {
			retTp.Flen = int(math.Min(float64(retTp.Flen), float64(mysql.MaxRealWidth)))
			return
		}
		retTp.Flen = int(math.Min(float64(retTp.Flen), float64(mysql.MaxDecimalWidth)))
		return
	}
	retTp.Decimal = types.UnspecifiedLength
	retTp.Flen = types.UnspecifiedLength
}

type arithmeticPlusFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticPlusFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	tpA, tpB := args[0].GetType(), args[1].GetType()
	tcA, tcB := numericContextResultType(tpA), numericContextResultType(tpB)
	if tcA == types.ClassReal || tcB == types.ClassReal {
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal, tpReal)
		if err != nil {
			return nil, errors.Trace(err)
		}
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), true)
		sig := &builtinArithmeticPlusRealSig{baseRealBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	} else if tcA == types.ClassDecimal || tcB == types.ClassDecimal {
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpDecimal, tpDecimal, tpDecimal)
		if err != nil {
			return nil, errors.Trace(err)
		}
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), false)
		sig := &builtinArithmeticPlusDecimalSig{baseDecimalBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	} else {
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if mysql.HasUnsignedFlag(tpA.Flag) || mysql.HasUnsignedFlag(tpB.Flag) {
			bf.tp.Flag |= mysql.UnsignedFlag
			setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
			sig := &builtinArithmeticPlusIntUnsignedSig{baseIntBuiltinFunc{bf}}
			return sig.setSelf(sig), nil
		}
		setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
		sig := &builtinArithmeticPlusIntSig{baseIntBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	}
}

type builtinArithmeticPlusIntSig struct {
	baseIntBuiltinFunc
}

func (s *builtinArithmeticPlusIntSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	a, isNull, err := s.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	b, isNull, err := s.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if (a > 0 && b > math.MaxInt64-a) || (a < 0 && b < math.MinInt64-a) {
		return 0, true, types.ErrOverflow.GenByArgs("BIGINT", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
	}
	return a + b, false, nil
}

type builtinArithmeticPlusIntUnsignedSig struct {
	baseIntBuiltinFunc
}

func (s *builtinArithmeticPlusIntUnsignedSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	a, isNull, err := s.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	unsignedA := uint64(a)
	b, isNull, err := s.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	unsignedB := uint64(b)
	if unsignedA > math.MaxUint64-unsignedB {
		return 0, true, types.ErrOverflow.GenByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
	}
	return a + b, false, nil
}

type builtinArithmeticPlusDecimalSig struct {
	baseDecimalBuiltinFunc
}

func (s *builtinArithmeticPlusDecimalSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	a, isNull, err := s.args[0].EvalDecimal(row, sc)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}
	b, isNull, err := s.args[1].EvalDecimal(row, sc)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}
	c := &types.MyDecimal{}
	err = types.DecimalAdd(a, b, c)
	if err != nil {
		return nil, true, errors.Trace(err)
	}
	return c, false, nil
}

type builtinArithmeticPlusRealSig struct {
	baseRealBuiltinFunc
}

func (s *builtinArithmeticPlusRealSig) evalReal(row []types.Datum) (float64, bool, error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	a, isNull, err := s.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	b, isNull, err := s.args[1].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if (a > 0 && b > math.MaxFloat64-a) || (a < 0 && b < -math.MaxFloat64-a) {
		return 0, true, types.ErrOverflow.GenByArgs("DOUBLE", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
	}
	return a + b, false, nil
}

type arithmeticMinusFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticMinusFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	tpA, tpB := args[0].GetType(), args[1].GetType()
	tcA, tcB := numericContextResultType(tpA), numericContextResultType(tpB)
	if tcA == types.ClassReal || tcB == types.ClassReal {
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal, tpReal)
		if err != nil {
			return nil, errors.Trace(err)
		}
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), true)
		sig := &builtinArithmeticMinusRealSig{baseRealBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	} else if tcA == types.ClassDecimal || tcB == types.ClassDecimal {
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpDecimal, tpDecimal, tpDecimal)
		if err != nil {
			return nil, errors.Trace(err)
		}
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), false)
		sig := &builtinArithmeticMinusDecimalSig{baseDecimalBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	} else {
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if mysql.HasUnsignedFlag(tpA.Flag) || mysql.HasUnsignedFlag(tpB.Flag) {
			bf.tp.Flag |= mysql.UnsignedFlag
			setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
			sig := &builtinArithmeticMinusIntUnsignedSig{baseIntBuiltinFunc{bf}}
			return sig.setSelf(sig), nil
		}
		setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
		sig := &builtinArithmeticMinusIntSig{baseIntBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	}
}

type builtinArithmeticMinusRealSig struct{ baseRealBuiltinFunc }
type builtinArithmeticMinusDecimalSig struct{ baseDecimalBuiltinFunc }
type builtinArithmeticMinusIntUnsignedSig struct{ baseIntBuiltinFunc }
type builtinArithmeticMinusIntSig struct{ baseIntBuiltinFunc }

func (s *builtinArithmeticMinusRealSig) evalReal(row []types.Datum) (float64, bool, error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	a, isNull, err := s.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	b, isNull, err := s.args[1].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if (a > 0 && -b > math.MaxFloat64-a) || (a < 0 && -b < -math.MaxFloat64-a) {
		return 0, true, types.ErrOverflow.GenByArgs("DOUBLE", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
	}
	return a - b, false, nil
}

func (s *builtinArithmeticMinusDecimalSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	a, isNull, err := s.args[0].EvalDecimal(row, sc)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}
	b, isNull, err := s.args[1].EvalDecimal(row, sc)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}
	c := &types.MyDecimal{}
	err = types.DecimalSub(a, b, c)
	if err != nil {
		return nil, true, errors.Trace(err)
	}
	return c, false, nil
}

func (s *builtinArithmeticMinusIntUnsignedSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	a, isNull, err := s.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	unsignedA := uint64(a)
	b, isNull, err := s.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	unsignedB := uint64(b)
	if unsignedA < unsignedB {
		return 0, true, types.ErrOverflow.GenByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
	}
	return a - b, false, nil
}

func (s *builtinArithmeticMinusIntSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	a, isNull, err := s.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	b, isNull, err := s.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if (a > 0 && -b > math.MaxInt64-a) || (a < 0 && -b < math.MinInt64-a) {
		return 0, true, types.ErrOverflow.GenByArgs("BIGINT", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
	}
	return a - b, false, nil
}

type arithmeticMultiplyFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticMultiplyFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	tpA, tpB := args[0].GetType(), args[1].GetType()
	tcA, tcB := numericContextResultType(tpA), numericContextResultType(tpB)
	if tcA == types.ClassReal || tcB == types.ClassReal {
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal, tpReal)
		if err != nil {
			return nil, errors.Trace(err)
		}
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), true)
		sig := &builtinArithmeticMultiplyRealSig{baseRealBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	} else if tcA == types.ClassDecimal || tcB == types.ClassDecimal {
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpDecimal, tpDecimal, tpDecimal)
		if err != nil {
			return nil, errors.Trace(err)
		}
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), false)
		sig := &builtinArithmeticMultiplyDecimalSig{baseDecimalBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	} else {
		bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if mysql.HasUnsignedFlag(tpA.Flag) || mysql.HasUnsignedFlag(tpB.Flag) {
			bf.tp.Flag |= mysql.UnsignedFlag
			setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
			sig := &builtinArithmeticMultiplyIntUnsignedSig{baseIntBuiltinFunc{bf}}
			return sig.setSelf(sig), nil
		}
		setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
		sig := &builtinArithmeticMultiplyIntSig{baseIntBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	}
}

type builtinArithmeticMultiplyRealSig struct{ baseRealBuiltinFunc }
type builtinArithmeticMultiplyDecimalSig struct{ baseDecimalBuiltinFunc }
type builtinArithmeticMultiplyIntUnsignedSig struct{ baseIntBuiltinFunc }
type builtinArithmeticMultiplyIntSig struct{ baseIntBuiltinFunc }

func (s *builtinArithmeticMultiplyRealSig) evalReal(row []types.Datum) (float64, bool, error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	a, isNull, err := s.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	b, isNull, err := s.args[1].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	result := a * b
	if math.IsInf(result, 0) {
		return 0, true, types.ErrOverflow.GenByArgs("DOUBLE", fmt.Sprintf("(%s * %s)", s.args[0].String(), s.args[1].String()))
	}
	return result, false, nil
}

func (s *builtinArithmeticMultiplyDecimalSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	a, isNull, err := s.args[0].EvalDecimal(row, sc)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}
	b, isNull, err := s.args[1].EvalDecimal(row, sc)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}
	c := &types.MyDecimal{}
	err = types.DecimalMul(a, b, c)
	if err != nil {
		return nil, true, errors.Trace(err)
	}
	return c, false, nil
}

func (s *builtinArithmeticMultiplyIntUnsignedSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	a, isNull, err := s.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	unsignedA := uint64(a)
	b, isNull, err := s.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	unsignedB := uint64(b)
	result := unsignedA * unsignedB
	if unsignedA != 0 && result/unsignedA != unsignedB {
		return 0, true, types.ErrOverflow.GenByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s * %s)", s.args[0].String(), s.args[1].String()))
	}
	return int64(result), false, nil
}

func (s *builtinArithmeticMultiplyIntSig) evalInt(row []types.Datum) (val int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	a, isNull, err := s.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	b, isNull, err := s.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	result := a * b
	if a != 0 && result/a != b {
		return 0, true, types.ErrOverflow.GenByArgs("BIGINT", fmt.Sprintf("(%s * %s)", s.args[0].String(), s.args[1].String()))
	}
	return result, false, nil
}

type arithmeticFunctionClass struct {
	baseFunctionClass
	op opcode.Op
}

func (c *arithmeticFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinArithmeticSig{newBaseBuiltinFunc(args, ctx), c.op}
	return sig.setSelf(sig), nil
}

type builtinArithmeticSig struct {
	baseBuiltinFunc
	op opcode.Op
}

func (s *builtinArithmeticSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := s.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	sc := s.ctx.GetSessionVars().StmtCtx
	a, err := types.CoerceArithmetic(sc, args[0])
	if err != nil {
		return d, errors.Trace(err)
	}

	b, err := types.CoerceArithmetic(sc, args[1])
	if err != nil {
		return d, errors.Trace(err)
	}
	a, b, err = types.CoerceDatum(sc, a, b)
	if err != nil {
		return d, errors.Trace(err)
	}
	if a.IsNull() || b.IsNull() {
		return
	}

	switch s.op {
	case opcode.Mul:
		return types.ComputeMul(a, b)
	case opcode.Div:
		return types.ComputeDiv(sc, a, b)
	case opcode.Mod:
		return types.ComputeMod(sc, a, b)
	case opcode.IntDiv:
		return types.ComputeIntDiv(sc, a, b)
	default:
		return d, errInvalidOperation.Gen("invalid op %v in arithmetic operation", s.op)
	}
}
