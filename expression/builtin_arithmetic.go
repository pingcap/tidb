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
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &arithmeticPlusFunctionClass{}
	_ functionClass = &arithmeticMinusFunctionClass{}
	_ functionClass = &arithmeticDivideFunctionClass{}
	_ functionClass = &arithmeticMultiplyFunctionClass{}
	_ functionClass = &arithmeticIntDivideFunctionClass{}
	_ functionClass = &arithmeticFunctionClass{}
)

var (
	_ builtinFunc = &builtinArithmeticPlusRealSig{}
	_ builtinFunc = &builtinArithmeticPlusDecimalSig{}
	_ builtinFunc = &builtinArithmeticPlusIntSig{}
	_ builtinFunc = &builtinArithmeticMinusRealSig{}
	_ builtinFunc = &builtinArithmeticMinusDecimalSig{}
	_ builtinFunc = &builtinArithmeticMinusIntSig{}
	_ builtinFunc = &builtinArithmeticDivideRealSig{}
	_ builtinFunc = &builtinArithmeticDivideDecimalSig{}
	_ builtinFunc = &builtinArithmeticMultiplyRealSig{}
	_ builtinFunc = &builtinArithmeticMultiplyDecimalSig{}
	_ builtinFunc = &builtinArithmeticMultiplyIntUnsignedSig{}
	_ builtinFunc = &builtinArithmeticMultiplyIntSig{}
	_ builtinFunc = &builtinArithmeticIntDivideIntSig{}
	_ builtinFunc = &builtinArithmeticIntDivideDecimalSig{}
	_ builtinFunc = &builtinArithmeticSig{}
)

// precIncrement indicates the number of digits by which to increase the scale of the result of division operations
// performed with the / operator.
const precIncrement = 4

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
		retTp.Decimal = a.Decimal + b.Decimal
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

func (c *arithmeticDivideFunctionClass) setType4DivDecimal(retTp, a, b *types.FieldType) {
	var deca, decb = a.Decimal, b.Decimal
	if deca == types.UnspecifiedFsp {
		deca = 0
	}
	if decb == types.UnspecifiedFsp {
		decb = 0
	}
	retTp.Decimal = deca + precIncrement
	if retTp.Decimal > mysql.MaxDecimalScale {
		retTp.Decimal = mysql.MaxDecimalScale
	}
	if a.Flen == types.UnspecifiedLength {
		retTp.Flen = types.UnspecifiedLength
		return
	}
	retTp.Flen = a.Flen + decb + precIncrement
	if retTp.Flen > mysql.MaxDecimalWidth {
		retTp.Flen = mysql.MaxDecimalWidth
	}
}

func (c *arithmeticDivideFunctionClass) setType4DivReal(retTp *types.FieldType) {
	retTp.Decimal = mysql.NotFixedDec
	retTp.Flen = mysql.MaxRealWidth
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
		bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal, tpReal)
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), true)
		sig := &builtinArithmeticPlusRealSig{baseRealBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_PlusReal)
		return sig.setSelf(sig), nil
	} else if tcA == types.ClassDecimal || tcB == types.ClassDecimal {
		bf := newBaseBuiltinFuncWithTp(args, ctx, tpDecimal, tpDecimal, tpDecimal)
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), false)
		sig := &builtinArithmeticPlusDecimalSig{baseDecimalBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_PlusDecimal)
		return sig.setSelf(sig), nil
	} else {
		bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
		if mysql.HasUnsignedFlag(args[0].GetType().Flag) || mysql.HasUnsignedFlag(args[1].GetType().Flag) {
			bf.tp.Flag |= mysql.UnsignedFlag
		}
		setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
		sig := &builtinArithmeticPlusIntSig{baseIntBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_PlusInt)
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

	isLHSUnsigned := mysql.HasUnsignedFlag(s.args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(s.args[1].GetType().Flag)

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		if uint64(a) > math.MaxUint64-uint64(b) {
			return 0, true, types.ErrOverflow.GenByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
	case isLHSUnsigned && !isRHSUnsigned:
		if b < 0 && uint64(-b) > uint64(a) {
			return 0, true, types.ErrOverflow.GenByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
		if b > 0 && uint64(a) > math.MaxUint64-uint64(b) {
			return 0, true, types.ErrOverflow.GenByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
	case !isLHSUnsigned && isRHSUnsigned:
		if a < 0 && uint64(-a) > uint64(b) {
			return 0, true, types.ErrOverflow.GenByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
		if a > 0 && uint64(b) > math.MaxInt64-uint64(a) {
			return 0, true, types.ErrOverflow.GenByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
	case !isLHSUnsigned && !isRHSUnsigned:
		if (a > 0 && b > math.MaxInt64-a) || (a < 0 && b < math.MinInt64-a) {
			return 0, true, types.ErrOverflow.GenByArgs("BIGINT", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
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
		bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal, tpReal)
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), true)
		sig := &builtinArithmeticMinusRealSig{baseRealBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_MinusReal)
		return sig.setSelf(sig), nil
	} else if tcA == types.ClassDecimal || tcB == types.ClassDecimal {
		bf := newBaseBuiltinFuncWithTp(args, ctx, tpDecimal, tpDecimal, tpDecimal)
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), false)
		sig := &builtinArithmeticMinusDecimalSig{baseDecimalBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_MinusDecimal)
		return sig.setSelf(sig), nil
	} else {
		bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
		setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
		if mysql.HasUnsignedFlag(args[0].GetType().Flag) || mysql.HasUnsignedFlag(args[1].GetType().Flag) {
			bf.tp.Flag |= mysql.UnsignedFlag
		}
		sig := &builtinArithmeticMinusIntSig{baseIntBuiltinFunc: baseIntBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_MinusInt)
		return sig.setSelf(sig), nil
	}
}

type builtinArithmeticMinusRealSig struct {
	baseRealBuiltinFunc
}

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

type builtinArithmeticMinusDecimalSig struct {
	baseDecimalBuiltinFunc
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

type builtinArithmeticMinusIntSig struct {
	baseIntBuiltinFunc
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

	isLHSUnsigned := mysql.HasUnsignedFlag(s.args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(s.args[1].GetType().Flag)

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		if uint64(a) < uint64(b) {
			return 0, true, types.ErrOverflow.GenByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
	case isLHSUnsigned && !isRHSUnsigned:
		if b >= 0 && uint64(a) < uint64(b) {
			return 0, true, types.ErrOverflow.GenByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
		if b < 0 && uint64(a) > math.MaxUint64-uint64(-b) {
			return 0, true, types.ErrOverflow.GenByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
	case !isLHSUnsigned && isRHSUnsigned:
		if uint64(a-math.MinInt64) < uint64(b) {
			return 0, true, types.ErrOverflow.GenByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
	case !isLHSUnsigned && !isRHSUnsigned:
		if (a > 0 && -b > math.MaxInt64-a) || (a < 0 && -b < math.MinInt64-a) {
			return 0, true, types.ErrOverflow.GenByArgs("BIGINT", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
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
		bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal, tpReal)
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), true)
		sig := &builtinArithmeticMultiplyRealSig{baseRealBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_MultiplyReal)
		return sig.setSelf(sig), nil
	} else if tcA == types.ClassDecimal || tcB == types.ClassDecimal {
		bf := newBaseBuiltinFuncWithTp(args, ctx, tpDecimal, tpDecimal, tpDecimal)
		setFlenDecimal4RealOrDecimal(bf.tp, args[0].GetType(), args[1].GetType(), false)
		sig := &builtinArithmeticMultiplyDecimalSig{baseDecimalBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_MultiplyDecimal)
		return sig.setSelf(sig), nil
	} else {
		bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
		if mysql.HasUnsignedFlag(tpA.Flag) || mysql.HasUnsignedFlag(tpB.Flag) {
			bf.tp.Flag |= mysql.UnsignedFlag
			setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
			sig := &builtinArithmeticMultiplyIntUnsignedSig{baseIntBuiltinFunc{bf}}
			sig.setPbCode(tipb.ScalarFuncSig_MultiplyInt)
			return sig.setSelf(sig), nil
		}
		setFlenDecimal4Int(bf.tp, args[0].GetType(), args[1].GetType())
		sig := &builtinArithmeticMultiplyIntSig{baseIntBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_MultiplyInt)
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

type arithmeticDivideFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticDivideFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	tpA, tpB := args[0].GetType(), args[1].GetType()
	tcA, tcB := numericContextResultType(tpA), numericContextResultType(tpB)
	if tcA == types.ClassReal || tcB == types.ClassReal {
		bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal, tpReal)
		c.setType4DivReal(bf.tp)
		sig := &builtinArithmeticDivideRealSig{baseRealBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpDecimal, tpDecimal, tpDecimal)
	c.setType4DivDecimal(bf.tp, tpA, tpB)
	sig := &builtinArithmeticDivideDecimalSig{baseDecimalBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinArithmeticDivideRealSig struct{ baseRealBuiltinFunc }
type builtinArithmeticDivideDecimalSig struct{ baseDecimalBuiltinFunc }

func (s *builtinArithmeticDivideRealSig) evalReal(row []types.Datum) (float64, bool, error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	a, isNull, err := s.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	b, isNull, err := s.args[1].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if b == 0 {
		return 0, true, nil
	}
	result := a / b
	if math.IsInf(result, 0) {
		return 0, true, types.ErrOverflow.GenByArgs("DOUBLE", fmt.Sprintf("(%s / %s)", s.args[0].String(), s.args[1].String()))
	}
	return result, false, nil
}

func (s *builtinArithmeticDivideDecimalSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
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
	err = types.DecimalDiv(a, b, c, types.DivFracIncr)
	if err == types.ErrDivByZero {
		return c, true, nil
	}
	return c, false, err
}

type arithmeticIntDivideFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticIntDivideFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	tpA, tpB := args[0].GetType(), args[1].GetType()
	tcA, tcB := numericContextResultType(tpA), numericContextResultType(tpB)
	if tcA == types.ClassInt && tcB == types.ClassInt {
		bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpInt, tpInt)
		if mysql.HasUnsignedFlag(tpA.Flag) || mysql.HasUnsignedFlag(tpB.Flag) {
			bf.tp.Flag |= mysql.UnsignedFlag
		}
		sig := &builtinArithmeticIntDivideIntSig{baseIntBuiltinFunc{bf}}
		return sig.setSelf(sig), nil
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpDecimal, tpDecimal)
	if mysql.HasUnsignedFlag(tpA.Flag) || mysql.HasUnsignedFlag(tpB.Flag) {
		bf.tp.Flag |= mysql.UnsignedFlag
	}
	sig := &builtinArithmeticIntDivideDecimalSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinArithmeticIntDivideIntSig struct{ baseIntBuiltinFunc }
type builtinArithmeticIntDivideDecimalSig struct{ baseIntBuiltinFunc }

func (s *builtinArithmeticIntDivideIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	b, isNull, err := s.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	if b == 0 {
		return 0, true, nil
	}

	a, isNull, err := s.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	var (
		ret int64
		val uint64
	)
	isLHSUnsigned := mysql.HasUnsignedFlag(s.args[0].GetType().Flag)
	isRHSUnsigned := mysql.HasUnsignedFlag(s.args[1].GetType().Flag)

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		ret = a / b
	case isLHSUnsigned && !isRHSUnsigned:
		val, err = types.DivUintWithInt(uint64(a), b)
		ret = int64(val)
	case !isLHSUnsigned && isRHSUnsigned:
		val, err = types.DivIntWithUint(a, uint64(b))
		ret = int64(val)
	case !isLHSUnsigned && !isRHSUnsigned:
		ret, err = types.DivInt64(a, b)
	}

	if err != nil {
		return ret, false, errors.Trace(err)
	}

	return ret, false, nil
}

func (s *builtinArithmeticIntDivideDecimalSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	a, isNull, err := s.args[0].EvalDecimal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	b, isNull, err := s.args[1].EvalDecimal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	c := &types.MyDecimal{}
	err = types.DecimalDiv(a, b, c, types.DivFracIncr)
	if err == types.ErrDivByZero {
		return 0, true, errors.Trace(err)
	}

	if err != nil {
		return 0, false, errors.Trace(err)
	}

	ret, err := c.ToInt()
	if err == types.ErrOverflow {
		return 0, false, errors.Trace(err)
	}

	return ret, false, nil
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
	case opcode.Mod:
		return types.ComputeMod(sc, a, b)
	default:
		return d, errInvalidOperation.Gen("invalid op %v in arithmetic operation", s.op)
	}
}
