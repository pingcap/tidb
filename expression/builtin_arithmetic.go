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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"math"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &arithmeticPlusFunctionClass{}
	_ functionClass = &arithmeticMinusFunctionClass{}
	_ functionClass = &arithmeticDivideFunctionClass{}
	_ functionClass = &arithmeticMultiplyFunctionClass{}
	_ functionClass = &arithmeticIntDivideFunctionClass{}
	_ functionClass = &arithmeticModFunctionClass{}
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
	_ builtinFunc = &builtinArithmeticModIntUnsignedUnsignedSig{}
	_ builtinFunc = &builtinArithmeticModIntUnsignedSignedSig{}
	_ builtinFunc = &builtinArithmeticModIntSignedUnsignedSig{}
	_ builtinFunc = &builtinArithmeticModIntSignedSignedSig{}
	_ builtinFunc = &builtinArithmeticModRealSig{}
	_ builtinFunc = &builtinArithmeticModDecimalSig{}
)

// precIncrement indicates the number of digits by which to increase the scale of the result of division operations
// performed with the / operator.
const precIncrement = 4

// numericContextResultType returns types.EvalType for numeric function's parameters.
// the returned types.EvalType should be one of: types.ETInt, types.ETDecimal, types.ETReal
func numericContextResultType(ft *types.FieldType) types.EvalType {
	if types.IsTypeTemporal(ft.GetType()) {
		if ft.GetDecimal() > 0 {
			return types.ETDecimal
		}
		return types.ETInt
	}
	if types.IsBinaryStr(ft) || ft.GetType() == mysql.TypeBit {
		return types.ETInt
	}
	evalTp4Ft := types.ETReal
	if !ft.Hybrid() {
		evalTp4Ft = ft.EvalType()
		if evalTp4Ft != types.ETDecimal && evalTp4Ft != types.ETInt {
			evalTp4Ft = types.ETReal
		}
	}
	return evalTp4Ft
}

// setFlenDecimal4RealOrDecimal is called to set proper `flen` and `decimal` of return
// type according to the two input parameter's types.
func setFlenDecimal4RealOrDecimal(ctx sessionctx.Context, retTp *types.FieldType, arg0, arg1 Expression, isReal bool, isMultiply bool) {
	a, b := arg0.GetType(), arg1.GetType()
	if a.GetDecimal() != types.UnspecifiedLength && b.GetDecimal() != types.UnspecifiedLength {
		retTp.SetDecimal(a.GetDecimal() + b.GetDecimal())
		if !isMultiply {
			retTp.SetDecimal(mathutil.Max(a.GetDecimal(), b.GetDecimal()))
		}
		if !isReal && retTp.GetDecimal() > mysql.MaxDecimalScale {
			retTp.SetDecimal(mysql.MaxDecimalScale)
		}
		if a.GetFlen() == types.UnspecifiedLength || b.GetFlen() == types.UnspecifiedLength {
			retTp.SetFlen(types.UnspecifiedLength)
			return
		}
		digitsInt := mathutil.Max(a.GetFlen()-a.GetDecimal(), b.GetFlen()-b.GetDecimal())
		if isMultiply {
			digitsInt = a.GetFlen() - a.GetDecimal() + b.GetFlen() - b.GetDecimal()
		}
		retTp.SetFlen(digitsInt + retTp.GetDecimal() + 1)
		if isReal {
			retTp.SetFlen(mathutil.Min(retTp.GetFlen(), mysql.MaxRealWidth))
			return
		}
		retTp.SetFlen(mathutil.Min(retTp.GetFlen(), mysql.MaxDecimalWidth))
		return
	}
	if isReal {
		retTp.SetFlen(types.UnspecifiedLength)
		retTp.SetDecimal(types.UnspecifiedLength)
	} else {
		retTp.SetFlen(mysql.MaxDecimalWidth)
		retTp.SetDecimal(mysql.MaxDecimalScale)
	}
}

func (c *arithmeticDivideFunctionClass) setType4DivDecimal(retTp, a, b *types.FieldType) {
	var deca, decb = a.GetDecimal(), b.GetDecimal()
	if deca == types.UnspecifiedFsp {
		deca = 0
	}
	if decb == types.UnspecifiedFsp {
		decb = 0
	}
	retTp.SetDecimal(deca + precIncrement)
	if retTp.GetDecimal() > mysql.MaxDecimalScale {
		retTp.SetDecimal(mysql.MaxDecimalScale)
	}
	if a.GetFlen() == types.UnspecifiedLength {
		retTp.SetFlen(mysql.MaxDecimalWidth)
		return
	}
	aPrec := types.DecimalLength2Precision(a.GetFlen(), a.GetDecimal(), mysql.HasUnsignedFlag(a.GetFlag()))
	retTp.SetFlen(aPrec + decb + precIncrement)
	retTp.SetFlen(types.Precision2LengthNoTruncation(retTp.GetFlen(), retTp.GetDecimal(), mysql.HasUnsignedFlag(retTp.GetFlag())))
	if retTp.GetFlen() > mysql.MaxDecimalWidth {
		retTp.SetFlen(mysql.MaxDecimalWidth)
	}
}

func (c *arithmeticDivideFunctionClass) setType4DivReal(retTp *types.FieldType) {
	retTp.SetDecimal(types.UnspecifiedLength)
	retTp.SetFlen(mysql.MaxRealWidth)
}

type arithmeticPlusFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticPlusFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal, types.ETReal)
		if err != nil {
			return nil, err
		}
		setFlenDecimal4RealOrDecimal(ctx, bf.tp, args[0], args[1], true, false)
		sig := &builtinArithmeticPlusRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_PlusReal)
		return sig, nil
	} else if lhsEvalTp == types.ETDecimal || rhsEvalTp == types.ETDecimal {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
		if err != nil {
			return nil, err
		}
		setFlenDecimal4RealOrDecimal(ctx, bf.tp, args[0], args[1], false, false)
		sig := &builtinArithmeticPlusDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_PlusDecimal)
		return sig, nil
	} else {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
		if err != nil {
			return nil, err
		}
		if mysql.HasUnsignedFlag(args[0].GetType().GetFlag()) || mysql.HasUnsignedFlag(args[1].GetType().GetFlag()) {
			bf.tp.AddFlag(mysql.UnsignedFlag)
		}
		sig := &builtinArithmeticPlusIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_PlusInt)
		return sig, nil
	}
}

type builtinArithmeticPlusIntSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticPlusIntSig) Clone() builtinFunc {
	newSig := &builtinArithmeticPlusIntSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticPlusIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	isLHSUnsigned := mysql.HasUnsignedFlag(s.args[0].GetType().GetFlag())
	isRHSUnsigned := mysql.HasUnsignedFlag(s.args[1].GetType().GetFlag())

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		if uint64(a) > math.MaxUint64-uint64(b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
	case isLHSUnsigned && !isRHSUnsigned:
		if b < 0 && uint64(-b) > uint64(a) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
		if b > 0 && uint64(a) > math.MaxUint64-uint64(b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
	case !isLHSUnsigned && isRHSUnsigned:
		if a < 0 && uint64(-a) > uint64(b) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
		if a > 0 && uint64(b) > math.MaxUint64-uint64(a) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
	case !isLHSUnsigned && !isRHSUnsigned:
		if (a > 0 && b > math.MaxInt64-a) || (a < 0 && b < math.MinInt64-a) {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
	}

	return a + b, false, nil
}

type builtinArithmeticPlusDecimalSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticPlusDecimalSig) Clone() builtinFunc {
	newSig := &builtinArithmeticPlusDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticPlusDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	a, isNull, err := s.args[0].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	b, isNull, err := s.args[1].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	c := &types.MyDecimal{}
	err = types.DecimalAdd(a, b, c)
	if err != nil {
		if err == types.ErrOverflow {
			err = types.ErrOverflow.GenWithStackByArgs("DECIMAL", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
		}
		return nil, true, err
	}
	return c, false, nil
}

type builtinArithmeticPlusRealSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticPlusRealSig) Clone() builtinFunc {
	newSig := &builtinArithmeticPlusRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticPlusRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	a, isLHSNull, err := s.args[0].EvalReal(s.ctx, row)
	if err != nil {
		return 0, isLHSNull, err
	}
	b, isRHSNull, err := s.args[1].EvalReal(s.ctx, row)
	if err != nil {
		return 0, isRHSNull, err
	}
	if isLHSNull || isRHSNull {
		return 0, true, nil
	}
	if !mathutil.IsFinite(a + b) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s + %s)", s.args[0].String(), s.args[1].String()))
	}
	return a + b, false, nil
}

type arithmeticMinusFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticMinusFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal, types.ETReal)
		if err != nil {
			return nil, err
		}
		setFlenDecimal4RealOrDecimal(ctx, bf.tp, args[0], args[1], true, false)
		sig := &builtinArithmeticMinusRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_MinusReal)
		return sig, nil
	} else if lhsEvalTp == types.ETDecimal || rhsEvalTp == types.ETDecimal {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
		if err != nil {
			return nil, err
		}
		setFlenDecimal4RealOrDecimal(ctx, bf.tp, args[0], args[1], false, false)
		sig := &builtinArithmeticMinusDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_MinusDecimal)
		return sig, nil
	} else {

		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
		if err != nil {
			return nil, err
		}
		if (mysql.HasUnsignedFlag(args[0].GetType().GetFlag()) || mysql.HasUnsignedFlag(args[1].GetType().GetFlag())) && !ctx.GetSessionVars().SQLMode.HasNoUnsignedSubtractionMode() {
			bf.tp.AddFlag(mysql.UnsignedFlag)
		}
		sig := &builtinArithmeticMinusIntSig{baseBuiltinFunc: bf}
		sig.setPbCode(tipb.ScalarFuncSig_MinusInt)
		return sig, nil
	}
}

type builtinArithmeticMinusRealSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticMinusRealSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMinusRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMinusRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	a, isNull, err := s.args[0].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.args[1].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if !mathutil.IsFinite(a - b) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
	}
	return a - b, false, nil
}

type builtinArithmeticMinusDecimalSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticMinusDecimalSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMinusDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMinusDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	a, isNull, err := s.args[0].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	b, isNull, err := s.args[1].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	c := &types.MyDecimal{}
	err = types.DecimalSub(a, b, c)
	if err != nil {
		if err == types.ErrOverflow {
			err = types.ErrOverflow.GenWithStackByArgs("DECIMAL", fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
		}
		return nil, true, err
	}
	return c, false, nil
}

type builtinArithmeticMinusIntSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticMinusIntSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMinusIntSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMinusIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	forceToSigned := s.ctx.GetSessionVars().SQLMode.HasNoUnsignedSubtractionMode()
	isLHSUnsigned := mysql.HasUnsignedFlag(s.args[0].GetType().GetFlag())
	isRHSUnsigned := mysql.HasUnsignedFlag(s.args[1].GetType().GetFlag())

	errType := "BIGINT UNSIGNED"
	signed := forceToSigned || (!isLHSUnsigned && !isRHSUnsigned)
	if signed {
		errType = "BIGINT"
	}
	overflow := s.overflowCheck(isLHSUnsigned, isRHSUnsigned, signed, a, b)
	if overflow {
		return 0, true, types.ErrOverflow.GenWithStackByArgs(errType, fmt.Sprintf("(%s - %s)", s.args[0].String(), s.args[1].String()))
	}

	return a - b, false, nil
}

// returns true when overflowed
func (s *builtinArithmeticMinusIntSig) overflowCheck(isLHSUnsigned, isRHSUnsigned, signed bool, a, b int64) bool {
	res := a - b
	ua, ub := uint64(a), uint64(b)
	resUnsigned := false
	if isLHSUnsigned {
		if isRHSUnsigned {
			if ua < ub {
				if res >= 0 {
					return true
				}
			} else {
				resUnsigned = true
			}
		} else {
			if b >= 0 {
				if ua > ub {
					resUnsigned = true
				}
			} else {
				if testIfSumOverflowsUll(ua, uint64(-b)) {
					return true
				}
				resUnsigned = true
			}
		}
	} else {
		if isRHSUnsigned {
			if uint64(a-math.MinInt64) < ub {
				return true
			}
		} else {
			if a > 0 && b < 0 {
				resUnsigned = true
			} else if a < 0 && b > 0 && res >= 0 {
				return true
			}
		}
	}

	if (!signed && !resUnsigned && res < 0) || (signed && resUnsigned && uint64(res) > uint64(math.MaxInt64)) {
		return true
	}

	return false
}

func testIfSumOverflowsUll(a, b uint64) bool {
	return math.MaxUint64-a < b
}

type arithmeticMultiplyFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticMultiplyFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal, types.ETReal)
		if err != nil {
			return nil, err
		}
		setFlenDecimal4RealOrDecimal(ctx, bf.tp, args[0], args[1], true, true)
		sig := &builtinArithmeticMultiplyRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_MultiplyReal)
		return sig, nil
	} else if lhsEvalTp == types.ETDecimal || rhsEvalTp == types.ETDecimal {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
		if err != nil {
			return nil, err
		}
		setFlenDecimal4RealOrDecimal(ctx, bf.tp, args[0], args[1], false, true)
		sig := &builtinArithmeticMultiplyDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_MultiplyDecimal)
		return sig, nil
	} else {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
		if err != nil {
			return nil, err
		}
		if mysql.HasUnsignedFlag(lhsTp.GetFlag()) || mysql.HasUnsignedFlag(rhsTp.GetFlag()) {
			bf.tp.AddFlag(mysql.UnsignedFlag)
			sig := &builtinArithmeticMultiplyIntUnsignedSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_MultiplyIntUnsigned)
			return sig, nil
		}
		sig := &builtinArithmeticMultiplyIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_MultiplyInt)
		return sig, nil
	}
}

type builtinArithmeticMultiplyRealSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticMultiplyRealSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMultiplyRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticMultiplyDecimalSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticMultiplyDecimalSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMultiplyDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticMultiplyIntUnsignedSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticMultiplyIntUnsignedSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMultiplyIntUnsignedSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticMultiplyIntSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticMultiplyIntSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMultiplyIntSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMultiplyRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	a, isNull, err := s.args[0].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.args[1].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	result := a * b
	if math.IsInf(result, 0) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s * %s)", s.args[0].String(), s.args[1].String()))
	}
	return result, false, nil
}

func (s *builtinArithmeticMultiplyDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	a, isNull, err := s.args[0].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	b, isNull, err := s.args[1].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	c := &types.MyDecimal{}
	err = types.DecimalMul(a, b, c)
	if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) {
		if err == types.ErrOverflow {
			err = types.ErrOverflow.GenWithStackByArgs("DECIMAL", fmt.Sprintf("(%s * %s)", s.args[0].String(), s.args[1].String()))
		}
		return nil, true, err
	}
	return c, false, nil
}

func (s *builtinArithmeticMultiplyIntUnsignedSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	unsignedA := uint64(a)
	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	unsignedB := uint64(b)
	result := unsignedA * unsignedB
	if unsignedA != 0 && result/unsignedA != unsignedB {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s * %s)", s.args[0].String(), s.args[1].String()))
	}
	return int64(result), false, nil
}

func (s *builtinArithmeticMultiplyIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	result := a * b
	if (a != 0 && result/a != b) || (result == math.MinInt64 && a == -1) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s * %s)", s.args[0].String(), s.args[1].String()))
	}
	return result, false, nil
}

type arithmeticDivideFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticDivideFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal, types.ETReal)
		if err != nil {
			return nil, err
		}
		c.setType4DivReal(bf.tp)
		sig := &builtinArithmeticDivideRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_DivideReal)
		return sig, nil
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
	if err != nil {
		return nil, err
	}
	c.setType4DivDecimal(bf.tp, lhsTp, rhsTp)
	sig := &builtinArithmeticDivideDecimalSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_DivideDecimal)
	return sig, nil
}

type builtinArithmeticDivideRealSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticDivideRealSig) Clone() builtinFunc {
	newSig := &builtinArithmeticDivideRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticDivideDecimalSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticDivideDecimalSig) Clone() builtinFunc {
	newSig := &builtinArithmeticDivideDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticDivideRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	a, isNull, err := s.args[0].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.args[1].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if b == 0 {
		return 0, true, handleDivisionByZeroError(s.ctx)
	}
	result := a / b
	if math.IsInf(result, 0) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s / %s)", s.args[0].String(), s.args[1].String()))
	}
	return result, false, nil
}

func (s *builtinArithmeticDivideDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	a, isNull, err := s.args[0].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}

	b, isNull, err := s.args[1].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}

	c := &types.MyDecimal{}
	err = types.DecimalDiv(a, b, c, types.DivFracIncr)
	if err == types.ErrDivByZero {
		return c, true, handleDivisionByZeroError(s.ctx)
	} else if err == types.ErrTruncated {
		sc := s.ctx.GetSessionVars().StmtCtx
		err = sc.HandleTruncate(errTruncatedWrongValue.GenWithStackByArgs("DECIMAL", c))
	} else if err == nil {
		_, frac := c.PrecisionAndFrac()
		if frac < s.baseBuiltinFunc.tp.GetDecimal() {
			err = c.Round(c, s.baseBuiltinFunc.tp.GetDecimal(), types.ModeHalfUp)
		}
	} else if err == types.ErrOverflow {
		err = types.ErrOverflow.GenWithStackByArgs("DECIMAL", fmt.Sprintf("(%s / %s)", s.args[0].String(), s.args[1].String()))
	}
	return c, false, err
}

type arithmeticIntDivideFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticIntDivideFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETInt && rhsEvalTp == types.ETInt {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
		if err != nil {
			return nil, err
		}
		if mysql.HasUnsignedFlag(lhsTp.GetFlag()) || mysql.HasUnsignedFlag(rhsTp.GetFlag()) {
			bf.tp.AddFlag(mysql.UnsignedFlag)
		}
		sig := &builtinArithmeticIntDivideIntSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_IntDivideInt)
		return sig, nil
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETDecimal, types.ETDecimal)
	if err != nil {
		return nil, err
	}
	if mysql.HasUnsignedFlag(lhsTp.GetFlag()) || mysql.HasUnsignedFlag(rhsTp.GetFlag()) {
		bf.tp.AddFlag(mysql.UnsignedFlag)
	}
	sig := &builtinArithmeticIntDivideDecimalSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_IntDivideDecimal)
	return sig, nil
}

type builtinArithmeticIntDivideIntSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticIntDivideIntSig) Clone() builtinFunc {
	newSig := &builtinArithmeticIntDivideIntSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticIntDivideDecimalSig struct{ baseBuiltinFunc }

func (s *builtinArithmeticIntDivideDecimalSig) Clone() builtinFunc {
	newSig := &builtinArithmeticIntDivideDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticIntDivideIntSig) evalInt(row chunk.Row) (int64, bool, error) {
	return s.evalIntWithCtx(s.ctx, row)
}

func (s *builtinArithmeticIntDivideIntSig) evalIntWithCtx(sctx sessionctx.Context, row chunk.Row) (int64, bool, error) {
	b, bIsNull, err := s.args[1].EvalInt(sctx, row)
	if bIsNull || err != nil {
		return 0, bIsNull, err
	}
	a, aIsNull, err := s.args[0].EvalInt(sctx, row)
	if aIsNull || err != nil {
		return 0, aIsNull, err
	}

	if b == 0 {
		return 0, true, handleDivisionByZeroError(sctx)
	}

	var (
		ret int64
		val uint64
	)
	isLHSUnsigned := mysql.HasUnsignedFlag(s.args[0].GetType().GetFlag())
	isRHSUnsigned := mysql.HasUnsignedFlag(s.args[1].GetType().GetFlag())

	switch {
	case isLHSUnsigned && isRHSUnsigned:
		ret = int64(uint64(a) / uint64(b))
	case isLHSUnsigned && !isRHSUnsigned:
		val, err = types.DivUintWithInt(uint64(a), b)
		ret = int64(val)
	case !isLHSUnsigned && isRHSUnsigned:
		val, err = types.DivIntWithUint(a, uint64(b))
		ret = int64(val)
	case !isLHSUnsigned && !isRHSUnsigned:
		ret, err = types.DivInt64(a, b)
	}

	return ret, err != nil, err
}

func (s *builtinArithmeticIntDivideDecimalSig) evalInt(row chunk.Row) (ret int64, isNull bool, err error) {
	sc := s.ctx.GetSessionVars().StmtCtx
	var num [2]*types.MyDecimal
	for i, arg := range s.args {
		num[i], isNull, err = arg.EvalDecimal(s.ctx, row)
		if isNull || err != nil {
			return 0, isNull, err
		}
	}

	c := &types.MyDecimal{}
	err = types.DecimalDiv(num[0], num[1], c, types.DivFracIncr)
	if err == types.ErrDivByZero {
		return 0, true, handleDivisionByZeroError(s.ctx)
	}
	if err == types.ErrTruncated {
		err = sc.HandleTruncate(errTruncatedWrongValue.GenWithStackByArgs("DECIMAL", c))
	}
	if err == types.ErrOverflow {
		newErr := errTruncatedWrongValue.GenWithStackByArgs("DECIMAL", c)
		err = sc.HandleOverflow(newErr, newErr)
	}
	if err != nil {
		return 0, true, err
	}

	isLHSUnsigned := mysql.HasUnsignedFlag(s.args[0].GetType().GetFlag())
	isRHSUnsigned := mysql.HasUnsignedFlag(s.args[1].GetType().GetFlag())

	if isLHSUnsigned || isRHSUnsigned {
		val, err := c.ToUint()
		// err returned by ToUint may be ErrTruncated or ErrOverflow, only handle ErrOverflow, ignore ErrTruncated.
		if err == types.ErrOverflow {
			v, err := c.ToInt()
			// when the final result is at (-1, 0], it should be return 0 instead of the error
			if v == 0 && err == types.ErrTruncated {
				ret = int64(0)
				return ret, false, nil
			}
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s DIV %s)", s.args[0].String(), s.args[1].String()))
		}
		ret = int64(val)
	} else {
		ret, err = c.ToInt()
		// err returned by ToInt may be ErrTruncated or ErrOverflow, only handle ErrOverflow, ignore ErrTruncated.
		if err == types.ErrOverflow {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s DIV %s)", s.args[0].String(), s.args[1].String()))
		}
	}

	return ret, false, nil
}

type arithmeticModFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticModFunctionClass) setType4ModRealOrDecimal(retTp, a, b *types.FieldType, isDecimal bool) {
	if a.GetDecimal() == types.UnspecifiedLength || b.GetDecimal() == types.UnspecifiedLength {
		retTp.SetDecimal(types.UnspecifiedLength)
	} else {
		retTp.SetDecimal(mathutil.Max(a.GetDecimal(), b.GetDecimal()))
		if isDecimal && retTp.GetDecimal() > mysql.MaxDecimalScale {
			retTp.SetDecimal(mysql.MaxDecimalScale)
		}
	}

	if a.GetFlen() == types.UnspecifiedLength || b.GetFlen() == types.UnspecifiedLength {
		retTp.SetFlen(types.UnspecifiedLength)
	} else {
		retTp.SetFlen(mathutil.Max(a.GetFlen(), b.GetFlen()))
		if isDecimal {
			retTp.SetFlen(mathutil.Min(retTp.GetFlen(), mysql.MaxDecimalWidth))
			return
		}
		retTp.SetFlen(mathutil.Min(retTp.GetFlen(), mysql.MaxRealWidth))
	}
}

func (c *arithmeticModFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhsTp, rhsTp := args[0].GetType(), args[1].GetType()
	lhsEvalTp, rhsEvalTp := numericContextResultType(lhsTp), numericContextResultType(rhsTp)
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal, types.ETReal)
		if err != nil {
			return nil, err
		}
		c.setType4ModRealOrDecimal(bf.tp, lhsTp, rhsTp, false)
		if mysql.HasUnsignedFlag(lhsTp.GetFlag()) {
			bf.tp.AddFlag(mysql.UnsignedFlag)
		}
		sig := &builtinArithmeticModRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ModReal)
		return sig, nil
	} else if lhsEvalTp == types.ETDecimal || rhsEvalTp == types.ETDecimal {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
		if err != nil {
			return nil, err
		}
		c.setType4ModRealOrDecimal(bf.tp, lhsTp, rhsTp, true)
		if mysql.HasUnsignedFlag(lhsTp.GetFlag()) {
			bf.tp.AddFlag(mysql.UnsignedFlag)
		}
		sig := &builtinArithmeticModDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_ModDecimal)
		return sig, nil
	} else {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
		if err != nil {
			return nil, err
		}
		if mysql.HasUnsignedFlag(lhsTp.GetFlag()) {
			bf.tp.AddFlag(mysql.UnsignedFlag)
		}
		isLHSUnsigned := mysql.HasUnsignedFlag(args[0].GetType().GetFlag())
		isRHSUnsigned := mysql.HasUnsignedFlag(args[1].GetType().GetFlag())

		switch {
		case isLHSUnsigned && isRHSUnsigned:
			sig := &builtinArithmeticModIntUnsignedUnsignedSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_ModIntUnsignedUnsigned)
			return sig, nil
		case isLHSUnsigned && !isRHSUnsigned:
			sig := &builtinArithmeticModIntUnsignedSignedSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_ModIntUnsignedSigned)
			return sig, nil
		case !isLHSUnsigned && isRHSUnsigned:
			sig := &builtinArithmeticModIntSignedUnsignedSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_ModIntSignedUnsigned)
			return sig, nil
		default:
			sig := &builtinArithmeticModIntSignedSignedSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_ModIntSignedSigned)
			return sig, nil
		}
	}
}

type builtinArithmeticModRealSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticModRealSig) Clone() builtinFunc {
	newSig := &builtinArithmeticModRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModRealSig) evalReal(row chunk.Row) (float64, bool, error) {
	b, isNull, err := s.args[1].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if b == 0 {
		return 0, true, handleDivisionByZeroError(s.ctx)
	}

	a, isNull, err := s.args[0].EvalReal(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return math.Mod(a, b), false, nil
}

type builtinArithmeticModDecimalSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticModDecimalSig) Clone() builtinFunc {
	newSig := &builtinArithmeticModDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModDecimalSig) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	a, isNull, err := s.args[0].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	b, isNull, err := s.args[1].EvalDecimal(s.ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	c := &types.MyDecimal{}
	err = types.DecimalMod(a, b, c)
	if err == types.ErrDivByZero {
		return c, true, handleDivisionByZeroError(s.ctx)
	}
	return c, err != nil, err
}

type builtinArithmeticModIntUnsignedUnsignedSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticModIntUnsignedUnsignedSig) Clone() builtinFunc {
	newSig := &builtinArithmeticModIntUnsignedUnsignedSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModIntUnsignedUnsignedSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if b == 0 {
		return 0, true, handleDivisionByZeroError(s.ctx)
	}

	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	ret := int64(uint64(a) % uint64(b))

	return ret, false, nil
}

type builtinArithmeticModIntUnsignedSignedSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticModIntUnsignedSignedSig) Clone() builtinFunc {
	newSig := &builtinArithmeticModIntUnsignedSignedSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModIntUnsignedSignedSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if b == 0 {
		return 0, true, handleDivisionByZeroError(s.ctx)
	}
	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	var ret int64
	if b < 0 {
		ret = int64(uint64(a) % uint64(-b))
	} else {
		ret = int64(uint64(a) % uint64(b))
	}

	return ret, false, nil
}

type builtinArithmeticModIntSignedUnsignedSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticModIntSignedUnsignedSig) Clone() builtinFunc {
	newSig := &builtinArithmeticModIntSignedUnsignedSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModIntSignedUnsignedSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if b == 0 {
		return 0, true, handleDivisionByZeroError(s.ctx)
	}

	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	var ret int64
	if a < 0 {
		ret = -int64(uint64(-a) % uint64(b))
	} else {
		ret = int64(uint64(a) % uint64(b))
	}

	return ret, false, nil
}

type builtinArithmeticModIntSignedSignedSig struct {
	baseBuiltinFunc
}

func (s *builtinArithmeticModIntSignedSignedSig) Clone() builtinFunc {
	newSig := &builtinArithmeticModIntSignedSignedSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModIntSignedSignedSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	b, isNull, err := s.args[1].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if b == 0 {
		return 0, true, handleDivisionByZeroError(s.ctx)
	}

	a, isNull, err := s.args[0].EvalInt(s.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	return a % b, false, nil
}
