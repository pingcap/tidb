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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type arithmeticMultiplyFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticMultiplyFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	if args[0].GetType(ctx.GetEvalCtx()).EvalType().IsVectorKind() || args[1].GetType(ctx.GetEvalCtx()).EvalType().IsVectorKind() {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETVectorFloat32, types.ETVectorFloat32, types.ETVectorFloat32)
		if err != nil {
			return nil, err
		}
		sig := &builtinArithmeticMultiplyVectorFloat32Sig{bf}
		// sig.setPbCode(tipb.ScalarFuncSig_PlusVectorFloat32)
		return sig, nil
	}
	lhsTp, rhsTp := args[0].GetType(ctx.GetEvalCtx()), args[1].GetType(ctx.GetEvalCtx())
	lhsEvalTp, rhsEvalTp := numericContextResultType(ctx.GetEvalCtx(), args[0]), numericContextResultType(ctx.GetEvalCtx(), args[1])
	if lhsEvalTp == types.ETReal || rhsEvalTp == types.ETReal {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal, types.ETReal)
		if err != nil {
			return nil, err
		}
		setFlenDecimal4RealOrDecimal(ctx.GetEvalCtx(), bf.tp, args[0], args[1], true, true)
		sig := &builtinArithmeticMultiplyRealSig{bf, false}
		sig.setPbCode(tipb.ScalarFuncSig_MultiplyReal)
		return sig, nil
	} else if lhsEvalTp == types.ETDecimal || rhsEvalTp == types.ETDecimal {
		bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETDecimal, types.ETDecimal, types.ETDecimal)
		if err != nil {
			return nil, err
		}
		setFlenDecimal4RealOrDecimal(ctx.GetEvalCtx(), bf.tp, args[0], args[1], false, true)
		sig := &builtinArithmeticMultiplyDecimalSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_MultiplyDecimal)
		return sig, nil
	}
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

type builtinArithmeticMultiplyRealSig struct {
	baseBuiltinFunc

	test bool
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticMultiplyRealSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMultiplyRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticMultiplyDecimalSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticMultiplyDecimalSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMultiplyDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticMultiplyIntUnsignedSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticMultiplyIntUnsignedSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMultiplyIntUnsignedSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticMultiplyIntSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticMultiplyIntSig) Clone() builtinFunc {
	newSig := &builtinArithmeticMultiplyIntSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMultiplyRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	a, isNull, err := s.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	result := a * b
	if math.IsInf(result, 0) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s * %s)", s.args[0].StringWithCtx(ctx, errors.RedactLogDisable), s.args[1].StringWithCtx(ctx, errors.RedactLogDisable)))
	}
	return result, false, nil
}

func (s *builtinArithmeticMultiplyDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	a, isNull, err := s.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	b, isNull, err := s.args[1].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	c := &types.MyDecimal{}
	err = types.DecimalMul(a, b, c)
	if err != nil && !terror.ErrorEqual(err, types.ErrTruncated) {
		if err == types.ErrOverflow {
			err = types.ErrOverflow.GenWithStackByArgs("DECIMAL", fmt.Sprintf("(%s * %s)", s.args[0].StringWithCtx(ctx, errors.RedactLogDisable), s.args[1].StringWithCtx(ctx, errors.RedactLogDisable)))
		}
		return nil, true, err
	}
	return c, false, nil
}

func (s *builtinArithmeticMultiplyIntUnsignedSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	a, isNull, err := s.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	unsignedA := uint64(a)
	b, isNull, err := s.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	unsignedB := uint64(b)
	result := unsignedA * unsignedB
	if unsignedA != 0 && result/unsignedA != unsignedB {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s * %s)", s.args[0].StringWithCtx(ctx, errors.RedactLogDisable), s.args[1].StringWithCtx(ctx, errors.RedactLogDisable)))
	}
	return int64(result), false, nil
}

func (s *builtinArithmeticMultiplyIntSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	a, isNull, err := s.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	result := a * b
	if (a != 0 && result/a != b) || (result == math.MinInt64 && a == -1) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s * %s)", s.args[0].StringWithCtx(ctx, errors.RedactLogDisable), s.args[1].StringWithCtx(ctx, errors.RedactLogDisable)))
	}
	return result, false, nil
}

type arithmeticDivideFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticDivideFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhsEvalTp, rhsEvalTp := numericContextResultType(ctx.GetEvalCtx(), args[0]), numericContextResultType(ctx.GetEvalCtx(), args[1])
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
	lhsTp, rhsTp := args[0].GetType(ctx.GetEvalCtx()), args[1].GetType(ctx.GetEvalCtx())
	c.setType4DivDecimal(bf.tp, lhsTp, rhsTp, ctx.GetEvalCtx().GetDivPrecisionIncrement())
	sig := &builtinArithmeticDivideDecimalSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_DivideDecimal)
	return sig, nil
}

type builtinArithmeticDivideRealSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticDivideRealSig) Clone() builtinFunc {
	newSig := &builtinArithmeticDivideRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticDivideDecimalSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticDivideDecimalSig) Clone() builtinFunc {
	newSig := &builtinArithmeticDivideDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticDivideRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	a, isNull, err := s.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	b, isNull, err := s.args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if b == 0 {
		return 0, true, handleDivisionByZeroError(ctx)
	}
	result := a / b
	if math.IsInf(result, 0) {
		return 0, true, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("(%s / %s)", s.args[0].StringWithCtx(ctx, errors.RedactLogDisable), s.args[1].StringWithCtx(ctx, errors.RedactLogDisable)))
	}
	return result, false, nil
}

func (s *builtinArithmeticDivideDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	a, isNull, err := s.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}

	b, isNull, err := s.args[1].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}

	c := &types.MyDecimal{}
	err = types.DecimalDiv(a, b, c, ctx.GetDivPrecisionIncrement())
	if err == types.ErrDivByZero {
		return c, true, handleDivisionByZeroError(ctx)
	} else if err == types.ErrTruncated {
		tc := typeCtx(ctx)
		err = tc.HandleTruncate(errTruncatedWrongValue.GenWithStackByArgs("DECIMAL", c))
	} else if err == nil {
		_, frac := c.PrecisionAndFrac()
		if frac < s.baseBuiltinFunc.tp.GetDecimal() {
			err = c.Round(c, s.baseBuiltinFunc.tp.GetDecimal(), types.ModeHalfUp)
		}
	} else if err == types.ErrOverflow {
		err = types.ErrOverflow.GenWithStackByArgs("DECIMAL", fmt.Sprintf("(%s / %s)", s.args[0].StringWithCtx(ctx, errors.RedactLogDisable), s.args[1].StringWithCtx(ctx, errors.RedactLogDisable)))
	}
	return c, false, err
}

type arithmeticIntDivideFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticIntDivideFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhsTp, rhsTp := args[0].GetType(ctx.GetEvalCtx()), args[1].GetType(ctx.GetEvalCtx())
	lhsEvalTp, rhsEvalTp := numericContextResultType(ctx.GetEvalCtx(), args[0]), numericContextResultType(ctx.GetEvalCtx(), args[1])
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

type builtinArithmeticIntDivideIntSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticIntDivideIntSig) Clone() builtinFunc {
	newSig := &builtinArithmeticIntDivideIntSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

type builtinArithmeticIntDivideDecimalSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticIntDivideDecimalSig) Clone() builtinFunc {
	newSig := &builtinArithmeticIntDivideDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticIntDivideIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	a, aIsNull, err := s.args[0].EvalInt(ctx, row)
	if aIsNull || err != nil {
		return 0, aIsNull, err
	}
	b, bIsNull, err := s.args[1].EvalInt(ctx, row)
	if bIsNull || err != nil {
		return 0, bIsNull, err
	}

	if b == 0 {
		return 0, true, handleDivisionByZeroError(ctx)
	}

	var (
		ret int64
		val uint64
	)
	isLHSUnsigned := mysql.HasUnsignedFlag(s.args[0].GetType(ctx).GetFlag())
	isRHSUnsigned := mysql.HasUnsignedFlag(s.args[1].GetType(ctx).GetFlag())

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

func (s *builtinArithmeticIntDivideDecimalSig) evalInt(ctx EvalContext, row chunk.Row) (ret int64, isNull bool, err error) {
	ec := errCtx(ctx)
	var num [2]*types.MyDecimal
	for i, arg := range s.args {
		num[i], isNull, err = arg.EvalDecimal(ctx, row)
		if isNull || err != nil {
			return 0, isNull, err
		}
	}

	c := &types.MyDecimal{}
	err = types.DecimalDiv(num[0], num[1], c, ctx.GetDivPrecisionIncrement())
	if err == types.ErrDivByZero {
		return 0, true, handleDivisionByZeroError(ctx)
	}
	if err == types.ErrTruncated {
		err = ec.HandleError(errTruncatedWrongValue.GenWithStackByArgs("DECIMAL", c))
	}
	if err == types.ErrOverflow {
		newErr := errTruncatedWrongValue.GenWithStackByArgs("DECIMAL", c)
		err = ec.HandleError(newErr)
	}
	if err != nil {
		return 0, true, err
	}

	isLHSUnsigned := mysql.HasUnsignedFlag(s.args[0].GetType(ctx).GetFlag())
	isRHSUnsigned := mysql.HasUnsignedFlag(s.args[1].GetType(ctx).GetFlag())

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
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", fmt.Sprintf("(%s DIV %s)", s.args[0].StringWithCtx(ctx, errors.RedactLogDisable), s.args[1].StringWithCtx(ctx, errors.RedactLogDisable)))
		}
		ret = int64(val)
	} else {
		ret, err = c.ToInt()
		// err returned by ToInt may be ErrTruncated or ErrOverflow, only handle ErrOverflow, ignore ErrTruncated.
		if err == types.ErrOverflow {
			return 0, true, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("(%s DIV %s)", s.args[0].StringWithCtx(ctx, errors.RedactLogDisable), s.args[1].StringWithCtx(ctx, errors.RedactLogDisable)))
		}
	}

	return ret, false, nil
}
