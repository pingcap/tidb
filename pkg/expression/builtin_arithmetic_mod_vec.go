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
	"math"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type arithmeticModFunctionClass struct {
	baseFunctionClass
}

func (c *arithmeticModFunctionClass) setType4ModRealOrDecimal(retTp, a, b *types.FieldType, isDecimal bool) {
	if a.GetDecimal() == types.UnspecifiedLength || b.GetDecimal() == types.UnspecifiedLength {
		retTp.SetDecimal(types.UnspecifiedLength)
	} else {
		retTp.SetDecimalUnderLimit(max(a.GetDecimal(), b.GetDecimal()))
	}

	if a.GetFlen() == types.UnspecifiedLength || b.GetFlen() == types.UnspecifiedLength {
		retTp.SetFlen(types.UnspecifiedLength)
	} else {
		retTp.SetFlen(max(a.GetFlen(), b.GetFlen()))
		if isDecimal {
			retTp.SetFlenUnderLimit(retTp.GetFlen())
			return
		}
		retTp.SetFlen(min(retTp.GetFlen(), mysql.MaxRealWidth))
	}
}

func (c *arithmeticModFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	lhsTp, rhsTp := args[0].GetType(ctx.GetEvalCtx()), args[1].GetType(ctx.GetEvalCtx())
	lhsEvalTp, rhsEvalTp := numericContextResultType(ctx.GetEvalCtx(), args[0]), numericContextResultType(ctx.GetEvalCtx(), args[1])
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
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	if mysql.HasUnsignedFlag(lhsTp.GetFlag()) {
		bf.tp.AddFlag(mysql.UnsignedFlag)
	}
	isLHSUnsigned := mysql.HasUnsignedFlag(args[0].GetType(ctx.GetEvalCtx()).GetFlag())
	isRHSUnsigned := mysql.HasUnsignedFlag(args[1].GetType(ctx.GetEvalCtx()).GetFlag())
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

type builtinArithmeticModRealSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticModRealSig) Clone() builtinFunc {
	newSig := &builtinArithmeticModRealSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	a, aIsNull, err := s.args[0].EvalReal(ctx, row)
	if err != nil {
		return 0, false, err
	}

	b, bIsNull, err := s.args[1].EvalReal(ctx, row)
	if err != nil {
		return 0, false, err
	}

	if aIsNull || bIsNull {
		return 0, true, nil
	}

	if b == 0 {
		return 0, true, handleDivisionByZeroError(ctx)
	}

	return math.Mod(a, b), false, nil
}

type builtinArithmeticModDecimalSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticModDecimalSig) Clone() builtinFunc {
	newSig := &builtinArithmeticModDecimalSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModDecimalSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	a, isNull, err := s.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	b, isNull, err := s.args[1].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	c := &types.MyDecimal{}
	err = types.DecimalMod(a, b, c)
	if err == types.ErrDivByZero {
		return c, true, handleDivisionByZeroError(ctx)
	}
	return c, err != nil, err
}

type builtinArithmeticModIntUnsignedUnsignedSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticModIntUnsignedUnsignedSig) Clone() builtinFunc {
	newSig := &builtinArithmeticModIntUnsignedUnsignedSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModIntUnsignedUnsignedSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	a, aIsNull, err := s.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, false, err
	}

	b, bIsNull, err := s.args[1].EvalInt(ctx, row)
	if err != nil {
		return 0, false, err
	}

	if aIsNull || bIsNull {
		return 0, true, nil
	}

	if b == 0 {
		return 0, true, handleDivisionByZeroError(ctx)
	}

	ret := int64(uint64(a) % uint64(b))

	return ret, false, nil
}

type builtinArithmeticModIntUnsignedSignedSig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticModIntUnsignedSignedSig) Clone() builtinFunc {
	newSig := &builtinArithmeticModIntUnsignedSignedSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModIntUnsignedSignedSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	a, aIsNull, err := s.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, false, err
	}

	b, bIsNull, err := s.args[1].EvalInt(ctx, row)
	if err != nil {
		return 0, false, err
	}

	if aIsNull || bIsNull {
		return 0, true, nil
	}

	if b == 0 {
		return 0, true, handleDivisionByZeroError(ctx)
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticModIntSignedUnsignedSig) Clone() builtinFunc {
	newSig := &builtinArithmeticModIntSignedUnsignedSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModIntSignedUnsignedSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	a, aIsNull, err := s.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, false, err
	}

	b, bIsNull, err := s.args[1].EvalInt(ctx, row)
	if err != nil {
		return 0, false, err
	}

	if aIsNull || bIsNull {
		return 0, true, nil
	}

	if b == 0 {
		return 0, true, handleDivisionByZeroError(ctx)
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

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticModIntSignedSignedSig) Clone() builtinFunc {
	newSig := &builtinArithmeticModIntSignedSignedSig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticModIntSignedSignedSig) evalInt(ctx EvalContext, row chunk.Row) (val int64, isNull bool, err error) {
	a, aIsNull, err := s.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, false, err
	}

	b, bIsNull, err := s.args[1].EvalInt(ctx, row)
	if err != nil {
		return 0, false, err
	}

	if aIsNull || bIsNull {
		return 0, true, nil
	}

	if b == 0 {
		return 0, true, handleDivisionByZeroError(ctx)
	}

	return a % b, false, nil
}

type builtinArithmeticPlusVectorFloat32Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticPlusVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinArithmeticPlusVectorFloat32Sig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticPlusVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (types.VectorFloat32, bool, error) {
	a, isLHSNull, err := s.args[0].EvalVectorFloat32(ctx, row)
	if err != nil {
		return types.ZeroVectorFloat32, isLHSNull, err
	}
	b, isRHSNull, err := s.args[1].EvalVectorFloat32(ctx, row)
	if err != nil {
		return types.ZeroVectorFloat32, isRHSNull, err
	}
	if isLHSNull || isRHSNull {
		return types.ZeroVectorFloat32, true, nil
	}
	v, err := a.Add(b)
	if err != nil {
		return types.ZeroVectorFloat32, true, err
	}
	return v, false, nil
}

type builtinArithmeticMinusVectorFloat32Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticMinusVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinArithmeticMinusVectorFloat32Sig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMinusVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (types.VectorFloat32, bool, error) {
	a, isLHSNull, err := s.args[0].EvalVectorFloat32(ctx, row)
	if err != nil {
		return types.ZeroVectorFloat32, isLHSNull, err
	}
	b, isRHSNull, err := s.args[1].EvalVectorFloat32(ctx, row)
	if err != nil {
		return types.ZeroVectorFloat32, isRHSNull, err
	}
	if isLHSNull || isRHSNull {
		return types.ZeroVectorFloat32, true, nil
	}
	v, err := a.Sub(b)
	if err != nil {
		return types.ZeroVectorFloat32, true, err
	}
	return v, false, nil
}

type builtinArithmeticMultiplyVectorFloat32Sig struct {
	baseBuiltinFunc

	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (s *builtinArithmeticMultiplyVectorFloat32Sig) Clone() builtinFunc {
	newSig := &builtinArithmeticMultiplyVectorFloat32Sig{}
	newSig.cloneFrom(&s.baseBuiltinFunc)
	return newSig
}

func (s *builtinArithmeticMultiplyVectorFloat32Sig) evalVectorFloat32(ctx EvalContext, row chunk.Row) (types.VectorFloat32, bool, error) {
	a, isLHSNull, err := s.args[0].EvalVectorFloat32(ctx, row)
	if err != nil {
		return types.ZeroVectorFloat32, isLHSNull, err
	}
	b, isRHSNull, err := s.args[1].EvalVectorFloat32(ctx, row)
	if err != nil {
		return types.ZeroVectorFloat32, isRHSNull, err
	}
	if isLHSNull || isRHSNull {
		return types.ZeroVectorFloat32, true, nil
	}
	v, err := a.Mul(b)
	if err != nil {
		return types.ZeroVectorFloat32, true, err
	}
	return v, false, nil
}
