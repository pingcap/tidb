// Copyright 2015 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package expression

import (
	"fmt"
	"math"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
)

type absFunctionClass struct {
	baseFunctionClass
}

func (c *absFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, c.verifyArgs(args)
	}

	argFieldTp := args[0].GetType(ctx.GetEvalCtx())
	argTp := argFieldTp.EvalType()
	if argTp != types.ETInt && argTp != types.ETDecimal {
		argTp = types.ETReal
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, argTp, argTp)
	if err != nil {
		return nil, err
	}
	if mysql.HasUnsignedFlag(argFieldTp.GetFlag()) {
		bf.tp.AddFlag(mysql.UnsignedFlag)
	}
	if argTp == types.ETReal {
		flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeDouble)
		bf.tp.SetFlen(flen)
		bf.tp.SetDecimal(decimal)
	} else {
		bf.tp.SetFlenUnderLimit(argFieldTp.GetFlen())
		bf.tp.SetDecimalUnderLimit(argFieldTp.GetDecimal())
	}
	var sig builtinFunc
	switch argTp {
	case types.ETInt:
		if mysql.HasUnsignedFlag(argFieldTp.GetFlag()) {
			sig = &builtinAbsUIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AbsUInt)
		} else {
			sig = &builtinAbsIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_AbsInt)
		}
	case types.ETDecimal:
		sig = &builtinAbsDecSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_AbsDecimal)
	case types.ETReal:
		sig = &builtinAbsRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_AbsReal)
	default:
		panic("unexpected argTp")
	}
	return sig, nil
}

type builtinAbsRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAbsRealSig) Clone() builtinFunc {
	newSig := &builtinAbsRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Abs(val), false, nil
}

type builtinAbsIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAbsIntSig) Clone() builtinFunc {
	newSig := &builtinAbsIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val >= 0 {
		return val, false, nil
	}
	if val == math.MinInt64 {
		return 0, false, types.ErrOverflow.GenWithStackByArgs("BIGINT", fmt.Sprintf("abs(%d)", val))
	}
	return -val, false, nil
}

type builtinAbsUIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAbsUIntSig) Clone() builtinFunc {
	newSig := &builtinAbsUIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsUIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	return b.args[0].EvalInt(ctx, row)
}

type builtinAbsDecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinAbsDecSig) Clone() builtinFunc {
	newSig := &builtinAbsDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	to := new(types.MyDecimal)
	if !val.IsNegative() {
		*to = *val
	} else {
		if err = types.DecimalSub(new(types.MyDecimal), val, to); err != nil {
			return nil, true, err
		}
	}
	return to, false, nil
}

func (c *roundFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, c.verifyArgs(args)
	}
	argTp := args[0].GetType(ctx.GetEvalCtx()).EvalType()
	if argTp != types.ETInt && argTp != types.ETDecimal {
		argTp = types.ETReal
	}
	argTps := []types.EvalType{argTp}
	if len(args) > 1 {
		argTps = append(argTps, types.ETInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, argTp, argTps...)
	if err != nil {
		return nil, err
	}
	argFieldTp := args[0].GetType(ctx.GetEvalCtx())
	if mysql.HasUnsignedFlag(argFieldTp.GetFlag()) {
		bf.tp.AddFlag(mysql.UnsignedFlag)
	}

	// ETInt or ETReal is set correctly by newBaseBuiltinFuncWithTp, only need to handle ETDecimal.
	if argTp == types.ETDecimal {
		bf.tp.SetFlenUnderLimit(argFieldTp.GetFlen())
		bf.tp.SetDecimalUnderLimit(calculateDecimal4RoundAndTruncate(ctx, args, argTp))
		if bf.tp.GetDecimal() != types.UnspecifiedLength {
			if argFieldTp.GetDecimal() != types.UnspecifiedLength {
				decimalDelta := bf.tp.GetDecimal() - argFieldTp.GetDecimal()
				bf.tp.SetFlenUnderLimit(bf.tp.GetFlen() + max(decimalDelta, 0))
			} else {
				bf.tp.SetFlenUnderLimit(argFieldTp.GetFlen() + bf.tp.GetDecimal())
			}
		}
	}

	var sig builtinFunc
	if len(args) > 1 {
		switch argTp {
		case types.ETInt:
			sig = &builtinRoundWithFracIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RoundWithFracInt)
		case types.ETDecimal:
			sig = &builtinRoundWithFracDecSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RoundWithFracDec)
		case types.ETReal:
			sig = &builtinRoundWithFracRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RoundWithFracReal)
		default:
			panic("unexpected argTp")
		}
	} else {
		switch argTp {
		case types.ETInt:
			sig = &builtinRoundIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RoundInt)
		case types.ETDecimal:
			sig = &builtinRoundDecSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RoundDec)
		case types.ETReal:
			sig = &builtinRoundRealSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_RoundReal)
		default:
			panic("unexpected argTp")
		}
	}
	return sig, nil
}

// calculateDecimal4RoundAndTruncate calculates tp.decimals of round/truncate func.
func calculateDecimal4RoundAndTruncate(ctx BuildContext, args []Expression, retType types.EvalType) int {
	if retType == types.ETInt || len(args) <= 1 {
		return 0
	}
	secondConst, secondIsConst := args[1].(*Constant)
	if !secondIsConst {
		return args[0].GetType(ctx.GetEvalCtx()).GetDecimal()
	}
	argDec, isNull, err := secondConst.EvalInt(ctx.GetEvalCtx(), chunk.Row{})
	if err != nil || isNull || argDec < 0 {
		return 0
	}
	if argDec > mysql.MaxDecimalScale {
		return mysql.MaxDecimalScale
	}
	return int(argDec)
}

type builtinRoundRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRoundRealSig) Clone() builtinFunc {
	newSig := &builtinRoundRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals ROUND(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return types.Round(val, 0), false, nil
}

type builtinRoundIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRoundIntSig) Clone() builtinFunc {
	newSig := &builtinRoundIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals ROUND(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	return b.args[0].EvalInt(ctx, row)
}

type builtinRoundDecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRoundDecSig) Clone() builtinFunc {
	newSig := &builtinRoundDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals ROUND(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	to := new(types.MyDecimal)
	if err = val.Round(to, 0, types.ModeHalfUp); err != nil {
		return nil, true, err
	}
	return to, false, nil
}

type builtinRoundWithFracRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRoundWithFracRealSig) Clone() builtinFunc {
	newSig := &builtinRoundWithFracRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals ROUND(value, frac).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundWithFracRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	frac, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return types.Round(val, int(frac)), false, nil
}

type builtinRoundWithFracIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRoundWithFracIntSig) Clone() builtinFunc {
	newSig := &builtinRoundWithFracIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals ROUND(value, frac).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundWithFracIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	frac, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return int64(types.Round(float64(val), int(frac))), false, nil
}

type builtinRoundWithFracDecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRoundWithFracDecSig) Clone() builtinFunc {
	newSig := &builtinRoundWithFracDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals ROUND(value, frac).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundWithFracDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	frac, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}
	to := new(types.MyDecimal)
	if err = val.Round(to, min(int(frac), b.tp.GetDecimal()), types.ModeHalfUp); err != nil {
		return nil, true, err
	}
	return to, false, nil
}

type ceilFunctionClass struct {
	baseFunctionClass
}

func (c *ceilFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	retTp, argTp := getEvalTp4FloorAndCeil(ctx.GetEvalCtx(), args[0])
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, retTp, argTp)
	if err != nil {
		return nil, err
	}
	setFlag4FloorAndCeil(ctx.GetEvalCtx(), bf.tp, args[0])
	// ETInt or ETReal is set correctly by newBaseBuiltinFuncWithTp, only need to handle ETDecimal.
	if retTp == types.ETDecimal {
		bf.tp.SetFlenUnderLimit(args[0].GetType(ctx.GetEvalCtx()).GetFlen())
		bf.tp.SetDecimal(0)
	}

	switch argTp {
	case types.ETInt:
		if retTp == types.ETInt {
			sig = &builtinCeilIntToIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_CeilIntToInt)
		} else {
			sig = &builtinCeilIntToDecSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_CeilIntToDec)
		}
	case types.ETDecimal:
		if retTp == types.ETInt {
			sig = &builtinCeilDecToIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_CeilDecToInt)
		} else {
			sig = &builtinCeilDecToDecSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_CeilDecToDec)
		}
	default:
		sig = &builtinCeilRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_CeilReal)
	}
	return sig, nil
}

type builtinCeilRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCeilRealSig) Clone() builtinFunc {
	newSig := &builtinCeilRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinCeilRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_ceil
func (b *builtinCeilRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Ceil(val), false, nil
}

type builtinCeilIntToIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCeilIntToIntSig) Clone() builtinFunc {
	newSig := &builtinCeilIntToIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCeilIntToIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_ceil
func (b *builtinCeilIntToIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	return b.args[0].EvalInt(ctx, row)
}

type builtinCeilIntToDecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCeilIntToDecSig) Clone() builtinFunc {
	newSig := &builtinCeilIntToDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinCeilIntToDecSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_Ceil
func (b *builtinCeilIntToDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return nil, true, err
	}

	if mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()) || val >= 0 {
		return types.NewDecFromUint(uint64(val)), false, nil
	}
	return types.NewDecFromInt(val), false, nil
}

type builtinCeilDecToIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCeilDecToIntSig) Clone() builtinFunc {
	newSig := &builtinCeilDecToIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinCeilDecToIntSig.
// Ceil receives
func (b *builtinCeilDecToIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	// err here will only be ErrOverFlow(will never happen) or ErrTruncate(can be ignored).
	res, err := val.ToInt()
	if err == types.ErrTruncated {
		err = nil
		if !val.IsNegative() {
			res = res + 1
		}
	}
	return res, false, err
}

type builtinCeilDecToDecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCeilDecToDecSig) Clone() builtinFunc {
	newSig := &builtinCeilDecToDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinCeilDecToDecSig.
func (b *builtinCeilDecToDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, isNull, err
	}

	res := new(types.MyDecimal)
	if val.IsNegative() {
		err = val.Round(res, 0, types.ModeTruncate)
		return res, err != nil, err
	}

	err = val.Round(res, 0, types.ModeTruncate)
	if err != nil || res.Compare(val) == 0 {
		return res, err != nil, err
	}

	err = types.DecimalAdd(res, types.NewDecFromInt(1), res)
	return res, err != nil, err
}

type floorFunctionClass struct {
	baseFunctionClass
}

// getEvalTp4FloorAndCeil gets the types.EvalType of FLOOR and CEIL.
func getEvalTp4FloorAndCeil(ctx EvalContext, arg Expression) (retTp, argTp types.EvalType) {
	fieldTp := arg.GetType(ctx)
	retTp, argTp = types.ETInt, fieldTp.EvalType()
	switch argTp {
	case types.ETInt:
		retTp = types.ETInt
	case types.ETDecimal:
		if fieldTp.GetFlen()-fieldTp.GetDecimal() > mysql.MaxIntWidth-2 { // len(math.MaxInt64) - 1
			retTp = types.ETDecimal
		}
	default:
		retTp, argTp = types.ETReal, types.ETReal
	}
	return retTp, argTp
}

// setFlag4FloorAndCeil sets return flag of FLOOR and CEIL.
func setFlag4FloorAndCeil(ctx EvalContext, tp *types.FieldType, arg Expression) {
	fieldTp := arg.GetType(ctx)
	if (fieldTp.GetType() == mysql.TypeLong || fieldTp.GetType() == mysql.TypeLonglong || fieldTp.GetType() == mysql.TypeNewDecimal) && mysql.HasUnsignedFlag(fieldTp.GetFlag()) {
		tp.AddFlag(mysql.UnsignedFlag)
	}
	// TODO: when argument type is timestamp, add not null flag.
}

func (c *floorFunctionClass) getFunction(ctx BuildContext, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, err
	}

	retTp, argTp := getEvalTp4FloorAndCeil(ctx.GetEvalCtx(), args[0])
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, retTp, argTp)
	if err != nil {
		return nil, err
	}
	setFlag4FloorAndCeil(ctx.GetEvalCtx(), bf.tp, args[0])

	// ETInt or ETReal is set correctly by newBaseBuiltinFuncWithTp, only need to handle ETDecimal.
	if retTp == types.ETDecimal {
		bf.tp.SetFlenUnderLimit(args[0].GetType(ctx.GetEvalCtx()).GetFlen())
		bf.tp.SetDecimal(0)
	}
	switch argTp {
	case types.ETInt:
		if retTp == types.ETInt {
			sig = &builtinFloorIntToIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_FloorIntToInt)
		} else {
			sig = &builtinFloorIntToDecSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_FloorIntToDec)
		}
	case types.ETDecimal:
		if retTp == types.ETInt {
			sig = &builtinFloorDecToIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_FloorDecToInt)
		} else {
			sig = &builtinFloorDecToDecSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_FloorDecToDec)
		}
	default:
		sig = &builtinFloorRealSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_FloorReal)
	}
	return sig, nil
}

type builtinFloorRealSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFloorRealSig) Clone() builtinFunc {
	newSig := &builtinFloorRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinFloorRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorRealSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	return math.Floor(val), false, nil
}

type builtinFloorIntToIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFloorIntToIntSig) Clone() builtinFunc {
	newSig := &builtinFloorIntToIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinFloorIntToIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorIntToIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	return b.args[0].EvalInt(ctx, row)
}

type builtinFloorIntToDecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFloorIntToDecSig) Clone() builtinFunc {
	newSig := &builtinFloorIntToDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinFloorIntToDecSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorIntToDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return nil, true, err
	}

	if mysql.HasUnsignedFlag(b.args[0].GetType(ctx).GetFlag()) || val >= 0 {
		return types.NewDecFromUint(uint64(val)), false, nil
	}
	return types.NewDecFromInt(val), false, nil
}

type builtinFloorDecToIntSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFloorDecToIntSig) Clone() builtinFunc {
	newSig := &builtinFloorDecToIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinFloorDecToIntSig.
// floor receives
func (b *builtinFloorDecToIntSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	// err here will only be ErrOverFlow(will never happen) or ErrTruncate(can be ignored).
	res, err := val.ToInt()
	if err == types.ErrTruncated {
		err = nil
		if val.IsNegative() {
			res--
		}
	}
	return res, false, err
}

type builtinFloorDecToDecSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinFloorDecToDecSig) Clone() builtinFunc {
	newSig := &builtinFloorDecToDecSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalDecimal evals a builtinFloorDecToDecSig.
func (b *builtinFloorDecToDecSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(ctx, row)
	if isNull || err != nil {
		return nil, true, err
	}

	res := new(types.MyDecimal)
	if !val.IsNegative() {
		err = val.Round(res, 0, types.ModeTruncate)
		return res, err != nil, err
	}

	err = val.Round(res, 0, types.ModeTruncate)
	if err != nil || res.Compare(val) == 0 {
		return res, err != nil, err
	}

	err = types.DecimalSub(res, types.NewDecFromInt(1), res)
	return res, err != nil, err
}
