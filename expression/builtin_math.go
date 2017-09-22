// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &absFunctionClass{}
	_ functionClass = &roundFunctionClass{}
	_ functionClass = &ceilFunctionClass{}
	_ functionClass = &floorFunctionClass{}
	_ functionClass = &logFunctionClass{}
	_ functionClass = &log2FunctionClass{}
	_ functionClass = &log10FunctionClass{}
	_ functionClass = &randFunctionClass{}
	_ functionClass = &powFunctionClass{}
	_ functionClass = &convFunctionClass{}
	_ functionClass = &crc32FunctionClass{}
	_ functionClass = &signFunctionClass{}
	_ functionClass = &sqrtFunctionClass{}
	_ functionClass = &acosFunctionClass{}
	_ functionClass = &asinFunctionClass{}
	_ functionClass = &atanFunctionClass{}
	_ functionClass = &cosFunctionClass{}
	_ functionClass = &cotFunctionClass{}
	_ functionClass = &degreesFunctionClass{}
	_ functionClass = &expFunctionClass{}
	_ functionClass = &piFunctionClass{}
	_ functionClass = &radiansFunctionClass{}
	_ functionClass = &sinFunctionClass{}
	_ functionClass = &tanFunctionClass{}
	_ functionClass = &truncateFunctionClass{}
)

var (
	_ builtinFunc = &builtinAbsRealSig{}
	_ builtinFunc = &builtinAbsIntSig{}
	_ builtinFunc = &builtinAbsUIntSig{}
	_ builtinFunc = &builtinAbsDecSig{}
	_ builtinFunc = &builtinRoundRealSig{}
	_ builtinFunc = &builtinRoundIntSig{}
	_ builtinFunc = &builtinRoundDecSig{}
	_ builtinFunc = &builtinRoundWithFracRealSig{}
	_ builtinFunc = &builtinRoundWithFracIntSig{}
	_ builtinFunc = &builtinRoundWithFracDecSig{}
	_ builtinFunc = &builtinCeilRealSig{}
	_ builtinFunc = &builtinCeilIntToDecSig{}
	_ builtinFunc = &builtinCeilIntToIntSig{}
	_ builtinFunc = &builtinCeilDecToIntSig{}
	_ builtinFunc = &builtinCeilDecToDecSig{}
	_ builtinFunc = &builtinFloorRealSig{}
	_ builtinFunc = &builtinFloorIntToDecSig{}
	_ builtinFunc = &builtinFloorIntToIntSig{}
	_ builtinFunc = &builtinFloorDecToIntSig{}
	_ builtinFunc = &builtinFloorDecToDecSig{}
	_ builtinFunc = &builtinLog1ArgSig{}
	_ builtinFunc = &builtinLog2ArgsSig{}
	_ builtinFunc = &builtinLog2Sig{}
	_ builtinFunc = &builtinLog10Sig{}
	_ builtinFunc = &builtinRandSig{}
	_ builtinFunc = &builtinRandWithSeedSig{}
	_ builtinFunc = &builtinPowSig{}
	_ builtinFunc = &builtinConvSig{}
	_ builtinFunc = &builtinCRC32Sig{}
	_ builtinFunc = &builtinSignSig{}
	_ builtinFunc = &builtinSqrtSig{}
	_ builtinFunc = &builtinAcosSig{}
	_ builtinFunc = &builtinAsinSig{}
	_ builtinFunc = &builtinAtan1ArgSig{}
	_ builtinFunc = &builtinAtan2ArgsSig{}
	_ builtinFunc = &builtinCosSig{}
	_ builtinFunc = &builtinCotSig{}
	_ builtinFunc = &builtinDegreesSig{}
	_ builtinFunc = &builtinExpSig{}
	_ builtinFunc = &builtinPISig{}
	_ builtinFunc = &builtinRadiansSig{}
	_ builtinFunc = &builtinSinSig{}
	_ builtinFunc = &builtinTanSig{}
	_ builtinFunc = &builtinTruncateIntSig{}
	_ builtinFunc = &builtinTruncateRealSig{}
	_ builtinFunc = &builtinTruncateDecimalSig{}
)

type absFunctionClass struct {
	baseFunctionClass
}

func (c *absFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(c.verifyArgs(args))
	}

	argFieldTp := args[0].GetType()
	argTp := fieldTp2EvalTp(argFieldTp)
	if argTp != tpInt && argTp != tpDecimal {
		argTp = tpReal
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, argTp, argTp)
	if mysql.HasUnsignedFlag(argFieldTp.Flag) {
		bf.tp.Flag |= mysql.UnsignedFlag
	}
	if argTp == tpReal {
		bf.tp.Flen, bf.tp.Decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeDouble)
	} else {
		bf.tp.Flen = argFieldTp.Flen
		bf.tp.Decimal = argFieldTp.Decimal
	}
	var sig builtinFunc
	switch argTp {
	case tpInt:
		if mysql.HasUnsignedFlag(argFieldTp.Flag) {
			sig = &builtinAbsUIntSig{baseIntBuiltinFunc{bf}}
			sig.setPbCode(tipb.ScalarFuncSig_AbsInt)
		} else {
			sig = &builtinAbsIntSig{baseIntBuiltinFunc{bf}}
			sig.setPbCode(tipb.ScalarFuncSig_AbsUInt)
		}
	case tpDecimal:
		sig = &builtinAbsDecSig{baseDecimalBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_AbsDecimal)
	case tpReal:
		sig = &builtinAbsRealSig{baseRealBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_AbsReal)
	default:
		panic("unexpected argTp")
	}
	return sig.setSelf(sig), nil
}

type builtinAbsRealSig struct {
	baseRealBuiltinFunc
}

// evalReal evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsRealSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return math.Abs(val), false, nil
}

type builtinAbsIntSig struct {
	baseIntBuiltinFunc
}

// evalInt evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val >= 0 {
		return val, false, nil
	}
	if val == math.MinInt64 {
		return 0, false, types.ErrOverflow.GenByArgs("BIGINT", fmt.Sprintf("abs(%d)", val))
	}
	return -val, false, nil
}

type builtinAbsUIntSig struct {
	baseIntBuiltinFunc
}

// evalInt evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsUIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	return b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
}

type builtinAbsDecSig struct {
	baseDecimalBuiltinFunc
}

// evalDecimal evals ABS(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsDecSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}
	to := new(types.MyDecimal)
	if !val.IsNegative() {
		*to = *val
	} else {
		if err = types.DecimalSub(new(types.MyDecimal), val, to); err != nil {
			return nil, false, err
		}
	}
	return to, false, errors.Trace(err)
}

func (c *roundFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(c.verifyArgs(args))
	}
	argTp := fieldTp2EvalTp(args[0].GetType())
	if argTp != tpInt && argTp != tpDecimal {
		argTp = tpReal
	}
	argTps := []evalTp{argTp}
	if len(args) > 1 {
		argTps = append(argTps, tpInt)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, argTp, argTps...)
	argFieldTp := args[0].GetType()
	if mysql.HasUnsignedFlag(argFieldTp.Flag) {
		bf.tp.Flag |= mysql.UnsignedFlag
	}
	bf.tp.Flen = argFieldTp.Flen
	bf.tp.Decimal = 0
	var sig builtinFunc
	if len(args) > 1 {
		switch argTp {
		case tpInt:
			sig = &builtinRoundWithFracIntSig{baseIntBuiltinFunc{bf}}
		case tpDecimal:
			sig = &builtinRoundWithFracDecSig{baseDecimalBuiltinFunc{bf}}
		case tpReal:
			sig = &builtinRoundWithFracRealSig{baseRealBuiltinFunc{bf}}
		default:
			panic("unexpected argTp")
		}
	} else {
		switch argTp {
		case tpInt:
			sig = &builtinRoundIntSig{baseIntBuiltinFunc{bf}}
		case tpDecimal:
			sig = &builtinRoundDecSig{baseDecimalBuiltinFunc{bf}}
		case tpReal:
			sig = &builtinRoundRealSig{baseRealBuiltinFunc{bf}}
		default:
			panic("unexpected argTp")
		}
	}
	return sig.setSelf(sig), nil
}

type builtinRoundRealSig struct {
	baseRealBuiltinFunc
}

// evalReal evals ROUND(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundRealSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return types.Round(val, 0), false, nil
}

type builtinRoundIntSig struct {
	baseIntBuiltinFunc
}

// evalInt evals ROUND(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	return b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
}

type builtinRoundDecSig struct {
	baseDecimalBuiltinFunc
}

// evalDecimal evals ROUND(value).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundDecSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}
	to := new(types.MyDecimal)
	if err = val.Round(to, 0, types.ModeHalfEven); err != nil {
		return nil, false, err
	}
	return to, false, errors.Trace(err)
}

type builtinRoundWithFracRealSig struct {
	baseRealBuiltinFunc
}

// evalReal evals ROUND(value, frac).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundWithFracRealSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	frac, isNull, err := b.args[1].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return types.Round(val, int(frac)), false, nil
}

type builtinRoundWithFracIntSig struct {
	baseIntBuiltinFunc
}

// evalInt evals ROUND(value, frac).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundWithFracIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	frac, isNull, err := b.args[1].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return int64(types.Round(float64(val), int(frac))), false, nil
}

type builtinRoundWithFracDecSig struct {
	baseDecimalBuiltinFunc
}

// evalDecimal evals ROUND(value, frac).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundWithFracDecSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}
	frac, isNull, err := b.args[1].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}
	to := new(types.MyDecimal)
	if err = val.Round(to, int(frac), types.ModeHalfEven); err != nil {
		return nil, false, err
	}
	return to, false, errors.Trace(err)
}

type ceilFunctionClass struct {
	baseFunctionClass
}

func (c *ceilFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	retTp, argTp := getEvalTp4FloorAndCeil(args[0])
	bf := newBaseBuiltinFuncWithTp(args, ctx, retTp, argTp)
	setFlag4FloorAndCeil(bf.tp, args[0])
	argFieldTp := args[0].GetType()
	bf.tp.Flen, bf.tp.Decimal = argFieldTp.Flen, 0

	switch argTp {
	case tpInt:
		if retTp == tpInt {
			sig = &builtinCeilIntToIntSig{baseIntBuiltinFunc{bf}}
			sig.setPbCode(tipb.ScalarFuncSig_CeilIntToInt)
		} else {
			sig = &builtinCeilIntToDecSig{baseDecimalBuiltinFunc{bf}}
			sig.setPbCode(tipb.ScalarFuncSig_CeilIntToDec)
		}
	case tpDecimal:
		if retTp == tpInt {
			sig = &builtinCeilDecToIntSig{baseIntBuiltinFunc{bf}}
			sig.setPbCode(tipb.ScalarFuncSig_CeilDecToInt)
		} else {
			sig = &builtinCeilDecToDecSig{baseDecimalBuiltinFunc{bf}}
			sig.setPbCode(tipb.ScalarFuncSig_CeilDecToDec)
		}
	default:
		sig = &builtinCeilRealSig{baseRealBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_CeilReal)
	}
	return sig.setSelf(sig), nil
}

type builtinCeilRealSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinCeilRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_ceil
func (b *builtinCeilRealSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return math.Ceil(val), false, nil
}

type builtinCeilIntToIntSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinCeilIntToIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_ceil
func (b *builtinCeilIntToIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	return b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
}

type builtinCeilIntToDecSig struct {
	baseDecimalBuiltinFunc
}

// evalDec evals a builtinCeilIntToDecSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_Ceil
func (b *builtinCeilIntToDecSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	return types.NewDecFromUint(uint64(val)), isNull, errors.Trace(err)
}

type builtinCeilDecToIntSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinCeilDecToIntSig.
// Ceil receives
func (b *builtinCeilDecToIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	// err here will only be ErrOverFlow(will never happen) or ErrTruncate(can be ignored).
	res, err := val.ToInt()
	if err == types.ErrTruncated && !val.IsNegative() {
		err = nil
		res = res + 1
	}
	return res, false, nil
}

type builtinCeilDecToDecSig struct {
	baseDecimalBuiltinFunc
}

// evalDec evals a builtinCeilDecToDecSig.
func (b *builtinCeilDecToDecSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}
	if val.GetDigitsFrac() > 0 && !val.IsNegative() {
		types.DecimalAdd(val, types.NewDecFromInt(1), val)
	}
	res := new(types.MyDecimal)
	val.Round(res, 0, types.ModeTruncate)
	return res, false, errors.Trace(err)
}

type floorFunctionClass struct {
	baseFunctionClass
}

// getEvalTp4FloorAndCeil gets the evalTp of FLOOR and CEIL.
func getEvalTp4FloorAndCeil(arg Expression) (retTp, argTp evalTp) {
	fieldTp := arg.GetType()
	retTp, argTp = tpInt, fieldTp2EvalTp(fieldTp)
	switch argTp {
	case tpInt:
		if fieldTp.Tp == mysql.TypeLonglong {
			retTp = tpDecimal
		}
	case tpDecimal:
		if fieldTp.Flen-fieldTp.Decimal > mysql.MaxIntWidth-2 { // len(math.MaxInt64) - 1
			retTp = tpDecimal
		}
	default:
		retTp, argTp = tpReal, tpReal
	}
	return retTp, argTp
}

// setFlag4FloorAndCeil sets return flag of FLOOR and CEIL.
func setFlag4FloorAndCeil(tp *types.FieldType, arg Expression) {
	fieldTp := arg.GetType()
	if (fieldTp.Tp == mysql.TypeLong || fieldTp.Tp == mysql.TypeNewDecimal) && mysql.HasUnsignedFlag(fieldTp.Flag) {
		tp.Flag |= mysql.UnsignedFlag
	}
	// TODO: when argument type is timestamp, add not null flag.
}

func (c *floorFunctionClass) getFunction(ctx context.Context, args []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	retTp, argTp := getEvalTp4FloorAndCeil(args[0])
	bf := newBaseBuiltinFuncWithTp(args, ctx, retTp, argTp)
	setFlag4FloorAndCeil(bf.tp, args[0])
	bf.tp.Flen, bf.tp.Decimal = args[0].GetType().Flen, 0
	switch argTp {
	case tpInt:
		if retTp == tpInt {
			sig = &builtinFloorIntToIntSig{baseIntBuiltinFunc{bf}}
			sig.setPbCode(tipb.ScalarFuncSig_FloorIntToInt)
		} else {
			sig = &builtinFloorIntToDecSig{baseDecimalBuiltinFunc{bf}}
			sig.setPbCode(tipb.ScalarFuncSig_FloorIntToDec)
		}
	case tpDecimal:
		if retTp == tpInt {
			sig = &builtinFloorDecToIntSig{baseIntBuiltinFunc{bf}}
			sig.setPbCode(tipb.ScalarFuncSig_FloorDecToInt)
		} else {
			sig = &builtinFloorDecToDecSig{baseDecimalBuiltinFunc{bf}}
			sig.setPbCode(tipb.ScalarFuncSig_FloorDecToDec)
		}
	default:
		sig = &builtinFloorRealSig{baseRealBuiltinFunc{bf}}
		sig.setPbCode(tipb.ScalarFuncSig_FloorReal)
	}
	return sig.setSelf(sig), nil
}

type builtinFloorRealSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinFloorRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorRealSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return math.Floor(val), false, nil
}

type builtinFloorIntToIntSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinFloorIntToIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorIntToIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	return b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
}

type builtinFloorIntToDecSig struct {
	baseDecimalBuiltinFunc
}

// evalDec evals a builtinFloorIntToDecSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorIntToDecSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	return types.NewDecFromUint(uint64(val)), isNull, errors.Trace(err)
}

type builtinFloorDecToIntSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinFloorDecToIntSig.
// floor receives
func (b *builtinFloorDecToIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	// err here will only be ErrOverFlow(will never happen) or ErrTruncate(can be ignored).
	res, err := val.ToInt()
	if err == types.ErrTruncated && val.IsNegative() {
		err = nil
		res--
	}
	return res, false, nil
}

type builtinFloorDecToDecSig struct {
	baseDecimalBuiltinFunc
}

// evalDec evals a builtinFloorDecToDecSig.
func (b *builtinFloorDecToDecSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	val, isNull, err := b.args[0].EvalDecimal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}
	if val.GetDigitsFrac() > 0 && val.IsNegative() {
		types.DecimalSub(val, types.NewDecFromInt(1), val)
	}
	res := new(types.MyDecimal)
	val.Round(res, 0, types.ModeTruncate)
	return res, false, errors.Trace(err)
}

type logFunctionClass struct {
	baseFunctionClass
}

func (c *logFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	var (
		sig     builtinFunc
		bf      baseBuiltinFunc
		argsLen = len(args)
	)

	if argsLen == 1 {
		bf = newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	} else {
		bf = newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal, tpReal)
	}

	if argsLen == 1 {
		sig = &builtinLog1ArgSig{baseRealBuiltinFunc{bf}}
	} else {
		sig = &builtinLog2ArgsSig{baseRealBuiltinFunc{bf}}
	}

	return sig.setSelf(sig), nil
}

type builtinLog1ArgSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinLog1ArgSig, corresponding to log(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log
func (b *builtinLog1ArgSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val <= 0 {
		return 0, true, nil
	}
	return math.Log(val), false, nil
}

type builtinLog2ArgsSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinLog2ArgsSig, corresponding to log(b, x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log
func (b *builtinLog2ArgsSig) evalReal(row []types.Datum) (float64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	val1, isNull, err := b.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	val2, isNull, err := b.args[1].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	if val1 <= 0 || val1 == 1 || val2 <= 0 {
		return 0, true, nil
	}

	return math.Log(val2) / math.Log(val1), false, nil
}

type log2FunctionClass struct {
	baseFunctionClass
}

func (c *log2FunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	sig := &builtinLog2Sig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinLog2Sig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinLog2Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log2
func (b *builtinLog2Sig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val <= 0 {
		return 0, true, nil
	}
	return math.Log2(val), false, nil
}

type log10FunctionClass struct {
	baseFunctionClass
}

func (c *log10FunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	sig := &builtinLog10Sig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinLog10Sig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinLog10Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log10
func (b *builtinLog10Sig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val <= 0 {
		return 0, true, nil
	}
	return math.Log10(val), false, nil
}

type randFunctionClass struct {
	baseFunctionClass
}

func (c *randFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	var sig builtinFunc
	var argTps []evalTp
	if len(args) > 0 {
		argTps = []evalTp{tpInt}
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, argTps...)
	bt := baseRealBuiltinFunc{bf}
	bt.foldable = false
	if len(args) == 0 {
		sig = &builtinRandSig{bt, nil}
	} else {
		sig = &builtinRandWithSeedSig{bt, nil}
	}
	return sig.setSelf(sig), nil
}

type builtinRandSig struct {
	baseRealBuiltinFunc
	randGen *rand.Rand
}

// evalReal evals RAND().
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_rand
func (b *builtinRandSig) evalReal(row []types.Datum) (float64, bool, error) {
	if b.randGen == nil {
		b.randGen = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return b.randGen.Float64(), false, nil
}

type builtinRandWithSeedSig struct {
	baseRealBuiltinFunc
	randGen *rand.Rand
}

// evalReal evals RAND(N).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_rand
func (b *builtinRandWithSeedSig) evalReal(row []types.Datum) (float64, bool, error) {
	seed, isNull, err := b.args[0].EvalInt(row, b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if b.randGen == nil {
		if isNull {
			// When seed is NULL, it is equal to RAND().
			b.randGen = rand.New(rand.NewSource(time.Now().UnixNano()))
		} else {
			b.randGen = rand.New(rand.NewSource(seed))
		}
	}
	return b.randGen.Float64(), false, nil
}

type powFunctionClass struct {
	baseFunctionClass
}

func (c *powFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal, tpReal)
	sig := &builtinPowSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinPowSig struct {
	baseRealBuiltinFunc
}

// evalReal evals POW(x, y).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_pow
func (b *builtinPowSig) evalReal(row []types.Datum) (float64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	x, isNull, err := b.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	y, isNull, err := b.args[1].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	power := math.Pow(x, y)
	if math.IsInf(power, -1) || math.IsInf(power, 1) || math.IsNaN(power) {
		return 0, false, types.ErrOverflow.GenByArgs("DOUBLE", fmt.Sprintf("pow(%s, %s)", strconv.FormatFloat(x, 'f', -1, 64), strconv.FormatFloat(y, 'f', -1, 64)))
	}
	return power, false, nil
}

type roundFunctionClass struct {
	baseFunctionClass
}

type convFunctionClass struct {
	baseFunctionClass
}

func (c *convFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	bf := newBaseBuiltinFuncWithTp(args, ctx, tpString, tpString, tpInt, tpInt)
	bf.tp.Flen = 64
	sig := &builtinConvSig{baseStringBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinConvSig struct {
	baseStringBuiltinFunc
}

// evalString evals CONV(N,from_base,to_base).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_conv.
func (b *builtinConvSig) evalString(row []types.Datum) (res string, isNull bool, err error) {
	sc := b.getCtx().GetSessionVars().StmtCtx
	n, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}

	fromBase, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}

	toBase, isNull, err := b.args[2].EvalInt(row, sc)
	if isNull || err != nil {
		return res, isNull, errors.Trace(err)
	}

	var (
		signed     bool
		negative   bool
		ignoreSign bool
	)
	if fromBase < 0 {
		fromBase = -fromBase
		signed = true
	}

	if toBase < 0 {
		toBase = -toBase
		ignoreSign = true
	}

	if fromBase > 36 || fromBase < 2 || toBase > 36 || toBase < 2 {
		return res, true, nil
	}

	n = getValidPrefix(strings.TrimSpace(n), fromBase)
	if len(n) == 0 {
		return "0", false, nil
	}

	if n[0] == '-' {
		negative = true
		n = n[1:]
	}

	val, err := strconv.ParseUint(n, int(fromBase), 64)
	if err != nil {
		return res, false, types.ErrOverflow.GenByArgs("BIGINT UNSINGED", n)
	}
	if signed {
		if negative && val > -math.MinInt64 {
			val = -math.MinInt64
		}
		if !negative && val > math.MaxInt64 {
			val = math.MaxInt64
		}
	}
	if negative {
		val = -val
	}

	if int64(val) < 0 {
		negative = true
	} else {
		negative = false
	}
	if ignoreSign && negative {
		val = 0 - val
	}

	s := strconv.FormatUint(val, int(toBase))
	if negative && ignoreSign {
		s = "-" + s
	}
	res = strings.ToUpper(s)
	return res, false, nil
}

type crc32FunctionClass struct {
	baseFunctionClass
}

func (c *crc32FunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString)
	bf.tp.Flen = 10
	bf.tp.Flag |= mysql.UnsignedFlag
	sig := &builtinCRC32Sig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinCRC32Sig struct {
	baseIntBuiltinFunc
}

// evalInt evals a CRC32(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_crc32
func (b *builtinCRC32Sig) evalInt(row []types.Datum) (int64, bool, error) {
	x, isNull, err := b.args[0].EvalString(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	r := crc32.ChecksumIEEE([]byte(x))
	return int64(r), false, nil
}

type signFunctionClass struct {
	baseFunctionClass
}

func (c *signFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpReal)
	sig := &builtinSignSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinSignSig struct {
	baseIntBuiltinFunc
}

// evalInt evals SIGN(v).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sign
func (b *builtinSignSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val > 0 {
		return 1, false, nil
	} else if val == 0 {
		return 0, false, nil
	} else {
		return -1, false, nil
	}
}

type sqrtFunctionClass struct {
	baseFunctionClass
}

func (c *sqrtFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	sig := &builtinSqrtSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinSqrtSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a SQRT(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sqrt
func (b *builtinSqrtSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val < 0 {
		return 0, true, nil
	}
	return math.Sqrt(val), false, nil
}

type acosFunctionClass struct {
	baseFunctionClass
}

func (c *acosFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	sig := &builtinAcosSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinAcosSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinAcosSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_acos
func (b *builtinAcosSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	if val < -1 || val > 1 {
		return 0, true, nil
	}

	return math.Acos(val), false, nil
}

type asinFunctionClass struct {
	baseFunctionClass
}

func (c *asinFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	sig := &builtinAsinSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinAsinSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinAsinSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_asin
func (b *builtinAsinSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	if val < -1 || val > 1 {
		return 0, true, nil
	}

	return math.Asin(val), false, nil
}

type atanFunctionClass struct {
	baseFunctionClass
}

func (c *atanFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	var (
		sig     builtinFunc
		bf      baseBuiltinFunc
		argsLen = len(args)
	)

	if argsLen == 1 {
		bf = newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	} else {
		bf = newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal, tpReal)
	}

	if argsLen == 1 {
		sig = &builtinAtan1ArgSig{baseRealBuiltinFunc{bf}}
	} else {
		sig = &builtinAtan2ArgsSig{baseRealBuiltinFunc{bf}}
	}

	return sig.setSelf(sig), nil
}

type builtinAtan1ArgSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinAtan1ArgSig, corresponding to atan(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_atan
func (b *builtinAtan1ArgSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	return math.Atan(val), false, nil
}

type builtinAtan2ArgsSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinAtan1ArgSig, corresponding to atan(y, x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_atan
func (b *builtinAtan2ArgsSig) evalReal(row []types.Datum) (float64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	val1, isNull, err := b.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	val2, isNull, err := b.args[1].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	return math.Atan2(val1, val2), false, nil
}

type cosFunctionClass struct {
	baseFunctionClass
}

func (c *cosFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	sig := &builtinCosSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinCosSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinCosSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_cos
func (b *builtinCosSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return math.Cos(val), false, nil
}

type cotFunctionClass struct {
	baseFunctionClass
}

func (c *cotFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	sig := &builtinCotSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinCotSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinCotSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_cot
func (b *builtinCotSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	tan := math.Tan(val)
	if tan != 0 {
		cot := 1 / tan
		if !math.IsInf(cot, 0) && !math.IsNaN(cot) {
			return cot, false, nil
		}
	}
	return 0, false, types.ErrOverflow.GenByArgs("DOUBLE", fmt.Sprintf("cot(%s)", strconv.FormatFloat(val, 'f', -1, 64)))
}

type degreesFunctionClass struct {
	baseFunctionClass
}

func (c *degreesFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	sig := &builtinDegreesSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinDegreesSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinDegreesSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_degrees
func (b *builtinDegreesSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	res := val * 180 / math.Pi
	return res, false, nil
}

type expFunctionClass struct {
	baseFunctionClass
}

func (c *expFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	sig := &builtinExpSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinExpSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinExpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_exp
func (b *builtinExpSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	exp := math.Exp(val)
	if math.IsInf(exp, 0) || math.IsNaN(exp) {
		s := fmt.Sprintf("exp(%s)", strconv.FormatFloat(val, 'f', -1, 64))
		return 0, false, types.ErrOverflow.GenByArgs("DOUBLE", s)
	}
	return exp, false, nil
}

type piFunctionClass struct {
	baseFunctionClass
}

func (c *piFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	var (
		bf  baseBuiltinFunc
		sig builtinFunc
	)

	bf = newBaseBuiltinFuncWithTp(args, ctx, tpReal)
	bf.tp.Decimal = 6
	bf.tp.Flen = 8
	sig = &builtinPISig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinPISig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinPISig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_pi
func (b *builtinPISig) evalReal(_ []types.Datum) (float64, bool, error) {
	return float64(math.Pi), false, nil
}

type radiansFunctionClass struct {
	baseFunctionClass
}

func (c *radiansFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	sig := &builtinRadiansSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinRadiansSig struct {
	baseRealBuiltinFunc
}

// evalReal evals RADIANS(X).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_radians
func (b *builtinRadiansSig) evalReal(row []types.Datum) (float64, bool, error) {
	x, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return x * math.Pi / 180, false, nil
}

type sinFunctionClass struct {
	baseFunctionClass
}

func (c *sinFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	sig := &builtinSinSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinSinSig struct {
	baseRealBuiltinFunc
}

// evalreal evals a builtinSinSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sin
func (b *builtinSinSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return math.Sin(val), false, nil
}

type tanFunctionClass struct {
	baseFunctionClass
}

func (c *tanFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	sig := &builtinTanSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinTanSig struct {
	baseRealBuiltinFunc
}

// eval evals a builtinTanSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_tan
func (b *builtinTanSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return math.Tan(val), false, nil
}

type truncateFunctionClass struct {
	baseFunctionClass
}

// getDecimal returns the `Decimal` value of return type for function `TRUNCATE`.
func (c *truncateFunctionClass) getDecimal(sc *variable.StatementContext, arg Expression) int {
	if constant, ok := arg.(*Constant); ok {
		decimal, isNull, err := constant.EvalInt(nil, sc)
		if isNull || err != nil {
			return 0
		} else if decimal > 30 {
			return 30
		} else if decimal < 0 {
			return 0
		}
		return int(decimal)
	}
	return 3
}

func (c *truncateFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	argTp := fieldTp2EvalTp(args[0].GetType())
	if argTp == tpTimestamp || argTp == tpDatetime || argTp == tpDuration || argTp == tpString {
		argTp = tpReal
	}

	bf := newBaseBuiltinFuncWithTp(args, ctx, argTp, argTp, tpInt)

	if argTp == tpInt {
		bf.tp.Decimal = 0
	} else {
		bf.tp.Decimal = c.getDecimal(bf.ctx.GetSessionVars().StmtCtx, args[1])
	}
	bf.tp.Flen = args[0].GetType().Flen - args[0].GetType().Decimal + bf.tp.Decimal
	bf.tp.Flag |= args[0].GetType().Flag

	var sig builtinFunc
	switch argTp {
	case tpInt:
		sig = &builtinTruncateIntSig{baseIntBuiltinFunc{bf}}
	case tpReal:
		sig = &builtinTruncateRealSig{baseRealBuiltinFunc{bf}}
	case tpDecimal:
		sig = &builtinTruncateDecimalSig{baseDecimalBuiltinFunc{bf}}
	}

	return sig.setSelf(sig), nil
}

type builtinTruncateDecimalSig struct {
	baseDecimalBuiltinFunc
}

// evalDecimal evals a TRUNCATE(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateDecimalSig) evalDecimal(row []types.Datum) (*types.MyDecimal, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	x, isNull, err := b.args[0].EvalDecimal(row, sc)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}

	d, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return nil, isNull, errors.Trace(err)
	}

	result := new(types.MyDecimal)
	if err := x.Round(result, int(d), types.ModeTruncate); err != nil {
		return nil, true, errors.Trace(err)
	}
	return result, false, nil
}

type builtinTruncateRealSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a TRUNCATE(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateRealSig) evalReal(row []types.Datum) (float64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	x, isNull, err := b.args[0].EvalReal(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	d, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	return types.Truncate(x, int(d)), false, nil
}

type builtinTruncateIntSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a TRUNCATE(X,D).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	x, isNull, err := b.args[0].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	d, isNull, err := b.args[1].EvalInt(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	floatX := float64(x)
	return int64(types.Truncate(floatX, int(d))), false, nil
}
