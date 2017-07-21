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
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ functionClass = &absFunctionClass{}
	_ functionClass = &ceilFunctionClass{}
	_ functionClass = &floorFunctionClass{}
	_ functionClass = &logFunctionClass{}
	_ functionClass = &log2FunctionClass{}
	_ functionClass = &log10FunctionClass{}
	_ functionClass = &randFunctionClass{}
	_ functionClass = &powFunctionClass{}
	_ functionClass = &roundFunctionClass{}
	_ functionClass = &convFunctionClass{}
	_ functionClass = &crc32FunctionClass{}
	_ functionClass = &signFunctionClass{}
	_ functionClass = &sqrtFunctionClass{}
	_ functionClass = &arithmeticFunctionClass{}
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
	_ builtinFunc = &builtinAbsSig{}

	_ builtinFunc = &builtinCeilRealSig{}
	_ builtinFunc = &builtinCeilIntSig{}

	_ builtinFunc = &builtinFloorRealSig{}
	_ builtinFunc = &builtinFloorIntSig{}

	_ builtinFunc = &builtinLog1ArgSig{}
	_ builtinFunc = &builtinLog2ArgsSig{}
	_ builtinFunc = &builtinLog2Sig{}
	_ builtinFunc = &builtinLog10Sig{}
	_ builtinFunc = &builtinRandSig{}
	_ builtinFunc = &builtinPowSig{}
	_ builtinFunc = &builtinRoundSig{}
	_ builtinFunc = &builtinConvSig{}
	_ builtinFunc = &builtinCRC32Sig{}
	_ builtinFunc = &builtinSignSig{}
	_ builtinFunc = &builtinSqrtSig{}
	_ builtinFunc = &builtinArithmeticSig{}
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
	_ builtinFunc = &builtinTruncateSig{}
)

type absFunctionClass struct {
	baseFunctionClass
}

func (c *absFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinAbsSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinAbsSig struct {
	baseBuiltinFunc
}

// eval evals a builtinAbsSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
func (b *builtinAbsSig) eval(row []types.Datum) (types.Datum, error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	d := args[0]
	switch d.Kind() {
	case types.KindNull:
		return d, nil
	case types.KindUint64:
		return d, nil
	case types.KindInt64:
		iv := d.GetInt64()
		if iv >= 0 {
			d.SetInt64(iv)
			return d, nil
		}
		d.SetInt64(-iv)
		return d, nil
	default:
		// we will try to convert other types to float
		// TODO: if time has no precision, it will be a integer
		f, err := d.ToFloat64(b.ctx.GetSessionVars().StmtCtx)
		d.SetFloat64(math.Abs(f))
		return d, errors.Trace(err)
	}
}

type ceilFunctionClass struct {
	baseFunctionClass
}

func (c *ceilFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	var (
		bf           baseBuiltinFunc
		sig          builtinFunc
		err          error
		retTp, argTp evalTp
		tpClass      types.TypeClass
	)

	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	tpClass = args[0].GetTypeClass()
	switch tpClass {
	case types.ClassInt, types.ClassDecimal:
		retTp, argTp = tpInt, tpReal
	default:
		retTp, argTp = tpReal, tpReal
	}

	bf, err = newBaseBuiltinFuncWithTp(args, ctx, retTp, argTp)
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch tpClass {
	case types.ClassInt, types.ClassDecimal:
		sig = &builtinCeilIntSig{baseIntBuiltinFunc{bf}}
	default:
		sig = &builtinCeilRealSig{baseRealBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), nil
}

type builtinCeilRealSig struct {
	baseRealBuiltinFunc
}

// evalReal evals a builtinCeilRealSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_ceiling
func (b *builtinCeilRealSig) evalReal(row []types.Datum) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return math.Ceil(val), false, nil
}

type builtinCeilIntSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinCeilIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_ceiling
func (b *builtinCeilIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return int64(math.Ceil(val)), false, nil
}

type floorFunctionClass struct {
	baseFunctionClass
}

func (c *floorFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	var (
		bf           baseBuiltinFunc
		sig          builtinFunc
		err          error
		retTp, argTp evalTp
		tpClass      types.TypeClass
	)

	if err = c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}

	tpClass = args[0].GetTypeClass()
	switch tpClass {
	case types.ClassInt, types.ClassDecimal:
		retTp, argTp = tpInt, tpReal
	default:
		retTp, argTp = tpReal, tpReal
	}

	bf, err = newBaseBuiltinFuncWithTp(args, ctx, retTp, argTp)
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch tpClass {
	case types.ClassInt, types.ClassDecimal:
		sig = &builtinFloorIntSig{baseIntBuiltinFunc{bf}}
	default:
		sig = &builtinFloorRealSig{baseRealBuiltinFunc{bf}}
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

type builtinFloorIntSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinFloorIntSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorIntSig) evalInt(row []types.Datum) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(row, b.ctx.GetSessionVars().StmtCtx)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	return int64(math.Floor(val)), false, nil
}

type logFunctionClass struct {
	baseFunctionClass
}

func (c *logFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	var (
		sig     builtinFunc
		bf      baseBuiltinFunc
		err     error
		argsLen = len(args)
	)

	if argsLen == 1 {
		bf, err = newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	} else {
		bf, err = newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal, tpReal)
	}

	if err != nil {
		return nil, errors.Trace(err)
	}

	if argsLen == 1 {
		sig = &builtinLog1ArgSig{baseRealBuiltinFunc{bf}}
	} else {
		sig = &builtinLog2ArgsSig{baseRealBuiltinFunc{bf}}
	}

	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
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

func (c *log2FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinLog2Sig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
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

func (c *log10FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinLog10Sig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
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

func (c *randFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinRandSig{baseBuiltinFunc: newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt.setSelf(bt), errors.Trace(err)
}

type builtinRandSig struct {
	baseBuiltinFunc
	randGen *rand.Rand
}

// eval evals a builtinRandSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_rand
func (b *builtinRandSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if b.randGen == nil {
		if len(args) == 1 && !args[0].IsNull() {
			seed, err := args[0].ToInt64(b.ctx.GetSessionVars().StmtCtx)
			if err != nil {
				return d, errors.Trace(err)
			}
			b.randGen = rand.New(rand.NewSource(seed))
		} else {
			// If seed is not set, we use current timestamp as seed.
			b.randGen = rand.New(rand.NewSource(time.Now().UnixNano()))
		}
	}
	d.SetFloat64(b.randGen.Float64())
	return d, nil
}

type powFunctionClass struct {
	baseFunctionClass
}

func (c *powFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinPowSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinPowSig struct {
	baseBuiltinFunc
}

// eval evals a builtinPowSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_pow
func (b *builtinPowSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	x, err := args[0].ToFloat64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	y, err := args[1].ToFloat64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	power := math.Pow(x, y)
	if math.IsInf(power, -1) || math.IsInf(power, 1) || math.IsNaN(power) {
		return d, types.ErrOverflow.GenByArgs("DOUBLE", fmt.Sprintf("pow(%s, %s)", strconv.FormatFloat(x, 'f', -1, 64), strconv.FormatFloat(y, 'f', -1, 64)))
	}
	d.SetFloat64(power)
	return d, nil
}

type roundFunctionClass struct {
	baseFunctionClass
}

func (c *roundFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinRoundSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinRoundSig struct {
	baseBuiltinFunc
}

// eval evals a builtinRoundSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
func (b *builtinRoundSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() {
		return
	}
	sc := b.ctx.GetSessionVars().StmtCtx

	frac := 0
	if len(args) == 2 {
		frac64, err1 := args[1].ToInt64(sc)
		if err1 != nil {
			return d, errors.Trace(err1)
		}
		frac = int(frac64)
	}

	if args[0].Kind() == types.KindMysqlDecimal {
		var dec types.MyDecimal
		err = args[0].GetMysqlDecimal().Round(&dec, frac, types.ModeHalfEven)
		if err != nil {
			return d, errors.Trace(err)
		}
		d.SetMysqlDecimal(&dec)
		return d, nil
	}

	x, err := args[0].ToFloat64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	val := types.Round(x, frac)
	switch args[0].Kind() {
	case types.KindInt64:
		d.SetInt64(int64(val))
	case types.KindUint64:
		d.SetUint64(uint64(val))
	default:
		d.SetFloat64(val)
		if frac > 0 {
			d.SetFrac(frac)
		}
	}
	return d, nil
}

type convFunctionClass struct {
	baseFunctionClass
}

func (c *convFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinConvSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinConvSig struct {
	baseBuiltinFunc
}

// eval evals a builtinConvSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_conv
func (b *builtinConvSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	var (
		signed     bool
		negative   bool
		ignoreSign bool
	)
	for _, arg := range args {
		if arg.IsNull() {
			return d, nil
		}
	}
	n, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	fromBase, err := args[1].ToInt64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	toBase, err := args[2].ToInt64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	if fromBase < 0 {
		fromBase = -fromBase
		signed = true
	}
	if toBase < 0 {
		ignoreSign = true
		toBase = -toBase
	}
	if fromBase > 36 || fromBase < 2 || toBase > 36 || toBase < 2 {
		return d, nil
	}
	n = getValidPrefix(strings.TrimSpace(n), fromBase)
	if len(n) == 0 {
		d.SetString("0")
		return d, nil
	}
	if n[0] == '-' {
		negative = true
		n = n[1:]
	}

	val, err := strconv.ParseUint(n, int(fromBase), 64)
	if err != nil {
		return d, types.ErrOverflow.GenByArgs("BIGINT UNSINGED", n)
	}
	// See https://github.com/mysql/mysql-server/blob/5.7/strings/ctype-simple.c#L598
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
	// See https://github.com/mysql/mysql-server/blob/5.7/strings/longlong2str.c#L58
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
	d.SetString(strings.ToUpper(s))
	return d, nil
}

type crc32FunctionClass struct {
	baseFunctionClass
}

func (c *crc32FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinCRC32Sig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinCRC32Sig struct {
	baseBuiltinFunc
}

// eval evals a builtinCRC32Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_crc32
func (b *builtinCRC32Sig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() {
		return d, nil
	}
	x, err := args[0].ToString()
	if err != nil {
		return d, errors.Trace(err)
	}
	r := crc32.ChecksumIEEE([]byte(x))
	d.SetUint64(uint64(r))
	return d, nil
}

type signFunctionClass struct {
	baseFunctionClass
}

func (c *signFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinSignSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinSignSig struct {
	baseBuiltinFunc
}

// eval evals a builtinSignSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sign
func (b *builtinSignSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() {
		return d, nil
	}
	cmp, err := args[0].CompareDatum(b.ctx.GetSessionVars().StmtCtx, types.NewIntDatum(0))
	d.SetInt64(int64(cmp))
	if err != nil {
		return d, errors.Trace(err)
	}
	return d, nil
}

type sqrtFunctionClass struct {
	baseFunctionClass
}

func (c *sqrtFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinSqrtSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinSqrtSig struct {
	baseBuiltinFunc
}

// eval evals a builtinSqrtSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sqrt
func (b *builtinSqrtSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() {
		return d, nil
	}

	sc := b.ctx.GetSessionVars().StmtCtx
	f, err := args[0].ToFloat64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	// negative value does not have any square root in rational number
	// Need return null directly.
	if f < 0 {
		return d, nil
	}

	d.SetFloat64(math.Sqrt(f))
	return
}

type arithmeticFunctionClass struct {
	baseFunctionClass

	op opcode.Op
}

func (c *arithmeticFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinArithmeticSig{newBaseBuiltinFunc(args, ctx), c.op}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
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
	case opcode.Plus:
		return types.ComputePlus(a, b)
	case opcode.Minus:
		return types.ComputeMinus(a, b)
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

type acosFunctionClass struct {
	baseFunctionClass
}

func (c *acosFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinAcosSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
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

func (c *asinFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinAsinSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
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

func (c *atanFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	var (
		sig     builtinFunc
		bf      baseBuiltinFunc
		err     error
		argsLen = len(args)
	)

	if argsLen == 1 {
		bf, err = newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	} else {
		bf, err = newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal, tpReal)
	}

	if err != nil {
		return nil, errors.Trace(err)
	}

	if argsLen == 1 {
		sig = &builtinAtan1ArgSig{baseRealBuiltinFunc{bf}}
	} else {
		sig = &builtinAtan2ArgsSig{baseRealBuiltinFunc{bf}}
	}

	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
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

func (c *cosFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinCosSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
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

func (c *cotFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinCotSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinCotSig struct {
	baseBuiltinFunc
}

// eval evals a builtinCotSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_cot
func (b *builtinCotSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() {
		return args[0], nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	degree, err := args[0].ToFloat64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	sin := math.Sin(degree)
	cos := math.Cos(degree)
	degreeString, _ := args[0].ToString()
	if sin == 0 {
		return d, errors.New("Value is out of range of cot(" + degreeString + ")")
	}
	// Set the result to be of type float64
	d.SetFloat64(cos / sin)
	return d, nil
}

type degreesFunctionClass struct {
	baseFunctionClass
}

func (c *degreesFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinDegreesSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
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

func (c *expFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinExpSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinExpSig struct {
	baseBuiltinFunc
}

// eval evals a builtinExpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_exp
func (b *builtinExpSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}

	arg := args[0]
	if arg.IsNull() {
		return d, nil
	}

	num, err := arg.ToFloat64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}

	d.SetFloat64(math.Exp(num))
	return d, nil
}

type piFunctionClass struct {
	baseFunctionClass
}

func (c *piFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinPISig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinPISig struct {
	baseBuiltinFunc
}

// eval evals a builtinPISig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_pi
func (b *builtinPISig) eval(row []types.Datum) (d types.Datum, err error) {
	d.SetFloat64(math.Pi)
	return d, nil
}

type radiansFunctionClass struct {
	baseFunctionClass
}

func (c *radiansFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinRadiansSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinRadiansSig struct {
	baseBuiltinFunc
}

// eval evals a builtinRadiansSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_radians
func (b *builtinRadiansSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	arg := args[0]
	if arg.IsNull() {
		return d, nil
	}
	degree, err := arg.ToFloat64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetFloat64(degree * math.Pi / 180)
	return d, nil
}

type sinFunctionClass struct {
	baseFunctionClass
}

func (c *sinFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinSinSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
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

func (c *tanFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpReal, tpReal)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sig := &builtinTanSig{baseRealBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
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

func (c *truncateFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinTruncateSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinTruncateSig struct {
	baseBuiltinFunc
}

// eval evals a builtinTruncateSig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if len(args) != 2 || args[0].IsNull() || args[1].IsNull() {
		return
	}
	sc := b.ctx.GetSessionVars().StmtCtx

	// Get the fraction as Int.
	frac64, err1 := args[1].ToInt64(sc)
	if err1 != nil {
		return d, errors.Trace(err1)
	}
	frac := int(frac64)

	// The number is a decimal, run decimal.Round(number, fraction, 9).
	if args[0].Kind() == types.KindMysqlDecimal {
		var dec types.MyDecimal
		err = args[0].GetMysqlDecimal().Round(&dec, frac, types.ModeTruncate)
		if err != nil {
			return d, errors.Trace(err)
		}
		d.SetMysqlDecimal(&dec)
		return d, nil
	}

	// The number is a float, run math.Trunc.
	x, err := args[0].ToFloat64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	// Get the truncate.
	val := types.Truncate(x, frac)

	// Return result as Round does.
	switch args[0].Kind() {
	case types.KindInt64:
		d.SetInt64(int64(val))
	case types.KindUint64:
		d.SetUint64(uint64(val))
	default:
		d.SetFloat64(val)
		if frac > 0 {
			d.SetFrac(frac)
		}
	}
	return d, nil
}
