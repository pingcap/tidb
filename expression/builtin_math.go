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
	"hash/crc32"
	"math"
	"math/rand"
	"strconv"
	"strings"

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
	_ builtinFunc = &builtinCeilSig{}
	_ builtinFunc = &builtinFloorSig{}
	_ builtinFunc = &builtinLogSig{}
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
	_ builtinFunc = &builtinAtanSig{}
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
	return &builtinAbsSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinAbsSig struct {
	baseBuiltinFunc
}

// See http://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_abs
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
	return &builtinCeilSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinCeilSig struct {
	baseBuiltinFunc
}

// See http://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_ceiling
func (b *builtinCeilSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() ||
		args[0].Kind() == types.KindUint64 || args[0].Kind() == types.KindInt64 {
		return args[0], nil
	}

	f, err := args[0].ToFloat64(b.ctx.GetSessionVars().StmtCtx)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetFloat64(math.Ceil(f))
	return
}

type floorFunctionClass struct {
	baseFunctionClass
}

func (c *floorFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinFloorSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinFloorSig struct {
	baseBuiltinFunc
}

// See http://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_floor
func (b *builtinFloorSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() ||
		args[0].Kind() == types.KindUint64 || args[0].Kind() == types.KindInt64 {
		return args[0], nil
	}

	sc := b.ctx.GetSessionVars().StmtCtx
	f, err := args[0].ToFloat64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	d.SetFloat64(math.Floor(f))
	return
}

type logFunctionClass struct {
	baseFunctionClass
}

func (c *logFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinLogSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinLogSig struct {
	baseBuiltinFunc
}

// See http://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log
func (b *builtinLogSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	sc := b.ctx.GetSessionVars().StmtCtx

	switch len(args) {
	case 1:
		x, err := args[0].ToFloat64(sc)
		if err != nil {
			return d, errors.Trace(err)
		}

		if x <= 0 {
			return d, nil
		}

		d.SetFloat64(math.Log(x))
		return d, nil
	case 2:
		b, err := args[0].ToFloat64(sc)
		if err != nil {
			return d, errors.Trace(err)
		}

		x, err := args[1].ToFloat64(sc)
		if err != nil {
			return d, errors.Trace(err)
		}

		if b <= 1 || x <= 0 {
			return d, nil
		}

		d.SetFloat64(math.Log(x) / math.Log(b))
		return d, nil
	}
	return
}

type log2FunctionClass struct {
	baseFunctionClass
}

func (c *log2FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinLog2Sig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinLog2Sig struct {
	baseBuiltinFunc
}

// See http://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log2
func (b *builtinLog2Sig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	x, err := args[0].ToFloat64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	if x <= 0 {
		return
	}

	d.SetFloat64(math.Log2(x))
	return
}

type log10FunctionClass struct {
	baseFunctionClass
}

func (c *log10FunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinLog10Sig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinLog10Sig struct {
	baseBuiltinFunc
}

// See http://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log10
func (b *builtinLog10Sig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	x, err := args[0].ToFloat64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	if x <= 0 {
		return
	}

	d.SetFloat64(math.Log10(x))
	return

}

type randFunctionClass struct {
	baseFunctionClass
}

func (c *randFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	err := errors.Trace(c.verifyArgs(args))
	bt := &builtinRandSig{newBaseBuiltinFunc(args, ctx)}
	bt.deterministic = false
	return bt, errors.Trace(err)
}

type builtinRandSig struct {
	baseBuiltinFunc
}

// See http://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_rand
func (b *builtinRandSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if len(args) == 1 && !args[0].IsNull() {
		seed, err := args[0].ToInt64(b.ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return d, errors.Trace(err)
		}
		rand.Seed(seed)
	}
	d.SetFloat64(rand.Float64())
	return d, nil
}

type powFunctionClass struct {
	baseFunctionClass
}

func (c *powFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinPowSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinPowSig struct {
	baseBuiltinFunc
}

// See http://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_pow
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
	d.SetFloat64(math.Pow(x, y))
	return d, nil
}

type roundFunctionClass struct {
	baseFunctionClass
}

func (c *roundFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinRoundSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinRoundSig struct {
	baseBuiltinFunc
}

// See http://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
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
		err = args[0].GetMysqlDecimal().Round(&dec, frac)
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
	return &builtinConvSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinConvSig struct {
	baseBuiltinFunc
}

// See http://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_conv
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
		return d, errors.Trace(types.ErrOverflow)
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
	return &builtinCRC32Sig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinCRC32Sig struct {
	baseBuiltinFunc
}

// See http://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_crc32
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
	return &builtinSignSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinSignSig struct {
	baseBuiltinFunc
}

// See http://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sign
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
	return &builtinSqrtSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinSqrtSig struct {
	baseBuiltinFunc
}

// See http://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sqrt
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
	return &builtinArithmeticSig{newBaseBuiltinFunc(args, ctx), c.op}, errors.Trace(c.verifyArgs(args))
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
	return &builtinAcosSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinAcosSig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_acos
func (b *builtinAcosSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() {
		return args[0], nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	f, err := args[0].ToFloat64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}
	if f < -1 || f > 1 {
		return d, nil
	}

	d.SetFloat64(math.Acos(f))
	return
}

type asinFunctionClass struct {
	baseFunctionClass
}

func (c *asinFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinAsinSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinAsinSig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_asin
func (b *builtinAsinSig) eval(row []types.Datum) (d types.Datum, err error) {
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
	if f < -1 || f > 1 {
		return d, nil
	}

	d.SetFloat64(math.Asin(f))
	return
}

type atanFunctionClass struct {
	baseFunctionClass
}

func (c *atanFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinAtanSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinAtanSig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_atan
func (b *builtinAtanSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return d, errors.Trace(err)
	}
	if args[0].IsNull() {
		return d, nil
	}
	sc := b.ctx.GetSessionVars().StmtCtx
	y, err := args[0].ToFloat64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	if len(args) == 2 {
		x, err := args[1].ToFloat64(sc)
		if err != nil {
			d.SetFloat64(math.Atan(y))
			// FIXME: Trigger a warning perhaps.
			err = nil
		} else {
			d.SetFloat64(math.Atan2(y, x))
		}
	} else {
		d.SetFloat64(math.Atan(y))
	}

	return
}

type cosFunctionClass struct {
	baseFunctionClass
}

func (c *cosFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinCosSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinCosSig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_cos
func (b *builtinCosSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("cos")
}

type cotFunctionClass struct {
	baseFunctionClass
}

func (c *cotFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinCotSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinCotSig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_cot
func (b *builtinCotSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("cot")
}

type degreesFunctionClass struct {
	baseFunctionClass
}

func (c *degreesFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinDegreesSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinDegreesSig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_degrees
func (b *builtinDegreesSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}

	if args[0].IsNull() {
		d.SetNull()
		return d, nil
	}

	sc := b.ctx.GetSessionVars().StmtCtx
	x, err := args[0].ToFloat64(sc)
	if err != nil {
		return d, errors.Trace(err)
	}

	d.SetFloat64(x * 180 / math.Pi)
	return d, nil
}

type expFunctionClass struct {
	baseFunctionClass
}

func (c *expFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinExpSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinExpSig struct {
	baseBuiltinFunc
}

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
	return &builtinPISig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinPISig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_pi
func (b *builtinPISig) eval(row []types.Datum) (d types.Datum, err error) {
	d.SetFloat64(math.Pi)
	return d, nil
}

type radiansFunctionClass struct {
	baseFunctionClass
}

func (c *radiansFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinRadiansSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinRadiansSig struct {
	baseBuiltinFunc
}

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
	return &builtinSinSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinSinSig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sin
func (b *builtinSinSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("sin")
}

type tanFunctionClass struct {
	baseFunctionClass
}

func (c *tanFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinTanSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinTanSig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_tan
func (b *builtinTanSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("tan")
}

type truncateFunctionClass struct {
	baseFunctionClass
}

func (c *truncateFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	return &builtinTruncateSig{newBaseBuiltinFunc(args, ctx)}, errors.Trace(c.verifyArgs(args))
}

type builtinTruncateSig struct {
	baseBuiltinFunc
}

// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_truncate
func (b *builtinTruncateSig) eval(row []types.Datum) (d types.Datum, err error) {
	return d, errFunctionNotExists.GenByArgs("truncate")
}
