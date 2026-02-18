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
	"hash/crc32"
	"math"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tipb/go-tipb"
)

type logFunctionClass struct {
	baseFunctionClass
}

func (c *logFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var (
		sig     builtinFunc
		bf      baseBuiltinFunc
		argsLen = len(args)
	)

	var err error
	if argsLen == 1 {
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
		if err != nil {
			return nil, err
		}
	} else {
		bf, err = newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal, types.ETReal)
		if err != nil {
			return nil, err
		}
	}

	if argsLen == 1 {
		sig = &builtinLog1ArgSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Log1Arg)
	} else {
		sig = &builtinLog2ArgsSig{bf}
		sig.setPbCode(tipb.ScalarFuncSig_Log2Args)
	}

	return sig, nil
}

type builtinLog1ArgSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLog1ArgSig) Clone() builtinFunc {
	newSig := &builtinLog1ArgSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog1ArgSig, corresponding to log(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log
func (b *builtinLog1ArgSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val <= 0 {
		tc := typeCtx(ctx)
		tc.AppendWarning(ErrInvalidArgumentForLogarithm)
		return 0, true, nil
	}
	return math.Log(val), false, nil
}

type builtinLog2ArgsSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLog2ArgsSig) Clone() builtinFunc {
	newSig := &builtinLog2ArgsSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog2ArgsSig, corresponding to log(b, x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log
func (b *builtinLog2ArgsSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val1, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	val2, isNull, err := b.args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	if val1 <= 0 || val1 == 1 || val2 <= 0 {
		tc := typeCtx(ctx)
		tc.AppendWarning(ErrInvalidArgumentForLogarithm)
		return 0, true, nil
	}

	return math.Log(val2) / math.Log(val1), false, nil
}

type log2FunctionClass struct {
	baseFunctionClass
}

func (c *log2FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinLog2Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Log2)
	return sig, nil
}

type builtinLog2Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLog2Sig) Clone() builtinFunc {
	newSig := &builtinLog2Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog2Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log2
func (b *builtinLog2Sig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val <= 0 {
		tc := typeCtx(ctx)
		tc.AppendWarning(ErrInvalidArgumentForLogarithm)
		return 0, true, nil
	}
	return math.Log2(val), false, nil
}

type log10FunctionClass struct {
	baseFunctionClass
}

func (c *log10FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinLog10Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Log10)
	return sig, nil
}

type builtinLog10Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinLog10Sig) Clone() builtinFunc {
	newSig := &builtinLog10Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a builtinLog10Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_log10
func (b *builtinLog10Sig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val <= 0 {
		tc := typeCtx(ctx)
		tc.AppendWarning(ErrInvalidArgumentForLogarithm)
		return 0, true, nil
	}
	return math.Log10(val), false, nil
}

type randFunctionClass struct {
	baseFunctionClass
}

func (c *randFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	var sig builtinFunc
	var argTps []types.EvalType
	if len(args) > 0 {
		argTps = []types.EvalType{types.ETInt}
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, argTps...)
	if err != nil {
		return nil, err
	}
	bt := bf
	if len(args) == 0 {
		sig = &builtinRandSig{bt, ctx.Rng()}
		sig.setPbCode(tipb.ScalarFuncSig_Rand)
	} else if _, isConstant := args[0].(*Constant); isConstant {
		// According to MySQL manual:
		// If an integer argument N is specified, it is used as the seed value:
		// With a constant initializer argument, the seed is initialized once
		// when the statement is prepared, prior to execution.
		seed, isNull, err := args[0].EvalInt(ctx.GetEvalCtx(), chunk.Row{})
		if err != nil {
			return nil, err
		}
		if isNull {
			// When the seed is null we need to use 0 as the seed.
			// The behavior same as MySQL.
			seed = 0
		}
		sig = &builtinRandSig{bt, mathutil.NewWithSeed(seed)}
		sig.setPbCode(tipb.ScalarFuncSig_Rand)
	} else {
		sig = &builtinRandWithSeedFirstGenSig{bt}
		sig.setPbCode(tipb.ScalarFuncSig_RandWithSeedFirstGen)
	}
	return sig, nil
}

type builtinRandSig struct {
	baseBuiltinFunc
	mysqlRng *mathutil.MysqlRng
}

func (b *builtinRandSig) Clone() builtinFunc {
	newSig := &builtinRandSig{mysqlRng: b.mysqlRng}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals RAND().
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_rand
func (b *builtinRandSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	res := b.mysqlRng.Gen()
	return res, false, nil
}

type builtinRandWithSeedFirstGenSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRandWithSeedFirstGenSig) Clone() builtinFunc {
	newSig := &builtinRandWithSeedFirstGenSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals RAND(N).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_rand
func (b *builtinRandWithSeedFirstGenSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	seed, isNull, err := b.args[0].EvalInt(ctx, row)
	if err != nil {
		return 0, true, err
	}
	// b.args[0] is promised to be a non-constant(such as a column name) in
	// builtinRandWithSeedFirstGenSig, the seed is initialized with the value for each
	// invocation of RAND().
	var rng *mathutil.MysqlRng
	if !isNull {
		rng = mathutil.NewWithSeed(seed)
	} else {
		rng = mathutil.NewWithSeed(0)
	}
	return rng.Gen(), false, nil
}

type powFunctionClass struct {
	baseFunctionClass
}

func (c *powFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinPowSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Pow)
	return sig, nil
}

type builtinPowSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinPowSig) Clone() builtinFunc {
	newSig := &builtinPowSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals POW(x, y).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_pow
func (b *builtinPowSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	x, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	y, isNull, err := b.args[1].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	power := math.Pow(x, y)
	if math.IsInf(power, -1) || math.IsInf(power, 1) || math.IsNaN(power) {
		return 0, false, types.ErrOverflow.GenWithStackByArgs("DOUBLE", fmt.Sprintf("pow(%s, %s)", strconv.FormatFloat(x, 'f', -1, 64), strconv.FormatFloat(y, 'f', -1, 64)))
	}
	return power, false, nil
}

type roundFunctionClass struct {
	baseFunctionClass
}

type convFunctionClass struct {
	baseFunctionClass
}

func (c *convFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt, types.ETInt)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(64)
	sig := &builtinConvSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Conv)
	return sig, nil
}

type builtinConvSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinConvSig) Clone() builtinFunc {
	newSig := &builtinConvSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals CONV(N,from_base,to_base).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_conv.
func (b *builtinConvSig) evalString(ctx EvalContext, row chunk.Row) (res string, isNull bool, err error) {
	var str string
	switch x := b.args[0].(type) {
	case *Constant:
		if x.Value.Kind() == types.KindBinaryLiteral {
			datum, err := x.Eval(ctx, row)
			if err != nil {
				return "", false, err
			}
			str = datum.GetBinaryLiteral().ToBitLiteralString(true)
		}
	case *ScalarFunction:
		if x.FuncName.L == ast.Cast {
			arg0 := x.GetArgs()[0]
			if arg0.GetType(ctx).Hybrid() || IsBinaryLiteral(arg0) {
				str, isNull, err = arg0.EvalString(ctx, row)
				if isNull || err != nil {
					return str, isNull, err
				}
				d := types.NewStringDatum(str)
				str = d.GetBinaryLiteral().ToBitLiteralString(true)
			}
		}
	}
	fromBase, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}

	toBase, isNull, err := b.args[2].EvalInt(ctx, row)
	if isNull || err != nil {
		return res, isNull, err
	}
	if len(str) == 0 {
		str, isNull, err = b.args[0].EvalString(ctx, row)
		if isNull || err != nil {
			return res, isNull, err
		}
	} else {
		str, isNull, err = b.conv(str[2:], 2, fromBase)
		if err != nil {
			return str, isNull, err
		}
	}
	return b.conv(str, fromBase, toBase)
}
func (b *builtinConvSig) conv(str string, fromBase, toBase int64) (res string, isNull bool, err error) {
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

	str = getValidPrefix(strings.TrimSpace(str), fromBase)
	if len(str) == 0 {
		return "0", false, nil
	}

	if str[0] == '-' {
		negative = true
		str = str[1:]
	}

	val, err := strconv.ParseUint(str, int(fromBase), 64)
	if err != nil {
		return res, false, types.ErrOverflow.GenWithStackByArgs("BIGINT UNSIGNED", str)
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

func (c *crc32FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(10)
	bf.tp.AddFlag(mysql.UnsignedFlag)
	sig := &builtinCRC32Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_CRC32)
	return sig, nil
}

type builtinCRC32Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCRC32Sig) Clone() builtinFunc {
	newSig := &builtinCRC32Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a CRC32(expr).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_crc32
func (b *builtinCRC32Sig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	x, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	r := crc32.ChecksumIEEE([]byte(x))
	return int64(r), false, nil
}

type signFunctionClass struct {
	baseFunctionClass
}

func (c *signFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinSignSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Sign)
	return sig, nil
}

type builtinSignSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSignSig) Clone() builtinFunc {
	newSig := &builtinSignSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals SIGN(v).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sign
func (b *builtinSignSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val > 0 {
		return 1, false, nil
	} else if val == 0 {
		return 0, false, nil
	}
	return -1, false, nil
}

type sqrtFunctionClass struct {
	baseFunctionClass
}

func (c *sqrtFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETReal, types.ETReal)
	if err != nil {
		return nil, err
	}
	sig := &builtinSqrtSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Sqrt)
	return sig, nil
}

type builtinSqrtSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSqrtSig) Clone() builtinFunc {
	newSig := &builtinSqrtSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalReal evals a SQRT(x).
// See https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_sqrt
func (b *builtinSqrtSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	val, isNull, err := b.args[0].EvalReal(ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val < 0 {
		return 0, true, nil
	}
	return math.Sqrt(val), false, nil
}
