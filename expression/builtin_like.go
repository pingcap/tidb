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
	"regexp"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &likeFunctionClass{}
	_ functionClass = &regexpFunctionClass{}
)

var (
	_ builtinFunc = &builtinLikeSig{}
	_ builtinFunc = &builtinRegexpBinarySig{}
	_ builtinFunc = &builtinRegexpSig{}
)

type likeFunctionClass struct {
	baseFunctionClass
}

func (c *likeFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	argTp := []types.EvalType{types.ETString, types.ETString, types.ETInt}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, argTp...)
	bf.tp.Flen = 1
	sig := &builtinLikeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_LikeSig)
	return sig, nil
}

type builtinLikeSig struct {
	baseBuiltinFunc
}

func (b *builtinLikeSig) Clone() builtinFunc {
	newSig := &builtinLikeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals a builtinLikeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html#operator_like
// NOTE: Currently tikv's like function is case sensitive, so we keep its behavior here.
func (b *builtinLikeSig) evalInt(row chunk.Row) (int64, bool, error) {
	valStr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}

	// TODO: We don't need to compile pattern if it has been compiled or it is static.
	patternStr, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	val, isNull, err := b.args[2].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return 0, isNull, err
	}
	escape := byte(val)
	patChars, patTypes := stringutil.CompilePattern(patternStr, escape)
	match := stringutil.DoMatch(valStr, patChars, patTypes)
	return boolToInt64(match), false, nil
}

type regexpFunctionClass struct {
	baseFunctionClass
}

func (c *regexpFunctionClass) getFunction(ctx sessionctx.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf := newBaseBuiltinFuncWithTp(ctx, args, types.ETInt, types.ETString, types.ETString)
	bf.tp.Flen = 1
	var sig builtinFunc
	if types.IsBinaryStr(args[0].GetType()) || types.IsBinaryStr(args[1].GetType()) {
		sig = &builtinRegexpBinarySig{builtinRegexpSharedSig{baseBuiltinFunc: bf}}
	} else {
		sig = &builtinRegexpSig{builtinRegexpSharedSig{baseBuiltinFunc: bf}}
	}
	return sig, nil
}

type regexpCompiler interface {
	compile(pat string) (*regexp.Regexp, error)
}

type builtinRegexpSharedSig struct {
	baseBuiltinFunc
	memoizedRegexp *regexp.Regexp
	memoizedErr    error
	// TODO: By having this struct store a compile() func pointer that is
	// customized for builtinRegexpBinarySig and builtinRegexpSig, we can
	// remove the regexpCompiler interface and dedupe Clone()/evalInt()/vecEvalInt().
}

func (b *builtinRegexpSharedSig) clone(from *builtinRegexpSharedSig) {
	b.cloneFrom(&from.baseBuiltinFunc)
	if from.memoizedRegexp != nil {
		b.memoizedRegexp = from.memoizedRegexp.Copy()
	}
	b.memoizedErr = from.memoizedErr
}

type builtinRegexpBinarySig struct {
	builtinRegexpSharedSig
}

func (b *builtinRegexpBinarySig) Clone() builtinFunc {
	newSig := &builtinRegexpBinarySig{}
	newSig.clone(&b.builtinRegexpSharedSig)
	return newSig
}

func (b *builtinRegexpBinarySig) evalInt(row chunk.Row) (int64, bool, error) {
	// TODO: this can also be moved into builtinRegexpSharedSig
	expr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	pat, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	// TODO: We don't need to compile pattern if it has been compiled or it is static.
	re, err := b.compile(pat)
	if err != nil {
		return 0, true, ErrRegexp.GenWithStackByArgs(err.Error())
	}
	return boolToInt64(re.MatchString(expr)), false, nil
}

func (b *builtinRegexpBinarySig) compile(pat string) (*regexp.Regexp, error) {
	return regexp.Compile(pat)
}

type builtinRegexpSig struct {
	builtinRegexpSharedSig
}

func (b *builtinRegexpSig) Clone() builtinFunc {
	newSig := &builtinRegexpSig{}
	newSig.clone(&b.builtinRegexpSharedSig)
	return newSig
}

// evalInt evals `expr REGEXP pat`, or `expr RLIKE pat`.
// See https://dev.mysql.com/doc/refman/5.7/en/regexp.html#operator_regexp
func (b *builtinRegexpSig) evalInt(row chunk.Row) (int64, bool, error) {
	// TODO: this can also be moved into builtinRegexpSharedSig
	expr, isNull, err := b.args[0].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	pat, isNull, err := b.args[1].EvalString(b.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}

	// TODO: We don't need to compile pattern if it has been compiled or it is static.
	re, err := b.compile(pat)
	if err != nil {
		return 0, true, ErrRegexp.GenWithStackByArgs(err.Error())
	}
	return boolToInt64(re.MatchString(expr)), false, nil
}

func (b *builtinRegexpSig) compile(pat string) (*regexp.Regexp, error) {
	return regexp.Compile("(?i)" + pat)
}
