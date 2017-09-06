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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tidb/util/types"
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

func (c *likeFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	argTp := []evalTp{tpString, tpString}
	if len(args) == 3 {
		argTp = append(argTp, tpInt)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, argTp...)
	bf.tp.Flen = 1
	sig := &builtinLikeSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), nil
}

type builtinLikeSig struct {
	baseIntBuiltinFunc
}

// evalInt evals a builtinLikeSig.
// See https://dev.mysql.com/doc/refman/5.7/en/string-comparison-functions.html#operator_like
func (b *builtinLikeSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx
	valStr, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}

	// TODO: We don't need to compile pattern if it has been compiled or it is static.
	patternStr, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return 0, isNull, errors.Trace(err)
	}
	var escape byte = '\\'
	// If this function is called by mock tikv, the args len will be 2 and the escape will be `\\`.
	// TODO: Remove this after remove old evaluator logic.
	if len(b.args) >= 3 {
		val, isNull, err := b.args[2].EvalInt(row, sc)
		if isNull || err != nil {
			return 0, isNull, errors.Trace(err)
		}
		escape = byte(val)
	}
	patChars, patTypes := stringutil.CompilePattern(patternStr, escape)
	match := stringutil.DoMatch(valStr, patChars, patTypes)
	return boolToInt64(match), false, nil
}

type regexpFunctionClass struct {
	baseFunctionClass
}

func (c *regexpFunctionClass) getFunction(ctx context.Context, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, errors.Trace(err)
	}
	bf := newBaseBuiltinFuncWithTp(args, ctx, tpInt, tpString, tpString)
	bf.tp.Flen = 1
	var sig builtinFunc
	if types.IsBinaryStr(args[0].GetType()) {
		sig = &builtinRegexpBinarySig{baseIntBuiltinFunc{bf}}
	} else {
		sig = &builtinRegexpSig{baseIntBuiltinFunc{bf}}
	}
	return sig.setSelf(sig), nil
}

type builtinRegexpBinarySig struct {
	baseIntBuiltinFunc
}

func (b *builtinRegexpBinarySig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	expr, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return 0, true, errors.Trace(err)
	}

	pat, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return 0, true, errors.Trace(err)
	}

	// TODO: We don't need to compile pattern if it has been compiled or it is static.
	re, err := regexp.Compile(pat)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	return boolToInt64(re.MatchString(expr)), false, nil
}

type builtinRegexpSig struct {
	baseIntBuiltinFunc
}

// evalInt evals `expr REGEXP pat`, or `expr RLIKE pat`.
// See https://dev.mysql.com/doc/refman/5.7/en/regexp.html#operator_regexp
func (b *builtinRegexpSig) evalInt(row []types.Datum) (int64, bool, error) {
	sc := b.ctx.GetSessionVars().StmtCtx

	expr, isNull, err := b.args[0].EvalString(row, sc)
	if isNull || err != nil {
		return 0, true, errors.Trace(err)
	}

	pat, isNull, err := b.args[1].EvalString(row, sc)
	if isNull || err != nil {
		return 0, true, errors.Trace(err)
	}

	// TODO: We don't need to compile pattern if it has been compiled or it is static.
	re, err := regexp.Compile("(?i)" + pat)
	if err != nil {
		return 0, true, errors.Trace(err)
	}
	return boolToInt64(re.MatchString(expr)), false, nil
}
