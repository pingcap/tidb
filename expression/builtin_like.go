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
	_ builtinFunc = &builtinRegexpSig{}
)

type likeFunctionClass struct {
	baseFunctionClass
}

func (c *likeFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	argTp := []evalTp{tpString, tpString}
	if len(args) == 3 {
		argTp = append(argTp, tpInt)
	}
	bf, err := newBaseBuiltinFuncWithTp(args, ctx, tpInt, argTp...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bf.tp.Flen = 1
	sig := &builtinLikeSig{baseIntBuiltinFunc{bf}}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
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

func (c *regexpFunctionClass) getFunction(args []Expression, ctx context.Context) (builtinFunc, error) {
	sig := &builtinRegexpSig{newBaseBuiltinFunc(args, ctx)}
	return sig.setSelf(sig), errors.Trace(c.verifyArgs(args))
}

type builtinRegexpSig struct {
	baseBuiltinFunc
}

// eval evals a builtinRegexpSig.
// See https://dev.mysql.com/doc/refman/5.7/en/regexp.html#operator_regexp
func (b *builtinRegexpSig) eval(row []types.Datum) (d types.Datum, err error) {
	args, err := b.evalArgs(row)
	if err != nil {
		return types.Datum{}, errors.Trace(err)
	}
	// TODO: We don't need to compile pattern if it has been compiled or it is static.
	if args[0].IsNull() || args[1].IsNull() {
		return
	}

	targetStr, err := args[0].ToString()
	if err != nil {
		return d, errors.Errorf("non-string Expression in LIKE: %v (Value of type %T)", args[0], args[0])
	}
	patternStr, err := args[1].ToString()
	if err != nil {
		return d, errors.Errorf("non-string Expression in LIKE: %v (Value of type %T)", args[1], args[1])
	}
	re, err := regexp.Compile(patternStr)
	if err != nil {
		return d, errors.Trace(err)
	}
	d.SetInt64(boolToInt64(re.MatchString(targetStr)))
	return
}
